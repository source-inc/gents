use std::collections::BTreeMap;
use std::io::Read;

use anyhow::{Context, Result};
use gents::graphql::escape_graphql_string;
use gents::Collection;
use serde_json::Value;

use crate::config_bundle::{sanitize_import_document, select_apply_collection_docs};
#[cfg(test)]
use crate::config_writes::ConfigAccess;
use crate::config_writes::{
    write_event_trigger_document, write_schedule_document, write_task_document, ConfigApplyTxn,
    ExistingDocumentRef,
};
use crate::desired_state;
use crate::desired_state::DesiredApplyBundle;
use crate::shared::{ConfigApplyCounts, ConfigExportBundle};
use crate::{
    extract_mutation_doc_id, graphql_input_literal, graphql_string_list_literal,
    CONFIG_EXPORT_FORMAT, CONFIG_EXPORT_FORMAT_V1,
};

#[cfg(test)]
#[path = "../../gents/src/lean_vocab_test/support.rs"]
mod lean_vocab_test;

const CONFIG_IMPORT_BATCH_SIZE: usize = 50;

const CONFIG_APPLY_ORDER: [Collection; 13] = [
    Collection::PeerPairingDesired,
    Collection::InferenceBackend,
    Collection::InferenceProfile,
    Collection::ToolServiceRegistry,
    Collection::DatastoreToolSurface,
    Collection::ToolSelection,
    Collection::Skill,
    Collection::AgentBehavior,
    Collection::ProjectionAcpBinding,
    Collection::Task,
    Collection::Schedule,
    Collection::EventTrigger,
    Collection::AgentPrincipal,
];

const CONFIG_PRUNE_ORDER: [Collection; 13] = [
    Collection::AgentPrincipal,
    Collection::EventTrigger,
    Collection::Schedule,
    Collection::Task,
    Collection::ProjectionAcpBinding,
    Collection::AgentBehavior,
    Collection::Skill,
    Collection::ToolSelection,
    Collection::DatastoreToolSurface,
    Collection::ToolServiceRegistry,
    Collection::InferenceProfile,
    Collection::InferenceBackend,
    Collection::PeerPairingDesired,
];

#[cfg(test)]
pub(crate) const CONFIG_APPLY_ORDER_FOR_TESTS: &[Collection] = &CONFIG_APPLY_ORDER;
#[cfg(test)]
pub(crate) const CONFIG_PRUNE_ORDER_FOR_TESTS: &[Collection] = &CONFIG_PRUNE_ORDER;

#[derive(Debug, Clone)]
struct PreparedImportDocument {
    unique_value: String,
    add_doc: Value,
    update_doc: Option<Value>,
}

#[derive(Debug, Clone)]
struct AliasedMutationField {
    alias: String,
    field: String,
}

pub(crate) fn read_config_import_bundle(
    path: Option<&std::path::Path>,
) -> Result<ConfigExportBundle> {
    let contents = match path {
        Some(path) => std::fs::read_to_string(path)
            .with_context(|| format!("reading config import from {}", path.display()))?,
        None => {
            let mut contents = String::new();
            std::io::stdin()
                .read_to_string(&mut contents)
                .context("reading config import from stdin")?;
            contents
        }
    };
    let mut bundle: ConfigExportBundle =
        serde_json::from_str(&contents).context("decoding config import JSON")?;
    migrate_config_import_bundle(&mut bundle);
    Ok(bundle)
}

pub(crate) fn validate_config_import_bundle(bundle: &ConfigExportBundle) -> Result<()> {
    if !matches!(
        bundle.format.as_str(),
        CONFIG_EXPORT_FORMAT | CONFIG_EXPORT_FORMAT_V1
    ) {
        anyhow::bail!(
            "unsupported config import format {}; expected {}",
            bundle.format,
            CONFIG_EXPORT_FORMAT
        );
    }
    if bundle.agent_did.trim().is_empty() {
        anyhow::bail!("config import is missing agent_did");
    }
    Ok(())
}

pub(crate) fn migrate_config_import_bundle(bundle: &mut ConfigExportBundle) {
    for selection in &mut bundle.tool_selections {
        if let Some(object) = selection.as_object_mut() {
            desired_state::strip_retired_tool_selection_fields(object);
        }
    }
    for backend in &mut bundle.inference_backends {
        if let Some(object) = backend.as_object_mut() {
            desired_state::strip_deprecated_inference_backend_fields(object);
        }
    }
    if bundle.format == CONFIG_EXPORT_FORMAT_V1 {
        bundle.format = CONFIG_EXPORT_FORMAT.to_string();
    }
}

pub(crate) async fn apply_import_collection(
    txn: &ConfigApplyTxn<'_>,
    collection_name: &str,
    unique_field: &str,
    docs: &[Value],
    override_existing: bool,
) -> Result<usize> {
    let prepared =
        prepare_import_documents(collection_name, unique_field, docs, override_existing)?;
    if prepared.is_empty() {
        return Ok(0);
    }

    if override_existing && collection_name == "PeerPairingDesired" {
        apply_manifest_pairing_documents(txn, &prepared).await?;
    } else if override_existing && uses_custom_apply_writer(collection_name) {
        apply_custom_override_collection_batched(txn, collection_name, unique_field, &prepared)
            .await?;
    } else {
        apply_generic_import_collection_batched(
            txn,
            collection_name,
            unique_field,
            &prepared,
            override_existing,
        )
        .await?;
    }

    Ok(docs.len())
}

async fn apply_manifest_pairing_documents(
    txn: &ConfigApplyTxn<'_>,
    docs: &[PreparedImportDocument],
) -> Result<()> {
    let fields = docs
        .iter()
        .enumerate()
        .map(|(index, doc)| {
            let source = doc
                .add_doc
                .get("source")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|source| {
                    source.starts_with(desired_state::PEER_PAIRING_MANIFEST_SOURCE_PREFIX)
                })
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "manifest PeerPairingDesired {} is missing manifest source provenance",
                        doc.unique_value
                    )
                })?;
            let add_doc = crate::config_writes::mint_recreate_identity(&doc.add_doc);
            let add_literal = graphql_input_literal(&add_doc)?;
            let update_literal =
                graphql_input_literal(doc.update_doc.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("missing update document for PeerPairingDesired")
                })?)?;
            Ok(AliasedMutationField {
                alias: format!("doc_{index}"),
                field: format!(
                    r#"doc_{index}: upsert_PeerPairingDesired(
                        filter: {{
                            peer_id: {{ _eq: "{peer_id}" }},
                            source: {{ _eq: "{source}" }}
                        }},
                        add: {add_literal},
                        update: {update_literal}
                    ) {{ _docID }}"#,
                    peer_id = escape_graphql_string(&doc.unique_value),
                    source = escape_graphql_string(source),
                ),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    execute_aliased_mutation_batches(txn, "PeerPairingDesired", &fields).await
}

async fn apply_delete_manifest_pairings(
    txn: &ConfigApplyTxn<'_>,
    ids: &[String],
    owner_agent_did: &str,
) -> Result<usize> {
    if ids.is_empty() {
        return Ok(0);
    }
    let source = desired_state::peer_pairing_manifest_source(owner_agent_did);
    let fields = ids
        .iter()
        .enumerate()
        .map(|(index, peer_id)| manifest_pairing_delete_mutation_field(index, peer_id, &source))
        .collect::<Vec<_>>();
    execute_aliased_mutation_batches(txn, "PeerPairingDesired", &fields).await?;
    Ok(ids.len())
}

fn manifest_pairing_delete_mutation_field(
    index: usize,
    peer_id: &str,
    source: &str,
) -> AliasedMutationField {
    AliasedMutationField {
        alias: format!("doc_{index}"),
        field: format!(
            r#"doc_{index}: delete_PeerPairingDesired(
                filter: {{
                    peer_id: {{ _eq: "{peer_id}" }},
                    source: {{ _eq: "{source}" }}
                }}
            ) {{ _docID }}"#,
            peer_id = escape_graphql_string(peer_id),
            source = escape_graphql_string(source),
        ),
    }
}

pub(crate) async fn apply_delete_collection(
    txn: &ConfigApplyTxn<'_>,
    collection_name: &str,
    unique_field: &str,
    ids: &[String],
) -> Result<usize> {
    if ids.is_empty() {
        return Ok(0);
    }

    let fields = ids
        .iter()
        .enumerate()
        .map(|(index, id)| delete_mutation_field(index, collection_name, unique_field, id))
        .collect::<Vec<_>>();
    execute_aliased_mutation_batches(txn, collection_name, &fields).await?;
    Ok(ids.len())
}

fn prepare_import_documents(
    collection_name: &str,
    unique_field: &str,
    docs: &[Value],
    override_existing: bool,
) -> Result<Vec<PreparedImportDocument>> {
    docs.iter()
        .map(|doc| {
            let unique_value = doc
                .get(unique_field)
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "{} import document is missing {}: {}",
                        collection_name,
                        unique_field,
                        doc
                    )
                })?
                .to_string();
            let add_doc = sanitize_import_document(collection_name, doc, false)?;
            let update_doc = if override_existing {
                Some(sanitize_import_document(collection_name, doc, true)?)
            } else {
                None
            };
            Ok(PreparedImportDocument {
                unique_value,
                add_doc,
                update_doc,
            })
        })
        .collect()
}

fn uses_custom_apply_writer(collection_name: &str) -> bool {
    matches!(collection_name, "Task" | "Schedule" | "EventTrigger")
}

async fn apply_generic_import_collection_batched(
    txn: &ConfigApplyTxn<'_>,
    collection_name: &str,
    unique_field: &str,
    docs: &[PreparedImportDocument],
    override_existing: bool,
) -> Result<()> {
    let fields = docs
        .iter()
        .enumerate()
        .map(|(index, doc)| {
            generic_import_mutation_field(
                index,
                collection_name,
                unique_field,
                doc,
                override_existing,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    match execute_aliased_mutation_batches(txn, collection_name, &fields).await {
        Ok(()) => Ok(()),
        Err(_) if override_existing => {
            for doc in docs {
                apply_generic_import_document(
                    txn,
                    collection_name,
                    unique_field,
                    doc,
                    override_existing,
                )
                .await?;
            }
            Ok(())
        }
        Err(error) => Err(anyhow::anyhow!(
            "importing {collection_name} batch failed: {error}\nNext:\n  1. If a document already exists, rerun with `gents config import --override`\n  2. Or remove the existing document and retry"
        )),
    }
}

fn generic_import_mutation_field(
    index: usize,
    collection_name: &str,
    unique_field: &str,
    doc: &PreparedImportDocument,
    override_existing: bool,
) -> Result<AliasedMutationField> {
    let alias = format!("doc_{index}");
    let add_doc = if override_existing {
        crate::config_writes::mint_recreate_identity(&doc.add_doc)
    } else {
        doc.add_doc.clone()
    };
    let add_literal = graphql_input_literal(&add_doc)?;
    let field = if override_existing {
        let update_literal =
            graphql_input_literal(doc.update_doc.as_ref().ok_or_else(|| {
                anyhow::anyhow!("missing update document for {collection_name}")
            })?)?;
        format!(
            r#"{alias}: upsert_{collection_name}(
                filter: {{ {unique_field}: {{ _eq: "{unique_value}" }} }},
                add: {add_literal},
                update: {update_literal}
            ) {{ _docID }}"#,
            unique_value = escape_graphql_string(&doc.unique_value),
        )
    } else {
        format!(r#"{alias}: create_{collection_name}(input: {add_literal}) {{ _docID }}"#)
    };
    Ok(AliasedMutationField { alias, field })
}

fn delete_mutation_field(
    index: usize,
    collection_name: &str,
    unique_field: &str,
    unique_value: &str,
) -> AliasedMutationField {
    let alias = format!("doc_{index}");
    let field = format!(
        r#"{alias}: delete_{collection_name}(
            filter: {{ {unique_field}: {{ _eq: "{unique_value}" }} }}
        ) {{ _docID }}"#,
        unique_value = escape_graphql_string(unique_value),
    );
    AliasedMutationField { alias, field }
}

async fn apply_generic_import_document(
    txn: &ConfigApplyTxn<'_>,
    collection_name: &str,
    unique_field: &str,
    doc: &PreparedImportDocument,
    override_existing: bool,
) -> Result<()> {
    let add_doc = if override_existing {
        crate::config_writes::mint_recreate_identity(&doc.add_doc)
    } else {
        doc.add_doc.clone()
    };
    let add_literal = graphql_input_literal(&add_doc)?;
    let mutation = if override_existing {
        let update_literal =
            graphql_input_literal(doc.update_doc.as_ref().ok_or_else(|| {
                anyhow::anyhow!("missing update document for {collection_name}")
            })?)?;
        format!(
            r#"mutation {{
                upsert_{collection_name}(
                    filter: {{ {unique_field}: {{ _eq: "{unique_value}" }} }},
                    add: {add_literal},
                    update: {update_literal}
                ) {{ _docID }}
            }}"#,
            unique_value = escape_graphql_string(&doc.unique_value),
        )
    } else {
        format!(r#"mutation {{ create_{collection_name}(input: {add_literal}) {{ _docID }} }}"#)
    };
    let response = txn.execute(&mutation).await.map_err(|error| {
        if override_existing {
            anyhow::anyhow!(
                "importing {collection_name} {} failed: {error}",
                doc.unique_value
            )
        } else {
            anyhow::anyhow!(
                "importing {collection_name} {} failed: {error}\nNext:\n  1. If the document already exists, rerun with `gents config import --override`\n  2. Or remove the existing document and retry",
                doc.unique_value
            )
        }
    })?;
    let _ = extract_mutation_doc_id(&response, collection_name)?;
    Ok(())
}

async fn apply_custom_override_collection_batched(
    txn: &ConfigApplyTxn<'_>,
    collection_name: &str,
    unique_field: &str,
    docs: &[PreparedImportDocument],
) -> Result<()> {
    if has_duplicate_unique_values(docs) {
        return apply_custom_override_documents_individually(txn, collection_name, docs).await;
    }

    let existing_by_unique =
        match query_existing_documents_by_unique_values(txn, collection_name, unique_field, docs)
            .await
        {
            Ok(existing_by_unique) => existing_by_unique,
            Err(_) => {
                return apply_custom_override_documents_individually(txn, collection_name, docs)
                    .await;
            }
        };
    let fields = docs
        .iter()
        .enumerate()
        .map(|(index, doc)| {
            custom_override_mutation_field(
                index,
                collection_name,
                unique_field,
                doc,
                existing_by_unique
                    .get(&doc.unique_value)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]),
            )
        })
        .collect::<Result<Vec<_>>>()?;

    match execute_aliased_mutation_batches(txn, collection_name, &fields).await {
        Ok(()) => Ok(()),
        Err(_) => apply_custom_override_documents_individually(txn, collection_name, docs).await,
    }
}

fn has_duplicate_unique_values(docs: &[PreparedImportDocument]) -> bool {
    let mut seen = std::collections::BTreeSet::new();
    docs.iter()
        .any(|doc| !seen.insert(doc.unique_value.as_str()))
}

async fn query_existing_documents_by_unique_values(
    txn: &ConfigApplyTxn<'_>,
    collection_name: &str,
    unique_field: &str,
    docs: &[PreparedImportDocument],
) -> Result<BTreeMap<String, Vec<ExistingDocumentRef>>> {
    let unique_values = docs
        .iter()
        .map(|doc| doc.unique_value.clone())
        .collect::<Vec<_>>();
    let mut by_unique = query_document_refs_by_unique_values(
        txn,
        collection_name,
        unique_field,
        &unique_values,
        false,
    )
    .await?;

    let without_live = unique_values
        .into_iter()
        .filter(|unique_value| !by_unique.contains_key(unique_value))
        .collect::<Vec<_>>();
    let tombstones = query_one_historical_document_per_unique_value(
        txn,
        collection_name,
        unique_field,
        &without_live,
    )
    .await?;
    for (unique_value, rows) in tombstones {
        by_unique.entry(unique_value).or_default().extend(rows);
    }
    Ok(by_unique)
}

async fn query_one_historical_document_per_unique_value(
    txn: &ConfigApplyTxn<'_>,
    collection_name: &str,
    unique_field: &str,
    unique_values: &[String],
) -> Result<BTreeMap<String, Vec<ExistingDocumentRef>>> {
    let mut by_unique = BTreeMap::new();
    for chunk in unique_values.chunks(CONFIG_IMPORT_BATCH_SIZE) {
        let fields = chunk
            .iter()
            .enumerate()
            .map(|(index, unique_value)| {
                format!(
                    r#"lookup_{index}: {collection_name}(
                        showDeleted: true,
                        filter: {{ {unique_field}: {{ _eq: "{unique_value}" }} }},
                        limit: 1
                    ) {{
                        _docID
                        _deleted
                    }}"#,
                    unique_value = escape_graphql_string(unique_value),
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        let response = txn.execute(&format!("{{\n{fields}\n}}")).await?;
        for (index, unique_value) in chunk.iter().enumerate() {
            let rows = response
                .pointer(&format!("/data/lookup_{index}"))
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            for row in rows {
                let doc_ref = ExistingDocumentRef {
                    doc_id: row
                        .get("_docID")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "{collection_name} history row missing _docID for {unique_field}={unique_value}: {row}"
                            )
                        })?
                        .to_string(),
                    deleted: row
                        .get("_deleted")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                };
                by_unique
                    .entry(unique_value.clone())
                    .or_insert_with(Vec::new)
                    .push(doc_ref);
            }
        }
    }
    Ok(by_unique)
}

async fn query_document_refs_by_unique_values(
    txn: &ConfigApplyTxn<'_>,
    collection_name: &str,
    unique_field: &str,
    unique_values: &[String],
    show_deleted: bool,
) -> Result<BTreeMap<String, Vec<ExistingDocumentRef>>> {
    if unique_values.is_empty() {
        return Ok(BTreeMap::new());
    }

    let show_deleted_arg = if show_deleted {
        "showDeleted: true,"
    } else {
        ""
    };
    let unique_values_literal = graphql_string_list_literal(unique_values);
    let limit = if show_deleted {
        unique_values.len().saturating_mul(16).max(16)
    } else {
        unique_values.len().saturating_mul(2).max(2)
    };
    let query = format!(
        r#"{{
            {collection_name}(
                {show_deleted_arg}
                filter: {{ {unique_field}: {{ _in: {unique_values_literal} }} }},
                limit: {limit}
            ) {{
                _docID
                _deleted
                {unique_field}
            }}
        }}"#,
    );
    let response = txn.execute(&query).await?;
    let rows = response
        .get("data")
        .and_then(|data| data.get(collection_name))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut by_unique: BTreeMap<String, Vec<ExistingDocumentRef>> = BTreeMap::new();
    for row in rows {
        let unique_value = row
            .get(unique_field)
            .and_then(Value::as_str)
            .ok_or_else(|| {
                anyhow::anyhow!("{collection_name} lookup row missing {unique_field}: {row}")
            })?
            .to_string();
        let doc_ref = ExistingDocumentRef {
            doc_id: row
                .get("_docID")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "{collection_name} lookup row missing _docID for {unique_field}={unique_value}: {row}"
                    )
                })?
                .to_string(),
            deleted: row
                .get("_deleted")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        };
        by_unique.entry(unique_value).or_default().push(doc_ref);
    }

    Ok(by_unique)
}

fn custom_override_mutation_field(
    index: usize,
    collection_name: &str,
    unique_field: &str,
    doc: &PreparedImportDocument,
    existing_rows: &[ExistingDocumentRef],
) -> Result<AliasedMutationField> {
    let alias = format!("doc_{index}");
    let existing = select_existing_import_document(
        collection_name,
        unique_field,
        &doc.unique_value,
        existing_rows,
    )?;
    let field = if existing.as_ref().is_some_and(|existing| !existing.deleted) {
        let update_literal =
            graphql_input_literal(doc.update_doc.as_ref().ok_or_else(|| {
                anyhow::anyhow!("missing update document for {collection_name}")
            })?)?;
        let doc_id = existing
            .as_ref()
            .expect("existing checked above")
            .doc_id
            .as_str();
        format!(
            r#"{alias}: update_{collection_name}(docID: "{doc_id}", input: {update_literal}) {{ _docID }}"#,
            doc_id = escape_graphql_string(doc_id),
        )
    } else {
        let add_doc = if existing.is_some() {
            crate::config_writes::mint_recreate_identity(&doc.add_doc)
        } else {
            doc.add_doc.clone()
        };
        let add_literal = graphql_input_literal(&add_doc)?;
        format!(r#"{alias}: create_{collection_name}(input: {add_literal}) {{ _docID }}"#)
    };

    Ok(AliasedMutationField { alias, field })
}

fn select_existing_import_document(
    collection_name: &str,
    unique_field: &str,
    unique_value: &str,
    rows: &[ExistingDocumentRef],
) -> Result<Option<ExistingDocumentRef>> {
    let live_rows = rows.iter().filter(|row| !row.deleted).collect::<Vec<_>>();
    if live_rows.len() > 1 {
        anyhow::bail!(
            "multiple live {collection_name} documents share {unique_field}={unique_value}"
        );
    }
    if let Some(row) = live_rows.first() {
        return Ok(Some((*row).clone()));
    }

    Ok(rows.iter().find(|row| row.deleted).cloned())
}

async fn apply_custom_override_documents_individually(
    txn: &ConfigApplyTxn<'_>,
    collection_name: &str,
    docs: &[PreparedImportDocument],
) -> Result<()> {
    for doc in docs {
        let update_doc = doc
            .update_doc
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing update document for {collection_name}"))?;
        let doc_id = match collection_name {
            "Task" => write_task_document(txn, &doc.unique_value, &doc.add_doc, update_doc).await,
            "Schedule" => {
                write_schedule_document(txn, &doc.unique_value, &doc.add_doc, update_doc).await
            }
            "EventTrigger" => {
                write_event_trigger_document(txn, &doc.unique_value, &doc.add_doc, update_doc).await
            }
            _ => unreachable!("custom apply writer only supports selected collections"),
        }
        .map_err(|error| {
            anyhow::anyhow!(
                "importing {collection_name} {} failed: {error}",
                doc.unique_value
            )
        })?;
        if doc_id.trim().is_empty() {
            anyhow::bail!(
                "importing {collection_name} {} returned an empty _docID",
                doc.unique_value
            );
        }
    }

    Ok(())
}

async fn execute_aliased_mutation_batches(
    txn: &ConfigApplyTxn<'_>,
    collection_name: &str,
    fields: &[AliasedMutationField],
) -> Result<()> {
    for chunk in fields.chunks(CONFIG_IMPORT_BATCH_SIZE) {
        let mutation = build_aliased_mutation(chunk);
        let response = txn.execute(&mutation).await?;
        for field in chunk {
            let _ = extract_aliased_mutation_doc_id(&response, &field.alias, collection_name)?;
        }
    }

    Ok(())
}

fn build_aliased_mutation(fields: &[AliasedMutationField]) -> String {
    let body = fields
        .iter()
        .map(|field| field.field.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    format!("mutation {{\n{body}\n}}")
}

fn extract_aliased_mutation_doc_id(
    response: &Value,
    alias: &str,
    collection_name: &str,
) -> Result<String> {
    let data = response
        .get("data")
        .ok_or_else(|| anyhow::anyhow!("graphql response missing data: {response}"))?;
    if let Some(doc_id) = data
        .get(alias)
        .and_then(|value| value.get("_docID"))
        .and_then(Value::as_str)
    {
        return Ok(doc_id.to_string());
    }
    if let Some(doc_id) = data
        .get(alias)
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("_docID"))
        .and_then(Value::as_str)
    {
        return Ok(doc_id.to_string());
    }
    anyhow::bail!(
        "graphql mutation alias {alias} returned no _docID for {collection_name}: {response}"
    );
}

pub(crate) fn diff_has_pending_apply(
    counts: &desired_state::DesiredStateDiffCollectionsCounts,
) -> bool {
    counts.has_pending_apply()
}

pub(crate) fn config_apply_counts_changed(counts: &ConfigApplyCounts) -> bool {
    counts.changed()
}

pub(crate) fn select_apply_principal_docs(
    doc: Option<&Value>,
    diff: &desired_state::DesiredStateCollectionDiff,
) -> Result<Vec<Value>> {
    if diff.create.is_empty() && diff.update.is_empty() {
        return Ok(Vec::new());
    }
    let doc =
        doc.ok_or_else(|| anyhow::anyhow!("desired-state apply is missing AgentPrincipal"))?;
    Ok(vec![doc.clone()])
}

pub(crate) async fn apply_desired_state_changes(
    txn: &ConfigApplyTxn<'_>,
    desired_bundle: &DesiredApplyBundle,
    planned: &desired_state::DesiredStateDiffReport,
) -> Result<ConfigApplyCounts> {
    let desired_bundle = desired_bundle.as_bundle();
    let mut counts = ConfigApplyCounts::default();

    let per_collection_sleep = std::env::var("GENTS_CONFIG_APPLY_SLEEP_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(std::time::Duration::from_millis);

    for collection in CONFIG_APPLY_ORDER {
        let docs = select_apply_docs_for_collection(desired_bundle, planned, collection)?;
        let applied = apply_import_collection(
            txn,
            collection.graphql_type(),
            collection.unique_field(),
            &docs,
            true,
        )
        .await?;
        counts.set(collection, applied);

        if let Some(sleep) = per_collection_sleep {
            tokio::time::sleep(sleep).await;
        }
    }

    for collection in CONFIG_PRUNE_ORDER {
        let diff = planned.collections.get(collection);
        let deleted = if collection == Collection::PeerPairingDesired {
            apply_delete_manifest_pairings(txn, &diff.delete, &planned.agent_did).await?
        } else {
            apply_delete_collection(
                txn,
                collection.graphql_type(),
                collection.unique_field(),
                &diff.delete,
            )
            .await?
        };
        counts.add(collection, deleted);

        if let Some(sleep) = per_collection_sleep {
            tokio::time::sleep(sleep).await;
        }
    }

    Ok(counts)
}

fn select_apply_docs_for_collection(
    desired_bundle: &ConfigExportBundle,
    planned: &desired_state::DesiredStateDiffReport,
    collection: Collection,
) -> Result<Vec<Value>> {
    let diff = planned.collections.get(collection);
    if collection == Collection::AgentPrincipal {
        return select_apply_principal_docs(desired_bundle.agent_principal.as_ref(), diff);
    }

    let docs = desired_bundle
        .docs_for_collection(collection)
        .expect("non-principal desired-state collection has document slice");
    select_apply_collection_docs(
        docs,
        collection.unique_field(),
        collection.graphql_type(),
        diff,
    )
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod lean_apply_write_boundary_tests;
