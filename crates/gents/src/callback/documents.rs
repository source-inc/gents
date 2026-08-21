use std::collections::BTreeSet;
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use defra_node::EmbeddedNode;
use serde::Deserialize;
use serde_json::Value;

use crate::graphql::{
    escape_graphql_string, first_row, graphql_mutation_with_transaction_retry, rows,
};
use crate::workspace::{
    isolated_workspace_upsert_mutation, workspace_placement_upsert_mutation, IsolatedWorkspaceDoc,
    MemoryWorkspaceDocuments, RepositoryPlacementRef, WorkspaceDocuments, WorkspacePlacementDoc,
};

use super::BUILTIN_CREATE_WORKSPACE;

const BINDING_FIELDS: &str = r#"
    binding_id
    source_collection
    event_kind
    filter
    source_fields
    module_id
    builtin_emitter
    principal_did
    capability_set
    retry_policy
    owner_deployment_id
    enabled
"#;

const INVOCATION_FIELDS: &str = r#"
    invocation_id
    owner_deployment_id
    binding_id
    source_collection
    source_doc_id
    source_version
    idempotency_key
    lifecycle_state
    attempts
    action_plan
    action_journal
    error
    claimed_at
    created_at
"#;

const RESULT_FIELDS: &str = r#"
    result_id
    invocation_id
    owner_deployment_id
    workspace_id
    caused_by_correlation
    created_at
"#;

const ISOLATED_WORKSPACE_FIELDS: &str = r#"
    workspace_id
    work_unit_id
    repository_id
    base_sha
    branch
    creation_policy
    adapter
    owner_deployment_id
    writer_principal
    integrator_principal
    instruction_manifest
    seal_hash
    lifecycle_state
    caused_by_invocation_id
    caused_by_correlation
"#;

const PLACEMENT_FIELDS: &str = r#"
    workspace_id
    deployment_id
    host_path
    repository_placement_id
    adapter
    adapter_version
    dirty_base
    dirty_base_summary
    provisioning_state
    observed_tree_hash
"#;

#[derive(Debug, Clone, Deserialize)]
pub struct CallbackBindingDoc {
    pub binding_id: String,
    pub source_collection: String,
    pub event_kind: String,
    #[serde(default)]
    pub filter: Option<String>,
    #[serde(default)]
    pub source_fields: Option<String>,
    #[serde(default)]
    pub module_id: Option<String>,
    #[serde(default)]
    pub builtin_emitter: Option<String>,
    pub principal_did: String,
    #[serde(default)]
    pub capability_set: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    pub retry_policy: Option<String>,
    pub owner_deployment_id: String,
    #[serde(default)]
    pub enabled: Option<bool>,
}

impl CallbackBindingDoc {
    #[allow(dead_code)]
    pub fn enabled(&self) -> bool {
        self.enabled.unwrap_or(true)
    }

    pub fn capabilities(&self) -> BTreeSet<String> {
        parse_string_list(self.capability_set.as_deref())
            .into_iter()
            .collect()
    }

    pub fn projected_fields(&self) -> Result<Vec<String>> {
        Ok(parse_string_list(self.source_fields.as_deref()))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CallbackInvocationDoc {
    pub invocation_id: String,
    pub owner_deployment_id: String,
    pub binding_id: String,
    pub source_collection: String,
    pub source_doc_id: String,
    #[serde(default)]
    pub source_version: Option<String>,
    pub idempotency_key: String,
    pub lifecycle_state: String,
    #[serde(default)]
    pub attempts: Option<i64>,
    #[serde(default)]
    pub action_plan: Option<String>,
    #[serde(default)]
    pub action_journal: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub claimed_at: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CallbackResultDoc {
    pub result_id: String,
    pub invocation_id: String,
    pub owner_deployment_id: String,
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub caused_by_correlation: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RepositoryPlacementRow {
    repository_id: String,
    deployment_id: String,
    host_path: String,
    #[serde(default)]
    enabled: Option<bool>,
}

pub fn idempotency_key(binding_id: &str, source_doc_id: &str, source_version: &str) -> String {
    format!("{binding_id}:{source_doc_id}:{source_version}")
}

pub fn parse_string_list(raw: Option<&str>) -> Vec<String> {
    let Some(raw) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return Vec::new();
    };
    if let Ok(Value::Array(items)) = serde_json::from_str::<Value>(raw) {
        return items
            .into_iter()
            .filter_map(|item| {
                item.as_str()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
            })
            .collect();
    }
    raw.split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

/// Bindings that list secret-bearing source fields fail closed at apply/load.
pub fn validate_callback_binding(binding: &CallbackBindingDoc) -> Result<()> {
    crate::graphql::validate_collection_identifier(&binding.source_collection)?;
    if binding.binding_id.trim().is_empty() {
        anyhow::bail!("CallbackBinding.binding_id must be non-empty");
    }
    if binding.owner_deployment_id.trim().is_empty() {
        anyhow::bail!("CallbackBinding.owner_deployment_id must be non-empty");
    }
    if binding.principal_did.trim().is_empty() {
        anyhow::bail!("CallbackBinding.principal_did must be non-empty");
    }
    if binding.event_kind.trim() != "created" {
        anyhow::bail!(
            "CallbackBinding {} event_kind must be created, got {}",
            binding.binding_id,
            binding.event_kind
        );
    }
    if let Some(filter) = binding
        .filter
        .as_deref()
        .map(str::trim)
        .filter(|filter| !filter.is_empty())
    {
        crate::graphql::validate_graphql_filter_fragment(filter)?;
    }
    for field in binding.projected_fields()? {
        crate::graphql::validate_graphql_name(&field)?;
        if crate::toolset::is_secret_env_name(&field) {
            anyhow::bail!(
                "CallbackBinding {} source field `{field}` is secret-bearing",
                binding.binding_id
            );
        }
    }
    let builtin = binding
        .builtin_emitter
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let module = binding
        .module_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    match (builtin, module) {
        (None, None) => anyhow::bail!(
            "CallbackBinding {} needs builtin_emitter or module_id",
            binding.binding_id
        ),
        (Some(name), _) if name != BUILTIN_CREATE_WORKSPACE => anyhow::bail!(
            "CallbackBinding {} unknown builtin_emitter `{name}`",
            binding.binding_id
        ),
        _ => Ok(()),
    }
}

pub fn strip_secret_fields(value: Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .filter(|(key, _)| !crate::toolset::is_secret_env_name(key))
                .map(|(key, child)| (key, strip_secret_fields(child)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.into_iter().map(strip_secret_fields).collect()),
        other => other,
    }
}

pub async fn list_enabled_bindings(node: &EmbeddedNode) -> Result<Vec<CallbackBindingDoc>> {
    let query = format!(
        r#"{{
            CallbackBinding(
                filter: {{ enabled: {{ _eq: true }} }}
            ) {{ {BINDING_FIELDS} }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!("query CallbackBinding failed: {:?}", response.errors);
    }
    let rows: Vec<CallbackBindingDoc> = rows(&response, "CallbackBinding")?;
    let mut out = Vec::new();
    for binding in rows {
        match validate_callback_binding(&binding) {
            Ok(()) => out.push(binding),
            Err(error) => tracing::warn!(
                binding_id = %binding.binding_id,
                %error,
                "skipping invalid CallbackBinding"
            ),
        }
    }
    out.sort_by(|left, right| left.binding_id.cmp(&right.binding_id));
    Ok(out)
}

pub async fn load_binding(
    node: &EmbeddedNode,
    binding_id: &str,
) -> Result<Option<CallbackBindingDoc>> {
    let query = format!(
        r#"{{
            CallbackBinding(
                filter: {{ binding_id: {{ _eq: "{id}" }} }},
                limit: 1
            ) {{ {BINDING_FIELDS} }}
        }}"#,
        id = escape_graphql_string(binding_id),
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query CallbackBinding {binding_id} failed: {:?}",
            response.errors
        );
    }
    first_row(&response, "CallbackBinding")
}

pub async fn load_invocation(
    node: &EmbeddedNode,
    invocation_id: &str,
) -> Result<Option<CallbackInvocationDoc>> {
    let query = format!(
        r#"{{
            CallbackInvocation(
                filter: {{ invocation_id: {{ _eq: "{id}" }} }},
                limit: 1
            ) {{ {INVOCATION_FIELDS} }}
        }}"#,
        id = escape_graphql_string(invocation_id),
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query CallbackInvocation {invocation_id} failed: {:?}",
            response.errors
        );
    }
    first_row(&response, "CallbackInvocation")
}

pub async fn load_invocation_by_key(
    node: &EmbeddedNode,
    key: &str,
) -> Result<Option<CallbackInvocationDoc>> {
    let query = format!(
        r#"{{
            CallbackInvocation(
                filter: {{ idempotency_key: {{ _eq: "{key}" }} }},
                limit: 1
            ) {{ {INVOCATION_FIELDS} }}
        }}"#,
        key = escape_graphql_string(key),
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query CallbackInvocation by idempotency_key failed: {:?}",
            response.errors
        );
    }
    first_row(&response, "CallbackInvocation")
}

pub async fn list_recoverable_invocations(
    node: &EmbeddedNode,
    owner_deployment_id: &str,
) -> Result<Vec<CallbackInvocationDoc>> {
    let query = format!(
        r#"{{
            CallbackInvocation(
                filter: {{
                    owner_deployment_id: {{ _eq: "{owner}" }},
                    lifecycle_state: {{ _in: ["pending", "claimed", "running"] }}
                }},
                order: {{ created_at: ASC }}
            ) {{ {INVOCATION_FIELDS} }}
        }}"#,
        owner = escape_graphql_string(owner_deployment_id),
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query recoverable CallbackInvocation failed: {:?}",
            response.errors
        );
    }
    rows(&response, "CallbackInvocation")
}

pub async fn load_callback_result(
    node: &EmbeddedNode,
    invocation_id: &str,
) -> Result<Option<CallbackResultDoc>> {
    let query = format!(
        r#"{{
            CallbackResult(
                filter: {{ invocation_id: {{ _eq: "{id}" }} }},
                limit: 1
            ) {{ {RESULT_FIELDS} }}
        }}"#,
        id = escape_graphql_string(invocation_id),
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query CallbackResult for {invocation_id} failed: {:?}",
            response.errors
        );
    }
    first_row(&response, "CallbackResult")
}

pub async fn create_pending_invocation(
    node: &EmbeddedNode,
    invocation: &CallbackInvocationDoc,
) -> Result<CallbackInvocationDoc> {
    if let Some(existing) = load_invocation_by_key(node, &invocation.idempotency_key).await? {
        return Ok(existing);
    }
    let now = invocation
        .created_at
        .clone()
        .unwrap_or_else(|| chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
    let mutation = format!(
        r#"mutation {{
            create_CallbackInvocation(input: {{
                invocation_id: "{invocation_id}",
                owner_deployment_id: "{owner_deployment_id}",
                binding_id: "{binding_id}",
                source_collection: "{source_collection}",
                source_doc_id: "{source_doc_id}",
                source_version: "{source_version}",
                idempotency_key: "{idempotency_key}",
                lifecycle_state: "pending",
                attempts: 0,
                action_plan: "",
                action_journal: "[]",
                error: "",
                claimed_at: "",
                created_at: "{created_at}"
            }}) {{ _docID }}
        }}"#,
        invocation_id = escape_graphql_string(&invocation.invocation_id),
        owner_deployment_id = escape_graphql_string(&invocation.owner_deployment_id),
        binding_id = escape_graphql_string(&invocation.binding_id),
        source_collection = escape_graphql_string(&invocation.source_collection),
        source_doc_id = escape_graphql_string(&invocation.source_doc_id),
        source_version =
            escape_graphql_string(invocation.source_version.as_deref().unwrap_or("created")),
        idempotency_key = escape_graphql_string(&invocation.idempotency_key),
        created_at = escape_graphql_string(&now),
    );
    match graphql_mutation_with_transaction_retry(node, &mutation, "create_CallbackInvocation")
        .await
    {
        Ok(_) => load_invocation_by_key(node, &invocation.idempotency_key)
            .await?
            .ok_or_else(|| anyhow!("created CallbackInvocation missing after write")),
        Err(error) => {
            if let Some(existing) =
                load_invocation_by_key(node, &invocation.idempotency_key).await?
            {
                return Ok(existing);
            }
            Err(error).context("create_CallbackInvocation")
        }
    }
}

pub async fn update_invocation(
    node: &EmbeddedNode,
    invocation: &CallbackInvocationDoc,
    expected_state: Option<&str>,
) -> Result<bool> {
    let state_filter = expected_state
        .map(|state| {
            format!(
                r#", lifecycle_state: {{ _eq: "{}" }}"#,
                escape_graphql_string(state)
            )
        })
        .unwrap_or_default();
    let journal = invocation.action_journal.as_deref().unwrap_or("[]");
    let plan = invocation.action_plan.as_deref().unwrap_or("");
    let error = invocation.error.as_deref().unwrap_or("");
    let claimed_at = invocation.claimed_at.as_deref().unwrap_or("");
    let mutation = format!(
        r#"mutation {{
            update_CallbackInvocation(
                filter: {{
                    invocation_id: {{ _eq: "{id}" }},
                    owner_deployment_id: {{ _eq: "{owner}" }}
                    {state_filter}
                }},
                input: {{
                    lifecycle_state: "{state}",
                    attempts: {attempts},
                    action_plan: "{plan}",
                    action_journal: "{journal}",
                    error: "{error}",
                    claimed_at: "{claimed_at}"
                }}
            ) {{ _docID }}
        }}"#,
        id = escape_graphql_string(&invocation.invocation_id),
        owner = escape_graphql_string(&invocation.owner_deployment_id),
        state = escape_graphql_string(&invocation.lifecycle_state),
        attempts = invocation.attempts.unwrap_or(0),
        plan = escape_graphql_string(plan),
        journal = escape_graphql_string(journal),
        error = escape_graphql_string(error),
        claimed_at = escape_graphql_string(claimed_at),
    );
    let response =
        graphql_mutation_with_transaction_retry(node, &mutation, "update_CallbackInvocation")
            .await?;
    Ok(crate::graphql::single_mutation_document(&response, "update_CallbackInvocation")?.is_some())
}

pub async fn create_callback_result(
    node: &EmbeddedNode,
    result: &CallbackResultDoc,
) -> Result<CallbackResultDoc> {
    if let Some(existing) = load_callback_result(node, &result.invocation_id).await? {
        return Ok(existing);
    }
    let now = result
        .created_at
        .clone()
        .unwrap_or_else(|| chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
    let workspace = result.workspace_id.as_deref().unwrap_or("");
    let correlation = result.caused_by_correlation.as_deref().unwrap_or("");
    let mutation = format!(
        r#"mutation {{
            create_CallbackResult(input: {{
                result_id: "{result_id}",
                invocation_id: "{invocation_id}",
                owner_deployment_id: "{owner}",
                workspace_id: "{workspace}",
                caused_by_correlation: "{correlation}",
                created_at: "{created_at}"
            }}) {{ _docID }}
        }}"#,
        result_id = escape_graphql_string(&result.result_id),
        invocation_id = escape_graphql_string(&result.invocation_id),
        owner = escape_graphql_string(&result.owner_deployment_id),
        workspace = escape_graphql_string(workspace),
        correlation = escape_graphql_string(correlation),
        created_at = escape_graphql_string(&now),
    );
    match graphql_mutation_with_transaction_retry(node, &mutation, "create_CallbackResult").await {
        Ok(_) => load_callback_result(node, &result.invocation_id)
            .await?
            .ok_or_else(|| anyhow!("created CallbackResult missing after write")),
        Err(error) => {
            if let Some(existing) = load_callback_result(node, &result.invocation_id).await? {
                return Ok(existing);
            }
            Err(error).context("create_CallbackResult")
        }
    }
}

pub async fn load_repository_placement(
    node: &EmbeddedNode,
    repository_id: &str,
    deployment_id: &str,
) -> Result<Option<RepositoryPlacementRef>> {
    let query = format!(
        r#"{{
            RepositoryPlacement(
                filter: {{ repository_id: {{ _eq: "{id}" }} }},
                limit: 1
            ) {{
                repository_id
                deployment_id
                host_path
                enabled
            }}
        }}"#,
        id = escape_graphql_string(repository_id),
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query RepositoryPlacement {repository_id} failed: {:?}",
            response.errors
        );
    }
    let Some(row) = first_row::<RepositoryPlacementRow>(&response, "RepositoryPlacement")? else {
        return Ok(None);
    };
    if row.deployment_id != deployment_id {
        anyhow::bail!(
            "RepositoryPlacement {} belongs to deployment {}, not {deployment_id}",
            row.repository_id,
            row.deployment_id
        );
    }
    Ok(Some(RepositoryPlacementRef {
        repository_id: row.repository_id,
        deployment_id: row.deployment_id,
        host_path: PathBuf::from(row.host_path),
        enabled: row.enabled.unwrap_or(true),
    }))
}

pub async fn load_memory_workspace_docs(
    node: &EmbeddedNode,
    workspace_id: &str,
) -> Result<MemoryWorkspaceDocuments> {
    let mut docs = MemoryWorkspaceDocuments::default();
    if let Some(workspace) = load_isolated_workspace(node, workspace_id).await? {
        docs.write_isolated_workspace(workspace)?;
    }
    if let Some(placement) = load_workspace_placement(node, workspace_id).await? {
        docs.write_placement(placement)?;
    }
    Ok(docs)
}

pub async fn flush_workspace_docs(
    node: &EmbeddedNode,
    docs: &MemoryWorkspaceDocuments,
) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    for workspace in docs.workspaces.values() {
        let mutation = isolated_workspace_upsert_mutation(workspace);
        graphql_mutation_with_transaction_retry(node, &mutation, "upsert_IsolatedWorkspace")
            .await
            .with_context(|| format!("persist IsolatedWorkspace {}", workspace.workspace_id))?;
    }
    for placement in docs.placements.values() {
        let mutation = workspace_placement_upsert_mutation(placement, &now);
        graphql_mutation_with_transaction_retry(node, &mutation, "upsert_WorkspacePlacement")
            .await
            .with_context(|| format!("persist WorkspacePlacement {}", placement.workspace_id))?;
    }
    Ok(())
}

async fn load_isolated_workspace(
    node: &EmbeddedNode,
    workspace_id: &str,
) -> Result<Option<IsolatedWorkspaceDoc>> {
    let query = format!(
        r#"{{
            IsolatedWorkspace(
                filter: {{ workspace_id: {{ _eq: "{id}" }} }},
                limit: 1
            ) {{ {ISOLATED_WORKSPACE_FIELDS} }}
        }}"#,
        id = escape_graphql_string(workspace_id),
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query IsolatedWorkspace {workspace_id} failed: {:?}",
            response.errors
        );
    }
    first_row(&response, "IsolatedWorkspace")
}

async fn load_workspace_placement(
    node: &EmbeddedNode,
    workspace_id: &str,
) -> Result<Option<WorkspacePlacementDoc>> {
    let query = format!(
        r#"{{
            WorkspacePlacement(
                filter: {{ workspace_id: {{ _eq: "{id}" }} }},
                limit: 1
            ) {{ {PLACEMENT_FIELDS} }}
        }}"#,
        id = escape_graphql_string(workspace_id),
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query WorkspacePlacement {workspace_id} failed: {:?}",
            response.errors
        );
    }
    first_row(&response, "WorkspacePlacement")
}
