use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use defra_node::EmbeddedNode;

use crate::graphql::{
    escape_graphql_string, first_row, graphql_mutation_with_transaction_retry,
    graphql_with_transaction_retry,
};
use crate::lifecycle::WorkspaceLineage;
use crate::toolset::WorkspaceAuthority;
use crate::watcher::AgentRequest;

use super::action_plan::{emit_seal_workspace_plan, SealWorkspaceAction, CAP_SEAL_WORKSPACE};
use super::binding::{admit_workspace_binding, new_binding, release_binding, AdmitBinding};
use super::documents::{
    workspace_seal_docs_mutation, IsolatedWorkspaceDoc, MemoryWorkspaceDocuments,
    WorkspaceBindingDoc, WorkspaceDocuments, WorkspacePlacementDoc, WorkspaceReceiptDoc,
    LIFECYCLE_SEALED, RECEIPT_KIND_WRITER,
};
use super::executor::{
    execute_seal_workspace_plan, HostExecutorContext, RepositoryPlacementRef, SealWorkspaceOutcome,
};
use super::overlay::{optional_id, IsolatedWorkspaceRecord};

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

/// Copy `seal_hash` onto post-seal ReadOnly/Integrate lineage. Fail closed
/// when `workspace_id` is set but IsolatedWorkspace is missing, and refuse
/// ReadWrite after Sealed / Integrate before Sealed.
pub async fn stamp_workspace_lineage(
    node: &EmbeddedNode,
    lineage: &mut WorkspaceLineage,
) -> Result<()> {
    let Some(workspace_id) = lineage
        .workspace_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let workspace = super::overlay::load_isolated_workspace_record(node, workspace_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("isolated workspace {workspace_id} not found"))?;
    apply_workspace_lineage_stamp(lineage, &workspace)
}

pub(crate) fn apply_workspace_lineage_stamp(
    lineage: &mut WorkspaceLineage,
    workspace: &IsolatedWorkspaceRecord,
) -> Result<()> {
    if lineage
        .workspace_owner_deployment_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_none()
    {
        lineage.workspace_owner_deployment_id = Some(workspace.owner_deployment_id.clone());
    }
    let state = crate::toolset::normalize_workspace_lifecycle_state(&workspace.lifecycle_state);
    let authority = lineage
        .workspace_authority
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(WorkspaceAuthority::parse)
        .transpose()?;
    match (state, authority) {
        (Some("sealed"), Some(WorkspaceAuthority::ReadWrite)) => anyhow::bail!(
            "isolated workspace {} is sealed; ReadWrite bindings are illegal",
            workspace.workspace_id
        ),
        (Some("sealed"), _) => {
            let Some(hash) = optional_id(workspace.seal_hash.as_deref()) else {
                anyhow::bail!(
                    "sealed workspace {} is missing seal_hash",
                    workspace.workspace_id
                );
            };
            match lineage
                .workspace_seal_hash
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                None => lineage.workspace_seal_hash = Some(hash.to_string()),
                Some(existing) if existing == hash => {}
                Some(existing) => anyhow::bail!(
                    "request workspace_seal_hash {existing} does not match workspace seal_hash {hash}"
                ),
            }
        }
        (Some("ready"), Some(WorkspaceAuthority::Integrate)) => anyhow::bail!(
            "isolated workspace {} is ready; Integrate bindings require Sealed",
            workspace.workspace_id
        ),
        (Some("ready"), _) => {}
        (other, Some(authority)) => anyhow::bail!(
            "isolated workspace {} in state {} is not bindable for authority {}",
            workspace.workspace_id,
            other.unwrap_or(workspace.lifecycle_state.as_str()),
            authority.as_str()
        ),
        _ => {}
    }
    Ok(())
}

pub async fn seal_on_writer_success(
    node: &EmbeddedNode,
    request: &AgentRequest,
    operator_tool_root: Option<&Path>,
) -> Result<()> {
    let Some(workspace_id) = optional_id(request.workspace_id.as_deref()) else {
        return Ok(());
    };
    let authority = match request.workspace_authority.as_deref().map(str::trim) {
        Some(value) if !value.is_empty() => WorkspaceAuthority::parse(value)?,
        _ => return Ok(()),
    };
    if !matches!(authority, WorkspaceAuthority::ReadWrite) {
        return Ok(());
    }

    let mut docs = load_docs(node, workspace_id).await?;
    let workspace = docs
        .load_isolated_workspace(workspace_id)?
        .ok_or_else(|| anyhow::anyhow!("isolated workspace {workspace_id} not found"))?;
    let placement = docs
        .load_placement(workspace_id)?
        .ok_or_else(|| anyhow::anyhow!("workspace placement {workspace_id} not found"))?;
    let repository = load_repository(
        node,
        &workspace.repository_id,
        &workspace.owner_deployment_id,
        Path::new(&placement.host_path),
    )
    .await?;

    let plan = emit_seal_workspace_plan(SealWorkspaceAction {
        workspace_id: workspace_id.to_string(),
        produced_by_request_id: request.request_id.clone(),
        produced_by_request_doc_id: request.doc_id.clone(),
    });
    let mut journal = Vec::new();
    let mut capabilities = BTreeSet::new();
    capabilities.insert(CAP_SEAL_WORKSPACE.to_string());
    let outcome = {
        let mut ctx = HostExecutorContext {
            deployment_id: workspace.owner_deployment_id.clone(),
            repository,
            ceiling: operator_tool_root,
            capabilities,
            writer_principal: workspace.writer_principal.clone(),
            integrator_principal: workspace.integrator_principal.clone(),
            caused_by_invocation_id: workspace.caused_by_invocation_id.clone(),
            caused_by_correlation: workspace.caused_by_correlation.clone(),
            documents: &mut docs,
        };
        execute_seal_workspace_plan(&plan, &mut journal, &mut ctx)
            .map_err(|err| anyhow::anyhow!("{err}"))?
    };
    flush_seal_outcome(node, &docs, &outcome).await
}

/// Crash recovery: this writer already sealed (or left a Sealed workspace
/// after a partial flush). Skip ReadWrite overlay/inference and only complete.
pub async fn writer_request_already_sealed(
    node: &EmbeddedNode,
    request: &AgentRequest,
) -> Result<bool> {
    let Some(workspace_id) = optional_id(request.workspace_id.as_deref()) else {
        return Ok(false);
    };
    let authority = match request.workspace_authority.as_deref().map(str::trim) {
        Some(value) if !value.is_empty() => WorkspaceAuthority::parse(value)?,
        _ => return Ok(false),
    };
    if !matches!(authority, WorkspaceAuthority::ReadWrite) {
        return Ok(false);
    }
    let Some(workspace) =
        super::overlay::load_isolated_workspace_record(node, workspace_id).await?
    else {
        return Ok(false);
    };
    let receipts = load_receipts(node, workspace_id).await?;
    let bindings = super::overlay::load_workspace_bindings_for(node, workspace_id).await?;
    Ok(writer_already_sealed(
        &workspace.lifecycle_state,
        &receipts,
        &bindings,
        &request.request_id,
    ))
}

fn writer_already_sealed(
    lifecycle_state: &str,
    receipts: &[WorkspaceReceiptDoc],
    bindings: &[WorkspaceBindingDoc],
    request_id: &str,
) -> bool {
    crate::toolset::normalize_workspace_lifecycle_state(lifecycle_state) == Some(LIFECYCLE_SEALED)
        && (receipts.iter().any(|receipt| {
            receipt.kind == RECEIPT_KIND_WRITER && receipt.produced_by_request_id == request_id
        }) || bindings
            .iter()
            .any(|binding| binding.request_id == request_id))
}

pub async fn materialize_workspace_binding(
    node: &EmbeddedNode,
    request_id: &str,
    request_doc_id: &str,
    lineage: &WorkspaceLineage,
    local_deployment_id: Option<&str>,
) -> Result<()> {
    let Some(workspace_id) = lineage
        .workspace_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let authority = match lineage.workspace_authority.as_deref().map(str::trim) {
        Some(value) if !value.is_empty() => WorkspaceAuthority::parse(value)?,
        _ => anyhow::bail!("workspace-bound request {workspace_id} is missing workspace_authority"),
    };
    let deployment_id = optional_id(local_deployment_id).ok_or_else(|| {
        anyhow::anyhow!("workspace-bound request {workspace_id} is missing local deployment_id")
    })?;
    let workspace = super::overlay::load_isolated_workspace_record(node, workspace_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("isolated workspace {workspace_id} not found"))?;
    let existing = super::overlay::load_workspace_bindings_for(node, workspace_id).await?;
    let candidate = new_binding(
        workspace_id,
        request_id,
        request_doc_id,
        authority,
        deployment_id,
        optional_id(lineage.workspace_seal_hash.as_deref())
            .or(optional_id(workspace.seal_hash.as_deref())),
    );
    let release_previous = matches!(authority, WorkspaceAuthority::ReadWrite)
        && super::overlay::previous_read_write_is_stale(node, workspace_id, &existing, request_id)
            .await?;
    match admit_workspace_binding(
        workspace_id,
        &workspace.lifecycle_state,
        optional_id(workspace.seal_hash.as_deref()),
        &existing,
        candidate,
        release_previous,
    )? {
        AdmitBinding::Reuse(_) => Ok(()),
        AdmitBinding::Create { binding, release } => {
            for released in release {
                super::overlay::persist_workspace_binding_doc(node, &released).await?;
            }
            super::overlay::persist_workspace_binding_doc(node, &binding).await
        }
    }
}

pub async fn release_writer_binding(node: &EmbeddedNode, request: &AgentRequest) -> Result<()> {
    let Some(workspace_id) = optional_id(request.workspace_id.as_deref()) else {
        return Ok(());
    };
    let authority = match request.workspace_authority.as_deref().map(str::trim) {
        Some(value) if !value.is_empty() => WorkspaceAuthority::parse(value)?,
        _ => return Ok(()),
    };
    if !matches!(authority, WorkspaceAuthority::ReadWrite) {
        return Ok(());
    }
    let bindings = super::overlay::load_workspace_bindings_for(node, workspace_id).await?;
    for binding in bindings {
        if binding.is_active_read_write() && binding.request_id == request.request_id {
            super::overlay::persist_workspace_binding_doc(node, &release_binding(binding)).await?;
        }
    }
    Ok(())
}

async fn load_docs(node: &EmbeddedNode, workspace_id: &str) -> Result<MemoryWorkspaceDocuments> {
    let mut docs = MemoryWorkspaceDocuments::default();
    if let Some(workspace) = load_isolated_workspace_doc(node, workspace_id).await? {
        docs.write_isolated_workspace(workspace)?;
    }
    if let Some(placement) = load_placement_doc(node, workspace_id).await? {
        docs.write_placement(placement)?;
    }
    for binding in super::overlay::load_workspace_bindings_for(node, workspace_id).await? {
        docs.write_binding(binding)?;
    }
    for receipt in load_receipts(node, workspace_id).await? {
        docs.write_receipt(receipt)?;
    }
    Ok(docs)
}

async fn load_receipts(
    node: &EmbeddedNode,
    workspace_id: &str,
) -> Result<Vec<WorkspaceReceiptDoc>> {
    let query = format!(
        r#"{{
            WorkspaceReceipt(
                filter: {{ workspace_id: {{ _eq: "{id}" }} }}
            ) {{
                receipt_id
                workspace_id
                produced_by_request_id
                produced_by_request_doc_id
                kind
                base_sha
                seal_hash
                head_sha
                changed_files
                diff_artifact
                checks_run
                unresolved_conflicts
                integration_instructions
            }}
        }}"#,
        id = escape_graphql_string(workspace_id),
    );
    let response =
        graphql_with_transaction_retry(node, &query, "load WorkspaceReceipt for seal").await?;
    crate::graphql::rows(&response, "WorkspaceReceipt")
}

async fn load_isolated_workspace_doc(
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
    let response =
        graphql_with_transaction_retry(node, &query, "load IsolatedWorkspace for seal").await?;
    first_row(&response, "IsolatedWorkspace")
}

async fn load_placement_doc(
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
    let response =
        graphql_with_transaction_retry(node, &query, "load WorkspacePlacement for seal").await?;
    first_row(&response, "WorkspacePlacement")
}

async fn load_repository(
    node: &EmbeddedNode,
    repository_id: &str,
    deployment_id: &str,
    fallback_path: &Path,
) -> Result<RepositoryPlacementRef> {
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
    let response =
        graphql_with_transaction_retry(node, &query, "load RepositoryPlacement for seal").await?;
    #[derive(serde::Deserialize)]
    struct Row {
        repository_id: String,
        deployment_id: String,
        host_path: String,
        enabled: Option<bool>,
    }
    if let Some(row) = first_row::<Row>(&response, "RepositoryPlacement")? {
        return Ok(RepositoryPlacementRef {
            repository_id: row.repository_id,
            deployment_id: row.deployment_id,
            host_path: PathBuf::from(row.host_path),
            enabled: row.enabled.unwrap_or(true),
        });
    }
    Ok(RepositoryPlacementRef {
        repository_id: repository_id.to_string(),
        deployment_id: deployment_id.to_string(),
        host_path: fallback_path.to_path_buf(),
        enabled: true,
    })
}

async fn flush_seal_outcome(
    node: &EmbeddedNode,
    docs: &MemoryWorkspaceDocuments,
    outcome: &SealWorkspaceOutcome,
) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let bindings: Vec<WorkspaceBindingDoc> = docs
        .bindings
        .values()
        .filter(|binding| binding.workspace_id == outcome.workspace.workspace_id)
        .cloned()
        .collect();
    let mutation = workspace_seal_docs_mutation(
        &outcome.workspace,
        &outcome.placement,
        &bindings,
        &outcome.receipt,
        &now,
    );
    graphql_mutation_with_transaction_retry(node, &mutation, "seal workspace docs")
        .await
        .with_context(|| {
            format!(
                "persist sealed workspace {} receipt {}",
                outcome.workspace.workspace_id, outcome.receipt.receipt_id
            )
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::WorkspaceLineage;
    use crate::toolset::WorkspaceAuthority;

    fn sealed_workspace() -> IsolatedWorkspaceRecord {
        IsolatedWorkspaceRecord {
            workspace_id: "ws-1".into(),
            owner_deployment_id: "dep-1".into(),
            lifecycle_state: "sealed".into(),
            seal_hash: Some("hash-1".into()),
            instruction_manifest: "{}".into(),
        }
    }

    fn lineage(authority: &str, hash: Option<&str>) -> WorkspaceLineage {
        WorkspaceLineage {
            workspace_id: Some("ws-1".into()),
            workspace_authority: Some(authority.into()),
            workspace_owner_deployment_id: None,
            workspace_seal_hash: hash.map(str::to_string),
        }
    }

    #[test]
    fn stamp_copies_seal_hash_and_denies_read_write_after_sealed() {
        let mut ro = lineage(WorkspaceAuthority::ReadOnly.as_str(), None);
        apply_workspace_lineage_stamp(&mut ro, &sealed_workspace()).unwrap();
        assert_eq!(ro.workspace_seal_hash.as_deref(), Some("hash-1"));
        assert_eq!(ro.workspace_owner_deployment_id.as_deref(), Some("dep-1"));

        let mut rw = lineage(WorkspaceAuthority::ReadWrite.as_str(), Some("hash-1"));
        let err = apply_workspace_lineage_stamp(&mut rw, &sealed_workspace()).unwrap_err();
        assert!(
            err.to_string().contains("ReadWrite bindings are illegal"),
            "{err:#}"
        );

        let mut missing_hash = lineage(WorkspaceAuthority::ReadOnly.as_str(), None);
        let mut sealed = sealed_workspace();
        sealed.seal_hash = None;
        let err = apply_workspace_lineage_stamp(&mut missing_hash, &sealed).unwrap_err();
        assert!(err.to_string().contains("missing seal_hash"), "{err:#}");
    }

    #[test]
    fn stamp_denies_integrate_before_seal() {
        let ready = IsolatedWorkspaceRecord {
            lifecycle_state: "ready".into(),
            seal_hash: None,
            ..sealed_workspace()
        };
        let mut lineage = lineage(WorkspaceAuthority::Integrate.as_str(), None);
        let err = apply_workspace_lineage_stamp(&mut lineage, &ready).unwrap_err();
        assert!(
            err.to_string()
                .contains("Integrate bindings require Sealed"),
            "{err:#}"
        );
    }

    fn writer_receipt(request_id: &str) -> WorkspaceReceiptDoc {
        WorkspaceReceiptDoc {
            receipt_id: format!("receipt-writer-ws-1-{request_id}"),
            workspace_id: "ws-1".into(),
            produced_by_request_id: request_id.into(),
            produced_by_request_doc_id: "doc-1".into(),
            kind: RECEIPT_KIND_WRITER.to_string(),
            base_sha: "base".into(),
            seal_hash: "hash-1".into(),
            head_sha: None,
            changed_files: None,
            diff_artifact: None,
            checks_run: None,
            unresolved_conflicts: None,
            integration_instructions: None,
        }
    }

    fn writer_binding(request_id: &str) -> WorkspaceBindingDoc {
        new_binding(
            "ws-1",
            request_id,
            "doc-1",
            WorkspaceAuthority::ReadWrite,
            "dep-1",
            Some("hash-1"),
        )
    }

    #[test]
    fn already_sealed_writer_skips_overlay_when_receipt_or_binding_exists() {
        assert!(writer_already_sealed(
            "sealed",
            &[writer_receipt("req-1")],
            &[],
            "req-1"
        ));
        assert!(writer_already_sealed(
            "sealed",
            &[],
            &[writer_binding("req-1")],
            "req-1"
        ));
        assert!(!writer_already_sealed("sealed", &[], &[], "req-1"));
        assert!(!writer_already_sealed(
            "ready",
            &[writer_receipt("req-1")],
            &[writer_binding("req-1")],
            "req-1"
        ));
        assert!(!writer_already_sealed(
            "sealed",
            &[writer_receipt("other")],
            &[writer_binding("other")],
            "req-1"
        ));
    }
}
