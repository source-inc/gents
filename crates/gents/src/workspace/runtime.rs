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
use super::binding::release_binding;
use super::documents::{
    isolated_workspace_upsert_mutation, workspace_binding_upsert_mutation,
    workspace_placement_upsert_mutation, workspace_receipt_create_mutation, IsolatedWorkspaceDoc,
    MemoryWorkspaceDocuments, WorkspaceDocuments, WorkspacePlacementDoc, WorkspaceReceiptDoc,
    LIFECYCLE_SEALED,
};
use super::executor::{execute_seal_workspace_plan, HostExecutorContext, RepositoryPlacementRef};
use super::overlay::optional_id;

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

/// Copy `seal_hash` onto post-seal ReadOnly/Integrate lineage.
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
    let Some(workspace) =
        super::overlay::load_isolated_workspace_record(node, workspace_id).await?
    else {
        return Ok(());
    };
    if lineage
        .workspace_owner_deployment_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_none()
    {
        lineage.workspace_owner_deployment_id = Some(workspace.owner_deployment_id.clone());
    }
    if crate::toolset::normalize_workspace_lifecycle_state(&workspace.lifecycle_state)
        != Some("sealed")
    {
        return Ok(());
    }
    let Some(hash) = optional_id(workspace.seal_hash.as_deref()) else {
        anyhow::bail!("sealed workspace {workspace_id} is missing seal_hash");
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
    if workspace.lifecycle_state == LIFECYCLE_SEALED {
        return Ok(());
    }
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
    {
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
            .map_err(|err| anyhow::anyhow!("{err}"))?;
    }
    flush_docs(node, &docs).await
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
    Ok(docs)
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

async fn flush_docs(node: &EmbeddedNode, docs: &MemoryWorkspaceDocuments) -> Result<()> {
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
    for binding in docs.bindings.values() {
        let mutation = workspace_binding_upsert_mutation(binding);
        graphql_mutation_with_transaction_retry(node, &mutation, "upsert_WorkspaceBinding")
            .await
            .with_context(|| format!("persist WorkspaceBinding {}", binding.binding_id))?;
    }
    for receipt in docs.receipts.values() {
        persist_receipt(node, receipt).await?;
    }
    Ok(())
}

async fn persist_receipt(node: &EmbeddedNode, doc: &WorkspaceReceiptDoc) -> Result<()> {
    let mutation = workspace_receipt_create_mutation(doc);
    graphql_mutation_with_transaction_retry(node, &mutation, "upsert_WorkspaceReceipt")
        .await
        .with_context(|| format!("persist WorkspaceReceipt {}", doc.receipt_id))?;
    Ok(())
}
