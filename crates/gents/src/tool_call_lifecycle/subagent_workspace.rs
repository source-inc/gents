//! Resolve `spawn_subagent` workspace inherit / bind-id / provision.

use std::collections::BTreeSet;

use anyhow::Result;
use defra_node::EmbeddedNode;
use serde::Deserialize;

use crate::background_tools::SpawnWorkspaceArg;
use crate::callback::{
    ensure_local_host_deployment, flush_workspace_docs, load_isolated_workspace,
    load_repository_placement,
};
use crate::graphql::escape_graphql_string;
use crate::lifecycle::WorkspaceLineage;
use crate::tool_call_lifecycle::FailureClass;
use crate::toolset::{normalize_workspace_lifecycle_state, WorkspaceAuthority};
use crate::workspace::{
    emit_create_workspace_plan, execute_create_workspace_plan, ActionJournalEntry,
    CreateWorkspaceAction, CreateWorkspaceOutcome, CreationPolicy, HostExecuteError,
    HostExecutorContext, IsolatedWorkspaceDoc, MemoryWorkspaceDocuments, WorkspaceAdapterKind,
    WorkspaceDocuments, CAP_CREATE_WORKSPACE, CAP_OBSERVE_DIRTY_BASE,
};

#[derive(Debug, Clone, Default)]
pub(crate) struct ParentWorkspaceStamp {
    pub workspace_id: Option<String>,
    pub workspace_authority: Option<String>,
    pub workspace_owner_deployment_id: Option<String>,
    pub workspace_seal_hash: Option<String>,
}

impl ParentWorkspaceStamp {
    pub(crate) fn from_fields(
        workspace_id: Option<&str>,
        workspace_authority: Option<&str>,
        workspace_owner_deployment_id: Option<&str>,
        workspace_seal_hash: Option<&str>,
    ) -> Self {
        Self {
            workspace_id: nonempty(workspace_id).map(str::to_string),
            workspace_authority: nonempty(workspace_authority).map(str::to_string),
            workspace_owner_deployment_id: nonempty(workspace_owner_deployment_id)
                .map(str::to_string),
            workspace_seal_hash: nonempty(workspace_seal_hash).map(str::to_string),
        }
    }

    fn lineage(&self) -> Option<WorkspaceLineage> {
        lineage_from_bridge(
            self.workspace_id.as_deref(),
            self.workspace_authority.as_deref(),
            self.workspace_owner_deployment_id.as_deref(),
            self.workspace_seal_hash.as_deref(),
        )
    }

    fn authority(&self) -> Result<Option<WorkspaceAuthority>, SpawnWorkspaceError> {
        match self.workspace_authority.as_deref().map(str::trim) {
            Some(value) if !value.is_empty() => WorkspaceAuthority::parse(value)
                .map(Some)
                .map_err(|error| SpawnWorkspaceError::invalid(error.to_string())),
            _ => Ok(None),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SpawnWorkspaceError {
    pub class: FailureClass,
    pub message: String,
}

impl SpawnWorkspaceError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            class: FailureClass::ArgumentInvalid,
            message: message.into(),
        }
    }

    fn unavailable(message: impl Into<String>) -> Self {
        Self {
            class: FailureClass::ServiceUnavailable,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for SpawnWorkspaceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SpawnWorkspaceError {}

pub(crate) fn lineage_from_bridge(
    workspace_id: Option<&str>,
    workspace_authority: Option<&str>,
    workspace_owner_deployment_id: Option<&str>,
    workspace_seal_hash: Option<&str>,
) -> Option<WorkspaceLineage> {
    let workspace_id = nonempty(workspace_id)?;
    Some(WorkspaceLineage {
        workspace_id: Some(workspace_id.to_string()),
        workspace_authority: nonempty(workspace_authority).map(str::to_string),
        workspace_owner_deployment_id: nonempty(workspace_owner_deployment_id).map(str::to_string),
        workspace_seal_hash: nonempty(workspace_seal_hash).map(str::to_string),
    })
}

pub(crate) fn merge_workspace_lineage(bridge: &mut serde_json::Value, lineage: &WorkspaceLineage) {
    let Some(object) = bridge.as_object_mut() else {
        return;
    };
    if let Some(value) = nonempty(lineage.workspace_id.as_deref()) {
        object.insert("workspace_id".to_string(), serde_json::json!(value));
    }
    if let Some(value) = nonempty(lineage.workspace_authority.as_deref()) {
        object.insert("workspace_authority".to_string(), serde_json::json!(value));
    }
    if let Some(value) = nonempty(lineage.workspace_owner_deployment_id.as_deref()) {
        object.insert(
            "workspace_owner_deployment_id".to_string(),
            serde_json::json!(value),
        );
    }
    if let Some(value) = nonempty(lineage.workspace_seal_hash.as_deref()) {
        object.insert("workspace_seal_hash".to_string(), serde_json::json!(value));
    }
}

/// Default inherit when the parent already has `workspace_id`.
pub(crate) async fn resolve_spawn_workspace(
    node: &EmbeddedNode,
    parent: &ParentWorkspaceStamp,
    arg: Option<&SpawnWorkspaceArg>,
    writer_principal: &str,
    caused_by_invocation_id: &str,
    caused_by_correlation: &str,
) -> Result<Option<WorkspaceLineage>, SpawnWorkspaceError> {
    match arg {
        None => {
            if parent.lineage().is_some() {
                inherit_workspace(node, parent).await.map(Some)
            } else {
                Ok(None)
            }
        }
        Some(SpawnWorkspaceArg::Inherit) => inherit_workspace(node, parent).await.map(Some),
        Some(SpawnWorkspaceArg::Bind { id, authority }) => {
            bind_workspace(node, parent, id, authority.as_deref())
                .await
                .map(Some)
        }
        Some(SpawnWorkspaceArg::Provision { policy }) => provision_workspace(
            node,
            parent,
            policy.as_deref(),
            writer_principal,
            caused_by_invocation_id,
            caused_by_correlation,
        )
        .await
        .map(Some),
    }
}

async fn inherit_workspace(
    node: &EmbeddedNode,
    parent: &ParentWorkspaceStamp,
) -> Result<WorkspaceLineage, SpawnWorkspaceError> {
    let parent_id = nonempty(parent.workspace_id.as_deref()).ok_or_else(|| {
        SpawnWorkspaceError::invalid("workspace inherit requires the parent to have workspace_id")
    })?;
    let parent_authority = parent.authority()?.ok_or_else(|| {
        SpawnWorkspaceError::invalid(
            "workspace inherit requires the parent to have workspace_authority",
        )
    })?;
    let workspace = load_workspace(node, parent_id).await?;
    let default_authority = default_authority_for_state(&workspace.lifecycle_state)?;
    stamp_from_workspace(&workspace, parent_authority.infimum(default_authority))
}

async fn bind_workspace(
    node: &EmbeddedNode,
    parent: &ParentWorkspaceStamp,
    workspace_id: &str,
    requested_authority: Option<&str>,
) -> Result<WorkspaceLineage, SpawnWorkspaceError> {
    let workspace = load_workspace(node, workspace_id).await?;
    let local = ensure_local_host_deployment(node)
        .await
        .map_err(|error| SpawnWorkspaceError::unavailable(error.to_string()))?;
    if workspace.owner_deployment_id.trim() != local.trim() {
        return Err(SpawnWorkspaceError::unavailable(format!(
            "workspace {workspace_id} is owned by deployment {}, not this host",
            workspace.owner_deployment_id
        )));
    }
    let default_authority = default_authority_for_state(&workspace.lifecycle_state)?;
    let requested = match requested_authority.map(str::trim).filter(|v| !v.is_empty()) {
        Some(value) => WorkspaceAuthority::parse(value)
            .map_err(|error| SpawnWorkspaceError::invalid(error.to_string()))?,
        None => default_authority,
    };
    let authority = match parent.authority()? {
        Some(parent_authority) => parent_authority.infimum(requested),
        None => requested,
    };
    stamp_from_workspace(&workspace, authority)
}

async fn provision_workspace(
    node: &EmbeddedNode,
    parent: &ParentWorkspaceStamp,
    policy: Option<&str>,
    writer_principal: &str,
    caused_by_invocation_id: &str,
    caused_by_correlation: &str,
) -> Result<WorkspaceLineage, SpawnWorkspaceError> {
    let creation_policy = parse_creation_policy(policy)?;
    let parent_id = nonempty(parent.workspace_id.as_deref()).ok_or_else(|| {
        SpawnWorkspaceError::invalid(
            "workspace provision requires the parent to be bound to an IsolatedWorkspace",
        )
    })?;
    let parent_workspace = load_workspace(node, parent_id).await?;
    let local = ensure_local_host_deployment(node)
        .await
        .map_err(|error| SpawnWorkspaceError::unavailable(error.to_string()))?;
    if parent_workspace.owner_deployment_id.trim() != local.trim() {
        return Err(SpawnWorkspaceError::unavailable(
            "workspace provision must run on the parent workspace owner deployment",
        ));
    }
    let repository = load_repository_placement(node, &parent_workspace.repository_id, &local)
        .await
        .map_err(|error| SpawnWorkspaceError::unavailable(error.to_string()))?
        .ok_or_else(|| {
            SpawnWorkspaceError::unavailable(format!(
                "RepositoryPlacement {} not found on this host",
                parent_workspace.repository_id
            ))
        })?;

    let workspace_id = uuid::Uuid::new_v4().to_string();
    let work_unit_id = uuid::Uuid::new_v4().to_string();
    let plan = emit_create_workspace_plan(CreateWorkspaceAction {
        workspace_id: workspace_id.clone(),
        work_unit_id,
        repository_id: parent_workspace.repository_id.clone(),
        base_sha: parent_workspace.base_sha.clone(),
        branch: parent_workspace.branch.clone(),
        creation_policy,
        adapter: WorkspaceAdapterKind::GitWorktree,
        clone_artifacts: None,
    });
    let mut docs = MemoryWorkspaceDocuments::default();
    let mut journal = Vec::<ActionJournalEntry>::new();
    let capabilities: BTreeSet<String> = [CAP_CREATE_WORKSPACE, CAP_OBSERVE_DIRTY_BASE]
        .into_iter()
        .map(str::to_string)
        .collect();
    let ceiling = repository.host_path.parent().map(ToOwned::to_owned);
    let execute_result = {
        let mut ctx = HostExecutorContext {
            deployment_id: local.clone(),
            repository,
            ceiling: ceiling.as_deref(),
            capabilities,
            writer_principal: writer_principal.to_string(),
            integrator_principal: writer_principal.to_string(),
            caused_by_invocation_id: caused_by_invocation_id.to_string(),
            caused_by_correlation: caused_by_correlation.to_string(),
            documents: &mut docs,
        };
        execute_create_workspace_plan(&plan, &mut journal, &mut ctx)
    };
    match execute_result {
        Ok(outcome) => {
            flush_outcome(node, &outcome).await?;
            let authority = match parent.authority()? {
                Some(parent_authority) => parent_authority.infimum(WorkspaceAuthority::ReadWrite),
                None => WorkspaceAuthority::ReadWrite,
            };
            Ok(WorkspaceLineage {
                workspace_id: Some(outcome.workspace.workspace_id),
                workspace_authority: Some(authority.as_str().to_string()),
                workspace_owner_deployment_id: Some(local),
                workspace_seal_hash: None,
            })
        }
        Err(error) => {
            if let Some(outcome) = error.outcome() {
                let _ = flush_outcome(node, outcome).await;
            } else if let Err(flush_error) = flush_workspace_docs(node, &docs).await {
                tracing::warn!(
                    %flush_error,
                    "failed to persist partial workspace docs after provision error"
                );
            }
            Err(match error {
                HostExecuteError::Denied { reason } => SpawnWorkspaceError::invalid(reason),
                HostExecuteError::Failed { reason, .. } => SpawnWorkspaceError::unavailable(reason),
            })
        }
    }
}

async fn flush_outcome(
    node: &EmbeddedNode,
    outcome: &CreateWorkspaceOutcome,
) -> Result<(), SpawnWorkspaceError> {
    let mut written = MemoryWorkspaceDocuments::default();
    written
        .write_isolated_workspace(outcome.workspace.clone())
        .map_err(|error| SpawnWorkspaceError::unavailable(error.to_string()))?;
    written
        .write_placement(outcome.placement.clone())
        .map_err(|error| SpawnWorkspaceError::unavailable(error.to_string()))?;
    flush_workspace_docs(node, &written)
        .await
        .map_err(|error| SpawnWorkspaceError::unavailable(error.to_string()))
}

fn stamp_from_workspace(
    workspace: &IsolatedWorkspaceDoc,
    authority: WorkspaceAuthority,
) -> Result<WorkspaceLineage, SpawnWorkspaceError> {
    if !authority.bindable_lifecycle_state(&workspace.lifecycle_state) {
        return Err(SpawnWorkspaceError::invalid(format!(
            "isolated workspace {} in state {} is not bindable for authority {}",
            workspace.workspace_id,
            workspace.lifecycle_state,
            authority.as_str()
        )));
    }
    let seal_hash = nonempty(workspace.seal_hash.as_deref()).map(str::to_string);
    if matches!(
        normalize_workspace_lifecycle_state(&workspace.lifecycle_state),
        Some("sealed")
    ) && seal_hash.is_none()
    {
        return Err(SpawnWorkspaceError::unavailable(format!(
            "sealed workspace {} is missing seal_hash",
            workspace.workspace_id
        )));
    }
    Ok(WorkspaceLineage {
        workspace_id: Some(workspace.workspace_id.clone()),
        workspace_authority: Some(authority.as_str().to_string()),
        workspace_owner_deployment_id: Some(workspace.owner_deployment_id.clone()),
        workspace_seal_hash: seal_hash,
    })
}

fn default_authority_for_state(state: &str) -> Result<WorkspaceAuthority, SpawnWorkspaceError> {
    match normalize_workspace_lifecycle_state(state) {
        Some("ready") => Ok(WorkspaceAuthority::ReadWrite),
        Some("sealed") => Ok(WorkspaceAuthority::ReadOnly),
        _ => Err(SpawnWorkspaceError::invalid(format!(
            "isolated workspace in state {state} is not Ready or Sealed"
        ))),
    }
}

fn parse_creation_policy(policy: Option<&str>) -> Result<CreationPolicy, SpawnWorkspaceError> {
    match policy.map(str::trim).filter(|value| !value.is_empty()) {
        None | Some("git_worktree_diff") => Ok(CreationPolicy::GitWorktreeDiff),
        Some(other) => Err(SpawnWorkspaceError::invalid(format!(
            "creation_policy '{other}' is not implemented in v1 (only git_worktree_diff)"
        ))),
    }
}

async fn load_workspace(
    node: &EmbeddedNode,
    workspace_id: &str,
) -> Result<IsolatedWorkspaceDoc, SpawnWorkspaceError> {
    match load_isolated_workspace(node, workspace_id).await {
        Ok(Some(doc)) => Ok(doc),
        Ok(None) => Err(SpawnWorkspaceError::unavailable(format!(
            "isolated workspace {workspace_id} not found"
        ))),
        Err(error) => Err(SpawnWorkspaceError::unavailable(error.to_string())),
    }
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AgentRequestWorkspaceRow {
    #[serde(default)]
    workspace_id: Option<String>,
    #[serde(default)]
    workspace_authority: Option<String>,
    #[serde(default)]
    workspace_owner_deployment_id: Option<String>,
    #[serde(default)]
    workspace_seal_hash: Option<String>,
}

#[allow(dead_code)]
pub(crate) async fn load_parent_workspace_stamp(
    node: &EmbeddedNode,
    parent_request_id: &str,
) -> Result<ParentWorkspaceStamp> {
    let escaped = escape_graphql_string(parent_request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped}" }} }},
                limit: 1
            ) {{
                workspace_id
                workspace_authority
                workspace_owner_deployment_id
                workspace_seal_hash
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query parent workspace fields failed: {:?}",
            response.errors
        );
    }
    let row: Option<AgentRequestWorkspaceRow> =
        crate::graphql::first_row(&response, "AgentRequest")?;
    Ok(row
        .map(|row| {
            ParentWorkspaceStamp::from_fields(
                row.workspace_id.as_deref(),
                row.workspace_authority.as_deref(),
                row.workspace_owner_deployment_id.as_deref(),
                row.workspace_seal_hash.as_deref(),
            )
        })
        .unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::background_tools::parse_spawn_workspace_arg;
    use serde_json::json;

    #[test]
    fn workspace_arg_parses_inherit_bind_provision() {
        assert_eq!(
            parse_spawn_workspace_arg(&json!("inherit")).unwrap(),
            SpawnWorkspaceArg::Inherit
        );
        assert_eq!(
            parse_spawn_workspace_arg(&json!({"id": "ws-1"})).unwrap(),
            SpawnWorkspaceArg::Bind {
                id: "ws-1".into(),
                authority: None,
            }
        );
        assert_eq!(
            parse_spawn_workspace_arg(&json!({"id": "ws-1", "authority": "readOnly"})).unwrap(),
            SpawnWorkspaceArg::Bind {
                id: "ws-1".into(),
                authority: Some("readOnly".into()),
            }
        );
        assert_eq!(
            parse_spawn_workspace_arg(&json!({"provision": {"policy": "git_worktree_diff"}}))
                .unwrap(),
            SpawnWorkspaceArg::Provision {
                policy: Some("git_worktree_diff".into()),
            }
        );
        assert_eq!(
            parse_spawn_workspace_arg(&json!({"provision": {}})).unwrap(),
            SpawnWorkspaceArg::Provision { policy: None }
        );
    }

    #[test]
    fn inherit_infimum_cannot_outrank_parent() {
        assert_eq!(
            WorkspaceAuthority::ReadOnly.infimum(WorkspaceAuthority::ReadWrite),
            WorkspaceAuthority::ReadOnly
        );
        assert_eq!(
            WorkspaceAuthority::ReadWrite.infimum(WorkspaceAuthority::ReadWrite),
            WorkspaceAuthority::ReadWrite
        );
        assert_eq!(
            WorkspaceAuthority::Integrate.infimum(WorkspaceAuthority::ReadWrite),
            WorkspaceAuthority::Integrate
        );
    }
}
