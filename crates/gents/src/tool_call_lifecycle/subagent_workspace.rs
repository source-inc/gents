//! Resolve `spawn_subagent` workspace inherit / bind-id / provision.

use std::collections::BTreeSet;
use std::path::Path;

use defra_node::EmbeddedNode;

use crate::background_tools::SpawnWorkspaceArg;
use crate::callback::{
    ensure_local_host_deployment, flush_workspace_docs, load_isolated_workspace,
    load_repository_placement, load_workspace_placement,
};
use crate::lifecycle::WorkspaceLineage;
use crate::tool_call_lifecycle::FailureClass;
use crate::toolset::{normalize_workspace_lifecycle_state, WorkspaceAuthority};
use crate::workspace::{
    emit_create_workspace_plan, execute_create_workspace_plan, load_enabled_workspace_roots,
    require_under_ceiling, workspace_host_path, ActionJournalEntry, CreateWorkspaceAction,
    CreateWorkspaceOutcome, CreationPolicy, HostExecuteError, HostExecutorContext,
    IsolatedWorkspaceDoc, MemoryWorkspaceDocuments, WorkspaceAdapterKind, WorkspaceDocuments,
    WorkspacePlacementDoc, CAP_CREATE_WORKSPACE, CAP_OBSERVE_DIRTY_BASE,
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

    fn has_workspace_id(&self) -> bool {
        nonempty(self.workspace_id.as_deref()).is_some()
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

    pub(crate) fn payload(&self) -> String {
        let failure_class = match self.class {
            FailureClass::ArgumentInvalid => "invalid_tool_arguments",
            _ => "service_unavailable",
        };
        serde_json::json!({
            "ok": false,
            "failure_class": failure_class,
            "path": "/workspace",
            "message": self.message,
            "retryable": false,
            "service_id": "subagent",
            "tool_name": "spawn_subagent"
        })
        .to_string()
    }
}

impl std::fmt::Display for SpawnWorkspaceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SpawnWorkspaceError {}

/// Skip re-resolve only when the bridge already carries a complete stamp.
pub(crate) fn complete_lineage_from_bridge(
    workspace_id: Option<&str>,
    workspace_authority: Option<&str>,
    workspace_owner_deployment_id: Option<&str>,
    workspace_seal_hash: Option<&str>,
) -> Option<WorkspaceLineage> {
    let workspace_id = nonempty(workspace_id)?;
    let workspace_authority = nonempty(workspace_authority)?;
    let workspace_owner_deployment_id = nonempty(workspace_owner_deployment_id)?;
    Some(WorkspaceLineage {
        workspace_id: Some(workspace_id.to_string()),
        workspace_authority: Some(workspace_authority.to_string()),
        workspace_owner_deployment_id: Some(workspace_owner_deployment_id.to_string()),
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

/// Re-validate a complete bridge stamp, or resolve inherit/bind/provision.
pub(crate) async fn resolve_child_workspace(
    node: &EmbeddedNode,
    parent: &ParentWorkspaceStamp,
    arg: Option<&SpawnWorkspaceArg>,
    stamped: Option<WorkspaceLineage>,
    writer_principal: &str,
    caused_by_invocation_id: &str,
    caused_by_correlation: &str,
) -> Result<Option<WorkspaceLineage>, SpawnWorkspaceError> {
    if let Some(lineage) = stamped {
        return revalidate_stamped_lineage(node, lineage).await.map(Some);
    }
    resolve_spawn_workspace(
        node,
        parent,
        arg,
        writer_principal,
        caused_by_invocation_id,
        caused_by_correlation,
    )
    .await
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
            if parent.has_workspace_id() {
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
    require_parent_stamp_agrees(parent, &workspace)?;
    require_local_workspace(node, &workspace).await?;
    let default_authority = default_authority_for_state(&workspace.lifecycle_state)?;
    stamp_from_workspace(&workspace, parent_authority.infimum(default_authority))
}

async fn bind_workspace(
    node: &EmbeddedNode,
    parent: &ParentWorkspaceStamp,
    workspace_id: &str,
    requested_authority: Option<&str>,
) -> Result<WorkspaceLineage, SpawnWorkspaceError> {
    let workspace_id = nonempty(Some(workspace_id)).ok_or_else(|| {
        SpawnWorkspaceError::invalid("workspace bind requires a non-empty IsolatedWorkspace id")
    })?;
    let workspace = load_workspace(node, workspace_id).await?;
    require_local_workspace(node, &workspace).await?;
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
    require_parent_stamp_agrees(parent, &parent_workspace)?;
    require_local_workspace(node, &parent_workspace).await?;
    let local = ensure_local_host_deployment(node)
        .await
        .map_err(|error| SpawnWorkspaceError::unavailable(error.to_string()))?;
    let authority = provision_authority(parent)?;
    if !authority.bindable_lifecycle_state("ready") {
        return Err(SpawnWorkspaceError::invalid(format!(
            "isolated workspace provision is not bindable for authority {}",
            authority.as_str()
        )));
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

    let workspace_id = spawn_provision_workspace_id(caused_by_invocation_id);
    let work_unit_id = spawn_provision_work_unit_id(caused_by_invocation_id);
    let branch = unique_child_branch(&parent_workspace.branch, &workspace_id);
    let enabled_roots = load_enabled_workspace_roots(node)
        .await
        .map_err(|error| SpawnWorkspaceError::unavailable(error.to_string()))?;
    let dest = workspace_host_path(&repository.host_path, &workspace_id, &branch, None)
        .map_err(|error| SpawnWorkspaceError::unavailable(error.to_string()))?;
    require_under_ceiling(&dest, None, &enabled_roots).map_err(|error| {
        SpawnWorkspaceError::invalid(format!(
            "provisioned workspace placement would escape operator ceiling: {error}"
        ))
    })?;
    let executor_ceiling = enabled_roots
        .iter()
        .find(|root| dest.starts_with(root))
        .cloned();

    let plan = emit_create_workspace_plan(CreateWorkspaceAction {
        workspace_id: workspace_id.clone(),
        work_unit_id,
        repository_id: parent_workspace.repository_id.clone(),
        base_sha: parent_workspace.base_sha.clone(),
        branch,
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
    let execute_result = {
        let mut ctx = HostExecutorContext {
            deployment_id: local.clone(),
            repository,
            ceiling: executor_ceiling.as_deref(),
            capabilities,
            writer_principal: writer_principal.to_string(),
            integrator_principal: writer_principal.to_string(),
            caused_by_invocation_id: caused_by_invocation_id.to_string(),
            caused_by_correlation: caused_by_correlation.to_string(),
            documents: &mut docs,
        };
        execute_create_workspace_plan(&plan, &mut journal, &mut ctx)
    };
    persist_provision_docs(node, &execute_result, &docs).await?;
    let outcome = execute_result.map_err(|error| match error {
        HostExecuteError::Denied { reason } => SpawnWorkspaceError::invalid(reason),
        HostExecuteError::Failed { reason, .. } => SpawnWorkspaceError::unavailable(reason),
    })?;
    stamp_from_workspace(&outcome.workspace, authority)
}

async fn persist_provision_docs(
    node: &EmbeddedNode,
    execute_result: &Result<CreateWorkspaceOutcome, HostExecuteError>,
    docs: &MemoryWorkspaceDocuments,
) -> Result<(), SpawnWorkspaceError> {
    match execute_result {
        Ok(outcome) => flush_outcome(node, outcome).await,
        Err(error) => {
            if let Some(outcome) = error.outcome() {
                flush_outcome(node, outcome).await
            } else {
                flush_workspace_docs(node, docs)
                    .await
                    .map_err(|error| SpawnWorkspaceError::unavailable(error.to_string()))
            }
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

async fn revalidate_stamped_lineage(
    node: &EmbeddedNode,
    lineage: WorkspaceLineage,
) -> Result<WorkspaceLineage, SpawnWorkspaceError> {
    let workspace_id = nonempty(lineage.workspace_id.as_deref())
        .ok_or_else(|| SpawnWorkspaceError::invalid("workspace stamp is missing workspace_id"))?;
    let workspace = load_workspace(node, workspace_id).await?;
    require_local_workspace(node, &workspace).await?;
    let authority = nonempty(lineage.workspace_authority.as_deref())
        .ok_or_else(|| {
            SpawnWorkspaceError::invalid("workspace stamp is missing workspace_authority")
        })
        .and_then(|value| {
            WorkspaceAuthority::parse(value)
                .map_err(|error| SpawnWorkspaceError::invalid(error.to_string()))
        })?;
    stamp_from_workspace(&workspace, authority)
}

fn provision_authority(
    parent: &ParentWorkspaceStamp,
) -> Result<WorkspaceAuthority, SpawnWorkspaceError> {
    Ok(match parent.authority()? {
        Some(parent_authority) => parent_authority.infimum(WorkspaceAuthority::ReadWrite),
        None => WorkspaceAuthority::ReadWrite,
    })
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

fn require_parent_stamp_agrees(
    parent: &ParentWorkspaceStamp,
    workspace: &IsolatedWorkspaceDoc,
) -> Result<(), SpawnWorkspaceError> {
    if let Some(parent_owner) = nonempty(parent.workspace_owner_deployment_id.as_deref()) {
        if parent_owner != workspace.owner_deployment_id.trim() {
            return Err(SpawnWorkspaceError::invalid(format!(
                "parent workspace_owner_deployment_id {parent_owner} does not match IsolatedWorkspace owner {}",
                workspace.owner_deployment_id
            )));
        }
    }
    if let Some(parent_seal) = nonempty(parent.workspace_seal_hash.as_deref()) {
        match nonempty(workspace.seal_hash.as_deref()) {
            Some(workspace_seal) if workspace_seal == parent_seal => {}
            Some(workspace_seal) => {
                return Err(SpawnWorkspaceError::invalid(format!(
                    "parent workspace_seal_hash {parent_seal} does not match IsolatedWorkspace seal_hash {workspace_seal}"
                )));
            }
            None => {
                return Err(SpawnWorkspaceError::invalid(format!(
                    "parent workspace_seal_hash {parent_seal} does not match IsolatedWorkspace {} (missing seal_hash)",
                    workspace.workspace_id
                )));
            }
        }
    }
    Ok(())
}

async fn require_local_workspace(
    node: &EmbeddedNode,
    workspace: &IsolatedWorkspaceDoc,
) -> Result<WorkspacePlacementDoc, SpawnWorkspaceError> {
    let local = ensure_local_host_deployment(node)
        .await
        .map_err(|error| SpawnWorkspaceError::unavailable(error.to_string()))?;
    if workspace.owner_deployment_id.trim() != local.trim() {
        return Err(SpawnWorkspaceError::unavailable(format!(
            "workspace {} is owned by deployment {}, not this host",
            workspace.workspace_id, workspace.owner_deployment_id
        )));
    }
    let placement = load_workspace_placement(node, &workspace.workspace_id)
        .await
        .map_err(|error| SpawnWorkspaceError::unavailable(error.to_string()))?
        .ok_or_else(|| {
            SpawnWorkspaceError::unavailable(format!(
                "workspace placement for {} not found on this host",
                workspace.workspace_id
            ))
        })?;
    if placement.deployment_id.trim() != local.trim() {
        return Err(SpawnWorkspaceError::unavailable(format!(
            "workspace placement for {} is owned by deployment {}, not this host",
            workspace.workspace_id, placement.deployment_id
        )));
    }
    let host_path = Path::new(placement.host_path.trim());
    if !host_path.is_absolute() || !host_path.is_dir() {
        return Err(SpawnWorkspaceError::unavailable(format!(
            "workspace placement for {} is missing a local directory",
            workspace.workspace_id
        )));
    }
    Ok(placement)
}

pub(crate) fn spawn_provision_workspace_id(caused_by_invocation_id: &str) -> String {
    format!(
        "spawn-ws-{}",
        sanitize_id(nonempty(Some(caused_by_invocation_id)).unwrap_or("unknown"))
    )
}

fn spawn_provision_work_unit_id(caused_by_invocation_id: &str) -> String {
    format!(
        "spawn-unit-{}",
        sanitize_id(nonempty(Some(caused_by_invocation_id)).unwrap_or("unknown"))
    )
}

pub(crate) fn unique_child_branch(parent_branch: &str, workspace_id: &str) -> String {
    let parent = sanitize_id(nonempty(Some(parent_branch)).unwrap_or("topic"));
    let id = sanitize_id(workspace_id);
    format!("{parent}-ws-{id}")
}

fn sanitize_id(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('-');
        }
    }
    if out.is_empty() {
        "workspace".to_string()
    } else {
        out
    }
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
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
        assert!(!WorkspaceAuthority::Integrate.bindable_lifecycle_state("ready"));
    }

    #[test]
    fn provision_ids_are_stable_per_tool_call() {
        assert_eq!(
            spawn_provision_workspace_id("internal-spawn-a"),
            spawn_provision_workspace_id("internal-spawn-a")
        );
        assert_ne!(
            spawn_provision_workspace_id("internal-spawn-a"),
            spawn_provision_workspace_id("internal-spawn-b")
        );
    }

    #[test]
    fn unique_child_branch_does_not_reuse_parent() {
        let branch = unique_child_branch("topic", "spawn-ws-child");
        assert_ne!(branch, "topic");
        assert!(branch.starts_with("topic-ws-"));
        assert_ne!(
            unique_child_branch("topic", "spawn-ws-a"),
            unique_child_branch("topic", "spawn-ws-b")
        );
    }

    #[test]
    fn complete_lineage_requires_authority_and_owner() {
        assert!(complete_lineage_from_bridge(Some("ws-1"), None, Some("deploy"), None).is_none());
        assert!(complete_lineage_from_bridge(Some("ws-1"), Some("readOnly"), None, None).is_none());
        let lineage = complete_lineage_from_bridge(
            Some("ws-1"),
            Some("readOnly"),
            Some("deploy"),
            Some("abc"),
        )
        .unwrap();
        assert_eq!(lineage.workspace_id.as_deref(), Some("ws-1"));
        assert_eq!(lineage.workspace_authority.as_deref(), Some("readOnly"));
        assert_eq!(
            lineage.workspace_owner_deployment_id.as_deref(),
            Some("deploy")
        );
        assert_eq!(lineage.workspace_seal_hash.as_deref(), Some("abc"));
    }
}
