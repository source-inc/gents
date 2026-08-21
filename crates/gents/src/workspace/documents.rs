use std::collections::HashMap;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::graphql::escape_graphql_string;

use super::action_plan::{CreationPolicy, WorkspaceAdapterKind};
use super::LogicalWorkspaceIdentity;

pub(crate) const ADAPTER_VERSION: &str = "gents-workspace-adapter/1";
pub(crate) const LIFECYCLE_READY: &str = "ready";
pub(crate) const LIFECYCLE_PROVISION_FAILED: &str = "provisionFailed";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IsolatedWorkspaceDoc {
    pub workspace_id: String,
    pub work_unit_id: String,
    pub repository_id: String,
    pub base_sha: String,
    pub branch: String,
    pub creation_policy: String,
    pub adapter: String,
    pub owner_deployment_id: String,
    pub writer_principal: String,
    pub integrator_principal: String,
    pub instruction_manifest: String,
    pub seal_hash: Option<String>,
    pub lifecycle_state: String,
    pub caused_by_invocation_id: String,
    pub caused_by_correlation: String,
}

impl IsolatedWorkspaceDoc {
    pub(crate) fn identity(&self) -> LogicalWorkspaceIdentity {
        LogicalWorkspaceIdentity {
            workspace_id: self.workspace_id.clone(),
            work_unit_id: self.work_unit_id.clone(),
            repository_id: self.repository_id.clone(),
            base_sha: self.base_sha.clone(),
            branch: self.branch.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspacePlacementDoc {
    pub workspace_id: String,
    pub deployment_id: String,
    pub host_path: String,
    pub repository_placement_id: String,
    pub adapter: String,
    pub adapter_version: String,
    pub dirty_base: bool,
    pub dirty_base_summary: String,
    pub provisioning_state: String,
    pub observed_tree_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProvisioningObservation {
    pub path_exists: bool,
    pub worktree_registered: bool,
    pub identity_recorded: bool,
    pub artifacts_cloned: bool,
}

impl ProvisioningObservation {
    pub fn to_json_string(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }
}

pub trait WorkspaceDocuments {
    fn load_isolated_workspace(&self, workspace_id: &str) -> Result<Option<IsolatedWorkspaceDoc>>;
    fn load_placement(&self, workspace_id: &str) -> Result<Option<WorkspacePlacementDoc>>;
    fn write_isolated_workspace(&mut self, doc: IsolatedWorkspaceDoc) -> Result<()>;
    fn write_placement(&mut self, doc: WorkspacePlacementDoc) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct MemoryWorkspaceDocuments {
    pub workspaces: HashMap<String, IsolatedWorkspaceDoc>,
    pub placements: HashMap<String, WorkspacePlacementDoc>,
}

impl WorkspaceDocuments for MemoryWorkspaceDocuments {
    fn load_isolated_workspace(&self, workspace_id: &str) -> Result<Option<IsolatedWorkspaceDoc>> {
        Ok(self.workspaces.get(workspace_id).cloned())
    }

    fn load_placement(&self, workspace_id: &str) -> Result<Option<WorkspacePlacementDoc>> {
        Ok(self.placements.get(workspace_id).cloned())
    }

    fn write_isolated_workspace(&mut self, doc: IsolatedWorkspaceDoc) -> Result<()> {
        self.workspaces.insert(doc.workspace_id.clone(), doc);
        Ok(())
    }

    fn write_placement(&mut self, doc: WorkspacePlacementDoc) -> Result<()> {
        self.placements.insert(doc.workspace_id.clone(), doc);
        Ok(())
    }
}

pub(crate) fn new_isolated_workspace(
    identity: &LogicalWorkspaceIdentity,
    creation_policy: CreationPolicy,
    adapter: WorkspaceAdapterKind,
    owner_deployment_id: &str,
    writer_principal: &str,
    integrator_principal: &str,
    caused_by_invocation_id: &str,
    caused_by_correlation: &str,
    lifecycle_state: &str,
) -> IsolatedWorkspaceDoc {
    IsolatedWorkspaceDoc {
        workspace_id: identity.workspace_id.clone(),
        work_unit_id: identity.work_unit_id.clone(),
        repository_id: identity.repository_id.clone(),
        base_sha: identity.base_sha.clone(),
        branch: identity.branch.clone(),
        creation_policy: creation_policy.as_str().to_string(),
        adapter: adapter.as_str().to_string(),
        owner_deployment_id: owner_deployment_id.to_string(),
        writer_principal: writer_principal.to_string(),
        integrator_principal: integrator_principal.to_string(),
        instruction_manifest: "{}".to_string(),
        seal_hash: None,
        lifecycle_state: lifecycle_state.to_string(),
        caused_by_invocation_id: caused_by_invocation_id.to_string(),
        caused_by_correlation: caused_by_correlation.to_string(),
    }
}

pub fn isolated_workspace_create_mutation(doc: &IsolatedWorkspaceDoc) -> String {
    let seal_hash = match doc
        .seal_hash
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        Some(value) => format!(r#""{}""#, escape_graphql_string(value)),
        None => "null".to_string(),
    };
    format!(
        r#"mutation {{
            create_IsolatedWorkspace(input: {{
                workspace_id: "{workspace_id}",
                work_unit_id: "{work_unit_id}",
                repository_id: "{repository_id}",
                base_sha: "{base_sha}",
                branch: "{branch}",
                creation_policy: "{creation_policy}",
                adapter: "{adapter}",
                owner_deployment_id: "{owner_deployment_id}",
                writer_principal: "{writer_principal}",
                integrator_principal: "{integrator_principal}",
                instruction_manifest: "{instruction_manifest}",
                seal_hash: {seal_hash},
                lifecycle_state: "{lifecycle_state}",
                caused_by_invocation_id: "{caused_by_invocation_id}",
                caused_by_correlation: "{caused_by_correlation}"
            }}) {{ _docID }}
        }}"#,
        workspace_id = escape_graphql_string(&doc.workspace_id),
        work_unit_id = escape_graphql_string(&doc.work_unit_id),
        repository_id = escape_graphql_string(&doc.repository_id),
        base_sha = escape_graphql_string(&doc.base_sha),
        branch = escape_graphql_string(&doc.branch),
        creation_policy = escape_graphql_string(&doc.creation_policy),
        adapter = escape_graphql_string(&doc.adapter),
        owner_deployment_id = escape_graphql_string(&doc.owner_deployment_id),
        writer_principal = escape_graphql_string(&doc.writer_principal),
        integrator_principal = escape_graphql_string(&doc.integrator_principal),
        instruction_manifest = escape_graphql_string(&doc.instruction_manifest),
        lifecycle_state = escape_graphql_string(&doc.lifecycle_state),
        caused_by_invocation_id = escape_graphql_string(&doc.caused_by_invocation_id),
        caused_by_correlation = escape_graphql_string(&doc.caused_by_correlation),
    )
}

pub fn workspace_placement_upsert_mutation(
    doc: &WorkspacePlacementDoc,
    updated_at: &str,
) -> String {
    let dirty_base = if doc.dirty_base { "true" } else { "false" };
    let updated_at = escape_graphql_string(updated_at);
    format!(
        r#"mutation {{
            upsert_WorkspacePlacement(
                filter: {{ workspace_id: {{ _eq: "{workspace_id}" }} }},
                add: {{
                    workspace_id: "{workspace_id}",
                    deployment_id: "{deployment_id}",
                    host_path: "{host_path}",
                    repository_placement_id: "{repository_placement_id}",
                    adapter: "{adapter}",
                    adapter_version: "{adapter_version}",
                    dirty_base: {dirty_base},
                    dirty_base_summary: "{dirty_base_summary}",
                    provisioning_state: "{provisioning_state}",
                    observed_tree_hash: "{observed_tree_hash}",
                    updated_at: "{updated_at}"
                }},
                update: {{
                    host_path: "{host_path}",
                    adapter: "{adapter}",
                    adapter_version: "{adapter_version}",
                    dirty_base: {dirty_base},
                    dirty_base_summary: "{dirty_base_summary}",
                    provisioning_state: "{provisioning_state}",
                    observed_tree_hash: "{observed_tree_hash}",
                    updated_at: "{updated_at}"
                }}
            ) {{ _docID }}
        }}"#,
        workspace_id = escape_graphql_string(&doc.workspace_id),
        deployment_id = escape_graphql_string(&doc.deployment_id),
        host_path = escape_graphql_string(&doc.host_path),
        repository_placement_id = escape_graphql_string(&doc.repository_placement_id),
        adapter = escape_graphql_string(&doc.adapter),
        adapter_version = escape_graphql_string(&doc.adapter_version),
        dirty_base_summary = escape_graphql_string(&doc.dirty_base_summary),
        provisioning_state = escape_graphql_string(&doc.provisioning_state),
        observed_tree_hash = escape_graphql_string(&doc.observed_tree_hash),
    )
}
