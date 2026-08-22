use std::collections::HashMap;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::graphql::escape_graphql_string;

use super::action_plan::{CreationPolicy, WorkspaceAdapterKind};
use super::LogicalWorkspaceIdentity;

pub(crate) const ADAPTER_VERSION: &str = "gents-workspace-adapter/1";
pub(crate) const LIFECYCLE_READY: &str = "ready";
pub(crate) const LIFECYCLE_PROVISION_FAILED: &str = "provisionFailed";
pub(crate) const LIFECYCLE_SEALED: &str = "sealed";
pub(crate) const BINDING_ACTIVE: &str = "active";
pub(crate) const BINDING_RELEASED: &str = "released";
pub(crate) const RECEIPT_KIND_WRITER: &str = "writer";

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
    #[serde(default, deserialize_with = "deserialize_null_string")]
    pub instruction_manifest: String,
    #[serde(default)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceBindingDoc {
    pub binding_id: String,
    pub workspace_id: String,
    pub request_id: String,
    pub request_doc_id: String,
    pub authority: String,
    pub deployment_id: String,
    pub seal_hash: Option<String>,
    pub lifecycle_state: String,
}

impl WorkspaceBindingDoc {
    pub fn is_active(&self) -> bool {
        self.lifecycle_state.trim() == BINDING_ACTIVE
    }

    pub fn is_active_read_write(&self) -> bool {
        self.is_active() && self.authority.trim() == "readWrite"
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceReceiptDoc {
    pub receipt_id: String,
    pub workspace_id: String,
    pub produced_by_request_id: String,
    pub produced_by_request_doc_id: String,
    pub kind: String,
    pub base_sha: String,
    pub seal_hash: String,
    pub head_sha: Option<String>,
    pub changed_files: Option<String>,
    pub diff_artifact: Option<String>,
    pub checks_run: Option<String>,
    pub unresolved_conflicts: Option<String>,
    pub integration_instructions: Option<String>,
}

pub trait WorkspaceDocuments {
    fn load_isolated_workspace(&self, workspace_id: &str) -> Result<Option<IsolatedWorkspaceDoc>>;
    fn load_placement(&self, workspace_id: &str) -> Result<Option<WorkspacePlacementDoc>>;
    fn write_isolated_workspace(&mut self, doc: IsolatedWorkspaceDoc) -> Result<()>;
    fn write_placement(&mut self, doc: WorkspacePlacementDoc) -> Result<()>;
    fn load_bindings(&self, workspace_id: &str) -> Result<Vec<WorkspaceBindingDoc>>;
    fn write_binding(&mut self, doc: WorkspaceBindingDoc) -> Result<()>;
    fn load_receipts(&self, workspace_id: &str) -> Result<Vec<WorkspaceReceiptDoc>>;
    fn write_receipt(&mut self, doc: WorkspaceReceiptDoc) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct MemoryWorkspaceDocuments {
    pub workspaces: HashMap<String, IsolatedWorkspaceDoc>,
    pub placements: HashMap<String, WorkspacePlacementDoc>,
    pub bindings: HashMap<String, WorkspaceBindingDoc>,
    pub receipts: HashMap<String, WorkspaceReceiptDoc>,
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

    fn load_bindings(&self, workspace_id: &str) -> Result<Vec<WorkspaceBindingDoc>> {
        Ok(self
            .bindings
            .values()
            .filter(|binding| binding.workspace_id == workspace_id)
            .cloned()
            .collect())
    }

    fn write_binding(&mut self, doc: WorkspaceBindingDoc) -> Result<()> {
        self.bindings.insert(doc.binding_id.clone(), doc);
        Ok(())
    }

    fn load_receipts(&self, workspace_id: &str) -> Result<Vec<WorkspaceReceiptDoc>> {
        Ok(self
            .receipts
            .values()
            .filter(|receipt| receipt.workspace_id == workspace_id)
            .cloned()
            .collect())
    }

    fn write_receipt(&mut self, doc: WorkspaceReceiptDoc) -> Result<()> {
        self.receipts.insert(doc.receipt_id.clone(), doc);
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
    instruction_manifest: String,
    seal_hash: Option<String>,
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
        instruction_manifest,
        seal_hash,
        lifecycle_state: lifecycle_state.to_string(),
        caused_by_invocation_id: caused_by_invocation_id.to_string(),
        caused_by_correlation: caused_by_correlation.to_string(),
    }
}

/// Upsert IsolatedWorkspace. Identity fields are add-only; retries may only
/// update mutable `lifecycle_state` / `seal_hash`.
pub fn isolated_workspace_upsert_mutation(doc: &IsolatedWorkspaceDoc) -> String {
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
            upsert_IsolatedWorkspace(
                filter: {{ workspace_id: {{ _eq: "{workspace_id}" }} }},
                add: {{
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
                }},
                update: {{
                    lifecycle_state: "{lifecycle_state}",
                    seal_hash: {seal_hash}
                }}
            ) {{ _docID }}
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

pub fn workspace_binding_upsert_mutation(doc: &WorkspaceBindingDoc) -> String {
    let seal_hash = graphql_nullable_string(doc.seal_hash.as_deref());
    format!(
        r#"mutation {{
            upsert_WorkspaceBinding(
                filter: {{ binding_id: {{ _eq: "{binding_id}" }} }},
                add: {{
                    binding_id: "{binding_id}",
                    workspace_id: "{workspace_id}",
                    request_id: "{request_id}",
                    request_doc_id: "{request_doc_id}",
                    authority: "{authority}",
                    deployment_id: "{deployment_id}",
                    seal_hash: {seal_hash},
                    lifecycle_state: "{lifecycle_state}"
                }},
                update: {{
                    lifecycle_state: "{lifecycle_state}"
                }}
            ) {{ _docID }}
        }}"#,
        binding_id = escape_graphql_string(&doc.binding_id),
        workspace_id = escape_graphql_string(&doc.workspace_id),
        request_id = escape_graphql_string(&doc.request_id),
        request_doc_id = escape_graphql_string(&doc.request_doc_id),
        authority = escape_graphql_string(&doc.authority),
        deployment_id = escape_graphql_string(&doc.deployment_id),
        lifecycle_state = escape_graphql_string(&doc.lifecycle_state),
    )
}

pub fn workspace_bindings_upsert_mutation(docs: &[WorkspaceBindingDoc]) -> String {
    let fields: Vec<String> = docs
        .iter()
        .enumerate()
        .map(|(index, doc)| workspace_binding_upsert_field(&format!("bind{index}"), doc))
        .collect();
    format!("mutation {{\n{}\n}}", fields.join("\n"))
}

/// One GraphQL mutation for the seal recovery unit: workspace, placement,
/// writer receipt, and any binding releases.
pub fn workspace_seal_docs_mutation(
    workspace: &IsolatedWorkspaceDoc,
    placement: &WorkspacePlacementDoc,
    bindings: &[WorkspaceBindingDoc],
    receipt: &WorkspaceReceiptDoc,
    updated_at: &str,
) -> String {
    let mut fields = vec![
        isolated_workspace_upsert_field("ws", workspace),
        workspace_placement_upsert_field("place", placement, updated_at),
        workspace_receipt_upsert_field("receipt", receipt),
    ];
    for (index, binding) in bindings.iter().enumerate() {
        fields.push(workspace_binding_upsert_field(
            &format!("bind{index}"),
            binding,
        ));
    }
    format!("mutation {{\n{}\n}}", fields.join("\n"))
}

fn isolated_workspace_upsert_field(alias: &str, doc: &IsolatedWorkspaceDoc) -> String {
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
        r#"{alias}: upsert_IsolatedWorkspace(
                filter: {{ workspace_id: {{ _eq: "{workspace_id}" }} }},
                add: {{
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
                }},
                update: {{
                    lifecycle_state: "{lifecycle_state}",
                    seal_hash: {seal_hash}
                }}
            ) {{ _docID }}"#,
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

fn workspace_placement_upsert_field(
    alias: &str,
    doc: &WorkspacePlacementDoc,
    updated_at: &str,
) -> String {
    let dirty_base = if doc.dirty_base { "true" } else { "false" };
    let updated_at = escape_graphql_string(updated_at);
    format!(
        r#"{alias}: upsert_WorkspacePlacement(
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
            ) {{ _docID }}"#,
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

fn workspace_binding_upsert_field(alias: &str, doc: &WorkspaceBindingDoc) -> String {
    let seal_hash = graphql_nullable_string(doc.seal_hash.as_deref());
    format!(
        r#"{alias}: upsert_WorkspaceBinding(
                filter: {{ binding_id: {{ _eq: "{binding_id}" }} }},
                add: {{
                    binding_id: "{binding_id}",
                    workspace_id: "{workspace_id}",
                    request_id: "{request_id}",
                    request_doc_id: "{request_doc_id}",
                    authority: "{authority}",
                    deployment_id: "{deployment_id}",
                    seal_hash: {seal_hash},
                    lifecycle_state: "{lifecycle_state}"
                }},
                update: {{
                    lifecycle_state: "{lifecycle_state}"
                }}
            ) {{ _docID }}"#,
        binding_id = escape_graphql_string(&doc.binding_id),
        workspace_id = escape_graphql_string(&doc.workspace_id),
        request_id = escape_graphql_string(&doc.request_id),
        request_doc_id = escape_graphql_string(&doc.request_doc_id),
        authority = escape_graphql_string(&doc.authority),
        deployment_id = escape_graphql_string(&doc.deployment_id),
        lifecycle_state = escape_graphql_string(&doc.lifecycle_state),
    )
}

fn workspace_receipt_upsert_field(alias: &str, doc: &WorkspaceReceiptDoc) -> String {
    format!(
        r#"{alias}: upsert_WorkspaceReceipt(
                filter: {{ receipt_id: {{ _eq: "{receipt_id}" }} }},
                add: {{
                    receipt_id: "{receipt_id}",
                    workspace_id: "{workspace_id}",
                    produced_by_request_id: "{produced_by_request_id}",
                    produced_by_request_doc_id: "{produced_by_request_doc_id}",
                    kind: "{kind}",
                    base_sha: "{base_sha}",
                    seal_hash: "{seal_hash}",
                    head_sha: {head_sha},
                    changed_files: {changed_files},
                    diff_artifact: {diff_artifact},
                    checks_run: {checks_run},
                    unresolved_conflicts: {unresolved_conflicts},
                    integration_instructions: {integration_instructions}
                }},
                update: {{
                    head_sha: {head_sha},
                    changed_files: {changed_files},
                    diff_artifact: {diff_artifact},
                    checks_run: {checks_run},
                    unresolved_conflicts: {unresolved_conflicts},
                    integration_instructions: {integration_instructions}
                }}
            ) {{ _docID }}"#,
        receipt_id = escape_graphql_string(&doc.receipt_id),
        workspace_id = escape_graphql_string(&doc.workspace_id),
        produced_by_request_id = escape_graphql_string(&doc.produced_by_request_id),
        produced_by_request_doc_id = escape_graphql_string(&doc.produced_by_request_doc_id),
        kind = escape_graphql_string(&doc.kind),
        base_sha = escape_graphql_string(&doc.base_sha),
        seal_hash = escape_graphql_string(&doc.seal_hash),
        head_sha = graphql_nullable_string(doc.head_sha.as_deref()),
        changed_files = graphql_nullable_string(doc.changed_files.as_deref()),
        diff_artifact = graphql_nullable_string(doc.diff_artifact.as_deref()),
        checks_run = graphql_nullable_string(doc.checks_run.as_deref()),
        unresolved_conflicts = graphql_nullable_string(doc.unresolved_conflicts.as_deref()),
        integration_instructions = graphql_nullable_string(doc.integration_instructions.as_deref()),
    )
}

pub fn workspace_receipt_create_mutation(doc: &WorkspaceReceiptDoc) -> String {
    format!(
        r#"mutation {{
            upsert_WorkspaceReceipt(
                filter: {{ receipt_id: {{ _eq: "{receipt_id}" }} }},
                add: {{
                    receipt_id: "{receipt_id}",
                    workspace_id: "{workspace_id}",
                    produced_by_request_id: "{produced_by_request_id}",
                    produced_by_request_doc_id: "{produced_by_request_doc_id}",
                    kind: "{kind}",
                    base_sha: "{base_sha}",
                    seal_hash: "{seal_hash}",
                    head_sha: {head_sha},
                    changed_files: {changed_files},
                    diff_artifact: {diff_artifact},
                    checks_run: {checks_run},
                    unresolved_conflicts: {unresolved_conflicts},
                    integration_instructions: {integration_instructions}
                }},
                update: {{
                    head_sha: {head_sha},
                    changed_files: {changed_files},
                    diff_artifact: {diff_artifact},
                    checks_run: {checks_run},
                    unresolved_conflicts: {unresolved_conflicts},
                    integration_instructions: {integration_instructions}
                }}
            ) {{ _docID }}
        }}"#,
        receipt_id = escape_graphql_string(&doc.receipt_id),
        workspace_id = escape_graphql_string(&doc.workspace_id),
        produced_by_request_id = escape_graphql_string(&doc.produced_by_request_id),
        produced_by_request_doc_id = escape_graphql_string(&doc.produced_by_request_doc_id),
        kind = escape_graphql_string(&doc.kind),
        base_sha = escape_graphql_string(&doc.base_sha),
        seal_hash = escape_graphql_string(&doc.seal_hash),
        head_sha = graphql_nullable_string(doc.head_sha.as_deref()),
        changed_files = graphql_nullable_string(doc.changed_files.as_deref()),
        diff_artifact = graphql_nullable_string(doc.diff_artifact.as_deref()),
        checks_run = graphql_nullable_string(doc.checks_run.as_deref()),
        unresolved_conflicts = graphql_nullable_string(doc.unresolved_conflicts.as_deref()),
        integration_instructions = graphql_nullable_string(doc.integration_instructions.as_deref()),
    )
}

fn deserialize_null_string<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_default())
}

fn graphql_nullable_string(value: Option<&str>) -> String {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value) => format!(r#""{}""#, escape_graphql_string(value)),
        None => "null".to_string(),
    }
}

pub(crate) fn writer_receipt_id(workspace_id: &str, request_id: &str) -> String {
    format!("receipt-writer-{workspace_id}-{request_id}")
}

pub(crate) fn binding_id_for(workspace_id: &str, request_id: &str) -> String {
    format!("wb-{workspace_id}-{request_id}")
}
