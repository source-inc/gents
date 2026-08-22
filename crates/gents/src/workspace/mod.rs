//! Journaled host executor for isolated workspaces (PR 4).
//!
//! ActionPlans carry no absolute destination. The host places a worktree as a
//! deterministic child of the repository checkout parent, journals each action
//! Validated → Executing → EffectObserved → ResultDocsWritten, and writes a
//! replicated [`IsolatedWorkspaceDoc`] plus a local [`WorkspacePlacementDoc`].

mod action_plan;
pub(crate) mod adapter;
mod documents;
mod executor;
pub(crate) mod journal;
pub(crate) mod overlay;

pub(crate) use action_plan::{
    action_plan_canonical_json, canonical_json_string, parse_action_plan_json, ACTION_PLAN_ABI,
};
pub use action_plan::{
    emit_create_workspace_plan, ActionPlan, CreateWorkspaceAction, CreationPolicy, HostAction,
    WorkspaceAdapterKind, CAP_CLONE_ARTIFACTS, CAP_CREATE_WORKSPACE, CAP_OBSERVE_DIRTY_BASE,
    DEFAULT_MAKE_WORKTREE_ARTIFACTS,
};
pub use documents::{
    isolated_workspace_upsert_mutation, workspace_placement_upsert_mutation, IsolatedWorkspaceDoc,
    MemoryWorkspaceDocuments, ProvisioningObservation, WorkspaceDocuments, WorkspacePlacementDoc,
};
pub use executor::{
    execute_create_workspace_plan, workspace_host_path, CreateWorkspaceOutcome, HostExecuteError,
    HostExecutorContext, LogicalWorkspaceIdentity, RepositoryPlacementRef,
};
pub use journal::{action_journal_prefix_legal, ActionJournalEntry, ActionJournalState};

pub(crate) use overlay::{
    request_workspace_cwd, resolve_request_workspace_overlay, workspace_authority_file_mode,
};

#[cfg(test)]
pub(crate) use overlay::{
    bind_workspace_overlay, IsolatedWorkspaceRecord, WorkspaceBindInput, WorkspacePlacementRecord,
};

#[cfg(test)]
mod tests;
