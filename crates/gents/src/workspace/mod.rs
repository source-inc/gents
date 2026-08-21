//! Journaled host executor for isolated workspaces (PR 4 / PR 7).
//!
//! ActionPlans carry no absolute destination. The host places a worktree as a
//! deterministic child of the repository checkout parent, journals each action
//! Validated → Executing → EffectObserved → ResultDocsWritten, and writes a
//! replicated [`IsolatedWorkspaceDoc`] plus a local [`WorkspacePlacementDoc`].
//! Writer success seals the tree, persists a [`WorkspaceReceiptDoc`], and
//! freezes instruction provenance from `base_sha`.

mod action_plan;
pub(crate) mod adapter;
mod binding;
mod documents;
mod executor;
mod instructions;
pub(crate) mod journal;
pub(crate) mod overlay;
mod runtime;

pub(crate) use action_plan::{
    action_plan_canonical_json, canonical_json_string, parse_action_plan_json, ACTION_PLAN_ABI,
};
pub use action_plan::{
    emit_create_workspace_plan, emit_seal_workspace_plan, ActionPlan, CreateWorkspaceAction,
    CreationPolicy, HostAction, SealWorkspaceAction, WorkspaceAdapterKind, CAP_CLONE_ARTIFACTS,
    CAP_CREATE_WORKSPACE, CAP_OBSERVE_DIRTY_BASE, CAP_SEAL_WORKSPACE,
    DEFAULT_MAKE_WORKTREE_ARTIFACTS,
};
pub use documents::{
    isolated_workspace_upsert_mutation, workspace_binding_upsert_mutation,
    workspace_placement_upsert_mutation, workspace_receipt_create_mutation, IsolatedWorkspaceDoc,
    MemoryWorkspaceDocuments, ProvisioningObservation, WorkspaceBindingDoc, WorkspaceDocuments,
    WorkspacePlacementDoc, WorkspaceReceiptDoc,
};
pub use executor::{
    execute_create_workspace_plan, execute_seal_workspace_plan, workspace_host_path,
    CreateWorkspaceOutcome, HostExecuteError, HostExecutorContext, LogicalWorkspaceIdentity,
    RepositoryPlacementRef, SealWorkspaceOutcome,
};
pub use instructions::{
    instruction_context_section, InstructionFile, InstructionManifest, DEFAULT_INSTRUCTION_PATHS,
};
pub use journal::{action_journal_prefix_legal, ActionJournalEntry, ActionJournalState};
pub(crate) use runtime::{release_writer_binding, seal_on_writer_success, stamp_workspace_lineage};

pub(crate) use overlay::{
    request_workspace_cwd, resolve_request_workspace_overlay, workspace_authority_file_mode,
};

#[cfg(test)]
pub(crate) use overlay::{
    bind_workspace_overlay, IsolatedWorkspaceRecord, WorkspaceBindInput, WorkspacePlacementRecord,
};

#[cfg(test)]
mod tests;
