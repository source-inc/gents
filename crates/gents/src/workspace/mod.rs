//! Journaled host executor for isolated workspaces (PR 4 / PR 7 / PR 8).
//!
//! ActionPlans carry no absolute destination. The host places a worktree as a
//! deterministic child of the repository checkout parent, journals each action
//! Validated → Executing → EffectObserved → ResultDocsWritten, and writes a
//! replicated [`IsolatedWorkspaceDoc`] plus a local [`WorkspacePlacementDoc`].
//! Writer success seals the tree, persists a [`WorkspaceReceiptDoc`], and
//! freezes instruction provenance from `base_sha`. After seal, concurrent
//! ReadOnly reviewers bind with `seal_hash` checks. Integration and cleanup
//! are typed host actions — never worker bash against trunk, never implicit
//! disk teardown.

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
    emit_cleanup_workspace_plan, emit_create_workspace_plan, emit_integrate_workspace_plan,
    emit_seal_workspace_plan, ActionPlan, CleanupWorkspaceAction, CreateWorkspaceAction,
    CreationPolicy, HostAction, IntegrateMode, IntegrateWorkspaceAction, SealWorkspaceAction,
    WorkspaceAdapterKind, CAP_CLEANUP_WORKSPACE, CAP_CLONE_ARTIFACTS, CAP_CREATE_WORKSPACE,
    CAP_INTEGRATE_WORKSPACE, CAP_OBSERVE_DIRTY_BASE, CAP_SEAL_WORKSPACE,
    DEFAULT_MAKE_WORKTREE_ARTIFACTS,
};
pub use documents::{
    isolated_workspace_upsert_mutation, workspace_binding_upsert_mutation,
    workspace_cleanup_docs_mutation, workspace_placement_upsert_mutation,
    workspace_receipt_create_mutation, IsolatedWorkspaceDoc, MemoryWorkspaceDocuments,
    ProvisioningObservation, WorkspaceBindingDoc, WorkspaceDocuments, WorkspacePlacementDoc,
    WorkspaceReceiptDoc,
};
pub use executor::{
    execute_cleanup_workspace_plan, execute_create_workspace_plan,
    execute_integrate_workspace_plan, execute_seal_workspace_plan, finalize_integrate_trunk,
    workspace_host_path, CleanupWorkspaceOutcome, CreateWorkspaceOutcome, HostExecuteError,
    HostExecutorContext, IntegrateWorkspaceOutcome, LogicalWorkspaceIdentity,
    RepositoryPlacementRef, SealWorkspaceOutcome,
};
pub use instructions::{
    instruction_context_section, InstructionFile, InstructionManifest, DEFAULT_INSTRUCTION_PATHS,
};
pub use journal::{action_journal_prefix_legal, ActionJournalEntry, ActionJournalState};
pub use runtime::cleanup_workspace;
pub(crate) use runtime::{
    integrate_on_integrator_success, materialize_workspace_binding, release_writer_binding,
    seal_on_writer_success, stamp_workspace_lineage, writer_request_already_sealed,
};

pub(crate) use overlay::{
    install_process_operator_tool_root, load_enabled_workspace_roots, process_operator_tool_root,
    request_workspace_cwd, require_under_ceiling, resolve_request_workspace_overlay,
    workspace_authority_file_mode,
};

#[cfg(test)]
pub(crate) use overlay::{
    bind_workspace_overlay, IsolatedWorkspaceRecord, WorkspaceBindInput, WorkspacePlacementRecord,
};

#[cfg(test)]
mod tests;
