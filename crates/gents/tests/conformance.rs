use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use gents::defra_node::EmbeddedNode;
use gents::event_delivery_contract::{
    runtime_event_delivery_source_contracts, EventDeliverySourceContract,
};
use gents::graphql::escape_graphql_string;
use gents::lifecycle::{ClaimOutcome, ExecutionOrigin, TriggerLineage};
use gents::llm::message::{
    AssistantContent, Message, Text, ToolCall, ToolFunction, ToolResult, ToolResultContent,
    UserContent,
};
use gents::llm::tool::BoxFuture;
use gents::llm::tool::ToolDefinition;
use gents::llm::tool::{ToolDyn, ToolError};
use gents::llm::{HookAction, ToolCallHookAction};
use gents::tool_call_lifecycle::{
    AwaitMode, CancelCause, CancelPolicy, CascadeDispatch, ChildTerminal, FailureClass,
    ToolCallLifecycle, MAX_SUBAGENT_DEPTH,
};
use gents::{
    fetch_interrupt_requested_at, interrupt_request, upsert_agent_behavior, upsert_tool_selection,
    write_manual_agent_request, AgentBehaviorDocument, AgentIdentity, BackgroundToolRegistry,
    DefraSessionHook, DefraStreamWriter, DefraWatcher, FailurePolicy, InferenceCall,
    RequestLifecycle, ToolSelectionDocument, Watcher,
};
use serde::Deserialize;
use serde_json::{json, Value};

#[path = "../src/admission/slot_accounting.rs"]
mod admission_slot_accounting;
#[path = "../src/lean_vocab_test/support.rs"]
mod lean_vocab_test;
mod support;

use admission_slot_accounting::{
    reconstructed_running_slot_count, slot_contribution, InferenceCallSlotRow,
};
use lean_vocab_test::{
    assert_lean_transition_is_illegal, assert_lean_transition_is_legal,
    assert_lifecycle_transition_cases_partition, assert_state_machine_contract_is_complete,
    lean_backend_health_cases, lean_bridge_step_cases, lean_cancel_propagation_cases,
    lean_client_shell_case, lean_codex_shim_behavior_selection_cases,
    lean_codex_shim_binding_cases, lean_codex_shim_compaction_projection_cases,
    lean_codex_shim_context_usage_cases, lean_codex_shim_projection_case,
    lean_codex_shim_projection_cases, lean_codex_shim_reasoning_projection_cases,
    lean_codex_shim_subagent_listing_cases, lean_codex_shim_subagent_metadata_cases,
    lean_codex_shim_subagent_status_cases, lean_codex_shim_subagent_thread_shape_cases,
    lean_codex_shim_subagent_tool_cases, lean_codex_shim_subagent_visibility_cases,
    lean_codex_shim_thread_status_cases, lean_codex_shim_tool_metadata_cases,
    lean_codex_shim_turn_lifecycle_cases, lean_command_env_case, lean_command_policy_case,
    lean_command_sandbox_case, lean_compaction_reducer_cases, lean_composed_invariant_witnesses,
    lean_contract_snapshot, lean_event_delivery_convergence_traces,
    lean_event_delivery_durable_admission_cases, lean_event_delivery_source_instances,
    lean_event_delivery_transition_cases, lean_fleet_slot_accounting_case,
    lean_inference_call_exact_target_cases, lean_inference_call_exact_target_trace_cases,
    lean_inference_slot_accounting_case, lean_inference_slot_accounting_cases,
    lean_managed_exec_liveness_cases, lean_managed_exec_tool_boundary_cases, lean_mcp_health_cases,
    lean_process_transition_cases, lean_queue_deadline_case, lean_queue_deadline_cases,
    lean_r4c_background_work_case, lean_r4c_background_work_cases, lean_r5_cross_deployment_cases,
    lean_r6_background_theorem_witness, lean_r6_background_theorem_witnesses,
    lean_r6_backgrounding_case, lean_r6_backgrounding_cases, lean_recovery_equivalence_cases,
    lean_recovery_outcome_cases, lean_recovery_sweep_cases, lean_request_transition_cases,
    lean_response_interrupt_flow_cases, lean_response_transition_cases,
    lean_restart_disposition_cases, lean_runtime_reconcile_case, lean_runtime_reconcile_cases,
    lean_session_recovery_case, lean_startup_readiness_cases, lean_state_machine_contract,
    lean_subagent_delegation_graph_cases, lean_tool_output_paging_cases, lean_transcript_case,
    lean_transcript_cases, lean_transcript_finalization_cases,
    lean_transcript_provider_history_cases, lean_vocabulary_values, LeanEventDeliveryAction,
    LeanLifecycleTransitionCase, LeanR4cBackgroundWorkCase,
};
use support::conformance_consumers::assert_registered_conformance_consumers_resolve;
use support::snapshots::{
    fetch_conversation_snapshot, fetch_message_snapshots_for_session,
    fetch_request_lineage_snapshot, fetch_request_lineage_snapshot_by_tuple,
    fetch_request_snapshot, fetch_request_snapshot_raw, fetch_response_content,
    fetch_response_interrupted_at, fetch_response_snapshot, fetch_session_snapshot,
    fetch_tool_call_snapshots_for_session, ConversationSnapshot, MessageSnapshot,
    RequestLineageSnapshot, RequestSnapshot, ResponseSnapshot, SessionSnapshot, ToolCallSnapshot,
};
use support::{
    build_request, conversation_status_by_doc_id, create_agent_session, create_conversation_row,
    create_request, create_response_with_content_and_status, create_response_with_status,
    first_optional_row, first_row, set_interrupt_requested_at, set_request_lifecycle_state,
    set_valid_until, test_db, test_db_with_duplicate_tolerant_conversations, test_db_with_identity,
    upsert_conversation, AGENT_DID, AGENT_NAME, BACKEND_ID, DEADLINE_SECS,
};

async fn signed_materializer_test_db(name: &str) -> support::TestDb {
    let identity: Arc<dyn AgentIdentity> = Arc::new(support::fixtures::test_identity(name));
    test_db_with_identity(name, identity).await
}

fn signed_materializer_agent_did(db: &support::TestDb) -> &str {
    db.node
        .node_identity_did()
        .expect("signed materializer fixture must configure a node identity")
}

#[path = "conformance/backend_health.rs"]
mod backend_health;
#[path = "conformance/background.rs"]
mod background;
#[path = "conformance/bearer_claim.rs"]
mod bearer_claim;
#[path = "conformance/cancel_propagation.rs"]
mod cancel_propagation;
#[path = "conformance/client_runtime.rs"]
mod client_runtime;
#[path = "conformance/codex_shim.rs"]
mod codex_shim;
#[path = "conformance/command_policy.rs"]
mod command_policy;
#[path = "conformance/compaction_gate.rs"]
mod compaction_gate;
#[path = "conformance/compaction_source_manifest.rs"]
mod compaction_source_manifest;
#[path = "conformance/completion_retry.rs"]
mod completion_retry;
#[path = "conformance/composed_invariants.rs"]
mod composed_invariants;
#[path = "conformance/config_replication.rs"]
mod config_replication;
#[path = "conformance/coverage.rs"]
mod coverage;
#[path = "conformance/directory_projection.rs"]
mod directory_projection;
#[path = "conformance/event_delivery.rs"]
mod event_delivery;
#[path = "conformance/fleet.rs"]
mod fleet;
#[path = "conformance/fork_provenance.rs"]
mod fork_provenance;
#[path = "conformance/goals.rs"]
mod goals;
#[path = "conformance/inference_call.rs"]
mod inference_call;
#[path = "conformance/inference_rendered_capture.rs"]
mod inference_rendered_capture;
#[path = "conformance/interrupts_manual.rs"]
mod interrupts_manual;
#[path = "conformance/managed_exec.rs"]
mod managed_exec;
#[path = "conformance/mcp_health.rs"]
mod mcp_health;
#[path = "conformance/p2p_observability.rs"]
mod p2p_observability;
#[path = "conformance/process.rs"]
mod process;
#[path = "conformance/prompt_template.rs"]
mod prompt_template;
#[path = "conformance/r5_cross_deployment.rs"]
mod r5_cross_deployment;
#[path = "conformance/reciprocal_conversation.rs"]
mod reciprocal_conversation;
#[path = "conformance/recovery_sweeps.rs"]
mod recovery_sweeps;
#[path = "conformance/replicated_request_convergence.rs"]
mod replicated_request_convergence;
#[path = "conformance/request_lifecycle.rs"]
mod request_lifecycle;
#[path = "conformance/response_outcome.rs"]
mod response_outcome;
#[path = "conformance/session_recovery.rs"]
mod session_recovery;
#[path = "conformance/startup_readiness.rs"]
mod startup_readiness;
#[path = "conformance/streaming_compaction.rs"]
mod streaming_compaction;
#[path = "conformance/tool_call.rs"]
mod tool_call;
#[path = "conformance/tool_fact.rs"]
mod tool_fact;
#[path = "conformance/transcript.rs"]
mod transcript;
#[path = "conformance/workflow_barrier.rs"]
mod workflow_barrier;

#[test]
fn lean_executable_contracts_cover_initial_domains() {
    coverage::lean_executable_contracts_cover_initial_domains();
}

#[tokio::test]
async fn generated_recovery_sweep_cases_drive_startup_recovery_contract() {
    recovery_sweeps::generated_recovery_sweep_cases_drive_startup_recovery_contract().await;
}

#[test]
fn generated_recovery_equivalence_cases_pin_uninterrupted_convergence_contract() {
    recovery_sweeps::generated_recovery_equivalence_cases_pin_uninterrupted_convergence_contract();
}

#[tokio::test]
async fn generated_recovery_outcome_cases_fence_duplicate_tolerant_counting() {
    recovery_sweeps::generated_recovery_outcome_cases_fence_duplicate_tolerant_counting().await;
}

#[tokio::test]
async fn generated_restart_disposition_cases_drive_recover_all() {
    recovery_sweeps::generated_restart_disposition_cases_drive_recover_all().await;
}

#[tokio::test]
async fn generated_read_tool_output_witness_drives_hook_dispatch() {
    background::generated_read_tool_output_witness_drives_hook_dispatch().await;
}

#[tokio::test]
async fn generated_bridge_step_cases_drive_bridge_lifecycle() {
    background::generated_bridge_step_cases_drive_bridge_lifecycle().await;
}

#[tokio::test]
async fn subagent_liveness_reconciliation_converges_expired_processing_to_zero() {
    recovery_sweeps::subagent_liveness_reconciliation_converges_expired_processing_to_zero().await;
}

#[tokio::test]
async fn startup_recovery_order_terminalizes_crash_orphaned_calls() {
    recovery_sweeps::startup_recovery_order_terminalizes_crash_orphaned_calls().await;
}

#[tokio::test]
async fn single_claimer_watcher_never_claims_foreign_replica() {
    replicated_request_convergence::single_claimer_watcher_never_claims_foreign_replica().await;
}

#[tokio::test]
async fn terminal_convergence_redrive_reasserts_unconverged_terminal() {
    replicated_request_convergence::terminal_convergence_redrive_reasserts_unconverged_terminal()
        .await;
}

#[tokio::test]
async fn terminal_redrive_window_advances_past_sixty_four_rows() {
    replicated_request_convergence::terminal_redrive_window_advances_past_sixty_four_rows().await;
}

#[tokio::test]
async fn durable_response_repairs_request_after_terminal_write_gap() {
    replicated_request_convergence::durable_response_repairs_request_after_terminal_write_gap()
        .await;
}

#[tokio::test]
async fn recover_stuck_requests_recovers_claimed_lifecycle_state() {
    replicated_request_convergence::recover_stuck_requests_recovers_claimed_lifecycle_state().await;
}

#[tokio::test]
async fn reconcile_coalesce_never_supersedes_foreign_replica() {
    replicated_request_convergence::reconcile_coalesce_never_supersedes_foreign_replica().await;
}

#[tokio::test]
async fn drain_wakeups_never_interrupts_foreign_replica() {
    replicated_request_convergence::drain_wakeups_never_interrupts_foreign_replica().await;
}

#[tokio::test]
async fn generated_r6_backgrounding_cases_drive_tool_backgrounding_contract() {
    background::generated_r6_backgrounding_cases_drive_tool_backgrounding_contract().await;
}

#[tokio::test]
async fn generated_r6_background_theorem_witnesses_drive_admission_budget_invariant() {
    background::generated_r6_background_theorem_witnesses_drive_admission_budget_invariant().await;
}

#[tokio::test]
async fn generated_r6_background_theorem_witnesses_drive_cascade_cancellation_trace() {
    background::generated_r6_background_theorem_witnesses_drive_cascade_cancellation_trace().await;
}

#[test]
fn generated_subagent_delegation_graph_cases_pin_gap2_contract() {
    background::generated_subagent_delegation_graph_cases_pin_gap2_contract();
}

#[tokio::test]
async fn generated_r5_cross_deployment_cases_drive_production_dispatch() {
    r5_cross_deployment::generated_r5_cross_deployment_cases_drive_production_dispatch().await;
}

#[tokio::test]
async fn generated_composed_invariant_witnesses_drive_tool_lifecycle_conformance() {
    composed_invariants::generated_composed_invariant_witnesses_drive_tool_lifecycle_conformance()
        .await;
}

#[tokio::test]
async fn cancel_propagation_cases_drive_production_interrupt() {
    cancel_propagation::cancel_propagation_cases_drive_production_interrupt().await;
}

#[test]
fn generated_r4c_background_work_cases_pin_observable_shapes() {
    background::generated_r4c_background_work_cases_pin_observable_shapes();
}

#[test]
fn generated_codex_shim_projection_cases_pin_adapter_mapping() {
    codex_shim::generated_codex_shim_projection_cases_pin_adapter_mapping();
}

#[test]
fn generated_codex_shim_binding_cases_pin_runnable_gated_binding() {
    codex_shim::generated_codex_shim_binding_cases_pin_runnable_gated_binding();
}

#[test]
fn generated_startup_readiness_cases_pin_bounded_barrier_release() {
    startup_readiness::generated_startup_readiness_cases_pin_bounded_barrier_release();
}

#[tokio::test]
async fn generated_transcript_cases_drive_agent_message_ordering_contract() {
    transcript::generated_transcript_cases_drive_agent_message_ordering_contract().await;
}

#[test]
fn generated_transcript_finalization_and_provider_history_cases_pin_split_contract() {
    transcript::generated_transcript_finalization_and_provider_history_cases_pin_split_contract();
}

#[tokio::test]
async fn generated_streaming_response_cases_pin_lifecycle_contract() {
    streaming_compaction::generated_streaming_response_cases_pin_lifecycle_contract().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generated_streaming_response_interrupt_flow_cases_drive_daemon_contract() {
    streaming_compaction::generated_streaming_response_interrupt_flow_cases_drive_daemon_contract()
        .await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn generated_streaming_response_idle_timeout_case_drives_daemon_contract() {
    streaming_compaction::generated_streaming_response_idle_timeout_case_drives_daemon_contract()
        .await;
}

#[test]
fn generated_compaction_reducer_cases_pin_contract() {
    streaming_compaction::generated_compaction_reducer_cases_pin_contract();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_gate_blocks_reduction_while_a_response_streams() {
    compaction_gate::compaction_gate_blocks_reduction_while_a_response_streams().await;
}

#[tokio::test]
async fn generated_session_recovery_cases_drive_db_backed_reissue_contract() {
    session_recovery::generated_session_recovery_cases_drive_db_backed_reissue_contract().await;
}

#[test]
fn generated_tool_execution_cases_cover_preflight_and_retry_contracts() {
    tool_execution::generated_tool_execution_cases_cover_preflight_and_retry_contracts();
}

#[test]
fn generated_tool_policy_cases_match_lean_composition() {
    tool_policy::generated_tool_policy_cases_match_lean_composition();
}

#[test]
fn self_config_field_tables_match_lean_contract() {
    self_config::self_config_field_tables_match_lean_contract();
}

#[test]
fn generated_self_config_cases_fence_patch_merge() {
    self_config::generated_self_config_cases_fence_patch_merge();
}

#[test]
fn completion_retry_lean_witness_cases_hold() {
    completion_retry::completion_retry_lean_witness_cases_hold();
}

#[test]
fn managed_exec_liveness_cases_pin_native_process_boundary() {
    managed_exec::managed_exec_liveness_cases_pin_native_process_boundary();
}

#[test]
fn managed_exec_tool_boundary_cases_cover_every_native_subprocess_tool() {
    managed_exec::managed_exec_tool_boundary_cases_cover_every_native_subprocess_tool();
}

#[test]
fn pairing_reconcile_shutdown_boundary_preempts_in_flight_sweep() {
    pairing_reconcile::pairing_reconcile_shutdown_boundary_preempts_in_flight_sweep();
}

#[test]
fn pairing_reconcile_top_level_sweep_failure_is_nonterminal_and_retried() {
    pairing_reconcile::pairing_reconcile_top_level_sweep_failure_is_nonterminal_and_retried();
}

#[test]
fn pairing_reconcile_sweep_does_not_head_of_line_block_ready_peer() {
    pairing_reconcile::pairing_reconcile_sweep_does_not_head_of_line_block_ready_peer();
}

#[test]
fn generated_mcp_health_cases_pin_threshold_projection_shape() {
    mcp_health::generated_mcp_health_cases_pin_threshold_projection_shape();
}

#[test]
fn generated_backend_health_cases_pin_threshold_and_veto_shape() {
    backend_health::generated_backend_health_cases_pin_threshold_and_veto_shape();
}

#[test]
fn edit_match_exact_priority_is_never_shadowed() {
    edit_match::exact_priority_is_never_shadowed();
}

#[test]
fn edit_match_ladder_fires_at_the_strictest_matching_rung() {
    edit_match::ladder_fires_at_the_strictest_matching_rung();
}

#[test]
fn edit_match_ambiguity_gate_requires_unique_or_replace_all() {
    edit_match::ambiguity_gate_requires_unique_or_replace_all();
}

#[test]
fn edit_match_decision_is_pure_and_deterministic() {
    edit_match::decision_is_pure_and_deterministic();
}

#[test]
fn edit_match_noop_is_reported_not_applied() {
    edit_match::noop_is_reported_not_applied();
}

#[test]
fn edit_match_operations_desugar_onto_the_single_matcher() {
    edit_match::operations_desugar_onto_the_single_matcher();
}

#[test]
fn edit_match_near_miss_is_diagnosed_never_applied() {
    edit_match::near_miss_is_diagnosed_never_applied();
}

#[test]
fn edit_match_overlapping_windows_apply_disjoint_selection() {
    edit_match::overlapping_windows_apply_disjoint_selection();
}

#[test]
fn generated_process_transition_cases_cover_runtime_status_policy_shape() {
    process::generated_process_transition_cases_cover_runtime_status_policy_shape();
}

#[tokio::test]
async fn generated_inference_slot_accounting_cases_drive_db_backed_reconstruction() {
    inference_call::generated_inference_slot_accounting_cases_drive_db_backed_reconstruction()
        .await;
}

#[tokio::test]
async fn generated_inference_call_exact_target_cases_drive_fenced_updates() {
    inference_call::generated_inference_call_exact_target_cases_drive_fenced_updates().await;
}

#[test]
fn generated_inference_rendered_capture_cases_pin_exact_version_composition() {
    inference_rendered_capture::generated_cases_pin_exact_version_composition();
}

#[test]
fn generated_tool_fact_cases_pin_exact_immutable_tool_facts() {
    tool_fact::generated_cases_pin_exact_immutable_tool_facts();
}

#[test]
fn generated_fork_provenance_cases_pin_exact_child_sources() {
    fork_provenance::generated_cases_pin_exact_fork_sources();
}

#[test]
fn generated_compaction_source_manifest_cases_pin_exact_immutable_sources() {
    compaction_source_manifest::generated_cases_pin_exact_immutable_compaction_sources();
}

#[test]
fn generated_response_outcome_cases_pin_exact_immutable_outcomes() {
    response_outcome::generated_cases_pin_exact_immutable_response_outcomes();
}

#[tokio::test]
async fn concurrent_exact_target_cas_serializes_one_winner_and_terminal_absorbs() {
    inference_call::concurrent_exact_target_cas_serializes_one_winner_and_terminal_absorbs().await;
}

#[test]
fn lean_emits_await_mode_vocabulary() {
    tool_call::lean_emits_await_mode_vocabulary();
}

#[test]
fn lean_emits_cancel_policy_vocabulary() {
    tool_call::lean_emits_cancel_policy_vocabulary();
}

#[test]
fn lean_emits_child_terminal_vocabulary_and_projections() {
    tool_call::lean_emits_child_terminal_vocabulary_and_projections();
}

#[test]
fn lean_tool_call_cancel_actions_name_cancel_cause() {
    tool_call::lean_tool_call_cancel_actions_name_cancel_cause();
}

#[test]
fn generated_slot_accounting_cases_pin_inference_and_fleet_contracts() {
    fleet::generated_slot_accounting_cases_pin_inference_and_fleet_contracts();
}

#[tokio::test]
async fn generated_queue_deadline_cases_pin_r4a_contract_rows() {
    request_lifecycle::generated_queue_deadline_cases_pin_r4a_contract_rows().await;
}

#[tokio::test]
async fn generated_request_transition_cases_cover_lifecycle_policy() {
    request_lifecycle::generated_request_transition_cases_cover_lifecycle_policy().await;
}

#[tokio::test]
async fn event_delivery_transition_cases_match_contract() {
    event_delivery::event_delivery_transition_cases_match_contract().await;
}

#[test]
fn event_delivery_source_instances_match_runtime() {
    event_delivery::event_delivery_source_instances_match_runtime();
}

#[test]
fn durable_event_admission_cases_match_contract() {
    event_delivery::durable_event_admission_cases_match_contract();
}

#[tokio::test]
async fn event_delivery_convergence_traces_match_runtime_or_deviation() {
    event_delivery::event_delivery_convergence_traces_match_runtime_or_deviation().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn event_delivery_sources_reopen_closed_subscriptions() {
    event_delivery::event_delivery_sources_reopen_closed_subscriptions().await;
}

#[path = "conformance/apply_reconcile.rs"]
mod apply_reconcile;
#[path = "conformance/docs.rs"]
mod docs;
#[path = "conformance/edit_match.rs"]
mod edit_match;
#[path = "conformance/identity.rs"]
mod identity;
#[path = "conformance/identity_proptest.rs"]
mod identity_proptest;
#[path = "conformance/live_overlay.rs"]
mod live_overlay;
#[path = "conformance/manual_run.rs"]
mod manual_run;
#[path = "conformance/pairing_invariant_tests.rs"]
mod pairing_invariant_tests;
#[path = "conformance/pairing_reconcile.rs"]
mod pairing_reconcile;
#[path = "conformance/peer_registry_discovery.rs"]
mod peer_registry_discovery;
#[path = "conformance/persona_request.rs"]
mod persona_request;
#[path = "conformance/prompt_assembly.rs"]
mod prompt_assembly;
#[path = "conformance/r5_scenarios.rs"]
mod r5_scenarios;
#[path = "conformance/rendered_capture.rs"]
mod rendered_capture;
#[path = "conformance/request_ingest.rs"]
mod request_ingest;
#[path = "conformance/scheduling.rs"]
mod scheduling;
#[path = "conformance/scope_templates.rs"]
mod scope_templates;
#[path = "conformance/self_config.rs"]
mod self_config;
#[path = "conformance/structure.rs"]
mod structure;
#[path = "conformance/subagent_source.rs"]
mod subagent_source;
#[path = "conformance/tool_execution.rs"]
mod tool_execution;
#[path = "conformance/tool_execution_subagent.rs"]
mod tool_execution_subagent;
#[path = "conformance/tool_policy.rs"]
mod tool_policy;
#[path = "conformance/triggers.rs"]
mod triggers;
