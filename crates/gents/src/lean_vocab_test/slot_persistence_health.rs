use super::*;

#[derive(Debug, Deserialize)]
pub(crate) struct LeanInferenceSlotAccountingCase {
    pub(crate) name: String,
    pub(crate) property: String,
    pub(crate) backend_id: String,
    pub(crate) pre_state: String,
    pub(crate) post_state: String,
    pub(crate) contribution: usize,
    pub(crate) expected_contribution: usize,
    pub(crate) pre_contribution: usize,
    pub(crate) post_contribution: usize,
    pub(crate) released_slot: bool,
    pub(crate) permit_drop_terminalization: bool,
    pub(crate) row_states: Vec<String>,
    pub(crate) row_backend_ids: Vec<String>,
    pub(crate) reconstructed_running_count: usize,
    pub(crate) max_concurrent: usize,
    pub(crate) bounded_by_max_concurrent: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanInferenceCallExactTargetCase {
    pub(crate) name: String,
    pub(crate) action: String,
    pub(crate) write_target: String,
    pub(crate) target_present: bool,
    pub(crate) expected_state: String,
    pub(crate) target_owner: usize,
    pub(crate) target_epoch: usize,
    pub(crate) expected_owner: usize,
    pub(crate) expected_epoch: usize,
    pub(crate) requested_post_state: String,
    pub(crate) target_pre_state: Option<String>,
    pub(crate) target_post_state: Option<String>,
    pub(crate) sibling_pre_state: String,
    pub(crate) sibling_post_state: String,
    pub(crate) write_matched: bool,
    pub(crate) sibling_isolated: bool,
    pub(crate) same_logical_call_id: bool,
    pub(crate) terminal_pre_state: bool,
    pub(crate) terminal_irreversible: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanInferenceCallExactTargetTraceCase {
    pub(crate) name: String,
    pub(crate) scenario: String,
    pub(crate) target_pre_state: String,
    pub(crate) sibling_pre_state: String,
    pub(crate) visible_logical_document_count: usize,
    pub(crate) unique_admission_required: bool,
    pub(crate) raw_independent_cas_possible: bool,
    pub(crate) first_target: String,
    pub(crate) first_action: String,
    pub(crate) first_expected_state: String,
    pub(crate) first_expected_owner: usize,
    pub(crate) first_expected_epoch: usize,
    pub(crate) first_requested_post_state: String,
    pub(crate) first_cas_matched: bool,
    pub(crate) second_target: String,
    pub(crate) second_action: String,
    pub(crate) second_expected_state: String,
    pub(crate) second_expected_owner: usize,
    pub(crate) second_expected_epoch: usize,
    pub(crate) second_requested_post_state: String,
    pub(crate) second_cas_matched: bool,
    pub(crate) second_disposition: String,
    pub(crate) final_target_state: String,
    pub(crate) final_sibling_state: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanFleetSlotAccountingCase {
    pub(crate) name: String,
    pub(crate) property: String,
    pub(crate) backend_id: String,
    pub(crate) request_state: String,
    pub(crate) admission_state: String,
    pub(crate) contribution: usize,
    pub(crate) expected_contribution: usize,
    pub(crate) active_count: usize,
    pub(crate) scheduler_running: usize,
    pub(crate) slot_count: usize,
    pub(crate) row_states: Vec<String>,
    pub(crate) row_backend_ids: Vec<String>,
    pub(crate) reconstructed_running_count: usize,
    pub(crate) max_concurrent: usize,
    pub(crate) bounded_by_max_concurrent: bool,
    pub(crate) aggregate_reconstructed_not_persisted: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanPersistenceFailurePolicyCase {
    pub(crate) name: String,
    pub(crate) policy: String,
    pub(crate) action: String,
    pub(crate) pre_persistence: String,
    pub(crate) post_persistence: String,
    pub(crate) post_storage_observation: String,
    pub(crate) hook_decision: String,
    pub(crate) records_failure: bool,
    pub(crate) records_success: bool,
    pub(crate) external_durability_claimed: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanStorageObservationRuntimeCase {
    pub(crate) name: String,
    pub(crate) policy: String,
    pub(crate) action: String,
    pub(crate) pre_observation: String,
    pub(crate) mutation_result: String,
    pub(crate) post_observation: String,
    pub(crate) post_persistence: String,
    pub(crate) hook_result: String,
    pub(crate) records_failure: bool,
    pub(crate) records_success: bool,
    pub(crate) terminal_write_observed: bool,
    pub(crate) external_visibility_claimed: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanBackendHealthAdmissionCase {
    pub(crate) name: String,
    pub(crate) enabled: bool,
    pub(crate) probe_status: String,
    pub(crate) expected_available: bool,
    pub(crate) admission_decision: String,
    pub(crate) observed_document_only: bool,
    pub(crate) external_endpoint_freshness_claimed: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanNativeFilesystemBoundaryCase {
    pub(crate) name: String,
    pub(crate) tool_name: String,
    pub(crate) work_class: String,
    pub(crate) boundary: String,
    pub(crate) inner_poll_blocks: bool,
    pub(crate) request_deadline_ms: usize,
    pub(crate) blocker_ms: usize,
    pub(crate) expected_terminal: String,
    pub(crate) expected_failure_class: Option<String>,
    pub(crate) queue_advances_before_blocker_returns: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanManagedExecToolBoundaryCase {
    pub(crate) name: String,
    pub(crate) tool_name: String,
    pub(crate) work_class: String,
    pub(crate) boundary: String,
    pub(crate) kill_scope: String,
    pub(crate) timeout_requires_kill: bool,
    pub(crate) cancel_requires_kill: bool,
    pub(crate) descendants_in_termination_scope: bool,
    pub(crate) capture_drain_bounded: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanPairingReconcileShutdownBoundaryCase {
    pub(crate) name: String,
    pub(crate) supervisor: String,
    pub(crate) work_class: String,
    pub(crate) boundary: String,
    pub(crate) per_admin_call_timeout_ms: usize,
    pub(crate) cancellation_observed_inside_sweep: bool,
    pub(crate) current_admin_future_dropped: bool,
    pub(crate) remaining_peers_skipped: bool,
    pub(crate) shutdown_join_bounded: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanPairingReconcileSweepRetryBoundaryCase {
    pub(crate) name: String,
    pub(crate) supervisor: String,
    pub(crate) work_class: String,
    pub(crate) boundary: String,
    pub(crate) failure_scope: String,
    pub(crate) failure_terminal: bool,
    pub(crate) retry_trigger: String,
    pub(crate) cancellation_prioritized: bool,
    pub(crate) convergence_retried: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanPairingReconcileSweepSchedulingCase {
    pub(crate) name: String,
    pub(crate) supervisor: String,
    pub(crate) work_class: String,
    pub(crate) boundary: String,
    pub(crate) max_concurrent_peer_preparations: usize,
    pub(crate) peer_preparation_bounded: bool,
    pub(crate) topology_mutation_serialized: bool,
    pub(crate) stale_peer_blocks_ready_peer: bool,
    pub(crate) every_peer_result_accounted: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanManagedExecLivenessCase {
    pub(crate) name: String,
    pub(crate) trigger: String,
    pub(crate) pre_exec_state: String,
    pub(crate) pre_tool_state: String,
    pub(crate) expected_exec_state: String,
    pub(crate) expected_tool_state: String,
    pub(crate) max_steps: usize,
    pub(crate) kill_signal_required: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanToolPreflightCase {
    pub(crate) name: String,
    pub(crate) health: String,
    pub(crate) schema_status: String,
    pub(crate) decision: String,
    pub(crate) failure_class: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanToolRetryCase {
    pub(crate) name: String,
    pub(crate) operation: String,
    pub(crate) idempotency: String,
    pub(crate) failure_class: String,
    pub(crate) disposition: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanCompletionRetryCase {
    pub(crate) name: String,
    pub(crate) domain: String,
    pub(crate) action: String,
    pub(crate) rust_surface: String,
    pub(crate) failure_class: Option<String>,
    pub(crate) selected_wake: Option<usize>,
    pub(crate) legal: bool,
    pub(crate) pre_phase: String,
    pub(crate) expected_phase: Option<String>,
    pub(crate) intermediate_phase: Option<String>,
    pub(crate) expected_transport_used: Option<usize>,
    pub(crate) expected_resample_used: Option<usize>,
    pub(crate) expected_repair_used: Option<bool>,
    pub(crate) expected_last_parse_error: Option<String>,
    pub(crate) expected_turn_index: Option<usize>,
    pub(crate) intermediate_turn_index: Option<usize>,
    pub(crate) expected_effects: Option<usize>,
    pub(crate) expected_rendered: Option<usize>,
    pub(crate) intermediate_rendered: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanMcpHealthCase {
    pub(crate) name: String,
    pub(crate) start_state: String,
    pub(crate) start_count: usize,
    pub(crate) event: String,
    pub(crate) threshold_k: usize,
    pub(crate) next_state: Option<String>,
    pub(crate) next_count: Option<usize>,
    pub(crate) rust_projection: Option<String>,
}

/// Generated witness for `Proofs.BackendHealth.step` (#640): the scheduled
/// inference-backend prober's per-runtime hysteresis machine. Unlike
/// `LeanMcpHealthCase` the machine is total (no removal), so `next_state` /
/// `next_count` are non-optional, and each row carries the `blocks_routing`
/// projection of the next state (the routing veto the admission merge
/// consumes).
#[derive(Debug, Deserialize)]
pub(crate) struct LeanBackendHealthCase {
    pub(crate) name: String,
    pub(crate) start_state: String,
    pub(crate) start_count: usize,
    pub(crate) event: String,
    pub(crate) threshold_k: usize,
    pub(crate) next_state: String,
    pub(crate) next_count: usize,
    pub(crate) blocks_routing: bool,
}
