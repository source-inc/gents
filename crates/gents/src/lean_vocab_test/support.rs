#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use serde::Deserialize;

pub(crate) type LeanFeatureMatrix = BTreeMap<String, BTreeMap<String, LeanFeatureMatrixCell>>;

#[derive(Debug, Clone, Copy)]
pub(crate) struct LeanVocabulary<'a> {
    pub(crate) lean_file: &'a str,
    pub(crate) model: &'a str,
    pub(crate) namespace: &'a str,
    pub(crate) rust_source: &'a str,
    pub(crate) rust_values: &'a [&'a str],
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LeanContractVocabulary<'a> {
    pub(crate) domain: &'a str,
    pub(crate) rust_source: &'a str,
    pub(crate) rust_values: &'a [&'a str],
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanContractSnapshot {
    pub(crate) generated_by: String,
    pub(crate) vocabularies: Vec<LeanVocabularyContract>,
    pub(crate) state_machines: Vec<LeanStateMachineContract>,
    #[serde(default)]
    pub(crate) graph_pipeline_validation_cases: Vec<LeanGraphPipelineValidationCase>,
    #[serde(default)]
    pub(crate) graph_pipeline_revision_gate_cases: Vec<LeanGraphPipelineRevisionGateCase>,
    #[serde(default)]
    pub(crate) graph_pipeline_run_terminal_cases: Vec<LeanGraphPipelineRunTerminalCase>,
    pub(crate) request_transition_cases: Vec<LeanLifecycleTransitionCase>,
    pub(crate) process_transition_cases: Vec<LeanLifecycleTransitionCase>,
    pub(crate) provider_eof_cases: Vec<LeanProviderEofCase>,
    pub(crate) request_execution_lease_cases: Vec<LeanRequestExecutionLeaseCase>,
    pub(crate) request_execution_lease_trace_cases: Vec<LeanRequestExecutionLeaseTraceCase>,
    pub(crate) trigger_dispatch_case_count: usize,
    pub(crate) trigger_dispatch_cases: Vec<LeanTriggerDispatchCase>,
    pub(crate) trigger_group_case_count: usize,
    pub(crate) trigger_group_cases: Vec<LeanTriggerGroupCase>,
    #[serde(default)]
    pub(crate) goal_decision_cases: Vec<LeanGoalDecisionCase>,
    #[serde(default)]
    pub(crate) goal_transition_cases: Vec<LeanGoalTransitionCase>,
    #[serde(default)]
    pub(crate) goal_create_cases: Vec<LeanGoalCreateCase>,
    #[serde(default)]
    pub(crate) task_goal_publication_cases: Vec<LeanTaskGoalPublicationCase>,
    #[serde(default)]
    pub(crate) task_goal_recovery_cases: Vec<LeanTaskGoalRecoveryCase>,
    #[serde(default)]
    pub(crate) goal_submission_cases: Vec<LeanGoalSubmissionCase>,
    #[serde(default)]
    pub(crate) goal_continuation_materialization_cases:
        Vec<LeanGoalContinuationMaterializationCase>,
    #[serde(default)]
    pub(crate) session_hydration_decision_cases: Vec<LeanSessionHydrationDecisionCase>,
    #[serde(default)]
    pub(crate) session_hydration_progress_cases: Vec<LeanSessionHydrationProgressCase>,
    #[serde(default)]
    pub(crate) session_hydration_durable_cases: Vec<LeanSessionHydrationDurableCase>,
    #[serde(default)]
    pub(crate) enrollment_cases: Vec<LeanEnrollmentCase>,
    pub(crate) enrollment_durable_projection_cases: Vec<LeanEnrollmentDurableProjectionCase>,
    #[serde(default)]
    pub(crate) enrollment_encoding_cases: Vec<LeanEnrollmentEncodingCase>,
    #[serde(default)]
    pub(crate) enrollment_digest_cases: Vec<LeanEnrollmentDigestCase>,
    #[serde(default)]
    pub(crate) agent_request_admission_cases: Vec<LeanAgentRequestAdmissionCase>,
    pub(crate) frontend_client_shell_case_count: usize,
    pub(crate) frontend_client_shell_cases: Vec<LeanClientShellCase>,
    pub(crate) desktop_client_shell_case_count: usize,
    pub(crate) desktop_client_shell_cases: Vec<LeanClientShellCase>,
    pub(crate) request_lifecycle_operator_ui_cases: Vec<LeanClientShellCase>,
    pub(crate) runtime_reconcile_cases: Vec<LeanRuntimeReconcileCase>,
    pub(crate) client_behavior_readiness_cases: Vec<LeanClientBehaviorReadinessCase>,
    #[serde(default)]
    pub(crate) startup_readiness_cases: Vec<LeanStartupReadinessCase>,
    pub(crate) apply_reconcile_cases: Vec<LeanApplyReconcileCase>,
    #[serde(default)]
    pub(crate) tool_policy_cases: Vec<LeanToolPolicyCase>,
    #[serde(default)]
    pub(crate) goal_capability_resolution_cases: Vec<LeanGoalCapabilityResolutionCase>,
    #[serde(default)]
    pub(crate) lsp_action_cases: Vec<LeanLspActionCase>,
    #[serde(default)]
    pub(crate) self_config_field_tables: Vec<LeanSelfConfigFieldTable>,
    #[serde(default)]
    pub(crate) self_config_cases: Vec<LeanSelfConfigCase>,
    pub(crate) session_recovery_cases: Vec<LeanSessionRecoveryCase>,
    pub(crate) inference_slot_accounting_cases: Vec<LeanInferenceSlotAccountingCase>,
    pub(crate) fleet_slot_accounting_cases: Vec<LeanFleetSlotAccountingCase>,
    pub(crate) persistence_failure_policy_cases: Vec<LeanPersistenceFailurePolicyCase>,
    pub(crate) storage_observation_runtime_cases: Vec<LeanStorageObservationRuntimeCase>,
    pub(crate) backend_health_admission_cases: Vec<LeanBackendHealthAdmissionCase>,
    pub(crate) native_filesystem_boundary_cases: Vec<LeanNativeFilesystemBoundaryCase>,
    pub(crate) managed_exec_tool_boundary_cases: Vec<LeanManagedExecToolBoundaryCase>,
    pub(crate) pairing_reconcile_shutdown_boundary_cases:
        Vec<LeanPairingReconcileShutdownBoundaryCase>,
    pub(crate) pairing_reconcile_sweep_retry_boundary_cases:
        Vec<LeanPairingReconcileSweepRetryBoundaryCase>,
    pub(crate) pairing_reconcile_sweep_scheduling_cases:
        Vec<LeanPairingReconcileSweepSchedulingCase>,
    pub(crate) managed_exec_liveness_cases: Vec<LeanManagedExecLivenessCase>,
    pub(crate) tool_preflight_cases: Vec<LeanToolPreflightCase>,
    pub(crate) tool_retry_cases: Vec<LeanToolRetryCase>,
    #[serde(default)]
    pub(crate) completion_retry_cases: Vec<LeanCompletionRetryCase>,
    pub(crate) mcp_health_cases: Vec<LeanMcpHealthCase>,
    pub(crate) backend_health_cases: Vec<LeanBackendHealthCase>,
    pub(crate) boundaries: Vec<LeanBoundary>,
    pub(crate) deviations: Vec<LeanDeviation>,
    pub(crate) command_policy_cases: Vec<LeanCommandPolicyCase>,
    pub(crate) command_sandbox_cases: Vec<LeanCommandSandboxCase>,
    pub(crate) command_env_cases: Vec<LeanCommandEnvCase>,
    pub(crate) live_overlay_cases: Vec<LeanLiveOverlayCase>,
    pub(crate) request_progress_cases: Vec<LeanRequestProgressCase>,
    pub(crate) pending_user_turn_cases: Vec<LeanPendingUserTurnCase>,
    pub(crate) queue_deadline_conformance_cases: Vec<LeanQueueDeadlineConformanceCase>,
    pub(crate) recovery_sweep_cases: Vec<LeanRecoverySweepCase>,
    #[serde(default)]
    pub(crate) recovery_equivalence_cases: Vec<LeanRecoveryEquivalenceCase>,
    #[serde(default)]
    pub(crate) restart_disposition_cases: Vec<LeanRestartDispositionCase>,
    #[serde(default)]
    pub(crate) r4c_background_work_cases: Vec<LeanR4cBackgroundWorkCase>,
    #[serde(default)]
    pub(crate) tool_output_paging_cases: Vec<LeanToolOutputPagingCase>,
    #[serde(default)]
    pub(crate) bridge_step_cases: Vec<LeanBridgeStepCase>,
    #[serde(default)]
    pub(crate) codex_shim_projection_cases: Vec<LeanCodexShimProjectionCase>,
    #[serde(default)]
    pub(crate) codex_shim_subagent_tool_cases: Vec<LeanCodexShimSubagentToolCase>,
    #[serde(default)]
    pub(crate) codex_shim_subagent_status_cases: Vec<LeanCodexShimSubagentStatusCase>,
    #[serde(default)]
    pub(crate) codex_shim_subagent_visibility_cases: Vec<LeanCodexShimSubagentVisibilityCase>,
    #[serde(default)]
    pub(crate) codex_shim_subagent_metadata_cases: Vec<LeanCodexShimSubagentMetadataCase>,
    #[serde(default)]
    pub(crate) codex_shim_subagent_listing_cases: Vec<LeanCodexShimSubagentListingCase>,
    #[serde(default)]
    pub(crate) codex_shim_subagent_thread_shape_cases: Vec<LeanCodexShimSubagentThreadShapeCase>,
    #[serde(default)]
    pub(crate) codex_shim_reasoning_projection_cases: Vec<LeanCodexShimReasoningProjectionCase>,
    #[serde(default)]
    pub(crate) codex_shim_thread_status_cases: Vec<LeanCodexShimThreadStatusCase>,
    #[serde(default)]
    pub(crate) codex_shim_behavior_selection_cases: Vec<LeanCodexShimBehaviorSelectionCase>,
    #[serde(default)]
    pub(crate) codex_shim_tool_metadata_cases: Vec<LeanCodexShimToolMetadataCase>,
    #[serde(default)]
    pub(crate) codex_shim_context_usage_cases: Vec<LeanCodexShimContextUsageCase>,
    #[serde(default)]
    pub(crate) codex_shim_compaction_projection_cases: Vec<LeanCodexShimCompactionProjectionCase>,
    #[serde(default)]
    pub(crate) codex_shim_turn_lifecycle_cases: Vec<LeanCodexShimTurnLifecycleCase>,
    #[serde(default)]
    pub(crate) codex_shim_binding_cases: Vec<LeanCodexShimBindingCase>,
    pub(crate) r6_backgrounding_cases: Vec<LeanR6BackgroundingCase>,
    #[serde(default)]
    pub(crate) descendant_graph_cases: Vec<LeanDescendantGraphCase>,
    #[serde(default)]
    pub(crate) r5_cross_deployment_cases: Vec<LeanR5CrossDeploymentCase>,
    #[serde(default)]
    pub(crate) composed_invariant_witnesses: Vec<LeanComposedInvariantWitness>,
    #[serde(default)]
    pub(crate) cancel_propagation_cases: Vec<LeanCancelPropagationCase>,
    pub(crate) r6_background_theorem_witnesses: Vec<LeanBackgroundTheoremWitness>,
    #[serde(default)]
    pub(crate) subagent_delegation_graph_cases: Vec<LeanSubagentDelegationGraphCase>,
    pub(crate) transcript_conformance_cases: Vec<LeanTranscriptCase>,
    pub(crate) streaming_response_cases: Vec<LeanResponseTransitionCase>,
    #[serde(default)]
    pub(crate) streaming_response_interrupt_flow_cases: Vec<LeanResponseInterruptFlowCase>,
    pub(crate) compaction_reducer_cases: Vec<LeanCompactionReducerCase>,
    #[serde(default)]
    pub(crate) compaction_cursor_cases: Vec<LeanCompactionCursorCase>,
    #[serde(default)]
    pub(crate) prompt_assembly_sanitize_cases: Vec<LeanPromptAssemblySanitizeCase>,
    #[serde(default)]
    pub(crate) prompt_assembly_layer_cases: Vec<LeanPromptAssemblyLayerCase>,
    #[serde(default)]
    pub(crate) prompt_assembly_repair_cases: Vec<LeanPromptAssemblyRepairCase>,
    #[serde(default)]
    pub(crate) prompt_assembly_budget_cases: Vec<LeanPromptAssemblyBudgetCase>,
    #[serde(default)]
    pub(crate) prompt_assembly_turn_budget_cases: Vec<LeanPromptAssemblyTurnBudgetCase>,
    #[serde(default)]
    pub(crate) prompt_assembly_retention_cases: Vec<LeanPromptAssemblyRetentionCase>,
    #[serde(default)]
    pub(crate) prompt_assembly_claude_map_cases: Vec<LeanPromptAssemblyClaudeMapCase>,
    #[serde(default)]
    pub(crate) prompt_assembly_claude_body_cases: Vec<LeanPromptAssemblyClaudeBodyCase>,
    #[serde(default)]
    pub(crate) prompt_assembly_claude_stream_cases: Vec<LeanPromptAssemblyClaudeStreamCase>,
    #[serde(default)]
    pub(crate) rendered_capture_cases: Vec<LeanRenderedCaptureCase>,
    #[serde(default)]
    pub(crate) durable_reduction_cases: Vec<LeanDurableReductionCase>,
    #[serde(default)]
    pub(crate) rolling_compaction_cases: Vec<LeanRollingCompactionCase>,
    #[serde(default)]
    pub(crate) reduction_engine_cases: Vec<LeanReductionEngineCase>,
    #[serde(default)]
    pub(crate) rendered_capture_key_cases: Vec<LeanRenderedCaptureKeyCase>,
    #[serde(default)]
    pub(crate) capture_scope_cases: Vec<LeanCaptureScopeCase>,
    #[serde(default)]
    pub(crate) capture_order_cases: Vec<LeanCaptureOrderCase>,
    #[serde(default)]
    pub(crate) aggregate_token_budget_cases: Vec<LeanAggregateTokenBudgetCase>,
    pub(crate) follow_up_hooks: Vec<String>,
    pub(crate) coverage_ledger: Vec<LeanCoverageEntry>,
    pub(crate) feature_surface_requirements: Vec<LeanFeatureSurfaceRequirement>,
    pub(crate) feature_matrix: LeanFeatureMatrix,
    pub(crate) identity_structural_cases: Vec<LeanIdentityStructuralCase>,
    pub(crate) identity_permission_cases: Vec<LeanIdentityPermissionCase>,
    pub(crate) identity_contracts: Vec<LeanIdentityContract>,
    #[serde(default)]
    pub(crate) workspace_cases: Vec<LeanWorkspaceCase>,
    #[serde(default)]
    pub(crate) workspace_binding_cases: Vec<LeanWorkspaceBindingCase>,
    #[serde(default)]
    pub(crate) callback_cases: Vec<LeanCallbackCase>,
    #[serde(default)]
    pub(crate) event_delivery_transition_case_count: usize,
    #[serde(default)]
    pub(crate) event_delivery_transition_cases: Vec<LeanEventDeliveryTransitionCase>,
    #[serde(default)]
    pub(crate) event_delivery_source_instances: Vec<LeanEventDeliverySourceInstance>,
    #[serde(default)]
    pub(crate) event_delivery_convergence_traces: Vec<LeanEventDeliveryConvergenceTrace>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct LeanGraphPipelineValidationCase {
    pub(crate) name: String,
    pub(crate) types_valid: bool,
    pub(crate) topology_valid: bool,
    pub(crate) capabilities_authorized: bool,
    pub(crate) within_bounds: bool,
    pub(crate) terminal_result_declared: bool,
    pub(crate) expected_valid: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct LeanGraphPipelineRevisionGateCase {
    pub(crate) name: String,
    pub(crate) status: String,
    pub(crate) artifacts_complete: bool,
    pub(crate) activation_precondition_met: bool,
    pub(crate) pointer_matches: bool,
    pub(crate) expected_activate: bool,
    pub(crate) expected_start: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct LeanGraphPipelineRunTerminalCase {
    pub(crate) name: String,
    pub(crate) status: String,
    pub(crate) cancellation_requested: bool,
    pub(crate) result_contract_satisfied: bool,
    pub(crate) active_work_terminal: bool,
    pub(crate) failure_proven: bool,
    pub(crate) expected_succeed: bool,
    pub(crate) expected_fail: bool,
    pub(crate) expected_cancel: bool,
}
#[derive(Debug, Deserialize)]
pub(crate) struct LeanVocabularyContract {
    pub(crate) domain: String,
    pub(crate) values: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanStateMachineContract {
    pub(crate) domain: String,
    pub(crate) states: Vec<String>,
    pub(crate) state_count: usize,
    pub(crate) terminal_states: Vec<String>,
    pub(crate) nonterminal_states: Vec<String>,
    pub(crate) actions: Vec<String>,
    pub(crate) legal_transitions: Vec<LeanTransitionPair>,
    pub(crate) illegal_transitions: Vec<LeanTransitionPair>,
    /// Named transition rows emitted by `Conformance.Contracts.NamedTransition`.
    /// Defaults to empty so existing call sites (Bucket 0/Bucket 1) don't need
    /// to deserialize this field on machines that don't emit any.
    #[serde(default)]
    pub(crate) named_transitions: Vec<LeanNamedTransition>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanWorkspaceCase {
    pub(crate) name: String,
    pub(crate) from: String,
    pub(crate) to: String,
    pub(crate) seal_hash: Option<String>,
    pub(crate) legal: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct LeanWorkspaceBindingRef {
    pub(crate) binding_id: String,
    pub(crate) workspace_id: String,
    pub(crate) request_id: String,
    pub(crate) authority: String,
    pub(crate) deployment_id: String,
    pub(crate) seal_hash: Option<String>,
    pub(crate) state: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanWorkspaceBindingCase {
    pub(crate) name: String,
    pub(crate) workspace_id: String,
    pub(crate) workspace_state: String,
    pub(crate) workspace_seal_hash: Option<String>,
    pub(crate) owner_deployment_id: String,
    pub(crate) creation_policy: String,
    pub(crate) existing: Vec<LeanWorkspaceBindingRef>,
    pub(crate) candidate: LeanWorkspaceBindingRef,
    pub(crate) git_metadata_write: bool,
    pub(crate) behavior_command_mode: String,
    pub(crate) legal: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanCallbackCase {
    pub(crate) name: String,
    pub(crate) invocation_id: String,
    pub(crate) owner_deployment_id: String,
    pub(crate) state: String,
    pub(crate) journal: Vec<String>,
    pub(crate) result_emitted: bool,
    pub(crate) legal: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanNamedTransition {
    pub(crate) name: String,
    pub(crate) from: String,
    pub(crate) to: String,
    #[serde(default)]
    pub(crate) requires_native: bool,
    #[serde(default)]
    pub(crate) requires_child: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanCoverageEntry {
    pub(crate) category: String,
    pub(crate) domain: String,
    pub(crate) consumer: String,
    pub(crate) accepted_boundary: String,
    pub(crate) accepted_follow_up: String,
    #[serde(default)]
    pub(crate) feature: String,
    #[serde(default)]
    pub(crate) surfaces: Vec<LeanSurface>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum LeanSurface {
    AgentFacing,
    OperatorCli,
    OperatorUi,
    Api,
    RuntimeInternal,
}

impl LeanSurface {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::AgentFacing => "agentFacing",
            Self::OperatorCli => "operatorCli",
            Self::OperatorUi => "operatorUi",
            Self::Api => "api",
            Self::RuntimeInternal => "runtimeInternal",
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanFeatureSurfaceRequirement {
    pub(crate) feature: String,
    pub(crate) required: Vec<LeanSurface>,
    pub(crate) deferred: Vec<LeanFeatureSurfaceDeferral>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanFeatureSurfaceDeferral {
    pub(crate) surface: LeanSurface,
    pub(crate) note: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanFeatureMatrixCell {
    pub(crate) coverage_strength: String,
    pub(crate) row_count: usize,
    pub(crate) pending_follow_ups: usize,
    pub(crate) deferred_note: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanBoundary {
    pub(crate) id: String,
    pub(crate) domain: String,
    pub(crate) subject: String,
    pub(crate) statement: String,
    pub(crate) accepted_failure_mode: Option<String>,
    pub(crate) accepted_follow_up: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanDeviation {
    pub(crate) id: String,
    pub(crate) domain: String,
    pub(crate) subject: String,
    pub(crate) statement: String,
    pub(crate) accepted_failure_mode: Option<String>,
    pub(crate) accepted_follow_up: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanTransitionPair {
    pub(crate) from: String,
    pub(crate) to: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanGoalDecisionCase {
    pub(crate) name: String,
    pub(crate) status: String,
    pub(crate) terminal: String,
    pub(crate) session_idle: bool,
    pub(crate) child_exists: bool,
    pub(crate) budget_reached: bool,
    pub(crate) has_activity: bool,
    pub(crate) request_is_wrapup: bool,
    pub(crate) infrastructure_retries: i64,
    pub(crate) wrapup_requested: bool,
    pub(crate) wrapup_completed: bool,
    pub(crate) expected_decision: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanGoalTransitionCase {
    pub(crate) name: String,
    pub(crate) pre_status: String,
    pub(crate) pre_blocked_audits: i64,
    pub(crate) pre_wrapup_requested: bool,
    pub(crate) pre_wrapup_completed: bool,
    pub(crate) action: String,
    pub(crate) accepted: bool,
    pub(crate) expected_status: String,
    pub(crate) expected_blocked_audits: i64,
    pub(crate) expected_wrapup_requested: bool,
    pub(crate) expected_wrapup_completed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanGoalCreateCase {
    pub(crate) name: String,
    pub(crate) caller: String,
    pub(crate) current_session: String,
    pub(crate) requested_owner: String,
    pub(crate) requested_session: String,
    pub(crate) objective: String,
    pub(crate) objective_nonempty: bool,
    pub(crate) token_budget: Option<i128>,
    pub(crate) goal_tools: bool,
    pub(crate) goal_create: bool,
    pub(crate) existing: bool,
    pub(crate) existing_matches: bool,
    pub(crate) expected: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanTaskGoalPublicationCase {
    pub(crate) name: String,
    pub(crate) agent_did: String,
    pub(crate) task_id: String,
    pub(crate) fire_key: String,
    pub(crate) goal_objective: Option<String>,
    pub(crate) goal_token_budget: Option<i128>,
    pub(crate) declaration_valid: bool,
    pub(crate) expected_mode: String,
    pub(crate) expected_published: bool,
    pub(crate) expected_runnable_request: bool,
    pub(crate) expected_durable_goal: bool,
    pub(crate) expected_session_id: Option<String>,
    pub(crate) expected_request_id: Option<String>,
    pub(crate) expected_retry_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanTaskGoalRecoveryCase {
    pub(crate) name: String,
    pub(crate) agent_did: String,
    pub(crate) behavior_id: String,
    pub(crate) task_id: String,
    pub(crate) fire_key: String,
    pub(crate) request_present: bool,
    pub(crate) request_binding_matches: bool,
    pub(crate) observed_agent_did: Option<String>,
    pub(crate) observed_behavior_id: Option<String>,
    pub(crate) observed_session_id: Option<String>,
    pub(crate) observed_request_id: Option<String>,
    pub(crate) observed_retry_key: Option<String>,
    pub(crate) durable_goal_present: bool,
    pub(crate) creation_claim_present: bool,
    pub(crate) expected_disposition: String,
    pub(crate) expected_recovered_request_id: Option<String>,
    pub(crate) expected_checkpointable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanGoalSubmissionCase {
    pub(crate) name: String,
    pub(crate) durable_goal: bool,
    pub(crate) runnable_request: bool,
    pub(crate) staged_goal: bool,
    pub(crate) staged_request: bool,
    pub(crate) action: String,
    pub(crate) expected_durable_goal: bool,
    pub(crate) expected_runnable_request: bool,
    pub(crate) expected_staged_goal: bool,
    pub(crate) expected_staged_request: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanGoalContinuationMaterializationCase {
    pub(crate) name: String,
    pub(crate) phase: String,
    pub(crate) action: String,
    pub(crate) expected_phase: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanSessionHydrationDecisionCase {
    pub(crate) name: String,
    pub(crate) paired: bool,
    pub(crate) pairing_requester_matches: bool,
    pub(crate) pairing_agent_matches: bool,
    pub(crate) active_member: bool,
    pub(crate) membership_network_matches: bool,
    pub(crate) owns_session: bool,
    pub(crate) expected_admit: bool,
    pub(crate) expected_selected_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanSessionHydrationProgressCase {
    pub(crate) name: String,
    pub(crate) prev_session: String,
    pub(crate) prev_agent: String,
    pub(crate) session: String,
    pub(crate) agent: String,
    pub(crate) prev_phase: String,
    pub(crate) prev_merged: usize,
    pub(crate) prev_served: Option<usize>,
    pub(crate) merged: usize,
    pub(crate) served: Option<usize>,
    pub(crate) served_matches: bool,
    pub(crate) failed: bool,
    pub(crate) begin_request: bool,
    pub(crate) expected_phase: String,
    pub(crate) expected_merged: usize,
    pub(crate) expected_covered: usize,
    pub(crate) expected_retry_admit: bool,
    pub(crate) expected_complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanSessionHydrationDurableCase {
    pub(crate) name: String,
    pub(crate) status: String,
    pub(crate) merged: usize,
    pub(crate) served: Option<usize>,
    pub(crate) served_matches: bool,
    pub(crate) expected_phase: String,
    pub(crate) expected_merged: usize,
    pub(crate) expected_covered: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanEnrollmentCase {
    pub(crate) name: String,
    pub(crate) steps: Vec<LeanEnrollmentTraceStep>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanEnrollmentDurableProjectionCase {
    pub(crate) name: String,
    pub(crate) documents: Vec<LeanEnrollmentTraceStep>,
    pub(crate) expected_current_approval: bool,
    pub(crate) expected_current_route_receipt: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanEnrollmentTraceStep {
    pub(crate) action: String,
    pub(crate) peer_admission_did: String,
    pub(crate) offer_id: String,
    pub(crate) offer_challenge: String,
    pub(crate) offer_network_id: String,
    pub(crate) offer_admin_did: String,
    pub(crate) offer_server_peer: String,
    pub(crate) offer_owner_agent: String,
    pub(crate) offer_profile: String,
    pub(crate) challenge: String,
    pub(crate) request_id: String,
    pub(crate) request_digest: String,
    pub(crate) request_offer_id: String,
    pub(crate) network_id: String,
    pub(crate) admin_did: String,
    pub(crate) server_peer: String,
    pub(crate) server_ticket_peer: String,
    pub(crate) resolved_server_did: String,
    pub(crate) profile: String,
    pub(crate) schema_compatible: bool,
    pub(crate) offer_admin_signed: bool,
    pub(crate) offer_fresh: bool,
    pub(crate) candidate_did: String,
    pub(crate) candidate_peer: String,
    pub(crate) observed_candidate_peer: String,
    pub(crate) resolved_candidate_did: String,
    pub(crate) candidate_ticket_peer: String,
    pub(crate) owner_agent: String,
    pub(crate) client_nonce: String,
    pub(crate) issued_at: String,
    pub(crate) expires_at: String,
    pub(crate) candidate_signed: bool,
    pub(crate) request_fresh: bool,
    pub(crate) decision_authorization_sequence: usize,
    pub(crate) decision_authorization_expires_at: String,
    pub(crate) decision_signer_did: String,
    pub(crate) decision_kind: String,
    pub(crate) decision_request_id: String,
    pub(crate) decision_request_digest: String,
    pub(crate) decision_network_id: String,
    pub(crate) decision_admin_did: String,
    pub(crate) decision_candidate_did: String,
    pub(crate) decision_candidate_peer: String,
    pub(crate) decision_owner_agent: String,
    pub(crate) decision_admin_signed: bool,
    pub(crate) decision_fresh: bool,
    pub(crate) revision_kind: String,
    pub(crate) revision_sequence: usize,
    pub(crate) revision_authorization_expires_at: String,
    pub(crate) revision_signer_did: String,
    pub(crate) revision_request_id: String,
    pub(crate) revision_request_digest: String,
    pub(crate) revision_network_id: String,
    pub(crate) revision_admin_did: String,
    pub(crate) revision_member_did: String,
    pub(crate) revision_member_peer: String,
    pub(crate) revision_owner_agent: String,
    pub(crate) revision_admin_signed: bool,
    pub(crate) receipt_request_id: String,
    pub(crate) receipt_request_digest: String,
    pub(crate) receipt_network_id: String,
    pub(crate) receipt_admin_did: String,
    pub(crate) receipt_member_did: String,
    pub(crate) receipt_member_peer: String,
    pub(crate) receipt_server_peer: String,
    pub(crate) receipt_owner_agent: String,
    pub(crate) receipt_authorization_sequence: usize,
    pub(crate) receipt_authorization_expires_at: String,
    pub(crate) receipt_direction: String,
    pub(crate) receipt_signer_did: String,
    pub(crate) receipt_admin_signed: bool,
    pub(crate) receipt_applied: bool,
    pub(crate) observed_offer_count: usize,
    pub(crate) admin_pin_count: usize,
    pub(crate) challenge_binding_count: usize,
    pub(crate) request_binding_count: usize,
    pub(crate) request_count: usize,
    pub(crate) decision_count: usize,
    pub(crate) authorization_count: usize,
    pub(crate) membership_count: usize,
    pub(crate) receipt_count: usize,
    pub(crate) route_count: usize,
    pub(crate) request_accepted: bool,
    pub(crate) decision_recorded: bool,
    pub(crate) authorization_recorded: bool,
    pub(crate) revision_recorded: bool,
    pub(crate) receipt_recorded: bool,
    pub(crate) membership_present: bool,
    pub(crate) client_route_present: bool,
    pub(crate) server_route_present: bool,
    pub(crate) admin_pin_present: bool,
    pub(crate) admin_pin_conflict: bool,
    pub(crate) challenge_binding_conflict: bool,
    pub(crate) request_binding_conflict: bool,
    pub(crate) current_approval: bool,
    pub(crate) peer_admitted: bool,
    pub(crate) ready: bool,
    pub(crate) client_hydration_admits: bool,
    pub(crate) server_hydration_admits: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanEnrollmentEncodingCase {
    pub(crate) name: String,
    pub(crate) value: String,
    pub(crate) expected_frame: String,
    pub(crate) actual_frame: String,
    pub(crate) frame_matches: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanEnrollmentDigestCase {
    pub(crate) name: String,
    pub(crate) fields: Vec<String>,
    pub(crate) expected_payload: String,
    pub(crate) actual_payload: String,
    pub(crate) expected_digest: String,
    pub(crate) actual_digest: String,
    pub(crate) payload_matches: bool,
    pub(crate) digest_matches: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct LeanAgentRequestAdmissionCase {
    pub(crate) name: String,
    pub(crate) observation_available: bool,
    pub(crate) kind: String,
    pub(crate) signature_valid: bool,
    pub(crate) signed_fields_match: bool,
    pub(crate) branch_fields_exact: bool,
    pub(crate) pending_deadline_absent: bool,
    pub(crate) signer_matches_requester: bool,
    pub(crate) requester_matches_target: bool,
    pub(crate) signer_matches_target: bool,
    pub(crate) signer_matches_issuer: bool,
    pub(crate) requester_matches_issuer: bool,
    pub(crate) current_approval: bool,
    pub(crate) exact_generation: bool,
    pub(crate) authorization_fresh: bool,
    pub(crate) runtime_evidence_present: bool,
    pub(crate) runtime_source_kind: String,
    pub(crate) target_runtime_attestation_valid: bool,
    pub(crate) source_binding_current: bool,
    pub(crate) trigger_config_document_binding_current: bool,
    pub(crate) source_document_binding_current: bool,
    pub(crate) source_tool_call_binding_current: bool,
    pub(crate) target_policy_allows: bool,
    pub(crate) bridge_author_binding_current: bool,
    pub(crate) bridge_author_authorization_fresh: bool,
    pub(crate) target_cross_deployment_policy_allows: bool,
    pub(crate) expected_admitted: bool,
    pub(crate) expected_disposition: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanLifecycleTransitionCase {
    pub(crate) name: String,
    pub(crate) domain: String,
    pub(crate) from: String,
    pub(crate) to: String,
    pub(crate) classification: String,
    pub(crate) action: Option<String>,
    pub(crate) boundary: Option<String>,
}

#[path = "background_transcript.rs"]
mod background_transcript;
#[path = "client_session.rs"]
mod client_session;
#[path = "codex_shim.rs"]
mod codex_shim;
#[path = "command_identity_queue.rs"]
mod command_identity_queue;
#[path = "composed_invariants.rs"]
mod composed_invariants;
#[path = "descendant_graph.rs"]
mod descendant_graph;
#[path = "durable_reduction.rs"]
mod durable_reduction;
#[path = "event_delivery.rs"]
mod event_delivery;
#[path = "prompt_assembly.rs"]
mod prompt_assembly;
#[path = "reduction_engine.rs"]
mod reduction_engine;
#[path = "rendered_capture.rs"]
mod rendered_capture;
#[path = "request_execution_lease.rs"]
mod request_execution_lease;
#[path = "rolling_compaction.rs"]
mod rolling_compaction;
#[path = "self_config.rs"]
mod self_config;
#[path = "slot_persistence_health.rs"]
mod slot_persistence_health;
#[path = "tool_policy.rs"]
mod tool_policy;
#[path = "triggers_runtime_apply.rs"]
mod triggers_runtime_apply;

pub(crate) use background_transcript::*;
pub(crate) use client_session::*;
pub(crate) use codex_shim::*;
pub(crate) use command_identity_queue::*;
pub(crate) use composed_invariants::*;
pub(crate) use descendant_graph::*;
pub(crate) use durable_reduction::*;
pub(crate) use event_delivery::*;
pub(crate) use prompt_assembly::*;
pub(crate) use reduction_engine::*;
pub(crate) use rendered_capture::*;
pub(crate) use request_execution_lease::*;
pub(crate) use rolling_compaction::*;
pub(crate) use self_config::*;
pub(crate) use slot_persistence_health::*;
pub(crate) use tool_policy::*;
pub(crate) use triggers_runtime_apply::*;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum LeanVocabularyParseError<'a> {
    MissingNamespace,
    MissingToDefraDB,
    EmptyToDefraDB,
    MalformedArm {
        line_number: usize,
        line: &'a str,
        reason: &'static str,
    },
}

static LEAN_CONTRACT_SNAPSHOT: OnceLock<LeanContractSnapshot> = OnceLock::new();

pub(crate) fn lean_contract_snapshot() -> &'static LeanContractSnapshot {
    LEAN_CONTRACT_SNAPSHOT.get_or_init(load_lean_contract_snapshot)
}

pub(crate) fn lean_workspace_cases() -> &'static [LeanWorkspaceCase] {
    &lean_contract_snapshot().workspace_cases
}

pub(crate) fn lean_workspace_binding_cases() -> &'static [LeanWorkspaceBindingCase] {
    &lean_contract_snapshot().workspace_binding_cases
}

pub(crate) fn lean_callback_cases() -> &'static [LeanCallbackCase] {
    &lean_contract_snapshot().callback_cases
}

pub(crate) fn lean_vocabulary_contract(domain: &str) -> &'static LeanVocabularyContract {
    lean_contract_snapshot()
        .vocabularies
        .iter()
        .find(|contract| contract.domain == domain)
        .unwrap_or_else(|| panic!("Lean vocabulary contract {domain:?} was not emitted"))
}

pub(crate) fn lean_state_machine_contract(domain: &str) -> &'static LeanStateMachineContract {
    lean_contract_snapshot()
        .state_machines
        .iter()
        .find(|contract| contract.domain == domain)
        .unwrap_or_else(|| panic!("Lean state-machine contract {domain:?} was not emitted"))
}

pub(crate) fn lean_feature_surface_requirements() -> &'static [LeanFeatureSurfaceRequirement] {
    &lean_contract_snapshot().feature_surface_requirements
}

pub(crate) fn lean_feature_matrix() -> &'static LeanFeatureMatrix {
    &lean_contract_snapshot().feature_matrix
}

pub(crate) fn lean_request_transition_cases() -> &'static [LeanLifecycleTransitionCase] {
    &lean_contract_snapshot().request_transition_cases
}

pub(crate) fn lean_process_transition_cases() -> &'static [LeanLifecycleTransitionCase] {
    &lean_contract_snapshot().process_transition_cases
}

pub(crate) fn lean_provider_eof_cases() -> &'static [LeanProviderEofCase] {
    &lean_contract_snapshot().provider_eof_cases
}

pub(crate) fn lean_request_execution_lease_cases() -> &'static [LeanRequestExecutionLeaseCase] {
    &lean_contract_snapshot().request_execution_lease_cases
}

pub(crate) fn lean_request_execution_lease_trace_cases(
) -> &'static [LeanRequestExecutionLeaseTraceCase] {
    &lean_contract_snapshot().request_execution_lease_trace_cases
}

pub(crate) fn lean_startup_readiness_cases() -> &'static [LeanStartupReadinessCase] {
    &lean_contract_snapshot().startup_readiness_cases
}

pub(crate) fn lean_runtime_reconcile_cases() -> &'static [LeanRuntimeReconcileCase] {
    &lean_contract_snapshot().runtime_reconcile_cases
}

pub(crate) fn lean_client_behavior_readiness_cases() -> &'static [LeanClientBehaviorReadinessCase] {
    &lean_contract_snapshot().client_behavior_readiness_cases
}

pub(crate) fn lean_runtime_reconcile_case(name: &str) -> &'static LeanRuntimeReconcileCase {
    lean_contract_snapshot()
        .runtime_reconcile_cases
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("Lean runtime-reconcile case {name:?} was not emitted"))
}

pub(crate) fn lean_apply_reconcile_cases() -> &'static [LeanApplyReconcileCase] {
    &lean_contract_snapshot().apply_reconcile_cases
}

pub(crate) fn lean_apply_reconcile_case(name: &str) -> &'static LeanApplyReconcileCase {
    lean_contract_snapshot()
        .apply_reconcile_cases
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("Lean apply-reconcile case {name:?} was not emitted"))
}

pub(crate) fn lean_tool_policy_cases() -> &'static [LeanToolPolicyCase] {
    &lean_contract_snapshot().tool_policy_cases
}

pub(crate) fn lean_goal_capability_resolution_cases() -> &'static [LeanGoalCapabilityResolutionCase]
{
    &lean_contract_snapshot().goal_capability_resolution_cases
}

pub(crate) fn lean_lsp_action_cases() -> &'static [LeanLspActionCase] {
    &lean_contract_snapshot().lsp_action_cases
}

pub(crate) fn lean_self_config_field_tables() -> &'static [LeanSelfConfigFieldTable] {
    &lean_contract_snapshot().self_config_field_tables
}

pub(crate) fn lean_self_config_cases() -> &'static [LeanSelfConfigCase] {
    &lean_contract_snapshot().self_config_cases
}

pub(crate) fn lean_tool_policy_case(name: &str) -> &'static LeanToolPolicyCase {
    lean_tool_policy_cases()
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("Lean tool-policy case {name:?} was not emitted"))
}

pub(crate) fn lean_session_recovery_case(name: &str) -> &'static LeanSessionRecoveryCase {
    lean_contract_snapshot()
        .session_recovery_cases
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("Lean session-recovery case {name:?} was not emitted"))
}

pub(crate) fn lean_client_shell_case(name: &str) -> &'static LeanClientShellCase {
    lean_contract_snapshot()
        .frontend_client_shell_cases
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("Lean ClientShell case {name:?} was not emitted"))
}

pub(crate) fn lean_desktop_client_shell_cases() -> &'static [LeanClientShellCase] {
    &lean_contract_snapshot().desktop_client_shell_cases
}

pub(crate) fn lean_request_lifecycle_operator_ui_cases() -> &'static [LeanClientShellCase] {
    &lean_contract_snapshot().request_lifecycle_operator_ui_cases
}

pub(crate) fn lean_inference_slot_accounting_cases() -> &'static [LeanInferenceSlotAccountingCase] {
    &lean_contract_snapshot().inference_slot_accounting_cases
}

pub(crate) fn lean_inference_slot_accounting_case(
    name: &str,
) -> &'static LeanInferenceSlotAccountingCase {
    lean_contract_snapshot()
        .inference_slot_accounting_cases
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("Lean inference slot-accounting case {name:?} was not emitted"))
}

pub(crate) fn lean_fleet_slot_accounting_case(name: &str) -> &'static LeanFleetSlotAccountingCase {
    lean_contract_snapshot()
        .fleet_slot_accounting_cases
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("Lean fleet slot-accounting case {name:?} was not emitted"))
}

pub(crate) fn lean_persistence_failure_policy_cases() -> &'static [LeanPersistenceFailurePolicyCase]
{
    &lean_contract_snapshot().persistence_failure_policy_cases
}

pub(crate) fn lean_storage_observation_runtime_cases(
) -> &'static [LeanStorageObservationRuntimeCase] {
    &lean_contract_snapshot().storage_observation_runtime_cases
}

pub(crate) fn lean_backend_health_admission_cases() -> &'static [LeanBackendHealthAdmissionCase] {
    &lean_contract_snapshot().backend_health_admission_cases
}

pub(crate) fn lean_native_filesystem_boundary_cases() -> &'static [LeanNativeFilesystemBoundaryCase]
{
    &lean_contract_snapshot().native_filesystem_boundary_cases
}

pub(crate) fn lean_managed_exec_tool_boundary_cases() -> &'static [LeanManagedExecToolBoundaryCase]
{
    &lean_contract_snapshot().managed_exec_tool_boundary_cases
}

pub(crate) fn lean_pairing_reconcile_shutdown_boundary_cases(
) -> &'static [LeanPairingReconcileShutdownBoundaryCase] {
    &lean_contract_snapshot().pairing_reconcile_shutdown_boundary_cases
}

pub(crate) fn lean_pairing_reconcile_sweep_retry_boundary_cases(
) -> &'static [LeanPairingReconcileSweepRetryBoundaryCase] {
    &lean_contract_snapshot().pairing_reconcile_sweep_retry_boundary_cases
}

pub(crate) fn lean_pairing_reconcile_sweep_scheduling_cases(
) -> &'static [LeanPairingReconcileSweepSchedulingCase] {
    &lean_contract_snapshot().pairing_reconcile_sweep_scheduling_cases
}

pub(crate) fn lean_managed_exec_liveness_cases() -> &'static [LeanManagedExecLivenessCase] {
    &lean_contract_snapshot().managed_exec_liveness_cases
}

pub(crate) fn lean_tool_preflight_cases() -> &'static [LeanToolPreflightCase] {
    &lean_contract_snapshot().tool_preflight_cases
}

pub(crate) fn lean_tool_preflight_case(name: &str) -> &'static LeanToolPreflightCase {
    lean_contract_snapshot()
        .tool_preflight_cases
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("Lean tool preflight case {name:?} was not emitted"))
}

pub(crate) fn lean_tool_retry_cases() -> &'static [LeanToolRetryCase] {
    &lean_contract_snapshot().tool_retry_cases
}

pub(crate) fn lean_completion_retry_cases() -> &'static [LeanCompletionRetryCase] {
    &lean_contract_snapshot().completion_retry_cases
}

pub(crate) fn lean_queue_deadline_cases() -> &'static [LeanQueueDeadlineConformanceCase] {
    &lean_contract_snapshot().queue_deadline_conformance_cases
}

pub(crate) fn lean_queue_deadline_case(name: &str) -> &'static LeanQueueDeadlineConformanceCase {
    lean_contract_snapshot()
        .queue_deadline_conformance_cases
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("Lean queue/deadline case {name:?} was not emitted"))
}

pub(crate) fn lean_recovery_sweep_cases() -> &'static [LeanRecoverySweepCase] {
    &lean_contract_snapshot().recovery_sweep_cases
}

pub(crate) fn lean_recovery_sweep_case(name: &str) -> &'static LeanRecoverySweepCase {
    lean_contract_snapshot()
        .recovery_sweep_cases
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("Lean recovery sweep case {name:?} was not emitted"))
}

pub(crate) fn lean_recovery_equivalence_cases() -> &'static [LeanRecoveryEquivalenceCase] {
    &lean_contract_snapshot().recovery_equivalence_cases
}

pub(crate) fn lean_restart_disposition_cases() -> &'static [LeanRestartDispositionCase] {
    &lean_contract_snapshot().restart_disposition_cases
}

pub(crate) fn lean_r4c_background_work_cases() -> &'static [LeanR4cBackgroundWorkCase] {
    &lean_contract_snapshot().r4c_background_work_cases
}

pub(crate) fn lean_tool_output_paging_cases() -> &'static [LeanToolOutputPagingCase] {
    &lean_contract_snapshot().tool_output_paging_cases
}

pub(crate) fn lean_bridge_step_cases() -> &'static [LeanBridgeStepCase] {
    &lean_contract_snapshot().bridge_step_cases
}

pub(crate) fn lean_r4c_background_work_case(witness: &str) -> &'static LeanR4cBackgroundWorkCase {
    lean_contract_snapshot()
        .r4c_background_work_cases
        .iter()
        .find(|case| case.witness() == witness)
        .unwrap_or_else(|| panic!("Lean R4c background-work witness {witness:?} was not emitted"))
}

pub(crate) fn lean_codex_shim_projection_cases() -> &'static [LeanCodexShimProjectionCase] {
    &lean_contract_snapshot().codex_shim_projection_cases
}

pub(crate) fn lean_codex_shim_projection_case(
    witness: &str,
) -> &'static LeanCodexShimProjectionCase {
    lean_contract_snapshot()
        .codex_shim_projection_cases
        .iter()
        .find(|case| case.witness == witness)
        .unwrap_or_else(|| panic!("Lean Codex shim projection witness {witness:?} was not emitted"))
}

pub(crate) fn lean_codex_shim_subagent_tool_cases() -> &'static [LeanCodexShimSubagentToolCase] {
    &lean_contract_snapshot().codex_shim_subagent_tool_cases
}

pub(crate) fn lean_codex_shim_subagent_status_cases() -> &'static [LeanCodexShimSubagentStatusCase]
{
    &lean_contract_snapshot().codex_shim_subagent_status_cases
}

pub(crate) fn lean_codex_shim_subagent_visibility_cases(
) -> &'static [LeanCodexShimSubagentVisibilityCase] {
    &lean_contract_snapshot().codex_shim_subagent_visibility_cases
}

pub(crate) fn lean_codex_shim_subagent_metadata_cases(
) -> &'static [LeanCodexShimSubagentMetadataCase] {
    &lean_contract_snapshot().codex_shim_subagent_metadata_cases
}

pub(crate) fn lean_codex_shim_subagent_listing_cases() -> &'static [LeanCodexShimSubagentListingCase]
{
    &lean_contract_snapshot().codex_shim_subagent_listing_cases
}

pub(crate) fn lean_codex_shim_subagent_thread_shape_cases(
) -> &'static [LeanCodexShimSubagentThreadShapeCase] {
    &lean_contract_snapshot().codex_shim_subagent_thread_shape_cases
}

pub(crate) fn lean_codex_shim_reasoning_projection_cases(
) -> &'static [LeanCodexShimReasoningProjectionCase] {
    &lean_contract_snapshot().codex_shim_reasoning_projection_cases
}

pub(crate) fn lean_codex_shim_thread_status_cases() -> &'static [LeanCodexShimThreadStatusCase] {
    &lean_contract_snapshot().codex_shim_thread_status_cases
}

pub(crate) fn lean_codex_shim_behavior_selection_cases(
) -> &'static [LeanCodexShimBehaviorSelectionCase] {
    &lean_contract_snapshot().codex_shim_behavior_selection_cases
}

pub(crate) fn lean_codex_shim_tool_metadata_cases() -> &'static [LeanCodexShimToolMetadataCase] {
    &lean_contract_snapshot().codex_shim_tool_metadata_cases
}

pub(crate) fn lean_codex_shim_context_usage_cases() -> &'static [LeanCodexShimContextUsageCase] {
    &lean_contract_snapshot().codex_shim_context_usage_cases
}

pub(crate) fn lean_codex_shim_compaction_projection_cases(
) -> &'static [LeanCodexShimCompactionProjectionCase] {
    &lean_contract_snapshot().codex_shim_compaction_projection_cases
}

pub(crate) fn lean_codex_shim_turn_lifecycle_cases() -> &'static [LeanCodexShimTurnLifecycleCase] {
    &lean_contract_snapshot().codex_shim_turn_lifecycle_cases
}

pub(crate) fn lean_codex_shim_binding_cases() -> &'static [LeanCodexShimBindingCase] {
    &lean_contract_snapshot().codex_shim_binding_cases
}

pub(crate) fn lean_r6_backgrounding_cases() -> &'static [LeanR6BackgroundingCase] {
    &lean_contract_snapshot().r6_backgrounding_cases
}

pub(crate) fn lean_descendant_graph_cases() -> &'static [LeanDescendantGraphCase] {
    &lean_contract_snapshot().descendant_graph_cases
}

pub(crate) fn lean_r6_backgrounding_case(name: &str) -> &'static LeanR6BackgroundingCase {
    lean_contract_snapshot()
        .r6_backgrounding_cases
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("Lean R6 backgrounding case {name:?} was not emitted"))
}

pub(crate) fn lean_r5_cross_deployment_cases() -> &'static [LeanR5CrossDeploymentCase] {
    &lean_contract_snapshot().r5_cross_deployment_cases
}

pub(crate) fn lean_composed_invariant_witnesses() -> &'static [LeanComposedInvariantWitness] {
    &lean_contract_snapshot().composed_invariant_witnesses
}

pub(crate) fn lean_composed_invariant_witness(
    theorem_name: &str,
) -> &'static LeanComposedInvariantWitness {
    lean_contract_snapshot()
        .composed_invariant_witnesses
        .iter()
        .find(|witness| witness.theorem_name == theorem_name)
        .unwrap_or_else(|| {
            panic!("Lean composed invariant witness {theorem_name:?} was not emitted")
        })
}

/// Look up a composed-invariant witness by its (unique) `scenario`. Needed when
/// several witnesses share a `theorem_name` — e.g. the C2 theorem's pending and
/// running arms.
pub(crate) fn lean_composed_invariant_witness_by_scenario(
    scenario: &str,
) -> &'static LeanComposedInvariantWitness {
    lean_contract_snapshot()
        .composed_invariant_witnesses
        .iter()
        .find(|witness| witness.scenario == scenario)
        .unwrap_or_else(|| {
            panic!("Lean composed invariant witness scenario {scenario:?} was not emitted")
        })
}

pub(crate) fn lean_cancel_propagation_cases() -> &'static [LeanCancelPropagationCase] {
    &lean_contract_snapshot().cancel_propagation_cases
}

pub(crate) fn lean_r6_background_theorem_witnesses() -> &'static [LeanBackgroundTheoremWitness] {
    &lean_contract_snapshot().r6_background_theorem_witnesses
}

pub(crate) fn lean_r6_background_theorem_witness(
    theorem_name: &str,
) -> &'static LeanBackgroundTheoremWitness {
    lean_contract_snapshot()
        .r6_background_theorem_witnesses
        .iter()
        .find(|witness| witness.theorem_name == theorem_name)
        .unwrap_or_else(|| {
            panic!("Lean R6 background theorem witness {theorem_name:?} was not emitted")
        })
}

pub(crate) fn lean_subagent_delegation_graph_cases() -> &'static [LeanSubagentDelegationGraphCase] {
    &lean_contract_snapshot().subagent_delegation_graph_cases
}

pub(crate) fn lean_transcript_cases() -> &'static [LeanTranscriptCase] {
    &lean_contract_snapshot().transcript_conformance_cases
}

pub(crate) fn lean_transcript_case(name: &str) -> &'static LeanTranscriptCase {
    lean_contract_snapshot()
        .transcript_conformance_cases
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("Lean transcript case {name:?} was not emitted"))
}

pub(crate) fn lean_response_transition_cases() -> &'static [LeanResponseTransitionCase] {
    &lean_contract_snapshot().streaming_response_cases
}

pub(crate) fn lean_response_transition_case(name: &str) -> &'static LeanResponseTransitionCase {
    lean_contract_snapshot()
        .streaming_response_cases
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("Lean response-transition case {name:?} was not emitted"))
}

pub(crate) fn lean_response_interrupt_flow_cases() -> &'static [LeanResponseInterruptFlowCase] {
    &lean_contract_snapshot().streaming_response_interrupt_flow_cases
}

pub(crate) fn lean_response_interrupt_flow_case(
    name: &str,
) -> &'static LeanResponseInterruptFlowCase {
    lean_contract_snapshot()
        .streaming_response_interrupt_flow_cases
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("Lean response-interrupt-flow case {name:?} was not emitted"))
}

pub(crate) fn lean_compaction_reducer_cases() -> &'static [LeanCompactionReducerCase] {
    &lean_contract_snapshot().compaction_reducer_cases
}

pub(crate) fn lean_compaction_cursor_cases() -> &'static [LeanCompactionCursorCase] {
    &lean_contract_snapshot().compaction_cursor_cases
}

pub(crate) fn lean_prompt_assembly_sanitize_cases() -> &'static [LeanPromptAssemblySanitizeCase] {
    &lean_contract_snapshot().prompt_assembly_sanitize_cases
}

pub(crate) fn lean_prompt_assembly_layer_cases() -> &'static [LeanPromptAssemblyLayerCase] {
    &lean_contract_snapshot().prompt_assembly_layer_cases
}

pub(crate) fn lean_prompt_assembly_repair_cases() -> &'static [LeanPromptAssemblyRepairCase] {
    &lean_contract_snapshot().prompt_assembly_repair_cases
}

pub(crate) fn lean_prompt_assembly_budget_cases() -> &'static [LeanPromptAssemblyBudgetCase] {
    &lean_contract_snapshot().prompt_assembly_budget_cases
}

pub(crate) fn lean_prompt_assembly_turn_budget_cases() -> &'static [LeanPromptAssemblyTurnBudgetCase]
{
    &lean_contract_snapshot().prompt_assembly_turn_budget_cases
}

pub(crate) fn lean_prompt_assembly_retention_cases() -> &'static [LeanPromptAssemblyRetentionCase] {
    &lean_contract_snapshot().prompt_assembly_retention_cases
}

pub(crate) fn lean_prompt_assembly_claude_map_cases() -> &'static [LeanPromptAssemblyClaudeMapCase]
{
    &lean_contract_snapshot().prompt_assembly_claude_map_cases
}

pub(crate) fn lean_prompt_assembly_claude_body_cases() -> &'static [LeanPromptAssemblyClaudeBodyCase]
{
    &lean_contract_snapshot().prompt_assembly_claude_body_cases
}

pub(crate) fn lean_prompt_assembly_claude_stream_cases(
) -> &'static [LeanPromptAssemblyClaudeStreamCase] {
    &lean_contract_snapshot().prompt_assembly_claude_stream_cases
}

pub(crate) fn lean_rendered_capture_cases() -> &'static [LeanRenderedCaptureCase] {
    &lean_contract_snapshot().rendered_capture_cases
}

pub(crate) fn lean_durable_reduction_cases() -> &'static [LeanDurableReductionCase] {
    &lean_contract_snapshot().durable_reduction_cases
}

pub(crate) fn lean_rolling_compaction_cases() -> &'static [LeanRollingCompactionCase] {
    &lean_contract_snapshot().rolling_compaction_cases
}

pub(crate) fn lean_reduction_engine_cases() -> &'static [LeanReductionEngineCase] {
    &lean_contract_snapshot().reduction_engine_cases
}

pub(crate) fn lean_rendered_capture_key_cases() -> &'static [LeanRenderedCaptureKeyCase] {
    &lean_contract_snapshot().rendered_capture_key_cases
}

pub(crate) fn lean_capture_scope_cases() -> &'static [LeanCaptureScopeCase] {
    &lean_contract_snapshot().capture_scope_cases
}

pub(crate) fn lean_capture_order_cases() -> &'static [LeanCaptureOrderCase] {
    &lean_contract_snapshot().capture_order_cases
}

pub(crate) fn lean_aggregate_token_budget_cases() -> &'static [LeanAggregateTokenBudgetCase] {
    &lean_contract_snapshot().aggregate_token_budget_cases
}

pub(crate) fn lean_compaction_reducer_case(name: &str) -> &'static LeanCompactionReducerCase {
    lean_contract_snapshot()
        .compaction_reducer_cases
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("Lean compaction-reducer case {name:?} was not emitted"))
}

pub(crate) fn lean_event_delivery_transition_cases() -> &'static [LeanEventDeliveryTransitionCase] {
    &lean_contract_snapshot().event_delivery_transition_cases
}

pub(crate) fn lean_event_delivery_source_instances() -> &'static [LeanEventDeliverySourceInstance] {
    &lean_contract_snapshot().event_delivery_source_instances
}

pub(crate) fn lean_event_delivery_convergence_traces(
) -> &'static [LeanEventDeliveryConvergenceTrace] {
    &lean_contract_snapshot().event_delivery_convergence_traces
}

pub(crate) fn lean_tool_retry_case(name: &str) -> &'static LeanToolRetryCase {
    lean_contract_snapshot()
        .tool_retry_cases
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("Lean tool retry case {name:?} was not emitted"))
}

pub(crate) fn lean_mcp_health_cases() -> &'static [LeanMcpHealthCase] {
    &lean_contract_snapshot().mcp_health_cases
}

pub(crate) fn lean_backend_health_cases() -> &'static [LeanBackendHealthCase] {
    &lean_contract_snapshot().backend_health_cases
}

pub(crate) fn lean_command_policy_cases() -> &'static [LeanCommandPolicyCase] {
    &lean_contract_snapshot().command_policy_cases
}

pub(crate) fn lean_command_policy_case(name: &str) -> &'static LeanCommandPolicyCase {
    lean_contract_snapshot()
        .command_policy_cases
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("Lean command policy case {name:?} was not emitted"))
}

pub(crate) fn lean_command_sandbox_cases() -> &'static [LeanCommandSandboxCase] {
    &lean_contract_snapshot().command_sandbox_cases
}

pub(crate) fn lean_command_sandbox_case(name: &str) -> &'static LeanCommandSandboxCase {
    lean_contract_snapshot()
        .command_sandbox_cases
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("Lean command sandbox case {name:?} was not emitted"))
}

pub(crate) fn lean_command_env_cases() -> &'static [LeanCommandEnvCase] {
    &lean_contract_snapshot().command_env_cases
}

pub(crate) fn lean_command_env_case(name: &str) -> &'static LeanCommandEnvCase {
    lean_contract_snapshot()
        .command_env_cases
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("Lean command env case {name:?} was not emitted"))
}

pub(crate) fn lean_identity_structural_cases() -> &'static [LeanIdentityStructuralCase] {
    &lean_contract_snapshot().identity_structural_cases
}

pub(crate) fn lean_identity_permission_cases() -> &'static [LeanIdentityPermissionCase] {
    &lean_contract_snapshot().identity_permission_cases
}

pub(crate) fn lean_identity_contracts() -> &'static [LeanIdentityContract] {
    &lean_contract_snapshot().identity_contracts
}

pub(crate) fn lean_live_overlay_cases() -> &'static [LeanLiveOverlayCase] {
    &lean_contract_snapshot().live_overlay_cases
}

pub(crate) fn lean_request_progress_cases() -> &'static [LeanRequestProgressCase] {
    &lean_contract_snapshot().request_progress_cases
}

pub(crate) fn lean_pending_user_turn_cases() -> &'static [LeanPendingUserTurnCase] {
    &lean_contract_snapshot().pending_user_turn_cases
}

pub(crate) fn lean_vocabulary_values(domain: &str) -> Vec<&'static str> {
    lean_vocabulary_contract(domain)
        .values
        .iter()
        .map(String::as_str)
        .collect()
}

pub(crate) fn lean_trigger_dispatch_cases() -> &'static [LeanTriggerDispatchCase] {
    &lean_contract_snapshot().trigger_dispatch_cases
}

pub(crate) fn lean_goal_decision_cases() -> &'static [LeanGoalDecisionCase] {
    &lean_contract_snapshot().goal_decision_cases
}

pub(crate) fn lean_goal_transition_cases() -> &'static [LeanGoalTransitionCase] {
    &lean_contract_snapshot().goal_transition_cases
}

pub(crate) fn lean_goal_create_cases() -> &'static [LeanGoalCreateCase] {
    &lean_contract_snapshot().goal_create_cases
}

pub(crate) fn lean_task_goal_publication_cases() -> &'static [LeanTaskGoalPublicationCase] {
    &lean_contract_snapshot().task_goal_publication_cases
}

pub(crate) fn lean_task_goal_recovery_cases() -> &'static [LeanTaskGoalRecoveryCase] {
    &lean_contract_snapshot().task_goal_recovery_cases
}

pub(crate) fn lean_goal_submission_cases() -> &'static [LeanGoalSubmissionCase] {
    &lean_contract_snapshot().goal_submission_cases
}

pub(crate) fn lean_goal_continuation_materialization_cases(
) -> &'static [LeanGoalContinuationMaterializationCase] {
    &lean_contract_snapshot().goal_continuation_materialization_cases
}

pub(crate) fn lean_session_hydration_decision_cases() -> &'static [LeanSessionHydrationDecisionCase]
{
    &lean_contract_snapshot().session_hydration_decision_cases
}

pub(crate) fn lean_session_hydration_progress_cases() -> &'static [LeanSessionHydrationProgressCase]
{
    &lean_contract_snapshot().session_hydration_progress_cases
}

pub(crate) fn lean_session_hydration_durable_cases() -> &'static [LeanSessionHydrationDurableCase] {
    &lean_contract_snapshot().session_hydration_durable_cases
}

pub(crate) fn lean_enrollment_cases() -> &'static [LeanEnrollmentCase] {
    &lean_contract_snapshot().enrollment_cases
}

pub(crate) fn lean_enrollment_durable_projection_cases(
) -> &'static [LeanEnrollmentDurableProjectionCase] {
    &lean_contract_snapshot().enrollment_durable_projection_cases
}

pub(crate) fn lean_enrollment_encoding_cases() -> &'static [LeanEnrollmentEncodingCase] {
    &lean_contract_snapshot().enrollment_encoding_cases
}

pub(crate) fn lean_enrollment_digest_cases() -> &'static [LeanEnrollmentDigestCase] {
    &lean_contract_snapshot().enrollment_digest_cases
}

pub(crate) fn lean_agent_request_admission_cases() -> &'static [LeanAgentRequestAdmissionCase] {
    &lean_contract_snapshot().agent_request_admission_cases
}

pub(crate) fn lean_trigger_dispatch_case_count() -> usize {
    lean_contract_snapshot().trigger_dispatch_case_count
}

pub(crate) fn lean_trigger_group_cases() -> &'static [LeanTriggerGroupCase] {
    &lean_contract_snapshot().trigger_group_cases
}

pub(crate) fn lean_trigger_group_case_count() -> usize {
    lean_contract_snapshot().trigger_group_case_count
}

pub(crate) fn assert_lean_contract_vocabulary_matches(spec: LeanContractVocabulary<'_>) {
    let lean_values = lean_vocabulary_values(spec.domain);
    let missing_from_lean = values_missing_from(spec.rust_values, &lean_values);
    let extra_in_lean = values_missing_from(&lean_values, spec.rust_values);
    let duplicate_rust_values = duplicate_values(spec.rust_values);
    let duplicate_lean_values = duplicate_values(&lean_values);

    assert!(
        spec.rust_values == lean_values.as_slice()
            && missing_from_lean.is_empty()
            && extra_in_lean.is_empty()
            && duplicate_rust_values.is_empty()
            && duplicate_lean_values.is_empty(),
        "Rust/Lean vocabulary contract mismatch\n  Lean contract domain: {}\n  Rust vocabulary source: {}\n  missing Lean values (present in Rust): {:?}\n  extra Lean values (absent from Rust): {:?}\n  duplicate Rust values: {:?}\n  duplicate Lean values: {:?}\n  Rust values: {:?}\n  Lean values: {:?}",
        spec.domain,
        spec.rust_source,
        missing_from_lean,
        extra_in_lean,
        duplicate_rust_values,
        duplicate_lean_values,
        spec.rust_values,
        lean_values
    );
}

pub(crate) fn assert_lean_contract_vocabulary_set_matches(spec: LeanContractVocabulary<'_>) {
    let lean_values = lean_vocabulary_values(spec.domain);
    let missing_from_lean = values_missing_from(spec.rust_values, &lean_values);
    let extra_in_lean = values_missing_from(&lean_values, spec.rust_values);
    let duplicate_rust_values = duplicate_values(spec.rust_values);
    let duplicate_lean_values = duplicate_values(&lean_values);

    assert!(
        missing_from_lean.is_empty()
            && extra_in_lean.is_empty()
            && duplicate_rust_values.is_empty()
            && duplicate_lean_values.is_empty(),
        "Rust/Lean vocabulary contract set mismatch\n  Lean contract domain: {}\n  Rust vocabulary source: {}\n  missing Lean values (present in Rust): {:?}\n  extra Lean values (absent from Rust): {:?}\n  duplicate Rust values: {:?}\n  duplicate Lean values: {:?}\n  Rust values: {:?}\n  Lean values: {:?}",
        spec.domain,
        spec.rust_source,
        missing_from_lean,
        extra_in_lean,
        duplicate_rust_values,
        duplicate_lean_values,
        spec.rust_values,
        lean_values
    );
}

pub(crate) fn assert_lean_transition_is_legal(domain: &str, from: &str, to: &str) {
    let machine = lean_state_machine_contract(domain);
    assert!(
        machine
            .legal_transitions
            .iter()
            .any(|pair| pair.from == from && pair.to == to),
        "Lean state-machine contract {domain:?} does not allow transition {from:?} -> {to:?}\n  legal transitions: {:?}",
        machine.legal_transitions
    );
    assert!(
        !machine
            .illegal_transitions
            .iter()
            .any(|pair| pair.from == from && pair.to == to),
        "Lean state-machine contract {domain:?} marks transition {from:?} -> {to:?} as both legal and illegal"
    );
}

pub(crate) fn assert_lean_transition_is_illegal(domain: &str, from: &str, to: &str) {
    let machine = lean_state_machine_contract(domain);
    assert!(
        machine
            .illegal_transitions
            .iter()
            .any(|pair| pair.from == from && pair.to == to),
        "Lean state-machine contract {domain:?} does not mark transition {from:?} -> {to:?} illegal\n  illegal transitions: {:?}",
        machine.illegal_transitions
    );
    assert!(
        !machine
            .legal_transitions
            .iter()
            .any(|pair| pair.from == from && pair.to == to),
        "Lean state-machine contract {domain:?} marks transition {from:?} -> {to:?} as both legal and illegal"
    );
}

pub(crate) fn assert_state_machine_contract_is_complete(domain: &str) {
    let machine = lean_state_machine_contract(domain);
    let duplicate_states = duplicate_string_values(&machine.states);
    let duplicate_actions = duplicate_string_values(&machine.actions);
    let duplicate_legal_pairs = duplicate_transition_pairs(&machine.legal_transitions);
    let duplicate_illegal_pairs = duplicate_transition_pairs(&machine.illegal_transitions);
    let expected_pairs = machine.state_count * machine.state_count;
    let actual_pairs = machine.legal_transitions.len() + machine.illegal_transitions.len();

    assert!(
        machine.state_count == machine.states.len()
            && duplicate_states.is_empty()
            && duplicate_actions.is_empty()
            && duplicate_legal_pairs.is_empty()
            && duplicate_illegal_pairs.is_empty()
            && actual_pairs == expected_pairs
            && machine
                .legal_transitions
                .iter()
                .all(|pair| !machine.illegal_transitions.contains(pair))
            && machine.legal_transitions.iter().all(
                |pair| machine.states.contains(&pair.from) && machine.states.contains(&pair.to)
            )
            && machine.illegal_transitions.iter().all(
                |pair| machine.states.contains(&pair.from) && machine.states.contains(&pair.to)
            ),
        "Lean state-machine contract {domain:?} is incomplete or malformed\n  state_count: {}\n  states: {:?}\n  actions: {:?}\n  legal transitions: {:?}\n  illegal transitions: {:?}\n  duplicate states: {:?}\n  duplicate actions: {:?}\n  duplicate legal pairs: {:?}\n  duplicate illegal pairs: {:?}\n  expected pair partition size: {}\n  actual pair partition size: {}",
        machine.state_count,
        machine.states,
        machine.actions,
        machine.legal_transitions,
        machine.illegal_transitions,
        duplicate_states,
        duplicate_actions,
        duplicate_legal_pairs,
        duplicate_illegal_pairs,
        expected_pairs,
        actual_pairs
    );
}

pub(crate) fn assert_lifecycle_transition_cases_partition(
    domain: &str,
    states: &[&str],
    cases: &[LeanLifecycleTransitionCase],
) {
    let expected_pairs = states
        .iter()
        .flat_map(|from| {
            states
                .iter()
                .map(move |to| ((*from).to_string(), (*to).to_string()))
        })
        .collect::<BTreeSet<_>>();
    let mut actual_pairs = BTreeSet::new();
    let mut invalid_cases = Vec::new();

    for case in cases {
        if case.domain != domain {
            invalid_cases.push(format!("{} has wrong domain {:?}", case.name, case.domain));
        }
        if !matches!(
            case.classification.as_str(),
            "legal" | "illegal" | "productUnreachable" | "recoveryReachable"
        ) {
            invalid_cases.push(format!(
                "{} has invalid classification {:?}",
                case.name, case.classification
            ));
        }
        if case.classification == "legal" && case.action.is_none() {
            invalid_cases.push(format!("{} legal case missing action", case.name));
        }
        if case.classification != "legal" && case.action.is_some() {
            invalid_cases.push(format!("{} non-legal case has action", case.name));
        }
        // `productUnreachable` and `recoveryReachable` are the two classifications
        // that stand outside the machine's own transition relation, so each must
        // name the boundary that licenses it. `legal` and `illegal` are decided by
        // the relation itself and must not carry one.
        let requires_boundary = matches!(
            case.classification.as_str(),
            "productUnreachable" | "recoveryReachable"
        );
        if requires_boundary && case.boundary.is_none() {
            invalid_cases.push(format!(
                "{} {} case missing boundary",
                case.name, case.classification
            ));
        }
        if !requires_boundary && case.boundary.is_some() {
            invalid_cases.push(format!("{} reachable case has boundary", case.name));
        }
        if !expected_pairs.contains(&(case.from.clone(), case.to.clone())) {
            invalid_cases.push(format!(
                "{} has pair outside {domain} vocabulary: {:?} -> {:?}",
                case.name, case.from, case.to
            ));
        }
        if !actual_pairs.insert((case.from.clone(), case.to.clone())) {
            invalid_cases.push(format!(
                "{} duplicates pair {:?} -> {:?}",
                case.name, case.from, case.to
            ));
        }
    }

    let missing = expected_pairs
        .difference(&actual_pairs)
        .cloned()
        .collect::<Vec<_>>();
    let extra = actual_pairs
        .difference(&expected_pairs)
        .cloned()
        .collect::<Vec<_>>();

    assert!(
        invalid_cases.is_empty() && missing.is_empty() && extra.is_empty(),
        "Lean lifecycle transition cases for {domain:?} do not form a state^2 partition\n  invalid cases: {:?}\n  missing pairs: {:?}\n  extra pairs: {:?}\n  cases: {:?}",
        invalid_cases,
        missing,
        extra,
        cases
    );
}

pub(crate) fn assert_lean_to_defradb_vocabulary_matches(spec: LeanVocabulary<'_>) {
    let lean_values = lean_to_defradb_values(spec.lean_file, spec.model, spec.namespace);
    let missing_from_lean = values_missing_from(spec.rust_values, &lean_values);
    let extra_in_lean = values_missing_from(&lean_values, spec.rust_values);
    let duplicate_rust_values = duplicate_values(spec.rust_values);
    let duplicate_lean_values = duplicate_values(&lean_values);

    assert!(
        spec.rust_values == lean_values.as_slice()
            && missing_from_lean.is_empty()
            && extra_in_lean.is_empty()
            && duplicate_rust_values.is_empty()
            && duplicate_lean_values.is_empty(),
        "Rust/Lean toDefraDB vocabulary mismatch\n  Lean file: {}\n  namespace: {}\n  Rust vocabulary source: {}\n  missing Lean values (present in Rust): {:?}\n  extra Lean values (absent from Rust): {:?}\n  duplicate Rust values: {:?}\n  duplicate Lean values: {:?}\n  Rust values: {:?}\n  Lean values: {:?}",
        spec.lean_file,
        spec.namespace,
        spec.rust_source,
        missing_from_lean,
        extra_in_lean,
        duplicate_rust_values,
        duplicate_lean_values,
        spec.rust_values,
        lean_values
    );
}

fn load_lean_contract_snapshot() -> LeanContractSnapshot {
    gents_lean_contract::load_contract_snapshot().unwrap_or_else(|error| panic!("{error:#}"))
}

pub(crate) fn lean_to_defradb_values<'a>(
    lean_file: &str,
    model: &'a str,
    namespace: &str,
) -> Vec<&'a str> {
    parse_lean_to_defradb_values(model, namespace)
        .unwrap_or_else(|error| panic!("{}", error.message(lean_file, namespace)))
}

pub(super) fn parse_lean_to_defradb_values<'a>(
    model: &'a str,
    namespace: &str,
) -> Result<Vec<&'a str>, LeanVocabularyParseError<'a>> {
    let namespace_start = format!("namespace {namespace}");
    let namespace_end = format!("end {namespace}");
    let mut found_namespace = false;
    let mut found_to_defradb = false;
    let mut in_namespace = false;
    let mut in_to_defradb = false;
    let mut values = Vec::new();

    for (index, line) in model.lines().enumerate() {
        let line_number = index + 1;
        let trimmed = line.trim();

        if !in_namespace {
            if trimmed == namespace_start {
                found_namespace = true;
                in_namespace = true;
            }
            continue;
        }

        if trimmed == namespace_end {
            break;
        }

        if !in_to_defradb {
            if trimmed.starts_with("def toDefraDB") {
                found_to_defradb = true;
                in_to_defradb = true;
            }
            continue;
        }

        if trimmed.is_empty() || trimmed.starts_with("--") {
            continue;
        }

        if trimmed.starts_with('|') {
            values.push(parse_to_defradb_arm(trimmed, line_number)?);
            continue;
        }

        if values.is_empty() {
            return Err(LeanVocabularyParseError::MalformedArm {
                line_number,
                line: trimmed,
                reason: "expected a toDefraDB pattern arm starting with `| .`",
            });
        }

        break;
    }

    if !found_namespace {
        return Err(LeanVocabularyParseError::MissingNamespace);
    }
    if !found_to_defradb {
        return Err(LeanVocabularyParseError::MissingToDefraDB);
    }
    if values.is_empty() {
        return Err(LeanVocabularyParseError::EmptyToDefraDB);
    }

    Ok(values)
}

fn parse_to_defradb_arm<'a>(
    trimmed: &'a str,
    line_number: usize,
) -> Result<&'a str, LeanVocabularyParseError<'a>> {
    let Some(rest) = trimmed.strip_prefix("| .") else {
        return Err(LeanVocabularyParseError::MalformedArm {
            line_number,
            line: trimmed,
            reason: "expected a toDefraDB pattern arm starting with `| .`",
        });
    };
    let Some((_constructor, value)) = rest.split_once("=>") else {
        return Err(LeanVocabularyParseError::MalformedArm {
            line_number,
            line: trimmed,
            reason: "missing `=>`",
        });
    };

    parse_string_literal(value.trim(), line_number, trimmed)
}

fn parse_string_literal<'a>(
    value: &'a str,
    line_number: usize,
    line: &'a str,
) -> Result<&'a str, LeanVocabularyParseError<'a>> {
    let Some(after_opening_quote) = value.strip_prefix('"') else {
        return Err(LeanVocabularyParseError::MalformedArm {
            line_number,
            line,
            reason: "expected a string literal after `=>`",
        });
    };
    let Some(end_index) = after_opening_quote.find('"') else {
        return Err(LeanVocabularyParseError::MalformedArm {
            line_number,
            line,
            reason: "string literal is missing a closing quote",
        });
    };
    let literal = &after_opening_quote[..end_index];
    let trailing = after_opening_quote[end_index + 1..].trim();
    if !trailing.is_empty() && !trailing.starts_with("--") {
        return Err(LeanVocabularyParseError::MalformedArm {
            line_number,
            line,
            reason: "expected only optional comment text after the string literal",
        });
    }

    Ok(literal)
}

fn values_missing_from<'a>(expected: &[&'a str], actual: &[&str]) -> Vec<&'a str> {
    expected
        .iter()
        .copied()
        .filter(|value| !actual.contains(value))
        .collect()
}

fn duplicate_values<'a>(values: &[&'a str]) -> Vec<&'a str> {
    let mut seen = Vec::new();
    let mut duplicates = Vec::new();
    for value in values {
        if seen.contains(value) {
            if !duplicates.contains(value) {
                duplicates.push(*value);
            }
        } else {
            seen.push(*value);
        }
    }
    duplicates
}

fn duplicate_string_values(values: &[String]) -> Vec<String> {
    let refs = values.iter().map(String::as_str).collect::<Vec<_>>();
    duplicate_values(&refs)
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn duplicate_transition_pairs(values: &[LeanTransitionPair]) -> Vec<LeanTransitionPair> {
    let mut seen = Vec::new();
    let mut duplicates = Vec::new();
    for value in values {
        if seen.contains(value) {
            if !duplicates.contains(value) {
                duplicates.push(value.clone());
            }
        } else {
            seen.push(value.clone());
        }
    }
    duplicates
}

impl LeanVocabularyParseError<'_> {
    fn message(&self, lean_file: &str, namespace: &str) -> String {
        match self {
            Self::MissingNamespace => format!(
                "Lean toDefraDB vocabulary parse failed\n  Lean file: {lean_file}\n  namespace: {namespace}\n  reason: namespace block was not found"
            ),
            Self::MissingToDefraDB => format!(
                "Lean toDefraDB vocabulary parse failed\n  Lean file: {lean_file}\n  namespace: {namespace}\n  reason: def toDefraDB was not found in the namespace"
            ),
            Self::EmptyToDefraDB => format!(
                "Lean toDefraDB vocabulary parse failed\n  Lean file: {lean_file}\n  namespace: {namespace}\n  reason: def toDefraDB has no parsed string-valued arms"
            ),
            Self::MalformedArm {
                line_number,
                line,
                reason,
            } => format!(
                "Lean toDefraDB vocabulary parse failed\n  Lean file: {lean_file}\n  namespace: {namespace}\n  line: {line_number}\n  reason: {reason}\n  source: {line}"
            ),
        }
    }
}
