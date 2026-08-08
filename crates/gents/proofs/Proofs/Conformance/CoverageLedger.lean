import Proofs.Conformance.ContractTypes
import Proofs.Conformance.Boundaries

namespace Conformance.Contracts

inductive Surface where
  | agentFacing
  | operatorCli
  | operatorUi
  | api
  | runtimeInternal
  deriving Repr, DecidableEq

def Surface.toString : Surface → String
  | Surface.agentFacing => "agentFacing"
  | Surface.operatorCli => "operatorCli"
  | Surface.operatorUi => "operatorUi"
  | Surface.api => "api"
  | Surface.runtimeInternal => "runtimeInternal"

def Surface.toJson (surface : Surface) : String :=
  jsonString surface.toString

def surfacesJson (surfaces : List Surface) : String :=
  jsonArray (surfaces.map Surface.toJson)

def allSurfaces : List Surface :=
  [ Surface.agentFacing
  , Surface.operatorCli
  , Surface.operatorUi
  , Surface.api
  , Surface.runtimeInternal
  ]

structure CoverageEntry where
  category : String
  domain : String
  consumer : String
  acceptedBoundary : String
  acceptedFollowUp : String
  feature : String := ""
  surfaces : List Surface := []
  deriving Repr

def consumerCoverage
    (category domain consumer : String) : CoverageEntry :=
  { category := category
  , domain := domain
  , consumer := consumer
  , acceptedBoundary := ""
  , acceptedFollowUp := ""
  }

def boundaryCoverage
    (category domain acceptedBoundary : String)
    (consumer : String := "") : CoverageEntry :=
  { category := category
  , domain := domain
  , consumer := consumer
  , acceptedBoundary := acceptedBoundary
  , acceptedFollowUp := ""
  }

def followUpCoverage
    (category domain acceptedFollowUp : String) : CoverageEntry :=
  { category := category
  , domain := domain
  , consumer := ""
  , acceptedBoundary := ""
  , acceptedFollowUp := acceptedFollowUp
  }

def consumerWithFollowUpCoverage
    (category domain consumer acceptedFollowUp : String) : CoverageEntry :=
  { category := category
  , domain := domain
  , consumer := consumer
  , acceptedBoundary := ""
  , acceptedFollowUp := acceptedFollowUp
  }

def tagged (entry : CoverageEntry)
    (feature : String) (surfaces : List Surface) : CoverageEntry :=
  { entry with feature := feature, surfaces := surfaces }

structure FeatureSurfaceRequirement where
  feature : String
  required : List Surface
  deferred : List (Surface × String)
  deriving Repr

def featureSurfaceDeferralJson (deferred : Surface × String) : String :=
  "{"
    ++ "\"surface\":" ++ Surface.toJson deferred.1 ++ ","
    ++ "\"note\":" ++ jsonString deferred.2
    ++ "}"

def FeatureSurfaceRequirement.toJson (req : FeatureSurfaceRequirement) : String :=
  "{"
    ++ "\"feature\":" ++ jsonString req.feature ++ ","
    ++ "\"required\":" ++ surfacesJson req.required ++ ","
    ++ "\"deferred\":" ++ jsonArray (req.deferred.map featureSurfaceDeferralJson)
    ++ "}"

def featureSurfaceRequirements : List FeatureSurfaceRequirement :=
  [ { feature := "request-lifecycle"
    , required := [Surface.agentFacing, Surface.runtimeInternal, Surface.operatorUi]
    , deferred := []
    }
  , { feature := "process-lifecycle"
    , required := [Surface.runtimeInternal]
    , deferred := []
    }
  , { feature := "inference-call"
    , required := [Surface.agentFacing, Surface.runtimeInternal]
    , deferred := []
    }
  , { feature := "completion-retry"
    , required := [Surface.agentFacing, Surface.runtimeInternal]
    , deferred := []
    }
  , { feature := "tool-call"
    , required := [Surface.agentFacing, Surface.runtimeInternal]
    , deferred := []
    }
  , { feature := "composed-invariants"
    , required := [Surface.runtimeInternal]
    , deferred := []
    }
  , { feature := "managed-exec"
    , required := [Surface.agentFacing]
    , deferred := []
    }
  , { feature := "pairing-reconcile"
    , required := [Surface.runtimeInternal]
    , deferred := []
    }
  , { feature := "runtime-reconcile"
    , required := [Surface.runtimeInternal]
    , deferred := []
    }
  , { feature := "session-recovery"
    , required := [Surface.runtimeInternal]
    , deferred := []
    }
  , { feature := "background-tools"
    , required := [Surface.agentFacing, Surface.operatorUi]
    , deferred :=
        [ (Surface.operatorCli, "#268")
        ]
    }
  , { feature := "subagents-cross-deployment"
    , required := [Surface.agentFacing, Surface.api, Surface.operatorUi, Surface.runtimeInternal]
    , deferred := []
    }
  , { feature := "interrupt-and-cancel"
    , required := [Surface.agentFacing, Surface.operatorUi]
    , deferred :=
        [ (Surface.operatorCli, "#266")
        ]
    }
  , { feature := "mcp-health"
    , required := [Surface.runtimeInternal, Surface.operatorCli, Surface.operatorUi]
    , deferred := []
    }
  , { feature := "identity-permission"
    , required := [Surface.runtimeInternal, Surface.api]
    , deferred := []
    }
  , { feature := "apply-reconcile"
    , required := [Surface.operatorCli]
    , deferred := [(Surface.operatorUi, "#281")]
    }
  , { feature := "event-delivery"
    , required := [Surface.runtimeInternal]
    , deferred := []
    }
  , { feature := "triggers"
    , required := [Surface.runtimeInternal, Surface.operatorCli, Surface.operatorUi]
    , deferred := []
    }
  , { feature := "compaction"
    , required := [Surface.agentFacing]
    , deferred := []
    }
  , { feature := "transcript"
    , required := [Surface.agentFacing, Surface.operatorUi]
    , deferred := []
    }
  , { feature := "prompt-assembly"
    , required := [Surface.agentFacing, Surface.runtimeInternal]
    , deferred := []
    }
  , { feature := "rendered-capture"
    , required := [Surface.runtimeInternal]
    , deferred :=
        [ (Surface.operatorCli,
            "#840 — `trace timeline|project` surfacing of captured requests lands with the projection slice; rows accumulate from this release onward with no reader")
        , (Surface.operatorUi,
            "#840 — the desktop rendered-request view lands with the same projection slice")
        ]
    }
  , { feature := "streaming-response"
    , required := [Surface.agentFacing, Surface.operatorUi]
    , deferred := []
    }
  , { feature := "client-shell"
    , required := [Surface.operatorUi]
    , deferred := []
    }
  , { feature := "codex-shim"
    , required := [Surface.api, Surface.runtimeInternal]
    , deferred := []
    }
  , { feature := "durable-goals"
    , required := allSurfaces
    , deferred := []
    }
  , { feature := "command-policy"
    , required := [Surface.agentFacing, Surface.operatorUi]
    , deferred := []
    }
  , { feature := "tool-policy"
    , required := [Surface.agentFacing, Surface.operatorUi]
    , deferred := []
    }
  , { feature := "self-config"
    , required := [Surface.agentFacing]
    , deferred :=
        [ (Surface.api, "MCP self-config surface deferred until MCP calls carry a DID (#654)")
        ]
    }
  , { feature := "recovery"
    , required := [Surface.runtimeInternal]
    , deferred := []
    }
  , { feature := "fleet-slot-accounting"
    , required := [Surface.runtimeInternal, Surface.api]
    , deferred := []
    }
  , { feature := "storage-observation"
    , required := [Surface.runtimeInternal]
    , deferred := []
    }
  , { feature := "persistence-failure-policy"
    , required := [Surface.runtimeInternal]
    , deferred := []
    }
  , { feature := "backend-health"
    , required := [Surface.runtimeInternal, Surface.operatorCli, Surface.operatorUi]
    , deferred := []
    }
  ]

def vocabularyCoverage : List CoverageEntry :=
  [ tagged (consumerCoverage
      "vocabulary"
      "RequestState"
      "lifecycle::tests::rust_request_lifecycle_state_vocabulary_matches_lean_model")
      "request-lifecycle" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "vocabulary"
      "ExecutionOrigin"
      "lifecycle::tests::rust_execution_origin_vocabulary_matches_lean_model")
      "request-lifecycle" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "vocabulary"
      "ProcessState"
      "runtime_status::tests::rust_process_state_vocabulary_matches_lean_model")
      "process-lifecycle" [Surface.runtimeInternal]
  , tagged (boundaryCoverage
      "vocabulary"
      "PersistenceState"
      boundaryPersistenceAbstractLifecycleId)
      "persistence-failure-policy" [Surface.runtimeInternal]
  , tagged (boundaryCoverage
      "vocabulary"
      "PersistenceFailurePolicy"
      boundaryStorageHookFailurePolicyId)
      "persistence-failure-policy" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "vocabulary"
      "ReconcilePhase"
      "runtime_status::tests::rust_reconcile_phase_vocabulary_matches_lean_model")
      "runtime-reconcile" [Surface.runtimeInternal]
  , tagged (boundaryCoverage
      "vocabulary"
      "StorageObservation"
      boundaryStorageObservationDaemonVisibleId)
      "storage-observation" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "vocabulary"
      "SessionRecoveryLatestRequestState"
      "conformance::generated_session_recovery_cases_drive_db_backed_reissue_contract")
      "session-recovery" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "vocabulary"
      "InferenceCallState"
      "admission::tests::rust_inference_call_state_vocabulary_matches_lean_model")
      "inference-call" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "vocabulary"
      "InferenceCallTerminalReason"
      "admission::tests::rust_inference_call_terminal_reason_vocabulary_matches_lean_model")
      "inference-call" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "vocabulary"
      "CompletionRetryFailureClass"
      "conformance::completion_retry_lean_witness_cases_hold")
      "completion-retry" [Surface.agentFacing, Surface.runtimeInternal]
  , tagged (consumerCoverage
      "vocabulary"
      "ToolRetryDisposition"
      "mcp_pool::tests::tool_retry_disposition_contract_cases_match_mcp_pool_policy")
      "tool-call" [Surface.agentFacing]
  , tagged (consumerCoverage
      "vocabulary"
      "ToolCallState"
      "tool_call_lifecycle::tests::rust_tool_call_state_vocabulary_matches_lean_model")
      "tool-call" [Surface.agentFacing]
  , tagged (consumerCoverage
      "vocabulary"
      "CancelCause"
      "tool_call_lifecycle::tests::rust_cancel_cause_vocabulary_matches_lean_model")
      "interrupt-and-cancel" [Surface.agentFacing]
  , tagged (consumerCoverage
      "vocabulary"
      "ManagedExecState"
      "managed_exec::tests::rust_managed_exec_state_vocabulary_matches_lean_model")
      "managed-exec" [Surface.agentFacing]
  , tagged (consumerCoverage
      "vocabulary"
      "ToolFailureClass"
      "tool_call_lifecycle::tests::rust_failure_class_vocabulary_matches_lean_model")
      "tool-call" [Surface.agentFacing]
  , tagged (consumerCoverage
      "vocabulary"
      "AwaitMode"
      "conformance::lean_emits_await_mode_vocabulary")
      "background-tools" [Surface.agentFacing]
  , tagged (consumerCoverage
      "vocabulary"
      "CancelPolicy"
      "conformance::lean_emits_cancel_policy_vocabulary")
      "background-tools" [Surface.agentFacing]
  , tagged (consumerCoverage
      "vocabulary"
      "ChildTerminal"
      "conformance::lean_emits_child_terminal_vocabulary_and_projections")
      "background-tools" [Surface.agentFacing]
  , tagged (consumerCoverage
      "vocabulary"
      "GoalStatus"
      "conformance::goals::rust_goal_status_vocabulary_and_machine_match_lean_contract")
      "durable-goals" [Surface.runtimeInternal]
  ]

def stateMachineCoverage : List CoverageEntry :=
  [ tagged (consumerCoverage
      "state_machine"
      "Request"
      "lifecycle::tests::request_state_machine_contract_is_complete")
      "request-lifecycle" [Surface.agentFacing, Surface.runtimeInternal]
  , tagged (consumerCoverage
      "state_machine"
      "Process"
      "runtime_status::tests::rust_process_state_transitions_match_lean_contract")
      "process-lifecycle" [Surface.runtimeInternal]
  , tagged (boundaryCoverage
      "state_machine"
      "Persistence.failClosed"
      boundaryStorageHookFailurePolicyId
      "conformance::lean_executable_contracts_cover_initial_domains")
      "persistence-failure-policy" [Surface.runtimeInternal]
  , tagged (boundaryCoverage
      "state_machine"
      "Persistence.failOpen"
      boundaryStorageHookFailurePolicyId
      "conformance::lean_executable_contracts_cover_initial_domains")
      "persistence-failure-policy" [Surface.runtimeInternal]
  , tagged (boundaryCoverage
      "state_machine"
      "StorageObservation.failClosed"
      boundaryStorageObservationDaemonVisibleId
      "conformance::lean_executable_contracts_cover_initial_domains")
      "storage-observation" [Surface.runtimeInternal]
  , tagged (boundaryCoverage
      "state_machine"
      "StorageObservation.failOpen"
      boundaryStorageObservationDaemonVisibleId
      "conformance::lean_executable_contracts_cover_initial_domains")
      "storage-observation" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "state_machine"
      "RuntimeReconcile"
      "runtime_status::tests::runtime_reconcile_state_machine_contract_is_complete")
      "runtime-reconcile" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "state_machine"
      "PairingReconcile"
      "agent::reconcile::tests::pairing_reconcile_state_machine_contract_is_complete")
      "pairing-reconcile" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "state_machine"
      "SessionRecovery"
      "conformance::generated_session_recovery_cases_drive_db_backed_reissue_contract")
      "session-recovery" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "state_machine"
      "InferenceCall"
      "admission::tests::rust_inference_call_transition_table_matches_lean_contract")
      "inference-call" [Surface.agentFacing, Surface.runtimeInternal]
  , tagged (consumerCoverage
      "state_machine"
      "ToolCall"
      "tool_call_lifecycle::tests::tool_call_state_machine_contract_is_complete")
      "tool-call" [Surface.agentFacing, Surface.runtimeInternal]
  , tagged (consumerCoverage
      "state_machine"
      "ManagedExec"
      "managed_exec::tests::managed_exec_state_machine_contract_is_complete")
      "managed-exec" [Surface.agentFacing]
  , tagged (consumerCoverage
      "state_machine"
      "AwaitMode"
      "conformance::lean_emits_await_mode_vocabulary")
      "background-tools" [Surface.agentFacing]
  , tagged (consumerCoverage
      "state_machine"
      "CancelPolicy"
      "conformance::lean_emits_cancel_policy_vocabulary")
      "background-tools" [Surface.agentFacing]
  , tagged (consumerCoverage
      "state_machine"
      "ChildTerminal"
      "conformance::lean_emits_child_terminal_vocabulary_and_projections")
      "background-tools" [Surface.agentFacing]
  , tagged (consumerCoverage
      "state_machine"
      "Goal"
      "conformance::goals::rust_goal_status_vocabulary_and_machine_match_lean_contract")
      "durable-goals" [Surface.runtimeInternal]
  ]

def caseCoverage : List CoverageEntry :=
  [ tagged (consumerCoverage
      "lifecycle_transition_cases"
      "RequestTransitions"
      "conformance::generated_request_transition_cases_cover_lifecycle_policy")
      "request-lifecycle" [Surface.agentFacing, Surface.runtimeInternal]
  , tagged (consumerCoverage
      "lifecycle_transition_cases"
      "ProcessTransitions"
      "runtime_status::tests::generated_process_transition_cases_match_runtime_status_policy")
      "process-lifecycle" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "trigger_cases"
      "TriggerDispatch"
      "trigger_engine::tests::trigger_engine_dispatch_matches_lean_generated_contract_cases")
      "triggers" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "trigger_cases"
      "TriggerDispatch"
      "cli_config_task_run::config_task_run_matches_lean_manual_dispatch_contract")
      "triggers" [Surface.operatorCli]
  , tagged (consumerCoverage
      "trigger_cases"
      "TriggerDispatch"
      "gents_desktop_bridge::snapshot::tests::runtime::task_recent_runs_view_consumes_generated_trigger_dispatch_lineage_contract_cases")
      "triggers" [Surface.operatorUi]
  , tagged (consumerCoverage
      "goal_decision_cases"
      "GoalDecisionCases"
      "conformance::goals::generated_goal_decision_cases_fence_runtime_controller")
      "durable-goals" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "goal_transition_cases"
      "GoalTransitionCases"
      "conformance::goals::generated_goal_transition_cases_fence_runtime_state_machine")
      "durable-goals" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "goal_decision_cases"
      "GoalDecisionCases"
      "goal_continuation_live::durable_goal_continues_with_real_inference_until_model_completes")
      "durable-goals" [Surface.agentFacing]
  , tagged (consumerCoverage
      "goal_decision_cases"
      "GoalDecisionCases"
      "cli_goal::goal_set_get_pause_resume_and_clear_are_durable")
      "durable-goals" [Surface.operatorCli]
  , tagged (consumerCoverage
      "goal_decision_cases"
      "GoalDecisionCases"
      "apps/gents-desktop/tests/durable-goal-card.test.tsx::durable goal transcript card renders persisted goal status, objective, token usage, and active time")
      "durable-goals" [Surface.operatorUi]
  , tagged (consumerCoverage
      "goal_decision_cases"
      "GoalDecisionCases"
      "cli_codex_shim::thread_goal_round_trip_survives_shim_restart")
      "durable-goals" [Surface.api]
  , tagged (consumerCoverage
      "runtime_cases"
      "RuntimeReconcileCases"
      "runtime_status::tests::runtime_status_generation_updates_match_lean_runtime_reconcile_cases")
      "runtime-reconcile" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "startup_readiness_cases"
      "StartupReadinessCases"
      "conformance::generated_startup_readiness_cases_pin_bounded_barrier_release")
      "runtime-reconcile" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "apply_reconcile_cases"
      "ApplyReconcileCases"
      "config_import::lean_apply_write_boundary_tests::generated_apply_reconcile_cases_fence_production_apply_write_boundary")
      "apply-reconcile" [Surface.operatorCli]
  , tagged (consumerCoverage
      "tool_policy_cases"
      "ToolPolicyCases"
      "conformance::generated_tool_policy_cases_match_lean_composition")
      "tool-policy" [Surface.operatorUi, Surface.agentFacing]
  , tagged (consumerCoverage
      "self_config_field_tables"
      "SelfConfigFieldTables"
      "conformance::self_config_field_tables_match_lean_contract")
      "self-config" [Surface.agentFacing]
  , tagged (consumerCoverage
      "self_config_cases"
      "SelfConfigCases"
      "conformance::generated_self_config_cases_fence_patch_merge")
      "self-config" [Surface.agentFacing]
  , tagged (consumerCoverage
      "session_recovery_cases"
      "SessionRecoveryCases"
      "conformance::generated_session_recovery_cases_drive_db_backed_reissue_contract")
      "session-recovery" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "slot_cases"
      "InferenceCallSlotAccounting"
      "conformance::generated_inference_slot_accounting_cases_drive_db_backed_reconstruction")
      "inference-call" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "inference_exact_target_cases"
      "InferenceCallExactTarget"
      "conformance::generated_inference_call_exact_target_cases_drive_fenced_updates")
      "inference-call" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "inference_exact_target_trace_cases"
      "InferenceCallExactTargetTraces"
      "conformance::generated_inference_call_exact_target_cases_drive_fenced_updates")
      "inference-call" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "completion_retry_cases"
      "completionRetry"
      "conformance::completion_retry_lean_witness_cases_hold")
      "completion-retry" [Surface.agentFacing, Surface.runtimeInternal]
  , tagged (boundaryCoverage
      "fleet_cases"
      "FleetSlotAccounting"
      boundaryFleetSlotAccountingDerivedViewId
      "admission::tests::generated_slot_accounting_fleet_cases_match_admission_runtime_boundary")
      "fleet-slot-accounting" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "fleet_cases"
      "FleetSlotAccounting"
      "cli_server::server_exposes_fleet_slot_snapshot_endpoint")
      "fleet-slot-accounting" [Surface.api]
  , tagged (boundaryCoverage
      "persistence_policy_cases"
      "PersistenceFailurePolicyCases"
      boundaryStorageHookFailurePolicyId
      "hook::tests::generated_persistence_failure_policy_cases_match_hook_decisions")
      "persistence-failure-policy" [Surface.runtimeInternal]
  , tagged (boundaryCoverage
      "storage_observation_cases"
      "StorageObservationRuntimeCases"
      boundaryStorageObservationDaemonVisibleId
      "hook::tests::generated_storage_observation_cases_match_hook_runtime_classification")
      "storage-observation" [Surface.runtimeInternal]
  , tagged (boundaryCoverage
      "backend_health_cases"
      "BackendHealthAdmissionCases"
      boundaryBackendHealthAdmissionFreshnessId
      "backend_registry::tests::generated_backend_health_admission_cases_match_registry_and_admission_policy")
      "backend-health" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "backend_health_cases"
      "BackendHealthAdmissionCases"
      "backend_registry::tests::display_state_matches_every_lean_backend_health_admission_case")
      "backend-health" [Surface.operatorUi]
  , tagged (consumerCoverage
      "backend_health_cases"
      "BackendHealthTransitionCases"
      "backend_health::tests::generated_backend_health_cases_match_prober_transitions")
      "backend-health" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "backend_health_cases"
      "BackendHealthTransitionCases"
      "http::prometheus::tests::backend_probe_status_metric_reflects_measured_health")
      "backend-health" [Surface.operatorCli]
  , tagged (consumerCoverage
      "native_filesystem_boundary_cases"
      "NativeFilesystemBoundaryCases"
      "toolset::tests::generated_native_filesystem_boundary_cases_match_preemptible_boundary_contract")
      "tool-call" [Surface.agentFacing]
  , tagged (consumerCoverage
      "managed_exec_cases"
      "ManagedExecLivenessCases"
      "conformance::managed_exec_liveness_cases_pin_native_process_boundary")
      "managed-exec" [Surface.agentFacing]
  , tagged (consumerCoverage
      "managed_exec_cases"
      "ManagedExecToolBoundaryCases"
      "conformance::managed_exec_tool_boundary_cases_cover_every_native_subprocess_tool")
      "managed-exec" [Surface.agentFacing]
  , tagged (consumerCoverage
      "pairing_reconcile_cases"
      "PairingReconcileShutdownBoundaryCases"
      "conformance::pairing_reconcile_shutdown_boundary_preempts_in_flight_sweep")
      "pairing-reconcile" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "pairing_reconcile_cases"
      "PairingReconcileSweepRetryBoundaryCases"
      "conformance::pairing_reconcile_top_level_sweep_failure_is_nonterminal_and_retried")
      "pairing-reconcile" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "pairing_reconcile_cases"
      "PairingReconcileSweepSchedulingCases"
      "conformance::pairing_reconcile_sweep_does_not_head_of_line_block_ready_peer")
      "pairing-reconcile" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "frontend_client_shell_cases"
      "FrontendClientShellCases"
      "packages/gents-desktop-chat/src/chat-shell.test.ts::projectChatShell matches generated Lean ClientShell projection contracts")
      "client-shell" [Surface.operatorUi]
  , tagged (consumerCoverage
      "desktop_client_shell_cases"
      "DesktopClientShellCases"
      "gents_desktop_bridge::snapshot::tests::session_state::session_snapshot_projection_consumes_generated_client_shell_contract_cases")
      "client-shell" [Surface.operatorUi]
  , tagged (consumerCoverage
      "live_overlay_cases"
      "LiveOverlayCases"
      "live_overlay::live_overlay_cases_match_lean_table")
      "client-shell" [Surface.operatorUi]
  , tagged (consumerCoverage
      "request_lifecycle_operator_ui_cases"
      "RequestLifecycleOperatorUiCases"
      "gents_desktop_bridge::snapshot::tests::session_state::session_snapshot_binds_request_lifecycle_operator_ui_cases")
      "request-lifecycle" [Surface.operatorUi]
  , tagged (consumerCoverage
      "tool_cases"
      "ToolExecutionPreflight"
      "conformance::generated_tool_execution_cases_cover_preflight_and_retry_contracts")
      "tool-call" [Surface.agentFacing]
  , tagged (consumerCoverage
      "tool_cases"
      "ToolExecutionRetry"
      "mcp_pool::tests::tool_retry_disposition_contract_cases_match_mcp_pool_policy")
      "tool-call" [Surface.agentFacing]
  , tagged (consumerCoverage
      "command_policy_cases"
      "CommandPolicyValidation"
      "toolset::tests::generated_command_policy_cases_match_rust_validation")
      "command-policy" [Surface.agentFacing]
  , tagged (consumerCoverage
      "command_policy_cases"
      "CommandPolicySandbox"
      "toolset::tests::generated_command_sandbox_cases_match_rust_selection")
      "command-policy" [Surface.agentFacing]
  , tagged (consumerCoverage
      "command_policy_cases"
      "CommandPolicyEnv"
      "toolset::tests::generated_command_env_cases_match_rust_filtering")
      "command-policy" [Surface.agentFacing]
  , tagged (consumerCoverage
      "command_policy_cases"
      "CommandPolicyOperatorUi"
      "gents_desktop_bridge::snapshot::tests::session_timeline::structured_command_policy_denial_projects_to_rendered_tool")
      "command-policy" [Surface.operatorUi]
  , tagged (consumerCoverage
      "queue_deadline_cases"
      "QueueDeadlineConformanceCases"
      "conformance::generated_queue_deadline_cases_pin_r4a_contract_rows")
      "request-lifecycle" [Surface.agentFacing, Surface.runtimeInternal]
  , tagged (consumerCoverage
      "recovery_sweep_cases"
      "RecoverySweepCases"
      "conformance::generated_recovery_sweep_cases_drive_startup_recovery_contract")
      "recovery" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "recovery_equivalence_cases"
      "RecoveryEquivalenceCases"
      "conformance::generated_recovery_equivalence_cases_pin_uninterrupted_convergence_contract")
      "recovery" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "recovery_outcome_cases"
      "RecoveryOutcomeCases"
      "conformance::generated_recovery_outcome_cases_fence_duplicate_tolerant_counting")
      "recovery" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "restart_disposition_cases"
      "RestartDispositionCases"
      "conformance::generated_restart_disposition_cases_drive_recover_all")
      "recovery" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "r6_background_cases"
      "R6BackgroundingCases"
      "conformance::generated_r6_backgrounding_cases_drive_tool_backgrounding_contract")
      "background-tools" [Surface.agentFacing]
  , tagged (consumerCoverage
      "r5_cross_deployment_cases"
      "R5CrossDeploymentCases"
      "conformance::generated_r5_cross_deployment_cases_drive_production_dispatch")
      "subagents-cross-deployment" [Surface.agentFacing]
  , tagged (consumerCoverage
      "r5_cross_deployment_cases"
      "R5CrossDeploymentCases"
      "http::r5_dispatch::tests::subagent_dispatch_endpoint_matches_agent_request_parent_walk")
      "subagents-cross-deployment" [Surface.api]
  , tagged (consumerCoverage
      "r5_cross_deployment_cases"
      "R5CrossDeploymentCases"
      "gents_desktop_bridge::snapshot::tests::subagent_lineage::subagent_tree_view_consumes_generated_r5_cross_deployment_contract_cases")
      "subagents-cross-deployment" [Surface.operatorUi]
  , tagged (consumerCoverage
      "composed_invariant_witnesses"
      "ComposedInvariantWitnesses"
      "conformance::generated_composed_invariant_witnesses_drive_tool_lifecycle_conformance")
      "composed-invariants" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "cancel_propagation_cases"
      "CancelPropagationCases"
      "conformance::cancel_propagation_cases_drive_production_interrupt")
      "interrupt-and-cancel" [Surface.agentFacing, Surface.runtimeInternal]
  , tagged (consumerCoverage
      "r6_background_theorem_witnesses"
      "BackgroundBudgetBoundedTheoremWitness"
      "conformance::generated_r6_background_theorem_witnesses_drive_admission_budget_invariant")
      "background-tools" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "r6_background_theorem_witnesses"
      "CascadeCancelsChildTheoremWitness"
      "conformance::generated_r6_background_theorem_witnesses_drive_cascade_cancellation_trace")
      "background-tools" [Surface.agentFacing]
  , tagged (consumerCoverage
      "subagent_delegation_graph_cases"
      "SubagentDelegationGraphCases"
      "conformance::generated_subagent_delegation_graph_cases_pin_gap2_contract")
      "background-tools" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "r4c_background_work_cases"
      "R4cBackgroundWorkCases"
      "conformance::generated_r4c_background_work_cases_pin_observable_shapes")
      "background-tools" [Surface.agentFacing]
  , tagged (consumerCoverage
      "r4c_background_work_cases"
      "R4cBackgroundWorkCases"
      "gents_desktop_bridge::snapshot::operations_snapshot::tests::project_filters_to_background_await_mode_only")
      "background-tools" [Surface.operatorUi]
  , tagged (consumerCoverage
      "r4c_background_work_cases"
      "R4cBackgroundWorkCases"
      "conformance::generated_read_tool_output_witness_drives_hook_dispatch")
      "background-tools" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "tool_output_paging_cases"
      "ToolOutputPagingCases"
      "background_tools::tests::generated_tool_output_paging_cases_match_slice_function")
      "background-tools" [Surface.agentFacing]
  , tagged (consumerCoverage
      "bridge_step_cases"
      "BridgeStepCases"
      "conformance::generated_bridge_step_cases_drive_bridge_lifecycle")
      "background-tools" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "codex_shim_projection_cases"
      "CodexShimProjectionCases"
      "conformance::generated_codex_shim_projection_cases_pin_adapter_mapping")
      "codex-shim" [Surface.api, Surface.runtimeInternal]
  , tagged (consumerCoverage
      "codex_shim_subagent_tool_cases"
      "CodexShimSubagentToolCases"
      "conformance::generated_codex_shim_projection_cases_pin_adapter_mapping")
      "codex-shim" [Surface.api, Surface.runtimeInternal]
  , tagged (consumerCoverage
      "codex_shim_subagent_status_cases"
      "CodexShimSubagentStatusCases"
      "conformance::generated_codex_shim_projection_cases_pin_adapter_mapping")
      "codex-shim" [Surface.api, Surface.runtimeInternal]
  , tagged (consumerCoverage
      "codex_shim_subagent_visibility_cases"
      "CodexShimSubagentVisibilityCases"
      "conformance::generated_codex_shim_projection_cases_pin_adapter_mapping")
      "codex-shim" [Surface.api, Surface.runtimeInternal]
  , tagged (consumerCoverage
      "codex_shim_subagent_metadata_cases"
      "CodexShimSubagentMetadataCases"
      "conformance::generated_codex_shim_projection_cases_pin_adapter_mapping")
      "codex-shim" [Surface.api, Surface.runtimeInternal]
  , tagged (consumerCoverage
      "codex_shim_subagent_listing_cases"
      "CodexShimSubagentListingCases"
      "conformance::generated_codex_shim_projection_cases_pin_adapter_mapping")
      "codex-shim" [Surface.api, Surface.runtimeInternal]
  , tagged (consumerCoverage
      "codex_shim_subagent_thread_shape_cases"
      "CodexShimSubagentThreadShapeCases"
      "conformance::generated_codex_shim_projection_cases_pin_adapter_mapping")
      "codex-shim" [Surface.api, Surface.runtimeInternal]
  , tagged (consumerCoverage
      "codex_shim_reasoning_projection_cases"
      "CodexShimReasoningProjectionCases"
      "conformance::generated_codex_shim_projection_cases_pin_adapter_mapping")
      "codex-shim" [Surface.api, Surface.runtimeInternal]
  , tagged (consumerCoverage
      "codex_shim_thread_status_cases"
      "CodexShimThreadStatusCases"
      "conformance::generated_codex_shim_projection_cases_pin_adapter_mapping")
      "codex-shim" [Surface.api, Surface.runtimeInternal]
  , tagged (consumerCoverage
      "codex_shim_behavior_selection_cases"
      "CodexShimBehaviorSelectionCases"
      "conformance::generated_codex_shim_projection_cases_pin_adapter_mapping")
      "codex-shim" [Surface.api, Surface.runtimeInternal]
  , tagged (consumerCoverage
      "codex_shim_tool_metadata_cases"
      "CodexShimToolMetadataCases"
      "conformance::generated_codex_shim_projection_cases_pin_adapter_mapping")
      "codex-shim" [Surface.api, Surface.runtimeInternal]
  , tagged (consumerCoverage
      "codex_shim_context_usage_cases"
      "CodexShimContextUsageCases"
      "conformance::generated_codex_shim_projection_cases_pin_adapter_mapping")
      "codex-shim" [Surface.api, Surface.runtimeInternal]
  , tagged (consumerCoverage
      "codex_shim_compaction_projection_cases"
      "CodexShimCompactionProjectionCases"
      "conformance::generated_codex_shim_projection_cases_pin_adapter_mapping")
      "codex-shim" [Surface.api, Surface.runtimeInternal]
  , tagged (consumerCoverage
      "codex_shim_turn_lifecycle_cases"
      "CodexShimTurnLifecycleCases"
      "conformance::generated_codex_shim_projection_cases_pin_adapter_mapping")
      "codex-shim" [Surface.api, Surface.runtimeInternal]
  , tagged (consumerCoverage
      "codex_shim_binding_cases"
      "CodexShimBindingCases"
      "conformance::generated_codex_shim_binding_cases_pin_runnable_gated_binding")
      "codex-shim" [Surface.api, Surface.runtimeInternal]
  , tagged (consumerCoverage
      "transcript_cases"
      "TranscriptConformanceCases"
      "conformance::generated_transcript_cases_drive_agent_message_ordering_contract")
      "transcript" [Surface.agentFacing]
  , tagged (consumerCoverage
      "transcript_finalization_cases"
      "TranscriptFinalizationCases"
      "conformance::generated_transcript_finalization_and_provider_history_cases_pin_split_contract")
      "transcript" [Surface.agentFacing, Surface.runtimeInternal]
  , tagged (consumerCoverage
      "transcript_provider_history_cases"
      "TranscriptProviderHistoryCases"
      "conformance::generated_transcript_finalization_and_provider_history_cases_pin_split_contract")
      "transcript" [Surface.agentFacing, Surface.runtimeInternal]
  , tagged (consumerCoverage
      "transcript_cases"
      "TranscriptConformanceCases"
      "gents_desktop_bridge::snapshot::tests::session_state::session_snapshot_transcript_rendering_consumes_generated_transcript_cases")
      "transcript" [Surface.operatorUi]
  , tagged (consumerCoverage
      "identity_structural_cases"
      "IdentityStructuralCases"
      "identity::identity_structural_cases_match_lean_verdicts")
      "identity-permission" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "identity_permission_cases"
      "IdentityPermissionCases"
      "identity::identity_permission_cases_pin_runtime_permission_contract_shape")
      "identity-permission" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "identity_permission_cases"
      "IdentityPermissionCases"
      "http::identity_decide::tests::identity_decide_endpoint_matches_lean_permission_cases")
      "identity-permission" [Surface.api]
  , tagged (consumerCoverage
      "identity_contracts"
      "IdentityContracts"
      "identity::identity_respects_principal_contract_enforced_by_runtime_routing")
      "identity-permission" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "streaming_response_cases"
      "ResponseTransitionCases"
      "conformance::generated_streaming_response_cases_pin_lifecycle_contract")
      "streaming-response" [Surface.agentFacing]
  , tagged (consumerCoverage
      "streaming_response_interrupt_flow_cases"
      "ResponseInterruptFlowCases"
      "conformance::generated_streaming_response_interrupt_flow_cases_drive_daemon_contract")
      "streaming-response" [Surface.agentFacing]
  , tagged (consumerCoverage
      "streaming_response_cases"
      "ResponseTransitionCases"
      "gents_desktop_bridge::snapshot::tests::session_state::session_snapshot_streaming_response_overlay_consumes_generated_transition_cases")
      "streaming-response" [Surface.operatorUi]
  , tagged (consumerCoverage
      "compaction_reducer_cases"
      "CompactionReducerCases"
      "conformance::generated_compaction_reducer_cases_pin_contract")
      "compaction" [Surface.agentFacing]
  , tagged (consumerCoverage
      "prompt_assembly_cases"
      "PromptAssemblySanitizeCases"
      "conformance::prompt_assembly::generated_sanitize_cases_drive_the_production_sanitizer")
      "prompt-assembly" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "prompt_assembly_cases"
      "PromptAssemblyLayerCases"
      "agent::loop_stream::tests::generated_layer_cases_pin_the_assembled_request_order")
      "prompt-assembly" [Surface.agentFacing]
  , tagged (consumerCoverage
      "prompt_assembly_cases"
      "PromptAssemblyRepairCases"
      "agent::loop_stream::tests::generated_repair_cases_drive_tool_argument_repair")
      "prompt-assembly" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "prompt_assembly_cases"
      "PromptAssemblyBudgetCases"
      "agent::daemon::request::budget_contract_tests::generated_budget_cases_drive_dynamic_output_compaction_trigger")
      "prompt-assembly" [Surface.agentFacing]
  , tagged (consumerCoverage
      "prompt_assembly_cases"
      "PromptAssemblyTurnBudgetCases"
      "agent::loop_stream::tests::generated_turn_budget_cases_drive_every_completion_dispatch")
      "prompt-assembly" [Surface.agentFacing]
  , tagged (consumerCoverage
      "rendered_capture_cases"
      "RenderedCaptureCases"
      "agent::loop_stream::tests::generated_rendered_capture_cases_fence_persist_before_send")
      "rendered-capture" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "rendered_capture_cases"
      "RenderedCaptureKeyCases"
      "conformance::rendered_capture::generated_rendered_capture_key_cases_pin_the_capture_key_tuple")
      "rendered-capture" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "request_ingest_cases"
      "RequestIngestCases"
      "conformance::request_ingest::generated_request_ingest_cases_fence_provenance_invariants")
      "request-lifecycle" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "subagent_bridge_admission_cases"
      "SubagentBridgeAdmissionCases"
      "conformance::subagent_source::generated_bridge_admission_cases_require_signed_exact_parent_evidence")
      "subagents-cross-deployment" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "event_delivery_cases"
      "EventDeliveryTransitionCases"
      "conformance::event_delivery_transition_cases_match_contract")
      "event-delivery" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "event_delivery_cases"
      "EventDeliverySourceInstances"
      "conformance::event_delivery_source_instances_match_runtime")
      "event-delivery" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "event_delivery_cases"
      "EventDeliveryConvergenceTraces"
      "conformance::event_delivery_convergence_traces_match_runtime_or_deviation")
      "event-delivery" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "mcp_health_cases"
      "MCPHealthCases"
      "health_checker::tests::generated_mcp_health_cases_match_health_checker_transitions")
      "mcp-health" [Surface.runtimeInternal]
  , tagged (consumerCoverage
      "mcp_health_cases"
      "MCPHealthCases"
      "cli_mcp_probe::mcp_probe_json_reports_health_snapshot_for_registry_service")
      "mcp-health" [Surface.operatorCli]
  , tagged (consumerCoverage
      "mcp_health_cases"
      "MCPHealthCases"
      "gents_desktop_bridge::snapshot::tests::mcp_health::mcp_health_view_preserves_every_generated_lean_mcp_health_case_transition")
      "mcp-health" [Surface.operatorUi]
  , tagged (consumerCoverage
      "vocabulary"
      "CancelCause"
      "gents_desktop_bridge::snapshot::tests::session_state::session_snapshot_derives_cancel_cause_for_interrupted_response_and_cancelled_tool_call")
      "interrupt-and-cancel" [Surface.operatorUi]
  , tagged (consumerCoverage
      "state_machine"
      "ToolCall"
      "gents_desktop_bridge::tests::operations_cascade::preview_returns_four_classified_groups_and_a_signature")
      "interrupt-and-cancel" [Surface.operatorUi]
  , tagged (consumerCoverage
      "state_machine"
      "Request"
      "gents_desktop_bridge::tests::operations_interrupt::interrupt_request_cascade_returns_accepted_when_signature_matches")
      "interrupt-and-cancel" [Surface.operatorUi]
  ]

def followUpHookCoverage : List CoverageEntry :=
  [ tagged (followUpCoverage
      "follow_up_hook"
      "Subagent.BridgedState.foreground_blocks_parent_advance"
      "Subagent.BridgedState.foreground_blocks_parent_advance proves live foreground tools block parent progress/message advance; related aliases: Subagent.BridgedState.subagent_depth_bounded and Subagent.BridgedState.bridge_link_symmetric. Accepted Lean-only today because the invariant is a proof-layer bridge guard rather than an emitted runtime witness.")
      "background-tools" []
  , tagged (followUpCoverage
      "follow_up_hook"
      "Subagent.BridgedState.bridged_child_completion_propagates"
      "Subagent.BridgedState.bridged_child_completion_propagates proves child completion projects to parent bridge-tool completion; related failure projection: Subagent.BridgedState.bridged_child_failure_projects. Accepted Lean-only today because R6Background emits data-shape cases and this theorem remains a formal trace projection.")
      "background-tools" []
  , tagged (followUpCoverage
      "follow_up_hook"
      "Subagent.BridgedState.inv_depth"
      "Subagent.BridgedState.inv_depth proves bridged traces preserve max subagent depth; related link invariant: Subagent.BridgedState.inv_link. The arbitrary graph-level closure is emitted through subagent_delegation_graph_cases; this hook remains for the paired bridge trace invariant.")
      "background-tools" []
  , tagged (followUpCoverage
      "follow_up_hook"
      "Subagent.BridgedState.bridgedUniqueCallIds_preserved"
      "Subagent.BridgedState.bridgedUniqueCallIds_preserved proves parent and child tool call ids remain unique across bridged traces. Accepted Lean-only today because the theorem lifts a structural uniqueness proof rather than an operational R6 witness.")
      "background-tools" []
  , tagged (boundaryCoverage
      "follow_up_hook"
      "StreamingResponse.Transition.streamIdleTimeout.deadlinePrecondition"
      boundaryStreamingResponseIdleTimeoutDeadlineId)
      "streaming-response" [Surface.runtimeInternal]
  , tagged (boundaryCoverage
      "follow_up_hook"
      "PromptAssembly.providerInput.sanitizeLoadedHistory"
      boundaryPromptAssemblyProviderInputSanitizationId)
      "compaction" [Surface.agentFacing]
  , tagged (boundaryCoverage
      "follow_up_hook"
      "Compaction.safeToReduce.sessionScopeResolver"
      boundaryCompactionSafeToReduceSessionScopeId)
      "compaction" [Surface.agentFacing]
  , tagged (boundaryCoverage
      "follow_up_hook"
      "Compaction.providerViewAppend.uniqueCallIdsChecked"
      boundaryCompactionUniqueCallIdsCheckedId)
      "compaction" [Surface.agentFacing]
  , tagged (followUpCoverage
      "follow_up_hook"
      "PromptAssembly.Template.system_render_stable"
      "system_render_stable proves a well-formed system template renders identically across requests that agree on run-constant values — the cacheable prefix is byte-stable. validateSystem_correct ties the apply-time guard to well-formedness. Fenced by tests/conformance/prompt_template.rs.")
      "compaction" []
  , tagged (followUpCoverage
      "follow_up_hook"
      "PromptAssembly.Template.assembleWithContext_tail"
      "assembleWithContext_tail proves the per-request assembly ends with exactly [contextPreamble, prompt] — the rendered <context> message rides immediately before the prompt. Fenced by the loop_stream::assemble_new_messages helper and its unit test assembles_context_immediately_before_prompt; a reorder there breaks the test and contradicts the proof.")
      "compaction" []
  ]

def followUpHookIds : List String :=
  followUpHookCoverage.map (fun entry => entry.domain)

def followUpHooksJson : String :=
  jsonArray (followUpHookIds.map jsonString)

def coverageLedger : List CoverageEntry :=
  vocabularyCoverage ++ stateMachineCoverage ++ caseCoverage ++ followUpHookCoverage

structure FeatureMatrixCell where
  feature : String
  surface : Surface
  coverageStrength : String
  rowCount : Nat
  pendingFollowUps : Nat
  deferredNote : String
  deriving Repr

def featureSurfaceRequirementsJson : String :=
  jsonArray (featureSurfaceRequirements.map FeatureSurfaceRequirement.toJson)

def stringPresent (value : String) : Bool :=
  !(value == "")

def rowCoverageStrength (entry : CoverageEntry) : String :=
  let hasConsumer := stringPresent entry.consumer
  let hasBoundary := stringPresent entry.acceptedBoundary
  let hasFollowUp := stringPresent entry.acceptedFollowUp
  if hasConsumer && !hasFollowUp then
    "consumer"
  else if hasConsumer && hasFollowUp then
    "consumer_with_follow_up"
  else if !hasConsumer && hasBoundary then
    "boundary"
  else if !hasConsumer && !hasBoundary && hasFollowUp then
    "follow_up_only"
  else
    "missing"

def rowHasSurface (surface : Surface) (entry : CoverageEntry) : Bool :=
  entry.surfaces.any (fun candidate => candidate == surface)

def matchingFeatureSurfaceRows (feature : String) (surface : Surface) : List CoverageEntry :=
  coverageLedger.filter (fun entry =>
    (entry.feature == feature) && rowHasSurface surface entry)

def rowsHaveStrength (rows : List CoverageEntry) (strength : String) : Bool :=
  rows.any (fun entry => rowCoverageStrength entry == strength)

def strongestCoverageStrength (rows : List CoverageEntry) : String :=
  if rowsHaveStrength rows "consumer" then
    "consumer"
  else if rowsHaveStrength rows "consumer_with_follow_up" then
    "consumer_with_follow_up"
  else if rowsHaveStrength rows "boundary" then
    "boundary"
  else if rowsHaveStrength rows "follow_up_only" then
    "follow_up_only"
  else
    "missing"

def pendingFollowUpCount (rows : List CoverageEntry) : Nat :=
  (rows.filter (fun entry => stringPresent entry.acceptedFollowUp)).length

def requiredSurface (req : FeatureSurfaceRequirement) (surface : Surface) : Bool :=
  req.required.any (fun candidate => candidate == surface)

def deferredSurfaceNote (req : FeatureSurfaceRequirement) (surface : Surface) : Option String :=
  match req.deferred.find? (fun deferred => deferred.1 == surface) with
  | some deferred => some deferred.2
  | none => none

def featureMatrixCell? (req : FeatureSurfaceRequirement)
    (surface : Surface) : Option FeatureMatrixCell :=
  let rows := matchingFeatureSurfaceRows req.feature surface
  match rows with
  | [] =>
      match deferredSurfaceNote req surface with
      | some note =>
          some
            { feature := req.feature
            , surface := surface
            , coverageStrength := "deferred"
            , rowCount := 0
            , pendingFollowUps := 0
            , deferredNote := note
            }
      | none =>
          if requiredSurface req surface then
            some
              { feature := req.feature
              , surface := surface
              , coverageStrength := "missing"
              , rowCount := 0
              , pendingFollowUps := 0
              , deferredNote := ""
              }
          else
            none
  | _ :: _ =>
      some
        { feature := req.feature
        , surface := surface
        , coverageStrength := strongestCoverageStrength rows
        , rowCount := rows.length
        , pendingFollowUps := pendingFollowUpCount rows
        , deferredNote := ""
        }

def FeatureMatrixCell.toJson (cell : FeatureMatrixCell) : String :=
  "{"
    ++ "\"coverage_strength\":" ++ jsonString cell.coverageStrength ++ ","
    ++ "\"row_count\":" ++ toString cell.rowCount ++ ","
    ++ "\"pending_follow_ups\":" ++ toString cell.pendingFollowUps ++ ","
    ++ "\"deferred_note\":" ++ jsonString cell.deferredNote
    ++ "}"

def featureMatrixSurfaceCellJson? (req : FeatureSurfaceRequirement)
    (surface : Surface) : Option String :=
  match featureMatrixCell? req surface with
  | some cell => some (Surface.toJson surface ++ ":" ++ FeatureMatrixCell.toJson cell)
  | none => none

def featureMatrixFeatureJson (req : FeatureSurfaceRequirement) : String :=
  jsonString req.feature ++ ":"
    ++ "{"
    ++ String.intercalate ","
      (allSurfaces.filterMap (fun surface => featureMatrixSurfaceCellJson? req surface))
    ++ "}"

def featureMatrixJson : String :=
  "{"
    ++ String.intercalate "," (featureSurfaceRequirements.map featureMatrixFeatureJson)
    ++ "}"

def CoverageEntry.toJson (entry : CoverageEntry) : String :=
  "{"
    ++ "\"category\":" ++ jsonString entry.category ++ ","
    ++ "\"domain\":" ++ jsonString entry.domain ++ ","
    ++ "\"consumer\":" ++ jsonString entry.consumer ++ ","
    ++ "\"accepted_boundary\":" ++ jsonString entry.acceptedBoundary ++ ","
    ++ "\"accepted_follow_up\":" ++ jsonString entry.acceptedFollowUp ++ ","
    ++ "\"feature\":" ++ jsonString entry.feature ++ ","
    ++ "\"surfaces\":" ++ surfacesJson entry.surfaces
    ++ "}"

def coverageLedgerJson : String :=
  jsonArray (coverageLedger.map CoverageEntry.toJson)

end Conformance.Contracts
