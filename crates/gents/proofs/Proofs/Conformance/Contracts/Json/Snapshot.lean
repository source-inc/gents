import Proofs.Conformance.Contracts.Json.Core
import Proofs.Conformance.Contracts.Json.Runtime
import Proofs.Conformance.Contracts.Json.Scheduling
import Proofs.Conformance.Contracts.Json.ToolExecution
import Proofs.Conformance.Contracts.Json.CommandPolicy
import Proofs.Conformance.Contracts.Json.ToolPolicy
import Proofs.Conformance.Contracts.Json.ClientRuntime
import Proofs.Conformance.Contracts.Json.BackgroundWork
import Proofs.Conformance.Contracts.Json.ComposedInvariants
import Proofs.Conformance.Contracts.Json.CodexShim
import Proofs.Conformance.Contracts.Json.Workflow
import Proofs.Conformance.Contracts.Json.SelfConfig
import Proofs.Conformance.Contracts.Json.Goal
import Proofs.Conformance.Contracts.Json.PromptAssembly
import Proofs.Conformance.Contracts.Json.RenderedCapture
import Proofs.CompletionRetry.Contracts
import Proofs.Conformance.Triggers.Contracts
import Proofs.Conformance.ClientShell.Contracts
import Proofs.ApplyReconcile.ContractCases
import Proofs.Conformance.Deviations
import Proofs.Conformance.CoverageLedger
import Proofs.Identity.Conformance
import Proofs.Conformance.EventDelivery

namespace Conformance.Contracts

open Conformance.ContractCases

def snapshotJson : String :=
  "{"
    ++ "\"generated_by\":\"lake env lean --run Proofs/Conformance/Contracts.lean\","
    ++ "\"vocabularies\":"
      ++ jsonArray (vocabularies.map VocabularyContract.toJson) ++ ","
    ++ "\"state_machines\":"
      ++ jsonArray (stateMachines.map StateMachineContract.toJson) ++ ","
    ++ "\"request_transition_cases\":"
      ++ jsonArray (requestTransitionCases.map lifecycleTransitionCaseJson) ++ ","
    ++ "\"process_transition_cases\":"
      ++ jsonArray (processTransitionCases.map lifecycleTransitionCaseJson) ++ ","
    ++ "\"trigger_dispatch_case_count\":"
      ++ toString Conformance.TriggerContracts.triggerDispatchCaseCount ++ ","
    ++ "\"trigger_dispatch_cases\":"
      ++ Conformance.TriggerContracts.triggerDispatchCasesJson ++ ","
    ++ "\"goal_decision_cases\":"
      ++ goalDecisionCasesJson ++ ","
    ++ "\"goal_transition_cases\":"
      ++ goalTransitionCasesJson ++ ","
    ++ "\"frontend_client_shell_case_count\":"
      ++ toString Conformance.ClientShellContracts.frontendClientShellCaseCount ++ ","
    ++ "\"frontend_client_shell_cases\":"
      ++ Conformance.ClientShellContracts.frontendClientShellCasesJson ++ ","
    ++ "\"desktop_client_shell_case_count\":"
      ++ toString Conformance.ClientShellContracts.desktopClientShellCaseCount ++ ","
    ++ "\"desktop_client_shell_cases\":"
      ++ Conformance.ClientShellContracts.desktopClientShellCasesJson ++ ","
    ++ "\"request_lifecycle_operator_ui_cases\":"
      ++ Conformance.ClientShellContracts.requestLifecycleOperatorUiCasesJson ++ ","
    ++ "\"startup_readiness_cases\":"
      ++ startupReadinessCasesJson ++ ","
    ++ "\"runtime_reconcile_cases\":"
      ++ jsonArray (runtimeReconcileCases.map runtimeReconcileCaseJson) ++ ","
    ++ "\"apply_reconcile_cases\":"
      ++ ApplyReconcile.ContractCases.applyReconcileCasesJson ++ ","
    ++ "\"tool_policy_cases\":"
      ++ toolPolicyCasesJson ++ ","
    ++ "\"self_config_field_tables\":"
      ++ selfConfigFieldTablesJson ++ ","
    ++ "\"self_config_cases\":"
      ++ selfConfigCasesJson ++ ","
    ++ "\"session_recovery_cases\":"
      ++ jsonArray (sessionRecoveryCases.map sessionRecoveryCaseJson) ++ ","
    ++ "\"inference_slot_accounting_cases\":"
      ++ jsonArray (inferenceSlotAccountingCases.map inferenceSlotAccountingCaseJson) ++ ","
    ++ "\"fleet_slot_accounting_cases\":"
      ++ jsonArray (fleetSlotAccountingCases.map fleetSlotAccountingCaseJson) ++ ","
    ++ "\"persistence_failure_policy_cases\":"
      ++ jsonArray
        (persistenceFailurePolicyCases.map persistenceFailurePolicyCaseJson) ++ ","
    ++ "\"storage_observation_runtime_cases\":"
      ++ jsonArray
        (storageObservationRuntimeCases.map storageObservationRuntimeCaseJson) ++ ","
    ++ "\"backend_health_admission_cases\":"
      ++ jsonArray
        (backendHealthAdmissionCases.map backendHealthAdmissionCaseJson) ++ ","
    ++ "\"native_filesystem_boundary_cases\":"
      ++ jsonArray
        (nativeFilesystemBoundaryCases.map nativeFilesystemBoundaryCaseJson) ++ ","
    ++ "\"managed_exec_tool_boundary_cases\":"
      ++ jsonArray
        (managedExecToolBoundaryCases.map managedExecToolBoundaryCaseJson) ++ ","
    ++ "\"pairing_reconcile_shutdown_boundary_cases\":"
      ++ jsonArray
        (pairingReconcileShutdownBoundaryCases.map
          pairingReconcileShutdownBoundaryCaseJson) ++ ","
    ++ "\"pairing_reconcile_sweep_retry_boundary_cases\":"
      ++ jsonArray
        (pairingReconcileSweepRetryBoundaryCases.map
          pairingReconcileSweepRetryBoundaryCaseJson) ++ ","
    ++ "\"pairing_reconcile_sweep_scheduling_cases\":"
      ++ jsonArray
        (pairingReconcileSweepSchedulingCases.map
          pairingReconcileSweepSchedulingCaseJson) ++ ","
    ++ "\"managed_exec_liveness_cases\":"
      ++ jsonArray
        (managedExecLivenessCases.map managedExecLivenessCaseJson) ++ ","
    ++ "\"tool_preflight_cases\":"
      ++ jsonArray (ToolExecution.preflightCases.map toolPreflightCaseJson) ++ ","
    ++ "\"tool_retry_cases\":"
      ++ jsonArray (ToolExecution.retryCases.map toolRetryCaseJson) ++ ","
    ++ "\"completion_retry_cases\":"
      ++ CompletionRetry.Contracts.casesJson ++ ","
    ++ "\"boundaries\":"
      ++ boundariesJson ++ ","
    ++ "\"deviations\":"
      ++ deviationsJson ++ ","
    ++ "\"command_policy_cases\":"
      ++ jsonArray (CommandPolicy.commandPolicyCases.map commandPolicyCaseJson) ++ ","
    ++ "\"command_sandbox_cases\":"
      ++ jsonArray (CommandPolicy.commandSandboxCases.map commandSandboxCaseJson) ++ ","
    ++ "\"command_env_cases\":"
      ++ jsonArray (CommandPolicy.commandEnvCases.map commandEnvCaseJson) ++ ","
    ++ "\"live_overlay_cases\":"
      ++ jsonArray (liveOverlayCases.map liveOverlayCaseJson) ++ ","
    ++ "\"queue_deadline_conformance_cases\":"
      ++ jsonArray
        (queueDeadlineConformanceCases.map queueDeadlineConformanceCaseJson) ++ ","
    ++ "\"recovery_sweep_cases\":"
      ++ jsonArray
        (Recovery.recoverySweepCases.map recoverySweepCaseJson) ++ ","
    ++ "\"recovery_outcome_cases\":"
      ++ jsonArray
        (Recovery.recoveryOutcomeCases.map recoveryOutcomeCaseJson) ++ ","
    ++ "\"recovery_equivalence_cases\":"
      ++ jsonArray
        (Recovery.recoveryEquivalenceCases.map recoveryEquivalenceCaseJson) ++ ","
    ++ "\"restart_disposition_cases\":"
      ++ jsonArray
        (Recovery.restartDispositionCases.map restartDispositionCaseJson) ++ ","
    ++ "\"r4c_background_work_cases\":"
      ++ jsonArray r4cBackgroundWorkCasesJson ++ ","
    ++ "\"tool_output_paging_cases\":"
      ++ jsonArray
        (toolOutputPagingCases.map toolOutputPagingCaseJson) ++ ","
    ++ "\"bridge_step_cases\":"
      ++ jsonArray
        (bridgeStepCases.map bridgeStepCaseJson) ++ ","
    ++ "\"codex_shim_projection_cases\":"
      ++ codexShimProjectionCasesJson ++ ","
    ++ "\"codex_shim_subagent_tool_cases\":"
      ++ codexShimSubagentToolCasesJson ++ ","
    ++ "\"codex_shim_subagent_status_cases\":"
      ++ codexShimSubagentStatusCasesJson ++ ","
    ++ "\"codex_shim_subagent_visibility_cases\":"
      ++ codexShimSubagentVisibilityCasesJson ++ ","
    ++ "\"codex_shim_subagent_metadata_cases\":"
      ++ codexShimSubagentMetadataCasesJson ++ ","
    ++ "\"codex_shim_subagent_listing_cases\":"
      ++ codexShimSubagentListingCasesJson ++ ","
    ++ "\"codex_shim_subagent_thread_shape_cases\":"
      ++ codexShimSubagentThreadShapeCasesJson ++ ","
    ++ "\"codex_shim_reasoning_projection_cases\":"
      ++ codexShimReasoningProjectionCasesJson ++ ","
    ++ "\"codex_shim_thread_status_cases\":"
      ++ codexShimThreadStatusCasesJson ++ ","
    ++ "\"codex_shim_behavior_selection_cases\":"
      ++ codexShimBehaviorSelectionCasesJson ++ ","
    ++ "\"codex_shim_tool_metadata_cases\":"
      ++ codexShimToolMetadataCasesJson ++ ","
    ++ "\"codex_shim_context_usage_cases\":"
      ++ codexShimContextUsageCasesJson ++ ","
    ++ "\"codex_shim_compaction_projection_cases\":"
      ++ codexShimCompactionProjectionCasesJson ++ ","
    ++ "\"codex_shim_turn_lifecycle_cases\":"
      ++ codexShimTurnLifecycleCasesJson ++ ","
    ++ "\"codex_shim_binding_cases\":"
      ++ codexShimBindingCasesJson ++ ","
    ++ "\"r6_backgrounding_cases\":"
      ++ jsonArray
        (r6BackgroundingCases.map r6BackgroundingCaseJson) ++ ","
    ++ "\"r5_cross_deployment_cases\":"
      ++ jsonArray
        (r5CrossDeploymentCases.map r5CrossDeploymentCaseJson) ++ ","
    ++ "\"composed_invariant_witnesses\":"
      ++ jsonArray
        (composedInvariantWitnesses.map composedInvariantWitnessJson) ++ ","
    ++ "\"cancel_propagation_cases\":"
      ++ jsonArray
        (cancelPropagationCases.map cancelPropagationCaseJson) ++ ","
    ++ "\"workflow_cases\":"
      ++ workflowCasesJson ++ ","
    ++ "\"workflow_composite_interrupt_cases\":"
      ++ compositeInterruptCasesJson ++ ","
    ++ "\"r6_background_theorem_witnesses\":"
      ++ jsonArray
        (r6BackgroundTheoremWitnesses.map backgroundTheoremWitnessJson) ++ ","
    ++ "\"subagent_delegation_graph_cases\":"
      ++ jsonArray
        (subagentDelegationGraphCases.map subagentDelegationGraphCaseJson) ++ ","
    ++ "\"transcript_conformance_cases\":"
      ++ jsonArray
        (transcriptConformanceCases.map transcriptCaseJson) ++ ","
    ++ "\"streaming_response_cases\":"
      ++ jsonArray
        (StreamingResponse.responseTransitionCases.map responseTransitionCaseJson) ++ ","
    ++ "\"streaming_response_interrupt_flow_cases\":"
      ++ jsonArray
        (StreamingResponse.responseInterruptFlowCases.map responseInterruptFlowCaseJson) ++ ","
    ++ "\"prompt_assembly_sanitize_cases\":"
      ++ promptAssemblySanitizeCasesJson ++ ","
    ++ "\"prompt_assembly_layer_cases\":"
      ++ promptAssemblyLayerCasesJson ++ ","
    ++ "\"prompt_assembly_repair_cases\":"
      ++ promptAssemblyRepairCasesJson ++ ","
    ++ "\"prompt_assembly_budget_cases\":"
      ++ promptAssemblyBudgetCasesJson ++ ","
    ++ "\"prompt_assembly_turn_budget_cases\":"
      ++ promptAssemblyTurnBudgetCasesJson ++ ","
    ++ "\"rendered_capture_cases\":"
      ++ renderedCaptureCasesJson ++ ","
    ++ "\"rendered_capture_key_cases\":"
      ++ renderedCaptureKeyCasesJson ++ ","
    ++ "\"capture_scope_cases\":"
      ++ captureScopeCasesJson ++ ","
    ++ "\"capture_order_cases\":"
      ++ captureOrderCasesJson ++ ","
    ++ "\"compaction_reducer_cases\":"
      ++ jsonArray
        (Compaction.compactionReducerCases.map compactionReducerCaseJson) ++ ","
    ++ "\"mcp_health_cases\":"
      ++ jsonArray
        (Proofs.MCPHealth.transitionCases.map mcpHealthCaseJson) ++ ","
    ++ "\"backend_health_cases\":"
      ++ jsonArray
        (Proofs.BackendHealth.transitionCases.map backendHealthCaseJson) ++ ","
    ++ "\"follow_up_hooks\":"
      ++ followUpHooksJson ++ ","
    ++ "\"event_delivery_transition_case_count\":"
      ++ toString Conformance.EventDelivery.transitionCaseCount ++ ","
    ++ "\"event_delivery_transition_cases\":"
      ++ Conformance.EventDelivery.transitionCasesJson ++ ","
    ++ "\"event_delivery_source_instances\":"
      ++ Conformance.EventDelivery.sourceInstancesJson ++ ","
    ++ "\"event_delivery_convergence_traces\":"
      ++ Conformance.EventDelivery.convergenceTracesJson ++ ","
    ++ "\"coverage_ledger\":"
      ++ coverageLedgerJson
    ++ ",\"feature_surface_requirements\":"
      ++ featureSurfaceRequirementsJson
    ++ ",\"feature_matrix\":"
      ++ featureMatrixJson
    ++ ",\"identity_structural_cases\":"
      ++ Identity.Conformance.structuralCasesJson
    ++ ",\"identity_permission_cases\":"
      ++ Identity.Conformance.identityPermissionCasesJson
    ++ ",\"identity_contracts\":"
      ++ Identity.Conformance.identityContractsJson
    ++ "}"

end Conformance.Contracts
