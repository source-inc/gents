import Proofs.Conformance.Contracts.Json.Helpers
import Proofs.Conformance.ContractCases
import Proofs.StreamingResponse.Executable
import Proofs.Compaction.Executable
import Proofs.Recovery.ContractCases

namespace Conformance.Contracts

open Conformance.ContractCases

def liveOverlayCaseJson (witness : LiveOverlayCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString witness.name ++ ","
    ++ "\"responseStatus\":" ++ jsonString witness.responseStatus ++ ","
    ++ "\"materialized\":" ++ boolString witness.materialized ++ ","
    ++ "\"hasDurableOwner\":" ++ boolString witness.hasDurableOwner ++ ","
    ++ "\"precedingToolCalls\":" ++ toString witness.precedingToolCalls ++ ","
    ++ "\"turnTerminal\":" ++ boolString witness.turnTerminal ++ ","
    ++ "\"turnLabel\":" ++ jsonString witness.turnLabel ++ ","
    ++ "\"hasContent\":" ++ boolString witness.hasContent ++ ","
    ++ "\"hasReasoning\":" ++ boolString witness.hasReasoning ++ ","
    ++ "\"expectOverlay\":" ++ boolString witness.expectOverlay
    ++ "}"

def requestProgressCaseJson (witness : RequestProgressCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString witness.name ++ ","
    ++ "\"lifecycleState\":" ++ jsonString witness.lifecycleState ++ ","
    ++ "\"label\":" ++ jsonString witness.label ++ ","
    ++ "\"animated\":" ++ boolString witness.animated
    ++ "}"

def pendingUserTurnCaseJson (witness : PendingUserTurnCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString witness.name ++ ","
    ++ "\"hasDurableUserOwner\":"
      ++ boolString witness.hasDurableUserOwner ++ ","
    ++ "\"unrelatedUserTurns\":" ++ toString witness.unrelatedUserTurns ++ ","
    ++ "\"expectPendingTurn\":" ++ boolString witness.expectPendingTurn
    ++ "}"

def responseTransitionCaseJson
    (witness : StreamingResponse.ResponseTransitionCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString witness.name ++ ","
    ++ "\"group\":" ++ jsonString witness.group ++ ","
    ++ "\"action\":" ++ jsonString witness.action ++ ","
    ++ "\"legal\":" ++ boolString witness.legal ++ ","
    ++ "\"pre_status\":" ++ jsonString witness.preStatus ++ ","
    ++ "\"post_status\":" ++ jsonString witness.postStatus ++ ","
    ++ "\"pre_live_tail\":" ++ jsonString witness.preLiveTail ++ ","
    ++ "\"post_live_tail\":" ++ jsonString witness.postLiveTail ++ ","
    ++ "\"pre_tail_reasoning\":" ++ jsonString witness.preTailReasoning ++ ","
    ++ "\"post_tail_reasoning\":" ++ jsonString witness.postTailReasoning ++ ","
    ++ "\"pre_durable_reasoning\":" ++ jsonString witness.preDurableReasoning ++ ","
    ++ "\"post_durable_reasoning\":" ++ jsonString witness.postDurableReasoning ++ ","
    ++ "\"pre_token_count\":" ++ toString witness.preTokenCount ++ ","
    ++ "\"post_token_count\":" ++ toString witness.postTokenCount ++ ","
    ++ "\"error_reason\":" ++ jsonOptionalString witness.errorReason ++ ","
    ++ "\"pre_materialized_seq\":"
      ++ jsonOptionalNat witness.preMaterializedSeq ++ ","
    ++ "\"post_materialized_seq\":"
      ++ jsonOptionalNat witness.postMaterializedSeq ++ ","
    ++ "\"expected_request_state\":"
      ++ jsonOptionalString witness.expectedRequestState ++ ","
    ++ "\"expected_request_persistence\":"
      ++ jsonOptionalString witness.expectedRequestPersistence
    ++ "}"

def responseInterruptFlowCaseJson
    (witness : StreamingResponse.ResponseInterruptFlowCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString witness.name ++ ","
    ++ "\"group\":" ++ jsonString witness.group ++ ","
    ++ "\"action\":" ++ jsonString witness.action ++ ","
    ++ "\"pre_request_state\":"
      ++ jsonString witness.preRequestState ++ ","
    ++ "\"post_request_state\":"
      ++ jsonString witness.postRequestState ++ ","
    ++ "\"pre_response_status\":"
      ++ jsonString witness.preResponseStatus ++ ","
    ++ "\"post_response_status\":"
      ++ jsonString witness.postResponseStatus ++ ","
    ++ "\"pre_inference_call_state\":"
      ++ jsonString witness.preInferenceCallState ++ ","
    ++ "\"post_inference_call_state\":"
      ++ jsonString witness.postInferenceCallState ++ ","
    ++ "\"response_error_reason\":"
      ++ jsonString witness.responseErrorReason ++ ","
    ++ "\"interrupted_at_required\":"
      ++ boolString witness.interruptedAtRequired ++ ","
    ++ "\"completed_at_required\":"
      ++ boolString witness.completedAtRequired ++ ","
    ++ "\"live_tail_cleared\":"
      ++ boolString witness.liveTailCleared ++ ","
    ++ "\"partial_turn_materialized\":"
      ++ boolString witness.partialTurnMaterialized ++ ","
    ++ "\"request_terminal\":"
      ++ boolString witness.requestTerminal ++ ","
    ++ "\"response_terminal\":"
      ++ boolString witness.responseTerminal ++ ","
    ++ "\"inference_call_terminal\":"
      ++ boolString witness.inferenceCallTerminal
    ++ "}"

def compactionReducerCaseJson (witness : Compaction.CompactionReducerCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString witness.name ++ ","
    ++ "\"group\":" ++ jsonString witness.group ++ ","
    ++ "\"reducer\":" ++ jsonString witness.reducer ++ ","
    ++ "\"legal\":" ++ boolString witness.legal ++ ","
    ++ "\"pre_message_count\":" ++ toString witness.preMessageCount ++ ","
    ++ "\"post_message_count\":" ++ toString witness.postMessageCount ++ ","
    ++ "\"preserves_pairs\":" ++ boolString witness.preservesPairs ++ ","
    ++ "\"preserves_order\":" ++ boolString witness.preservesOrder ++ ","
    ++ "\"gate_open\":" ++ boolString witness.gateOpen ++ ","
    ++ "\"safe_to_reduce\":" ++ boolString witness.safeToReduce ++ ","
    ++ "\"reducer_is_identity\":"
      ++ boolString witness.reducerIsIdentity ++ ","
    ++ "\"split_index\":" ++ toString witness.splitIndex ++ ","
    ++ "\"safe_boundary\":" ++ toString witness.safeBoundary ++ ","
    ++ "\"retained_count\":" ++ toString witness.retainedCount
    ++ "}"

def recoverySweepCaseJson (witness : RecoverySweepCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString witness.name ++ ","
    ++ "\"sweep_id\":" ++ jsonString witness.sweepId ++ ","
    ++ "\"collection\":" ++ jsonString witness.collection ++ ","
    ++ "\"rust_function\":" ++ jsonString witness.rustFunction ++ ","
    ++ "\"cadence\":" ++ jsonString witness.cadence ++ ","
    ++ "\"implementation_status\":"
      ++ jsonString witness.implementationStatus ++ ","
    ++ "\"pre_state\":" ++ jsonString witness.preState ++ ","
    ++ "\"terminal_state\":" ++ jsonString witness.terminalState ++ ","
    ++ "\"measure_before\":" ++ toString witness.measureBefore ++ ","
    ++ "\"measure_after\":" ++ toString witness.measureAfter ++ ","
    ++ "\"deadline_expired\":" ++ jsonOptionalBool witness.deadlineExpired ++ ","
    ++ "\"unclaimed_expired\":" ++ jsonOptionalBool witness.unclaimedExpired ++ ","
    ++ "\"parent_live\":" ++ jsonOptionalBool witness.parentLive ++ ","
    ++ "\"parent_interrupted\":" ++ jsonOptionalBool witness.parentInterrupted ++ ","
    ++ "\"parent_terminal\":" ++ jsonOptionalBool witness.parentTerminal ++ ","
    ++ "\"execution_registered\":"
      ++ jsonOptionalBool witness.executionRegistered ++ ","
    ++ "\"recovery_cause\":" ++ jsonOptionalString witness.recoveryCause ++ ","
    ++ "\"notification_reason\":"
      ++ jsonOptionalString witness.notificationReason ++ ","
    ++ "\"deadline_audit_ref\":"
    ++ jsonString witness.deadlineAuditRef
    ++ "}"

def restartDispositionCaseJson (witness : RestartDispositionCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString witness.name ++ ","
    ++ "\"rust_function\":" ++ jsonString witness.rustFunction ++ ","
    ++ "\"await_mode\":" ++ jsonString witness.awaitMode ++ ","
    ++ "\"cancel_policy\":" ++ jsonString witness.cancelPolicy ++ ","
    ++ "\"child_linked\":" ++ boolString witness.childLinked ++ ","
    ++ "\"parent_observation\":"
      ++ jsonString witness.parentObservation ++ ","
    ++ "\"deadline_expired\":" ++ boolString witness.deadlineExpired ++ ","
    ++ "\"unclaimed_expired\":" ++ boolString witness.unclaimedExpired ++ ","
    ++ "\"disposition\":" ++ jsonString witness.disposition ++ ","
    ++ "\"cause\":" ++ jsonOptionalString witness.cause ++ ","
    ++ "\"terminal_state\":" ++ jsonOptionalString witness.terminalState ++ ","
    ++ "\"notification_reason\":"
      ++ jsonOptionalString witness.notificationReason ++ ","
    ++ "\"queue_source\":" ++ jsonOptionalString witness.queueSource ++ ","
    ++ "\"queue_key_prefix\":"
      ++ jsonOptionalString witness.queueKeyPrefix ++ ","
    ++ "\"theorem\":" ++ jsonString witness.theoremName
    ++ "}"

def recoveryEquivalenceCaseJson (witness : RecoveryEquivalenceCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString witness.name ++ ","
    ++ "\"source_sweep_case\":" ++ jsonString witness.sourceSweepCase ++ ","
    ++ "\"sweep_id\":" ++ jsonString witness.sweepId ++ ","
    ++ "\"collection\":" ++ jsonString witness.collection ++ ","
    ++ "\"rust_function\":" ++ jsonString witness.rustFunction ++ ","
    ++ "\"cadence\":" ++ jsonString witness.cadence ++ ","
    ++ "\"pre_state\":" ++ jsonString witness.preState ++ ","
    ++ "\"recovered_state\":" ++ jsonString witness.recoveredState ++ ","
    ++ "\"uninterrupted_state\":"
      ++ jsonString witness.uninterruptedState ++ ","
    ++ "\"equivalent\":" ++ boolString witness.equivalent ++ ","
    ++ "\"reexecutes\":" ++ boolString witness.reexecutes ++ ","
    ++ "\"can_hang\":" ++ boolString witness.canHang ++ ","
    ++ "\"theorem\":" ++ jsonString witness.theoremName ++ ","
    ++ "\"aggregate_theorem\":"
      ++ jsonString witness.aggregateTheoremName
    ++ "}"

end Conformance.Contracts
