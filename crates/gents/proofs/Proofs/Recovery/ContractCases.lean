import Proofs.Recovery.Sweeps
import Proofs.Conformance.ContractCases

namespace Recovery

open Conformance.ContractCases

def recoveryCase
    (sweep : RecoverySweep)
    (name preState terminalState deadlineAuditRef : String)
    (measureBefore : Nat := 1)
    (measureAfter : Nat := 0) : RecoverySweepCase :=
  { name := name
  , sweepId := sweep.sweepId
  , collection := sweep.collection.toContract
  , rustFunction := sweep.rustFunction
  , cadence := sweep.cadence.toContract
  , implementationStatus := sweep.implementationStatus.toContract
  , preState := preState
  , terminalState := terminalState
  , measureBefore := measureBefore
  , measureAfter := measureAfter
  , deadlineAuditRef := deadlineAuditRef
  }

def orphanedBackgroundRecoveryCase
    (name : String)
    (deadlineExpired unclaimedExpired parentLive parentInterrupted
      parentTerminal executionRegistered : Bool) : RecoverySweepCase :=
  let row : OrphanedBackgroundToolRow :=
    { call := r6NativeToolFixture
    , deadlineExpired := deadlineExpired
    , unclaimedExpired := unclaimedExpired
    , parentLive := parentLive
    , parentInterrupted := parentInterrupted
    , parentTerminal := parentTerminal
    , executionRegistered := executionRegistered
    }
  let recovered := orphanedBackgroundToolRecover row
  let cause := orphanedBackgroundToolCause row
  let notificationReason :=
    if row.parentLive || row.parentInterrupted || row.parentTerminal then
      cause.map fun recoveryCause =>
        match recoveryCause with
        | .deadlineExceeded => "deadline_exceeded"
        | .parentInterrupted => "parent_interrupted"
        | .parentTerminal => "parent_terminal"
        | .terminalizeBackgroundedAsInterrupted => "interrupted_on_restart"
        | .childCompleted => "child_completed"
        | .childFailed => "child_failed"
        | .childDead => "child_dead"
        | .childInterrupted => "child_interrupted"
        | .childSuperseded => "child_superseded"
        | .unclaimedCrossDeploymentSpawn => "unclaimed_spawn_timeout"
    else
      none
  { (recoveryCase
      orphanedBackgroundToolSweep
      name
      row.call.state.toDefraDB
      recovered.call.state.toDefraDB
      "r6-cross-turn-background-process-durability"
      (orphanedBackgroundToolMeasure row)
      (orphanedBackgroundToolMeasure recovered)) with
    deadlineExpired := some row.deadlineExpired
    unclaimedExpired := some row.unclaimedExpired
    parentLive := some row.parentLive
    parentInterrupted := some row.parentInterrupted
    parentTerminal := some row.parentTerminal
    executionRegistered := some row.executionRegistered
    recoveryCause := cause.map ToolRecoveryCause.toContract
    notificationReason := notificationReason
  }

def recoverySweepCases : List RecoverySweepCase :=
  [ recoveryCase
      requestRecoverySweep
      "request_claimed_recovery_to_failed"
      "claimed"
      "failed"
      "formal-coverage-audit-2026-05-13-gap-6"
  , recoveryCase
      requestRecoverySweep
      "request_processing_terminal_response_recovery_to_completed"
      "processing"
      "completed"
      "gents-664-durable-terminal-repair"
  , recoveryCase
      requestRecoverySweep
      "request_processing_recovery_to_failed"
      "processing"
      "failed"
      "formal-coverage-audit-2026-05-13-gap-6"
  , recoveryCase
      requestRecoverySweep
      "request_processing_interrupted_response_recovery_to_interrupted"
      "processing"
      "interrupted"
      "gents-664-durable-terminal-repair"
  , recoveryCase
      responseRecoverySweep
      "response_streaming_recovery_to_error"
      "streaming"
      "error"
      "deadline-plumbing-audit-2026-05-12-streaming-response-lifetime"
  , recoveryCase
      toolCallRecoverySweep
      "tool_running_deadline_exceeded_to_timed_out"
      "running"
      "timedOut"
      "deadline-plumbing-audit-2026-05-12-tool-call-persisted-deadline"
  , recoveryCase
      toolCallRecoverySweep
      "tool_running_parent_interrupted_to_cancelled"
      "running"
      "cancelled"
      "deadline-plumbing-audit-2026-05-12-request-interrupt-lifetime"
  , recoveryCase
      toolCallRecoverySweep
      "tool_running_terminal_parent_to_failed"
      "running"
      "failed"
      "deadline-plumbing-audit-2026-05-12-tool-call-persisted-deadline"
  , recoveryCase
      terminalParentOwnedToolSweep
      "live_running_native_tool_parent_interrupted_to_cancelled"
      "running"
      "cancelled"
      "terminal-parent-owned-tool-cleanup"
  , recoveryCase
      terminalParentOwnedToolSweep
      "live_running_tool_parent_terminal_to_failed"
      "running"
      "failed"
      "terminal-parent-owned-tool-cleanup"
  ,
    recoveryCase
      terminalParentOwnedToolSweep
      "live_detached_bridge_parent_failed_to_failed"
      "running"
      "failed"
      "terminal-parent-owned-tool-cleanup"
  , recoveryCase
      toolCallRecoverySweep
      "tool_backgrounded_running_live_parent_to_cancelled"
      "running"
      "cancelled"
      "r6-TerminalizeBackgroundedAsInterrupted"
  , orphanedBackgroundRecoveryCase
      "orphaned_background_tool_without_execution_to_cancelled"
      false false true false false false
  , orphanedBackgroundRecoveryCase
      "orphaned_background_tool_expired_missing_parent_to_timed_out"
      true false false false false false
  , orphanedBackgroundRecoveryCase
      "orphaned_background_tool_expired_terminal_parent_to_timed_out"
      true false false false true false
  , orphanedBackgroundRecoveryCase
      "orphaned_background_tool_unclaimed_to_failed"
      false true true false false false
  , orphanedBackgroundRecoveryCase
      "orphaned_background_tool_terminal_parent_to_failed"
      false false false false true false
  , recoveryCase
      backgroundCompletionSideEffectSweep
      "terminal_background_tool_missing_completion_side_effects_to_converged"
      "failed"
      "failed"
      "r6-cross-turn-background-process-durability"
  , recoveryCase
      toolCallRecoverySweep
      "tool_running_unclaimed_cross_deployment_spawn_to_failed"
      "running"
      "failed"
      "r5-cross-deployment-subagents-design"
  , recoveryCase
      toolCallRecoverySweep
      "tool_running_child_completed_to_completed"
      "running"
      "completed"
      "deadline-plumbing-audit-2026-05-12-subagent-bridge-terminal-lifetime"
  , recoveryCase
      toolCallRecoverySweep
      "tool_running_child_failed_to_failed"
      "running"
      "failed"
      "deadline-plumbing-audit-2026-05-12-subagent-bridge-terminal-lifetime"
  , recoveryCase
      toolCallRecoverySweep
      "tool_running_child_interrupted_to_cancelled"
      "running"
      "cancelled"
      "deadline-plumbing-audit-2026-05-12-subagent-bridge-terminal-lifetime"
  , recoveryCase
      detachedBridgeRecoverySweep
      "detached_bridge_child_completed_to_completed"
      "running"
      "completed"
      "deadline-plumbing-audit-2026-05-12-subagent-bridge-terminal-lifetime"
  , recoveryCase
      detachedBridgeRecoverySweep
      "detached_bridge_child_failed_to_failed"
      "running"
      "failed"
      "deadline-plumbing-audit-2026-05-12-subagent-bridge-terminal-lifetime"
  , recoveryCase
      detachedBridgeRecoverySweep
      "detached_bridge_child_interrupted_to_cancelled"
      "running"
      "cancelled"
      "deadline-plumbing-audit-2026-05-12-subagent-bridge-terminal-lifetime"
  , recoveryCase
      detachedBridgeRecoverySweep
      "detached_bridge_terminal_parent_to_failed"
      "running"
      "failed"
      "deadline-plumbing-audit-2026-05-12-subagent-bridge-terminal-lifetime"
  , recoveryCase
      detachedBridgeRecoverySweep
      "detached_bridge_deadline_exceeded_to_timed_out"
      "running"
      "timedOut"
      "deadline-plumbing-audit-2026-05-12-subagent-bridge-terminal-lifetime"
  , recoveryCase
      toolCallRecoverySweep
      "tool_running_child_dead_to_failed"
      "running"
      "failed"
      "gents-465-subagent-liveness"
  , recoveryCase
      expiredSubagentChildSweep
      "expired_processing_child_to_dead"
      "processing"
      "dead"
      "gents-465-subagent-liveness"
  , recoveryCase
      expiredSubagentChildSweep
      "expired_claimed_child_to_dead"
      "claimed"
      "dead"
      "gents-465-subagent-liveness"
  , recoveryCase
      queuedDescendantSweep
      "queued_descendant_terminal_parent_to_interrupted"
      "pending"
      "interrupted"
      "gents-465-subagent-liveness"
  , recoveryCase
      inferenceCallRecoverySweep
      "inference_queued_stale_to_cancelled"
      "queued"
      "cancelled"
      "deadline-plumbing-audit-2026-05-12-follow-up-6-pr-e"
  , recoveryCase
      inferenceCallRecoverySweep
      "inference_running_stale_to_failed"
      "running"
      "failed"
      "deadline-plumbing-audit-2026-05-12-follow-up-6-pr-e"
  , recoveryCase
      inferenceCallRecoverySweep
      "inference_interrupted_parent_to_cancelled"
      "running"
      "cancelled"
      "deadline-plumbing-audit-2026-05-12-follow-up-6-pr-e"
  ]

def recoveryEquivalenceTheorem (sweepId : String) : String :=
  if sweepId = requestRecoverySweep.sweepId then
    "Recovery.requestRecover_matches_uninterrupted"
  else if sweepId = responseRecoverySweep.sweepId then
    "Recovery.responseRecover_matches_uninterrupted"
  else if sweepId = toolCallRecoverySweep.sweepId then
    "Recovery.toolCallRecover_matches_uninterrupted"
  else if sweepId = orphanedBackgroundToolSweep.sweepId then
    "Recovery.orphanedBackgroundToolRecover_matches_uninterrupted"
  else if sweepId = backgroundCompletionSideEffectSweep.sweepId then
    "Recovery.backgroundCompletionSideEffectRecover_matches_uninterrupted"
  else if sweepId = terminalParentOwnedToolSweep.sweepId then
    "Recovery.terminalParentToolRecover_matches_uninterrupted"
  else if sweepId = detachedBridgeRecoverySweep.sweepId then
    "Recovery.detachedBridgeRecover_matches_uninterrupted"
  else if sweepId = inferenceCallRecoverySweep.sweepId then
    "Recovery.inferenceCallRecover_matches_uninterrupted"
  else if sweepId = expiredSubagentChildSweep.sweepId then
    "Recovery.expiredChildRecover_matches_uninterrupted"
  else if sweepId = queuedDescendantSweep.sweepId then
    "Recovery.queuedDescendantRecover_matches_uninterrupted"
  else
    "unregistered_recovery_equivalence"

def recoveryEquivalenceCase
    (witness : RecoverySweepCase) : RecoveryEquivalenceCase :=
  { name := witness.name ++ "_same_as_uninterrupted"
  , sourceSweepCase := witness.name
  , sweepId := witness.sweepId
  , collection := witness.collection
  , rustFunction := witness.rustFunction
  , cadence := witness.cadence
  , preState := witness.preState
  , recoveredState := witness.terminalState
  , uninterruptedState := witness.terminalState
  , equivalent := true
  , reexecutes := false
  , canHang := false
  , theoremName := recoveryEquivalenceTheorem witness.sweepId
  , aggregateTheoremName :=
      "Recovery.RecoveryEquivalence.finite_stale_rows_converge_to_uninterrupted"
  }

def recoveryEquivalenceCases : List RecoveryEquivalenceCase :=
  recoverySweepCases.map recoveryEquivalenceCase

/-! ## Restart disposition witnesses (#937)

Finite rows for the startup classifier in
`recover_stuck_running_tool_calls`. Unlike `recoverySweepCases`, the
`disposition`/`cause`/`terminalState`/notification fields are **computed from
`Recovery.restartDisposition`**, so these rows cannot drift from the model:
changing a classifier branch changes the emitted JSON and fails the Rust
consumer. The leave-running rows are the previously inexpressible outcomes —
background subagent bridges and detached/clean-complete bridges that startup
recovery must preserve. -/

def restartDispositionCase
    (name : String)
    (awaitMode : Subagent.AwaitMode)
    (cancelPolicy : Subagent.CancelPolicy)
    (childLinked : Bool)
    (parent : ParentObservation)
    (theoremName : String)
    (deadlineExpired : Bool := false)
    (unclaimedExpired : Bool := false) : RestartDispositionCase :=
  let row : RestartRow :=
    { awaitMode := awaitMode
    , cancelPolicy := cancelPolicy
    , childLinked := childLinked
    , parent := parent
    , deadlineExpired := deadlineExpired
    , unclaimedExpired := unclaimedExpired
    }
  let disposition := restartDisposition row
  { name := name
  , rustFunction := "ToolCallLifecycle::recover_all"
  , awaitMode := awaitMode.toDefraDB
  , cancelPolicy := cancelPolicy.toDefraDB
  , childLinked := childLinked
  , parentObservation := parent.toContract
  , deadlineExpired := deadlineExpired
  , unclaimedExpired := unclaimedExpired
  , disposition := disposition.toContract
  , cause := disposition.causeContract
  , terminalState := disposition.terminalStateContract
  , notificationReason :=
      row.notification.map RestartNotificationObligation.notificationReason
  , queueSource :=
      row.notification.map RestartNotificationObligation.queueSource
  , queueKeyPrefix :=
      row.notification.map RestartNotificationObligation.queueKeyPrefix
  , theoremName := theoremName
  }

def restartDispositionCases : List RestartDispositionCase :=
  [ restartDispositionCase
      "restart_native_background_live_parent_interrupted"
      .background .cascade false .live
      "Recovery.native_background_tool_live_parent_interrupted_on_restart"
  , restartDispositionCase
      "restart_background_subagent_live_parent_left_running"
      .background .cascade true .live
      "Recovery.background_subagent_bridge_live_parent_left_running"
  , restartDispositionCase
      "restart_detached_bridge_interrupted_parent_left_running"
      .background .detach true .interrupted
      "Recovery.detached_bridge_interrupted_parent_left_running"
  , restartDispositionCase
      "restart_clean_complete_child_linked_left_running"
      .background .cascade true .cleanlyCompleted
      "Recovery.clean_completion_child_linked_left_running"
  , restartDispositionCase
      "restart_native_background_deadline_expired_times_out"
      .background .cascade false .live
      "Recovery.deadline_precedes_restart_interrupt"
      (deadlineExpired := true)
  , restartDispositionCase
      "restart_native_background_interrupted_parent_cancelled"
      .background .cascade false .interrupted
      "Recovery.notification_iff_terminalized_native_background"
  , restartDispositionCase
      "restart_native_background_terminal_parent_failed"
      .background .cascade false .otherTerminal
      "Recovery.notification_iff_terminalized_native_background"
  , restartDispositionCase
      "restart_foreground_live_parent_left_running"
      .foreground .cascade false .live
      "Recovery.leave_running_iff_preserved_shapes"
  , restartDispositionCase
      "restart_subagent_missing_parent_left_running"
      .background .cascade true .missing
      "Recovery.leave_running_iff_preserved_shapes"
  , -- Unclaimed cross-deployment spawn expiry outranks every leave-running
    -- exemption: an unclaimed bridge under a live parent still fails.
    restartDispositionCase
      "restart_unclaimed_spawn_expired_fails"
      .background .cascade true .live
      "Recovery.unclaimed_precedes_leave_running_exemptions"
      (unclaimedExpired := true)
  ]

/-- The witness family covers both dispositions and pins the expected split:
    five leave-running rows (background subagent + live parent, detached +
    interrupted parent, clean-complete + child-linked, foreground + live
    parent, missing parent), five terminalize rows. -/
theorem restartDispositionCases_cover_both_dispositions :
    (restartDispositionCases.filter
        (fun witness => witness.disposition = "leave_running")).length = 5 ∧
      (restartDispositionCases.filter
        (fun witness => witness.disposition = "terminalize")).length = 5 := by
  native_decide

/-- Every terminal native background witness with a resolvable parent owes a
    completion notification and coalesced wake. -/
theorem restartDispositionCases_notifications_pinned :
    (restartDispositionCases.filter
        (fun witness => witness.notificationReason.isSome)).map
        (fun witness =>
          (witness.name, witness.notificationReason, witness.queueSource,
            witness.queueKeyPrefix)) =
      [ ("restart_native_background_live_parent_interrupted"
        , some "interrupted_on_restart"
        , some "background_completion"
        , some "background_completion:"
        )
      , ("restart_native_background_deadline_expired_times_out"
        , some "deadline_exceeded"
        , some "background_completion"
        , some "background_completion:"
        )
      , ("restart_native_background_interrupted_parent_cancelled"
        , some "parent_interrupted"
        , some "background_completion"
        , some "background_completion:"
        )
      , ("restart_native_background_terminal_parent_failed"
        , some "parent_terminal"
        , some "background_completion"
        , some "background_completion:"
        ) ] := by
  native_decide

/-- Leave-running rows carry no cause and no terminal state — the row is
    preserved verbatim. -/
theorem restartDispositionCases_leave_running_rows_carry_no_terminal :
    ∀ witness ∈ restartDispositionCases,
      witness.disposition = "leave_running" →
        witness.cause = none ∧ witness.terminalState = none := by
  native_decide

theorem restartDispositionCases_all_recover_all :
    ∀ witness ∈ restartDispositionCases,
      witness.rustFunction = "ToolCallLifecycle::recover_all" := by
  native_decide

theorem recoverySweepCases_registered_sweeps :
    ∀ witness : RecoverySweepCase,
      witness ∈ recoverySweepCases →
      (witness.sweepId, witness.collection) ∈ registeredRecoverySweepContracts := by
  native_decide

theorem recoverySweepCases_decrease_to_zero :
    ∀ witness,
      witness ∈ recoverySweepCases →
      witness.measureBefore > witness.measureAfter ∧ witness.measureAfter = 0 := by
  native_decide

theorem recoveryEquivalenceCases_cover_recoverySweepCases :
    recoveryEquivalenceCases.length = recoverySweepCases.length := by
  native_decide

theorem recoveryEquivalenceCases_same_as_uninterrupted :
    ∀ witness,
      witness ∈ recoveryEquivalenceCases →
      witness.recoveredState = witness.uninterruptedState ∧
      witness.equivalent = true ∧
      witness.reexecutes = false ∧
      witness.canHang = false ∧
      witness.theoremName ≠ "unregistered_recovery_equivalence" := by
  native_decide

end Recovery
