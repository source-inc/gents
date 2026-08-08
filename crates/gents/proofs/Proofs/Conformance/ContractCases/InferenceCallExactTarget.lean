import Proofs.Conformance.ContractCases.Types
import Proofs.InferenceCall.ExactTarget

namespace Conformance.ContractCases

open InferenceCall

def exactTargetDocumentId : DocumentId := 101
def exactTargetSiblingDocumentId : DocumentId := 202
def exactTargetMissingDocumentId : DocumentId := 303
def exactTargetOwner : OwnerToken := 17
def exactTargetEpoch : Epoch := 3

def exactTargetCall (state : InferenceCallState) : InferenceCall :=
  { callId := 7
  , requestId := 11
  , backend := contractBackend
  , state := state
  }

def exactTargetStore
    (targetPresent : Bool)
    (targetState siblingState : InferenceCallState) : Store :=
  fun docId =>
    if targetPresent && docId = exactTargetDocumentId then
      some ⟨exactTargetCall targetState, exactTargetOwner, exactTargetEpoch⟩
    else if docId = exactTargetSiblingDocumentId then
      some ⟨exactTargetCall siblingState, exactTargetOwner, exactTargetEpoch⟩
    else
      none

def inferenceCallActionName : Action → String
  | .start => "start"
  | .complete => "complete"
  | .fail => "fail"
  | .cancel => "cancel"

def inferenceCallRequestedState : Action → InferenceCallState
  | .start => .running
  | .complete => .completed
  | .fail => .failed
  | .cancel => .cancelled

def stateAt (store : Store) (docId : DocumentId) : Option String :=
  (store docId).map fun row => row.call.state.toDefraDB

def inferenceCallExactTargetCase
    (name : String)
    (targetPresent : Bool)
    (targetState siblingState expectedState : InferenceCallState)
    (action : Action)
    (expectedOwner : OwnerToken := exactTargetOwner)
    (expectedEpoch : Epoch := exactTargetEpoch) : InferenceCallExactTargetCase :=
  let pre := exactTargetStore targetPresent targetState siblingState
  let writeTarget := if targetPresent then exactTargetDocumentId else exactTargetMissingDocumentId
  let write : FencedUpdate :=
    { target := writeTarget
    , expectedState := expectedState
    , expectedOwner := expectedOwner
    , expectedEpoch := expectedEpoch
    , action := action
    }
  let result := applyFenced pre write
  let post := result.getD pre
  let terminalPreState := decide (isTerminal targetState)
  { name := name
  , action := inferenceCallActionName action
  , writeTarget := if targetPresent then "target" else "missing"
  , targetPresent := targetPresent
  , expectedState := expectedState.toDefraDB
  , targetOwner := exactTargetOwner
  , targetEpoch := exactTargetEpoch
  , expectedOwner := expectedOwner
  , expectedEpoch := expectedEpoch
  , requestedPostState := (inferenceCallRequestedState action).toDefraDB
  , targetPreState := stateAt pre exactTargetDocumentId
  , targetPostState := stateAt post exactTargetDocumentId
  , siblingPreState := siblingState.toDefraDB
  , siblingPostState :=
      (stateAt post exactTargetSiblingDocumentId).getD "missing"
  , writeMatched := result.isSome
  , siblingIsolated :=
      decide (stateAt post exactTargetSiblingDocumentId =
        stateAt pre exactTargetSiblingDocumentId)
  , sameLogicalCallId := decide (
      (pre exactTargetDocumentId).map (fun row => row.call.callId) =
        (pre exactTargetSiblingDocumentId).map (fun row => row.call.callId))
  , terminalPreState := terminalPreState
  , terminalIrreversible := !terminalPreState || !result.isSome
  }

def inferenceCallExactTargetCases : List InferenceCallExactTargetCase :=
  [ inferenceCallExactTargetCase
      "queued_to_running_targets_exact_document"
      true .queued .queued .queued .start
  , inferenceCallExactTargetCase
      "queued_to_cancelled_targets_exact_document"
      true .queued .running .queued .cancel
  , inferenceCallExactTargetCase
      "running_to_completed_targets_exact_document"
      true .running .queued .running .complete
  , inferenceCallExactTargetCase
      "running_to_failed_targets_exact_document"
      true .running .running .running .fail
  , inferenceCallExactTargetCase
      "running_to_cancelled_targets_exact_document"
      true .running .completed .running .cancel
  , inferenceCallExactTargetCase
      "stale_expected_state_rejects_write"
      true .queued .running .running .complete
  , inferenceCallExactTargetCase
      "missing_physical_document_rejects_write"
      false .queued .running .queued .start
  , inferenceCallExactTargetCase
      "stale_owner_rejects_write"
      true .running .queued .running .complete (expectedOwner := 99)
  , inferenceCallExactTargetCase
      "stale_epoch_rejects_write"
      true .running .queued .running .complete (expectedEpoch := 99)
  , inferenceCallExactTargetCase
      "completed_cannot_reopen"
      true .completed .running .completed .start
  , inferenceCallExactTargetCase
      "completed_cannot_change_terminal_outcome"
      true .completed .failed .completed .fail
  , inferenceCallExactTargetCase
      "failed_cannot_change_terminal_outcome"
      true .failed .cancelled .failed .cancel
  ]

def fencedDispositionName : FencedDisposition → String
  | .applied => "applied"
  | .observedDesired => "observed_desired"
  | .rejected => "rejected"

def traceDocumentId : String → DocumentId
  | "target" => exactTargetDocumentId
  | "sibling" => exactTargetSiblingDocumentId
  | _ => exactTargetMissingDocumentId

def inferenceCallExactTargetTraceCase
    (name scenario : String)
    (targetPreState siblingPreState : InferenceCallState)
    (firstTarget : String)
    (firstExpectedState : InferenceCallState)
    (firstAction : Action)
    (secondTarget : String)
    (secondExpectedState : InferenceCallState)
    (secondAction : Action)
    (visibleTargets : List String := ["target"]) : InferenceCallExactTargetTraceCase :=
  let pre := exactTargetStore true targetPreState siblingPreState
  let visibleLogicalDocuments := visibleTargets.map traceDocumentId
  let firstWrite : FencedUpdate :=
    { target := traceDocumentId firstTarget
    , expectedState := firstExpectedState
    , expectedOwner := exactTargetOwner
    , expectedEpoch := exactTargetEpoch
    , action := firstAction
    }
  let firstResult := applyAdmitted pre visibleLogicalDocuments firstWrite
  let afterFirst := firstResult.getD pre
  let secondWrite : FencedUpdate :=
    { target := traceDocumentId secondTarget
    , expectedState := secondExpectedState
    , expectedOwner := exactTargetOwner
    , expectedEpoch := exactTargetEpoch
    , action := secondAction
    }
  let secondStrict := applyAdmitted afterFirst visibleLogicalDocuments secondWrite
  let secondObserved :=
    applyAdmittedOrObserve afterFirst visibleLogicalDocuments secondWrite
  -- Exact `_docID` CAS alone would allow both sibling rows to advance.  The
  -- visible logical conflict set is therefore a separate admission fence.
  let rawFirst := applyFenced pre firstWrite
  let rawAfterFirst := rawFirst.getD pre
  let rawSecond := applyFenced rawAfterFirst secondWrite
  let finalStore := secondObserved.store
  { name := name
  , scenario := scenario
  , targetPreState := targetPreState.toDefraDB
  , siblingPreState := siblingPreState.toDefraDB
  , visibleLogicalDocumentCount := visibleLogicalDocuments.length
  , uniqueAdmissionRequired := true
  , rawIndependentCasPossible := rawFirst.isSome && rawSecond.isSome
  , firstTarget := firstTarget
  , firstAction := inferenceCallActionName firstAction
  , firstExpectedState := firstExpectedState.toDefraDB
  , firstExpectedOwner := exactTargetOwner
  , firstExpectedEpoch := exactTargetEpoch
  , firstRequestedPostState := (inferenceCallRequestedState firstAction).toDefraDB
  , firstCasMatched := firstResult.isSome
  , secondTarget := secondTarget
  , secondAction := inferenceCallActionName secondAction
  , secondExpectedState := secondExpectedState.toDefraDB
  , secondExpectedOwner := exactTargetOwner
  , secondExpectedEpoch := exactTargetEpoch
  , secondRequestedPostState := (inferenceCallRequestedState secondAction).toDefraDB
  , secondCasMatched := secondStrict.isSome
  , secondDisposition := fencedDispositionName secondObserved.disposition
  , finalTargetState :=
      (stateAt finalStore exactTargetDocumentId).getD "missing"
  , finalSiblingState :=
      (stateAt finalStore exactTargetSiblingDocumentId).getD "missing"
  }

def identicalRetryTrace : InferenceCallExactTargetTraceCase :=
  inferenceCallExactTargetTraceCase
      "identical_retry_observes_desired_state"
      "strict_cas_then_idempotent_observation"
      .queued .queued "target" .queued .start "target" .queued .start

def duplicatePhysicalRowsTrace : InferenceCallExactTargetTraceCase :=
  inferenceCallExactTargetTraceCase
      "logical_conflict_rejects_both_admissions"
      "logical_conflict_rejects_admission"
      .queued .queued "target" .queued .start "sibling" .queued .start
      (visibleTargets := ["target", "sibling"])

def recoveryStaleFlipTrace : InferenceCallExactTargetTraceCase :=
  inferenceCallExactTargetTraceCase
      "recovery_stale_running_after_completion_rejects"
      "recovery_like_source_state_flip"
      .running .queued "target" .running .complete "target" .running .fail

def inferenceCallExactTargetTraceCases : List InferenceCallExactTargetTraceCase :=
  [ identicalRetryTrace
  , duplicatePhysicalRowsTrace
  , recoveryStaleFlipTrace
  ]

theorem exact_target_cases_isolate_every_sibling :
    inferenceCallExactTargetCases.all (fun witness => witness.siblingIsolated) = true := by
  native_decide

theorem exact_target_cases_never_rewrite_terminal_rows :
    inferenceCallExactTargetCases.all
      (fun witness => !witness.terminalPreState || witness.terminalIrreversible) = true := by
  native_decide

theorem identical_retry_is_strictly_rejected_then_observed :
    let witness := identicalRetryTrace
    witness.firstCasMatched = true ∧
    witness.secondCasMatched = false ∧
    witness.secondDisposition = "observed_desired" := by
  native_decide

theorem duplicate_physical_rows_require_logical_conflict_rejection :
    let witness := duplicatePhysicalRowsTrace
    witness.visibleLogicalDocumentCount = 2 ∧
    witness.uniqueAdmissionRequired = true ∧
    witness.rawIndependentCasPossible = true ∧
    witness.firstCasMatched = false ∧
    witness.secondCasMatched = false ∧
    witness.secondDisposition = "rejected" ∧
    witness.finalTargetState = "queued" ∧
    witness.finalSiblingState = "queued" := by
  native_decide

theorem recovery_stale_source_flip_is_rejected :
    let witness := recoveryStaleFlipTrace
    witness.firstCasMatched = true ∧
    witness.secondCasMatched = false ∧
    witness.secondDisposition = "rejected" ∧
    witness.finalTargetState = "completed" := by
  native_decide

end Conformance.ContractCases
