import Proofs.Conformance.ContractCases.Types
import Proofs.ToolFact

/-! Generated exact durable tool-fact witnesses for #1073. -/

namespace Conformance.ContractCases

open ToolFact

private abbrev VersionRef := RenderedCapture.DocumentVersionRef

structure ToolFactCase where
  name : String
  operation : String
  disposition : String
  visibleLogicalTwins : Nat
  fullOutput : Bool
  callDocId : Nat
  callCid : Nat
  callSignerDid : Nat
  resultDocId : Nat
  resultCid : Nat
  resultSignerDid : Nat
  approvalDocId : Option Nat
  approvalCid : Option Nat
  approvalSignerDid : Option Nat
  resultDurable : Bool
  approvalDurable : Bool
  resultPinsExactCall : Bool
  approvalPinsExactCall : Bool
  exactProjection : Bool
  immutableNoop : Bool
  deriving Repr

private def callVersion : VersionRef :=
  { docId := 100, compositeCommitCid := 10 }

private def callRef : SignedRef :=
  { version := callVersion, signerDid := 7, signatureValid := true }

private def callIntent : ToolCallIntent :=
  { key := 1, argsHash := 101 }

private def callApplied : CommitObservation :=
  commitCall State.empty [] callIntent callRef

private def base : State := callApplied.state

private def callReplay : CommitObservation :=
  commitCall base [callVersion.docId] callIntent callRef

private def callTwin : CommitObservation :=
  commitCall State.empty [callVersion.docId, 101] callIntent callRef

private def unsignedCallRef : SignedRef :=
  { callRef with signatureValid := false }

private def unsignedCall : CommitObservation :=
  commitCall State.empty [] callIntent unsignedCallRef

private def resultVersion : VersionRef :=
  { docId := 200, compositeCommitCid := 20 }

private def resultRef : SignedRef :=
  { version := resultVersion, signerDid := 8, signatureValid := true }

private def resultIntent : ToolResultIntent :=
  { key := 2, call := callRef, outputHash := 202, fullOutput := true }

private def resultApplied : CommitObservation :=
  commitResult base [] resultIntent resultRef

private def withResult : State := resultApplied.state

private def resultReplay : CommitObservation :=
  commitResult withResult [resultVersion.docId] resultIntent resultRef

private def conflictIntent : ToolResultIntent :=
  { resultIntent with outputHash := 999 }

private def resultConflict : CommitObservation :=
  commitResult withResult [resultVersion.docId] conflictIntent resultRef

private def resultTwin : CommitObservation :=
  commitResult base [resultVersion.docId, 201] resultIntent resultRef

private def incompleteIntent : ToolResultIntent :=
  { resultIntent with fullOutput := false }

private def incompleteResult : CommitObservation :=
  commitResult base [] incompleteIntent resultRef

private def unsignedResultRef : SignedRef :=
  { resultRef with signatureValid := false }

private def unsignedResult : CommitObservation :=
  commitResult base [] resultIntent unsignedResultRef

private def approvalVersion : VersionRef :=
  { docId := 300, compositeCommitCid := 30 }

private def approvalRef : SignedRef :=
  { version := approvalVersion, signerDid := 9, signatureValid := true }

private def approvalIntent : ToolApprovalIntent :=
  { key := 3, call := callRef, decision := .approved, reasonHash := 303 }

private def approvalApplied : CommitObservation :=
  commitApproval withResult [] approvalIntent approvalRef

private def withApproval : State := approvalApplied.state

private def approvalReplay : CommitObservation :=
  commitApproval withApproval [approvalVersion.docId] approvalIntent approvalRef

private def approvalTwin : CommitObservation :=
  commitApproval withResult [approvalVersion.docId, 301] approvalIntent approvalRef

private def exactJoin : TranscriptJoin :=
  { call := callRef, result := resultRef, approval := some approvalRef }

private def wrongResultRef : SignedRef :=
  { resultRef with version := { resultVersion with compositeCommitCid := 21 } }

private def wrongResultJoin : TranscriptJoin :=
  { exactJoin with result := wrongResultRef }

private def wrongSignerJoin : TranscriptJoin :=
  { exactJoin with call := { callRef with signerDid := 77 } }

private def resultDurable (state : State) : Bool :=
  (exactResult? state.results resultRef).isSome

private def approvalDurable (state : State) : Bool :=
  (exactApproval? state.approvals approvalRef).isSome

private def resultPinsCall (state : State) : Bool :=
  match exactResult? state.results resultRef with
  | some fact => fact.call == callRef
  | none => false

private def approvalPinsCall (state : State) : Bool :=
  match exactApproval? state.approvals approvalRef with
  | some fact => fact.call == callRef
  | none => false

private def optionalVersionDocId : Option SignedRef → Option Nat
  | some ref => some ref.version.docId
  | none => none

private def optionalVersionCid : Option SignedRef → Option Nat
  | some ref => some ref.version.compositeCommitCid
  | none => none

private def optionalSigner : Option SignedRef → Option Nat
  | some ref => some ref.signerDid
  | none => none

private def caseOf
    (name operation : String)
    (disposition : Option CommitDisposition)
    (visibleLogicalTwins : Nat)
    (fullOutput : Bool)
    (state : State)
    (join : TranscriptJoin)
    (immutableNoop : Bool) : ToolFactCase :=
  { name := name
  , operation := operation
  , disposition := disposition.map CommitDisposition.toContract |>.getD "not_attempted"
  , visibleLogicalTwins := visibleLogicalTwins
  , fullOutput := fullOutput
  , callDocId := join.call.version.docId
  , callCid := join.call.version.compositeCommitCid
  , callSignerDid := join.call.signerDid
  , resultDocId := join.result.version.docId
  , resultCid := join.result.version.compositeCommitCid
  , resultSignerDid := join.result.signerDid
  , approvalDocId := optionalVersionDocId join.approval
  , approvalCid := optionalVersionCid join.approval
  , approvalSignerDid := optionalSigner join.approval
  , resultDurable := resultDurable state
  , approvalDurable := approvalDurable state
  , resultPinsExactCall := resultPinsCall state
  , approvalPinsExactCall := approvalPinsCall state
  , exactProjection := (projectExact state join).isSome
  , immutableNoop := immutableNoop }

def toolFactCases : List ToolFactCase :=
  [ caseOf "call_fact_applied" "commit_call" (some callApplied.disposition)
      0 true callApplied.state { exactJoin with approval := none } false
  , caseOf "identical_call_replay_is_idempotent" "commit_call"
      (some callReplay.disposition) 1 true callReplay.state
      { exactJoin with approval := none } true
  , caseOf "logical_key_call_twins_fail_closed" "commit_call"
      (some callTwin.disposition) 2 true callTwin.state
      { exactJoin with approval := none } true
  , caseOf "unsigned_call_is_rejected" "commit_call"
      (some unsignedCall.disposition) 0 true unsignedCall.state
      { exactJoin with approval := none } true
  , caseOf "full_result_applied" "commit_result" (some resultApplied.disposition)
      0 true withResult { exactJoin with approval := none } false
  , caseOf "identical_result_replay_is_idempotent" "commit_result"
      (some resultReplay.disposition) 1 true resultReplay.state
      { exactJoin with approval := none } true
  , caseOf "mismatched_result_replay_is_rejected" "commit_result"
      (some resultConflict.disposition) 1 true resultConflict.state
      { exactJoin with approval := none } true
  , caseOf "logical_key_result_twins_fail_closed" "commit_result"
      (some resultTwin.disposition) 2 true resultTwin.state
      { exactJoin with approval := none } true
  , caseOf "incomplete_output_is_rejected" "commit_result"
      (some incompleteResult.disposition) 0 false incompleteResult.state
      { exactJoin with approval := none } true
  , caseOf "unsigned_result_is_rejected" "commit_result"
      (some unsignedResult.disposition) 0 true unsignedResult.state
      { exactJoin with approval := none } true
  , caseOf "approval_applied_with_exact_call" "commit_approval"
      (some approvalApplied.disposition) 0 true withApproval exactJoin false
  , caseOf "identical_approval_replay_is_idempotent" "commit_approval"
      (some approvalReplay.disposition) 1 true approvalReplay.state exactJoin true
  , caseOf "logical_key_approval_twins_fail_closed" "commit_approval"
      (some approvalTwin.disposition) 2 true approvalTwin.state exactJoin true
  , caseOf "wrong_result_cid_projection_is_rejected" "project" none
      1 true withApproval wrongResultJoin true
  , caseOf "wrong_call_signer_projection_is_rejected" "project" none
      1 true withApproval wrongSignerJoin true
  ]

theorem toolFactCases_pinned :
    toolFactCases.map (fun row =>
      (row.name, row.disposition, row.resultDurable, row.approvalDurable,
        row.exactProjection, row.immutableNoop)) =
      [ ("call_fact_applied", "applied", false, false, false, false)
      , ("identical_call_replay_is_idempotent", "observed_identical", false, false, false, true)
      , ("logical_key_call_twins_fail_closed", "rejected", false, false, false, true)
      , ("unsigned_call_is_rejected", "rejected", false, false, false, true)
      , ("full_result_applied", "applied", true, false, true, false)
      , ("identical_result_replay_is_idempotent", "observed_identical", true, false, true, true)
      , ("mismatched_result_replay_is_rejected", "rejected", true, false, true, true)
      , ("logical_key_result_twins_fail_closed", "rejected", false, false, false, true)
      , ("incomplete_output_is_rejected", "rejected", false, false, false, true)
      , ("unsigned_result_is_rejected", "rejected", false, false, false, true)
      , ("approval_applied_with_exact_call", "applied", true, true, true, false)
      , ("identical_approval_replay_is_idempotent", "observed_identical", true, true, true, true)
      , ("logical_key_approval_twins_fail_closed", "rejected", true, false, false, true)
      , ("wrong_result_cid_projection_is_rejected", "not_attempted", true, true, false, true)
      , ("wrong_call_signer_projection_is_rejected", "not_attempted", true, true, false, true)
      ] := by
  native_decide

end Conformance.ContractCases
