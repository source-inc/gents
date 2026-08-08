import Proofs.Conformance.ContractCases.Types
import Proofs.ResponseOutcome

namespace Conformance.ContractCases

open ResponseOutcome

structure ResponseOutcomeCase where
  name : String
  kind : String
  hasFinalMessage : Bool
  finalMessageRole : Option String
  requestDocId : Nat
  requestCid : Nat
  finalMessageDocId : Option Nat
  finalMessageCid : Option Nat
  finalMessageSignerDid : Option Nat
  visibleSiblingCount : Nat
  publishOutcome : String
  resultingFactCount : Nat
  deriving Repr

structure ResponsePersistenceCutCase where
  name : String
  preCut : String
  postCut : String
  requestTerminal : Bool
  liveStage : String
  outcomeCount : Nat
  deriving Repr

structure ResponseRecoveryCutCase where
  name : String
  responsePresent : Bool
  sourceCid : Nat
  claimCid : Nat
  claimParentCid : Nat
  provenanceReconstructed : Bool
  publishOutcome : String
  terminalizedAtSource : String
  requestTerminal : Bool
  outcomeCount : Nat
  deriving Repr

private def signed (docId cid signerDid : Nat) :
    RenderedCapture.SignedDocumentVersionRef :=
  { version := { docId := docId, compositeCommitCid := cid }
  , signerDid := signerDid }

private def requestA := signed 100 11 7
private def claimA := signed 100 12 7
private def requestANewer := signed 100 13 7
private def messageVersion := signed 200 21 7
private def otherMessageVersion := signed 201 22 7
private def outcomeVersion := signed 300 31 7
private def otherOutcomeVersion := signed 301 32 7

private def provenanceA : ExecutionProvenance :=
  { source := requestA, claim := claimA }

private def assistantMessage : MessageEvidence :=
  { version := messageVersion
  , request := claimA
  , sessionId := 9
  , role := .assistant }

private def otherAssistantMessage : MessageEvidence :=
  { version := otherMessageVersion
  , request := claimA
  , sessionId := 9
  , role := .assistant }

private def userMessage : MessageEvidence :=
  { assistantMessage with role := .user }

private def completeFact : OutcomeFact :=
  { version := outcomeVersion
  , provenance := provenanceA
  , request := provenanceA.claim
  , kind := .complete
  , finalMessage := some assistantMessage
  , reasonCode := none
  , terminalizedAt := 41 }

private def conflictingCompleteFact : OutcomeFact :=
  { completeFact with version := otherOutcomeVersion
                    , finalMessage := some otherAssistantMessage }

private def missingMessageComplete : OutcomeFact :=
  { completeFact with version := otherOutcomeVersion, finalMessage := none }

private def userMessageComplete : OutcomeFact :=
  { completeFact with version := otherOutcomeVersion
                    , finalMessage := some userMessage }

private def errorFact : OutcomeFact :=
  { version := otherOutcomeVersion
  , provenance := provenanceA
  , request := provenanceA.claim
  , kind := .error
  , finalMessage := none
  , reasonCode := some 51
  , terminalizedAt := 42 }

private def interruptedFact : OutcomeFact :=
  { errorFact with kind := .interrupted
                   , finalMessage := some assistantMessage
                   , reasonCode := some 52 }

private def reboundRequestFact : OutcomeFact :=
  { completeFact with version := otherOutcomeVersion
                    , provenance := { provenanceA with claim := requestANewer }
                    , request := requestANewer }

private def roleName : MessageRole → String
  | .assistant => "assistant"
  | .user => "user"

private def caseOf (name : String) (store : OutcomeStore)
    (fact : OutcomeFact) : ResponseOutcomeCase :=
  let result := publish store fact
  { name := name
  , kind := fact.kind.toContract
  , hasFinalMessage := fact.finalMessage.isSome
  , finalMessageRole := fact.finalMessage.map (fun message => roleName message.role)
  , requestDocId := fact.request.version.docId
  , requestCid := fact.request.version.compositeCommitCid
  , finalMessageDocId := fact.finalMessage.map (fun message => message.version.version.docId)
  , finalMessageCid := fact.finalMessage.map
      (fun message => message.version.version.compositeCommitCid)
  , finalMessageSignerDid := fact.finalMessage.map (fun message => message.version.signerDid)
  , visibleSiblingCount := (factsForRequestDoc store fact.request.version.docId).length
  , publishOutcome := result.1.toContract
  , resultingFactCount := result.2.length
  }

def responseOutcomeCases : List ResponseOutcomeCase :=
  [ caseOf "complete_exact_message_fresh" [] completeFact
  , caseOf "complete_identical_replay_idempotent" [completeFact] completeFact
  , caseOf "complete_different_message_conflict" [completeFact] conflictingCompleteFact
  , caseOf "complete_missing_message_rejected" [] missingMessageComplete
  , caseOf "complete_user_message_rejected" [] userMessageComplete
  , caseOf "error_without_message_fresh" [] errorFact
  , caseOf "interrupted_with_partial_message_fresh" [] interruptedFact
  , caseOf "same_request_doc_different_version_conflict" [completeFact] reboundRequestFact
  , caseOf "visible_sibling_set_rejected" [completeFact, errorFact] completeFact
  ]

private def cutName : PersistenceCut → String
  | .claimDurable => "claim_durable"
  | .streaming => "streaming"
  | .messageDurable => "message_durable"
  | .outcomeDurable => "outcome_durable"
  | .requestTerminal => "request_terminal"
  | .liveSuperseded => "live_superseded"

private def liveStageName : LiveStage → String
  | .active => "active"
  | .superseded => "superseded"
  | .expired => "expired"

private def live : LiveProjection :=
  { docId := 400
  , request := claimA
  , sessionId := 9
  , stage := .active
  , revision := 0
  , tailPresent := true
  , materializedMessage := none }

private def streaming : Machine :=
  { live := live
  , responsePresent := true
  , outcomes := []
  , requestTerminal := false
  , cut := .streaming }

private def messageDurable : Machine :=
  { streaming with
    live := { live with revision := 1, materializedMessage := some assistantMessage }
  , cut := .messageDurable }

private def outcomeDurable : Machine :=
  { messageDurable with outcomes := [completeFact], cut := .outcomeDurable }

private def requestTerminal : Machine :=
  { outcomeDurable with requestTerminal := true, cut := .requestTerminal }

private def liveSuperseded : Machine :=
  { requestTerminal with
    live := { requestTerminal.live with stage := .superseded }
  , cut := .liveSuperseded }

private theorem completePublishFresh :
    publish [] completeFact = (.fresh, [completeFact]) := by
  native_decide

private theorem messageCutReachable : Step streaming messageDurable := by
  exact Step.bindMessage assistantMessage rfl (by native_decide) rfl

private theorem outcomeCutReachable : Step messageDurable outcomeDurable := by
  exact Step.publishComplete completeFact rfl rfl rfl rfl completePublishFresh rfl

private theorem requestCutReachable : Step outcomeDurable requestTerminal := by
  exact Step.terminalizeRequest rfl rfl rfl

private theorem supersededCutReachable : Step requestTerminal liveSuperseded := by
  exact Step.supersedeLive rfl rfl rfl

private def cutCase (name : String) (pre post : Machine) : ResponsePersistenceCutCase :=
  { name := name
  , preCut := cutName pre.cut
  , postCut := cutName post.cut
  , requestTerminal := post.requestTerminal
  , liveStage := liveStageName post.live.stage
  , outcomeCount := post.outcomes.length }

def responsePersistenceCutCases : List ResponsePersistenceCutCase :=
  [ cutCase "message_before_outcome" streaming messageDurable
  , cutCase "outcome_before_request_terminal" messageDurable outcomeDurable
  , cutCase "request_terminal_after_outcome" outcomeDurable requestTerminal
  , cutCase "live_superseded_after_request_terminal" requestTerminal liveSuperseded
  ]

private def exactClaimEvidence : ClaimCommitEvidence :=
  { source := provenanceA.source
  , claim := provenanceA.claim
  , expectedSourceSignerDid := provenanceA.source.signerDid
  , targetAgentDid := provenanceA.claim.signerDid
  , claimParents := [provenanceA.source.version.compositeCommitCid]
  , sourcePending := true
  , claimClaimed := true
  , payloadPreserved := true }

private def wrongParentEvidence : ClaimCommitEvidence :=
  { exactClaimEvidence with claimParents := [999] }

private def claimDurableMissingResponse : Machine :=
  { streaming with responsePresent := false, cut := .claimDurable }

private def recoveredFailure : Machine :=
  { claimDurableMissingResponse with
      outcomes := [errorFact]
    , cut := .outcomeDurable }

private def recoveredRequestTerminal : Machine :=
  { recoveredFailure with requestTerminal := true, cut := .requestTerminal }

private theorem exactClaimReconstructs :
    reconstructExecutionProvenance exactClaimEvidence = some provenanceA := by
  native_decide

private theorem recoveryFailureFresh :
    publish [] errorFact = (.fresh, [errorFact]) := by
  native_decide

private theorem missingResponseRecoveryReachable :
    Step claimDurableMissingResponse recoveredFailure := by
  exact Step.recoverMissingResponse exactClaimEvidence provenanceA errorFact
    rfl rfl exactClaimReconstructs rfl rfl (Or.inl rfl) recoveryFailureFresh rfl

private theorem missingResponseRecoveryTerminalizes :
    Step recoveredFailure recoveredRequestTerminal := by
  exact Step.terminalizeRequest rfl rfl rfl

private theorem missingResponseRecoveryReplayIdempotent :
    publish recoveredFailure.outcomes errorFact =
      (.idempotent, recoveredFailure.outcomes) := by
  exact recovery_outcome_retry_idempotent errorFact (by native_decide)

private def recoveryCase
    (name : String)
    (evidence : ClaimCommitEvidence)
    (store : OutcomeStore)
    (requestTerminal : Bool) : ResponseRecoveryCutCase :=
  let reconstructed := reconstructExecutionProvenance evidence
  let result := reconstructed.map
    (fun _ => (publish store errorFact).1.toContract) |>.getD "rejected"
  { name := name
  , responsePresent := false
  , sourceCid := evidence.source.version.compositeCommitCid
  , claimCid := evidence.claim.version.compositeCommitCid
  , claimParentCid := evidence.claimParents.head?.getD 0
  , provenanceReconstructed := reconstructed.isSome
  , publishOutcome := result
  , terminalizedAtSource :=
      if result = "fresh" then "recovery_decision"
      else if result = "idempotent" then "persisted_outcome"
      else "none"
  , requestTerminal := requestTerminal
  , outcomeCount :=
      if reconstructed.isSome then (publish store errorFact).2.length else store.length }

def responseRecoveryCutCases : List ResponseRecoveryCutCase :=
  [ recoveryCase
      "missing_response_exact_claim_publishes_failure"
      exactClaimEvidence [] false
  , recoveryCase
      "missing_response_identical_retry_is_idempotent"
      exactClaimEvidence [errorFact] false
  , recoveryCase
      "missing_response_wrong_claim_parent_rejected"
      wrongParentEvidence [] false
  , recoveryCase
      "missing_response_terminalizes_only_after_outcome"
      exactClaimEvidence [errorFact] true
  ]

end Conformance.ContractCases
