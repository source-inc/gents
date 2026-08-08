import Proofs.Conformance.ContractCases.Types
import Proofs.Transcript.Finalization

namespace Conformance.ContractCases

open Transcript
open Transcript.Finalization

def transcriptDraftDoc : DocumentId := 91
def transcriptFactDoc : DocumentId := 101
def transcriptSiblingFactDoc : DocumentId := 202
def transcriptSecondFactDoc : DocumentId := 303

def transcriptOrderOne : LogicalOrder := ⟨1, 1⟩
def transcriptOrderTwo : LogicalOrder := ⟨1, 2⟩

def transcriptPayload (hash : ContentHash) : Payload :=
  { role := .assistant
  , contentHash := hash
  }

def transcriptDraft (hash : Nat) : Draft :=
  { order := transcriptOrderOne
  , payload := transcriptPayload hash
  }

def transcriptFact
    (order : LogicalOrder)
    (hash cid : Nat)
    (signerDid : Nat := 7001)
    (signatureValid : Bool := true)
    (policyAuthorized : Bool := true) : FinalizedFact :=
  { order := order
  , payload := transcriptPayload hash
  , commitCid := cid
  , signerDid := signerDid
  , signatureValid := signatureValid
  , policyAuthorized := policyAuthorized
  }

def splitState
    (draft : Option Draft)
    (fact : Option FinalizedFact)
    (sibling : Option FinalizedFact :=
      some (transcriptFact transcriptOrderOne 99 2999)) : State :=
  { drafts := fun docId => if docId = transcriptDraftDoc then draft else none
  , facts := fun docId =>
      if docId = transcriptFactDoc then fact
      else if docId = transcriptSiblingFactDoc then sibling
      else none
  }

def draftHashAt (state : State) : Option Nat :=
  (state.drafts transcriptDraftDoc).map fun draft => draft.payload.contentHash

def factAt (state : State) : Option FinalizedFact :=
  state.facts transcriptFactDoc

def factCidAt (state : State) : Option Nat :=
  (factAt state).map FinalizedFact.commitCid

def factHashAt (state : State) : Option Nat :=
  (factAt state).map fun fact => fact.payload.contentHash

def factSignerDidAt (state : State) : Option Nat :=
  (factAt state).map FinalizedFact.signerDid

def transcriptCommitDispositionName : CommitDisposition → String
  | .applied => "applied"
  | .observedIdentical => "observed_identical"
  | .rejected => "rejected"

def finalizationWitness
    (name action : String)
    (visible : List DocumentId)
    (pre post : State)
    (writeHash : Nat)
    (evidence : Option PublishEvidence)
    (disposition : String) : TranscriptFinalizationCase :=
  { name := name
  , action := action
  , visibleLogicalFactCount := visible.length
  , checkpointPresent := (pre.drafts transcriptDraftDoc).isSome
  , factPresentBefore := (factAt pre).isSome
  , factPresentAfter := (factAt post).isSome
  , factCommitCid := factCidAt post
  , checkpointPayloadHash := draftHashAt pre
  , writePayloadHash := writeHash
  , factPayloadHash := factHashAt post
  , writeSignerDid := evidence.map PublishEvidence.signerDid
  , writeSignatureValid := evidence.map PublishEvidence.signatureValid
  , writePolicyAuthorized := evidence.map PublishEvidence.policyAuthorized
  , factSignerDid := factSignerDidAt post
  , disposition := disposition
  , checkpointPreserved :=
      decide (post.drafts transcriptDraftDoc = pre.drafts transcriptDraftDoc)
  , siblingIsolated :=
      decide (post.facts transcriptSiblingFactDoc = pre.facts transcriptSiblingFactDoc)
  }

def checkpointUpdateCase (name : String) (preDraft : Option Draft) (nextHash : Nat) :
    TranscriptFinalizationCase :=
  let pre := splitState preDraft none
  let write : DraftWrite :=
    { target := transcriptDraftDoc
    , nextPayload := transcriptPayload nextHash
    }
  let result := applyDraftUpdate pre write
  let post := result.getD pre
  finalizationWitness name "update_checkpoint" [] pre post nextHash none
    (if result.isSome then "applied" else "rejected")

def authorizedEvidence (cid signerDid : Nat := 7001) : PublishEvidence :=
  { resultCommitCid := cid
  , signerDid := signerDid
  , signatureValid := true
  , policyAuthorized := true
  }

def publishCase
    (name : String)
    (preDraft : Option Draft)
    (preFact : Option FinalizedFact)
    (visible : List DocumentId)
    (hash : Nat)
    (evidence : PublishEvidence) : TranscriptFinalizationCase :=
  let pre := splitState preDraft preFact
  let intent : PublishIntent :=
    { target := transcriptFactDoc
    , order := transcriptOrderOne
    , payload := transcriptPayload hash
    }
  let observation := publishOrObserve pre visible intent evidence
  finalizationWitness name "publish_final_fact" visible pre observation.state hash
    (some evidence) (transcriptCommitDispositionName observation.disposition)

def transcriptFinalizationCases : List TranscriptFinalizationCase :=
  [ checkpointUpdateCase
      "checkpoint_revision_is_mutable_and_non_authoritative"
      (some (transcriptDraft 10)) 11
  , publishCase
      "post_checkpoint_publish_creates_authoritative_fact"
      (some (transcriptDraft 10)) none [] 10 (authorizedEvidence 2001)
  , publishCase
      "checkpoint_payload_does_not_constrain_publication"
      (some (transcriptDraft 10)) none [] 11 (authorizedEvidence 2001)
  , publishCase
      "direct_publish_uses_same_authoritative_rule"
      none none [] 10 (authorizedEvidence 2001)
  , publishCase
      "identical_publish_replay_is_observation"
      none (some (transcriptFact transcriptOrderOne 10 2001))
      [transcriptFactDoc] 10 (authorizedEvidence 2002)
  , publishCase
      "conflicting_payload_replay_is_rejected"
      none (some (transcriptFact transcriptOrderOne 10 2001))
      [transcriptFactDoc] 77 (authorizedEvidence 2002)
  , publishCase
      "logical_twins_reject_even_if_unique_index_has_a_winner"
      (some (transcriptDraft 10)) none
      [transcriptFactDoc, transcriptSiblingFactDoc] 10 (authorizedEvidence 2001)
  , publishCase
      "missing_result_commit_cid_is_rejected"
      none none [] 10 (authorizedEvidence 0)
  , publishCase
      "empty_signer_evidence_is_rejected"
      none none [] 10 (authorizedEvidence 2001 0)
  , publishCase
      "invalid_final_signature_is_rejected"
      none none [] 10
      { resultCommitCid := 2001
      , signerDid := 7001
      , signatureValid := false
      , policyAuthorized := true
      }
  , publishCase
      "policy_unauthorized_signer_is_rejected"
      none none [] 10
      { resultCommitCid := 2001
      , signerDid := 7001
      , signatureValid := true
      , policyAuthorized := false
      }
  ]

def providerFactStore : FactStore :=
  fun docId =>
    if docId = transcriptFactDoc then
      some (transcriptFact transcriptOrderOne 10 2001)
    else if docId = transcriptSecondFactDoc then
      some (transcriptFact transcriptOrderTwo 20 2002)
    else if docId = transcriptSiblingFactDoc then
      some (transcriptFact transcriptOrderOne 99 2999)
    else
      none

def visibleProviderFacts
    (conflict : Bool) (order : LogicalOrder) : List DocumentId :=
  if order = transcriptOrderOne then
    if conflict then [transcriptFactDoc, transcriptSiblingFactDoc]
    else [transcriptFactDoc]
  else if order = transcriptOrderTwo then
    [transcriptSecondFactDoc]
  else
    []

def factsStrictlyIncreasing : List FinalizedFact → Bool
  | [] => true
  | [_] => true
  | first :: second :: rest =>
      first.order.sequence < second.order.sequence &&
        factsStrictlyIncreasing (second :: rest)

def providerHistoryCase
    (name : String)
    (facts : FactStore)
    (conflict : Bool)
    (sessionId : SessionId)
    (refs : List DocumentVersionRef) : TranscriptProviderHistoryCase :=
  let result := assembleProviderHistory facts (visibleProviderFacts conflict) sessionId refs
  let rows := result.getD []
  { name := name
  , referenceCount := refs.length
  , visibleConflictCount := if conflict then 2 else 1
  , accepted := result.isSome
  , outputCount := rows.length
  , outputPayloadHashes := rows.map fun fact => fact.payload.contentHash
  , exactFinalizedDomainOnly := true
  , strictlyIncreasing := factsStrictlyIncreasing rows
  }

def transcriptProviderHistoryCases : List TranscriptProviderHistoryCase :=
  [ providerHistoryCase
      "provider_accepts_one_finalized_exact_fact"
      providerFactStore false 1 [⟨transcriptFactDoc, 2001⟩]
  , providerHistoryCase
      "provider_accepts_two_exact_facts_in_order"
      providerFactStore false 1
      [⟨transcriptFactDoc, 2001⟩, ⟨transcriptSecondFactDoc, 2002⟩]
  , providerHistoryCase
      "provider_cannot_resolve_draft_document"
      providerFactStore false 1 [⟨transcriptDraftDoc, 1001⟩]
  , providerHistoryCase
      "provider_rejects_wrong_composite_cid"
      providerFactStore false 1 [⟨transcriptFactDoc, 9999⟩]
  , providerHistoryCase
      "provider_rejects_logical_twins_despite_canonical_index_winner"
      providerFactStore true 1 [⟨transcriptFactDoc, 2001⟩]
  , providerHistoryCase
      "provider_rejects_out_of_order_exact_facts"
      providerFactStore false 1
      [⟨transcriptSecondFactDoc, 2002⟩, ⟨transcriptFactDoc, 2001⟩]
  , providerHistoryCase
      "provider_rejects_cross_session_fact"
      providerFactStore false 2 [⟨transcriptFactDoc, 2001⟩]
  ]

theorem finalization_cases_isolate_siblings :
    transcriptFinalizationCases.all (fun witness => witness.siblingIsolated) = true := by
  native_decide

theorem accepted_provider_history_is_strictly_ordered_final_fact_domain :
    transcriptProviderHistoryCases.all
      (fun witness => !witness.accepted ||
        (witness.exactFinalizedDomainOnly && witness.strictlyIncreasing)) = true := by
  native_decide

end Conformance.ContractCases
