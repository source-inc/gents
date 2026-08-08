import Proofs.Conformance.ContractCases.Types
import Proofs.RequestIngest

/-!
# Request ingest contract cases

Finite witnesses for the signed-ingest provenance gate. Every decision is
computed by `RequestIngest.evaluate`; the pinned theorem makes model drift fail
at Lean build time before Rust consumes the rows.
-/

namespace Conformance.ContractCases

open RequestIngest

structure RequestIngestCase where
  name : String
  origin : String
  requesterDid : Nat
  sourceAuthorDid : Nat
  targetAgentDid : Nat
  sourceSignerDid : Nat
  expectedSourceSignerDid : Nat
  sourceSignatureValid : Bool
  sourceClaimable : Bool
  logicalMatchCount : Nat
  sourceDocId : Nat
  observedDocId : Nat
  sourceHeadCount : Nat
  observedSourceCid : Nat
  sourceCid : Nat
  sourcePayload : Nat
  sourceAdmitted : Bool
  claimSignerDid : Nat
  claimSignatureValid : Bool
  claimParentCid : Nat
  claimPayload : Nat
  outcome : String
  deriving Repr

private def originContract : Origin → String
  | .external => "external"
  | .internal => "internal"

private def requestIngestCase
    (name : String) (source : SourceEvidence)
    (claimSignerDid : Nat) (claimSignatureValid : Bool)
    (claimParentCid claimPayload : Nat) : RequestIngestCase :=
  let claim : ClaimEvidence :=
    { source := source
    , claimSignerDid := claimSignerDid
    , claimSignatureValid := claimSignatureValid
    , claimParentCid := claimParentCid
    , claimPayload := claimPayload
    }
  { name := name
  , origin := originContract source.origin
  , requesterDid := source.requesterDid
  , sourceAuthorDid := source.sourceAuthorDid
  , targetAgentDid := source.targetAgentDid
  , sourceSignerDid := source.sourceSignerDid
  , expectedSourceSignerDid := source.expectedSigner
  , sourceSignatureValid := source.sourceSignatureValid
  , sourceClaimable := source.sourceClaimable
  , logicalMatchCount := source.logicalMatchCount
  , sourceDocId := source.sourceDocId
  , observedDocId := source.observedDocId
  , sourceHeadCount := source.sourceHeadCount
  , observedSourceCid := source.observedSourceCid
  , sourceCid := source.sourceCid
  , sourcePayload := source.payload
  , sourceAdmitted := source.admitted
  , claimSignerDid := claimSignerDid
  , claimSignatureValid := claimSignatureValid
  , claimParentCid := claimParentCid
  , claimPayload := claimPayload
  , outcome := (evaluate claim).toContract
  }

private def externalSource : SourceEvidence :=
  { origin := .external
  , requesterDid := 7
  , sourceAuthorDid := 7
  , targetAgentDid := 11
  , sourceSignerDid := 7
  , sourceSignatureValid := true
  , sourceClaimable := true
  , logicalMatchCount := 1
  , sourceDocId := 41
  , observedDocId := 41
  , sourceHeadCount := 1
  , observedSourceCid := 101
  , sourceCid := 101
  , payload := 303
  }

private def internalSource : SourceEvidence :=
  { externalSource with
    origin := .internal
    sourceAuthorDid := 13
    sourceSignerDid := 13
  }

/-- One positive external row, one positive internal row with distinct requester
attribution, and one negative row for each provenance obligation. -/
def requestIngestCases : List RequestIngestCase :=
  [ requestIngestCase "valid_external_request" externalSource 11 true 101 303
  , requestIngestCase "invalid_source_signature"
      { externalSource with sourceSignatureValid := false } 11 true 101 303
  , requestIngestCase "unexpected_source_signer"
      { externalSource with sourceSignerDid := 19 } 11 true 101 303
  , requestIngestCase "replayed_or_restarted_claim"
      { externalSource with sourceClaimable := false } 11 true 101 303
  , requestIngestCase "duplicate_logical_request_documents"
      { externalSource with logicalMatchCount := 2 } 11 true 101 303
  , requestIngestCase "selected_request_document_mismatch"
      { externalSource with observedDocId := 42 } 11 true 101 303
  , requestIngestCase "missing_source_head"
      { externalSource with sourceHeadCount := 0 } 11 true 101 303
  , requestIngestCase "ambiguous_source_heads"
      { externalSource with sourceHeadCount := 2 } 11 true 101 303
  , requestIngestCase "source_cid_does_not_match_selected_head"
      { externalSource with observedSourceCid := 102 } 11 true 101 303
  , requestIngestCase "invalid_claim_signature" externalSource 11 false 101 303
  , requestIngestCase "claim_not_signed_by_target_agent" externalSource 19 true 101 303
  , requestIngestCase "claim_parent_not_bound_to_source" externalSource 11 true 102 303
  , requestIngestCase "claim_changes_payload" externalSource 11 true 101 304
  , requestIngestCase "valid_internal_request_with_distinct_requester"
      internalSource 11 true 101 303
  , requestIngestCase "internal_request_signed_by_carried_requester"
      { internalSource with sourceSignerDid := internalSource.requesterDid }
      11 true 101 303
  ]

theorem requestIngestCases_pinned :
    requestIngestCases.map
      (fun row => (row.name, row.expectedSourceSignerDid,
        row.sourceAdmitted, row.outcome)) =
      [ ("valid_external_request", 7, true, "admitted")
      , ("invalid_source_signature", 7, false, "sourceRejected")
      , ("unexpected_source_signer", 7, false, "sourceRejected")
      , ("replayed_or_restarted_claim", 7, false, "sourceRejected")
      , ("duplicate_logical_request_documents", 7, false, "sourceRejected")
      , ("selected_request_document_mismatch", 7, false, "sourceRejected")
      , ("missing_source_head", 7, false, "sourceRejected")
      , ("ambiguous_source_heads", 7, false, "sourceRejected")
      , ("source_cid_does_not_match_selected_head", 7, false, "sourceRejected")
      , ("invalid_claim_signature", 7, true, "claimRejected")
      , ("claim_not_signed_by_target_agent", 7, true, "claimRejected")
      , ("claim_parent_not_bound_to_source", 7, true, "claimRejected")
      , ("claim_changes_payload", 7, true, "claimRejected")
      , ("valid_internal_request_with_distinct_requester", 13, true, "admitted")
      , ("internal_request_signed_by_carried_requester", 13, false,
          "sourceRejected")
      ] := by
  rfl

end Conformance.ContractCases
