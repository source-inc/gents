import Proofs.Conformance.Contracts.Json.Helpers
import Proofs.Conformance.ContractCases.TranscriptFinalization

namespace Conformance.Contracts

open Conformance.ContractCases

def natArray (values : List Nat) : String :=
  jsonArray (values.map toString)

def transcriptFinalizationCaseJson
    (witness : TranscriptFinalizationCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString witness.name ++ ","
    ++ "\"action\":" ++ jsonString witness.action ++ ","
    ++ "\"visible_logical_fact_count\":"
      ++ toString witness.visibleLogicalFactCount ++ ","
    ++ "\"checkpoint_present\":" ++ boolString witness.checkpointPresent ++ ","
    ++ "\"fact_present_before\":" ++ boolString witness.factPresentBefore ++ ","
    ++ "\"fact_present_after\":" ++ boolString witness.factPresentAfter ++ ","
    ++ "\"fact_commit_cid\":" ++ jsonOptionalNat witness.factCommitCid ++ ","
    ++ "\"checkpoint_payload_hash\":"
      ++ jsonOptionalNat witness.checkpointPayloadHash ++ ","
    ++ "\"write_payload_hash\":" ++ toString witness.writePayloadHash ++ ","
    ++ "\"fact_payload_hash\":" ++ jsonOptionalNat witness.factPayloadHash ++ ","
    ++ "\"write_signer_did\":" ++ jsonOptionalNat witness.writeSignerDid ++ ","
    ++ "\"write_signature_valid\":"
      ++ jsonOptionalBool witness.writeSignatureValid ++ ","
    ++ "\"write_policy_authorized\":"
      ++ jsonOptionalBool witness.writePolicyAuthorized ++ ","
    ++ "\"fact_signer_did\":" ++ jsonOptionalNat witness.factSignerDid ++ ","
    ++ "\"disposition\":" ++ jsonString witness.disposition ++ ","
    ++ "\"checkpoint_preserved\":" ++ boolString witness.checkpointPreserved ++ ","
    ++ "\"sibling_isolated\":" ++ boolString witness.siblingIsolated
    ++ "}"

def transcriptProviderHistoryCaseJson
    (witness : TranscriptProviderHistoryCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString witness.name ++ ","
    ++ "\"reference_count\":" ++ toString witness.referenceCount ++ ","
    ++ "\"visible_conflict_count\":" ++ toString witness.visibleConflictCount ++ ","
    ++ "\"accepted\":" ++ boolString witness.accepted ++ ","
    ++ "\"output_count\":" ++ toString witness.outputCount ++ ","
    ++ "\"output_payload_hashes\":" ++ natArray witness.outputPayloadHashes ++ ","
    ++ "\"exact_finalized_domain_only\":"
      ++ boolString witness.exactFinalizedDomainOnly ++ ","
    ++ "\"strictly_increasing\":" ++ boolString witness.strictlyIncreasing
    ++ "}"

end Conformance.Contracts
