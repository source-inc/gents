import Proofs.Conformance.Contracts.Json.Helpers
import Proofs.Conformance.ContractCases.RequestIngest

namespace Conformance.Contracts

open Conformance.ContractCases

def requestIngestCaseJson (witness : RequestIngestCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString witness.name ++ ","
    ++ "\"origin\":" ++ jsonString witness.origin ++ ","
    ++ "\"requester_did\":" ++ toString witness.requesterDid ++ ","
    ++ "\"source_author_did\":" ++ toString witness.sourceAuthorDid ++ ","
    ++ "\"target_agent_did\":" ++ toString witness.targetAgentDid ++ ","
    ++ "\"source_signer_did\":" ++ toString witness.sourceSignerDid ++ ","
    ++ "\"expected_source_signer_did\":"
      ++ toString witness.expectedSourceSignerDid ++ ","
    ++ "\"source_signature_valid\":"
      ++ boolString witness.sourceSignatureValid ++ ","
    ++ "\"source_claimable\":" ++ boolString witness.sourceClaimable ++ ","
    ++ "\"logical_match_count\":" ++ toString witness.logicalMatchCount ++ ","
    ++ "\"source_doc_id\":" ++ toString witness.sourceDocId ++ ","
    ++ "\"observed_doc_id\":" ++ toString witness.observedDocId ++ ","
    ++ "\"source_head_count\":" ++ toString witness.sourceHeadCount ++ ","
    ++ "\"observed_source_cid\":" ++ toString witness.observedSourceCid ++ ","
    ++ "\"source_cid\":" ++ toString witness.sourceCid ++ ","
    ++ "\"source_payload\":" ++ toString witness.sourcePayload ++ ","
    ++ "\"source_admitted\":" ++ boolString witness.sourceAdmitted ++ ","
    ++ "\"claim_signer_did\":" ++ toString witness.claimSignerDid ++ ","
    ++ "\"claim_signature_valid\":"
      ++ boolString witness.claimSignatureValid ++ ","
    ++ "\"claim_parent_cid\":" ++ toString witness.claimParentCid ++ ","
    ++ "\"claim_payload\":" ++ toString witness.claimPayload ++ ","
    ++ "\"outcome\":" ++ jsonString witness.outcome
    ++ "}"

def requestIngestCasesJson : String :=
  jsonArray (requestIngestCases.map requestIngestCaseJson)

end Conformance.Contracts
