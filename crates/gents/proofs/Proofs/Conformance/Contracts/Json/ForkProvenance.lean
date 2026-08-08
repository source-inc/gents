import Proofs.Conformance.Contracts.Json.Helpers
import Proofs.Conformance.ContractCases.ForkProvenance

namespace Conformance.Contracts

open Conformance.ContractCases

def forkProvenanceCaseJson (row : ForkProvenanceCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString row.name ++ ","
    ++ "\"kind\":" ++ jsonString row.kind ++ ","
    ++ "\"disposition\":" ++ jsonString row.disposition ++ ","
    ++ "\"visible_logical_twins\":" ++ toString row.visibleLogicalTwins ++ ","
    ++ "\"source_authoritative\":" ++ boolString row.sourceAuthoritative ++ ","
    ++ "\"source_session_id\":" ++ toString row.sourceSessionId ++ ","
    ++ "\"child_session_id\":" ++ toString row.childSessionId ++ ","
    ++ "\"source_doc_id\":" ++ toString row.sourceDocId ++ ","
    ++ "\"source_cid\":" ++ toString row.sourceCid ++ ","
    ++ "\"source_signer_did\":" ++ toString row.sourceSignerDid ++ ","
    ++ "\"child_doc_id\":" ++ toString row.childDocId ++ ","
    ++ "\"child_cid\":" ++ toString row.childCid ++ ","
    ++ "\"child_signer_did\":" ++ toString row.childSignerDid ++ ","
    ++ "\"child_call_required\":" ++ boolString row.childCallRequired ++ ","
    ++ "\"child_call_satisfied\":" ++ boolString row.childCallSatisfied ++ ","
    ++ "\"exact_source_pinned\":" ++ boolString row.exactSourcePinned ++ ","
    ++ "\"immutable_noop\":" ++ boolString row.immutableNoop
    ++ "}"

def forkProvenanceCasesJson : String :=
  jsonArray (forkProvenanceCases.map forkProvenanceCaseJson)

end Conformance.Contracts
