import Proofs.Conformance.Contracts.Json.Helpers
import Proofs.Conformance.ContractCases.ToolFact

namespace Conformance.Contracts

open Conformance.ContractCases

def toolFactCaseJson (row : ToolFactCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString row.name ++ ","
    ++ "\"operation\":" ++ jsonString row.operation ++ ","
    ++ "\"disposition\":" ++ jsonString row.disposition ++ ","
    ++ "\"visible_logical_twins\":" ++ toString row.visibleLogicalTwins ++ ","
    ++ "\"full_output\":" ++ boolString row.fullOutput ++ ","
    ++ "\"call_doc_id\":" ++ toString row.callDocId ++ ","
    ++ "\"call_cid\":" ++ toString row.callCid ++ ","
    ++ "\"call_signer_did\":" ++ toString row.callSignerDid ++ ","
    ++ "\"result_doc_id\":" ++ toString row.resultDocId ++ ","
    ++ "\"result_cid\":" ++ toString row.resultCid ++ ","
    ++ "\"result_signer_did\":" ++ toString row.resultSignerDid ++ ","
    ++ "\"approval_doc_id\":" ++ jsonOptionalNat row.approvalDocId ++ ","
    ++ "\"approval_cid\":" ++ jsonOptionalNat row.approvalCid ++ ","
    ++ "\"approval_signer_did\":" ++ jsonOptionalNat row.approvalSignerDid ++ ","
    ++ "\"result_durable\":" ++ boolString row.resultDurable ++ ","
    ++ "\"approval_durable\":" ++ boolString row.approvalDurable ++ ","
    ++ "\"result_pins_exact_call\":" ++ boolString row.resultPinsExactCall ++ ","
    ++ "\"approval_pins_exact_call\":" ++ boolString row.approvalPinsExactCall ++ ","
    ++ "\"exact_projection\":" ++ boolString row.exactProjection ++ ","
    ++ "\"immutable_noop\":" ++ boolString row.immutableNoop
    ++ "}"

def toolFactCasesJson : String :=
  jsonArray (toolFactCases.map toolFactCaseJson)

end Conformance.Contracts
