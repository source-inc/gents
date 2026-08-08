import Proofs.Conformance.Contracts.Json.Helpers
import Proofs.Conformance.ContractCases.SubagentBridgeAdmission

namespace Conformance.Contracts

open Conformance.ContractCases

def subagentBridgeAdmissionCaseJson (row : SubagentBridgeAdmissionCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString row.name ++ ","
    ++ "\"bridge_signature_valid\":" ++ boolString row.bridgeSignatureValid ++ ","
    ++ "\"bridge_signer_did\":" ++ toString row.bridgeSignerDid ++ ","
    ++ "\"bridge_author_did\":" ++ toString row.bridgeAuthorDid ++ ","
    ++ "\"admitted_parent_did\":" ++ toString row.admittedParentDid ++ ","
    ++ "\"bridge_head_count\":" ++ toString row.bridgeHeadCount ++ ","
    ++ "\"observed_bridge_cid\":" ++ toString row.observedBridgeCid ++ ","
    ++ "\"current_bridge_cid\":" ++ toString row.currentBridgeCid ++ ","
    ++ "\"parent_request_matches\":" ++ boolString row.parentRequestMatches ++ ","
    ++ "\"parent_tool_call_matches\":" ++ boolString row.parentToolCallMatches ++ ","
    ++ "\"child_request_matches\":" ++ boolString row.childRequestMatches ++ ","
    ++ "\"admitted\":" ++ boolString row.admitted ++ ","
    ++ "\"outcome\":" ++ jsonString row.outcome
    ++ "}"

def subagentBridgeAdmissionCasesJson : String :=
  jsonArray (subagentBridgeAdmissionCases.map subagentBridgeAdmissionCaseJson)

end Conformance.Contracts
