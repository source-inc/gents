import Proofs.Conformance.Contracts.Json.Helpers
import Proofs.Conformance.ContractCases.InferenceCallExactTarget

namespace Conformance.Contracts

open Conformance.ContractCases

def inferenceCallExactTargetCaseJson
    (witness : InferenceCallExactTargetCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString witness.name ++ ","
    ++ "\"action\":" ++ jsonString witness.action ++ ","
    ++ "\"write_target\":" ++ jsonString witness.writeTarget ++ ","
    ++ "\"target_present\":" ++ boolString witness.targetPresent ++ ","
    ++ "\"expected_state\":" ++ jsonString witness.expectedState ++ ","
    ++ "\"target_owner\":" ++ toString witness.targetOwner ++ ","
    ++ "\"target_epoch\":" ++ toString witness.targetEpoch ++ ","
    ++ "\"expected_owner\":" ++ toString witness.expectedOwner ++ ","
    ++ "\"expected_epoch\":" ++ toString witness.expectedEpoch ++ ","
    ++ "\"requested_post_state\":" ++ jsonString witness.requestedPostState ++ ","
    ++ "\"target_pre_state\":" ++ jsonOptionalString witness.targetPreState ++ ","
    ++ "\"target_post_state\":" ++ jsonOptionalString witness.targetPostState ++ ","
    ++ "\"sibling_pre_state\":" ++ jsonString witness.siblingPreState ++ ","
    ++ "\"sibling_post_state\":" ++ jsonString witness.siblingPostState ++ ","
    ++ "\"write_matched\":" ++ boolString witness.writeMatched ++ ","
    ++ "\"sibling_isolated\":" ++ boolString witness.siblingIsolated ++ ","
    ++ "\"same_logical_call_id\":" ++ boolString witness.sameLogicalCallId ++ ","
    ++ "\"terminal_pre_state\":" ++ boolString witness.terminalPreState ++ ","
    ++ "\"terminal_irreversible\":" ++ boolString witness.terminalIrreversible
    ++ "}"

def inferenceCallExactTargetTraceCaseJson
    (witness : InferenceCallExactTargetTraceCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString witness.name ++ ","
    ++ "\"scenario\":" ++ jsonString witness.scenario ++ ","
    ++ "\"target_pre_state\":" ++ jsonString witness.targetPreState ++ ","
    ++ "\"sibling_pre_state\":" ++ jsonString witness.siblingPreState ++ ","
    ++ "\"visible_logical_document_count\":"
    ++ toString witness.visibleLogicalDocumentCount ++ ","
    ++ "\"unique_admission_required\":"
    ++ boolString witness.uniqueAdmissionRequired ++ ","
    ++ "\"raw_independent_cas_possible\":"
    ++ boolString witness.rawIndependentCasPossible ++ ","
    ++ "\"first_target\":" ++ jsonString witness.firstTarget ++ ","
    ++ "\"first_action\":" ++ jsonString witness.firstAction ++ ","
    ++ "\"first_expected_state\":" ++ jsonString witness.firstExpectedState ++ ","
    ++ "\"first_expected_owner\":" ++ toString witness.firstExpectedOwner ++ ","
    ++ "\"first_expected_epoch\":" ++ toString witness.firstExpectedEpoch ++ ","
    ++ "\"first_requested_post_state\":" ++ jsonString witness.firstRequestedPostState ++ ","
    ++ "\"first_cas_matched\":" ++ boolString witness.firstCasMatched ++ ","
    ++ "\"second_target\":" ++ jsonString witness.secondTarget ++ ","
    ++ "\"second_action\":" ++ jsonString witness.secondAction ++ ","
    ++ "\"second_expected_state\":" ++ jsonString witness.secondExpectedState ++ ","
    ++ "\"second_expected_owner\":" ++ toString witness.secondExpectedOwner ++ ","
    ++ "\"second_expected_epoch\":" ++ toString witness.secondExpectedEpoch ++ ","
    ++ "\"second_requested_post_state\":" ++ jsonString witness.secondRequestedPostState ++ ","
    ++ "\"second_cas_matched\":" ++ boolString witness.secondCasMatched ++ ","
    ++ "\"second_disposition\":" ++ jsonString witness.secondDisposition ++ ","
    ++ "\"final_target_state\":" ++ jsonString witness.finalTargetState ++ ","
    ++ "\"final_sibling_state\":" ++ jsonString witness.finalSiblingState
    ++ "}"

end Conformance.Contracts
