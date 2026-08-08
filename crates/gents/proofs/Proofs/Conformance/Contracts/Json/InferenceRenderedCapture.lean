import Proofs.Conformance.Contracts.Json.Helpers
import Proofs.Conformance.ContractCases.InferenceRenderedCapture

namespace Conformance.Contracts

open Conformance.ContractCases

def inferenceRenderedCaptureCaseJson (row : InferenceRenderedCaptureCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString row.name ++ ","
    ++ "\"initial_stage\":" ++ jsonString row.initialStage ++ ","
    ++ "\"final_stage\":" ++ jsonString row.finalStage ++ ","
    ++ "\"initial_call_state\":" ++ jsonString row.initialCallState ++ ","
    ++ "\"final_call_state\":" ++ jsonString row.finalCallState ++ ","
    ++ "\"capture_outcome\":" ++ jsonString row.captureOutcome ++ ","
    ++ "\"running_call_doc_id\":" ++ jsonOptionalNat row.runningCallDocId ++ ","
    ++ "\"running_call_cid\":" ++ jsonOptionalNat row.runningCallCid ++ ","
    ++ "\"render_doc_id\":" ++ jsonOptionalNat row.renderDocId ++ ","
    ++ "\"render_cid\":" ++ jsonOptionalNat row.renderCid ++ ","
    ++ "\"current_call_cid\":" ++ toString row.currentCallCid ++ ","
    ++ "\"render_durable\":" ++ boolString row.renderDurable ++ ","
    ++ "\"render_pins_running\":" ++ boolString row.renderPinsRunning ++ ","
    ++ "\"call_pins_render\":" ++ boolString row.callPinsRender ++ ","
    ++ "\"http_requests_observed\":" ++ toString row.httpRequestsObserved ++ ","
    ++ "\"terminal_failed\":" ++ boolString row.terminalFailed ++ ","
    ++ "\"second_send_permitted\":" ++ boolString row.secondSendPermitted
    ++ "}"

def inferenceRenderedCaptureCasesJson : String :=
  jsonArray (inferenceRenderedCaptureCases.map inferenceRenderedCaptureCaseJson)

end Conformance.Contracts
