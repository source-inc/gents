import Proofs.Conformance.Contracts.Json.Helpers
import Proofs.Conformance.ContractCases.RenderedCapture

namespace Conformance.Contracts

open Conformance.ContractCases

def renderedCaptureCaseJson (witness : RenderedCaptureCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString witness.name ++ ","
    ++ "\"agent_did\":" ++ toString witness.agentDid ++ ","
    ++ "\"session_id\":" ++ toString witness.sessionId ++ ","
    ++ "\"request_id\":" ++ toString witness.requestId ++ ","
    ++ "\"turn_index\":" ++ toString witness.turnIndex ++ ","
    ++ "\"attempt\":" ++ toString witness.attempt ++ ","
    ++ "\"request\":" ++ toString witness.request ++ ","
    ++ "\"prior_binding\":" ++ jsonOptionalNat witness.priorBinding ++ ","
    ++ "\"capture_outcome\":" ++ jsonString witness.captureOutcome ++ ","
    ++ "\"capture_durable\":" ++ boolString witness.captureDurable ++ ","
    ++ "\"post_stage\":" ++ jsonString witness.postStage ++ ","
    ++ "\"send_permitted\":" ++ boolString witness.sendPermitted ++ ","
    ++ "\"provider_requests_observed\":"
      ++ toString witness.providerRequestsObserved ++ ","
    ++ "\"durable_after\":" ++ jsonOptionalNat witness.durableAfter ++ ","
    ++ "\"final_stage\":" ++ jsonString witness.finalStage
    ++ "}"

def renderedCaptureCasesJson : String :=
  jsonArray (renderedCaptureCases.map renderedCaptureCaseJson)

def renderedCaptureKeyCaseJson (witness : RenderedCaptureKeyCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString witness.name ++ ","
    ++ "\"left_agent_did\":" ++ toString witness.leftAgentDid ++ ","
    ++ "\"left_session_id\":" ++ toString witness.leftSessionId ++ ","
    ++ "\"left_request_id\":" ++ toString witness.leftRequestId ++ ","
    ++ "\"left_turn_index\":" ++ toString witness.leftTurnIndex ++ ","
    ++ "\"left_attempt\":" ++ toString witness.leftAttempt ++ ","
    ++ "\"right_agent_did\":" ++ toString witness.rightAgentDid ++ ","
    ++ "\"right_session_id\":" ++ toString witness.rightSessionId ++ ","
    ++ "\"right_request_id\":" ++ toString witness.rightRequestId ++ ","
    ++ "\"right_turn_index\":" ++ toString witness.rightTurnIndex ++ ","
    ++ "\"right_attempt\":" ++ toString witness.rightAttempt ++ ","
    ++ "\"same_fact\":" ++ boolString witness.sameFact
    ++ "}"

def renderedCaptureKeyCasesJson : String :=
  jsonArray (renderedCaptureKeyCases.map renderedCaptureKeyCaseJson)

def captureScopeCaseJson (witness : CaptureScopeCase) : String :=
  "{"
    ++ "\"label\":" ++ jsonString witness.label ++ ","
    ++ "\"kind\":" ++ jsonString witness.kind ++ ","
    ++ "\"seq\":" ++ toString witness.seq ++ ","
    ++ "\"valid\":" ++ boolString witness.valid
    ++ "}"

def captureScopeCasesJson : String :=
  jsonArray (captureScopeCases.map captureScopeCaseJson)

def captureOrderCaseJson (witness : CaptureOrderCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString witness.name ++ ","
    ++ "\"left_label\":" ++ jsonString witness.leftLabel ++ ","
    ++ "\"left_turn\":" ++ toString witness.leftTurn ++ ","
    ++ "\"left_attempt\":" ++ toString witness.leftAttempt ++ ","
    ++ "\"right_label\":" ++ jsonString witness.rightLabel ++ ","
    ++ "\"right_turn\":" ++ toString witness.rightTurn ++ ","
    ++ "\"right_attempt\":" ++ toString witness.rightAttempt ++ ","
    ++ "\"left_before_right\":" ++ boolString witness.leftBeforeRight
    ++ "}"

def captureOrderCasesJson : String :=
  jsonArray (captureOrderCases.map captureOrderCaseJson)

end Conformance.Contracts
