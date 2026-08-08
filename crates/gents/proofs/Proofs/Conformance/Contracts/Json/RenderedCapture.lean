import Proofs.Conformance.Contracts.Json.Helpers
import Proofs.Conformance.ContractCases.RenderedCapture

namespace Conformance.Contracts

open Conformance.ContractCases

def renderedConfigSourceRefCaseJson (witness : RenderedConfigSourceRefCase) : String :=
  "{"
    ++ "\"source_class\":" ++ jsonString witness.sourceClass ++ ","
    ++ "\"logical_id\":" ++ jsonOptionalNat witness.logicalId ++ ","
    ++ "\"doc_id\":" ++ toString witness.docId ++ ","
    ++ "\"composite_commit_cid\":" ++ toString witness.compositeCommitCid ++ ","
    ++ "\"signer_did\":" ++ toString witness.signerDid
    ++ "}"

def renderedConfigSourceRefCasesJson (witnesses : List RenderedConfigSourceRefCase) : String :=
  jsonArray (witnesses.map renderedConfigSourceRefCaseJson)

def renderedCaptureCaseJson (witness : RenderedCaptureCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString witness.name ++ ","
    ++ "\"agent_did\":" ++ toString witness.agentDid ++ ","
    ++ "\"session_id\":" ++ toString witness.sessionId ++ ","
    ++ "\"request_id\":" ++ toString witness.requestId ++ ","
    ++ "\"turn_index\":" ++ toString witness.turnIndex ++ ","
    ++ "\"attempt\":" ++ toString witness.attempt ++ ","
    ++ "\"request\":" ++ toString witness.request ++ ","
    ++ "\"config_scope\":" ++ jsonString witness.configScope ++ ","
    ++ "\"config_required\":" ++ boolString witness.configRequired ++ ","
    ++ "\"config_present\":" ++ boolString witness.configPresent ++ ","
    ++ "\"config_sources\":" ++ renderedConfigSourceRefCasesJson witness.configSources ++ ","
    ++ "\"config_complete\":" ++ boolString witness.configComplete ++ ","
    ++ "\"config_admitted\":" ++ boolString witness.configAdmitted ++ ","
    ++ "\"prior_binding\":" ++ jsonOptionalNat witness.priorBinding ++ ","
    ++ "\"prior_config_present\":" ++ boolString witness.priorConfigPresent ++ ","
    ++ "\"prior_config_sources\":"
      ++ renderedConfigSourceRefCasesJson witness.priorConfigSources ++ ","
    ++ "\"capture_outcome\":" ++ jsonString witness.captureOutcome ++ ","
    ++ "\"capture_durable\":" ++ boolString witness.captureDurable ++ ","
    ++ "\"post_stage\":" ++ jsonString witness.postStage ++ ","
    ++ "\"send_permitted\":" ++ boolString witness.sendPermitted ++ ","
    ++ "\"provider_requests_observed\":"
      ++ toString witness.providerRequestsObserved ++ ","
    ++ "\"durable_after\":" ++ jsonOptionalNat witness.durableAfter ++ ","
    ++ "\"durable_config_present\":" ++ boolString witness.durableConfigPresent ++ ","
    ++ "\"durable_config_sources\":"
      ++ renderedConfigSourceRefCasesJson witness.durableConfigSources ++ ","
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

end Conformance.Contracts
