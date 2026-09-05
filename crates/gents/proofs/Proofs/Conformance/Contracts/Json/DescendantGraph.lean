import Proofs.Conformance.Contracts.Json.Helpers
import Proofs.Conformance.ContractCases.DescendantGraph

namespace Conformance.Contracts

open Conformance.ContractCases

def descendantGraphCaseJson (value : DescendantGraphCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString value.name ++ ","
    ++ "\"root_request_id\":" ++ toString value.rootRequestId ++ ","
    ++ "\"parent_request_id\":" ++ toString value.parentRequestId ++ ","
    ++ "\"child_request_id\":" ++ toString value.childRequestId ++ ","
    ++ "\"await_mode\":" ++ jsonString value.awaitMode ++ ","
    ++ "\"materialization\":" ++ jsonString value.materialization ++ ","
    ++ "\"lifecycle\":" ++ jsonString value.lifecycle ++ ","
    ++ "\"direct\":" ++ boolString value.direct ++ ","
    ++ "\"visible\":" ++ boolString value.visible ++ ","
    ++ "\"readable\":" ++ boolString value.readable ++ ","
    ++ "\"retryable\":" ++ boolString value.retryable ++ ","
    ++ "\"listed_by_default\":" ++ boolString value.listedByDefault ++ ","
    ++ "\"controllable\":" ++ boolString value.controllable ++ ","
    ++ "\"cursor_anchor_survives_terminal\":"
      ++ boolString value.cursorAnchorSurvivesTerminal
    ++ ",\"caller_session\":" ++ jsonString value.callerSession
    ++ ",\"caller_agent\":" ++ jsonString value.callerAgent
    ++ ",\"caller_requester\":" ++ (match value.callerRequester with
        | none => "null"
        | some requester => jsonString requester)
    ++ ",\"session_authorized\":" ++ boolString value.sessionAuthorized
    ++ ",\"session_controllable\":" ++ boolString value.sessionControllable
    ++ "}"

def descendantGraphCasesJson : String :=
  jsonArray (descendantGraphCases.map descendantGraphCaseJson)

end Conformance.Contracts
