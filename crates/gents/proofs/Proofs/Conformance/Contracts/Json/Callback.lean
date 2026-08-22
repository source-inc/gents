import Proofs.Conformance.Contracts.Json.Helpers
import Proofs.Conformance.ContractCases.Types
import Proofs.Callback.Conformance

namespace Conformance.Contracts

open Conformance.ContractCases

def callbackCaseJson (witness : Callback.Conformance.CallbackCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString witness.name ++ ","
    ++ "\"invocation_id\":" ++ jsonString witness.invocationId ++ ","
    ++ "\"owner_deployment_id\":" ++ jsonString witness.ownerDeploymentId ++ ","
    ++ "\"state\":" ++ jsonString witness.state.toDefraDB ++ ","
    ++ "\"journal\":"
      ++ jsonStringArray (witness.journal.map ActionJournalState.toDefraDB) ++ ","
    ++ "\"result_emitted\":" ++ boolString witness.resultEmitted ++ ","
    ++ "\"legal\":" ++ boolString witness.legal
    ++ "}"

def callbackCasesJson : String :=
  jsonArray (Callback.Conformance.callbackCases.map callbackCaseJson)

end Conformance.Contracts
