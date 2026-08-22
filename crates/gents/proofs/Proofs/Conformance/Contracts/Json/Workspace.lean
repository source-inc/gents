import Proofs.Conformance.Contracts.Json.Helpers
import Proofs.Conformance.ContractCases.Types
import Proofs.Workspace.Conformance

namespace Conformance.Contracts

open Conformance.ContractCases
open CommandPolicy

def workspaceCaseJson (witness : Workspace.Conformance.WorkspaceCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString witness.name ++ ","
    ++ "\"from\":" ++ jsonString witness.fromState.toDefraDB ++ ","
    ++ "\"to\":" ++ jsonString witness.toState.toDefraDB ++ ","
    ++ "\"seal_hash\":" ++ jsonOptionalString witness.sealHash ++ ","
    ++ "\"legal\":" ++ boolString witness.legal
    ++ "}"

def workspaceCasesJson : String :=
  jsonArray (Workspace.Conformance.workspaceCases.map workspaceCaseJson)

def bindingWitnessJson (workspaceId : String)
    (witness : Workspace.Conformance.BindingWitness) : String :=
  "{"
    ++ "\"binding_id\":" ++ jsonString witness.bindingId ++ ","
    ++ "\"workspace_id\":" ++ jsonString workspaceId ++ ","
    ++ "\"request_id\":" ++ jsonString witness.requestId ++ ","
    ++ "\"authority\":" ++ jsonString witness.authority.toDefraDB ++ ","
    ++ "\"deployment_id\":" ++ jsonString witness.deploymentId ++ ","
    ++ "\"seal_hash\":" ++ jsonOptionalString witness.sealHash ++ ","
    ++ "\"state\":" ++ jsonString witness.state.toDefraDB
    ++ "}"

def workspaceBindingCaseJson
    (witness : Workspace.Conformance.WorkspaceBindingCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString witness.name ++ ","
    ++ "\"workspace_id\":" ++ jsonString witness.workspaceId ++ ","
    ++ "\"workspace_state\":" ++ jsonString witness.workspaceState.toDefraDB ++ ","
    ++ "\"workspace_seal_hash\":" ++ jsonOptionalString witness.workspaceSealHash ++ ","
    ++ "\"owner_deployment_id\":" ++ jsonString witness.ownerDeploymentId ++ ","
    ++ "\"creation_policy\":" ++ jsonString witness.creationPolicy.toDefraDB ++ ","
    ++ "\"existing\":"
      ++ jsonArray
        (witness.existing.map (bindingWitnessJson witness.workspaceId)) ++ ","
    ++ "\"candidate\":"
      ++ bindingWitnessJson witness.workspaceId witness.candidate ++ ","
    ++ "\"git_metadata_write\":" ++ boolString witness.gitMetadataWrite ++ ","
    ++ "\"behavior_command_mode\":"
      ++ jsonString witness.behaviorCommandMode.toDefraDB ++ ","
    ++ "\"legal\":" ++ boolString witness.legal
    ++ "}"

def workspaceBindingCasesJson : String :=
  jsonArray
    (Workspace.Conformance.workspaceBindingCases.map workspaceBindingCaseJson)

end Conformance.Contracts
