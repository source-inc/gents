import Proofs.Workspace.Properties

namespace Workspace
namespace Conformance

open IsolatedWorkspace
open CommandPolicy

structure WorkspaceCase where
  name : String
  fromState : WorkspaceState
  toState : WorkspaceState
  sealHash : Option String
  legal : Bool
  deriving Repr

def preWorkspace (c : WorkspaceCase) : IsolatedWorkspace :=
  { workspaceId := "ws-1"
  , workUnitId := "wu-1"
  , repositoryId := "repo-1"
  , baseSha := "base"
  , branch := "main"
  , creationPolicy := .gitWorktreeDiff
  , ownerDeploymentId := "dep-1"
  , sealHash := c.sealHash
  , state := c.fromState }

def caseLegalCorrect (c : WorkspaceCase) : Bool :=
  c.legal == transitionLegal (preWorkspace c) c.toState

def workspaceCases : List WorkspaceCase :=
  [ { name := "provision_success"
    , fromState := .provisioning
    , toState := .ready
    , sealHash := none
    , legal := true }
  , { name := "provision_fail"
    , fromState := .provisioning
    , toState := .provisionFailed
    , sealHash := none
    , legal := true }
  , { name := "seal_requires_hash"
    , fromState := .ready
    , toState := .sealed
    , sealHash := none
    , legal := false }
  , { name := "seal_with_hash"
    , fromState := .ready
    , toState := .sealed
    , sealHash := some "seal-1"
    , legal := true }
  , { name := "sealed_to_cleaning"
    , fromState := .sealed
    , toState := .cleaning
    , sealHash := some "seal-1"
    , legal := true }
  , { name := "cleaning_to_cleaned"
    , fromState := .cleaning
    , toState := .cleaned
    , sealHash := some "seal-1"
    , legal := true }
  , { name := "sealed_to_cleaned_illegal"
    , fromState := .sealed
    , toState := .cleaned
    , sealHash := some "seal-1"
    , legal := false }
  ]

theorem workspaceCasesLegalCorrect :
    workspaceCases.all caseLegalCorrect = true := by
  native_decide

structure BindingWitness where
  bindingId : String
  requestId : String
  authority : BindingAuthority
  deploymentId : String
  sealHash : Option String
  state : BindingState
  deriving Repr

def BindingWitness.toBinding (workspaceId : String) (b : BindingWitness) :
    WorkspaceBinding :=
  { bindingId := b.bindingId
  , workspaceId := workspaceId
  , requestId := b.requestId
  , authority := b.authority
  , deploymentId := b.deploymentId
  , sealHash := b.sealHash
  , state := b.state }

structure WorkspaceBindingCase where
  name : String
  workspaceId : String
  workspaceState : WorkspaceState
  workspaceSealHash : Option String
  ownerDeploymentId : String
  creationPolicy : CreationPolicy
  existing : List BindingWitness
  candidate : BindingWitness
  gitMetadataWrite : Bool
  behaviorCommandMode : ExecutionMode
  legal : Bool
  deriving Repr

def WorkspaceBindingCase.workspace (c : WorkspaceBindingCase) : IsolatedWorkspace :=
  { workspaceId := c.workspaceId
  , workUnitId := "wu-1"
  , repositoryId := "repo-1"
  , baseSha := "base"
  , branch := "main"
  , creationPolicy := c.creationPolicy
  , ownerDeploymentId := c.ownerDeploymentId
  , sealHash := c.workspaceSealHash
  , state := c.workspaceState }

def WorkspaceBindingCase.existingBindings (c : WorkspaceBindingCase) :
    List WorkspaceBinding :=
  c.existing.map (BindingWitness.toBinding c.workspaceId)

def WorkspaceBindingCase.candidateBinding (c : WorkspaceBindingCase) : WorkspaceBinding :=
  BindingWitness.toBinding c.workspaceId c.candidate

def candidateBindingLegal (w : IsolatedWorkspace) (b : WorkspaceBinding) : Bool :=
  decide (b.workspaceId = w.workspaceId) &&
    decide (OwnerClaimable b.deploymentId w) &&
    decide (ReadWriteOk w b) &&
    decide (ReadOnlyOk w b) &&
    decide (IntegrateOk w b)

def bindingCaseLegal (c : WorkspaceBindingCase) : Bool :=
  let w := c.workspace
  let candidate := c.candidateBinding
  let bindings := candidate :: c.existingBindings
  candidateBindingLegal w candidate &&
    decide (UniqueActiveReadWrite w.workspaceId bindings) &&
    decide (UniqueActiveIntegrate w.workspaceId bindings) &&
    (!c.gitMetadataWrite || decide (GitMetadataWriteOk w.creationPolicy candidate.authority)) &&
    decide (AuthorityMeetOk c.behaviorCommandMode candidate.authority)

def caseBindingLegalCorrect (c : WorkspaceBindingCase) : Bool :=
  c.legal == bindingCaseLegal c

def mkWitness
    (id : String)
    (authority : BindingAuthority)
    (deploymentId : String := "dep-1")
    (state : BindingState := .active)
    (sealHash : Option String := none)
    (requestId : String := "req-1") : BindingWitness :=
  { bindingId := id
  , requestId := requestId
  , authority := authority
  , deploymentId := deploymentId
  , sealHash := sealHash
  , state := state }

def mkBindingCase
    (name : String)
    (workspaceState : WorkspaceState)
    (candidate : BindingWitness)
    (legal : Bool)
    (workspaceSealHash : Option String := none)
    (existing : List BindingWitness := [])
    (ownerDeploymentId : String := "dep-1")
    (creationPolicy : CreationPolicy := .gitWorktreeDiff)
    (gitMetadataWrite : Bool := false)
    (behaviorCommandMode : ExecutionMode := .unrestricted) :
    WorkspaceBindingCase :=
  { name := name
  , workspaceId := "ws-1"
  , workspaceState := workspaceState
  , workspaceSealHash := workspaceSealHash
  , ownerDeploymentId := ownerDeploymentId
  , creationPolicy := creationPolicy
  , existing := existing
  , candidate := candidate
  , gitMetadataWrite := gitMetadataWrite
  , behaviorCommandMode := behaviorCommandMode
  , legal := legal }

def workspaceBindingCases : List WorkspaceBindingCase :=
  [ mkBindingCase "provision_fail_no_bind" .provisionFailed
      (mkWitness "b-1" .readOnly) false
  , mkBindingCase "read_write_after_sealed_illegal" .sealed
      (mkWitness "b-1" .readWrite) false (workspaceSealHash := some "seal-1")
  , mkBindingCase "second_active_read_write_illegal" .ready
      (mkWitness "b-2" .readWrite (requestId := "req-2")) false
      (existing := [mkWitness "b-1" .readWrite])
  , mkBindingCase "two_active_read_only_after_seal_legal" .sealed
      (mkWitness "b-2" .readOnly (sealHash := some "seal-1") (requestId := "req-2"))
      true
      (workspaceSealHash := some "seal-1")
      (existing := [mkWitness "b-1" .readOnly (sealHash := some "seal-1")])
  , mkBindingCase "integrate_before_seal_illegal" .ready
      (mkWitness "b-1" .integrate) false
  , mkBindingCase "integrate_mismatched_seal_hash_illegal" .sealed
      (mkWitness "b-1" .integrate (sealHash := some "other")) false
      (workspaceSealHash := some "seal-1")
  , mkBindingCase "non_owner_deployment_cannot_claim" .ready
      (mkWitness "b-1" .readWrite (deploymentId := "dep-other")) false
  , mkBindingCase "git_worktree_diff_read_write_git_metadata_write_illegal" .ready
      (mkWitness "b-1" .readWrite) false
      (gitMetadataWrite := true)
  , mkBindingCase "authority_meet_read_write_not_unrestricted" .ready
      (mkWitness "b-1" .readWrite) true
      (behaviorCommandMode := .unrestricted)
  , mkBindingCase "read_write_on_ready_legal" .ready
      (mkWitness "b-1" .readWrite) true
  , mkBindingCase "integrate_matching_seal_legal" .sealed
      (mkWitness "b-1" .integrate (sealHash := some "seal-1")) true
      (workspaceSealHash := some "seal-1")
  , mkBindingCase "second_active_integrate_illegal" .sealed
      (mkWitness "b-2" .integrate (sealHash := some "seal-1") (requestId := "req-2")) false
      (workspaceSealHash := some "seal-1")
      (existing := [mkWitness "b-1" .integrate (sealHash := some "seal-1")])
  ]

theorem workspaceBindingCasesLegalCorrect :
    workspaceBindingCases.all caseBindingLegalCorrect = true := by
  native_decide

end Conformance
end Workspace
