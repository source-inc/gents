import Proofs.Workspace.Transition
import Proofs.CommandPolicy.Types

namespace IsolatedWorkspace

open CommandPolicy

def ReadWriteOk (w : IsolatedWorkspace) (b : WorkspaceBinding) : Prop :=
  (b.authority = .readWrite ∧ b.state = .active) → w.state = .ready

instance (w : IsolatedWorkspace) (b : WorkspaceBinding) :
    Decidable (ReadWriteOk w b) := by
  unfold ReadWriteOk
  infer_instance

def activeReadWriteCount (workspaceId : String) (bindings : List WorkspaceBinding) : Nat :=
  (bindings.filter fun b =>
      decide (b.workspaceId = workspaceId) &&
        decide (b.authority = .readWrite) &&
        decide (b.state = .active)).length

def UniqueActiveReadWrite (workspaceId : String) (bindings : List WorkspaceBinding) : Prop :=
  activeReadWriteCount workspaceId bindings ≤ 1

instance (workspaceId : String) (bindings : List WorkspaceBinding) :
    Decidable (UniqueActiveReadWrite workspaceId bindings) :=
  Nat.decLe (activeReadWriteCount workspaceId bindings) 1

def ReadOnlyAllowed (w : IsolatedWorkspace) : Prop :=
  w.state = .ready ∨ w.state = .sealed

instance (w : IsolatedWorkspace) : Decidable (ReadOnlyAllowed w) := by
  unfold ReadOnlyAllowed
  infer_instance

def ReadOnlyOk (w : IsolatedWorkspace) (b : WorkspaceBinding) : Prop :=
  b.authority = .readOnly →
    ReadOnlyAllowed w ∧ (w.state = .sealed → b.sealHash = w.sealHash)

instance (w : IsolatedWorkspace) (b : WorkspaceBinding) :
    Decidable (ReadOnlyOk w b) := by
  unfold ReadOnlyOk
  infer_instance

def ReadOnlyConcurrent (w : IsolatedWorkspace) (bindings : List WorkspaceBinding) : Prop :=
  ReadOnlyAllowed w →
    bindings.all (fun b =>
      !decide (b.authority = .readOnly ∧ b.state = .active) ||
        decide (ReadOnlyOk w b)) = true

instance (w : IsolatedWorkspace) (bindings : List WorkspaceBinding) :
    Decidable (ReadOnlyConcurrent w bindings) := by
  unfold ReadOnlyConcurrent
  infer_instance

def IntegrateOk (w : IsolatedWorkspace) (b : WorkspaceBinding) : Prop :=
  b.authority = .integrate → w.state = .sealed ∧ b.sealHash = w.sealHash

instance (w : IsolatedWorkspace) (b : WorkspaceBinding) :
    Decidable (IntegrateOk w b) := by
  unfold IntegrateOk
  infer_instance

def OwnerClaimable (deploymentId : String) (w : IsolatedWorkspace) : Prop :=
  w.ownerDeploymentId = deploymentId ∧ WorkspaceState.bindable w.state

instance (deploymentId : String) (w : IsolatedWorkspace) :
    Decidable (OwnerClaimable deploymentId w) := by
  unfold OwnerClaimable
  infer_instance

def GitMetadataWriteOk (policy : CreationPolicy) (authority : BindingAuthority) : Prop :=
  ¬ (policy = .gitWorktreeDiff ∧ authority = .readWrite)

instance (policy : CreationPolicy) (authority : BindingAuthority) :
    Decidable (GitMetadataWriteOk policy authority) := by
  unfold GitMetadataWriteOk
  infer_instance

def BindingAuthority.commandMode : BindingAuthority → ExecutionMode
  | .readOnly => .readOnly
  | .readWrite => .workspaceWrite
  | .integrate => .readOnly

def modeRank : ExecutionMode → Nat
  | .readOnly => 0
  | .workspaceWrite => 1
  | .unrestricted => 2

def authorityMeet (behavior : ExecutionMode) (authority : BindingAuthority) : ExecutionMode :=
  let cap := BindingAuthority.commandMode authority
  if modeRank behavior ≤ modeRank cap then behavior else cap

def AuthorityMeetOk (behavior : ExecutionMode) (authority : BindingAuthority) : Prop :=
  ¬ (authority = .readWrite ∧ authorityMeet behavior authority = .unrestricted)

instance (behavior : ExecutionMode) (authority : BindingAuthority) :
    Decidable (AuthorityMeetOk behavior authority) := by
  unfold AuthorityMeetOk
  infer_instance

theorem identity_fields_preserved
    {pre post : IsolatedWorkspace}
    (h : Transition pre post) :
    post.workspaceId = pre.workspaceId ∧
    post.workUnitId = pre.workUnitId ∧
    post.repositoryId = pre.repositoryId ∧
    post.baseSha = pre.baseSha ∧
    post.branch = pre.branch ∧
    post.creationPolicy = pre.creationPolicy ∧
    post.ownerDeploymentId = pre.ownerDeploymentId := by
  cases h <;> simp_all

theorem seal_requires_hash
    {pre post : IsolatedWorkspace}
    (h : Transition pre post)
    (hsealed : post.state = .sealed) :
    pre.sealHash.isSome = true := by
  cases h <;> simp_all

theorem sealed_not_terminal : ¬ isTerminal WorkspaceState.sealed := by
  decide

theorem provisionFailed_terminal : isTerminal WorkspaceState.provisionFailed := by
  decide

theorem cleaned_terminal : isTerminal WorkspaceState.cleaned := by
  decide

theorem sealed_not_cleaned
    {pre post : IsolatedWorkspace}
    (h : Transition pre post)
    (hpre : pre.state = .sealed) :
    post.state ≠ .cleaned := by
  cases h <;> simp_all

theorem git_worktree_diff_readWrite_denies_git_metadata :
    ¬ GitMetadataWriteOk .gitWorktreeDiff .readWrite := by
  decide

theorem meet_readWrite_ne_unrestricted (behavior : ExecutionMode) :
    authorityMeet behavior .readWrite ≠ .unrestricted := by
  cases behavior <;> simp [authorityMeet, BindingAuthority.commandMode, modeRank]

theorem meet_unrestricted_readWrite_is_workspaceWrite :
    authorityMeet .unrestricted .readWrite = .workspaceWrite := by
  simp [authorityMeet, BindingAuthority.commandMode, modeRank]

theorem AuthorityMeetOk_readWrite (behavior : ExecutionMode) :
    AuthorityMeetOk behavior .readWrite := by
  intro h
  exact meet_readWrite_ne_unrestricted behavior h.2

theorem unique_active_readWrite_nil (workspaceId : String) :
    UniqueActiveReadWrite workspaceId [] := by
  simp [UniqueActiveReadWrite, activeReadWriteCount]

theorem readWrite_active_requires_ready
    (w : IsolatedWorkspace) (b : WorkspaceBinding)
    (h : ReadWriteOk w b)
    (hauth : b.authority = .readWrite)
    (hactive : b.state = .active) :
    w.state = .ready :=
  h ⟨hauth, hactive⟩

theorem integrate_requires_matching_seal
    (w : IsolatedWorkspace) (b : WorkspaceBinding)
    (h : IntegrateOk w b)
    (hauth : b.authority = .integrate) :
    w.state = .sealed ∧ b.sealHash = w.sealHash :=
  h hauth

theorem owner_claimable_requires_owner
    (deploymentId : String) (w : IsolatedWorkspace)
    (h : OwnerClaimable deploymentId w) :
    w.ownerDeploymentId = deploymentId :=
  h.1

theorem owner_claimable_requires_bindable
    (deploymentId : String) (w : IsolatedWorkspace)
    (h : OwnerClaimable deploymentId w) :
    WorkspaceState.bindable w.state :=
  h.2

theorem step?_of_legal
    (pre : IsolatedWorkspace) (postState : WorkspaceState)
    (h : transitionLegal pre postState = true) :
    step? pre postState = some { pre with state := postState } := by
  simp [step?, h]

end IsolatedWorkspace
