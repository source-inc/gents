import Proofs.Basic

inductive WorkspaceState where
  | provisioning
  | ready
  | provisionFailed
  | sealed
  | cleaning
  | cleaned
  deriving DecidableEq, Repr

namespace WorkspaceState

def toDefraDB : WorkspaceState → String
  | .provisioning => "provisioning"
  | .ready => "ready"
  | .provisionFailed => "provisionFailed"
  | .sealed => "sealed"
  | .cleaning => "cleaning"
  | .cleaned => "cleaned"

def fromDefraDB? : String → Option WorkspaceState
  | "provisioning" => some .provisioning
  | "ready" => some .ready
  | "provisionFailed" => some .provisionFailed
  | "sealed" => some .sealed
  | "cleaning" => some .cleaning
  | "cleaned" => some .cleaned
  | _ => none

theorem fromDefraDB_toDefraDB (s : WorkspaceState) :
    fromDefraDB? s.toDefraDB = some s := by
  cases s <;> rfl

instance : HasTerminal WorkspaceState where
  isTerminal s := s = .provisionFailed ∨ s = .cleaned
  isTerminal_dec s :=
    match s with
    | .provisionFailed => isTrue (Or.inl rfl)
    | .cleaned => isTrue (Or.inr rfl)
    | .provisioning => isFalse (by
        intro h
        cases h with
        | inl h => cases h
        | inr h => cases h)
    | .ready => isFalse (by
        intro h
        cases h with
        | inl h => cases h
        | inr h => cases h)
    | .sealed => isFalse (by
        intro h
        cases h with
        | inl h => cases h
        | inr h => cases h)
    | .cleaning => isFalse (by
        intro h
        cases h with
        | inl h => cases h
        | inr h => cases h)

def bindable : WorkspaceState → Prop
  | .ready | .sealed => True
  | .provisioning | .provisionFailed | .cleaning | .cleaned => False

instance : DecidablePred bindable := fun s =>
  match s with
  | .ready => isTrue trivial
  | .sealed => isTrue trivial
  | .provisioning => isFalse (fun h => h)
  | .provisionFailed => isFalse (fun h => h)
  | .cleaning => isFalse (fun h => h)
  | .cleaned => isFalse (fun h => h)

end WorkspaceState

inductive BindingAuthority where
  | readOnly
  | readWrite
  | integrate
  deriving DecidableEq, Repr

namespace BindingAuthority

def toDefraDB : BindingAuthority → String
  | .readOnly => "readOnly"
  | .readWrite => "readWrite"
  | .integrate => "integrate"

def fromDefraDB? : String → Option BindingAuthority
  | "readOnly" => some .readOnly
  | "readWrite" => some .readWrite
  | "integrate" => some .integrate
  | _ => none

theorem fromDefraDB_toDefraDB (a : BindingAuthority) :
    fromDefraDB? a.toDefraDB = some a := by
  cases a <;> rfl

end BindingAuthority

inductive BindingState where
  | active
  | released
  | denied
  deriving DecidableEq, Repr

namespace BindingState

def toDefraDB : BindingState → String
  | .active => "active"
  | .released => "released"
  | .denied => "denied"

def fromDefraDB? : String → Option BindingState
  | "active" => some .active
  | "released" => some .released
  | "denied" => some .denied
  | _ => none

theorem fromDefraDB_toDefraDB (s : BindingState) :
    fromDefraDB? s.toDefraDB = some s := by
  cases s <;> rfl

end BindingState

inductive CreationPolicy where
  | gitWorktreeDiff
  | isolatedClone
  deriving DecidableEq, Repr

namespace CreationPolicy

def toDefraDB : CreationPolicy → String
  | .gitWorktreeDiff => "git_worktree_diff"
  | .isolatedClone => "isolated_clone"

def fromDefraDB? : String → Option CreationPolicy
  | "git_worktree_diff" => some .gitWorktreeDiff
  | "isolated_clone" => some .isolatedClone
  | _ => none

theorem fromDefraDB_toDefraDB (p : CreationPolicy) :
    fromDefraDB? p.toDefraDB = some p := by
  cases p <;> rfl

end CreationPolicy

/-- Logical workspace identity. Replicated rows never carry a host path. -/
structure IsolatedWorkspace where
  workspaceId : String
  workUnitId : String
  repositoryId : String
  baseSha : String
  branch : String
  creationPolicy : CreationPolicy
  ownerDeploymentId : String
  sealHash : Option String
  state : WorkspaceState
  deriving DecidableEq, Repr

structure WorkspaceBinding where
  bindingId : String
  workspaceId : String
  requestId : String
  authority : BindingAuthority
  deploymentId : String
  sealHash : Option String
  state : BindingState
  deriving DecidableEq, Repr
