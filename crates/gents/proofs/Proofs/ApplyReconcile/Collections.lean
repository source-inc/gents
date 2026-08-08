import Proofs.Basic
import Proofs.RuntimeReconcile
import Mathlib.Data.Finset.Basic
import Mathlib.Data.Finset.Image
import Mathlib.Data.Finset.SDiff

namespace ApplyReconcile

inductive Collection where
  | agentPrincipal
  | agentBehavior
  | skill
  | datastoreToolSurface
  | toolSelection
  | inferenceBackend
  | inferenceProfile
  | toolServiceRegistry
  | projectionAcpBinding
  | peerPairingDesired
  | task
  | schedule
  | eventTrigger
  deriving DecidableEq, Repr

def Collection.applyOrder : Collection → Nat
  | .inferenceBackend      => 0
  | .toolSelection         => 0
  | .inferenceProfile      => 0
  | .toolServiceRegistry   => 0
  | .skill                 => 0
  | .datastoreToolSurface  => 0
  | .peerPairingDesired    => 0
  | .agentBehavior         => 1
  | .projectionAcpBinding  => 2
  | .task                  => 2
  | .schedule              => 2
  | .agentPrincipal        => 3
  | .eventTrigger          => 3

def Collection.manifestAuthoritative : Collection → Bool
  | .peerPairingDesired => true
  | _ => false

instance : LT Collection where
  lt a b := Collection.applyOrder a < Collection.applyOrder b

instance : LE Collection where
  le a b := Collection.applyOrder a ≤ Collection.applyOrder b

instance (a b : Collection) : Decidable (a < b) :=
  Nat.decLt (Collection.applyOrder a) (Collection.applyOrder b)

instance (a b : Collection) : Decidable (a ≤ b) :=
  Nat.decLe (Collection.applyOrder a) (Collection.applyOrder b)

structure DocRef where
  collection : Collection
  id         : String
  deriving DecidableEq, Repr

def DocRef.le (a b : DocRef) : Bool :=
  if a.collection.applyOrder < b.collection.applyOrder then true
  else if a.collection.applyOrder > b.collection.applyOrder then false
  else a.id ≤ b.id

instance : LE DocRef where
  le a b := DocRef.le a b = true

instance (a b : DocRef) : Decidable (a ≤ b) := by
  unfold LE.le instLEDocRef
  infer_instance

example (c : Collection) : Nat :=
  match c with
  | .agentPrincipal       => 3
  | .agentBehavior        => 1
  | .skill                => 0
  | .datastoreToolSurface => 0
  | .toolSelection        => 0
  | .inferenceBackend     => 0
  | .inferenceProfile     => 0
  | .toolServiceRegistry  => 0
  | .projectionAcpBinding => 2
  | .peerPairingDesired   => 0
  | .task                 => 2
  | .schedule             => 2
  | .eventTrigger         => 3

theorem applyOrder_matches_parity_contract : ∀ c : Collection,
    Collection.applyOrder c =
      (match c with
       | .agentPrincipal       => 3
       | .agentBehavior        => 1
       | .skill                => 0
       | .datastoreToolSurface => 0
       | .toolSelection        => 0
       | .inferenceBackend     => 0
       | .inferenceProfile     => 0
       | .toolServiceRegistry  => 0
       | .projectionAcpBinding => 2
       | .peerPairingDesired   => 0
       | .task                 => 2
       | .schedule             => 2
       | .eventTrigger         => 3) := by
  intro c
  cases c <;> rfl

end ApplyReconcile
