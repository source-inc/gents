import Proofs.Basic

inductive InvocationState where
  | pending
  | claimed
  | running
  | succeeded
  | failed
  | denied
  deriving DecidableEq, Repr

namespace InvocationState

def toDefraDB : InvocationState → String
  | .pending => "pending"
  | .claimed => "claimed"
  | .running => "running"
  | .succeeded => "succeeded"
  | .failed => "failed"
  | .denied => "denied"

def fromDefraDB? : String → Option InvocationState
  | "pending" => some .pending
  | "claimed" => some .claimed
  | "running" => some .running
  | "succeeded" => some .succeeded
  | "failed" => some .failed
  | "denied" => some .denied
  | _ => none

theorem fromDefraDB_toDefraDB (s : InvocationState) :
    fromDefraDB? s.toDefraDB = some s := by
  cases s <;> rfl

instance : HasTerminal InvocationState where
  isTerminal s := s = .succeeded ∨ s = .failed ∨ s = .denied
  isTerminal_dec s :=
    match s with
    | .succeeded => isTrue (Or.inl rfl)
    | .failed => isTrue (Or.inr (Or.inl rfl))
    | .denied => isTrue (Or.inr (Or.inr rfl))
    | .pending => isFalse (by
        intro h
        cases h with
        | inl h => cases h
        | inr h =>
            cases h with
            | inl h => cases h
            | inr h => cases h)
    | .claimed => isFalse (by
        intro h
        cases h with
        | inl h => cases h
        | inr h =>
            cases h with
            | inl h => cases h
            | inr h => cases h)
    | .running => isFalse (by
        intro h
        cases h with
        | inl h => cases h
        | inr h =>
            cases h with
            | inl h => cases h
            | inr h => cases h)

end InvocationState

inductive ActionJournalState where
  | validated
  | executing
  | effectObserved
  | resultDocsWritten
  deriving DecidableEq, Repr

namespace ActionJournalState

def toDefraDB : ActionJournalState → String
  | .validated => "validated"
  | .executing => "executing"
  | .effectObserved => "effectObserved"
  | .resultDocsWritten => "resultDocsWritten"

def fromDefraDB? : String → Option ActionJournalState
  | "validated" => some .validated
  | "executing" => some .executing
  | "effectObserved" => some .effectObserved
  | "resultDocsWritten" => some .resultDocsWritten
  | _ => none

theorem fromDefraDB_toDefraDB (s : ActionJournalState) :
    fromDefraDB? s.toDefraDB = some s := by
  cases s <;> rfl

def laterThanValidated : ActionJournalState → Bool
  | .validated => false
  | .executing | .effectObserved | .resultDocsWritten => true

end ActionJournalState

structure ActionJournalEntry where
  index : Nat
  state : ActionJournalState
  deriving DecidableEq, Repr

structure CallbackInvocation where
  invocationId : String
  ownerDeploymentId : String
  state : InvocationState
  journal : List ActionJournalEntry
  resultEmitted : Bool
  deriving DecidableEq, Repr
