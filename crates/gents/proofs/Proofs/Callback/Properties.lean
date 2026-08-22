import Proofs.Callback.Transition

namespace CallbackInvocation

def journalPrefixOk : List ActionJournalEntry → Bool
  | [] => true
  | [_] => true
  | a :: b :: rest =>
      (!ActionJournalState.laterThanValidated b.state ||
        decide (a.state = .resultDocsWritten)) &&
      journalPrefixOk (b :: rest)

def journalPrefix (journal : List ActionJournalEntry) : Prop :=
  journalPrefixOk journal = true

instance (journal : List ActionJournalEntry) : Decidable (journalPrefix journal) :=
  inferInstanceAs (Decidable (_ = true))

def resultEmittedOk (inv : CallbackInvocation) : Bool :=
  !inv.resultEmitted ||
    (decide (inv.state = .succeeded) &&
      inv.journal.all (fun e => decide (e.state = .resultDocsWritten)))

def deniedFailedNoExecute (inv : CallbackInvocation) : Bool :=
  !(decide (inv.state = .denied) || decide (inv.state = .failed)) ||
    inv.journal.isEmpty

def invocationLegal (inv : CallbackInvocation) : Bool :=
  journalPrefixOk inv.journal && resultEmittedOk inv && deniedFailedNoExecute inv

def activeClaim (inv : CallbackInvocation) : Bool :=
  decide (inv.state = .claimed) || decide (inv.state = .running)

def countActive (ownerId invocationId : String) (invs : List CallbackInvocation) : Nat :=
  (invs.filter fun inv =>
      activeClaim inv &&
        decide (inv.ownerDeploymentId = ownerId) &&
        decide (inv.invocationId = invocationId)).length

def ClaimUnique (invs : List CallbackInvocation) : Prop :=
  invs.all (fun inv => decide (countActive inv.ownerDeploymentId inv.invocationId invs ≤ 1)) = true

instance (invs : List CallbackInvocation) : Decidable (ClaimUnique invs) :=
  inferInstanceAs (Decidable (_ = true))

theorem identity_fields_preserved
    {pre post : CallbackInvocation}
    (h : Transition pre post) :
    post.invocationId = pre.invocationId ∧
    post.ownerDeploymentId = pre.ownerDeploymentId := by
  cases h <;> simp_all

theorem denied_failed_keep_empty_journal
    {pre post : CallbackInvocation}
    (h : Transition pre post)
    (hterm : post.state = .denied ∨ post.state = .failed) :
    post.journal = [] ∧ post.resultEmitted = false := by
  cases h with
  | claim _ hpost =>
      simp [hpost] at hterm
  | run _ hpost =>
      simp [hpost] at hterm
  | succeed _ _ hpost =>
      simp [hpost] at hterm
  | fail _ hjournal hpost =>
      simp [hpost, hjournal]
  | deny_claimed _ hjournal hpost =>
      simp [hpost, hjournal]
  | deny_running _ hjournal hpost =>
      simp [hpost, hjournal]

theorem result_emitted_only_on_success
    (inv : CallbackInvocation)
    (h : resultEmittedOk inv = true)
    (hemitted : inv.resultEmitted = true) :
    inv.state = .succeeded ∧
      ∀ e ∈ inv.journal, e.state = .resultDocsWritten := by
  simp [resultEmittedOk, hemitted] at h
  exact h

theorem journal_prefix_blocks_early_execute :
    journalPrefixOk
      [{ index := 0, state := .validated }, { index := 1, state := .executing }] = false := by
  native_decide

theorem journal_prefix_allows_written_then_executing :
    journalPrefixOk
      [{ index := 0, state := .resultDocsWritten }, { index := 1, state := .executing }] =
      true := by
  native_decide

theorem claim_unique_nil : ClaimUnique [] := by
  simp [ClaimUnique]

theorem succeeded_not_pending : InvocationState.succeeded ≠ .pending := by
  decide

end CallbackInvocation
