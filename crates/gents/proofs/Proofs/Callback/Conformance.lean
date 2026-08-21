import Proofs.Callback.Properties

namespace Callback
namespace Conformance

open CallbackInvocation

structure CallbackCase where
  name : String
  invocationId : String
  ownerDeploymentId : String
  state : InvocationState
  journal : List ActionJournalState
  resultEmitted : Bool
  legal : Bool
  deriving Repr

def numberedJournal (states : List ActionJournalState) : List ActionJournalEntry :=
  go 0 states
where
  go (i : Nat) : List ActionJournalState → List ActionJournalEntry
    | [] => []
    | s :: rest => { index := i, state := s } :: go (i + 1) rest

def CallbackCase.invocation (c : CallbackCase) : CallbackInvocation :=
  { invocationId := c.invocationId
  , ownerDeploymentId := c.ownerDeploymentId
  , state := c.state
  , journal := numberedJournal c.journal
  , resultEmitted := c.resultEmitted }

def caseLegalCorrect (c : CallbackCase) : Bool :=
  c.legal == invocationLegal c.invocation

def mkCase
    (name : String)
    (state : InvocationState)
    (journal : List ActionJournalState)
    (resultEmitted legal : Bool) : CallbackCase :=
  { name := name
  , invocationId := "inv-1"
  , ownerDeploymentId := "dep-1"
  , state := state
  , journal := journal
  , resultEmitted := resultEmitted
  , legal := legal }

def callbackCases : List CallbackCase :=
  [ mkCase "happy_journal_prefix" .running
      [.resultDocsWritten, .executing] false true
  , mkCase "action_1_executing_while_0_not_result_docs_written_illegal" .running
      [.validated, .executing] false false
  , mkCase "result_emitted_while_running_illegal" .running
      [.resultDocsWritten] true false
  , mkCase "result_emitted_on_succeeded_with_complete_journal_legal" .succeeded
      [.resultDocsWritten, .resultDocsWritten] true true
  , mkCase "denied_empty_journal_legal" .denied [] false true
  , mkCase "denied_executing_journal_illegal" .denied [.executing] false false
  ]

theorem callbackCasesLegalCorrect :
    callbackCases.all caseLegalCorrect = true := by
  native_decide

def mkInv
    (invocationId ownerDeploymentId : String)
    (state : InvocationState) : CallbackInvocation :=
  { invocationId := invocationId
  , ownerDeploymentId := ownerDeploymentId
  , state := state
  , journal := []
  , resultEmitted := false }

theorem claim_unique_two_active_same_key :
    ¬ ClaimUnique
      [mkInv "inv-1" "dep-1" .running, mkInv "inv-1" "dep-1" .claimed] := by
  native_decide

theorem claim_unique_different_ids :
    ClaimUnique
      [mkInv "inv-1" "dep-1" .running, mkInv "inv-2" "dep-1" .running] := by
  native_decide

end Conformance
end Callback
