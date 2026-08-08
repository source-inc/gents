import Proofs.ToolFact.State

namespace ToolFact

inductive CommitDisposition where
  | applied
  | observedIdentical
  | rejected
  deriving DecidableEq, Repr

namespace CommitDisposition

def toContract : CommitDisposition → String
  | .applied => "applied"
  | .observedIdentical => "observed_identical"
  | .rejected => "rejected"

end CommitDisposition

structure CommitObservation where
  disposition : CommitDisposition
  state : State

/-- Tool invocations are immutable signed facts too. The mutable execution
lifecycle may point at this fact, but it cannot replace its arguments. -/
def commitCall
    (state : State)
    (visibleLogicalDocs : List Nat)
    (intent : ToolCallIntent)
    (evidence : SignedRef) : CommitObservation :=
  let target := evidence.version.docId
  if intent.argsHash ≠ 0 ∧ evidence.authoritative = true then
    if visibleLogicalDocs = [] ∧ state.calls target = none then
      let fact := ToolCallFact.forIntent intent evidence
      ⟨.applied, { state with calls := Store.bind state.calls target fact }⟩
    else
      match state.calls target with
      | some existing =>
          if visibleLogicalDocs = [target] ∧
              existing = ToolCallFact.forIntent intent evidence then
            ⟨.observedIdentical, state⟩
          else
            ⟨.rejected, state⟩
      | none => ⟨.rejected, state⟩
  else
    ⟨.rejected, state⟩

/-- Create a full immutable result, or observe the one identical physical fact.
The complete visible logical-key conflict set is an explicit input: zero rows
permits create, exactly the target permits replay, and twins fail closed. -/
def commitResult
    (state : State)
    (visibleLogicalDocs : List Nat)
    (intent : ToolResultIntent)
    (evidence : SignedRef) : CommitObservation :=
  let target := evidence.version.docId
  match exactCall? state.calls intent.call with
  | none => ⟨.rejected, state⟩
  | some _ =>
      if intent.fullOutput = true ∧ intent.outputHash ≠ 0 ∧
          evidence.authoritative = true then
        if visibleLogicalDocs = [] ∧ state.results target = none then
          let fact := ToolResultFact.forIntent intent evidence
          ⟨.applied,
            { state with results := Store.bind state.results target fact }⟩
        else
          match state.results target with
          | some existing =>
              if visibleLogicalDocs = [target] ∧
                  existing = ToolResultFact.forIntent intent evidence then
                ⟨.observedIdentical, state⟩
              else
                ⟨.rejected, state⟩
          | none => ⟨.rejected, state⟩
      else
        ⟨.rejected, state⟩

/-- Approval publication has the same immutable create-or-observe discipline
and requires the exact signed tool-call parent. -/
def commitApproval
    (state : State)
    (visibleLogicalDocs : List Nat)
    (intent : ToolApprovalIntent)
    (evidence : SignedRef) : CommitObservation :=
  let target := evidence.version.docId
  match exactCall? state.calls intent.call with
  | none => ⟨.rejected, state⟩
  | some _ =>
      if evidence.authoritative = true then
        if visibleLogicalDocs = [] ∧ state.approvals target = none then
          let fact := ToolApprovalFact.forIntent intent evidence
          ⟨.applied,
            { state with approvals := Store.bind state.approvals target fact }⟩
        else
          match state.approvals target with
          | some existing =>
              if visibleLogicalDocs = [target] ∧
                  existing = ToolApprovalFact.forIntent intent evidence then
                ⟨.observedIdentical, state⟩
              else
                ⟨.rejected, state⟩
          | none => ⟨.rejected, state⟩
      else
        ⟨.rejected, state⟩

/-- Exact references stored by one transcript/tool projection row. -/
structure TranscriptJoin where
  call : SignedRef
  result : SignedRef
  approval : Option SignedRef
  deriving DecidableEq, Repr

structure Projection where
  call : ToolCallFact
  result : ToolResultFact
  approval : Option ToolApprovalFact
  deriving DecidableEq, Repr

/-- Projection never guesses through a logical id or a newer collection head.
All exact physical refs must resolve and child facts must point back to the
same exact signed call ref. -/
def projectExact (state : State) (join : TranscriptJoin) : Option Projection :=
  match exactCall? state.calls join.call, exactResult? state.results join.result with
  | some call, some result =>
      if result.call = join.call then
        match join.approval with
        | none => some { call := call, result := result, approval := none }
        | some approvalRef =>
            match exactApproval? state.approvals approvalRef with
            | some approval =>
                if approval.call = join.call then
                  some { call := call, result := result, approval := some approval }
                else none
            | none => none
      else none
  | _, _ => none

end ToolFact
