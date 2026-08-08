import Proofs.ForkProvenance.State

namespace ForkProvenance

open ToolFact

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

/-- Mint a child fact from one exact signed source snapshot.  The complete
logical-key conflict set is explicit so replicated twins cannot be hidden by a
`limit: 1` query. -/
def commitChild
    (state : State)
    (visibleLogicalDocs : List Nat)
    (intent : ChildIntent)
    (evidence : SignedRef) : CommitObservation :=
  let target := evidence.version.docId
  match exactSource? state.sources intent.source with
  | none => ⟨.rejected, state⟩
  | some source =>
      if source.kind = intent.kind ∧
          source.sessionId = intent.sourceSessionId ∧
          source.payloadHash = intent.payloadHash ∧
          intent.sourceSessionId ≠ intent.childSessionId ∧
          intent.childSessionId ≠ 0 ∧
          intent.nodeSignerDid ≠ 0 ∧
          evidence.authoritative = true ∧
          evidence.signerDid = intent.nodeSignerDid ∧
          childCallValid state intent = true then
        if visibleLogicalDocs = [] ∧ state.children target = none then
          let fact := ChildFact.forIntent intent evidence
          ⟨.applied,
            { state with children := Store.bind state.children target fact }⟩
        else
          match state.children target with
          | some existing =>
              if visibleLogicalDocs = [target] ∧
                  existing = ChildFact.forIntent intent evidence then
                ⟨.observedIdentical, state⟩
              else
                ⟨.rejected, state⟩
          | none => ⟨.rejected, state⟩
      else
        ⟨.rejected, state⟩

end ForkProvenance
