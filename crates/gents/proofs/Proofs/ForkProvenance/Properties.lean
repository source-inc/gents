import Proofs.ForkProvenance.Transition

namespace ForkProvenance

open ToolFact

theorem twins_rejected
    (state : State) (intent : ChildIntent) (evidence : SignedRef)
    (source : SourceFact)
    (h_source : exactSource? state.sources intent.source = some source)
    (left right : Nat) :
    (commitChild state [left, right] intent evidence).disposition = .rejected := by
  simp [commitChild, h_source]
  split
  · cases h_child : state.children evidence.version.docId <;> simp [h_child]
  · rfl

theorem same_session_rejected
    (state : State) (intent : ChildIntent) (evidence : SignedRef)
    (source : SourceFact)
    (h_source : exactSource? state.sources intent.source = some source)
    (h_same : intent.sourceSessionId = intent.childSessionId) :
    (commitChild state [] intent evidence).disposition = .rejected := by
  simp [commitChild, h_source, h_same]

theorem wrong_node_signer_rejected
    (state : State) (intent : ChildIntent) (evidence : SignedRef)
    (source : SourceFact)
    (h_source : exactSource? state.sources intent.source = some source)
    (h_wrong : evidence.signerDid ≠ intent.nodeSignerDid) :
    (commitChild state [] intent evidence).disposition = .rejected := by
  simp [commitChild, h_source, h_wrong]

theorem unsigned_source_rejected
    (state : State) (intent : ChildIntent) (evidence : SignedRef)
    (h_source : exactSource? state.sources intent.source = none) :
    (commitChild state [] intent evidence).disposition = .rejected := by
  simp [commitChild, h_source]

theorem replay_idempotent
    (state : State) (intent : ChildIntent) (evidence : SignedRef)
    (source : SourceFact)
    (h_source : exactSource? state.sources intent.source = some source)
    (h_admit : source.kind = intent.kind ∧
      source.sessionId = intent.sourceSessionId ∧
      source.payloadHash = intent.payloadHash ∧
      intent.sourceSessionId ≠ intent.childSessionId ∧
      intent.childSessionId ≠ 0 ∧ intent.nodeSignerDid ≠ 0 ∧
      evidence.authoritative = true ∧
      evidence.signerDid = intent.nodeSignerDid ∧
      childCallValid state intent = true)
    (h_existing : state.children evidence.version.docId =
      some (ChildFact.forIntent intent evidence)) :
    commitChild state [evidence.version.docId] intent evidence =
      ⟨.observedIdentical, state⟩ := by
  simp [commitChild, h_source, h_admit, h_existing]

theorem admitted_child_pins_exact_source
    (state : State) (intent : ChildIntent) (evidence : SignedRef)
    (h_applied : (commitChild state [] intent evidence).disposition = .applied) :
    (commitChild state [] intent evidence).state.children evidence.version.docId =
      some (ChildFact.forIntent intent evidence) := by
  unfold commitChild at h_applied
  split at h_applied
  · simp at h_applied
  · split at h_applied
    · by_cases h_empty : state.children evidence.version.docId = none
      · simp [commitChild, *, h_empty]
      · simp [h_empty] at h_applied
        cases h_child : state.children evidence.version.docId <;> simp_all
    · simp at h_applied

end ForkProvenance
