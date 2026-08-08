import Proofs.Transcript.State

/-!
# Transcript checkpoints and authoritative finalized facts

Mutable assistant assembly and immutable provider history are separate storage
domains. `AgentMessageDraft`-like rows are non-authoritative checkpoints: they
may be replaced while a turn is being assembled, are never provider input, and
are not a provenance ancestor of the published fact.

Publication has one rule whether or not a checkpoint exists. It creates an
immutable `AgentMessage`-like fact from the desired logical order and payload,
then relies on the resulting exact composite CID and verified signer evidence.
Signer authorization is an explicit policy assumption and does not require the
node signer DID to equal an `agent_did` attribution field.

The complete visible fact conflict set for `(session_id, sequence)` is an
explicit input. DefraDB local uniqueness is detection, not cross-peer
consensus, so a sibling fact makes publication and provider assembly fail
closed. Provider history is structurally typed over authoritative finalized
facts and accepts only ordered `_docID` + composite-CID references.
-/

namespace Transcript.Finalization

abbrev DocumentId := Nat
abbrev CommitCid := Nat
abbrev ContentHash := Nat
abbrev SignerDid := Nat

structure LogicalOrder where
  sessionId : SessionId
  sequence : Sequence
  deriving DecidableEq, Repr

structure DocumentVersionRef where
  documentId : DocumentId
  commitCid : CommitCid
  deriving DecidableEq, Repr

structure Payload where
  role : MessageRole
  contentHash : ContentHash
  deriving DecidableEq, Repr

/-- Mutable, non-authoritative recovery/assembly state. -/
structure Draft where
  order : LogicalOrder
  payload : Payload
  deriving DecidableEq, Repr

/-- Immutable fact plus the DefraDB evidence that makes it authoritative.
`policyAuthorized` is the deferred ACP assumption; it deliberately does not
compare `signerDid` with any application attribution field. -/
structure FinalizedFact where
  order : LogicalOrder
  payload : Payload
  commitCid : CommitCid
  signerDid : SignerDid
  signatureValid : Bool
  policyAuthorized : Bool
  deriving DecidableEq, Repr

abbrev DraftStore := DocumentId → Option Draft
abbrev FactStore := DocumentId → Option FinalizedFact

structure State where
  drafts : DraftStore
  facts : FactStore

structure DraftWrite where
  target : DocumentId
  nextPayload : Payload
  deriving DecidableEq, Repr

/-- A checkpoint writer replaces the current payload by physical document
identity. There is no caller-owned expected-CID claim: checkpoints are not a
trust root, and production converges retries toward the caller's latest
assembled payload. -/
def applyDraftUpdate (state : State) (write : DraftWrite) : Option State :=
  match state.drafts write.target with
  | some pre =>
      some
        { state with
          drafts := fun docId =>
            if docId = write.target then
              some { pre with payload := write.nextPayload }
            else
              state.drafts docId }
  | none => none

/-- Desired immutable logical fact. Checkpoint presence is intentionally not an
input. -/
structure PublishIntent where
  target : DocumentId
  order : LogicalOrder
  payload : Payload
  deriving DecidableEq, Repr

/-- Evidence produced by the successful DefraDB create. A non-zero CID and
signer stand in for non-empty production strings. -/
structure PublishEvidence where
  resultCommitCid : CommitCid
  signerDid : SignerDid
  signatureValid : Bool
  policyAuthorized : Bool
  deriving DecidableEq, Repr

def evidenceAuthoritative (evidence : PublishEvidence) : Prop :=
  evidence.resultCommitCid ≠ 0 ∧
    evidence.signerDid ≠ 0 ∧
    evidence.signatureValid = true ∧
    evidence.policyAuthorized = true

instance (evidence : PublishEvidence) : Decidable (evidenceAuthoritative evidence) := by
  unfold evidenceAuthoritative
  infer_instance

def finalizedFactFor
    (intent : PublishIntent) (evidence : PublishEvidence) : FinalizedFact :=
  { order := intent.order
  , payload := intent.payload
  , commitCid := evidence.resultCommitCid
  , signerDid := evidence.signerDid
  , signatureValid := evidence.signatureValid
  , policyAuthorized := evidence.policyAuthorized
  }

def factAuthoritative (fact : FinalizedFact) : Bool :=
  decide (fact.commitCid ≠ 0) &&
    decide (fact.signerDid ≠ 0) &&
    fact.signatureValid &&
    fact.policyAuthorized

def factMatchesIntent (fact : FinalizedFact) (intent : PublishIntent) : Bool :=
  decide (fact.order = intent.order) &&
    decide (fact.payload = intent.payload) &&
    factAuthoritative fact

/-- The sole publication rule. It does not consult or consume checkpoints. -/
def publishStrict
    (state : State)
    (visibleLogicalFacts : List DocumentId)
    (intent : PublishIntent)
    (evidence : PublishEvidence) : Option State :=
  if visibleLogicalFacts = [] ∧
      state.facts intent.target = none ∧
      evidenceAuthoritative evidence then
    some
      { state with
        facts := fun docId =>
          if docId = intent.target then some (finalizedFactFor intent evidence)
          else state.facts docId }
  else
    none

inductive CommitDisposition where
  | applied
  | observedIdentical
  | rejected
  deriving DecidableEq, Repr

structure CommitObservation where
  disposition : CommitDisposition
  state : State

/-- A retry observes the already-authoritative exact logical fact. The new
create evidence is irrelevant on replay; production verifies the existing
fact's current CID and signer instead. -/
def publishOrObserve
    (state : State)
    (visibleLogicalFacts : List DocumentId)
    (intent : PublishIntent)
    (evidence : PublishEvidence) : CommitObservation :=
  match publishStrict state visibleLogicalFacts intent evidence with
  | some post => ⟨.applied, post⟩
  | none =>
      match state.facts intent.target with
      | some existing =>
          if visibleLogicalFacts = [intent.target] ∧
              factMatchesIntent existing intent = true then
            ⟨.observedIdentical, state⟩
          else
            ⟨.rejected, state⟩
      | none => ⟨.rejected, state⟩

/-- Draft documents are absent from this resolver by construction. -/
def exactFinalized? (facts : FactStore) (ref : DocumentVersionRef) : Option FinalizedFact :=
  match facts ref.documentId with
  | some fact =>
      if fact.commitCid = ref.commitCid ∧ factAuthoritative fact = true then some fact else none
  | none => none

def sequenceFollows (previous : Option Sequence) (current : Sequence) : Bool :=
  match previous with
  | none => true
  | some value => value < current

def assembleProviderHistoryFrom
    (facts : FactStore)
    (visibleAt : LogicalOrder → List DocumentId)
    (sessionId : SessionId)
    (previousSequence : Option Sequence) :
    List DocumentVersionRef → Option (List FinalizedFact)
  | [] => some []
  | ref :: rest =>
      match exactFinalized? facts ref with
      | none => none
      | some fact =>
          if fact.order.sessionId == sessionId &&
              visibleAt fact.order == [ref.documentId] &&
              sequenceFollows previousSequence fact.order.sequence then
            match assembleProviderHistoryFrom
                facts visibleAt sessionId (some fact.order.sequence) rest with
            | some tail => some (fact :: tail)
            | none => none
          else
            none

def assembleProviderHistory
    (facts : FactStore)
    (visibleAt : LogicalOrder → List DocumentId)
    (sessionId : SessionId)
    (refs : List DocumentVersionRef) : Option (List FinalizedFact) :=
  assembleProviderHistoryFrom facts visibleAt sessionId none refs

theorem successful_draft_update_preserves_fact_store
    {state post : State} {write : DraftWrite}
    (h_apply : applyDraftUpdate state write = some post) :
    post.facts = state.facts := by
  cases h_row : state.drafts write.target with
  | none => simp [applyDraftUpdate, h_row] at h_apply
  | some draft =>
      simp [applyDraftUpdate, h_row] at h_apply
      subst post
      rfl

theorem successful_publish_is_authoritative_and_checkpoint_preserving
    {state post : State} {intent : PublishIntent} {evidence : PublishEvidence}
    (h_apply : publishStrict state [] intent evidence = some post) :
    post.facts intent.target = some (finalizedFactFor intent evidence) ∧
      post.drafts = state.drafts := by
  by_cases h_target : state.facts intent.target = none
  · by_cases h_evidence : evidenceAuthoritative evidence
    · simp [publishStrict, h_target, h_evidence] at h_apply
      subst post
      simp
    · simp [publishStrict, h_target, h_evidence] at h_apply
  · simp [publishStrict, h_target] at h_apply

theorem finalized_fact_has_no_update_transition
    {state : State} {intent : PublishIntent} {fact : FinalizedFact}
    (h_fact : state.facts intent.target = some fact) :
    ∀ evidence : PublishEvidence,
      publishStrict state [] intent evidence = none := by
  intro evidence
  simp [publishStrict, h_fact]

theorem sibling_conflict_rejects_publish
    {state : State} {intent : PublishIntent} {evidence : PublishEvidence}
    {sibling : DocumentId} :
    publishStrict state [intent.target, sibling] intent evidence = none := by
  simp [publishStrict]

theorem invalid_evidence_rejects_publish
    {state : State} {intent : PublishIntent} {evidence : PublishEvidence}
    (h_invalid : ¬ evidenceAuthoritative evidence) :
    publishStrict state [] intent evidence = none := by
  simp [publishStrict, h_invalid]

theorem observed_identical_is_non_mutating
    {state : State} {intent : PublishIntent} {evidence : PublishEvidence}
    (_h_observed : (publishOrObserve state [intent.target] intent evidence).disposition =
      .observedIdentical) :
    (publishOrObserve state [intent.target] intent evidence).state = state := by
  have h_strict : publishStrict state [intent.target] intent evidence = none := by
    simp [publishStrict]
  cases h_fact : state.facts intent.target with
  | none => simp [publishOrObserve, h_strict, h_fact]
  | some existing =>
      by_cases h_match : factMatchesIntent existing intent = true
      · simp [publishOrObserve, h_strict, h_fact, h_match]
      · simp [publishOrObserve, h_strict, h_fact, h_match]

theorem conflicting_publish_replay_is_rejected
    {state : State} {intent : PublishIntent} {evidence : PublishEvidence}
    {existing : FinalizedFact}
    (h_existing : state.facts intent.target = some existing)
    (h_conflict : factMatchesIntent existing intent = false) :
    (publishOrObserve state [intent.target] intent evidence).disposition = .rejected := by
  have h_strict : publishStrict state [intent.target] intent evidence = none := by
    simp [publishStrict]
  simp [publishOrObserve, h_strict, h_existing, h_conflict]

theorem exact_finalized_rejects_wrong_cid
    {facts : FactStore} {fact : FinalizedFact} {ref : DocumentVersionRef}
    (h_fact : facts ref.documentId = some fact)
    (h_wrong : fact.commitCid ≠ ref.commitCid) :
    exactFinalized? facts ref = none := by
  simp [exactFinalized?, h_fact, h_wrong]

theorem provider_rejects_visible_sibling_at_head
    {facts : FactStore} {visibleAt : LogicalOrder → List DocumentId}
    {sessionId : SessionId} {ref : DocumentVersionRef} {rest : List DocumentVersionRef}
    {fact : FinalizedFact} {sibling : DocumentId}
    (h_exact : exactFinalized? facts ref = some fact)
    (h_conflict : visibleAt fact.order = [ref.documentId, sibling]) :
    assembleProviderHistory facts visibleAt sessionId (ref :: rest) = none := by
  simp [assembleProviderHistory, assembleProviderHistoryFrom, h_exact, h_conflict]

end Transcript.Finalization
