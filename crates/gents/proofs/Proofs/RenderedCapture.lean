import Proofs.Basic

/-!
# RenderedCapture — persist-before-send at the provider boundary (#840, #523)

The owned completion loop assembles a provider request, arms this attempt's
capture (`on_rendered_request`, `crates/gents/src/agent/loop_stream.rs`), and
the innermost HTTP transport then persists the body it is about to post before
it posts it (`crates/gents/src/rendered_request/transport.rs`). This model
fences the *order*: a provider send is legal only after the matching
`(capture key, canonical request)` pair is durable, and one capture key never
names two different canonical requests.

## What is modeled, and what is deliberately not

* The provider body inside `CanonicalRequest` is **opaque**. The model never
  looks inside it; it only ever compares two bodies for equality. A capture also
  carries the exact DefraDB request-document version that supplied its input
  and a bounded resolved-config manifest: four required signed exact refs,
  optional tool-selection evidence, and canonically ordered signed skill refs.
  That is the whole point of stating
  `capture_key_determines_request` over the request *value* rather than over a
  stored digest: DefraDB already supplies a content address for the persisted
  field as a per-field commit, so a modeled hash column would add an unmodeled
  assumption — that the writer computed the digest honestly — to a theorem that
  does not need one.

* `CaptureKey` is a **structured tuple**, not a delimited string. Componentwise
  decidable equality is what makes "the same key" mean "the same five facts".
  The durable column is a string, and that encoding must be injective on this
  tuple; the model does not prove it
  (`boundary.rendered-capture.key-encoding-injectivity`). This matters: the one
  composite key gents already ships, `AgentToolCall.tool_call_key`, is an
  unescaped `"{session_id}:{tool_call_id}"` concatenation over a caller-supplied
  `session_id`, so a delimited encoding is a live defect class here rather than
  a hypothetical one.

* `requestId` is the **DefraDB document identity** of the durable request, widened
  with the provider-call scope inside that document. It is not the non-unique
  logical `AgentRequest.request_id` field. One request runs several completion loops — the owned
  inference loop, the per-turn compaction summarizer plus its strict-JSON
  fallback, conversation title generation — and each starts its own `turnIndex`
  and `attempt` at zero, so the request document id alone does not identify a
  provider attempt. Production encodes this component as the injective JSON pair
  `[request_doc_id, capture_scope]`, keeping the tuple five components wide;
  `crates/gents/tests/conformance/rendered_capture.rs` fences both halves.

* `CanonicalRequest` is opaque, so the model does not say *which* artifact the
  implementation binds to a key — only that a key binds one of them. Production
  binds the **serialized HTTP body at the transport seam**
  (`crates/gents/src/rendered_request/transport.rs`), captured after the
  ChatGPT-Codex and xAI Grok body rewrites and immediately before the network
  client is called. `boundary.rendered-capture.assembled-request-artifact`,
  which recorded the earlier assembled-request binding, is closed on the
  production side by that move; capture and send are now the same function call,
  in that order.

* `attempt` is part of the key, so a repair retry whose assembled input
  legitimately differs is a *separate fact*, never a rebinding of the previous
  one (`attempt_distinguishes_facts`).

## Properties

* `sent_implies_durably_captured` — reaching `sent` forces the store to bind the
  key to exactly this request.
* `sent_requires_a_capture_step` — stronger: every `assembled → … → sent` trace
  contains an intervening successful capture step for this `(key, request)`.
* `capture_key_determines_request` — one key binds at most one canonical
  request, for the whole life of the store.
* `capture_idempotent`, `capture_rejects_rebinding` — redelivery of an identical
  capture succeeds without a write; a conflicting one is an integrity error and
  never an update.
* `capture_rejects_request_provenance_rebinding` — identical provider bytes do
  not make different signed source/claim chains the same fact.
* `capture_rejects_config_provenance_rebinding` — identical provider bytes and
  request provenance do not make different resolved config inputs the same
  fact; required-missing or malformed references fail closed in their scope.
* `reconciled_document_send_implies_exact_config_provenance` — every reachable
  reconciled document-runtime send carries the structurally complete exact
  config bundle that was durably captured. Static/one-shot sends are outside
  that claim and may honestly omit the bundle.
* `capture_failure_blocks_send` — a rejected capture leaves the store unchanged
  and makes `sent` unreachable forever. This is the fail-closed property the
  sink must satisfy; the implementation is not free to choose fail-open after
  this model.
-/

namespace RenderedCapture

/-- The permission and audit boundary the capture is attributed to. `Nat` under
`boundary.model.nat-typed-ids-time`. -/
abbrev AgentDid := Nat

/-! ## Capture key -/

/-- Identity of exactly one provider attempt.

The five components are the ones `RenderedRequestContext` plus the owned loop
already carry: the agent principal, the session, the exact request `_docID`, the
completion turn, and the attempt within that turn. Equality is componentwise —
there is no delimiter, and therefore no delimiter collision. -/
structure CaptureKey where
  agentDid : AgentDid
  sessionId : SessionId
  requestId : RequestId
  turnIndex : Nat
  attempt : Nat
  deriving DecidableEq, Repr

/-- One exact DefraDB document version. `_docID` identifies the document while
the composite commit CID identifies the immutable snapshot within its history.
Both are opaque natural numbers under `boundary.model.nat-typed-ids-time`. -/
structure DocumentVersionRef where
  docId : Nat
  compositeCommitCid : Nat
  deriving DecidableEq, Repr

/-- One exact DefraDB version together with the DID derived from its verified
commit signature. Signature validity is established by `RequestIngest`; this
record carries the evidence selected at that gate without reinterpreting it. -/
structure SignedDocumentVersionRef where
  version : DocumentVersionRef
  signerDid : AgentDid
  deriving DecidableEq, Repr

/-- The immutable request chain admitted by `RequestIngest`: the signed source
fact accepted before claiming and the signed target-agent claim descended from
that exact source. Both references are required because the claim snapshot is
the value executed while the source snapshot establishes who supplied it. -/
structure RequestExecutionProvenance where
  source : SignedDocumentVersionRef
  claim : SignedDocumentVersionRef
  deriving DecidableEq, Repr

/-! ## Bounded resolved-config provenance -/

namespace SignedDocumentVersionRef

/-- A config source is exact only when both DefraDB identities and the DID from
its verified commit signature are present. This proves authorship evidence, not
that the signer was authorized by a policy. -/
def exact (source : SignedDocumentVersionRef) : Bool :=
  source.version.docId != 0 &&
    source.version.compositeCommitCid != 0 &&
    source.signerDid != 0

end SignedDocumentVersionRef

/-- One selected skill source. `logicalId` is the resolver's stable skill
identity; it is modeled separately from physical `_docID` so canonical ordering
and duplicate rejection do not depend on storage identity. -/
structure ResolvedSkillSourceRef where
  logicalId : Nat
  source : SignedDocumentVersionRef
  deriving DecidableEq, Repr

namespace ResolvedSkillSourceRef

def exact (skill : ResolvedSkillSourceRef) : Bool :=
  skill.source.exact

/-- Strict logical-id ordering gives the variable skill list one canonical
representation and rejects duplicates. Empty is valid: a behavior may select
no skills. -/
def canonicalAfter : Nat → List ResolvedSkillSourceRef → Bool
  | _, [] => true
  | previous, skill :: rest =>
      skill.exact && decide (previous < skill.logicalId) &&
        canonicalAfter skill.logicalId rest

def canonical (skills : List ResolvedSkillSourceRef) : Bool :=
  canonicalAfter 0 skills

end ResolvedSkillSourceRef

/-- The direct resolved-config evidence captured with one provider request.

Principal, behavior, inference backend, and inference profile are required by
the resolver. Tool selection is genuinely optional. Skills are a variable,
possibly empty list ordered by stable logical identity. Every present ref also
carries a nonzero DID derived from a verified DefraDB commit signature.

The fields are data rather than a hash supplied by the same writer. This model
does not invent an application digest, claim authorization, or claim that the
bundle is a separately persisted `ResolvedAgentGeneration`. -/
structure ResolvedConfigProvenance where
  principal : SignedDocumentVersionRef
  behavior : SignedDocumentVersionRef
  inferenceBackend : SignedDocumentVersionRef
  inferenceProfile : SignedDocumentVersionRef
  toolSelection : Option SignedDocumentVersionRef
  skills : List ResolvedSkillSourceRef
  deriving DecidableEq, Repr

namespace ResolvedConfigProvenance

def valid (provenance : ResolvedConfigProvenance) : Bool :=
  provenance.principal.exact &&
    provenance.behavior.exact &&
    provenance.inferenceBackend.exact &&
    provenance.inferenceProfile.exact &&
    provenance.toolSelection.all SignedDocumentVersionRef.exact &&
    ResolvedSkillSourceRef.canonical provenance.skills

theorem valid_has_exact_required_sources (provenance : ResolvedConfigProvenance)
    (h : provenance.valid = true) :
    provenance.principal.exact = true ∧
      provenance.behavior.exact = true ∧
      provenance.inferenceBackend.exact = true ∧
      provenance.inferenceProfile.exact = true := by
  have h_valid := h
  simp [valid] at h_valid
  exact ⟨h_valid.1.1.1.1.1, h_valid.1.1.1.1.2,
    h_valid.1.1.1.2, h_valid.1.1.2⟩

end ResolvedConfigProvenance

/-- Which runtime path assembled the provider request. Only reconciled
document-runtime requests claim the bounded resolved-config contract. Static,
legacy, and one-shot paths remain honest about not having that evidence. -/
inductive ConfigProvenanceScope where
  | reconciledDocumentRuntime
  | staticOrOneShot
  deriving DecidableEq, Repr

namespace ConfigProvenanceScope

def toContract : ConfigProvenanceScope → String
  | .reconciledDocumentRuntime => "reconciled_document_runtime"
  | .staticOrOneShot => "static_or_one_shot"

def requiresExactConfig : ConfigProvenanceScope → Bool
  | .reconciledDocumentRuntime => true
  | .staticOrOneShot => false

end ConfigProvenanceScope

/-- The canonical provider request and the request-document snapshot from which
the runtime executed it.

`value` is opaque on purpose: the only operation the model performs on provider
bytes is equality. `requestProvenance` is `none` only for a one-shot run, which
has no `AgentRequest` document. `configProvenance` is optional only so the
decision procedure can distinguish scoped absence from malformed evidence.
Reconciled document-runtime requests require the bundle. Static, legacy, and
one-shot paths may omit it; any bundle they do carry must still be valid. Both
provenance values and the scope are immutable fact data, not capture-key
components. -/
structure CanonicalRequest where
  value : Nat
  requestProvenance : Option RequestExecutionProvenance := none
  configScope : ConfigProvenanceScope := .staticOrOneShot
  configProvenance : Option ResolvedConfigProvenance := none
  deriving DecidableEq, Repr

namespace CanonicalRequest

def hasExactConfigProvenance (request : CanonicalRequest) : Bool :=
  match request.configProvenance with
  | none => false
  | some provenance => provenance.valid

/-- Scope-aware admission. The bounded bundle is mandatory only for reconciled
document-runtime requests. An optional bundle on any other path cannot be
malformed. -/
def configAdmitted (request : CanonicalRequest) : Bool :=
  match request.configScope, request.configProvenance with
  | .reconciledDocumentRuntime, some provenance => provenance.valid
  | .reconciledDocumentRuntime, none => false
  | .staticOrOneShot, some provenance => provenance.valid
  | .staticOrOneShot, none => true

theorem reconciled_config_admitted_iff_exact (request : CanonicalRequest)
    (h_scope : request.configScope = .reconciledDocumentRuntime) :
    request.configAdmitted = true ↔ request.hasExactConfigProvenance = true := by
  cases h_config : request.configProvenance <;>
    simp [configAdmitted, hasExactConfigProvenance, h_scope, h_config]

theorem exact_config_has_bundle (request : CanonicalRequest)
    (h : request.hasExactConfigProvenance = true) :
    ∃ provenance, request.configProvenance = some provenance ∧ provenance.valid = true := by
  cases h_config : request.configProvenance with
  | none => simp [hasExactConfigProvenance, h_config] at h
  | some provenance =>
      exact ⟨provenance, rfl,
        by simpa [hasExactConfigProvenance, h_config] using h⟩

end CanonicalRequest

/-! ## Durable capture table -/

/-- The durable `RenderedRequest` collection, viewed as a partial map from
capture key to the canonical request stored under it. -/
abbrev Store := CaptureKey → Option CanonicalRequest

namespace Store

def empty : Store := fun _ => none

/-- Write a fact. Only ever applied to a key the writer just observed to be
unbound; `capture` is the only caller. -/
def bind (s : Store) (k : CaptureKey) (r : CanonicalRequest) : Store :=
  fun probe => if probe = k then some r else s probe

@[simp] theorem bind_self (s : Store) (k : CaptureKey) (r : CanonicalRequest) :
    bind s k r k = some r := by
  simp [bind]

@[simp] theorem bind_other (s : Store) (k probe : CaptureKey) (r : CanonicalRequest)
    (h : probe ≠ k) : bind s k r probe = s probe := by
  simp [bind, h]

@[simp] theorem empty_lookup (k : CaptureKey) : empty k = none := rfl

end Store

/-! ## The capture decision -/

/-- What the sink decided for one `(key, request)` delivery. -/
inductive CaptureOutcome where
  /-- The key was unbound; the fact is now durable. -/
  | fresh
  /-- The key already held exactly this request — a redelivery, a restart, or a
  lost acknowledgement. Success without a write. -/
  | idempotent
  /-- The key already held a *different* request. An integrity violation, never
  an update. -/
  | rejected
  deriving DecidableEq, Repr

namespace CaptureOutcome

def toContract : CaptureOutcome → String
  | .fresh => "fresh"
  | .idempotent => "idempotent"
  | .rejected => "rejected"

/-- Fail-closed: exactly the outcomes that leave the fact durable. -/
def durable : CaptureOutcome → Bool
  | .fresh => true
  | .idempotent => true
  | .rejected => false

end CaptureOutcome

/-- The sink's decision procedure. Never overwrites a prior capture, and never
binds a request whose scope-aware config evidence is inadmissible. -/
def capture (s : Store) (k : CaptureKey) (r : CanonicalRequest) :
    CaptureOutcome × Store :=
  if r.configAdmitted then
    match s k with
    | none => (.fresh, Store.bind s k r)
    | some stored => if stored = r then (.idempotent, s) else (.rejected, s)
  else
    (.rejected, s)

theorem capture_fresh (s : Store) (k : CaptureKey) (r : CanonicalRequest)
    (h_config : r.configAdmitted = true) (h : s k = none) :
    capture s k r = (.fresh, Store.bind s k r) := by
  simp [capture, h_config, h]

/-- Redelivering the identical canonical request succeeds and writes nothing. -/
theorem capture_idempotent (s : Store) (k : CaptureKey) (r : CanonicalRequest)
    (h_config : r.configAdmitted = true) (h : s k = some r) :
    capture s k r = (.idempotent, s) := by
  simp [capture, h_config, h]

/-- Required-missing or malformed config provenance rejects before any durable bind. -/
theorem capture_rejects_invalid_config (s : Store) (k : CaptureKey)
    (r : CanonicalRequest) (h : r.configAdmitted = false) :
    capture s k r = (.rejected, s) := by
  simp [capture, h]

/-- Reusing a key with a different canonical value is an integrity error, and
the store is left exactly as it was. -/
theorem capture_rejects_rebinding (s : Store) (k : CaptureKey)
    (stored r : CanonicalRequest) (h : s k = some stored) (h_conflict : stored ≠ r) :
    capture s k r = (.rejected, s) := by
  by_cases h_config : r.configAdmitted = true
  · simp [capture, h_config, h, h_conflict]
  · cases h_value : r.configAdmitted
    · simp [capture, h_value]
    · exact absurd h_value h_config

/-- A source/claim provenance change is a fact change even when the rendered
provider body is byte-identical. This prevents an idempotency key from
laundering a different signed request chain into an existing capture. -/
theorem capture_rejects_request_provenance_rebinding (s : Store) (k : CaptureKey)
    (stored r : CanonicalRequest) (h : s k = some stored)
    (h_provenance : stored.requestProvenance ≠ r.requestProvenance) :
    capture s k r = (.rejected, s) := by
  apply capture_rejects_rebinding s k stored r h
  intro h_request
  apply h_provenance
  exact congrArg CanonicalRequest.requestProvenance h_request

/-- Resolved config is immutable canonical fact data, not key data. Equal body
bytes and an equal signed request chain cannot launder another exact config
bundle into a row already bound under this provider-attempt key. -/
theorem capture_rejects_config_provenance_rebinding (s : Store) (k : CaptureKey)
    (stored r : CanonicalRequest) (h : s k = some stored)
    (h_provenance : stored.configProvenance ≠ r.configProvenance) :
    capture s k r = (.rejected, s) := by
  apply capture_rejects_rebinding s k stored r h
  intro h_request
  apply h_provenance
  exact congrArg CanonicalRequest.configProvenance h_request

/-- Capture is total and its three outcomes are mutually exclusive. -/
theorem capture_outcome_classified (s : Store) (k : CaptureKey) (r : CanonicalRequest) :
    ((capture s k r).1 = .fresh ↔
        r.configAdmitted = true ∧ s k = none) ∧
      ((capture s k r).1 = .idempotent ↔
        r.configAdmitted = true ∧ s k = some r) ∧
      ((capture s k r).1 = .rejected ↔
        r.configAdmitted = false ∨
          ∃ stored, s k = some stored ∧ stored ≠ r) := by
  cases h_config : r.configAdmitted with
  | false => simp [capture, h_config]
  | true =>
      cases h : s k with
      | none => simp [capture, h_config, h]
      | some stored =>
          by_cases h_eq : stored = r
          · subst h_eq; simp [capture, h_config, h]
          · simp [capture, h_config, h, h_eq]

/-- The fact is durable after capture exactly when the outcome says it is. This
is the equation the sink must not weaken: a `rejected` outcome may never be
reported as durable. -/
theorem capture_durable_iff (s : Store) (k : CaptureKey) (r : CanonicalRequest) :
    (capture s k r).1.durable = true ↔
      r.configAdmitted = true ∧ (capture s k r).2 k = some r := by
  cases h_config : r.configAdmitted with
  | false => simp [capture, h_config, CaptureOutcome.durable]
  | true =>
      cases h : s k with
      | none => simp [capture, h_config, h, CaptureOutcome.durable]
      | some stored =>
          by_cases h_eq : stored = r
          · subst h_eq; simp [capture, h_config, h, CaptureOutcome.durable]
          · simp [capture, h_config, h, h_eq, CaptureOutcome.durable]

/-- Capture never disturbs a binding it did not create. -/
theorem capture_preserves_bindings (s : Store) (k : CaptureKey) (r : CanonicalRequest)
    {probe : CaptureKey} {stored : CanonicalRequest} (h : s probe = some stored) :
    (capture s k r).2 probe = some stored := by
  cases h_config : r.configAdmitted with
  | false => simp [capture, h_config, h]
  | true =>
      cases h_lookup : s k with
      | none =>
          have h_ne : probe ≠ k := by
            intro h_eq
            subst h_eq
            rw [h_lookup] at h
            exact Option.noConfusion h
          simp [capture, h_config, h_lookup, Store.bind, h_ne, h]
      | some other =>
          by_cases h_eq : other = r
          · simp [capture, h_config, h_lookup, h_eq, h]
          · simp [capture, h_config, h_lookup, h_eq, h]

/-- Capturing under a different key leaves this key's fact untouched. -/
theorem capture_at_other_key_preserves (s : Store) (k probe : CaptureKey)
    (r : CanonicalRequest) (h : probe ≠ k) :
    (capture s k r).2 probe = s probe := by
  cases h_config : r.configAdmitted with
  | false => simp [capture, h_config]
  | true =>
      cases h_lookup : s k with
      | none => simp [capture, h_config, h_lookup, Store.bind, h]
      | some other =>
          by_cases h_eq : other = r
          · simp [capture, h_config, h_lookup, h_eq]
          · simp [capture, h_config, h_lookup, h_eq]

/-- Attempts separate facts. A repair retry may legitimately assemble a
different request; because `attempt` is in the key, that is a new row, not a
rebinding of the previous attempt's row. -/
theorem attempt_distinguishes_facts (k : CaptureKey) {a b : Nat} (h : a ≠ b) :
    ({ k with attempt := a } : CaptureKey) ≠ { k with attempt := b } := by
  intro h_eq
  exact h (congrArg CaptureKey.attempt h_eq)

/-! ## Stages and the transition relation -/

/-- Where one provider attempt is in its life. -/
inductive Stage where
  /-- The request exists in memory; nothing durable, nothing sent. -/
  | assembled
  /-- The capture is durable under this attempt's key. -/
  | durablyCaptured
  /-- The provider call has been issued. -/
  | sent
  deriving DecidableEq, Repr

namespace Stage

def toContract : Stage → String
  | .assembled => "assembled"
  | .durablyCaptured => "durablyCaptured"
  | .sent => "sent"

def all : List Stage := [.assembled, .durablyCaptured, .sent]

theorem all_complete (stage : Stage) : stage ∈ all := by
  cases stage <;> simp [all]

end Stage

/-- One in-flight provider attempt, against the whole durable store. `key` and
`request` are the attempt's fixed identity; no transition may change them. -/
structure Machine where
  store : Store
  stage : Stage
  key : CaptureKey
  request : CanonicalRequest

/-- The legal transitions. A capture or send is legal only for a canonical
request satisfying its declared config-provenance scope. -/
inductive Step : Machine → Machine → Prop where
  /-- Capture wrote a new fact. -/
  | captureFresh {pre post : Machine}
      (h_stage : pre.stage = .assembled)
      (h_config : pre.request.configAdmitted = true)
      (h_unbound : pre.store pre.key = none)
      (h_post : post =
        { pre with stage := .durablyCaptured
                 , store := Store.bind pre.store pre.key pre.request })
      : Step pre post
  /-- Capture found the identical fact already durable. -/
  | captureIdempotent {pre post : Machine}
      (h_stage : pre.stage = .assembled)
      (h_config : pre.request.configAdmitted = true)
      (h_bound : pre.store pre.key = some pre.request)
      (h_post : post = { pre with stage := .durablyCaptured })
      : Step pre post
  /-- Capture found a conflicting fact. Fail-closed: nothing moves. -/
  | captureRejected {pre post : Machine} (stored : CanonicalRequest)
      (h_stage : pre.stage = .assembled)
      (h_config : pre.request.configAdmitted = true)
      (h_bound : pre.store pre.key = some stored)
      (h_conflict : stored ≠ pre.request)
      (h_post : post = pre)
      : Step pre post
  /-- The candidate omitted or malformed the bounded exact config bundle. -/
  | captureInvalidConfig {pre post : Machine}
      (h_stage : pre.stage = .assembled)
      (h_config : pre.request.configAdmitted = false)
      (h_post : post = pre)
      : Step pre post
  /-- The provider call. Legal only against a durable, matching fact. -/
  | send {pre post : Machine}
      (h_stage : pre.stage = .durablyCaptured)
      (h_config : pre.request.configAdmitted = true)
      (h_durable : pre.store pre.key = some pre.request)
      (h_post : post = { pre with stage := .sent })
      : Step pre post
  /-- Another attempt — another turn, another request, another node — captured
  concurrently. Only a `fresh` capture changes the store, so this is the only
  interference shape there is. -/
  | concurrentCapture {pre post : Machine} (k : CaptureKey) (r : CanonicalRequest)
      (h_config : r.configAdmitted = true)
      (h_unbound : pre.store k = none)
      (h_post : post = { pre with store := Store.bind pre.store k r })
      : Step pre post

inductive Trace : Machine → Machine → Prop where
  | refl {m : Machine} : Trace m m
  | step {a b c : Machine} : Step a b → Trace b c → Trace a c

namespace Step

theorem preserves_identity {pre post : Machine} (h : Step pre post) :
    post.key = pre.key ∧ post.request = pre.request := by
  cases h with
  | captureFresh _ _ _ h_post => subst h_post; exact ⟨rfl, rfl⟩
  | captureIdempotent _ _ _ h_post => subst h_post; exact ⟨rfl, rfl⟩
  | captureRejected _ _ _ _ _ h_post => subst h_post; exact ⟨rfl, rfl⟩
  | captureInvalidConfig _ _ h_post => subst h_post; exact ⟨rfl, rfl⟩
  | send _ _ _ h_post => subst h_post; exact ⟨rfl, rfl⟩
  | concurrentCapture _ _ _ _ h_post => subst h_post; exact ⟨rfl, rfl⟩

/-- No transition ever unbinds or rebinds an existing fact. This is the
mechanical content of "capture never overwrites a prior capture". -/
theorem store_monotone {pre post : Machine} (h : Step pre post)
    {probe : CaptureKey} {stored : CanonicalRequest}
    (h_bound : pre.store probe = some stored) :
    post.store probe = some stored := by
  cases h with
  | captureFresh _ _ h_unbound h_post =>
      subst h_post
      have h_ne : probe ≠ pre.key := by
        intro h_eq
        subst h_eq
        rw [h_unbound] at h_bound
        exact Option.noConfusion h_bound
      simpa [Store.bind, h_ne] using h_bound
  | captureIdempotent _ _ _ h_post => subst h_post; exact h_bound
  | captureRejected _ _ _ _ _ h_post => subst h_post; exact h_bound
  | captureInvalidConfig _ _ h_post => subst h_post; exact h_bound
  | send _ _ _ h_post => subst h_post; exact h_bound
  | concurrentCapture k _ _ h_unbound h_post =>
      subst h_post
      have h_ne : probe ≠ k := by
        intro h_eq
        subst h_eq
        rw [h_unbound] at h_bound
        exact Option.noConfusion h_bound
      simpa [Store.bind, h_ne] using h_bound

end Step

namespace Trace

theorem preserves_identity {a b : Machine} (h : Trace a b) :
    b.key = a.key ∧ b.request = a.request := by
  induction h with
  | refl => exact ⟨rfl, rfl⟩
  | step h_step _ ih =>
      obtain ⟨hk, hr⟩ := h_step.preserves_identity
      exact ⟨ih.1.trans hk, ih.2.trans hr⟩

theorem store_monotone {a b : Machine} (h : Trace a b) :
    ∀ {probe : CaptureKey} {stored : CanonicalRequest},
      a.store probe = some stored → b.store probe = some stored := by
  induction h with
  | refl => intro _ _ h_bound; exact h_bound
  | step h_step _ ih => intro _ _ h_bound; exact ih (h_step.store_monotone h_bound)

end Trace

/-! ## The durability invariant -/

/-- This attempt's fact is durable under this attempt's key. -/
def Machine.durable (m : Machine) : Prop :=
  m.store m.key = some m.request

/-- Anything past `assembled` is durable and carries the exact bounded config
provenance bundle. -/
def Machine.Invariant (m : Machine) : Prop :=
  m.stage ≠ Stage.assembled →
    m.durable ∧ m.request.configAdmitted = true

theorem Machine.invariant_of_assembled {m : Machine} (h : m.stage = Stage.assembled) :
    Machine.Invariant m := by
  intro h_ne
  exact absurd h h_ne

theorem Step.preserves_invariant {pre post : Machine}
    (h_inv : Machine.Invariant pre) (h : Step pre post) :
    Machine.Invariant post := by
  cases h with
  | captureFresh _ h_config _ h_post =>
      subst h_post
      intro _
      exact ⟨by simp [Machine.durable], h_config⟩
  | captureIdempotent _ h_config h_bound h_post =>
      subst h_post
      intro _
      exact ⟨by simpa [Machine.durable] using h_bound, h_config⟩
  | captureRejected _ _ _ _ _ h_post => subst h_post; exact h_inv
  | captureInvalidConfig _ _ h_post => subst h_post; exact h_inv
  | send _ h_config h_durable h_post =>
      subst h_post
      intro _
      exact ⟨by simpa [Machine.durable] using h_durable, h_config⟩
  | concurrentCapture k r _ h_unbound h_post =>
      subst h_post
      intro h_ne
      have h_pre := h_inv h_ne
      have h_ne_key : pre.key ≠ k := by
        intro h_eq
        have h_lookup : pre.store pre.key = some pre.request := h_pre.1
        rw [h_eq, h_unbound] at h_lookup
        exact Option.noConfusion h_lookup
      exact ⟨by simpa [Machine.durable, Store.bind, h_ne_key] using h_pre.1, h_pre.2⟩

theorem Trace.preserves_invariant {a b : Machine} (h : Trace a b)
    (h_inv : Machine.Invariant a) : Machine.Invariant b := by
  induction h with
  | refl => exact h_inv
  | step h_step _ ih => exact ih (h_step.preserves_invariant h_inv)

/-! ## The fenced properties -/

/-- **P1 — persist before send.** No `sent` state is reachable from an
`assembled` start unless the store binds this attempt's key to exactly this
attempt's canonical request. -/
theorem sent_implies_durably_captured {init final : Machine}
    (h_start : init.stage = Stage.assembled)
    (h_trace : Trace init final)
    (h_sent : final.stage = Stage.sent) :
    final.store final.key = some final.request := by
  have h_inv := h_trace.preserves_invariant (Machine.invariant_of_assembled h_start)
  exact (h_inv (by rw [h_sent]; decide)).1

/-- **P1-config — exact resolved inputs before a reconciled document send.**
This is direct capture evidence and deliberately not a claim about static,
legacy, or one-shot paths, nor that a separate resolved-generation document was
published. -/
theorem reconciled_document_send_implies_exact_config_provenance {init final : Machine}
    (h_start : init.stage = Stage.assembled)
    (h_trace : Trace init final)
    (h_sent : final.stage = Stage.sent)
    (h_scope : final.request.configScope = .reconciledDocumentRuntime) :
    final.request.hasExactConfigProvenance = true := by
  have h_inv := h_trace.preserves_invariant (Machine.invariant_of_assembled h_start)
  have h_admitted := (h_inv (by rw [h_sent]; decide)).2
  exact (CanonicalRequest.reconciled_config_admitted_iff_exact
    final.request h_scope).mp h_admitted

/-- A step that made this attempt's fact durable. -/
def CaptureStep (pre post : Machine) : Prop :=
  pre.stage = Stage.assembled ∧
    post.stage = Stage.durablyCaptured ∧
    post.store post.key = some post.request ∧
    post.request.configAdmitted = true ∧
    post.key = pre.key ∧
    post.request = pre.request

/-- **P1' — the send has a witness.** Stronger than P1: every trace from
`assembled` to `sent` factors through an actual successful capture step for this
`(key, request)`. A fail-open sink that skipped the write could not produce such
a factorization, so the implementation is not free to choose one. -/
theorem sent_requires_a_capture_step {init final : Machine} (h_trace : Trace init final) :
    init.stage = Stage.assembled → final.stage = Stage.sent →
      ∃ pre post, Trace init pre ∧ Step pre post ∧ CaptureStep pre post ∧ Trace post final := by
  induction h_trace with
  | @refl m =>
      intro h_start h_sent
      rw [h_start] at h_sent
      exact absurd h_sent (by decide)
  | @step a b c h_step _ ih =>
      intro h_start h_sent
      cases h_step with
      | captureFresh h_stage h_config h_unbound h_post =>
          refine ⟨a, b, Trace.refl, Step.captureFresh h_stage h_config h_unbound h_post,
            ?_, by assumption⟩
          subst h_post
          exact ⟨h_stage, rfl, by simp, h_config, rfl, rfl⟩
      | captureIdempotent h_stage h_config h_bound h_post =>
          refine ⟨a, b, Trace.refl, Step.captureIdempotent h_stage h_config h_bound h_post,
            ?_, by assumption⟩
          subst h_post
          exact ⟨h_stage, rfl, by simpa using h_bound, h_config, rfl, rfl⟩
      | captureRejected stored h_stage h_config h_bound h_conflict h_post =>
          have h_b : b.stage = Stage.assembled := by subst h_post; exact h_start
          obtain ⟨pre, post, t_head, s, cs, t_tail⟩ := ih h_b h_sent
          exact ⟨pre, post,
            Trace.step (Step.captureRejected stored h_stage h_config h_bound h_conflict h_post)
              t_head,
            s, cs, t_tail⟩
      | captureInvalidConfig h_stage h_config h_post =>
          have h_b : b.stage = Stage.assembled := by subst h_post; exact h_start
          obtain ⟨pre, post, t_head, s, cs, t_tail⟩ := ih h_b h_sent
          exact ⟨pre, post,
            Trace.step (Step.captureInvalidConfig h_stage h_config h_post) t_head,
            s, cs, t_tail⟩
      | send h_stage _ _ _ =>
          rw [h_start] at h_stage
          exact absurd h_stage (by decide)
      | concurrentCapture k r h_config h_unbound h_post =>
          have h_b : b.stage = Stage.assembled := by subst h_post; exact h_start
          obtain ⟨pre, post, t_head, s, cs, t_tail⟩ := ih h_b h_sent
          exact ⟨pre, post,
            Trace.step (Step.concurrentCapture k r h_config h_unbound h_post) t_head,
            s, cs, t_tail⟩

/-- **P2 — one capture key names at most one canonical request.** Stated over
the request *value*, not over a digest: no honest-writer assumption enters the
statement. -/
theorem capture_key_determines_request {init final : Machine} (h_trace : Trace init final)
    {k : CaptureKey} {r r' : CanonicalRequest}
    (h_init : init.store k = some r) (h_final : final.store k = some r') :
    r = r' := by
  have h_carried := h_trace.store_monotone h_init
  rw [h_carried] at h_final
  exact Option.some.inj h_final

/-- **P3 — fail-closed.** A key already bound to a conflicting request makes
`sent` unreachable, permanently: the rejected capture writes nothing, no later
transition can rebind the key, and `send` has no other precondition to satisfy. -/
theorem capture_failure_blocks_send {init final : Machine} {stored : CanonicalRequest}
    (h_start : init.stage = Stage.assembled)
    (h_bound : init.store init.key = some stored)
    (h_conflict : stored ≠ init.request)
    (h_trace : Trace init final) :
    final.stage ≠ Stage.sent := by
  intro h_sent
  have h_durable := sent_implies_durably_captured h_start h_trace h_sent
  obtain ⟨hk, hr⟩ := h_trace.preserves_identity
  rw [hk, hr] at h_durable
  have h_carried := h_trace.store_monotone h_bound
  rw [h_carried] at h_durable
  exact h_conflict (Option.some.inj h_durable)

/-! ## Executable scenarios (the emitted conformance rows)

Everything the contract emits is computed from `capture`, so a change to the
decision procedure changes the rows and breaks the Rust fence, rather than
silently disagreeing with it. `Scenario.trace_realizes` ties the computed rows
back to the relational model above, so the emitted rows are not a second,
unproven story. -/

/-- One capture delivery evaluated against a store that already holds
`priorBinding` (if anything) under the same key. -/
structure Scenario where
  key : CaptureKey
  request : CanonicalRequest
  priorBinding : Option CanonicalRequest
  deriving Repr

namespace Scenario

def store (sc : Scenario) : Store :=
  match sc.priorBinding with
  | none => Store.empty
  | some stored => Store.bind Store.empty sc.key stored

@[simp] theorem store_lookup (sc : Scenario) : store sc sc.key = sc.priorBinding := by
  cases h : sc.priorBinding <;> simp [store, h]

def outcome (sc : Scenario) : CaptureOutcome :=
  (capture (store sc) sc.key sc.request).1

def postStore (sc : Scenario) : Store :=
  (capture (store sc) sc.key sc.request).2

def durableAfter (sc : Scenario) : Option CanonicalRequest :=
  postStore sc sc.key

/-- The stage the attempt reaches once the sink has answered. -/
def postStage (sc : Scenario) : Stage :=
  if (outcome sc).durable then .durablyCaptured else .assembled

/-- Whether the loop may issue the provider call. -/
def sendPermitted (sc : Scenario) : Bool :=
  (outcome sc).durable &&
    sc.request.configAdmitted &&
    decide (durableAfter sc = some sc.request)

/-- How many requests the provider is allowed to observe for this attempt. -/
def providerRequests (sc : Scenario) : Nat :=
  if sendPermitted sc then 1 else 0

def finalStage (sc : Scenario) : Stage :=
  if sendPermitted sc then .sent else postStage sc

def initialMachine (sc : Scenario) : Machine :=
  { store := store sc, stage := .assembled, key := sc.key, request := sc.request }

def finalMachine (sc : Scenario) : Machine :=
  { store := postStore sc, stage := finalStage sc, key := sc.key, request := sc.request }

/-- The emitted `send_permitted` requires both the durable canonical fact and
scope-aware config admission. -/
theorem sendPermitted_iff_durable_and_exact_config (sc : Scenario) :
    sendPermitted sc = true ↔
      (outcome sc).durable = true ∧
      sc.request.configAdmitted = true ∧
      durableAfter sc = some sc.request := by
  simp [sendPermitted, and_assoc]

/-- A rejected capture never permits a send, and never writes. -/
theorem rejected_blocks_send (sc : Scenario) (h : outcome sc = .rejected) :
    sendPermitted sc = false ∧ postStore sc = store sc := by
  have h_classified :=
    (capture_outcome_classified (store sc) sc.key sc.request).2.2.mp h
  cases h_classified with
  | inl h_invalid =>
      have h_capture := capture_rejects_invalid_config (store sc) sc.key sc.request h_invalid
      constructor
      · simp [sendPermitted, outcome, h_capture, CaptureOutcome.durable]
      · simp [postStore, h_capture]
  | inr h_conflict =>
      obtain ⟨stored, h_bound, h_ne⟩ := h_conflict
      have h_capture := capture_rejects_rebinding
        (store sc) sc.key stored sc.request h_bound h_ne
      constructor
      · simp [sendPermitted, outcome, h_capture, CaptureOutcome.durable]
      · simp [postStore, h_capture]

/-- `providerRequests` is 1 exactly when the modeled trace reaches `sent`. -/
theorem providerRequests_iff_sent (sc : Scenario) :
    providerRequests sc = 1 ↔ finalStage sc = Stage.sent := by
  cases h : sendPermitted sc <;> simp [providerRequests, finalStage, h, postStage] <;>
    cases h_outcome : (outcome sc).durable <;> simp

/-- **The emitted rows are the relational model.** Every scenario's computed
`(finalStore, finalStage)` is reachable from its `assembled` start by legal
steps, so a Rust implementation that reproduces the rows inherits P1–P3 rather
than merely agreeing with a spreadsheet. -/
theorem trace_realizes (sc : Scenario) : Trace (initialMachine sc) (finalMachine sc) := by
  cases h_config : sc.request.configAdmitted with
  | false =>
      have h_capture : capture (store sc) sc.key sc.request = (.rejected, store sc) :=
        capture_rejects_invalid_config _ _ _ h_config
      have h_post : postStore sc = store sc := by simp [postStore, h_capture]
      have h_outcome : outcome sc = .rejected := by simp [outcome, h_capture]
      have h_send : sendPermitted sc = false := (rejected_blocks_send sc h_outcome).1
      refine Trace.step (Step.captureInvalidConfig (pre := initialMachine sc)
        (post := initialMachine sc) rfl (by simpa [initialMachine] using h_config) rfl) ?_
      have : finalMachine sc = initialMachine sc := by
        simp [finalMachine, initialMachine, finalStage, h_send, h_post, postStage,
          h_outcome, CaptureOutcome.durable]
      rw [this]
      exact Trace.refl
  | true =>
      cases h_prior : sc.priorBinding with
      | none =>
          have h_unbound : store sc sc.key = none := by simp [h_prior]
          have h_capture : capture (store sc) sc.key sc.request =
              (.fresh, Store.bind (store sc) sc.key sc.request) :=
            capture_fresh _ _ _ h_config h_unbound
          have h_post : postStore sc = Store.bind (store sc) sc.key sc.request := by
            simp [postStore, h_capture]
          have h_send : sendPermitted sc = true := by
            simp [sendPermitted, outcome, h_capture, durableAfter, h_post, h_config,
              CaptureOutcome.durable]
          refine Trace.step (Step.captureFresh (pre := initialMachine sc)
            (post := { initialMachine sc with stage := .durablyCaptured
                                            , store := Store.bind (store sc) sc.key sc.request })
            rfl (by simpa [initialMachine] using h_config) h_unbound rfl) ?_
          refine Trace.step (Step.send
            (post := { initialMachine sc with stage := .sent
                                            , store := Store.bind (store sc) sc.key sc.request })
            rfl (by simpa [initialMachine] using h_config) (by simp [initialMachine]) rfl) ?_
          have : finalMachine sc =
              { initialMachine sc with stage := .sent
                                     , store := Store.bind (store sc) sc.key sc.request } := by
            simp [finalMachine, initialMachine, finalStage, h_send, h_post]
          rw [this]
          exact Trace.refl
      | some stored =>
          have h_bound : store sc sc.key = some stored := by simp [h_prior]
          by_cases h_eq : stored = sc.request
          · subst h_eq
            have h_capture : capture (store sc) sc.key sc.request = (.idempotent, store sc) :=
              capture_idempotent _ _ _ h_config h_bound
            have h_post : postStore sc = store sc := by simp [postStore, h_capture]
            have h_send : sendPermitted sc = true := by
              simp [sendPermitted, outcome, h_capture, durableAfter, h_post, h_bound, h_config,
                CaptureOutcome.durable]
            refine Trace.step (Step.captureIdempotent (pre := initialMachine sc)
              (post := { initialMachine sc with stage := .durablyCaptured })
              rfl (by simpa [initialMachine] using h_config)
              (by simpa [initialMachine] using h_bound) rfl) ?_
            refine Trace.step (Step.send
              (post := { initialMachine sc with stage := .sent })
              rfl (by simpa [initialMachine] using h_config)
              (by simpa [initialMachine] using h_bound) rfl) ?_
            have : finalMachine sc = { initialMachine sc with stage := .sent } := by
              simp [finalMachine, initialMachine, finalStage, h_send, h_post]
            rw [this]
            exact Trace.refl
          · have h_capture : capture (store sc) sc.key sc.request = (.rejected, store sc) :=
              capture_rejects_rebinding _ _ _ _ h_bound h_eq
            have h_post : postStore sc = store sc := by simp [postStore, h_capture]
            have h_outcome : outcome sc = .rejected := by simp [outcome, h_capture]
            have h_send : sendPermitted sc = false := (rejected_blocks_send sc h_outcome).1
            refine Trace.step (Step.captureRejected (pre := initialMachine sc)
              (post := initialMachine sc) stored rfl (by simpa [initialMachine] using h_config)
              (by simpa [initialMachine] using h_bound)
              (by simpa [initialMachine] using h_eq) rfl) ?_
            have : finalMachine sc = initialMachine sc := by
              simp [finalMachine, initialMachine, finalStage, h_send, h_post, postStage,
                h_outcome, CaptureOutcome.durable]
            rw [this]
            exact Trace.refl

end Scenario

end RenderedCapture
