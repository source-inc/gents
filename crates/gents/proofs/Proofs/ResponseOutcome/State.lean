import Proofs.RenderedCapture

/-!
# Response live projection and immutable outcome facts

`AgentResponseLive` is replaceable observed state. It can wake clients and
retain a partial tail, but it is never evidence that a request completed.
`AgentResponseOutcome` is an immutable signed fact keyed by the exact request
document. A complete outcome must pin the exact signed assistant message that
was materialized for that same request.

ACP authorization and encryption are deliberately outside this model. Signer
DIDs are verified authorship evidence; later policy decides authorization.
-/

namespace ResponseOutcome

open RenderedCapture

abbrev RequestRef := SignedDocumentVersionRef
abbrev MessageRef := SignedDocumentVersionRef
abbrev ExecutionProvenance := RequestExecutionProvenance

def ExecutionProvenance.exactFor
    (provenance : ExecutionProvenance)
    (requestDocId targetAgentDid : Nat) : Bool :=
  provenance.source.exact && provenance.claim.exact &&
    provenance.source.version.docId == requestDocId &&
    provenance.claim.version.docId == requestDocId &&
    provenance.source.version.compositeCommitCid !=
      provenance.claim.version.compositeCommitCid &&
    provenance.claim.signerDid == targetAgentDid

/-- Persisted evidence sufficient to rediscover the admitted source/claim pair
after a crash has erased the in-memory lifecycle and before any live response
document exists. `claimParents` contains only composite (`_C`) parents. -/
structure ClaimCommitEvidence where
  source : RequestRef
  claim : RequestRef
  expectedSourceSignerDid : Nat
  targetAgentDid : Nat
  claimParents : List Nat
  sourcePending : Bool
  claimClaimed : Bool
  payloadPreserved : Bool
  deriving DecidableEq, Repr

def reconstructExecutionProvenance
    (evidence : ClaimCommitEvidence) : Option ExecutionProvenance :=
  let provenance : ExecutionProvenance :=
    { source := evidence.source, claim := evidence.claim }
  if provenance.exactFor evidence.source.version.docId evidence.targetAgentDid &&
      evidence.source.signerDid == evidence.expectedSourceSignerDid &&
      evidence.claimParents == [evidence.source.version.compositeCommitCid] &&
      evidence.sourcePending && evidence.claimClaimed && evidence.payloadPreserved then
    some provenance
  else
    none

inductive MessageRole where
  | assistant
  | user
  deriving DecidableEq, Repr

structure MessageEvidence where
  version : MessageRef
  request : RequestRef
  sessionId : Nat
  role : MessageRole
  deriving DecidableEq, Repr

def MessageEvidence.exactFor
    (message : MessageEvidence) (request : RequestRef) : Bool :=
  message.version.exact && request.exact &&
    decide (message.request = request) &&
    decide (message.role = .assistant)

inductive OutcomeKind where
  | complete
  | error
  | interrupted
  deriving DecidableEq, Repr

namespace OutcomeKind

def toContract : OutcomeKind → String
  | .complete => "complete"
  | .error => "error"
  | .interrupted => "interrupted"

end OutcomeKind

/-- Immutable terminal fact. `version` is the outcome fact's own signed exact
DefraDB version; `provenance` pins the admitted source/claim pair, `request`
must repeat that exact claim, and `finalMessage` is the optional consumed
assistant fact. -/
structure OutcomeFact where
  version : SignedDocumentVersionRef
  provenance : ExecutionProvenance
  request : RequestRef
  kind : OutcomeKind
  finalMessage : Option MessageEvidence
  reasonCode : Option Nat
  terminalizedAt : Nat
  deriving DecidableEq, Repr

def OutcomeFact.wellFormed (fact : OutcomeFact) : Bool :=
  fact.version.exact &&
    fact.provenance.exactFor fact.request.version.docId fact.request.signerDid &&
    decide (fact.request = fact.provenance.claim) &&
    fact.terminalizedAt != 0 &&
    match fact.kind with
    | .complete =>
        fact.reasonCode.isNone &&
          fact.finalMessage.any (fun message => message.exactFor fact.request)
    | .error | .interrupted => fact.reasonCode.isSome

/-- Conflict-visible store. We retain every physical sibling so a local unique
index can never hide the losing fact from the authority decision. -/
abbrev OutcomeStore := List OutcomeFact

def factsForRequestDoc (store : OutcomeStore) (requestDocId : Nat) : List OutcomeFact :=
  store.filter (fun fact => fact.request.version.docId == requestDocId)

inductive PublishOutcome where
  | fresh
  | idempotent
  | rejected
  deriving DecidableEq, Repr

namespace PublishOutcome

def toContract : PublishOutcome → String
  | .fresh => "fresh"
  | .idempotent => "idempotent"
  | .rejected => "rejected"

end PublishOutcome

/-- Create-and-compare. One identical visible fact is idempotent; any sibling,
different request version, malformed edge, or non-exact evidence fails closed. -/
def publish (store : OutcomeStore) (candidate : OutcomeFact) :
    PublishOutcome × OutcomeStore :=
  if !candidate.wellFormed then
    (.rejected, store)
  else
    match factsForRequestDoc store candidate.request.version.docId with
    | [] => (.fresh, candidate :: store)
    | [stored] =>
        if stored = candidate then (.idempotent, store) else (.rejected, store)
    | _ => (.rejected, store)

inductive LiveStage where
  | active
  | superseded
  | expired
  deriving DecidableEq, Repr

/-- Replaceable client overlay. A materialized message reference is staging
evidence for outcome creation, not a terminal fact by itself. -/
structure LiveProjection where
  docId : Nat
  request : RequestRef
  sessionId : Nat
  stage : LiveStage
  revision : Nat
  tailPresent : Bool
  materializedMessage : Option MessageEvidence
  deriving DecidableEq, Repr

inductive PersistenceCut where
  | claimDurable
  | streaming
  | messageDurable
  | outcomeDurable
  | requestTerminal
  | liveSuperseded
  deriving DecidableEq, Repr

structure Machine where
  /-- A dummy live value remains available to keep ordinary streaming steps
  total; `responsePresent = false` makes it observationally absent at the
  post-claim/pre-response recovery cut. -/
  live : LiveProjection
  responsePresent : Bool
  outcomes : OutcomeStore
  requestTerminal : Bool
  cut : PersistenceCut
  deriving DecidableEq, Repr

end ResponseOutcome
