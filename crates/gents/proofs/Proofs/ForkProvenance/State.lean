import Proofs.ToolFact.State

/-!
# Exact fork provenance

A fork never relabels a source row as though it originated in the child
session.  It mints a new node-signed child fact whose immutable provenance edge
pins the exact source `_docID`, composite CID, and verified signer.  Derived
tool results and approvals additionally pin the newly minted child call.
-/

namespace ForkProvenance

open ToolFact

abbrev SessionId := Nat
abbrev PayloadHash := Nat
abbrev LogicalKey := Nat

inductive FactKind where
  | message
  | toolCall
  | toolResult
  | toolApproval
  | compaction
  deriving DecidableEq, Repr

structure SourceFact where
  kind : FactKind
  sessionId : SessionId
  payloadHash : PayloadHash
  signed : SignedRef
  deriving DecidableEq, Repr

structure ChildIntent where
  key : LogicalKey
  kind : FactKind
  source : SignedRef
  sourceSessionId : SessionId
  childSessionId : SessionId
  payloadHash : PayloadHash
  childCall : Option SignedRef
  nodeSignerDid : SignerDid
  deriving DecidableEq, Repr

structure ChildFact where
  key : LogicalKey
  kind : FactKind
  source : SignedRef
  sourceSessionId : SessionId
  childSessionId : SessionId
  payloadHash : PayloadHash
  childCall : Option SignedRef
  signed : SignedRef
  deriving DecidableEq, Repr

abbrev SourceStore := Store SourceFact
abbrev ChildStore := Store ChildFact

structure State where
  sources : SourceStore
  children : ChildStore

def State.empty : State :=
  { sources := Store.empty, children := Store.empty }

def SourceFact.valid (fact : SourceFact) : Bool :=
  fact.sessionId != 0 && fact.payloadHash != 0 && fact.signed.authoritative

def exactSource? (store : SourceStore) (ref : SignedRef) : Option SourceFact :=
  match store ref.version.docId with
  | some fact =>
      if fact.signed = ref && fact.valid then some fact else none
  | none => none

def exactChild? (store : ChildStore) (ref : SignedRef) : Option ChildFact :=
  match store ref.version.docId with
  | some fact =>
      if fact.signed = ref && ref.authoritative then some fact else none
  | none => none

def ChildFact.forIntent (intent : ChildIntent) (signed : SignedRef) : ChildFact :=
  { key := intent.key
  , kind := intent.kind
  , source := intent.source
  , sourceSessionId := intent.sourceSessionId
  , childSessionId := intent.childSessionId
  , payloadHash := intent.payloadHash
  , childCall := intent.childCall
  , signed := signed }

def requiresChildCall : FactKind → Bool
  | .toolResult | .toolApproval => true
  | _ => false

def childCallValid (state : State) (intent : ChildIntent) : Bool :=
  if requiresChildCall intent.kind then
    match intent.childCall with
    | some callRef =>
        match exactChild? state.children callRef with
        | some call =>
            call.kind == .toolCall && call.childSessionId == intent.childSessionId
        | none => false
    | none => false
  else
    intent.childCall.isNone

end ForkProvenance
