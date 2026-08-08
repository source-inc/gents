import Proofs.Conformance.ContractCases.Types
import Proofs.ForkProvenance

namespace Conformance.ContractCases

open ToolFact ForkProvenance

structure ForkProvenanceCase where
  name : String
  kind : String
  disposition : String
  visibleLogicalTwins : Nat
  sourceAuthoritative : Bool
  sourceSessionId : Nat
  childSessionId : Nat
  sourceDocId : Nat
  sourceCid : Nat
  sourceSignerDid : Nat
  childDocId : Nat
  childCid : Nat
  childSignerDid : Nat
  childCallRequired : Bool
  childCallSatisfied : Bool
  exactSourcePinned : Bool
  immutableNoop : Bool
  deriving Repr

private def kindName : FactKind → String
  | .message => "message"
  | .toolCall => "tool_call"
  | .toolResult => "tool_result"
  | .toolApproval => "tool_approval"
  | .compaction => "compaction"

private def sourceRef : SignedRef :=
  { version := { docId := 100, compositeCommitCid := 10 }
  , signerDid := 7
  , signatureValid := true }

private def unsignedSourceRef : SignedRef :=
  { sourceRef with signatureValid := false }

private def source (kind : FactKind) (ref := sourceRef) : SourceFact :=
  { kind := kind, sessionId := 1, payloadHash := 50, signed := ref }

private def stateWithSource (kind : FactKind) : ForkProvenance.State :=
  { ForkProvenance.State.empty with
    sources := Store.bind Store.empty sourceRef.version.docId (source kind) }

private def childRef : SignedRef :=
  { version := { docId := 200, compositeCommitCid := 20 }
  , signerDid := 8
  , signatureValid := true }

private def intent (kind : FactKind) : ChildIntent :=
  { key := 2
  , kind := kind
  , source := sourceRef
  , sourceSessionId := 1
  , childSessionId := 2
  , payloadHash := 50
  , childCall := none
  , nodeSignerDid := 8 }

private def messageState : ForkProvenance.State := stateWithSource .message
private def messageIntent : ChildIntent := intent .message
private def messageApplied := commitChild messageState [] messageIntent childRef
private def messageReplay :=
  commitChild messageApplied.state [childRef.version.docId] messageIntent childRef
private def messageTwin := commitChild messageState [200, 201] messageIntent childRef
private def sameSession :=
  commitChild messageState [] { messageIntent with childSessionId := 1 } childRef
private def wrongNode :=
  commitChild messageState [] { messageIntent with nodeSignerDid := 9 } childRef
private def unsignedChild :=
  commitChild messageState [] messageIntent { childRef with signatureValid := false }
private def unsignedSourceState : ForkProvenance.State :=
  { ForkProvenance.State.empty with
    sources := Store.bind Store.empty sourceRef.version.docId
      (source .message unsignedSourceRef) }
private def unsignedSource := commitChild unsignedSourceState [] messageIntent childRef
private def payloadConflict :=
  commitChild messageApplied.state [childRef.version.docId]
    { messageIntent with payloadHash := 51 } childRef

private def callSourceRef : SignedRef :=
  { version := { docId := 110, compositeCommitCid := 11 }
  , signerDid := 7
  , signatureValid := true }
private def callChildRef : SignedRef :=
  { version := { docId := 210, compositeCommitCid := 21 }
  , signerDid := 8
  , signatureValid := true }
private def callSource : SourceFact :=
  { kind := .toolCall, sessionId := 1, payloadHash := 60, signed := callSourceRef }
private def callIntent : ChildIntent :=
  { key := 3, kind := .toolCall, source := callSourceRef
  , sourceSessionId := 1, childSessionId := 2, payloadHash := 60
  , childCall := none, nodeSignerDid := 8 }
private def callBase : ForkProvenance.State :=
  { ForkProvenance.State.empty with
    sources := Store.bind Store.empty callSourceRef.version.docId callSource }
private def callApplied := commitChild callBase [] callIntent callChildRef

private def resultSourceRef : SignedRef :=
  { version := { docId := 120, compositeCommitCid := 12 }
  , signerDid := 7
  , signatureValid := true }
private def resultChildRef : SignedRef :=
  { version := { docId := 220, compositeCommitCid := 22 }
  , signerDid := 8
  , signatureValid := true }
private def resultSource : SourceFact :=
  { kind := .toolResult, sessionId := 1, payloadHash := 70, signed := resultSourceRef }
private def resultBase : ForkProvenance.State :=
  { callApplied.state with
    sources := Store.bind callApplied.state.sources resultSourceRef.version.docId resultSource }
private def resultIntent : ChildIntent :=
  { key := 4, kind := .toolResult, source := resultSourceRef
  , sourceSessionId := 1, childSessionId := 2, payloadHash := 70
  , childCall := some callChildRef, nodeSignerDid := 8 }
private def resultApplied := commitChild resultBase [] resultIntent resultChildRef
private def resultWithoutChildCall :=
  commitChild resultBase [] { resultIntent with childCall := none } resultChildRef

private def pinsSource (observation : CommitObservation) (ref : SignedRef) : Bool :=
  match observation.state.children childRef.version.docId with
  | some fact => fact.source == ref
  | none => false

private def caseOf
    (name : String) (kind : FactKind) (observation : CommitObservation)
    (visibleTwins : Nat) (sourceAuthoritative childCallSatisfied : Bool)
    (sourceRef childRef : SignedRef) (sourceSession childSession : Nat)
    (immutableNoop : Bool) : ForkProvenanceCase :=
  let exactPinned :=
    match observation.state.children childRef.version.docId with
    | some fact => fact.source == sourceRef
    | none => false
  { name := name
  , kind := kindName kind
  , disposition := observation.disposition.toContract
  , visibleLogicalTwins := visibleTwins
  , sourceAuthoritative := sourceAuthoritative
  , sourceSessionId := sourceSession
  , childSessionId := childSession
  , sourceDocId := sourceRef.version.docId
  , sourceCid := sourceRef.version.compositeCommitCid
  , sourceSignerDid := sourceRef.signerDid
  , childDocId := childRef.version.docId
  , childCid := childRef.version.compositeCommitCid
  , childSignerDid := childRef.signerDid
  , childCallRequired := requiresChildCall kind
  , childCallSatisfied := childCallSatisfied
  , exactSourcePinned := exactPinned
  , immutableNoop := immutableNoop }

def forkProvenanceCases : List ForkProvenanceCase :=
  [ caseOf "message_child_fact_applied" .message messageApplied 0 true true
      sourceRef childRef 1 2 false
  , caseOf "identical_child_replay_is_idempotent" .message messageReplay 1 true true
      sourceRef childRef 1 2 true
  , caseOf "logical_key_twins_fail_closed" .message messageTwin 2 true true
      sourceRef childRef 1 2 true
  , caseOf "same_session_relabel_is_rejected" .message sameSession 0 true true
      sourceRef childRef 1 1 true
  , caseOf "wrong_node_signer_is_rejected" .message wrongNode 0 true true
      sourceRef childRef 1 2 true
  , caseOf "unsigned_child_is_rejected" .message unsignedChild 0 true true
      sourceRef { childRef with signatureValid := false } 1 2 true
  , caseOf "unsigned_source_is_rejected" .message unsignedSource 0 false true
      unsignedSourceRef childRef 1 2 true
  , caseOf "payload_rebinding_is_rejected" .message payloadConflict 1 true true
      sourceRef childRef 1 2 true
  , caseOf "tool_result_pins_new_child_call" .toolResult resultApplied 0 true true
      resultSourceRef resultChildRef 1 2 false
  , caseOf "tool_result_without_child_call_is_rejected" .toolResult
      resultWithoutChildCall 0 true false resultSourceRef resultChildRef 1 2 true
  ]

end Conformance.ContractCases
