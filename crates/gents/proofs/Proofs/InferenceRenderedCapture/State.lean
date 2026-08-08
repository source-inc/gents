import Proofs.InferenceCall
import Proofs.RenderedCapture

/-!
# Inference-call / rendered-request composition

The provider boundary is a bidirectional exact-version chain:

```
running InferenceCall V1
  -> immutable RenderedRequest R (R pins V1)
  -> pre-send InferenceCall V2 (V2 pins R)
  -> one HTTP send
  -> terminal InferenceCall V3 (V3 preserves R)
```

The rendered fact and every call edge use DefraDB `_docID` plus composite CID,
never only the logical `call_id`. A one-shot attempt is not exceptional: it
must create the same running call chain before it may send.
-/

namespace InferenceRenderedCapture

open RenderedCapture

/-- One immutable rendered-request fact and its exact DefraDB version. -/
structure RenderedFact where
  version : DocumentVersionRef
  key : CaptureKey
  request : CanonicalRequest
  /-- Exact running `InferenceCall` V1 consumed by this render. -/
  runningCall : DocumentVersionRef
  deriving DecidableEq, Repr

/-- Durable immutable rendered facts, keyed by provider attempt. -/
abbrev Store := CaptureKey → Option RenderedFact

namespace Store

def empty : Store := fun _ => none

def bind (store : Store) (fact : RenderedFact) : Store :=
  fun probe => if probe = fact.key then some fact else store probe

@[simp] theorem bind_self (store : Store) (fact : RenderedFact) :
    bind store fact fact.key = some fact := by
  simp [bind]

@[simp] theorem bind_other (store : Store) (fact : RenderedFact) (probe : CaptureKey)
    (h : probe ≠ fact.key) : bind store fact probe = store probe := by
  simp [bind, h]

end Store

/-- A usable exact version has both DefraDB identities. -/
def versionExact (version : DocumentVersionRef) : Bool :=
  version.docId != 0 && version.compositeCommitCid != 0

/-- Provider-attempt phase. The two failed stages deliberately distinguish
whether the HTTP send happened. -/
inductive Stage where
  | queueOnly
  | running
  | captureFailed
  | renderDurable
  | preSendBound
  | sent
  | recoveredBeforeSend
  | networkFailed
  deriving DecidableEq, Repr

namespace Stage

def toContract : Stage → String
  | .queueOnly => "queue_only"
  | .running => "running"
  | .captureFailed => "capture_failed"
  | .renderDurable => "render_durable"
  | .preSendBound => "pre_send_bound"
  | .sent => "sent"
  | .recoveredBeforeSend => "recovered_before_send"
  | .networkFailed => "network_failed"

end Stage

/-- Composed state for one physical inference call and one provider attempt.

`callVersion` is the mutable call's current exact head. `renderVersion` and
`runningVersion` become immutable once capture succeeds. `callRenderVersion`
is the reverse edge first written into call V2 and preserved by V3. -/
structure Machine where
  callVersion : DocumentVersionRef
  callState : InferenceCallState
  callRenderVersion : Option DocumentVersionRef
  rendered : Store
  stage : Stage
  key : CaptureKey
  request : CanonicalRequest
  runningVersion : Option DocumentVersionRef
  renderVersion : Option DocumentVersionRef

def Machine.expectedFact (machine : Machine)
    (renderVersion runningVersion : DocumentVersionRef) : RenderedFact :=
  { version := renderVersion
  , key := machine.key
  , request := machine.request
  , runningCall := runningVersion
  }

def Machine.queueOnly
    (callVersion : DocumentVersionRef)
    (key : CaptureKey)
    (request : CanonicalRequest) : Machine :=
  { callVersion := callVersion
  , callState := .queued
  , callRenderVersion := none
  , rendered := Store.empty
  , stage := .queueOnly
  , key := key
  , request := request
  , runningVersion := none
  , renderVersion := none
  }

def Machine.running
    (callVersion : DocumentVersionRef)
    (key : CaptureKey)
    (request : CanonicalRequest)
    (rendered : Store := Store.empty) : Machine :=
  { callVersion := callVersion
  , callState := .running
  , callRenderVersion := none
  , rendered := rendered
  , stage := .running
  , key := key
  , request := request
  , runningVersion := none
  , renderVersion := none
  }

end InferenceRenderedCapture
