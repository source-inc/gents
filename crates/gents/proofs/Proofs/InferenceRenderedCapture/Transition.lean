import Proofs.InferenceRenderedCapture.State

namespace InferenceRenderedCapture

open RenderedCapture

inductive CaptureOutcome where
  | fresh
  | idempotent
  | rejected
  deriving DecidableEq, Repr

namespace CaptureOutcome

def toContract : CaptureOutcome → String
  | .fresh => "fresh"
  | .idempotent => "idempotent"
  | .rejected => "rejected"

def durable : CaptureOutcome → Bool
  | .fresh => true
  | .idempotent => true
  | .rejected => false

end CaptureOutcome

/-- Immutable rendered-fact decision. Both the rendered version and the pinned
running-call version must be exact. -/
def capture (store : Store) (fact : RenderedFact) : CaptureOutcome × Store :=
  if versionExact fact.version && versionExact fact.runningCall then
    match store fact.key with
    | none => (.fresh, Store.bind store fact)
    | some stored =>
        if stored = fact then (.idempotent, store) else (.rejected, store)
  else
    (.rejected, store)

/-- Legal composed transitions. There is intentionally no `send` transition
from `sent`: one capture arm permits one explained HTTP send. A retry creates a
new `InferenceCall`/capture key and therefore a new machine. -/
inductive Step : Machine → Machine → Prop where
  | startRunning {pre post : Machine} (runningVersion : DocumentVersionRef)
      (h_stage : pre.stage = .queueOnly)
      (h_doc : runningVersion.docId = pre.callVersion.docId)
      (h_exact : versionExact runningVersion = true)
      (h_post : post =
        { pre with callVersion := runningVersion
                 , callState := .running
                 , stage := .running }) :
      Step pre post
  | captureFresh {pre post : Machine} (renderVersion : DocumentVersionRef)
      (h_stage : pre.stage = .running)
      (h_call : pre.callState = .running)
      (h_render_exact : versionExact renderVersion = true)
      (h_call_exact : versionExact pre.callVersion = true)
      (h_unbound : pre.rendered pre.key = none)
      (h_post : post =
        let fact := pre.expectedFact renderVersion pre.callVersion
        { pre with rendered := Store.bind pre.rendered fact
                 , stage := .renderDurable
                 , runningVersion := some pre.callVersion
                 , renderVersion := some renderVersion }) :
      Step pre post
  | captureIdempotent {pre post : Machine}
      (renderVersion runningVersion : DocumentVersionRef)
      (h_stage : pre.stage = .running)
      (h_call : pre.callState = .running)
      (h_current : pre.callVersion = runningVersion)
      (h_render_exact : versionExact renderVersion = true)
      (h_call_exact : versionExact runningVersion = true)
      (h_bound : pre.rendered pre.key =
        some (pre.expectedFact renderVersion runningVersion))
      (h_post : post =
        { pre with stage := .renderDurable
                 , runningVersion := some runningVersion
                 , renderVersion := some renderVersion }) :
      Step pre post
  | captureRejected {pre post : Machine} (candidate stored : RenderedFact)
      (h_stage : pre.stage = .running)
      (h_candidate : candidate =
        pre.expectedFact candidate.version pre.callVersion)
      (h_bound : pre.rendered pre.key = some stored)
      (h_conflict : stored ≠ candidate)
      (h_post : post = { pre with stage := .captureFailed }) :
      Step pre post
  /-- Exact pre-send call V2 writes the reverse edge to rendered fact R. -/
  | bindRenderToCall {pre post : Machine}
      (runningVersion renderVersion preSendVersion : DocumentVersionRef)
      (h_stage : pre.stage = .renderDurable)
      (h_call : pre.callState = .running)
      (h_running : pre.runningVersion = some runningVersion)
      (h_render : pre.renderVersion = some renderVersion)
      (h_current : pre.callVersion = runningVersion)
      (h_durable : pre.rendered pre.key =
        some (pre.expectedFact renderVersion runningVersion))
      (h_same_doc : preSendVersion.docId = runningVersion.docId)
      (h_new_head : preSendVersion.compositeCommitCid ≠
        runningVersion.compositeCommitCid)
      (h_exact : versionExact preSendVersion = true)
      (h_post : post =
        { pre with callVersion := preSendVersion
                 , callRenderVersion := some renderVersion
                 , stage := .preSendBound }) :
      Step pre post
  /-- The one HTTP send. Legal only after V2 and both exact edges exist. -/
  | send {pre post : Machine}
      (runningVersion renderVersion : DocumentVersionRef)
      (h_stage : pre.stage = .preSendBound)
      (h_call : pre.callState = .running)
      (h_running : pre.runningVersion = some runningVersion)
      (h_render : pre.renderVersion = some renderVersion)
      (h_reverse : pre.callRenderVersion = some renderVersion)
      (h_running_exact : versionExact runningVersion = true)
      (h_render_exact : versionExact renderVersion = true)
      (h_durable : pre.rendered pre.key =
        some (pre.expectedFact renderVersion runningVersion))
      (h_post : post = { pre with stage := .sent }) :
      Step pre post
  /-- Crash after V2 but before HTTP. Recovery may fail the call, but it cannot
claim a send occurred. -/
  | recoverBeforeSend {pre post : Machine}
      (runningVersion renderVersion terminalVersion : DocumentVersionRef)
      (h_stage : pre.stage = .preSendBound)
      (h_call : pre.callState = .running)
      (h_running : pre.runningVersion = some runningVersion)
      (h_render : pre.renderVersion = some renderVersion)
      (h_reverse : pre.callRenderVersion = some renderVersion)
      (h_durable : pre.rendered pre.key =
        some (pre.expectedFact renderVersion runningVersion))
      (h_same_doc : terminalVersion.docId = pre.callVersion.docId)
      (h_new_head : terminalVersion.compositeCommitCid ≠
        pre.callVersion.compositeCommitCid)
      (h_exact : versionExact terminalVersion = true)
      (h_post : post =
        { pre with callVersion := terminalVersion
                 , callState := .failed
                 , stage := .recoveredBeforeSend }) :
      Step pre post
  /-- HTTP failed after the send. Terminal V3 preserves the reverse edge R. -/
  | networkFailure {pre post : Machine}
      (runningVersion renderVersion terminalVersion : DocumentVersionRef)
      (h_stage : pre.stage = .sent)
      (h_call : pre.callState = .running)
      (h_running : pre.runningVersion = some runningVersion)
      (h_render : pre.renderVersion = some renderVersion)
      (h_reverse : pre.callRenderVersion = some renderVersion)
      (h_durable : pre.rendered pre.key =
        some (pre.expectedFact renderVersion runningVersion))
      (h_same_doc : terminalVersion.docId = pre.callVersion.docId)
      (h_new_head : terminalVersion.compositeCommitCid ≠
        pre.callVersion.compositeCommitCid)
      (h_exact : versionExact terminalVersion = true)
      (h_post : post =
        { pre with callVersion := terminalVersion
                 , callState := .failed
                 , stage := .networkFailed }) :
      Step pre post

inductive Trace : Machine → Machine → Prop where
  | refl {machine : Machine} : Trace machine machine
  | step {pre next post : Machine} : Step pre next → Trace next post → Trace pre post

end InferenceRenderedCapture
