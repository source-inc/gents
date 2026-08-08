import Proofs.Conformance.ContractCases.Types
import Proofs.InferenceRenderedCapture

/-!
# InferenceRenderedCapture contract cases (#1075)

The rows below are projections of concrete machines and stores built from the
composed model. The accompanying trace theorems prove the positive, crash, and
network-failure states are reachable through the relational transition model.
-/

namespace Conformance.ContractCases

open InferenceRenderedCapture

private abbrev VersionRef := RenderedCapture.DocumentVersionRef

structure InferenceRenderedCaptureCase where
  name : String
  initialStage : String
  finalStage : String
  initialCallState : String
  finalCallState : String
  captureOutcome : String
  runningCallDocId : Option Nat
  runningCallCid : Option Nat
  renderDocId : Option Nat
  renderCid : Option Nat
  currentCallCid : Nat
  renderDurable : Bool
  renderPinsRunning : Bool
  callPinsRender : Bool
  httpRequestsObserved : Nat
  terminalFailed : Bool
  secondSendPermitted : Bool
  deriving Repr

private def key : RenderedCapture.CaptureKey :=
  { agentDid := 7, sessionId := 11, requestId := 23, turnIndex := 0, attempt := 0 }

private def request : RenderedCapture.CanonicalRequest :=
  { value := 101, configScope := .staticOrOneShot, configProvenance := none }

private def callQueued : VersionRef :=
  { docId := 300, compositeCommitCid := 30 }

private def callV1 : VersionRef :=
  { docId := 300, compositeCommitCid := 31 }

private def renderR : VersionRef :=
  { docId := 400, compositeCommitCid := 41 }

private def callV2 : VersionRef :=
  { docId := 300, compositeCommitCid := 32 }

private def callV3 : VersionRef :=
  { docId := 300, compositeCommitCid := 33 }

private def queued : Machine := Machine.queueOnly callQueued key request
private def running : Machine := Machine.running callV1 key request
private def fact : RenderedFact := running.expectedFact renderR callV1

private def renderDurable : Machine :=
  { running with rendered := Store.bind running.rendered fact
               , stage := .renderDurable
               , runningVersion := some callV1
               , renderVersion := some renderR }

private def preSend : Machine :=
  { renderDurable with callVersion := callV2
                     , callRenderVersion := some renderR
                     , stage := .preSendBound }

private def sent : Machine := { preSend with stage := .sent }

private def networkFailed : Machine :=
  { sent with callVersion := callV3, callState := .failed, stage := .networkFailed }

private def recoveredBeforeSend : Machine :=
  { preSend with callVersion := callV3
               , callState := .failed
               , stage := .recoveredBeforeSend }

private def idempotentRunning : Machine :=
  Machine.running callV1 key request (Store.bind Store.empty fact)

private def idempotentCaptured : Machine :=
  { idempotentRunning with stage := .renderDurable
                           , runningVersion := some callV1
                           , renderVersion := some renderR }

private def conflictingFact : RenderedFact :=
  { version := { docId := 401, compositeCommitCid := 42 }
  , key := key
  , request := { request with value := 999 }
  , runningCall := callV1 }

private def conflictRunning : Machine :=
  Machine.running callV1 key request (Store.bind Store.empty conflictingFact)

private def captureFailed : Machine :=
  { conflictRunning with stage := .captureFailed }

private theorem renderDurable_bound :
    renderDurable.rendered renderDurable.key =
      some (renderDurable.expectedFact renderR callV1) := by
  simpa [renderDurable, fact, running, Machine.running, Machine.expectedFact] using
    (Store.bind_self Store.empty fact)

private theorem preSend_bound :
    preSend.rendered preSend.key = some (preSend.expectedFact renderR callV1) := by
  simpa [preSend] using renderDurable_bound

private theorem sent_bound :
    sent.rendered sent.key = some (sent.expectedFact renderR callV1) := by
  simpa [sent] using preSend_bound

private theorem idempotentRunning_bound :
    idempotentRunning.rendered idempotentRunning.key =
      some (idempotentRunning.expectedFact renderR callV1) := by
  simpa [idempotentRunning, fact, running, Machine.running, Machine.expectedFact] using
    (Store.bind_self Store.empty fact)

private theorem idempotentCaptured_bound :
    idempotentCaptured.rendered idempotentCaptured.key =
      some (idempotentCaptured.expectedFact renderR callV1) := by
  simpa [idempotentCaptured] using idempotentRunning_bound

private def optionalVersionField
    (field : Machine → Option VersionRef)
    (select : VersionRef → Nat)
    (machine : Machine) : Option Nat :=
  (field machine).map select

private def renderIsDurable (machine : Machine) : Bool :=
  match machine.runningVersion, machine.renderVersion with
  | some runningVersion, some renderVersion =>
      decide (machine.rendered machine.key =
        some (machine.expectedFact renderVersion runningVersion))
  | _, _ => false

private def renderPinsRunning (machine : Machine) : Bool :=
  match machine.rendered machine.key, machine.runningVersion with
  | some rendered, some runningVersion => rendered.runningCall == runningVersion
  | _, _ => false

private def callPinsRender (machine : Machine) : Bool :=
  machine.callRenderVersion == machine.renderVersion && machine.renderVersion.isSome

private def httpRequestsObserved (machine : Machine) : Nat :=
  match machine.stage with
  | .sent | .networkFailed => 1
  | _ => 0

/-- Only the exact V2 state may take the single send transition. Every emitted
final state is therefore non-sendable a second time. -/
private def sendPermitted (machine : Machine) : Bool :=
  machine.stage == .preSendBound && renderIsDurable machine && callPinsRender machine

private def caseOf (name : String) (initial final : Machine)
    (outcome : Option CaptureOutcome) : InferenceRenderedCaptureCase :=
  { name := name
  , initialStage := initial.stage.toContract
  , finalStage := final.stage.toContract
  , initialCallState := initial.callState.toDefraDB
  , finalCallState := final.callState.toDefraDB
  , captureOutcome := outcome.map CaptureOutcome.toContract |>.getD "not_attempted"
  , runningCallDocId := optionalVersionField Machine.runningVersion (fun ref => ref.docId) final
  , runningCallCid := optionalVersionField Machine.runningVersion
      (fun ref => ref.compositeCommitCid) final
  , renderDocId := optionalVersionField Machine.renderVersion (fun ref => ref.docId) final
  , renderCid := optionalVersionField Machine.renderVersion
      (fun ref => ref.compositeCommitCid) final
  , currentCallCid := final.callVersion.compositeCommitCid
  , renderDurable := renderIsDurable final
  , renderPinsRunning := renderPinsRunning final
  , callPinsRender := callPinsRender final
  , httpRequestsObserved := httpRequestsObserved final
  , terminalFailed := decide (final.callState = .failed)
  , secondSendPermitted := sendPermitted final
  }

def inferenceRenderedCaptureCases : List InferenceRenderedCaptureCase :=
  [ caseOf "queue_only_has_no_render" queued queued none
  , caseOf "fresh_capture_exact_v1_r_v2_then_send" running sent (some .fresh)
  , caseOf "idempotent_capture_exact_v1_r_v2_then_send"
      idempotentRunning sent (some .idempotent)
  , caseOf "conflicting_capture_blocks_send" conflictRunning captureFailed (some .rejected)
  , caseOf "crash_after_v2_before_send_recovers_failed_unsent"
      running recoveredBeforeSend (some .fresh)
  , caseOf "network_failure_after_send_preserves_render_and_fails_call"
      running networkFailed (some .fresh)
  , caseOf "one_shot_still_requires_explicit_call_chain" running sent (some .fresh)
  ]

theorem fresh_send_trace : Trace running sent := by
  exact Trace.step
    (Step.captureFresh renderR rfl rfl rfl rfl (by simp [running, Machine.running, Store.empty]) rfl)
    (Trace.step
      (Step.bindRenderToCall callV1 renderR callV2 rfl rfl rfl rfl rfl
        renderDurable_bound rfl (by decide) rfl rfl)
      (Trace.step
        (Step.send callV1 renderR rfl rfl rfl rfl rfl rfl rfl
          preSend_bound rfl)
        Trace.refl))

theorem idempotent_send_trace : Trace idempotentRunning sent := by
  exact Trace.step
    (Step.captureIdempotent renderR callV1 rfl rfl rfl rfl rfl
      idempotentRunning_bound rfl)
    (Trace.step
      (Step.bindRenderToCall callV1 renderR callV2 rfl rfl rfl rfl rfl
        idempotentCaptured_bound rfl (by decide) rfl rfl)
      (Trace.step
        (Step.send callV1 renderR rfl rfl rfl rfl rfl rfl rfl
          preSend_bound rfl)
        Trace.refl))

theorem conflict_trace : Trace conflictRunning captureFailed := by
  exact Trace.step
    (Step.captureRejected fact conflictingFact rfl rfl rfl
      (by decide) rfl)
    Trace.refl

theorem recovery_before_send_trace : Trace running recoveredBeforeSend := by
  exact Trace.step
    (Step.captureFresh renderR rfl rfl rfl rfl (by simp [running, Machine.running, Store.empty]) rfl)
    (Trace.step
      (Step.bindRenderToCall callV1 renderR callV2 rfl rfl rfl rfl rfl
        renderDurable_bound rfl (by decide) rfl rfl)
      (Trace.step
        (Step.recoverBeforeSend callV1 renderR callV3 rfl rfl rfl rfl rfl
          preSend_bound
          rfl (by decide) rfl rfl)
        Trace.refl))

theorem network_failure_trace : Trace running networkFailed := by
  exact Trace.step
    (Step.captureFresh renderR rfl rfl rfl rfl (by simp [running, Machine.running, Store.empty]) rfl)
    (Trace.step
      (Step.bindRenderToCall callV1 renderR callV2 rfl rfl rfl rfl rfl
        renderDurable_bound rfl (by decide) rfl rfl)
      (Trace.step
        (Step.send callV1 renderR rfl rfl rfl rfl rfl rfl rfl
          preSend_bound rfl)
        (Trace.step
          (Step.networkFailure callV1 renderR callV3 rfl rfl rfl rfl rfl
            sent_bound
            rfl (by decide) rfl rfl)
          Trace.refl)))

theorem inferenceRenderedCaptureCases_pinned :
    inferenceRenderedCaptureCases.map
      (fun row => (row.name, row.finalStage, row.captureOutcome,
        row.renderDurable, row.renderPinsRunning, row.callPinsRender,
        row.httpRequestsObserved, row.terminalFailed, row.secondSendPermitted)) =
      [ ("queue_only_has_no_render", "queue_only", "not_attempted",
          false, false, false, 0, false, false)
      , ("fresh_capture_exact_v1_r_v2_then_send", "sent", "fresh",
          true, true, true, 1, false, false)
      , ("idempotent_capture_exact_v1_r_v2_then_send", "sent", "idempotent",
          true, true, true, 1, false, false)
      , ("conflicting_capture_blocks_send", "capture_failed", "rejected",
          false, false, false, 0, false, false)
      , ("crash_after_v2_before_send_recovers_failed_unsent", "recovered_before_send", "fresh",
          true, true, true, 0, true, false)
      , ("network_failure_after_send_preserves_render_and_fails_call", "network_failed", "fresh",
          true, true, true, 1, true, false)
      , ("one_shot_still_requires_explicit_call_chain", "sent", "fresh",
          true, true, true, 1, false, false)
      ] := by
  rfl

end Conformance.ContractCases
