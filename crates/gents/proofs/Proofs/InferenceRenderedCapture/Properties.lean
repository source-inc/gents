import Proofs.InferenceRenderedCapture.Transition

namespace InferenceRenderedCapture

open RenderedCapture

/-- Queue admission alone creates no rendered request. -/
theorem queue_only_has_no_render
    (callVersion : DocumentVersionRef) (key : CaptureKey) (request : CanonicalRequest) :
    let machine := Machine.queueOnly callVersion key request
    machine.stage = .queueOnly ∧ machine.callState = .queued ∧
      machine.rendered machine.key = none ∧
      machine.runningVersion = none ∧ machine.renderVersion = none ∧
      machine.callRenderVersion = none := by
  simp [Machine.queueOnly, Store.empty]

/-- Existing immutable rendered facts are never rewritten by later call heads. -/
theorem Step.rendered_monotone {pre post : Machine} (h_step : Step pre post)
    {probe : CaptureKey} {stored : RenderedFact}
    (h_stored : pre.rendered probe = some stored) :
    post.rendered probe = some stored := by
  cases h_step with
  | startRunning _ _ _ h_post => subst post; exact h_stored
  | captureFresh _ _ _ _ _ h_unbound h_post =>
      subst post
      by_cases h_key : probe = pre.key
      · subst probe
        rw [h_unbound] at h_stored
        contradiction
      · simpa [Store.bind, Machine.expectedFact, h_key] using h_stored
  | captureIdempotent _ _ _ _ _ _ _ _ h_post => subst post; exact h_stored
  | captureRejected _ _ _ _ _ h_post => subst post; exact h_stored
  | bindRenderToCall _ _ _ _ _ _ _ _ _ _ _ h_post => subst post; exact h_stored
  | send _ _ _ _ _ _ _ _ _ h_post => subst post; exact h_stored
  | recoverBeforeSend _ _ _ _ _ _ _ _ _ _ _ h_post => subst post; exact h_stored
  | networkFailure _ _ _ _ _ _ _ _ _ _ _ h_post => subst post; exact h_stored

theorem Trace.rendered_monotone {pre post : Machine} (h_trace : Trace pre post) :
    ∀ {probe : CaptureKey} {stored : RenderedFact},
      pre.rendered probe = some stored → post.rendered probe = some stored := by
  induction h_trace with
  | refl => intro _ _ h; exact h
  | step h_step _ ih => intro _ _ h; exact ih (h_step.rendered_monotone h)

/-- A send is possible only after V2 pins R and R pins exact running V1. -/
theorem send_requires_bidirectional_exact_chain
    {pre post : Machine} (h_step : Step pre post) (h_sent : post.stage = .sent) :
    ∃ runningVersion renderVersion,
      versionExact runningVersion = true ∧
      versionExact renderVersion = true ∧
      pre.callState = .running ∧
      pre.runningVersion = some runningVersion ∧
      pre.renderVersion = some renderVersion ∧
      pre.callRenderVersion = some renderVersion ∧
      pre.rendered pre.key = some (pre.expectedFact renderVersion runningVersion) := by
  cases h_step with
  | startRunning _ _ _ h_post => subst post; simp at h_sent
  | captureFresh _ _ _ _ _ _ h_post => subst post; simp at h_sent
  | captureIdempotent _ _ _ _ _ _ _ _ h_post => subst post; simp at h_sent
  | captureRejected _ _ _ _ _ h_post => subst post; simp at h_sent
  | bindRenderToCall _ _ _ _ _ _ _ _ _ _ _ h_post => subst post; simp at h_sent
  | send running render _ h_call h_running h_render h_reverse h_running_exact
      h_render_exact h_durable h_post =>
      subst post
      exact ⟨running, render, h_running_exact, h_render_exact, h_call,
        h_running, h_render, h_reverse, h_durable⟩
  | recoverBeforeSend _ _ _ _ _ _ _ _ _ _ _ h_post => subst post; simp at h_sent
  | networkFailure _ _ _ _ _ _ _ _ _ _ _ h_post => subst post; simp at h_sent

/-- Direct form of the V1 -> R -> V2 fence. -/
theorem pre_send_call_pins_immutable_render
    {pre post : Machine} (h_step : Step pre post)
    (h_bound : post.stage = .preSendBound) :
    ∃ runningVersion renderVersion,
      post.callRenderVersion = some renderVersion ∧
      post.rendered post.key = some (post.expectedFact renderVersion runningVersion) ∧
      post.callVersion.docId = runningVersion.docId ∧
      post.callVersion.compositeCommitCid ≠ runningVersion.compositeCommitCid := by
  cases h_step with
  | startRunning _ _ _ h_post => subst post; simp at h_bound
  | captureFresh _ _ _ _ _ _ h_post => subst post; simp at h_bound
  | captureIdempotent _ _ _ _ _ _ _ _ h_post => subst post; simp at h_bound
  | captureRejected _ _ _ _ _ h_post => subst post; simp at h_bound
  | bindRenderToCall running render preSend _ _ _ _ _ h_durable h_same h_new _ h_post =>
      subst post
      exact ⟨running, render, rfl, h_durable, h_same, h_new⟩
  | send _ _ _ _ _ _ _ _ _ h_post => subst post; simp at h_bound
  | recoverBeforeSend _ _ _ _ _ _ _ _ _ _ _ h_post => subst post; simp at h_bound
  | networkFailure _ _ _ _ _ _ _ _ _ _ _ h_post => subst post; simp at h_bound

/-- Rejected capture is an explicit absorbing state, not a fail-open retry. -/
theorem capture_rejected_enters_failure
    {pre post : Machine} (h_step : Step pre post)
    (h_failure : post.stage = .captureFailed) :
    post.rendered = pre.rendered ∧ post.callState = pre.callState := by
  cases h_step with
  | startRunning _ _ _ h_post => subst post; simp at h_failure
  | captureFresh _ _ _ _ _ _ h_post => subst post; simp at h_failure
  | captureIdempotent _ _ _ _ _ _ _ _ h_post => subst post; simp at h_failure
  | captureRejected _ _ _ _ _ h_post => subst post; exact ⟨rfl, rfl⟩
  | bindRenderToCall _ _ _ _ _ _ _ _ _ _ _ h_post => subst post; simp at h_failure
  | send _ _ _ _ _ _ _ _ _ h_post => subst post; simp at h_failure
  | recoverBeforeSend _ _ _ _ _ _ _ _ _ _ _ h_post => subst post; simp at h_failure
  | networkFailure _ _ _ _ _ _ _ _ _ _ _ h_post => subst post; simp at h_failure

theorem capture_failed_has_no_successor {pre post : Machine}
    (h_stage : pre.stage = .captureFailed) : ¬ Step pre post := by
  intro h_step
  cases h_step with
  | startRunning _ h _ _ => rw [h_stage] at h; contradiction
  | captureFresh _ h _ _ _ _ _ => rw [h_stage] at h; contradiction
  | captureIdempotent _ _ h _ _ _ _ _ _ => rw [h_stage] at h; contradiction
  | captureRejected _ _ h _ _ _ => rw [h_stage] at h; contradiction
  | bindRenderToCall _ _ _ h _ _ _ _ _ _ _ _ => rw [h_stage] at h; contradiction
  | send _ _ h _ _ _ _ _ _ _ => rw [h_stage] at h; contradiction
  | recoverBeforeSend _ _ _ h _ _ _ _ _ _ _ _ => rw [h_stage] at h; contradiction
  | networkFailure _ _ _ h _ _ _ _ _ _ _ _ => rw [h_stage] at h; contradiction

/-- Capture failure permanently blocks the HTTP send. -/
theorem capture_failure_blocks_send {init final : Machine}
    (h_stage : init.stage = .captureFailed)
    (h_trace : Trace init final) :
    final.stage ≠ .sent := by
  intro h_sent
  cases h_trace with
  | refl => rw [h_stage] at h_sent; contradiction
  | step h_step _ => exact capture_failed_has_no_successor h_stage h_step

/-- Crash after V2 and before HTTP leaves R durable and terminalizes the call,
but the state is explicitly not `sent`. -/
theorem recovery_before_send_is_not_a_send_and_preserves_render
    {pre post : Machine} (h_step : Step pre post)
    (h_recovered : post.stage = .recoveredBeforeSend) :
    post.stage ≠ .sent ∧ post.callState = .failed ∧
      post.callRenderVersion = pre.callRenderVersion ∧
      post.rendered = pre.rendered := by
  cases h_step with
  | startRunning _ _ _ h_post => subst post; simp at h_recovered
  | captureFresh _ _ _ _ _ _ h_post => subst post; simp at h_recovered
  | captureIdempotent _ _ _ _ _ _ _ _ h_post => subst post; simp at h_recovered
  | captureRejected _ _ _ _ _ h_post => subst post; simp at h_recovered
  | bindRenderToCall _ _ _ _ _ _ _ _ _ _ _ h_post => subst post; simp at h_recovered
  | send _ _ _ _ _ _ _ _ _ h_post => subst post; simp at h_recovered
  | recoverBeforeSend _ _ _ _ _ _ _ _ _ _ _ h_post =>
      subst post
      exact ⟨by simp, rfl, rfl, rfl⟩
  | networkFailure _ _ _ _ _ _ _ _ _ _ _ h_post => subst post; simp at h_recovered

/-- HTTP/network failure occurs after the one send. Terminal V3 leaves R and
the V2 reverse edge intact. -/
theorem network_failure_leaves_render_and_failed_call
    {pre post : Machine} (h_step : Step pre post)
    (h_failed : post.stage = .networkFailed) :
    pre.stage = .sent ∧ post.callState = .failed ∧
      post.callRenderVersion = pre.callRenderVersion ∧
      post.rendered = pre.rendered := by
  cases h_step with
  | startRunning _ _ _ h_post => subst post; simp at h_failed
  | captureFresh _ _ _ _ _ _ h_post => subst post; simp at h_failed
  | captureIdempotent _ _ _ _ _ _ _ _ h_post => subst post; simp at h_failed
  | captureRejected _ _ _ _ _ h_post => subst post; simp at h_failed
  | bindRenderToCall _ _ _ _ _ _ _ _ _ _ _ h_post => subst post; simp at h_failed
  | send _ _ _ _ _ _ _ _ _ h_post => subst post; simp at h_failed
  | recoverBeforeSend _ _ _ _ _ _ _ _ _ _ _ h_post => subst post; simp at h_failed
  | networkFailure _ _ _ h_stage _ _ _ _ _ _ _ h_post =>
      subst post
      exact ⟨h_stage, rfl, rfl, rfl⟩

/-- Later terminal call heads cannot rewrite either side of the pinned V1/R
pair. -/
theorem terminal_head_preserves_version_stable_pair
    {pre post : Machine} (h_step : Step pre post)
    (h_terminal : post.stage = .recoveredBeforeSend ∨ post.stage = .networkFailed) :
    post.runningVersion = pre.runningVersion ∧
      post.renderVersion = pre.renderVersion ∧
      post.callRenderVersion = pre.callRenderVersion ∧
      post.rendered = pre.rendered := by
  cases h_step with
  | startRunning _ _ _ h_post => subst post; simp at h_terminal
  | captureFresh _ _ _ _ _ _ h_post => subst post; simp at h_terminal
  | captureIdempotent _ _ _ _ _ _ _ _ h_post => subst post; simp at h_terminal
  | captureRejected _ _ _ _ _ h_post => subst post; simp at h_terminal
  | bindRenderToCall _ _ _ _ _ _ _ _ _ _ _ h_post => subst post; simp at h_terminal
  | send _ _ _ _ _ _ _ _ _ h_post => subst post; simp at h_terminal
  | recoverBeforeSend _ _ _ _ _ _ _ _ _ _ _ h_post => subst post; exact ⟨rfl, rfl, rfl, rfl⟩
  | networkFailure _ _ _ _ _ _ _ _ _ _ _ h_post => subst post; exact ⟨rfl, rfl, rfl, rfl⟩

end InferenceRenderedCapture
