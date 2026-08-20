import Proofs.Background.Properties.Structure

namespace Subagent
namespace BridgedState

theorem foreground_blocks_parent_advance
    (pre post : BridgedState)
    (h_fg     : ∃ t ∈ pre.parent.tools,
                  t.awaitMode = .foreground ∧
                  ¬ isTerminal t.state)
    (h_step   : Transition pre post) :
    pre.parent.request.progressSeq = post.parent.request.progressSeq ∧
    pre.parent.request.messageSeq  = post.parent.request.messageSeq := by
  cases h_step with
  | parent_step h_inner h_child_eq h_bridge_eq _ _ =>
    cases h_inner with
    | request_step h_req _ _ h_tools _ _ h_no_block =>
      cases h_req with
      | claim _ _ _ h_post =>
        constructor <;> rw [h_post]
      | dedup_lose _ _ h_post =>
        constructor <;> rw [h_post]
      | admission_reject _ _ h_post =>
        constructor <;> rw [h_post]
      | begin_inference h_pre_claimed _ h_post =>
        exfalso
        apply h_no_block
        · refine Or.inr ⟨h_pre_claimed, ?_⟩
          rw [h_post]
        ·
          exact h_fg
      | advance _ _ h_post =>
        exfalso
        apply h_no_block
        · refine Or.inl ?_
          rw [h_post]
          exact Nat.lt_succ_self _
        · exact h_fg
      | finish _ _ h_post =>
        constructor <;> rw [h_post]
      | fail _ _ h_post =>
        constructor <;> rw [h_post]
      | fail_before_stream _ _ h_post =>
        constructor <;> rw [h_post]
      | expire _ _ _ _ h_post =>
        constructor <;> rw [h_post]
      | interrupt_before_claim _ _ _ h_post =>
        constructor <;> rw [h_post]
      | interrupt_claimed _ _ _ h_post =>
        constructor <;> rw [h_post]
      | interrupt_processing _ _ _ h_post =>
        constructor <;> rw [h_post]
    | tool_step _ _ _ h_req_eq _ _ _ _ _ =>
      constructor <;> rw [h_req_eq]
    | process_step _ h_req _ _ _ =>
      constructor <;> rw [h_req]
    | slot_acquire _ _ h_req _ _ _ _ =>
      constructor <;> simp [h_req]
    | request_interrupt _ h_req _ _ _ _ =>
      constructor <;> simp [h_req]
    | clock_advance _ _ h_req _ _ _ _ =>
      constructor <;> simp [h_req]
    | persistence_step _ _ _ h_req _ _ _ _ =>
      constructor <;> rw [h_req]
    | call_step _ h_req _ _ _ =>
      constructor <;> rw [h_req]
    | tool_spawn _ _ _ h_req _ _ _ _ _ _ =>
      constructor <;> rw [h_req]
  | child_step _ h_parent_eq _ _ _ =>
    constructor <;> rw [h_parent_eq]
  | bridge_spawn _ _ _ _ _ _ _ _ h_request_eq _ _ =>
    constructor <;> rw [h_request_eq]
  | bridge_complete _ _ _ _ _ _ _ _ _ _ h_request_eq _ _ _ =>
    constructor <;> rw [h_request_eq]
  | bridge_failure _ _ _ _ _ _ _ _ _ h_request_eq _ _ _ =>
    constructor <;> rw [h_request_eq]
  | bridge_cancel_cascade _ _ _ h_parent_eq _ _ _ _ _ _ =>
    constructor <;> rw [h_parent_eq]

theorem subagent_depth_bounded
    (pre post : BridgedState)
    (h_init  : pre.parent.request.subagentDepth ≤ maxSubagentDepth ∧
               pre.child.request.subagentDepth ≤ maxSubagentDepth)
    (h_trace : Trace pre post) :
    post.parent.request.subagentDepth ≤ maxSubagentDepth ∧
    post.child.request.subagentDepth ≤ maxSubagentDepth :=
  inv_depth pre post h_init h_trace

theorem bridge_link_symmetric
    (pre post : BridgedState)
    (h_init  : pre.linked)
    (h_trace : Trace pre post) :
    post.linked :=
  inv_link pre post h_init h_trace

theorem steer_subagent_interrupt_preserves_link_symmetry
    {pre post : BridgedState}
    {childSessionId : SessionId}
    {queuePre queueDrained queuePost : SessionQueue.SessionQueueState}
    {transcriptPre transcriptPost : Transcript.TranscriptState}
    {childRequestId steeringRequestId : RequestId}
    {message : String}
    (h_step : SteerWithInterrupt
      pre post
      childSessionId
      queuePre queueDrained queuePost
      transcriptPre transcriptPost
      childRequestId steeringRequestId message)
    (h_pre  : pre.linked) :
    post.linked := by
  rcases h_step.h_bridge_compose with
    ⟨cascaded, interrupted, h_cascade, h_interrupt, _h_child_id, h_tail⟩
  have _h_queue_session : queuePost.sessionId = childSessionId :=
    h_step.h_queue_post_session
  have _h_drain_uses_child_key :
      queueDrained =
        queuePre.drainAutomatedWakeups
          SessionQueue.QueueSource.backgroundCompletion
          (some (backgroundCompletionQueueKey childSessionId)) :=
    h_step.h_drain_shape
  rcases h_step.h_append_compose with
    ⟨entry, _h_append_transition, _h_append_shape,
      h_entry_request, h_entry_source, h_entry_policy, _h_entry_key⟩
  have _h_append_is_steering :
      entry.source = SessionQueue.QueueSource.steering ∧
      entry.policy = SessionQueue.QueuePolicy.append ∧
      entry.requestId = steeringRequestId :=
    ⟨h_entry_source, h_entry_policy, h_entry_request⟩
  have _h_transcript_session : transcriptPost.sessionId = childSessionId :=
    h_step.h_transcript_post_session
  rcases h_step.h_transcript_append with
    ⟨messageId, _h_message_nonempty, _h_transcript_transition, h_transcript_shape⟩
  have _h_transcript_is_user_append :
      transcriptPost =
        transcriptPre.appendUserMessage
          messageId
          Transcript.MessageKind.ordinary :=
    h_transcript_shape
  have h_trace : Trace pre post :=
    Trace.step
      (BridgeCancelCascadeStep.to_transition h_cascade)
      (Trace.step (ChildInterruptStep.to_transition h_interrupt) h_tail)
  exact bridge_link_symmetric pre post h_pre h_trace

end BridgedState
end Subagent
