import Proofs.Background.Transition

namespace Subagent
namespace BridgedState

theorem cascade_cancels_child
    (pre : BridgedState)
    (h_parent_term : isTerminal pre.parent.request.state)
    (h_cascade     : ∃ t ∈ pre.parent.tools,
                       t.callId = pre.bridgeCallId ∧
                       t.cancelPolicy = .cascade ∧
                       ¬ isTerminal t.state)
    (h_child_proc      : pre.child.request.state = .processing)
    (h_child_admission : pre.child.request.admission = .executing)
    (h_child_no_fg     : ¬ ∃ t ∈ pre.child.tools, t.awaitMode = .foreground ∧
                                                    ¬ isTerminal t.state)
    (h_linked          : pre.linked) :
    ∃ post, Trace pre post ∧ post.child.request.state = .interrupted := by
  obtain ⟨tCascade, h_in, h_id, h_pol, _h_live⟩ := h_cascade
  let midChildReq : RequestContext :=
    { pre.child.request with
        interruptRequestedAt := some pre.child.request.currentTime }
  let midChild : ComposedState :=
    { pre.child with request := midChildReq }
  let mid : BridgedState := { pre with child := midChild, secondLeg := .subagent midChild }
  let postChildReq : RequestContext :=
    { midChildReq with state := .interrupted, admission := .released }
  let postChild : ComposedState :=
    { midChild with request := postChildReq }
  let post : BridgedState := { mid with child := postChild, secondLeg := .subagent postChild }
  refine ⟨post, ?_, ?_⟩
  ·
    refine @Trace.step pre mid post ?_ (@Trace.step mid post post ?_ Trace.refl)
    ·
      refine Transition.bridge_cancel_cascade
        (Or.inl h_parent_term)
        ⟨tCascade, h_in, h_id, h_pol⟩
        ?_
        rfl
        rfl
        rfl
        ?_
        ?_
        ?_
        rfl
      ·
        show midChildReq.interruptRequestedAt.isSome
        simp [midChildReq]
      ·
        show midChildReq.causedByParentRequestId = pre.child.request.causedByParentRequestId
        rfl
      · show midChildReq.causedByParentToolCallId = pre.child.request.causedByParentToolCallId
        rfl
      · show midChildReq.subagentDepth = pre.child.request.subagentDepth
        rfl
    ·
      have h_link_mid : mid.linked := by
        obtain ⟨h_pLink, h_cReq, h_cTool⟩ := h_linked
        refine ⟨h_pLink, ?_, ?_⟩
        · show midChildReq.causedByParentRequestId = some pre.parent.requestId
          exact h_cReq
        · show midChildReq.causedByParentToolCallId = some pre.bridgeCallId
          exact h_cTool
      have h_link_post : post.linked := by
        obtain ⟨h_pLink, h_cReq, h_cTool⟩ := h_link_mid
        refine ⟨h_pLink, ?_, ?_⟩
        · show postChildReq.causedByParentRequestId = some mid.parent.requestId
          exact h_cReq
        · show postChildReq.causedByParentToolCallId = some mid.bridgeCallId
          exact h_cTool
      have h_inner_req :
          RequestContext.Transition mid.child.request post.child.request := by
        show RequestContext.Transition midChildReq postChildReq
        refine RequestContext.Transition.interrupt_processing ?_ ?_ ?_ ?_
        ·
          show pre.child.request.state = .processing
          exact h_child_proc
        ·
          show pre.child.request.admission = .executing
          exact h_child_admission
        ·
          show (some pre.child.request.currentTime).isSome
          rfl
        ·
          rfl
      have h_inner_composed :
          ComposedState.Transition mid.child post.child := by
        refine ComposedState.Transition.request_step
          h_inner_req
          rfl
          rfl
          rfl
          rfl
          ?_
          ?_
        ·
          intro h_pending
          exfalso
          have h_eq : midChildReq.state = pre.child.request.state := rfl
          rw [h_eq, h_child_proc] at h_pending
          cases h_pending
        ·
          intro h_advance
          exfalso
          rcases h_advance with h_progress | ⟨h_claimed, _⟩
          ·
            have h_eq_seq : postChildReq.progressSeq = midChildReq.progressSeq := rfl
            rw [h_eq_seq] at h_progress
            exact Nat.lt_irrefl _ h_progress
          ·
            have h_eq_state : midChildReq.state = pre.child.request.state := rfl
            rw [h_eq_state, h_child_proc] at h_claimed
            cases h_claimed
      refine Transition.child_step
        h_inner_composed
        rfl
        rfl
        h_link_mid
        h_link_post
  ·
    show postChildReq.state = .interrupted
    rfl

theorem detach_does_not_cancel_child
    (pre post : BridgedState)
    (h_detach    : ∃ t ∈ pre.parent.tools,
                     t.callId = pre.bridgeCallId ∧ t.cancelPolicy = .detach)
    (h_step      : Transition pre post)
    (h_no_other  : ¬ pre.child.request.interruptRequestedAt.isSome)
    (h_uniq      : pre.parent.UniqueCallIds)
    (h_not_direct_interrupt :
      ∀ t : Time,
        post.child.request ≠
          { pre.child.request with interruptRequestedAt := some t }) :
    post.child.request.interruptRequestedAt =
      pre.child.request.interruptRequestedAt := by
  cases h_step with
  | parent_step _ h_child_eq _ _ _ =>
    rw [h_child_eq]
  | child_step h_inner _ _ _ _ =>
    cases h_inner with
    | process_step _ h_req _ _ _ =>
      rw [h_req]
    | request_step h_req_inner _ _ _ _ _ _ =>
      cases h_req_inner with
      | claim _ _ _ h_post =>
        rw [h_post]
      | dedup_lose _ _ h_post =>
        rw [h_post]
      | admission_reject _ _ h_post =>
        rw [h_post]
      | begin_inference _ _ h_post =>
        rw [h_post]
      | advance _ _ h_post =>
        rw [h_post]
      | finish _ _ h_post =>
        rw [h_post]
      | fail _ _ h_post =>
        rw [h_post]
      | fail_before_stream _ _ h_post =>
        rw [h_post]
      | expire _ _ _ _ h_post =>
        rw [h_post]
      | interrupt_before_claim _ _ _ h_post =>
        rw [h_post]
      | interrupt_claimed _ _ _ h_post =>
        rw [h_post]
      | interrupt_processing _ _ _ h_post =>
        rw [h_post]
    | slot_acquire _ _ h_req _ _ _ _ =>
      simp [h_req]
    | request_interrupt t h_req _ _ _ _ =>
      exact absurd h_req (h_not_direct_interrupt t)
    | clock_advance _ _ h_req _ _ _ _ =>
      simp [h_req]
    | persistence_step _ _ _ h_req _ _ _ _ =>
      rw [h_req]
    | call_step _ h_req _ _ _ =>
      rw [h_req]
    | tool_spawn _ _ _ h_req _ _ _ _ _ _ =>
      rw [h_req]
    | tool_step _ _ _ h_req _ _ _ _ _ =>
      rw [h_req]
  | bridge_spawn h_parent_proc _ _ _ _ _ h_post_child _ h_request_eq _ _ =>
    have h_post_none : post.child.request.interruptRequestedAt = none :=
      h_post_child.2.2.2.2
    have h_pre_none : pre.child.request.interruptRequestedAt = none := by
      cases h : pre.child.request.interruptRequestedAt with
      | none => rfl
      | some _ => simp [h] at h_no_other
    rw [h_post_none, h_pre_none]
  | bridge_complete _ _ _ _ _ _ _ _ _ _ _ h_child_eq _ _ =>
    rw [h_child_eq]
  | bridge_failure _ _ _ _ _ _ _ _ _ _ h_child_eq _ _ =>
    rw [h_child_eq]
  | bridge_cancel_cascade _ h_cascade _ _ _ _ _ _ _ _ =>
    obtain ⟨tDet, h_in_d, h_id_d, h_pol_d⟩ := h_detach
    obtain ⟨tCas, h_in_c, h_id_c, h_pol_c⟩ := h_cascade
    have h_callIds : tDet.callId = tCas.callId := by rw [h_id_d, h_id_c]
    have h_same_tool : tDet = tCas :=
      ComposedState.UniqueCallIds.eq_of_callId_eq h_uniq h_in_d h_in_c h_callIds
    rw [h_same_tool] at h_pol_d
    rw [h_pol_c] at h_pol_d
    cases h_pol_d

end BridgedState
end Subagent
