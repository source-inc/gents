import Proofs.Request.Executable

namespace RequestContext

theorem terminal_implies_released_local
    {r : RequestContext}
    (h_coherent : r.coherent)
    (h_term : isTerminal r.state) :
    r.admission = .released := by
  cases r with
  | mk state origin backend admission deadline requestDeadline claimTime currentTime retryCount maxRetries progressSeq messageSeq isLatest persistence interruptRequestedAt validUntil subagentDepth causedByParentRequestId causedByParentToolCallId =>
    cases h_term with
    | inl h =>
      cases h
      cases admission <;> simp [coherent, coherentStateAdmission] at h_coherent
      rfl
    | inr h =>
      cases h with
      | inl h =>
        cases h
        cases admission <;> simp [coherent, coherentStateAdmission] at h_coherent
        rfl
      | inr h =>
        cases h with
        | inl h =>
          cases h
          cases admission <;> simp [coherent, coherentStateAdmission] at h_coherent
          rfl
        | inr h =>
          cases h with
          | inl h =>
            cases h
            cases admission <;> simp [coherent, coherentStateAdmission] at h_coherent
            rfl
          | inr h =>
            cases h
            cases admission <;> simp [coherent, coherentStateAdmission] at h_coherent
            rfl

theorem identity_fields_preserved
    {pre post : RequestContext}
    (h_trans : Transition pre post) :
    post.origin = pre.origin ∧
    post.backend = pre.backend ∧
    post.maxRetries = pre.maxRetries ∧
    post.messageSeq = pre.messageSeq ∧
    post.isLatest = pre.isLatest ∧
    post.interruptRequestedAt = pre.interruptRequestedAt ∧
    post.validUntil = pre.validUntil ∧
    post.subagentDepth = pre.subagentDepth ∧
    post.causedByParentRequestId = pre.causedByParentRequestId ∧
    post.causedByParentToolCallId = pre.causedByParentToolCallId ∧
    post.requestDeadline = pre.requestDeadline ∧
    post.retryCount = pre.retryCount ∧
    post.currentTime = pre.currentTime := by
  cases h_trans <;> simp_all

theorem backend_binding_preserved
    {pre post : RequestContext}
    (h_trans : Transition pre post) :
    post.backend = pre.backend :=
  (identity_fields_preserved h_trans).2.1

theorem origin_preserved
    {pre post : RequestContext}
    (h_trans : Transition pre post) :
    post.origin = pre.origin :=
  (identity_fields_preserved h_trans).1

theorem transition_produces_coherent
    {pre post : RequestContext}
    (h_trans : Transition pre post) :
    post.coherent := by
  cases h_trans with
  | claim _ _ _ h_post =>
    rw [coherent, h_post]
    simp [coherentStateAdmission]
  | dedup_lose _ h_release h_post =>
    rw [coherent, h_post]
    simp [coherentStateAdmission, h_release]
  | admission_reject _ h_release h_post =>
    rw [coherent, h_post]
    simp [coherentStateAdmission, h_release]
  | begin_inference _ _ h_post =>
    rw [coherent, h_post]
    simp [coherentStateAdmission]
  | advance h_state h_admission h_post =>
    rw [coherent, h_post]
    simp [coherentStateAdmission, h_state, h_admission]
  | finish _ _ h_post =>
    rw [coherent, h_post]
    simp [coherentStateAdmission]
  | fail _ _ h_post =>
    rw [coherent, h_post]
    simp [coherentStateAdmission]
  | fail_before_stream _ _ h_post =>
    rw [coherent, h_post]
    simp [coherentStateAdmission]
  | expire _ _ _ _ h_post =>
    rw [coherent, h_post]
    simp [coherentStateAdmission]
  | interrupt_before_claim _ _ _ h_post =>
    rw [coherent, h_post]
    simp [coherentStateAdmission]
  | interrupt_claimed _ _ _ h_post =>
    rw [coherent, h_post]
    simp [coherentStateAdmission]
  | interrupt_processing _ _ _ h_post =>
    rw [coherent, h_post]
    simp [coherentStateAdmission]

theorem claimed_coherent_cases
    {r : RequestContext}
    (h_state : r.state = .claimed)
    (h_coherent : r.coherent) :
    r.admission = .waiting ∨ r.admission = .acquired := by
  cases r with
  | mk state origin backend admission deadline requestDeadline claimTime currentTime retryCount maxRetries progressSeq messageSeq isLatest persistence interruptRequestedAt validUntil subagentDepth causedByParentRequestId causedByParentToolCallId =>
    cases h_state
    cases admission <;> simp [coherent, coherentStateAdmission] at h_coherent ⊢

theorem claim_requires_ttl_open
    {pre post : RequestContext}
    (h_trans : Transition pre post)
    (h_claimed : post.state = .claimed) :
    pre.ttlOpen := by
  cases h_trans with
  | claim _ _ h_ttl _ =>
      exact h_ttl
  | dedup_lose _ _ h_post =>
      simp [h_post] at h_claimed
  | admission_reject _ _ h_post =>
      simp [h_post] at h_claimed
  | begin_inference _ _ h_post =>
      simp [h_post] at h_claimed
  | advance h_state _ h_post =>
      simp [h_post, h_state] at h_claimed
  | finish _ _ h_post =>
      simp [h_post] at h_claimed
  | fail _ _ h_post =>
      simp [h_post] at h_claimed
  | fail_before_stream _ _ h_post =>
      simp [h_post] at h_claimed
  | expire _ _ _ _ h_post =>
      simp [h_post] at h_claimed
  | interrupt_before_claim _ _ _ h_post =>
      simp [h_post] at h_claimed
  | interrupt_claimed _ _ _ h_post =>
      simp [h_post] at h_claimed
  | interrupt_processing _ _ _ h_post =>
      simp [h_post] at h_claimed

theorem claim_with_ttl_bounds_time
    {pre post : RequestContext}
    {t : Time}
    (h_trans : Transition pre post)
    (h_claimed : post.state = .claimed)
    (h_ttl : pre.validUntil = some t) :
    pre.currentTime ≤ t := by
  have h_open := claim_requires_ttl_open h_trans h_claimed
  unfold ttlOpen at h_open
  simpa [h_ttl] using h_open

theorem claim_deadline_explicit
    {pre post : RequestContext}
    {t : Time}
    (h_trans : Transition pre post)
    (h_claimed : post.state = .claimed)
    (h_requestDeadline : pre.requestDeadline = some t) :
    post.deadline = t := by
  cases h_trans with
  | claim _ _ _ h_post =>
      simp [h_post, claimDeadline, h_requestDeadline]
  | dedup_lose _ _ h_post =>
      simp [h_post] at h_claimed
  | admission_reject _ _ h_post =>
      simp [h_post] at h_claimed
  | begin_inference _ _ h_post =>
      simp [h_post] at h_claimed
  | advance h_state _ h_post =>
      simp [h_post, h_state] at h_claimed
  | finish _ _ h_post =>
      simp [h_post] at h_claimed
  | fail _ _ h_post =>
      simp [h_post] at h_claimed
  | fail_before_stream _ _ h_post =>
      simp [h_post] at h_claimed
  | expire _ _ _ _ h_post =>
      simp [h_post] at h_claimed
  | interrupt_before_claim _ _ _ h_post =>
      simp [h_post] at h_claimed
  | interrupt_claimed _ _ _ h_post =>
      simp [h_post] at h_claimed
  | interrupt_processing _ _ _ h_post =>
      simp [h_post] at h_claimed

theorem claim_deadline_default
    {pre post : RequestContext}
    (h_trans : Transition pre post)
    (h_claimed : post.state = .claimed)
    (h_requestDeadline : pre.requestDeadline = none) :
    post.deadline = pre.currentTime + 1 := by
  cases h_trans with
  | claim _ _ _ h_post =>
      simp [h_post, claimDeadline, h_requestDeadline]
  | dedup_lose _ _ h_post =>
      simp [h_post] at h_claimed
  | admission_reject _ _ h_post =>
      simp [h_post] at h_claimed
  | begin_inference _ _ h_post =>
      simp [h_post] at h_claimed
  | advance h_state _ h_post =>
      simp [h_post, h_state] at h_claimed
  | finish _ _ h_post =>
      simp [h_post] at h_claimed
  | fail _ _ h_post =>
      simp [h_post] at h_claimed
  | fail_before_stream _ _ h_post =>
      simp [h_post] at h_claimed
  | expire _ _ _ _ h_post =>
      simp [h_post] at h_claimed
  | interrupt_before_claim _ _ _ h_post =>
      simp [h_post] at h_claimed
  | interrupt_claimed _ _ _ h_post =>
      simp [h_post] at h_claimed
  | interrupt_processing _ _ _ h_post =>
      simp [h_post] at h_claimed

end RequestContext
