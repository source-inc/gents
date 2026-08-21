import Proofs.Client.Types

theorem deriveAttempt_total (view : AttemptView) :
    ∃ s : ClientTurnState, deriveAttempt view = s :=
  ⟨deriveAttempt view, rfl⟩

theorem deriveTurn_total
    {attempts : List AttemptView}
    (h : attempts ≠ []) :
    ∃ s : ClientTurnState, deriveTurn attempts = some s := by
  induction attempts with
  | nil => contradiction
  | cons head tail ih =>
    cases tail with
    | nil => exact ⟨deriveAttempt head, rfl⟩
    | cons h' t' =>
      simp [deriveTurn]
      exact ih (by simp)

theorem deriveAttempt_nonterminal_response_driven
    {req : RequestSnapshot}
    {resp : Option ResponseSnapshot}
    (h_not_super : req.isSuperseded = false)
    (h_state : req.lifecycleState = .pending ∨ req.lifecycleState = .claimed ∨
               req.lifecycleState = .processing ∨ req.lifecycleState = .inputRequired) :
    deriveAttempt ⟨req, resp⟩ = match resp with
      | some r => match r.status with
        | .complete => .completed
        | .error => .failed
        | .streaming => .streaming
      | none => .waitingForClaim := by
  cases req with
  | mk lifecycleState isSuperseded =>
    rcases h_state with h | h | h | h <;>
      cases h <;> cases h_not_super <;> rfl

def LifecycleTransition : RequestState → RequestState → Prop
  | .pending,        .claimed         => True
  | .pending,        .superseded      => True
  | .pending,        .failed          => True
  | .claimed,        .processing      => True
  | .processing,     .processing      => True
  | .processing,     .completed       => True
  | .processing,     .failed          => True
  | .claimed,        .failed          => True
  | .pending,        .dead            => True
  | .pending,        .interrupted     => True
  | .claimed,        .interrupted     => True
  | .processing,     .interrupted     => True
  | _,               _                => False

theorem transition_implies_lifecycle
    {pre post : RequestContext}
    (h : RequestContext.Transition pre post) :
    LifecycleTransition pre.state post.state := by
  cases h with
  | claim h_state _ _ h_post =>
    subst h_post; simp [LifecycleTransition, h_state]
  | dedup_lose h_state _ h_post =>
    subst h_post; simp [LifecycleTransition, h_state]
  | admission_reject h_state _ h_post =>
    subst h_post; simp [LifecycleTransition, h_state]
  | begin_inference h_state _ h_post =>
    subst h_post; simp [LifecycleTransition, h_state]
  | advance h_state _ h_post =>
    subst h_post; simp [LifecycleTransition, h_state]
  | finish h_state _ h_post =>
    subst h_post; simp [LifecycleTransition, h_state]
  | fail h_state _ h_post =>
    subst h_post; simp [LifecycleTransition, h_state]
  | fail_before_stream h_state _ h_post =>
    subst h_post; simp [LifecycleTransition, h_state]
  | expire h_state _ _ _ h_post =>
    subst h_post; simp [LifecycleTransition, h_state]
  | interrupt_before_claim h_state _ _ h_post =>
    subst h_post; simp [LifecycleTransition, h_state]
  | interrupt_claimed h_state _ _ h_post =>
    subst h_post; simp [LifecycleTransition, h_state]
  | interrupt_processing h_state _ _ h_post =>
    subst h_post; simp [LifecycleTransition, h_state]

theorem lifecycle_transition_monotonic
    {pre_state post_state : RequestState}
    (h_trans : LifecycleTransition pre_state post_state)
    (isSuperseded : Bool)
    (resp : Option ResponseSnapshot) :
    (deriveAttempt ⟨⟨post_state, isSuperseded⟩, resp⟩).rank ≥
    (deriveAttempt ⟨⟨pre_state, isSuperseded⟩, resp⟩).rank := by
  cases pre_state <;> cases post_state <;>
    try (simp [LifecycleTransition] at h_trans)
  all_goals
    cases isSuperseded
    · cases resp with
      | none => simp [deriveAttempt, ClientTurnState.rank]
      | some r =>
        obtain ⟨status, _⟩ := r
        cases status <;> simp [deriveAttempt, ClientTurnState.rank]
    · simp [deriveAttempt, ClientTurnState.rank]

theorem response_advance_monotonic_none_to_some
    {req : RequestSnapshot}
    {resp : ResponseSnapshot}
    (h_not_super : req.isSuperseded = false)
    (h_nonterminal : req.lifecycleState = .pending ∨ req.lifecycleState = .claimed ∨
                     req.lifecycleState = .processing ∨ req.lifecycleState = .inputRequired) :
    (deriveAttempt ⟨req, some resp⟩).rank ≥
    (deriveAttempt ⟨req, none⟩).rank := by
  rw [deriveAttempt_nonterminal_response_driven h_not_super h_nonterminal,
      deriveAttempt_nonterminal_response_driven h_not_super h_nonterminal]
  cases resp.status <;> simp [ClientTurnState.rank]

theorem response_advance_monotonic_streaming_to_terminal
    {req : RequestSnapshot}
    {resp_new : ResponseSnapshot}
    (h_not_super : req.isSuperseded = false)
    (h_nonterminal : req.lifecycleState = .pending ∨ req.lifecycleState = .claimed ∨
                     req.lifecycleState = .processing ∨ req.lifecycleState = .inputRequired)
    (h_terminal : resp_new.status = .complete ∨ resp_new.status = .error) :
    (deriveAttempt ⟨req, some resp_new⟩).rank ≥
    (deriveAttempt ⟨req, some ⟨.streaming, false⟩⟩).rank := by
  rw [deriveAttempt_nonterminal_response_driven h_not_super h_nonterminal,
      deriveAttempt_nonterminal_response_driven h_not_super h_nonterminal]
  rcases h_terminal with h | h <;> simp [h, ClientTurnState.rank]
