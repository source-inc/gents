import Proofs.RuntimeReconcile.Transition

namespace RuntimeState

inductive Action where
  | ackWrite (resolved : ResolvedSnapshot)
  | observeDoc (resolved : ResolvedSnapshot)
  | startResolve
  | resolveVisible (resolved : ResolvedSnapshot)
  | diffNoop (resolved : ResolvedSnapshot)
  | beginApply (resolved : ResolvedSnapshot)
  | publish (resolved : ResolvedSnapshot)
  | applyFailed
  | routerObserve
  | acceptRequest (sessionId : SessionId) (requestId : RequestId)
  | finishRequest (requestId : RequestId)
  | retireGeneration (generation : Generation)
  deriving DecidableEq, Repr

def noInFlightDependsOn (s : RuntimeState) (generation : Generation) : Prop :=
  s.inFlight.filter (fun rid => s.requestGeneration rid = generation) = ∅

instance (s : RuntimeState) (generation : Generation) :
    Decidable (noInFlightDependsOn s generation) := by
  unfold noInFlightDependsOn
  infer_instance

theorem noInFlightDependsOn_iff
    {s : RuntimeState}
    {generation : Generation} :
    noInFlightDependsOn s generation ↔
      ∀ rid, rid ∈ s.inFlight → s.requestGeneration rid ≠ generation := by
  unfold noInFlightDependsOn
  constructor
  · intro h_clear rid h_rid h_eq
    exact (Finset.filter_eq_empty_iff.mp h_clear (x := rid) h_rid) h_eq
  · intro h_clear
    exact Finset.filter_eq_empty_iff.mpr (by intro rid h_rid h_eq; exact h_clear rid h_rid h_eq)

def step? (pre : RuntimeState) : Action → Option RuntimeState
  | .ackWrite resolved =>
      some { pre with ackedResolved := some resolved }
  | .observeDoc resolved =>
      if pre.ackedResolved = some resolved ∧ pre.pendingResolved = none then
        some { pre with phase := .debouncing, observedResolved := some resolved }
      else
        none
  | .startResolve =>
      if pre.phase = .debouncing then
        some { pre with phase := .resolving }
      else
        none
  | .resolveVisible resolved =>
      if pre.phase = .resolving ∧ pre.observedResolved = some resolved ∧ resolved.wellFormed then
        some { pre with phase := .diffing, pendingResolved := some resolved }
      else
        none
  | .diffNoop resolved =>
      if pre.phase = .diffing ∧ pre.pendingResolved = some resolved ∧ resolved = pre.lastResolved then
        some { pre with phase := .idle, pendingResolved := none }
      else
        none
  | .beginApply resolved =>
      if pre.phase = .diffing ∧ pre.pendingResolved = some resolved ∧ resolved ≠ pre.lastResolved then
        some { pre with phase := .applying }
      else
        none
  | .publish resolved =>
      if pre.phase = .applying ∧ pre.pendingResolved = some resolved ∧ resolved ≠ pre.lastResolved then
        some
          { pre with
            phase := .idle
          , lastResolved := resolved
          , pendingResolved := none
          , active := resolved.activate (pre.active.generation + 1)
          , readyGenerations := insert (pre.active.generation + 1) pre.readyGenerations
          , liveGenerations := insert (pre.active.generation + 1) pre.liveGenerations
          }
      else
        none
  | .applyFailed =>
      if pre.phase = .applying then
        some { pre with phase := .idle, pendingResolved := none }
      else
        none
  | .routerObserve =>
      if pre.active.generation ∈ pre.readyGenerations then
        some { pre with routerObservedGeneration := pre.active.generation }
      else
        none
  | .acceptRequest sessionId requestId =>
      if CanAdmitRequest pre sessionId requestId then
        some
          { pre with
            accepted := insert requestId pre.accepted
          , inFlight := insert requestId pre.inFlight
          , requestGeneration := Function.update pre.requestGeneration requestId pre.routerObservedGeneration
          , requestSession := Function.update pre.requestSession requestId sessionId
          , requestBehavior := Function.update pre.requestBehavior requestId (pre.selectedBehavior sessionId)
          , sessionBehavior := pre.bindSessionIfNeeded sessionId (pre.selectedBehavior sessionId)
          }
      else
        none
  | .finishRequest requestId =>
      if requestId ∈ pre.inFlight then
        some { pre with inFlight := pre.inFlight.erase requestId }
      else
        none
  | .retireGeneration generation =>
      if generation ∈ pre.liveGenerations ∧
          generation ≠ pre.active.generation ∧
          generation ≠ pre.routerObservedGeneration ∧
          noInFlightDependsOn pre generation then
        some
          { pre with
            liveGenerations := pre.liveGenerations.erase generation
          , readyGenerations := pre.readyGenerations.erase generation
          }
      else
        none

inductive Trace : RuntimeState → RuntimeState → Prop where
  | refl {s : RuntimeState} : Trace s s
  | step {s₁ s₂ s₃ : RuntimeState} :
      Transition s₁ s₂ → Trace s₂ s₃ → Trace s₁ s₃

def replay? : RuntimeState → List Action → Option RuntimeState
  | s, [] => some s
  | s, action :: rest =>
      match step? s action with
      | some s' => replay? s' rest
      | none => none

theorem step_sound
    {pre post : RuntimeState}
    {action : Action}
    (h_step : step? pre action = some post) :
    Transition pre post := by
  cases action with
  | ackWrite resolved =>
      simp [step?] at h_step
      exact Transition.ack_write resolved h_step.symm
  | observeDoc resolved =>
      simp [step?] at h_step
      rcases h_step with ⟨⟨h_observed, h_pending⟩, h_post⟩
      exact Transition.observe_doc resolved h_observed h_pending h_post.symm
  | startResolve =>
      simp [step?] at h_step
      exact Transition.start_resolve h_step.1 h_step.2.symm
  | resolveVisible resolved =>
      simp [step?] at h_step
      rcases h_step with ⟨⟨h_phase, h_observed, h_resolved⟩, h_post⟩
      exact Transition.resolve_visible resolved h_phase h_observed h_resolved h_post.symm
  | diffNoop resolved =>
      simp [step?] at h_step
      rcases h_step with ⟨⟨h_phase, h_pending, h_same⟩, h_post⟩
      exact Transition.diff_noop resolved h_phase h_pending h_same h_post.symm
  | beginApply resolved =>
      simp [step?] at h_step
      rcases h_step with ⟨⟨h_phase, h_pending, h_changed⟩, h_post⟩
      exact Transition.begin_apply resolved h_phase h_pending h_changed h_post.symm
  | publish resolved =>
      simp [step?] at h_step
      rcases h_step with ⟨⟨h_phase, h_pending, h_changed⟩, h_post⟩
      exact Transition.publish resolved h_phase h_pending h_changed h_post.symm
  | applyFailed =>
      simp [step?] at h_step
      exact Transition.apply_failed h_step.1 h_step.2.symm
  | routerObserve =>
      simp [step?] at h_step
      exact Transition.router_observe h_step.1 h_step.2.symm
  | acceptRequest sessionId requestId =>
      simp [step?] at h_step
      exact Transition.accept_request sessionId requestId h_step.1 h_step.2.symm
  | finishRequest requestId =>
      simp [step?] at h_step
      exact Transition.finish_request requestId h_step.1 h_step.2.symm
  | retireGeneration generation =>
      simp [step?] at h_step
      rcases h_step with ⟨⟨h_live, h_not_active, h_not_router, h_clear⟩, h_post⟩
      exact Transition.retire_generation generation h_live h_not_active h_not_router
        (noInFlightDependsOn_iff.mp h_clear) h_post.symm

theorem transition_complete
    {pre post : RuntimeState}
    (h_trans : Transition pre post) :
    ∃ action : Action, step? pre action = some post := by
  cases h_trans with
  | ack_write resolved h_post =>
      exact ⟨.ackWrite resolved, by simp [step?, h_post]⟩
  | observe_doc resolved h_observed h_pending h_post =>
      exact ⟨.observeDoc resolved, by simp [step?, h_observed, h_pending, h_post]⟩
  | start_resolve h_phase h_post =>
      exact ⟨.startResolve, by simp [step?, h_phase, h_post]⟩
  | resolve_visible resolved h_phase h_observed h_resolved h_post =>
      exact ⟨.resolveVisible resolved, by simp [step?, h_phase, h_observed, h_resolved, h_post]⟩
  | diff_noop resolved h_phase h_pending h_same h_post =>
      exact ⟨.diffNoop resolved, by simp [step?, h_phase, h_pending, h_same, h_post]⟩
  | begin_apply resolved h_phase h_pending h_changed h_post =>
      exact ⟨.beginApply resolved, by simp [step?, h_phase, h_pending, h_changed, h_post]⟩
  | publish resolved h_phase h_pending h_changed h_post =>
      exact ⟨.publish resolved, by simp [step?, h_phase, h_pending, h_changed, h_post]⟩
  | apply_failed h_phase h_post =>
      exact ⟨.applyFailed, by simp [step?, h_phase, h_post]⟩
  | router_observe h_ready h_post =>
      exact ⟨.routerObserve, by simp [step?, h_ready, h_post]⟩
  | accept_request sessionId requestId h_can h_post =>
      exact ⟨.acceptRequest sessionId requestId, by simp [step?, h_can, h_post]⟩
  | finish_request requestId h_inFlight h_post =>
      exact ⟨.finishRequest requestId, by simp [step?, h_inFlight, h_post]⟩
  | retire_generation generation h_live h_not_active h_not_router h_clear h_post =>
      have h_no_dep : noInFlightDependsOn pre generation :=
        noInFlightDependsOn_iff.mpr h_clear
      exact ⟨.retireGeneration generation,
        by simp [step?, h_live, h_not_active, h_not_router, h_no_dep, h_post]⟩

theorem replay_sound
    {pre post : RuntimeState}
    {actions : List Action}
    (h_replay : replay? pre actions = some post) :
    Trace pre post := by
  induction actions generalizing pre with
  | nil =>
      simp [replay?] at h_replay
      subst h_replay
      exact Trace.refl
  | cons action rest ih =>
      simp [replay?] at h_replay
      rcases h_step : step? pre action with (_ | next)
      · simp [h_step] at h_replay
      · simp [h_step] at h_replay
        exact Trace.step (step_sound h_step) (ih h_replay)

theorem trace_complete
    {pre post : RuntimeState}
    (h_trace : Trace pre post) :
    ∃ actions : List Action, replay? pre actions = some post := by
  induction h_trace with
  | refl =>
      exact ⟨[], rfl⟩
  | step h_trans h_trace ih =>
      rcases transition_complete h_trans with ⟨action, h_action⟩
      rcases ih with ⟨actions, h_actions⟩
      exact ⟨action :: actions, by simp [replay?, h_action, h_actions]⟩

theorem step_generation_monotone
    {pre post : RuntimeState}
    {action : Action}
    (h_step : step? pre action = some post) :
    pre.active.generation ≤ post.active.generation :=
  transition_generation_monotone (step_sound h_step)

theorem step_coherent_preserved
    {pre post : RuntimeState}
    {action : Action}
    (h_coherent : pre.coherent)
    (h_step : step? pre action = some post) :
    post.coherent :=
  coherent_preserved h_coherent (step_sound h_step)

theorem publish_step_resolved_wellFormed
    {pre post : RuntimeState}
    {resolved : ResolvedSnapshot}
    (h_coherent : pre.coherent)
    (h_step : step? pre (.publish resolved) = some post) :
    resolved.wellFormed := by
  rcases h_coherent with
    ⟨_, _, _, _, _, _, _, _, _, _, h_pending, _, _⟩
  simp [step?] at h_step
  exact (h_pending resolved h_step.1.2.1).2

theorem accept_step_router_observed_ready_live
    {pre post : RuntimeState}
    {sessionId : SessionId}
    {requestId : RequestId}
    (h_coherent : pre.coherent)
    (h_step : step? pre (.acceptRequest sessionId requestId) = some post) :
    pre.routerObservedGeneration = pre.active.generation ∧
      pre.routerObservedGeneration ∈ pre.readyGenerations ∧
      pre.routerObservedGeneration ∈ pre.liveGenerations := by
  rcases h_coherent with
    ⟨_, _, _, _, _, _, _, _, h_ready_live, _, _, _, _⟩
  simp [step?] at h_step
  rcases h_step.1 with ⟨_, _, h_router_eq, h_router_ready, _⟩
  exact ⟨h_router_eq, h_router_ready, h_ready_live _ h_router_ready⟩

theorem accept_step_binding_coherent
    {pre post : RuntimeState}
    {sessionId : SessionId}
    {requestId : RequestId}
    (h_step : step? pre (.acceptRequest sessionId requestId) = some post) :
    requestId ∈ post.accepted ∧
      requestId ∈ post.inFlight ∧
      post.requestGeneration requestId = pre.routerObservedGeneration ∧
      post.requestSession requestId = sessionId ∧
      post.requestBehavior requestId = pre.selectedBehavior sessionId ∧
      post.sessionBehavior (post.requestSession requestId) =
        some (post.requestBehavior requestId) := by
  simp [step?] at h_step
  rcases h_step with ⟨h_can, h_post⟩
  rcases h_can with ⟨_, h_fresh, _, _, _⟩
  cases h_post
  simp [Function.update, bindSessionIfNeeded_selected, h_fresh]

/-- Admission is the atomic boundary: an accepted request already owns its
session/behavior projection; there is no later repair transition. -/
theorem accept_step_projects_session_atomically
    {pre post : RuntimeState}
    {sessionId : SessionId}
    {requestId : RequestId}
    (h_step : step? pre (.acceptRequest sessionId requestId) = some post) :
    requestId ∈ post.accepted ∧
      post.requestSession requestId = sessionId ∧
      post.sessionBehavior sessionId = some (post.requestBehavior requestId) := by
  have h := accept_step_binding_coherent h_step
  exact ⟨h.1, h.2.2.2.1, by simpa [h.2.2.2.1] using h.2.2.2.2.2⟩

theorem accept_step_replay_rejected
    {pre post : RuntimeState}
    {sessionId : SessionId}
    {requestId : RequestId}
    (h_step : step? pre (.acceptRequest sessionId requestId) = some post) :
    step? post (.acceptRequest sessionId requestId) = none := by
  simp [step?] at h_step
  rcases h_step with ⟨h_can, h_post⟩
  rcases h_can with ⟨h_unaccepted, _, _, _, _⟩
  cases h_post
  simp [step?, CanAdmitRequest, h_unaccepted]

theorem accepted_step_monotone
    {pre post : RuntimeState}
    {action : Action}
    (h_step : step? pre action = some post) :
    pre.accepted ⊆ post.accepted :=
  transition_accepted_monotone (step_sound h_step)

theorem retire_generation_denies_inFlight_dependency
    {pre post : RuntimeState}
    {generation : Generation}
    {requestId : RequestId}
    (h_inFlight : requestId ∈ pre.inFlight)
    (h_generation : pre.requestGeneration requestId = generation) :
    step? pre (.retireGeneration generation) ≠ some post := by
  intro h_step
  simp [step?] at h_step
  rcases h_step with ⟨⟨_, _, _, h_clear⟩, _⟩
  exact (noInFlightDependsOn_iff.mp h_clear requestId h_inFlight) h_generation

end RuntimeState
