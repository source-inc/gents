import Proofs.RuntimeReconcile.State

namespace RuntimeState

inductive Transition : RuntimeState → RuntimeState → Prop where
  | ack_write {pre post : RuntimeState} (resolved : ResolvedSnapshot) :
      post = { pre with ackedResolved := some resolved } →
      Transition pre post
  | observe_doc {pre post : RuntimeState} (resolved : ResolvedSnapshot) :
      pre.ackedResolved = some resolved →
      pre.pendingResolved = none →
      post = { pre with phase := .debouncing, observedResolved := some resolved } →
      Transition pre post
  | start_resolve {pre post : RuntimeState} :
      pre.phase = .debouncing →
      post = { pre with phase := .resolving } →
      Transition pre post
  | resolve_visible {pre post : RuntimeState} (resolved : ResolvedSnapshot) :
      pre.phase = .resolving →
      pre.observedResolved = some resolved →
      resolved.wellFormed →
      post = { pre with phase := .diffing, pendingResolved := some resolved } →
      Transition pre post
  | diff_noop {pre post : RuntimeState} (resolved : ResolvedSnapshot) :
      pre.phase = .diffing →
      pre.pendingResolved = some resolved →
      resolved = pre.lastResolved →
      post = { pre with phase := .idle, pendingResolved := none } →
      Transition pre post
  | begin_apply {pre post : RuntimeState} (resolved : ResolvedSnapshot) :
      pre.phase = .diffing →
      pre.pendingResolved = some resolved →
      resolved ≠ pre.lastResolved →
      post = { pre with phase := .applying } →
      Transition pre post
  | publish {pre post : RuntimeState} (resolved : ResolvedSnapshot) :
      pre.phase = .applying →
      pre.pendingResolved = some resolved →
      resolved ≠ pre.lastResolved →
      post =
        { pre with
          phase := .idle
        , lastResolved := resolved
        , pendingResolved := none
        , active := resolved.activate (pre.active.generation + 1)
        , readyGenerations := insert (pre.active.generation + 1) pre.readyGenerations
        , liveGenerations := insert (pre.active.generation + 1) pre.liveGenerations
        } →
      Transition pre post
  | apply_failed {pre post : RuntimeState} :
      pre.phase = .applying →
      post = { pre with phase := .idle, pendingResolved := none } →
      Transition pre post
  | router_observe {pre post : RuntimeState} :
      pre.active.generation ∈ pre.readyGenerations →
      post = { pre with routerObservedGeneration := pre.active.generation } →
      Transition pre post
  | accept_request {pre post : RuntimeState} (sessionId : SessionId) (requestId : RequestId) :
      CanAdmitRequest pre sessionId requestId →
      post =
        { pre with
          accepted := insert requestId pre.accepted
        , inFlight := insert requestId pre.inFlight
        , requestGeneration := Function.update pre.requestGeneration requestId pre.routerObservedGeneration
        , requestSession := Function.update pre.requestSession requestId sessionId
        , requestBehavior := Function.update pre.requestBehavior requestId (pre.selectedBehavior sessionId)
        , sessionBehavior := pre.bindSessionIfNeeded sessionId (pre.selectedBehavior sessionId)
        } →
      Transition pre post
  | finish_request {pre post : RuntimeState} (requestId : RequestId) :
      requestId ∈ pre.inFlight →
      post = { pre with inFlight := pre.inFlight.erase requestId } →
      Transition pre post
  | retire_generation {pre post : RuntimeState} (generation : Generation) :
      generation ∈ pre.liveGenerations →
      generation ≠ pre.active.generation →
      generation ≠ pre.routerObservedGeneration →
      (∀ rid, rid ∈ pre.inFlight → pre.requestGeneration rid ≠ generation) →
      post =
        { pre with
          liveGenerations := pre.liveGenerations.erase generation
        , readyGenerations := pre.readyGenerations.erase generation
        } →
      Transition pre post

theorem transition_generation_monotone
    {pre post : RuntimeState}
    (h_trans : Transition pre post) :
    pre.active.generation ≤ post.active.generation := by
  cases h_trans <;>
    simp_all [ResolvedSnapshot.activate, Nat.le_succ]

theorem transition_accepted_monotone
    {pre post : RuntimeState}
    (h_trans : Transition pre post) :
    pre.accepted ⊆ post.accepted := by
  cases h_trans <;> simp_all

theorem coherent_preserved
    {pre post : RuntimeState}
    (h_coherent : pre.coherent)
    (h_trans : Transition pre post) :
    post.coherent := by
  rcases h_coherent with
    ⟨h_active, h_last, h_default, h_runnable, h_unavailable, h_generation_live,
      h_generation_ready, h_router_live, h_ready_live, h_live_bound,
      h_pending, h_request_live, h_session⟩
  cases h_trans with
  | ack_write _ h_post =>
      cases h_post
      exact ⟨h_active, h_last, h_default, h_runnable, h_unavailable, h_generation_live,
        h_generation_ready, h_router_live, h_ready_live, h_live_bound,
        h_pending, h_request_live, h_session⟩
  | observe_doc resolved _ h_pending_none h_post =>
      cases h_post
      refine ⟨h_active, h_last, h_default, h_runnable, h_unavailable, h_generation_live,
        h_generation_ready, h_router_live, h_ready_live, h_live_bound, ?_, h_request_live, h_session⟩
      intro candidate h_candidate
      simp [h_pending_none] at h_candidate
  | start_resolve _ h_post =>
      cases h_post
      exact ⟨h_active, h_last, h_default, h_runnable, h_unavailable, h_generation_live,
        h_generation_ready, h_router_live, h_ready_live, h_live_bound,
        h_pending, h_request_live, h_session⟩
  | resolve_visible resolved _ h_observed h_resolved h_post =>
      cases h_post
      refine ⟨h_active, h_last, h_default, h_runnable, h_unavailable, h_generation_live,
        h_generation_ready, h_router_live, h_ready_live, h_live_bound,
        ?_, h_request_live, h_session⟩
      intro candidate h_candidate
      simp at h_candidate
      cases h_candidate
      exact ⟨h_observed, h_resolved⟩
  | diff_noop _ _ _ _ h_post =>
      cases h_post
      refine ⟨h_active, h_last, h_default, h_runnable, h_unavailable, h_generation_live,
        h_generation_ready, h_router_live, h_ready_live, h_live_bound, ?_, h_request_live, h_session⟩
      intro candidate h_candidate
      simp at h_candidate
  | begin_apply _ _ _ _ h_post =>
      cases h_post
      exact ⟨h_active, h_last, h_default, h_runnable, h_unavailable, h_generation_live,
        h_generation_ready, h_router_live, h_ready_live, h_live_bound,
        h_pending, h_request_live, h_session⟩
  | publish resolved _ h_pendingResolved _ h_post =>
      cases h_post
      have h_resolved : resolved.wellFormed := (h_pending resolved h_pendingResolved).2
      refine ⟨activate_wellFormed h_resolved (Nat.succ_pos _), h_resolved, rfl, rfl, rfl, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
      · simp [ResolvedSnapshot.activate]
      · simp [ResolvedSnapshot.activate]
      · exact Finset.mem_insert_of_mem h_router_live
      · intro generation h_generation
        simp at h_generation
        rcases h_generation with h_new | h_old
        · simp [h_new, ResolvedSnapshot.activate]
        · exact Finset.mem_insert_of_mem (h_ready_live generation h_old)
      · intro generation h_generation
        simp at h_generation
        rcases h_generation with h_new | h_old
        · simp [h_new, ResolvedSnapshot.activate]
        · have h_old_bound := h_live_bound generation h_old
          exact Nat.le_trans h_old_bound (Nat.le_succ _)
      · intro candidate h_candidate
        simp at h_candidate
      · intro rid h_rid
        exact Finset.mem_insert_of_mem (h_request_live rid h_rid)
      · intro rid h_rid
        exact h_session rid h_rid
  | apply_failed _ h_post =>
      cases h_post
      refine ⟨h_active, h_last, h_default, h_runnable, h_unavailable, h_generation_live,
        h_generation_ready, h_router_live, h_ready_live, h_live_bound,
        ?_, h_request_live, h_session⟩
      intro candidate h_candidate
      simp at h_candidate
  | router_observe h_ready h_post =>
      cases h_post
      refine ⟨h_active, h_last, h_default, h_runnable, h_unavailable, h_generation_live,
        h_generation_ready, ?_, h_ready_live, h_live_bound, h_pending, h_request_live, h_session⟩
      exact h_ready_live _ h_ready
  | accept_request sessionId requestId h_can h_post =>
      cases h_post
      rcases h_can with ⟨_h_unaccepted, h_fresh, h_router_eq, h_router_ready, _h_dispatch⟩
      refine ⟨h_active, h_last, h_default, h_runnable, h_unavailable, h_generation_live,
        h_generation_ready, h_router_live, h_ready_live, h_live_bound, h_pending, ?_, ?_⟩
      · intro rid h_rid
        simp at h_rid
        rcases h_rid with rfl | h_old
        · have h_live : pre.routerObservedGeneration ∈ pre.liveGenerations :=
            h_ready_live _ h_router_ready
          simpa [Function.update] using h_live
        · have h_ne : rid ≠ requestId := by
            intro h_eq
            subst h_eq
            exact h_fresh h_old
          simpa [Function.update, h_ne] using h_request_live rid h_old
      · intro rid h_rid
        simp at h_rid
        rcases h_rid with rfl | h_old
        · simpa [Function.update] using bindSessionIfNeeded_selected pre sessionId
        · have h_ne : rid ≠ requestId := by
            intro h_eq
            subst h_eq
            exact h_fresh h_old
          by_cases h_same : pre.requestSession rid = sessionId
          · have h_bound : pre.sessionBehavior sessionId = some (pre.requestBehavior rid) := by
              simpa [h_same] using h_session rid h_old
            have h_bind_eq :
                pre.bindSessionIfNeeded sessionId (pre.selectedBehavior sessionId) =
                  pre.sessionBehavior :=
              bindSessionIfNeeded_eq_self_of_bound h_bound
            simpa [h_bind_eq, Function.update, h_ne, h_same]
              using h_session rid h_old
          · have h_other :
              pre.bindSessionIfNeeded sessionId (pre.selectedBehavior sessionId)
                  (pre.requestSession rid) =
                pre.sessionBehavior (pre.requestSession rid) :=
              bindSessionIfNeeded_other
                (s := pre)
                (sessionId := sessionId)
                (other := pre.requestSession rid)
                (behaviorId := pre.selectedBehavior sessionId)
                h_same
            simpa [h_other, Function.update, h_ne, h_same]
              using h_session rid h_old
  | finish_request _ _ h_post =>
      cases h_post
      refine ⟨h_active, h_last, h_default, h_runnable, h_unavailable, h_generation_live,
        h_generation_ready, h_router_live, h_ready_live, h_live_bound,
        h_pending, ?_, ?_⟩
      · intro rid h_rid
        exact h_request_live rid (Finset.mem_of_mem_erase h_rid)
      · intro rid h_rid
        exact h_session rid (Finset.mem_of_mem_erase h_rid)
  | retire_generation generation _ h_not_active h_not_router h_clear h_post =>
      cases h_post
      refine ⟨h_active, h_last, h_default, h_runnable, h_unavailable, ?_, ?_, ?_, ?_, ?_,
        h_pending, ?_, h_session⟩
      · have h_keep : pre.active.generation ≠ generation := by
          intro h_eq
          exact h_not_active h_eq.symm
        exact Finset.mem_erase.mpr ⟨h_keep, h_generation_live⟩
      · have h_keep : pre.active.generation ≠ generation := by
          intro h_eq
          exact h_not_active h_eq.symm
        exact Finset.mem_erase.mpr ⟨h_keep, h_generation_ready⟩
      · have h_keep : pre.routerObservedGeneration ≠ generation := by
          intro h_eq
          exact h_not_router h_eq.symm
        exact Finset.mem_erase.mpr ⟨h_keep, h_router_live⟩
      · intro current h_current
        rcases Finset.mem_erase.mp h_current with ⟨h_ne, h_mem⟩
        exact Finset.mem_erase.mpr ⟨h_ne, h_ready_live current h_mem⟩
      · intro current h_current
        exact h_live_bound current (Finset.mem_of_mem_erase h_current)
      · intro rid h_rid
        exact Finset.mem_erase.mpr ⟨h_clear rid h_rid, h_request_live rid h_rid⟩

end RuntimeState
