import Proofs.ResponseOutcome.Transition

namespace ResponseOutcome

theorem complete_requires_exact_matching_assistant
    {store postStore : OutcomeStore} {fact : OutcomeFact}
    (h_kind : fact.kind = .complete)
    (h_publish : publish store fact = (.fresh, postStore)) :
    ∃ message,
      fact.finalMessage = some message ∧
      message.exactFor fact.request = true := by
  cases h_message : fact.finalMessage with
  | none =>
      simp [publish, OutcomeFact.wellFormed, h_kind, h_message] at h_publish
  | some message =>
      refine ⟨message, rfl, ?_⟩
      by_cases h_exact : message.exactFor fact.request = true
      · exact h_exact
      · have h_false : message.exactFor fact.request = false := by
          simpa only [Bool.not_eq_true] using h_exact
        simp [publish, OutcomeFact.wellFormed, h_kind, h_message, h_false] at h_publish

theorem Step.outcomes_monotone {pre post : Machine} (h : Step pre post) :
    ∀ fact, fact ∈ pre.outcomes → fact ∈ post.outcomes := by
  intro fact h_mem
  cases h with
  | updateLive _ _ h_post => subst post; exact h_mem
  | bindMessage _ _ _ h_post => subst post; exact h_mem
  | publishComplete outcome _ _ _ _ _ h_post =>
      subst post; exact List.mem_cons_of_mem outcome h_mem
  | publishFailure outcome _ _ _ _ h_post =>
      subst post; exact List.mem_cons_of_mem outcome h_mem
  | recoverMissingResponse _ outcomeProvenance outcome _ _ _ _ _ _ _ h_post =>
      subst post; exact List.mem_cons_of_mem outcome h_mem
  | observeIdempotentOutcome _ _ h_post => subst post; exact h_mem
  | rejectConflictingOutcome _ _ h_post => subst post; exact h_mem
  | terminalizeRequest _ _ h_post => subst post; exact h_mem
  | supersedeLive _ _ h_post => subst post; exact h_mem

theorem Step.complete_outcome_has_exact_message
    {pre post : Machine} (h : Step pre post)
    {fact : OutcomeFact} (h_new : fact ∈ post.outcomes)
    (h_old : fact ∉ pre.outcomes) (h_kind : fact.kind = .complete) :
    ∃ message,
      fact.finalMessage = some message ∧
      message.exactFor fact.request = true := by
  cases h with
  | updateLive _ _ h_post => subst post; contradiction
  | bindMessage _ _ _ h_post => subst post; contradiction
  | publishComplete candidate _ _ _ _ h_fresh h_post =>
      subst post
      simp only [List.mem_cons] at h_new
      rcases h_new with h_eq | h_mem
      · subst candidate
        exact complete_requires_exact_matching_assistant h_kind h_fresh
      · exact absurd h_mem h_old
  | publishFailure candidate _ _ h_failure _ h_post =>
      subst post
      simp only [List.mem_cons] at h_new
      rcases h_new with h_eq | h_mem
      · subst candidate
        rcases h_failure with h_failure | h_failure <;> simp_all
      · exact absurd h_mem h_old
  | recoverMissingResponse _ _ candidate _ _ _ _ _ h_failure _ h_post =>
      subst post
      simp only [List.mem_cons] at h_new
      rcases h_new with h_eq | h_mem
      · subst candidate
        rcases h_failure with h_failure | h_failure <;> simp_all
      · exact absurd h_mem h_old
  | observeIdempotentOutcome _ _ h_post => subst post; contradiction
  | rejectConflictingOutcome _ _ h_post => subst post; contradiction
  | terminalizeRequest _ _ h_post => subst post; contradiction
  | supersedeLive _ _ h_post => subst post; contradiction

/-- Recovery can publish only an explicit error/interrupted outcome. It cannot
reinterpret a partial live tail as successful completion. -/
theorem recovery_failure_never_completes
    {fact : OutcomeFact}
    (h_failure : fact.kind = .error ∨ fact.kind = .interrupted) :
    fact.kind ≠ .complete := by
  rcases h_failure with h_failure | h_failure <;> simp_all

/-- Commit-chain reconstruction succeeds only for an exact source/claim pair,
with the claim authored by the target agent. -/
theorem reconstruct_execution_provenance_exact
    {evidence : ClaimCommitEvidence} {provenance : ExecutionProvenance}
    (h_reconstructed : reconstructExecutionProvenance evidence = some provenance) :
    provenance.exactFor evidence.source.version.docId evidence.targetAgentDid = true := by
  simp [reconstructExecutionProvenance] at h_reconstructed
  rcases h_reconstructed with
    ⟨⟨⟨⟨⟨⟨h_exact, _h_source_signer⟩, _h_parent⟩,
      _h_source_pending⟩, _h_claimed⟩, _h_payload⟩, h_eq⟩
  subst provenance
  exact h_exact

/-- The no-live-response recovery edge can only publish the exact provenance
reconstructed from the persisted claim ancestry. -/
theorem recovery_missing_response_pins_reconstructed_provenance
    {pre post : Machine} (h : Step pre post)
    {fact : OutcomeFact} (h_new : fact ∈ post.outcomes)
    (h_old : fact ∉ pre.outcomes)
    (h_missing : pre.responsePresent = false)
    (h_cut : pre.cut = .claimDurable) :
    ∃ evidence provenance,
      reconstructExecutionProvenance evidence = some provenance ∧
      fact.provenance = provenance ∧
      fact.request = provenance.claim ∧
      (fact.kind = .error ∨ fact.kind = .interrupted) := by
  cases h with
  | updateLive _ _ h_post => subst post; contradiction
  | bindMessage _ _ _ h_post => subst post; contradiction
  | publishComplete _ h_step_cut _ _ _ _ h_post => simp_all
  | publishFailure _ h_present _ _ _ h_post => subst post; simp_all
  | recoverMissingResponse evidence provenance candidate _ _ h_reconstructed
      h_provenance h_request h_kind _ h_post =>
      subst post
      simp only [List.mem_cons] at h_new
      rcases h_new with h_eq | h_mem
      · subst candidate
        exact ⟨evidence, provenance, h_reconstructed, h_provenance, h_request, h_kind⟩
      · exact absurd h_mem h_old
  | observeIdempotentOutcome _ _ h_post => subst post; contradiction
  | rejectConflictingOutcome _ _ h_post => subst post; contradiction
  | terminalizeRequest _ _ h_post => subst post; contradiction
  | supersedeLive _ _ h_post => subst post; contradiction

/-- A retry that proposes the identical accepted immutable fact is a no-op. -/
theorem recovery_outcome_retry_idempotent
    (fact : OutcomeFact) (h_well_formed : fact.wellFormed = true) :
    publish [fact] fact = (.idempotent, [fact]) := by
  simp [publish, h_well_formed, factsForRequestDoc]

theorem request_terminalization_requires_durable_outcome
    {pre post : Machine} (h : Step pre post)
    (h_became_terminal : pre.requestTerminal = false ∧ post.requestTerminal = true) :
    pre.cut = .outcomeDurable := by
  cases h with
  | updateLive _ _ h_post => subst post; simp at h_became_terminal
  | bindMessage _ _ _ h_post => subst post; simp at h_became_terminal
  | publishComplete _ _ _ _ _ _ h_post => subst post; simp at h_became_terminal
  | publishFailure _ _ _ _ _ h_post => subst post; simp at h_became_terminal
  | recoverMissingResponse _ _ _ _ _ _ _ _ _ _ h_post =>
      subst post; simp at h_became_terminal
  | observeIdempotentOutcome _ _ h_post => subst post; simp at h_became_terminal
  | rejectConflictingOutcome _ _ h_post => subst post; simp at h_became_terminal
  | terminalizeRequest h_outcome _ _ => exact h_outcome
  | supersedeLive _ _ h_post => subst post; simp at h_became_terminal

theorem superseded_live_requires_terminal_request
    {pre post : Machine} (h : Step pre post)
    (h_active : pre.live.stage = .active)
    (h_superseded : post.live.stage = .superseded) :
    pre.requestTerminal = true := by
  cases h with
  | updateLive _ _ h_post => subst post; rw [h_active] at h_superseded; contradiction
  | bindMessage _ _ _ h_post => subst post; rw [h_active] at h_superseded; contradiction
  | publishComplete _ _ _ _ _ _ h_post => subst post; rw [h_active] at h_superseded; contradiction
  | publishFailure _ _ _ _ _ h_post =>
      subst post; rw [h_active] at h_superseded; contradiction
  | recoverMissingResponse _ _ _ _ _ _ _ _ _ _ h_post =>
      subst post; rw [h_active] at h_superseded; contradiction
  | observeIdempotentOutcome _ _ h_post => subst post; rw [h_active] at h_superseded; contradiction
  | rejectConflictingOutcome _ _ h_post => subst post; rw [h_active] at h_superseded; contradiction
  | terminalizeRequest _ _ h_post => subst post; rw [h_active] at h_superseded; contradiction
  | supersedeLive h_terminal _ _ => exact h_terminal

end ResponseOutcome
