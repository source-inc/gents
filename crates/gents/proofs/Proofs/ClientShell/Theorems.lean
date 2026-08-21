import Proofs.ClientShell.Projection

theorem projection_pure
    (s : ShellState) (store : LocalStore) (ctx : SubmitContext) :
    projectChat s store ctx = projectChat s store ctx := rfl

theorem snapshot_preserves_selection
    (s : ShellState) (store store' : LocalStore) (h : TransportHealth)
    (ctx : SubmitContext) :
    (step s (.snapshot store') store h ctx).selection = s.selection := rfl

theorem snapshot_workflow_envelope
    (s : ShellState) (store store' : LocalStore) (h : TransportHealth)
    (ctx : SubmitContext) :
    (step s (.snapshot store') store h ctx).workflow =
      snapshotAdvanceWorkflow s.workflow store' := rfl

theorem transport_is_noop
    (s : ShellState) (store : LocalStore) (h h' : TransportHealth)
    (ctx : SubmitContext) :
    step s (.transport h') store h ctx = s := rfl

theorem local_switch_independent_of_transport
    (s : ShellState) (store : LocalStore) (ctx : SubmitContext)
    (sid : SessionId) (h h' : TransportHealth) :
    step s (.user (.selectSession sid)) store h  ctx
      = step s (.user (.selectSession sid)) store h' ctx := rfl

theorem select_session_latches
    (s : ShellState) (store : LocalStore) (ctx : SubmitContext)
    (sid : SessionId) (h : TransportHealth) :
    (step s (.user (.selectSession sid)) store h ctx).selection.session
      = some sid := rfl

theorem select_deployment_clears_session
    (s : ShellState) (store : LocalStore) (ctx : SubmitContext)
    (peer : PeerId) (agent : AgentDid) (h : TransportHealth) :
    (step s (.user (.selectDeployment peer agent)) store h ctx).selection.session
      = none := rfl

theorem selection_sticky_under_inflight
    (s : ShellState) (store store' : LocalStore) (h : TransportHealth)
    (ctx : SubmitContext)
    (_h_inflight :
      (∃ a opt, s.workflow = .submitting a opt)
      ∨ (∃ sid req, s.workflow = .awaiting sid req)) :
    (step s (.snapshot store') store h ctx).selection = s.selection :=
  snapshot_preserves_selection s store store' h ctx

theorem start_submit_gated
    (s : ShellState) (store : LocalStore) (h : TransportHealth)
    (ctx : SubmitContext) :
    canSubmit s store ctx = false →
    step s (.user .startSubmit) store h ctx = s := by
  intro h_cannot
  show (if canSubmit s store ctx then _ else s) = s
  rw [h_cannot]
  rfl

theorem trustworthy_transport_irrelevant
    (s : ShellState) (store : LocalStore) (b : Option BehaviorId) :
    trustworthyForFollowUp s store b = trustworthyForFollowUp s store b :=
  rfl

theorem selected_in_store_is_resolved
    (s : ShellState) (store : LocalStore) (ctx : SubmitContext)
    (sid : SessionId) (obs : SessionObservation)
    (h_sel  : s.selection.session = some sid)
    (h_find : store.find sid = some obs) :
    (projectChat s store ctx).selectionHealth = .resolved := by
  simp [projectChat, classifySelection, h_sel, h_find]

theorem awaiting_retires_only_on_matching_tip
    (sid : SessionId) (req : RequestId)
    (s : ShellState) (store store' : LocalStore) (h : TransportHealth)
    (ctx : SubmitContext)
    (h_wf   : s.workflow = .awaiting sid req)
    (h_idle : (step s (.snapshot store') store h ctx).workflow = .idle) :
    ∃ obs, store'.find sid = some obs
         ∧ obs.latestObservedRequest = some req := by
  have h_adv : snapshotAdvanceWorkflow s.workflow store' = .idle := h_idle
  rw [h_wf] at h_adv
  cases h_find : store'.find sid with
  | none =>
    simp [snapshotAdvanceWorkflow, h_find] at h_adv
  | some obs =>
    by_cases h_tip : obs.latestObservedRequest = some req
    · exact ⟨obs, rfl, h_tip⟩
    · simp [snapshotAdvanceWorkflow, h_find, h_tip] at h_adv

theorem projection_reflects_observed_tip
    (s : ShellState) (store : LocalStore) (ctx : SubmitContext)
    (sid : SessionId) (obs : SessionObservation)
    (h_sel  : s.selection.session = some sid)
    (h_find : store.find sid = some obs) :
    (projectChat s store ctx).turnState = obs.latestTurn := by
  simp [projectChat, classifySelection, h_sel, h_find]

theorem mutation_submitted_selects_session
    (s : ShellState) (store : LocalStore) (h : TransportHealth)
    (ctx : SubmitContext) (sid : SessionId) (req : RequestId) :
    (step s (.mutation (.submitted sid req)) store h ctx).selection.session
      = some sid := rfl

theorem new_conversation_is_ephemeral
    (s : ShellState) (store : LocalStore) (h : TransportHealth)
    (ctx : SubmitContext) :
    (step s (.user .requestNewConversation) store h ctx).selection.session
      = none := rfl

theorem mutation_failed_preserves_selection
    (s : ShellState) (store : LocalStore) (h : TransportHealth)
    (ctx : SubmitContext) (r : BlockedReason) :
    (step s (.mutation (.failed r)) store h ctx).selection
      = s.selection := rfl

theorem select_session_clears_blocker
    (s : ShellState) (store : LocalStore) (ctx : SubmitContext)
    (sid : SessionId) (r : BlockedReason) (h : TransportHealth)
    (h_wf : s.workflow = .blocked r) :
    (step s (.user (.selectSession sid)) store h ctx).workflow = .idle := by
  simp [step, workflowAfterSelectSession, h_wf]

theorem select_session_clears_stale_awaiting
    (s : ShellState) (store : LocalStore) (ctx : SubmitContext)
    (oldSid newSid : SessionId) (req : RequestId) (h : TransportHealth)
    (h_wf : s.workflow = .awaiting oldSid req)
    (h_ne : oldSid ≠ newSid) :
    (step s (.user (.selectSession newSid)) store h ctx).workflow = .idle := by
  simp [step, workflowAfterSelectSession, h_wf, h_ne]

theorem select_session_preserves_same_session_awaiting
    (s : ShellState) (store : LocalStore) (ctx : SubmitContext)
    (sid : SessionId) (req : RequestId) (h : TransportHealth)
    (h_wf : s.workflow = .awaiting sid req) :
    (step s (.user (.selectSession sid)) store h ctx).workflow = .awaiting sid req := by
  simp [step, workflowAfterSelectSession, h_wf]
