import Proofs.ClientShell.Transition

inductive SelectionHealth where
  | noSelection
  | resolved
  | pendingObservation
  | absent
  deriving DecidableEq, Repr

inductive SendBlockedReason where
  | clientOffline
  | agentNotSelected
  | composerEmpty
  | mutationInFlight
  | awaitingObservation
  | awaitingTurnTerminality (turn : ClientTurnState)
  | sessionBehaviorMismatch
  | sessionAbsent
  | inconsistentObservation
  | workflowBlocked
  deriving DecidableEq, Repr

inductive SendDecision where
  | ready
  | blocked (reason : SendBlockedReason)
  deriving DecidableEq, Repr

structure ChatView where
  selection       : Selection
  selectionHealth : SelectionHealth
  visibleSession  : Option SessionObservation
  turnState       : Option ClientTurnState
  workflow        : SubmissionWorkflow
  sendDecision    : SendDecision
  deriving DecidableEq, Repr

def workflowReferences (w : SubmissionWorkflow) (sid : SessionId) : Bool :=
  match w with
  | .submitting _ (some sid') => decide (sid = sid')
  | .awaiting sid' _          => decide (sid = sid')
  | _                         => false

def classifySelection
    (sel : Selection) (store : LocalStore) (w : SubmissionWorkflow)
    : SelectionHealth × Option SessionObservation :=
  match sel.session with
  | none     => (.noSelection, none)
  | some sid =>
    match store.find sid with
    | some obs => (.resolved, some obs)
    | none     =>
      if workflowReferences w sid then (.pendingObservation, none)
      else (.absent, none)

def projectSendDecision
    (s : ShellState) (store : LocalStore) (ctx : SubmitContext) : SendDecision :=
  if ¬ ctx.clientAvailable then .blocked .clientOffline
  else if s.selection.agent.isNone then .blocked .agentNotSelected
  else if ¬ ctx.composerNonEmpty then .blocked .composerEmpty
  else match s.workflow with
    | .submitting _ _ => .blocked .mutationInFlight
    | .awaiting _ _                 => .blocked .awaitingObservation
    | .blocked _                    => .blocked .workflowBlocked
    | .idle =>
      match s.selection.session with
      | none     => .ready
      | some sid =>
        match store.find sid with
        | none     =>
          if workflowReferences s.workflow sid then
            .blocked .awaitingObservation
          else .blocked .sessionAbsent
        | some obs =>
          if behaviorMismatch store sid ctx.requestedBehavior then
            .blocked .sessionBehaviorMismatch
          else
            match obs.latestObservedRequest, obs.latestTurn with
            | none,   none   => .ready
            | some _, some t =>
              if t.isTerminal then .ready
              else .blocked (.awaitingTurnTerminality t)
            | _,      _      => .blocked .inconsistentObservation

def projectChat
    (s : ShellState) (store : LocalStore) (ctx : SubmitContext) : ChatView :=
  let classified := classifySelection s.selection store s.workflow
  { selection       := s.selection,
    selectionHealth := classified.fst,
    visibleSession  := classified.snd,
    turnState       := classified.snd.bind (·.latestTurn),
    workflow        := s.workflow,
    sendDecision    := projectSendDecision s store ctx }

inductive TransportIndicator where
  | quiet
  | degradedNotice
  | wedgedNotice
  deriving DecidableEq, Repr

def projectTransportIndicator : TransportHealth → TransportIndicator
  | .healthy  => .quiet
  | .degraded => .degradedNotice
  | .wedged   => .wedgedNotice

/-- Request progress is projected directly from the persisted request
lifecycle. Clients may choose presentation, but must not collapse the active
states into one generic spinner. -/
inductive RequestProgressIndicator where
  | queued
  | claimed
  | working
  | waitingForInput
  | completed
  | failed
  | superseded
  | expired
  | interrupted
  deriving DecidableEq, Repr

def projectRequestProgress : RequestState → RequestProgressIndicator
  | .pending       => .queued
  | .claimed       => .claimed
  | .processing    => .working
  | .inputRequired => .waitingForInput
  | .completed     => .completed
  | .failed        => .failed
  | .superseded    => .superseded
  | .dead          => .expired
  | .interrupted   => .interrupted

def RequestProgressIndicator.label : RequestProgressIndicator → String
  | .queued          => "Queued"
  | .claimed         => "Claimed"
  | .working         => "Working"
  | .waitingForInput => "Waiting for input"
  | .completed       => "Completed"
  | .failed          => "Failed"
  | .superseded      => "Superseded"
  | .expired         => "Expired"
  | .interrupted     => "Interrupted"

def RequestProgressIndicator.animated : RequestProgressIndicator → Bool
  | .queued | .claimed | .working => true
  | _ => false

theorem projectRequestProgress_active_animated (state : RequestState)
    (h : state = .pending ∨ state = .claimed ∨ state = .processing) :
    (projectRequestProgress state).animated = true := by
  rcases h with rfl | rfl | rfl <;> rfl

/-- The request document owns the pending user projection until the durable
user message for that exact request arrives. Unrelated messages and their
relative replication order are deliberately irrelevant. -/
def projectPendingUserTurn (hasDurableUserOwner : Bool) : Bool :=
  !hasDurableUserOwner

theorem projectPendingUserTurn_without_owner_visible :
    projectPendingUserTurn false = true := by
  rfl

theorem projectPendingUserTurn_with_owner_hidden :
    projectPendingUserTurn true = false := by
  rfl

structure OverlayBlock where
  hasContent   : Bool
  hasReasoning : Bool
  deriving DecidableEq, Repr

def projectActiveOverlay
    (resp : Option ResponseSnapshot)
    (turn : Option ClientTurnState)
    (materialized : Bool)
    (hasDurableOwner : Bool)
    (hasContent hasReasoning : Bool)
    : Option OverlayBlock :=
  match resp with
  | none => none
  | some r =>
    if materialized then none
    else if hasDurableOwner then none
    else if r.status = .complete ∨ r.status = .error then none
    else
      match turn with
      | none => none
      | some t =>
        if t.isTerminal then none
        else if t = .waitingForClaim ∨ t = .streaming then
          if hasContent ∨ hasReasoning then
            some { hasContent := hasContent, hasReasoning := hasReasoning }
          else none
        else none

theorem projectActiveOverlay_at_most_one
    (resp : Option ResponseSnapshot)
    (turn : Option ClientTurnState)
    (materialized hasDurableOwner hasContent hasReasoning : Bool) :
    ∀ b₁ b₂,
      projectActiveOverlay resp turn materialized hasDurableOwner hasContent hasReasoning = some b₁ →
      projectActiveOverlay resp turn materialized hasDurableOwner hasContent hasReasoning = some b₂ →
      b₁ = b₂ := by
  intros b₁ b₂ h₁ h₂
  rw [h₁] at h₂
  injection h₂

theorem projectActiveOverlay_terminal_hides
    (resp : Option ResponseSnapshot)
    (t : ClientTurnState)
    (h : t.isTerminal = true)
    (materialized hasDurableOwner hasContent hasReasoning : Bool) :
    projectActiveOverlay resp (some t) materialized hasDurableOwner hasContent hasReasoning = none := by
  cases resp with
  | none => rfl
  | some r =>
    cases materialized with
    | true =>
      simp [projectActiveOverlay]
    | false =>
      cases hasDurableOwner with
      | true => simp [projectActiveOverlay]
      | false =>
        cases r with
        | mk status tail =>
          cases status with
          | streaming =>
            cases t with
            | waitingForClaim => simp [ClientTurnState.isTerminal] at h
            | streaming       => simp [ClientTurnState.isTerminal] at h
            | completed       => simp [projectActiveOverlay, ClientTurnState.isTerminal]
            | failed          => simp [projectActiveOverlay, ClientTurnState.isTerminal]
            | superseded      => simp [projectActiveOverlay, ClientTurnState.isTerminal]
            | interrupted     => simp [projectActiveOverlay, ClientTurnState.isTerminal]
          | complete =>
            simp [projectActiveOverlay]
          | error =>
            simp [projectActiveOverlay]

theorem projectActiveOverlay_materialized_hides
    (resp : Option ResponseSnapshot)
    (turn : Option ClientTurnState)
    (hasDurableOwner : Bool)
    (hasContent hasReasoning : Bool) :
    projectActiveOverlay resp turn true hasDurableOwner hasContent hasReasoning = none := by
  cases resp with
  | none => rfl
  | some _ => rfl

/-- A replicated live-tail snapshot is hidden once the same request already has
a durable assistant turn owning that content, even when the response snapshot
itself predates the materialization marker. -/
theorem projectActiveOverlay_durable_owner_hides
    (resp : Option ResponseSnapshot)
    (turn : Option ClientTurnState)
    (materialized hasContent hasReasoning : Bool) :
    projectActiveOverlay resp turn materialized true hasContent hasReasoning = none := by
  cases resp with
  | none => rfl
  | some _ => cases materialized <;> rfl
