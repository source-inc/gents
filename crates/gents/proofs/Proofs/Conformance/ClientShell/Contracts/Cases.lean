import Proofs.Conformance.ClientShell.Contracts.Types

namespace Conformance.ClientShellContracts

def contractPeer : PeerId := 40
def contractAgent : AgentDid := 20
def alternateAgent : AgentDid := 21
def contractBehavior : BehaviorId := 30
def alternateBehavior : BehaviorId := 31
def sid1 : SessionId := 1
def sid2 : SessionId := 2
def reqOld : RequestId := 100
def reqNew : RequestId := 101
def reqOther : RequestId := 202

def turnWaiting : ClientTurnState :=
  deriveAttempt
    { request := { lifecycleState := .pending, isSuperseded := false }
    , response := none
    }

def turnStreaming : ClientTurnState :=
  deriveAttempt
    { request := { lifecycleState := .processing, isSuperseded := false }
    , response := some { status := .streaming, tailEmpty := false }
    }

def turnCompleted : ClientTurnState :=
  deriveAttempt
    { request := { lifecycleState := .completed, isSuperseded := false }
    , response := none
    }

def sessionObs
    (sid : SessionId)
    (req : Option RequestId)
    (turn : Option ClientTurnState)
    (agent : AgentDid := contractAgent)
    (behavior : Option BehaviorId := some contractBehavior)
    : SessionObservation :=
  { sessionId := sid
  , agentDid := agent
  , behaviorId := behavior
  , latestObservedRequest := req
  , latestTurn := turn
  }

def storeWith (sessions : List SessionObservation) : LocalStore :=
  { deployments := [(contractPeer, contractAgent), (contractPeer + 1, alternateAgent)]
  , sessions := sessions
  }

def emptyStore : LocalStore :=
  storeWith []

def storeOldCompleted : LocalStore :=
  storeWith [sessionObs sid1 (some reqOld) (some turnCompleted)]

def storeNewCompleted : LocalStore :=
  storeWith [sessionObs sid1 (some reqNew) (some turnCompleted)]

def storeNewStreaming : LocalStore :=
  storeWith [sessionObs sid1 (some reqNew) (some turnStreaming)]

def storeSid2Completed : LocalStore :=
  storeWith [sessionObs sid2 (some reqOther) (some turnCompleted) alternateAgent]

def selectedShell
    (session : Option SessionId)
    (workflow : SubmissionWorkflow := .idle)
    (agent : Option AgentDid := some contractAgent)
    : ShellState :=
  { selection := { peer := some contractPeer, agent := agent, session := session }
  , workflow := workflow
  }

def ctxReady : SubmitContext :=
  { clientAvailable := true
  , composerNonEmpty := true
  , requestedBehavior := some contractBehavior
  }

def ctxOffline : SubmitContext :=
  { ctxReady with clientAvailable := false }

def ctxEmptyComposer : SubmitContext :=
  { ctxReady with composerNonEmpty := false }

def optionOr {α : Type} (first second : Option α) : Option α :=
  match first with
  | some value => some value
  | none       => second

def selectedObservation (s : ShellState) (store : LocalStore) : Option SessionObservation :=
  match s.selection.session with
  | some sid => store.find sid
  | none     => none

def turnStateOptionName : Option ClientTurnState → Option String
  | some turn => some (clientTurnStateName turn)
  | none      => none

def pendingRequestForFrontend (obs : Option SessionObservation) : Option RequestId :=
  match obs with
  | some observation =>
      match observation.latestObservedRequest, observation.latestTurn with
      | some req, some turn =>
          if turn.isTerminal then none else some req
      | _, _ => none
  | none => none

def trackedRequestForFrontend
    (selection : Selection)
    (obs : Option SessionObservation)
    (workflow : SubmissionWorkflow) : Option RequestId :=
  match workflow with
  | .awaiting sid req =>
      let selectedMatches := decide (selection.session = some sid)
      let sessionMatches :=
        match obs with
        | some observation => decide (observation.sessionId = sid)
        | none             => false
      if selectedMatches || sessionMatches then some req else none
  | _ => none

def activeRequestForFrontend
    (selection : Selection)
    (obs : Option SessionObservation)
    (workflow : SubmissionWorkflow) : Option RequestId :=
  let tracked := trackedRequestForFrontend selection obs workflow
  let pending := pendingRequestForFrontend obs
  let observed := obs.bind (·.latestObservedRequest)
  optionOr tracked (optionOr pending observed)

def frontendWorkflowFromProjection
    (state : ShellState)
    (store : LocalStore)
    (ctx : SubmitContext)
    (activeRequest : Option RequestId) : FrontendWorkflowContract :=
  let chat := projectChat state store ctx
  match state.workflow with
  | .submitting _ sid =>
      { kind := "submittingRequest"
      , sessionId := sid
      , requestId := none
      , turnState := none
      , reason := none
      }
  | .awaiting sid req =>
      { kind := "awaitingObservation"
      , sessionId := some sid
      , requestId := some req
      , turnState := none
      , reason := none
      }
  | .blocked reason =>
      { kind := "blocked"
      , sessionId := none
      , requestId := none
      , turnState := none
      , reason := some (blockedReasonName reason)
      }
  | .idle =>
      match chat.sendDecision with
      | .ready =>
          { kind := "ready"
          , sessionId := none
          , requestId := none
          , turnState := none
          , reason := none
          }
      | .blocked (.awaitingTurnTerminality turn) =>
          { kind := "turnInProgress"
          , sessionId := state.selection.session
          , requestId := activeRequest
          , turnState := some (clientTurnStateName turn)
          , reason := none
          }
      | .blocked .composerEmpty =>
          { kind := "ready"
          , sessionId := none
          , requestId := none
          , turnState := none
          , reason := none
          }
      | .blocked reason =>
          { kind := "blocked"
          , sessionId := none
          , requestId := none
          , turnState := none
          , reason := some (frontendBlockedReasonName reason)
          }

def workflowTurnStateForFrontend
    (store : LocalStore)
    (workflow : SubmissionWorkflow) : Option String :=
  match workflow with
  | .awaiting sid _ => turnStateOptionName ((store.find sid).bind (·.latestTurn))
  | _               => none

def desktopPendingExpectation (obs : Option SessionObservation) : Option Bool :=
  match obs with
  | some observation =>
      match observation.latestTurn with
      | some turn => if turn.isTerminal then none else some true
      | none      => none
  | none => none

def clientShellCaseFromStep
    (name property : String)
    (pre : ShellState)
    (input : ShellInput)
    (store : LocalStore)
    (transport : TransportHealth)
    (ctx : SubmitContext)
    (frontendLocal frontendExpected : ShellState)
    (frontendStore : LocalStore)
    (conversationPresent : Bool := true) : ClientShellContractCase :=
  let post := step pre input store transport ctx
  let chat := projectChat frontendExpected frontendStore ctx
  let obs := selectedObservation frontendLocal frontendStore
  let activeRequest := activeRequestForFrontend frontendLocal.selection obs frontendLocal.workflow
  let expectedWorkflow :=
    frontendWorkflowFromProjection frontendExpected frontendStore ctx activeRequest
  let selectedObs := selectedObservation frontendExpected frontendStore
  let desktopPreferred :=
    trackedRequestForFrontend frontendLocal.selection obs frontendLocal.workflow
  { name := name
  , property := property
  , input := inputName input
  , preSelectionAgent := pre.selection.agent
  , preSelectionSession := pre.selection.session
  , postSelectionAgent := post.selection.agent
  , postSelectionSession := post.selection.session
  , preWorkflowKind := workflowKind pre.workflow
  , preWorkflowSession := workflowSession pre.workflow
  , preWorkflowRequest := workflowRequest pre.workflow
  , postWorkflowKind := workflowKind post.workflow
  , postWorkflowSession := workflowSession post.workflow
  , postWorkflowRequest := workflowRequest post.workflow
  , selectionPreserved := decide (post.selection = pre.selection)
  , workflowAdvanced := decide (post.workflow ≠ pre.workflow)
  , transportNoop :=
      match input with
      | .transport _ => decide (post = pre)
      | _            => false
  , canSubmitBefore := canSubmit pre store ctx
  , canSubmitAfter := canSubmit post frontendStore ctx
  , selectionHealth := selectionHealthName chat.selectionHealth
  , projectionTurnState := turnStateOptionName chat.turnState
  , projectionWorkflowKind := workflowKind chat.workflow
  , projectionWorkflowSession := workflowSession chat.workflow
  , projectionWorkflowRequest := workflowRequest chat.workflow
  , sendDecision := sendDecisionKind chat.sendDecision
  , sendBlockedReason := sendDecisionReason chat.sendDecision
  , frontendClientAvailable := ctx.clientAvailable
  , frontendSelectedAgentDid := frontendLocal.selection.agent
  , frontendSelectedSessionId := frontendLocal.selection.session
  , frontendComposerNonEmpty := ctx.composerNonEmpty
  , frontendSending :=
      match frontendLocal.workflow with
      | .submitting _ _ => true
      | _               => false
  , frontendSessionPresent := obs.isSome
  , frontendSessionId := obs.map (·.sessionId)
  , frontendSessionLatestRequestId := obs.bind (·.latestObservedRequest)
  , frontendSessionTurnState := turnStateOptionName (obs.bind (·.latestTurn))
  , frontendSessionPendingRequestId := pendingRequestForFrontend obs
  , frontendConversationPresent := conversationPresent && obs.isSome
  , frontendConversationSessionId :=
      if conversationPresent then obs.map (·.sessionId) else none
  , frontendConversationLatestRequestId :=
      if conversationPresent then obs.bind (·.latestObservedRequest) else none
  , frontendConversationTurnState :=
      if conversationPresent then turnStateOptionName (obs.bind (·.latestTurn)) else none
  , frontendLocalWorkflowKind :=
      match frontendLocal.workflow with
      | .idle           => "ready"
      | .submitting _ _ => "submittingRequest"
      | .awaiting _ _   => "awaitingObservation"
      | .blocked _      => "blocked"
  , frontendLocalWorkflowSession := workflowSession frontendLocal.workflow
  , frontendLocalWorkflowRequest := workflowRequest frontendLocal.workflow
  , frontendLocalWorkflowTurnState :=
      workflowTurnStateForFrontend frontendStore frontendLocal.workflow
  , frontendExpectedWorkflowKind := expectedWorkflow.kind
  , frontendExpectedWorkflowSession := expectedWorkflow.sessionId
  , frontendExpectedWorkflowRequest := expectedWorkflow.requestId
  , frontendExpectedWorkflowTurnState := expectedWorkflow.turnState
  , frontendExpectedWorkflowReason := expectedWorkflow.reason
  , frontendExpectedSendStatus := frontendSendStatus chat.sendDecision
  , frontendExpectedSendBlockedReason := frontendSendReason chat.sendDecision
  , frontendExpectedActiveRequestId := activeRequest
  , frontendExpectedTurnState := turnStateOptionName (obs.bind (·.latestTurn))
  , desktopSelectedSessionId := frontendLocal.selection.session
  , desktopSnapshotPresent := selectedObs.isSome
  , desktopPreferredRequestId := desktopPreferred
  , desktopObservedRequestId := selectedObs.bind (·.latestObservedRequest)
  , desktopObservedTurnState := turnStateOptionName (selectedObs.bind (·.latestTurn))
  , desktopExpectedLatestRequestId := selectedObs.bind (·.latestObservedRequest)
  , desktopExpectedTurnState := turnStateOptionName (selectedObs.bind (·.latestTurn))
  , desktopExpectPendingTurn := desktopPendingExpectation selectedObs
  }

def clientShellCases : List ClientShellContractCase :=
  let awaitingNew := selectedShell (some sid1) (.awaiting sid1 reqNew)
  let noAgent := selectedShell none .idle none
  let staleBeforeSwitch :=
    { selectedShell (some sid1) (.awaiting sid1 reqOld) with
      selection := { peer := some contractPeer, agent := some alternateAgent, session := some sid1 }
    }
  let switchedStaleLocal :=
    { staleBeforeSwitch with selection := { staleBeforeSwitch.selection with session := some sid2 } }
  [ let pre := selectedShell (some sid1)
    let input := ShellInput.user .requestNewConversation
    let post := step pre input storeNewCompleted .healthy ctxReady
    clientShellCaseFromStep
      "new_conversation_is_ephemeral"
      "request_owned_session_creation"
      pre input storeNewCompleted .healthy ctxReady post post emptyStore
  , let pre := selectedShell none (.submitting contractAgent none)
    let input := ShellInput.mutation (.submitted sid1 reqNew)
    let post := step pre input emptyStore .healthy ctxReady
    clientShellCaseFromStep
      "submitted_request_selects_session"
      "request_owned_session_creation"
      pre input emptyStore .healthy ctxReady post post emptyStore
  , let input := ShellInput.snapshot storeNewCompleted
    let post := step awaitingNew input emptyStore .healthy ctxReady
    clientShellCaseFromStep
      "snapshot_preserves_selection"
      "selection_preservation"
      awaitingNew input emptyStore .healthy ctxReady awaitingNew post storeNewCompleted
  , let input := ShellInput.snapshot storeNewCompleted
    let post := step awaitingNew input emptyStore .healthy ctxReady
    clientShellCaseFromStep
      "snapshot_workflow_advances_on_matching_request"
      "snapshot_workflow_advance"
      awaitingNew input emptyStore .healthy ctxReady awaitingNew post storeNewCompleted
  , let input := ShellInput.snapshot storeOldCompleted
    let post := step awaitingNew input emptyStore .healthy ctxReady
    clientShellCaseFromStep
      "awaiting_stale_request_observation"
      "awaiting_stale_request_observation"
      awaitingNew input emptyStore .healthy ctxReady awaitingNew post storeOldCompleted
  , let input := ShellInput.snapshot storeNewStreaming
    let post := step awaitingNew input emptyStore .healthy ctxReady
    clientShellCaseFromStep
      "awaiting_matching_request_observation"
      "awaiting_matching_request_observation"
      awaitingNew input emptyStore .healthy ctxReady awaitingNew post storeNewStreaming
  , let input := ShellInput.user (.selectSession sid2)
    let post := step staleBeforeSwitch input storeSid2Completed .wedged ctxReady
    clientShellCaseFromStep
      "stale_workflow_after_session_switch"
      "stale_workflow_after_session_switch"
      staleBeforeSwitch input storeSid2Completed .wedged ctxReady switchedStaleLocal post storeSid2Completed
  , let pre := selectedShell (some sid1)
    let input := ShellInput.transport .wedged
    let post := step pre input storeNewCompleted .healthy ctxReady
    clientShellCaseFromStep
      "transport_noop"
      "transport_noop"
      pre input storeNewCompleted .healthy ctxReady pre post storeNewCompleted
  , let pre := selectedShell none
    let input := ShellInput.user .startSubmit
    clientShellCaseFromStep
      "blocked_submit_client_offline"
      "blocked_submit_gates"
      pre input emptyStore .healthy ctxOffline pre pre emptyStore
  , let input := ShellInput.user .startSubmit
    clientShellCaseFromStep
      "blocked_submit_agent_not_selected"
      "blocked_submit_gates"
      noAgent input emptyStore .healthy ctxReady noAgent noAgent emptyStore
  , let pre := selectedShell none
    let input := ShellInput.user .startSubmit
    clientShellCaseFromStep
      "blocked_submit_composer_empty"
      "blocked_submit_gates"
      pre input emptyStore .healthy ctxEmptyComposer pre pre emptyStore
  , let pre := selectedShell (some sid1) (.submitting contractAgent (some sid1))
    let input := ShellInput.user .startSubmit
    clientShellCaseFromStep
      "blocked_submit_mutation_in_flight"
      "blocked_submit_gates"
      pre input storeNewCompleted .healthy ctxReady pre pre storeNewCompleted
  , let input := ShellInput.user .startSubmit
    clientShellCaseFromStep
      "blocked_submit_awaiting_observation"
      "blocked_submit_gates"
      awaitingNew input storeOldCompleted .healthy ctxReady awaitingNew awaitingNew storeOldCompleted
  , let pre := selectedShell (some sid1)
    let input := ShellInput.user .startSubmit
    clientShellCaseFromStep
      "blocked_submit_session_absent"
      "blocked_submit_gates"
      pre input emptyStore .healthy ctxReady pre pre emptyStore
  , let pre := selectedShell (some sid1)
    let input := ShellInput.user .startSubmit
    clientShellCaseFromStep
      "blocked_submit_nonterminal_turn"
      "blocked_submit_gates"
      pre input storeNewStreaming .healthy ctxReady pre pre storeNewStreaming
  , let pre := selectedShell (some sid1)
    let input := ShellInput.user .startSubmit
    clientShellCaseFromStep
      "terminal_follow_up_allowed"
      "terminal_follow_up_allowance"
      pre input storeNewCompleted .healthy ctxReady pre pre storeNewCompleted
  , let pre := selectedShell (some sid1)
    let input := ShellInput.user .startSubmit
    clientShellCaseFromStep
      "terminal_follow_up_session_snapshot_without_summary"
      "terminal_follow_up_allowance"
      pre input storeNewCompleted .healthy ctxReady pre pre storeNewCompleted false
  ]

end Conformance.ClientShellContracts
