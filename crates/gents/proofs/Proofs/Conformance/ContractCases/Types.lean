import Proofs.Basic
import Proofs.Scheduling
import Proofs.RuntimeReconcile.State

namespace Conformance.ContractCases

structure RuntimeReconcileCase where
  name : String
  action : String
  legal : Bool
  prePhase : String
  postPhase : String
  preActiveGeneration : Nat
  postActiveGeneration : Nat
  preRouterGeneration : Nat
  postRouterGeneration : Nat
  preReadyGenerationCount : Nat
  postReadyGenerationCount : Nat
  preLiveGenerationCount : Nat
  postLiveGenerationCount : Nat
  preInFlightCount : Nat
  postInFlightCount : Nat
  trackedRequestId : RequestId
  trackedSessionId : SessionId
  trackedRequestGeneration : Generation
  trackedRequestSession : SessionId
  trackedRequestBehavior : BehaviorId
  trackedSessionBehavior : BehaviorId
  deriving Repr

structure SessionRecoveryCase where
  name : String
  action : String
  legal : Bool
  preLatestState : String
  preFailedState : String
  postLatestState : String
  postFailedState : String
  postNewState : String
  preLatestAdmission : String
  postLatestAdmission : String
  preFailedAdmission : String
  postFailedAdmission : String
  postNewAdmission : String
  preOrigin : String
  postNewOrigin : String
  preBackend : String
  postNewBackend : String
  failedId : RequestId
  newId : RequestId
  preLatestId : RequestId
  postLatestId : RequestId
  preSessionId : SessionId
  postSessionId : SessionId
  preBehaviorId : BehaviorId
  postBehaviorId : BehaviorId
  preRequestCount : Nat
  postRequestCount : Nat
  preRetryCount : Nat
  postRetryCount : Nat
  maxRetries : Nat
  preDeadlineExceeded : Bool
  postDeadlineExceeded : Bool
  preFailedIsLatest : Bool
  postFailedIsLatest : Bool
  postNewIsLatest : Bool
  preRequestIds : List RequestId
  preFailedExists : Bool
  preLatestExists : Bool
  preNewRequestExists : Bool
  oldRequestRetained : Bool
  newRequestInserted : Bool
  originPreserved : Bool
  backendPreserved : Bool
  deriving Repr

structure InferenceSlotAccountingCase where
  name : String
  property : String
  backendId : String
  preState : String
  postState : String
  contribution : Nat
  expectedContribution : Nat
  preContribution : Nat
  postContribution : Nat
  releasedSlot : Bool
  permitDropTerminalization : Bool
  rowStates : List String
  rowBackendIds : List String
  reconstructedRunningCount : Nat
  maxConcurrent : Nat
  boundedByMaxConcurrent : Bool
  deriving Repr

structure FleetSlotAccountingCase where
  name : String
  property : String
  backendId : String
  requestState : String
  admissionState : String
  contribution : Nat
  expectedContribution : Nat
  activeCount : Nat
  schedulerRunning : Nat
  slotCount : Nat
  rowStates : List String
  rowBackendIds : List String
  reconstructedRunningCount : Nat
  maxConcurrent : Nat
  boundedByMaxConcurrent : Bool
  aggregateReconstructedNotPersisted : Bool
  deriving Repr

structure PersistenceFailurePolicyCase where
  name : String
  policy : String
  action : String
  prePersistence : String
  postPersistence : String
  postStorageObservation : String
  hookDecision : String
  recordsFailure : Bool
  recordsSuccess : Bool
  externalDurabilityClaimed : Bool
  deriving Repr

structure StorageObservationRuntimeCase where
  name : String
  policy : String
  action : String
  preObservation : String
  mutationResult : String
  postObservation : String
  postPersistence : String
  hookResult : String
  recordsFailure : Bool
  recordsSuccess : Bool
  terminalWriteObserved : Bool
  externalVisibilityClaimed : Bool
  deriving Repr

structure BackendHealthAdmissionCase where
  name : String
  enabled : Bool
  probeStatus : String
  expectedAvailable : Bool
  admissionDecision : String
  observedDocumentOnly : Bool
  externalEndpointFreshnessClaimed : Bool
  deriving Repr

structure NativeFilesystemBoundaryCase where
  name : String
  toolName : String
  workClass : String
  boundary : String
  innerPollBlocks : Bool
  requestDeadlineMs : Nat
  blockerMs : Nat
  expectedTerminal : String
  expectedFailureClass : Option String
  queueAdvancesBeforeBlockerReturns : Bool
  deriving Repr

structure ManagedExecToolBoundaryCase where
  name : String
  toolName : String
  workClass : String
  boundary : String
  killScope : String
  timeoutRequiresKill : Bool
  cancelRequiresKill : Bool
  descendantsInTerminationScope : Bool
  captureDrainBounded : Bool
  deriving Repr

structure PairingReconcileShutdownBoundaryCase where
  name : String
  supervisor : String
  workClass : String
  boundary : String
  perAdminCallTimeoutMs : Nat
  cancellationObservedInsideSweep : Bool
  currentAdminFutureDropped : Bool
  remainingPeersSkipped : Bool
  shutdownJoinBounded : Bool
  deriving Repr

structure PairingReconcileSweepRetryBoundaryCase where
  name : String
  supervisor : String
  workClass : String
  boundary : String
  failureScope : String
  failureTerminal : Bool
  retryTrigger : String
  cancellationPrioritized : Bool
  convergenceRetried : Bool
  deriving Repr

structure PairingReconcileSweepSchedulingCase where
  name : String
  supervisor : String
  workClass : String
  boundary : String
  maxConcurrentPeerPreparations : Nat
  peerPreparationBounded : Bool
  topologyMutationSerialized : Bool
  stalePeerBlocksReadyPeer : Bool
  everyPeerResultAccounted : Bool
  deriving Repr

structure ManagedExecLivenessCase where
  name : String
  trigger : String
  preExecState : String
  preToolState : String
  expectedExecState : String
  expectedToolState : String
  maxSteps : Nat
  killSignalRequired : Bool
  deriving Repr

structure LifecycleTransitionCase where
  name : String
  domain : String
  fromState : String
  toState : String
  classification : String
  action : Option String
  boundary : Option String
  deriving Repr

structure QueueDeadlineConformanceCase where
  name : String
  group : String
  action : String
  sessionId : SessionId
  legal : Bool
  preActiveRequestId : Option RequestId
  postActiveRequestId : Option RequestId
  prePendingRequestIds : List RequestId
  postPendingRequestIds : List RequestId
  claimedRequestId : Option RequestId
  blockedByActive : Bool
  supersededRequestIds : List RequestId
  queueKey : Option String
  postCoalescedPendingCount : Nat
  automatedDrainedRequestIds : List RequestId
  preservedUserPendingRequestIds : List RequestId
  postTerminalRequestIds : List RequestId
  preRequestDeadline : Option Time
  synthesizedClaimDeadline : Option Time
  postDeadline : Option Time
  explicitDeadlinePreserved : Bool
  deriving Repr

structure RecoverySweepCase where
  name : String
  sweepId : String
  collection : String
  rustFunction : String
  cadence : String
  implementationStatus : String
  preState : String
  terminalState : String
  measureBefore : Nat
  measureAfter : Nat
  deadlineAuditRef : String
  deadlineExpired : Option Bool := none
  unclaimedExpired : Option Bool := none
  parentLive : Option Bool := none
  parentInterrupted : Option Bool := none
  parentTerminal : Option Bool := none
  executionRegistered : Option Bool := none
  recoveryCause : Option String := none
  notificationReason : Option String := none
  deriving DecidableEq, Repr

structure RecoveryEquivalenceCase where
  name : String
  sourceSweepCase : String
  sweepId : String
  collection : String
  rustFunction : String
  cadence : String
  preState : String
  recoveredState : String
  uninterruptedState : String
  equivalent : Bool
  reexecutes : Bool
  canHang : Bool
  theoremName : String
  aggregateTheoremName : String
  deriving DecidableEq, Repr

/-- Startup restart-disposition witness (#937): one running `AgentToolCall`
    row shape and what `ToolCallLifecycle::recover_all` must do with it —
    terminalize with a pinned cause/terminal state, or leave it running.
    `disposition`, `cause`, `terminalState`, and the notification/wake fields
    are computed from `Recovery.restartDisposition`, never hand-written. -/
structure RestartDispositionCase where
  name : String
  rustFunction : String
  awaitMode : String
  cancelPolicy : String
  childLinked : Bool
  parentObservation : String
  deadlineExpired : Bool
  unclaimedExpired : Bool
  disposition : String
  cause : Option String
  terminalState : Option String
  notificationReason : Option String
  queueSource : Option String
  queueKeyPrefix : Option String
  theoremName : String
  deriving DecidableEq, Repr

/-- Executable bridge-step witness (#937): one concrete subagent-bridge
    fixture, one bridge event, and the outcome of
    `Subagent.BridgedState.step` — `legal`, the bridge tool's post state, and
    whether the child's interrupt flag was latched are all computed by
    running the step, never hand-written. -/
structure BridgeStepCase where
  name : String
  event : String
  childState : String
  parentState : String
  cancelPolicy : String
  bridgeCommitted : Bool
  legal : Bool
  postToolState : Option String
  postChildInterruptSet : Bool
  theoremName : String
  deriving DecidableEq, Repr

/-- Paging witness over the retained output window (#937): inputs plus the
    slice outputs, computed from `Subagent.ToolOutput.readSlice` — never
    hand-written. Consumed by the `background_tools` unit test against
    `read_retained_output_slice`. -/
structure ToolOutputPagingCase where
  name : String
  firstOffset : Nat
  retainedLen : Nat
  totalBytes : Nat
  offset : Nat
  maxBytes : Nat
  start : Nat
  sliceLen : Nat
  nextOffset : Nat
  firstAvailableOffset : Nat
  totalBytesOut : Nat
  hasMore : Bool
  theoremName : String
  deriving DecidableEq, Repr

structure R6BackgroundingCase where
  name : String
  group : String
  action : String
  legal : Bool
  preLiveCount : Nat
  maxBackgrounded : Nat
  awaitMode : String
  cancelPolicy : String
  childRequestId : Option String
  terminalState : String
  result : Option String
  reason : Option String
  errorCode : Option String
  queueSource : Option String
  queueKey : Option String
  retryCount : Option Nat := none
  maxRetries : Option Nat := none
  postRetryCount : Option Nat := none
  retryDelaySeconds : Option Nat := none
  isLatest : Option Bool := none
  deriving Repr

structure R5CrossDeploymentCase where
  name : String
  route : String
  action : String
  parentDeployment : String
  childDeployment : String
  parentRequestId : String
  parentToolCallId : String
  childRequestId : String
  targetBehaviorId : String
  awaitMode : String
  cancelPolicy : String
  parentTriggerPersisted : Bool
  childMaterialized : Bool
  childOwnedByTargetDeployment : Bool
  causedByParentRequestIdMatches : Bool
  causedByParentToolCallIdMatches : Bool
  causedByTriggerKind : String
  crossDeploymentRoutingFired : Bool
  singleDeploymentFallback : Bool
  unclaimedDeadlineSet : Bool
  deriving Repr

structure CancelPropagationCase where
  name : String
  route : String
  action : String
  parentDeployment : String
  childDeployment : String
  parentRequestId : String
  parentToolCallId : String
  childRequestId : String
  bridgeCollection : String
  childRequestCollection : String
  cancelIntentWrittenOnBridge : Bool
  bridgeCancelReplicatesToHost : Bool
  hostInterruptsChild : Bool
  childTerminalReplicatesToCoordinator : Bool
  cancelAckReturnsToCoordinator : Bool
  noThirdPartyRows : Bool
  deriving Repr

structure BackgroundTheoremWitness where
  theoremName : String
  witnessKind : String
  scenario : String
  numericBound : Nat
  kindFields : List (String × String)
  deriving Repr

structure ComposedInvariantWitness where
  theoremName : String
  witnessKind : String
  scenario : String
  rustPath : String
  traceStepCount : Nat
  transitionPath : List String
  preRequestState : String
  preRequestAdmission : String
  toolPreState : String
  toolPostState : String
  requestId : Nat
  toolRequestId : Nat
  toolCallId : Nat
  requestDeadline : Nat
  requestCurrentTime : Nat
  toolDeadline : Nat
  toolCurrentTime : Nat
  deadlineExceeded : Bool
  wellFormedSource : String
  preToolPersisted : Bool
  cancelCause : Option String
  deriving Repr

structure SubagentDelegationGraphCase where
  name : String
  theoremName : String
  property : String
  witnessKind : String
  maxDepth : Nat
  pathLength : Nat
  parentDepth : Nat
  terminalDepth : Nat
  cascadePath : Bool
  acyclic : Bool
  bounded : Bool
  cascadeCovered : Bool
  edgeTheorem : String
  cascadeEdgeTheorem : Option String
  deriving Repr

namespace R4cWitnesses

structure ListSubagentsLineageRejects where
  callerRequestId : String
  siblingRequestId : String
  siblingChildId : String
  callerSeesSiblingChild : Bool
  deriving Repr

structure ReadTranscriptCursorAdvances where
  childSessionId : String
  firstSinceSequence : Nat
  firstThroughSequence : Nat
  firstNextSequence : Nat
  secondSinceSequence : Nat
  secondThroughSequence : Nat
  noGap : Bool
  noOverlap : Bool
  deriving Repr

structure ReadTranscriptHidesBridgeRows where
  childSessionId : String
  bridgeCallId : String
  renderedTranscript : String
  deriving Repr

/-- Three-way `read_tool_output` dispatch (#937): a running row with a live
    ring-buffer snapshot serves the live tail; a running row with no
    snapshot — the post-restart shape, since the registry is volatile —
    serves empty output; a terminal row serves the persisted completion.
    Sources and paging numbers are computed from
    `Subagent.ToolOutput.readDispatch` / `readSlice`. -/
structure ReadToolOutputDispatchesByState where
  toolCallId : String
  runningSource : String
  runningNoBufferSource : String
  terminalSource : String
  runningPayload : String
  runningNoBufferPayload : String
  terminalPayload : String
  runningNextOffset : Nat
  runningTotalBytes : Nat
  runningHasMore : Bool
  terminalTotalBytes : Nat
  deriving Repr

structure SteerAppendPreservesLineage where
  callerRequestId : String
  callerRequestDocId : String
  childSessionId : String
  queuedRequestId : String
  causedByParentRequestId : String
  causedByParentRequestDocId : String
  causedByParentToolCallIdPresent : Bool
  causedByParentToolCallDocIdPresent : Bool
  lineageAdmissible : Bool
  depthZeroLineageAdmissible : Bool
  backgroundCompletionDepthZeroAdmissible : Bool
  requestVisibleBeforeMessageAllowed : Bool
  messageThenRequestAllowed : Bool
  queueSource : String
  queuePolicy : String
  deriving Repr

structure SteerInterruptComposes where
  callerRequestId : String
  childSessionId : String
  interruptedActiveRequestId : String
  drainedWakeUpRequestIds : List String
  drainedWakeUpQueueKey : String
  queuedRequestId : String
  queueInterruptedRequestId : String
  deriving Repr

structure UnmaterializedChildVisible where
  callerRequestId : String
  bridgeToolCallId : String
  childRequestId : String
  childMaterialized : Bool
  bridgeLifecycleState : String
  listedStatus : String
  listedUnderAllFilter : Bool
  listedUnderRunningFilter : Bool
  readLifecycleState : String
  readTerminal : Bool
  waitRetryable : Bool
  deriving Repr

end R4cWitnesses

structure TranscriptCase where
  name : String
  group : String
  action : String
  legal : Bool
  preMessageCount : Nat
  postMessageCount : Nat
  preToolCallCount : Nat
  postToolCallCount : Nat
  preInFlightCount : Nat
  postInFlightCount : Nat
  assistantSequence : Nat
  resultSequence : Nat
  logicalResultId : Nat
  payloadHash : Nat
  expectedPairClosed : Bool
  expectedOrdered : Bool
  expectedDuplicateReusedSequence : Bool
  expectedStrongDrain : Bool
  deriving Repr

def boolString (value : Bool) : String :=
  if value then "true" else "false"

def contractBackend : BackendId :=
  { val := "contract-backend" }

def admissionName : AdmissionState → String
  | .released => "released"
  | .waiting => "waiting"
  | .acquired => "acquired"
  | .executing => "executing"

end Conformance.ContractCases
