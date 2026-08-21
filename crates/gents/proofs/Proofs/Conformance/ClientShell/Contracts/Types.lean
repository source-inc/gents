import Proofs.ClientShell
import Proofs.Conformance.ContractTypes

namespace Conformance.ClientShellContracts

open Conformance.Contracts

structure FrontendWorkflowContract where
  kind      : String
  sessionId : Option SessionId
  requestId : Option RequestId
  turnState : Option String
  reason    : Option String
  deriving Repr

structure ClientShellContractCase where
  name : String
  property : String
  input : String
  preSelectionAgent : Option AgentDid
  preSelectionSession : Option SessionId
  postSelectionAgent : Option AgentDid
  postSelectionSession : Option SessionId
  preWorkflowKind : String
  preWorkflowSession : Option SessionId
  preWorkflowRequest : Option RequestId
  postWorkflowKind : String
  postWorkflowSession : Option SessionId
  postWorkflowRequest : Option RequestId
  selectionPreserved : Bool
  workflowAdvanced : Bool
  transportNoop : Bool
  canSubmitBefore : Bool
  canSubmitAfter : Bool
  selectionHealth : String
  projectionTurnState : Option String
  projectionWorkflowKind : String
  projectionWorkflowSession : Option SessionId
  projectionWorkflowRequest : Option RequestId
  sendDecision : String
  sendBlockedReason : Option String
  frontendClientAvailable : Bool
  frontendSelectedAgentDid : Option AgentDid
  frontendSelectedSessionId : Option SessionId
  frontendComposerNonEmpty : Bool
  frontendSending : Bool
  frontendSessionPresent : Bool
  frontendSessionId : Option SessionId
  frontendSessionLatestRequestId : Option RequestId
  frontendSessionTurnState : Option String
  frontendSessionPendingRequestId : Option RequestId
  frontendConversationPresent : Bool
  frontendConversationSessionId : Option SessionId
  frontendConversationLatestRequestId : Option RequestId
  frontendConversationTurnState : Option String
  frontendLocalWorkflowKind : String
  frontendLocalWorkflowSession : Option SessionId
  frontendLocalWorkflowRequest : Option RequestId
  frontendLocalWorkflowTurnState : Option String
  frontendExpectedWorkflowKind : String
  frontendExpectedWorkflowSession : Option SessionId
  frontendExpectedWorkflowRequest : Option RequestId
  frontendExpectedWorkflowTurnState : Option String
  frontendExpectedWorkflowReason : Option String
  frontendExpectedSendStatus : String
  frontendExpectedSendBlockedReason : Option String
  frontendExpectedActiveRequestId : Option RequestId
  frontendExpectedTurnState : Option String
  desktopSelectedSessionId : Option SessionId
  desktopSnapshotPresent : Bool
  desktopPreferredRequestId : Option RequestId
  desktopObservedRequestId : Option RequestId
  desktopObservedTurnState : Option String
  desktopExpectedLatestRequestId : Option RequestId
  desktopExpectedTurnState : Option String
  desktopExpectPendingTurn : Option Bool
  deriving Repr

def boolJson (value : Bool) : String :=
  if value then "true" else "false"

def jsonNatOption : Option Nat → String
  | some value => toString value
  | none       => "null"

def jsonStringOption : Option String → String
  | some value => jsonString value
  | none       => "null"

def jsonBoolOption : Option Bool → String
  | some value => boolJson value
  | none       => "null"

def clientTurnStateName : ClientTurnState → String
  | .waitingForClaim => "waitingForClaim"
  | .streaming       => "streaming"
  | .completed       => "completed"
  | .failed          => "failed"
  | .superseded      => "superseded"
  | .interrupted     => "interrupted"

def selectionHealthName : SelectionHealth → String
  | .noSelection        => "noSelection"
  | .resolved           => "resolved"
  | .pendingObservation => "pendingObservation"
  | .absent             => "absent"

def workflowKind : SubmissionWorkflow → String
  | .idle           => "idle"
  | .submitting _ _ => "submitting"
  | .awaiting _ _   => "awaiting"
  | .blocked _      => "blocked"

def workflowSession : SubmissionWorkflow → Option SessionId
  | .submitting _ sid => sid
  | .awaiting sid _   => some sid
  | _                 => none

def workflowRequest : SubmissionWorkflow → Option RequestId
  | .awaiting _ req => some req
  | _               => none

def blockedReasonName : BlockedReason → String
  | .clientOffline       => "clientOffline"
  | .behaviorMismatch _ _ => "sessionBehaviorMismatch"
  | .mutationRejected    => "mutationRejected"

def sendBlockedReasonName : SendBlockedReason → String
  | .clientOffline              => "clientOffline"
  | .agentNotSelected           => "agentNotSelected"
  | .composerEmpty              => "composerEmpty"
  | .mutationInFlight           => "mutationInFlight"
  | .awaitingObservation        => "awaitingObservation"
  | .awaitingTurnTerminality _  => "awaitingTurnTerminality"
  | .sessionBehaviorMismatch    => "sessionBehaviorMismatch"
  | .sessionAbsent              => "sessionAbsent"
  | .inconsistentObservation    => "inconsistentObservation"
  | .workflowBlocked            => "workflowBlocked"

def frontendBlockedReasonName : SendBlockedReason → String
  | .clientOffline              => "clientOffline"
  | .agentNotSelected           => "agentNotSelected"
  | .composerEmpty              => "composerEmpty"
  | .mutationInFlight           => "submittingRequest"
  | .awaitingObservation        => "waitingForRequestObservation"
  | .awaitingTurnTerminality _  => "awaitingTurnTerminality"
  | .sessionBehaviorMismatch    => "sessionBehaviorMismatch"
  | .sessionAbsent              => "conversationMissingFromSnapshot"
  | .inconsistentObservation    => "inconsistentTurnObservation"
  | .workflowBlocked            => "workflowBlocked"

def sendDecisionKind : SendDecision → String
  | .ready     => "ready"
  | .blocked _ => "blocked"

def sendDecisionReason : SendDecision → Option String
  | .ready     => none
  | .blocked r => some (sendBlockedReasonName r)

def frontendSendStatus : SendDecision → String
  | .ready     => "ready"
  | .blocked _ => "disabled"

def frontendSendReason : SendDecision → Option String
  | .ready     => none
  | .blocked r => some (frontendBlockedReasonName r)

def inputName : ShellInput → String
  | .user (.selectDeployment _ _)     => "selectDeployment"
  | .user (.selectSession _)          => "selectSession"
  | .user .requestNewConversation     => "requestNewConversation"
  | .user .startSubmit                => "startSubmit"
  | .user .acknowledgeBlocker         => "acknowledgeBlocker"
  | .snapshot _                       => "snapshot"
  | .mutation (.submitted _ _)        => "mutation.submitted"
  | .mutation (.failed _)             => "mutation.failed"
  | .transport .healthy               => "transport.healthy"
  | .transport .degraded              => "transport.degraded"
  | .transport .wedged                => "transport.wedged"

end Conformance.ClientShellContracts
