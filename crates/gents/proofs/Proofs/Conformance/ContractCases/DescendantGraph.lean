import Proofs.DescendantGraph

namespace Conformance.ContractCases

open DescendantGraph

structure DescendantGraphCase where
  name : String
  rootRequestId : Nat
  parentRequestId : Nat
  childRequestId : Nat
  awaitMode : String
  materialization : String
  lifecycle : String
  direct : Bool
  visible : Bool
  readable : Bool
  retryable : Bool
  listedByDefault : Bool
  controllable : Bool
  cursorAnchorSurvivesTerminal : Bool
  callerSession : String
  callerAgent : String
  callerRequester : Option String
  sessionAuthorized : Bool
  sessionControllable : Bool
  deriving Repr

def viewer : Viewer :=
  { rootRequestId := 1
  , rootPrincipal := 10
  , rootSessionId := 100
  , lineageId := 1000 }

def baseEdge : Edge :=
  { rootRequestId := 1
  , rootSessionId := 100
  , parentRequestId := 1
  , parentToolCallId := 20
  , childRequestId := 2
  , childSessionId := some 200
  , ownerPrincipal := 10
  , controlPrincipal := 10
  , childPrincipal := 11
  , behaviorId := 30
  , deploymentId := 40
  , lineageId := 1000
  , awaitMode := .background
  , materialization := .local
  , lifecycle := .running
  , bridgeDurable := true
  , physicalCorroborated := true
  , directFromRoot := true }

def awaitModeString : AwaitMode → String
  | .foreground => "foreground"
  | .background => "background"

def materializationString : Materialization → String
  | .pending => "pending"
  | .local => "local"
  | .replicated => "replicated"

def lifecycleString : Lifecycle → String
  | .pending => "pending"
  | .running => "running"
  | .completed => "completed"
  | .failed => "failed"
  | .cancelled => "cancelled"

def sessionOwner : SessionOwner :=
  { sessionId := "conversation", agentDid := "did:owner", requesterDid := none }

def descendantCase (name : String) (edge : Edge)
    (caller : SessionOwner := sessionOwner) : DescendantGraphCase :=
  { name
  , rootRequestId := edge.rootRequestId
  , parentRequestId := edge.parentRequestId
  , childRequestId := edge.childRequestId
  , awaitMode := awaitModeString edge.awaitMode
  , materialization := materializationString edge.materialization
  , lifecycle := lifecycleString edge.lifecycle
  , direct := edge.directFromRoot
  , visible := DescendantGraph.visible viewer edge
  , readable := DescendantGraph.readable viewer edge
  , retryable := DescendantGraph.retryable viewer edge
  , listedByDefault := DescendantGraph.listedByDefault viewer edge
  , controllable := DescendantGraph.controllable viewer edge
  , cursorAnchorSurvivesTerminal :=
      (DescendantGraph.afterCursor
        (DescendantGraph.cursor edge)
        [{ edge with lifecycle := .completed }, baseEdge]).isSome
  , callerSession := caller.sessionId
  , callerAgent := caller.agentDid
  , callerRequester := caller.requesterDid
  , sessionAuthorized := sameSessionOwner caller sessionOwner
  , sessionControllable := DescendantGraph.sessionControllable caller sessionOwner viewer edge }

def descendantGraphCases : List DescendantGraphCase :=
  [ descendantCase "background_direct" baseEdge
  , descendantCase "foreground_direct"
      { { baseEdge with awaitMode := .foreground } with childRequestId := 3 }
  , descendantCase "nested_visible_not_controllable"
      { { { { { baseEdge with parentRequestId := 5 } with
          parentToolCallId := 21 } with childRequestId := 6 } with
          directFromRoot := false } with controlPrincipal := 11 }
  , descendantCase "unmaterialized_remote_bridge"
      { { { { { baseEdge with childRequestId := 7 } with childSessionId := none } with
          materialization := .pending } with physicalCorroborated := false } with
          deploymentId := 41 }
  , descendantCase "terminal_unmaterialized_remote_bridge"
      { { { { { baseEdge with childRequestId := 14 } with childSessionId := none } with
          materialization := .pending } with physicalCorroborated := false } with
          lifecycle := .failed }
  , descendantCase "terminal_result_edge"
      { { baseEdge with childRequestId := 8 } with lifecycle := .completed }
  , descendantCase "replicated_remote_materialization"
      { { { baseEdge with childRequestId := 9 } with
          materialization := .replicated } with deploymentId := 42 }
  , descendantCase "unauthorized_principal"
      { { baseEdge with childRequestId := 10 } with ownerPrincipal := 99 }
  , descendantCase "unauthorized_session"
      { { baseEdge with childRequestId := 11 } with rootSessionId := 999 }
  , descendantCase "unauthorized_lineage"
      { { baseEdge with childRequestId := 12 } with lineageId := 9999 }
  , descendantCase "uncorroborated_materialized"
      { { baseEdge with childRequestId := 13 } with physicalCorroborated := false }
  , descendantCase "later_user_turn" baseEdge sessionOwner
  , descendantCase "other_conversation" baseEdge
      { sessionOwner with sessionId := "other" }
  , descendantCase "other_agent" baseEdge
      { sessionOwner with agentDid := "did:other" }
  , descendantCase "other_requester" baseEdge
      { sessionOwner with requesterDid := some "did:requester" }
  , descendantCase "empty_requester_is_not_absent" baseEdge
      { sessionOwner with requesterDid := some "" }
  , descendantCase "missing_agent" baseEdge
      { sessionOwner with agentDid := "" }
  , descendantCase "missing_session" baseEdge
      { sessionOwner with sessionId := "" }
  , descendantCase "blank_agent" baseEdge
      { sessionOwner with agentDid := " \t" }
  , descendantCase "blank_session" baseEdge
      { sessionOwner with sessionId := " \t" }
  ]

end Conformance.ContractCases
