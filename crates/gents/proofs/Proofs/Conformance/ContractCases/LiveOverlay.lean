import Proofs.Client.Types
import Proofs.ClientShell.Projection

namespace Conformance.ContractCases

structure LiveOverlayCase where
  name                : String
  responseStatus      : String
  materialized        : Bool
  hasDurableOwner     : Bool
  precedingToolCalls  : Nat
  turnTerminal        : Bool
  turnLabel           : String
  hasContent          : Bool
  hasReasoning        : Bool
  expectOverlay       : Bool
  deriving Repr

structure RequestProgressCase where
  name           : String
  lifecycleState : String
  label          : String
  animated       : Bool
  deriving Repr

structure PendingUserTurnCase where
  name                : String
  hasDurableUserOwner : Bool
  unrelatedUserTurns  : Nat
  expectPendingTurn   : Bool
  deriving Repr

def pendingUserTurnCases : List PendingUserTurnCase :=
  [ { name := "request_without_message_remains_visible"
    , hasDurableUserOwner := false
    , unrelatedUserTurns := 0
    , expectPendingTurn := projectPendingUserTurn false }
  , { name := "later_rows_do_not_hide_unmaterialized_request"
    , hasDurableUserOwner := false
    , unrelatedUserTurns := 7
    , expectPendingTurn := projectPendingUserTurn false }
  , { name := "matching_durable_message_hands_off_projection"
    , hasDurableUserOwner := true
    , unrelatedUserTurns := 7
    , expectPendingTurn := projectPendingUserTurn true }
  ]

def requestProgressCase
    (name : String) (state : RequestState) : RequestProgressCase :=
  let progress := projectRequestProgress state
  { name := name
  , lifecycleState := state.toDefraDB
  , label := progress.label
  , animated := progress.animated }

def requestProgressCases : List RequestProgressCase :=
  [ requestProgressCase "pending_is_queued" .pending
  , requestProgressCase "claimed_is_claimed" .claimed
  , requestProgressCase "processing_is_working" .processing
  , requestProgressCase "input_required_waits_for_input" .inputRequired
  , requestProgressCase "completed_is_completed" .completed
  , requestProgressCase "failed_is_failed" .failed
  , requestProgressCase "superseded_is_superseded" .superseded
  , requestProgressCase "dead_is_expired" .dead
  , requestProgressCase "interrupted_is_interrupted" .interrupted
  ]

def liveOverlayCases : List LiveOverlayCase :=
  [ { name := "pre_first_tool"
    , responseStatus := "streaming", materialized := false
    , hasDurableOwner := false
    , precedingToolCalls := 0
    , turnTerminal := false, turnLabel := "streaming"
    , hasContent := true, hasReasoning := false
    , expectOverlay := true }
  , { name := "post_tool_resumed"
    , responseStatus := "streaming", materialized := false
    , hasDurableOwner := false
    , precedingToolCalls := 1
    , turnTerminal := false, turnLabel := "streaming"
    , hasContent := true, hasReasoning := false
    , expectOverlay := true }
  , { name := "interleaved_two_tools"
    , responseStatus := "streaming", materialized := false
    , hasDurableOwner := false
    , precedingToolCalls := 2
    , turnTerminal := false, turnLabel := "streaming"
    , hasContent := true, hasReasoning := false
    , expectOverlay := true }
  , { name := "tool_first_no_pre_text"
    , responseStatus := "streaming", materialized := false
    , hasDurableOwner := false
    , precedingToolCalls := 1
    , turnTerminal := false, turnLabel := "streaming"
    , hasContent := false, hasReasoning := false
    , expectOverlay := false }
  , { name := "interrupted_mid_stream"
    , responseStatus := "streaming", materialized := false
    , hasDurableOwner := false
    , precedingToolCalls := 0
    , turnTerminal := true, turnLabel := "interrupted"
    , hasContent := true, hasReasoning := false
    , expectOverlay := false }
  , { name := "error_mid_stream"
    , responseStatus := "error", materialized := false
    , hasDurableOwner := false
    , precedingToolCalls := 0
    , turnTerminal := false, turnLabel := "streaming"
    , hasContent := false, hasReasoning := false
    , expectOverlay := false }
  , { name := "materialized_final"
    , responseStatus := "complete", materialized := true
    , hasDurableOwner := true
    , precedingToolCalls := 0
    , turnTerminal := true, turnLabel := "completed"
    , hasContent := false, hasReasoning := false
    , expectOverlay := false }
  , { name := "replicated_stale_tail_has_durable_owner"
    , responseStatus := "streaming", materialized := false
    , hasDurableOwner := true
    , precedingToolCalls := 2
    , turnTerminal := false, turnLabel := "streaming"
    , hasContent := true, hasReasoning := false
    , expectOverlay := false }
  ]

def liveOverlayCaseNames : List String :=
  liveOverlayCases.map LiveOverlayCase.name

end Conformance.ContractCases
