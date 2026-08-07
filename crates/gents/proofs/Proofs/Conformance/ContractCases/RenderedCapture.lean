import Proofs.Conformance.ContractCases.Types
import Proofs.RenderedCapture

/-!
# RenderedCapture contract cases

Witness rows for persist-before-send at the provider boundary (#840).

**Every expected value in this file is computed by running the Lean model.**
`captureOutcome`, `durableAfter`, `sendPermitted`, and
`providerRequestsObserved` are literally `RenderedCapture.Scenario.outcome`,
`.durableAfter`, `.sendPermitted`, and `.providerRequests`. Nothing here is
transcribed by hand, so a change to `RenderedCapture.capture` changes the rows
and breaks the Rust fence rather than quietly disagreeing with it.

The rows are also not a second, unproven story about the transition order:
`RenderedCapture.Scenario.trace_realizes` proves each scenario's computed
`(store, stage)` pair is reachable from its `assembled` start by legal `Step`s,
so production that reproduces these rows inherits `sent_implies_durably_captured`,
`sent_requires_a_capture_step`, and `capture_failure_blocks_send`.

`renderedCaptureKeyCases` fences the other half: that the capture key is a
five-component tuple and that equality is componentwise. The distinctness of
each pair is decided by the model, not asserted here.
-/

namespace Conformance.ContractCases

open RenderedCapture

/-- One capture delivery, flattened for emission. -/
structure RenderedCaptureCase where
  name : String
  agentDid : Nat
  sessionId : Nat
  requestId : Nat
  turnIndex : Nat
  attempt : Nat
  /-- Opaque canonical-request identity. Equal values mean equal canonical JSON. -/
  request : Nat
  /-- What the durable row already held under this key before the delivery. -/
  priorBinding : Option Nat
  captureOutcome : String
  captureDurable : Bool
  postStage : String
  sendPermitted : Bool
  /-- How many requests the provider is allowed to observe for this attempt. -/
  providerRequestsObserved : Nat
  /-- What the durable row holds under this key afterwards. -/
  durableAfter : Option Nat
  finalStage : String
  deriving Repr

/-- Two capture keys and whether the model considers them the same fact. -/
structure RenderedCaptureKeyCase where
  name : String
  leftAgentDid : Nat
  leftSessionId : Nat
  leftRequestId : Nat
  leftTurnIndex : Nat
  leftAttempt : Nat
  rightAgentDid : Nat
  rightSessionId : Nat
  rightRequestId : Nat
  rightTurnIndex : Nat
  rightAttempt : Nat
  sameFact : Bool
  deriving Repr

/-! ## Building the rows -/

private def contractAgentDid : Nat := 7
private def contractSessionId : Nat := 11
private def contractRequestId : Nat := 23

private def contractKey (turnIndex attempt : Nat) : CaptureKey :=
  { agentDid := contractAgentDid
  , sessionId := contractSessionId
  , requestId := contractRequestId
  , turnIndex := turnIndex
  , attempt := attempt
  }

private def renderedCaptureCase
    (name : String) (turnIndex attempt request : Nat)
    (priorBinding : Option Nat) : RenderedCaptureCase :=
  let scenario : Scenario :=
    { key := contractKey turnIndex attempt
    , request := { value := request }
    , priorBinding := priorBinding.map (fun value => { value := value })
    }
  { name := name
  , agentDid := contractAgentDid
  , sessionId := contractSessionId
  , requestId := contractRequestId
  , turnIndex := turnIndex
  , attempt := attempt
  , request := request
  , priorBinding := priorBinding
  , captureOutcome := (Scenario.outcome scenario).toContract
  , captureDurable := (Scenario.outcome scenario).durable
  , postStage := (Scenario.postStage scenario).toContract
  , sendPermitted := Scenario.sendPermitted scenario
  , providerRequestsObserved := Scenario.providerRequests scenario
  , durableAfter := (Scenario.durableAfter scenario).map CanonicalRequest.value
  , finalStage := (Scenario.finalStage scenario).toContract
  }

/-- The five delivery shapes the sink has to get right.

* a first capture,
* a redelivery of the identical canonical request (restart, lost ack, retried
  mutation) — success without a write,
* a reused key carrying a *different* canonical request — an integrity error
  that must block the provider call,
* a transport retry, which re-sends an identical request under a new `attempt`
  and is therefore a second durable fact rather than an idempotent hit,
* a repair retry, whose assembled input legitimately differs from attempt 0's
  and which is likewise a separate fact. -/
def renderedCaptureCases : List RenderedCaptureCase :=
  [ renderedCaptureCase "fresh_capture_then_send" 0 0 100 none
  , renderedCaptureCase "idempotent_recapture_then_send" 0 0 100 (some 100)
  , renderedCaptureCase "rebound_key_is_an_integrity_violation" 0 0 100 (some 101)
  , renderedCaptureCase "transport_retry_same_request_new_attempt" 0 1 100 none
  , renderedCaptureCase "repair_retry_different_request_new_attempt" 0 1 102 none
  ]

/-- Pinned expected outputs: this fails at Lean build time if `capture` drifts,
so the emitted rows stay honest instead of self-referential. -/
theorem renderedCaptureCases_pinned :
    renderedCaptureCases.map
        (fun row =>
          (row.name, row.captureOutcome, row.captureDurable, row.postStage,
            row.sendPermitted, row.providerRequestsObserved, row.durableAfter,
            row.finalStage)) =
      [ ("fresh_capture_then_send", "fresh", true, "durablyCaptured", true, 1,
          some 100, "sent")
      , ("idempotent_recapture_then_send", "idempotent", true, "durablyCaptured",
          true, 1, some 100, "sent")
      , ("rebound_key_is_an_integrity_violation", "rejected", false, "assembled",
          false, 0, some 101, "assembled")
      , ("transport_retry_same_request_new_attempt", "fresh", true,
          "durablyCaptured", true, 1, some 100, "sent")
      , ("repair_retry_different_request_new_attempt", "fresh", true,
          "durablyCaptured", true, 1, some 102, "sent")
      ] := by
  rfl

/-- No emitted row may permit a send without leaving the fact durable under its
own key. This is the fail-open guard on the emitted data itself. -/
theorem renderedCaptureCases_no_fail_open :
    renderedCaptureCases.all
      (fun row =>
        (!row.sendPermitted || (row.durableAfter == some row.request &&
            row.captureDurable && row.providerRequestsObserved == 1)) &&
        (row.sendPermitted || row.providerRequestsObserved == 0)) = true := by
  rfl

private def renderedCaptureKeyCase
    (name : String) (left right : CaptureKey) : RenderedCaptureKeyCase :=
  { name := name
  , leftAgentDid := left.agentDid
  , leftSessionId := left.sessionId
  , leftRequestId := left.requestId
  , leftTurnIndex := left.turnIndex
  , leftAttempt := left.attempt
  , rightAgentDid := right.agentDid
  , rightSessionId := right.sessionId
  , rightRequestId := right.requestId
  , rightTurnIndex := right.turnIndex
  , rightAttempt := right.attempt
  , sameFact := decide (left = right)
  }

/-- One pair per key component, plus the identical pair. Componentwise equality
is the whole contract: any component that production drops from the key silently
merges two facts. -/
def renderedCaptureKeyCases : List RenderedCaptureKeyCase :=
  [ renderedCaptureKeyCase "identical_tuple_is_one_fact"
      (contractKey 0 0) (contractKey 0 0)
  , renderedCaptureKeyCase "attempt_separates_facts"
      (contractKey 0 0) (contractKey 0 1)
  , renderedCaptureKeyCase "turn_index_separates_facts"
      (contractKey 0 0) (contractKey 1 0)
  , renderedCaptureKeyCase "agent_did_separates_facts"
      (contractKey 0 0) { contractKey 0 0 with agentDid := contractAgentDid + 1 }
  , renderedCaptureKeyCase "session_id_separates_facts"
      (contractKey 0 0) { contractKey 0 0 with sessionId := contractSessionId + 1 }
  , renderedCaptureKeyCase "request_doc_id_separates_facts"
      (contractKey 0 0) { contractKey 0 0 with requestId := contractRequestId + 1 }
  ]

theorem renderedCaptureKeyCases_pinned :
    renderedCaptureKeyCases.map (fun row => (row.name, row.sameFact)) =
      [ ("identical_tuple_is_one_fact", true)
      , ("attempt_separates_facts", false)
      , ("turn_index_separates_facts", false)
      , ("agent_did_separates_facts", false)
      , ("session_id_separates_facts", false)
      , ("request_doc_id_separates_facts", false)
      ] := by
  rfl

/-! ## Capture scope labels and the consumer ordering (#1066)

The producer writes the loop discriminator as `"{kind}.{seq}"`. These cases fix
the label vocabulary a consumer's parser must accept, the reject vectors it
must error on (never default), and the identity ordering
`(kind rank, seq, turn, attempt)` — with `seq` compared numerically, because a
lexical sort of the raw column puts `inference.10` before `inference.2`. -/

inductive CaptureScopeKind
  | inference
  | compaction
  | compactionFallback
  | title
  | oneshot
  deriving Repr, DecidableEq

def CaptureScopeKind.label : CaptureScopeKind → String
  | .inference => "inference"
  | .compaction => "compaction"
  | .compactionFallback => "compaction_fallback"
  | .title => "title"
  | .oneshot => "oneshot"

/-- Declaration-order rank — the stable identity order across kinds. Not a
temporal order: loops interleave in wall-clock time. -/
def CaptureScopeKind.rank : CaptureScopeKind → Nat
  | .inference => 0
  | .compaction => 1
  | .compactionFallback => 2
  | .title => 3
  | .oneshot => 4

def allCaptureScopeKinds : List CaptureScopeKind :=
  [.inference, .compaction, .compactionFallback, .title, .oneshot]

structure CaptureScopeCase where
  label : String
  kind : String
  seq : Nat
  valid : Bool
  deriving Repr

private def validScopeCase (kind : CaptureScopeKind) (seq : Nat) : CaptureScopeCase :=
  { label := kind.label ++ "." ++ toString seq
  , kind := kind.label
  , seq := seq
  , valid := true
  }

private def invalidScopeCase (label : String) : CaptureScopeCase :=
  { label := label, kind := "", seq := 0, valid := false }

/-- Every kind at seq 1, multi-digit seqs (so a lexical sort cannot pass the
ordering fence), and the parser's reject vectors. -/
def captureScopeCases : List CaptureScopeCase :=
  (allCaptureScopeKinds.map (validScopeCase · 1))
    ++ [ validScopeCase .inference 2
       , validScopeCase .inference 10
       , validScopeCase .compaction 2
       ]
    ++ [ invalidScopeCase ""
       , invalidScopeCase "inference"
       , invalidScopeCase "inference."
       , invalidScopeCase ".1"
       , invalidScopeCase "mystery.1"
       , invalidScopeCase "inference.0x2"
       , invalidScopeCase "inference.1.2"
       , invalidScopeCase "inference. 2"
       ]

/-- Valid labels are pairwise distinct — the label round-trip is injective over
the emitted vocabulary. -/
theorem captureScopeCases_valid_labels_distinct :
    ((captureScopeCases.filter (·.valid)).map (·.label)).Nodup := by
  decide

structure CaptureOrderCase where
  name : String
  leftLabel : String
  leftTurn : Nat
  leftAttempt : Nat
  rightLabel : String
  rightTurn : Nat
  rightAttempt : Nat
  leftBeforeRight : Bool
  deriving Repr

private def orderTuple
    (kind : CaptureScopeKind) (seq turn attempt : Nat) : Nat × Nat × Nat × Nat :=
  (kind.rank, seq, turn, attempt)

private def tupleLt : Nat × Nat × Nat × Nat → Nat × Nat × Nat × Nat → Bool
  | (a1, a2, a3, a4), (b1, b2, b3, b4) =>
    if a1 ≠ b1 then a1 < b1
    else if a2 ≠ b2 then a2 < b2
    else if a3 ≠ b3 then a3 < b3
    else a4 < b4

private def orderCase (name : String)
    (leftKind : CaptureScopeKind) (leftSeq leftTurn leftAttempt : Nat)
    (rightKind : CaptureScopeKind) (rightSeq rightTurn rightAttempt : Nat) :
    CaptureOrderCase :=
  { name := name
  , leftLabel := (validScopeCase leftKind leftSeq).label
  , leftTurn := leftTurn
  , leftAttempt := leftAttempt
  , rightLabel := (validScopeCase rightKind rightSeq).label
  , rightTurn := rightTurn
  , rightAttempt := rightAttempt
  , leftBeforeRight :=
      tupleLt (orderTuple leftKind leftSeq leftTurn leftAttempt)
        (orderTuple rightKind rightSeq rightTurn rightAttempt)
  }

def captureOrderCases : List CaptureOrderCase :=
  [ orderCase "numeric_seq_dominates_lexical" .inference 2 3 1 .inference 10 0 0
  , orderCase "attempt_breaks_turn_tie" .inference 1 0 0 .inference 1 0 1
  , orderCase "turn_dominates_attempt" .inference 1 0 5 .inference 1 1 0
  , orderCase "kind_rank_dominates_seq" .inference 9 9 9 .compaction 1 0 0
  , orderCase "fallback_ranks_after_compaction" .compaction 5 0 0 .compactionFallback 1 0 0
  , orderCase "oneshot_ranks_last" .title 1 0 0 .oneshot 1 0 0
  , orderCase "equal_keys_do_not_order" .inference 1 2 3 .inference 1 2 3
  ]

/-- Pinned so a change to the rank or comparison shows up as a Lean build
failure and a conscious decision, not a silent re-sort of every consumer. -/
theorem captureOrderCases_pinned :
    captureOrderCases.map (fun row => (row.name, row.leftBeforeRight)) =
      [ ("numeric_seq_dominates_lexical", true)
      , ("attempt_breaks_turn_tie", true)
      , ("turn_dominates_attempt", true)
      , ("kind_rank_dominates_seq", true)
      , ("fallback_ranks_after_compaction", true)
      , ("oneshot_ranks_last", true)
      , ("equal_keys_do_not_order", false)
      ] := by
  rfl

end Conformance.ContractCases
