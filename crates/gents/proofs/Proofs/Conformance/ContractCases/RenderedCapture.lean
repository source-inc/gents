import Proofs.Conformance.ContractCases.Types
import Proofs.RenderedCapture

/-!
# RenderedCapture contract cases

Witness rows for persist-before-send at the provider boundary (#840).

**Every expected value in this file is computed by running the Lean model.**
`configComplete`, `captureOutcome`, `durableAfter`, `sendPermitted`, and
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

/-- One exact resolved-config source reference, flattened for JSON emission. -/
structure RenderedConfigSourceRefCase where
  sourceClass : String
  logicalId : Option Nat
  docId : Nat
  compositeCommitCid : Nat
  signerDid : Nat
  deriving Repr

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
  configScope : String
  configRequired : Bool
  configPresent : Bool
  configSources : List RenderedConfigSourceRefCase
  configComplete : Bool
  configAdmitted : Bool
  /-- What the durable row already held under this key before the delivery. -/
  priorBinding : Option Nat
  priorConfigPresent : Bool
  priorConfigSources : List RenderedConfigSourceRefCase
  captureOutcome : String
  captureDurable : Bool
  postStage : String
  sendPermitted : Bool
  /-- How many requests the provider is allowed to observe for this attempt. -/
  providerRequestsObserved : Nat
  /-- What the durable row holds under this key afterwards. -/
  durableAfter : Option Nat
  durableConfigPresent : Bool
  durableConfigSources : List RenderedConfigSourceRefCase
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

private def signedConfigSource (docId cid signerDid : Nat) : SignedDocumentVersionRef :=
  { version := { docId := docId, compositeCommitCid := cid }
  , signerDid := signerDid
  }

/-- Full production-shaped fixture: four required refs, an optional tool
selection, and two skills in strict logical-id order. -/
private def contractConfig (seed : Nat) : ResolvedConfigProvenance :=
  { principal := signedConfigSource (seed + 1) (seed + 101) (seed + 201)
  , behavior := signedConfigSource (seed + 2) (seed + 102) (seed + 202)
  , inferenceBackend := signedConfigSource (seed + 3) (seed + 103) (seed + 203)
  , inferenceProfile := signedConfigSource (seed + 4) (seed + 104) (seed + 204)
  , toolSelection := some (signedConfigSource (seed + 5) (seed + 105) (seed + 205))
  , skills :=
      [ { logicalId := 10
        , source := signedConfigSource (seed + 6) (seed + 106) (seed + 206) }
      , { logicalId := 20
        , source := signedConfigSource (seed + 7) (seed + 107) (seed + 207) }
      ]
  }

/-- Tool selection and skills are both legitimately absent. -/
private def minimalConfig (seed : Nat) : ResolvedConfigProvenance :=
  { principal := signedConfigSource (seed + 1) (seed + 101) (seed + 201)
  , behavior := signedConfigSource (seed + 2) (seed + 102) (seed + 202)
  , inferenceBackend := signedConfigSource (seed + 3) (seed + 103) (seed + 203)
  , inferenceProfile := signedConfigSource (seed + 4) (seed + 104) (seed + 204)
  , toolSelection := none
  , skills := []
  }

private def emptyCidConfig : ResolvedConfigProvenance :=
  { contractConfig 1000 with
    inferenceProfile := signedConfigSource 1004 0 1204 }

private def emptySignerConfig : ResolvedConfigProvenance :=
  { contractConfig 1000 with
    behavior := signedConfigSource 1002 1102 0 }

private def invalidOptionalToolConfig : ResolvedConfigProvenance :=
  { contractConfig 1000 with
    toolSelection := some (signedConfigSource 1005 0 1205) }

private def nonCanonicalSkillsConfig : ResolvedConfigProvenance :=
  let complete := contractConfig 1000
  { complete with skills := complete.skills.reverse }

private def duplicateSkillsConfig : ResolvedConfigProvenance :=
  let complete := contractConfig 1000
  { complete with skills :=
      [ { logicalId := 10, source := signedConfigSource 1006 1106 1206 }
      , { logicalId := 10, source := signedConfigSource 1007 1107 1207 }
      ] }

private def canonicalRequest (value : Nat)
    (config : Option ResolvedConfigProvenance) : CanonicalRequest :=
  { value := value
  , configScope := .reconciledDocumentRuntime
  , configProvenance := config
  }

private def staticRequest (value : Nat)
    (config : Option ResolvedConfigProvenance := none) : CanonicalRequest :=
  { value := value, configScope := .staticOrOneShot, configProvenance := config }

private def configRow (sourceClass : String) (logicalId : Option Nat)
    (source : SignedDocumentVersionRef) : RenderedConfigSourceRefCase :=
  { sourceClass := sourceClass
  , logicalId := logicalId
  , docId := source.version.docId
  , compositeCommitCid := source.version.compositeCommitCid
  , signerDid := source.signerDid
  }

private def configRows : Option ResolvedConfigProvenance → List RenderedConfigSourceRefCase
  | none => []
  | some provenance =>
      [ configRow "principal" none provenance.principal
      , configRow "behavior" none provenance.behavior
      , configRow "inference_backend" none provenance.inferenceBackend
      , configRow "inference_profile" none provenance.inferenceProfile
      ] ++
      provenance.toolSelection.toList.map (configRow "tool_selection" none) ++
      provenance.skills.map (fun skill => configRow "skill" (some skill.logicalId) skill.source)

private def configPresent (request : Option CanonicalRequest) : Bool :=
  request.any (fun candidate => candidate.configProvenance.isSome)

private def requestConfigRows (request : Option CanonicalRequest) :
    List RenderedConfigSourceRefCase :=
  match request with
  | none => []
  | some candidate => configRows candidate.configProvenance

private def renderedCaptureCase
    (name : String) (turnIndex attempt : Nat) (request : CanonicalRequest)
    (priorBinding : Option CanonicalRequest) : RenderedCaptureCase :=
  let scenario : Scenario :=
    { key := contractKey turnIndex attempt
    , request := request
    , priorBinding := priorBinding
    }
  let durable := Scenario.durableAfter scenario
  { name := name
  , agentDid := contractAgentDid
  , sessionId := contractSessionId
  , requestId := contractRequestId
  , turnIndex := turnIndex
  , attempt := attempt
  , request := request.value
  , configScope := request.configScope.toContract
  , configRequired := request.configScope.requiresExactConfig
  , configPresent := request.configProvenance.isSome
  , configSources := configRows request.configProvenance
  , configComplete := request.hasExactConfigProvenance
  , configAdmitted := request.configAdmitted
  , priorBinding := priorBinding.map CanonicalRequest.value
  , priorConfigPresent := configPresent priorBinding
  , priorConfigSources := requestConfigRows priorBinding
  , captureOutcome := (Scenario.outcome scenario).toContract
  , captureDurable := (Scenario.outcome scenario).durable
  , postStage := (Scenario.postStage scenario).toContract
  , sendPermitted := Scenario.sendPermitted scenario
  , providerRequestsObserved := Scenario.providerRequests scenario
  , durableAfter := durable.map CanonicalRequest.value
  , durableConfigPresent := configPresent durable
  , durableConfigSources := requestConfigRows durable
  , finalStage := (Scenario.finalStage scenario).toContract
  }

/-- The thirteen delivery shapes the sink has to get right.

* a first capture,
* a redelivery of the identical canonical request (restart, lost ack, retried
  mutation) — success without a write,
* a reused key carrying a *different* canonical request — an integrity error
  that must block the provider call,
* a transport retry, which re-sends an identical request under a new `attempt`
  and is therefore a second durable fact rather than an idempotent hit,
* a repair retry, whose assembled input legitimately differs from attempt 0's
  and which is likewise a separate fact,
* optional tool selection and an empty skill list as a valid positive shape,
* a static/legacy/one-shot request that honestly omits config as a positive,
* missing reconciled config, empty source identity, empty signer, invalid optional tool,
  non-canonical skill order, and duplicate skill identity — all rejected
  without a write, and
* equal body/request-chain data rebound to another complete config bundle — a
  canonical fact conflict rather than idempotent replay. -/
def renderedCaptureCases : List RenderedCaptureCase :=
  let config := some (contractConfig 1000)
  let minimal := some (minimalConfig 3000)
  let alternateConfig := some (contractConfig 2000)
  [ renderedCaptureCase "fresh_capture_then_send" 0 0 (canonicalRequest 100 config) none
  , renderedCaptureCase "idempotent_recapture_then_send" 0 0
      (canonicalRequest 100 config) (some (canonicalRequest 100 config))
  , renderedCaptureCase "rebound_key_is_an_integrity_violation" 0 0
      (canonicalRequest 100 config) (some (canonicalRequest 101 config))
  , renderedCaptureCase "transport_retry_same_request_new_attempt" 0 1
      (canonicalRequest 100 minimal) none
  , renderedCaptureCase "repair_retry_different_request_new_attempt" 0 1
      (canonicalRequest 102 config) none
  , renderedCaptureCase "reconciled_runtime_missing_config_blocks_send" 0 0
      (canonicalRequest 100 none) none
  , renderedCaptureCase "static_or_one_shot_without_config_can_send" 0 0
      (staticRequest 100) none
  , renderedCaptureCase "empty_config_source_ref_blocks_send" 0 0
      (canonicalRequest 100 (some emptyCidConfig)) none
  , renderedCaptureCase "empty_config_signer_blocks_send" 0 0
      (canonicalRequest 100 (some emptySignerConfig)) none
  , renderedCaptureCase "invalid_optional_tool_ref_blocks_send" 0 0
      (canonicalRequest 100 (some invalidOptionalToolConfig)) none
  , renderedCaptureCase "noncanonical_skill_order_blocks_send" 0 0
      (canonicalRequest 100 (some nonCanonicalSkillsConfig)) none
  , renderedCaptureCase "duplicate_skill_identity_blocks_send" 0 0
      (canonicalRequest 100 (some duplicateSkillsConfig)) none
  , renderedCaptureCase "config_provenance_rebinding_is_an_integrity_violation" 0 0
      (canonicalRequest 100 alternateConfig) (some (canonicalRequest 100 config))
  ]

/-- Pinned expected outputs: this fails at Lean build time if `capture` drifts,
so the emitted rows stay honest instead of self-referential. -/
theorem renderedCaptureCases_pinned :
    renderedCaptureCases.map
        (fun row =>
          (row.name, row.captureOutcome, row.captureDurable, row.postStage,
            row.sendPermitted, row.providerRequestsObserved, row.durableAfter,
            row.finalStage, row.configComplete, row.configAdmitted)) =
      [ ("fresh_capture_then_send", "fresh", true, "durablyCaptured", true, 1,
          some 100, "sent", true, true)
      , ("idempotent_recapture_then_send", "idempotent", true, "durablyCaptured",
          true, 1, some 100, "sent", true, true)
      , ("rebound_key_is_an_integrity_violation", "rejected", false, "assembled",
          false, 0, some 101, "assembled", true, true)
      , ("transport_retry_same_request_new_attempt", "fresh", true,
          "durablyCaptured", true, 1, some 100, "sent", true, true)
      , ("repair_retry_different_request_new_attempt", "fresh", true,
          "durablyCaptured", true, 1, some 102, "sent", true, true)
      , ("reconciled_runtime_missing_config_blocks_send", "rejected", false, "assembled",
          false, 0, none, "assembled", false, false)
      , ("static_or_one_shot_without_config_can_send", "fresh", true,
          "durablyCaptured", true, 1, some 100, "sent", false, true)
      , ("empty_config_source_ref_blocks_send", "rejected", false, "assembled",
          false, 0, none, "assembled", false, false)
      , ("empty_config_signer_blocks_send", "rejected", false, "assembled",
          false, 0, none, "assembled", false, false)
      , ("invalid_optional_tool_ref_blocks_send", "rejected", false, "assembled",
          false, 0, none, "assembled", false, false)
      , ("noncanonical_skill_order_blocks_send", "rejected", false, "assembled",
          false, 0, none, "assembled", false, false)
      , ("duplicate_skill_identity_blocks_send", "rejected", false, "assembled",
          false, 0, none, "assembled", false, false)
      , ("config_provenance_rebinding_is_an_integrity_violation", "rejected", false,
          "assembled", false, 0, some 100, "assembled", true, true)
      ] := by
  rfl

/-- No emitted row may permit a send without leaving the fact durable under its
own key. This is the fail-open guard on the emitted data itself. -/
theorem renderedCaptureCases_no_fail_open :
    renderedCaptureCases.all
      (fun row =>
        (!row.sendPermitted || (row.durableAfter == some row.request &&
            row.captureDurable && row.configAdmitted &&
            row.providerRequestsObserved == 1)) &&
        (row.sendPermitted || row.providerRequestsObserved == 0)) = true := by
  native_decide

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

end Conformance.ContractCases
