import Proofs.EventDelivery
import Proofs.EventDelivery.DurableAdmission
import Proofs.Conformance.ContractTypes

namespace Conformance.EventDelivery

open _root_.EventDelivery
open Conformance.Contracts

structure TransitionCase where
  name   : String
  pre    : World
  action : Action
  post   : World

private def doc (s : String) : DocId := { raw := s }

private def w0 : World := World.empty

private def mkWorld
    (ps : List DocId) (sq : List DocId) (proc : List DocId) (h : List DocId) : World :=
  { persistentSet := ps, subscriptionQueue := sq, processedSet := proc, handled := h }

def transitionCases : List TransitionCase :=
  [
    { name   := "persist_into_empty"
    , pre    := w0
    , action := .persist (doc "a")
    , post   := mkWorld [doc "a"] [] [] []
    }
  ,
    { name   := "persist_extends_set"
    , pre    := mkWorld [doc "a"] [] [] []
    , action := .persist (doc "b")
    , post   := mkWorld [doc "b", doc "a"] [] [] []
    }
  ,
    { name   := "depersist_removes"
    , pre    := mkWorld [doc "a", doc "b"] [] [] []
    , action := .depersist (doc "a")
    , post   := mkWorld [doc "b"] [] [] []
    }
  ,
    { name   := "enqueue_from_persistent"
    , pre    := mkWorld [doc "a"] [] [] []
    , action := .enqueue (doc "a")
    , post   := mkWorld [doc "a"] [doc "a"] [] []
    }
  ,
    { name   := "drop_from_queue"
    , pre    := mkWorld [doc "a"] [doc "a"] [] []
    , action := .drop (doc "a")
    , post   := mkWorld [doc "a"] [] [] []
    }
  ,
    { name   := "deliver_consumes_queue"
    , pre    := mkWorld [doc "a"] [doc "a"] [] []
    , action := .deliverFromQueue (doc "a")
    , post   := mkWorld [doc "a"] [] [] []
    }
  ,
    { name   := "rescan_on_empty"
    , pre    := w0
    , action := .rescanTick
    , post   := w0
    }
  ,
    { name   := "rescan_fills_queue"
    , pre    := mkWorld [doc "a"] [] [] []
    , action := .rescanTick
    , post   := mkWorld [doc "a"] [doc "a"] [] []
    }
  ,
    { name   := "rescan_skips_processed"
    , pre    := mkWorld [doc "a", doc "b"] [] [doc "a"] []
    , action := .rescanTick
    , post   := mkWorld [doc "a", doc "b"] [doc "b"] [doc "a"] []
    }
  ,
    { name   := "handle_legal_drains_queue"
    , pre    := mkWorld [doc "a"] [doc "a"] [] []
    , action := .handle (doc "a")
    , post   := mkWorld [doc "a"] [] [doc "a"] [doc "a"]
    }
  ,
    { name   := "handle_marks_processed"
    , pre    := mkWorld [doc "a", doc "b"] [doc "a", doc "b"] [] []
    , action := .handle (doc "a")
    , post   := mkWorld [doc "a", doc "b"] [doc "b"] [doc "a"] [doc "a"]
    }
  ,
    { name   := "enqueue_twice_multiset"
    , pre    := mkWorld [doc "a"] [doc "a"] [] []
    , action := .enqueue (doc "a")
    , post   := mkWorld [doc "a"] [doc "a", doc "a"] [] []
    }
  ,
    { name   := "rescan_prepends_to_queue"
    , pre    := mkWorld [doc "a"] [doc "z"] [] []
    , action := .rescanTick
    , post   := mkWorld [doc "a"] [doc "a", doc "z"] [] []
    }
  ]

def transitionCaseCount : Nat := transitionCases.length

structure SourceInstanceRow where
  name             : String
  dedupePolicy     : String
  rescanBoundedBy  : Nat
  deviation        : Option String

def sourceInstances : List SourceInstanceRow :=
  [ { name := "Watcher"
    , dedupePolicy := DedupePolicy.toContract .ttlCooldown
    , rescanBoundedBy := 1
    , deviation := none
    }
  , { name := "EventSource"
    , dedupePolicy := DedupePolicy.toContract .monotoneOnce
    , rescanBoundedBy := EventSource.eventSourceSrc.rescanBoundedBy
    , deviation := none
    }
  , { name := "SubagentSource"
    , dedupePolicy := DedupePolicy.toContract .monotoneOnce
    , rescanBoundedBy := SubagentSource.subagentSourceSrc.rescanBoundedBy
    , deviation := none
    }
  ]

def sourceInstanceCount : Nat := sourceInstances.length

structure ConvergenceTraceRow where
  name           : String
  instanceName   : String
  initialWorld   : World
  actions        : List Action
  finalWorld     : World
  status         : String

def watcherTrace : ConvergenceTraceRow :=
  { name := "watcher_persist_rescan_handle"
  , instanceName := "Watcher"
  , initialWorld := World.empty
  , actions :=
      [ .persist (doc "req-1")
      , .rescanTick
      , .handle (doc "req-1") ]
  , finalWorld := mkWorld [doc "req-1"] [] [doc "req-1"] [doc "req-1"]
  , status := "substantive"
  }

def eventSourceTrace : ConvergenceTraceRow :=
  { name := "event_source_persist_rescan_handle"
  , instanceName := "EventSource"
  , initialWorld := World.empty
  , actions :=
      [ .persist (doc "doc-1")
      , .rescanTick
      , .handle (doc "doc-1") ]
  , finalWorld := mkWorld [doc "doc-1"] [] [doc "doc-1"] [doc "doc-1"]
  , status := "substantive"
  }

def subagentSourceTrace : ConvergenceTraceRow :=
  { name := "subagent_orphan_rescan_handle"
  , instanceName := "SubagentSource"
  , initialWorld := World.empty
  , actions :=
      [ .persist (doc "tool-call-1")
      , .rescanTick
      , .handle (doc "tool-call-1") ]
  , finalWorld := mkWorld [doc "tool-call-1"] [] [doc "tool-call-1"] [doc "tool-call-1"]
  , status := "substantive"
  }

def droppedWakeRecoveryTrace : ConvergenceTraceRow :=
  { name := "subscription_drop_rescan_handle"
  , instanceName := "SubagentSource"
  , initialWorld := mkWorld [doc "doc-lossy"] [doc "doc-lossy"] [] []
  , actions :=
      [ .drop (doc "doc-lossy")
      , .rescanTick
      , .handle (doc "doc-lossy") ]
  , finalWorld :=
      mkWorld [doc "doc-lossy"] [] [doc "doc-lossy"] [doc "doc-lossy"]
  , status := "substantive"
  }

def convergenceTraces : List ConvergenceTraceRow :=
  [ watcherTrace, eventSourceTrace, subagentSourceTrace, droppedWakeRecoveryTrace ]

def convergenceTraceCount : Nat := convergenceTraces.length

def jsonOptionString : Option String → String := jsonOptionalString

def docIdJson (d : DocId) : String := jsonString d.raw

def docIdListJson (ds : List DocId) : String :=
  jsonArray (ds.map docIdJson)

def worldJson (w : World) : String :=
  "{"
    ++ "\"persistent_set\":" ++ docIdListJson w.persistentSet ++ ","
    ++ "\"subscription_queue\":" ++ docIdListJson w.subscriptionQueue ++ ","
    ++ "\"processed_set\":" ++ docIdListJson w.processedSet ++ ","
    ++ "\"handled\":" ++ docIdListJson w.handled
    ++ "}"

def actionJson : Action → String
  | .persist d => "{\"kind\":\"persist\",\"doc\":" ++ docIdJson d ++ "}"
  | .depersist d => "{\"kind\":\"depersist\",\"doc\":" ++ docIdJson d ++ "}"
  | .enqueue d => "{\"kind\":\"enqueue\",\"doc\":" ++ docIdJson d ++ "}"
  | .drop d => "{\"kind\":\"drop\",\"doc\":" ++ docIdJson d ++ "}"
  | .deliverFromQueue d =>
      "{\"kind\":\"deliver_from_queue\",\"doc\":" ++ docIdJson d ++ "}"
  | .rescanTick => "{\"kind\":\"rescan_tick\"}"
  | .handle d => "{\"kind\":\"handle\",\"doc\":" ++ docIdJson d ++ "}"

def transitionCaseJson (c : TransitionCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString c.name ++ ","
    ++ "\"pre\":" ++ worldJson c.pre ++ ","
    ++ "\"action\":" ++ actionJson c.action ++ ","
    ++ "\"post\":" ++ worldJson c.post
    ++ "}"

def transitionCasesJson : String :=
  jsonArray (transitionCases.map transitionCaseJson)

def sourceInstanceRowJson (r : SourceInstanceRow) : String :=
  "{"
    ++ "\"name\":" ++ jsonString r.name ++ ","
    ++ "\"dedupe_policy\":" ++ jsonString r.dedupePolicy ++ ","
    ++ "\"rescan_bounded_by\":" ++ toString r.rescanBoundedBy ++ ","
    ++ "\"deviation\":" ++ jsonOptionString r.deviation
    ++ "}"

def sourceInstancesJson : String :=
  jsonArray (sourceInstances.map sourceInstanceRowJson)

def convergenceTraceRowJson (r : ConvergenceTraceRow) : String :=
  "{"
    ++ "\"name\":" ++ jsonString r.name ++ ","
    ++ "\"instance_name\":" ++ jsonString r.instanceName ++ ","
    ++ "\"initial_world\":" ++ worldJson r.initialWorld ++ ","
    ++ "\"actions\":" ++ jsonArray (r.actions.map actionJson) ++ ","
    ++ "\"final_world\":" ++ worldJson r.finalWorld ++ ","
    ++ "\"status\":" ++ jsonString r.status
    ++ "}"

def convergenceTracesJson : String :=
  jsonArray (convergenceTraces.map convergenceTraceRowJson)

/-! Durable EventSource activation/admission witnesses. -/

open _root_.EventDelivery.DurableAdmission

structure DurableAdmissionCase where
  name : String
  operation : String
  disposition : String
  activationTwins : Nat
  deliveryTwins : Nat
  baselineContainsSource : Bool
  triggerCid : Nat
  sourceCid : Nat
  durableActivations : Nat
  durableDeliveries : Nat
  deriving Repr

private def exact (docId cid signerDid : Nat) : ExactRef :=
  { docId, compositeCommitCid := cid, signerDid, signatureValid := true }

private def activationKey : ActivationKey :=
  { triggerDocId := 10, triggerCommitCid := 2000, sourceCollection := 20, eventKind := 30 }

private def baselineSource := exact 100 1000 7
private def triggerV1 := exact 10 2000 8
private def triggerV2 := exact 10 2001 8
private def activation : ActivationFact :=
  { key := activationKey, trigger := triggerV1, baseline := [baselineSource] }
private def activationV2 : ActivationFact :=
  { key := { activationKey with triggerCommitCid := 2001 }
  , trigger := triggerV2
  , baseline := [baselineSource] }
private def activationRef := exact 50 5000 8

private def deliveryKey (sourceDocId : Nat) : DeliveryKey :=
  { triggerDocId := 10, sourceCollection := 20, sourceDocId, eventKind := 30 }

private def delivery (source : ExactRef) (trigger := triggerV1) : DeliveryFact :=
  { key := deliveryKey source.docId, requestId := 9000, activation := activationRef, trigger, source }

private def newSource := exact 101 1001 7
private def desired := delivery newSource

private def dispositionString : Disposition → String
  | .activated => "activated"
  | .baselined => "baselined"
  | .admitted => "admitted"
  | .idempotent => "idempotent"
  | .alreadyDelivered => "already_delivered"
  | .recoveringRequest => "recovering_request"
  | .recoveredAdmission => "recovered_admission"
  | .rejected => "rejected"

private def activationCase
    (name : String) (twins : Nat) (observation : ActivationObservation) :
    DurableAdmissionCase :=
  { name
  , operation := "activate"
  , disposition := dispositionString observation.disposition
  , activationTwins := twins
  , deliveryTwins := 0
  , baselineContainsSource := false
  , triggerCid := activation.trigger.compositeCommitCid
  , sourceCid := 0
  , durableActivations := observation.facts.length
  , durableDeliveries := 0 }

private def deliveryCase
    (name : String) (deliveryTwins : Nat) (trigger source : ExactRef)
    (baseline : Bool) (observation : DeliveryObservation) : DurableAdmissionCase :=
  { name
  , operation := "admit"
  , disposition := dispositionString observation.disposition
  , activationTwins := 1
  , deliveryTwins
  , baselineContainsSource := baseline
  , triggerCid := trigger.compositeCommitCid
  , sourceCid := source.compositeCommitCid
  , durableActivations := 1
  , durableDeliveries := observation.facts.length }

private def unsignedActivation : ActivationFact :=
  { activation with trigger := { triggerV1 with signatureValid := false } }
private def changedTriggerDelivery := delivery newSource triggerV2
private def updatedSourceDelivery := delivery { newSource with compositeCommitCid := 1002 }
private def unsignedSourceDelivery :=
  delivery { newSource with signatureValid := false }

def durableAdmissionCases : List DurableAdmissionCase :=
  [ activationCase "activation_baselines_current_docs" 0
      (activate [] activation)
  , activationCase "activation_replay_is_idempotent" 1
      (activate [activation] activation)
  , activationCase "activation_twins_fail_closed" 2
      (activate [activation, { activation with trigger := triggerV2 }] activation)
  , activationCase "config_edit_gets_new_activation_snapshot" 0
      (activate [] activationV2)
  , activationCase "unsigned_activation_rejected" 0
      (activate [] unsignedActivation)
  , deliveryCase "baseline_doc_is_not_fired" 0 triggerV1 baselineSource true
      (admit [activation] [] [] (delivery baselineSource))
  , deliveryCase "offline_creation_is_admitted_by_rescan" 0 triggerV1 newSource false
      (admit [activation] [] [] desired)
  , deliveryCase "dropped_wake_is_admitted_by_rescan" 0 triggerV1 newSource false
      (admit [activation] [] [] desired)
  , deliveryCase "exact_delivery_replay_is_idempotent" 1 triggerV1 newSource false
      (admit [activation] [desired] [desired.requestId] desired)
  , deliveryCase "same_trigger_doc_config_change_does_not_readmit" 1 triggerV2 newSource false
      (admit [activation] [desired] [desired.requestId] changedTriggerDelivery)
  , deliveryCase "source_update_does_not_reclassify_created" 1 triggerV1 newSource false
      (admit [activation] [desired] [desired.requestId] updatedSourceDelivery)
  , deliveryCase "delivery_twins_fail_closed" 2 triggerV1 newSource false
      (admit [activation] [desired, changedTriggerDelivery] [] desired)
  , deliveryCase "unsigned_source_rejected" 0 triggerV1 newSource false
      (admit [activation] [] [] unsignedSourceDelivery)
  , deliveryCase "admission_durable_request_absent_recovers" 1 triggerV1 newSource false
      (admit [activation] [desired] [] desired)
  , deliveryCase "request_durable_admission_absent_recovers" 0 triggerV1 newSource false
      (admit [activation] [] [desired.requestId] desired) ]

def durableAdmissionCaseJson (row : DurableAdmissionCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString row.name ++ ","
    ++ "\"operation\":" ++ jsonString row.operation ++ ","
    ++ "\"disposition\":" ++ jsonString row.disposition ++ ","
    ++ "\"activation_twins\":" ++ toString row.activationTwins ++ ","
    ++ "\"delivery_twins\":" ++ toString row.deliveryTwins ++ ","
    ++ "\"baseline_contains_source\":"
      ++ (if row.baselineContainsSource then "true" else "false") ++ ","
    ++ "\"trigger_cid\":" ++ toString row.triggerCid ++ ","
    ++ "\"source_cid\":" ++ toString row.sourceCid ++ ","
    ++ "\"durable_activations\":" ++ toString row.durableActivations ++ ","
    ++ "\"durable_deliveries\":" ++ toString row.durableDeliveries
    ++ "}"

def durableAdmissionCasesJson : String :=
  jsonArray (durableAdmissionCases.map durableAdmissionCaseJson)

end Conformance.EventDelivery
