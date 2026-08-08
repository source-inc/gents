/-!
Durable EventSource activation and delivery admission.

Subscriptions are wake hints.  Correctness comes from complete source scans,
an immutable activation baseline, and immutable per-source-document delivery
facts.  Dedupe keys use the physical trigger document id so bookkeeping or
configuration versions of that same document cannot re-admit a source doc.
-/

namespace EventDelivery.DurableAdmission

structure ExactRef where
  docId : Nat
  compositeCommitCid : Nat
  signerDid : Nat
  signatureValid : Bool
  deriving BEq, DecidableEq, Repr

def ExactRef.valid (ref : ExactRef) : Bool :=
  ref.docId != 0 && ref.compositeCommitCid != 0 && ref.signerDid != 0
    && ref.signatureValid

structure ActivationKey where
  triggerDocId : Nat
  triggerCommitCid : Nat
  sourceCollection : Nat
  eventKind : Nat
  deriving BEq, DecidableEq, Repr

structure ActivationFact where
  key : ActivationKey
  trigger : ExactRef
  baseline : List ExactRef
  deriving BEq, DecidableEq, Repr

def ActivationFact.valid (fact : ActivationFact) : Bool :=
  fact.key.triggerDocId == fact.trigger.docId
    && fact.key.triggerCommitCid == fact.trigger.compositeCommitCid
    && fact.key.sourceCollection != 0
    && fact.key.eventKind != 0
    && fact.trigger.valid
    && fact.baseline.all ExactRef.valid

structure DeliveryKey where
  triggerDocId : Nat
  sourceCollection : Nat
  sourceDocId : Nat
  eventKind : Nat
  deriving BEq, DecidableEq, Repr

structure DeliveryFact where
  key : DeliveryKey
  requestId : Nat
  activation : ExactRef
  trigger : ExactRef
  source : ExactRef
  deriving BEq, DecidableEq, Repr

def DeliveryFact.valid (fact : DeliveryFact) : Bool :=
  fact.key.triggerDocId == fact.trigger.docId
    && fact.key.sourceDocId == fact.source.docId
    && fact.key.sourceCollection != 0
    && fact.key.eventKind != 0
    && fact.requestId != 0
    && fact.activation.valid
    && fact.trigger.valid
    && fact.source.valid

inductive Disposition where
  | activated
  | baselined
  | admitted
  | idempotent
  | alreadyDelivered
  | recoveringRequest
  | recoveredAdmission
  | rejected
  deriving BEq, DecidableEq, Repr

structure ActivationObservation where
  facts : List ActivationFact
  disposition : Disposition
  deriving BEq, DecidableEq, Repr

/-- Create-and-compare over every visible activation candidate. -/
def activate (visible : List ActivationFact) (desired : ActivationFact) : ActivationObservation :=
  if desired.valid then
    match visible with
    | [] => { facts := [desired], disposition := .activated }
    | [current] =>
        if current = desired then
          { facts := visible, disposition := .idempotent }
        else
          { facts := visible, disposition := .rejected }
    | _ => { facts := visible, disposition := .rejected }
  else
    { facts := visible, disposition := .rejected }

structure DeliveryObservation where
  facts : List DeliveryFact
  disposition : Disposition
  deriving BEq, DecidableEq, Repr

def baselineContains (activation : ActivationFact) (sourceDocId : Nat) : Bool :=
  activation.baseline.any (fun source => source.docId == sourceDocId)

/--
Reconcile one exact current source document.  A sole valid fact for the same
physical delivery key suppresses replay even if the trigger CID changed; the
stored fact continues to name the exact version used for the original admit.
-/
def admit
    (activationCandidates : List ActivationFact)
    (deliveryCandidates : List DeliveryFact)
    (materializedRequestIds : List Nat)
    (desired : DeliveryFact) : DeliveryObservation :=
  match activationCandidates with
  | [activation] =>
      if !activation.valid || activation.key.triggerDocId != desired.key.triggerDocId
          || activation.key.sourceCollection != desired.key.sourceCollection
          || activation.key.eventKind != desired.key.eventKind then
        { facts := deliveryCandidates, disposition := .rejected }
      else if baselineContains activation desired.key.sourceDocId then
        { facts := deliveryCandidates, disposition := .baselined }
      else if !desired.valid then
        { facts := deliveryCandidates, disposition := .rejected }
      else
        match deliveryCandidates with
        | [] =>
            if materializedRequestIds.contains desired.requestId then
              { facts := [desired], disposition := .recoveredAdmission }
            else
              { facts := [desired], disposition := .admitted }
        | [current] =>
            if !current.valid then
              { facts := deliveryCandidates, disposition := .rejected }
            else if current.key ≠ desired.key then
              { facts := deliveryCandidates, disposition := .rejected }
            else if !materializedRequestIds.contains current.requestId then
              { facts := deliveryCandidates, disposition := .recoveringRequest }
            else if current = desired then
              { facts := deliveryCandidates, disposition := .idempotent }
            else
              { facts := deliveryCandidates, disposition := .alreadyDelivered }
        | _ => { facts := deliveryCandidates, disposition := .rejected }
  | _ => { facts := deliveryCandidates, disposition := .rejected }

theorem activation_replay_idempotent
    (fact : ActivationFact) (valid : fact.valid = true) :
    (activate [fact] fact).disposition = .idempotent := by
  simp [activate, valid]

theorem activation_twins_fail_closed
    (left right desired : ActivationFact) (valid : desired.valid = true) :
    (activate [left, right] desired).disposition = .rejected := by
  simp [activate, valid]

theorem baseline_never_admitted
    (activation : ActivationFact) (desired : DeliveryFact)
    (valid : activation.valid = true)
    (key : activation.key.triggerDocId = desired.key.triggerDocId
      ∧ activation.key.sourceCollection = desired.key.sourceCollection
      ∧ activation.key.eventKind = desired.key.eventKind)
    (baseline : baselineContains activation desired.key.sourceDocId = true) :
    (admit [activation] [] [] desired).disposition = .baselined := by
  rcases key with ⟨trigger, collection, kind⟩
  simp [admit, valid, trigger, collection, kind, baseline]

theorem offline_source_without_fact_is_admitted
    (activation : ActivationFact) (desired : DeliveryFact)
    (activationValid : activation.valid = true)
    (desiredValid : desired.valid = true)
    (key : activation.key.triggerDocId = desired.key.triggerDocId
      ∧ activation.key.sourceCollection = desired.key.sourceCollection
      ∧ activation.key.eventKind = desired.key.eventKind)
    (notBaseline : baselineContains activation desired.key.sourceDocId = false) :
    (admit [activation] [] [] desired).disposition = .admitted := by
  rcases key with ⟨trigger, collection, kind⟩
  simp [admit, activationValid, desiredValid, trigger, collection, kind, notBaseline]

theorem same_physical_trigger_config_change_does_not_readmit
    (activation : ActivationFact) (current desired : DeliveryFact)
    (materializedRequestIds : List Nat)
    (activationValid : activation.valid = true)
    (currentValid : current.valid = true)
    (desiredValid : desired.valid = true)
    (sameKey : current.key = desired.key)
    (differentVersion : current ≠ desired)
    (activationKey : activation.key.triggerDocId = desired.key.triggerDocId
      ∧ activation.key.sourceCollection = desired.key.sourceCollection
      ∧ activation.key.eventKind = desired.key.eventKind)
    (notBaseline : baselineContains activation desired.key.sourceDocId = false)
    (requestPresent : current.requestId ∈ materializedRequestIds) :
    (admit [activation] [current] materializedRequestIds desired).disposition = .alreadyDelivered := by
  rcases activationKey with ⟨trigger, collection, kind⟩
  simp [admit, activationValid, currentValid, desiredValid, sameKey, requestPresent,
    differentVersion, trigger, collection, kind, notBaseline]

theorem delivery_twins_fail_closed
    (activation : ActivationFact) (left right desired : DeliveryFact)
    (activationValid : activation.valid = true)
    (activationKey : activation.key.triggerDocId = desired.key.triggerDocId
      ∧ activation.key.sourceCollection = desired.key.sourceCollection
      ∧ activation.key.eventKind = desired.key.eventKind)
    (notBaseline : baselineContains activation desired.key.sourceDocId = false) :
    (admit [activation] [left, right] [] desired).disposition = .rejected := by
  rcases activationKey with ⟨trigger, collection, kind⟩
  simp [admit, activationValid, trigger, collection, kind, notBaseline]

theorem admission_without_request_recovers
    (activation : ActivationFact) (desired : DeliveryFact)
    (activationValid : activation.valid = true)
    (desiredValid : desired.valid = true)
    (key : activation.key.triggerDocId = desired.key.triggerDocId
      ∧ activation.key.sourceCollection = desired.key.sourceCollection
      ∧ activation.key.eventKind = desired.key.eventKind)
    (notBaseline : baselineContains activation desired.key.sourceDocId = false) :
    (admit [activation] [desired] [] desired).disposition = .recoveringRequest := by
  rcases key with ⟨trigger, collection, kind⟩
  simp [admit, activationValid, desiredValid, trigger, collection, kind, notBaseline]

theorem request_without_admission_recovers
    (activation : ActivationFact) (desired : DeliveryFact)
    (activationValid : activation.valid = true)
    (desiredValid : desired.valid = true)
    (key : activation.key.triggerDocId = desired.key.triggerDocId
      ∧ activation.key.sourceCollection = desired.key.sourceCollection
      ∧ activation.key.eventKind = desired.key.eventKind)
    (notBaseline : baselineContains activation desired.key.sourceDocId = false) :
    (admit [activation] [] [desired.requestId] desired).disposition = .recoveredAdmission := by
  rcases key with ⟨trigger, collection, kind⟩
  simp [admit, activationValid, desiredValid, trigger, collection, kind, notBaseline]

end EventDelivery.DurableAdmission
