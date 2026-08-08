import Proofs.ApplyReconcile.Collections
import Proofs.Conformance.ContractTypes

namespace ApplyReconcile.ContractCases

open Conformance.Contracts

structure ContractDoc where
  ref : DocRef
  content : String
  refs : List DocRef

structure ContractLiveDoc where
  ref : DocRef
  content : String

structure ContractStep where
  action : String
  target : DocRef
  content : String
  refs : List DocRef

structure ContractCollectionWrite where
  collection : Collection
  graphqlType : String
  uniqueField : String
  applyOrder : Nat

structure ContractSelectedDoc where
  action : String
  target : DocRef
  graphqlType : String
  uniqueField : String
  uniqueValue : String
  content : String
  refs : List DocRef

structure ApplyReconcileScenario where
  name : String
  manifest : List ContractDoc
  preDesired : List ContractDoc
  preLive : List ContractLiveDoc
  pruneMode : Bool
  prefixLen : Nat

structure ApplyReconcileCase where
  name : String
  pruneMode : Bool
  manifest : List ContractDoc
  preDesired : List ContractDoc
  preLive : List ContractLiveDoc
  expectedExternalStateAfterAbort : List ContractLiveDoc
  expectedCreate : List DocRef
  expectedUpdate : List DocRef
  expectedDelete : List DocRef
  expectedUnchanged : List DocRef
  expectedLiveOnly : List DocRef
  expectedSteps : List ContractStep
  expectedWriteOrder : List ContractCollectionWrite
  expectedPruneOrder : List ContractCollectionWrite
  expectedSelectedCreateDocs : List ContractSelectedDoc
  expectedSelectedUpdateDocs : List ContractSelectedDoc
  expectedSelectedDeleteDocs : List ContractSelectedDoc
  expectedSelectedWrites : List ContractSelectedDoc
  prefixLen : Nat
  expectedPrefixDesired : List ContractDoc
  expectedAfterDesired : List ContractDoc
  expectedRetryDesired : List ContractDoc
  expectedRetryStepCount : Nat
  expectedRediffStepCount : Nat
  livePreserved : Bool
  manifestRealizedAfter : Bool
  retryConverges : Bool
  idempotentAfter : Bool
  writeOrderPrefixSafe : Bool
  pruneOrderReferrersBeforeDependencies : Bool
  productionPrefixesReferrersClosed : Bool
  prefixReferrersClosed : Bool
  desiredReferencesClosedAfterPrefix : Bool
  deleteSafetyHolds : Bool

def boolString (value : Bool) : String :=
  if value then "true" else "false"

def collectionName : Collection → String
  | .agentPrincipal => "AgentPrincipal"
  | .agentBehavior => "AgentBehavior"
  | .skill => "Skill"
  | .datastoreToolSurface => "DatastoreToolSurface"
  | .toolSelection => "ToolSelection"
  | .inferenceBackend => "InferenceBackend"
  | .inferenceProfile => "InferenceProfile"
  | .toolServiceRegistry => "ToolServiceRegistry"
  | .projectionAcpBinding => "ProjectionAcpBinding"
  | .peerPairingDesired => "PeerPairingDesired"
  | .task => "Task"
  | .schedule => "Schedule"
  | .eventTrigger => "EventTrigger"

def collectionUniqueField : Collection → String
  | .agentPrincipal => "agent_did"
  | .agentBehavior => "behavior_id"
  | .skill => "skill_id"
  | .datastoreToolSurface => "surface_id"
  | .toolSelection => "selection_id"
  | .inferenceBackend => "backend_id"
  | .inferenceProfile => "profile_id"
  | .toolServiceRegistry => "service_id"
  | .projectionAcpBinding => "binding_id"
  | .peerPairingDesired => "peer_id"
  | .task => "task_id"
  | .schedule => "schedule_id"
  | .eventTrigger => "trigger_id"

-- Mirrors crates/gents-cli CONFIG_APPLY_ORDER: surfaces before tool selections.
def productionWriteOrder : List Collection :=
  [ .peerPairingDesired
  , .inferenceBackend
  , .inferenceProfile
  , .toolServiceRegistry
  , .datastoreToolSurface
  , .toolSelection
  , .skill
  , .agentBehavior
  , .projectionAcpBinding
  , .task
  , .schedule
  , .eventTrigger
  , .agentPrincipal
  ]

def productionPruneOrder : List Collection :=
  productionWriteOrder.reverse

def collectionWriteProjection (collection : Collection) : ContractCollectionWrite :=
  { collection := collection
  , graphqlType := collectionName collection
  , uniqueField := collectionUniqueField collection
  , applyOrder := collection.applyOrder
  }

def collectionBEq (a b : Collection) : Bool :=
  if a = b then true else false

def docRefBEq (a b : DocRef) : Bool :=
  if a = b then true else false

def docRefLt (a b : DocRef) : Bool :=
  if a.collection.applyOrder < b.collection.applyOrder then true
  else if b.collection.applyOrder < a.collection.applyOrder then false
  else a.id < b.id

def docRefLe (a b : DocRef) : Bool :=
  docRefLt a b || docRefBEq a b

def sortedDocRefs (refs : List DocRef) : List DocRef :=
  refs.mergeSort docRefLe

def docRefsEq (left right : List DocRef) : Bool :=
  let left := sortedDocRefs left
  let right := sortedDocRefs right
  left.length == right.length &&
    ((left.zip right).all (fun pair => docRefBEq pair.fst pair.snd))

def desiredDocEq (left right : ContractDoc) : Bool :=
  left.content == right.content && docRefsEq left.refs right.refs

def lookupDoc? (docs : List ContractDoc) (ref : DocRef) : Option ContractDoc :=
  docs.find? (fun doc => docRefBEq doc.ref ref)

def containsDoc (docs : List ContractDoc) (ref : DocRef) : Bool :=
  (lookupDoc? docs ref).isSome

def contractDocLe (a b : ContractDoc) : Bool :=
  docRefLe a.ref b.ref

def contractStepLe (a b : ContractStep) : Bool :=
  docRefLe a.target b.target

def sortedDocs (docs : List ContractDoc) : List ContractDoc :=
  docs.mergeSort contractDocLe

def sortedSteps (steps : List ContractStep) : List ContractStep :=
  steps.mergeSort contractStepLe

def productionOrderedSteps (steps : List ContractStep) : List ContractStep :=
  productionWriteOrder.flatMap fun collection =>
    steps.filter fun step => collectionBEq step.target.collection collection

def productionPruneOrderedSteps (steps : List ContractStep) : List ContractStep :=
  productionPruneOrder.flatMap fun collection =>
    steps.filter fun step => collectionBEq step.target.collection collection

end ApplyReconcile.ContractCases
