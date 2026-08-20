import Proofs.Request
import Proofs.Process
import Proofs.Conformance.Boundaries
import Proofs.Conformance.ContractCases.Types

namespace Conformance.ContractCases

inductive LifecycleTransitionClassification where
  | legal
  | illegal
  | productUnreachable
  | recoveryReachable
  deriving DecidableEq, Repr

namespace LifecycleTransitionClassification

def toContract : LifecycleTransitionClassification → String
  | .legal => "legal"
  | .illegal => "illegal"
  | .productUnreachable => "productUnreachable"
  | .recoveryReachable => "recoveryReachable"

end LifecycleTransitionClassification

def lifecycleTransitionCaseName (domain source target : String) : String :=
  domain ++ ":" ++ source ++ "->" ++ target

def actionForPairFromSamples {σ α : Type}
    (samples : List σ)
    (actions : List (String × α))
    (step : σ → α → Option σ)
    (stateName : σ → String)
    (source target : String) : Option String :=
  let candidates :=
    samples.flatMap fun pre =>
      actions.filterMap fun action =>
        match step pre action.snd with
        | some post =>
            if stateName pre = source ∧ stateName post = target then
              some action.fst
            else
              none
        | none => none
  candidates.head?

def requestTransitionStates : List RequestState :=
  [ .pending, .claimed, .processing, .inputRequired, .completed
  , .failed, .superseded, .dead, .interrupted ]

def requestTransitionActions : List (String × RequestContext.Action) :=
  [ ("claim", .claim)
  , ("dedupLose", .dedupLose)
  , ("admissionReject", .admissionReject)
  , ("beginInference", .beginInference)
  , ("advance", .advance)
  , ("finish", .finish)
  , ("fail", .fail)
  , ("failBeforeStream", .failBeforeStream)
  , ("expire", .expire)
  , ("interruptBeforeClaim", .interruptBeforeClaim)
  , ("interruptClaimed", .interruptClaimed)
  , ("interruptProcessing", .interruptProcessing)
  ]

def requestTransitionContext
    (state : RequestState)
    (admission : AdmissionState)
    (hasInterrupt : Bool := false)
    (validUntil : Option Time := none)
    (currentTime : Time := 0) : RequestContext :=
  { state := state
  , origin := .interactive
  , backend := contractBackend
  , admission := admission
  , deadline := 10
  , claimTime := 0
  , currentTime := currentTime
  , retryCount := 0
  , maxRetries := 3
  , progressSeq := 0
  , messageSeq := 0
  , isLatest := true
  , persistence := .uncommitted
  , interruptRequestedAt := if hasInterrupt then some currentTime else none
  , validUntil := validUntil
  }

def requestTransitionSamples : List RequestContext :=
  [ requestTransitionContext .pending .released
  , requestTransitionContext .pending .released true
  , requestTransitionContext .pending .released false (some 0) 1
  , requestTransitionContext .claimed .waiting
  , requestTransitionContext .claimed .acquired
  , requestTransitionContext .claimed .waiting true
  , requestTransitionContext .claimed .acquired true
  , requestTransitionContext .processing .executing
  , requestTransitionContext .processing .executing true
  , requestTransitionContext .inputRequired .executing
  , requestTransitionContext .completed .released
  , requestTransitionContext .failed .released
  , requestTransitionContext .superseded .released
  , requestTransitionContext .dead .released
  , requestTransitionContext .interrupted .released
  ]

def requestTransitionAction? (source target : String) : Option String :=
  actionForPairFromSamples
    requestTransitionSamples
    requestTransitionActions
    RequestContext.step?
    (fun ctx => ctx.state.toDefraDB)
    source
    target

/-- Request edges that no single `RequestContext.Action` takes, but that
registered recovery sweeps legitimately perform on persisted rows.

`claimed -> completed` is terminal repair finishing a claimed request whose
response document already landed; `claimed -> dead` and `processing -> dead` are
the subagent-liveness sweep terminalizing an expired child. The licensing models
live in `Proofs/Recovery/` — the request machine alone does not model them, so
publishing these as `illegal` made the emitted contract assert that Rust has no
writer for edges the product actually performs. -/
def requestRecoverySweepReachable : RequestState → RequestState → Bool
  | .claimed, .completed => true
  | .claimed, .dead => true
  | .processing, .dead => true
  | _, _ => false

def requestTransitionClassification
    (source target : RequestState)
    (action : Option String) : LifecycleTransitionClassification :=
  match action with
  | some _ => .legal
  | none =>
      if source = .inputRequired ∨ target = .inputRequired then
        .productUnreachable
      else if requestRecoverySweepReachable source target then
        .recoveryReachable
      else
        .illegal

def requestTransitionCase (source target : RequestState) : LifecycleTransitionCase :=
  let sourceName := source.toDefraDB
  let targetName := target.toDefraDB
  let action := requestTransitionAction? sourceName targetName
  let classification := requestTransitionClassification source target action
  { name := lifecycleTransitionCaseName "Request" sourceName targetName
  , domain := "Request"
  , fromState := sourceName
  , toState := targetName
  , classification := classification.toContract
  , action := action
  , boundary :=
      match classification with
      | .productUnreachable =>
          some Conformance.Contracts.boundaryRequestInputRequiredReservedId
      | .recoveryReachable =>
          some Conformance.Contracts.boundaryRequestRecoverySweepReachableId
      | _ => none
  }

def requestTransitionCases : List LifecycleTransitionCase :=
  requestTransitionStates.flatMap fun source =>
    requestTransitionStates.map fun target =>
      requestTransitionCase source target

def processTransitionStates : List ProcessState :=
  [ .uninitialized, .recovering, .ready, .shuttingDown, .shutdown ]

def processTransitionActions : List (String × ProcessState.Action) :=
  [ ("startupRecover", .startupRecover { hasStuckRequests := true, activeRequestCount := 1 })
  , ("startupClean", .startupClean { hasStuckRequests := false, activeRequestCount := 0 })
  , ("recoveryComplete", .recoveryComplete)
  , ("beginShutdown", .beginShutdown)
  , ("finishShutdown", .finishShutdown 0)
  ]

def processTransitionAction? (source target : String) : Option String :=
  actionForPairFromSamples
    processTransitionStates
    processTransitionActions
    ProcessState.step?
    ProcessState.toDefraDB
    source
    target

def processTransitionClassification
    (action : Option String) : LifecycleTransitionClassification :=
  match action with
  | some _ => .legal
  | none => .illegal

def processTransitionCase (source target : ProcessState) : LifecycleTransitionCase :=
  let sourceName := source.toDefraDB
  let targetName := target.toDefraDB
  let action := processTransitionAction? sourceName targetName
  let classification := processTransitionClassification action
  { name := lifecycleTransitionCaseName "Process" sourceName targetName
  , domain := "Process"
  , fromState := sourceName
  , toState := targetName
  , classification := classification.toContract
  , action := action
  , boundary := none
  }

def processTransitionCases : List LifecycleTransitionCase :=
  processTransitionStates.flatMap fun source =>
    processTransitionStates.map fun target =>
      processTransitionCase source target

end Conformance.ContractCases
