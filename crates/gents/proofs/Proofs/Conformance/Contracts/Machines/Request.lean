import Proofs.Request
import Proofs.Conformance.ContractTypes
import Proofs.Conformance.ContractCases.Types

namespace Conformance.Contracts

open Conformance.ContractCases

def requestStates : List RequestState :=
  [ .pending, .claimed, .processing, .inputRequired, .completed
  , .failed, .superseded, .dead, .interrupted ]

def requestStateNames : List String :=
  requestStates.map RequestState.toDefraDB

def requestActions : List (String × RequestContext.Action) :=
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

def requestContext
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

def requestSamples : List RequestContext :=
  [ requestContext .pending .released
  , requestContext .pending .released true
  , requestContext .pending .released false (some 0) 1
  , requestContext .claimed .waiting
  , requestContext .claimed .acquired
  , requestContext .claimed .waiting true
  , requestContext .claimed .acquired true
  , requestContext .processing .executing
  , requestContext .processing .executing true
  , requestContext .inputRequired .executing
  , requestContext .completed .released
  , requestContext .failed .released
  , requestContext .superseded .released
  , requestContext .dead .released
  , requestContext .interrupted .released
  ]

def requestMachine : StateMachineContract :=
  machineContract
    "Request"
    requestStateNames
    (terminalNames requestStates RequestState.toDefraDB)
    (actionNames requestActions)
    (transitionPairsFromSamples
      requestSamples
      requestActions
      RequestContext.step?
      (fun ctx => ctx.state.toDefraDB))

end Conformance.Contracts
