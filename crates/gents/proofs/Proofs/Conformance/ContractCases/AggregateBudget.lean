import Proofs.PromptAssembly.AggregateBudget

namespace Conformance.ContractCases

open PromptAssembly.AggregateBudget

structure AggregateTokenBudgetCase where
  name : String
  limit : Nat
  used : Nat
  inputTokens : Nat
  configuredMaxOutputTokens : Nat
  reportedInputTokens : Nat
  reportedOutputTokens : Nat
  reportedTotalTokens : Nat
  usagePresent : Bool
  terminalValid : Bool
  effectiveOutputTokens : Nat
  canDispatch : Bool
  chargedTokens : Nat
  chargeResult : String
  nextUsed : Option Nat
  postChargeAction : String
  deriving Repr

private structure AggregateTokenBudgetWitness where
  name : String
  ledger : Ledger
  inputTokens : Nat
  configuredMaxOutputTokens : Nat
  usage : Option Usage
  terminalValid : Bool

private def chargeResultName : ChargeResult → String
  | .missing => "missing"
  | .within _ => "within"
  | .exhausted _ => "exhausted"
  | .overrun _ => "overrun"

private def chargeResultLedger : ChargeResult → Option Ledger
  | .missing => none
  | .within ledger => some ledger
  | .exhausted ledger => some ledger
  | .overrun ledger => some ledger

private def postChargeActionName : PostChargeAction → String
  | .continue => "continue"
  | .succeed => "succeed"
  | .fail => "fail"

private def witnesses : List AggregateTokenBudgetWitness :=
  [ { name := "first-dispatch-clamps-to-total-budget"
    , ledger := { limit := 1000, used := 0 }
    , inputTokens := 200
    , configuredMaxOutputTokens := 900
    , terminalValid := false
    , usage := some { inputTokens := 200, outputTokens := 300, reportedTotal := 500 } }
  , { name := "tool-turn-uses-request-wide-remainder"
    , ledger := { limit := 1000, used := 500 }
    , inputTokens := 300
    , configuredMaxOutputTokens := 900
    , terminalValid := false
    , usage := some { inputTokens := 300, outputTokens := 199, reportedTotal := 499 } }
  , { name := "retracted-attempt-still-reduces-retry-budget"
    , ledger := { limit := 1000, used := 500 }
    , inputTokens := 100
    , configuredMaxOutputTokens := 900
    , terminalValid := false
    , usage := some { inputTokens := 100, outputTokens := 400, reportedTotal := 500 } }
  , { name := "exhausted-ledger-blocks-next-dispatch"
    , ledger := { limit := 1000, used := 1000 }
    , inputTokens := 1
    , configuredMaxOutputTokens := 900
    , terminalValid := false
    , usage := some { inputTokens := 1, outputTokens := 1, reportedTotal := 2 } }
  , { name := "input-consumes-remaining-budget"
    , ledger := { limit := 1000, used := 700 }
    , inputTokens := 300
    , configuredMaxOutputTokens := 900
    , terminalValid := false
    , usage := none }
  , { name := "missing-usage-fails-closed"
    , ledger := { limit := 1000, used := 100 }
    , inputTokens := 100
    , configuredMaxOutputTokens := 900
    , terminalValid := false
    , usage := none }
  , { name := "all-zero-usage-fails-closed"
    , ledger := { limit := 1000, used := 100 }
    , inputTokens := 100
    , configuredMaxOutputTokens := 900
    , terminalValid := false
    , usage := some { inputTokens := 0, outputTokens := 0, reportedTotal := 0 } }
  , { name := "inconsistent-total-cannot-undercharge"
    , ledger := { limit := 1000, used := 100 }
    , inputTokens := 100
    , configuredMaxOutputTokens := 900
    , terminalValid := false
    , usage := some { inputTokens := 300, outputTokens := 200, reportedTotal := 400 } }
  , { name := "exact-limit-is-exhausted"
    , ledger := { limit := 1000, used := 500 }
    , inputTokens := 100
    , configuredMaxOutputTokens := 900
    , terminalValid := true
    , usage := some { inputTokens := 100, outputTokens := 400, reportedTotal := 500 } }
  , { name := "observed-overrun-fails-closed"
    , ledger := { limit := 1000, used := 800 }
    , inputTokens := 100
    , configuredMaxOutputTokens := 900
    , terminalValid := false
    , usage := some { inputTokens := 150, outputTokens := 100, reportedTotal := 250 } }
  ]

private def toCase (witness : AggregateTokenBudgetWitness) : AggregateTokenBudgetCase :=
  let report := witness.usage.getD
    { inputTokens := 0, outputTokens := 0, reportedTotal := 0 }
  let result := chargeReported witness.ledger witness.usage
  { name := witness.name
  , limit := witness.ledger.limit
  , used := witness.ledger.used
  , inputTokens := witness.inputTokens
  , configuredMaxOutputTokens := witness.configuredMaxOutputTokens
  , reportedInputTokens := report.inputTokens
  , reportedOutputTokens := report.outputTokens
  , reportedTotalTokens := report.reportedTotal
  , usagePresent := witness.usage.isSome
  , terminalValid := witness.terminalValid
  , effectiveOutputTokens := effectiveOutputBudget witness.ledger witness.inputTokens
      witness.configuredMaxOutputTokens
  , canDispatch := decide (CanDispatch witness.ledger witness.inputTokens
      witness.configuredMaxOutputTokens)
  , chargedTokens := report.chargedTotal
  , chargeResult := chargeResultName result
  , nextUsed := (chargeResultLedger result).map Ledger.used
  , postChargeAction := postChargeActionName (postChargeAction result witness.terminalValid) }

def aggregateTokenBudgetCases : List AggregateTokenBudgetCase :=
  witnesses.map toCase

end Conformance.ContractCases
