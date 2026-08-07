import Proofs.Conformance.Contracts.Json.Helpers
import Proofs.Conformance.ContractCases.AggregateBudget

namespace Conformance.Contracts

open Conformance.ContractCases

private def optionalNatJson : Option Nat → String
  | none => "null"
  | some value => toString value

def aggregateTokenBudgetCaseJson (witness : AggregateTokenBudgetCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString witness.name ++ ","
    ++ "\"limit\":" ++ toString witness.limit ++ ","
    ++ "\"used\":" ++ toString witness.used ++ ","
    ++ "\"input_tokens\":" ++ toString witness.inputTokens ++ ","
    ++ "\"configured_max_output_tokens\":"
      ++ toString witness.configuredMaxOutputTokens ++ ","
    ++ "\"reported_input_tokens\":" ++ toString witness.reportedInputTokens ++ ","
    ++ "\"reported_output_tokens\":" ++ toString witness.reportedOutputTokens ++ ","
    ++ "\"reported_total_tokens\":" ++ toString witness.reportedTotalTokens ++ ","
    ++ "\"usage_present\":" ++ boolString witness.usagePresent ++ ","
    ++ "\"terminal_valid\":" ++ boolString witness.terminalValid ++ ","
    ++ "\"effective_output_tokens\":" ++ toString witness.effectiveOutputTokens ++ ","
    ++ "\"can_dispatch\":" ++ boolString witness.canDispatch ++ ","
    ++ "\"charged_tokens\":" ++ toString witness.chargedTokens ++ ","
    ++ "\"charge_result\":" ++ jsonString witness.chargeResult ++ ","
    ++ "\"next_used\":" ++ optionalNatJson witness.nextUsed
    ++ ",\"post_charge_action\":" ++ jsonString witness.postChargeAction
    ++ "}"

def aggregateTokenBudgetCasesJson : String :=
  jsonArray (aggregateTokenBudgetCases.map aggregateTokenBudgetCaseJson)

end Conformance.Contracts
