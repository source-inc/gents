import Proofs.Conformance.ContractCases.ResponseOutcome
import Proofs.Conformance.Contracts.Json.Core

namespace Conformance.Contracts

open Conformance.ContractCases

def responseOutcomeCaseJson (row : ResponseOutcomeCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString row.name ++ ","
    ++ "\"kind\":" ++ jsonString row.kind ++ ","
    ++ "\"has_final_message\":" ++ toString row.hasFinalMessage ++ ","
    ++ "\"final_message_role\":" ++ jsonOptionalString row.finalMessageRole ++ ","
    ++ "\"request_doc_id\":" ++ toString row.requestDocId ++ ","
    ++ "\"request_cid\":" ++ toString row.requestCid ++ ","
    ++ "\"final_message_doc_id\":" ++ jsonOptionalNat row.finalMessageDocId ++ ","
    ++ "\"final_message_cid\":" ++ jsonOptionalNat row.finalMessageCid ++ ","
    ++ "\"final_message_signer_did\":" ++ jsonOptionalNat row.finalMessageSignerDid ++ ","
    ++ "\"visible_sibling_count\":" ++ toString row.visibleSiblingCount ++ ","
    ++ "\"publish_outcome\":" ++ jsonString row.publishOutcome ++ ","
    ++ "\"resulting_fact_count\":" ++ toString row.resultingFactCount
    ++ "}"

def responseOutcomeCasesJson : String :=
  jsonArray (responseOutcomeCases.map responseOutcomeCaseJson)

def responsePersistenceCutCaseJson (row : ResponsePersistenceCutCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString row.name ++ ","
    ++ "\"pre_cut\":" ++ jsonString row.preCut ++ ","
    ++ "\"post_cut\":" ++ jsonString row.postCut ++ ","
    ++ "\"request_terminal\":" ++ toString row.requestTerminal ++ ","
    ++ "\"live_stage\":" ++ jsonString row.liveStage ++ ","
    ++ "\"outcome_count\":" ++ toString row.outcomeCount
    ++ "}"

def responsePersistenceCutCasesJson : String :=
  jsonArray (responsePersistenceCutCases.map responsePersistenceCutCaseJson)

def responseRecoveryCutCaseJson (row : ResponseRecoveryCutCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString row.name ++ ","
    ++ "\"response_present\":" ++ toString row.responsePresent ++ ","
    ++ "\"source_cid\":" ++ toString row.sourceCid ++ ","
    ++ "\"claim_cid\":" ++ toString row.claimCid ++ ","
    ++ "\"claim_parent_cid\":" ++ toString row.claimParentCid ++ ","
    ++ "\"provenance_reconstructed\":" ++ toString row.provenanceReconstructed ++ ","
    ++ "\"publish_outcome\":" ++ jsonString row.publishOutcome ++ ","
    ++ "\"terminalized_at_source\":" ++ jsonString row.terminalizedAtSource ++ ","
    ++ "\"request_terminal\":" ++ toString row.requestTerminal ++ ","
    ++ "\"outcome_count\":" ++ toString row.outcomeCount
    ++ "}"

def responseRecoveryCutCasesJson : String :=
  jsonArray (responseRecoveryCutCases.map responseRecoveryCutCaseJson)

end Conformance.Contracts
