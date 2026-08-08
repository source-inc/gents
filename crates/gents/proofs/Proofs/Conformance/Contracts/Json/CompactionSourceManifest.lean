import Proofs.Conformance.Contracts.Json.Helpers
import Proofs.Conformance.ContractCases.CompactionSourceManifest

namespace Conformance.Contracts

open Conformance.ContractCases

def compactionSourceManifestCaseJson (row : CompactionSourceManifestCase) : String :=
  "{"
    ++ "\"name\":" ++ jsonString row.name ++ ","
    ++ "\"disposition\":" ++ jsonString row.disposition ++ ","
    ++ "\"visible_logical_twins\":" ++ toString row.visibleLogicalTwins ++ ","
    ++ "\"manifest_valid\":" ++ boolString row.manifestValid ++ ","
    ++ "\"sources_current\":" ++ boolString row.sourcesCurrent ++ ","
    ++ "\"durable_rows\":" ++ toString row.durableRows
    ++ "}"

def compactionSourceManifestCasesJson : String :=
  jsonArray (compactionSourceManifestCases.map compactionSourceManifestCaseJson)

end Conformance.Contracts
