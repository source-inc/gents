import Proofs.Conformance.Contracts.Json.Helpers
import Proofs.ToolPolicy.Cases

namespace Conformance.Contracts

open ToolPolicy.ContractCases

def writeGrantViewJson (grant : WriteGrantView) : String :=
  "{"
    ++ "\"tool\":" ++ jsonString grant.tool ++ ","
    ++ "\"collection\":" ++ jsonString grant.collection ++ ","
    ++ "\"fields\":" ++ jsonStringArray grant.fields
  ++ "}"

def boolJson (b : Bool) : String := if b then "true" else "false"

def surfaceViewJson (v : SurfaceView) : String :=
  "{"
    ++ "\"file_rank\":" ++ toString v.fileRank ++ ","
    ++ "\"meta\":" ++ boolJson v.meta ++ ","
    ++ "\"defra_query\":" ++ boolJson v.defraQuery ++ ","
    ++ "\"self_config\":" ++ boolJson v.selfConfig ++ ","
    ++ "\"memory\":" ++ boolJson v.memory ++ ","
    ++ "\"session_history\":" ++ boolJson v.sessionHistory ++ ","
    ++ "\"context_budget\":" ++ boolJson v.contextBudget ++ ","
    ++ "\"spawn\":" ++ boolJson v.spawn ++ ","
    ++ "\"steering\":" ++ boolJson v.steering ++ ","
    ++ "\"background\":" ++ boolJson v.background ++ ","
    ++ "\"cross_deployment\":" ++ boolJson v.crossDeployment ++ ","
    ++ "\"skills\":" ++ boolJson v.skills ++ ","
    ++ "\"lsp\":" ++ boolJson v.lsp ++ ","
    ++ "\"bash_mode\":" ++ toString v.bashMode ++ ","
    ++ "\"bash_net\":" ++ toString v.bashNet ++ ","
    ++ "\"bash_sandbox\":" ++ boolJson v.bashSandbox ++ ","
    ++ "\"bash_allowed_kind\":" ++ jsonString v.bashAllowedKind ++ ","
    ++ "\"bash_allowed_prefixes\":" ++ jsonStringMatrix v.bashAllowedPrefixes ++ ","
    ++ "\"bash_forbidden\":" ++ jsonStringMatrix v.bashForbidden ++ ","
    ++ "\"bash_read_only_kind\":" ++ jsonString v.bashReadOnlyKind ++ ","
    ++ "\"bash_read_only_keys\":" ++ jsonStringArray v.bashReadOnlyKeys ++ ","
    ++ "\"cli_scope_kind\":" ++ jsonString v.cliScopeKind ++ ","
    ++ "\"cli_keys\":" ++ jsonStringArray v.cliKeys ++ ","
    ++ "\"mcp_probe\":" ++ jsonString v.mcpProbe ++ ","
    ++ "\"mcp_scope_kind\":" ++ jsonString v.mcpScopeKind ++ ","
    ++ "\"mcp_services\":" ++ jsonStringArray v.mcpServices ++ ","
    ++ "\"mcp_permits\":" ++ boolJson v.mcpPermits ++ ","
    ++ "\"defra_collections_scope_kind\":" ++ jsonString v.defraCollectionsScopeKind ++ ","
    ++ "\"defra_collections_keys\":" ++ jsonStringArray v.defraCollectionsKeys ++ ","
    ++ "\"self_config_categories_scope_kind\":"
      ++ jsonString v.selfConfigCategoriesScopeKind ++ ","
    ++ "\"self_config_categories_keys\":"
      ++ jsonStringArray v.selfConfigCategoriesKeys ++ ","
    ++ "\"subagent_targets_scope_kind\":" ++ jsonString v.subagentTargetsScopeKind ++ ","
    ++ "\"subagent_targets_keys\":" ++ jsonStringArray v.subagentTargetsKeys ++ ","
    ++ "\"background_tools_scope_kind\":" ++ jsonString v.backgroundToolsScopeKind ++ ","
    ++ "\"background_tools_keys\":" ++ jsonStringArray v.backgroundToolsKeys ++ ","
    ++ "\"write_probe_tool\":" ++ jsonString v.writeProbe.1 ++ ","
    ++ "\"write_probe_collection\":" ++ jsonString v.writeProbe.2 ++ ","
    ++ "\"write_scope_kind\":" ++ jsonString v.writeScopeKind ++ ","
    ++ "\"write_grants\":"
      ++ jsonArray (v.writeGrants.map writeGrantViewJson) ++ ","
    ++ "\"write_fields\":" ++ jsonArray (v.writeFields.map jsonString) ++ ","
    ++ "\"query_probe_tool\":" ++ jsonString v.queryProbe.1 ++ ","
    ++ "\"query_probe_collection\":" ++ jsonString v.queryProbe.2 ++ ","
    ++ "\"query_scope_kind\":" ++ jsonString v.queryScopeKind ++ ","
    ++ "\"query_grants\":"
      ++ jsonArray (v.queryGrants.map writeGrantViewJson) ++ ","
    ++ "\"query_fields\":" ++ jsonArray (v.queryFields.map jsonString)
  ++ "}"

def toolPolicyCaseJson (c : Case) : String :=
  "{"
    ++ "\"name\":" ++ jsonString c.name ++ ","
    ++ "\"behavior\":" ++ surfaceViewJson c.behavior ++ ","
    ++ "\"ceiling\":" ++ surfaceViewJson c.ceiling ++ ","
    ++ "\"runtime\":" ++ surfaceViewJson c.runtime ++ ","
    ++ "\"expected\":" ++ surfaceViewJson c.expected
  ++ "}"

def toolPolicyCasesJson : String :=
  jsonArray (ToolPolicy.ContractCases.cases.map toolPolicyCaseJson)

end Conformance.Contracts
