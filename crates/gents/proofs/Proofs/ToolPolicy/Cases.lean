import Proofs.ToolPolicy.Instances

namespace ToolPolicy.ContractCases

open ToolPolicy

structure WriteGrantView where
  tool : String
  collection : String
  fields : List String
  deriving Repr

structure SurfaceView where
  fileRank : Nat
  meta : Bool
  defraQuery : Bool
  selfConfig : Bool
  memory : Bool
  sessionHistory : Bool
  contextBudget : Bool
  spawn : Bool
  steering : Bool
  background : Bool
  crossDeployment : Bool
  skills : Bool
  lsp : Bool
  bashMode : Nat
  bashNet : Nat
  bashSandbox : Bool
  bashAllowedKind : String
  bashAllowedPrefixes : List (List String)
  bashForbidden : List (List String)
  bashReadOnlyKind : String
  bashReadOnlyKeys : List String
  cliScopeKind : String
  cliKeys : List String
  mcpProbe : String
  mcpScopeKind : String
  mcpServices : List String
  mcpPermits : Bool
  defraCollectionsScopeKind : String
  defraCollectionsKeys : List String
  selfConfigCategoriesScopeKind : String
  selfConfigCategoriesKeys : List String
  subagentTargetsScopeKind : String
  subagentTargetsKeys : List String
  backgroundToolsScopeKind : String
  backgroundToolsKeys : List String
  writeProbe : String × String
  writeScopeKind : String
  writeGrants : List WriteGrantView
  writeFields : List String
  queryProbe : String × String
  queryScopeKind : String
  queryGrants : List WriteGrantView
  queryFields : List String
  deriving Repr

structure Case where
  name : String
  behavior : SurfaceView
  ceiling : SurfaceView
  runtime : SurfaceView
  expected : SurfaceView
  deriving Repr

def scopeKind {K V : Type} : EndpointScope K V → String
  | .all => "all"
  | .only _ _ => "only"
  | .none => "none"

def stringSet (items : List String) : Finset String :=
  items.toFinset

def knownToolIds : List String :=
  ["svc-a", "svc-x", "svc-y"]

def knownArgvPrefixes : List (List String) :=
  [["git", "status"], ["ls"]]

def knownForbiddenPrefixes : List (List String) :=
  [["curl"], ["rm"], ["sudo"]]

def knownReadOnlyCmds : List String :=
  ["cat", "ls", "pwd"]

def knownWriteKeys : List (String × String) :=
  [("wt", "coll"), ("wt", "coll1"), ("wt", "coll2")]

def knownQueryKeys : List (String × String) :=
  [("qt", "coll")]

def probeQuery : String × String := ("qt", "coll")

def knownSelfConfigCategories : List String :=
  ["automation", "backend", "behavior", "mcp_service", "profile", "tools"]

def knownSubagentTargets : List (String × String) :=
  [("did-a", "beh-a"), ("did-b", "beh-b")]

def knownFieldNames : List String :=
  ["field_a", "field_b", "field_c"]

def fieldList (fields : Finset String) : List String :=
  knownFieldNames.filter (fun field => decide (field ∈ fields))

def toolScopeKeys {V : Type} : EndpointScope ToolId V → List String
  | .only keys _ => knownToolIds.filter (fun key => decide (key ∈ keys))
  | .all => []
  | .none => []

def selfConfigScopeKeys {V : Type} : EndpointScope ToolId V → List String
  | .only keys _ =>
      knownSelfConfigCategories.filter (fun key => decide (key ∈ keys))
  | .all => []
  | .none => []

def subagentScopeKeys {V : Type} : EndpointScope (String × String) V → List String
  | .only keys _ =>
      knownSubagentTargets.filterMap (fun key =>
        if key ∈ keys then some (key.1 ++ "::" ++ key.2) else none)
  | .all => []
  | .none => []

def bashAllowedPrefixes : EndpointScope (List String) Unit → List (List String)
  | .only keys _ => knownArgvPrefixes.filter (fun key => decide (key ∈ keys))
  | .all => []
  | .none => []

def bashForbiddenView (forbidden : Finset (List String)) : List (List String) :=
  knownForbiddenPrefixes.filter (fun key => decide (key ∈ forbidden))

def bashReadOnlyKeys : EndpointScope String Unit → List String
  | .only keys _ => knownReadOnlyCmds.filter (fun key => decide (key ∈ keys))
  | .all => []
  | .none => []

def grantViews (known : List (String × String)) :
    EndpointScope (String × String) (Finset String) → List WriteGrantView
  | .only keys val =>
      known.filterMap (fun key =>
        if key ∈ keys then
          some
            { tool := key.1
            , collection := key.2
            , fields := fieldList (val key) }
        else
          none)
  | .all => []
  | .none => []

def writeGrantViews := grantViews knownWriteKeys
def queryGrantViews := grantViews knownQueryKeys

def unitOnly {K : Type} (keys : Finset K) : EndpointScope K Unit :=
  .only keys (fun _ => ())

def toolOnly (tool : ToolId) : EndpointScope ToolId Unit :=
  unitOnly [tool].toFinset

def toolsOnly (tools : List ToolId) : EndpointScope ToolId Unit :=
  unitOnly tools.toFinset

def subagentOnly (keys : List (String × String)) :
    EndpointScope (String × String) Unit :=
  unitOnly keys.toFinset

def cliOnly (entries : List (String × List String)) :
    EndpointScope ToolId (Finset String) :=
  .only (entries.map Prod.fst).toFinset
    (fun key =>
      match entries.find? (fun entry => entry.1 == key) with
      | some entry => stringSet entry.2
      | none => ∅)

def writeOnly (key : String × String) (fields : List String) :
    EndpointScope (String × String) (Finset String) :=
  .only [key].toFinset (fun _ => stringSet fields)

def bashPolicy (mode : ExecMode) (network : NetMode)
    (allowed : EndpointScope (List String) Unit) : BashPolicy :=
  { mode := mode
  , network := network
  , forbidden := ∅
  , allowed := allowed
  , readOnly := .all
  , sandbox := true }

def readOnlyOnly (cmds : List String) : EndpointScope String Unit :=
  unitOnly cmds.toFinset

def bashPolicyRich (forbidden : List (List String))
    (readOnly : EndpointScope String Unit) : BashPolicy :=
  { mode := .unrestricted
  , network := .enabled
  , forbidden := forbidden.toFinset
  , allowed := .all
  , readOnly := readOnly
  , sandbox := true }

def surface (file : FileCap) (bash : BashPolicy)
    (meta defraQuery spawn : Bool)
    (mcp : EndpointScope ToolId Unit)
    (write : EndpointScope (String × String) (Finset String)) : Surface :=
  { file := file
  , bash := bash
  , meta := meta
  , defraQuery := defraQuery
  , selfConfig := defraQuery
  , memory := meta
  , sessionHistory := meta
  , contextBudget := meta
  , spawn := spawn
  , steering := meta
  , background := spawn
  , crossDeployment := spawn
  , skills := meta
  , lsp := meta
  , cliTools := .all
  , mcpServices := mcp
  , defraCollections := .all
  , selfConfigCategories := .all
  , subagentTargets := .all
  , backgroundTools := .all
  , writeTools := write
  , queryTools := .all }

def view (s : Surface) (mcpProbe : String) (writeProbe : String × String) : SurfaceView :=
  { fileRank := s.file.rank
  , meta := s.meta
  , defraQuery := s.defraQuery
  , selfConfig := s.selfConfig
  , memory := s.memory
  , sessionHistory := s.sessionHistory
  , contextBudget := s.contextBudget
  , spawn := s.spawn
  , steering := s.steering
  , background := s.background
  , crossDeployment := s.crossDeployment
  , skills := s.skills
  , lsp := s.lsp
  , bashMode := s.bash.mode.rank
  , bashNet := s.bash.network.rank
  , bashSandbox := s.bash.sandbox
  , bashAllowedKind := scopeKind s.bash.allowed
  , bashAllowedPrefixes := bashAllowedPrefixes s.bash.allowed
  , bashForbidden := bashForbiddenView s.bash.forbidden
  , bashReadOnlyKind := scopeKind s.bash.readOnly
  , bashReadOnlyKeys := bashReadOnlyKeys s.bash.readOnly
  , cliScopeKind := scopeKind s.cliTools
  , cliKeys := toolScopeKeys s.cliTools
  , mcpProbe := mcpProbe
  , mcpScopeKind := scopeKind s.mcpServices
  , mcpServices := toolScopeKeys s.mcpServices
  , mcpPermits := decide (s.mcpServices.permits mcpProbe)
  , defraCollectionsScopeKind := scopeKind s.defraCollections
  , defraCollectionsKeys := toolScopeKeys s.defraCollections
  , selfConfigCategoriesScopeKind := scopeKind s.selfConfigCategories
  , selfConfigCategoriesKeys := selfConfigScopeKeys s.selfConfigCategories
  , subagentTargetsScopeKind := scopeKind s.subagentTargets
  , subagentTargetsKeys := subagentScopeKeys s.subagentTargets
  , backgroundToolsScopeKind := scopeKind s.backgroundTools
  , backgroundToolsKeys := toolScopeKeys s.backgroundTools
  , writeProbe := writeProbe
  , writeScopeKind := scopeKind s.writeTools
  , writeGrants := writeGrantViews s.writeTools
  , writeFields := match s.writeTools.lookup writeProbe with
      | some fields => fieldList fields
      | none => []
  , queryProbe := probeQuery
  , queryScopeKind := scopeKind s.queryTools
  , queryGrants := queryGrantViews s.queryTools
  , queryFields := match s.queryTools.lookup probeQuery with
      | some fields => fieldList fields
      | none => [] }

def probeWrite : String × String := ("wt", "coll")

def allowedOnlyGit : EndpointScope (List String) Unit :=
  unitOnly [["git", "status"]].toFinset

def allowedOnlyLs : EndpointScope (List String) Unit :=
  unitOnly [["ls"]].toFinset

def writeA : EndpointScope (String × String) (Finset String) :=
  writeOnly probeWrite ["field_a"]

def writeB : EndpointScope (String × String) (Finset String) :=
  writeOnly probeWrite ["field_b"]

def writeEmpty : EndpointScope (String × String) (Finset String) :=
  writeOnly probeWrite []

def wideOpen : Surface :=
  surface .readWrite
    (bashPolicy .unrestricted .enabled .all)
    true true true .all writeA

def secureMinimal : Surface :=
  surface .off
    (bashPolicy .readOnly .inherit .none)
    false false false .none writeEmpty

def ceilingMcpOnly : Surface :=
  surface .readWrite
    (bashPolicy .unrestricted .enabled .all)
    true true true (toolOnly "svc-a") writeA

def runtimeNoMcp : Surface :=
  surface .readWrite
    (bashPolicy .unrestricted .enabled .all)
    true true true .none writeA

def behaviorWriteB : Surface :=
  surface .readWrite
    (bashPolicy .unrestricted .enabled .all)
    true true true .all writeB

def ceilingWriteFieldsNarrowed : Surface :=
  surface .readWrite
    (bashPolicy .unrestricted .enabled .all)
    true true true .all writeA

def writeCollA : EndpointScope (String × String) (Finset String) :=
  writeOnly ("wt", "coll1") ["field_a"]

def writeCollB : EndpointScope (String × String) (Finset String) :=
  writeOnly ("wt", "coll2") ["field_a"]

def behaviorWriteCollA : Surface :=
  surface .readWrite
    (bashPolicy .unrestricted .enabled .all)
    true true true .all writeCollA

def ceilingWriteCollB : Surface :=
  surface .readWrite
    (bashPolicy .unrestricted .enabled .all)
    true true true .all writeCollB

def behaviorDisjointOnly : Surface :=
  { surface .readWrite
      (bashPolicy .unrestricted .enabled allowedOnlyGit)
      true true true (toolOnly "svc-x") writeA with
    defraCollections := toolOnly "svc-x" }

def ceilingDisjointOnly : Surface :=
  { surface .readWrite
      (bashPolicy .unrestricted .enabled allowedOnlyLs)
      true true true (toolOnly "svc-y") writeA with
    defraCollections := toolOnly "svc-y" }

def behaviorEachCategory : Surface :=
  { wideOpen with
    cliTools := cliOnly [("svc-a", ["field_a", "field_b"]), ("svc-x", ["field_a"])]
  , defraCollections := toolsOnly ["svc-a", "svc-x"]
  , selfConfigCategories := toolsOnly ["behavior", "profile", "tools"]
  , subagentTargets := subagentOnly [("did-a", "beh-a"), ("did-b", "beh-b")]
  , backgroundTools := toolsOnly ["svc-a", "svc-x"] }

def ceilingClampsEachCategory : Surface :=
  { wideOpen with
    memory := false
  , lsp := false
  , sessionHistory := false
  , contextBudget := false
  , steering := false
  , background := false
  , crossDeployment := false
  , skills := false
  , selfConfig := false
  , cliTools := cliOnly [("svc-a", ["field_a"])]
  , defraCollections := toolOnly "svc-a"
  , selfConfigCategories := toolsOnly ["tools"]
  , subagentTargets := subagentOnly [("did-a", "beh-a")]
  , backgroundTools := toolOnly "svc-a" }

def ceilingScopesOnly : Surface :=
  { wideOpen with
    cliTools := cliOnly [("svc-a", ["field_a"])]
  , defraCollections := toolOnly "svc-a"
  , selfConfigCategories := toolsOnly ["behavior"]
  , subagentTargets := subagentOnly [("did-a", "beh-a")]
  , backgroundTools := toolOnly "svc-a" }

def behaviorBashRich : Surface :=
  surface .readWrite
    (bashPolicyRich [["rm"]] (readOnlyOnly ["cat", "ls"]))
    true true true .all writeA

def ceilingBashRich : Surface :=
  surface .readWrite
    (bashPolicyRich [["curl"]] (readOnlyOnly ["ls", "pwd"]))
    true true true .all writeA

def queryA : EndpointScope (String × String) (Finset String) :=
  writeOnly probeQuery ["field_a"]

def queryB : EndpointScope (String × String) (Finset String) :=
  writeOnly probeQuery ["field_b"]

def behaviorQueryA : Surface :=
  { surface .readWrite
      (bashPolicy .unrestricted .enabled .all)
      true true true .all writeA with
    queryTools := queryA }

def behaviorQueryB : Surface :=
  { surface .readWrite
      (bashPolicy .unrestricted .enabled .all)
      true true true .all writeA with
    queryTools := queryB }

def ceilingQueryA : Surface :=
  { surface .readWrite
      (bashPolicy .unrestricted .enabled .all)
      true true true .all writeA with
    queryTools := queryA }

def writeAllNoQuery : Surface :=
  { surface .readWrite
      (bashPolicy .unrestricted .enabled .all)
      true true true .all writeA with
    writeTools := .all
    queryTools := .none }

def mkCase (name : String) (b c : Surface) (r : Avail)
    (mcpProbe : String) (writeProbe : String × String) : Case :=
  { name := name
  , behavior := view b mcpProbe writeProbe
  , ceiling := view c mcpProbe writeProbe
  , runtime := view r mcpProbe writeProbe
  , expected := view (effective b c r) mcpProbe writeProbe }

def cases : List Case :=
  [ mkCase "wide_open_clamped_by_secure_ceiling"
      wideOpen secureMinimal wideOpen "svc-a" probeWrite
  , mkCase "ceiling_mcp_only_clamps_behavior"
      wideOpen ceilingMcpOnly wideOpen "svc-a" probeWrite
  , mkCase "runtime_offline_drops_permitted_mcp"
      wideOpen wideOpen runtimeNoMcp "svc-a" probeWrite
  , mkCase "write_fields_narrowed_by_ceiling"
      behaviorWriteB ceilingWriteFieldsNarrowed wideOpen "svc-a" probeWrite
  , mkCase "write_tool_collection_mismatch_denies"
      behaviorWriteCollA ceilingWriteCollB wideOpen "svc-a" ("wt", "coll1")
  , mkCase "disjoint_only_scopes_intersect_to_empty"
      behaviorDisjointOnly ceilingDisjointOnly wideOpen "svc-x" probeWrite
  , mkCase "bash_all_allowed_kind_idempotent"
      wideOpen wideOpen wideOpen "svc-a" probeWrite
  , mkCase "ceiling_clamps_each_category"
      behaviorEachCategory ceilingClampsEachCategory wideOpen "svc-a" probeWrite
  , mkCase "behavior_all_scopes_clamped_by_ceiling_only"
      wideOpen ceilingScopesOnly wideOpen "svc-a" probeWrite
  , mkCase "bash_forbidden_union_and_readonly_intersection"
      behaviorBashRich ceilingBashRich wideOpen "svc-a" probeWrite
  , mkCase "query_fields_narrowed_by_ceiling"
      behaviorQueryB ceilingQueryA wideOpen "svc-a" probeWrite
  , mkCase "write_all_does_not_grant_query"
      behaviorQueryA writeAllNoQuery wideOpen "svc-a" probeWrite
  ]

end ToolPolicy.ContractCases
