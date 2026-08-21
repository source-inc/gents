import Proofs.ToolPolicy.Theorems

namespace ToolPolicy

def fieldsVM : ValueMeet (Finset String) :=
  { vmeet := fun a b => a ∩ b
  , vle := fun a b => a ⊆ b
  , vle_refl := by intro _; exact subset_rfl
  , vmeet_le_left := by intro _ _; exact Finset.inter_subset_left
  , vmeet_le_right := by intro _ _; exact Finset.inter_subset_right }

def rootVM : ValueMeet (Finset String) := fieldsVM

def Surface.meet (a b : Surface) : Surface :=
  { file := a.file.meet b.file
  , bash := a.bash.meet b.bash
  , meta := a.meta && b.meta
  , defraQuery := a.defraQuery && b.defraQuery
  , selfConfig := a.selfConfig && b.selfConfig
  , memory := a.memory && b.memory
  , sessionHistory := a.sessionHistory && b.sessionHistory
  , contextBudget := a.contextBudget && b.contextBudget
  , spawn := a.spawn && b.spawn
  , steering := a.steering && b.steering
  , background := a.background && b.background
  , crossDeployment := a.crossDeployment && b.crossDeployment
  , skills := a.skills && b.skills
  , lsp := a.lsp && b.lsp
  , cliTools := a.cliTools.meet rootVM b.cliTools
  , mcpServices := a.mcpServices.meet unitVM b.mcpServices
  , defraCollections := a.defraCollections.meet unitVM b.defraCollections
  , selfConfigCategories := a.selfConfigCategories.meet unitVM b.selfConfigCategories
  , subagentTargets := a.subagentTargets.meet unitVM b.subagentTargets
  , backgroundTools := a.backgroundTools.meet unitVM b.backgroundTools
  , writeTools := a.writeTools.meet fieldsVM b.writeTools
  , queryTools := a.queryTools.meet fieldsVM b.queryTools }

def effective (behavior ceiling : Surface) (runtime : Avail) : Surface :=
  (behavior.meet ceiling).meet runtime

variable (behavior ceiling : Surface) (runtime : Avail)

theorem effective_file_le_ceiling :
    (effective behavior ceiling runtime).file.rank ≤ ceiling.file.rank := by
  unfold effective Surface.meet
  exact le_trans (FileCap.meet_rank_le_left _ _) (FileCap.meet_rank_le_right _ _)

theorem effective_file_le_behavior :
    (effective behavior ceiling runtime).file.rank ≤ behavior.file.rank := by
  unfold effective Surface.meet
  exact le_trans (FileCap.meet_rank_le_left _ _) (FileCap.meet_rank_le_left _ _)

theorem effective_meta_le_ceiling :
    (effective behavior ceiling runtime).meta = true → ceiling.meta = true := by
  unfold effective Surface.meet
  intro h
  exact bool_and_right (bool_and_left h)

theorem effective_meta_le_behavior :
    (effective behavior ceiling runtime).meta = true → behavior.meta = true := by
  unfold effective Surface.meet
  intro h
  exact bool_and_left (bool_and_left h)

theorem effective_defraQuery_le_ceiling :
    (effective behavior ceiling runtime).defraQuery = true → ceiling.defraQuery = true := by
  unfold effective Surface.meet
  intro h
  exact bool_and_right (bool_and_left h)

theorem effective_defraQuery_le_behavior :
    (effective behavior ceiling runtime).defraQuery = true → behavior.defraQuery = true := by
  unfold effective Surface.meet
  intro h
  exact bool_and_left (bool_and_left h)

theorem effective_selfConfig_le_ceiling :
    (effective behavior ceiling runtime).selfConfig = true → ceiling.selfConfig = true := by
  unfold effective Surface.meet
  intro h
  exact bool_and_right (bool_and_left h)

theorem effective_selfConfig_le_behavior :
    (effective behavior ceiling runtime).selfConfig = true → behavior.selfConfig = true := by
  unfold effective Surface.meet
  intro h
  exact bool_and_left (bool_and_left h)

theorem effective_selfConfigCategories_subset_ceiling (k : ToolId) :
    (effective behavior ceiling runtime).selfConfigCategories.permits k →
      ceiling.selfConfigCategories.permits k := by
  unfold effective Surface.meet
  intro h
  have hin := EndpointScope.meet_permits_left unitVM
    (behavior.selfConfigCategories.meet unitVM ceiling.selfConfigCategories)
    runtime.selfConfigCategories k h
  exact EndpointScope.meet_permits_right unitVM behavior.selfConfigCategories
    ceiling.selfConfigCategories k hin

theorem effective_selfConfigCategories_subset_behavior (k : ToolId) :
    (effective behavior ceiling runtime).selfConfigCategories.permits k →
      behavior.selfConfigCategories.permits k := by
  unfold effective Surface.meet
  intro h
  have hin := EndpointScope.meet_permits_left unitVM
    (behavior.selfConfigCategories.meet unitVM ceiling.selfConfigCategories)
    runtime.selfConfigCategories k h
  exact EndpointScope.meet_permits_left unitVM behavior.selfConfigCategories
    ceiling.selfConfigCategories k hin

theorem effective_memory_le_ceiling :
    (effective behavior ceiling runtime).memory = true → ceiling.memory = true := by
  unfold effective Surface.meet
  intro h
  exact bool_and_right (bool_and_left h)

theorem effective_memory_le_behavior :
    (effective behavior ceiling runtime).memory = true → behavior.memory = true := by
  unfold effective Surface.meet
  intro h
  exact bool_and_left (bool_and_left h)

theorem effective_sessionHistory_le_ceiling :
    (effective behavior ceiling runtime).sessionHistory = true → ceiling.sessionHistory = true := by
  unfold effective Surface.meet
  intro h
  exact bool_and_right (bool_and_left h)

theorem effective_sessionHistory_le_behavior :
    (effective behavior ceiling runtime).sessionHistory = true → behavior.sessionHistory = true := by
  unfold effective Surface.meet
  intro h
  exact bool_and_left (bool_and_left h)

theorem effective_contextBudget_le_ceiling :
    (effective behavior ceiling runtime).contextBudget = true → ceiling.contextBudget = true := by
  unfold effective Surface.meet
  intro h
  exact bool_and_right (bool_and_left h)

theorem effective_contextBudget_le_behavior :
    (effective behavior ceiling runtime).contextBudget = true → behavior.contextBudget = true := by
  unfold effective Surface.meet
  intro h
  exact bool_and_left (bool_and_left h)

theorem effective_spawn_le_ceiling :
    (effective behavior ceiling runtime).spawn = true → ceiling.spawn = true := by
  unfold effective Surface.meet
  intro h
  exact bool_and_right (bool_and_left h)

theorem effective_spawn_le_behavior :
    (effective behavior ceiling runtime).spawn = true → behavior.spawn = true := by
  unfold effective Surface.meet
  intro h
  exact bool_and_left (bool_and_left h)

theorem effective_steering_le_ceiling :
    (effective behavior ceiling runtime).steering = true → ceiling.steering = true := by
  unfold effective Surface.meet
  intro h
  exact bool_and_right (bool_and_left h)

theorem effective_steering_le_behavior :
    (effective behavior ceiling runtime).steering = true → behavior.steering = true := by
  unfold effective Surface.meet
  intro h
  exact bool_and_left (bool_and_left h)

theorem effective_background_le_ceiling :
    (effective behavior ceiling runtime).background = true → ceiling.background = true := by
  unfold effective Surface.meet
  intro h
  exact bool_and_right (bool_and_left h)

theorem effective_background_le_behavior :
    (effective behavior ceiling runtime).background = true → behavior.background = true := by
  unfold effective Surface.meet
  intro h
  exact bool_and_left (bool_and_left h)

theorem effective_crossDeployment_le_ceiling :
    (effective behavior ceiling runtime).crossDeployment = true → ceiling.crossDeployment = true := by
  unfold effective Surface.meet
  intro h
  exact bool_and_right (bool_and_left h)

theorem effective_crossDeployment_le_behavior :
    (effective behavior ceiling runtime).crossDeployment = true → behavior.crossDeployment = true := by
  unfold effective Surface.meet
  intro h
  exact bool_and_left (bool_and_left h)

theorem effective_skills_le_ceiling :
    (effective behavior ceiling runtime).skills = true → ceiling.skills = true := by
  unfold effective Surface.meet
  intro h
  exact bool_and_right (bool_and_left h)

theorem effective_skills_le_behavior :
    (effective behavior ceiling runtime).skills = true → behavior.skills = true := by
  unfold effective Surface.meet
  intro h
  exact bool_and_left (bool_and_left h)

theorem effective_lsp_le_ceiling :
    (effective behavior ceiling runtime).lsp = true → ceiling.lsp = true := by
  unfold effective Surface.meet
  intro h
  exact bool_and_right (bool_and_left h)

theorem effective_lsp_le_behavior :
    (effective behavior ceiling runtime).lsp = true → behavior.lsp = true := by
  unfold effective Surface.meet
  intro h
  exact bool_and_left (bool_and_left h)

theorem effective_mcp_subset_ceiling (k : ToolId) :
    (effective behavior ceiling runtime).mcpServices.permits k →
      ceiling.mcpServices.permits k := by
  unfold effective Surface.meet
  intro h
  have hin := EndpointScope.meet_permits_left unitVM
    (behavior.mcpServices.meet unitVM ceiling.mcpServices) runtime.mcpServices k h
  exact EndpointScope.meet_permits_right unitVM behavior.mcpServices ceiling.mcpServices k hin

theorem effective_mcp_subset_behavior (k : ToolId) :
    (effective behavior ceiling runtime).mcpServices.permits k →
      behavior.mcpServices.permits k := by
  unfold effective Surface.meet
  intro h
  have hin := EndpointScope.meet_permits_left unitVM
    (behavior.mcpServices.meet unitVM ceiling.mcpServices) runtime.mcpServices k h
  exact EndpointScope.meet_permits_left unitVM behavior.mcpServices ceiling.mcpServices k hin

theorem effective_cli_keys_subset_ceiling (k : ToolId) :
    (effective behavior ceiling runtime).cliTools.permits k →
      ceiling.cliTools.permits k := by
  unfold effective Surface.meet
  intro h
  have hin := EndpointScope.meet_permits_left rootVM
    (behavior.cliTools.meet rootVM ceiling.cliTools) runtime.cliTools k h
  exact EndpointScope.meet_permits_right rootVM behavior.cliTools ceiling.cliTools k hin

theorem effective_cli_keys_subset_behavior (k : ToolId) :
    (effective behavior ceiling runtime).cliTools.permits k →
      behavior.cliTools.permits k := by
  unfold effective Surface.meet
  intro h
  have hin := EndpointScope.meet_permits_left rootVM
    (behavior.cliTools.meet rootVM ceiling.cliTools) runtime.cliTools k h
  exact EndpointScope.meet_permits_left rootVM behavior.cliTools ceiling.cliTools k hin

theorem effective_defraCollections_subset_ceiling (k : ToolId) :
    (effective behavior ceiling runtime).defraCollections.permits k →
      ceiling.defraCollections.permits k := by
  unfold effective Surface.meet
  intro h
  have hin := EndpointScope.meet_permits_left unitVM
    (behavior.defraCollections.meet unitVM ceiling.defraCollections) runtime.defraCollections k h
  exact EndpointScope.meet_permits_right unitVM behavior.defraCollections ceiling.defraCollections k hin

theorem effective_defraCollections_subset_behavior (k : ToolId) :
    (effective behavior ceiling runtime).defraCollections.permits k →
      behavior.defraCollections.permits k := by
  unfold effective Surface.meet
  intro h
  have hin := EndpointScope.meet_permits_left unitVM
    (behavior.defraCollections.meet unitVM ceiling.defraCollections) runtime.defraCollections k h
  exact EndpointScope.meet_permits_left unitVM behavior.defraCollections ceiling.defraCollections k hin

theorem effective_subagentTargets_subset_ceiling (k : String × String) :
    (effective behavior ceiling runtime).subagentTargets.permits k →
      ceiling.subagentTargets.permits k := by
  unfold effective Surface.meet
  intro h
  have hin := EndpointScope.meet_permits_left unitVM
    (behavior.subagentTargets.meet unitVM ceiling.subagentTargets) runtime.subagentTargets k h
  exact EndpointScope.meet_permits_right unitVM behavior.subagentTargets ceiling.subagentTargets k hin

theorem effective_subagentTargets_subset_behavior (k : String × String) :
    (effective behavior ceiling runtime).subagentTargets.permits k →
      behavior.subagentTargets.permits k := by
  unfold effective Surface.meet
  intro h
  have hin := EndpointScope.meet_permits_left unitVM
    (behavior.subagentTargets.meet unitVM ceiling.subagentTargets) runtime.subagentTargets k h
  exact EndpointScope.meet_permits_left unitVM behavior.subagentTargets ceiling.subagentTargets k hin

theorem effective_backgroundTools_subset_ceiling (k : ToolId) :
    (effective behavior ceiling runtime).backgroundTools.permits k →
      ceiling.backgroundTools.permits k := by
  unfold effective Surface.meet
  intro h
  have hin := EndpointScope.meet_permits_left unitVM
    (behavior.backgroundTools.meet unitVM ceiling.backgroundTools) runtime.backgroundTools k h
  exact EndpointScope.meet_permits_right unitVM behavior.backgroundTools ceiling.backgroundTools k hin

theorem effective_backgroundTools_subset_behavior (k : ToolId) :
    (effective behavior ceiling runtime).backgroundTools.permits k →
      behavior.backgroundTools.permits k := by
  unfold effective Surface.meet
  intro h
  have hin := EndpointScope.meet_permits_left unitVM
    (behavior.backgroundTools.meet unitVM ceiling.backgroundTools) runtime.backgroundTools k h
  exact EndpointScope.meet_permits_left unitVM behavior.backgroundTools ceiling.backgroundTools k hin

theorem effective_write_keys_subset_ceiling (k : String × String) :
    (effective behavior ceiling runtime).writeTools.permits k →
      ceiling.writeTools.permits k := by
  unfold effective Surface.meet
  intro h
  have hin := EndpointScope.meet_permits_left fieldsVM
    (behavior.writeTools.meet fieldsVM ceiling.writeTools) runtime.writeTools k h
  exact EndpointScope.meet_permits_right fieldsVM behavior.writeTools ceiling.writeTools k hin

theorem effective_write_keys_subset_behavior (k : String × String) :
    (effective behavior ceiling runtime).writeTools.permits k →
      behavior.writeTools.permits k := by
  unfold effective Surface.meet
  intro h
  have hin := EndpointScope.meet_permits_left fieldsVM
    (behavior.writeTools.meet fieldsVM ceiling.writeTools) runtime.writeTools k h
  exact EndpointScope.meet_permits_left fieldsVM behavior.writeTools ceiling.writeTools k hin

theorem effective_bash_permits_subset_ceiling (req : CmdReq) :
    (effective behavior ceiling runtime).bash.permits req →
      ceiling.bash.permits req := by
  unfold effective Surface.meet
  intro h
  have hin := BashPolicy.meet_permits_left (behavior.bash.meet ceiling.bash) runtime.bash req h
  exact BashPolicy.meet_permits_right behavior.bash ceiling.bash req hin

theorem effective_bash_permits_subset_behavior (req : CmdReq) :
    (effective behavior ceiling runtime).bash.permits req →
      behavior.bash.permits req := by
  unfold effective Surface.meet
  intro h
  have hin := BashPolicy.meet_permits_left (behavior.bash.meet ceiling.bash) runtime.bash req h
  exact BashPolicy.meet_permits_left behavior.bash ceiling.bash req hin

theorem effective_within_ceiling :
    (effective behavior ceiling runtime).file.rank ≤ ceiling.file.rank ∧
    ((effective behavior ceiling runtime).meta = true → ceiling.meta = true) ∧
    ((effective behavior ceiling runtime).defraQuery = true → ceiling.defraQuery = true) ∧
    ((effective behavior ceiling runtime).skills = true → ceiling.skills = true) := by
  exact ⟨effective_file_le_ceiling behavior ceiling runtime,
    effective_meta_le_ceiling behavior ceiling runtime,
    effective_defraQuery_le_ceiling behavior ceiling runtime,
    effective_skills_le_ceiling behavior ceiling runtime⟩

theorem effective_query_keys_subset_ceiling (k : String × String) :
    (effective behavior ceiling runtime).queryTools.permits k →
      ceiling.queryTools.permits k := by
  unfold effective Surface.meet
  intro h
  have hin := EndpointScope.meet_permits_left fieldsVM
    (behavior.queryTools.meet fieldsVM ceiling.queryTools) runtime.queryTools k h
  exact EndpointScope.meet_permits_right fieldsVM behavior.queryTools ceiling.queryTools k hin

theorem effective_query_keys_subset_behavior (k : String × String) :
    (effective behavior ceiling runtime).queryTools.permits k →
      behavior.queryTools.permits k := by
  unfold effective Surface.meet
  intro h
  have hin := EndpointScope.meet_permits_left fieldsVM
    (behavior.queryTools.meet fieldsVM ceiling.queryTools) runtime.queryTools k h
  exact EndpointScope.meet_permits_left fieldsVM behavior.queryTools ceiling.queryTools k hin

theorem effective_query_fields_narrow
    (behavior ceiling : Surface) (runtime : Avail)
    (hrt : runtime.queryTools = .all)
    (k : String × String) (vc ve : Finset String)
    (hck : ceiling.queryTools.lookup k = some vc)
    (hek : (effective behavior ceiling runtime).queryTools.lookup k = some ve) :
    ve ⊆ vc := by
  unfold effective Surface.meet at hek
  rw [hrt] at hek
  have hek' : (behavior.queryTools.meet fieldsVM ceiling.queryTools).lookup k = some ve := by
    simpa using hek
  exact EndpointScope.meet_lookup_vle_right fieldsVM
    behavior.queryTools ceiling.queryTools k ve vc hek' hck

theorem effective_write_fields_narrow
    (behavior ceiling : Surface) (runtime : Avail)
    (hrt : runtime.writeTools = .all)
    (k : String × String) (vc ve : Finset String)
    (hck : ceiling.writeTools.lookup k = some vc)
    (hek : (effective behavior ceiling runtime).writeTools.lookup k = some ve) :
    ve ⊆ vc := by
  unfold effective Surface.meet at hek
  rw [hrt] at hek
  have hek' : (behavior.writeTools.meet fieldsVM ceiling.writeTools).lookup k = some ve := by
    simpa using hek
  exact EndpointScope.meet_lookup_vle_right fieldsVM
    behavior.writeTools ceiling.writeTools k ve vc hek' hck

@[simp] theorem FileCap.meet_idem (a : FileCap) : a.meet a = a := by
  cases a <;> simp [FileCap.meet, FileCap.rank]

theorem FileCap.meet_comm (a b : FileCap) : a.meet b = b.meet a := by
  cases a <;> cases b <;> simp [FileCap.meet, FileCap.rank]

@[simp] theorem EndpointScope.meet_idem {K V : Type} [DecidableEq K]
    (vm : ValueMeet V) (hidem : ∀ v, vm.vmeet v v = v)
    (a : EndpointScope K V) :
    a.meet vm a = a := by
  cases a with
  | none => rfl
  | all => rfl
  | only keys val =>
      simp [EndpointScope.meet, Finset.inter_self]
      funext k
      exact hidem (val k)

@[simp] theorem BashPolicy.meet_idem (p : BashPolicy) : p.meet p = p := by
  unfold BashPolicy.meet
  have hu : ∀ v : Unit, unitVM.vmeet v v = v := by intro v; cases v; rfl
  simp [EndpointScope.meet_idem unitVM hu, Finset.union_self, Bool.and_self]

@[simp] theorem Surface.meet_idem (s : Surface) : s.meet s = s := by
  unfold Surface.meet
  have hf : ∀ v : Finset String, fieldsVM.vmeet v v = v := by
    intro v
    simp [fieldsVM, Finset.inter_self]
  have hr : ∀ v : Finset String, rootVM.vmeet v v = v := hf
  have hu : ∀ v : Unit, unitVM.vmeet v v = v := by intro v; cases v; rfl
  simp [FileCap.meet_idem, BashPolicy.meet_idem, Bool.and_self,
    EndpointScope.meet_idem unitVM hu,
    EndpointScope.meet_idem fieldsVM hf,
    EndpointScope.meet_idem rootVM hr]

end ToolPolicy
