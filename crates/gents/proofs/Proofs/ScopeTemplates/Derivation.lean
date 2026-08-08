import Proofs.ScopeTemplates.State
import Mathlib.Data.List.Basic

namespace ScopeTemplates

def resolveTemplate (cat : Catalog) (id : TemplateId) : Option Template :=
  cat.find? (fun t => t.id = id)

theorem resolveTemplate_deterministic (cat : Catalog) (id : TemplateId) :
    resolveTemplate cat id = resolveTemplate cat id := rfl

theorem resolveTemplate_id_eq {cat : Catalog} {id : TemplateId} {t : Template}
    (h : resolveTemplate cat id = some t) : t.id = id := by
  unfold resolveTemplate at h
  have hp := List.find?_some h
  simpa using hp

theorem resolveTemplate_mem {cat : Catalog} {id : TemplateId} {t : Template}
    (h : resolveTemplate cat id = some t) : t ∈ cat := by
  unfold resolveTemplate at h
  exact List.mem_of_find?_eq_some h

theorem resolveTemplate_total {cat : Catalog} {id : TemplateId}
    (h : ∃ t ∈ cat, t.id = id) :
    ∃ t, resolveTemplate cat id = some t := by
  obtain ⟨t, ht_mem, ht_id⟩ := h
  unfold resolveTemplate
  cases hfind : cat.find? (fun t => t.id = id) with
  | some r => exact ⟨r, rfl⟩
  | none =>
      exfalso
      have hnone := List.find?_eq_none.mp hfind t ht_mem
      simp [ht_id] at hnone

theorem resolveTemplate_unknown {cat : Catalog} {id : TemplateId}
    (h : ∀ t ∈ cat, t.id ≠ id) :
    resolveTemplate cat id = none := by
  unfold resolveTemplate
  apply List.find?_eq_none.mpr
  intro t ht_mem
  simp only [decide_eq_true_eq]
  exact h t ht_mem

theorem resolveTemplate_isSome_iff {cat : Catalog} {id : TemplateId} :
    (resolveTemplate cat id).isSome ↔ ∃ t ∈ cat, t.id = id := by
  constructor
  · intro h
    obtain ⟨t, ht⟩ := Option.isSome_iff_exists.mp h
    exact ⟨t, resolveTemplate_mem ht, resolveTemplate_id_eq ht⟩
  · intro h
    obtain ⟨t, ht⟩ := resolveTemplate_total h
    rw [ht]
    rfl

def scopeFilter (scope : Scope) (collections : List String)
    (peerDid localDid : Did) : List CollectionScopeFilter :=
  match scope with
  | .peerDid field =>
      collections.map
        (fun c => { collection := c, field := field, value := peerDid })
  | .unscoped => []
  | .perCollection rules =>
      rules.map
        (fun r =>
          { collection := r.collection
          , field := r.field
          , value :=
              match r.source with
              | .localDid => localDid
              | .peerDid => peerDid
              | .homeDid => localDid })

theorem scopeFilter_spec (s : Scope) (collections : List String)
    (peerDid localDid : Did) :
    scopeFilter s collections peerDid localDid =
      match s with
      | .peerDid f =>
          collections.map
            (fun c => { collection := c, field := f, value := peerDid })
      | .unscoped => []
      | .perCollection rules =>
          rules.map
            (fun r =>
              { collection := r.collection
              , field := r.field
              , value :=
                  match r.source with
                  | .localDid => localDid
                  | .peerDid => peerDid
                  | .homeDid => localDid }) := by
  cases s <;> rfl

theorem scopeFilter_peerDid (f : String) (collections : List String)
    (peerDid localDid : Did) :
    scopeFilter (.peerDid f) collections peerDid localDid =
      collections.map
        (fun c => { collection := c, field := f, value := peerDid }) := rfl

theorem scopeFilter_unscoped (collections : List String) (peerDid localDid : Did) :
    scopeFilter .unscoped collections peerDid localDid = [] := rfl

theorem conversation_filter_eq (peerDid localDid : Did) :
    scopeFilter (.perCollection conversationRules) [] peerDid localDid
      = [ { collection := "AgentRequest",      field := "requester_did", value := peerDid }
        , { collection := "AgentResponse",     field := "requester_did", value := peerDid }
        , { collection := "AgentResponseOutcome", field := "requester_did", value := peerDid }
        , { collection := "AgentMessage",      field := "requester_did", value := peerDid }
        , { collection := "AgentToolCall",     field := "requester_did", value := peerDid }
        , { collection := "AgentToolResult",   field := "requester_did", value := peerDid }
        , { collection := "AgentToolApproval", field := "requester_did", value := peerDid }
        , { collection := "AgentSession",      field := "requester_did", value := peerDid }
        , { collection := "AgentConversation", field := "requester_did", value := peerDid }
        , { collection := "CompactionEntry",   field := "requester_did", value := peerDid }
        , { collection := "BearerPairingReady", field := "claimant_did", value := peerDid } ] := by
  simp [scopeFilter, conversationRules]

theorem conversation_filters_requester_lineage (peerDid localDid : Did) :
    (scopeFilter conversationTemplate.scope [] peerDid localDid).all
      (fun k =>
        k.value = peerDid ∧
        (if k.collection = "BearerPairingReady"
          then k.field = "claimant_did"
          else k.field = "requester_did")) = true := by
  simp [scopeFilter, conversationTemplate, conversationRules]

theorem conversation_filters_exactly_transcript_collections (peerDid localDid : Did) :
    ((scopeFilter conversationTemplate.scope [] peerDid localDid).map
        (fun k => k.collection)).toFinset
      = conversationTranscriptCollections.toFinset := by
  simp [scopeFilter, conversationTemplate, conversationRules,
    conversationTranscriptCollections]

theorem conversation_config_is_unfiltered (peerDid localDid : Did) :
    agentConfigCollections.all (fun collection =>
      (scopeFilter conversationTemplate.scope [] peerDid localDid).all
        (fun filter => filter.collection ≠ collection)) = true := by
  simp [scopeFilter, conversationTemplate, conversationRules,
    agentConfigCollections]

theorem conversation_grants_agent_config :
    agentConfigCollections.toFinset ⊆ conversationTemplate.collections := by
  simp only [conversationTemplate, conversationCollections, List.toFinset_append]
  exact Finset.subset_union_right

theorem conversation_request_crossing_is_peer_scoped (peerDid localDid : Did) :
    (scopeFilter conversationTemplate.scope [] peerDid localDid).find?
        (fun k => k.collection = "AgentRequest") =
          some { collection := "AgentRequest", field := "requester_did", value := peerDid } := by
  simp [scopeFilter, conversationTemplate, conversationRules]

theorem conversation_readiness_crossing_is_claimant_scoped (peerDid localDid : Did) :
    (scopeFilter conversationTemplate.scope [] peerDid localDid).find?
        (fun k => k.collection = "BearerPairingReady") =
          some { collection := "BearerPairingReady", field := "claimant_did", value := peerDid } := by
  simp [scopeFilter, conversationTemplate, conversationRules]

theorem machine_filter_eq (peerDid homeDid : Did) :
    scopeFilter machineTemplate.scope [] peerDid homeDid =
      scopeFilter conversationTemplate.scope [] peerDid homeDid ++
        [ { collection := "AgentDirectoryEntry"
          , field := "source_did"
          , value := homeDid } ] := by
  simp [scopeFilter, machineTemplate, machineRules, conversationTemplate]

theorem machine_filters_transcript_and_directory (peerDid homeDid : Did) :
    ((scopeFilter machineTemplate.scope [] peerDid homeDid).map
        (fun k => k.collection)).toFinset
      = (conversationTranscriptCollections ++ ["AgentDirectoryEntry"]).toFinset := by
  simp [scopeFilter, machineTemplate, machineRules, machineCollections,
    conversationRules, conversationCollections, conversationTranscriptCollections]

theorem machine_directory_crossing_is_home_scoped (peerDid homeDid : Did) :
    (scopeFilter machineTemplate.scope [] peerDid homeDid).find?
        (fun k => k.collection = "AgentDirectoryEntry") =
          some { collection := "AgentDirectoryEntry"
               , field := "source_did"
               , value := homeDid } := by
  simp [scopeFilter, machineTemplate, machineRules, conversationRules]

theorem subagentCoordinator_filter_eq (peerDid localDid : Did) :
    scopeFilter (.perCollection subagentCoordinatorRules) [] peerDid localDid
      = [ { collection := "AgentToolCall", field := "spawn_target_did", value := peerDid } ] := by
  simp [scopeFilter, subagentCoordinatorRules]

theorem subagentHost_filter_eq (peerDid localDid : Did) :
    scopeFilter (.perCollection subagentHostRules) [] peerDid localDid
      = [ { collection := "AgentRequest",      field := "requester_did", value := peerDid }
        , { collection := "AgentResponse",     field := "requester_did", value := peerDid }
        , { collection := "AgentResponseOutcome", field := "requester_did", value := peerDid }
        , { collection := "AgentMessage",      field := "requester_did", value := peerDid }
        , { collection := "AgentToolCall",     field := "requester_did", value := peerDid }
        , { collection := "AgentToolApproval", field := "requester_did", value := peerDid } ] := by
  simp [scopeFilter, subagentHostRules, subagentHostCollections]

theorem schedulerOwner_filter_eq (peerDid localDid : Did) :
    scopeFilter (.perCollection schedulerOwnerRules) [] peerDid localDid
      = [ { collection := "EventTriggerActivation", field := "agent_did", value := localDid }
        , { collection := "EventDeliveryAdmission", field := "agent_did", value := localDid } ] := by
  simp [scopeFilter, schedulerOwnerRules]

theorem schedulerOwner_is_owner_scoped_replicate :
    schedulerOwnerTemplate.delivery = .replicate
      ∧ schedulerOwnerTemplate.scope = .perCollection schedulerOwnerRules := by
  simp [schedulerOwnerTemplate]

theorem subagentHost_filters_requester_lineage (peerDid localDid : Did) :
    (scopeFilter subagentHostTemplate.scope [] peerDid localDid).all
      (fun k => k.field = "requester_did" ∧ k.value = peerDid) = true := by
  simp [scopeFilter, subagentHostTemplate, subagentHostRules]

theorem subagentRequest_crossing_is_peer_scoped (peerDid localDid : Did) :
    (scopeFilter subagentCoordinatorTemplate.scope [] peerDid localDid).all
        (fun k => k.collection ≠ "AgentRequest") = true ∧
    (scopeFilter subagentHostTemplate.scope [] peerDid localDid).find?
        (fun k => k.collection = "AgentRequest") =
          some { collection := "AgentRequest", field := "requester_did", value := peerDid } := by
  simp [scopeFilter, subagentCoordinatorTemplate, subagentCoordinatorRules,
    subagentHostTemplate, subagentHostRules]

theorem subagentCoordinator_filters_declared_collections (peerDid localDid : Did) :
    ((scopeFilter subagentCoordinatorTemplate.scope [] peerDid localDid).map
        (fun k => k.collection)).toFinset
      = subagentCoordinatorTemplate.collections := by
  simp [scopeFilter, subagentCoordinatorTemplate, subagentCoordinatorRules]

theorem subagentHost_filters_declared_collections (peerDid localDid : Did) :
    ((scopeFilter subagentHostTemplate.scope [] peerDid localDid).map
        (fun k => k.collection)).toFinset
      = subagentHostTemplate.collections := by
  simp [scopeFilter, subagentHostTemplate, subagentHostRules,
    subagentHostCollections]

theorem subagentHost_excludes_host_local_artifacts :
    "AgentToolResult" ∉ subagentHostTemplate.collections ∧
    "AgentSession" ∉ subagentHostTemplate.collections ∧
    "AgentConversation" ∉ subagentHostTemplate.collections ∧
    "CompactionEntry" ∉ subagentHostTemplate.collections := by
  decide

theorem subagentCoordinator_in_catalog :
    resolveTemplate builtinCatalog "subagent-coordinator" = some subagentCoordinatorTemplate := by
  decide

theorem subagentHost_in_catalog :
    resolveTemplate builtinCatalog "subagent-host" = some subagentHostTemplate := by
  decide

theorem machine_in_catalog :
    resolveTemplate builtinCatalog "machine" = some machineTemplate := by
  decide

theorem appCollections_in_catalog :
    resolveTemplate builtinCatalog "app-collections" = some appCollectionsTemplate := by
  decide

theorem appCollections_collections_empty :
    appCollectionsTemplate.collections = (∅ : Finset String) := rfl

theorem appCollections_unscoped_no_filter (collections : List String) (peerDid localDid : Did) :
    scopeFilter appCollectionsTemplate.scope collections peerDid localDid = [] := rfl

theorem subagent_filter_values_local_or_peer
    (rules : List CollectionRule) (peerDid localDid : Did)
    (k : CollectionScopeFilter)
    (hk : k ∈ scopeFilter (.perCollection rules) [] peerDid localDid) :
    k.value = localDid ∨ k.value = peerDid := by
  simp [scopeFilter] at hk
  obtain ⟨r, _, hr⟩ := hk
  cases hsrc : r.source <;> simp [hsrc] at hr <;> subst hr <;> simp

end ScopeTemplates
