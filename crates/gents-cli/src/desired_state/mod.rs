pub(crate) mod apply_bundle;
pub(crate) mod convert;
pub(crate) mod diff;
pub(crate) mod load;
pub(crate) mod normalize;
pub(crate) mod prune;
#[cfg(test)]
mod tests;
pub(crate) mod validate;
pub(crate) mod write;

pub(crate) use apply_bundle::DesiredApplyBundle;
pub(crate) use convert::{
    export_bundle_from_manifest, manifest_from_export_bundle,
    normalize_tool_service_registry_storage_fields,
};
pub(crate) use diff::diff_manifests;
pub(crate) use load::load_manifest_root;
pub(crate) use normalize::strip_deprecated_inference_backend_fields;
pub(crate) use write::write_manifest_root;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};

use gents::{BackendProviderKind, Collection};

pub(crate) const DEFAULT_TOOL_SERVICE_MCP_PATH: &str = "/mcp";
pub(crate) const TOOL_SERVICE_ADDRESS_FIELDS: &[&str] = &["hostname", "tailscale_ip", "lan_ip"];
pub(crate) const PEER_PAIRING_MANIFEST_SOURCE_PREFIX: &str =
    gents::agent::p2p_reconcile::SOURCE_MANIFEST_PREFIX;

pub(crate) fn peer_pairing_manifest_source(owner_agent_did: &str) -> String {
    format!(
        "{PEER_PAIRING_MANIFEST_SOURCE_PREFIX}{}",
        owner_agent_did.trim()
    )
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DesiredAgentPrincipal {
    pub(crate) agent_did: String,
    pub(crate) display_name: Option<String>,
    pub(crate) default_behavior_id: Option<String>,
    pub(crate) enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DesiredAgentBehavior {
    pub(crate) behavior_id: String,
    pub(crate) agent_did: String,
    pub(crate) display_name: Option<String>,
    #[serde(default)]
    pub(crate) description: Option<String>,
    #[serde(default)]
    pub(crate) summary: Option<String>,
    pub(crate) system_prompt: Option<String>,
    #[serde(default)]
    pub(crate) request_context_template: Option<String>,
    pub(crate) backend_id: Option<String>,
    pub(crate) model_name: Option<String>,
    pub(crate) tool_selection_id: Option<String>,
    pub(crate) inference_profile_id: Option<String>,
    pub(crate) compaction_strategy: Option<String>,
    pub(crate) compaction_threshold: Option<f64>,
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) skill_refs: Vec<String>,
    #[serde(default)]
    pub(crate) skill_excludes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DesiredTask {
    pub(crate) task_id: String,
    pub(crate) name: String,
    pub(crate) description: Option<String>,
    pub(crate) behavior_id: String,
    pub(crate) prompt_template: String,
    pub(crate) enabled: bool,
    pub(crate) output_schema_ref: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DesiredSchedule {
    pub(crate) schedule_id: String,
    pub(crate) task_id: String,
    #[serde(default)]
    pub(crate) interval_secs: Option<i64>,
    #[serde(default)]
    pub(crate) cron: Option<String>,
    #[serde(default)]
    pub(crate) timezone: Option<String>,
    #[serde(default)]
    pub(crate) missed_run_policy: Option<String>,
    pub(crate) enabled: bool,
    pub(crate) concurrency: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DesiredEventTrigger {
    pub(crate) trigger_id: String,
    pub(crate) task_id: String,
    pub(crate) source_collection: String,
    #[serde(default = "default_event_kind")]
    pub(crate) event_kind: String,
    #[serde(default)]
    pub(crate) filter: Option<String>,
    pub(crate) enabled: bool,
    pub(crate) concurrency: String,
}

fn default_event_kind() -> String {
    "created".to_string()
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DesiredToolSelection {
    pub(crate) selection_id: String,
    pub(crate) agent_did: String,
    pub(crate) display_name: Option<String>,
    #[serde(default)]
    pub(crate) tool_policy_version: Option<String>,
    pub(crate) enable_file_tools: bool,
    pub(crate) file_tools_mode: String,
    pub(crate) file_tool_root: Option<String>,
    pub(crate) enable_bash: bool,
    pub(crate) bash_mode: String,
    #[serde(default)]
    pub(crate) command_execution_policy: Option<String>,
    /// Argv-prefix allow gate (extend / subcommand-precise). Empty = no gate;
    /// non-empty requires every command to match a prefix. Prefer over
    /// `read_only_command_allowlist` when adding diagnostic families without
    /// replacing the base. See `docs/macos-bash-sandbox.md`.
    #[serde(default)]
    pub(crate) command_allowed_argv_prefixes: Vec<String>,
    #[serde(default)]
    pub(crate) command_forbidden_argv_prefixes: Vec<String>,
    #[serde(default)]
    pub(crate) read_only_command_allowlist: Vec<String>,
    #[serde(default)]
    pub(crate) command_network_mode: Option<String>,
    #[serde(default)]
    pub(crate) cli_tool_names: Vec<String>,
    pub(crate) enable_meta_tools: bool,
    #[serde(default)]
    pub(crate) allowed_mcp_service_ids: Vec<String>,
    #[serde(default)]
    pub(crate) delegate_to: Vec<String>,
    #[serde(default)]
    pub(crate) backgroundable_tool_names: Vec<String>,
    #[serde(default)]
    pub(crate) enable_memory: bool,
    #[serde(default)]
    pub(crate) enable_session_history_tool: bool,
    #[serde(default = "default_true")]
    pub(crate) enable_context_budget: bool,
    #[serde(default = "default_true")]
    pub(crate) enable_defra_query: bool,
    #[serde(default)]
    pub(crate) defra_query_collections: Vec<String>,
    #[serde(default)]
    pub(crate) subagent_targets: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_write_tools_storage")]
    pub(crate) write_tools: Vec<String>,
    #[serde(default)]
    pub(crate) subagent_spawn_enabled: bool,
    #[serde(default)]
    pub(crate) orchestration_enabled: bool,
    #[serde(default)]
    pub(crate) subagent_steering_enabled: bool,
    #[serde(default)]
    pub(crate) subagent_background_enabled: bool,
    #[serde(default)]
    pub(crate) subagent_default_await_mode: Option<String>,
    #[serde(default)]
    pub(crate) subagent_allow_cross_deployment: bool,
    #[serde(default)]
    pub(crate) cross_deployment_spawn_timeout_seconds: Option<i64>,
    #[serde(default)]
    pub(crate) enable_self_config: bool,
    #[serde(default)]
    pub(crate) self_config_categories: Vec<String>,
    #[serde(default)]
    pub(crate) self_config_no_lockout: bool,
    #[serde(default)]
    pub(crate) self_config_dry_run: bool,
}

fn deserialize_write_tools_storage<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    use gents::WriteToolDecl;
    use serde_json::Value;

    let value = Option::<Value>::deserialize(deserializer)?;
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let items = match value {
        Value::Null => return Ok(Vec::new()),
        Value::Array(items) => items,
        other => {
            return Err(D::Error::custom(format!(
                "write_tools must be a list of WriteToolDecl objects or JSON strings, got {other}"
            )))
        }
    };

    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let decl: WriteToolDecl = match item {
            Value::String(s) => serde_json::from_str(&s).map_err(D::Error::custom)?,
            other => serde_json::from_value(other).map_err(D::Error::custom)?,
        };
        out.push(serde_json::to_string(&decl).map_err(D::Error::custom)?);
    }
    Ok(out)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DesiredSkill {
    pub(crate) skill_id: String,
    pub(crate) agent_did: String,
    pub(crate) scope: String,
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) description: Option<String>,
    #[serde(default)]
    pub(crate) instructions: Option<String>,
    #[serde(default)]
    pub(crate) tool_refs: Vec<String>,
    #[serde(default)]
    pub(crate) display_name: Option<String>,
    #[serde(default)]
    pub(crate) interface_json: Option<String>,
    pub(crate) enabled: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct DesiredInferenceBackend {
    pub(crate) backend_id: String,
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) provider_kind: BackendProviderKind,
    #[serde(default)]
    pub(crate) openai_wire_api: Option<gents::OpenAiWireApi>,
    pub(crate) endpoint: String,
    pub(crate) api_key: Option<String>,
    pub(crate) api_key_env_var: Option<String>,
    pub(crate) max_concurrent: i64,
    #[serde(default = "normalize::default_max_queue_depth")]
    pub(crate) max_queue_depth: i64,
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) models: Vec<String>,
}

impl<'de> Deserialize<'de> for DesiredInferenceBackend {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            backend_id: String,
            name: String,
            #[serde(default)]
            provider_kind: BackendProviderKind,
            #[serde(default)]
            openai_wire_api: Option<gents::OpenAiWireApi>,
            endpoint: String,
            api_key: Option<String>,
            api_key_env_var: Option<String>,
            max_concurrent: i64,
            #[serde(default = "normalize::default_max_queue_depth")]
            max_queue_depth: i64,
            enabled: bool,
            #[serde(default)]
            models: Vec<String>,
        }

        let mut value = serde_json::Value::deserialize(deserializer)?;
        if let serde_json::Value::Object(object) = &mut value {
            normalize::strip_deprecated_inference_backend_fields(object);
        }
        let wire = Wire::deserialize(value).map_err(D::Error::custom)?;

        Ok(Self {
            backend_id: wire.backend_id,
            name: wire.name,
            provider_kind: wire.provider_kind,
            openai_wire_api: wire.openai_wire_api,
            endpoint: wire.endpoint,
            api_key: wire.api_key,
            api_key_env_var: wire.api_key_env_var,
            max_concurrent: wire.max_concurrent,
            max_queue_depth: wire.max_queue_depth,
            enabled: wire.enabled,
            models: wire.models,
        })
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DesiredInferenceProfile {
    pub(crate) profile_id: String,
    pub(crate) display_name: Option<String>,
    pub(crate) context_window: Option<i64>,
    pub(crate) max_output_tokens: Option<i64>,
    pub(crate) max_turns: Option<i64>,
    pub(crate) temperature: Option<f64>,
    pub(crate) top_p: Option<f64>,
    pub(crate) top_k: Option<i64>,
    pub(crate) seed: Option<i64>,
    pub(crate) min_p: Option<f64>,
    pub(crate) frequency_penalty: Option<f64>,
    pub(crate) presence_penalty: Option<f64>,
    pub(crate) repetition_penalty: Option<f64>,
    pub(crate) reasoning_effort: Option<String>,
    pub(crate) stream_batch_ms: Option<i64>,
    pub(crate) stream_liveness_timeout_secs: Option<i64>,
    pub(crate) deadline_duration_secs: Option<i64>,
    pub(crate) retry_max_transport: Option<i64>,
    pub(crate) retry_backoff_ms: Option<Vec<i64>>,
    pub(crate) retry_max_resample: Option<i64>,
    pub(crate) retry_allow_repair: Option<bool>,
    pub(crate) retry_interactive_max: Option<i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct DesiredToolServiceRegistry {
    pub(crate) service_id: String,
    pub(crate) display_name: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) hostname: Option<String>,
    pub(crate) tailscale_ip: Option<String>,
    pub(crate) lan_ip: Option<String>,
    pub(crate) mcp_port: Option<i64>,
    pub(crate) mcp_path: Option<String>,
    pub(crate) send_agent_did: bool,
}

impl<'de> Deserialize<'de> for DesiredToolServiceRegistry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            service_id: String,
            display_name: Option<String>,
            description: Option<String>,
            hostname: Option<String>,
            tailscale_ip: Option<String>,
            lan_ip: Option<String>,
            mcp_port: Option<i64>,
            mcp_path: Option<String>,
            #[serde(default)]
            send_agent_did: bool,
        }

        let wire = Wire::deserialize(deserializer)?;
        Ok(Self {
            service_id: wire.service_id,
            display_name: wire.display_name,
            description: wire.description,
            hostname: Some(validate::normalize_tool_service_string(wire.hostname)),
            tailscale_ip: Some(validate::normalize_tool_service_string(wire.tailscale_ip)),
            lan_ip: Some(validate::normalize_tool_service_string(wire.lan_ip)),
            mcp_port: wire.mcp_port,
            mcp_path: Some(validate::normalize_tool_service_mcp_path(wire.mcp_path)),
            send_agent_did: wire.send_agent_did,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DesiredProjectionAcpBinding {
    pub(crate) binding_id: String,
    #[serde(default)]
    pub(crate) agent_did: Option<String>,
    #[serde(default)]
    pub(crate) behavior_id: Option<String>,
    #[serde(default)]
    pub(crate) projection_id: Option<String>,
    pub(crate) policy_id: String,
    #[serde(default)]
    pub(crate) staged_policy_id: Option<String>,
    #[serde(default)]
    pub(crate) previous_policy_id: Option<String>,
    #[serde(default)]
    pub(crate) resource_map_json: Option<String>,
    #[serde(default)]
    pub(crate) publication_status: Option<String>,
    #[serde(default)]
    pub(crate) published_at: Option<String>,
    pub(crate) enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DesiredPeerPairing {
    pub(crate) peer_did: String,
    pub(crate) addresses: Vec<String>,
    pub(crate) template: String,
    pub(crate) enabled: bool,
    #[serde(skip)]
    pub(crate) peer_id: String,
}

impl DesiredPeerPairing {
    pub(crate) fn resolved_peer_id(&self) -> Option<String> {
        let stored = self.peer_id.trim();
        if !stored.is_empty() {
            return Some(stored.to_string());
        }
        self.addresses.iter().find_map(|address| {
            p2p::iroh::parse_public_peer_addr(address.trim())
                .ok()
                .map(|(peer_id, _)| peer_id.to_string())
        })
    }

    fn normalized_addresses(&self) -> Vec<(String, Vec<String>)> {
        let mut normalized = self
            .addresses
            .iter()
            .map(|address| {
                p2p::iroh::parse_public_peer_addr(address.trim())
                    .map(|(peer_id, addresses)| {
                        let mut addresses = addresses
                            .iter()
                            .map(|address| address.as_str().to_string())
                            .collect::<Vec<_>>();
                        addresses.sort();
                        (peer_id.to_string(), addresses)
                    })
                    .unwrap_or_else(|_| (address.trim().to_string(), Vec::new()))
            })
            .collect::<Vec<_>>();
        normalized.sort();
        normalized.dedup();
        normalized
    }
}

impl PartialEq for DesiredPeerPairing {
    fn eq(&self, other: &Self) -> bool {
        self.peer_did.trim() == other.peer_did.trim()
            && self.template.trim() == other.template.trim()
            && self.enabled == other.enabled
            && self.resolved_peer_id() == other.resolved_peer_id()
            && self.normalized_addresses() == other.normalized_addresses()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DesiredStateManifest {
    pub(crate) agent_principal: DesiredAgentPrincipal,
    pub(crate) agent_behaviors: Vec<DesiredAgentBehavior>,
    pub(crate) skills: Vec<DesiredSkill>,
    pub(crate) tool_selections: Vec<DesiredToolSelection>,
    pub(crate) inference_backends: Vec<DesiredInferenceBackend>,
    pub(crate) inference_profiles: Vec<DesiredInferenceProfile>,
    pub(crate) tool_service_registries: Vec<DesiredToolServiceRegistry>,
    pub(crate) projection_acp_bindings: Vec<DesiredProjectionAcpBinding>,
    pub(crate) peer_pairings: Vec<DesiredPeerPairing>,
    pub(crate) tasks: Vec<DesiredTask>,
    pub(crate) schedules: Vec<DesiredSchedule>,
    pub(crate) event_triggers: Vec<DesiredEventTrigger>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DesiredStateCollectionDiff {
    pub(crate) create: Vec<String>,
    pub(crate) update: Vec<String>,
    pub(crate) delete: Vec<String>,
    pub(crate) unchanged: Vec<String>,
    pub(crate) live_only: Vec<String>,
}

impl DesiredStateCollectionDiff {
    pub(super) fn counts(&self) -> DesiredStateDiffCounts {
        DesiredStateDiffCounts {
            create: self.create.len(),
            update: self.update.len(),
            delete: self.delete.len(),
            unchanged: self.unchanged.len(),
            live_only: self.live_only.len(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DesiredStateDiffCounts {
    pub(crate) create: usize,
    pub(crate) update: usize,
    pub(crate) delete: usize,
    pub(crate) unchanged: usize,
    pub(crate) live_only: usize,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DesiredStateDiffCollections {
    pub(crate) agent_principal: DesiredStateCollectionDiff,
    pub(crate) agent_behaviors: DesiredStateCollectionDiff,
    pub(crate) skills: DesiredStateCollectionDiff,
    // WorkspaceRoot has no desired-state file/GraphQL wiring yet (not part
    // of Collection::ALL) — always empty until that CRUD surface lands.
    pub(crate) workspace_roots: DesiredStateCollectionDiff,
    pub(crate) tool_selections: DesiredStateCollectionDiff,
    pub(crate) inference_backends: DesiredStateCollectionDiff,
    pub(crate) inference_profiles: DesiredStateCollectionDiff,
    pub(crate) tool_service_registries: DesiredStateCollectionDiff,
    pub(crate) projection_acp_bindings: DesiredStateCollectionDiff,
    pub(crate) peer_pairings: DesiredStateCollectionDiff,
    pub(crate) tasks: DesiredStateCollectionDiff,
    pub(crate) schedules: DesiredStateCollectionDiff,
    pub(crate) event_triggers: DesiredStateCollectionDiff,
}

impl DesiredStateDiffCollections {
    pub(crate) fn get(&self, collection: Collection) -> &DesiredStateCollectionDiff {
        match collection {
            Collection::AgentPrincipal => &self.agent_principal,
            Collection::AgentBehavior => &self.agent_behaviors,
            Collection::Skill => &self.skills,
            Collection::WorkspaceRoot => &self.workspace_roots,
            Collection::ToolSelection => &self.tool_selections,
            Collection::InferenceBackend => &self.inference_backends,
            Collection::InferenceProfile => &self.inference_profiles,
            Collection::ToolServiceRegistry => &self.tool_service_registries,
            Collection::ProjectionAcpBinding => &self.projection_acp_bindings,
            Collection::PeerPairingDesired => &self.peer_pairings,
            Collection::Task => &self.tasks,
            Collection::Schedule => &self.schedules,
            Collection::EventTrigger => &self.event_triggers,
        }
    }

    fn get_mut(&mut self, collection: Collection) -> &mut DesiredStateCollectionDiff {
        match collection {
            Collection::AgentPrincipal => &mut self.agent_principal,
            Collection::AgentBehavior => &mut self.agent_behaviors,
            Collection::Skill => &mut self.skills,
            Collection::WorkspaceRoot => &mut self.workspace_roots,
            Collection::ToolSelection => &mut self.tool_selections,
            Collection::InferenceBackend => &mut self.inference_backends,
            Collection::InferenceProfile => &mut self.inference_profiles,
            Collection::ToolServiceRegistry => &mut self.tool_service_registries,
            Collection::ProjectionAcpBinding => &mut self.projection_acp_bindings,
            Collection::PeerPairingDesired => &mut self.peer_pairings,
            Collection::Task => &mut self.tasks,
            Collection::Schedule => &mut self.schedules,
            Collection::EventTrigger => &mut self.event_triggers,
        }
    }

    pub(crate) fn record_prune_deletes(&mut self, deletes: &[gents::apply_model::DocRef]) {
        for doc in deletes {
            let diff = self.get_mut(doc.collection);
            diff.live_only.retain(|id| id != &doc.id);
            if !diff.delete.contains(&doc.id) {
                diff.delete.push(doc.id.clone());
            }
        }
    }

    pub(crate) fn counts(&self) -> DesiredStateDiffCollectionsCounts {
        DesiredStateDiffCollectionsCounts {
            agent_principal: self.agent_principal.counts(),
            agent_behaviors: self.agent_behaviors.counts(),
            skills: self.skills.counts(),
            workspace_roots: self.workspace_roots.counts(),
            tool_selections: self.tool_selections.counts(),
            inference_backends: self.inference_backends.counts(),
            inference_profiles: self.inference_profiles.counts(),
            tool_service_registries: self.tool_service_registries.counts(),
            projection_acp_bindings: self.projection_acp_bindings.counts(),
            peer_pairings: self.peer_pairings.counts(),
            tasks: self.tasks.counts(),
            schedules: self.schedules.counts(),
            event_triggers: self.event_triggers.counts(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DesiredStateDiffCollectionsCounts {
    pub(crate) agent_principal: DesiredStateDiffCounts,
    pub(crate) agent_behaviors: DesiredStateDiffCounts,
    pub(crate) skills: DesiredStateDiffCounts,
    pub(crate) workspace_roots: DesiredStateDiffCounts,
    pub(crate) tool_selections: DesiredStateDiffCounts,
    pub(crate) inference_backends: DesiredStateDiffCounts,
    pub(crate) inference_profiles: DesiredStateDiffCounts,
    pub(crate) tool_service_registries: DesiredStateDiffCounts,
    pub(crate) projection_acp_bindings: DesiredStateDiffCounts,
    pub(crate) peer_pairings: DesiredStateDiffCounts,
    pub(crate) tasks: DesiredStateDiffCounts,
    pub(crate) schedules: DesiredStateDiffCounts,
    pub(crate) event_triggers: DesiredStateDiffCounts,
}

impl DesiredStateDiffCollectionsCounts {
    pub(crate) fn iter(&self) -> impl Iterator<Item = &DesiredStateDiffCounts> {
        Collection::ALL
            .iter()
            .copied()
            .map(|collection| self.get(collection))
    }

    pub(crate) fn get(&self, collection: Collection) -> &DesiredStateDiffCounts {
        match collection {
            Collection::AgentPrincipal => &self.agent_principal,
            Collection::AgentBehavior => &self.agent_behaviors,
            Collection::Skill => &self.skills,
            Collection::WorkspaceRoot => &self.workspace_roots,
            Collection::ToolSelection => &self.tool_selections,
            Collection::InferenceBackend => &self.inference_backends,
            Collection::InferenceProfile => &self.inference_profiles,
            Collection::ToolServiceRegistry => &self.tool_service_registries,
            Collection::ProjectionAcpBinding => &self.projection_acp_bindings,
            Collection::PeerPairingDesired => &self.peer_pairings,
            Collection::Task => &self.tasks,
            Collection::Schedule => &self.schedules,
            Collection::EventTrigger => &self.event_triggers,
        }
    }

    pub(crate) fn is_exact_match(&self) -> bool {
        self.iter().all(|count| {
            count.create == 0 && count.update == 0 && count.delete == 0 && count.live_only == 0
        })
    }

    pub(crate) fn has_pending_apply(&self) -> bool {
        self.iter()
            .any(|count| count.create > 0 || count.update > 0 || count.delete > 0)
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DesiredStateDiffReport {
    pub(crate) status: &'static str,
    pub(crate) ok: bool,
    pub(crate) root: String,
    pub(crate) access_mode: String,
    pub(crate) agent_did: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) live_validation_errors: Vec<String>,
    pub(crate) counts: DesiredStateDiffCollectionsCounts,
    pub(crate) collections: DesiredStateDiffCollections,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DesiredStateCounts {
    pub(crate) agent_principal: usize,
    pub(crate) agent_behaviors: usize,
    pub(crate) skills: usize,
    pub(crate) tool_selections: usize,
    pub(crate) inference_backends: usize,
    pub(crate) inference_profiles: usize,
    pub(crate) tool_service_registries: usize,
    pub(crate) projection_acp_bindings: usize,
    pub(crate) peer_pairings: usize,
    pub(crate) tasks: usize,
    pub(crate) schedules: usize,
    pub(crate) event_triggers: usize,
}

impl DesiredStateCounts {
    pub(crate) fn empty() -> Self {
        Self {
            agent_principal: 0,
            agent_behaviors: 0,
            skills: 0,
            tool_selections: 0,
            inference_backends: 0,
            inference_profiles: 0,
            tool_service_registries: 0,
            projection_acp_bindings: 0,
            peer_pairings: 0,
            tasks: 0,
            schedules: 0,
            event_triggers: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DesiredStateValidationReport {
    pub(crate) status: &'static str,
    pub(crate) ok: bool,
    pub(crate) root: String,
    pub(crate) agent_did: Option<String>,
    pub(crate) counts: DesiredStateCounts,
    pub(crate) errors: Vec<String>,
}

impl DesiredStateValidationReport {
    pub(crate) fn is_ok(&self) -> bool {
        self.ok
    }
}

use gents::DesiredFields;

impl DesiredFields for DesiredAgentPrincipal {
    fn collection_tag(&self) -> &'static str {
        "agent_principal"
    }
}
impl DesiredFields for DesiredAgentBehavior {
    fn collection_tag(&self) -> &'static str {
        "agent_behaviors"
    }
}
impl DesiredFields for DesiredToolSelection {
    fn collection_tag(&self) -> &'static str {
        "tool_selections"
    }
}
impl DesiredFields for DesiredSkill {
    fn collection_tag(&self) -> &'static str {
        "skills"
    }
}
impl DesiredFields for DesiredInferenceBackend {
    fn collection_tag(&self) -> &'static str {
        "inference_backends"
    }
}
impl DesiredFields for DesiredInferenceProfile {
    fn collection_tag(&self) -> &'static str {
        "inference_profiles"
    }
}
impl DesiredFields for DesiredToolServiceRegistry {
    fn collection_tag(&self) -> &'static str {
        "tool_service_registries"
    }
}
impl DesiredFields for DesiredProjectionAcpBinding {
    fn collection_tag(&self) -> &'static str {
        "projection_acp_bindings"
    }
}
impl DesiredFields for DesiredPeerPairing {
    fn collection_tag(&self) -> &'static str {
        "peer_pairings"
    }
}
impl DesiredFields for DesiredTask {
    fn collection_tag(&self) -> &'static str {
        "tasks"
    }
}
impl DesiredFields for DesiredSchedule {
    fn collection_tag(&self) -> &'static str {
        "schedules"
    }
}
impl DesiredFields for DesiredEventTrigger {
    fn collection_tag(&self) -> &'static str {
        "event_triggers"
    }
}

#[allow(dead_code)]
pub(crate) trait HasUniqueId {
    fn unique_id(&self) -> &str;
}

impl HasUniqueId for DesiredAgentBehavior {
    fn unique_id(&self) -> &str {
        &self.behavior_id
    }
}
impl HasUniqueId for DesiredToolSelection {
    fn unique_id(&self) -> &str {
        &self.selection_id
    }
}
impl HasUniqueId for DesiredSkill {
    fn unique_id(&self) -> &str {
        &self.skill_id
    }
}
impl HasUniqueId for DesiredInferenceBackend {
    fn unique_id(&self) -> &str {
        &self.backend_id
    }
}
impl HasUniqueId for DesiredInferenceProfile {
    fn unique_id(&self) -> &str {
        &self.profile_id
    }
}
impl HasUniqueId for DesiredToolServiceRegistry {
    fn unique_id(&self) -> &str {
        &self.service_id
    }
}
impl HasUniqueId for DesiredProjectionAcpBinding {
    fn unique_id(&self) -> &str {
        &self.binding_id
    }
}
impl HasUniqueId for DesiredPeerPairing {
    fn unique_id(&self) -> &str {
        &self.peer_did
    }
}
impl HasUniqueId for DesiredTask {
    fn unique_id(&self) -> &str {
        &self.task_id
    }
}
impl HasUniqueId for DesiredSchedule {
    fn unique_id(&self) -> &str {
        &self.schedule_id
    }
}
impl HasUniqueId for DesiredEventTrigger {
    fn unique_id(&self) -> &str {
        &self.trigger_id
    }
}

#[cfg(test)]
mod desired_fields_tests {
    use super::*;
    use gents::DesiredFields;

    #[test]
    fn desired_structs_report_their_collection_tags() {
        let p = DesiredAgentPrincipal {
            agent_did: "did:x".into(),
            display_name: None,
            default_behavior_id: None,
            enabled: true,
        };
        assert_eq!(p.collection_tag(), "agent_principal");
    }
}
