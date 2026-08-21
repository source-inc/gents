use gents_desktop_core::client::BearerPairingResult;
use serde::Serialize;
use ts_rs::TS;

use gents_desktop_core::client::PeerMutationResult;

use super::bootstrap::DesktopBootstrapSummary;

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct P2PHealthView {
    pub status: String,
    pub connected_peer_count: usize,
    pub replicator_count: usize,
    pub consecutive_failures: u32,
    pub last_error: Option<String>,
    pub last_ok_at: Option<String>,
    pub last_failure_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeView {
    pub process_state: Option<String>,
    pub reconcile_phase: Option<String>,
    pub last_reconcile_result: Option<String>,
    pub last_reconcile_error: Option<String>,
    pub updated_at: Option<String>,
    pub behavior_executor_capacity: Option<i64>,
    pub behavior_executor_queue_depth: Option<i64>,
    pub runnable_behavior_count: Option<i64>,
    pub unavailable_behavior_count: Option<i64>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct AgentPrincipalView {
    pub agent_did: String,
    pub display_name: Option<String>,
    pub default_behavior_id: Option<String>,
    pub enabled: Option<bool>,
    pub created_at: Option<String>,
    pub created_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorView {
    pub behavior_id: String,
    pub display_name: String,
    pub system_prompt: Option<String>,
    pub backend_id: Option<String>,
    pub model_name: Option<String>,
    pub tool_selection_id: Option<String>,
    pub inference_profile_id: Option<String>,
    pub compaction_strategy: Option<String>,
    pub compaction_threshold: Option<f64>,
    pub enabled: bool,
    pub is_default: bool,
    pub skill_refs: Vec<String>,
    pub skill_excludes: Vec<String>,
}

/// Resolved, presentation-safe description of a configured behavior environment.
///
/// AgentBehavior stores references to shared configuration documents. Clients
/// should not have to repeat those joins (or infer tool semantics), so the
/// bridge materializes the environment once alongside the raw configuration
/// projection.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorEnvironmentView {
    pub behavior_id: String,
    pub display_name: String,
    pub enabled: bool,
    pub is_default: bool,
    pub model_name: Option<String>,
    pub inference_profile_name: Option<String>,
    pub workspace_root: Option<String>,
    pub file_access: String,
    pub bash_access: String,
    pub network_access: Option<String>,
    pub skill_names: Vec<String>,
    pub session_count: usize,
    pub active_session_count: usize,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct InferenceBackendView {
    pub backend_id: String,
    pub name: Option<String>,
    pub provider_kind: Option<String>,
    pub openai_wire_api: Option<String>,
    pub endpoint: Option<String>,
    pub api_key_configured: bool,
    pub api_key_env_var: Option<String>,
    pub max_concurrent: Option<i64>,
    pub max_queue_depth: Option<i64>,
    pub enabled: Option<bool>,
    pub models: Vec<String>,
    pub probe_status: Option<String>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct InferenceProfileView {
    pub profile_id: String,
    pub display_name: Option<String>,
    pub context_window: Option<i64>,
    pub max_output_tokens: Option<i64>,
    pub max_turns: Option<i64>,
    pub temperature: Option<f64>,
    pub reasoning_effort: Option<String>,
    pub stream_batch_ms: Option<i64>,
    pub stream_liveness_timeout_secs: Option<i64>,
    pub deadline_duration_secs: Option<i64>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ToolSelectionView {
    pub selection_id: String,
    pub agent_did: Option<String>,
    pub display_name: Option<String>,
    pub enable_file_tools: Option<bool>,
    pub file_tools_mode: Option<String>,
    pub file_tool_root: Option<String>,
    pub enable_bash: Option<bool>,
    pub bash_mode: Option<String>,
    pub command_execution_policy: Option<String>,
    pub command_allowed_argv_prefixes: Vec<String>,
    pub command_forbidden_argv_prefixes: Vec<String>,
    pub command_network_mode: Option<String>,
    pub cli_tool_names: Vec<String>,
    pub enable_meta_tools: Option<bool>,
    pub allowed_mcp_service_ids: Vec<String>,
    pub delegate_to: Vec<String>,
    pub backgroundable_tool_names: Vec<String>,
    pub subagent_targets: Vec<String>,
    pub subagent_spawn_enabled: Option<bool>,
    pub subagent_steering_enabled: Option<bool>,
    pub subagent_background_enabled: Option<bool>,
    pub subagent_allow_cross_deployment: Option<bool>,
    pub cross_deployment_spawn_timeout_seconds: Option<i64>,
    pub enable_memory: Option<bool>,
    pub enable_session_history_tool: Option<bool>,
    pub enable_context_budget: Option<bool>,
    pub enable_defra_query: Option<bool>,
    pub defra_query_collections: Vec<String>,
    pub write_tools: Vec<String>,
    pub tool_policy_version: Option<String>,
    pub subagent_default_await_mode: Option<String>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ToolServiceRegistryView {
    pub service_id: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub hostname: Option<String>,
    pub tailscale_ip: Option<String>,
    pub lan_ip: Option<String>,
    pub mcp_port: Option<i64>,
    pub mcp_path: Option<String>,
    pub status: Option<String>,
    pub version: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct TaskView {
    pub task_id: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub behavior_id: Option<String>,
    pub prompt_template: Option<String>,
    pub enabled: Option<bool>,
    pub output_schema_ref: Option<String>,
    pub recent_runs: TaskRecentRunsView,
    pub run_history: Vec<TaskRunSummaryView>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct TaskRecentRunsView {
    pub total_fires: u64,
    pub last_attempt_at: Option<String>,
    pub last_status: Option<String>,
    pub last_error: Option<String>,
    pub schedule_count: usize,
    pub event_trigger_count: usize,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct TaskRunSummaryView {
    pub request_id: String,
    pub session_id: Option<String>,
    pub behavior_id: Option<String>,
    pub status: Option<String>,
    pub lifecycle_state: Option<String>,
    pub execution_origin: Option<String>,
    pub caused_by_trigger_id: Option<String>,
    pub caused_by_trigger_kind: Option<String>,
    pub created_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SkillView {
    pub skill_id: String,
    pub agent_did: Option<String>,
    pub scope: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub instructions: Option<String>,
    pub tool_refs: Vec<String>,
    pub display_name: Option<String>,
    pub enabled: Option<bool>,
    pub created_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleView {
    pub schedule_id: String,
    pub task_id: Option<String>,
    pub interval_secs: Option<i64>,
    pub cron: Option<String>,
    pub timezone: Option<String>,
    pub missed_run_policy: Option<String>,
    pub enabled: Option<bool>,
    pub concurrency: Option<String>,
    pub next_run_at: Option<String>,
    pub last_attempt_at: Option<String>,
    pub last_status: Option<String>,
    pub last_error: Option<String>,
    pub fire_count: Option<i64>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct EventTriggerView {
    pub trigger_id: String,
    pub task_id: Option<String>,
    pub source_collection: Option<String>,
    pub event_kind: Option<String>,
    pub filter: Option<String>,
    pub enabled: Option<bool>,
    pub concurrency: Option<String>,
    pub last_attempt_at: Option<String>,
    pub last_fired_source_doc_id: Option<String>,
    pub last_status: Option<String>,
    pub last_error: Option<String>,
    pub fire_count: Option<i64>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ConversationSummary {
    pub session_id: String,
    pub title: Option<String>,
    pub preview_text: Option<String>,
    pub status: Option<String>,
    pub behavior_id: Option<String>,
    pub latest_request_id: Option<String>,
    pub task_id: Option<String>,
    pub task_name: Option<String>,
    pub trigger_id: Option<String>,
    pub trigger_kind: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub turn_state: Option<String>,
    pub message_count: usize,
    pub tool_call_count: usize,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct DeploymentView {
    pub peer_id: String,
    pub label: String,
    pub agent_did: String,
    pub addr: String,
    pub source: Option<String>,
    pub graphql: Option<String>,
    pub dial_succeeded: bool,
    pub pairing_ready: bool,
    pub last_error: Option<String>,
    pub default_behavior_id: Option<String>,
    pub agent_principal: AgentPrincipalView,
    pub runtime: Option<RuntimeView>,
    pub behaviors: Vec<BehaviorView>,
    pub behavior_environments: Vec<BehaviorEnvironmentView>,
    pub inference_backends: Vec<InferenceBackendView>,
    pub inference_profiles: Vec<InferenceProfileView>,
    pub tool_selections: Vec<ToolSelectionView>,
    pub tool_service_registries: Vec<ToolServiceRegistryView>,
    pub skills: Vec<SkillView>,
    pub tasks: Vec<TaskView>,
    pub schedules: Vec<ScheduleView>,
    pub event_triggers: Vec<EventTriggerView>,
    pub conversations: Vec<ConversationSummary>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct DesktopRuntimeSnapshot {
    pub local_peer_id: String,
    pub listen_addresses: Vec<String>,
    pub p2p_health: P2PHealthView,
    pub bootstrap_errors: Vec<String>,
    pub last_mutation_error: Option<String>,
    pub focused_request_id: Option<String>,
    pub configured_peer_count: usize,
    pub dialed_peer_count: usize,
    pub peer_issue_count: usize,
    pub row_count: usize,
    pub approx_serialized_bytes: usize,
    pub deployments: Vec<DeploymentView>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct DesktopClientSnapshot {
    pub bootstrap: DesktopBootstrapSummary,
    pub client: Option<DesktopRuntimeSnapshot>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct PeerMutationView {
    pub peer_id: String,
    pub label: String,
    pub addr: String,
    pub connected: bool,
    pub warning: Option<String>,
}

impl From<PeerMutationResult> for PeerMutationView {
    fn from(result: PeerMutationResult) -> Self {
        Self {
            peer_id: result.peer_id,
            label: result.label,
            addr: result.addr,
            connected: result.connected,
            warning: result.warning,
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct PeerRemoveResponse {
    #[serde(flatten)]
    pub snapshot: DesktopClientSnapshot,
    pub mutation: PeerMutationView,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct BearerPairingView {
    pub peer_id: String,
    pub label: String,
    pub addr: String,
    pub issuer_did: String,
    pub claimant_did: String,
    pub network_id: String,
    pub template: String,
    pub connected: bool,
    pub claim_submitted: bool,
    pub endpoint_published: bool,
    pub replication_configured: bool,
    pub membership_observed: bool,
    pub bidirectional_replication_observed: bool,
}

impl From<BearerPairingResult> for BearerPairingView {
    fn from(result: BearerPairingResult) -> Self {
        Self {
            peer_id: result.peer_id,
            label: result.label,
            addr: result.addr,
            issuer_did: result.issuer_did,
            claimant_did: result.claimant_did,
            network_id: result.network_id,
            template: result.template,
            connected: result.connected,
            claim_submitted: result.claim_submitted,
            endpoint_published: result.endpoint_published,
            replication_configured: result.replication_configured,
            membership_observed: result.membership_observed,
            bidirectional_replication_observed: result.bidirectional_replication_observed,
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct BearerPairingResponse {
    #[serde(flatten)]
    pub snapshot: DesktopClientSnapshot,
    pub pairing: BearerPairingView,
}

impl BearerPairingResponse {
    pub fn new(snapshot: DesktopClientSnapshot, pairing: BearerPairingResult) -> Self {
        Self {
            snapshot,
            pairing: pairing.into(),
        }
    }
}

impl PeerRemoveResponse {
    pub fn new(snapshot: DesktopClientSnapshot, mutation: PeerMutationResult) -> Self {
        Self {
            snapshot,
            mutation: mutation.into(),
        }
    }
}

#[cfg(test)]
mod peer_remove_response_tests {
    use super::*;

    #[test]
    fn response_preserves_snapshot_shape_and_surfaces_mutation_result() {
        let snapshot = DesktopClientSnapshot {
            bootstrap: DesktopBootstrapSummary {
                default_agent_home: "/agent".to_string(),
                init_agent_name: None,
                init_agent_did: None,
                init_tool_ceiling: None,
                init_tool_root: None,
                desktop_home: "/desktop".to_string(),
                peer_directory_path: "/desktop/peers.json".to_string(),
                node_data_dir: "/desktop/node".to_string(),
                log_file_path: "/desktop/desktop.log".to_string(),
                agent_home_exists: true,
                desktop_home_exists: true,
                peer_directory_exists: true,
                saved_peers: Vec::new(),
            },
            client: None,
        };
        let response = PeerRemoveResponse::new(
            snapshot,
            PeerMutationResult {
                peer_id: "peer-1".to_string(),
                label: "Workshop".to_string(),
                addr: "iroh://peer-1".to_string(),
                connected: false,
                warning: Some("partial cleanup".to_string()),
            },
        );

        let value = serde_json::to_value(response).expect("serialize peer remove response");
        assert_eq!(value["bootstrap"]["desktopHome"], "/desktop");
        assert!(value["client"].is_null());
        assert_eq!(value["mutation"]["peerId"], "peer-1");
        assert_eq!(value["mutation"]["warning"], "partial cleanup");
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct NetworkStatusView {
    pub local_peer_id: Option<String>,
    pub local_peer_id_error: Option<String>,
    pub listen_addresses: Vec<String>,
    pub listen_addresses_error: Option<String>,
    pub connected_peers: Vec<String>,
    pub connected_peers_error: Option<String>,
    pub replicators: Vec<NetworkReplicatorView>,
    pub replicators_error: Option<String>,
    pub saved_peers: Vec<NetworkSavedPeerView>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct NetworkReplicatorView {
    pub peer_id: Option<String>,
    pub address: Option<String>,
    pub collections: Vec<String>,
    pub status: Option<u8>,
    pub last_status_change: Option<String>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct NetworkSavedPeerView {
    pub peer_id: String,
    pub label: String,
    pub addr: String,
    pub agent_did: String,
    pub source: Option<String>,
}
