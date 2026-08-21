use serde::Deserialize;
use ts_rs::TS;

/// Local-runtime init request. Filesystem paths are **not** accepted from the
/// webview — they come from `BridgeConfig` resolved at plugin init.
#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct DesktopInitRequest {
    pub label: Option<String>,
    pub dangerously_overwrite: bool,
    pub reset: bool,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ManagedServerStartRequest {
    pub agent_name: String,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct PeerAddRequest {
    pub label: String,
    pub agent_did: String,
    pub addr: String,
    #[serde(default)]
    pub graphql: Option<String>,
    #[serde(default)]
    pub default_behavior_id: Option<String>,
}

/// Fetch peer runtime status by **saved peer id** only — read grants never
/// accept arbitrary addresses (SSRF).
#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct PeerStatusFetchRequest {
    pub peer_id: String,
}

/// Fleet-admin probe of an arbitrary address before the peer is saved.
/// Lives only in `fleet-admin` — not a read grant.
#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct PeerProbeRequest {
    pub server_address: String,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct BearerPairingRequest {
    pub token: String,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ChatSendRequest {
    pub agent_did: String,
    pub behavior_id: Option<String>,
    pub session_id: Option<String>,
    pub content: String,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ConversationRenameRequest {
    pub agent_did: Option<String>,
    pub session_id: String,
    pub title: String,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct AgentConfigSaveRequest {
    pub agent_did: String,
    pub display_name: String,
    pub default_behavior_id: String,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorSaveRequest {
    pub agent_did: String,
    pub behavior_id: String,
    pub display_name: String,
    pub system_prompt: String,
    pub backend_id: Option<String>,
    pub tool_selection_id: Option<String>,
    pub inference_profile_id: Option<String>,
    pub compaction_strategy: Option<String>,
    pub compaction_threshold: Option<f64>,
    pub enabled: Option<bool>,
    #[serde(default)]
    pub skill_refs: Vec<String>,
    #[serde(default)]
    pub skill_excludes: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SkillDeleteRequest {
    pub skill_id: String,
    pub agent_did: String,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct TaskDeleteRequest {
    pub task_id: String,
    pub agent_did: String,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleDeleteRequest {
    pub schedule_id: String,
    pub agent_did: String,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct EventTriggerDeleteRequest {
    pub trigger_id: String,
    pub agent_did: String,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct BackendDeleteRequest {
    pub backend_id: String,
    pub agent_did: String,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct InferenceProfileDeleteRequest {
    pub profile_id: String,
    pub agent_did: String,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ToolSelectionDeleteRequest {
    pub selection_id: String,
    pub agent_did: String,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ToolServiceDeleteRequest {
    pub service_id: String,
    pub agent_did: String,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorDeleteRequest {
    pub behavior_id: String,
    pub agent_did: String,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct BackendSaveRequest {
    pub backend_id: String,
    pub name: String,
    pub provider_kind: String,
    #[serde(default)]
    pub openai_wire_api: Option<String>,
    pub endpoint: String,
    pub api_key: Option<String>,
    pub api_key_env_var: Option<String>,
    pub clear_api_key: Option<bool>,
    pub models: Vec<String>,
    pub max_concurrent: Option<i64>,
    pub max_queue_depth: Option<i64>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct InferenceProfileSaveRequest {
    pub profile_id: String,
    pub display_name: String,
    pub context_window: Option<i64>,
    pub max_output_tokens: Option<i64>,
    pub max_turns: Option<i64>,
    pub temperature: Option<f64>,
    pub reasoning_effort: Option<String>,
    pub stream_batch_ms: Option<i64>,
    pub stream_liveness_timeout_secs: Option<i64>,
    pub deadline_duration_secs: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ToolSelectionSaveRequest {
    pub agent_did: String,
    pub selection_id: String,
    pub display_name: String,
    pub enable_file_tools: Option<bool>,
    pub file_tools_mode: Option<String>,
    pub file_tool_root: Option<String>,
    pub enable_bash: Option<bool>,
    pub bash_mode: Option<String>,
    #[serde(default)]
    pub command_execution_policy: Option<String>,
    #[serde(default)]
    pub command_allowed_argv_prefixes: Vec<String>,
    #[serde(default)]
    pub command_forbidden_argv_prefixes: Vec<String>,
    #[serde(default)]
    pub command_network_mode: Option<String>,
    pub cli_tool_names: Vec<String>,
    pub enable_meta_tools: Option<bool>,
    #[serde(default)]
    pub allowed_mcp_service_ids: Vec<String>,
    pub delegate_to: Vec<String>,
    #[serde(default)]
    pub backgroundable_tool_names: Vec<String>,
    #[serde(default)]
    pub subagent_targets: Vec<String>,
    pub subagent_spawn_enabled: Option<bool>,
    pub subagent_steering_enabled: Option<bool>,
    pub subagent_background_enabled: Option<bool>,
    pub subagent_allow_cross_deployment: Option<bool>,
    pub cross_deployment_spawn_timeout_seconds: Option<i64>,
    pub enable_memory: Option<bool>,
    #[serde(default)]
    pub enable_session_history_tool: Option<bool>,
    #[serde(default)]
    pub enable_context_budget: Option<bool>,
    #[serde(default)]
    pub enable_defra_query: Option<bool>,
    /// Editable query allowlist. `None` = field absent → preserve the stored
    /// value (so a save that doesn't touch it can't wipe it); `Some(list)` sets
    /// it (empty list clears). This is the field whose silent revert was the SP2
    /// data-loss bug.
    #[serde(default)]
    pub defra_query_collections: Option<Vec<String>>,
    #[serde(default)]
    pub subagent_default_await_mode: Option<String>,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ToolServiceSaveRequest {
    pub service_id: String,
    pub display_name: String,
    pub description: Option<String>,
    pub hostname: Option<String>,
    pub tailscale_ip: Option<String>,
    pub lan_ip: Option<String>,
    pub mcp_port: Option<i64>,
    pub mcp_path: Option<String>,
    pub status: Option<String>,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ToolServiceTestRequest {
    pub service_id: String,
    pub hostname: Option<String>,
    pub tailscale_ip: Option<String>,
    pub lan_ip: Option<String>,
    pub mcp_port: Option<i64>,
    pub mcp_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct TaskSaveRequest {
    pub task_id: String,
    pub name: String,
    pub description: Option<String>,
    pub behavior_id: String,
    pub prompt_template: String,
    pub enabled: Option<bool>,
    pub output_schema_ref: Option<String>,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SkillSaveRequest {
    pub skill_id: String,
    pub agent_did: String,
    pub scope: String,
    pub name: String,
    pub description: Option<String>,
    pub instructions: String,
    #[serde(default)]
    pub tool_refs: Vec<String>,
    pub display_name: Option<String>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct TaskRunRequest {
    pub task_id: String,
    #[ts(type = "unknown", optional)]
    pub args: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleSaveRequest {
    pub schedule_id: String,
    pub task_id: String,
    pub interval_secs: Option<i64>,
    pub cron: Option<String>,
    pub timezone: Option<String>,
    pub missed_run_policy: Option<String>,
    pub enabled: Option<bool>,
    pub concurrency: Option<String>,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleRunRequest {
    pub schedule_id: String,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct EventTriggerSaveRequest {
    pub trigger_id: String,
    pub task_id: String,
    pub source_collection: String,
    pub event_kind: String,
    pub filter: Option<String>,
    pub enabled: Option<bool>,
    pub concurrency: Option<String>,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct DesktopOperationsSnapshotRequest {
    #[serde(default)]
    pub agent_did: Option<String>,
    #[serde(default)]
    /// Accepted from the client but not yet consumed: snapshot filtering by
    /// root request / terminal inclusion is staged (operator-surfaces spec).
    #[allow(dead_code)]
    pub root_request_id: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    pub include_terminal: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct DesktopListSubagentTreeRequest {
    pub root_request_id: String,
    #[serde(default)]
    pub agent_did: Option<String>,
    #[serde(default)]
    pub include_terminal: Option<bool>,
    #[serde(default)]
    pub max_depth: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct DesktopPreviewInterruptCascadeRequest {
    pub request_id: String,
    #[serde(default)]
    pub agent_did: Option<String>,
    #[serde(default)]
    pub include_terminal: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct DesktopListHoldsRequest {
    pub agent_did: String,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct DesktopResolveHoldRequest {
    pub agent_did: String,
    pub tool_call_id: String,
    pub approve: bool,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct DesktopInterruptRequest {
    pub request_id: String,
    #[serde(default)]
    pub agent_did: Option<String>,
    /// Currently always `"userCancelled"` per spec line 907. Kept as a String
    /// so future cause variants don't require an enum migration here.
    pub cause: String,
    pub cascade: bool,
    #[serde(default)]
    pub expected_preview_signature: Option<String>,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct DesktopProbeMcpServiceRequest {
    pub service_id: String,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    macro_rules! assert_source_routed_delete_request {
        ($request_type:ty, $id_key:literal, $id_field:ident, $id:literal) => {{
            let mut value = json!({ "agentDid": "did:test:source" });
            value[$id_key] = json!($id);
            let request: $request_type = serde_json::from_value(value).unwrap();

            assert_eq!(request.$id_field, $id);
            assert_eq!(request.agent_did, "did:test:source");

            let mut missing_source = json!({});
            missing_source[$id_key] = json!($id);
            assert!(serde_json::from_value::<$request_type>(missing_source).is_err());
        }};
    }

    #[test]
    fn config_delete_requests_require_camel_case_source_agent_did() {
        assert_source_routed_delete_request!(SkillDeleteRequest, "skillId", skill_id, "skill-a");
        assert_source_routed_delete_request!(TaskDeleteRequest, "taskId", task_id, "task-a");
        assert_source_routed_delete_request!(
            ScheduleDeleteRequest,
            "scheduleId",
            schedule_id,
            "schedule-a"
        );
        assert_source_routed_delete_request!(
            EventTriggerDeleteRequest,
            "triggerId",
            trigger_id,
            "trigger-a"
        );
        assert_source_routed_delete_request!(
            BackendDeleteRequest,
            "backendId",
            backend_id,
            "backend-a"
        );
        assert_source_routed_delete_request!(
            InferenceProfileDeleteRequest,
            "profileId",
            profile_id,
            "profile-a"
        );
        assert_source_routed_delete_request!(
            ToolSelectionDeleteRequest,
            "selectionId",
            selection_id,
            "selection-a"
        );
        assert_source_routed_delete_request!(
            ToolServiceDeleteRequest,
            "serviceId",
            service_id,
            "service-a"
        );
        assert_source_routed_delete_request!(
            BehaviorDeleteRequest,
            "behaviorId",
            behavior_id,
            "behavior-a"
        );
    }
}
