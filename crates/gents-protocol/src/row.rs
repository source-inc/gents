//! Serde mirrors for replicated collection rows.
//!
//! These types are deliberately permissive: stable identity keys remain
//! required, while other nullable scalars are wrapped in `Option<T>` because
//! DefraDB may omit unpopulated fields from GraphQL responses. Collection/list
//! fields use a custom deserializer so both missing arrays and explicit `null`
//! values deserialize as empty vectors. Callers should treat these as the wire
//! shape, not a runtime invariant.

use std::fmt;

use serde::de::{SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

fn deserialize_string_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    struct StringVecVisitor;

    impl<'de> Visitor<'de> for StringVecVisitor {
        type Value = Vec<String>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a string list, null, or empty string")
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(Vec::new())
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(Vec::new())
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if value.trim().is_empty() {
                Ok(Vec::new())
            } else {
                Ok(vec![value.to_string()])
            }
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            self.visit_str(&value)
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut values = Vec::new();
            while let Some(value) = seq.next_element::<String>()? {
                values.push(value);
            }
            Ok(values)
        }
    }

    deserializer.deserialize_any(StringVecVisitor)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentPrincipalRow {
    pub agent_did: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub default_behavior_id: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub created_by: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentBehaviorRow {
    pub behavior_id: String,
    #[serde(default)]
    pub agent_did: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub backend_id: Option<String>,
    #[serde(default)]
    pub model_name: Option<String>,
    #[serde(default)]
    pub tool_selection_id: Option<String>,
    #[serde(default)]
    pub inference_profile_id: Option<String>,
    #[serde(default)]
    pub compaction_strategy: Option<String>,
    #[serde(default)]
    pub compaction_threshold: Option<f64>,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_string_vec")]
    pub skill_refs: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_string_vec")]
    pub skill_excludes: Vec<String>,
    #[serde(default)]
    pub created_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRuntimeRow {
    pub agent_did: String,
    #[serde(default)]
    pub process_state: Option<String>,
    #[serde(default)]
    pub reconcile_phase: Option<String>,
    #[serde(default)]
    pub active_generation: Option<i64>,
    #[serde(default)]
    pub router_generation: Option<i64>,
    #[serde(default)]
    pub default_behavior_id: Option<String>,
    #[serde(default)]
    pub runnable_behavior_count: Option<i64>,
    #[serde(default)]
    pub unavailable_behavior_count: Option<i64>,
    #[serde(default)]
    pub behavior_executor_capacity: Option<i64>,
    #[serde(default)]
    pub behavior_executor_queue_depth: Option<i64>,
    #[serde(default)]
    pub behavior_executor_status_json: Option<String>,
    #[serde(default)]
    pub last_reconcile_result: Option<String>,
    #[serde(default)]
    pub last_reconcile_error: Option<String>,
    #[serde(default)]
    pub last_reconcile_completed_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentMemoryRow {
    pub memory_id: String,
    #[serde(default)]
    pub agent_did: Option<String>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentConversationRow {
    pub session_id: String,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub agent_did: Option<String>,
    #[serde(default)]
    pub requester_did: Option<String>,
    #[serde(default)]
    pub behavior_id: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub title_source: Option<String>,
    #[serde(default)]
    pub preview_text: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub latest_request_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRequestRow {
    pub request_id: String,
    #[serde(default)]
    pub agent_did: Option<String>,
    #[serde(default)]
    pub requester_did: Option<String>,
    #[serde(default)]
    pub behavior_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub retry_parent_request: Option<String>,
    #[serde(default)]
    pub retry_root_request: Option<String>,
    #[serde(default)]
    pub superseded_by_request: Option<String>,
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub temperature: Option<f64>,
    #[serde(default)]
    pub top_p: Option<f64>,
    #[serde(default)]
    pub top_k: Option<i64>,
    #[serde(default)]
    pub seed: Option<i64>,
    #[serde(default)]
    pub max_tokens: Option<i64>,
    #[serde(default)]
    pub max_total_tokens: Option<i64>,
    #[serde(default)]
    pub metadata: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub lifecycle_state: Option<String>,
    #[serde(default)]
    pub backend_id: Option<String>,
    #[serde(default)]
    pub execution_origin: Option<String>,
    #[serde(default)]
    pub caused_by_trigger_id: Option<String>,
    #[serde(default)]
    pub caused_by_trigger_kind: Option<String>,
    #[serde(default)]
    pub caused_by_correlation: Option<String>,
    #[serde(default)]
    pub caused_by_trigger_context: Option<String>,
    #[serde(default)]
    pub caused_by_parent_request_id: Option<String>,
    #[serde(default)]
    pub failure_reason: Option<String>,
    #[serde(default)]
    pub terminalized_at: Option<String>,
    #[serde(default)]
    pub terminal_redrive_attempts: Option<i64>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub claimed_at: Option<String>,
    #[serde(default)]
    pub deadline: Option<String>,
    #[serde(default)]
    pub retry_count: Option<i64>,
    #[serde(default)]
    pub max_retries: Option<i64>,
    #[serde(default)]
    pub interrupt_requested_at: Option<String>,
    #[serde(default)]
    pub valid_until: Option<String>,
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub workspace_authority: Option<String>,
    #[serde(default)]
    pub workspace_owner_deployment_id: Option<String>,
    #[serde(default)]
    pub workspace_seal_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentResponseRow {
    pub response_key: String,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub agent_did: Option<String>,
    #[serde(default)]
    pub requester_did: Option<String>,
    #[serde(default)]
    pub behavior_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub reasoning: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub error_message: Option<String>,
    #[serde(default)]
    pub token_count: Option<i64>,
    #[serde(default)]
    pub progress_seq: Option<i64>,
    #[serde(default)]
    pub reasoning_progress_seq: Option<i64>,
    #[serde(default)]
    pub materialized_message_sequence: Option<i64>,
    #[serde(default)]
    pub materialized_at: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub completed_at: Option<String>,
    #[serde(default)]
    pub interrupted_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentMessageRow {
    pub message_key: String,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub requester_did: Option<String>,
    #[serde(default)]
    pub sequence: Option<i64>,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub reasoning: Option<String>,
    #[serde(default)]
    pub timestamp: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentSessionRow {
    pub session_id: String,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub requester_did: Option<String>,
    #[serde(default)]
    pub behavior_id: Option<String>,
    #[serde(default)]
    pub started: Option<String>,
    #[serde(default)]
    pub ended: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GoalRow {
    pub goal_id: String,
    pub session_id: String,
    pub agent_did: String,
    #[serde(default)]
    pub objective: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub token_budget: Option<i64>,
    #[serde(default)]
    pub tokens_used: Option<i64>,
    #[serde(default)]
    pub active_time_seconds: Option<i64>,
    #[serde(default)]
    pub active_started_at: Option<String>,
    #[serde(default)]
    pub consecutive_blocked_audits: Option<i64>,
    #[serde(default)]
    pub last_blocked_request_id: Option<String>,
    #[serde(default)]
    pub last_blocked_reason: Option<String>,
    #[serde(default)]
    pub last_continued_from_request_id: Option<String>,
    #[serde(default)]
    pub continuation_sequence: Option<i64>,
    #[serde(default)]
    pub wrapup_requested: Option<bool>,
    #[serde(default)]
    pub wrapup_completed: Option<bool>,
    #[serde(default)]
    pub infrastructure_retry_count: Option<i64>,
    #[serde(default)]
    pub last_failure: Option<String>,
    #[serde(default)]
    pub completion_evidence: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentToolCallRow {
    pub tool_call_key: String,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub requester_did: Option<String>,
    #[serde(default)]
    pub message_sequence: Option<i64>,
    #[serde(default)]
    pub tool_name: Option<String>,
    #[serde(default)]
    pub tool_call_id: Option<String>,
    #[serde(default)]
    pub args: Option<String>,
    #[serde(default)]
    pub result: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub lifecycle_state: Option<String>,
    #[serde(default)]
    pub child_request_id: Option<String>,
    #[serde(default)]
    pub await_mode: Option<String>,
    #[serde(default)]
    pub cancel_policy: Option<String>,
    #[serde(default)]
    pub started_at: Option<String>,
    #[serde(default)]
    pub deadline_at: Option<String>,
    #[serde(default)]
    pub completed_at: Option<String>,
    #[serde(default)]
    pub selected_service_id: Option<String>,
    #[serde(default)]
    pub selected_tool_name: Option<String>,
    #[serde(default)]
    pub tool_failure_class: Option<String>,
    #[serde(default)]
    pub denial_reason: Option<String>,
    #[serde(default)]
    pub denied_argv: Option<Vec<String>>,
    #[serde(default)]
    pub denied_command: Option<String>,
    #[serde(default)]
    pub denied_argument: Option<String>,
    #[serde(default)]
    pub denied_subcommand: Option<String>,
    #[serde(default)]
    pub denied_prefix: Option<Vec<String>>,
    #[serde(default)]
    pub policy_mode: Option<String>,
    #[serde(default)]
    pub policy_network: Option<String>,
    #[serde(default)]
    pub cancel_cause: Option<String>,
    #[serde(default)]
    pub latency_ms: Option<i64>,
    #[serde(default)]
    pub partial_output_tail: Option<String>,
    #[serde(default)]
    pub partial_output_seq: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentToolResultRow {
    #[serde(default)]
    pub agent_did: Option<String>,
    #[serde(default)]
    pub requester_did: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub tool_name: Option<String>,
    #[serde(default)]
    pub tool_input: Option<String>,
    #[serde(default)]
    pub output_text: Option<String>,
    #[serde(default)]
    pub truncated: Option<bool>,
    #[serde(default)]
    pub truncation_metadata: Option<String>,
    #[serde(default)]
    pub conversation_doc_id: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub discarded_because_interrupted: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompactionEntryRow {
    pub compaction_key: String,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub requester_did: Option<String>,
    #[serde(default)]
    pub sequence: Option<i64>,
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub files_read: Option<String>,
    #[serde(default)]
    pub files_modified: Option<String>,
    #[serde(default)]
    pub messages_compacted: Option<i64>,
    #[serde(default)]
    pub original_tokens: Option<i64>,
    #[serde(default)]
    pub compacted_tokens: Option<i64>,
    #[serde(default)]
    pub created_at: Option<String>,
}

/// One durable rendered-request fact (#840/#1059): the exact provider request
/// body persisted before it was sent, keyed uniquely by `capture_key`.
///
/// Wire-shape mirror like every row here — only the identity key is required.
/// `request_json` is the captured provider body; consumers that only need
/// metadata should not select it (and the run timeline never does).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RenderedRequestRow {
    pub capture_key: String,
    #[serde(default)]
    pub request_doc_id: Option<String>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub agent_did: Option<String>,
    #[serde(default)]
    pub requester_did: Option<String>,
    #[serde(default)]
    pub behavior_id: Option<String>,
    #[serde(default)]
    pub capture_scope: Option<String>,
    #[serde(default)]
    pub turn_index: Option<i64>,
    #[serde(default)]
    pub attempt: Option<i64>,
    #[serde(default)]
    pub capture_version: Option<i64>,
    #[serde(default)]
    pub model_name: Option<String>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub request_json: Option<String>,
    #[serde(default)]
    pub prompt_hash: Option<String>,
    #[serde(default)]
    pub tools_hash: Option<String>,
    #[serde(default)]
    pub provenance_json: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
}

impl RenderedRequestRow {
    /// The row's stable identity ordering. Errors when `capture_scope`,
    /// `turn_index`, or `attempt` is missing or malformed: those are core
    /// facts, and defaulting them to zero would silently collide the row with
    /// a real first-turn capture.
    pub fn order_key(
        &self,
    ) -> Result<
        crate::rendered_request::CaptureOrderKey,
        crate::rendered_request::CaptureOrderKeyError,
    > {
        use crate::rendered_request::{CaptureOrderKey, CaptureOrderKeyError, CaptureScope};

        let scope: CaptureScope = self
            .capture_scope
            .as_deref()
            .ok_or(CaptureOrderKeyError::MissingScope)?
            .parse()
            .map_err(CaptureOrderKeyError::Scope)?;
        let turn_index = self
            .turn_index
            .ok_or(CaptureOrderKeyError::MissingTurnIndex)?;
        let attempt = self.attempt.ok_or(CaptureOrderKeyError::MissingAttempt)?;
        Ok(CaptureOrderKey {
            scope,
            turn_index,
            attempt,
        })
    }

    /// Read the row's provenance manifest through the versioned reader.
    pub fn provenance(
        &self,
    ) -> Result<
        crate::rendered_request::ParsedProvenance,
        crate::rendered_request::ProvenanceParseError,
    > {
        crate::rendered_request::ProvenanceManifest::parse(
            self.provenance_json
                .as_deref()
                .ok_or(crate::rendered_request::ProvenanceParseError::Empty)?,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskRow {
    pub task_id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub behavior_id: Option<String>,
    #[serde(default)]
    pub prompt_template: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub output_schema_ref: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SkillRow {
    pub skill_id: String,
    #[serde(default)]
    pub agent_did: Option<String>,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub instructions: Option<String>,
    #[serde(default, deserialize_with = "deserialize_string_vec")]
    pub tool_refs: Vec<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub interface_json: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub created_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScheduleRow {
    pub schedule_id: String,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub interval_secs: Option<i64>,
    #[serde(default)]
    pub cron: Option<String>,
    #[serde(default)]
    pub timezone: Option<String>,
    #[serde(default)]
    pub missed_run_policy: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub concurrency: Option<String>,
    #[serde(default)]
    pub next_run_at: Option<String>,
    #[serde(default)]
    pub last_attempt_at: Option<String>,
    #[serde(default)]
    pub last_status: Option<String>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub fire_count: Option<i64>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventTriggerRow {
    pub trigger_id: String,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub source_collection: Option<String>,
    #[serde(default)]
    pub event_kind: Option<String>,
    #[serde(default)]
    pub filter: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub concurrency: Option<String>,
    #[serde(default)]
    pub correlation_field: Option<String>,
    #[serde(default)]
    pub fire_mode: Option<String>,
    #[serde(default)]
    pub expected_count: Option<i64>,
    #[serde(default)]
    pub expected_count_field: Option<String>,
    #[serde(default)]
    pub group_timeout_secs: Option<i64>,
    #[serde(default)]
    pub group_min_count: Option<i64>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub last_attempt_at: Option<String>,
    #[serde(default)]
    pub last_fired_source_doc_id: Option<String>,
    #[serde(default)]
    pub last_status: Option<String>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub fire_count: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolSelectionRow {
    pub selection_id: String,
    #[serde(default)]
    pub agent_did: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub tool_policy_version: Option<String>,
    #[serde(default)]
    pub enable_file_tools: Option<bool>,
    #[serde(default)]
    pub file_tools_mode: Option<String>,
    #[serde(default)]
    pub file_tool_root: Option<String>,
    #[serde(default)]
    pub enable_bash: Option<bool>,
    #[serde(default)]
    pub bash_mode: Option<String>,
    #[serde(default)]
    pub command_execution_policy: Option<String>,
    #[serde(default, deserialize_with = "deserialize_string_vec")]
    pub command_allowed_argv_prefixes: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_string_vec")]
    pub command_forbidden_argv_prefixes: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_string_vec")]
    pub read_only_command_allowlist: Vec<String>,
    #[serde(default)]
    pub command_network_mode: Option<String>,
    #[serde(default, deserialize_with = "deserialize_string_vec")]
    pub cli_tool_names: Vec<String>,
    #[serde(default)]
    pub enable_meta_tools: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_string_vec")]
    pub allowed_mcp_service_ids: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_string_vec")]
    pub delegate_to: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_string_vec")]
    pub backgroundable_tool_names: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_string_vec")]
    pub subagent_targets: Vec<String>,
    #[serde(default)]
    pub subagent_spawn_enabled: Option<bool>,
    #[serde(default)]
    pub subagent_steering_enabled: Option<bool>,
    #[serde(default)]
    pub subagent_background_enabled: Option<bool>,
    #[serde(default)]
    pub subagent_default_await_mode: Option<String>,
    #[serde(default)]
    pub subagent_allow_cross_deployment: Option<bool>,
    #[serde(default)]
    pub cross_deployment_spawn_timeout_seconds: Option<i64>,
    #[serde(default)]
    pub enable_memory: Option<bool>,
    #[serde(default)]
    pub enable_session_history_tool: Option<bool>,
    #[serde(default)]
    pub enable_context_budget: Option<bool>,
    #[serde(default)]
    pub enable_defra_query: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_string_vec")]
    pub defra_query_collections: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_string_vec")]
    pub write_tools: Vec<String>,
    #[serde(default)]
    pub enable_self_config: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_string_vec")]
    pub self_config_categories: Vec<String>,
    #[serde(default)]
    pub self_config_no_lockout: Option<bool>,
    #[serde(default)]
    pub self_config_dry_run: Option<bool>,
    #[serde(default)]
    pub enable_lsp: Option<bool>,
    #[serde(default)]
    pub lsp_config: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InferenceBackendRow {
    pub backend_id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub provider_kind: Option<String>,
    #[serde(default)]
    pub openai_wire_api: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub api_key_env_var: Option<String>,
    #[serde(default)]
    pub max_concurrent: Option<i64>,
    #[serde(default)]
    pub max_queue_depth: Option<i64>,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_string_vec")]
    pub models: Vec<String>,
    #[serde(default)]
    pub last_probe: Option<String>,
    #[serde(default)]
    pub probe_status: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OAuthCredentialRow {
    #[serde(default, rename = "_docID")]
    pub doc_id: Option<String>,
    pub credential_id: String,
    #[serde(default)]
    pub agent_did: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub access_token: Option<String>,
    #[serde(default)]
    pub refresh_token: Option<String>,
    #[serde(default)]
    pub id_token: Option<String>,
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(default)]
    pub chatgpt_plan_type: Option<String>,
    #[serde(default)]
    pub is_fedramp: Option<bool>,
    #[serde(default)]
    pub access_token_expires_at: Option<String>,
    #[serde(default)]
    pub last_refresh: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InferenceProfileRow {
    pub profile_id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub context_window: Option<i64>,
    #[serde(default)]
    pub max_output_tokens: Option<i64>,
    #[serde(default)]
    pub max_turns: Option<i64>,
    #[serde(default)]
    pub temperature: Option<f64>,
    #[serde(default)]
    pub top_p: Option<f64>,
    #[serde(default)]
    pub top_k: Option<i64>,
    #[serde(default)]
    pub seed: Option<i64>,
    #[serde(default)]
    pub min_p: Option<f64>,
    #[serde(default)]
    pub frequency_penalty: Option<f64>,
    #[serde(default)]
    pub presence_penalty: Option<f64>,
    #[serde(default)]
    pub repetition_penalty: Option<f64>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub stream_batch_ms: Option<i64>,
    #[serde(default)]
    pub stream_liveness_timeout_secs: Option<i64>,
    #[serde(default)]
    pub deadline_duration_secs: Option<i64>,
    #[serde(default)]
    pub retry_max_transport: Option<i64>,
    #[serde(default)]
    pub retry_backoff_ms: Option<Vec<i64>>,
    #[serde(default)]
    pub retry_max_resample: Option<i64>,
    #[serde(default)]
    pub retry_allow_repair: Option<bool>,
    #[serde(default)]
    pub retry_interactive_max: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolServiceEntry {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolServiceRegistryRow {
    pub service_id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub hostname: Option<String>,
    #[serde(default)]
    pub tailscale_ip: Option<String>,
    #[serde(default)]
    pub lan_ip: Option<String>,
    #[serde(default)]
    pub mcp_port: Option<i64>,
    #[serde(default)]
    pub mcp_path: Option<String>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub send_agent_did: bool,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub tools: Vec<ToolServiceEntry>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

/// Persisted snapshot of one MCP service's health, written by the agent's
/// `health_checker` on every probe cycle. `status` carries the precise
/// `HealthStateInternal` projection ("healthy" / "stale" / "evicted" /
/// "reconnecting") so the operator UI can distinguish back-off from
/// in-flight retry without going through the collapsed three-state
/// `HealthStatus`. `failure_count` / `k_max` / `backoff_until` give the
/// K-model context per the design in
/// `Proofs/MCPHealth/{State,Transition}.lean`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolServiceHealthStateRow {
    pub service_id: String,
    #[serde(default)]
    pub agent_did: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub tool_count: Option<i64>,
    #[serde(default)]
    pub failure_count: Option<i64>,
    #[serde(default)]
    pub k_max: Option<i64>,
    #[serde(default)]
    pub backoff_until: Option<String>,
    #[serde(default)]
    pub last_probe_at: Option<String>,
    #[serde(default)]
    pub last_seen: Option<String>,
    #[serde(default)]
    pub last_error_class: Option<String>,
    #[serde(default)]
    pub last_error_message: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rendered_request_row_parses_a_graphql_response_row() {
        let json = r#"{
            "capture_key": "rendered:v1:abc",
            "request_doc_id": "bae-doc-1",
            "request_id": "req-1",
            "session_id": "s-1",
            "agent_did": "did:test:amy",
            "requester_did": "",
            "behavior_id": "amy-code",
            "capture_scope": "compaction.2",
            "turn_index": 1,
            "attempt": 0,
            "capture_version": 1,
            "model_name": "test-model",
            "source": "openai_chat_completions",
            "request_json": "{\"model\":\"test-model\"}",
            "prompt_hash": "aa",
            "tools_hash": "bb",
            "provenance_json": "{\"manifest_version\":99}",
            "created_at": "2026-08-07T12:00:00Z"
        }"#;
        let row: RenderedRequestRow = serde_json::from_str(json).expect("parse");
        assert_eq!(row.capture_key, "rendered:v1:abc");

        let key = row.order_key().expect("order key");
        assert_eq!(key.scope.kind.as_str(), "compaction");
        assert_eq!(key.scope.seq, 2);
        assert_eq!((key.turn_index, key.attempt), (1, 0));

        // Unknown manifest versions are reported, not guessed at.
        assert_eq!(
            row.provenance().expect("well-formed provenance"),
            crate::rendered_request::ParsedProvenance::Unsupported {
                manifest_version: 99
            }
        );
    }

    /// A DefraDB response may omit unpopulated columns entirely; the row still
    /// deserializes, but it cannot be ordered — and must say so rather than
    /// defaulting into a first-turn collision.
    #[test]
    fn rendered_request_row_without_order_facts_deserializes_but_will_not_order() {
        let row: RenderedRequestRow =
            serde_json::from_str(r#"{ "capture_key": "rendered:v1:abc" }"#).expect("parse");
        assert_eq!(
            row.order_key(),
            Err(crate::rendered_request::CaptureOrderKeyError::MissingScope)
        );

        let row: RenderedRequestRow = serde_json::from_str(
            r#"{ "capture_key": "rendered:v1:abc", "capture_scope": "inference.1", "attempt": 0 }"#,
        )
        .expect("parse");
        assert_eq!(
            row.order_key(),
            Err(crate::rendered_request::CaptureOrderKeyError::MissingTurnIndex)
        );
        assert!(matches!(
            row.provenance(),
            Err(crate::rendered_request::ProvenanceParseError::Empty)
        ));
    }

    #[test]
    fn agent_request_row_roundtrips() {
        let json = r#"{
            "request_id": "req-1",
            "agent_did": "did:test:amy",
            "behavior_id": "amy-code",
            "session_id": "s-1",
            "retry_parent_request": "",
            "retry_root_request": "req-1",
            "superseded_by_request": "",
            "content": "hello",
            "temperature": 0.0,
            "top_p": 0.95,
            "top_k": 40,
            "max_tokens": 512,
            "max_total_tokens": 4096,
            "metadata": "{\"run_id\":\"run-1\"}",
            "status": "pending",
            "lifecycle_state": "pending",
            "backend_id": "",
            "execution_origin": "interactive",
            "failure_reason": "",
            "created_at": "2026-04-13T12:00:00Z",
            "retry_count": 0,
            "max_retries": 3
        }"#;
        let row: AgentRequestRow = serde_json::from_str(json).expect("parse");
        assert_eq!(row.request_id, "req-1");
        assert_eq!(row.retry_count, Some(0));
        assert_eq!(row.temperature, Some(0.0));
        assert_eq!(row.top_p, Some(0.95));
        assert_eq!(row.top_k, Some(40));
        assert_eq!(row.max_tokens, Some(512));
        assert_eq!(row.max_total_tokens, Some(4096));
        assert_eq!(row.metadata.as_deref(), Some(r#"{"run_id":"run-1"}"#));
        let re: String = serde_json::to_string(&row).expect("serialize");
        let round: AgentRequestRow = serde_json::from_str(&re).expect("reparse");
        assert_eq!(row, round);
    }

    #[test]
    fn tool_selection_row_handles_missing_arrays() {
        let json = r#"{
            "selection_id": "sel-1",
            "agent_did": "did:test:amy",
            "display_name": "tools-engineering",
            "enable_file_tools": true,
            "file_tools_mode": "read",
            "enable_bash": false,
            "bash_mode": "deny",
            "enable_meta_tools": true
        }"#;
        let row: ToolSelectionRow = serde_json::from_str(json).expect("parse");
        assert!(row.cli_tool_names.is_empty());
        assert!(row.allowed_mcp_service_ids.is_empty());
        assert!(row.delegate_to.is_empty());
        assert!(row.backgroundable_tool_names.is_empty());
        assert!(row.subagent_targets.is_empty());
        assert!(row.write_tools.is_empty());
        assert_eq!(row.subagent_default_await_mode, None);
    }

    #[test]
    fn tool_selection_row_handles_null_arrays() {
        let json = r#"{
            "selection_id": "sel-2",
            "agent_did": "did:test:amy",
            "cli_tool_names": null,
            "allowed_mcp_service_ids": null,
            "delegate_to": null,
            "backgroundable_tool_names": null,
            "subagent_targets": null
        }"#;
        let row: ToolSelectionRow = serde_json::from_str(json).expect("parse");
        assert!(row.cli_tool_names.is_empty());
        assert!(row.allowed_mcp_service_ids.is_empty());
        assert!(row.delegate_to.is_empty());
        assert!(row.backgroundable_tool_names.is_empty());
        assert!(row.subagent_targets.is_empty());
    }

    #[test]
    fn tool_selection_row_handles_empty_string_arrays() {
        let json = r#"{
            "selection_id": "sel-3",
            "agent_did": "did:test:amy",
            "cli_tool_names": "",
            "allowed_mcp_service_ids": "",
            "delegate_to": "",
            "backgroundable_tool_names": "",
            "subagent_targets": "",
            "write_tools": ""
        }"#;
        let row: ToolSelectionRow = serde_json::from_str(json).expect("parse");
        assert!(row.cli_tool_names.is_empty());
        assert!(row.allowed_mcp_service_ids.is_empty());
        assert!(row.delegate_to.is_empty());
        assert!(row.backgroundable_tool_names.is_empty());
        assert!(row.subagent_targets.is_empty());
        assert!(row.write_tools.is_empty());
    }

    #[test]
    fn tool_selection_row_round_trips_subagent_fields() {
        let json = r#"{
            "selection_id": "sel-4",
            "agent_did": "did:test:amy",
            "subagent_targets": ["amy-research"],
            "subagent_spawn_enabled": true,
            "subagent_steering_enabled": true,
            "subagent_background_enabled": true,
            "subagent_allow_cross_deployment": true,
            "cross_deployment_spawn_timeout_seconds": 45,
            "enable_memory": true,
            "enable_session_history_tool": true
        }"#;
        let row: ToolSelectionRow = serde_json::from_str(json).expect("parse");
        assert_eq!(row.subagent_targets, vec!["amy-research".to_string()]);
        assert_eq!(row.subagent_spawn_enabled, Some(true));
        assert_eq!(row.subagent_steering_enabled, Some(true));
        assert_eq!(row.subagent_background_enabled, Some(true));
        assert_eq!(row.subagent_allow_cross_deployment, Some(true));
        assert_eq!(row.cross_deployment_spawn_timeout_seconds, Some(45));
        assert_eq!(row.enable_memory, Some(true));
        assert_eq!(row.enable_session_history_tool, Some(true));

        let re: String = serde_json::to_string(&row).expect("serialize");
        let round: ToolSelectionRow = serde_json::from_str(&re).expect("reparse");
        assert_eq!(row, round);
    }

    #[test]
    fn tool_selection_row_round_trips_write_tools_and_await_mode() {
        let json = r#"{
            "selection_id": "sel-5",
            "agent_did": "did:test:amy",
            "subagent_default_await_mode": "foreground",
            "write_tools": ["{\"tool_name\":\"upsert_note\",\"collection\":\"Note\",\"fields\":[]}"]
        }"#;
        let row: ToolSelectionRow = serde_json::from_str(json).expect("parse");
        assert_eq!(
            row.subagent_default_await_mode.as_deref(),
            Some("foreground")
        );
        assert_eq!(row.write_tools.len(), 1);
        assert!(row.write_tools[0].contains("\"tool_name\":\"upsert_note\""));

        let re: String = serde_json::to_string(&row).expect("serialize");
        let round: ToolSelectionRow = serde_json::from_str(&re).expect("reparse");
        assert_eq!(row, round);
    }

    #[test]
    fn tool_service_registry_defaults_send_agent_did_to_false() {
        let json = r#"{
            "service_id": "observability-mcp",
            "hostname": "studio-1",
            "mcp_port": 9201,
            "mcp_path": "/mcp"
        }"#;
        let row: ToolServiceRegistryRow = serde_json::from_str(json).expect("parse");
        assert!(!row.send_agent_did);
    }

    #[test]
    fn tool_service_registry_treats_null_send_agent_did_as_false() {
        let json = r#"{
            "service_id": "observability-mcp",
            "hostname": "studio-1",
            "mcp_port": 9201,
            "mcp_path": "/mcp",
            "send_agent_did": null
        }"#;
        let row: ToolServiceRegistryRow = serde_json::from_str(json).expect("parse");
        assert!(!row.send_agent_did);
    }
}
