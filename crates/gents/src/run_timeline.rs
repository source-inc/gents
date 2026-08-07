use std::collections::BTreeSet;

use chrono::DateTime;
use gents_protocol::rendered_request::{
    CaptureOrderKey, CaptureScope, ParsedProvenance, ProvenanceManifest, ProvenanceStatus,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RunTimelineRows {
    pub request: TimelineRequestRow,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<TimelineSessionRow>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation: Option<TimelineConversationRow>,
    #[serde(default)]
    pub requests: Vec<TimelineRequestRow>,
    #[serde(default)]
    pub messages: Vec<TimelineMessageRow>,
    #[serde(default)]
    pub tool_calls: Vec<TimelineToolCallRow>,
    #[serde(default)]
    pub inference_calls: Vec<TimelineInferenceCallRow>,
    #[serde(default)]
    pub responses: Vec<TimelineResponseRow>,
    #[serde(default)]
    pub rendered_requests: Vec<TimelineRenderedRequestRow>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunTimeline {
    pub request_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_did: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub behavior_id: Option<String>,
    pub request: TimelineRequestRow,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session: Option<TimelineSessionRow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation: Option<TimelineConversationRow>,
    pub child_request_ids: Vec<String>,
    #[serde(default)]
    pub inference_calls: Vec<TimelineInferenceCallRow>,
    pub events: Vec<RunTimelineEvent>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineRequestRow {
    #[serde(default, rename = "_docID", skip_serializing)]
    pub doc_id: Option<String>,
    #[serde(default)]
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_did: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behavior_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle_state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_count: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interrupt_requested_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caused_by_parent_request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caused_by_parent_tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_origin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caused_by_trigger_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caused_by_trigger_kind: Option<String>,
    #[serde(default, skip_serializing_if = "RetrySummary::is_empty")]
    pub retry_summary: RetrySummary,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetrySummary {
    pub retry_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transient_error: Option<String>,
    pub recovered: bool,
}

impl RetrySummary {
    fn is_empty(&self) -> bool {
        self.retry_count == 0 && self.last_transient_error.is_none() && !self.recovered
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineMessageRow {
    #[serde(default, rename = "_docID", skip_serializing)]
    pub doc_id: Option<String>,
    #[serde(default)]
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default)]
    pub sequence: i64,
    #[serde(default)]
    pub role: String,
    #[serde(default)]
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineResponseRow {
    #[serde(default, rename = "_docID", skip_serializing)]
    pub doc_id: Option<String>,
    #[serde(default)]
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_did: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behavior_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_count: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_seq: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub materialized_message_sequence: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub materialized_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interrupted_at: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineInferenceCallRow {
    #[serde(default, rename = "_docID", skip_serializing)]
    pub doc_id: Option<String>,
    #[serde(default)]
    pub call_id: String,
    #[serde(default)]
    pub request_id: String,
    #[serde(default)]
    pub call_seq: i64,
    #[serde(default)]
    pub attempt: i64,
    #[serde(default)]
    pub call_state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_id: Option<String>,
    #[serde(default)]
    pub call_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_tokens: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_tokens: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_input_tokens: Option<i64>,
}

/// One `RenderedRequest` capture row, as the timeline reads it.
///
/// Deliberately has **no `request_json` field**: the fetch layer never selects
/// the captured body, so nothing downstream of the timeline — events,
/// projections, default CLI output — can leak it. The one body read in the
/// system is the CLI's explicit `--include-body`, which goes straight to the
/// collection, not through here.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineRenderedRequestRow {
    #[serde(default, rename = "_docID", skip_serializing)]
    pub doc_id: Option<String>,
    #[serde(default)]
    pub capture_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_doc_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_index: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_version: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance_json: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineToolCallRow {
    #[serde(default, rename = "_docID", skip_serializing)]
    pub doc_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default)]
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_sequence: Option<i64>,
    #[serde(default)]
    pub tool_name: String,
    #[serde(default)]
    pub tool_call_id: String,
    #[serde(default)]
    pub args: String,
    #[serde(default)]
    pub result: String,
    #[serde(default)]
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle_state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_service_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_tool_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_failure_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub denial_reason: Option<String>,
    #[serde(default, deserialize_with = "empty_vec_if_null")]
    pub denied_argv: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub denied_command: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub denied_argument: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub denied_subcommand: Option<String>,
    #[serde(default, deserialize_with = "empty_vec_if_null")]
    pub denied_prefix: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_network: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub await_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_policy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_cause: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_request_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineSessionRow {
    #[serde(default, rename = "_docID", skip_serializing)]
    pub doc_id: Option<String>,
    #[serde(default)]
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behavior_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineConversationRow {
    #[serde(default, rename = "_docID", skip_serializing)]
    pub doc_id: Option<String>,
    #[serde(default)]
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_did: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behavior_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forked_from_session_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunTimelineEvent {
    Request(TimelineRequestEvent),
    RenderedRequest(TimelineRenderedRequestEvent),
    InferenceCall(TimelineInferenceCallEvent),
    Message(TimelineMessageEvent),
    ToolCall(TimelineToolCallEvent),
    Response(TimelineResponseEvent),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineRequestEvent {
    pub request_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_did: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub behavior_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifecycle_state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_origin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caused_by_trigger_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caused_by_trigger_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
    #[serde(default, skip_serializing_if = "RetrySummary::is_empty")]
    pub retry_summary: RetrySummary,
}

/// A rendered-request capture in the timeline — metadata only, always.
///
/// `provenance_status` is derived through the versioned reader:
/// `"captured_only"` (the manifest's own claim), `"unsupported_manifest"`
/// (well-formed, unknown version — the row is still listed), `"unavailable"`
/// (no provenance column), or `"malformed"` (unreadable column; surfaced,
/// never a panic). `call_id`/`call_seq` are the exact admission join when the
/// manifest carries one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineRenderedRequestEvent {
    pub capture_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_doc_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capture_scope: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope_seq: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn_index: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capture_version: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools_hash: Option<String>,
    pub provenance_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest_version: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_seq: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineInferenceCallEvent {
    pub call_id: String,
    pub request_id: String,
    pub call_seq: i64,
    pub attempt: i64,
    pub call_state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend_id: Option<String>,
    pub call_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_tokens: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_tokens: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cached_input_tokens: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queued_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineMessageEvent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    pub session_id: String,
    pub sequence: i64,
    pub role: String,
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineToolCallEvent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_sequence: Option<i64>,
    pub tool_name: String,
    pub tool_call_id: String,
    pub args: String,
    pub result: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifecycle_state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_service_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_tool_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_failure_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub denial_reason: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub denied_argv: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub denied_command: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub denied_argument: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub denied_subcommand: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub denied_prefix: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_network: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub await_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancel_policy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancel_cause: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub child_request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineResponseEvent {
    pub request_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub materialized_message_sequence: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
}

pub fn build_run_timeline(mut rows: RunTimelineRows) -> RunTimeline {
    let root_request_id = rows.request.request_id.clone();
    let session_id = rows.request.session_id.clone();
    let mut included_request_ids = BTreeSet::from([root_request_id.clone()]);
    for request in &rows.requests {
        if request
            .caused_by_parent_request_id
            .as_deref()
            .is_some_and(|parent| parent == root_request_id)
        {
            included_request_ids.insert(request.request_id.clone());
        }
    }
    for tool_call in &rows.tool_calls {
        if let Some(child_request_id) = nonempty(tool_call.child_request_id.as_deref()) {
            included_request_ids.insert(child_request_id.to_string());
        }
    }
    let inference_calls = sorted_inference_calls(rows.inference_calls, &included_request_ids);
    apply_retry_summaries(&mut rows.request, &mut rows.requests, &inference_calls);

    let mut events = Vec::new();
    push_request_event(&mut events, &rows.request);
    for request in &rows.requests {
        if request.request_id != root_request_id
            && included_request_ids.contains(&request.request_id)
        {
            push_request_event(&mut events, request);
        }
    }

    for call in &inference_calls {
        events.push(RunTimelineEvent::InferenceCall(
            TimelineInferenceCallEvent {
                call_id: call.call_id.clone(),
                request_id: call.request_id.clone(),
                call_seq: call.call_seq,
                attempt: call.attempt,
                call_state: call.call_state.clone(),
                failure_reason: call.failure_reason.clone(),
                backend_id: call.backend_id.clone(),
                call_kind: call.call_kind.clone(),
                prompt_tokens: call.prompt_tokens,
                completion_tokens: call.completion_tokens,
                cached_input_tokens: call.cached_input_tokens,
                queued_at: call.queued_at.clone(),
                started_at: call.started_at.clone(),
                ended_at: call.ended_at.clone(),
            },
        ));
    }

    for rendered in &rows.rendered_requests {
        let belongs = rendered
            .request_id
            .as_deref()
            .is_some_and(|request_id| included_request_ids.contains(request_id));
        if belongs {
            events.push(RunTimelineEvent::RenderedRequest(rendered_request_event(
                rendered,
            )));
        }
    }

    for message in &rows.messages {
        let request_id = nonempty(message.request_id.as_deref())
            .map(ToOwned::to_owned)
            .or_else(|| {
                infer_request_id_for_message(message, &rows.responses, &rows.requests)
                    .map(ToOwned::to_owned)
            });
        if should_include_event(
            request_id.as_deref(),
            Some(message.session_id.as_str()),
            &included_request_ids,
            session_id.as_deref(),
        ) {
            events.push(RunTimelineEvent::Message(TimelineMessageEvent {
                request_id,
                session_id: message.session_id.clone(),
                sequence: message.sequence,
                role: message.role.clone(),
                content: message.content.clone(),
                timestamp: message.timestamp.clone(),
            }));
        }
    }

    for tool_call in &rows.tool_calls {
        let request_id = infer_request_id_for_tool_call(tool_call, &rows.requests, &rows.responses);
        if should_include_event(
            request_id.as_deref(),
            Some(tool_call.session_id.as_str()),
            &included_request_ids,
            session_id.as_deref(),
        ) {
            events.push(RunTimelineEvent::ToolCall(TimelineToolCallEvent {
                request_id,
                session_id: tool_call.session_id.clone(),
                message_sequence: tool_call.message_sequence,
                tool_name: tool_call.tool_name.clone(),
                tool_call_id: tool_call.tool_call_id.clone(),
                args: tool_call.args.clone(),
                result: tool_call.result.clone(),
                status: tool_call.status.clone(),
                lifecycle_state: tool_call.lifecycle_state.clone(),
                selected_service_id: tool_call.selected_service_id.clone(),
                selected_tool_name: tool_call.selected_tool_name.clone(),
                tool_failure_class: tool_call.tool_failure_class.clone(),
                denial_reason: tool_call.denial_reason.clone(),
                denied_argv: tool_call.denied_argv.clone(),
                denied_command: tool_call.denied_command.clone(),
                denied_argument: tool_call.denied_argument.clone(),
                denied_subcommand: tool_call.denied_subcommand.clone(),
                denied_prefix: tool_call.denied_prefix.clone(),
                policy_mode: tool_call.policy_mode.clone(),
                policy_network: tool_call.policy_network.clone(),
                latency_ms: tool_call.latency_ms,
                await_mode: tool_call.await_mode.clone(),
                cancel_policy: tool_call.cancel_policy.clone(),
                cancel_cause: tool_call.cancel_cause.clone(),
                child_request_id: tool_call.child_request_id.clone(),
                started_at: tool_call.started_at.clone(),
                completed_at: tool_call.completed_at.clone(),
            }));
        }
    }

    for response in &rows.responses {
        if included_request_ids.contains(&response.request_id) {
            events.push(RunTimelineEvent::Response(TimelineResponseEvent {
                request_id: response.request_id.clone(),
                session_id: response.session_id.clone(),
                status: response.status.clone(),
                content: response.content.clone(),
                reasoning: response.reasoning.clone(),
                error_message: response.error_message.clone(),
                materialized_message_sequence: response.materialized_message_sequence,
                timestamp: first_nonempty([
                    response.completed_at.as_deref(),
                    response.materialized_at.as_deref(),
                    response.created_at.as_deref(),
                ])
                .map(ToOwned::to_owned),
            }));
        }
    }

    events.sort_by_key(event_sort_key);
    let child_request_ids = child_request_ids(&included_request_ids, &root_request_id);

    RunTimeline {
        request_id: root_request_id,
        session_id,
        agent_did: first_owned([
            rows.request.agent_did.as_deref(),
            rows.conversation
                .as_ref()
                .and_then(|conversation| conversation.agent_did.as_deref()),
        ]),
        behavior_id: first_owned([
            rows.request.behavior_id.as_deref(),
            rows.conversation
                .as_ref()
                .and_then(|conversation| conversation.behavior_id.as_deref()),
            rows.session
                .as_ref()
                .and_then(|session| session.behavior_id.as_deref()),
        ]),
        request: rows.request,
        session: rows.session,
        conversation: rows.conversation,
        child_request_ids,
        inference_calls,
        events,
    }
}

fn sorted_inference_calls(
    inference_calls: Vec<TimelineInferenceCallRow>,
    included_request_ids: &BTreeSet<String>,
) -> Vec<TimelineInferenceCallRow> {
    let mut inference_calls = inference_calls
        .into_iter()
        .filter(|call| included_request_ids.contains(&call.request_id))
        .collect::<Vec<_>>();
    inference_calls.sort_by_key(|call| {
        (
            call.request_id.clone(),
            call.call_seq,
            call.queued_at
                .as_deref()
                .and_then(timestamp_millis)
                .unwrap_or(i64::MIN),
            call.call_id.clone(),
        )
    });
    inference_calls
}

fn apply_retry_summaries(
    root_request: &mut TimelineRequestRow,
    requests: &mut [TimelineRequestRow],
    inference_calls: &[TimelineInferenceCallRow],
) {
    root_request.retry_summary = retry_summary_for_request(root_request, inference_calls);
    for request in requests {
        request.retry_summary = retry_summary_for_request(request, inference_calls);
    }
}

fn retry_summary_for_request(
    request: &TimelineRequestRow,
    inference_calls: &[TimelineInferenceCallRow],
) -> RetrySummary {
    let request_calls = inference_calls
        .iter()
        .filter(|call| call.request_id == request.request_id)
        .filter(|call| call.call_kind == "inference")
        .collect::<Vec<_>>();

    let mut retry_count = 0;
    let mut last_transient_error = None;
    for (index, call) in request_calls.iter().enumerate() {
        if call.call_state == "failed" && index + 1 < request_calls.len() {
            retry_count += 1;
            last_transient_error = call.failure_reason.clone();
        }
    }

    RetrySummary {
        retry_count,
        last_transient_error,
        recovered: request_completed(request) && retry_count > 0,
    }
}

fn request_completed(request: &TimelineRequestRow) -> bool {
    [
        request.lifecycle_state.as_deref(),
        request.status.as_deref(),
    ]
    .into_iter()
    .flatten()
    .any(|state| state == "completed")
}

fn push_request_event(events: &mut Vec<RunTimelineEvent>, request: &TimelineRequestRow) {
    events.push(RunTimelineEvent::Request(TimelineRequestEvent {
        request_id: request.request_id.clone(),
        parent_request_id: request.caused_by_parent_request_id.clone(),
        parent_tool_call_id: request.caused_by_parent_tool_call_id.clone(),
        agent_did: request.agent_did.clone(),
        behavior_id: request.behavior_id.clone(),
        session_id: request.session_id.clone(),
        status: request.status.clone(),
        lifecycle_state: request.lifecycle_state.clone(),
        failure_reason: request.failure_reason.clone(),
        metadata: request.metadata.clone(),
        execution_origin: request.execution_origin.clone(),
        caused_by_trigger_id: request.caused_by_trigger_id.clone(),
        caused_by_trigger_kind: request.caused_by_trigger_kind.clone(),
        timestamp: request.created_at.clone(),
        retry_summary: request.retry_summary.clone(),
    }));
}

/// Derive the metadata-only capture event from its row. The scope parse and
/// the provenance read both go through the shared `gents-protocol` vocabulary;
/// their failures are surfaced as explicit statuses, never defaults.
///
/// Public because the CLI's `trace capture` renders single rows through the
/// same derivation the timeline uses — one metadata surface, not two.
pub fn rendered_request_event(row: &TimelineRenderedRequestRow) -> TimelineRenderedRequestEvent {
    let scope = row
        .capture_scope
        .as_deref()
        .and_then(|scope| scope.parse::<CaptureScope>().ok());
    let (provenance_status, manifest_version, call_id, call_seq) =
        match row.provenance_json.as_deref() {
            None => ("unavailable".to_string(), None, None, None),
            Some(raw) => match ProvenanceManifest::parse(raw) {
                Ok(ParsedProvenance::Manifest(manifest)) => (
                    provenance_status_label(manifest.status).to_string(),
                    Some(i64::from(manifest.manifest_version)),
                    manifest
                        .admission
                        .as_ref()
                        .map(|admission| admission.call_id.clone()),
                    manifest.admission.as_ref().map(|admission| admission.call_seq),
                ),
                Ok(ParsedProvenance::Unsupported { manifest_version }) => (
                    "unsupported_manifest".to_string(),
                    Some(i64::from(manifest_version)),
                    None,
                    None,
                ),
                Err(_) => ("malformed".to_string(), None, None, None),
            },
        };

    TimelineRenderedRequestEvent {
        capture_key: row.capture_key.clone(),
        request_doc_id: row.request_doc_id.clone(),
        request_id: row.request_id.clone(),
        session_id: row.session_id.clone(),
        capture_scope: row.capture_scope.clone(),
        scope_kind: scope.map(|scope| scope.kind.as_str().to_string()),
        scope_seq: scope.map(|scope| i64::try_from(scope.seq).unwrap_or(i64::MAX)),
        turn_index: row.turn_index,
        attempt: row.attempt,
        capture_version: row.capture_version,
        model_name: row.model_name.clone(),
        source: row.source.clone(),
        prompt_hash: row.prompt_hash.clone(),
        tools_hash: row.tools_hash.clone(),
        provenance_status,
        manifest_version,
        call_id,
        call_seq,
        created_at: row.created_at.clone(),
    }
}

fn provenance_status_label(status: ProvenanceStatus) -> &'static str {
    match status {
        ProvenanceStatus::CapturedOnly => "captured_only",
    }
}

/// Deterministic same-timestamp ordering for capture events: the numeric
/// order key when the row's identity facts parse, the capture key otherwise
/// (unique, so still deterministic — just not meaningfully ordered).
fn rendered_request_tiebreak(event: &TimelineRenderedRequestEvent) -> String {
    let scope = event
        .capture_scope
        .as_deref()
        .and_then(|scope| scope.parse::<CaptureScope>().ok());
    match (scope, event.turn_index, event.attempt) {
        (Some(scope), Some(turn_index), Some(attempt)) => CaptureOrderKey {
            scope,
            turn_index,
            attempt,
        }
        .padded(),
        _ => event.capture_key.clone(),
    }
}

fn infer_request_id_for_message<'a>(
    message: &TimelineMessageRow,
    responses: &'a [TimelineResponseRow],
    requests: &'a [TimelineRequestRow],
) -> Option<&'a str> {
    responses
        .iter()
        .filter(|response| response.session_id.as_deref() == Some(message.session_id.as_str()))
        .filter_map(|response| {
            let materialized = response.materialized_message_sequence?;
            (materialized >= message.sequence)
                .then_some((response.request_id.as_str(), materialized))
        })
        .min_by_key(|(_, materialized)| *materialized)
        .map(|(request_id, _)| request_id)
        .or_else(|| {
            let mut session_requests = requests.iter().filter(|request| {
                request.session_id.as_deref() == Some(message.session_id.as_str())
            });
            let request = session_requests.next()?;
            session_requests
                .next()
                .is_none()
                .then_some(request.request_id.as_str())
        })
}

fn infer_request_id_for_tool_call(
    tool_call: &TimelineToolCallRow,
    requests: &[TimelineRequestRow],
    responses: &[TimelineResponseRow],
) -> Option<String> {
    if let Some(request_id) = nonempty(tool_call.request_id.as_deref()) {
        return Some(request_id.to_string());
    }

    if let Some(sequence) = tool_call.message_sequence {
        if let Some(response) = responses
            .iter()
            .filter(|response| {
                response.session_id.as_deref() == Some(tool_call.session_id.as_str())
            })
            .filter_map(|response| {
                let materialized = response.materialized_message_sequence?;
                (materialized >= sequence).then_some((response.request_id.as_str(), materialized))
            })
            .min_by_key(|(_, materialized)| *materialized)
            .map(|(request_id, _)| request_id)
        {
            return Some(response.to_string());
        }
    }

    if let Some(started_at) = tool_call.started_at.as_deref().and_then(timestamp_millis) {
        if let Some(request_id) = requests
            .iter()
            .filter(|request| request.session_id.as_deref() == Some(tool_call.session_id.as_str()))
            .filter_map(|request| {
                let created_at = request.created_at.as_deref().and_then(timestamp_millis)?;
                (created_at <= started_at).then_some((request.request_id.as_str(), created_at))
            })
            .max_by_key(|(_, created_at)| *created_at)
            .map(|(request_id, _)| request_id)
        {
            return Some(request_id.to_string());
        }
    }

    let mut session_requests = requests
        .iter()
        .filter(|request| request.session_id.as_deref() == Some(tool_call.session_id.as_str()));
    let request = session_requests.next()?;
    session_requests
        .next()
        .is_none()
        .then(|| request.request_id.clone())
}

fn should_include_event(
    request_id: Option<&str>,
    event_session_id: Option<&str>,
    included_request_ids: &BTreeSet<String>,
    root_session_id: Option<&str>,
) -> bool {
    request_id
        .map(|request_id| included_request_ids.contains(request_id))
        .unwrap_or_else(|| event_session_id.is_some() && event_session_id == root_session_id)
}

/// `(timestamp_millis, family_rank, intra_rank, tiebreak)`. Family ranks are
/// unserialized internals and only break same-millisecond ties: request 0,
/// rendered_request 1 (persist-before-send puts the capture before the bytes
/// leave), inference_call 2, message 3, tool_call 4, response 5.
fn event_sort_key(event: &RunTimelineEvent) -> (i64, i64, i64, String) {
    match event {
        RunTimelineEvent::Request(event) => (
            event
                .timestamp
                .as_deref()
                .and_then(timestamp_millis)
                .unwrap_or(i64::MIN),
            0,
            -1,
            event.request_id.clone(),
        ),
        RunTimelineEvent::RenderedRequest(event) => (
            event
                .created_at
                .as_deref()
                .and_then(timestamp_millis)
                .unwrap_or(i64::MIN),
            1,
            event.call_seq.unwrap_or(i64::MAX),
            rendered_request_tiebreak(event),
        ),
        RunTimelineEvent::InferenceCall(event) => (
            first_nonempty([
                event.queued_at.as_deref(),
                event.started_at.as_deref(),
                event.ended_at.as_deref(),
            ])
            .and_then(timestamp_millis)
            .unwrap_or(i64::MIN),
            2,
            event.call_seq,
            event.call_id.clone(),
        ),
        RunTimelineEvent::Message(event) => (
            event
                .timestamp
                .as_deref()
                .and_then(timestamp_millis)
                .unwrap_or(i64::MIN),
            3,
            event.sequence,
            format!("{}:{}", event.session_id, event.sequence),
        ),
        RunTimelineEvent::ToolCall(event) => (
            event
                .started_at
                .as_deref()
                .and_then(timestamp_millis)
                .unwrap_or(i64::MIN),
            4,
            event.message_sequence.unwrap_or(i64::MAX),
            event.tool_call_id.clone(),
        ),
        RunTimelineEvent::Response(event) => (
            event
                .timestamp
                .as_deref()
                .and_then(timestamp_millis)
                .unwrap_or(i64::MIN),
            5,
            event.materialized_message_sequence.unwrap_or(i64::MAX),
            event.request_id.clone(),
        ),
    }
}

fn child_request_ids(
    included_request_ids: &BTreeSet<String>,
    root_request_id: &str,
) -> Vec<String> {
    included_request_ids
        .iter()
        .filter(|request_id| request_id.as_str() != root_request_id)
        .cloned()
        .collect()
}

fn first_owned<'a>(values: impl IntoIterator<Item = Option<&'a str>>) -> Option<String> {
    first_nonempty(values).map(ToOwned::to_owned)
}

fn first_nonempty<'a>(values: impl IntoIterator<Item = Option<&'a str>>) -> Option<&'a str> {
    values
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|value| !value.is_empty())
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn timestamp_millis(value: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

fn empty_vec_if_null<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<Vec<String>>::deserialize(deserializer)?.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_ordered_timeline_with_inferred_tool_call_and_child_request() {
        let rows = RunTimelineRows {
            request: TimelineRequestRow {
                request_id: "req-1".to_string(),
                agent_did: Some("did:test:amy".to_string()),
                behavior_id: Some("amy".to_string()),
                session_id: Some("session-1".to_string()),
                lifecycle_state: Some("complete".to_string()),
                created_at: Some("2026-05-04T12:00:01Z".to_string()),
                ..Default::default()
            },
            requests: vec![
                TimelineRequestRow {
                    request_id: "req-1".to_string(),
                    session_id: Some("session-1".to_string()),
                    created_at: Some("2026-05-04T12:00:01Z".to_string()),
                    ..Default::default()
                },
                TimelineRequestRow {
                    request_id: "child-1".to_string(),
                    session_id: Some("session-2".to_string()),
                    caused_by_parent_request_id: Some("req-1".to_string()),
                    caused_by_parent_tool_call_id: Some("call-delegate".to_string()),
                    created_at: Some("2026-05-04T12:00:04Z".to_string()),
                    ..Default::default()
                },
            ],
            messages: vec![TimelineMessageRow {
                doc_id: None,
                session_id: "session-1".to_string(),
                request_id: Some("req-1".to_string()),
                sequence: 2,
                role: "assistant".to_string(),
                content: "calling tool".to_string(),
                timestamp: Some("2026-05-04T12:00:02Z".to_string()),
            }],
            tool_calls: vec![TimelineToolCallRow {
                session_id: "session-1".to_string(),
                message_sequence: Some(2),
                tool_name: "delegate".to_string(),
                tool_call_id: "call-delegate".to_string(),
                status: "completed".to_string(),
                child_request_id: Some("child-1".to_string()),
                denial_reason: Some("not allowed".to_string()),
                started_at: Some("2026-05-04T12:00:03Z".to_string()),
                completed_at: Some("2026-05-04T12:00:03.500Z".to_string()),
                ..Default::default()
            }],
            responses: vec![TimelineResponseRow {
                request_id: "req-1".to_string(),
                session_id: Some("session-1".to_string()),
                status: Some("completed".to_string()),
                materialized_message_sequence: Some(4),
                completed_at: Some("2026-05-04T12:00:05Z".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        };

        let timeline = build_run_timeline(rows);

        assert_eq!(timeline.request_id, "req-1");
        assert_eq!(timeline.child_request_ids, vec!["child-1"]);
        assert_eq!(timeline.events.len(), 5);
        assert!(matches!(timeline.events[0], RunTimelineEvent::Request(_)));
        assert!(matches!(timeline.events[1], RunTimelineEvent::Message(_)));
        assert!(matches!(timeline.events[2], RunTimelineEvent::ToolCall(_)));
        assert!(matches!(timeline.events[3], RunTimelineEvent::Request(_)));
        assert!(matches!(timeline.events[4], RunTimelineEvent::Response(_)));
        let RunTimelineEvent::ToolCall(tool) = &timeline.events[2] else {
            panic!("expected tool call");
        };
        assert_eq!(tool.request_id.as_deref(), Some("req-1"));
        assert_eq!(tool.child_request_id.as_deref(), Some("child-1"));
        assert_eq!(tool.denial_reason.as_deref(), Some("not allowed"));
    }

    fn provenance_fixture(scope: &str, call_id: Option<(&str, i64)>) -> String {
        let mut manifest = serde_json::json!({
            "manifest_version": 3,
            "status": "captured_only",
            "status_reason": "fixture",
            "capture_seam": "transport_body",
            "capture_scope": scope,
            "assembly_trace": {
                "trace_version": 2,
                "build_path": "budgeted",
                "effective_message_count": 0,
                "assistant_message_ids": [],
                "threaded_tool_results": []
            }
        });
        if let Some((call_id, call_seq)) = call_id {
            manifest["admission"] =
                serde_json::json!({ "call_id": call_id, "call_seq": call_seq });
        }
        manifest.to_string()
    }

    fn rendered_row(
        capture_key: &str,
        scope: &str,
        turn_index: i64,
        attempt: i64,
        created_at: &str,
        provenance_json: Option<String>,
    ) -> TimelineRenderedRequestRow {
        TimelineRenderedRequestRow {
            capture_key: capture_key.to_string(),
            request_doc_id: Some("doc-req-1".to_string()),
            request_id: Some("req-1".to_string()),
            session_id: Some("session-1".to_string()),
            capture_scope: Some(scope.to_string()),
            turn_index: Some(turn_index),
            attempt: Some(attempt),
            capture_version: Some(1),
            model_name: Some("test-model".to_string()),
            source: Some("openai_chat_completions".to_string()),
            prompt_hash: Some("aa".to_string()),
            tools_hash: Some("bb".to_string()),
            provenance_json,
            created_at: Some(created_at.to_string()),
            ..Default::default()
        }
    }

    /// Captures interleave with the inference calls they precede, the exact
    /// admission join surfaces on the event, an unknown manifest version is
    /// reported rather than guessed at, and `inference.2` sorts before
    /// `inference.10` — the ordering is numeric, not lexical.
    #[test]
    fn rendered_captures_interleave_with_their_inference_calls() {
        let rows = RunTimelineRows {
            request: TimelineRequestRow {
                request_id: "req-1".to_string(),
                session_id: Some("session-1".to_string()),
                execution_origin: Some("triggered".to_string()),
                caused_by_trigger_id: Some("trigger-9".to_string()),
                caused_by_trigger_kind: Some("schedule".to_string()),
                created_at: Some("2026-08-07T12:00:00Z".to_string()),
                ..Default::default()
            },
            inference_calls: vec![
                TimelineInferenceCallRow {
                    call_id: "call-1".to_string(),
                    request_id: "req-1".to_string(),
                    call_seq: 1,
                    attempt: 1,
                    call_state: "completed".to_string(),
                    call_kind: "inference".to_string(),
                    prompt_tokens: Some(120),
                    completion_tokens: Some(30),
                    cached_input_tokens: Some(64),
                    queued_at: Some("2026-08-07T12:00:01Z".to_string()),
                    started_at: Some("2026-08-07T12:00:03Z".to_string()),
                    ..Default::default()
                },
                TimelineInferenceCallRow {
                    call_id: "call-2".to_string(),
                    request_id: "req-1".to_string(),
                    call_seq: 2,
                    attempt: 2,
                    call_state: "completed".to_string(),
                    call_kind: "inference".to_string(),
                    queued_at: Some("2026-08-07T12:00:05Z".to_string()),
                    ..Default::default()
                },
            ],
            rendered_requests: vec![
                rendered_row(
                    "rendered:v1:b-second",
                    "inference.1",
                    0,
                    1,
                    "2026-08-07T12:00:06Z",
                    Some(provenance_fixture("inference.1", Some(("call-2", 2)))),
                ),
                rendered_row(
                    "rendered:v1:a-first",
                    "inference.1",
                    0,
                    0,
                    "2026-08-07T12:00:02Z",
                    Some(provenance_fixture("inference.1", Some(("call-1", 1)))),
                ),
                rendered_row(
                    "rendered:v1:c-unknown",
                    "compaction.1",
                    0,
                    0,
                    "2026-08-07T12:00:04Z",
                    Some(r#"{"manifest_version":99,"opaque":true}"#.to_string()),
                ),
                // Same timestamp: only the numeric order key separates these.
                rendered_row(
                    "rendered:v1:e-ten",
                    "inference.10",
                    0,
                    0,
                    "2026-08-07T12:00:07Z",
                    None,
                ),
                rendered_row(
                    "rendered:v1:d-two",
                    "inference.2",
                    3,
                    0,
                    "2026-08-07T12:00:07Z",
                    None,
                ),
            ],
            ..Default::default()
        };

        let timeline = build_run_timeline(rows);

        let kinds: Vec<&str> = timeline
            .events
            .iter()
            .map(|event| match event {
                RunTimelineEvent::Request(_) => "request",
                RunTimelineEvent::RenderedRequest(event) => {
                    event.capture_scope.as_deref().unwrap_or("rendered")
                }
                RunTimelineEvent::InferenceCall(event) => match event.call_seq {
                    1 => "call-1",
                    _ => "call-2",
                },
                _ => "other",
            })
            .collect();
        assert_eq!(
            kinds,
            vec![
                "request",
                "call-1",       // queued 12:00:01
                "inference.1",  // captured 12:00:02 (turn 0 attempt 0)
                "compaction.1", // captured 12:00:04, unknown manifest
                "call-2",       // queued 12:00:05
                "inference.1",  // captured 12:00:06 (turn 0 attempt 1)
                "inference.2",  // 12:00:07 — numeric order key…
                "inference.10", // …not lexical
            ]
        );

        let captures: Vec<&TimelineRenderedRequestEvent> = timeline
            .events
            .iter()
            .filter_map(|event| match event {
                RunTimelineEvent::RenderedRequest(event) => Some(event),
                _ => None,
            })
            .collect();
        assert_eq!(captures[0].call_id.as_deref(), Some("call-1"));
        assert_eq!(captures[0].call_seq, Some(1));
        assert_eq!(captures[0].provenance_status, "captured_only");
        assert_eq!(captures[0].manifest_version, Some(3));
        assert_eq!(captures[0].scope_kind.as_deref(), Some("inference"));
        assert_eq!(captures[0].scope_seq, Some(1));

        assert_eq!(captures[1].provenance_status, "unsupported_manifest");
        assert_eq!(captures[1].manifest_version, Some(99));
        assert_eq!(captures[1].call_seq, None);

        assert_eq!(captures[2].call_id.as_deref(), Some("call-2"));
        assert_eq!(captures[3].provenance_status, "unavailable");

        let request_event = timeline
            .events
            .iter()
            .find_map(|event| match event {
                RunTimelineEvent::Request(event) => Some(event),
                _ => None,
            })
            .expect("request event");
        assert_eq!(request_event.execution_origin.as_deref(), Some("triggered"));
        assert_eq!(
            request_event.caused_by_trigger_id.as_deref(),
            Some("trigger-9")
        );
        assert_eq!(
            request_event.caused_by_trigger_kind.as_deref(),
            Some("schedule")
        );

        let call_event = timeline
            .events
            .iter()
            .find_map(|event| match event {
                RunTimelineEvent::InferenceCall(event) if event.call_seq == 1 => Some(event),
                _ => None,
            })
            .expect("inference call event");
        assert_eq!(call_event.prompt_tokens, Some(120));
        assert_eq!(call_event.completion_tokens, Some(30));
        assert_eq!(call_event.cached_input_tokens, Some(64));
    }

    /// A capture row whose `request_id` is not part of this timeline (another
    /// request in the session) stays out of the event stream.
    #[test]
    fn rendered_captures_for_other_requests_are_excluded() {
        let mut row = rendered_row(
            "rendered:v1:other",
            "inference.1",
            0,
            0,
            "2026-08-07T12:00:02Z",
            None,
        );
        row.request_id = Some("req-other".to_string());
        let rows = RunTimelineRows {
            request: TimelineRequestRow {
                request_id: "req-1".to_string(),
                session_id: Some("session-1".to_string()),
                created_at: Some("2026-08-07T12:00:00Z".to_string()),
                ..Default::default()
            },
            rendered_requests: vec![row],
            ..Default::default()
        };

        let timeline = build_run_timeline(rows);
        assert!(!timeline
            .events
            .iter()
            .any(|event| matches!(event, RunTimelineEvent::RenderedRequest(_))));
    }

    #[test]
    fn retry_summary_counts_failed_attempt_rows_with_successors() {
        let rows = RunTimelineRows {
            request: TimelineRequestRow {
                request_id: "req-recovered".to_string(),
                status: Some("completed".to_string()),
                lifecycle_state: Some("completed".to_string()),
                created_at: Some("2026-05-04T12:00:00Z".to_string()),
                ..Default::default()
            },
            inference_calls: vec![
                inference_call("req-recovered", 3, "completed", None),
                inference_call("req-recovered", 1, "failed", Some("first transient")),
                inference_call("req-recovered", 2, "failed", Some("second transient")),
            ],
            ..Default::default()
        };

        let timeline = build_run_timeline(rows);

        assert_eq!(
            timeline.request.retry_summary,
            RetrySummary {
                retry_count: 2,
                last_transient_error: Some("second transient".to_string()),
                recovered: true,
            }
        );
        assert_eq!(
            timeline
                .inference_calls
                .iter()
                .map(|call| call.call_seq)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        let retry_summary = timeline.events.iter().find_map(|event| match event {
            RunTimelineEvent::Request(request) => Some(&request.retry_summary),
            _ => None,
        });
        assert_eq!(retry_summary, Some(&timeline.request.retry_summary));
    }

    #[test]
    fn retry_summary_excludes_terminal_failed_attempt_without_successor() {
        let rows = RunTimelineRows {
            request: TimelineRequestRow {
                request_id: "req-failed".to_string(),
                status: Some("failed".to_string()),
                lifecycle_state: Some("error".to_string()),
                created_at: Some("2026-05-04T12:00:00Z".to_string()),
                ..Default::default()
            },
            inference_calls: vec![
                inference_call("req-failed", 1, "failed", Some("retried transient")),
                inference_call("req-failed", 2, "failed", Some("terminal provider error")),
            ],
            ..Default::default()
        };

        let timeline = build_run_timeline(rows);

        assert_eq!(
            timeline.request.retry_summary,
            RetrySummary {
                retry_count: 1,
                last_transient_error: Some("retried transient".to_string()),
                recovered: false,
            }
        );
    }

    fn inference_call(
        request_id: &str,
        call_seq: i64,
        call_state: &str,
        failure_reason: Option<&str>,
    ) -> TimelineInferenceCallRow {
        TimelineInferenceCallRow {
            call_id: format!("call-{call_seq}"),
            request_id: request_id.to_string(),
            call_seq,
            attempt: 1,
            call_state: call_state.to_string(),
            failure_reason: failure_reason.map(ToOwned::to_owned),
            queued_at: Some(format!("2026-05-04T12:00:0{call_seq}Z")),
            backend_id: Some("backend-a".to_string()),
            call_kind: "inference".to_string(),
            ..Default::default()
        }
    }
}
