//! Argument and envelope types for the R4c agent-facing background-work tools.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::descendant_graph::{
    DescendantAuthorizationState, DescendantControlAuthority, DescendantEdge,
    DescendantMaterializationState, DescendantScope,
};

const DEFAULT_LIST_LIMIT: u32 = 20;
const MAX_LIST_LIMIT: u32 = 50;
const MAX_TRANSCRIPT_LIMIT: u32 = 100;

/// Token budget for `read_subagent`. The runtime estimates tokens with the
/// codebase-wide `chars ≈ 4 × tokens` approximation (see
/// `crate::compaction::estimate_tokens`); the rendered transcript is capped at
/// `max_tokens × CHARS_PER_TOKEN_ESTIMATE` characters and `has_more`/
/// `next_sequence` always describe where to resume when the budget caps a read.
pub(crate) const CHARS_PER_TOKEN_ESTIMATE: u32 = 4;
const DEFAULT_TRANSCRIPT_MAX_TOKENS: u32 = 1500;
const MAX_TRANSCRIPT_MAX_TOKENS: u32 = 6000;
const MIN_TRANSCRIPT_MAX_TOKENS: u32 = 16;
/// Token budget for `read_process`. Same `chars ≈ 4 × tokens` approximation.
const DEFAULT_READ_PROCESS_MAX_TOKENS: u32 = 4096;
const MAX_READ_PROCESS_MAX_TOKENS: u32 = 65536;
const MIN_READ_PROCESS_MAX_TOKENS: u32 = 64;

pub(crate) const PER_TOOL_RESULT_SNIPPET_BYTES: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ListStatusFilter {
    #[default]
    Running,
    Terminal,
    All,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListSubagentsArgs {
    #[serde(default)]
    pub(crate) status: ListStatusFilter,
    #[serde(default = "default_list_limit")]
    pub(crate) limit: u32,
    #[serde(default)]
    pub(crate) scope: DescendantScope,
    #[serde(default)]
    pub(crate) after: Option<String>,
}

fn default_list_limit() -> u32 {
    DEFAULT_LIST_LIMIT
}

impl Default for ListSubagentsArgs {
    fn default() -> Self {
        Self {
            status: ListStatusFilter::default(),
            limit: DEFAULT_LIST_LIMIT,
            scope: DescendantScope::DirectChildren,
            after: None,
        }
    }
}

impl ListSubagentsArgs {
    pub(crate) fn validated_limit(&self) -> u32 {
        self.limit.clamp(1, MAX_LIST_LIMIT)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ListBackgroundToolsArgs {
    #[serde(default)]
    pub(crate) status: ListStatusFilter,
    #[serde(default = "default_list_limit")]
    pub(crate) limit: u32,
}

impl ListBackgroundToolsArgs {
    pub(crate) fn validated_limit(&self) -> u32 {
        self.limit.clamp(1, MAX_LIST_LIMIT)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReadSubagentArgs {
    pub(crate) child_request_id: String,
    /// Resume cursor: first transcript sequence to include (default 0 = start).
    #[serde(default)]
    pub(crate) since_sequence: u64,
    /// Token budget for the returned transcript slice. Honest: when the budget
    /// caps the read, the response sets `has_more = true` and `next_sequence`
    /// points at the exact resume cursor (no gap, no overlap).
    #[serde(default = "default_transcript_max_tokens")]
    pub(crate) max_tokens: u32,
    #[serde(default)]
    pub(crate) include_user_messages: bool,
    #[serde(default)]
    pub(crate) include_tool_results: bool,
}

fn default_transcript_max_tokens() -> u32 {
    DEFAULT_TRANSCRIPT_MAX_TOKENS
}

impl ReadSubagentArgs {
    /// Message-block ceiling per page. Kept internal (no longer a model-facing
    /// knob) so the token budget is the single honest cap; this just bounds the
    /// per-page block count so one read can't materialize an unbounded slice.
    pub(crate) fn validated_limit(&self) -> u32 {
        MAX_TRANSCRIPT_LIMIT
    }

    pub(crate) fn validated_max_tokens(&self) -> u32 {
        self.max_tokens
            .clamp(MIN_TRANSCRIPT_MAX_TOKENS, MAX_TRANSCRIPT_MAX_TOKENS)
    }

    /// Byte budget derived from the token budget via the codebase-wide
    /// `chars ≈ 4 × tokens` approximation.
    pub(crate) fn validated_max_chars(&self) -> u32 {
        self.validated_max_tokens()
            .saturating_mul(CHARS_PER_TOKEN_ESTIMATE)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ReadToolOutputArgs {
    pub(crate) tool_call_id: String,
    /// Byte cursor into the captured combined output (default 0 = from start).
    /// Reads forward from `offset`; pages are contiguous (no head/tail drop).
    #[serde(default)]
    pub(crate) offset: u64,
    /// Token budget for the returned slice. The byte budget is derived via the
    /// codebase-wide `chars ≈ 4 × tokens` approximation.
    #[serde(default = "default_read_process_max_tokens")]
    pub(crate) max_tokens: u32,
}

fn default_read_process_max_tokens() -> u32 {
    DEFAULT_READ_PROCESS_MAX_TOKENS
}

impl ReadToolOutputArgs {
    pub(crate) fn validated_max_tokens(&self) -> u32 {
        self.max_tokens
            .clamp(MIN_READ_PROCESS_MAX_TOKENS, MAX_READ_PROCESS_MAX_TOKENS)
    }

    /// Byte budget derived from the token budget via the codebase-wide
    /// `chars ≈ 4 × tokens` approximation.
    pub(crate) fn validated_max_bytes(&self) -> usize {
        (self.validated_max_tokens() as usize).saturating_mul(CHARS_PER_TOKEN_ESTIMATE as usize)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct SteerSubagentArgs {
    pub(crate) child_request_id: String,
    pub(crate) message: String,
    #[serde(default)]
    pub(crate) interrupt: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ListSubagentsEntry {
    pub root_request_id: String,
    pub immediate_parent_request_id: String,
    pub parent_tool_call_id: String,
    pub child_request_id: String,
    pub child_session_id: String,
    /// Friendly model-facing name of the subagent target (from the spawn args).
    /// Matches the `name` passed to `spawn_subagent`. Empty string if the
    /// bridge args did not carry a name (legacy or malformed record).
    pub name: String,
    pub principal_did: String,
    pub behavior_id: String,
    pub deployment_id: String,
    pub await_mode: String,
    pub cancel_policy: Option<String>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub last_update: DateTime<Utc>,
    pub depth: u32,
    pub materialization_state: DescendantMaterializationState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_result_ref: Option<String>,
    pub transcript_cursor: u64,
    pub authorization_state: DescendantAuthorizationState,
    pub control_authority: DescendantControlAuthority,
    pub cursor: String,
    /// #593: present only on a bridge-level entry whose child `AgentRequest`
    /// has not materialized (status `awaiting_child_materialization`) or
    /// whose bridge went terminal without one; explains the bridge state.
    /// Materialized children never carry it, so the happy-path shape is
    /// unchanged.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ListSubagentsResponse {
    pub read_at: DateTime<Utc>,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub entries: Vec<ListSubagentsEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ListBackgroundToolsEntry {
    pub(crate) tool_call_id: String,
    pub(crate) tool_name: String,
    pub(crate) deployment_id: String,
    pub(crate) await_mode: String,
    pub(crate) status: String,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) last_update: DateTime<Utc>,
    pub(crate) stdout_bytes: u64,
    pub(crate) stderr_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ListBackgroundToolsResponse {
    pub(crate) read_at: DateTime<Utc>,
    pub(crate) truncated: bool,
    pub(crate) entries: Vec<ListBackgroundToolsEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReadSubagentResponse {
    pub edge: DescendantEdge,
    pub child_request_id: String,
    pub child_session_id: String,
    pub from_sequence: u64,
    pub through_sequence: u64,
    /// Resume cursor: pass as `since_sequence` on the next read to continue
    /// gap-free from exactly where this page stopped.
    pub next_sequence: u64,
    /// True when the token budget (or per-page block ceiling) capped this read
    /// and more messages exist at or after `next_sequence`.
    pub has_more: bool,
    /// True when the subagent has reached a terminal lifecycle state and will
    /// produce no further transcript output (stop polling once drained).
    pub terminal: bool,
    /// The subagent's current lifecycle state (e.g. "running", "completed",
    /// "failed"), so the model can decide whether to keep polling. #593: for
    /// a background bridge whose child has not materialized, this is the
    /// projected `awaiting_child_materialization` (non-terminal).
    pub lifecycle_state: String,
    /// #593: present only when the child `AgentRequest` has not materialized;
    /// explains the bridge state behind the empty transcript.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
    pub transcript: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ReadToolOutputResponse {
    pub(crate) tool_call_id: String,
    pub(crate) tool_name: String,
    pub(crate) status: String,
    /// The contiguous output slice starting at the requested `offset`. The
    /// captured stdout and stderr are concatenated in capture order behind a
    /// single byte cursor (stdout first, then a labeled `\n--- stderr ---\n`
    /// boundary, then stderr) so an orchestrator pages through ALL output
    /// gap-free with one cursor.
    pub(crate) output: String,
    /// Resume cursor = `offset` + bytes returned in `output`. Pass as `offset`
    /// on the next read to continue with no gap and no overlap.
    pub(crate) next_offset: u64,
    /// Earliest byte offset still available to read. 0 for finished tools
    /// (their full output is persisted, nothing is ever dropped). For a
    /// running tool the live buffer retains only the most recent output, so
    /// this can be > 0; if it exceeds your requested `offset`, the bytes in
    /// between were produced but evicted before you read them.
    pub(crate) first_available_offset: u64,
    /// Total bytes captured so far across the combined stdout/stderr buffer.
    pub(crate) total_bytes: u64,
    /// True when `next_offset < total_bytes` (more output remains to be paged).
    pub(crate) has_more: bool,
    /// True when the process has reached a terminal state (finished).
    pub(crate) exited: bool,
    pub(crate) exit_code: Option<i32>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SteerSubagentResponse {
    pub(crate) child_request_id: String,
    pub(crate) child_session_id: String,
    pub(crate) queued_request_id: String,
    pub(crate) interrupted_active_request_id: Option<String>,
    pub(crate) drained_wake_up_request_ids: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn list_subagents_args_round_trip() {
        let args: ListSubagentsArgs = serde_json::from_value(json!({
            "status": "running",
            "limit": 20
        }))
        .expect("parse");
        assert_eq!(args.status, ListStatusFilter::Running);
        assert_eq!(args.limit, 20);
        assert_eq!(args.validated_limit(), 20);
    }

    #[test]
    fn list_subagents_args_defaults_and_caps() {
        let args: ListSubagentsArgs = serde_json::from_value(json!({})).expect("parse");
        assert_eq!(args.status, ListStatusFilter::Running);
        assert_eq!(args.limit, DEFAULT_LIST_LIMIT);

        let capped: ListSubagentsArgs =
            serde_json::from_value(json!({ "status": "all", "limit": 500 })).expect("parse");
        assert_eq!(capped.status, ListStatusFilter::All);
        assert_eq!(capped.validated_limit(), MAX_LIST_LIMIT);

        let floored: ListSubagentsArgs =
            serde_json::from_value(json!({ "limit": 0 })).expect("parse");
        assert_eq!(floored.validated_limit(), 1);
    }

    #[test]
    fn list_background_tools_args_round_trip_and_defaults() {
        let defaults: ListBackgroundToolsArgs =
            serde_json::from_value(json!({})).expect("parse defaults");
        assert_eq!(defaults.status, ListStatusFilter::Running);
        assert_eq!(defaults.limit, DEFAULT_LIST_LIMIT);

        let explicit: ListBackgroundToolsArgs = serde_json::from_value(json!({
            "status": "terminal",
            "limit": 51
        }))
        .expect("parse explicit");
        assert_eq!(explicit.status, ListStatusFilter::Terminal);
        assert_eq!(explicit.validated_limit(), MAX_LIST_LIMIT);
    }

    #[test]
    fn read_subagent_args_round_trip_and_defaults() {
        let defaults: ReadSubagentArgs = serde_json::from_value(json!({
            "child_request_id": "child-1"
        }))
        .expect("parse defaults");
        assert_eq!(defaults.child_request_id, "child-1");
        assert_eq!(defaults.since_sequence, 0);
        assert_eq!(defaults.max_tokens, DEFAULT_TRANSCRIPT_MAX_TOKENS);
        assert!(!defaults.include_user_messages);
        assert!(!defaults.include_tool_results);
        // Token budget -> byte budget via the 4x approximation.
        assert_eq!(
            defaults.validated_max_chars(),
            DEFAULT_TRANSCRIPT_MAX_TOKENS * CHARS_PER_TOKEN_ESTIMATE
        );

        let explicit: ReadSubagentArgs = serde_json::from_value(json!({
            "child_request_id": "child-2",
            "since_sequence": 7,
            "max_tokens": 999999,
            "include_user_messages": true,
            "include_tool_results": true
        }))
        .expect("parse explicit");
        assert_eq!(explicit.child_request_id, "child-2");
        assert_eq!(explicit.since_sequence, 7);
        // Token budget is capped, and the byte budget tracks the capped tokens.
        assert_eq!(explicit.validated_max_tokens(), MAX_TRANSCRIPT_MAX_TOKENS);
        assert_eq!(
            explicit.validated_max_chars(),
            MAX_TRANSCRIPT_MAX_TOKENS * CHARS_PER_TOKEN_ESTIMATE
        );
        assert!(explicit.include_user_messages);
        assert!(explicit.include_tool_results);

        // Floor: tiny budgets clamp up to the minimum.
        let tiny: ReadSubagentArgs = serde_json::from_value(json!({
            "child_request_id": "child-3",
            "max_tokens": 1
        }))
        .expect("parse tiny");
        assert_eq!(tiny.validated_max_tokens(), MIN_TRANSCRIPT_MAX_TOKENS);
    }

    #[test]
    fn read_tool_output_args_round_trip_and_defaults() {
        let defaults: ReadToolOutputArgs = serde_json::from_value(json!({
            "tool_call_id": "tool-1"
        }))
        .expect("parse defaults");
        assert_eq!(defaults.tool_call_id, "tool-1");
        assert_eq!(defaults.offset, 0);
        assert_eq!(defaults.max_tokens, DEFAULT_READ_PROCESS_MAX_TOKENS);
        assert_eq!(
            defaults.validated_max_bytes(),
            DEFAULT_READ_PROCESS_MAX_TOKENS as usize * CHARS_PER_TOKEN_ESTIMATE as usize
        );

        let explicit: ReadToolOutputArgs = serde_json::from_value(json!({
            "tool_call_id": "tool-2",
            "offset": 512,
            "max_tokens": 1
        }))
        .expect("parse explicit");
        assert_eq!(explicit.tool_call_id, "tool-2");
        assert_eq!(explicit.offset, 512);
        // Token floor applies.
        assert_eq!(explicit.validated_max_tokens(), MIN_READ_PROCESS_MAX_TOKENS);
        assert_eq!(
            explicit.validated_max_bytes(),
            MIN_READ_PROCESS_MAX_TOKENS as usize * CHARS_PER_TOKEN_ESTIMATE as usize
        );

        let capped: ReadToolOutputArgs = serde_json::from_value(json!({
            "tool_call_id": "tool-3",
            "max_tokens": 999999999
        }))
        .expect("parse capped");
        assert_eq!(capped.validated_max_tokens(), MAX_READ_PROCESS_MAX_TOKENS);
    }

    #[test]
    fn steer_subagent_args_round_trip_and_defaults() {
        let defaults: SteerSubagentArgs = serde_json::from_value(json!({
            "child_request_id": "child-1",
            "message": "continue"
        }))
        .expect("parse defaults");
        assert_eq!(defaults.child_request_id, "child-1");
        assert_eq!(defaults.message, "continue");
        assert!(!defaults.interrupt);

        let explicit: SteerSubagentArgs = serde_json::from_value(json!({
            "child_request_id": "child-2",
            "message": "restart with this",
            "interrupt": true
        }))
        .expect("parse explicit");
        assert_eq!(explicit.child_request_id, "child-2");
        assert_eq!(explicit.message, "restart with this");
        assert!(explicit.interrupt);
    }
}
