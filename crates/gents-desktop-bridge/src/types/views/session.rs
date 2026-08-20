use serde::Serialize;
use ts_rs::TS;

use super::operations::DerivedCancelCauseView;

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct MessageView {
    pub message_key: String,
    pub request_id: Option<String>,
    pub sequence: Option<i64>,
    pub role: Option<String>,
    pub content: Option<String>,
    pub display_role: Option<String>,
    pub display_content: Option<String>,
    pub reasoning: Option<String>,
    pub has_tool_calls: bool,
    pub has_tool_results: bool,
    pub runtime_control: bool,
    pub timestamp: Option<String>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ToolCallView {
    pub tool_call_key: String,
    pub request_id: Option<String>,
    pub message_sequence: Option<i64>,
    pub tool_name: Option<String>,
    pub tool_call_id: Option<String>,
    pub args: Option<String>,
    pub partial_output_tail: Option<String>,
    pub partial_output_seq: Option<i64>,
    pub result: Option<String>,
    pub status: Option<String>,
    pub lifecycle_state: Option<String>,
    pub child_request_id: Option<String>,
    pub await_mode: Option<String>,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    #[ts(optional = nullable)]
    pub denial: Option<CommandDenialView>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    #[ts(optional = nullable)]
    pub cancel_cause: Option<DerivedCancelCauseView>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CommandDenialView {
    pub category: String,
    pub category_label: String,
    pub rule_id: String,
    pub reason_line: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    #[ts(optional = nullable)]
    pub denied_command: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    #[ts(optional = nullable)]
    pub denied_argument: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    #[ts(optional = nullable)]
    pub denied_subcommand: Option<String>,
    pub diagnostic: String,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ToolDetailFieldView {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ToolDetailValueView {
    pub raw_text: String,
    pub fields: Vec<ToolDetailFieldView>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RenderedToolCallView {
    pub item_key: String,
    pub tool_name: String,
    pub status: Option<String>,
    pub status_kind: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    #[ts(optional = nullable)]
    pub child_request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    #[ts(optional = nullable)]
    pub await_mode: Option<String>,
    pub args: Option<ToolDetailValueView>,
    pub result: Option<ToolDetailValueView>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    #[ts(optional = nullable)]
    pub partial_output_tail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    #[ts(optional = nullable)]
    pub partial_output_seq: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    #[ts(optional = nullable)]
    pub denial: Option<CommandDenialView>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    #[ts(optional = nullable)]
    pub cancel_cause: Option<DerivedCancelCauseView>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ToolResultView {
    pub tool_name: Option<String>,
    pub tool_input: Option<String>,
    pub output_text: Option<String>,
    pub truncated: Option<bool>,
    pub created_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ResponseView {
    pub status: Option<String>,
    pub content: Option<String>,
    pub reasoning: Option<String>,
    pub error_message: Option<String>,
    pub token_count: Option<i64>,
    pub materialized_message_sequence: Option<i64>,
    pub materialized_at: Option<String>,
    pub interrupted_at: Option<String>,
    pub completed_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    #[ts(optional = nullable)]
    pub cancel_cause: Option<DerivedCancelCauseView>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    #[ts(optional = nullable)]
    pub backend_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct PendingTurnView {
    pub request_id: String,
    pub content: String,
    pub selected_skill_ids: Vec<String>,
    pub lifecycle_state: Option<String>,
    pub created_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum RenderedTimelineItem {
    #[serde(rename_all = "camelCase")]
    UserMessage {
        item_key: String,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        #[ts(optional = nullable)]
        request_id: Option<String>,
        sequence: Option<i64>,
        content: String,
        timestamp: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    AssistantMessage {
        item_key: String,
        sequence: Option<i64>,
        content: Option<String>,
        reasoning: Option<String>,
        timestamp: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    ToolGroup {
        item_key: String,
        message_sequence: Option<i64>,
        tools: Vec<RenderedToolCallView>,
    },
    #[serde(rename_all = "camelCase")]
    PendingUserTurn {
        item_key: String,
        request_id: String,
        content: String,
        selected_skill_ids: Vec<String>,
        lifecycle_state: Option<String>,
        created_at: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    LiveAssistant {
        item_key: String,
        content: Option<String>,
        reasoning: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct GoalView {
    pub goal_id: String,
    pub objective: Option<String>,
    pub status: Option<String>,
    pub token_budget: Option<i64>,
    pub tokens_used: i64,
    pub active_time_seconds: i64,
    pub consecutive_blocked_audits: i64,
    pub continuation_sequence: i64,
    pub wrapup_requested: bool,
    pub wrapup_completed: bool,
    pub last_blocked_reason: Option<String>,
    pub last_failure: Option<String>,
    pub completion_evidence: Option<String>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RetryEligibilityView {
    pub eligible: bool,
    pub denial_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct DesktopSessionSnapshot {
    pub session_id: String,
    pub agent_did: Option<String>,
    pub behavior_id: Option<String>,
    pub title: Option<String>,
    pub preview_text: Option<String>,
    pub status: Option<String>,
    pub goal: Option<GoalView>,
    pub turn_state: Option<String>,
    pub latest_request_id: Option<String>,
    pub retry_eligibility: RetryEligibilityView,
    pub latest_response: Option<ResponseView>,
    pub active_response_overlay: Option<ResponseView>,
    pub pending_turn: Option<PendingTurnView>,
    pub timeline_items: Vec<RenderedTimelineItem>,
    #[allow(dead_code)]
    #[serde(skip_serializing)]
    #[ts(skip)]
    pub messages: Vec<MessageView>,
    #[allow(dead_code)]
    #[serde(skip_serializing)]
    #[ts(skip)]
    pub tool_calls: Vec<ToolCallView>,
    #[allow(dead_code)]
    #[serde(skip_serializing)]
    #[ts(skip)]
    pub tool_results: Vec<ToolResultView>,
}
