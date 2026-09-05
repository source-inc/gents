//! Grok shim tool projection.
//!
//! Durable `AgentToolCall` rows are the authoritative source for the Grok
//! pager's `tool_call` / `tool_call_update` `session/update` notifications.
//! This leaf projects those rows, request-id scoped and ordered by
//! `started_at`, into fresh Grok notification payloads: tracker updates
//! (`tool_call` then `tool_call_update` when the observed lifecycle status
//! changes), command titles/status/content, available-command updates, and
//! the execute-kind subprocess lifecycle the pager renders for shell work.
//!
//! Ordering rules mirrored from `xai-grok-pager/src/acp/tracker.rs`:
//!
//! - suppressed tool families (`todo`, `bg-plumbing`, `goal`,
//!   `scheduler`, `workflow`) are never rendered as scrollback blocks;
//! - `task`/`Task`/`spawn_subagent` are deliberately **not**
//!   suppressed: they emit an ordinary standard ACP `tool_call` plus a
//!   same-id terminal `tool_call_update` (title = the durable tool name,
//!   kind `other`), and the pager handles title recognition, waiting, and
//!   suppression on its side;
//! - `send_subagent_message` is recognized by canonical tool meta
//!   (the `x.ai/tool` envelope carrying
//!   `{"version": <TOOL_META_VERSION>, "kind": "ActiveAgentMessage"}`) or
//!   the title `send_subagent_message`, with rawInput
//!   `{"subagent_id", "text"}`;
//! - `available_commands_update` carries an empty ACP command catalog plus
//!   Grok's tool names in `_meta.tools`;
//! - orphan `tool_call_update` values arriving before their `tool_call` are
//!   merged into the pending base by `toolCallId` on arrival.
//!
//! Streaming bash output deltas: while a call is still running, the runtime
//! flushes a bounded `partial_output_tail` (the last bytes of the combined
//! stream, lossily decoded when persisted) onto the `AgentToolCall` row. A
//! running row with no durable result projects that tail as its
//! `rawOutput` — a streaming window, never replayed from scratch or
//! duplicated as durable materialization — decoded with incremental UTF-8
//! semantics: the longest valid UTF-8 prefix is emitted and a trailing
//! replacement character that stands for an incomplete final sequence is
//! held back until the next flush proves it terminal (terminal rows keep
//! it).
//!
//! Terminal ACP client methods `terminal/create`, `terminal/output`,
//! `terminal/wait_for_exit`, `terminal/kill`, and `terminal/release` remain
//! explicit shaped unsupported results: the shim registers
//! `clientTerminal: false`, so shell work runs agent-side and reaches the
//! client purely as execute-kind tool_call events. `terminal/wait_for_exit`
//! answers with the pager's exact `METHOD_NOT_FOUND` error
//! (`wait_for_exit_not_supported("pager")`); the other terminal methods
//! answer with ordinary shaped method-not-found errors of the shim's own
//! wording. No permission document is ever created by this leaf.
//!
//! All queries go through the in-process embedded node (`node.execute`) with
//! every interpolated value passed through `escape_graphql_string`; no HTTP
//! GraphQL helper is used. Projection is bounded and request-id-scoped: one
//! `AgentToolCall` query and one `AgentToolResult` query per request id,
//! with no graph walks beyond the rows of the request being projected.

use std::{char::REPLACEMENT_CHARACTER, sync::Arc};

use anyhow::Result;
use defra_node::EmbeddedNode;
use gents::graphql::{ensure_no_errors, escape_graphql_string};
use serde::Deserialize;
use serde_json::{json, Map, Value};

use super::nonempty;

mod background;

/// JSON-RPC error code the pager uses for a client method the client does
/// not implement (method not supported by the connection).
pub(crate) const JSONRPC_METHOD_NOT_SUPPORTED: i64 = -32601;

/// The exact message the reference pager answers `terminal/wait_for_exit`
/// with: `xai-grok-pager/src/acp/mod.rs::wait_for_exit_not_supported("pager")`
/// builds a `METHOD_NOT_FOUND` error whose message is
/// `"{context} does not handle WaitForTerminalExit"`. The shim reproduces
/// that error exactly, because the pager's adapter falls back to polling
/// when it sees it.
pub(crate) const PAGER_WAIT_FOR_EXIT_MESSAGE: &str = "pager does not handle WaitForTerminalExit";

/// Canonical tool meta envelope key the pager recognizes: the `_meta` field
/// of a `tool_call` carries an envelope object whose `x.ai/tool` entry holds
/// the canonical tool meta (`version`/`kind`), with a `subagentBackground`
/// boolean sibling merged from the row's persisted `await_mode`.
pub(super) const TOOL_META_KEY: &str = "x.ai/tool";

/// Canonical tool meta version marker.
pub(super) const TOOL_META_VERSION: u64 = 1;

/// Canonical tool meta kind for an active agent message
/// (`send_subagent_message`).
pub(super) const TOOL_META_KIND_ACTIVE_AGENT_MESSAGE: &str = "ActiveAgentMessage";

/// Title the pager falls back to when recognizing
/// `send_subagent_message` without canonical meta.
pub(super) const SEND_SUBAGENT_MESSAGE_TITLE: &str = "send_subagent_message";

/// Grok pager tool-call kinds, mapped from durable tool names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ToolCallKind {
    Read,
    Edit,
    Delete,
    Move,
    Search,
    Execute,
    Think,
    Fetch,
    Other,
}

impl ToolCallKind {
    /// The `kind` wire string for this tool call.
    pub(super) fn wire_name(self) -> &'static str {
        match self {
            ToolCallKind::Read => "read",
            ToolCallKind::Edit => "edit",
            ToolCallKind::Delete => "delete",
            ToolCallKind::Move => "move",
            ToolCallKind::Search => "search",
            ToolCallKind::Execute => "execute",
            ToolCallKind::Think => "think",
            ToolCallKind::Fetch => "fetch",
            ToolCallKind::Other => "other",
        }
    }

    /// Map a durable Gents tool name onto the pager kind vocabulary.
    pub(super) fn from_tool_name(tool_name: &str) -> Self {
        match tool_name {
            "read_file" => ToolCallKind::Read,
            "edit_file" | "write_file" | "apply_patch" | "create_file" => ToolCallKind::Edit,
            "delete_file" | "remove_file" => ToolCallKind::Delete,
            "move_file" | "rename_file" => ToolCallKind::Move,
            "grep" | "glob" | "list_files" | "search" => ToolCallKind::Search,
            "bash"
            | "bash_unrestricted"
            | "shell"
            | "execute"
            | "execute_command"
            | "run_command"
            | "run_terminal_command"
            | "run_terminal_cmd"
            | "terminal" => ToolCallKind::Execute,
            "think" | "reasoning" => ToolCallKind::Think,
            "fetch" | "web_fetch" | "web_search" => ToolCallKind::Fetch,
            _ => ToolCallKind::Other,
        }
    }
}

/// Grok pager tool-call statuses, mapped from the authoritative durable
/// lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ToolCallStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

impl ToolCallStatus {
    /// The `status` wire string for this tool call.
    pub(super) fn wire_name(self) -> &'static str {
        match self {
            ToolCallStatus::Pending => "pending",
            ToolCallStatus::InProgress => "in_progress",
            ToolCallStatus::Completed => "completed",
            ToolCallStatus::Failed => "failed",
        }
    }

    /// True when the pager treats the tool call as settled
    /// (`Completed | Failed`); a settled call emits no further updates.
    pub(super) fn is_completed(self) -> bool {
        matches!(self, ToolCallStatus::Completed | ToolCallStatus::Failed)
    }
}

/// One durable `AgentToolCall` row scoped to the projected request. `_docID`
/// is the document identity the spill association joins on: the
/// `AgentToolResult` audit row references it as `tool_call_doc_id`.
#[derive(Clone, Debug, Deserialize)]
pub(super) struct ToolCallRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    tool_call_key: String,
    #[serde(default)]
    tool_call_id: Option<String>,
    #[serde(default)]
    tool_name: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    lifecycle_state: Option<String>,
    #[serde(default)]
    args: Option<String>,
    #[serde(default)]
    result: Option<String>,
    #[serde(default)]
    selected_tool_name: Option<String>,
    #[serde(default)]
    tool_failure_class: Option<String>,
    /// Persisted await mode of the call, when the runtime recorded one.
    /// The exact value `background` marks a background subagent spawn
    /// (`subagentBackground: true` in the projected meta envelope);
    /// anything else — including `None` — is foreground.
    #[serde(default)]
    await_mode: Option<String>,
    /// Durable transcript position of the assistant turn this call belongs
    /// to. The runtime stamps it from the session hook's transcript turn
    /// sequence, the same sequence space `AgentMessage.sequence` allocates
    /// from, which makes it the cross-family chronology key for the
    /// projection engine's merged emission order.
    #[serde(default)]
    message_sequence: Option<i64>,
    /// Bounded live-output tail the runtime flushes onto a running row
    /// (`hook.rs::flush_live_output_tails`): the last bytes of the combined
    /// stream, persisted lossily decoded. Empty for rows with no live
    /// output yet.
    #[serde(default)]
    partial_output_tail: Option<String>,
    #[serde(default)]
    started_at: Option<String>,
    #[serde(default)]
    completed_at: Option<String>,
}

/// One durable `AgentToolResult` conversation audit row for the projected
/// request, when the runtime wrote one. The schema keys the audit row by
/// `tool_call_doc_id` and carries `output_text`; oversized outputs spill
/// here from their `AgentToolCall`.
#[derive(Clone, Debug, Deserialize)]
pub(super) struct ToolResultRow {
    tool_call_doc_id: String,
    #[serde(default)]
    output_text: Option<String>,
}

const TOOL_CALL_FIELDS: &str = r#"
    _docID
    tool_call_key
    tool_call_id
    tool_name
    status
    lifecycle_state
    args
    result
    selected_tool_name
    tool_failure_class
    await_mode
    message_sequence
    partial_output_tail
    started_at
    completed_at
"#;

const TOOL_RESULT_FIELDS: &str = r#"
    tool_call_doc_id
    output_text
"#;

/// The full set of projection events for one request id, ordered so a client
/// can replay each tool call's lifecycle exactly once.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ToolProjection {
    /// Tracker-shaped `tool_call` / `tool_call_update` notifications in
    /// emission order.
    pub updates: Vec<ToolUpdate>,
    /// Durable chronology key per update, aligned 1:1 with `updates`: the
    /// row's `message_sequence` (the shared transcript sequence space) for
    /// per-call events, and `None` for the trailing
    /// `available_commands_update` (which is positionless bookkeeping and
    /// stays last in its family). The projection engine merges families by
    /// this key; `None` sorts after every positioned event of the family.
    pub chronology: Vec<Option<i64>>,
}

/// A single projected tool update, already split by kind so the caller (the
/// projection engine) only needs to stamp `_meta` and wrap it in a
/// `session/update` notification.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum ToolUpdate {
    /// A full `tool_call` tracker registration.
    ToolCall(ToolCallUpdate),
    /// A `tool_call_update` merging fields into the pending base by
    /// `toolCallId`.
    ToolCallUpdate(ToolCallFieldsUpdate),
    /// An `available_commands_update` carrying the visible tool list.
    AvailableCommands(AvailableCommandsUpdate),
    BackgroundTask(background::BackgroundTaskUpdate),
}

impl ToolUpdate {
    /// The `sessionUpdate` discriminator for this update. Test observation
    /// helper: the projection engine discriminates by matching the enum.
    #[cfg(test)]
    pub fn session_update_kind(&self) -> &'static str {
        match self {
            ToolUpdate::ToolCall(_) => "tool_call",
            ToolUpdate::ToolCallUpdate(_) => "tool_call_update",
            ToolUpdate::AvailableCommands(_) => "available_commands_update",
            ToolUpdate::BackgroundTask(update) => update.kind,
        }
    }
}

/// A full `tool_call` tracker registration payload.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ToolCallUpdate {
    pub tool_call_id: String,
    pub title: String,
    pub kind: ToolCallKind,
    pub status: ToolCallStatus,
    pub content: Vec<Value>,
    pub raw_input: Option<Value>,
    pub raw_output: Option<Value>,
    pub meta: Option<Value>,
}

impl ToolCallUpdate {
    /// True when canonical tool meta recognizes this call as an active agent
    /// message (`send_subagent_message`). Test observation helper.
    #[cfg(test)]
    pub fn is_active_agent_message(&self) -> bool {
        is_active_agent_message_meta(self.meta.as_ref())
            || self.title == SEND_SUBAGENT_MESSAGE_TITLE
    }

    /// Render the `tool_call` payload. Optional absent objects
    /// (`rawInput`/`rawOutput`/`_meta`/`content`) are omitted entirely rather
    /// than sent as nulls, matching the pager decoder.
    pub fn to_payload(&self) -> Value {
        let mut payload = json!({
            "sessionUpdate": "tool_call",
            "toolCallId": self.tool_call_id,
            "title": self.title,
            "kind": self.kind.wire_name(),
            "status": self.status.wire_name(),
        });
        let object = payload
            .as_object_mut()
            .expect("tool_call payload is a JSON object");
        if !self.content.is_empty() {
            object.insert("content".to_string(), Value::Array(self.content.clone()));
        }
        if let Some(raw_input) = self.raw_input.as_ref() {
            object.insert("rawInput".to_string(), raw_input.clone());
        }
        if let Some(raw_output) = self.raw_output.as_ref() {
            object.insert("rawOutput".to_string(), raw_output.clone());
        }
        if let Some(meta) = self.meta.as_ref() {
            object.insert("_meta".to_string(), meta.clone());
        }
        payload
    }
}

/// A `tool_call_update` payload: the changed fields merged into the pending
/// base by `toolCallId`.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ToolCallFieldsUpdate {
    pub tool_call_id: String,
    pub fields: Value,
}

impl ToolCallFieldsUpdate {
    /// Render ACP's flattened `tool_call_update` payload.
    #[cfg(test)]
    pub fn to_payload(&self) -> Value {
        tool_call_update_payload(&self.tool_call_id, &self.fields)
    }
}

/// Render ACP's flattened `tool_call_update` shape. The schema models the
/// changed fields with `#[serde(flatten)]`, so nesting them below a synthetic
/// `fields` key makes the pager silently ignore status/content revisions.
pub(super) fn tool_call_update_payload(tool_call_id: &str, fields: &Value) -> Value {
    let mut payload = json!({
        "sessionUpdate": "tool_call_update",
        "toolCallId": tool_call_id,
    });
    if let (Some(payload), Some(fields)) = (payload.as_object_mut(), fields.as_object()) {
        payload.extend(fields.clone());
    }
    payload
}

/// An `available_commands_update` payload.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct AvailableCommandsUpdate {
    pub tools: Vec<String>,
}

impl AvailableCommandsUpdate {
    pub fn to_payload(&self) -> Value {
        json!({
            "sessionUpdate": "available_commands_update",
            "availableCommands": [],
            "_meta": {
                "tools": self.tools,
            },
        })
    }
}

// ---------------------------------------------------------------------------
// Suppressed tool families and canonical meta
// ---------------------------------------------------------------------------

/// The tool families the pager never renders as scrollback blocks. The
/// `task` family (`task`/`Task`/`spawn_subagent`) is deliberately
/// **not** suppressed: those rows emit an ordinary standard ACP
/// `tool_call` plus a same-id terminal `tool_call_update`, and the pager
/// handles title recognition, waiting, and suppression on its side.
pub(super) fn suppressed_tool_family(tool_name: &str) -> Option<&'static str> {
    match tool_name {
        "todo" | "todos" => Some("todo"),
        "bg-plumbing" | "background_plumbing" => Some("bg-plumbing"),
        "goal" | "goals" => Some("goal"),
        "scheduler" => Some("scheduler"),
        "workflow" | "workflows" => Some("workflow"),
        _ => None,
    }
}

/// True when the durable tool name belongs to the `task` family the pager
/// recognizes for subagent spawns: `task`/`Task`/`spawn_subagent`.
/// Every such row projects an object `meta` carrying an explicit
/// `subagentBackground` boolean sibling.
pub(super) fn is_task_family(tool_name: &str) -> bool {
    matches!(tool_name, "task" | "Task" | "spawn_subagent")
}

/// True when canonical tool meta recognizes the call as an active agent
/// message (`send_subagent_message`): the `meta` envelope's `x.ai/tool`
/// entry is a JSON object carrying
/// `version == TOOL_META_VERSION` and `kind == ActiveAgentMessage`.
pub(super) fn is_active_agent_message_meta(meta: Option<&Value>) -> bool {
    let Some(meta) = meta else {
        return false;
    };
    let Some(tool_meta) = meta.get(TOOL_META_KEY) else {
        return false;
    };
    let Some(object) = tool_meta.as_object() else {
        return false;
    };
    object.get("version").and_then(Value::as_u64) == Some(TOOL_META_VERSION)
        && object.get("kind").and_then(Value::as_str) == Some(TOOL_META_KIND_ACTIVE_AGENT_MESSAGE)
}

// ---------------------------------------------------------------------------
// Projection entry point
// ---------------------------------------------------------------------------

/// Project the tool-call lifecycle for one request id.
///
/// Bounded and request-id-scoped: the query set is exactly
/// 1. one `AgentToolCall` query for the rows of this request id, and
/// 2. one `AgentToolResult` query for the same session id (the audit
///    collection is keyed by `tool_call_doc_id`/`session_id`, so the
///    in-memory cross-check below keeps the observation scoped to this
///    request's call rows).
///
/// The projection is read-only: it never replays the session, never
/// duplicates durable materialization, and never writes a document.
pub(super) async fn project_tools(
    node: &Arc<EmbeddedNode>,
    request_id: &str,
    session_id: &str,
    executions: &gents::hook::BackgroundExecutionRegistry,
) -> Result<ToolProjection> {
    let tool_response = node.execute(&tool_calls_query(request_id)).await;
    ensure_no_errors(&tool_response, "grok shim tool call query")?;
    let mut rows = decode_tool_call_rows(&tool_response);
    // Runtime authorization and retained buffers remain the single output
    // owner. These are ephemeral projection inputs, never persisted copies.
    if rows.iter().any(|row| {
        row.await_mode.as_deref() == Some("background") && !observed_status(row).is_completed()
    }) {
        let scope = node.execute(&format!(r#"{{ AgentRequest(filter: {{request_id: {{_eq: "{}"}}}}, limit: 2) {{request_id session_id agent_did requester_did}} }}"#, gents::graphql::escape_graphql_string(request_id))).await;
        ensure_no_errors(&scope, "Grok output scope")?;
        let owners: Vec<gents_protocol::row::AgentRequestRow> = serde_json::from_value(
            scope
                .data
                .as_ref()
                .and_then(|v| v.get("AgentRequest"))
                .cloned()
                .unwrap_or(json!([])),
        )?;
        if let [owner] = owners.as_slice() {
            if owner.session_id.as_deref() == Some(session_id) {
                if let Some(principal) = owner.agent_did.as_deref() {
                    for row in &mut rows {
                        if row.await_mode.as_deref() != Some("background")
                            || observed_status(row).is_completed()
                        {
                            continue;
                        }
                        let Some(id) = row.tool_call_key_tool_call_id() else {
                            continue;
                        };
                        if let Some(snapshot) = executions
                            .read_process_output_snapshot(
                                node,
                                session_id,
                                principal,
                                owner.requester_did.as_deref(),
                                &id,
                            )
                            .await?
                        {
                            if let Some(output) =
                                snapshot["output"].as_str().filter(|s| !s.is_empty())
                            {
                                row.partial_output_tail = Some(json!({
                                "stdout": output,
                                "_gents_output_start": snapshot["first_available_offset"],
                                "stdout_truncation": {"truncated": snapshot["first_available_offset"].as_u64().unwrap_or(0) > 0}
                            }).to_string());
                            }
                        }
                    }
                }
            }
        }
    }
    let result_response = node.execute(&tool_results_query(session_id)).await;
    ensure_no_errors(&result_response, "grok shim tool result query")?;
    let results = decode_tool_result_rows(&result_response);

    let projection = project_tool_rows(&rows, &results);
    Ok(projection)
}

/// The durable chronology sort key of one tool call row:
/// `(message_sequence, stable identity)`. Rows with a `message_sequence`
/// sort before rows without one (a missing sequence falls back to
/// `i64::MAX`), and equal sequences break by the durable stable
/// identity: `tool_call_id`, then `tool_call_key`, then `_docID` — each
/// when non-empty, in that order. The fallback keeps every row keyed by
/// *some* durable identity, so the projected wire order never depends on
/// the query's iteration order, storage layout, or hash-map placement.
fn tool_call_row_sort_key(row: &ToolCallRow) -> (i64, String) {
    (
        row.message_sequence.unwrap_or(i64::MAX),
        row.stable_identity().to_string(),
    )
}

/// Sort decoded `AgentToolCall` rows into the deterministic durable
/// chronology order (sequence, then stable identity). The query's
/// iteration order and any equal-`started_at`/equal-time ties it
/// produces never leak into the projected wire order: this stable sort
/// is the single ordering authority for the family.
fn sort_tool_call_rows(rows: &mut [ToolCallRow]) {
    rows.sort_by_key(tool_call_row_sort_key);
}

/// Pure projection over decoded rows; unit-testable without a node.
pub(super) fn project_tool_rows(rows: &[ToolCallRow], results: &[ToolResultRow]) -> ToolProjection {
    let mut updates: Vec<ToolUpdate> = Vec::new();
    let mut chronology: Vec<Option<i64>> = Vec::new();

    // Deterministic durable chronology first: sequence, then the row's
    // stable durable identity. Without this, two rows with the same
    // `message_sequence` (and rows without one) would emit in whatever
    // order the query iterated them, which is not a wire contract.
    let mut ordered = rows.to_vec();
    sort_tool_call_rows(&mut ordered);

    // Streaming windows owned across the loop so `result_text` can borrow
    // them; one entry per row that needed the live-output fallback.
    let mut live_windows: Vec<String> = Vec::new();

    for row in &ordered {
        let Some(tool_call_id) = row.tool_call_key_tool_call_id() else {
            continue;
        };
        let tool_name = row
            .tool_name
            .as_deref()
            .and_then(nonempty)
            .unwrap_or_default();
        let args = row.args.as_deref().and_then(nonempty).unwrap_or("");
        // Streaming first: a running row with no durable result text yet
        // projects its live output window, so bash output deltas stream
        // through `rawOutput` instead of appearing only at terminalization.
        let durable_result = effective_result_text(row, results);
        let result_text: &str = if durable_result.is_empty() {
            if let Some(window) = live_output_window(row) {
                live_windows.push(window);
                live_windows.last().expect("just pushed").as_str()
            } else {
                ""
            }
        } else {
            durable_result
        };
        // The `meta` envelope: the canonical `x.ai/tool` entry decoded from
        // the recorded args, with a `subagentBackground` boolean sibling
        // merged from the row's persisted `await_mode` (the exact value
        // `background` => true; anything else, including absent, => false).
        // Task-family rows always carry the explicit boolean — false is
        // never omitted — so the pager never has to read a missing key.
        let meta = tool_meta_envelope(tool_name, args, row.await_mode.as_deref());

        // Family suppression drops the rendered block. The `task` family is
        // deliberately not suppressed here: those rows emit an ordinary
        // standard ACP `tool_call` plus a same-id terminal
        // `tool_call_update`, and the pager handles title recognition,
        // waiting, and suppression on its side.
        if suppressed_tool_family(tool_name).is_some() {
            continue;
        }

        let status = observed_status(row);
        let kind = ToolCallKind::from_tool_name(tool_name);
        let title = tool_title(row, &kind);
        let content = tool_content(result_text);
        let mut raw_input = raw_input_value(args, meta.as_ref());
        let background = background::project(row, &tool_call_id, result_text);
        if background.is_some() {
            if let Some(input) = raw_input.as_mut().and_then(Value::as_object_mut) {
                // Native pager defers this execute registration until its
                // task_backgrounded card, including fast terminal calls.
                input.insert("is_background".to_string(), Value::Bool(true));
            }
        }
        let raw_output = if background.is_some() && !result_text.is_empty() {
            Some(background::raw_output(result_text))
        } else {
            raw_output_value(result_text)
        };

        updates.push(ToolUpdate::ToolCall(ToolCallUpdate {
            tool_call_id: tool_call_id.clone(),
            title,
            kind,
            status,
            content,
            raw_input,
            raw_output: raw_output.clone(),
            meta: meta.clone(),
        }));
        chronology.push(row.message_sequence);

        if let Some((started, _)) = &background {
            updates.push(ToolUpdate::BackgroundTask(started.clone()));
            chronology.push(row.message_sequence);
            // The stock pager maps task IDs only on task_backgrounded and
            // consumes cumulative output via tool_call_update, not the base.
            if let Some(raw) = raw_output {
                updates.push(ToolUpdate::BackgroundTask(background::BackgroundTaskUpdate {
                    method: "session/update", kind: "tool_call_update", key: tool_call_id.clone(),
                    payload: json!({"sessionUpdate":"tool_call_update","toolCallId":tool_call_id,"rawOutput":raw}),
                    output_start: serde_json::from_str::<Value>(result_text).ok()
                        .and_then(|value| value["_gents_output_start"].as_u64())
                        .or_else(|| (!durable_result.is_empty()).then_some(0)),
                }));
                chronology.push(row.message_sequence);
            }
        }

        // A terminal first observation still emits the base `tool_call`
        // (a fast call may first be observed already completed); a later
        // lifecycle change emits `tool_call_update`.
        if status.is_completed() {
            updates.push(ToolUpdate::ToolCallUpdate(ToolCallFieldsUpdate {
                tool_call_id,
                fields: json!({
                    "status": status.wire_name(),
                }),
            }));
            chronology.push(row.message_sequence);
        }
        if let Some((_, Some(completed))) = background {
            updates.push(ToolUpdate::BackgroundTask(completed));
            chronology.push(row.message_sequence);
        }
    }

    if !updates.is_empty() {
        updates.push(ToolUpdate::AvailableCommands(AvailableCommandsUpdate {
            tools: available_commands(rows),
        }));
        // The available-commands bookkeeping is positionless: it sorts after
        // every positioned tool event of the family.
        chronology.push(None);
    }

    ToolProjection {
        updates,
        chronology,
    }
}

impl ToolCallRow {
    /// The pager-visible `toolCallId`: the durable `tool_call_id` when
    /// recorded, otherwise the `tool_call_key` (which the runtime shapes as
    /// `<session_id>:<tool_call_id>`).
    fn tool_call_key_tool_call_id(&self) -> Option<String> {
        if let Some(id) = self.tool_call_id.as_deref().and_then(nonempty) {
            return Some(id.to_string());
        }
        nonempty(&self.tool_call_key).map(|key| {
            // Strip a `<session>:` prefix when the key carries one so the
            // pager sees the bare call id it correlates updates by.
            match key.split_once(':') {
                Some((_, tail)) if !tail.is_empty() => tail.to_string(),
                _ => key.to_string(),
            }
        })
    }

    /// The durable stable identity of this row for deterministic ordering
    /// ties: the first non-empty of `tool_call_id`, `tool_call_key`, and
    /// `_docID`. At least one is always non-empty for a decodable row
    /// (`tool_call_key` is the schema's unique index), so equal-sequence
    /// rows always order by a durable value, never by query iteration
    /// order.
    fn stable_identity(&self) -> &str {
        self.tool_call_id
            .as_deref()
            .and_then(nonempty)
            .or_else(|| nonempty(&self.tool_call_key))
            .or_else(|| nonempty(&self.doc_id))
            .unwrap_or("")
    }
}

/// The effective result text for one tool row: the call row's own `result`
/// when present, otherwise the spilled `output_text` of the audit row joined
/// by `tool_call_doc_id` — the audit row names the exact `AgentToolCall`
/// document it spilled for, so two same-name calls never borrow each other's
/// output. The association is only ever an output source, never a status
/// override. When neither durable source has text, a still-streaming row
/// falls back to its live output window (see [`live_output_window`]), which
/// is *not* trimmed: trailing whitespace is meaningful shell output.
fn effective_result_text<'a>(row: &'a ToolCallRow, results: &'a [ToolResultRow]) -> &'a str {
    if let Some(result) = row.result.as_deref().and_then(nonempty) {
        return result;
    }
    let doc_id = nonempty(&row.doc_id);
    doc_id
        .and_then(|doc_id| {
            results
                .iter()
                .find(|result| {
                    result.tool_call_doc_id == doc_id
                        && result.output_text.as_deref().and_then(nonempty).is_some()
                })
                .and_then(|result| result.output_text.as_deref())
                .and_then(nonempty)
        })
        .unwrap_or("")
}

/// The streaming live-output window of one tool row, when the durable
/// result sources have no text yet.
///
/// While a call runs, the runtime flushes a bounded tail of the combined
/// stdout/stderr bytes onto the row (`partial_output_tail`) so the pager's
/// bash output streams through `rawOutput` instead of appearing only at
/// terminalization. The window is a streaming view, never a durable
/// materialization: it is decoded incrementally — the longest valid UTF-8
/// prefix is emitted, and a trailing replacement character that stands for
/// an incomplete final sequence is held back while the row is still
/// running (the next flush or the durable result completes it). A terminal
/// row's tail is kept verbatim: its stream is finished, so a replacement
/// character is genuine decoded data, not a pending sequence.
fn live_output_window(row: &ToolCallRow) -> Option<String> {
    let tail = row.partial_output_tail.as_deref()?;
    // Whitespace is meaningful shell output (trailing newlines are real
    // stream content), so the window is not trimmed — only a blank tail is
    // rejected.
    if tail.trim().is_empty() {
        return None;
    }
    if observed_status(row).is_completed() {
        return Some(tail.to_string());
    }
    let mut decoded = tail.to_string();
    if decoded.ends_with(REPLACEMENT_CHARACTER) {
        decoded.pop();
    }
    if decoded.trim().is_empty() {
        return None;
    }
    Some(decoded)
}

/// The authoritative observed lifecycle status of one tool row. The durable
/// `lifecycle_state` wins; a persisted failure class is always `failed`; a
/// blank row falls back to the legacy `status` vocabulary; anything else is
/// still in progress. (Mirrors the codex shim's `observed_tool_status`
/// without importing it.) The `AgentToolResult` audit collection is
/// conversation-scoped (`tool_call_doc_id`/`session_id`, no request id), so
/// it never overrides the call row's authoritative lifecycle.
fn observed_status(row: &ToolCallRow) -> ToolCallStatus {
    if row
        .tool_failure_class
        .as_deref()
        .and_then(nonempty)
        .is_some()
    {
        return ToolCallStatus::Failed;
    }
    if let Some(lifecycle) = row.lifecycle_state.as_deref().and_then(nonempty) {
        return match lifecycle {
            "pending" | "awaitingApproval" => ToolCallStatus::Pending,
            "running" => ToolCallStatus::InProgress,
            "completed" => ToolCallStatus::Completed,
            "failed" | "timedOut" | "cancelled" => ToolCallStatus::Failed,
            _ => ToolCallStatus::InProgress,
        };
    }
    match row.status.as_deref().and_then(nonempty) {
        Some(status) => match status.to_ascii_lowercase().as_str() {
            "cancelled" | "dead" | "error" | "failed" | "failure" | "timedout" => {
                ToolCallStatus::Failed
            }
            "completed" | "complete" | "success" | "succeeded" => ToolCallStatus::Completed,
            "pending" => ToolCallStatus::Pending,
            _ => ToolCallStatus::InProgress,
        },
        None => ToolCallStatus::InProgress,
    }
}

/// The pager title for a tool call. `send_subagent_message` keeps its
/// recognized title; shell tools surface their command; other tools surface
/// their durable name.
fn tool_title(row: &ToolCallRow, kind: &ToolCallKind) -> String {
    let tool_name = row
        .tool_name
        .as_deref()
        .and_then(nonempty)
        .unwrap_or_default();
    if is_active_agent_message_meta(tool_meta_from_args(row.args.as_deref().unwrap_or("")).as_ref())
        || tool_name == SEND_SUBAGENT_MESSAGE_TITLE
    {
        return SEND_SUBAGENT_MESSAGE_TITLE.to_string();
    }
    if matches!(kind, ToolCallKind::Execute) {
        if let Some(command) = shell_command_from_args(row.args.as_deref().unwrap_or("")) {
            return command;
        }
    }
    if let Some(selected) = row.selected_tool_name.as_deref().and_then(nonempty) {
        return selected.to_string();
    }
    if tool_name.is_empty() {
        "tool".to_string()
    } else {
        tool_name.to_string()
    }
}

/// Extract a shell command from JSON args (`command` key).
fn shell_command_from_args(args: &str) -> Option<String> {
    let object = serde_json::from_str::<Value>(args).ok()?;
    object
        .get("command")
        .and_then(Value::as_str)
        .and_then(nonempty)
        .map(ToOwned::to_owned)
}

/// The `content` blocks for a tool call, derived from the recorded result
/// text. Absent results produce no blocks (the field is omitted on the
/// wire).
fn tool_content(result_text: &str) -> Vec<Value> {
    let trimmed = result_text.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    vec![json!({
        "type": "content",
        "content": {"type": "text", "text": trimmed},
    })]
}

/// The structured `rawInput` for a tool call. JSON args are passed through
/// as the object; `send_subagent_message` is shaped to
/// `{"subagent_id", "text"}`; non-JSON args are wrapped as a single
/// `input` string.
fn raw_input_value(args: &str, meta: Option<&Value>) -> Option<Value> {
    let trimmed = args.trim();
    if trimmed.is_empty() {
        return None;
    }
    if is_active_agent_message_meta(meta) {
        if let Ok(object) = serde_json::from_str::<Value>(trimmed) {
            let mut shaped = Map::new();
            for key in ["subagent_id", "subagentId", "text"] {
                if let Some(value) = object.get(key) {
                    shaped.insert(key.to_string(), value.clone());
                }
            }
            return Some(Value::Object(shaped));
        }
    }
    if let Ok(object) = serde_json::from_str::<Value>(trimmed) {
        return Some(object);
    }
    Some(json!({ "input": trimmed }))
}

/// The structured `rawOutput` for a tool call. JSON results pass through;
/// shell results surface their recorded exit code when present; plain text
/// is wrapped as a single `output` string.
fn raw_output_value(result_text: &str) -> Option<Value> {
    let trimmed = result_text.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(object) = serde_json::from_str::<Value>(trimmed) {
        return Some(object);
    }
    Some(json!({ "output": trimmed }))
}

/// Decode the canonical tool meta envelope for a tool call. The recorded
/// args may carry the canonical meta under the `x.ai/tool` key; the
/// projected `meta` is the full envelope object — the `x.ai/tool` entry
/// verbatim plus a `subagentBackground` boolean sibling merged from the
/// row's persisted `await_mode` (the exact value `background` => true;
/// anything else, including absent, => false).
///
/// Task-family rows (`task`/`Task`/`spawn_subagent`) always carry
/// an object `meta` with the explicit `subagentBackground` boolean — false
/// is never omitted, so the pager reads a definite value for every
/// recognized spawn. When canonical meta exists it is preserved verbatim
/// inside the full `"x.ai/tool": {...}` envelope with the boolean merged
/// beside it. Other rows keep the settled shape: no canonical meta and no
/// background await mode project no `meta` at all, and a canonical meta
/// without a background await mode keeps the bare envelope.
fn tool_meta_envelope(tool_name: &str, args: &str, await_mode: Option<&str>) -> Option<Value> {
    let tool_meta = tool_meta_from_args(args);
    let background = matches!(await_mode, Some("background"));
    if is_task_family(tool_name) {
        let mut envelope = Map::new();
        if let Some(tool_meta) = tool_meta {
            envelope.insert(TOOL_META_KEY.to_string(), tool_meta);
        }
        envelope.insert("subagentBackground".to_string(), Value::Bool(background));
        return Some(Value::Object(envelope));
    }
    match (tool_meta, background) {
        (None, false) => None,
        (Some(tool_meta), false) => Some(json!({
            TOOL_META_KEY: tool_meta,
        })),
        (tool_meta, true) => {
            let mut envelope = Map::new();
            if let Some(tool_meta) = tool_meta {
                envelope.insert(TOOL_META_KEY.to_string(), tool_meta);
            }
            envelope.insert("subagentBackground".to_string(), Value::Bool(true));
            Some(Value::Object(envelope))
        }
    }
}

/// Decode the canonical tool meta recorded in the row's args, when present.
fn tool_meta_from_args(args: &str) -> Option<Value> {
    let trimmed = args.trim();
    if trimmed.is_empty() {
        return None;
    }
    let object = serde_json::from_str::<Value>(trimmed).ok()?;
    object.get(TOOL_META_KEY).cloned()
}

/// The visible tool list for `available_commands_update`, derived from the
/// projected request's non-suppressed durable tool names, deduplicated and
/// ordered.
fn available_commands(rows: &[ToolCallRow]) -> Vec<String> {
    let mut tools: Vec<String> = rows
        .iter()
        .filter_map(|row| row.tool_name.as_deref().and_then(nonempty))
        .filter(|name| suppressed_tool_family(name).is_none())
        .map(ToOwned::to_owned)
        .collect();
    tools.sort();
    tools.dedup();
    tools
}

// ---------------------------------------------------------------------------
// Terminal ACP client method stubs
// ---------------------------------------------------------------------------

/// The exact not-supported JSON-RPC error value the reference pager answers
/// `terminal/wait_for_exit` with:
/// `xai-grok-pager/src/acp/mod.rs::wait_for_exit_not_supported("pager")`
/// returns a `METHOD_NOT_FOUND` error with the message
/// `"pager does not handle WaitForTerminalExit"`. The pager's adapter
/// recognizes this failure and falls back to polling, so the shim answers
/// the method with the same code and message verbatim.
pub(crate) fn wait_for_exit_not_supported() -> Value {
    json!({
        "code": JSONRPC_METHOD_NOT_SUPPORTED,
        "message": PAGER_WAIT_FOR_EXIT_MESSAGE,
    })
}

/// The shaped method-not-found error value for a terminal ACP client method
/// other than `terminal/wait_for_exit` (`terminal/create`, `terminal/output`,
/// `terminal/kill`, `terminal/release`). The shim registers
/// `clientTerminal: false` and never routes terminal work to the client, so
/// these stay explicit on the wire. The wording is the shim's own — only
/// `terminal/wait_for_exit` reproduces the pager's exact message.
pub(crate) fn terminal_not_supported_error(method: &str) -> Value {
    json!({
        "code": JSONRPC_METHOD_NOT_SUPPORTED,
        "message": format!(
            "{method}: terminal methods are not supported by the Gents Grok shim; the \
             connection registers clientTerminal=false and shell work runs agent-side"
        ),
    })
}

/// Route one terminal ACP client method to its shaped unsupported result.
///
/// Returns `Err(not_supported_error)` for the five known terminal methods
/// (`terminal/create`, `terminal/output`, `terminal/wait_for_exit`,
/// `terminal/kill`, `terminal/release`) so the ACP service can surface the
/// shaped error, and `Ok(())` never: the shim does not implement a client
/// terminal, does not synthesize terminal documents, and does not create
/// permission documents. `terminal/wait_for_exit` carries the pager's exact
/// `METHOD_NOT_FOUND` error; the other four carry the shim's shaped
/// method-not-found wording. Unknown methods are routed through the caller's
/// generic method-not-found handling.
pub(crate) fn handle_terminal_client_method(method: &str) -> std::result::Result<(), Value> {
    match method {
        "terminal/wait_for_exit" => Err(wait_for_exit_not_supported()),
        "terminal/create" | "terminal/output" | "terminal/kill" | "terminal/release" => {
            Err(terminal_not_supported_error(method))
        }
        other => Err(json!({
            "code": JSONRPC_METHOD_NOT_SUPPORTED,
            "message": format!(
                "{other}: unknown terminal method; the Grok shim supports only \
                 terminal/create, terminal/output, terminal/wait_for_exit, \
                 terminal/kill, and terminal/release as shaped unsupported results"
            ),
        })),
    }
}

// ---------------------------------------------------------------------------
// Queries and decoding
// ---------------------------------------------------------------------------

fn tool_calls_query(request_id: &str) -> String {
    format!(
        r#"{{
            AgentToolCall(
                filter: {{ request_id: {{ _eq: "{request_id}" }} }},
                order: {{ started_at: ASC }}
            ) {{ {TOOL_CALL_FIELDS} }}
        }}"#,
        request_id = escape_graphql_string(request_id),
    )
}

fn tool_results_query(session_id: &str) -> String {
    format!(
        r#"{{
            AgentToolResult(
                filter: {{ session_id: {{ _eq: "{session_id}" }} }},
                order: {{ created_at: ASC }}
            ) {{ {TOOL_RESULT_FIELDS} }}
        }}"#,
        session_id = escape_graphql_string(session_id),
    )
}

fn decode_tool_call_rows(response: &defra_node::QueryResponse) -> Vec<ToolCallRow> {
    response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentToolCall"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|row| match serde_json::from_value::<ToolCallRow>(row) {
            Ok(row) => Some(row),
            Err(error) => {
                tracing::debug!(
                    %error,
                    "grok shim skipped an undecodable AgentToolCall row"
                );
                None
            }
        })
        .collect()
}

fn decode_tool_result_rows(response: &defra_node::QueryResponse) -> Vec<ToolResultRow> {
    response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentToolResult"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|row| match serde_json::from_value::<ToolResultRow>(row) {
            Ok(row) => Some(row),
            Err(error) => {
                tracing::debug!(
                    %error,
                    "grok shim skipped an undecodable AgentToolResult row"
                );
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    #[test]
    fn tool_results_use_acp_tool_call_content_not_bare_message_blocks() {
        assert_eq!(
            super::tool_content("tool call cancelled"),
            vec![serde_json::json!({
                "type":"content", "content":{"type":"text","text":"tool call cancelled"}
            })]
        );
        assert!(super::tool_content("  ").is_empty());
    }

    use super::*;

    fn tool_row(tool_name: &str, lifecycle_state: Option<&str>) -> ToolCallRow {
        ToolCallRow {
            doc_id: format!("doc-{tool_name}"),
            tool_call_key: format!("session-1:call-{tool_name}"),
            tool_call_id: Some(format!("call-{tool_name}")),
            tool_name: Some(tool_name.to_string()),
            status: None,
            lifecycle_state: lifecycle_state.map(ToOwned::to_owned),
            args: Some(r#"{"command":"echo gents-subprocess-probe"}"#.to_string()),
            result: Some("gents-subprocess-probe".to_string()),
            selected_tool_name: None,
            tool_failure_class: None,
            await_mode: None,
            message_sequence: None,
            partial_output_tail: None,
            started_at: None,
            completed_at: None,
        }
    }

    #[test]
    fn kind_wire_names_match_grok_pager() {
        assert_eq!(ToolCallKind::Read.wire_name(), "read");
        assert_eq!(ToolCallKind::Edit.wire_name(), "edit");
        assert_eq!(ToolCallKind::Delete.wire_name(), "delete");
        assert_eq!(ToolCallKind::Move.wire_name(), "move");
        assert_eq!(ToolCallKind::Search.wire_name(), "search");
        assert_eq!(ToolCallKind::Execute.wire_name(), "execute");
        assert_eq!(ToolCallKind::Think.wire_name(), "think");
        assert_eq!(ToolCallKind::Fetch.wire_name(), "fetch");
        assert_eq!(ToolCallKind::Other.wire_name(), "other");
    }

    #[test]
    fn native_task_output_follows_registration_and_keeps_large_snapshot() {
        let mut row = tool_row("bash", Some("running"));
        row.result = None;
        row.await_mode = Some("background".into());
        row.args = Some(r#"{"command":"produce output"}"#.into());
        row.started_at = Some("2026-01-01T00:00:00Z".into());
        let text = format!("PREFIX{}SUFFIX", "0123456789".repeat(10_000));
        row.partial_output_tail =
            Some(json!({"stdout":text,"stdout_truncation":{"truncated":true}}).to_string());
        let projected = project_tool_rows(&[row], &[]);
        let start = projected
            .updates
            .iter()
            .position(
                |u| matches!(u, ToolUpdate::BackgroundTask(v) if v.kind == "task_backgrounded"),
            )
            .unwrap();
        let output = projected
            .updates
            .iter()
            .position(
                |u| matches!(u, ToolUpdate::BackgroundTask(v) if v.kind == "tool_call_update"),
            )
            .unwrap();
        assert!(start < output);
        let ToolUpdate::BackgroundTask(update) = &projected.updates[output] else {
            unreachable!()
        };
        assert_eq!(update.payload["rawOutput"]["output_for_prompt"], text);
        assert_eq!(update.payload["rawOutput"]["truncated"], true);
    }

    #[test]
    fn kind_mapping_covers_durable_tool_names() {
        assert_eq!(
            ToolCallKind::from_tool_name("read_file"),
            ToolCallKind::Read
        );
        for name in [
            "bash",
            "bash_unrestricted",
            "shell",
            "execute",
            "execute_command",
            "run_command",
            "run_terminal_command",
            "run_terminal_cmd",
            "terminal",
        ] {
            assert_eq!(
                ToolCallKind::from_tool_name(name),
                ToolCallKind::Execute,
                "{name}"
            );
        }
        assert_eq!(ToolCallKind::from_tool_name("grep"), ToolCallKind::Search);
        assert_eq!(
            ToolCallKind::from_tool_name("unknown_tool"),
            ToolCallKind::Other
        );
    }

    #[test]
    fn status_wire_names_match_grok_pager() {
        assert_eq!(ToolCallStatus::Pending.wire_name(), "pending");
        assert_eq!(ToolCallStatus::InProgress.wire_name(), "in_progress");
        assert_eq!(ToolCallStatus::Completed.wire_name(), "completed");
        assert_eq!(ToolCallStatus::Failed.wire_name(), "failed");
    }

    #[test]
    fn completed_tool_call_payload_matches_grok_wire_shape() {
        let row = tool_row("bash", Some("completed"));
        let projection = project_tool_rows(&[row], &[]);
        let ToolUpdate::ToolCall(call) = &projection.updates[0] else {
            panic!("first update should be a tool_call");
        };
        assert_eq!(call.tool_call_id, "call-bash");
        assert_eq!(call.kind, ToolCallKind::Execute);
        assert_eq!(call.status, ToolCallStatus::Completed);
        assert_eq!(call.title, "echo gents-subprocess-probe");
        assert_eq!(
            call.raw_input
                .as_ref()
                .and_then(|input| input.get("command"))
                .and_then(Value::as_str),
            Some("echo gents-subprocess-probe")
        );
        assert_eq!(
            call.raw_output
                .as_ref()
                .and_then(|output| output.get("output"))
                .and_then(Value::as_str),
            Some("gents-subprocess-probe")
        );
        let payload = call.to_payload();
        assert_eq!(payload["sessionUpdate"], "tool_call");
        assert_eq!(payload["toolCallId"], "call-bash");
        assert_eq!(payload["kind"], "execute");
        assert_eq!(payload["status"], "completed");
        assert_eq!(payload["title"], "echo gents-subprocess-probe");
    }

    #[test]
    fn terminal_status_emits_tool_call_update() {
        let row = tool_row("bash", Some("completed"));
        let projection = project_tool_rows(&[row], &[]);
        assert!(projection.updates.len() >= 2);
        let ToolUpdate::ToolCallUpdate(update) = &projection.updates[1] else {
            panic!("second update should be a tool_call_update");
        };
        assert_eq!(update.tool_call_id, "call-bash");
        assert_eq!(update.fields["status"], "completed");
        let payload = update.to_payload();
        assert_eq!(payload["sessionUpdate"], "tool_call_update");
        assert_eq!(payload["toolCallId"], "call-bash");
        assert_eq!(payload["status"], "completed");
        assert!(payload.get("fields").is_none());
    }

    #[test]
    fn running_tool_call_has_no_update_and_stays_in_progress() {
        let row = tool_row("bash", Some("running"));
        let projection = project_tool_rows(&[row], &[]);
        assert_eq!(projection.updates.len(), 2); // tool_call + available_commands
        let ToolUpdate::ToolCall(call) = &projection.updates[0] else {
            panic!("first update should be a tool_call");
        };
        assert_eq!(call.status, ToolCallStatus::InProgress);
    }

    #[test]
    fn task_family_rows_render_ordinary_tool_calls_and_terminal_updates() {
        // The `task` family is deliberately NOT suppressed: `task`, `Task`,
        // and `spawn_subagent` rows emit an ordinary standard ACP
        // `tool_call` plus a same-id terminal `tool_call_update` (title =
        // the durable tool name, kind `other`); the pager handles title
        // recognition, waiting, and suppression on its side.
        for name in ["task", "Task", "spawn_subagent"] {
            let mut row = tool_row(name, Some("completed"));
            row.args = Some(r#"{"description":"scout the repo"}"#.to_string());
            row.result = None;
            let projection = project_tool_rows(&[row], &[]);
            let ToolUpdate::ToolCall(call) = &projection.updates[0] else {
                panic!("{name} must render an ordinary tool_call");
            };
            assert_eq!(call.tool_call_id, format!("call-{name}"), "{name}");
            assert_eq!(call.title, name, "{name} keeps its durable tool name");
            assert_eq!(call.kind, ToolCallKind::Other, "{name}");
            assert_eq!(call.status, ToolCallStatus::Completed, "{name}");
            // The same-id terminal update follows the base.
            let ToolUpdate::ToolCallUpdate(update) = &projection.updates[1] else {
                panic!("{name} must emit a same-id terminal tool_call_update");
            };
            assert_eq!(update.tool_call_id, format!("call-{name}"), "{name}");
            assert_eq!(update.fields["status"], "completed", "{name}");
        }
    }

    #[test]
    fn task_family_rows_always_carry_explicit_subagent_background() {
        // Every recognized task-family row projects an object `meta` with an
        // explicit `subagentBackground` boolean: absent, `foreground`, and
        // unknown persisted await modes are all explicit false; only the
        // exact value `background` is true. False is never omitted.
        for name in ["task", "Task", "spawn_subagent"] {
            for (await_mode, expected) in [
                (None, false),
                (Some("foreground"), false),
                (Some("Background"), false),
                (Some("background "), false),
                (Some("unknown-mode"), false),
                (Some("background"), true),
            ] {
                let mut row = tool_row(name, Some("running"));
                row.args = Some(r#"{"description":"scout the repo"}"#.to_string());
                row.result = None;
                row.await_mode = await_mode.map(ToOwned::to_owned);
                let projection = project_tool_rows(&[row], &[]);
                let ToolUpdate::ToolCall(call) = &projection.updates[0] else {
                    panic!("{name} must render a tool_call");
                };
                let meta = call
                    .meta
                    .as_ref()
                    .unwrap_or_else(|| panic!("{name} must always carry an object meta"));
                assert!(meta.is_object(), "{name} meta must be an object: {meta}");
                assert_eq!(
                    meta.get("subagentBackground"),
                    Some(&Value::Bool(expected)),
                    "{name} with await_mode {await_mode:?} must carry subagentBackground \
                     == {expected} explicitly"
                );
                let payload = call.to_payload();
                assert_eq!(
                    payload["_meta"]["subagentBackground"], expected,
                    "{name} payload must carry the explicit boolean"
                );
            }
        }
    }

    #[test]
    fn task_family_canonical_meta_envelope_survives_with_boolean_beside_it() {
        // A task-family row whose args carry the canonical `x.ai/tool` meta
        // keeps the full envelope verbatim, with `subagentBackground` merged
        // as a sibling — false for a foreground await mode, true for the
        // exact `background` value.
        let tool_meta = json!({
            "version": TOOL_META_VERSION,
            "kind": "SubagentSpawn",
        });
        for (await_mode, expected) in [(Some("foreground"), false), (Some("background"), true)] {
            let mut row = tool_row("spawn_subagent", Some("running"));
            row.args = Some(format!(
                r#"{{"description":"scout the repo","{TOOL_META_KEY}":{tool_meta}}}"#
            ));
            row.result = None;
            row.await_mode = await_mode.map(ToOwned::to_owned);
            let projection = project_tool_rows(&[row], &[]);
            let ToolUpdate::ToolCall(call) = &projection.updates[0] else {
                panic!("spawn_subagent must render a tool_call");
            };
            let meta = call.meta.as_ref().expect("canonical meta row carries meta");
            assert_eq!(
                meta[TOOL_META_KEY], tool_meta,
                "the full x.ai/tool envelope must survive verbatim"
            );
            assert_eq!(meta["subagentBackground"], expected);
            let payload = call.to_payload();
            assert_eq!(payload["_meta"][TOOL_META_KEY], tool_meta);
            assert_eq!(payload["_meta"]["subagentBackground"], expected);
        }
    }

    #[test]
    fn background_await_mode_merges_subagent_background_true_into_meta() {
        // The exact persisted value `background` => `subagentBackground:
        // true` in the meta envelope; anything else stays foreground.
        let mut row = tool_row("spawn_subagent", Some("running"));
        row.args = Some(r#"{"description":"scout the repo"}"#.to_string());
        row.result = None;
        row.await_mode = Some("background".to_string());
        let projection = project_tool_rows(&[row], &[]);
        let ToolUpdate::ToolCall(call) = &projection.updates[0] else {
            panic!("spawn_subagent must render a tool_call");
        };
        let meta = call.meta.as_ref().expect("background row carries meta");
        assert_eq!(meta["subagentBackground"], true);
        let payload = call.to_payload();
        assert_eq!(payload["_meta"]["subagentBackground"], true);

        // A non-`background` await mode is foreground: explicit false, never
        // an omitted key.
        let mut foreground = tool_row("spawn_subagent", Some("running"));
        foreground.args = Some(r#"{"description":"scout the repo"}"#.to_string());
        foreground.result = None;
        foreground.await_mode = Some("foreground".to_string());
        let projection = project_tool_rows(&[foreground], &[]);
        let ToolUpdate::ToolCall(call) = &projection.updates[0] else {
            panic!("foreground spawn_subagent must render a tool_call");
        };
        let meta = call
            .meta
            .as_ref()
            .expect("task-family rows always carry meta");
        assert_eq!(meta["subagentBackground"], false);
    }

    #[test]
    fn other_suppressed_families_render_nothing() {
        for name in ["todo", "bg-plumbing", "goal", "scheduler", "workflow"] {
            let row = tool_row(name, Some("running"));
            let projection = project_tool_rows(&[row], &[]);
            assert!(
                projection
                    .updates
                    .iter()
                    .all(|update| update.session_update_kind() != "tool_call"),
                "{name} must not render"
            );
        }
    }

    #[test]
    fn send_subagent_message_is_recognized_by_canonical_meta() {
        let tool_meta = json!({
            "version": TOOL_META_VERSION,
            "kind": TOOL_META_KIND_ACTIVE_AGENT_MESSAGE,
        });
        let envelope = json!({ TOOL_META_KEY: tool_meta });
        assert!(is_active_agent_message_meta(Some(&envelope)));
        assert!(!is_active_agent_message_meta(None));
        assert!(!is_active_agent_message_meta(Some(&json!({"version": 2}))));
        assert!(!is_active_agent_message_meta(Some(&json!({
            TOOL_META_KEY: {"version": 2},
        }))));
        let mut row = tool_row("send_subagent_message", Some("running"));
        row.args = Some(format!(
            r#"{{"subagent_id":"sub-1","text":"hi","{TOOL_META_KEY}":{tool_meta}}}"#
        ));
        row.result = None;
        let projection = project_tool_rows(&[row], &[]);
        let ToolUpdate::ToolCall(call) = &projection.updates[0] else {
            panic!("first update should be a tool_call");
        };
        assert!(call.is_active_agent_message());
        assert_eq!(call.title, SEND_SUBAGENT_MESSAGE_TITLE);
        assert_eq!(
            call.raw_input
                .as_ref()
                .and_then(|input| input.get("subagent_id"))
                .and_then(Value::as_str),
            Some("sub-1")
        );
    }

    #[test]
    fn send_subagent_message_is_recognized_by_title_fallback() {
        let mut row = tool_row("send_subagent_message", Some("running"));
        row.args = Some(r#"{"subagent_id":"sub-1","text":"hi"}"#.to_string());
        row.result = None;
        let projection = project_tool_rows(&[row], &[]);
        let ToolUpdate::ToolCall(call) = &projection.updates[0] else {
            panic!("first update should be a tool_call");
        };
        assert!(call.is_active_agent_message());
        assert_eq!(call.title, SEND_SUBAGENT_MESSAGE_TITLE);
    }

    #[test]
    fn available_commands_update_carries_visible_tools_meta() {
        let rows = vec![
            tool_row("bash", Some("completed")),
            tool_row("read_file", Some("completed")),
            tool_row("todo", Some("completed")),
        ];
        let projection = project_tool_rows(&rows, &[]);
        let ToolUpdate::AvailableCommands(update) = projection
            .updates
            .iter()
            .find(|update| update.session_update_kind() == "available_commands_update")
            .cloned()
            .expect("available_commands_update should be emitted")
        else {
            unreachable!("already checked the kind");
        };
        assert_eq!(update.tools, vec!["bash", "read_file"]);
        let payload = update.to_payload();
        assert_eq!(payload["sessionUpdate"], "available_commands_update");
        assert_eq!(payload["availableCommands"], json!([]));
        assert_eq!(payload["_meta"]["tools"], json!(["bash", "read_file"]));
    }

    #[test]
    fn empty_rows_project_nothing() {
        let projection = project_tool_rows(&[], &[]);
        assert!(projection.updates.is_empty());
        assert!(projection.chronology.is_empty());
    }

    #[test]
    fn blank_lifecycle_falls_back_to_status_vocabulary() {
        let mut row = tool_row("bash", None);
        row.status = Some("completed".to_string());
        let projection = project_tool_rows(&[row], &[]);
        let ToolUpdate::ToolCall(call) = &projection.updates[0] else {
            panic!("first update should be a tool_call");
        };
        assert_eq!(call.status, ToolCallStatus::Completed);
    }

    #[test]
    fn failure_class_is_always_failed() {
        let mut row = tool_row("bash", Some("running"));
        row.tool_failure_class = Some("transport".to_string());
        let projection = project_tool_rows(&[row], &[]);
        let ToolUpdate::ToolCall(call) = &projection.updates[0] else {
            panic!("first update should be a tool_call");
        };
        assert_eq!(call.status, ToolCallStatus::Failed);
    }

    #[test]
    fn audit_rows_never_override_the_call_row_lifecycle() {
        let mut row = tool_row("bash", Some("running"));
        row.status = Some("success".to_string());
        row.result = None;
        let results = vec![ToolResultRow {
            tool_call_doc_id: "doc-bash".to_string(),
            output_text: Some("spilled oversized output".to_string()),
        }];
        let projection = project_tool_rows(&[row], &results);
        let ToolUpdate::ToolCall(call) = &projection.updates[0] else {
            panic!("first update should be a tool_call");
        };
        // The durable call row's lifecycle_state is authoritative; the
        // conversation-scoped audit row cannot downgrade it. The spilled
        // output still surfaces as the call's rawOutput/content.
        assert_eq!(call.status, ToolCallStatus::InProgress);
        assert_eq!(
            call.raw_output
                .as_ref()
                .and_then(|output| output.get("output"))
                .and_then(Value::as_str),
            Some("spilled oversized output")
        );
    }

    #[test]
    fn spilled_results_join_by_tool_call_doc_id_not_tool_name() {
        // Two same-name calls in one request, each with its own distinct
        // spilled audit row. The spill association is the audit row's
        // `tool_call_doc_id` → the call row's `_docID`, so neither call may
        // borrow the other's output by matching on tool name.
        let mut first = tool_row("bash", Some("completed"));
        first.result = None;
        let mut second = tool_row("bash", Some("completed"));
        second.doc_id = "doc-bash-second".to_string();
        second.tool_call_key = "session-1:call-bash-second".to_string();
        second.tool_call_id = Some("call-bash-second".to_string());
        second.result = None;

        // Deliberately ordered so a naive first-same-tool_name match would
        // hand both calls the first spill.
        let results = vec![
            ToolResultRow {
                tool_call_doc_id: "doc-bash-second".to_string(),
                output_text: Some("second spilled output".to_string()),
            },
            ToolResultRow {
                tool_call_doc_id: "doc-bash".to_string(),
                output_text: Some("first spilled output".to_string()),
            },
        ];

        let projection = project_tool_rows(&[first, second], &results);
        let output_for = |tool_call_id: &str| {
            projection
                .updates
                .iter()
                .find_map(|update| match update {
                    ToolUpdate::ToolCall(call) if call.tool_call_id == tool_call_id => call
                        .raw_output
                        .as_ref()
                        .and_then(|output| output.get("output"))
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                    _ => None,
                })
                .unwrap_or_default()
        };
        assert_eq!(output_for("call-bash"), "first spilled output");
        assert_eq!(output_for("call-bash-second"), "second spilled output");

        // And a call whose `_docID` has no audit row gets no borrowed
        // output, even when a same-name spill exists.
        let mut orphan = tool_row("bash", Some("completed"));
        orphan.doc_id = "doc-bash-orphan".to_string();
        orphan.tool_call_key = "session-1:call-bash-orphan".to_string();
        orphan.tool_call_id = Some("call-bash-orphan".to_string());
        orphan.result = None;
        let projection = project_tool_rows(&[orphan], &results);
        let ToolUpdate::ToolCall(call) = &projection.updates[0] else {
            panic!("first update should be a tool_call");
        };
        assert_eq!(call.raw_output, None);
    }

    #[test]
    fn absent_result_omits_content_and_raw_output() {
        let mut row = tool_row("bash", Some("completed"));
        row.result = None;
        let projection = project_tool_rows(&[row], &[]);
        let ToolUpdate::ToolCall(call) = &projection.updates[0] else {
            panic!("first update should be a tool_call");
        };
        let payload = call.to_payload();
        assert!(payload.get("content").is_none());
        assert!(payload.get("rawOutput").is_none());
    }

    #[test]
    fn tool_call_id_falls_back_to_key_without_session_prefix() {
        let mut row = tool_row("bash", Some("completed"));
        row.tool_call_id = None;
        let projection = project_tool_rows(&[row], &[]);
        let ToolUpdate::ToolCall(call) = &projection.updates[0] else {
            panic!("first update should be a tool_call");
        };
        assert_eq!(call.tool_call_id, "call-bash");
    }

    #[test]
    fn running_row_with_no_durable_result_streams_live_output_window() {
        // While a bash call runs, the runtime flushes a bounded
        // `partial_output_tail` onto the row; the projection surfaces it as
        // the call's `rawOutput` so output streams through the pager
        // instead of appearing only at terminalization.
        let mut row = tool_row("bash", Some("running"));
        row.result = None;
        row.partial_output_tail = Some("streaming line one\n".to_string());
        let projection = project_tool_rows(&[row], &[]);
        let ToolUpdate::ToolCall(call) = &projection.updates[0] else {
            panic!("first update should be a tool_call");
        };
        assert_eq!(call.status, ToolCallStatus::InProgress);
        assert_eq!(
            call.raw_output
                .as_ref()
                .and_then(|output| output.get("output"))
                .and_then(Value::as_str),
            Some("streaming line one"),
            "the live tail streams as rawOutput while the call runs (payload rendering trims, as for a durable result)"
        );
        assert_eq!(
            call.content
                .first()
                .and_then(|block| block.pointer("/content/text"))
                .and_then(Value::as_str),
            Some("streaming line one"),
            "the live tail is also the call's content block"
        );
    }

    #[test]
    fn running_row_streams_incremental_growth_not_replay() {
        // Two successive live flushes grow the tail monotonically. The
        // projection is a pure function of the row, so each projection pass
        // emits the current window; the projection engine's cursor diffs
        // `rawOutput` and streams only the delta to the pager. The window
        // itself must be the row's tail, never a replay of a durable
        // materialization.
        let mut row = tool_row("bash", Some("running"));
        row.result = None;
        row.partial_output_tail = Some("first".to_string());
        let first = project_tool_rows(&[row.clone()], &[]);
        row.partial_output_tail = Some("first second".to_string());
        let second = project_tool_rows(&[row], &[]);
        let window = |projection: &ToolProjection| {
            projection
                .updates
                .iter()
                .find_map(|update| match update {
                    ToolUpdate::ToolCall(call) => call.raw_output.as_ref().map(|output| {
                        output
                            .get("output")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string()
                    }),
                    _ => None,
                })
                .expect("a running row with a tail projects rawOutput")
        };
        assert_eq!(window(&first), "first");
        assert_eq!(window(&second), "first second");
    }

    #[test]
    fn running_row_holds_back_a_trailing_incomplete_utf8_sequence() {
        // The runtime persists the tail lossily: an incomplete final
        // multibyte sequence becomes a trailing replacement character. On a
        // running row that trailing replacement is a placeholder for bytes
        // the next flush completes, so the streaming window holds it back;
        // the next projection pass (with the completed sequence) streams
        // the full character.
        let mut row = tool_row("bash", Some("running"));
        row.result = None;
        let mut split_tail = "counter: 3 ".to_string();
        split_tail.push(REPLACEMENT_CHARACTER);
        row.partial_output_tail = Some(split_tail);
        let held = project_tool_rows(&[row.clone()], &[]);
        let ToolUpdate::ToolCall(call) = &held.updates[0] else {
            panic!("first update should be a tool_call");
        };
        assert_eq!(
            call.raw_output
                .as_ref()
                .and_then(|output| output.get("output"))
                .and_then(Value::as_str),
            Some("counter: 3"),
            "incremental decoding streams the longest valid prefix and holds back only the pending sequence"
        );

        row.partial_output_tail = Some("counter: 3 ✅".to_string());
        let complete = project_tool_rows(&[row], &[]);
        let ToolUpdate::ToolCall(call) = &complete.updates[0] else {
            panic!("first update should be a tool_call");
        };
        assert_eq!(
            call.raw_output
                .as_ref()
                .and_then(|output| output.get("output"))
                .and_then(Value::as_str),
            Some("counter: 3 ✅"),
            "the completed sequence streams whole once flushed"
        );
    }

    #[test]
    fn terminal_row_keeps_a_trailing_replacement_character() {
        // A terminal row's stream is finished: a replacement character in
        // the tail is genuine decoded data (lossy persistence of invalid
        // bytes), not a pending sequence, so it is kept verbatim.
        let mut row = tool_row("bash", Some("completed"));
        row.result = None;
        let mut tail = "done ".to_string();
        tail.push(REPLACEMENT_CHARACTER);
        row.partial_output_tail = Some(tail);
        let projection = project_tool_rows(&[row], &[]);
        let ToolUpdate::ToolCall(call) = &projection.updates[0] else {
            panic!("first update should be a tool_call");
        };
        let mut expected = "done ".to_string();
        expected.push(REPLACEMENT_CHARACTER);
        assert_eq!(
            call.raw_output
                .as_ref()
                .and_then(|output| output.get("output"))
                .and_then(Value::as_str),
            Some(expected.as_str()),
            "a terminal row keeps its genuine lossy decoding"
        );
    }

    #[test]
    fn durable_result_always_beats_the_live_window() {
        // Once a durable result exists it is authoritative: the live window
        // never overrides or duplicates it.
        let mut row = tool_row("bash", Some("completed"));
        row.partial_output_tail = Some("stale streaming tail".to_string());
        let projection = project_tool_rows(&[row], &[]);
        let ToolUpdate::ToolCall(call) = &projection.updates[0] else {
            panic!("first update should be a tool_call");
        };
        assert_eq!(
            call.raw_output
                .as_ref()
                .and_then(|output| output.get("output"))
                .and_then(Value::as_str),
            Some("gents-subprocess-probe"),
            "the durable result wins"
        );
    }

    #[test]
    fn durable_spill_also_beats_the_live_window() {
        let mut row = tool_row("bash", Some("running"));
        row.result = None;
        row.partial_output_tail = Some("stale streaming tail".to_string());
        let results = vec![ToolResultRow {
            tool_call_doc_id: "doc-bash".to_string(),
            output_text: Some("spilled durable output".to_string()),
        }];
        let projection = project_tool_rows(&[row], &results);
        let ToolUpdate::ToolCall(call) = &projection.updates[0] else {
            panic!("first update should be a tool_call");
        };
        assert_eq!(
            call.raw_output
                .as_ref()
                .and_then(|output| output.get("output"))
                .and_then(Value::as_str),
            Some("spilled durable output")
        );
    }

    #[test]
    fn queries_escape_interpolated_values() {
        let query = tool_calls_query(r#"request-"quoted\"-id"#);
        assert!(
            !query.contains(r#""request-"quoted\"-id""#),
            "raw value must not appear unescaped: {query}"
        );
        assert!(query.contains("AgentToolCall"));
        // The spill association joins on the call document's identity, so the
        // query must select `_docID` alongside the projection fields.
        assert!(query.contains("_docID"), "{query}");

        let results = tool_results_query(r#"request-"quoted\"-id"#);
        assert!(
            !results.contains(r#""request-"quoted\"-id""#),
            "raw value must not appear unescaped: {results}"
        );
        assert!(results.contains("AgentToolResult"));
    }

    #[test]
    fn wait_for_exit_answers_the_pagers_exact_method_not_found_error() {
        // The reference pager answers `terminal/wait_for_exit` with
        // `wait_for_exit_not_supported("pager")`: a `METHOD_NOT_FOUND` error
        // whose message is exactly "pager does not handle
        // WaitForTerminalExit". The shim reproduces it verbatim so the
        // adapter's poll fallback keeps working.
        let error = handle_terminal_client_method("terminal/wait_for_exit")
            .expect_err("wait_for_exit must be unsupported");
        assert_eq!(error["code"], JSONRPC_METHOD_NOT_SUPPORTED);
        assert_eq!(
            error["message"],
            "pager does not handle WaitForTerminalExit"
        );
        assert_eq!(error["message"], PAGER_WAIT_FOR_EXIT_MESSAGE);
        // The shim's own generic method-not-found wording never leaks into
        // the pager-exact answer.
        assert!(!error["message"]
            .as_str()
            .expect("message")
            .contains("is not supported by the Gents Grok shim"));
    }

    #[test]
    fn other_terminal_methods_answer_shaped_method_not_found() {
        // The other known terminal methods are also unsupported (the
        // connection registers clientTerminal=false), but their messages are
        // the shim's own shaped wording — only wait_for_exit reproduces the
        // pager's exact message.
        for method in [
            "terminal/create",
            "terminal/output",
            "terminal/kill",
            "terminal/release",
        ] {
            let error = handle_terminal_client_method(method)
                .expect_err("terminal methods must be unsupported");
            assert_eq!(error["code"], JSONRPC_METHOD_NOT_SUPPORTED);
            let message = error["message"].as_str().expect("message");
            assert!(
                message.contains(&format!("{method}: ")),
                "{method} must be named in the shaped error: {message}"
            );
            assert!(
                message.contains("clientTerminal=false"),
                "{method} must explain the agent-side terminal routing: {message}"
            );
        }
        let unknown = handle_terminal_client_method("terminal/invent")
            .expect_err("unknown terminal methods are also rejected");
        assert_eq!(unknown["code"], JSONRPC_METHOD_NOT_SUPPORTED);
    }

    /// A sequenced tool call row for the deterministic chronology tests:
    /// same-shape rows whose only differences are the durable sequence and
    /// the stable identity, so the assertions isolate the sort.
    fn sequenced_tool_row(
        tool_call_id: &str,
        tool_name: &str,
        message_sequence: Option<i64>,
    ) -> ToolCallRow {
        let mut row = tool_row(tool_name, Some("completed"));
        row.tool_call_id = Some(tool_call_id.to_string());
        row.tool_call_key = format!("session-1:{tool_call_id}");
        row.message_sequence = message_sequence;
        row
    }

    /// The projected `tool_call_id`s (the pager-visible stable identity) of
    /// a projection's base `tool_call` registrations, in emission order.
    fn projected_call_ids(projection: &ToolProjection) -> Vec<String> {
        projection
            .updates
            .iter()
            .filter_map(|update| match update {
                ToolUpdate::ToolCall(call) => Some(call.tool_call_id.clone()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn decoded_rows_sort_by_sequence_then_stable_identity_regardless_of_input_order() {
        // Two rows share `message_sequence: 5` and arrive in *reverse*
        // stable-identity order (the "later" call first), a third carries a
        // lower sequence and a fourth a higher one, and one row has no
        // sequence at all. The projection must emit the positioned rows in
        // sequence order, break the equal-sequence tie by the stable
        // identity (tool_call_id), and place the sequenceless row after
        // every positioned one — never in the input order.
        let rows = vec![
            sequenced_tool_row("call-z-late", "bash", Some(5)),
            sequenced_tool_row("call-a-first", "grep", Some(5)),
            sequenced_tool_row("call-m-later", "read_file", Some(9)),
            sequenced_tool_row("call-b-early", "edit", Some(2)),
            sequenced_tool_row("call-x-unsequenced", "fetch", None),
        ];
        let projection = project_tool_rows(&rows, &[]);
        assert_eq!(
            projected_call_ids(&projection),
            vec![
                "call-b-early".to_string(),
                "call-a-first".to_string(),
                "call-z-late".to_string(),
                "call-m-later".to_string(),
                "call-x-unsequenced".to_string(),
            ],
            "wire order must follow (sequence, stable identity), not input order"
        );

        // The same rows in a different input order project to the exact same
        // wire order: the sort is a function of the rows alone.
        let mut reversed = rows.clone();
        reversed.reverse();
        assert_eq!(
            projected_call_ids(&project_tool_rows(&reversed, &[])),
            projected_call_ids(&projection),
            "reversing the decoded input order must not change the wire order"
        );
    }

    #[test]
    fn stable_identity_falls_back_to_key_then_doc_id() {
        // A row without `tool_call_id` breaks its sequence tie by the
        // `tool_call_key`; a row with only `_docID` breaks by that.
        let mut by_key = sequenced_tool_row("call-z", "bash", Some(1));
        by_key.tool_call_id = None;
        by_key.tool_call_key = "session-1:call-z".to_string();
        let mut by_doc = sequenced_tool_row("call-y", "bash", Some(1));
        by_doc.tool_call_id = None;
        by_doc.tool_call_key = String::new();
        by_doc.doc_id = "doc-call-y".to_string();
        assert_eq!(by_key.stable_identity(), "session-1:call-z");
        assert_eq!(by_doc.stable_identity(), "doc-call-y");
        assert_eq!(
            tool_call_row_sort_key(&by_key),
            (1, "session-1:call-z".to_string())
        );
        assert_eq!(
            tool_call_row_sort_key(&by_doc),
            (1, "doc-call-y".to_string())
        );
    }

    #[test]
    fn equal_sequence_rows_keep_base_and_update_semantics_and_result_join() {
        // Two same-sequence `bash` calls with distinct stable identities and
        // distinct results: each keeps its own base registration, the
        // terminal-status `tool_call_update` follows its own base (same
        // chronology), and the exact-result join is preserved — the spill
        // audit row for doc-b never leaks into call a.
        let mut call_a = sequenced_tool_row("call-a", "bash", Some(4));
        call_a.doc_id = "doc-a".to_string();
        call_a.result = None;
        let mut call_b = sequenced_tool_row("call-b", "bash", Some(4));
        call_b.doc_id = "doc-b".to_string();
        call_b.result = None;
        let results = vec![
            ToolResultRow {
                tool_call_doc_id: "doc-b".to_string(),
                output_text: Some("output for b".to_string()),
            },
            ToolResultRow {
                tool_call_doc_id: "doc-a".to_string(),
                output_text: Some("output for a".to_string()),
            },
        ];
        let projection = project_tool_rows(&[call_b, call_a], &results);
        // Emission: base a, update a, base b, update b, commands. The update
        // interleaves inside the projection stream (the merged engine sort
        // re-groups by family later); what matters here is the identity
        // pairing and the per-call result join.
        let pairs: Vec<(String, &str)> = projection
            .updates
            .iter()
            .filter_map(|update| match update {
                ToolUpdate::ToolCall(call) => Some((
                    call.tool_call_id.clone(),
                    call.raw_output.as_ref()?.get("output")?.as_str()?,
                )),
                _ => None,
            })
            .collect();
        assert_eq!(
            pairs,
            vec![
                ("call-a".to_string(), "output for a"),
                ("call-b".to_string(), "output for b"),
            ],
            "each same-sequence call joins its own spilled result, in stable order"
        );
    }

    /// Start an embedded node with the runtime schemas, the production
    /// shape the exact spill-association regression runs against.
    async fn embedded_node() -> (tempfile::TempDir, Arc<EmbeddedNode>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let node = Arc::new(
            EmbeddedNode::builder()
                // The staging `TempDir` guard stays in scope (`dir`) for the
                // test's lifetime, so the node's storage directory is
                // deleted when the test ends — never abandoned or leaked.
                .data_path(dir.path().join("node"))
                .with_storage_backend(gents::defra_node::StorageBackend::Regolith)
                .build()
                .await
                .expect("embedded node"),
        );
        gents::schema::ensure_runtime_schemas(node.as_ref())
            .await
            .expect("runtime schemas");
        (dir, node)
    }

    /// The embedded-node exact spill-association regression: two same-name
    /// `AgentToolCall` rows plus two `AgentToolResult` audit rows, with
    /// **crossed** creation/query order and distinct `tool_call_doc_id`
    /// references and outputs. The full production path — query, deserialize,
    /// projection — must hand each pager `toolCallId` only its exact durable
    /// result in `rawOutput`, never the other call's spill by tool-name
    /// matching or query-iteration accident.
    #[tokio::test]
    async fn embedded_exact_spill_association_survives_query_deserialize_projection() {
        let (_dir, node) = embedded_node().await;
        let session_id = "s-embedded-spill";
        let request_id = "req-embedded-spill";

        // Seed the second call's row first (crossed creation order), with
        // an explicit `created_at` earlier than the first call's so the
        // results query's `created_at: ASC` iteration crosses the two
        // calls' identities. Both calls share the tool name `bash`.
        let seed_calls = r#"mutation {
            second: create_AgentToolCall(input: {
                    tool_call_key: "s-embedded-spill:call-spill-second"
                    request_id: "req-embedded-spill"
                    session_id: "s-embedded-spill"
                    agent_did: "did:test:grok-shim"
                    requester_did: "did:test:grok-shim"
                    tool_call_id: "call-spill-second"
                    tool_name: "bash"
                    lifecycle_state: "completed"
                    status: "completed"
                    result: ""
                    message_sequence: 4
                    started_at: "2026-08-31T22:46:45Z"
            }) { _docID }
            first: create_AgentToolCall(input: {
                    tool_call_key: "s-embedded-spill:call-spill-first"
                    request_id: "req-embedded-spill"
                    session_id: "s-embedded-spill"
                    agent_did: "did:test:grok-shim"
                    requester_did: "did:test:grok-shim"
                    tool_call_id: "call-spill-first"
                    tool_name: "bash"
                    lifecycle_state: "completed"
                    status: "completed"
                    result: ""
                    message_sequence: 4
                    started_at: "2026-08-31T22:46:46Z"
            }) { _docID }
        }"#
        .to_string();
        let response = node.execute(&seed_calls).await;
        assert!(
            !response.has_errors(),
            "seed calls failed: {:?}",
            response.errors
        );

        // Capture each call row's `_docID` — the durable identity the spill
        // audit rows must reference by `tool_call_doc_id`.
        let lookup = r#"query {
            AgentToolCall(
                filter: { request_id: { _eq: "req-embedded-spill" } }
            ) { _docID tool_call_id }
        }"#
        .to_string();
        let response = node.execute(&lookup).await;
        assert!(
            !response.has_errors(),
            "lookup failed: {:?}",
            response.errors
        );
        let rows = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentToolCall"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let doc_id_for = |tool_call_id: &str| -> String {
            rows.iter()
                .find(|row| row.get("tool_call_id").and_then(Value::as_str) == Some(tool_call_id))
                .and_then(|row| row.get("_docID"))
                .and_then(Value::as_str)
                .unwrap_or_else(|| panic!("no doc id for {tool_call_id}"))
                .to_string()
        };
        let doc_first = doc_id_for("call-spill-first");
        let doc_second = doc_id_for("call-spill-second");
        assert_ne!(doc_first, doc_second, "the two calls are distinct docs");

        // Seed the audit rows in crossed order relative to the call
        // creation: the FIRST call's spill is created later, so both the
        // creation order and the results query's `created_at: ASC`
        // iteration would hand a naive join the wrong row.
        let escaped_doc_first = escape_graphql_string(&doc_first);
        let escaped_doc_second = escape_graphql_string(&doc_second);
        let seed_results = format!(
            r#"mutation {{
                spill_second: create_AgentToolResult(input: {{
                    tool_call_doc_id: "{escaped_doc_second}"
                    agent_did: "did:test:grok-shim"
                    requester_did: "did:test:grok-shim"
                    session_id: "s-embedded-spill"
                    tool_name: "bash"
                    tool_input: ""
                    output_text: "durable output for the second call"
                    truncated: true
                    created_at: "2026-08-31T22:46:47Z"
                }}) {{ _docID }}
                spill_first: create_AgentToolResult(input: {{
                    tool_call_doc_id: "{escaped_doc_first}"
                    agent_did: "did:test:grok-shim"
                    requester_did: "did:test:grok-shim"
                    session_id: "s-embedded-spill"
                    tool_name: "bash"
                    tool_input: ""
                    output_text: "durable output for the first call"
                    truncated: true
                    created_at: "2026-08-31T22:46:48Z"
                }}) {{ _docID }}
            }}"#
        );
        let response = node.execute(&seed_results).await;
        assert!(
            !response.has_errors(),
            "seed results failed: {:?}",
            response.errors
        );

        // The actual production path: query + deserialize + projection.
        let projection = project_tools(&node, request_id, session_id, &Default::default())
            .await
            .expect("tool projection");
        let output_for = |tool_call_id: &str| -> Option<String> {
            projection.updates.iter().find_map(|update| match update {
                ToolUpdate::ToolCall(call) if call.tool_call_id == tool_call_id => call
                    .raw_output
                    .as_ref()
                    .and_then(|output| output.get("output"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                _ => None,
            })
        };
        assert_eq!(
            output_for("call-spill-first").as_deref(),
            Some("durable output for the first call"),
            "the first pager toolCallId must receive exactly its own durable result"
        );
        assert_eq!(
            output_for("call-spill-second").as_deref(),
            Some("durable output for the second call"),
            "the second pager toolCallId must receive exactly its own durable result"
        );
        // No other spill ever leaks into either call, and no third tool_call
        // registration exists to borrow one.
        let call_ids = projected_call_ids(&projection);
        assert_eq!(
            call_ids,
            vec![
                "call-spill-first".to_string(),
                "call-spill-second".to_string()
            ],
            "the two same-sequence calls emit in stable-identity order"
        );
    }

    /// The projected `tool_call` registration for one pager `toolCallId`.
    fn call_for<'a>(projection: &'a ToolProjection, tool_call_id: &str) -> &'a ToolCallUpdate {
        projection
            .updates
            .iter()
            .find_map(|update| match update {
                ToolUpdate::ToolCall(call) if call.tool_call_id == tool_call_id => Some(call),
                _ => None,
            })
            .unwrap_or_else(|| panic!("no tool_call registration for {tool_call_id}"))
    }

    /// The embedded-node task/spawn regression: seed `task`/`spawn_subagent`
    /// `AgentToolCall` rows through DefraDB with persisted
    /// foreground/background/unknown/absent `await_mode` (one carrying the
    /// canonical `x.ai/tool` args envelope), run the full production
    /// query/decoder/projection path, and prove the task family is not
    /// suppressed, the plural `tasks` lookalike remains an ordinary tool,
    /// the `subagentBackground` false/true mapping is exact, the full
    /// canonical envelope survives, and a terminal mutation keeps the same
    /// `toolCallId` with the terminal status.
    #[tokio::test]
    async fn embedded_task_family_meta_mapping_and_terminal_update_keep_identity() {
        let (_dir, node) = embedded_node().await;
        let session_id = "s-embedded-task";
        let request_id = "req-embedded-task";

        // Canonical tool meta recorded in the background row's args: the
        // full envelope must survive the projection verbatim.
        let canonical_args = r#"{"description":"background scout","x.ai/tool":{"version":1,"kind":"SubagentSpawn"}}"#;
        let escaped_canonical_args = escape_graphql_string(canonical_args);
        let escaped_fg_args = escape_graphql_string(r#"{"description":"foreground scout"}"#);
        let escaped_absent_args = escape_graphql_string(r#"{"description":"absent mode scout"}"#);
        let escaped_unknown_args = escape_graphql_string(r#"{"description":"unknown mode scout"}"#);

        // Four rows: three valid task-family cases covering foreground,
        // background (with canonical args), and unknown await modes, plus
        // the plural `tasks` lookalike with an absent await_mode.
        let seed_calls = format!(
            r#"mutation {{
                fg: create_AgentToolCall(input: {{
                        tool_call_key: "s-embedded-task:call-task-fg"
                        request_id: "req-embedded-task"
                        session_id: "s-embedded-task"
                        agent_did: "did:test:grok-shim"
                        requester_did: "did:test:grok-shim"
                        tool_call_id: "call-task-fg"
                        tool_name: "task"
                        lifecycle_state: "running"
                        status: "running"
                        result: ""
                        await_mode: "foreground"
                        args: "{escaped_fg_args}"
                        message_sequence: 1
                        started_at: "2026-08-31T23:00:01Z"
                }}) {{ _docID }}
                bg: create_AgentToolCall(input: {{
                        tool_call_key: "s-embedded-task:call-task-bg"
                        request_id: "req-embedded-task"
                        session_id: "s-embedded-task"
                        agent_did: "did:test:grok-shim"
                        requester_did: "did:test:grok-shim"
                        tool_call_id: "call-task-bg"
                        tool_name: "Task"
                        lifecycle_state: "running"
                        status: "running"
                        result: ""
                        await_mode: "background"
                        args: "{escaped_canonical_args}"
                        message_sequence: 2
                        started_at: "2026-08-31T23:00:02Z"
                }}) {{ _docID }}
                absent: create_AgentToolCall(input: {{
                        tool_call_key: "s-embedded-task:call-tasks-absent"
                        request_id: "req-embedded-task"
                        session_id: "s-embedded-task"
                        agent_did: "did:test:grok-shim"
                        requester_did: "did:test:grok-shim"
                        tool_call_id: "call-tasks-absent"
                        tool_name: "tasks"
                        lifecycle_state: "running"
                        status: "running"
                        result: ""
                        args: "{escaped_absent_args}"
                        message_sequence: 3
                        started_at: "2026-08-31T23:00:03Z"
                }}) {{ _docID }}
                unknown: create_AgentToolCall(input: {{
                        tool_call_key: "s-embedded-task:call-spawn-unknown"
                        request_id: "req-embedded-task"
                        session_id: "s-embedded-task"
                        agent_did: "did:test:grok-shim"
                        requester_did: "did:test:grok-shim"
                        tool_call_id: "call-spawn-unknown"
                        tool_name: "spawn_subagent"
                        lifecycle_state: "running"
                        status: "running"
                        result: ""
                        await_mode: "detached"
                        args: "{escaped_unknown_args}"
                        message_sequence: 4
                        started_at: "2026-08-31T23:00:04Z"
                }}) {{ _docID }}
            }}"#
        );
        let response = node.execute(&seed_calls).await;
        assert!(
            !response.has_errors(),
            "seed calls failed: {:?}",
            response.errors
        );

        // The full production path: query + deserialize + projection.
        let projection = project_tools(&node, request_id, session_id, &Default::default())
            .await
            .expect("tool projection");

        // Task-family rows are not suppressed, and the unrecognized plural
        // `tasks` remains an ordinary tool call rather than being assigned
        // task-only pager metadata.
        assert_eq!(
            projected_call_ids(&projection),
            vec![
                "call-task-fg".to_string(),
                "call-task-bg".to_string(),
                "call-tasks-absent".to_string(),
                "call-spawn-unknown".to_string(),
            ],
            "task/spawn rows and ordinary unknown rows must render"
        );

        // The false/true mapping is exact and always explicit: foreground
        // and unknown await modes are explicit false; only the exact persisted
        // value `background` is true.
        for (id, expected) in [
            ("call-task-fg", false),
            ("call-task-bg", true),
            ("call-spawn-unknown", false),
        ] {
            let call = call_for(&projection, id);
            let meta = call
                .meta
                .as_ref()
                .unwrap_or_else(|| panic!("{id} must always carry an object meta"));
            assert!(meta.is_object(), "{id} meta must be an object: {meta}");
            assert_eq!(
                meta.get("subagentBackground"),
                Some(&Value::Bool(expected)),
                "{id} must carry subagentBackground == {expected} explicitly"
            );
        }

        let plural = call_for(&projection, "call-tasks-absent");
        assert!(
            plural.meta.is_none(),
            "plural tasks is not a Grok task alias and must not receive task-only metadata"
        );

        // The full canonical `x.ai/tool` envelope survives verbatim beside
        // the merged boolean on the background row.
        let bg = call_for(&projection, "call-task-bg");
        let meta = bg.meta.as_ref().expect("background row carries meta");
        assert_eq!(
            meta.get(TOOL_META_KEY),
            Some(&json!({
                "version": TOOL_META_VERSION,
                "kind": "SubagentSpawn",
            })),
            "the full x.ai/tool envelope must survive the projection"
        );
        assert_eq!(meta["subagentBackground"], true);

        // A terminal mutation on the absent-mode row: the runtime finalizes
        // the call through a durable update, and the re-projection keeps the
        // same toolCallId with the terminal status.
        let escaped_id = escape_graphql_string("call-tasks-absent");
        let escaped_state = escape_graphql_string("completed");
        let escaped_result = escape_graphql_string("scout finished");
        let mutation = format!(
            r#"mutation {{
                update_AgentToolCall(
                    filter: {{ tool_call_id: {{ _eq: "{escaped_id}" }} }},
                    input: {{
                        lifecycle_state: "{escaped_state}"
                        status: "{escaped_state}"
                        result: "{escaped_result}"
                    }}
                ) {{ _docID }}
            }}"#
        );
        let response = node.execute(&mutation).await;
        assert!(
            !response.has_errors(),
            "terminal update failed: {:?}",
            response.errors
        );

        let reprojection = project_tools(&node, request_id, session_id, &Default::default())
            .await
            .expect("tool reprojection");
        // Same toolCallId, still rendered, now terminal.
        let call = call_for(&reprojection, "call-tasks-absent");
        assert_eq!(call.status, ToolCallStatus::Completed);
        assert_eq!(
            call.raw_output
                .as_ref()
                .and_then(|output| output.get("output"))
                .and_then(Value::as_str),
            Some("scout finished")
        );
        assert!(
            call.meta.is_none(),
            "plural tasks remains an ordinary tool after terminal update"
        );
        // The same-id terminal tool_call_update follows the base.
        let update = reprojection
            .updates
            .iter()
            .find_map(|update| match update {
                ToolUpdate::ToolCallUpdate(update)
                    if update.tool_call_id == "call-tasks-absent" =>
                {
                    Some(update)
                }
                _ => None,
            })
            .expect("terminal call emits a same-id tool_call_update");
        assert_eq!(update.fields["status"], "completed");
    }

    /// The embedded-node live-flush regression, mirroring the runtime's
    /// `hook.rs::flush_live_output_tails`: a running bash row receives a
    /// bounded `partial_output_tail` through the exact production update
    /// mutation, the full production path streams it as `rawOutput`, and a
    /// terminalization with a durable result replaces the window (the live
    /// window is streaming evidence, never durable materialization).
    #[tokio::test]
    async fn embedded_live_output_tail_streams_then_durable_result_replaces_it() {
        let (_dir, node) = embedded_node().await;
        let session_id = "s-embedded-live";
        let request_id = "req-embedded-live";

        let seed_calls = r#"mutation {
            live: create_AgentToolCall(input: {
                    tool_call_key: "s-embedded-live:call-live"
                    request_id: "req-embedded-live"
                    session_id: "s-embedded-live"
                    agent_did: "did:test:grok-shim"
                    requester_did: "did:test:grok-shim"
                    tool_call_id: "call-live"
                    tool_name: "bash"
                    lifecycle_state: "running"
                    status: "running"
                    args: "{\"command\":\"echo gents-subprocess-probe\"}"
                    result: ""
                    message_sequence: 1
                    started_at: "2026-08-31T23:10:01Z"
            }) { _docID }
        }"#
        .to_string();
        let response = node.execute(&seed_calls).await;
        assert!(
            !response.has_errors(),
            "seed call failed: {:?}",
            response.errors
        );

        // Mirror `flush_live_output_tails`: the runtime stamps the bounded
        // tail plus the monotonic byte sequence on the still-running row.
        let live_flush = r#"mutation {
            update_AgentToolCall(
                filter: { tool_call_id: { _eq: "call-live" } },
                input: {
                    partial_output_tail: "streaming probe\n"
                }
            ) { _docID }
        }"#
        .to_string();
        let response = node.execute(&live_flush).await;
        assert!(
            !response.has_errors(),
            "live flush failed: {:?}",
            response.errors
        );

        // The production path streams the window while the call runs.
        let projection = project_tools(&node, request_id, session_id, &Default::default())
            .await
            .expect("live projection");
        let live = call_for(&projection, "call-live");
        assert_eq!(live.status, ToolCallStatus::InProgress);
        assert_eq!(
            live.raw_output
                .as_ref()
                .and_then(|output| output.get("output"))
                .and_then(Value::as_str),
            Some("streaming probe"),
            "the live tail streams through rawOutput while the call runs"
        );

        // Terminalization: the durable result replaces the window; the
        // projection must not duplicate or replay the streaming evidence.
        let mutation = r#"mutation {
            update_AgentToolCall(
                filter: { tool_call_id: { _eq: "call-live" } },
                input: {
                    lifecycle_state: "completed"
                    status: "completed"
                    result: "gents-subprocess-probe"
                    partial_output_tail: ""
                }
            ) { _docID }
        }"#
        .to_string();
        let response = node.execute(&mutation).await;
        assert!(
            !response.has_errors(),
            "terminal update failed: {:?}",
            response.errors
        );

        let reprojection = project_tools(&node, request_id, session_id, &Default::default())
            .await
            .expect("terminal projection");
        let terminal = call_for(&reprojection, "call-live");
        assert_eq!(terminal.status, ToolCallStatus::Completed);
        assert_eq!(
            terminal
                .raw_output
                .as_ref()
                .and_then(|output| output.get("output"))
                .and_then(Value::as_str),
            Some("gents-subprocess-probe"),
            "the durable result replaces the live window at terminalization"
        );
    }
}
