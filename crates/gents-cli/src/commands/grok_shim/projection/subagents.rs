//! Grok shim subagent projection.
//!
//! Runtime subagents are observed child `AgentRequest` rows whose logical and
//! physical parent/tool provenance passes the runtime timeline's shared
//! `child_bridge_is_corroborated` rule.
//! This leaf projects those durable rows into the Grok pager's
//! `subagent_spawned` / `subagent_progress` / `subagent_finished`
//! `session/update` notification payloads, routed by `childSessionId` on the
//! parent session's channel.
//!
//! The `x.ai/subagent/get`, `x.ai/subagent/list_running`, and
//! `x.ai/subagent/cancel` ext requests use the runtime's session descendant
//! graph for authorization. Inspection projects persisted child rows into
//! the stock shell DTOs; cancellation delegates to the runtime's shared
//! control operation. Static `Task` configuration is never lifecycle state.
//!
//! All queries go through the in-process embedded node (`node.execute`) with
//! every interpolated value passed through `escape_graphql_string`; no HTTP
//! GraphQL helper is used. Projection is bounded and request-id-scoped: one
//! child-request query, one spawn-tool query, one child-response query, and
//! one child-tool query per projected parent request, with no graph walks
//! beyond the direct children of the request being projected.

use anyhow::{Context, Result};
use defra_node::EmbeddedNode;
use gents::graphql::{ensure_no_errors, escape_graphql_string};
use gents::run_timeline::{child_bridge_is_corroborated, TimelineRequestRow, TimelineToolCallRow};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};

use super::{effective_context_window_tokens, nonempty};

pub(crate) mod control;

/// Ext request methods routed to this leaf by the ACP service.
pub(crate) const SUBAGENT_GET_METHOD: &str = "x.ai/subagent/get";
pub(crate) const SUBAGENT_LIST_RUNNING_METHOD: &str = "x.ai/subagent/list_running";
pub(crate) const SUBAGENT_CANCEL_METHOD: &str = "x.ai/subagent/cancel";

/// JSON-RPC code used for an unknown ext method. The three known subagent
/// ext stubs return a *successful* result matching the generated Grok shell
/// contract (`GetSubagentResponse`/`ListRunningSubagentsResponse`/
/// `CancelSubagentResponse`); anything else surfaces this code through the
/// error envelope.

/// Shape of a `subagent_spawned` update payload (Grok pager
/// `extensions::notification::SubagentSpawned`).
///
/// The trailing optional wire fields (`effective_context_source`,
/// `capability_mode`, `persona`, `role`, `model`, `resumed_from`,
/// `workflow_run_id`) stay absent: no durable Gents document carries them, and
/// fabricating them would lie about the child's provenance. The required
/// `context_normalized` flag is always `true` because every projected context
/// window passes through the shim's normalization rule
/// (`effective_context_window_tokens`), never a raw child-reported window.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct SubagentSpawnedUpdate {
    pub subagent_id: String,
    pub parent_session_id: String,
    pub parent_prompt_id: Option<String>,
    pub child_session_id: String,
    pub subagent_type: String,
    pub description: String,
    pub context_normalized: bool,
}

/// Shape of a `subagent_progress` update payload.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct SubagentProgressUpdate {
    pub subagent_id: String,
    pub parent_session_id: String,
    pub child_session_id: String,
    pub duration_ms: u64,
    pub turn_count: u32,
    pub tool_call_count: u32,
    pub tokens_used: u64,
    pub context_window_tokens: u64,
    pub context_usage_pct: u8,
    pub tools_used: Vec<String>,
    pub error_count: u32,
}

/// Shape of a terminal `subagent_finished` update payload. Deliberately
/// carries no `parent_session_id`: the pager routes the finish by the
/// subagent id alone.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct SubagentFinishedUpdate {
    pub subagent_id: String,
    pub child_session_id: String,
    pub status: SubagentFinishStatus,
    pub error: Option<String>,
    pub output: Option<String>,
    pub tool_calls: u32,
    pub turns: u32,
    pub duration_ms: u64,
    pub tokens_used: u64,
    pub will_wake: bool,
}

/// Terminal statuses carried by `subagent_finished`, mapped from the durable
/// child request/response lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SubagentFinishStatus {
    Completed,
    Failed,
    Cancelled,
}

impl SubagentFinishStatus {
    fn wire_name(self) -> &'static str {
        match self {
            SubagentFinishStatus::Completed => "completed",
            SubagentFinishStatus::Failed => "failed",
            SubagentFinishStatus::Cancelled => "cancelled",
        }
    }
}

/// One child `AgentRequest` row linked to the projected parent request.
#[derive(Clone, Debug, Deserialize)]
struct ChildRequestRow {
    #[serde(default, rename = "_docID")]
    doc_id: Option<String>,
    request_id: String,
    session_id: String,
    #[serde(default)]
    behavior_id: Option<String>,
    #[serde(default)]
    content: String,
    #[serde(default)]
    lifecycle_state: Option<String>,
    #[serde(default)]
    failure_reason: Option<String>,
    #[serde(default)]
    terminalized_at: Option<String>,
    #[serde(default)]
    created_at: Option<String>,
    #[serde(default)]
    caused_by_parent_request_id: Option<String>,
    #[serde(default)]
    caused_by_parent_request_doc_id: Option<String>,
    #[serde(default)]
    caused_by_parent_tool_call_id: Option<String>,
    #[serde(default)]
    caused_by_parent_tool_call_doc_id: Option<String>,
    #[serde(default)]
    metadata: Option<String>,
}

/// Latest `AgentResponse` row for a child request; the durable source of
/// token usage and error details (request lifecycle owns terminality).
#[derive(Clone, Debug, Deserialize)]
struct ChildResponseRow {
    request_id: String,
    #[serde(default)]
    token_count: Option<i64>,
    #[serde(default)]
    error_message: Option<String>,
}

/// An `AgentToolCall` row of the spawn tool, when the parent recorded one
/// linking to the child request.
#[derive(Clone, Debug, Deserialize)]
struct SpawnToolRow {
    #[serde(default, rename = "_docID")]
    doc_id: Option<String>,
    request_id: String,
    #[serde(default)]
    request_doc_id: Option<String>,
    tool_call_id: String,
    #[serde(default)]
    child_request_id: Option<String>,
    #[serde(default)]
    args: Option<String>,
    /// Durable transcript position of the spawn call: the chronology key the
    /// subagent family merges by (`subagent_spawned` follows its spawn tool
    /// at the same sequence; progress/finished follow it after).
    #[serde(default)]
    message_sequence: Option<i64>,
}

/// An `AgentToolCall` row executed by a child request; the durable source of
/// the progress/finished tool counts, tool names, and error count.
#[derive(Clone, Debug, Deserialize)]
struct ChildToolRow {
    request_id: String,
    #[serde(default)]
    tool_name: Option<String>,
    #[serde(default)]
    lifecycle_state: Option<String>,
}

const CHILD_REQUEST_FIELDS: &str = r#"
    _docID
    request_id
    session_id
    behavior_id
    content
    lifecycle_state
    failure_reason
    terminalized_at
    created_at
    caused_by_parent_request_id
    caused_by_parent_request_doc_id
    caused_by_parent_tool_call_id
    caused_by_parent_tool_call_doc_id
    metadata
"#;

const CHILD_RESPONSE_FIELDS: &str = r#"
    request_id
    token_count
    error_message
"#;

const SPAWN_TOOL_FIELDS: &str = r#"
    _docID
    request_id
    request_doc_id
    tool_call_id
    child_request_id
    args
    message_sequence
"#;

const CHILD_TOOL_FIELDS: &str = r#"
    request_id
    tool_name
    lifecycle_state
"#;

/// The full set of projection events for one parent request id, ordered so a
/// client can replay spawned → (progress) → finished per subagent.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct SubagentProjection {
    pub updates: Vec<SubagentUpdate>,
    /// Durable chronology key per update, aligned 1:1 with `updates`: the
    /// spawn tool's `message_sequence` when the parent recorded a spawn row
    /// (the shared transcript sequence space), else `None`. The projection
    /// engine merges families by this key so `subagent_spawned` follows its
    /// spawn tool call instead of arriving after every message chunk.
    pub chronology: Vec<Option<i64>>,
}

/// A single projected subagent update, already split by kind so the caller
/// (the projection engine) only needs to stamp `_meta` and wrap it in a
/// `session/update` notification.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum SubagentUpdate {
    Spawned(SubagentSpawnedUpdate),
    Progress(SubagentProgressUpdate),
    Finished(SubagentFinishedUpdate),
}

impl SubagentUpdate {
    /// The `sessionUpdate` discriminator for this update.
    pub fn session_update_kind(&self) -> &'static str {
        match self {
            SubagentUpdate::Spawned(_) => "subagent_spawned",
            SubagentUpdate::Progress(_) => "subagent_progress",
            SubagentUpdate::Finished(_) => "subagent_finished",
        }
    }

    /// The subagent id this update belongs to. The pager routes subagent
    /// updates by `childSessionId`; the id doubles as the `subagentId` the
    /// ext controls address.
    pub fn subagent_id(&self) -> &str {
        match self {
            SubagentUpdate::Spawned(update) => &update.subagent_id,
            SubagentUpdate::Progress(update) => &update.subagent_id,
            SubagentUpdate::Finished(update) => &update.subagent_id,
        }
    }

    /// Render the Grok pager payload for this update. The `sessionUpdate`
    /// discriminator keeps its camelCase enum tag; every inner DTO field
    /// renders with its snake_case key.
    pub fn to_payload(&self) -> Value {
        match self {
            SubagentUpdate::Spawned(update) => {
                let mut payload = json!({
                    "sessionUpdate": "subagent_spawned",
                    "subagent_id": update.subagent_id,
                    "parent_session_id": update.parent_session_id,
                    "child_session_id": update.child_session_id,
                    "subagent_type": update.subagent_type,
                    "description": update.description,
                    "context_normalized": update.context_normalized,
                });
                let object = payload
                    .as_object_mut()
                    .expect("spawned payload is a JSON object");
                if let Some(prompt_id) = update.parent_prompt_id.as_deref() {
                    object.insert("parent_prompt_id".to_string(), json!(prompt_id));
                }
                payload
            }
            SubagentUpdate::Progress(update) => json!({
                "sessionUpdate": "subagent_progress",
                "subagent_id": update.subagent_id,
                "parent_session_id": update.parent_session_id,
                "child_session_id": update.child_session_id,
                "duration_ms": update.duration_ms,
                "turn_count": update.turn_count,
                "tool_call_count": update.tool_call_count,
                "tokens_used": update.tokens_used,
                "context_window_tokens": update.context_window_tokens,
                "context_usage_pct": update.context_usage_pct,
                "tools_used": update.tools_used,
                "error_count": update.error_count,
            }),
            SubagentUpdate::Finished(update) => {
                let mut payload = json!({
                    "sessionUpdate": "subagent_finished",
                    "subagent_id": update.subagent_id,
                    "child_session_id": update.child_session_id,
                    "status": update.status.wire_name(),
                    "tool_calls": update.tool_calls,
                    "turns": update.turns,
                    "duration_ms": update.duration_ms,
                    "tokens_used": update.tokens_used,
                    "will_wake": update.will_wake,
                });
                let object = payload
                    .as_object_mut()
                    .expect("finished payload is a JSON object");
                if let Some(error) = update.error.as_deref() {
                    object.insert("error".to_string(), json!(error));
                }
                if let Some(output) = update.output.as_deref() {
                    object.insert("output".to_string(), json!(output));
                }
                payload
            }
        }
    }
}

/// Project the subagent lifecycle for one parent request id.
///
/// Bounded and request-id-scoped: the query set is exactly
/// 1. one `AgentRequest` query for children of this request id,
/// 2. one `AgentToolCall` query for the spawn rows of this request id,
/// 3. one `AgentResponse` query for those child request ids,
/// 4. one `AgentToolCall` query for the tool rows of those child request ids.
///
/// Returns at most one `spawned` plus one terminal `finished` update per
/// child, plus one `progress` update per still-running child. It never
/// replays the session or duplicates durable materialization: the projection
/// is read-only and every payload is a fresh notification value.
pub(super) async fn project_subagents(
    node: &EmbeddedNode,
    parent_request_id: &str,
    parent_session_id: &str,
    context_window_tokens: u64,
) -> Result<SubagentProjection> {
    let response = node.execute(&child_requests_query(parent_request_id)).await;
    ensure_no_errors(&response, "grok shim subagent child request query")?;
    let parent_rows =
        decode_rows::<TimelineRequestRow>(&response, "parent", "parent AgentRequest provenance");
    let [parent] = parent_rows.as_slice() else {
        tracing::debug!(
            parent_request_id,
            rows = parent_rows.len(),
            "grok shim cannot uniquely corroborate the subagent parent document"
        );
        return Ok(SubagentProjection {
            updates: Vec::new(),
            chronology: Vec::new(),
        });
    };
    // Durable child chronology: `created_at`, then the child request id,
    // computed after decoding so equal-timestamp rows and any query
    // iteration order never decide the projected wire order.
    let candidate_children = decode_child_rows(&response);

    if candidate_children.is_empty() {
        return Ok(SubagentProjection {
            updates: Vec::new(),
            chronology: Vec::new(),
        });
    }

    let spawn_response = node.execute(&spawn_tools_query(parent_request_id)).await;
    ensure_no_errors(&spawn_response, "grok shim subagent spawn tool query")?;
    let mut spawn_tools = decode_spawn_rows(&spawn_response);
    // Spawn rows sort by their durable transcript chronology
    // (`message_sequence`, then the stable tool call id) — the same
    // sequence space and tie-break the tool family uses — so a
    // spawned-subagent event always follows its spawn tool call and
    // equal-sequence spawn rows never follow the query's iteration order.
    sort_spawn_rows(&mut spawn_tools);
    let timeline_tools = spawn_tools
        .iter()
        .map(spawn_timeline_row)
        .collect::<Vec<_>>();

    // The timeline includes control continuations as well as spawned agents.
    // Only a tool-spawned child in a distinct session is a pager subagent;
    // retain the shared provenance predicate to validate its physical bridge.
    let mut children = candidate_children
        .into_iter()
        .filter(|child| {
            if child.session_id.trim().is_empty()
                || child.session_id == parent_session_id
                || child
                    .caused_by_parent_tool_call_id
                    .as_deref()
                    .and_then(nonempty)
                    .is_none()
                || child
                    .caused_by_parent_tool_call_doc_id
                    .as_deref()
                    .and_then(nonempty)
                    .is_none()
            {
                return false;
            }
            let Some(timeline_child) = child_timeline_row(child) else {
                return false;
            };
            child_bridge_is_corroborated(parent, &timeline_child, &timeline_tools)
        })
        .collect::<Vec<_>>();
    sort_child_rows(&mut children);

    if children.is_empty() {
        return Ok(SubagentProjection {
            updates: Vec::new(),
            chronology: Vec::new(),
        });
    }

    let child_request_ids = children.iter().map(|child| child.request_id.as_str());

    let response_response = node
        .execute(&child_responses_query(child_request_ids.clone()))
        .await;
    ensure_no_errors(&response_response, "grok shim subagent response query")?;
    let child_responses = decode_response_rows(&response_response);

    let tools_response = node.execute(&child_tools_query(child_request_ids)).await;
    ensure_no_errors(&tools_response, "grok shim subagent child tool query")?;
    let child_tools = decode_child_tool_rows(&tools_response);

    let (updates, chronology) = project_child_rows(
        &children,
        &spawn_tools,
        &child_responses,
        &child_tools,
        parent_request_id,
        parent_session_id,
        context_window_tokens,
    );
    Ok(SubagentProjection {
        updates,
        chronology,
    })
}

/// The durable chronology sort key of one child request row:
/// `(timestamp-missing flag, normalized created_at, request_id)`. The
/// flag places rows without a parseable timestamp after every real one;
/// the timestamps compare in RFC3339-normalized UTC form, so mixed-offset
/// representations of the same instant tie; and equal timestamps break
/// by the child request id — a durable unique identity — so equal-time
/// children never follow the query's iteration order.
fn child_row_sort_key(child: &ChildRequestRow) -> (bool, String, String) {
    match normalize_rfc3339(child.created_at.as_deref()) {
        Some(created) => (false, created, child.request_id.clone()),
        None => (true, String::new(), child.request_id.clone()),
    }
}

/// Sort decoded child request rows into durable creation chronology:
/// `created_at`, then request id. The query already asks for
/// `created_at: ASC`, but equal timestamps there are a storage-order
/// accident; this sort makes the wire order a function of the rows
/// alone.
fn sort_child_rows(children: &mut [ChildRequestRow]) {
    children.sort_by(|a, b| child_row_sort_key(a).cmp(&child_row_sort_key(b)));
}

/// The durable chronology sort key of one spawn tool row:
/// `(message_sequence, stable tool identity)` — the same key space the
/// tool family sorts by. Rows without a sequence sort after positioned
/// rows (`i64::MAX`), and equal sequences break by the spawn row's
/// `tool_call_id` (the durable `AgentToolCall.tool_call_id` field the
/// runtime records), so spawned events follow their spawn tool calls in
/// the same order the tool family emits them.
fn spawn_row_sort_key(spawn: &SpawnToolRow) -> (i64, String) {
    (
        spawn.message_sequence.unwrap_or(i64::MAX),
        spawn.tool_call_id.clone(),
    )
}

/// Sort decoded spawn rows into durable transcript chronology:
/// `message_sequence`, then the stable tool call id. This must agree
/// with the tool family's ordering of the same rows, which it does
/// because both sort by sequence first and the durable tool call
/// identity second.
fn sort_spawn_rows(spawn_tools: &mut [SpawnToolRow]) {
    spawn_tools.sort_by(|a, b| spawn_row_sort_key(a).cmp(&spawn_row_sort_key(b)));
}

/// Normalize one RFC3339 timestamp for lexical comparison: blank or
/// unparseable values yield `None` (the caller sorts them last);
/// parseable values render in a single UTC normal form (`chrono`'s
/// `to_rfc3339`), so all representations of the same instant compare
/// equal and the ordering is chronological.
fn normalize_rfc3339(value: Option<&str>) -> Option<String> {
    let value = value.and_then(nonempty)?;
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|parsed| {
            parsed
                .with_timezone(&chrono::Utc)
                .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
        })
}

fn project_child_rows(
    children: &[ChildRequestRow],
    spawn_tools: &[SpawnToolRow],
    child_responses: &[ChildResponseRow],
    child_tools: &[ChildToolRow],
    parent_request_id: &str,
    parent_session_id: &str,
    context_window_tokens: u64,
) -> (Vec<SubagentUpdate>, Vec<Option<i64>>) {
    let mut updates = Vec::new();
    let mut chronology = Vec::new();
    for child in children {
        // Only rows that actually link back to this parent request project;
        // the query filter already guarantees this, but a defense-in-depth
        // re-check keeps the payload scoped even if the filter is widened.
        if child
            .caused_by_parent_request_id
            .as_deref()
            .and_then(nonempty)
            != Some(parent_request_id)
        {
            continue;
        }
        let spawn_tool = spawn_tools.iter().find(|tool| {
            tool.request_id == parent_request_id
                && tool.doc_id.as_deref().and_then(nonempty)
                    == child
                        .caused_by_parent_tool_call_doc_id
                        .as_deref()
                        .and_then(nonempty)
                && tool.request_doc_id.as_deref().and_then(nonempty)
                    == child
                        .caused_by_parent_request_doc_id
                        .as_deref()
                        .and_then(nonempty)
                && child
                    .caused_by_parent_tool_call_id
                    .as_deref()
                    .and_then(nonempty)
                    == Some(tool.tool_call_id.as_str())
                && tool
                    .child_request_id
                    .as_deref()
                    .and_then(nonempty)
                    .is_some_and(|child_request_id| child_request_id == child.request_id)
        });
        let spawn_sequence = spawn_tool.and_then(|tool| tool.message_sequence);
        let child_response = child_responses
            .iter()
            .find(|response| response.request_id == child.request_id);
        let child_tools = child_tools
            .iter()
            .filter(|tool| tool.request_id == child.request_id)
            .collect::<Vec<_>>();

        let subagent_id = subagent_id_for(child);
        let subagent_type = child
            .behavior_id
            .as_deref()
            .and_then(nonempty)
            .unwrap_or("general-purpose")
            .to_string();
        let description = spawn_description(spawn_tool, child);

        updates.push(SubagentUpdate::Spawned(SubagentSpawnedUpdate {
            subagent_id: subagent_id.clone(),
            parent_session_id: parent_session_id.to_string(),
            parent_prompt_id: parent_prompt_id(child),
            child_session_id: child.session_id.clone(),
            subagent_type: subagent_type.clone(),
            description: description.clone(),
            // The projection always normalizes the child's context window
            // through `effective_context_window_tokens` before it reaches the
            // wire, so the audited spawned flag is unconditionally true.
            context_normalized: true,
        }));
        chronology.push(spawn_sequence);

        let finished = child.is_terminal(child_response);
        if !finished {
            let progress = progress_update(
                child,
                child_response,
                &child_tools,
                &subagent_id,
                parent_session_id,
                context_window_tokens,
            );
            updates.push(SubagentUpdate::Progress(progress));
            chronology.push(spawn_sequence);
        }

        if let Some(finished) = finished_update(child, child_response, &child_tools, &subagent_id) {
            updates.push(SubagentUpdate::Finished(finished));
            chronology.push(spawn_sequence);
        }
    }
    (updates, chronology)
}

impl ChildRequestRow {
    /// Only the canonical request lifecycle owns terminality. An interrupt
    /// marker is a request to stop, not evidence that execution has stopped.
    fn is_terminal(&self, _response: Option<&ChildResponseRow>) -> bool {
        gents_protocol::request_lifecycle::RequestLifecycleState::is_terminal_str(
            self.lifecycle_state.as_deref(),
        )
    }
    /// Project terminal status from the canonical runtime lifecycle:
    /// `completed` completes,
    /// `interrupted` cancels (directly: the canonical lifecycle state is
    /// the authority, no interrupt marker required), the failure terminal
    /// states (`failed`, `dead`, `superseded`) fail. Anything else — the
    /// still-active states — only ever projects progress, never a finish.
    fn finish_status(&self, response: Option<&ChildResponseRow>) -> Option<SubagentFinishStatus> {
        if !self.is_terminal(response) {
            return None;
        }
        match self.lifecycle_state.as_deref().and_then(nonempty) {
            Some("completed") => Some(SubagentFinishStatus::Completed),
            Some("interrupted") => Some(SubagentFinishStatus::Cancelled),
            Some("failed" | "dead" | "superseded") => Some(SubagentFinishStatus::Failed),
            _ => None,
        }
    }
}

fn progress_update(
    child: &ChildRequestRow,
    child_response: Option<&ChildResponseRow>,
    child_tools: &[&ChildToolRow],
    subagent_id: &str,
    parent_session_id: &str,
    context_window_tokens: u64,
) -> SubagentProgressUpdate {
    let tokens_used = child_response
        .and_then(|response| response.token_count)
        .and_then(|tokens| u64::try_from(tokens.max(0)).ok())
        .unwrap_or(0);
    let context_window_tokens = effective_context_window_tokens(context_window_tokens);
    SubagentProgressUpdate {
        subagent_id: subagent_id.to_string(),
        parent_session_id: parent_session_id.to_string(),
        child_session_id: child.session_id.clone(),
        duration_ms: elapsed_millis(
            child.created_at.as_deref(),
            child.terminalized_at.as_deref(),
        ),
        turn_count: 1,
        tool_call_count: u32::try_from(child_tools.len()).unwrap_or(u32::MAX),
        tokens_used,
        context_window_tokens,
        context_usage_pct: context_usage_pct(tokens_used, context_window_tokens),
        tools_used: distinct_tool_names(child_tools),
        error_count: child_tool_error_count(child_tools, child),
    }
}

fn finished_update(
    child: &ChildRequestRow,
    child_response: Option<&ChildResponseRow>,
    child_tools: &[&ChildToolRow],
    subagent_id: &str,
) -> Option<SubagentFinishedUpdate> {
    let status = child.finish_status(child_response)?;
    let error = child
        .failure_reason
        .as_deref()
        .and_then(nonempty)
        .map(ToOwned::to_owned)
        .or_else(|| {
            child_response
                .and_then(|response| response.error_message.as_deref())
                .and_then(nonempty)
                .map(ToOwned::to_owned)
        });
    let tokens_used = child_response
        .and_then(|response| response.token_count)
        .and_then(|tokens| u64::try_from(tokens.max(0)).ok())
        .unwrap_or(0);
    Some(SubagentFinishedUpdate {
        subagent_id: subagent_id.to_string(),
        child_session_id: child.session_id.clone(),
        status,
        error,
        // No truthful durable source for the child's final output text is
        // projected here, so the field stays absent rather than fabricated.
        output: None,
        tool_calls: u32::try_from(child_tools.len()).unwrap_or(u32::MAX),
        turns: 1,
        duration_ms: elapsed_millis(
            child.created_at.as_deref(),
            child.terminalized_at.as_deref(),
        ),
        tokens_used,
        will_wake: false,
    })
}

/// Distinct, insertion-ordered tool names the child actually executed, as
/// recorded on durable `AgentToolCall` rows.
fn distinct_tool_names(child_tools: &[&ChildToolRow]) -> Vec<String> {
    let mut names = Vec::new();
    for tool in child_tools {
        let Some(name) = tool.tool_name.as_deref().and_then(nonempty) else {
            continue;
        };
        if !names.iter().any(|seen: &String| seen == name) {
            names.push(name.to_string());
        }
    }
    names
}

/// Errors observed on the child's own tool calls, plus the child request's
/// own failure reason when present.
fn child_tool_error_count(child_tools: &[&ChildToolRow], child: &ChildRequestRow) -> u32 {
    let failed_tools = child_tools
        .iter()
        .filter(|tool| {
            tool.lifecycle_state
                .as_deref()
                .and_then(nonempty)
                .is_some_and(|state| state == "failed")
        })
        .count();
    let request_failure = usize::from(child.failure_reason.as_deref().and_then(nonempty).is_some());
    u32::try_from(failed_tools + request_failure).unwrap_or(u32::MAX)
}

/// The subagent id the pager addresses: exactly the child session id for
/// every lifecycle update (spawned, progress, and finished). The spawn
/// tool call id is never used — the pager routes subagent updates by the
/// child session, and the ext controls address the same id.
fn subagent_id_for(child: &ChildRequestRow) -> String {
    child.session_id.clone()
}

/// `parentPromptId` comes from the spawn tool call's recorded metadata or the
/// child request metadata, when either carries the prompt id.
fn parent_prompt_id(child: &ChildRequestRow) -> Option<String> {
    let metadata = child.metadata.as_deref().and_then(nonempty)?;
    let value: Value = serde_json::from_str(metadata).ok()?;
    value
        .get("promptId")
        .and_then(Value::as_str)
        .and_then(nonempty)
        .map(ToOwned::to_owned)
}

/// Short description for the spawned update. The spawn tool's recorded name
/// argument wins; otherwise the child request content is truncated to the
/// pager's short-description scale.
fn spawn_description(spawn_tool: Option<&SpawnToolRow>, child: &ChildRequestRow) -> String {
    if let Some(tool) = spawn_tool {
        if let Some(args) = tool.args.as_deref().and_then(nonempty) {
            if let Ok(Value::Object(fields)) = serde_json::from_str::<Value>(args) {
                for key in ["name", "description", "prompt"] {
                    if let Some(value) = fields.get(key).and_then(Value::as_str).and_then(nonempty)
                    {
                        return truncate_description(value);
                    }
                }
            }
        }
    }
    truncate_description(child.content.as_str())
}

fn truncate_description(text: &str) -> String {
    const MAX_DESCRIPTION_CHARS: usize = 120;
    let trimmed = text.trim();
    if trimmed.chars().count() <= MAX_DESCRIPTION_CHARS {
        return trimmed.to_string();
    }
    trimmed.chars().take(MAX_DESCRIPTION_CHARS).collect()
}

fn context_usage_pct(tokens_used: u64, context_window_tokens: u64) -> u8 {
    if context_window_tokens == 0 {
        return 0;
    }
    u8::try_from(
        tokens_used
            .saturating_mul(100)
            .saturating_div(context_window_tokens),
    )
    .unwrap_or(100)
}

fn elapsed_millis(started_at: Option<&str>, ended_at: Option<&str>) -> u64 {
    let (Some(started), Some(ended)) = (started_at, ended_at) else {
        return 0;
    };
    let (Ok(started), Ok(ended)) = (
        chrono::DateTime::parse_from_rfc3339(started),
        chrono::DateTime::parse_from_rfc3339(ended),
    ) else {
        return 0;
    };
    ended
        .signed_duration_since(started)
        .num_milliseconds()
        .max(0)
        .try_into()
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Generated-contract not-found results
// ---------------------------------------------------------------------------

/// Stock GetSubagentResponse when the authorized descendant graph has no
/// matching child. The extension envelope is added at the ACP boundary.
pub(super) fn subagent_get_not_found_result() -> Value {
    json!({
        "snapshot": null,
    })
}

/// Empty stock ListRunningSubagentsResponse fixture.
#[cfg(test)]
fn subagent_list_running_empty_result() -> Value {
    json!({
        "subagents": [],
    })
}

/// Result of `x.ai/subagent/cancel` for an id with no observed live child
/// request. `cancelled` is false and the outcome is the audited
/// `not_found` kind; no `Task` row and no child `AgentRequest` is mutated.
pub(super) fn subagent_cancel_not_found_result(subagent_id: &str) -> Value {
    json!({
        "subagentId": subagent_id,
        "cancelled": false,
        "outcome": {
            "kind": "not_found",
        },
    })
}

// ---------------------------------------------------------------------------
// Queries and decoding
// ---------------------------------------------------------------------------

fn child_requests_query(parent_request_id: &str) -> String {
    format!(
        r#"{{
            parent: AgentRequest(
                filter: {{ request_id: {{ _eq: "{parent_request_id}" }} }},
                limit: 2
            ) {{ {CHILD_REQUEST_FIELDS} }}
            children: AgentRequest(
                filter: {{
                    caused_by_parent_request_id: {{ _eq: "{parent_request_id}" }}
                }},
                order: {{ created_at: ASC }}
            ) {{ {CHILD_REQUEST_FIELDS} }}
        }}"#,
        parent_request_id = escape_graphql_string(parent_request_id),
    )
}

fn spawn_tools_query(parent_request_id: &str) -> String {
    format!(
        r#"{{
            AgentToolCall(
                filter: {{
                    request_id: {{ _eq: "{parent_request_id}" }},
                    child_request_id: {{ _ne: "" }}
                }}
            ) {{ {SPAWN_TOOL_FIELDS} }}
        }}"#,
        parent_request_id = escape_graphql_string(parent_request_id),
    )
}

fn child_responses_query<'a>(request_ids: impl IntoIterator<Item = &'a str>) -> String {
    let ids = graphql_string_list(request_ids);
    format!(
        r#"{{
            AgentResponse(
                filter: {{ request_id: {{ _in: [{ids}] }} }},
                order: {{ created_at: ASC }}
            ) {{ {CHILD_RESPONSE_FIELDS} }}
        }}"#
    )
}

fn child_tools_query<'a>(request_ids: impl IntoIterator<Item = &'a str>) -> String {
    let ids = graphql_string_list(request_ids);
    format!(
        r#"{{
            AgentToolCall(
                filter: {{ request_id: {{ _in: [{ids}] }} }}
            ) {{ {CHILD_TOOL_FIELDS} }}
        }}"#
    )
}

fn graphql_string_list<'a>(values: impl IntoIterator<Item = &'a str>) -> String {
    values
        .into_iter()
        .map(|value| format!(r#""{}""#, escape_graphql_string(value)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn decode_child_rows(response: &defra_node::QueryResponse) -> Vec<ChildRequestRow> {
    decode_rows(response, "children", "child AgentRequest")
}

fn decode_spawn_rows(response: &defra_node::QueryResponse) -> Vec<SpawnToolRow> {
    decode_rows(response, "AgentToolCall", "spawn AgentToolCall")
}

fn decode_response_rows(response: &defra_node::QueryResponse) -> Vec<ChildResponseRow> {
    decode_rows(response, "AgentResponse", "child AgentResponse")
}

fn decode_child_tool_rows(response: &defra_node::QueryResponse) -> Vec<ChildToolRow> {
    decode_rows(response, "AgentToolCall", "child AgentToolCall")
}

fn decode_rows<T: DeserializeOwned>(
    response: &defra_node::QueryResponse,
    collection: &str,
    label: &str,
) -> Vec<T> {
    response
        .data
        .as_ref()
        .and_then(|data| data.get(collection))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|row| match serde_json::from_value::<T>(row) {
            Ok(row) => Some(row),
            Err(error) => {
                tracing::debug!(%error, %label, "grok shim skipped an undecodable row");
                None
            }
        })
        .collect()
}

fn child_timeline_row(child: &ChildRequestRow) -> Option<TimelineRequestRow> {
    nonempty(&child.request_id)?;
    Some(TimelineRequestRow {
        doc_id: child.doc_id.clone(),
        request_id: child.request_id.clone(),
        session_id: Some(child.session_id.clone()),
        metadata: child.metadata.clone(),
        caused_by_parent_request_id: child.caused_by_parent_request_id.clone(),
        caused_by_parent_request_doc_id: child.caused_by_parent_request_doc_id.clone(),
        caused_by_parent_tool_call_id: child.caused_by_parent_tool_call_id.clone(),
        caused_by_parent_tool_call_doc_id: child.caused_by_parent_tool_call_doc_id.clone(),
        ..TimelineRequestRow::default()
    })
}

/// Project the narrow spawn row into the runtime DTO used by the shared
/// provenance rule. Do not deserialize the sparse query into the full DTO:
/// unrelated nullable tool fields (for example `args`) are intentionally not
/// selected and must not make otherwise complete bridge evidence undecodable.
fn spawn_timeline_row(spawn: &SpawnToolRow) -> TimelineToolCallRow {
    TimelineToolCallRow {
        doc_id: spawn.doc_id.clone(),
        request_id: Some(spawn.request_id.clone()),
        request_doc_id: spawn.request_doc_id.clone(),
        message_sequence: spawn.message_sequence,
        tool_call_id: spawn.tool_call_id.clone(),
        child_request_id: spawn.child_request_id.clone(),
        ..TimelineToolCallRow::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn child_row(request_id: &str, lifecycle_state: Option<&str>) -> ChildRequestRow {
        ChildRequestRow {
            doc_id: Some(format!("doc-{request_id}")),
            request_id: request_id.to_string(),
            session_id: format!("session-{request_id}"),
            behavior_id: Some("explore".to_string()),
            content: "List the top-level directories of this repository and summarize each \
                      one in five words."
                .to_string(),
            lifecycle_state: lifecycle_state.map(ToOwned::to_owned),
            failure_reason: None,
            terminalized_at: None,
            created_at: Some("2026-08-31T22:46:45Z".to_string()),
            caused_by_parent_request_id: Some("parent-request".to_string()),
            caused_by_parent_request_doc_id: Some("doc-parent-request".to_string()),
            caused_by_parent_tool_call_id: None,
            caused_by_parent_tool_call_doc_id: None,
            metadata: None,
        }
    }

    fn response_row(request_id: &str, token_count: Option<i64>) -> ChildResponseRow {
        ChildResponseRow {
            request_id: request_id.to_string(),
            token_count,
            error_message: None,
        }
    }

    #[test]
    fn spawned_update_payload_matches_grok_wire_shape() {
        let update = SubagentUpdate::Spawned(SubagentSpawnedUpdate {
            subagent_id: "session-child-1".to_string(),
            parent_session_id: "parent-session".to_string(),
            parent_prompt_id: Some("prompt-1".to_string()),
            child_session_id: "session-child-1".to_string(),
            subagent_type: "explore".to_string(),
            description: "list and summarize".to_string(),
            context_normalized: true,
        });
        let payload = update.to_payload();
        // Exact key set and casing: the `sessionUpdate` enum tag stays
        // camelCase; every inner DTO field renders snake_case.
        assert_eq!(
            payload,
            json!({
                "sessionUpdate": "subagent_spawned",
                "subagent_id": "session-child-1",
                "parent_session_id": "parent-session",
                "child_session_id": "session-child-1",
                "subagent_type": "explore",
                "description": "list and summarize",
                "parent_prompt_id": "prompt-1",
                "context_normalized": true,
            })
        );
        assert_eq!(update.session_update_kind(), "subagent_spawned");
        assert_eq!(update.subagent_id(), "session-child-1");
    }

    #[test]
    fn spawned_update_omits_absent_parent_prompt_id() {
        let update = SubagentUpdate::Spawned(SubagentSpawnedUpdate {
            subagent_id: "session-child-1".to_string(),
            parent_session_id: "parent-session".to_string(),
            parent_prompt_id: None,
            child_session_id: "session-child-1".to_string(),
            subagent_type: "general-purpose".to_string(),
            description: String::new(),
            context_normalized: true,
        });
        let payload = update.to_payload();
        assert!(payload.get("parent_prompt_id").is_none());
        assert!(payload.get("parentPromptId").is_none());
        // The required normalization flag is always present, even when every
        // optional provenance field is absent.
        assert_eq!(payload["context_normalized"], true);
    }

    #[test]
    fn progress_update_payload_matches_grok_wire_shape() {
        let update = SubagentUpdate::Progress(SubagentProgressUpdate {
            subagent_id: "session-child-1".to_string(),
            parent_session_id: "parent-session".to_string(),
            child_session_id: "session-child-1".to_string(),
            duration_ms: 2_000,
            turn_count: 1,
            tool_call_count: 3,
            tokens_used: 512,
            context_window_tokens: 262_144,
            context_usage_pct: 0,
            tools_used: vec!["read_file".to_string()],
            error_count: 0,
        });
        assert_eq!(
            update.to_payload(),
            json!({
                "sessionUpdate": "subagent_progress",
                "subagent_id": "session-child-1",
                "parent_session_id": "parent-session",
                "child_session_id": "session-child-1",
                "duration_ms": 2_000,
                "turn_count": 1,
                "tool_call_count": 3,
                "tokens_used": 512,
                "context_window_tokens": 262_144,
                "context_usage_pct": 0,
                "tools_used": ["read_file"],
                "error_count": 0,
            })
        );
    }

    #[test]
    fn finished_update_payload_matches_grok_wire_shape_and_omits_absent_error() {
        let update = SubagentUpdate::Finished(SubagentFinishedUpdate {
            subagent_id: "session-child-1".to_string(),
            child_session_id: "session-child-1".to_string(),
            status: SubagentFinishStatus::Completed,
            error: None,
            output: None,
            tool_calls: 3,
            turns: 1,
            duration_ms: 2_000,
            tokens_used: 512,
            will_wake: false,
        });
        let payload = update.to_payload();
        // Exact key set and casing: the finish carries no
        // `parent_session_id` at all — the pager routes it by the
        // subagent id alone.
        assert_eq!(
            payload,
            json!({
                "sessionUpdate": "subagent_finished",
                "subagent_id": "session-child-1",
                "child_session_id": "session-child-1",
                "status": "completed",
                "tool_calls": 3,
                "turns": 1,
                "duration_ms": 2_000,
                "tokens_used": 512,
                "will_wake": false,
            })
        );
        assert!(
            payload.get("parent_session_id").is_none(),
            "the finish never carries parent_session_id: {payload}"
        );
        assert!(payload.get("error").is_none());
        assert!(payload.get("output").is_none());
    }

    #[test]
    fn finished_update_includes_error_when_present() {
        let update = SubagentUpdate::Finished(SubagentFinishedUpdate {
            subagent_id: "session-child-1".to_string(),
            child_session_id: "session-child-1".to_string(),
            status: SubagentFinishStatus::Failed,
            error: Some("backend unavailable".to_string()),
            output: None,
            tool_calls: 0,
            turns: 1,
            duration_ms: 100,
            tokens_used: 0,
            will_wake: false,
        });
        let payload = update.to_payload();
        assert_eq!(payload["status"], "failed");
        assert_eq!(payload["error"], "backend unavailable");
        assert_eq!(payload["tokens_used"], 0);
        assert_eq!(payload["will_wake"], false);
    }

    #[test]
    fn finish_status_wire_names_match_grok_pager() {
        assert_eq!(SubagentFinishStatus::Completed.wire_name(), "completed");
        assert_eq!(SubagentFinishStatus::Failed.wire_name(), "failed");
        assert_eq!(SubagentFinishStatus::Cancelled.wire_name(), "cancelled");
    }

    #[test]
    fn running_child_projects_spawned_then_progress_without_finished() {
        let children = vec![child_row("child-1", Some("processing"))];
        let (updates, _chronology) = project_child_rows(
            &children,
            &[],
            &[],
            &[],
            "parent-request",
            "parent-session",
            262_144,
        );
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].session_update_kind(), "subagent_spawned");
        assert_eq!(updates[1].session_update_kind(), "subagent_progress");
        let SubagentUpdate::Progress(progress) = &updates[1] else {
            panic!("second update should be progress");
        };
        assert_eq!(progress.tokens_used, 0);
        assert_eq!(progress.context_window_tokens, 262_144);
    }

    #[test]
    fn completed_child_projects_spawned_then_finished_with_cancelled_false() {
        let children = vec![child_row("child-1", Some("completed"))];
        let responses = vec![response_row("child-1", Some(1_024))];
        let (updates, _chronology) = project_child_rows(
            &children,
            &[],
            &responses,
            &[],
            "parent-request",
            "parent-session",
            262_144,
        );
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].session_update_kind(), "subagent_spawned");
        assert_eq!(updates[1].session_update_kind(), "subagent_finished");
        let SubagentUpdate::Finished(finished) = &updates[1] else {
            panic!("second update should be finished");
        };
        assert_eq!(finished.status, SubagentFinishStatus::Completed);
        assert_eq!(finished.child_session_id, "session-child-1");
        // The subagent id is the child session id, and the finish carries
        // the durable token count with no parent session id at all.
        assert_eq!(finished.subagent_id, "session-child-1");
        assert_eq!(finished.tokens_used, 1_024);
        assert!(!finished.will_wake);
        assert!(finished.output.is_none());
    }

    #[test]
    fn interrupted_child_projects_cancelled_finish() {
        let child = child_row("child-1", Some("interrupted"));
        let (updates, _chronology) = project_child_rows(
            &[child],
            &[],
            &[],
            &[],
            "parent-request",
            "parent-session",
            262_144,
        );
        let Some(SubagentUpdate::Finished(finished)) = updates
            .iter()
            .find(|update| update.session_update_kind() == "subagent_finished")
        else {
            panic!("interrupted child should finish");
        };
        assert_eq!(finished.status, SubagentFinishStatus::Cancelled);
    }

    #[test]
    fn response_interrupted_marker_waits_for_request_terminalization() {
        let children = vec![child_row("child-1", Some("processing"))];
        let response: ChildResponseRow = serde_json::from_value(json!({
            "request_id": "child-1", "interrupted_at": "2026-08-31T22:46:46Z"
        }))
        .unwrap();
        let (updates, _chronology) = project_child_rows(
            &children,
            &[],
            &[response],
            &[],
            "parent-request",
            "parent-session",
            262_144,
        );
        assert_eq!(updates.len(), 2);
        assert!(matches!(&updates[1], SubagentUpdate::Progress(_)));
    }

    #[test]
    fn interrupted_row_without_marker_is_terminal_and_cancelled() {
        // The canonical lifecycle is authoritative: `interrupted` is
        // terminal even when the durable `interrupt_requested_at` marker
        // never latched (a lost/cleared marker must not resurrect an
        // interrupted child as still-running progress).
        let children = vec![child_row("child-1", Some("interrupted"))];
        let (updates, _chronology) = project_child_rows(
            &children,
            &[],
            &[],
            &[],
            "parent-request",
            "parent-session",
            262_144,
        );
        assert_eq!(updates.len(), 2, "spawned then finished, no progress");
        assert_eq!(updates[0].session_update_kind(), "subagent_spawned");
        let SubagentUpdate::Finished(finished) = &updates[1] else {
            panic!("interrupted child without a marker must finish");
        };
        assert_eq!(finished.status, SubagentFinishStatus::Cancelled);
    }

    #[test]
    fn error_child_projects_failed_finish_with_reason() {
        let mut child = child_row("child-1", Some("failed"));
        child.failure_reason = Some("provider error".to_string());
        let (updates, _chronology) = project_child_rows(
            &[child],
            &[],
            &[],
            &[],
            "parent-request",
            "parent-session",
            262_144,
        );
        let Some(SubagentUpdate::Finished(finished)) = updates
            .iter()
            .find(|update| update.session_update_kind() == "subagent_finished")
        else {
            panic!("error child should finish");
        };
        assert_eq!(finished.status, SubagentFinishStatus::Failed);
        assert_eq!(finished.error.as_deref(), Some("provider error"));
    }

    #[test]
    fn every_canonical_lifecycle_state_maps_to_its_pager_status() {
        // The full canonical runtime vocabulary, mapped once and exhaustively:
        // terminal completes/cancels/fails per the audited table, and every
        // still-active state projects progress and never a finish.
        let finished_status = |lifecycle_state: &str| {
            // No interrupt marker is set for any state: the canonical
            // lifecycle alone decides terminality — `interrupted` is
            // terminal and cancels even when the marker is absent.
            let child = child_row("child-1", Some(lifecycle_state));
            let (updates, _chronology) = project_child_rows(
                &[child],
                &[],
                &[],
                &[],
                "parent-request",
                "parent-session",
                262_144,
            );
            match updates
                .iter()
                .find(|update| update.session_update_kind() == "subagent_finished")
            {
                Some(SubagentUpdate::Finished(finished)) => Some(finished.status),
                _ => None,
            }
        };

        // Terminal mappings.
        assert_eq!(
            finished_status("completed"),
            Some(SubagentFinishStatus::Completed)
        );
        assert_eq!(
            finished_status("interrupted"),
            Some(SubagentFinishStatus::Cancelled)
        );
        for failure_state in ["failed", "dead", "superseded"] {
            assert_eq!(
                finished_status(failure_state),
                Some(SubagentFinishStatus::Failed),
                "{failure_state} must project a failed finish"
            );
        }

        // Still-active states project progress only.
        for active_state in ["pending", "claimed", "processing", "inputRequired"] {
            let children = vec![child_row("child-1", Some(active_state))];
            let (updates, _chronology) = project_child_rows(
                &children,
                &[],
                &[],
                &[],
                "parent-request",
                "parent-session",
                262_144,
            );
            assert_eq!(updates.len(), 2, "{active_state} spawns then progresses");
            assert_eq!(updates[0].session_update_kind(), "subagent_spawned");
            assert_eq!(updates[1].session_update_kind(), "subagent_progress");
        }
    }

    #[test]
    fn rows_for_other_parents_are_ignored() {
        let mut child = child_row("child-1", Some("completed"));
        child.caused_by_parent_request_id = Some("other-parent".to_string());
        let (updates, _chronology) = project_child_rows(
            &[child],
            &[],
            &[],
            &[],
            "parent-request",
            "parent-session",
            262_144,
        );
        assert!(updates.is_empty());
    }

    #[test]
    fn spawn_tool_row_supplies_description_but_not_the_subagent_id() {
        let mut child = child_row("child-1", Some("completed"));
        child.caused_by_parent_tool_call_id = Some("call-9".to_string());
        child.caused_by_parent_tool_call_doc_id = Some("doc-call-1".to_string());
        let children = vec![child];
        let spawn_tools = vec![SpawnToolRow {
            doc_id: Some("doc-call-1".to_string()),
            request_id: "parent-request".to_string(),
            request_doc_id: Some("doc-parent-request".to_string()),
            tool_call_id: "call-9".to_string(),
            child_request_id: Some("child-1".to_string()),
            args: Some(r#"{"name":"repo scout"}"#.to_string()),
            message_sequence: Some(2),
        }];
        let (updates, _chronology) = project_child_rows(
            &children,
            &spawn_tools,
            &[],
            &[],
            "parent-request",
            "parent-session",
            262_144,
        );
        let SubagentUpdate::Spawned(spawned) = &updates[0] else {
            panic!("first update should be spawned");
        };
        // The subagent id is the child session id, never the spawn tool
        // call id: the pager routes subagent updates by `childSessionId`
        // and the ext controls address the same id.
        assert_eq!(spawned.subagent_id, "session-child-1");
        assert_ne!(spawned.subagent_id, "call-9");
        assert_eq!(spawned.description, "repo scout");
    }

    #[test]
    fn child_session_id_is_the_subagent_id_without_a_spawn_tool_row() {
        let children = vec![child_row("child-1", Some("processing"))];
        let (updates, _chronology) = project_child_rows(
            &children,
            &[],
            &[],
            &[],
            "parent-request",
            "parent-session",
            262_144,
        );
        let SubagentUpdate::Spawned(spawned) = &updates[0] else {
            panic!("first update should be spawned");
        };
        assert_eq!(spawned.subagent_id, "session-child-1");
        assert_eq!(spawned.child_session_id, "session-child-1");
    }

    #[test]
    fn parent_prompt_id_is_read_from_child_metadata() {
        let mut child = child_row("child-1", Some("processing"));
        child.metadata = Some(r#"{"promptId":"prompt-42"}"#.to_string());
        let (updates, _chronology) = project_child_rows(
            &[child],
            &[],
            &[],
            &[],
            "parent-request",
            "parent-session",
            262_144,
        );
        let SubagentUpdate::Spawned(spawned) = &updates[0] else {
            panic!("first update should be spawned");
        };
        assert_eq!(spawned.parent_prompt_id.as_deref(), Some("prompt-42"));
    }

    #[test]
    fn child_behavior_id_maps_to_subagent_type_with_default() {
        let mut untyped = child_row("child-1", Some("processing"));
        untyped.behavior_id = None;
        let (updates, _chronology) = project_child_rows(
            &[untyped],
            &[],
            &[],
            &[],
            "parent-request",
            "parent-session",
            262_144,
        );
        let SubagentUpdate::Spawned(spawned) = &updates[0] else {
            panic!("first update should be spawned");
        };
        assert_eq!(spawned.subagent_type, "general-purpose");
    }

    #[test]
    fn long_descriptions_are_truncated_on_char_boundaries() {
        let description = "å".repeat(200);
        assert_eq!(truncate_description(&description).chars().count(), 120);
        assert_eq!(truncate_description("  short  "), "short");
        assert_eq!(truncate_description(""), "");
        // Exactly at the limit is preserved verbatim, and the truncation is a
        // character count, not a byte count.
        assert_eq!(truncate_description(&"å".repeat(120)), "å".repeat(120));
        assert_eq!(truncate_description(&"å".repeat(121)).chars().count(), 120);
    }

    #[test]
    fn usage_percent_is_bounded_and_zero_window_safe() {
        assert_eq!(context_usage_pct(0, 262_144), 0);
        assert_eq!(context_usage_pct(262_144, 262_144), 100);
        assert_eq!(context_usage_pct(u64::MAX, 262_144), 100);
        assert_eq!(context_usage_pct(1_000, 0), 0);
    }

    #[test]
    fn zero_context_window_falls_back_to_catalog_default() {
        let children = vec![child_row("child-1", Some("processing"))];
        let (updates, _chronology) = project_child_rows(
            &children,
            &[],
            &[],
            &[],
            "parent-request",
            "parent-session",
            0,
        );
        let SubagentUpdate::Progress(progress) = &updates[1] else {
            panic!("second update should be progress");
        };
        assert_eq!(
            progress.context_window_tokens,
            super::super::DEFAULT_CONTEXT_WINDOW_TOKENS
        );
    }

    #[test]
    fn elapsed_millis_parses_rfc3339_and_tolerates_missing_bounds() {
        assert_eq!(
            elapsed_millis(Some("2026-08-31T22:46:45Z"), Some("2026-08-31T22:46:47Z")),
            2_000
        );
        assert_eq!(elapsed_millis(None, Some("2026-08-31T22:46:47Z")), 0);
        assert_eq!(elapsed_millis(Some("2026-08-31T22:46:45Z"), None), 0);
        assert_eq!(elapsed_millis(Some("not-a-date"), Some("not-a-date")), 0);
    }

    #[test]
    fn queries_escape_interpolated_values() {
        let query = child_requests_query(r#"parent-"quoted\"-id"#);
        assert!(
            !query.contains(r#""parent-"quoted\"-id""#),
            "raw value must not appear unescaped: {query}"
        );
        assert!(query.contains(r#"caused_by_parent_request_id"#));

        let responses = child_responses_query(["req-a", "req-b"]);
        assert!(responses.contains(r#""req-a", "req-b""#));

        // Quotes and backslashes in list members are escaped too: the raw
        // member text never reaches the query verbatim.
        let hostile = r#"req"a\b"#;
        let escaped_list = child_responses_query([hostile]);
        assert!(
            escaped_list.contains(r#""req\"a\\b""#),
            "escaped member missing: {escaped_list}"
        );
        assert!(
            !escaped_list.contains(r#""req"a\b""#),
            "raw member must not appear unescaped: {escaped_list}"
        );

        let tools = spawn_tools_query(r#"parent-'quoted"-id"#);
        assert!(
            !tools.contains(r#""parent-'quoted"-id""#),
            "raw value must not appear unescaped: {tools}"
        );
        assert!(
            tools.contains(r#"child_request_id: { _ne: "" }"#),
            "{tools}"
        );
    }

    #[test]
    fn child_query_is_scoped_to_the_parent_request_and_orders_by_creation() {
        let query = child_requests_query("parent-request");
        assert!(
            query.contains(r#"caused_by_parent_request_id: { _eq: "parent-request" }"#),
            "{query}"
        );
        assert!(query.contains("order: { created_at: ASC }"), "{query}");
        assert!(query.contains("parent: AgentRequest("), "{query}");
        assert!(query.contains("children: AgentRequest("), "{query}");
        assert!(query.contains("_docID"), "{query}");
        assert!(query.contains("caused_by_parent_request_doc_id"), "{query}");
        assert!(
            query.contains("caused_by_parent_tool_call_doc_id"),
            "{query}"
        );
        assert!(!query.to_lowercase().contains("task("), "{query}");
    }

    #[test]
    fn get_stub_matches_the_generated_get_subagent_response() {
        // The generated `GetSubagentResponse` is a single nullable `snapshot`
        // field: a missing id answers `{"snapshot": null}` exactly, with no
        // invented `subagentId` echo and no invented `outcome` wrapper.
        let result = subagent_get_not_found_result();
        assert_eq!(result["snapshot"], Value::Null);
        assert_eq!(result, json!({"snapshot": null}));
        assert!(
            result.get("subagentId").is_none(),
            "the generated get response never echoes the id: {result}"
        );
        assert!(
            result.get("outcome").is_none(),
            "the generated get response carries no outcome wrapper: {result}"
        );
    }

    #[test]
    fn list_running_stub_matches_the_generated_list_running_response() {
        // The generated `ListRunningSubagentsResponse` serializes as
        // `{"subagents": []}` — the key is `subagents`, never `running`.
        let result = subagent_list_running_empty_result();
        assert_eq!(result["subagents"], json!([]));
        assert_eq!(result, json!({"subagents": []}));
        assert!(
            result.get("running").is_none(),
            "the generated response never uses a `running` key: {result}"
        );
    }

    #[test]
    fn cancel_stub_returns_uncancelled_not_found_outcome() {
        let result = subagent_cancel_not_found_result("missing-subagent");
        assert_eq!(result["subagentId"], "missing-subagent");
        assert_eq!(result["cancelled"], false);
        assert_eq!(result["outcome"]["kind"], "not_found");
    }

    #[test]
    fn blank_child_rows_do_not_project_a_finished_update() {
        let children = vec![child_row("child-1", None)];
        let (updates, _chronology) = project_child_rows(
            &children,
            &[],
            &[],
            &[],
            "parent-request",
            "parent-session",
            262_144,
        );
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[1].session_update_kind(), "subagent_progress");
    }

    #[test]
    fn child_tool_rows_feed_counts_names_and_errors() {
        let children = vec![child_row("child-1", Some("processing"))];
        let child_tools = vec![
            ChildToolRow {
                request_id: "child-1".to_string(),
                tool_name: Some("read_file".to_string()),
                lifecycle_state: Some("completed".to_string()),
            },
            ChildToolRow {
                request_id: "child-1".to_string(),
                tool_name: Some("read_file".to_string()),
                lifecycle_state: Some("completed".to_string()),
            },
            ChildToolRow {
                request_id: "child-1".to_string(),
                tool_name: Some("bash".to_string()),
                lifecycle_state: Some("failed".to_string()),
            },
            // Belongs to a different child request: must not be counted.
            ChildToolRow {
                request_id: "child-2".to_string(),
                tool_name: Some("grep".to_string()),
                lifecycle_state: Some("failed".to_string()),
            },
        ];
        let (updates, _chronology) = project_child_rows(
            &children,
            &[],
            &[],
            &child_tools,
            "parent-request",
            "parent-session",
            262_144,
        );
        assert_eq!(updates.len(), 2);
        let SubagentUpdate::Progress(progress) = &updates[1] else {
            panic!("second update should be progress");
        };
        assert_eq!(progress.tool_call_count, 3);
        assert_eq!(progress.tools_used, vec!["read_file", "bash"]);
        assert_eq!(progress.error_count, 1);

        // A terminal child reports the same counts on subagent_finished.
        let mut finished_child = child_row("child-1", Some("completed"));
        finished_child.request_id = "child-1".to_string();
        let (updates, _chronology) = project_child_rows(
            &[finished_child],
            &[],
            &[],
            &child_tools,
            "parent-request",
            "parent-session",
            262_144,
        );
        let SubagentUpdate::Finished(finished) = &updates[1] else {
            panic!("second update should be finished");
        };
        assert_eq!(finished.tool_calls, 3);
    }

    #[test]
    fn child_tool_query_is_scoped_to_child_request_ids() {
        let query = child_tools_query(["child-1", "child-2"]);
        assert!(query.contains("AgentToolCall("), "{query}");
        assert!(
            query.contains(r#"request_id: { _in: ["child-1", "child-2"] }"#),
            "{query}"
        );
        assert!(query.contains("tool_name"), "{query}");
        assert!(query.contains("lifecycle_state"), "{query}");
        assert!(!query.to_lowercase().contains("task("), "{query}");
    }

    /// Equal-`created_at` children sort by request id: query iteration
    /// order and equal timestamps never decide the projected wire order.
    #[test]
    fn equal_time_children_sort_by_request_id() {
        let mut later_by_id = child_row("child-z", Some("processing"));
        later_by_id.created_at = Some("2026-08-31T22:46:45Z".to_string());
        let mut earlier_by_id = child_row("child-a", Some("processing"));
        earlier_by_id.created_at = Some("2026-08-31T22:46:45Z".to_string());

        // Delivered in reverse id order (the query's `created_at: ASC` is a
        // tie here, so iteration order decides): the sort must still order
        // by the durable request id.
        let mut children = vec![later_by_id, earlier_by_id.clone()];
        sort_child_rows(&mut children);
        assert_eq!(
            children
                .iter()
                .map(|child| child.request_id.as_str())
                .collect::<Vec<_>>(),
            vec!["child-a", "child-z"],
            "equal-time children must sort by request id, not input order"
        );

        // A strictly earlier timestamp wins over the id tie-break.
        let mut older = child_row("child-zz", Some("processing"));
        older.created_at = Some("2026-08-31T22:46:44Z".to_string());
        let mut children = vec![earlier_by_id, older];
        sort_child_rows(&mut children);
        assert_eq!(
            children
                .iter()
                .map(|child| child.request_id.as_str())
                .collect::<Vec<_>>(),
            vec!["child-zz", "child-a"],
            "created_at dominates; the request id only breaks exact ties"
        );
    }

    /// Mixed-offset RFC3339 `created_at` values compare chronologically
    /// (the normalization is instant-based, not literal-string based), and
    /// missing or unparseable timestamps sort after every real one.
    #[test]
    fn created_at_sort_key_normalizes_timestamp_offsets() {
        let utc = child_row_sort_key(&ChildRequestRow {
            created_at: Some("2026-08-31T22:46:45Z".to_string()),
            ..child_row("child-1", None)
        });
        let offset = child_row_sort_key(&ChildRequestRow {
            created_at: Some("2026-08-31T23:16:45+00:30".to_string()),
            ..child_row("child-1", None)
        });
        assert_eq!(
            utc.1, offset.1,
            "the same instant written with an offset must compare equal"
        );
        assert!(!utc.0, "a real timestamp is not flagged missing");
        let later_utc = child_row_sort_key(&ChildRequestRow {
            created_at: Some("2026-08-31T22:46:46Z".to_string()),
            ..child_row("child-1", None)
        });
        assert!(utc.1 < later_utc.1, "one second later must sort later");
        // Blank and unparseable timestamps sort last (missing flag).
        let blank = child_row_sort_key(&ChildRequestRow {
            created_at: None,
            ..child_row("child-1", None)
        });
        let unparseable = child_row_sort_key(&ChildRequestRow {
            created_at: Some("not-a-date".to_string()),
            ..child_row("child-1", None)
        });
        assert!(
            later_utc < blank,
            "a missing timestamp sorts after real ones"
        );
        assert!(
            later_utc < unparseable,
            "an unparseable timestamp sorts after real ones"
        );
        assert_eq!(normalize_rfc3339(Some("not-a-date")), None);
    }

    /// Equal-sequence spawn rows sort by the stable tool call identity,
    /// matching the tool family's ordering of the same rows; spawn rows
    /// without a sequence sort after positioned ones.
    #[test]
    fn spawn_rows_sort_by_sequence_then_stable_tool_identity() {
        let spawn = |tool_call_id: &str, sequence: Option<i64>, child: &str| SpawnToolRow {
            doc_id: Some(format!("doc-{tool_call_id}")),
            request_id: "parent-request".to_string(),
            request_doc_id: Some("doc-parent-request".to_string()),
            tool_call_id: tool_call_id.to_string(),
            child_request_id: Some(child.to_string()),
            args: Some(r#"{"name":"scout"}"#.to_string()),
            message_sequence: sequence,
        };
        let mut rows = vec![
            spawn("call-z", Some(5), "child-z"),
            spawn("call-a", Some(5), "child-a"),
            spawn("call-m", Some(9), "child-m"),
            spawn("call-b", Some(2), "child-b"),
            spawn("call-x", None, "child-x"),
        ];
        sort_spawn_rows(&mut rows);
        assert_eq!(
            rows.iter()
                .map(|row| row.tool_call_id.as_str())
                .collect::<Vec<_>>(),
            vec!["call-b", "call-a", "call-z", "call-m", "call-x"],
            "spawn rows must sort by (message_sequence, tool identity), not input order"
        );
        // Reversing the input leaves the order unchanged.
        let mut reversed = rows.clone();
        reversed.reverse();
        sort_spawn_rows(&mut reversed);
        assert_eq!(
            reversed
                .iter()
                .map(|row| row.tool_call_id.as_str())
                .collect::<Vec<_>>(),
            vec!["call-b", "call-a", "call-z", "call-m", "call-x"],
        );
    }

    /// Two same-sequence spawn rows and their equal-time children project
    /// in the agreed order: the child linked to `call-a` precedes the child
    /// linked to `call-z`, in both families, regardless of decoded input
    /// order.
    #[test]
    fn same_sequence_spawns_and_equal_time_children_agree_on_order() {
        let mut child_a = child_row("child-a", Some("processing"));
        child_a.created_at = Some("2026-08-31T22:46:45Z".to_string());
        child_a.caused_by_parent_tool_call_id = Some("call-a".to_string());
        child_a.caused_by_parent_tool_call_doc_id = Some("doc-call-a".to_string());
        let mut child_z = child_row("child-z", Some("processing"));
        child_z.created_at = Some("2026-08-31T22:46:45Z".to_string());
        child_z.caused_by_parent_tool_call_id = Some("call-z".to_string());
        child_z.caused_by_parent_tool_call_doc_id = Some("doc-call-z".to_string());
        let spawn_a = SpawnToolRow {
            doc_id: Some("doc-call-a".to_string()),
            request_id: "parent-request".to_string(),
            request_doc_id: Some("doc-parent-request".to_string()),
            tool_call_id: "call-a".to_string(),
            child_request_id: Some("child-a".to_string()),
            args: Some(r#"{"name":"scout a"}"#.to_string()),
            message_sequence: Some(5),
        };
        let spawn_z = SpawnToolRow {
            doc_id: Some("doc-call-z".to_string()),
            request_id: "parent-request".to_string(),
            request_doc_id: Some("doc-parent-request".to_string()),
            tool_call_id: "call-z".to_string(),
            child_request_id: Some("child-z".to_string()),
            args: Some(r#"{"name":"scout z"}"#.to_string()),
            message_sequence: Some(5),
        };
        // Reverse input order for both families, then apply the same sorts
        // the decode boundary applies (`sort_child_rows` at decode;
        // `sort_spawn_rows` at decode) — the unit test drives the pure
        // projection with the same rows the production path would.
        let mut children = vec![child_z, child_a];
        sort_child_rows(&mut children);
        let mut spawn_tools = vec![spawn_z, spawn_a];
        sort_spawn_rows(&mut spawn_tools);
        let (updates, chronology) = project_child_rows(
            &children,
            &spawn_tools,
            &[],
            &[],
            "parent-request",
            "parent-session",
            262_144,
        );
        assert_eq!(updates.len(), 4);
        // The subagent id is the child session id in every family member,
        // so the projected order follows the children's durable order.
        assert_eq!(updates[0].subagent_id(), "session-child-a");
        assert_eq!(updates[1].subagent_id(), "session-child-a");
        assert_eq!(updates[2].subagent_id(), "session-child-z");
        assert_eq!(updates[3].subagent_id(), "session-child-z");
        assert_eq!(chronology, vec![Some(5), Some(5), Some(5), Some(5)]);
    }

    /// Start an embedded node with the runtime schemas, the production
    /// shape every embedded subagent projection test uses.
    async fn embedded_node() -> (tempfile::TempDir, EmbeddedNode) {
        let dir = tempfile::tempdir().expect("tempdir");
        let node = EmbeddedNode::builder()
            // The staging `TempDir` guard stays in scope (`dir`) for the
            // test's lifetime, so the node's storage directory is deleted
            // when the test ends — never abandoned or leaked.
            .data_path(dir.path().join("node"))
            .with_storage_backend(gents::defra_node::StorageBackend::Regolith)
            .build()
            .await
            .expect("embedded node");
        gents::schema::ensure_runtime_schemas(&node)
            .await
            .expect("runtime schemas");
        (dir, node)
    }

    /// The production-shaped canonical-interrupt regression: a parent
    /// `AgentRequest` whose child row carries `lifecycle_state:
    /// "interrupted"` with **no** `interrupt_requested_at` marker latched.
    /// The actual subagent projection query/decoder path must observe the
    /// child and project exactly `subagent_finished` with status
    /// `cancelled` — never a still-running `subagent_progress`.
    #[tokio::test]
    async fn embedded_interrupted_child_without_marker_projects_cancelled_finish() {
        let (_dir, node) = embedded_node().await;
        let parent_request_id = "parent-req-embedded-interrupt";
        let child_request_id = "child-req-embedded-interrupt";
        let session_id = "s-embedded-interrupt";

        let escaped_parent = escape_graphql_string(parent_request_id);
        let escaped_child = escape_graphql_string(child_request_id);
        let escaped_session = escape_graphql_string(session_id);
        // Seed the parent first so both bridge rows can carry its physical
        // document id, exactly as runtime admission persists them.
        let seed_parent = format!(
            r#"mutation {{
                parent: create_AgentRequest(input: {{
                    request_id: "{escaped_parent}"
                    agent_did: "did:test:grok-shim"
                    requester_did: "did:test:grok-shim"
                    session_id: "{escaped_session}"
                    content: "parent work"
                    status: "completed"
                    lifecycle_state: "completed"
                    backend_id: ""
                    execution_origin: "interactive"
                    failure_reason: ""
                    created_at: "2026-08-31T22:46:44Z"
                    retry_count: 0
                    max_retries: 3
                }}) {{ _docID }}
            }}"#
        );
        let response = node.execute(&seed_parent).await;
        assert!(!response.has_errors(), "seed failed: {:?}", response.errors);
        let parent_doc_id = response
            .data
            .as_ref()
            .and_then(|data| data.pointer("/parent/0/_docID"))
            .and_then(Value::as_str)
            .expect("parent document id");
        let escaped_parent_doc = escape_graphql_string(parent_doc_id);

        let seed_tool = format!(
            r#"mutation {{
                bridge: create_AgentToolCall(input: {{
                    tool_call_key: "{escaped_session}:call-child"
                    request_id: "{escaped_parent}"
                    request_doc_id: "{escaped_parent_doc}"
                    session_id: "{escaped_session}"
                    agent_did: "did:test:grok-shim"
                    requester_did: "did:test:grok-shim"
                    tool_call_id: "call-child"
                    tool_name: "task"
                    args: "{{}}"
                    started_at: "2026-08-31T22:46:44Z"
                    deadline_at: "2099-08-31T22:46:44Z"
                    await_mode: "background"
                    cancel_policy: "cascade"
                    child_request_id: "{escaped_child}"
                    lifecycle_state: "running"
                    status: "running"
                    result: ""
                    message_sequence: 1
                }}) {{ _docID }}
            }}"#
        );
        let response = node.execute(&seed_tool).await;
        assert!(
            !response.has_errors(),
            "bridge seed failed: {:?}",
            response.errors
        );
        let tool_doc_id = response
            .data
            .as_ref()
            .and_then(|data| data.pointer("/bridge/0/_docID"))
            .and_then(Value::as_str)
            .expect("bridge document id");
        let escaped_tool_doc = escape_graphql_string(tool_doc_id);
        let control_metadata = escape_graphql_string(
            r#"{"queue":{"source":"background_completion","policy":"coalesce","key":"wake:1","queued_after_request_id":"parent-req-embedded-interrupt"},"background_completion_wake_version":1}"#,
        );

        // Only the first child has the runtime's full logical+physical
        // provenance. The forged logical-only sibling must not project.
        let seed_children = format!(
            r#"mutation {{
                child: create_AgentRequest(input: {{
                    request_id: "{escaped_child}"
                    agent_did: "did:test:grok-shim"
                    requester_did: "did:test:grok-shim"
                    behavior_id: "test-child"
                    session_id: "child-session"
                    caused_by_parent_request_id: "{escaped_parent}"
                    caused_by_parent_request_doc_id: "{escaped_parent_doc}"
                    caused_by_parent_tool_call_id: "call-child"
                    caused_by_parent_tool_call_doc_id: "{escaped_tool_doc}"
                    content: "child work"
                    status: "processing"
                    lifecycle_state: "interrupted"
                    backend_id: ""
                    execution_origin: "interactive"
                    failure_reason: ""
                    created_at: "2026-08-31T22:46:45Z"
                    terminalized_at: "2026-08-31T22:46:46Z"
                    retry_count: 0
                    max_retries: 3
                }}) {{ _docID }}
                forged: create_AgentRequest(input: {{
                    request_id: "forged-logical-child"
                    agent_did: "did:test:grok-shim"
                    session_id: "forged-child-session"
                    caused_by_parent_request_id: "{escaped_parent}"
                    content: "forged child"
                    status: "processing"
                    lifecycle_state: "processing"
                    backend_id: ""
                    execution_origin: "interactive"
                    failure_reason: ""
                    created_at: "2026-08-31T22:46:45Z"
                    retry_count: 0
                    max_retries: 3
                }}) {{ _docID }}
                control: create_AgentRequest(input: {{
                    request_id: "valid-background-control"
                    agent_did: "did:test:grok-shim"
                    session_id: "{escaped_session}"
                    caused_by_parent_request_id: "{escaped_parent}"
                    caused_by_parent_request_doc_id: "{escaped_parent_doc}"
                    content: "Review the new background completion results"
                    lifecycle_state: "processing"
                    metadata: "{control_metadata}"
                    created_at: "2026-08-31T22:46:47Z"
                }}) {{ _docID }}
            }}"#
        );
        let response = node.execute(&seed_children).await;
        assert!(
            !response.has_errors(),
            "child seed failed: {:?}",
            response.errors
        );

        // This is a valid timeline control link, not a malformed child. It
        // must still be absent from the narrower subagent UI projection.
        let candidates = node.execute(&child_requests_query(parent_request_id)).await;
        let parents = decode_rows::<TimelineRequestRow>(&candidates, "parent", "parent");
        let control = decode_child_rows(&candidates)
            .into_iter()
            .find(|child| child.request_id == "valid-background-control")
            .unwrap();
        assert!(child_bridge_is_corroborated(
            &parents[0],
            &child_timeline_row(&control).unwrap(),
            &[]
        ));

        // The actual production query/decoder path, not hand-built rows.
        let projection = project_subagents(&node, parent_request_id, session_id, 262_144)
            .await
            .expect("subagent projection");
        let kinds: Vec<&str> = projection
            .updates
            .iter()
            .map(|update| update.session_update_kind())
            .collect();
        assert_eq!(
            kinds,
            vec!["subagent_spawned", "subagent_finished"],
            "an interrupted child without an interrupt marker must finish \
             as cancelled, never project progress"
        );
        let SubagentUpdate::Finished(finished) = &projection.updates[1] else {
            panic!("second update should be the finish");
        };
        assert_eq!(finished.status, SubagentFinishStatus::Cancelled);
        assert_eq!(finished.child_session_id, "child-session");
        assert!(
            projection
                .updates
                .iter()
                .all(|update| update.subagent_id() != session_id),
            "a valid same-session control continuation is never a subagent"
        );
        assert!(projection
            .updates
            .iter()
            .all(|update| update.subagent_id() != "forged-child-session"));

        let node = std::sync::Arc::new(node);
        let sessions = vec![session_id.to_owned()];
        let params = json!({"subagentId": "child-session"});
        let get = control::handle(
            node.clone(),
            "did:test:grok-shim",
            &sessions,
            SUBAGENT_GET_METHOD,
            &params,
            262_144,
        )
        .await
        .unwrap();
        assert_eq!(get["snapshot"]["status"], "cancelled");
        assert_eq!(get["snapshot"]["parentSessionId"], session_id);
        let missing = control::handle(
            node.clone(),
            "did:test:grok-shim",
            &["unrelated".into()],
            SUBAGENT_GET_METHOD,
            &params,
            262_144,
        )
        .await
        .unwrap();
        assert_eq!(missing, json!({"snapshot": null}));
        let wrong_principal = control::handle(
            node.clone(),
            "did:test:other",
            &sessions,
            SUBAGENT_GET_METHOD,
            &params,
            262_144,
        )
        .await
        .unwrap();
        assert_eq!(wrong_principal, json!({"snapshot": null}));

        let active = node
            .execute(&format!(
                r#"mutation {{ update_AgentRequest(
            filter: {{request_id: {{_eq: "{escaped_child}"}}}},
            input: {{lifecycle_state: "processing", terminalized_at: null}}
        ) {{_docID}} }}"#
            ))
            .await;
        assert!(!active.has_errors(), "{:?}", active.errors);
        let list = control::handle(
            node.clone(),
            "did:test:grok-shim",
            &sessions,
            SUBAGENT_LIST_RUNNING_METHOD,
            &json!({}),
            262_144,
        )
        .await
        .unwrap();
        assert_eq!(list["subagents"].as_array().unwrap().len(), 1);
        assert_eq!(list["subagents"][0]["childSessionId"], "child-session");
        assert!(
            list["subagents"][0].get("status").is_none(),
            "list has the stock live DTO shape"
        );
        let wrong_cancel = control::handle(
            node.clone(),
            "did:test:grok-shim",
            &["unrelated".into()],
            SUBAGENT_CANCEL_METHOD,
            &params,
            262_144,
        )
        .await
        .unwrap();
        assert_eq!(wrong_cancel["outcome"]["kind"], "not_found");
        let cancelled = control::handle(
            node.clone(),
            "did:test:grok-shim",
            &sessions,
            SUBAGENT_CANCEL_METHOD,
            &params,
            262_144,
        )
        .await
        .unwrap();
        assert_eq!(cancelled["outcome"]["kind"], "cancelled");
        let state = node.execute(&format!(r#"{{ AgentRequest(filter: {{request_id: {{_eq: "{escaped_child}"}}}}) {{interrupt_requested_at lifecycle_state}} }}"#)).await;
        assert!(!state.has_errors());
        assert!(state
            .data
            .as_ref()
            .unwrap()
            .pointer("/AgentRequest/0/interrupt_requested_at")
            .is_some_and(|v| !v.is_null()));
    }
}
