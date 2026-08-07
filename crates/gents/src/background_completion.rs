//! Background subagent completion projection.
//!
//! R4b keeps background spawns non-blocking by leaving the parent bridge row
//! running until the child request reaches a terminal state. This module owns
//! the observer path that projects that terminal state into the parent
//! `AgentToolCall`, appends a compact transcript notification, and enqueues the
//! coalesced same-session wake-up request.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use defra_node::{EmbeddedNode, EventName};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::background_tools::{
    child_request_completed, fail_running_subagent_tool_call, load_authorized_child_edge,
    load_child_final_response, load_child_terminal_row, load_parent_subagent_context,
    project_child_terminal, subagent_tool_not_allowed_payload, ChildEdge,
};
use crate::graphql::escape_graphql_string;
use crate::lifecycle::queue::{
    enqueue_session_request, parse_queue_hints, QueueHints, QueuePolicy, QueueSource,
};
use crate::lifecycle::ExecutionOrigin;
use crate::session;
use crate::tool_call_lifecycle::{AwaitMode, ChildTerminal, FailureClass, ToolCallLifecycle};
use crate::watcher::{validate_agent_request, AgentRequest};

const AGENT_REQUEST_COLLECTION: &str = "AgentRequest";
pub const BACKGROUND_COMPLETION_WAKE_PROMPT: &str =
    "Review pending subagent completion notifications in this session and continue the task if needed.";
pub const BACKGROUND_COMPLETION_NOTIFICATION_REQUEST_PREFIX: &str =
    "background-completion-notification:";

pub fn background_completion_notification_request_id(stable_id: &str) -> String {
    format!("{BACKGROUND_COMPLETION_NOTIFICATION_REQUEST_PREFIX}{stable_id}")
}

pub fn is_background_completion_notification_request_id(request_id: Option<&str>) -> bool {
    request_id.is_some_and(|request_id| {
        request_id.starts_with(BACKGROUND_COMPLETION_NOTIFICATION_REQUEST_PREFIX)
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackgroundCompletionOutcome {
    Projected {
        child_request_id: String,
        parent_request_id: String,
        parent_tool_call_id: String,
        parent_session_id: String,
        notification_sequence: u32,
        wake_request_id: String,
    },
    NotTerminal,
    NotBackground,
    MissingFinalResponse,
    AlreadyProjected,
    NotLocalOwner,
    Unlinked,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnclaimedSpawnReconcileOutcome {
    Failed {
        parent_tool_call_id: String,
        parent_request_id: String,
    },
    Linked {
        parent_tool_call_id: String,
        parent_request_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelAckOutcome {
    Acked {
        parent_tool_call_id: String,
    },
    Stuck {
        parent_tool_call_id: String,
        since: DateTime<Utc>,
    },
    Pending {
        parent_tool_call_id: String,
    },
}

pub const STUCK_CANCEL_THRESHOLD_SECS: i64 = 5 * 60;

#[derive(Debug, Deserialize)]
struct UnclaimedBridgeRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    request_id: String,
    tool_call_id: String,
    child_request_id: String,
    started_at: Option<String>,
    deadline_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CancelPendingBridgeRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    request_id: String,
    tool_call_id: String,
    child_request_id: String,
    cancel_cascade_intent_at: Option<String>,
    stuck_since: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChildAckProbeRow {
    status: Option<String>,
    lifecycle_state: Option<String>,
    interrupt_requested_at: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct AgentToolCallDateTimeRow {
    pub(crate) started_at: Option<String>,
    pub(crate) deadline_at: Option<String>,
    pub(crate) completed_at: Option<String>,
    pub(crate) unclaimed_deadline_at: Option<String>,
    pub(crate) cancel_cascade_intent_at: Option<String>,
    pub(crate) stuck_since: Option<String>,
}

pub async fn reconcile_unclaimed_cross_deployment_spawns(
    node: Arc<EmbeddedNode>,
    local_did: &str,
) -> Result<Vec<UnclaimedSpawnReconcileOutcome>> {
    let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let now = escape_graphql_string(&now);
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{
                    _and: [
                        {{ lifecycle_state: {{ _eq: "running" }} }},
                        {{ await_mode: {{ _eq: "background" }} }},
                        {{ child_request_id: {{ _ne: "" }} }},
                        {{ unclaimed_deadline_at: {{ _lt: "{now}" }} }}
                    ]
                }}
            ) {{
                _docID
                request_id
                tool_call_id
                child_request_id
                started_at
                deadline_at
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "unclaimed-spawn reconcile query failed: {:?}",
            response.errors
        );
    }
    let rows: Vec<UnclaimedBridgeRow> = response
        .data
        .as_ref()
        .and_then(|d| d.get("AgentToolCall"))
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();

    let mut outcomes = Vec::with_capacity(rows.len());
    for row in rows {
        if !request_is_locally_owned(node.as_ref(), &row.request_id, local_did).await? {
            continue;
        }

        if child_request_exists_locally(node.as_ref(), &row.child_request_id).await? {
            clear_unclaimed_deadline_at(node.as_ref(), &row.doc_id).await?;
            outcomes.push(UnclaimedSpawnReconcileOutcome::Linked {
                parent_tool_call_id: row.tool_call_id,
                parent_request_id: row.request_id,
            });
            continue;
        }

        let payload = subagent_tool_not_allowed_payload(
            "spawn_subagent",
            "/behavior_id",
            "<unknown>",
            "no_peer_claimed_spawn: no paired peer claimed the cross-deployment spawn within unclaimed_spawn_timeout_seconds",
            &[],
        );
        fail_running_subagent_tool_call(
            node.as_ref(),
            &row.doc_id,
            row.started_at.as_deref(),
            row.deadline_at.as_deref(),
            &payload,
            FailureClass::ServiceUnavailable,
        )
        .await?;
        outcomes.push(UnclaimedSpawnReconcileOutcome::Failed {
            parent_tool_call_id: row.tool_call_id,
            parent_request_id: row.request_id,
        });
    }
    Ok(outcomes)
}

pub async fn observe_cancel_cascade_ack(
    node: Arc<EmbeddedNode>,
    local_did: &str,
) -> Result<Vec<CancelAckOutcome>> {
    let now = Utc::now();
    let query = r#"{
        AgentToolCall(filter: { cancel_pending_remote_ack: { _eq: true } }) {
            _docID
            request_id
            tool_call_id
            child_request_id
            cancel_cascade_intent_at
            stuck_since
        }
    }"#;
    let response = node.execute(query).await;
    if response.has_errors() {
        anyhow::bail!("cancel-ack observer query failed: {:?}", response.errors);
    }
    let rows: Vec<CancelPendingBridgeRow> = response
        .data
        .as_ref()
        .and_then(|d| d.get("AgentToolCall"))
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();

    let mut outcomes = Vec::with_capacity(rows.len());
    for row in rows {
        if !request_is_locally_owned(node.as_ref(), &row.request_id, local_did).await? {
            continue;
        }

        let probe = load_child_ack_probe(node.as_ref(), &row.child_request_id).await?;
        let child_done = probe
            .as_ref()
            .is_some_and(|p| request_terminal_or_interrupted(p));

        if child_done {
            clear_cancel_pending_ack(node.as_ref(), &row.doc_id).await?;
            outcomes.push(CancelAckOutcome::Acked {
                parent_tool_call_id: row.tool_call_id,
            });
            continue;
        }

        let intent_at = row
            .cancel_cascade_intent_at
            .as_deref()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc));
        if let Some(intent_at) = intent_at {
            let age = (now - intent_at).num_seconds();
            if age >= STUCK_CANCEL_THRESHOLD_SECS && row.stuck_since.is_none() {
                set_stuck_since(node.as_ref(), &row.doc_id, now).await?;
                outcomes.push(CancelAckOutcome::Stuck {
                    parent_tool_call_id: row.tool_call_id,
                    since: now,
                });
                continue;
            }
        }

        outcomes.push(CancelAckOutcome::Pending {
            parent_tool_call_id: row.tool_call_id,
        });
    }
    Ok(outcomes)
}

async fn child_request_exists_locally(node: &EmbeddedNode, child_request_id: &str) -> Result<bool> {
    let escaped = escape_graphql_string(child_request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped}" }} }},
                limit: 1
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!("child existence probe failed: {:?}", response.errors);
    }
    Ok(response
        .data
        .as_ref()
        .and_then(|d| d.get("AgentRequest"))
        .and_then(|v| v.as_array())
        .is_some_and(|rows| !rows.is_empty()))
}

async fn clear_unclaimed_deadline_at(node: &EmbeddedNode, doc_id: &str) -> Result<()> {
    let escaped = escape_graphql_string(doc_id);
    let datetime_fields =
        agent_tool_call_datetime_update_fragment(node, doc_id, &["unclaimed_deadline_at"]).await?;
    let mutation = format!(
        r#"mutation {{
            update_AgentToolCall(
                filter: {{ _docID: {{ _eq: "{escaped}" }} }},
                input: {{ unclaimed_deadline_at: null{datetime_fields} }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    if response.has_errors() {
        anyhow::bail!("clear unclaimed_deadline_at failed: {:?}", response.errors);
    }
    Ok(())
}

async fn load_child_ack_probe(
    node: &EmbeddedNode,
    child_request_id: &str,
) -> Result<Option<ChildAckProbeRow>> {
    let escaped = escape_graphql_string(child_request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped}" }} }},
                limit: 1
            ) {{
                status
                lifecycle_state
                interrupt_requested_at
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!("child ack probe failed: {:?}", response.errors);
    }
    let rows: Vec<ChildAckProbeRow> = response
        .data
        .as_ref()
        .and_then(|d| d.get("AgentRequest"))
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();
    Ok(rows.into_iter().next())
}

fn request_terminal_or_interrupted(row: &ChildAckProbeRow) -> bool {
    matches!(
        row.lifecycle_state.as_deref(),
        Some("completed" | "failed" | "dead" | "interrupted" | "superseded")
    ) || matches!(
        row.status.as_deref(),
        Some("completed" | "error" | "dead" | "interrupted" | "superseded")
    ) || row.interrupt_requested_at.is_some()
}

async fn request_is_locally_owned(
    node: &EmbeddedNode,
    request_id: &str,
    local_did: &str,
) -> Result<bool> {
    let escaped_request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                limit: 1
            ) {{ agent_did }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query parent AgentRequest owner {request_id} failed: {:?}",
            response.errors
        );
    }
    Ok(response
        .data
        .as_ref()
        .and_then(|d| d.get("AgentRequest"))
        .and_then(|v| v.as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("agent_did"))
        .and_then(|v| v.as_str())
        == Some(local_did))
}

async fn clear_cancel_pending_ack(node: &EmbeddedNode, doc_id: &str) -> Result<()> {
    let escaped = escape_graphql_string(doc_id);
    let datetime_fields =
        agent_tool_call_datetime_update_fragment(node, doc_id, &["stuck_since"]).await?;
    let mutation = format!(
        r#"mutation {{
            update_AgentToolCall(
                filter: {{ _docID: {{ _eq: "{escaped}" }} }},
                input: {{
                    cancel_pending_remote_ack: false,
                    stuck_since: null
                    {datetime_fields}
                }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    if response.has_errors() {
        anyhow::bail!(
            "clear cancel_pending_remote_ack failed: {:?}",
            response.errors
        );
    }
    Ok(())
}

async fn set_stuck_since(node: &EmbeddedNode, doc_id: &str, when: DateTime<Utc>) -> Result<()> {
    let escaped = escape_graphql_string(doc_id);
    let when = escape_graphql_string(&when.to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
    let datetime_fields =
        agent_tool_call_datetime_update_fragment(node, doc_id, &["stuck_since"]).await?;
    let mutation = format!(
        r#"mutation {{
            update_AgentToolCall(
                filter: {{ _docID: {{ _eq: "{escaped}" }} }},
                input: {{ stuck_since: "{when}"{datetime_fields} }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    if response.has_errors() {
        anyhow::bail!("set stuck_since failed: {:?}", response.errors);
    }
    Ok(())
}

async fn agent_tool_call_datetime_update_fragment(
    node: &EmbeddedNode,
    doc_id: &str,
    omit: &[&str],
) -> Result<String> {
    let escaped = escape_graphql_string(doc_id);
    let query = format!(
        r#"{{
            AgentToolCall(filter: {{ _docID: {{ _eq: "{escaped}" }} }}, limit: 1) {{
                started_at
                deadline_at
                completed_at
                unclaimed_deadline_at
                cancel_cascade_intent_at
                stuck_since
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query AgentToolCall DateTime fields failed: {:?}",
            response.errors
        );
    }
    let row = response
        .data
        .as_ref()
        .and_then(|d| d.get("AgentToolCall"))
        .and_then(|v| serde_json::from_value::<Vec<AgentToolCallDateTimeRow>>(v.clone()).ok())
        .and_then(|mut rows| rows.pop())
        .unwrap_or_default();

    let mut fields = Vec::new();
    push_datetime_field(&mut fields, omit, "started_at", row.started_at.as_deref());
    push_datetime_field(&mut fields, omit, "deadline_at", row.deadline_at.as_deref());
    push_datetime_field(
        &mut fields,
        omit,
        "completed_at",
        row.completed_at.as_deref(),
    );
    push_datetime_field(
        &mut fields,
        omit,
        "unclaimed_deadline_at",
        row.unclaimed_deadline_at.as_deref(),
    );
    push_datetime_field(
        &mut fields,
        omit,
        "cancel_cascade_intent_at",
        row.cancel_cascade_intent_at.as_deref(),
    );
    push_datetime_field(&mut fields, omit, "stuck_since", row.stuck_since.as_deref());

    if fields.is_empty() {
        Ok(String::new())
    } else {
        Ok(format!(", {}", fields.join(", ")))
    }
}

pub(crate) fn push_datetime_field(
    fields: &mut Vec<String>,
    omit: &[&str],
    field: &'static str,
    value: Option<&str>,
) {
    if omit.contains(&field) {
        return;
    }
    let Some(value) = value.filter(|value| !value.trim().is_empty()) else {
        return;
    };
    let value = DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(&Utc))
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|_| value.to_string());
    fields.push(format!(r#"{field}: "{}""#, escape_graphql_string(&value)));
}

pub async fn project_background_subagent_completion(
    node: Arc<EmbeddedNode>,
    child_request_id: &str,
    local_did: &str,
) -> Result<BackgroundCompletionOutcome> {
    project_background_subagent_completion_inner(
        node.as_ref(),
        Some(node.clone()),
        child_request_id,
        local_did,
    )
    .await
}

pub(crate) async fn ensure_background_subagent_completion_side_effects(
    node: &EmbeddedNode,
    child_request_id: &str,
    local_did: &str,
) -> Result<BackgroundCompletionOutcome> {
    project_background_subagent_completion_inner(node, None, child_request_id, local_did).await
}

async fn project_background_subagent_completion_inner(
    node: &EmbeddedNode,
    lifecycle_node: Option<Arc<EmbeddedNode>>,
    child_request_id: &str,
    local_did: &str,
) -> Result<BackgroundCompletionOutcome> {
    let Some(linkage) = load_child_linkage(node, child_request_id).await? else {
        return Ok(BackgroundCompletionOutcome::Unlinked);
    };
    let Some(parent_request_id) = non_empty(linkage.caused_by_parent_request_id.as_deref()) else {
        return Ok(BackgroundCompletionOutcome::Unlinked);
    };
    if non_empty(linkage.caused_by_parent_tool_call_id.as_deref()).is_none() {
        return Ok(BackgroundCompletionOutcome::Unlinked);
    }
    if !request_is_locally_owned(node, parent_request_id, local_did).await? {
        return Ok(BackgroundCompletionOutcome::NotLocalOwner);
    }

    let Some(terminal_row) = load_child_terminal_row(node, child_request_id).await? else {
        return Ok(BackgroundCompletionOutcome::Unlinked);
    };
    let completed = child_request_completed(&terminal_row);
    let terminal = if completed {
        None
    } else {
        let Some(terminal) = project_child_terminal(&terminal_row) else {
            return Ok(BackgroundCompletionOutcome::NotTerminal);
        };
        Some(terminal)
    };

    let parent_context = load_parent_subagent_context(node, parent_request_id).await?;
    let edge = load_authorized_child_edge(node, &parent_context, child_request_id).await?;
    if edge.await_mode != AwaitMode::Background {
        return Ok(BackgroundCompletionOutcome::NotBackground);
    }

    let (status, summary, bridge_result, terminal) = if completed {
        let Some(final_response) =
            load_projected_final_response(node, &parent_context.session_id, &edge).await?
        else {
            return Ok(BackgroundCompletionOutcome::MissingFinalResponse);
        };
        let summary = compact_summary(&final_response);
        ("completed".to_string(), summary, Some(final_response), None)
    } else {
        let terminal = terminal.expect("non-completed child terminal checked above");
        let status = child_terminal_status(&terminal).to_string();
        let summary = child_terminal_summary(&terminal);
        (status, summary, None, Some(terminal))
    };

    let mut transitioned = false;
    if edge.lifecycle_state == "running" {
        let Some(lifecycle_node) = lifecycle_node else {
            return Ok(BackgroundCompletionOutcome::NotTerminal);
        };
        let mut lifecycle = match ToolCallLifecycle::load(
            lifecycle_node,
            &parent_context.session_id,
            &edge.parent_tool_call_id,
        )
        .await?
        {
            Some(lifecycle) => lifecycle,
            None => return Ok(BackgroundCompletionOutcome::Unlinked),
        };

        transitioned = match (bridge_result.clone(), terminal.clone()) {
            (Some(final_response), None) => lifecycle.bridge_complete(final_response).await?,
            (None, Some(terminal)) => lifecycle.bridge_failure(terminal).await?,
            _ => false,
        };
    } else if !bridge_state_is_terminal(&edge.lifecycle_state) {
        return Ok(BackgroundCompletionOutcome::AlreadyProjected);
    }

    let side_effects = ensure_projection_side_effects(
        node,
        &parent_context.session_id,
        &parent_context.request_id,
        &edge,
        &status,
        &summary,
    )
    .await?;

    let outcome = if transitioned || side_effects.created_notification || side_effects.created_wake
    {
        BackgroundCompletionOutcome::Projected {
            child_request_id: edge.child_request_id,
            parent_request_id: parent_context.request_id,
            parent_tool_call_id: edge.parent_tool_call_id,
            parent_session_id: parent_context.session_id,
            notification_sequence: side_effects.notification_sequence,
            wake_request_id: side_effects.wake_request_id,
        }
    } else {
        BackgroundCompletionOutcome::AlreadyProjected
    };
    Ok(outcome)
}

async fn load_projected_final_response(
    node: &EmbeddedNode,
    parent_session_id: &str,
    edge: &ChildEdge,
) -> Result<Option<String>> {
    if let Some(final_response) = load_child_final_response(node, edge).await? {
        return Ok(Some(final_response));
    }
    if edge.lifecycle_state == "completed" {
        return match session::load_tool_call_result(
            node,
            parent_session_id,
            &edge.parent_tool_call_id,
        )
        .await
        {
            Ok(result) if !result.trim().is_empty() => Ok(Some(result)),
            Ok(_) => Ok(None),
            Err(error) => Err(error),
        };
    }
    Ok(None)
}

struct SideEffects {
    notification_sequence: u32,
    wake_request_id: String,
    created_notification: bool,
    created_wake: bool,
}

pub(crate) async fn append_background_tool_completion(
    node: &EmbeddedNode,
    parent_session_id: &str,
    parent_request_id: &str,
    tool_call_id: &str,
    tool_name: &str,
    status: &str,
    result: &str,
    reason: Option<&str>,
) -> Result<()> {
    // Load the parent request up front so the completion notification is stamped
    // with the parent session's owning agent_did.
    let parent_request = load_agent_request_for_queue(node, parent_request_id)
        .await?
        .ok_or_else(|| anyhow!("parent AgentRequest {parent_request_id} not found"))?;

    let (notification_timestamp, created_notification) =
        match existing_tool_completion_notification(node, parent_session_id, tool_call_id).await? {
            Some(existing) => (existing.timestamp, false),
            None => {
                let notification =
                    render_tool_completion(tool_call_id, tool_name, status, result, reason);
                let notification_request_id =
                    background_completion_notification_request_id(tool_call_id);
                let notification_message_key = format!("{notification_request_id}:tool");
                let (sequence, created) = session::append_message_once_with_key_and_requester_did(
                    node,
                    parent_session_id,
                    &parent_request.agent_did,
                    parent_request.requester_did.as_deref(),
                    "user",
                    &notification,
                    None,
                    Some(&notification_request_id),
                    &notification_message_key,
                    None,
                )
                .await?;
                let timestamp = load_message_timestamp(node, parent_session_id, sequence).await?;
                (timestamp, created)
            }
        };

    let queue_key = format!("background_completion:{parent_session_id}");
    if existing_wakeup_after(node, parent_session_id, &queue_key, &notification_timestamp)
        .await?
        .is_some()
    {
        mark_background_tool_notification_delivered(
            node,
            &parent_request.agent_did,
            parent_request_id,
            tool_call_id,
        )
        .await?;
        mark_background_tool_completion_side_effects_done(node, parent_session_id, tool_call_id)
            .await?;
        return Ok(());
    }

    let _wake = enqueue_session_request(
        node,
        &parent_request,
        BACKGROUND_COMPLETION_WAKE_PROMPT,
        ExecutionOrigin::Scheduled,
        QueueHints {
            source: QueueSource::BackgroundCompletion,
            policy: QueuePolicy::Coalesce,
            key: Some(queue_key),
            queued_after_request_id: Some(parent_request_id.to_string()),
            interrupted_request_id: None,
        },
    )
    .await?;
    mark_background_tool_notification_delivered(
        node,
        &parent_request.agent_did,
        parent_request_id,
        tool_call_id,
    )
    .await?;
    mark_background_tool_completion_side_effects_done(node, parent_session_id, tool_call_id)
        .await?;

    if created_notification {
        tracing::debug!(
            parent_session_id,
            parent_request_id,
            tool_call_id,
            "appended background tool completion notification"
        );
    }
    Ok(())
}

async fn mark_background_tool_notification_delivered(
    node: &EmbeddedNode,
    agent_did: &str,
    parent_request_id: &str,
    tool_call_id: &str,
) -> Result<()> {
    let agent_did = escape_graphql_string(agent_did);
    let parent_request_id = escape_graphql_string(parent_request_id);
    let tool_call_id = escape_graphql_string(tool_call_id);
    let delivered_at = escape_graphql_string(&Utc::now().to_rfc3339());
    let mutation = format!(
        r#"mutation {{
            update_AgentToolCall(
                filter: {{
                    agent_did: {{ _eq: "{agent_did}" }},
                    request_id: {{ _eq: "{parent_request_id}" }},
                    tool_call_id: {{ _eq: "{tool_call_id}" }},
                    completion_notification_delivered_at: {{ _eq: null }}
                }},
                input: {{
                    completion_notification_delivered_at: "{delivered_at}"
                }}
            ) {{ _docID }}
        }}"#
    );
    crate::session::execute_mutation_with_retry(
        node,
        &mutation,
        "mark_background_tool_notification_delivered",
    )
    .await?;
    Ok(())
}

async fn mark_background_tool_completion_side_effects_done(
    node: &EmbeddedNode,
    session_id: &str,
    tool_call_id: &str,
) -> Result<()> {
    let tool_call_key = escape_graphql_string(&format!("{session_id}:{tool_call_id}"));
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{ tool_call_key: {{ _eq: "{tool_call_key}" }} }},
                limit: 1
            ) {{ _docID status lifecycle_state }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query background completion tool row failed: {:?}",
            response.errors
        );
    }
    let row = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentToolCall"))
        .and_then(serde_json::Value::as_array)
        .and_then(|rows| rows.first())
        .ok_or_else(|| anyhow!("background completion tool row {tool_call_key} not found"))?;
    let doc_id = row
        .get("_docID")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow!("background completion tool row {tool_call_key} not found"))?;
    let status = row
        .get("status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if status == "completed" {
        return Ok(());
    }
    let lifecycle_state = row
        .get("lifecycle_state")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if !(status == "completionPending" || status.starts_with("completionPending:"))
        || !matches!(
            lifecycle_state,
            "completed" | "failed" | "timedOut" | "cancelled"
        )
    {
        anyhow::bail!(
            "background completion tool row {tool_call_key} is not awaiting terminal side effects"
        );
    }
    let escaped_doc_id = escape_graphql_string(doc_id);
    let escaped_status = escape_graphql_string(status);
    let datetime_fields = agent_tool_call_datetime_update_fragment(node, doc_id, &[]).await?;
    let mutation = format!(
        r#"mutation {{
            update_AgentToolCall(
                filter: {{
                    _docID: {{ _eq: "{escaped_doc_id}" }},
                    status: {{ _eq: "{escaped_status}" }}
                }},
                input: {{ status: "completed"{datetime_fields} }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    if response.has_errors() {
        anyhow::bail!(
            "mark background completion side effects done failed: {:?}",
            response.errors
        );
    }
    Ok(())
}

async fn ensure_projection_side_effects(
    node: &EmbeddedNode,
    parent_session_id: &str,
    parent_request_id: &str,
    edge: &ChildEdge,
    status: &str,
    summary: &str,
) -> Result<SideEffects> {
    // Load the parent request up front so the projection notification is stamped
    // with the parent session's owning agent_did.
    let parent_request = load_agent_request_for_queue(node, parent_request_id)
        .await?
        .ok_or_else(|| anyhow!("parent AgentRequest {parent_request_id} not found"))?;

    let (notification_sequence, notification_timestamp, created_notification) =
        match existing_notification(node, parent_session_id, &edge.child_request_id).await? {
            Some(existing) => (existing.sequence, existing.timestamp, false),
            None => {
                let notification = render_notification(edge, status, summary);
                let notification_request_id =
                    background_completion_notification_request_id(&edge.child_request_id);
                let notification_message_key = format!("{notification_request_id}:subagent");
                let (sequence, created) = session::append_message_once_with_key_and_requester_did(
                    node,
                    parent_session_id,
                    &parent_request.agent_did,
                    parent_request.requester_did.as_deref(),
                    "user",
                    &notification,
                    None,
                    Some(&notification_request_id),
                    &notification_message_key,
                    None,
                )
                .await?;
                let timestamp = load_message_timestamp(node, parent_session_id, sequence).await?;
                (sequence, timestamp, created)
            }
        };

    let queue_key = format!("background_completion:{parent_session_id}");
    if let Some(wake_request_id) =
        existing_wakeup_after(node, parent_session_id, &queue_key, &notification_timestamp).await?
    {
        return Ok(SideEffects {
            notification_sequence,
            wake_request_id,
            created_notification,
            created_wake: false,
        });
    }

    let wake = enqueue_session_request(
        node,
        &parent_request,
        BACKGROUND_COMPLETION_WAKE_PROMPT,
        ExecutionOrigin::Scheduled,
        QueueHints {
            source: QueueSource::BackgroundCompletion,
            policy: QueuePolicy::Coalesce,
            key: Some(queue_key),
            queued_after_request_id: Some(parent_request_id.to_string()),
            interrupted_request_id: None,
        },
    )
    .await?;

    Ok(SideEffects {
        notification_sequence,
        wake_request_id: wake.request_id,
        created_notification,
        created_wake: true,
    })
}

fn bridge_state_is_terminal(state: &str) -> bool {
    matches!(state, "completed" | "failed" | "timedOut" | "cancelled")
}

struct ExistingNotification {
    sequence: u32,
    timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct NotificationMessageRow {
    sequence: u32,
    content: String,
    timestamp: String,
}

async fn existing_notification(
    node: &EmbeddedNode,
    parent_session_id: &str,
    child_request_id: &str,
) -> Result<Option<ExistingNotification>> {
    let escaped_session_id = escape_graphql_string(parent_session_id);
    let query = format!(
        r#"{{
            AgentMessage(
                filter: {{ session_id: {{ _eq: "{escaped_session_id}" }} }},
                order: {{ sequence: ASC }}
            ) {{
                sequence
                content
                timestamp
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query parent AgentMessage notifications for session {parent_session_id} failed: {:?}",
            response.errors
        );
    }

    let marker = format!(
        r#"child_request_id="{}""#,
        xml_escape_attr(child_request_id)
    );
    let rows: Vec<NotificationMessageRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentMessage"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    for row in rows {
        if row.content.contains("<subagent-notification") && row.content.contains(&marker) {
            return Ok(Some(ExistingNotification {
                sequence: row.sequence,
                timestamp: parse_utc_timestamp(&row.timestamp, "AgentMessage.timestamp")?,
            }));
        }
    }

    Ok(None)
}

async fn existing_tool_completion_notification(
    node: &EmbeddedNode,
    parent_session_id: &str,
    tool_call_id: &str,
) -> Result<Option<ExistingNotification>> {
    let escaped_session_id = escape_graphql_string(parent_session_id);
    let query = format!(
        r#"{{
            AgentMessage(
                filter: {{ session_id: {{ _eq: "{escaped_session_id}" }} }},
                order: {{ sequence: ASC }}
            ) {{
                sequence
                content
                timestamp
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query AgentMessage for background tool completion session={parent_session_id} failed: {:?}",
            response.errors
        );
    }
    let rows: Vec<NotificationMessageRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentMessage"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    let needle = format!(
        r#"<tool-completion tool_call_id="{}""#,
        xml_escape_attr(tool_call_id)
    );
    for row in rows {
        if row.content.contains(&needle) {
            return Ok(Some(ExistingNotification {
                sequence: row.sequence,
                timestamp: parse_utc_timestamp(&row.timestamp, "AgentMessage.timestamp")?,
            }));
        }
    }
    Ok(None)
}

#[derive(Debug, Deserialize)]
struct MessageTimestampRow {
    timestamp: String,
}

async fn load_message_timestamp(
    node: &EmbeddedNode,
    parent_session_id: &str,
    sequence: u32,
) -> Result<DateTime<Utc>> {
    let escaped_session_id = escape_graphql_string(parent_session_id);
    let query = format!(
        r#"{{
            AgentMessage(
                filter: {{
                    session_id: {{ _eq: "{escaped_session_id}" }},
                    sequence: {{ _eq: {sequence} }}
                }},
                limit: 1
            ) {{ timestamp }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query AgentMessage timestamp session={parent_session_id} sequence={sequence} failed: {:?}",
            response.errors
        );
    }
    let row: MessageTimestampRow = first_row(response.data.as_ref(), "AgentMessage")
        .ok_or_else(|| anyhow!("AgentMessage session={parent_session_id} sequence={sequence} not found after append"))?;
    parse_utc_timestamp(&row.timestamp, "AgentMessage.timestamp")
}

#[derive(Debug, Deserialize)]
struct WakeupRow {
    request_id: String,
    metadata: Option<String>,
    created_at: String,
}

async fn existing_wakeup_after(
    node: &EmbeddedNode,
    parent_session_id: &str,
    queue_key: &str,
    notification_timestamp: &DateTime<Utc>,
) -> Result<Option<String>> {
    let escaped_session_id = escape_graphql_string(parent_session_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{
                    session_id: {{ _eq: "{escaped_session_id}" }},
                    execution_origin: {{ _eq: "scheduled" }}
                }},
                order: {{ created_at: ASC }}
            ) {{
                request_id
                metadata
                created_at
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query scheduled wake-ups for session {parent_session_id} failed: {:?}",
            response.errors
        );
    }

    let rows: Vec<WakeupRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    for row in rows {
        let matches_key = parse_queue_hints(row.metadata.as_deref()).is_some_and(|hints| {
            hints.source == QueueSource::BackgroundCompletion
                && hints.policy == QueuePolicy::Coalesce
                && hints.key.as_deref() == Some(queue_key)
        });
        if !matches_key {
            continue;
        }

        let created_at = parse_utc_timestamp(&row.created_at, "AgentRequest.created_at")?;
        if created_at >= *notification_timestamp {
            return Ok(Some(row.request_id));
        }
    }
    Ok(None)
}

fn parse_utc_timestamp(value: &str, field: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .map_err(|error| anyhow!("{field} is not RFC3339: {error}"))?
        .with_timezone(&Utc))
}

pub(crate) async fn run_background_completion_observer(
    node: Arc<EmbeddedNode>,
    local_did: String,
    background_executions: crate::hook::BackgroundExecutionRegistry,
    cancel: CancellationToken,
) -> Result<()> {
    let mut observer =
        BackgroundCompletionObserver::new(node, local_did, background_executions, cancel);
    observer.run().await
}

struct BackgroundCompletionObserver {
    node: Arc<EmbeddedNode>,
    local_did: String,
    background_executions: crate::hook::BackgroundExecutionRegistry,
    cancel: CancellationToken,
    subscription: events::Subscription,
    collection_id_to_name: HashMap<String, String>,
    processed_child_request_ids: HashSet<String>,
}

impl BackgroundCompletionObserver {
    fn new(
        node: Arc<EmbeddedNode>,
        local_did: String,
        background_executions: crate::hook::BackgroundExecutionRegistry,
        cancel: CancellationToken,
    ) -> Self {
        let subscription = node.subscribe(&[EventName::Update]);
        Self {
            node,
            local_did,
            background_executions,
            cancel,
            subscription,
            collection_id_to_name: HashMap::new(),
            processed_child_request_ids: HashSet::new(),
        }
    }

    async fn run(&mut self) -> Result<()> {
        self.project_ready_children().await?;
        self.run_reconcilers().await?;
        let mut reconciler_tick = tokio::time::interval(Duration::from_secs(5));
        loop {
            let message = tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return Ok(()),
                _ = reconciler_tick.tick() => {
                    self.project_ready_children().await?;
                    self.run_reconcilers().await?;
                    continue;
                }
                msg = self.subscription.recv() => {
                    match msg {
                        Some(message) => message,
                        None => anyhow::bail!("subagent completion subscription channel closed"),
                    }
                }
            };

            let dropped = self.subscription.check_and_reset_dropped();
            if dropped > 0 {
                tracing::warn!(
                    dropped,
                    "subagent completion observer dropped messages; scanning terminal children"
                );
                self.project_ready_children().await?;
                self.run_reconcilers().await?;
            }

            let Some(update) = message.as_update() else {
                continue;
            };
            let Some(collection_name) = self.resolve_collection_name(&update.collection_id).await
            else {
                continue;
            };
            if collection_name != AGENT_REQUEST_COLLECTION {
                continue;
            }

            let Some(child_request_id) =
                load_request_id_by_doc_id(self.node.as_ref(), &update.doc_id).await?
            else {
                continue;
            };
            self.project_child_if_needed(child_request_id).await;
        }
    }

    async fn project_ready_children(&mut self) -> Result<()> {
        for child_request_id in load_terminal_child_request_ids(self.node.as_ref()).await? {
            self.project_child_if_needed(child_request_id).await;
        }
        Ok(())
    }

    async fn run_reconcilers(&mut self) -> Result<()> {
        for run in crate::periodic_recovery::run_periodic_recovery_sweeps(
            self.node.as_ref(),
            &self.local_did,
            &self.background_executions,
        )
        .await?
        {
            if !run.is_noop() {
                tracing::debug!(
                    sweep_ids = ?run.metadata.sweep_ids,
                    rust_function = run.metadata.rust_function,
                    outcome = ?run.outcome,
                    "periodic recovery sweep applied"
                );
            }
        }
        let unclaimed =
            reconcile_unclaimed_cross_deployment_spawns(self.node.clone(), &self.local_did).await?;
        if !unclaimed.is_empty() {
            tracing::debug!(
                count = unclaimed.len(),
                "reconciled unclaimed subagent spawns"
            );
        }
        let cancel_ack = observe_cancel_cascade_ack(self.node.clone(), &self.local_did).await?;
        if !cancel_ack.is_empty() {
            tracing::debug!(
                count = cancel_ack.len(),
                "observed cross-deployment cancel acks"
            );
        }

        // Owner-scoped terminal-convergence re-drive (#664): re-assert the
        // terminal state of recently-terminalized own-requests so the terminal
        // delta reaches replicas that missed the one-shot PushLog.
        let redrive = crate::RequestLifecycle::redrive_terminal_convergence(
            self.node.as_ref(),
            &self.local_did,
        )
        .await?;
        if !redrive.is_noop() {
            tracing::debug!(
                reasserted = redrive.reasserted,
                scanned = redrive.scanned,
                "re-drove terminal request convergence to replicas"
            );
        }
        Ok(())
    }

    async fn project_child_if_needed(&mut self, child_request_id: String) {
        if self.processed_child_request_ids.contains(&child_request_id) {
            return;
        }

        match project_background_subagent_completion(
            self.node.clone(),
            &child_request_id,
            &self.local_did,
        )
        .await
        {
            Ok(BackgroundCompletionOutcome::Projected { .. })
            | Ok(BackgroundCompletionOutcome::AlreadyProjected)
            | Ok(BackgroundCompletionOutcome::NotLocalOwner) => {
                self.processed_child_request_ids.insert(child_request_id);
            }
            Ok(
                BackgroundCompletionOutcome::NotTerminal
                | BackgroundCompletionOutcome::NotBackground
                | BackgroundCompletionOutcome::MissingFinalResponse
                | BackgroundCompletionOutcome::Unlinked,
            ) => {}
            Err(error) => {
                tracing::warn!(
                    child_request_id = %child_request_id,
                    error = %error,
                    "failed to project background subagent completion"
                );
            }
        }
    }

    async fn resolve_collection_name(&mut self, collection_id: &str) -> Option<String> {
        if let Some(name) = self.collection_id_to_name.get(collection_id) {
            return Some(name.clone());
        }

        let names = match self.node.list_collections() {
            Ok(names) => names,
            Err(error) => {
                tracing::warn!(
                    collection_id = %collection_id,
                    %error,
                    "subagent completion observer failed to list collections"
                );
                return None;
            }
        };

        for name in names {
            let def = match self.node.get_collection(&name) {
                Ok(Some(def)) => def,
                Ok(None) => continue,
                Err(error) => {
                    tracing::warn!(
                        collection_name = %name,
                        %error,
                        "subagent completion observer failed to fetch collection definition",
                    );
                    continue;
                }
            };
            self.collection_id_to_name
                .insert(def.collection_id.clone(), def.name.clone());
        }

        self.collection_id_to_name.get(collection_id).cloned()
    }
}

#[derive(Debug, Deserialize)]
struct ChildLinkageRow {
    caused_by_parent_request_id: Option<String>,
    caused_by_parent_tool_call_id: Option<String>,
}

async fn load_child_linkage(
    node: &EmbeddedNode,
    child_request_id: &str,
) -> Result<Option<ChildLinkageRow>> {
    let escaped_child_request_id = escape_graphql_string(child_request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_child_request_id}" }} }},
                limit: 1
            ) {{
                caused_by_parent_request_id
                caused_by_parent_tool_call_id
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query child AgentRequest linkage {child_request_id} failed: {:?}",
            response.errors
        );
    }
    Ok(first_row(response.data.as_ref(), "AgentRequest"))
}

#[derive(Debug, Deserialize)]
struct RequestIdRow {
    request_id: String,
}

async fn load_request_id_by_doc_id(node: &EmbeddedNode, doc_id: &str) -> Result<Option<String>> {
    let escaped_doc_id = escape_graphql_string(doc_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ _docID: {{ _eq: "{escaped_doc_id}" }} }},
                limit: 1
            ) {{ request_id }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query AgentRequest doc {doc_id} failed: {:?}",
            response.errors
        );
    }
    Ok(first_row::<RequestIdRow>(response.data.as_ref(), "AgentRequest").map(|row| row.request_id))
}

#[derive(Debug, Deserialize)]
struct TerminalChildRow {
    request_id: String,
    caused_by_parent_request_id: Option<String>,
    caused_by_parent_tool_call_id: Option<String>,
}

async fn load_terminal_child_request_ids(node: &EmbeddedNode) -> Result<Vec<String>> {
    let query = r#"{
        AgentRequest(
            filter: {
                lifecycle_state: { _in: ["completed", "failed", "dead", "interrupted", "superseded"] }
            }
        ) {
            request_id
            caused_by_parent_request_id
            caused_by_parent_tool_call_id
        }
    }"#;
    let response = node.execute(query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query terminal child AgentRequests failed: {:?}",
            response.errors
        );
    }

    let rows: Vec<TerminalChildRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    Ok(rows
        .into_iter()
        .filter(|row| {
            non_empty(row.caused_by_parent_request_id.as_deref()).is_some()
                && non_empty(row.caused_by_parent_tool_call_id.as_deref()).is_some()
        })
        .map(|row| row.request_id)
        .collect())
}

#[derive(Debug, Deserialize)]
struct AgentRequestQueueRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    request_id: String,
    agent_did: String,
    requester_did: Option<String>,
    behavior_id: Option<String>,
    session_id: String,
    content: String,
    temperature: Option<f64>,
    top_p: Option<f64>,
    top_k: Option<i64>,
    seed: Option<i64>,
    max_tokens: Option<i64>,
    metadata: Option<String>,
    execution_origin: Option<String>,
    created_at: String,
    deadline: Option<String>,
    subagent_depth: Option<u32>,
    caused_by_parent_request_id: Option<String>,
    caused_by_parent_tool_call_id: Option<String>,
}

async fn load_agent_request_for_queue(
    node: &EmbeddedNode,
    request_id: &str,
) -> Result<Option<AgentRequest>> {
    let escaped_request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                limit: 1
            ) {{
                _docID
                request_id
                agent_did
                requester_did
                behavior_id
                session_id
                content
                temperature
                top_p
                top_k
                seed
                max_tokens
                metadata
                execution_origin
                created_at
                deadline
                subagent_depth
                caused_by_parent_request_id
                caused_by_parent_tool_call_id
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query parent AgentRequest {request_id} for wake-up failed: {:?}",
            response.errors
        );
    }
    let Some(row) = first_row::<AgentRequestQueueRow>(response.data.as_ref(), "AgentRequest")
    else {
        return Ok(None);
    };

    let request = AgentRequest {
        doc_id: row.doc_id,
        request_id: row.request_id,
        agent_did: row.agent_did,
        requester_did: normalize_optional_string(row.requester_did),
        behavior_id: normalize_optional_string(row.behavior_id),
        session_id: row.session_id,
        content: row.content,
        temperature: row.temperature,
        top_p: row.top_p,
        top_k: row.top_k,
        seed: row.seed,
        max_tokens: row.max_tokens,
        metadata: row.metadata,
        execution_origin: normalize_optional_string(row.execution_origin),
        created_at: row.created_at,
        deadline: normalize_optional_string(row.deadline),
        subagent_depth: row.subagent_depth.unwrap_or(0),
        caused_by_parent_request_id: normalize_optional_string(row.caused_by_parent_request_id),
        caused_by_parent_tool_call_id: normalize_optional_string(row.caused_by_parent_tool_call_id),
    };
    validate_agent_request(&request)?;
    Ok(Some(request))
}

fn render_notification(edge: &ChildEdge, status: &str, summary: &str) -> String {
    format!(
        r#"<subagent-notification child_request_id="{child_request_id}" child_session_id="{child_session_id}" behavior_id="{behavior_id}" parent_tool_call_id="{parent_tool_call_id}" status="{status}">
<summary>{summary}</summary>
</subagent-notification>"#,
        child_request_id = xml_escape_attr(&edge.child_request_id),
        child_session_id = xml_escape_attr(&edge.child_session_id),
        behavior_id = xml_escape_attr(&edge.behavior_id),
        parent_tool_call_id = xml_escape_attr(&edge.parent_tool_call_id),
        status = xml_escape_attr(status),
        summary = xml_escape_text(summary),
    )
}

fn render_tool_completion(
    tool_call_id: &str,
    tool_name: &str,
    status: &str,
    result: &str,
    reason: Option<&str>,
) -> String {
    let reason_element = reason
        .map(|reason| format!("\n  <reason>{}</reason>", xml_escape_text(reason)))
        .unwrap_or_default();
    format!(
        r#"<tool-completion tool_call_id="{tool_call_id}" tool_name="{tool_name}" status="{status}">
  <result>{result}</result>{reason_element}
</tool-completion>"#,
        tool_call_id = xml_escape_attr(tool_call_id),
        tool_name = xml_escape_attr(tool_name),
        status = xml_escape_attr(status),
        result = xml_escape_text(&compact_summary(result)),
        reason_element = reason_element,
    )
}

fn child_terminal_status(terminal: &ChildTerminal) -> &'static str {
    match terminal {
        ChildTerminal::Failed { .. } => "failed",
        ChildTerminal::Dead => "dead",
        ChildTerminal::Interrupted => "interrupted",
        ChildTerminal::Superseded => "superseded",
    }
}

fn child_terminal_summary(terminal: &ChildTerminal) -> String {
    match terminal {
        ChildTerminal::Failed { reason, .. } => compact_summary(reason),
        ChildTerminal::Dead => "child request reached the dead terminal state".to_string(),
        ChildTerminal::Interrupted => "child request was interrupted".to_string(),
        ChildTerminal::Superseded => "child request was superseded".to_string(),
    }
}

fn compact_summary(value: &str) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    const LIMIT: usize = 4000;
    if normalized.len() <= LIMIT {
        return normalized;
    }

    let boundary = normalized
        .char_indices()
        .map(|(idx, _)| idx)
        .take_while(|idx| *idx <= LIMIT)
        .last()
        .unwrap_or(0);
    let mut truncated = normalized[..boundary].to_string();
    truncated.push_str("...");
    truncated
}

fn xml_escape_attr(value: &str) -> String {
    xml_escape_text(value)
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn xml_escape_text(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then_some(trimmed)
    })
}

fn first_row<T>(data: Option<&serde_json::Value>, collection: &str) -> Option<T>
where
    T: for<'de> Deserialize<'de>,
{
    data.and_then(|data| data.get(collection))
        .and_then(|value| serde_json::from_value::<Vec<T>>(value.clone()).ok())
        .and_then(|mut rows| rows.pop())
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOCAL_DID: &str = "did:test:local-owner";
    const FOREIGN_DID: &str = "did:test:foreign-owner";

    #[test]
    fn recognizes_only_reserved_background_completion_notification_ids() {
        let request_id = background_completion_notification_request_id("child-1");
        assert!(is_background_completion_notification_request_id(Some(
            &request_id
        )));
        assert!(!is_background_completion_notification_request_id(Some(
            BACKGROUND_COMPLETION_WAKE_PROMPT
        )));
        assert!(!is_background_completion_notification_request_id(None));
    }

    async fn test_node() -> Arc<EmbeddedNode> {
        let node = Arc::new(EmbeddedNode::builder().build().await.unwrap());
        crate::ensure_runtime_schemas(node.as_ref()).await.unwrap();
        node
    }

    async fn exec(node: &EmbeddedNode, statement: &str) {
        let response = node.execute(statement).await;
        assert!(
            !response.has_errors(),
            "GraphQL errors: {:?}",
            response.errors
        );
    }

    async fn write_parent_request(node: &EmbeddedNode, request_id: &str, agent_did: &str) {
        let mutation = format!(
            r#"mutation {{
                create_AgentRequest(input: {{
                    request_id: "{request_id}",
                    agent_did: "{agent_did}",
                    behavior_id: "parent",
                    session_id: "session-{request_id}",
                    content: "parent",
                    status: "processing",
                    lifecycle_state: "processing",
                    created_at: "2026-05-15T00:00:00Z",
                    deadline: "2026-05-15T00:05:00Z"
                }}) {{ _docID }}
            }}"#
        );
        exec(node, &mutation).await;
    }

    async fn write_bridge(
        node: &EmbeddedNode,
        request_id: &str,
        tool_call_id: &str,
        extra_fields: &str,
    ) {
        let mutation = format!(
            r#"mutation {{
                create_AgentToolCall(input: {{
                    tool_call_key: "{request_id}:{tool_call_id}",
                    request_id: "{request_id}",
                    session_id: "session-{request_id}",
                    message_sequence: 1,
                    tool_name: "spawn_subagent",
                    tool_call_id: "{tool_call_id}",
                    args: "{{}}",
                    status: "running",
                    lifecycle_state: "running",
                    started_at: "2026-05-15T00:00:00Z",
                    deadline_at: "2026-05-15T00:05:00Z",
                    await_mode: "background",
                    cancel_policy: "cascade",
                    child_request_id: "child-{tool_call_id}"
                    {extra_fields}
                }}) {{ _docID }}
            }}"#
        );
        exec(node, &mutation).await;
    }

    #[derive(Debug, Deserialize)]
    struct ToolRow {
        lifecycle_state: Option<String>,
        unclaimed_deadline_at: Option<String>,
        cancel_pending_remote_ack: Option<bool>,
        stuck_since: Option<String>,
    }

    async fn load_tool(node: &EmbeddedNode, tool_call_id: &str) -> ToolRow {
        let query = format!(
            r#"{{
                AgentToolCall(filter: {{ tool_call_id: {{ _eq: "{tool_call_id}" }} }}, limit: 1) {{
                    lifecycle_state
                    unclaimed_deadline_at
                    cancel_pending_remote_ack
                    stuck_since
                }}
            }}"#
        );
        let response = node.execute(&query).await;
        assert!(
            !response.has_errors(),
            "query errors: {:?}",
            response.errors
        );
        first_row(response.data.as_ref(), "AgentToolCall").expect("tool row")
    }

    #[tokio::test]
    async fn unclaimed_reconciler_skips_foreign_parent_bridge() {
        let node = test_node().await;
        write_parent_request(node.as_ref(), "parent-foreign-unclaimed", FOREIGN_DID).await;
        write_bridge(
            node.as_ref(),
            "parent-foreign-unclaimed",
            "foreign-unclaimed",
            r#", unclaimed_deadline_at: "2020-01-01T00:00:00Z""#,
        )
        .await;

        let outcomes = reconcile_unclaimed_cross_deployment_spawns(node.clone(), LOCAL_DID)
            .await
            .unwrap();
        assert!(outcomes.is_empty());

        let tool = load_tool(node.as_ref(), "foreign-unclaimed").await;
        assert_eq!(tool.lifecycle_state.as_deref(), Some("running"));
        assert!(tool.unclaimed_deadline_at.is_some());
    }

    #[tokio::test]
    async fn cancel_ack_observer_skips_foreign_parent_bridge() {
        let node = test_node().await;
        write_parent_request(node.as_ref(), "parent-foreign-cancel", FOREIGN_DID).await;
        write_bridge(
            node.as_ref(),
            "parent-foreign-cancel",
            "foreign-cancel",
            r#", cancel_cascade_intent_at: "2020-01-01T00:00:00Z", cancel_pending_remote_ack: true"#,
        )
        .await;

        let outcomes = observe_cancel_cascade_ack(node.clone(), LOCAL_DID)
            .await
            .unwrap();
        assert!(outcomes.is_empty());

        let tool = load_tool(node.as_ref(), "foreign-cancel").await;
        assert_eq!(tool.cancel_pending_remote_ack, Some(true));
        assert!(tool.stuck_since.is_none());
    }
}
