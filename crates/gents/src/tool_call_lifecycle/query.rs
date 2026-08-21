//! Read-only queries for tool-call lifecycle reconstruction.

use std::sync::Arc;

use anyhow::{anyhow, Result};
use defra_node::EmbeddedNode;
use serde::Deserialize;

use crate::graphql::escape_graphql_string;

use super::{
    AwaitMode, CancelCause, CancelPolicy, FailureClass, SelectedToolIdentity, ToolCallLifecycle,
    ToolCallState,
};

#[derive(Debug, Clone, Deserialize)]
struct ToolCallResultRow {
    result: String,
}

fn decode_selected_tool_identity(
    service_id: Option<String>,
    tool_name: Option<String>,
) -> Result<Option<SelectedToolIdentity>> {
    match (service_id, tool_name) {
        (None, None) => Ok(None),
        (Some(service_id), Some(tool_name))
            if !service_id.trim().is_empty() && !tool_name.trim().is_empty() =>
        {
            Ok(Some(SelectedToolIdentity {
                service_id,
                tool_name,
            }))
        }
        _ => anyhow::bail!(
            "AgentToolCall selected tool identity must contain both non-empty \
             selected_service_id and selected_tool_name"
        ),
    }
}

/// Load the persisted result string for a tool call identified by
/// `session_id` + `tool_call_id`. Returns an error if the row is absent.
pub async fn load_tool_call_result(
    node: &EmbeddedNode,
    session_id: &str,
    tool_call_id: &str,
) -> Result<String> {
    let escaped_session_id = escape_graphql_string(session_id);
    let escaped_tool_call_id = escape_graphql_string(tool_call_id);
    let tool_call_key = format!("{escaped_session_id}:{escaped_tool_call_id}");
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{
                    tool_call_key: {{ _eq: "{tool_call_key}" }}
                }},
                limit: 1
            ) {{
                result
            }}
        }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!(
            "loading tool call result for session_id={} tool_call_id={}: {:?}",
            session_id,
            tool_call_id,
            resp.errors
        );
    }

    let mut rows: Vec<ToolCallResultRow> = match resp
        .data
        .as_ref()
        .and_then(|data| data.get("AgentToolCall"))
    {
        Some(value) => serde_json::from_value(value.clone())?,
        None => Vec::new(),
    };

    rows.pop().map(|row| row.result).ok_or_else(|| {
        anyhow::anyhow!(
            "loading tool call result: no AgentToolCall for session_id={session_id} tool_call_id={tool_call_id}"
        )
    })
}

#[derive(Debug, Deserialize)]
struct ToolCallRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    request_doc_id: Option<String>,
    #[serde(default)]
    agent_did: Option<String>,
    #[serde(default)]
    requester_did: Option<String>,
    message_sequence: u32,
    tool_name: String,
    args: String,
    lifecycle_state: Option<String>,
    started_at: Option<String>,
    #[serde(default)]
    deadline_at: Option<String>,
    tool_failure_class: Option<String>,
    cancel_cause: Option<String>,
    selected_service_id: Option<String>,
    selected_tool_name: Option<String>,
    // v3 subagent fields — nullable for v2 rows that pre-date the schema migration.
    await_mode: Option<String>,
    cancel_policy: Option<String>,
    child_request_id: Option<String>,
    spawn_target_did: Option<String>,
    unclaimed_deadline_at: Option<String>,
}

impl ToolCallLifecycle {
    /// Load an existing AgentToolCall row by session_id and tool_call_id.
    /// Returns `None` if the row does not exist.
    pub async fn load(
        node: Arc<EmbeddedNode>,
        session_id: &str,
        tool_call_id: &str,
    ) -> Result<Option<Self>> {
        let escaped_session_id = escape_graphql_string(session_id);
        let escaped_tool_call_id = escape_graphql_string(tool_call_id);
        let query = format!(
            r#"{{
                AgentToolCall(
                    filter: {{
                        session_id: {{ _eq: "{escaped_session_id}" }},
                        tool_call_id: {{ _eq: "{escaped_tool_call_id}" }}
                    }},
                    limit: 1
                ) {{
                    _docID
                    request_id
                    request_doc_id
                    agent_did
                    requester_did
                    message_sequence
                    tool_name
                    args
                    lifecycle_state
                    started_at
                    deadline_at
                    tool_failure_class
                    cancel_cause
                    selected_service_id
                    selected_tool_name
                    await_mode
                    cancel_policy
                    child_request_id
                    spawn_target_did
                    unclaimed_deadline_at
                }}
            }}"#
        );

        let resp = node.execute(&query).await;
        if resp.has_errors() {
            return Err(anyhow!(
                "load AgentToolCall query failed: {:?}",
                resp.errors
            ));
        }

        let rows: Vec<ToolCallRow> = resp
            .data
            .as_ref()
            .and_then(|d| d.get("AgentToolCall"))
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        let row = match rows.into_iter().next() {
            Some(r) => r,
            None => return Ok(None),
        };

        let state = row
            .lifecycle_state
            .as_deref()
            .and_then(ToolCallState::from_persisted)
            .unwrap_or(ToolCallState::Running); // legacy rows pre-migration default to Running

        let started_at = row
            .started_at
            .as_deref()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc));

        let deadline_at = row
            .deadline_at
            .as_deref()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(chrono::Utc::now);

        let failure_class = row
            .tool_failure_class
            .as_deref()
            .and_then(FailureClass::from_persisted);

        let cancel_cause = row
            .cancel_cause
            .as_deref()
            .and_then(CancelCause::from_persisted);

        // v3 subagent fields. v2 rows (where these columns are null) fall back
        // to the same defaults that Self::new() uses, preserving backwards compat.
        let await_mode = row
            .await_mode
            .as_deref()
            .and_then(AwaitMode::from_persisted)
            .unwrap_or(AwaitMode::Foreground);

        let cancel_policy = row
            .cancel_policy
            .as_deref()
            .and_then(CancelPolicy::from_persisted)
            .unwrap_or(CancelPolicy::Cascade);

        let child_request_id = row.child_request_id.filter(|s| !s.is_empty());
        let spawn_target_did = row.spawn_target_did.filter(|s| !s.is_empty());
        let unclaimed_deadline_at = row
            .unclaimed_deadline_at
            .as_deref()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc));
        let selected_tool_identity =
            decode_selected_tool_identity(row.selected_service_id, row.selected_tool_name)?;

        Ok(Some(Self {
            node,
            request_id: row.request_id.unwrap_or_default(),
            request_doc_id: row.request_doc_id.filter(|value| !value.trim().is_empty()),
            session_id: session_id.to_string(),
            agent_did: row.agent_did.unwrap_or_default(),
            // Current recovery paths only update the existing immutable row,
            // but preserve its route key so a future create transition cannot
            // silently rehydrate the lifecycle as unrouted.
            requester_did: row.requester_did.filter(|value| !value.trim().is_empty()),
            tool_call_id: tool_call_id.to_string(),
            message_sequence: row.message_sequence,
            tool_name: row.tool_name,
            args: row.args,
            doc_id: Some(row.doc_id),
            deadline_at,
            state,
            started_at,
            failure_class,
            cancel_cause,
            selected_tool_identity,
            await_mode,
            cancel_policy,
            child_request_id,
            spawn_target_did,
            unclaimed_deadline_at,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_tool_identity_is_an_atomic_pair() {
        assert!(decode_selected_tool_identity(None, None)
            .expect("legacy native tool call")
            .is_none());

        let selected = decode_selected_tool_identity(
            Some("metrics-prod".to_string()),
            Some("query_metrics".to_string()),
        )
        .expect("complete identity")
        .expect("selected identity");
        assert_eq!(selected.service_id, "metrics-prod");
        assert_eq!(selected.tool_name, "query_metrics");

        for (service_id, tool_name) in [
            (Some("metrics-prod".to_string()), None),
            (None, Some("query_metrics".to_string())),
            (Some(String::new()), Some("query_metrics".to_string())),
            (Some("metrics-prod".to_string()), Some("  ".to_string())),
        ] {
            assert!(decode_selected_tool_identity(service_id, tool_name).is_err());
        }
    }

    #[tokio::test]
    async fn load_preserves_requester_route_and_selected_tool_identity() {
        let node = Arc::new(
            defra_node::EmbeddedNode::builder()
                .build()
                .await
                .expect("embedded node"),
        );
        crate::ensure_schemas(node.as_ref())
            .await
            .expect("runtime schemas");
        let mut lifecycle = ToolCallLifecycle::new(
            node.clone(),
            "request-routed".to_string(),
            "session-routed".to_string(),
            "did:test:host".to_string(),
            "tool-call-routed".to_string(),
            1,
            "test_tool".to_string(),
            "{}".to_string(),
            chrono::Utc::now() + chrono::Duration::minutes(5),
        )
        .with_requester_did(Some("did:test:coordinator".to_string()))
        .with_selected_tool_identity(Some((
            "metrics-prod".to_string(),
            "query_metrics".to_string(),
        )));
        lifecycle.start_running().await.expect("persist tool call");

        let loaded = ToolCallLifecycle::load(node.clone(), "session-routed", "tool-call-routed")
            .await
            .expect("load tool call")
            .expect("persisted tool call");

        assert_eq!(
            loaded.requester_did.as_deref(),
            Some("did:test:coordinator")
        );
        let selected = loaded.selected_tool_identity.expect("selected identity");
        assert_eq!(selected.service_id, "metrics-prod");
        assert_eq!(selected.tool_name, "query_metrics");
        node.shutdown().await;
    }
}
