//! Read-only inspection and runtime-owned cancellation of authorized children.

use std::collections::BTreeMap;
use std::sync::Arc;

use gents::descendant_graph::{
    resolve_session_descendant_graph, DescendantEdge, DescendantGraphAccess, DescendantQuery,
};
use gents_protocol::message::{AssistantContent, Message};

use super::*;

/// `sessions` comes exclusively from this connection's validated registry.
/// The caller identity matches normal shim submission: agent == requester.
pub(crate) async fn handle(
    node: Arc<EmbeddedNode>,
    principal: &str,
    sessions: &[String],
    method: &str,
    params: &Value,
    context_window: u64,
) -> Result<Value> {
    let edges = authorized_children(&node, principal, sessions).await?;
    if method == SUBAGENT_LIST_RUNNING_METHOD {
        let mut subagents = Vec::new();
        for (_, edge) in edges.values().filter(|(_, edge)| !edge.is_terminal()) {
            if let Some(mut snapshot) = snapshot(&node, edge, context_window).await? {
                if snapshot["status"] == "running" {
                    snapshot
                        .as_object_mut()
                        .expect("snapshot object")
                        .remove("status");
                    subagents.push(snapshot);
                }
            }
        }
        return Ok(json!({"subagents": subagents}));
    }
    let id = params["subagentId"]
        .as_str()
        .context("subagentId required")?;
    let Some((caller, edge)) = edges.get(id) else {
        return Ok(if method == SUBAGENT_GET_METHOD {
            subagent_get_not_found_result()
        } else {
            subagent_cancel_not_found_result(id)
        });
    };
    if method == SUBAGENT_CANCEL_METHOD {
        return match gents::cancel_session_subagent(
            node.clone(),
            caller,
            &edge.child_request_id,
            Some("cancelled from Grok TUI"),
        )
        .await?
        {
            gents::CancelSubagentOutcome::Cancelled(_) => Ok(json!({
                "subagentId": id, "cancelled": true, "outcome": {"kind": "cancelled"},
            })),
            gents::CancelSubagentOutcome::AlreadyTerminal(_) => {
                let snapshot = snapshot(&node, edge, context_window).await?;
                let status = snapshot
                    .as_ref()
                    .and_then(|value| value["status"].as_str())
                    .unwrap_or("completed");
                Ok(json!({"subagentId": id, "cancelled": false,
                    "outcome": {"kind": "already_finished", "status": status}}))
            }
            gents::CancelSubagentOutcome::Unavailable { .. } => {
                Ok(subagent_cancel_not_found_result(id))
            }
            gents::CancelSubagentOutcome::NotAuthorized => {
                anyhow::bail!("subagent is visible but this session cannot control it")
            }
        };
    }
    let block = params
        .get("block")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let timeout = params
        .get("timeoutMs")
        .and_then(Value::as_u64)
        .unwrap_or(30_000);
    let deadline = tokio::time::Instant::now()
        .checked_add(std::time::Duration::from_millis(timeout))
        .context("subagent timeout exceeds the clock range")?;
    loop {
        let current = snapshot(&node, edge, context_window).await?;
        let terminal = current.as_ref().is_none_or(|value| {
            !matches!(value["status"].as_str(), Some("running" | "initializing"))
        });
        if !block || terminal || tokio::time::Instant::now() >= deadline {
            return Ok(json!({"snapshot": current}));
        }
        tokio::time::sleep_until(
            (tokio::time::Instant::now() + std::time::Duration::from_millis(100)).min(deadline),
        )
        .await;
    }
}

pub(crate) async fn authorized_children(
    node: &EmbeddedNode,
    principal: &str,
    sessions: &[String],
) -> Result<BTreeMap<String, (String, DescendantEdge)>> {
    let mut children = BTreeMap::new();
    for session in sessions {
        let query = format!(
            r#"{{ AgentRequest(filter: {{
            session_id: {{ _eq: "{}" }}, agent_did: {{ _eq: "{}" }},
            requester_did: {{ _eq: "{}" }}
        }}, order: {{ created_at: DESC }}, limit: 1) {{ request_id }} }}"#,
            escape_graphql_string(session),
            escape_graphql_string(principal),
            escape_graphql_string(principal)
        );
        let response = node.execute(&query).await;
        ensure_no_errors(&response, "Grok subagent caller scope")?;
        let Some(caller) = response
            .data
            .as_ref()
            .and_then(|v| v.pointer("/AgentRequest/0/request_id"))
            .and_then(Value::as_str)
        else {
            continue;
        };
        let mut query = DescendantQuery::all(caller);
        loop {
            let page = resolve_session_descendant_graph(DescendantGraphAccess::Local(node), &query)
                .await?;
            for edge in page.edges {
                if !edge.readable() {
                    continue;
                }
                let Some(id) = edge
                    .child_session_id
                    .as_ref()
                    .filter(|id| !id.is_empty() && *id != session)
                else {
                    continue;
                };
                // A session ID must resolve uniquely: never choose one of
                // conflicting child identities by incidental query ordering.
                if let Some((_, previous)) = children.get(id) {
                    let previous: &DescendantEdge = previous;
                    anyhow::ensure!(
                        previous.child_request_id == edge.child_request_id,
                        "ambiguous subagent session identity"
                    );
                    if edge.controllable() && !previous.controllable() {
                        children.insert(id.clone(), (caller.to_owned(), edge));
                    }
                } else {
                    children.insert(id.clone(), (caller.to_owned(), edge));
                }
            }
            if !page.has_more {
                break;
            }
            anyhow::ensure!(
                page.next_cursor.is_some() && page.next_cursor != query.after,
                "descendant pagination did not advance"
            );
            query.after = page.next_cursor;
        }
    }
    Ok(children)
}

async fn snapshot(
    node: &EmbeddedNode,
    edge: &DescendantEdge,
    context_window: u64,
) -> Result<Option<Value>> {
    let id = escape_graphql_string(&edge.child_request_id);
    let response = node.execute(&format!(r#"{{ child: AgentRequest(filter: {{request_id: {{_eq: "{id}"}}}}, limit: 2) {{ {CHILD_REQUEST_FIELDS} }} }}"#)).await;
    ensure_no_errors(&response, "Grok subagent snapshot")?;
    let children = decode_rows::<ChildRequestRow>(&response, "child", "child snapshot");
    let [child] = children.as_slice() else {
        return Ok(None);
    };
    anyhow::ensure!(
        Some(&child.session_id) == edge.child_session_id.as_ref(),
        "child session changed"
    );
    let response = node
        .execute(&child_responses_query([edge.child_request_id.as_str()]))
        .await;
    ensure_no_errors(&response, "Grok subagent response")?;
    let responses = decode_response_rows(&response);
    let response = responses.first();
    let tool_response = node
        .execute(&child_tools_query([edge.child_request_id.as_str()]))
        .await;
    ensure_no_errors(&tool_response, "Grok subagent tools")?;
    let tools = decode_child_tool_rows(&tool_response);
    let tools = tools.iter().collect::<Vec<_>>();
    let progress = progress_update(
        child,
        response,
        &tools,
        &child.session_id,
        &edge.immediate_parent_session_id,
        context_window,
    );
    let started = child
        .created_at
        .as_deref()
        .and_then(|v| chrono::DateTime::parse_from_rfc3339(v).ok())
        .map(|v| v.timestamp_millis().max(0) as u64)
        .unwrap_or(0);
    let status = child
        .finish_status(response)
        .map(|s| s.wire_name())
        .unwrap_or_else(|| match child.lifecycle_state.as_deref() {
            Some("pending" | "claimed") => "initializing",
            _ => "running",
        });
    let duration = if child.is_terminal(response) {
        progress.duration_ms
    } else if started == 0 {
        0
    } else {
        (chrono::Utc::now().timestamp_millis().max(0) as u64).saturating_sub(started)
    };
    let mut value = json!({
        "subagentId": child.session_id, "parentSessionId": edge.immediate_parent_session_id,
        "childSessionId": child.session_id, "subagentType": child.behavior_id.as_deref().unwrap_or("general-purpose"),
        "description": spawn_description(None, child), "startedAtEpochMs": started, "durationMs": duration, "status": status,
    });
    let object = value.as_object_mut().expect("snapshot object");
    match status {
        "running" => {
            object.extend(json!({"turnCount": progress.turn_count, "toolCallCount": progress.tool_call_count,
                "tokensUsed": progress.tokens_used, "contextWindowTokens": progress.context_window_tokens,
                "contextUsagePct": progress.context_usage_pct, "toolsUsed": progress.tools_used,
                "errorCount": progress.error_count}).as_object().unwrap().clone());
        }
        "completed" => {
            object.insert("output".into(), json!(final_output(node, child).await?));
            object.insert("toolCalls".into(), json!(progress.tool_call_count));
            object.insert("turns".into(), json!(progress.turn_count));
        }
        "failed" => {
            object.insert(
                "failureError".into(),
                json!(child
                    .failure_reason
                    .as_deref()
                    .and_then(nonempty)
                    .or(response
                        .and_then(|v| v.error_message.as_deref())
                        .and_then(nonempty))
                    .unwrap_or("subagent failed")),
            );
        }
        "cancelled" => {
            if let Some(reason) = child.failure_reason.as_deref().and_then(nonempty) {
                object.insert("cancelReason".into(), json!(reason));
            }
        }
        _ => {}
    }
    Ok(Some(value))
}

async fn final_output(node: &EmbeddedNode, child: &ChildRequestRow) -> Result<String> {
    let id = escape_graphql_string(&child.request_id);
    let response = node.execute(&format!(r#"{{ AgentResponse(filter: {{request_id: {{_eq: "{id}"}}}}, order: {{created_at: DESC}}, limit: 1) {{materialized_message_sequence content}} }}"#)).await;
    ensure_no_errors(&response, "Grok child final response")?;
    let row = response
        .data
        .as_ref()
        .and_then(|v| v.pointer("/AgentResponse/0"));
    if let Some(sequence) = row.and_then(|v| v["materialized_message_sequence"].as_i64()) {
        let session = escape_graphql_string(&child.session_id);
        let response = node.execute(&format!(r#"{{ AgentMessage(filter: {{session_id: {{_eq: "{session}"}}, request_id: {{_eq: "{id}"}}, sequence: {{_eq: {sequence}}}, role: {{_eq: "assistant"}}}}, limit: 2) {{content}} }}"#)).await;
        ensure_no_errors(&response, "Grok child final message")?;
        if let Some(blob) = response
            .data
            .as_ref()
            .and_then(|v| v.pointer("/AgentMessage/0/content"))
            .and_then(Value::as_str)
        {
            if let Message::Assistant { content, .. } =
                gents_protocol::transcript::decode_persisted_message("assistant", blob)
            {
                return Ok(content
                    .into_iter()
                    .filter_map(|item| match item {
                        AssistantContent::Text(text) => Some(text.text),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join(""));
            }
        }
    }
    Ok(row
        .and_then(|v| v["content"].as_str())
        .unwrap_or_default()
        .to_owned())
}
