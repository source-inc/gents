use super::rows::{ResponseTerminalRow, StatusRow};
use super::*;

pub(super) async fn lookup_response_status_by_request_id(
    node: &EmbeddedNode,
    agent_did: &str,
    request_id: &str,
) -> Result<Option<String>> {
    if request_id.is_empty() {
        return Ok(None);
    }

    let escaped_agent_did = escape_graphql_string(agent_did);
    let escaped_request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            AgentResponse(
                filter: {{
                    agent_did: {{ _eq: "{escaped_agent_did}" }},
                    request_id: {{ _eq: "{escaped_request_id}" }}
                }},
                order: {{ created_at: DESC }},
                limit: 1
            ) {{
                status
            }}
        }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!(
            "querying response status for request_id={request_id}: {:?}",
            resp.errors
        );
    }

    let rows: Vec<StatusRow> = resp
        .data
        .as_ref()
        .and_then(|d| d.get("AgentResponse"))
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();

    Ok(rows.into_iter().next().map(|r| r.status))
}

pub(super) async fn lookup_terminal_response_by_request_id(
    node: &EmbeddedNode,
    agent_did: &str,
    request_id: &str,
) -> Result<Option<ResponseTerminalRow>> {
    if request_id.is_empty() {
        return Ok(None);
    }

    let escaped_agent_did = escape_graphql_string(agent_did);
    let escaped_request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            AgentResponse(
                filter: {{
                    agent_did: {{ _eq: "{escaped_agent_did}" }},
                    request_id: {{ _eq: "{escaped_request_id}" }},
                    status: {{ _in: ["complete", "completed", "error"] }}
                }},
                order: {{ created_at: DESC }},
                limit: 1
            ) {{
                status
                error_message
                interrupted_at
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "querying terminal response for request_id={request_id}: {:?}",
            response.errors
        );
    }
    let rows: Vec<ResponseTerminalRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentResponse"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    Ok(rows.into_iter().next())
}
