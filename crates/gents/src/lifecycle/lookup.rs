use super::rows::StatusRow;
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

pub(super) async fn lookup_request_status_by_request_id(
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
            AgentRequest(
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
            "querying request status for request_id={request_id}: {:?}",
            resp.errors
        );
    }

    let rows: Vec<StatusRow> = resp
        .data
        .as_ref()
        .and_then(|d| d.get("AgentRequest"))
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();

    Ok(rows.into_iter().next().map(|r| r.status))
}
