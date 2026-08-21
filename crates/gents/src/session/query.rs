use super::rows::{ConversationDocument, SessionDocument};
use super::*;

pub(super) async fn load_session_document(
    node: &EmbeddedNode,
    session_id: &str,
) -> Result<SessionDocument> {
    load_session_document_optional(node, session_id)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "loading session for completion: no AgentSession for session_id={session_id}"
            )
        })
}

pub(crate) async fn require_session(node: &EmbeddedNode, session_id: &str) -> Result<()> {
    load_session_document(node, session_id).await.map(|_| ())
}

pub(super) async fn load_session_document_optional(
    node: &EmbeddedNode,
    session_id: &str,
) -> Result<Option<SessionDocument>> {
    let escaped_session_id = escape_graphql_string(session_id);
    let query = format!(
        r#"{{
            AgentSession(
                filter: {{
                    session_id: {{ _eq: "{escaped_session_id}" }}
                }},
                limit: 1
            ) {{
                _docID
                behavior_id
                started
            }}
        }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!(
            "loading session for completion session_id={}: {:?}",
            session_id,
            resp.errors
        );
    }

    let mut rows: Vec<SessionDocument> =
        match resp.data.as_ref().and_then(|data| data.get("AgentSession")) {
            Some(value) => serde_json::from_value(value.clone())?,
            None => Vec::new(),
        };

    Ok(rows.pop())
}

/// Whether any `AgentResponse` in this session is still streaming.
///
/// Backs the session-scope resolution of the modelled `safeToReduce` gate: a
/// live response means a turn is still being written into this session's
/// transcript, and compaction must not summarize a half-written turn. See
/// `boundary.compaction.safe-to-reduce-session-scope`.
pub(crate) async fn session_has_live_response(
    node: &EmbeddedNode,
    session_id: &str,
) -> Result<bool> {
    session_has_other_live_response(node, session_id, None).await
}

/// Whether this session has a streaming response other than the current
/// owned-loop request. At a completion-turn boundary the current response is
/// necessarily still marked `streaming`, but every message the loop is about
/// to compact has finished streaming and been yielded to persistence. A
/// different live response can still be half-written and closes the modelled
/// `safeToReduce` gate.
pub(crate) async fn session_has_other_live_response(
    node: &EmbeddedNode,
    session_id: &str,
    current_request_id: Option<&str>,
) -> Result<bool> {
    let escaped_session_id = escape_graphql_string(session_id);
    let query = format!(
        r#"{{
            AgentResponse(
                filter: {{
                    session_id: {{ _eq: "{escaped_session_id}" }},
                    status: {{ _eq: "streaming" }}
                }},
                limit: 2
            ) {{
                response_key
            }}
        }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!(
            "loading live responses for session_id={}: {:?}",
            session_id,
            resp.errors
        );
    }

    let rows = resp
        .data
        .as_ref()
        .and_then(|data| data.get("AgentResponse"))
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    Ok(rows.iter().any(|row| {
        let response_key = row.get("response_key").and_then(|value| value.as_str());
        match current_request_id {
            Some(current_request_id) => response_key != Some(current_request_id),
            None => true,
        }
    }))
}

pub(crate) async fn load_session_behavior_id(
    node: &EmbeddedNode,
    session_id: &str,
) -> Result<Option<String>> {
    Ok(load_session_document_optional(node, session_id)
        .await?
        .and_then(|session| {
            session.behavior_id.and_then(|behavior_id| {
                let trimmed = behavior_id.trim().to_string();
                (!trimmed.is_empty()).then_some(trimmed)
            })
        }))
}

pub(super) async fn load_conversation_document(
    node: &EmbeddedNode,
    session_id: &str,
) -> Result<Option<ConversationDocument>> {
    let escaped_session_id = escape_graphql_string(session_id);
    let query = format!(
        r#"{{
            AgentConversation(
                filter: {{
                    session_id: {{ _eq: "{escaped_session_id}" }}
                }},
                limit: 2
            ) {{
                title
                title_source
            }}
        }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!(
            "loading conversation documents for session_id={}: {:?}",
            session_id,
            resp.errors
        );
    }

    let rows: Vec<ConversationDocument> = match resp
        .data
        .as_ref()
        .and_then(|data| data.get("AgentConversation"))
    {
        Some(value) => serde_json::from_value(value.clone())?,
        None => Vec::new(),
    };

    if rows.len() > 1 {
        anyhow::bail!(
            "AgentConversation uniqueness violated for session_id={session_id}: {} documents",
            rows.len()
        );
    }
    Ok(rows.into_iter().next())
}

pub(super) async fn load_recent_conversation_titles(
    node: &EmbeddedNode,
    agent_did: &str,
    exclude_session_id: &str,
    limit: usize,
) -> Result<Vec<String>> {
    let escaped_agent_did = escape_graphql_string(agent_did);
    let escaped_session_id = escape_graphql_string(exclude_session_id);
    let query = format!(
        r#"{{
            AgentConversation(
                filter: {{
                    agent_did: {{ _eq: "{escaped_agent_did}" }},
                    session_id: {{ _ne: "{escaped_session_id}" }}
                }},
                order: {{ updated_at: DESC }},
                limit: {limit}
            ) {{
                title
                title_source
            }}
        }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!(
            "loading recent conversation titles for agent_did={}: {:?}",
            agent_did,
            resp.errors
        );
    }

    let rows: Vec<ConversationDocument> = match resp
        .data
        .as_ref()
        .and_then(|data| data.get("AgentConversation"))
    {
        Some(value) => serde_json::from_value(value.clone())?,
        None => Vec::new(),
    };

    Ok(rows
        .into_iter()
        .filter(|row| row.title_source.as_deref() != Some("placeholder"))
        .filter_map(|row| {
            let trimmed = row.title.trim();
            (!trimmed.is_empty()).then_some(trimmed.to_string())
        })
        .collect())
}
