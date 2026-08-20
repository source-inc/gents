use super::query::load_session_document;
#[cfg(test)]
use super::query::load_session_document_optional;
use super::retry::{execute_mutation_with_retry, execute_query_timed, retry_operation};
use super::*;

#[cfg(test)]
pub(crate) async fn create_session_with_id(
    node: &EmbeddedNode,
    session_id: &str,
    agent_name: &str,
    agent_did: &str,
) -> Result<()> {
    create_session_with_behavior_id(node, session_id, agent_name, agent_did, agent_name).await
}

#[cfg(test)]
pub(crate) async fn create_session_with_behavior_id(
    node: &EmbeddedNode,
    session_id: &str,
    agent_name: &str,
    agent_did: &str,
    behavior_id: &str,
) -> Result<()> {
    create_session_with_behavior_id_and_requester_did(
        node,
        session_id,
        agent_name,
        agent_did,
        behavior_id,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
async fn create_session_with_behavior_id_and_requester_did(
    node: &EmbeddedNode,
    session_id: &str,
    agent_name: &str,
    agent_did: &str,
    behavior_id: &str,
    requester_did: Option<&str>,
) -> Result<()> {
    let escaped_session_id = escape_graphql_string(session_id);
    let escaped_agent_name = escape_graphql_string(agent_name);
    let escaped_agent_did = escape_graphql_string(agent_did);
    let requester_did_field = super::requester_did_create_field(requester_did);

    let created = retry_operation("create_session", || async {
        let now = chrono::Utc::now().to_rfc3339();
        let existing = load_session_document_optional(node, session_id).await?;
        let created = existing.is_none();
        let started = existing
            .as_ref()
            .map(|session| session.started.clone())
            .unwrap_or_else(|| now.clone());
        let resolved_behavior_id =
            resolve_behavior_id(existing.as_ref(), behavior_id, "AgentSession")?;
        let escaped_started = escape_graphql_string(&started);
        let escaped_behavior_id = escape_graphql_string(&resolved_behavior_id);

        let mutation = format!(
            r#"mutation {{
                upsert_AgentSession(
                    filter: {{ session_id: {{ _eq: "{escaped_session_id}" }} }},
                    add: {{
                        session_id: "{escaped_session_id}",
                        agent_name: "{escaped_agent_name}",
                        agent_did: "{escaped_agent_did}",
                        {requester_did_field}
                        behavior_id: "{escaped_behavior_id}",
                        started: "{escaped_started}",
                        status: "active"
                    }},
                    update: {{
                        agent_name: "{escaped_agent_name}",
                        behavior_id: "{escaped_behavior_id}",
                        started: "{escaped_started}",
                        status: "active"
                    }}
                ) {{ _docID }}
            }}"#
        );

        execute_mutation_with_retry(node, &mutation, "create_session").await?;
        Ok(created)
    })
    .await?;

    let log_message = if created {
        "session created"
    } else {
        "session ensured"
    };
    tracing::info!(
        session_id = %session_id,
        agent = %agent_name,
        behavior_id = %behavior_id,
        created,
        "{log_message}"
    );
    Ok(())
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn ensure_session_with_behavior_id_and_requester_did(
    node: &EmbeddedNode,
    session_id: &str,
    agent_name: &str,
    agent_did: &str,
    behavior_id: &str,
    requester_did: Option<&str>,
) -> Result<()> {
    create_session_with_behavior_id_and_requester_did(
        node,
        session_id,
        agent_name,
        agent_did,
        behavior_id,
        requester_did,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn request_session_projection_field(
    session_id: &str,
    agent_name: &str,
    agent_did: &str,
    behavior_id: &str,
    requester_did: Option<&str>,
    started: &str,
) -> String {
    let session_id = escape_graphql_string(session_id);
    let agent_name = escape_graphql_string(agent_name);
    let agent_did = escape_graphql_string(agent_did);
    let behavior_id = escape_graphql_string(behavior_id);
    let started = escape_graphql_string(started);
    let requester_did_field = super::requester_did_create_field(requester_did);

    format!(
        r#"project_session: upsert_AgentSession(
            filter: {{
                session_id: {{ _eq: "{session_id}" }}
            }},
            add: {{
                session_id: "{session_id}",
                agent_name: "{agent_name}",
                agent_did: "{agent_did}",
                {requester_did_field}
                behavior_id: "{behavior_id}",
                started: "{started}",
                status: "active"
            }},
            update: {{
                agent_name: "{agent_name}",
                status: "active"
            }}
        ) {{ _docID }}"#
    )
}

pub(crate) async fn max_sequence(node: &EmbeddedNode, session_id: &str) -> Result<u32> {
    let escaped_session_id = escape_graphql_string(session_id);
    let query = format!(
        r#"{{
            AgentMessage(
                filter: {{ session_id: {{ _eq: "{escaped_session_id}" }} }},
                order: {{ sequence: DESC }},
                limit: 1
            ) {{ sequence }}
        }}"#
    );

    let resp = execute_query_timed(node, &query, "max_sequence").await;
    if resp.has_errors() {
        anyhow::bail!(
            "loading max sequence for session_id={}: {:?}",
            session_id,
            resp.errors
        );
    }

    Ok(resp
        .data
        .as_ref()
        .and_then(|data| data.get("AgentMessage"))
        .and_then(|value| value.as_array())
        .and_then(|rows| rows.first())
        .and_then(|message| message.get("sequence"))
        .and_then(|value| value.as_u64())
        .unwrap_or(0) as u32)
}

pub async fn close_session(node: &EmbeddedNode, session_id: &str) -> Result<()> {
    retry_operation("close_session", || async {
        let session = load_session_document(node, session_id).await?;

        let now = chrono::Utc::now().to_rfc3339();
        let escaped_started = escape_graphql_string(&session.started);
        let mutation = format!(
            r#"mutation {{
                update_AgentSession(
                    filter: {{ _docID: {{ _eq: "{doc_id}" }} }},
                    input: {{
                        started: "{escaped_started}",
                        status: "completed",
                        ended: "{now}"
                    }}
                ) {{ _docID }}
            }}"#,
            doc_id = escape_graphql_string(&session.doc_id),
        );

        execute_mutation_with_retry(node, &mutation, "close_session").await?;
        Ok(())
    })
    .await?;

    tracing::info!(session_id = %session_id, "session closed");
    Ok(())
}

#[cfg(test)]
fn resolve_behavior_id(
    existing: Option<&super::rows::SessionDocument>,
    requested_behavior_id: &str,
    collection_name: &str,
) -> Result<String> {
    let existing_behavior_id =
        existing.and_then(|session| normalize_optional_string(session.behavior_id.as_deref()));
    let requested_behavior_id = normalize_optional_string(Some(requested_behavior_id));

    match (existing_behavior_id, requested_behavior_id) {
        (Some(existing), Some(requested)) if existing != requested => anyhow::bail!(
            "{collection_name} session behavior mismatch: existing={existing} requested={requested}"
        ),
        (Some(existing), _) => Ok(existing.to_string()),
        (None, Some(requested)) => Ok(requested.to_string()),
        (None, None) => Ok(String::new()),
    }
}

#[cfg(test)]
fn normalize_optional_string(value: Option<&str>) -> Option<&str> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then_some(trimmed)
    })
}
