use crate::client::store::ClientStore;
use anyhow::Result;
use chrono::Utc;
use defra_node::EmbeddedNode;

use super::super::graphql::{escape_graphql_string, execute_mutation, normalize_required};

pub async fn rename_conversation(
    node: &EmbeddedNode,
    store: &ClientStore,
    agent_did: &str,
    requester_did: &str,
    session_id: &str,
    title: &str,
) -> Result<()> {
    let mutation =
        build_rename_conversation_mutation(store, agent_did, requester_did, session_id, title)?;
    execute_mutation(node, &mutation, "rename_conversation").await
}

fn build_rename_conversation_mutation(
    store: &ClientStore,
    agent_did: &str,
    requester_did: &str,
    session_id: &str,
    title: &str,
) -> Result<String> {
    let agent_did = normalize_required("agent_did", agent_did)?;
    let requester_did = normalize_required("requester_did", requester_did)?;
    let session_id = normalize_required("session_id", session_id)?;
    let title = normalize_required("title", title)?;
    store
        .conversations
        .iter()
        .find(|row| {
            row.session_id == session_id
                && row.agent_did.as_deref() == Some(agent_did)
                && row.requester_did.as_deref() == Some(requester_did)
        })
        .ok_or_else(|| anyhow::anyhow!("conversation {} not found", session_id))?;

    let now = Utc::now().to_rfc3339();
    let escaped_session_id = escape_graphql_string(session_id);
    let escaped_agent_did = escape_graphql_string(agent_did);
    let escaped_requester_did = escape_graphql_string(requester_did);
    let escaped_title = escape_graphql_string(title);
    let mutation = format!(
        r#"mutation {{
            update_AgentConversation(
                filter: {{
                    session_id: {{ _eq: "{escaped_session_id}" }},
                    agent_did: {{ _eq: "{escaped_agent_did}" }},
                    requester_did: {{ _eq: "{escaped_requester_did}" }}
                }},
                input: {{
                    title: "{escaped_title}",
                    title_source: "user",
                    updated_at: "{now}"
                }}
            ) {{ _docID }}
        }}"#
    );
    Ok(mutation)
}
