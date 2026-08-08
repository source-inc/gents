use anyhow::Result;
use defra_node::EmbeddedNode;
use serde::Deserialize;

#[derive(Debug, Clone, Default, Deserialize)]
pub(super) struct PersistedResponseState {
    #[serde(rename = "_docID")]
    pub doc_id: String,
    pub request_id: String,
    #[serde(default)]
    pub request_doc_id: Option<String>,
    #[serde(default)]
    pub request_source_composite_commit_cid: Option<String>,
    #[serde(default)]
    pub request_source_signer_did: Option<String>,
    #[serde(default)]
    pub request_claim_composite_commit_cid: Option<String>,
    #[serde(default)]
    pub request_claim_signer_did: Option<String>,
    #[serde(default)]
    pub agent_did: Option<String>,
    #[serde(default)]
    pub behavior_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    pub content: String,
    pub status: String,
    #[serde(default)]
    pub error_message: Option<String>,
    pub token_count: usize,
    #[serde(default)]
    pub interrupted_at: Option<String>,
    #[serde(default)]
    pub requester_did: Option<String>,
    #[serde(default)]
    pub final_message_doc_id: Option<String>,
    #[serde(default)]
    pub final_message_composite_commit_cid: Option<String>,
    #[serde(default)]
    pub final_message_signer_did: Option<String>,
    #[serde(default)]
    pub final_message_sequence: Option<u32>,
    #[serde(default)]
    pub outcome_terminalized_at: Option<String>,
}

pub(super) fn extract_mutation_doc_id<'a>(
    data: &'a serde_json::Value,
    collection_name: &str,
) -> Option<&'a str> {
    for field_name in [
        format!("upsert_{collection_name}"),
        format!("create_{collection_name}"),
        format!("add_{collection_name}"),
    ] {
        if let Some(value) = data.get(&field_name) {
            if let Some(doc_id) = value.get("_docID").and_then(|value| value.as_str()) {
                return Some(doc_id);
            }

            if let Some(doc_id) = value
                .as_array()
                .and_then(|rows| rows.first())
                .and_then(|row| row.get("_docID"))
                .and_then(|value| value.as_str())
            {
                return Some(doc_id);
            }
        }
    }

    None
}

pub(super) async fn load_response_state(
    node: &EmbeddedNode,
    doc_id: &str,
) -> Result<Option<PersistedResponseState>> {
    let escaped_doc_id = crate::graphql::escape_graphql_string(doc_id);
    let query = format!(
        r#"{{
            AgentResponse(
                filter: {{ _docID: {{ _eq: "{escaped_doc_id}" }} }},
                limit: 1
            ) {{
                _docID
                request_id
                request_doc_id
                request_source_composite_commit_cid
                request_source_signer_did
                request_claim_composite_commit_cid
                request_claim_signer_did
                agent_did
                requester_did
                behavior_id
                session_id
                content
                status
                error_message
                token_count
                interrupted_at
                final_message_doc_id
                final_message_composite_commit_cid
                final_message_signer_did
                final_message_sequence
                outcome_terminalized_at
            }}
        }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!(
            "loading AgentResponse state for doc_id={doc_id}: {:?}",
            resp.errors
        );
    }

    let mut rows: Vec<PersistedResponseState> = match resp
        .data
        .as_ref()
        .and_then(|data| data.get("AgentResponse"))
    {
        Some(value) => serde_json::from_value(value.clone())?,
        None => Vec::new(),
    };

    Ok(rows.pop())
}

pub(super) async fn load_response_state_by_key(
    node: &EmbeddedNode,
    response_key: &str,
) -> Result<Option<PersistedResponseState>> {
    let escaped_response_key = crate::graphql::escape_graphql_string(response_key);
    let query = format!(
        r#"{{
            AgentResponse(
                filter: {{ response_key: {{ _eq: "{escaped_response_key}" }} }},
                limit: 1
            ) {{
                _docID
                request_id
                request_doc_id
                request_source_composite_commit_cid
                request_source_signer_did
                request_claim_composite_commit_cid
                request_claim_signer_did
                agent_did
                requester_did
                behavior_id
                session_id
                content
                status
                error_message
                token_count
                interrupted_at
                final_message_doc_id
                final_message_composite_commit_cid
                final_message_signer_did
                final_message_sequence
                outcome_terminalized_at
            }}
        }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!(
            "loading AgentResponse state for response_key={response_key}: {:?}",
            resp.errors
        );
    }

    let mut rows: Vec<PersistedResponseState> = match resp
        .data
        .as_ref()
        .and_then(|data| data.get("AgentResponse"))
    {
        Some(value) => serde_json::from_value(value.clone())?,
        None => Vec::new(),
    };

    Ok(rows.pop())
}
