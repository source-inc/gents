//! Authoritative logical-to-physical AgentRequest binding lookup.
//!
//! `request_id` is a human-facing label; `_docID` is the provenance edge.
//! Every caller uses the same limit-two lookup so missing and ambiguous labels
//! cannot silently become half-bound writes.

use anyhow::Result;
use defra_node::EmbeddedNode;
use serde::Deserialize;

use crate::graphql::{escape_graphql_string, graphql_with_transaction_retry, rows};
use crate::watcher::{validate_agent_request, AgentRequest};

#[derive(Debug, Deserialize)]
struct RequestDocRow {
    #[serde(rename = "_docID")]
    doc_id: String,
}

#[derive(Debug, Deserialize)]
struct AgentRequestRow {
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
    max_total_tokens: Option<i64>,
    metadata: Option<String>,
    execution_origin: Option<String>,
    created_at: String,
    deadline: Option<String>,
    subagent_depth: Option<u32>,
    caused_by_parent_request_id: Option<String>,
    caused_by_parent_request_doc_id: Option<String>,
    caused_by_parent_tool_call_id: Option<String>,
    caused_by_parent_tool_call_doc_id: Option<String>,
    caused_by_trigger_id: Option<String>,
    caused_by_trigger_kind: Option<String>,
    caused_by_source_doc_id: Option<String>,
    caused_by_correlation: Option<String>,
    caused_by_trigger_context: Option<String>,
    #[serde(default)]
    workspace_id: Option<String>,
    #[serde(default)]
    workspace_authority: Option<String>,
    #[serde(default)]
    workspace_owner_deployment_id: Option<String>,
    #[serde(default)]
    workspace_seal_hash: Option<String>,
}

pub(crate) async fn resolve_request_doc_id(
    node: &EmbeddedNode,
    request_id: &str,
) -> Result<Option<String>> {
    let escaped_request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                limit: 2
            ) {{ _docID }}
        }}"#
    );
    let response =
        graphql_with_transaction_retry(node, &query, "resolve_agent_request_document_binding")
            .await?;
    let mut matches = rows::<RequestDocRow>(&response, "AgentRequest")?;
    match matches.len() {
        0 => Ok(None),
        1 => Ok(matches.pop().map(|row| row.doc_id)),
        count => anyhow::bail!(
            "AgentRequest request_id={request_id} is ambiguous across {count} documents"
        ),
    }
}

pub(crate) async fn require_request_doc_id(
    node: &EmbeddedNode,
    request_id: &str,
) -> Result<String> {
    resolve_request_doc_id(node, request_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("AgentRequest request_id={request_id} not found"))
}

pub(crate) async fn load_agent_request(
    node: &EmbeddedNode,
    request_id: &str,
) -> Result<Option<AgentRequest>> {
    let Some(request_doc_id) = resolve_request_doc_id(node, request_id).await? else {
        return Ok(None);
    };
    let query = format!(
        r#"{{
            AgentRequest(filter: {{ _docID: {{ _eq: "{}" }} }}, limit: 1) {{
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
                max_total_tokens
                metadata
                execution_origin
                created_at
                deadline
                subagent_depth
                caused_by_parent_request_id
                caused_by_parent_request_doc_id
                caused_by_parent_tool_call_id
                caused_by_parent_tool_call_doc_id
                caused_by_trigger_id
                caused_by_trigger_kind
                caused_by_source_doc_id
                caused_by_correlation
                caused_by_trigger_context
                workspace_id
                workspace_authority
                workspace_owner_deployment_id
                workspace_seal_hash
            }}
        }}"#,
        escape_graphql_string(&request_doc_id)
    );
    let response =
        graphql_with_transaction_retry(node, &query, "load_agent_request_by_document").await?;
    let mut matches = rows::<AgentRequestRow>(&response, "AgentRequest")?;
    let Some(row) = matches.pop() else {
        return Ok(None);
    };
    anyhow::ensure!(
        row.doc_id == request_doc_id && row.request_id == request_id,
        "AgentRequest {request_doc_id} changed logical request binding while loading {request_id}"
    );
    let request = AgentRequest {
        doc_id: row.doc_id,
        request_id: row.request_id,
        agent_did: row.agent_did,
        requester_did: nonempty(row.requester_did),
        behavior_id: nonempty(row.behavior_id),
        session_id: row.session_id,
        content: row.content,
        temperature: row.temperature,
        top_p: row.top_p,
        top_k: row.top_k,
        seed: row.seed,
        max_tokens: row.max_tokens,
        max_total_tokens: row.max_total_tokens,
        metadata: row.metadata,
        execution_origin: nonempty(row.execution_origin),
        created_at: row.created_at,
        deadline: nonempty(row.deadline),
        subagent_depth: row.subagent_depth.unwrap_or_default(),
        caused_by_parent_request_id: nonempty(row.caused_by_parent_request_id),
        caused_by_parent_request_doc_id: nonempty(row.caused_by_parent_request_doc_id),
        caused_by_parent_tool_call_id: nonempty(row.caused_by_parent_tool_call_id),
        caused_by_parent_tool_call_doc_id: nonempty(row.caused_by_parent_tool_call_doc_id),
        caused_by_trigger_id: nonempty(row.caused_by_trigger_id),
        caused_by_trigger_kind: nonempty(row.caused_by_trigger_kind),
        caused_by_source_doc_id: nonempty(row.caused_by_source_doc_id),
        caused_by_correlation: nonempty(row.caused_by_correlation),
        caused_by_trigger_context: nonempty(row.caused_by_trigger_context),
        workspace_id: nonempty(row.workspace_id),
        workspace_authority: nonempty(row.workspace_authority),
        workspace_owner_deployment_id: nonempty(row.workspace_owner_deployment_id),
        workspace_seal_hash: nonempty(row.workspace_seal_hash),
    };
    validate_agent_request(&request)?;
    Ok(Some(request))
}

fn nonempty(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}
