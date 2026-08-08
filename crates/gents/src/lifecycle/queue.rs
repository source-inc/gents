#![allow(dead_code)]

use anyhow::{Context, Result};
use defra_node::EmbeddedNode;
use serde::Deserialize;

use crate::graphql::{escape_graphql_string, response_has_documents};
use crate::session;
use crate::watcher::AgentRequest;

use super::materialize::EnqueuedAgentRequest;
use super::{extract_single_doc_id, ExecutionOrigin, DEFAULT_REQUEST_MAX_RETRIES};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RequestQueueMetadata {
    pub queue: QueueHints,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background_completion_wake_version: Option<u32>,
}

const BACKGROUND_COMPLETION_WAKE_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct QueueHints {
    pub source: QueueSource,
    pub policy: QueuePolicy,
    pub key: Option<String>,
    pub queued_after_request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interrupted_request_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueSource {
    User,
    #[serde(alias = "subagent_completion")]
    BackgroundCompletion,
    Steering,
    Goal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum QueuePolicy {
    Append,
    Coalesce,
}

pub(crate) fn parse_queue_hints(metadata: Option<&str>) -> Option<QueueHints> {
    parse_queue_metadata(metadata).map(|metadata| metadata.queue)
}

fn parse_queue_metadata(metadata: Option<&str>) -> Option<RequestQueueMetadata> {
    let metadata = metadata?.trim();
    if metadata.is_empty() {
        return None;
    }
    if metadata.contains("\"subagent_completion\"") {
        tracing::warn!(
            "parsed deprecated queue source alias subagent_completion as background_completion"
        );
    }

    serde_json::from_str::<RequestQueueMetadata>(metadata).ok()
}

pub(crate) fn queue_metadata_json(hints: &QueueHints) -> String {
    let background_completion_wake_version =
        queue_hints_are_automated_wakeup(hints).then_some(BACKGROUND_COMPLETION_WAKE_VERSION);
    serde_json::to_string(&RequestQueueMetadata {
        queue: hints.clone(),
        background_completion_wake_version,
    })
    .expect("queue metadata serialization should not fail")
}

pub(crate) fn is_automated_wakeup(metadata: Option<&str>) -> bool {
    parse_queue_hints(metadata).is_some_and(|hints| queue_hints_are_automated_wakeup(&hints))
}

fn queue_hints_are_automated_wakeup(hints: &QueueHints) -> bool {
    matches!(hints.source, QueueSource::BackgroundCompletion)
        && hints.policy == QueuePolicy::Coalesce
        && hints
            .key
            .as_deref()
            .is_some_and(|key| !key.trim().is_empty())
}

pub(crate) fn is_deprecated_background_completion_wakeup(
    execution_origin: Option<&str>,
    metadata: Option<&str>,
) -> bool {
    if execution_origin != Some("scheduled") {
        return false;
    }
    parse_queue_metadata(metadata).is_some_and(|metadata| {
        queue_hints_are_automated_wakeup(&metadata.queue)
            && metadata.background_completion_wake_version
                != Some(BACKGROUND_COMPLETION_WAKE_VERSION)
    })
}

pub(crate) fn is_subagent_owned_queue(metadata: Option<&str>) -> bool {
    parse_queue_hints(metadata).is_some_and(|hints| {
        matches!(hints.source, QueueSource::Steering)
            || (matches!(hints.source, QueueSource::BackgroundCompletion)
                && hints.policy == QueuePolicy::Coalesce
                && hints
                    .key
                    .as_deref()
                    .is_some_and(|key| !key.trim().is_empty()))
    })
}

pub(crate) fn is_goal_queue(metadata: Option<&str>) -> bool {
    parse_queue_hints(metadata).is_some_and(|hints| matches!(hints.source, QueueSource::Goal))
}

#[derive(Debug, Deserialize)]
struct PendingQueueRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    request_id: Option<String>,
    session_id: Option<String>,
    execution_origin: Option<String>,
    metadata: Option<String>,
}

fn queue_source_and_key_match(metadata: Option<&str>, source: QueueSource, key: &str) -> bool {
    parse_queue_hints(metadata).is_some_and(|hints| {
        hints.source == source
            && hints.policy == QueuePolicy::Coalesce
            && hints
                .key
                .as_deref()
                .is_some_and(|candidate| candidate.trim() == key)
    })
}

fn coalesce_key(hints: &QueueHints) -> Option<&str> {
    if hints.policy != QueuePolicy::Coalesce {
        return None;
    }
    hints
        .key
        .as_deref()
        .map(str::trim)
        .filter(|key| !key.is_empty())
}

pub(crate) async fn enqueue_session_request(
    node: &EmbeddedNode,
    parent: &AgentRequest,
    content: &str,
    execution_origin: ExecutionOrigin,
    queue_hints: QueueHints,
) -> Result<EnqueuedAgentRequest> {
    let source_author_did = require_node_signer_did(node, "enqueue_session_request")?;
    let behavior_id = parent_behavior_id(node, parent).await?;

    if let Some(key) = coalesce_key(&queue_hints) {
        if let Some(existing) = reconcile_coalesced_pending_request(
            node,
            &parent.session_id,
            &parent.agent_did,
            queue_hints.source,
            key,
        )
        .await?
        {
            return Ok(existing);
        }
    }

    let request_id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    let metadata = queue_metadata_json(&queue_hints);
    let parent_linkage_fields = parent_linkage_graphql_fields(parent);

    let escaped_request_id = escape_graphql_string(&request_id);
    let escaped_agent_did = escape_graphql_string(&parent.agent_did);
    let escaped_source_author_did = escape_graphql_string(&source_author_did);
    let requester_did_field = session::requester_did_create_field(parent.requester_did.as_deref());
    let escaped_behavior_id = escape_graphql_string(&behavior_id);
    let escaped_session_id = escape_graphql_string(&parent.session_id);
    let escaped_content = escape_graphql_string(content);
    let escaped_metadata = escape_graphql_string(&metadata);
    let escaped_created_at = escape_graphql_string(&now);
    let execution_origin = execution_origin.as_str();
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{escaped_request_id}",
                agent_did: "{escaped_agent_did}",
                source_author_did: "{escaped_source_author_did}",
                {requester_did_field}
                behavior_id: "{escaped_behavior_id}",
                session_id: "{escaped_session_id}",
                retry_parent_request: "",
                retry_root_request: "{escaped_request_id}",
                superseded_by_request: "",
                content: "{escaped_content}",
                metadata: "{escaped_metadata}",
                status: "pending",
                lifecycle_state: "pending",
                backend_id: "",
                execution_origin: "{execution_origin}",
                failure_reason: "",
                created_at: "{escaped_created_at}",
                retry_count: 0,
                max_retries: {max_retries},
                subagent_depth: {subagent_depth}{parent_linkage_fields}
            }}) {{ _docID }}
        }}"#,
        max_retries = DEFAULT_REQUEST_MAX_RETRIES,
        subagent_depth = parent.subagent_depth,
    );

    let response =
        session::execute_mutation_with_retry(node, &mutation, "enqueue_session_request").await?;
    let created_doc_id = match extract_single_doc_id(&response, "create_AgentRequest") {
        Some(doc_id) => doc_id,
        None => lookup_request_doc_id(node, &request_id)
            .await
            .context("enqueue_session_request create returned no _docID")?,
    };
    let mut enqueued = EnqueuedAgentRequest {
        doc_id: created_doc_id,
        request_id: request_id.clone(),
        session_id: parent.session_id.clone(),
    };

    if let Some(key) = coalesce_key(&queue_hints) {
        enqueued = reconcile_coalesced_pending_request(
            node,
            &parent.session_id,
            &parent.agent_did,
            queue_hints.source,
            key,
        )
        .await?
        .unwrap_or(enqueued);
        if enqueued.request_id != request_id {
            return Ok(enqueued);
        }
    }

    if let Err(error) = session::upsert_conversation_from_request_with_identity_and_requester_did(
        node,
        &parent.session_id,
        &behavior_id,
        &parent.agent_did,
        &behavior_id,
        &enqueued.request_id,
        content,
        "pending",
        parent.requester_did.as_deref(),
    )
    .await
    {
        tracing::warn!(
            request_id = %request_id,
            session_id = %parent.session_id,
            error = %error,
            "failed to update conversation for queued session request"
        );
    }

    Ok(enqueued)
}

pub(crate) async fn enqueue_goal_continuation(
    node: &EmbeddedNode,
    parent: &AgentRequest,
    goal_id: &str,
    content: &str,
    continuation_sequence: i64,
    wrapup: bool,
) -> Result<EnqueuedAgentRequest> {
    use sha2::{Digest, Sha256};

    let source_author_did = require_node_signer_did(node, "enqueue_goal_continuation")?;
    let behavior_id = parent_behavior_id(node, parent).await?;
    let digest = Sha256::digest(format!("{goal_id}\0{}", parent.request_id).as_bytes());
    let request_id = format!(
        "goal-cont-{}",
        digest[..16]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    if let Some(doc_id) = lookup_request_doc_id_optional(node, &request_id).await? {
        return Ok(EnqueuedAgentRequest {
            doc_id,
            request_id,
            session_id: parent.session_id.clone(),
        });
    }

    let now = chrono::Utc::now().to_rfc3339();
    let queue_hints = QueueHints {
        source: QueueSource::Goal,
        policy: QueuePolicy::Coalesce,
        key: Some(format!("goal:{goal_id}:{}", parent.request_id)),
        queued_after_request_id: Some(parent.request_id.clone()),
        interrupted_request_id: None,
    };
    let metadata = serde_json::json!({
        "queue": queue_hints,
        "goal": {
            "goal_id": goal_id,
            "parent_request_id": parent.request_id,
            "continuation_sequence": continuation_sequence,
            "wrapup": wrapup,
        }
    })
    .to_string();

    let escaped_request_id = escape_graphql_string(&request_id);
    let escaped_agent_did = escape_graphql_string(&parent.agent_did);
    let escaped_source_author_did = escape_graphql_string(&source_author_did);
    let requester_did_field = session::requester_did_create_field(parent.requester_did.as_deref());
    let escaped_behavior_id = escape_graphql_string(&behavior_id);
    let escaped_session_id = escape_graphql_string(&parent.session_id);
    let escaped_content = escape_graphql_string(content);
    let escaped_metadata = escape_graphql_string(&metadata);
    let escaped_created_at = escape_graphql_string(&now);
    let escaped_goal_id = escape_graphql_string(goal_id);
    let escaped_parent_request_id = escape_graphql_string(&parent.request_id);
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{escaped_request_id}",
                agent_did: "{escaped_agent_did}",
                source_author_did: "{escaped_source_author_did}",
                {requester_did_field}
                behavior_id: "{escaped_behavior_id}",
                session_id: "{escaped_session_id}",
                retry_parent_request: "",
                retry_root_request: "{escaped_request_id}",
                superseded_by_request: "",
                content: "{escaped_content}",
                metadata: "{escaped_metadata}",
                status: "pending",
                lifecycle_state: "pending",
                backend_id: "",
                execution_origin: "scheduled",
                caused_by_trigger_id: "{escaped_goal_id}",
                caused_by_trigger_kind: "goal",
                caused_by_parent_request_id: "{escaped_parent_request_id}",
                failure_reason: "",
                created_at: "{escaped_created_at}",
                retry_count: 0,
                max_retries: {max_retries},
                subagent_depth: 0
            }}) {{ _docID }}
        }}"#,
        max_retries = DEFAULT_REQUEST_MAX_RETRIES,
    );
    let response =
        session::execute_mutation_with_retry(node, &mutation, "enqueue_goal_continuation").await?;
    let doc_id = extract_single_doc_id(&response, "create_AgentRequest")
        .or(lookup_request_doc_id_optional(node, &request_id).await?)
        .context("goal continuation create returned no _docID")?;

    if let Err(error) = session::upsert_conversation_from_request_with_identity_and_requester_did(
        node,
        &parent.session_id,
        &behavior_id,
        &parent.agent_did,
        &behavior_id,
        &request_id,
        content,
        "pending",
        parent.requester_did.as_deref(),
    )
    .await
    {
        tracing::warn!(
            %request_id,
            session_id = %parent.session_id,
            %error,
            "failed to update conversation for goal continuation"
        );
    }

    Ok(EnqueuedAgentRequest {
        doc_id,
        request_id,
        session_id: parent.session_id.clone(),
    })
}

fn require_node_signer_did(node: &EmbeddedNode, operation: &str) -> Result<String> {
    node.node_identity_did()
        .map(str::trim)
        .filter(|did| !did.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            anyhow::anyhow!("{operation} requires a configured DefraDB node signing identity")
        })
}

// SAFETY (#664): `agent_did` scopes the candidate query AND the supersede
// mutation to the owning principal. Under P2P replication a foreign-DID
// `AgentRequest` sharing this `session_id` can be replicated onto this node;
// without the owner guard the session-only filter would supersede that foreign
// replica locally. Defense in depth: the foreign row never becomes a candidate,
// and the write is DID-scoped even if it somehow did.
pub async fn reconcile_coalesced_pending_request(
    node: &EmbeddedNode,
    session_id: &str,
    agent_did: &str,
    source: QueueSource,
    key: &str,
) -> Result<Option<EnqueuedAgentRequest>> {
    let matching =
        matching_coalesced_pending_requests(node, session_id, agent_did, source, key).await?;
    let Some(survivor) = matching.first().and_then(queue_row_to_enqueued_request) else {
        return Ok(None);
    };

    let escaped_agent_did = escape_graphql_string(agent_did);
    for duplicate in matching.iter().skip(1) {
        let terminalized_at = escape_graphql_string(&chrono::Utc::now().to_rfc3339());
        let duplicate_doc_id = escape_graphql_string(&duplicate.doc_id);
        let survivor_request_id = escape_graphql_string(&survivor.request_id);
        let mutation = format!(
            r#"mutation {{
                update_AgentRequest(
                    filter: {{
                        _docID: {{ _eq: "{duplicate_doc_id}" }},
                        agent_did: {{ _eq: "{escaped_agent_did}" }},
                        status: {{ _eq: "pending" }},
                        lifecycle_state: {{ _eq: "pending" }}
                    }},
                    input: {{
                        status: "superseded",
                        lifecycle_state: "superseded",
                        superseded_by_request: "{survivor_request_id}",
                        failure_reason: "coalesced into earlier queued request",
                        terminalized_at: "{terminalized_at}",
                        terminal_redrive_attempts: 0
                    }}
                ) {{ _docID }}
            }}"#
        );
        crate::retry::execute_graphql_with_terminal_persistence_retry(
            node,
            &mutation,
            "reconcile_coalesced_pending_request",
        )
        .await?;
    }

    Ok(Some(survivor))
}

async fn matching_coalesced_pending_requests(
    node: &EmbeddedNode,
    session_id: &str,
    agent_did: &str,
    source: QueueSource,
    key: &str,
) -> Result<Vec<PendingQueueRow>> {
    let escaped_session_id = escape_graphql_string(session_id);
    let escaped_agent_did = escape_graphql_string(agent_did);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{
                    session_id: {{ _eq: "{escaped_session_id}" }},
                    agent_did: {{ _eq: "{escaped_agent_did}" }},
                    status: {{ _eq: "pending" }},
                    lifecycle_state: {{ _eq: "pending" }}
                }},
                order: [{{ created_at: ASC }}, {{ request_id: ASC }}]
            ) {{
                _docID
                request_id
                session_id
                metadata
            }}
        }}"#
    );

    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query pending queue entries for session {session_id} failed: {:?}",
            response.errors
        );
    }

    let rows: Vec<PendingQueueRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();

    Ok(rows
        .into_iter()
        .filter(|row| queue_source_and_key_match(row.metadata.as_deref(), source, key))
        .collect())
}

fn queue_row_to_enqueued_request(row: &PendingQueueRow) -> Option<EnqueuedAgentRequest> {
    Some(EnqueuedAgentRequest {
        doc_id: row.doc_id.clone(),
        request_id: row.request_id.clone()?,
        session_id: row.session_id.clone()?,
    })
}

async fn parent_behavior_id(node: &EmbeddedNode, parent: &AgentRequest) -> Result<String> {
    if let Some(behavior_id) = parent
        .behavior_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(behavior_id.to_string());
    }

    let escaped_session_id = escape_graphql_string(&parent.session_id);
    let query = format!(
        r#"{{
            AgentConversation(
                filter: {{ session_id: {{ _eq: "{escaped_session_id}" }} }},
                limit: 1
            ) {{
                behavior_id
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query parent conversation for queued request failed: {:?}",
            response.errors
        );
    }

    response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentConversation"))
        .and_then(|value| value.as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("behavior_id"))
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "cannot enqueue same-session request: parent request {} has no behavior_id",
                parent.request_id
            )
        })
}

fn parent_linkage_graphql_fields(parent: &AgentRequest) -> String {
    match (
        parent.caused_by_parent_request_id.as_deref(),
        parent.caused_by_parent_tool_call_id.as_deref(),
    ) {
        (Some(parent_request_id), Some(parent_tool_call_id))
            if !parent_request_id.trim().is_empty() && !parent_tool_call_id.trim().is_empty() =>
        {
            format!(
                r#",
                caused_by_parent_request_id: "{}",
                caused_by_parent_tool_call_id: "{}""#,
                escape_graphql_string(parent_request_id),
                escape_graphql_string(parent_tool_call_id),
            )
        }
        (Some(parent_request_id), None) if !parent_request_id.trim().is_empty() => {
            format!(
                r#",
                caused_by_parent_request_id: "{}""#,
                escape_graphql_string(parent_request_id),
            )
        }
        _ => String::new(),
    }
}

async fn lookup_request_doc_id(node: &EmbeddedNode, request_id: &str) -> Result<String> {
    let escaped_request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                limit: 1
            ) {{
                _docID
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "lookup queued AgentRequest doc id failed for request_id={request_id}: {:?}",
            response.errors
        );
    }
    extract_single_doc_id(&response, "AgentRequest").ok_or_else(|| {
        anyhow::anyhow!("queued AgentRequest request_id={request_id} not found after create")
    })
}

async fn lookup_request_doc_id_optional(
    node: &EmbeddedNode,
    request_id: &str,
) -> Result<Option<String>> {
    let escaped_request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                order: {{ created_at: ASC }},
                limit: 1
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "lookup optional AgentRequest doc id failed for request_id={request_id}: {:?}",
            response.errors
        );
    }
    Ok(extract_single_doc_id(&response, "AgentRequest"))
}

pub async fn drain_automated_wakeups(
    node: &EmbeddedNode,
    session_id: &str,
    agent_did: &str,
    reason: &str,
) -> Result<usize> {
    drain_pending_session_requests_where(node, session_id, agent_did, reason, |row| {
        row.execution_origin.as_deref() == Some("scheduled")
            && is_automated_wakeup(row.metadata.as_deref())
    })
    .await
}

pub(crate) async fn drain_subagent_owned_queue(
    node: &EmbeddedNode,
    session_id: &str,
    agent_did: &str,
    reason: &str,
) -> Result<usize> {
    drain_pending_session_requests_where(node, session_id, agent_did, reason, |row| {
        is_subagent_owned_queue(row.metadata.as_deref())
    })
    .await
}

// SAFETY (#664): `agent_did` scopes both the pending-row scan AND the interrupt
// mutation to the owning principal. A foreign-DID replica sharing this
// `session_id` (P2P replication) is neither surfaced as a drain candidate nor
// interrupted by this owner's drain. Defense in depth on the query and the write.
async fn drain_pending_session_requests_where(
    node: &EmbeddedNode,
    session_id: &str,
    agent_did: &str,
    reason: &str,
    should_drain: impl Fn(&PendingQueueRow) -> bool,
) -> Result<usize> {
    let escaped_session_id = escape_graphql_string(session_id);
    let escaped_agent_did = escape_graphql_string(agent_did);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{
                    session_id: {{ _eq: "{escaped_session_id}" }},
                    agent_did: {{ _eq: "{escaped_agent_did}" }},
                    status: {{ _eq: "pending" }},
                    lifecycle_state: {{ _eq: "pending" }}
                }}
            ) {{
                _docID
                execution_origin
                metadata
            }}
        }}"#
    );

    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query pending automated wake-ups for session {session_id} failed: {:?}",
            response.errors
        );
    }

    let rows: Vec<PendingQueueRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();

    let escaped_reason = escape_graphql_string(reason);
    let mut drained = 0;
    for row in rows.into_iter().filter(should_drain) {
        let terminalized_at = escape_graphql_string(&chrono::Utc::now().to_rfc3339());
        let escaped_doc_id = escape_graphql_string(&row.doc_id);
        let mutation = format!(
            r#"mutation {{
                update_AgentRequest(
                    filter: {{
                        _docID: {{ _eq: "{escaped_doc_id}" }},
                        agent_did: {{ _eq: "{escaped_agent_did}" }},
                        status: {{ _eq: "pending" }},
                        lifecycle_state: {{ _eq: "pending" }}
                    }},
                    input: {{
                        status: "interrupted",
                        lifecycle_state: "interrupted",
                        failure_reason: "{escaped_reason}",
                        terminalized_at: "{terminalized_at}",
                        terminal_redrive_attempts: 0
                    }}
                ) {{ _docID }}
            }}"#
        );
        let response = crate::retry::execute_graphql_with_terminal_persistence_retry(
            node,
            &mutation,
            "drain_automated_wakeup",
        )
        .await?;
        if response
            .data
            .as_ref()
            .and_then(|data| data.get("update_AgentRequest"))
            .is_some_and(response_has_documents)
        {
            drained += 1;
        }
    }

    Ok(drained)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AgentIdentity;
    use tempfile::TempDir;

    const TEST_AGENT_DID: &str = "did:test:queue-test";
    const TEST_BEHAVIOR_ID: &str = "general";

    struct TestDb {
        node: EmbeddedNode,
        signer_did: Option<String>,
        _tempdir: TempDir,
    }

    #[derive(Debug, Deserialize)]
    struct QueueRow {
        #[serde(rename = "_docID")]
        doc_id: String,
        request_id: String,
        agent_did: String,
        source_author_did: String,
        requester_did: Option<String>,
        session_id: String,
        behavior_id: String,
        content: String,
        metadata: Option<String>,
        status: String,
        lifecycle_state: Option<String>,
        execution_origin: String,
        superseded_by_request: Option<String>,
        subagent_depth: Option<u32>,
        caused_by_parent_request_id: Option<String>,
        caused_by_parent_tool_call_id: Option<String>,
    }

    fn hints(source: QueueSource, policy: QueuePolicy) -> QueueHints {
        QueueHints {
            source,
            policy,
            key: Some("session:sess-1".to_string()),
            queued_after_request_id: Some("req-1".to_string()),
            interrupted_request_id: None,
        }
    }

    fn parent_request(session_id: &str) -> AgentRequest {
        AgentRequest {
            doc_id: "parent-doc".to_string(),
            request_id: "parent-request".to_string(),
            agent_did: TEST_AGENT_DID.to_string(),
            requester_did: None,
            behavior_id: Some(TEST_BEHAVIOR_ID.to_string()),
            session_id: session_id.to_string(),
            content: "parent".to_string(),
            temperature: None,
            top_p: None,
            top_k: None,
            max_tokens: None,
            metadata: None,
            execution_origin: Some("interactive".to_string()),
            created_at: chrono::Utc::now().to_rfc3339(),
            deadline: None,
            subagent_depth: 2,
            caused_by_parent_request_id: Some("root-parent-request".to_string()),
            caused_by_parent_tool_call_id: Some("root-parent-tool-call".to_string()),
        }
    }

    async fn test_db(name: &str) -> TestDb {
        let tempdir = tempfile::Builder::new()
            .prefix(&format!("gents-queue-{name}-"))
            .tempdir()
            .expect("tempdir");
        let identity = crate::identity::KeyIdentity::load_or_create(
            tempdir.path().join("node-identity.key"),
            None,
        )
        .expect("node identity");
        let signer_did = identity.did().to_string();
        let node = EmbeddedNode::builder()
            .data_path(tempdir.path())
            .with_node_identity_did(&signer_did)
            .build()
            .await
            .expect("embedded node");
        crate::schema::ensure_runtime_schemas(&node)
            .await
            .expect("runtime schemas");
        TestDb {
            node,
            signer_did: Some(signer_did),
            _tempdir: tempdir,
        }
    }

    async fn unsigned_test_db(name: &str) -> TestDb {
        let tempdir = tempfile::Builder::new()
            .prefix(&format!("gents-queue-{name}-"))
            .tempdir()
            .expect("tempdir");
        let node = EmbeddedNode::builder()
            .data_path(tempdir.path())
            .build()
            .await
            .expect("embedded node");
        crate::schema::ensure_runtime_schemas(&node)
            .await
            .expect("runtime schemas");
        TestDb {
            node,
            signer_did: None,
            _tempdir: tempdir,
        }
    }

    async fn queue_rows(node: &EmbeddedNode, session_id: &str) -> Vec<QueueRow> {
        let escaped_session_id = escape_graphql_string(session_id);
        let query = format!(
            r#"{{
                AgentRequest(
                    filter: {{ session_id: {{ _eq: "{escaped_session_id}" }} }},
                    order: [{{ created_at: ASC }}, {{ request_id: ASC }}]
                ) {{
                    _docID
                    request_id
                    agent_did
                    source_author_did
                    requester_did
                    session_id
                    behavior_id
                    content
                    metadata
                    status
                    lifecycle_state
                    execution_origin
                    superseded_by_request
                    subagent_depth
                    caused_by_parent_request_id
                    caused_by_parent_tool_call_id
                }}
            }}"#
        );
        let response = node.execute(&query).await;
        assert!(
            !response.has_errors(),
            "queue row query failed: {:?}",
            response.errors
        );
        response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(|value| serde_json::from_value(value.clone()).ok())
            .unwrap_or_default()
    }

    async fn insert_raw_queue_request(
        node: &EmbeddedNode,
        request_id: &str,
        session_id: &str,
        metadata: &str,
    ) -> String {
        let escaped_request_id = escape_graphql_string(request_id);
        let escaped_session_id = escape_graphql_string(session_id);
        let escaped_metadata = escape_graphql_string(metadata);
        let escaped_source_author_did = escape_graphql_string(
            node.node_identity_did()
                .expect("raw queue fixtures use a signed node"),
        );
        let created_at = chrono::Utc::now().to_rfc3339();
        let mutation = format!(
            r#"mutation {{
                create_AgentRequest(input: {{
                    request_id: "{escaped_request_id}",
                    agent_did: "{TEST_AGENT_DID}",
                    source_author_did: "{escaped_source_author_did}",
                    behavior_id: "{TEST_BEHAVIOR_ID}",
                    session_id: "{escaped_session_id}",
                    retry_parent_request: "",
                    retry_root_request: "{escaped_request_id}",
                    superseded_by_request: "",
                    content: "raw duplicate",
                    metadata: "{escaped_metadata}",
                    status: "pending",
                    lifecycle_state: "pending",
                    backend_id: "",
                    execution_origin: "scheduled",
                    failure_reason: "",
                    created_at: "{created_at}",
                    retry_count: 0,
                    max_retries: {max_retries},
                    subagent_depth: 0
                }}) {{ _docID }}
            }}"#,
            max_retries = DEFAULT_REQUEST_MAX_RETRIES,
        );
        session::execute_mutation_with_retry(node, &mutation, "insert_raw_queue_request")
            .await
            .unwrap();
        lookup_request_doc_id(node, request_id).await.unwrap()
    }

    #[test]
    fn parses_queue_hints_from_metadata_queue_field() {
        let metadata = r#"{
            "queue": {
                "source": "subagent_completion",
                "policy": "coalesce",
                "key": "session:sess-1",
                "queued_after_request_id": "req-1"
            }
        }"#;

        assert_eq!(
            parse_queue_hints(Some(metadata)),
            Some(hints(
                QueueSource::BackgroundCompletion,
                QueuePolicy::Coalesce
            ))
        );
    }

    #[test]
    fn parses_all_supported_string_values() {
        let cases = [
            ("user", QueueSource::User),
            ("background_completion", QueueSource::BackgroundCompletion),
            ("subagent_completion", QueueSource::BackgroundCompletion),
            ("steering", QueueSource::Steering),
            ("goal", QueueSource::Goal),
        ];

        for (source, expected_source) in cases {
            let metadata = format!(
                r#"{{
                    "queue": {{
                        "source": "{source}",
                        "policy": "append",
                        "key": null,
                        "queued_after_request_id": null
                    }}
                }}"#
            );

            assert_eq!(
                parse_queue_hints(Some(&metadata)),
                Some(QueueHints {
                    source: expected_source,
                    policy: QueuePolicy::Append,
                    key: None,
                    queued_after_request_id: None,
                    interrupted_request_id: None,
                })
            );
        }

        let metadata = r#"{
            "queue": {
                "source": "user",
                "policy": "coalesce",
                "key": null,
                "queued_after_request_id": null
            }
        }"#;

        assert_eq!(
            parse_queue_hints(Some(metadata)).map(|hints| hints.policy),
            Some(QueuePolicy::Coalesce)
        );
    }

    #[test]
    fn returns_none_for_absent_blank_invalid_or_non_queue_metadata() {
        assert_eq!(parse_queue_hints(None), None);
        assert_eq!(parse_queue_hints(Some("   ")), None);
        assert_eq!(parse_queue_hints(Some("not json")), None);
        assert_eq!(parse_queue_hints(Some(r#"{"run_id":"abc"}"#)), None);
        assert_eq!(
            parse_queue_hints(Some(r#"{"queue":{"source":"timer","policy":"append"}}"#)),
            None
        );
    }

    #[test]
    fn serializes_queue_metadata_json() {
        let json = queue_metadata_json(&QueueHints {
            source: QueueSource::Steering,
            policy: QueuePolicy::Coalesce,
            key: Some("agent:did:key:z123".to_string()),
            queued_after_request_id: None,
            interrupted_request_id: None,
        });

        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "queue": {
                    "source": "steering",
                    "policy": "coalesce",
                    "key": "agent:did:key:z123",
                    "queued_after_request_id": null
                }
            })
        );
    }

    #[test]
    fn automated_wakeup_is_true_only_for_keyed_subagent_completion_coalesce() {
        assert!(!is_automated_wakeup(None));
        assert!(!is_automated_wakeup(Some(&queue_metadata_json(
            &QueueHints {
                source: QueueSource::User,
                policy: QueuePolicy::Append,
                key: None,
                queued_after_request_id: None,
                interrupted_request_id: None,
            }
        ))));
        assert!(is_automated_wakeup(Some(&queue_metadata_json(
            &QueueHints {
                source: QueueSource::BackgroundCompletion,
                policy: QueuePolicy::Coalesce,
                key: Some("background_completion:session-1".to_string()),
                queued_after_request_id: None,
                interrupted_request_id: None,
            }
        ))));
        assert!(!is_automated_wakeup(Some(&queue_metadata_json(
            &QueueHints {
                source: QueueSource::BackgroundCompletion,
                policy: QueuePolicy::Append,
                key: Some("background_completion:session-1".to_string()),
                queued_after_request_id: None,
                interrupted_request_id: None,
            }
        ))));
        assert!(!is_automated_wakeup(Some(&queue_metadata_json(
            &QueueHints {
                source: QueueSource::BackgroundCompletion,
                policy: QueuePolicy::Coalesce,
                key: None,
                queued_after_request_id: None,
                interrupted_request_id: None,
            }
        ))));
        assert!(!is_automated_wakeup(Some(&queue_metadata_json(
            &QueueHints {
                source: QueueSource::Steering,
                policy: QueuePolicy::Coalesce,
                key: None,
                queued_after_request_id: None,
                interrupted_request_id: None,
            }
        ))));
    }

    #[tokio::test]
    async fn enqueue_session_request_coalesces_keyed_subagent_wakeups() {
        let db = test_db("coalesce").await;
        let session_id = "session-coalesced-wakeup";
        let mut parent = parent_request(session_id);
        parent.agent_did = db.signer_did.clone().unwrap();
        let hints = QueueHints {
            source: QueueSource::BackgroundCompletion,
            policy: QueuePolicy::Coalesce,
            key: Some(format!("background_completion:{session_id}")),
            queued_after_request_id: Some(parent.request_id.clone()),
            interrupted_request_id: None,
        };

        let first = enqueue_session_request(
            &db.node,
            &parent,
            "Process pending subagent completion notifications in this session.",
            ExecutionOrigin::Scheduled,
            hints.clone(),
        )
        .await
        .unwrap();
        let second = enqueue_session_request(
            &db.node,
            &parent,
            "This duplicate wake-up should coalesce.",
            ExecutionOrigin::Scheduled,
            hints,
        )
        .await
        .unwrap();

        assert_eq!(second.doc_id, first.doc_id);
        assert_eq!(second.request_id, first.request_id);
        assert_eq!(second.session_id, session_id);

        let rows = queue_rows(&db.node, session_id).await;
        assert_eq!(rows.len(), 1, "coalescing should leave one wake-up row");
        let row = &rows[0];
        assert_eq!(row.doc_id, first.doc_id);
        assert_eq!(row.source_author_did, db.signer_did.as_deref().unwrap());
        assert_eq!(row.session_id, session_id);
        assert_eq!(row.behavior_id, TEST_BEHAVIOR_ID);
        assert_eq!(
            row.content,
            "Process pending subagent completion notifications in this session."
        );
        assert_eq!(row.execution_origin, "scheduled");
        assert_eq!(row.subagent_depth, Some(2));
        assert_eq!(
            row.caused_by_parent_request_id.as_deref(),
            Some("root-parent-request")
        );
        assert_eq!(
            row.caused_by_parent_tool_call_id.as_deref(),
            Some("root-parent-tool-call")
        );
        assert!(is_automated_wakeup(row.metadata.as_deref()));
    }

    #[tokio::test]
    async fn queue_request_creation_requires_a_configured_node_signer() {
        let db = unsigned_test_db("unsigned-creator-rejection").await;
        let session_id = "session-unsigned-creator-rejection";
        let parent = parent_request(session_id);
        let error = enqueue_session_request(
            &db.node,
            &parent,
            "must not create an unsigned queue row",
            ExecutionOrigin::Scheduled,
            hints(QueueSource::User, QueuePolicy::Append),
        )
        .await
        .expect_err("unsigned session queue creation must fail closed");
        assert!(
            error.to_string().contains("node signing identity"),
            "unexpected error: {error:#}"
        );

        let error = enqueue_goal_continuation(
            &db.node,
            &parent,
            "goal-unsigned",
            "must not create an unsigned goal row",
            1,
            false,
        )
        .await
        .expect_err("unsigned goal continuation creation must fail closed");
        assert!(
            error.to_string().contains("node signing identity"),
            "unexpected error: {error:#}"
        );
        assert!(
            queue_rows(&db.node, session_id).await.is_empty(),
            "fail-closed creators must not persist poison rows"
        );
    }

    #[tokio::test]
    async fn signed_queue_request_preserves_requester_attribution() {
        let db = test_db("signed-requester-attribution").await;
        let session_id = "session-signed-requester-attribution";
        let mut parent = parent_request(session_id);
        parent.agent_did = db.signer_did.clone().unwrap();
        parent.requester_did = Some("did:key:z6MkInitiatingRequester".to_string());

        enqueue_session_request(
            &db.node,
            &parent,
            "signed queue request",
            ExecutionOrigin::Interactive,
            hints(QueueSource::User, QueuePolicy::Append),
        )
        .await
        .expect("signed queue request");

        let rows = queue_rows(&db.node, session_id).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].source_author_did, db.signer_did.as_deref().unwrap());
        assert_eq!(
            rows[0].requester_did.as_deref(),
            parent.requester_did.as_deref(),
            "the signer must not replace initiating-requester attribution"
        );
        assert_eq!(rows[0].source_author_did, rows[0].agent_did);
        assert_ne!(
            rows[0].source_author_did,
            rows[0].requester_did.as_deref().unwrap()
        );
    }

    #[tokio::test]
    async fn enqueue_session_request_ignores_append_row_with_same_source_and_key() {
        let db = test_db("coalesce-ignores-append").await;
        let session_id = "session-coalesce-ignores-append";
        let parent = parent_request(session_id);
        let append_hints = QueueHints {
            source: QueueSource::BackgroundCompletion,
            policy: QueuePolicy::Append,
            key: Some(format!("background_completion:{session_id}")),
            queued_after_request_id: Some(parent.request_id.clone()),
            interrupted_request_id: None,
        };
        insert_raw_queue_request(
            &db.node,
            "req-existing-append-same-key",
            session_id,
            &queue_metadata_json(&append_hints),
        )
        .await;
        let coalesce_hints = QueueHints {
            policy: QueuePolicy::Coalesce,
            ..append_hints
        };

        let enqueued = enqueue_session_request(
            &db.node,
            &parent,
            "coalesced wake-up",
            ExecutionOrigin::Scheduled,
            coalesce_hints,
        )
        .await
        .unwrap();

        let rows = queue_rows(&db.node, session_id).await;
        assert_eq!(rows.len(), 2);
        assert!(
            rows.iter()
                .any(|row| row.request_id == "req-existing-append-same-key"
                    && row.status == "pending")
        );
        assert!(rows
            .iter()
            .any(|row| row.request_id == enqueued.request_id && row.status == "pending"));
    }

    #[tokio::test]
    async fn reconcile_coalesced_pending_request_supersedes_duplicate_race_rows() {
        let db = test_db("coalesce-race-reconcile").await;
        let session_id = "session-coalesce-race-reconcile";
        let parent = parent_request(session_id);
        let hints = QueueHints {
            source: QueueSource::BackgroundCompletion,
            policy: QueuePolicy::Coalesce,
            key: Some(format!("background_completion:{session_id}")),
            queued_after_request_id: Some(parent.request_id.clone()),
            interrupted_request_id: None,
        };
        let key = hints.key.clone().unwrap();
        let survivor = enqueue_session_request(
            &db.node,
            &parent,
            "first wake-up",
            ExecutionOrigin::Scheduled,
            hints.clone(),
        )
        .await
        .unwrap();
        let duplicate_doc_id = insert_raw_queue_request(
            &db.node,
            "req-coalesce-race-duplicate",
            session_id,
            &queue_metadata_json(&hints),
        )
        .await;

        let reconciled = reconcile_coalesced_pending_request(
            &db.node,
            session_id,
            TEST_AGENT_DID,
            QueueSource::BackgroundCompletion,
            &key,
        )
        .await
        .unwrap()
        .expect("survivor");
        assert_eq!(reconciled.request_id, survivor.request_id);

        let rows = queue_rows(&db.node, session_id).await;
        let survivor_row = rows
            .iter()
            .find(|row| row.request_id == survivor.request_id)
            .expect("survivor row");
        assert_eq!(survivor_row.status, "pending");
        assert_eq!(survivor_row.lifecycle_state.as_deref(), Some("pending"));

        let duplicate = rows
            .iter()
            .find(|row| row.doc_id == duplicate_doc_id)
            .expect("duplicate row");
        assert_eq!(duplicate.status, "superseded");
        assert_eq!(duplicate.lifecycle_state.as_deref(), Some("superseded"));
        assert_eq!(
            duplicate.superseded_by_request.as_deref(),
            Some(survivor.request_id.as_str())
        );
    }

    #[tokio::test]
    async fn enqueue_session_request_reconciles_preexisting_duplicate_coalesce_rows() {
        let db = test_db("coalesce-preexisting-duplicates").await;
        let session_id = "session-coalesce-preexisting-duplicates";
        let parent = parent_request(session_id);
        let hints = QueueHints {
            source: QueueSource::BackgroundCompletion,
            policy: QueuePolicy::Coalesce,
            key: Some(format!("background_completion:{session_id}")),
            queued_after_request_id: Some(parent.request_id.clone()),
            interrupted_request_id: None,
        };
        let survivor_doc_id = insert_raw_queue_request(
            &db.node,
            "req-preexisting-coalesce-survivor",
            session_id,
            &queue_metadata_json(&hints),
        )
        .await;
        let duplicate_doc_id = insert_raw_queue_request(
            &db.node,
            "req-preexisting-coalesce-duplicate",
            session_id,
            &queue_metadata_json(&hints),
        )
        .await;

        let enqueued = enqueue_session_request(
            &db.node,
            &parent,
            "should reuse survivor",
            ExecutionOrigin::Scheduled,
            hints,
        )
        .await
        .unwrap();
        assert_eq!(enqueued.doc_id, survivor_doc_id);

        let rows = queue_rows(&db.node, session_id).await;
        let survivor = rows
            .iter()
            .find(|row| row.doc_id == survivor_doc_id)
            .expect("survivor");
        assert_eq!(survivor.status, "pending");
        let duplicate = rows
            .iter()
            .find(|row| row.doc_id == duplicate_doc_id)
            .expect("duplicate");
        assert_eq!(duplicate.status, "superseded");
        assert_eq!(
            duplicate.superseded_by_request.as_deref(),
            Some("req-preexisting-coalesce-survivor")
        );
    }

    #[tokio::test]
    async fn enqueue_session_request_without_key_does_not_coalesce() {
        let db = test_db("coalesce-without-key").await;
        let session_id = "session-unkeyed-wakeup";
        let parent = parent_request(session_id);
        let hints = QueueHints {
            source: QueueSource::BackgroundCompletion,
            policy: QueuePolicy::Coalesce,
            key: None,
            queued_after_request_id: Some(parent.request_id.clone()),
            interrupted_request_id: None,
        };

        let first = enqueue_session_request(
            &db.node,
            &parent,
            "first wake-up",
            ExecutionOrigin::Scheduled,
            hints.clone(),
        )
        .await
        .unwrap();
        let second = enqueue_session_request(
            &db.node,
            &parent,
            "second wake-up",
            ExecutionOrigin::Scheduled,
            hints,
        )
        .await
        .unwrap();

        assert_ne!(second.request_id, first.request_id);
        assert_eq!(queue_rows(&db.node, session_id).await.len(), 2);
    }
}
