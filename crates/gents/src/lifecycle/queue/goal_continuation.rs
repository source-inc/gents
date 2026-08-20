use super::*;

pub(crate) async fn enqueue_goal_continuation(
    node: &EmbeddedNode,
    parent: &AgentRequest,
    goal_id: &str,
    content: &str,
    continuation_sequence: i64,
    wrapup: bool,
) -> Result<EnqueuedAgentRequest> {
    use sha2::{Digest, Sha256};

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
    let requester_did_field = session::requester_did_create_field(parent.requester_did.as_deref());
    let escaped_behavior_id = escape_graphql_string(&behavior_id);
    let escaped_session_id = escape_graphql_string(&parent.session_id);
    let escaped_content = escape_graphql_string(content);
    let escaped_metadata = escape_graphql_string(&metadata);
    let escaped_created_at = escape_graphql_string(&now);
    let escaped_goal_id = escape_graphql_string(goal_id);
    let escaped_parent_request_id = escape_graphql_string(&parent.request_id);
    let escaped_parent_request_doc_id = escape_graphql_string(&parent.doc_id);
    let inherited_trigger_context = crate::lifecycle::inherited_trigger_context_graphql_fields(
        parent.caused_by_correlation.as_deref(),
        parent.caused_by_trigger_context.as_deref(),
    )?;
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{escaped_request_id}",
                agent_did: "{escaped_agent_did}",
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
                {inherited_trigger_context}
                caused_by_parent_request_id: "{escaped_parent_request_id}",
                caused_by_parent_request_doc_id: "{escaped_parent_request_doc_id}",
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

    Ok(EnqueuedAgentRequest {
        doc_id,
        request_id,
        session_id: parent.session_id.clone(),
    })
}

// SAFETY (#664): `agent_did` scopes the candidate query AND the supersede
// mutation to the owning principal. Under P2P replication a foreign-DID
// `AgentRequest` sharing this `session_id` can be replicated onto this node;
// without the owner guard the session-only filter would supersede that foreign
// replica locally. Defense in depth: the foreign row never becomes a candidate,
// and the write is DID-scoped even if it somehow did.
