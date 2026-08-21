use super::*;

/// Atomically bind a background-completion notification to the pending wake
/// request that will consume it. The transaction either reuses the coalesced
/// pending wake it read or creates a new one; a concurrent claim conflicts and
/// retries, so no visible wake can race ahead of its input message.
pub(crate) async fn enqueue_background_completion_with_message(
    node: &EmbeddedNode,
    parent: &AgentRequest,
    notification_content: &str,
    message_key: &str,
    wake_content: &str,
    queue_hints: QueueHints,
) -> Result<EnqueuedBackgroundCompletionInput> {
    anyhow::ensure!(
        queue_hints.source == QueueSource::BackgroundCompletion
            && queue_hints.policy == QueuePolicy::Coalesce,
        "atomic background completion enqueue requires coalescing background metadata"
    );
    let queue_key = queue_hints
        .key
        .as_deref()
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .context("atomic background completion enqueue requires a queue key")?
        .to_string();
    let parent = normalize_request_only_control_parent(node, parent).await?;
    let behavior_id = parent_behavior_id(node, &parent).await?;
    let request_id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    let metadata = queue_metadata_json(&queue_hints);
    let request_mutation = session_request_create_mutation(
        &parent,
        &behavior_id,
        wake_content,
        ExecutionOrigin::Scheduled,
        &metadata,
        &request_id,
        &now,
        true,
    )?;

    let mut retry_index = 0;
    let mut enqueued = loop {
        let txn = ConfigApplyTxn::begin_local(node, None).await?;
        let attempt = background_completion_transaction_attempt(
            &txn,
            &parent,
            notification_content,
            message_key,
            &queue_key,
            &request_id,
            &request_mutation,
        )
        .await;
        let result = match attempt {
            Ok(enqueued) => txn.commit().await.map(|()| enqueued),
            Err(error) => {
                if let Err(discard_error) = txn.discard().await {
                    tracing::warn!(
                        error = %discard_error,
                        "discarding failed background-completion transaction also failed"
                    );
                }
                Err(error)
            }
        };
        match result {
            Ok(enqueued) => break enqueued,
            Err(error)
                if retry_index < DEFRA_DB_CONFLICT_MAX_RETRIES
                    && steering_transaction_error_is_retryable(&error) =>
            {
                let backoff = defradb_conflict_retry_backoff(retry_index);
                retry_index += 1;
                tracing::warn!(
                    request_id,
                    attempt = retry_index,
                    backoff_ms = backoff.as_millis() as u64,
                    error = %error,
                    "retrying atomic background-completion persistence"
                );
                tokio::time::sleep(backoff).await;
            }
            Err(error) => return Err(error),
        }
    };

    if enqueued.created_request {
        let created_request_doc_id = enqueued.request.doc_id.clone();
        let active_request = reconcile_coalesced_pending_request(
            node,
            &parent.session_id,
            &parent.agent_did,
            QueueSource::BackgroundCompletion,
            &queue_key,
        )
        .await?
        .unwrap_or_else(|| enqueued.request.clone());
        enqueued.created_request = active_request.doc_id == created_request_doc_id;

        enqueued.request = active_request;
    }

    Ok(enqueued)
}

async fn background_completion_transaction_attempt(
    txn: &ConfigApplyTxn<'_>,
    parent: &AgentRequest,
    content: &str,
    message_key: &str,
    queue_key: &str,
    request_id: &str,
    request_mutation: &str,
) -> Result<EnqueuedBackgroundCompletionInput> {
    let escaped_session_id = escape_graphql_string(&parent.session_id);
    let escaped_agent_did = escape_graphql_string(&parent.agent_did);
    let response = txn
        .execute(&format!(
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
        ))
        .await?;
    let pending = response["data"]["AgentRequest"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|row| serde_json::from_value::<PendingQueueRow>(row).ok())
        .find(|row| {
            queue_source_and_key_match(
                row.metadata.as_deref(),
                QueueSource::BackgroundCompletion,
                queue_key,
            )
        })
        .and_then(|row| queue_row_to_enqueued_request(&row));

    let (request, created_request) = match pending {
        Some(request) => (request, false),
        None => {
            let response = txn.execute(request_mutation).await?;
            let doc_id = transaction_created_doc_id(&response, "AgentRequest")?;
            (
                EnqueuedAgentRequest {
                    doc_id,
                    request_id: request_id.to_string(),
                    session_id: parent.session_id.clone(),
                },
                true,
            )
        }
    };
    let message_sequence = next_append_sequence_in_transaction(txn, &parent.session_id).await?;
    let message_mutation = session::create_message_mutation(
        &parent.session_id,
        &parent.agent_did,
        parent.requester_did.as_deref(),
        message_sequence,
        "user",
        content,
        None,
        Some(&request.request_id),
        Some(&request.doc_id),
        Some(message_key),
    );
    txn.execute(&message_mutation).await?;

    Ok(EnqueuedBackgroundCompletionInput {
        request,
        message_sequence,
        created_request,
    })
}

pub(super) async fn normalize_request_only_control_parent(
    node: &EmbeddedNode,
    parent: &AgentRequest,
) -> Result<AgentRequest> {
    let mut normalized = parent.clone();
    match (
        normalized.caused_by_parent_request_id.as_deref(),
        normalized.caused_by_parent_request_doc_id.as_deref(),
    ) {
        (Some(request_id), None) if !request_id.trim().is_empty() => {
            normalized.caused_by_parent_request_doc_id =
                Some(crate::request_binding::require_request_doc_id(node, request_id).await?);
            tracing::warn!(
                request_id = %normalized.request_id,
                caused_by_parent_request_id = %request_id,
                "recovered legacy logical-only control-continuation parent binding",
            );
        }
        (Some(request_id), Some(request_doc_id))
            if !request_id.trim().is_empty() && !request_doc_id.trim().is_empty() => {}
        (None, None) => {}
        _ => anyhow::bail!("cannot enqueue control continuation from incoherent parent linkage"),
    }
    normalized.caused_by_parent_tool_call_id = None;
    normalized.caused_by_parent_tool_call_doc_id = None;
    Ok(normalized)
}

pub(super) async fn steering_transaction_attempt(
    txn: &ConfigApplyTxn<'_>,
    parent: &AgentRequest,
    content: &str,
    request_id: &str,
    request_mutation: &str,
) -> Result<EnqueuedAgentRequest> {
    let request_response = txn.execute(request_mutation).await?;
    let request_doc_id = transaction_created_doc_id(&request_response, "AgentRequest")?;
    let sequence = next_append_sequence_in_transaction(txn, &parent.session_id).await?;
    let message_key = steering_input_message_key(request_id);
    let message_mutation = session::create_message_mutation(
        &parent.session_id,
        &parent.agent_did,
        parent.requester_did.as_deref(),
        sequence,
        "user",
        content,
        None,
        Some(request_id),
        Some(&request_doc_id),
        Some(&message_key),
    );
    txn.execute(&message_mutation).await?;

    Ok(EnqueuedAgentRequest {
        doc_id: request_doc_id,
        request_id: request_id.to_string(),
        session_id: parent.session_id.clone(),
    })
}

async fn next_append_sequence_in_transaction(
    txn: &ConfigApplyTxn<'_>,
    session_id: &str,
) -> Result<u32> {
    let escaped_session_id = escape_graphql_string(session_id);
    let response = txn
        .execute(&format!(
            r#"{{
                AgentMessage(
                    filter: {{ session_id: {{ _eq: "{escaped_session_id}" }} }},
                    order: {{ sequence: DESC }},
                    limit: 1
                ) {{ sequence }}
                AgentToolCall(
                    filter: {{
                        session_id: {{ _eq: "{escaped_session_id}" }},
                        await_mode: {{ _eq: "background" }}
                    }}
                ) {{ message_sequence }}
            }}"#
        ))
        .await?;
    let message_max = response["data"]["AgentMessage"]
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(|row| row["sequence"].as_u64())
        .unwrap_or(0) as u32;
    let mut reserved_counts = std::collections::BTreeMap::<u32, u32>::new();
    if let Some(rows) = response["data"]["AgentToolCall"].as_array() {
        for row in rows {
            if let Some(sequence) = row["message_sequence"]
                .as_u64()
                .and_then(|value| u32::try_from(value).ok())
            {
                *reserved_counts.entry(sequence).or_default() += 1;
            }
        }
    }
    let reserved_max = reserved_counts
        .into_iter()
        .map(|(sequence, count)| sequence + count)
        .max()
        .unwrap_or(0);
    Ok(message_max.max(reserved_max) + 1)
}

pub(super) fn transaction_created_doc_id(response: &Value, collection: &str) -> Result<String> {
    let create_field = format!("create_{collection}");
    let add_field = format!("add_{collection}");
    let value = response
        .get("data")
        .and_then(|data| data.get(&create_field).or_else(|| data.get(&add_field)))
        .with_context(|| {
            format!("transaction create returned neither {create_field} nor {add_field}")
        })?;
    value
        .get("_docID")
        .or_else(|| {
            value
                .as_array()
                .and_then(|rows| rows.first())?
                .get("_docID")
        })
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .with_context(|| format!("transaction create {collection} returned no _docID"))
}

pub(super) fn steering_transaction_error_is_retryable(error: &anyhow::Error) -> bool {
    let text = error.to_string();
    let lower = text.to_ascii_lowercase();
    is_defradb_transaction_conflict_text(&text)
        || lower.contains("unique")
        || lower.contains("duplicate")
}
