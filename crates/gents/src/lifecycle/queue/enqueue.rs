use super::*;

pub(crate) const STEERING_WAKE_PROMPT: &str = "Continue with the new steering message.";

pub(crate) async fn enqueue_session_request(
    node: &EmbeddedNode,
    parent: &AgentRequest,
    content: &str,
    execution_origin: ExecutionOrigin,
    queue_hints: QueueHints,
) -> Result<EnqueuedAgentRequest> {
    let request_only_control = matches!(
        queue_hints.source,
        QueueSource::Steering | QueueSource::BackgroundCompletion
    );
    let normalized_parent = if request_only_control {
        Some(normalize_request_only_control_parent(node, parent).await?)
    } else {
        None
    };
    let parent = normalized_parent.as_ref().unwrap_or(parent);
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
    let mutation = session_request_create_mutation(
        parent,
        &behavior_id,
        content,
        execution_origin,
        &metadata,
        &request_id,
        &now,
        request_only_control,
    )?;

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

    Ok(enqueued)
}

/// Atomically persist a steering input and the continuation that consumes it.
///
/// The request is created first inside the private transaction so its exact
/// DefraDB document ID can be stamped on the message.  Neither document is
/// externally visible until both writes commit, so a watcher can never claim
/// the continuation before its input is durable.
pub(crate) async fn enqueue_steering_request_with_message(
    node: &EmbeddedNode,
    parent: &AgentRequest,
    content: &str,
    queue_hints: QueueHints,
) -> Result<EnqueuedAgentRequest> {
    anyhow::ensure!(
        queue_hints.source == QueueSource::Steering
            && queue_hints.policy == QueuePolicy::Append
            && queue_hints.key.is_none(),
        "atomic steering enqueue requires an unkeyed append"
    );

    let normalized_parent = normalize_request_only_control_parent(node, parent).await?;
    let parent = &normalized_parent;
    let behavior_id = parent_behavior_id(node, parent).await?;
    let request_id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    let metadata = queue_metadata_json(&queue_hints);
    let request_mutation = session_request_create_mutation(
        parent,
        &behavior_id,
        STEERING_WAKE_PROMPT,
        ExecutionOrigin::Interactive,
        &metadata,
        &request_id,
        &now,
        true,
    )?;

    let mut retry_index = 0;
    let enqueued = loop {
        let txn = ConfigApplyTxn::begin_local(node, None).await?;
        let attempt =
            steering_transaction_attempt(&txn, parent, content, &request_id, &request_mutation)
                .await;

        let result = match attempt {
            Ok(enqueued) => match txn.commit().await {
                Ok(()) => Ok(enqueued),
                Err(error) => Err(error),
            },
            Err(error) => {
                if let Err(discard_error) = txn.discard().await {
                    tracing::warn!(
                        error = %discard_error,
                        "discarding failed steering transaction also failed"
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
                    "retrying atomic steering persistence"
                );
                tokio::time::sleep(backoff).await;
            }
            Err(error) => return Err(error),
        }
    };

    Ok(enqueued)
}
