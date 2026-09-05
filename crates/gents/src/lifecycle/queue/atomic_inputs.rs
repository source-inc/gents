use super::*;

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};

use tokio::sync::Mutex;

type BackgroundCompletionGate = Mutex<()>;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct BackgroundCompletionGateKey {
    node: usize,
    session_id: String,
    agent_did: String,
    queue_key: String,
}

/// Serialize the read/create/reconcile path for one local coalescing domain.
/// The proven queue transition is sequential: one coalescing enqueue must
/// observe the pending entry created by the previous transition. DefraDB
/// transactions conflict only when they touch the same document, so two empty
/// reads could otherwise create disjoint requests and return before duplicate
/// reconciliation converges them. The weak registry does not retain either
/// nodes or idle gates; stale entries are pruned when a new gate is created.
fn background_completion_gate(
    node: &EmbeddedNode,
    session_id: &str,
    agent_did: &str,
    queue_key: &str,
) -> Arc<BackgroundCompletionGate> {
    static GATES: OnceLock<
        StdMutex<HashMap<BackgroundCompletionGateKey, Weak<BackgroundCompletionGate>>>,
    > = OnceLock::new();

    let key = BackgroundCompletionGateKey {
        node: node as *const EmbeddedNode as usize,
        session_id: session_id.to_string(),
        agent_did: agent_did.to_string(),
        queue_key: queue_key.to_string(),
    };
    let mut gates = GATES
        .get_or_init(|| StdMutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(gate) = gates.get(&key).and_then(Weak::upgrade) {
        return gate;
    }

    gates.retain(|_, gate| gate.strong_count() > 0);
    let gate = Arc::new(Mutex::new(()));
    gates.insert(key, Arc::downgrade(&gate));
    gate
}

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

    let gate = background_completion_gate(node, &parent.session_id, &parent.agent_did, &queue_key);
    let _guard = gate.lock().await;

    let behavior_id = parent_behavior_id(node, parent).await?;
    let metadata = queue_metadata_json(&queue_hints);

    let mut retry_index = 0;
    let mut enqueued = loop {
        let txn = ConfigApplyTxn::begin_local(node, None).await?;
        let attempt = background_completion_transaction_attempt(
            &txn,
            parent,
            notification_content,
            message_key,
            &queue_key,
            &behavior_id,
            wake_content,
            &metadata,
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
                    queue_key,
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
    behavior_id: &str,
    wake_content: &str,
    metadata: &str,
) -> Result<EnqueuedBackgroundCompletionInput> {
    use sha2::{Digest, Sha256};

    let escaped_session_id = escape_graphql_string(&parent.session_id);
    let escaped_agent_did = escape_graphql_string(&parent.agent_did);
    let escaped_message_key = escape_graphql_string(message_key);
    let mut scope_hasher = Sha256::new();
    for component in [&parent.agent_did, &parent.session_id, queue_key] {
        scope_hasher.update((component.len() as u64).to_be_bytes());
        scope_hasher.update(component.as_bytes());
    }
    let queue_scope = format!("{:x}", scope_hasher.finalize());
    let retry_key_prefix = format!("background-completion:{queue_scope}:");
    let escaped_retry_key_pattern = escape_graphql_string(&format!("{retry_key_prefix}%"));
    let response = txn
        .execute(&format!(
            r#"{{
                notification: AgentMessage(
                    filter: {{ message_key: {{ _eq: "{escaped_message_key}" }} }},
                    limit: 2
                ) {{
                    session_id
                    agent_did
                    request_id
                    request_doc_id
                    sequence
                    role
                    content
                }}
                pending: AgentRequest(
                    filter: {{
                        session_id: {{ _eq: "{escaped_session_id}" }},
                        agent_did: {{ _eq: "{escaped_agent_did}" }},
                        lifecycle_state: {{ _eq: "pending" }}
                    }},
                    order: [{{ created_at: ASC }}, {{ request_id: ASC }}]
                ) {{
                    _docID
                    request_id
                    session_id
                    metadata
                }}
                generations: AgentRequest(
                    filter: {{
                        session_id: {{ _eq: "{escaped_session_id}" }},
                        agent_did: {{ _eq: "{escaped_agent_did}" }},
                        retry_key: {{ _like: "{escaped_retry_key_pattern}" }}
                    }}
                ) {{
                    retry_key
                }}
            }}"#
        ))
        .await?;
    let notifications = response["data"]["notification"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    anyhow::ensure!(
        notifications.len() <= 1,
        "background completion notification key resolved to multiple rows"
    );
    if let Some(notification) = notifications.first() {
        let persisted_session = notification["session_id"].as_str().unwrap_or_default();
        let persisted_did = notification["agent_did"].as_str().unwrap_or_default();
        let persisted_role = notification["role"].as_str().unwrap_or_default();
        let persisted_content = notification["content"].as_str().unwrap_or_default();
        let persisted_request_id = notification["request_id"].as_str().unwrap_or_default();
        let persisted_request_doc_id = notification["request_doc_id"].as_str().unwrap_or_default();
        let persisted_sequence = notification["sequence"]
            .as_u64()
            .and_then(|value| u32::try_from(value).ok());
        anyhow::ensure!(
            persisted_session == parent.session_id
                && persisted_did == parent.agent_did
                && persisted_role == "user"
                && persisted_content == content
                && !persisted_request_id.is_empty()
                && !persisted_request_doc_id.is_empty()
                && persisted_sequence.is_some(),
            "background completion notification key conflicts with its persisted binding"
        );
        let binding_response = txn
            .execute(&format!(
                r#"{{
                    AgentRequest(
                        filter: {{ _docID: {{ _eq: "{}" }} }},
                        limit: 2
                    ) {{
                        _docID
                        request_id
                        agent_did
                        session_id
                        metadata
                    }}
                }}"#,
                escape_graphql_string(persisted_request_doc_id)
            ))
            .await?;
        let bindings = binding_response["data"]["AgentRequest"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        anyhow::ensure!(
            bindings.len() == 1
                && bindings[0]["request_id"].as_str() == Some(persisted_request_id)
                && bindings[0]["agent_did"].as_str() == Some(parent.agent_did.as_str())
                && bindings[0]["session_id"].as_str() == Some(parent.session_id.as_str())
                && queue_source_and_key_match(
                    bindings[0]["metadata"].as_str(),
                    QueueSource::BackgroundCompletion,
                    queue_key,
                ),
            "background completion notification key references an invalid wake binding"
        );
        return Ok(EnqueuedBackgroundCompletionInput {
            request: EnqueuedAgentRequest {
                doc_id: persisted_request_doc_id.to_string(),
                request_id: persisted_request_id.to_string(),
                session_id: persisted_session.to_string(),
            },
            message_sequence: persisted_sequence.expect("validated sequence"),
            created_request: false,
        });
    }

    let pending_rows: Vec<AgentRequestRow> =
        serde_json::from_value(response["data"]["pending"].clone())
            .context("decode pending AgentRequest rows")?;
    let pending = pending_rows
        .into_iter()
        .find(|row| {
            queue_source_and_key_match(
                row.metadata.as_deref(),
                QueueSource::BackgroundCompletion,
                queue_key,
            )
        })
        .and_then(|row| queue_row_to_enqueued_request(&row));
    let message_sequence = next_append_sequence_in_transaction(txn, &parent.session_id).await?;
    let mut max_generation = None::<u64>;
    for row in response["data"]["generations"]
        .as_array()
        .into_iter()
        .flatten()
    {
        let retry_key = row["retry_key"]
            .as_str()
            .context("background completion generation row has no retry key")?;
        let generation = retry_key
            .strip_prefix(&retry_key_prefix)
            .context("background completion generation row has the wrong scope")?
            .parse::<u64>()
            .context("background completion generation row is malformed")?;
        max_generation = Some(max_generation.map_or(generation, |current| current.max(generation)));
    }
    let next_generation = match max_generation {
        Some(generation) => generation
            .checked_add(1)
            .context("background completion queue generation overflow")?,
        None => 0,
    };

    let (request, created_request) = match pending {
        Some(request) => (request, false),
        None => {
            let request_id = format!("background-completion-{queue_scope}-{next_generation:020}");
            let retry_key = format!("{retry_key_prefix}{next_generation:020}");
            let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
            let request_mutation = session_request_create_mutation(
                parent,
                behavior_id,
                wake_content,
                ExecutionOrigin::Scheduled,
                metadata,
                &request_id,
                &now,
                Some(&retry_key),
            )
            .await?;
            let response = txn.execute(&request_mutation).await?;
            let doc_id = transaction_created_doc_id(&response, "AgentRequest")?;
            (
                EnqueuedAgentRequest {
                    doc_id,
                    request_id,
                    session_id: parent.session_id.clone(),
                },
                true,
            )
        }
    };
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
