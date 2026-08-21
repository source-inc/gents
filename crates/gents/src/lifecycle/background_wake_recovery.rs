use std::collections::BTreeSet;

use anyhow::{Context, Result};
use defra_node::EmbeddedNode;
use serde::Deserialize;

use crate::background_completion::BACKGROUND_COMPLETION_WAKE_PROMPT;
use crate::config_client::ConfigApplyTxn;
use crate::graphql::escape_graphql_string;
use crate::session;

use super::queue::{parse_queue_hints, QueuePolicy, QueueSource};
use super::{BackgroundWakeRedriveReport, RequestLifecycle};

const BACKGROUND_WAKE_REDRIVE_BATCH_LIMIT: usize = 64;
const BACKGROUND_WAKE_RETRY_BASE_SECONDS: i64 = 5;
const BACKGROUND_WAKE_RETRY_MAX_SECONDS: i64 = 60;

#[derive(Debug, Clone, Deserialize)]
struct FailedWakeRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    request_id: String,
    agent_did: String,
    requester_did: Option<String>,
    behavior_id: String,
    session_id: String,
    retry_root_request: Option<String>,
    temperature: Option<f64>,
    top_p: Option<f64>,
    top_k: Option<i64>,
    seed: Option<i64>,
    max_tokens: Option<i64>,
    max_total_tokens: Option<i64>,
    metadata: Option<String>,
    backend_id: Option<String>,
    caused_by_parent_request_id: Option<String>,
    caused_by_parent_request_doc_id: Option<String>,
    subagent_depth: Option<u32>,
    retry_count: i64,
    max_retries: i64,
    terminalized_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SuccessorRow {
    retry_parent_request_doc_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PendingWakeRow {
    session_id: String,
    metadata: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ConversationRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    latest_request_id: String,
    updated_at: String,
    title: String,
    preview_text: String,
}

impl ConversationRow {
    fn rank(&self) -> (String, usize, String) {
        let richness = [
            self.title.trim(),
            self.preview_text.trim(),
            self.latest_request_id.trim(),
        ]
        .iter()
        .filter(|field| !field.is_empty())
        .count();
        (self.updated_at.clone(), richness, self.doc_id.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RedriveOutcome {
    Created { request_id: String },
    AlreadyCreated,
    Coalesced,
    Ineligible,
}

impl RequestLifecycle {
    /// Create bounded retry successors for failed canonical background wakes.
    ///
    /// This is deliberately narrower than interactive request retry: the
    /// source must be a failed scheduled request carrying the versioned,
    /// coalesced `background_completion` queue metadata. The source must also
    /// remain the conversation's latest request and have retry budget left.
    /// A unique per-source `retry_key` plus a private transaction makes the
    /// sweep idempotent across concurrent ticks and process restarts.
    pub async fn redrive_failed_background_wakeups(
        node: &EmbeddedNode,
        agent_did: &str,
    ) -> Result<BackgroundWakeRedriveReport> {
        let (candidates, successors, pending) = load_candidates(node, agent_did).await?;
        let mut report = BackgroundWakeRedriveReport {
            scanned: candidates.len(),
            ..Default::default()
        };
        let successor_parents = successors
            .into_iter()
            .filter_map(|row| clean(row.retry_parent_request_doc_id))
            .collect::<BTreeSet<_>>();
        let pending_keys = pending
            .into_iter()
            .filter_map(|row| {
                let hints = parse_queue_hints(row.metadata.as_deref())?;
                automated_queue_key(&hints).map(|key| (row.session_id, key))
            })
            .collect::<BTreeSet<_>>();

        let mut eligible = Vec::new();
        for candidate in candidates {
            let Some(queue_key) = eligible_queue_key(&candidate) else {
                report.ineligible += 1;
                continue;
            };
            if successor_parents.contains(&candidate.doc_id) {
                report.already_redriven += 1;
                continue;
            }
            if pending_keys.contains(&(candidate.session_id.clone(), queue_key)) {
                report.coalesced += 1;
                continue;
            }
            if !retry_is_due(&candidate, chrono::Utc::now()) {
                report.deferred += 1;
                continue;
            }
            eligible.push(candidate);
        }

        for candidate in eligible
            .into_iter()
            .take(BACKGROUND_WAKE_REDRIVE_BATCH_LIMIT)
        {
            match redrive_one(node, &candidate).await {
                Ok(RedriveOutcome::Created { request_id }) => {
                    report.redriven += 1;
                    tracing::info!(
                        source_request_id = %candidate.request_id,
                        request_id,
                        session_id = %candidate.session_id,
                        retry_count = candidate.retry_count + 1,
                        max_retries = candidate.max_retries,
                        "redrove failed background-completion wake"
                    );
                }
                Ok(RedriveOutcome::AlreadyCreated) => report.already_redriven += 1,
                Ok(RedriveOutcome::Coalesced) => report.coalesced += 1,
                Ok(RedriveOutcome::Ineligible) => report.ineligible += 1,
                Err(error) => {
                    report.failed += 1;
                    tracing::warn!(
                        request_id = %candidate.request_id,
                        session_id = %candidate.session_id,
                        error = %error,
                        "failed to redrive background-completion wake"
                    );
                }
            }
        }
        Ok(report)
    }
}

async fn load_candidates(
    node: &EmbeddedNode,
    agent_did: &str,
) -> Result<(Vec<FailedWakeRow>, Vec<SuccessorRow>, Vec<PendingWakeRow>)> {
    let agent_did = escape_graphql_string(agent_did);
    let response = node
        .execute(&format!(
            r#"{{
                failed: AgentRequest(filter: {{
                    agent_did: {{ _eq: "{agent_did}" }},
                    status: {{ _eq: "error" }},
                    lifecycle_state: {{ _eq: "failed" }},
                    execution_origin: {{ _eq: "scheduled" }}
                }}, order: [{{ terminalized_at: ASC }}, {{ request_id: ASC }}]) {{
                    _docID request_id agent_did requester_did behavior_id session_id
                    retry_root_request temperature top_p top_k seed max_tokens
                    max_total_tokens metadata backend_id caused_by_parent_request_id
                    caused_by_parent_request_doc_id subagent_depth retry_count max_retries
                    terminalized_at
                }}
                successors: AgentRequest(filter: {{
                    agent_did: {{ _eq: "{agent_did}" }},
                    retry_parent_request_doc_id: {{ _neq: null }}
                }}) {{ retry_parent_request_doc_id }}
                pending: AgentRequest(filter: {{
                    agent_did: {{ _eq: "{agent_did}" }},
                    status: {{ _eq: "pending" }},
                    lifecycle_state: {{ _eq: "pending" }}
                }}) {{ session_id metadata }}
            }}"#
        ))
        .await;
    if response.has_errors() {
        anyhow::bail!("querying failed background wakes: {:?}", response.errors);
    }
    let data = response.data.context("background wake query has no data")?;
    Ok((
        serde_json::from_value(data["failed"].clone()).context("decoding failed wakes")?,
        serde_json::from_value(data["successors"].clone()).context("decoding successors")?,
        serde_json::from_value(data["pending"].clone()).context("decoding pending wakes")?,
    ))
}

fn clean(value: Option<String>) -> Option<String> {
    value.and_then(|value| (!value.trim().is_empty()).then_some(value))
}

fn automated_queue_key(hints: &super::queue::QueueHints) -> Option<String> {
    (hints.source == QueueSource::BackgroundCompletion && hints.policy == QueuePolicy::Coalesce)
        .then(|| clean(hints.key.clone()))
        .flatten()
}

fn eligible_queue_key(candidate: &FailedWakeRow) -> Option<String> {
    if candidate.retry_count < 0 || candidate.retry_count >= candidate.max_retries {
        return None;
    }
    let hints = parse_queue_hints(candidate.metadata.as_deref())?;
    if super::is_deprecated_background_completion_request(
        Some("scheduled"),
        candidate.metadata.as_deref(),
    ) {
        return None;
    }
    automated_queue_key(&hints)
}

pub fn background_wake_retry_delay(retry_count: i64) -> chrono::Duration {
    let exponent = u32::try_from(retry_count.max(0))
        .unwrap_or(u32::MAX)
        .min(30);
    let multiplier = 1_i64.checked_shl(exponent).unwrap_or(i64::MAX);
    chrono::Duration::seconds(
        BACKGROUND_WAKE_RETRY_BASE_SECONDS
            .saturating_mul(multiplier)
            .min(BACKGROUND_WAKE_RETRY_MAX_SECONDS),
    )
}

pub fn background_wake_next_retry_at(
    terminalized_at: Option<&str>,
    retry_count: i64,
) -> Option<chrono::DateTime<chrono::Utc>> {
    let terminalized_at = chrono::DateTime::parse_from_rfc3339(terminalized_at?)
        .ok()?
        .with_timezone(&chrono::Utc);
    Some(terminalized_at + background_wake_retry_delay(retry_count))
}

fn retry_is_due(candidate: &FailedWakeRow, now: chrono::DateTime<chrono::Utc>) -> bool {
    background_wake_next_retry_at(candidate.terminalized_at.as_deref(), candidate.retry_count)
        .is_none_or(|next_retry_at| next_retry_at <= now)
}

async fn redrive_one(node: &EmbeddedNode, candidate: &FailedWakeRow) -> Result<RedriveOutcome> {
    let request_id = uuid::Uuid::new_v4().to_string();
    let mut last_error = None;
    for retry_index in 0..=crate::graphql::DEFRA_DB_CONFLICT_MAX_RETRIES {
        let txn = ConfigApplyTxn::begin_local(node, None).await?;
        let attempt = redrive_in_transaction(&txn, candidate, &request_id).await;
        let result = match attempt {
            Ok(outcome) => txn.commit().await.map(|()| outcome),
            Err(error) => {
                let _ = txn.discard().await;
                Err(error)
            }
        };
        match result {
            Ok(outcome) => return Ok(outcome),
            Err(error)
                if retry_index < crate::graphql::DEFRA_DB_CONFLICT_MAX_RETRIES
                    && retryable_transaction_error(&error) =>
            {
                let backoff = crate::graphql::defradb_conflict_retry_backoff(retry_index);
                last_error = Some(error);
                tokio::time::sleep(backoff).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("background wake redrive exhausted")))
}

fn retryable_transaction_error(error: &anyhow::Error) -> bool {
    let text = error.to_string().to_ascii_lowercase();
    crate::graphql::is_defradb_transaction_conflict_text(&text)
        || text.contains("unique")
        || text.contains("constraint")
        || text.contains("database is locked")
        || text.contains("compare-and-set lost")
}

async fn redrive_in_transaction(
    txn: &ConfigApplyTxn<'_>,
    candidate: &FailedWakeRow,
    request_id: &str,
) -> Result<RedriveOutcome> {
    let retry_key = format!("retry:doc:{}", candidate.doc_id);
    let response = txn
        .execute(&precondition_query(candidate, &retry_key))
        .await?;
    let data = response.get("data").context("redrive query has no data")?;
    if rows(data, "successor").is_some_and(|rows| !rows.is_empty()) {
        return Ok(RedriveOutcome::AlreadyCreated);
    }
    if rows(data, "source").map_or(0, <[_]>::len) != 1 {
        return Ok(RedriveOutcome::Ineligible);
    }
    let queue_key = eligible_queue_key(candidate).context("wake became ineligible")?;
    if rows(data, "pending").into_iter().flatten().any(|row| {
        parse_queue_hints(row.get("metadata").and_then(serde_json::Value::as_str))
            .and_then(|hints| automated_queue_key(&hints))
            .is_some_and(|key| key == queue_key)
    }) {
        return Ok(RedriveOutcome::Coalesced);
    }

    let mut conversations: Vec<ConversationRow> = serde_json::from_value(
        data.get("conversations")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([])),
    )
    .context("decoding background wake conversations")?;
    conversations.sort_by(|left, right| right.rank().cmp(&left.rank()));
    let Some(conversation) = conversations.first() else {
        return Ok(RedriveOutcome::Ineligible);
    };
    if conversation.latest_request_id != candidate.request_id {
        return Ok(RedriveOutcome::Ineligible);
    }

    let response = txn
        .execute(&redrive_mutation(
            candidate,
            conversation,
            request_id,
            &retry_key,
        ))
        .await?;
    let data = response
        .get("data")
        .context("redrive mutation has no data")?;
    let created = data
        .get("successor")
        .is_some_and(crate::graphql::response_has_documents);
    let updated = data
        .get("conversation")
        .is_some_and(crate::graphql::response_has_documents);
    anyhow::ensure!(
        created && updated,
        "background wake redrive compare-and-set lost"
    );
    Ok(RedriveOutcome::Created {
        request_id: request_id.to_string(),
    })
}

fn rows<'a>(data: &'a serde_json::Value, name: &str) -> Option<&'a [serde_json::Value]> {
    data.get(name)
        .and_then(serde_json::Value::as_array)
        .map(Vec::as_slice)
}

fn precondition_query(candidate: &FailedWakeRow, retry_key: &str) -> String {
    let doc_id = escape_graphql_string(&candidate.doc_id);
    let agent_did = escape_graphql_string(&candidate.agent_did);
    let session_id = escape_graphql_string(&candidate.session_id);
    let retry_key = escape_graphql_string(retry_key);
    format!(
        r#"{{
            source: AgentRequest(filter: {{
                _docID: {{ _eq: "{doc_id}" }}, agent_did: {{ _eq: "{agent_did}" }},
                status: {{ _eq: "error" }}, lifecycle_state: {{ _eq: "failed" }},
                execution_origin: {{ _eq: "scheduled" }}
            }}, limit: 1) {{ _docID }}
            successor: AgentRequest(
                filter: {{ retry_key: {{ _eq: "{retry_key}" }} }}, limit: 1
            ) {{ _docID }}
            pending: AgentRequest(filter: {{
                session_id: {{ _eq: "{session_id}" }}, agent_did: {{ _eq: "{agent_did}" }},
                status: {{ _eq: "pending" }}, lifecycle_state: {{ _eq: "pending" }}
            }}) {{ metadata }}
            conversations: AgentConversation(filter: {{
                session_id: {{ _eq: "{session_id}" }}, agent_did: {{ _eq: "{agent_did}" }}
            }}) {{ _docID latest_request_id updated_at title preview_text }}
        }}"#
    )
}

fn optional_field<T: std::fmt::Display>(name: &str, value: Option<T>) -> String {
    value
        .map(|value| format!("{name}: {value},"))
        .unwrap_or_default()
}

fn optional_string_field(name: &str, value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| format!(r#"{name}: "{}","#, escape_graphql_string(value)))
        .unwrap_or_default()
}

fn redrive_mutation(
    candidate: &FailedWakeRow,
    conversation: &ConversationRow,
    request_id: &str,
    retry_key: &str,
) -> String {
    let now = chrono::Utc::now().to_rfc3339();
    let retry_root = candidate
        .retry_root_request
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(&candidate.request_id);
    let requester_did = session::requester_did_create_field(candidate.requester_did.as_deref());
    let parent_request_id = optional_string_field(
        "caused_by_parent_request_id",
        candidate.caused_by_parent_request_id.as_deref(),
    );
    let parent_request_doc_id = optional_string_field(
        "caused_by_parent_request_doc_id",
        candidate.caused_by_parent_request_doc_id.as_deref(),
    );
    format!(
        r#"mutation {{
            successor: create_AgentRequest(input: {{
                request_id: "{request_id}", agent_did: "{agent_did}", {requester_did}
                behavior_id: "{behavior_id}", session_id: "{session_id}",
                retry_parent_request: "{source_id}",
                retry_parent_request_doc_id: "{source_doc_id}",
                retry_root_request: "{retry_root}", retry_key: "{retry_key}",
                superseded_by_request: "", content: "{content}",
                {temperature} {top_p} {top_k} {seed} {max_tokens} {max_total_tokens}
                metadata: "{metadata}", status: "pending", lifecycle_state: "pending",
                backend_id: "{backend_id}", execution_origin: "scheduled", failure_reason: "",
                terminal_redrive_attempts: 0, created_at: "{created_at}",
                retry_count: {retry_count}, max_retries: {max_retries},
                subagent_depth: {subagent_depth}, {parent_request_id} {parent_request_doc_id}
            }}) {{ _docID }}
            conversation: update_AgentConversation(
                filter: {{ _docID: {{ _eq: "{conversation_doc_id}" }},
                    latest_request_id: {{ _eq: "{source_id}" }} }},
                input: {{ latest_request_id: "{request_id}", preview_text: "{content}",
                    status: "active", updated_at: "{created_at}" }}
            ) {{ _docID }}
        }}"#,
        request_id = escape_graphql_string(request_id),
        agent_did = escape_graphql_string(&candidate.agent_did),
        behavior_id = escape_graphql_string(&candidate.behavior_id),
        session_id = escape_graphql_string(&candidate.session_id),
        source_id = escape_graphql_string(&candidate.request_id),
        source_doc_id = escape_graphql_string(&candidate.doc_id),
        retry_root = escape_graphql_string(retry_root),
        retry_key = escape_graphql_string(retry_key),
        content = escape_graphql_string(BACKGROUND_COMPLETION_WAKE_PROMPT),
        temperature = optional_field("temperature", candidate.temperature),
        top_p = optional_field("top_p", candidate.top_p),
        top_k = optional_field("top_k", candidate.top_k),
        seed = optional_field("seed", candidate.seed),
        max_tokens = optional_field("max_tokens", candidate.max_tokens),
        max_total_tokens = optional_field("max_total_tokens", candidate.max_total_tokens),
        metadata = escape_graphql_string(candidate.metadata.as_deref().unwrap_or("")),
        backend_id = escape_graphql_string(candidate.backend_id.as_deref().unwrap_or("")),
        created_at = escape_graphql_string(&now),
        retry_count = candidate.retry_count + 1,
        max_retries = candidate.max_retries,
        subagent_depth = candidate.subagent_depth.unwrap_or_default(),
        conversation_doc_id = escape_graphql_string(&conversation.doc_id),
    )
}
