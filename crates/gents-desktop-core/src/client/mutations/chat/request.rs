use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use defra_node::EmbeddedNode;
use gents::config_client::ConfigApplyTxn;
use gents::skills::prompt_slash_skill_selection;
use gents_protocol::row::AgentRequestRow;
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::client::store::ClientStore;

use super::super::graphql::{
    escape_graphql_string, execute_mutation, normalize_optional_string, normalize_required,
};
use super::binding::resolve_agent_binding;

const DEFAULT_REQUEST_MAX_RETRIES: u32 = 3;
const RETRY_TRANSACTION_ATTEMPTS: usize = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmittedRequest {
    pub request_id: String,
    pub session_id: String,
    pub agent_did: String,
    pub behavior_id: Option<String>,
}

/// Optional submission-time controls. All fields default to "unset"; the
/// caller opts in to TTL enforcement or retry threading by populating them.
#[derive(Debug, Clone, Default)]
pub struct SubmitRequestOptions {
    /// When set, written to the request's `valid_until` field. The runtime's
    /// admission/scheduler layers treat requests past this deadline as `Stale`.
    /// None means no TTL is recorded on this row.
    pub valid_until: Option<DateTime<Utc>>,
    /// When this submission is a resend (or otherwise links to a prior
    /// request), the parent request id is threaded into `retry_parent_request`
    /// and the parent's retry root is carried forward into `retry_root_request`.
    pub retry_parent_request: Option<String>,
    /// Sampling override: if set, written to the request's `temperature` field.
    pub temperature: Option<f64>,
    /// Sampling override: if set, written to the request's `top_p` field.
    pub top_p: Option<f64>,
    /// Sampling override: if set, written to the request's `top_k` field.
    pub top_k: Option<i64>,
    /// Sampling override: if set, written to the request's `seed` field.
    pub seed: Option<i64>,
    /// Sampling override: if set, written to the request's `max_tokens` field.
    pub max_tokens: Option<i64>,
    /// Positive provider-token allowance shared by every completion call made
    /// for this durable request.
    pub max_total_tokens: Option<i64>,
    /// Free-form metadata attached to the request (submitter-defined JSON/string).
    pub metadata: Option<String>,
}

pub async fn submit_request(
    node: &EmbeddedNode,
    store: &ClientStore,
    session_id: &str,
    agent_did: &str,
    requester_did: &str,
    content: &str,
    behavior_id: Option<&str>,
    options: SubmitRequestOptions,
) -> Result<SubmittedRequest> {
    let session_id = normalize_required("session_id", session_id)?;
    let agent_did = normalize_required("agent_did", agent_did)?;
    let requester_did = normalize_required("requester_did", requester_did)?;
    let content = normalize_required("content", content)?;
    if options.seed.is_some_and(|seed| seed < 0) {
        bail!("seed must be non-negative");
    }
    if options.max_total_tokens.is_some_and(|limit| limit <= 0) {
        bail!("max_total_tokens must be positive");
    }
    let (content, options) = prepare_prompt_submission(content, options)?;
    let request_id = Uuid::new_v4().to_string();
    let created_at = Utc::now().to_rfc3339();
    let binding = resolve_agent_binding(store, agent_did, behavior_id, Some(session_id))?;

    // Thread retry linkage: carry parent's retry root forward, else this row is
    // the root of its own retry chain.
    let (retry_parent_request, retry_parent_request_doc_id, retry_root_request) =
        if let Some(parent_id) = options.retry_parent_request.as_deref() {
            let parent = fetch_retry_lineage(node, parent_id, agent_did, requester_did).await?;
            (
                parent_id.to_string(),
                Some(parent.doc_id),
                parent
                    .retry_root_request
                    .unwrap_or_else(|| parent_id.to_string()),
            )
        } else {
            (String::new(), None, request_id.clone())
        };

    let request_field = build_add_agent_request_field(
        "request",
        &request_id,
        agent_did,
        requester_did,
        binding.behavior_id.as_deref().unwrap_or(""),
        session_id,
        &retry_parent_request,
        retry_parent_request_doc_id.as_deref(),
        &retry_root_request,
        &content,
        &created_at,
        0,
        i64::from(DEFAULT_REQUEST_MAX_RETRIES),
        "",
        "interactive",
        &submit_request_extra_fields(&options),
    );
    let mutation = format!("mutation {{\n{request_field}\n}}");
    execute_mutation(node, &mutation, "submit_request").await?;

    Ok(SubmittedRequest {
        request_id,
        session_id: session_id.to_string(),
        agent_did: agent_did.to_string(),
        behavior_id: binding.behavior_id,
    })
}

fn prepare_prompt_submission(
    content: &str,
    mut options: SubmitRequestOptions,
) -> Result<(String, SubmitRequestOptions)> {
    let selection = prompt_slash_skill_selection(content);
    if selection.selected_skill_ids.is_empty() {
        return Ok((content.to_string(), options));
    }

    options.metadata = Some(merge_selected_skill_metadata(
        options.metadata.take(),
        &selection.selected_skill_ids,
    )?);
    Ok((selection.prompt, options))
}

fn merge_selected_skill_metadata(
    metadata: Option<String>,
    selected_skill_ids: &[String],
) -> Result<String> {
    let mut value = match metadata
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(raw) => serde_json::from_str::<Value>(raw)
            .with_context(|| "request metadata must be valid JSON to add selected_skill_ids")?,
        None => Value::Object(Map::new()),
    };

    let object = value
        .as_object_mut()
        .context("request metadata must be a JSON object to add selected_skill_ids")?;
    object.insert(
        "selected_skill_ids".to_string(),
        Value::Array(
            selected_skill_ids
                .iter()
                .cloned()
                .map(Value::String)
                .collect(),
        ),
    );

    serde_json::to_string(&value).context("serializing selected skill metadata")
}

pub async fn retry_request(
    node: &EmbeddedNode,
    store: &ClientStore,
    parent: &AgentRequestRow,
    requester_did: &str,
) -> Result<SubmittedRequest> {
    retry_request_with_request_id(
        node,
        store,
        parent,
        requester_did,
        Uuid::new_v4().to_string(),
    )
    .await
}

async fn retry_request_with_request_id(
    node: &EmbeddedNode,
    store: &ClientStore,
    parent: &AgentRequestRow,
    requester_did: &str,
    request_id: String,
) -> Result<SubmittedRequest> {
    let request_id = normalize_required("new_request_id", &request_id)?.to_string();
    let parent_request_id = normalize_required("request_id", &parent.request_id)?;
    let agent_did = normalize_required(
        "agent_did",
        parent
            .agent_did
            .as_deref()
            .context("retry parent request must have an agent_did")?,
    )?;
    let requester_did = normalize_required("requester_did", requester_did)?;

    let mut last_error = None;
    for attempt in 0..RETRY_TRANSACTION_ATTEMPTS {
        let txn = ConfigApplyTxn::begin_local(node, None).await?;
        match retry_request_in_txn(
            &txn,
            store,
            parent_request_id,
            agent_did,
            requester_did,
            &request_id,
        )
        .await
        {
            Ok(submitted) => match txn.commit().await {
                Ok(()) => return Ok(submitted),
                Err(error) => {
                    last_error = Some(error.context("committing retry transaction"));
                }
            },
            Err(error) => {
                let retryable = retry_transaction_error_is_retryable(&error);
                let _ = txn.discard().await;
                if !retryable {
                    return Err(error);
                }
                last_error = Some(error);
            }
        }
        if attempt + 1 < RETRY_TRANSACTION_ATTEMPTS {
            tokio::time::sleep(gents::retry::defradb_conflict_retry_backoff(attempt as u32)).await;
        }
    }
    Err(last_error
        .unwrap_or_else(|| anyhow::anyhow!("retry transaction exhausted without an error")))
}

async fn retry_request_in_txn(
    txn: &ConfigApplyTxn<'_>,
    store: &ClientStore,
    parent_request_id: &str,
    agent_did: &str,
    requester_did: &str,
    candidate_request_id: &str,
) -> Result<SubmittedRequest> {
    let (parent_doc_id, parent) =
        load_retry_parent_in_txn(txn, parent_request_id, agent_did, requester_did).await?;
    let retry_key = retry_successor_key(&parent_doc_id);
    if let Some(existing) = load_retry_successor_in_txn(txn, &retry_key).await? {
        return Ok(existing);
    }
    let parent_session_id = normalize_required(
        "session_id",
        parent
            .session_id
            .as_deref()
            .context("retry parent request must have a session_id")?,
    )?;
    let content = normalize_required(
        "content",
        parent
            .content
            .as_deref()
            .context("retry parent request must have content")?,
    )?;
    let execution_origin = normalize_required(
        "execution_origin",
        parent
            .execution_origin
            .as_deref()
            .context("retry parent request must have an execution_origin")?,
    )?;
    let retry_count = parent.retry_count.unwrap_or_default() + 1;
    let max_retries = parent
        .max_retries
        .unwrap_or(i64::from(DEFAULT_REQUEST_MAX_RETRIES));
    ensure_retry_parent_eligible(&parent, retry_count - 1, max_retries, execution_origin)?;

    let effective_latest_request_id =
        latest_interactive_request_in_txn(txn, parent_session_id, agent_did, requester_did).await?;
    if effective_latest_request_id != parent_request_id {
        bail!(
            "retry parent request must be latest for session {parent_session_id}, got latest_request_id={effective_latest_request_id}"
        );
    }
    ensure_retry_candidate_is_fresh_in_txn(txn, parent_session_id, candidate_request_id).await?;

    let behavior_id = normalize_optional_string(parent.behavior_id.as_deref());
    let retry_root_request = normalize_optional_string(parent.retry_root_request.as_deref())
        .unwrap_or(parent_request_id);
    let backend_id = normalize_optional_string(parent.backend_id.as_deref()).unwrap_or("");
    let created_at = Utc::now().to_rfc3339();
    let binding = resolve_agent_binding(store, agent_did, behavior_id, Some(parent_session_id))?;
    let retry_extra_fields = format!(
        "{},\n                retry_key: \"{}\"",
        submit_request_extra_fields(&SubmitRequestOptions {
            temperature: parent.temperature,
            top_p: parent.top_p,
            top_k: parent.top_k,
            seed: parent.seed,
            max_tokens: parent.max_tokens,
            max_total_tokens: parent.max_total_tokens,
            metadata: parent.metadata.clone(),
            ..SubmitRequestOptions::default()
        }),
        escape_graphql_string(&retry_key),
    );
    let request_field = build_add_agent_request_field(
        "request",
        candidate_request_id,
        agent_did,
        requester_did,
        binding.behavior_id.as_deref().unwrap_or(""),
        parent_session_id,
        parent_request_id,
        Some(&parent_doc_id),
        retry_root_request,
        content,
        &created_at,
        retry_count,
        max_retries,
        backend_id,
        execution_origin,
        &retry_extra_fields,
    );
    txn.execute(&format!("mutation {{\n{request_field}\n}}"))
        .await?;

    Ok(SubmittedRequest {
        request_id: candidate_request_id.to_string(),
        session_id: parent_session_id.to_string(),
        agent_did: agent_did.to_string(),
        behavior_id: binding.behavior_id,
    })
}

fn retry_successor_key(parent_request_doc_id: &str) -> String {
    format!("retry:doc:{parent_request_doc_id}")
}

fn retry_transaction_error_is_retryable(error: &anyhow::Error) -> bool {
    let text = error.to_string().to_ascii_lowercase();
    gents::retry::is_defradb_transaction_conflict_text(&text)
        || text.contains("unique")
        || text.contains("constraint")
        || text.contains("database is locked")
}

async fn load_retry_successor_in_txn(
    txn: &ConfigApplyTxn<'_>,
    retry_key: &str,
) -> Result<Option<SubmittedRequest>> {
    let retry_key = escape_graphql_string(retry_key);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ retry_key: {{ _eq: "{retry_key}" }} }},
                limit: 1
            ) {{
                request_id
                session_id
                agent_did
                behavior_id
            }}
        }}"#
    );
    let response = txn.execute(&query).await?;
    let Some(row) = response
        .get("data")
        .and_then(|data| data.get("AgentRequest"))
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
    else {
        return Ok(None);
    };
    Ok(Some(SubmittedRequest {
        request_id: row
            .get("request_id")
            .and_then(Value::as_str)
            .context("retry successor has no request_id")?
            .to_string(),
        session_id: row
            .get("session_id")
            .and_then(Value::as_str)
            .context("retry successor has no session_id")?
            .to_string(),
        agent_did: row
            .get("agent_did")
            .and_then(Value::as_str)
            .context("retry successor has no agent_did")?
            .to_string(),
        behavior_id: row
            .get("behavior_id")
            .and_then(Value::as_str)
            .and_then(|value| normalize_optional_string(Some(value)))
            .map(str::to_string),
    }))
}

async fn load_retry_parent_in_txn(
    txn: &ConfigApplyTxn<'_>,
    parent_request_id: &str,
    agent_did: &str,
    requester_did: &str,
) -> Result<(String, AgentRequestRow)> {
    let parent_request_id = escape_graphql_string(parent_request_id);
    let agent_did = escape_graphql_string(agent_did);
    let requester_did = escape_graphql_string(requester_did);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{
                    request_id: {{ _eq: "{parent_request_id}" }},
                    agent_did: {{ _eq: "{agent_did}" }},
                    requester_did: {{ _eq: "{requester_did}" }}
                }},
                limit: 2
            ) {{
                _docID
                request_id
                agent_did
                requester_did
                behavior_id
                session_id
                retry_parent_request
                retry_root_request
                superseded_by_request
                content
                temperature
                top_p
                top_k
                seed
                max_tokens
                max_total_tokens
                metadata
                status
                lifecycle_state
                backend_id
                execution_origin
                caused_by_trigger_id
                caused_by_trigger_kind
                caused_by_parent_request_id
                failure_reason
                terminalized_at
                terminal_redrive_attempts
                created_at
                claimed_at
                deadline
                retry_count
                max_retries
                interrupt_requested_at
                valid_until
            }}
        }}"#
    );
    let response = txn.execute(&query).await?;
    let rows = response
        .get("data")
        .and_then(|data| data.get("AgentRequest"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    match rows.len() {
        0 => bail!("retry parent request not found"),
        1 => {}
        count => bail!("retry parent request_id is ambiguous across {count} documents"),
    }
    let row = rows
        .into_iter()
        .next()
        .context("retry parent request absent")?;
    let doc_id = row
        .get("_docID")
        .and_then(Value::as_str)
        .context("retry parent request has no _docID")?
        .to_string();
    let request = serde_json::from_value(row).context("decoding retry parent request")?;
    Ok((doc_id, request))
}

async fn latest_interactive_request_in_txn(
    txn: &ConfigApplyTxn<'_>,
    session_id: &str,
    agent_did: &str,
    requester_did: &str,
) -> Result<String> {
    let agent = escape_graphql_string(agent_did);
    let requester = escape_graphql_string(requester_did);
    let session = escape_graphql_string(session_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{
                    session_id: {{ _eq: "{session}" }},
                    agent_did: {{ _eq: "{agent}" }},
                    requester_did: {{ _eq: "{requester}" }}
                }},
                order: [{{ created_at: DESC }}, {{ request_id: DESC }}]
            ) {{ request_id execution_origin metadata }}
        }}"#
    );
    let response = txn.execute(&query).await?;
    response
        .get("data")
        .and_then(|data| data.get("AgentRequest"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|row| {
            !gents::lifecycle::is_deprecated_background_completion_request(
                row.get("execution_origin").and_then(Value::as_str),
                row.get("metadata").and_then(Value::as_str),
            )
        })
        .and_then(|row| row.get("request_id"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .context("retry parent session has no non-legacy request")
}

async fn ensure_retry_candidate_is_fresh_in_txn(
    txn: &ConfigApplyTxn<'_>,
    session_id: &str,
    candidate_request_id: &str,
) -> Result<()> {
    let session_id = escape_graphql_string(session_id);
    let candidate_request_id = escape_graphql_string(candidate_request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{
                    session_id: {{ _eq: "{session_id}" }},
                    request_id: {{ _eq: "{candidate_request_id}" }}
                }},
                limit: 1
            ) {{ _docID }}
        }}"#
    );
    let response = txn.execute(&query).await?;
    let exists = response
        .get("data")
        .and_then(|data| data.get("AgentRequest"))
        .and_then(Value::as_array)
        .is_some_and(|rows| !rows.is_empty());
    if exists {
        bail!("retry new request id already exists: request_id={candidate_request_id}");
    }
    Ok(())
}

fn ensure_retry_parent_eligible(
    parent: &AgentRequestRow,
    parent_retry_count: i64,
    max_retries: i64,
    execution_origin: &str,
) -> Result<()> {
    // The Lean `.released` admission predicate is not persisted on
    // `AgentRequestRow`; on this desktop surface it is represented by requiring
    // the parent to be terminal failed/error. Non-terminal rows, including rows
    // still waiting on admission, fail this lifecycle/status gate.
    let lifecycle_state = normalize_required(
        "lifecycle_state",
        parent
            .lifecycle_state
            .as_deref()
            .context("retry parent request must have a lifecycle_state")?,
    )?;
    let status = normalize_required(
        "status",
        parent
            .status
            .as_deref()
            .context("retry parent request must have a status")?,
    )?;

    if lifecycle_state != "failed" || status != "error" {
        bail!(
            "retry parent request must be failed/error, got lifecycle_state={lifecycle_state} status={status}"
        );
    }
    if execution_origin != "interactive" {
        bail!(
            "client retry parent request must be interactive, got execution_origin={execution_origin}"
        );
    }
    if parent_retry_count >= max_retries {
        bail!(
            "retry parent request exhausted retry budget: retry_count={parent_retry_count} max_retries={max_retries}"
        );
    }
    if let Some(deadline) = normalize_optional_string(parent.deadline.as_deref()) {
        let deadline = DateTime::parse_from_rfc3339(deadline)
            .with_context(|| format!("retry parent request has invalid deadline {deadline:?}"))?
            .with_timezone(&Utc);
        if Utc::now() > deadline {
            bail!(
                "retry parent request deadline is closed: deadline={}",
                deadline.to_rfc3339()
            );
        }
    }

    Ok(())
}

fn submit_request_extra_fields(options: &SubmitRequestOptions) -> String {
    let valid_until_field = match options.valid_until.as_ref() {
        Some(valid_until) => {
            let escaped = escape_graphql_string(&valid_until.to_rfc3339());
            format!(
                r#",
                valid_until: "{escaped}""#,
            )
        }
        None => String::new(),
    };

    // Only emit sampling override + metadata fields when the caller actually
    // set them. Omitting a field leaves the schema default (null) in place;
    // emitting `null` explicitly also works but leaving the field out keeps
    // the mutation shape identical to what previously-submitted rows used
    // before the override plumbing landed.
    let mut override_parts: Vec<String> = Vec::new();
    if let Some(temperature) = options.temperature {
        override_parts.push(format!("temperature: {temperature}"));
    }
    if let Some(top_p) = options.top_p {
        override_parts.push(format!("top_p: {top_p}"));
    }
    if let Some(top_k) = options.top_k {
        override_parts.push(format!("top_k: {top_k}"));
    }
    if let Some(seed) = options.seed {
        override_parts.push(format!("seed: {seed}"));
    }
    if let Some(max_tokens) = options.max_tokens {
        override_parts.push(format!("max_tokens: {max_tokens}"));
    }
    if let Some(max_total_tokens) = options.max_total_tokens {
        override_parts.push(format!("max_total_tokens: {max_total_tokens}"));
    }
    if let Some(metadata) = options.metadata.as_deref() {
        override_parts.push(format!(
            r#"metadata: "{}""#,
            escape_graphql_string(metadata)
        ));
    }
    let override_fields = if override_parts.is_empty() {
        String::new()
    } else {
        format!(
            ",\n                {}",
            override_parts.join(",\n                ")
        )
    };

    format!("{valid_until_field}{override_fields}")
}

#[allow(clippy::too_many_arguments)]
fn build_add_agent_request_field(
    alias: &str,
    request_id: &str,
    agent_did: &str,
    requester_did: &str,
    behavior_id: &str,
    session_id: &str,
    retry_parent_request: &str,
    retry_parent_request_doc_id: Option<&str>,
    retry_root_request: &str,
    content: &str,
    created_at: &str,
    retry_count: i64,
    max_retries: i64,
    backend_id: &str,
    execution_origin: &str,
    extra_fields: &str,
) -> String {
    let escaped_request_id = escape_graphql_string(request_id);
    let escaped_agent_did = escape_graphql_string(agent_did);
    let escaped_requester_did = escape_graphql_string(requester_did);
    let escaped_behavior_id = escape_graphql_string(behavior_id);
    let escaped_session_id = escape_graphql_string(session_id);
    let escaped_retry_parent = escape_graphql_string(retry_parent_request);
    let retry_parent_doc_field = retry_parent_request_doc_id
        .map(escape_graphql_string)
        .map(|doc_id| format!(r#"retry_parent_request_doc_id: "{doc_id}","#))
        .unwrap_or_default();
    let escaped_retry_root = escape_graphql_string(retry_root_request);
    let escaped_content = escape_graphql_string(content);
    let escaped_created_at = escape_graphql_string(created_at);
    let escaped_backend_id = escape_graphql_string(backend_id);
    let escaped_execution_origin = escape_graphql_string(execution_origin);

    format!(
        r#"{alias}: add_AgentRequest(input: {{
                request_id: "{escaped_request_id}",
                agent_did: "{escaped_agent_did}",
                requester_did: "{escaped_requester_did}",
                behavior_id: "{escaped_behavior_id}",
                session_id: "{escaped_session_id}",
                retry_parent_request: "{escaped_retry_parent}",
                {retry_parent_doc_field}
                retry_root_request: "{escaped_retry_root}",
                superseded_by_request: "",
                content: "{escaped_content}",
                status: "pending",
                lifecycle_state: "pending",
                backend_id: "{escaped_backend_id}",
                execution_origin: "{escaped_execution_origin}",
                failure_reason: "",
                created_at: "{escaped_created_at}",
                retry_count: {retry_count},
                max_retries: {max_retries}{extra_fields}
            }}) {{ _docID }}
        "#
    )
}

/// Resend a stale-terminal request by reading its inputs and submitting a
/// fresh row whose `retry_parent_request` points back at the stale one.
/// Only `lifecycle_state=dead` with `failure_reason=Stale` is eligible — any
/// other state would risk bypassing legitimate terminal classifications.
pub async fn resend_request(
    node: &EmbeddedNode,
    store: &ClientStore,
    stale_request_id: &str,
    agent_did: &str,
    requester_did: &str,
) -> Result<SubmittedRequest> {
    let stale = fetch_request_view(node, stale_request_id, agent_did, requester_did).await?;
    if stale.lifecycle_state != "dead" || stale.failure_reason != "Stale" {
        anyhow::bail!(
            "request {stale_request_id} is not a stale terminal (lifecycle_state={}, failure_reason={})",
            stale.lifecycle_state,
            stale.failure_reason
        );
    }
    let retry_session_id = Uuid::new_v4().to_string();
    submit_request(
        node,
        store,
        &retry_session_id,
        &stale.agent_did,
        requester_did,
        &stale.content,
        stale.behavior_id.as_deref(),
        SubmitRequestOptions {
            valid_until: Some(Utc::now() + chrono::Duration::minutes(5)),
            retry_parent_request: Some(stale_request_id.to_string()),
            // Preserve sampling overrides + metadata from the stale row.
            // Dropping them would silently change model behavior on retry.
            temperature: stale.temperature,
            top_p: stale.top_p,
            top_k: stale.top_k,
            seed: stale.seed,
            max_tokens: stale.max_tokens,
            max_total_tokens: stale.max_total_tokens,
            metadata: stale.metadata.clone(),
        },
    )
    .await
}

/// Minimal projection of an AgentRequest used by resend to copy over inputs.
/// Carries sampling overrides + metadata so resend preserves submitter intent.
struct StaleRequestView {
    agent_did: String,
    behavior_id: Option<String>,
    content: String,
    lifecycle_state: String,
    failure_reason: String,
    temperature: Option<f64>,
    top_p: Option<f64>,
    top_k: Option<i64>,
    seed: Option<i64>,
    max_tokens: Option<i64>,
    max_total_tokens: Option<i64>,
    metadata: Option<String>,
}

async fn fetch_request_view(
    node: &EmbeddedNode,
    request_id: &str,
    agent_did: &str,
    requester_did: &str,
) -> Result<StaleRequestView> {
    let escaped = escape_graphql_string(request_id);
    let agent_did = escape_graphql_string(agent_did);
    let requester_did = escape_graphql_string(requester_did);
    let query = format!(
        r#"query {{
            AgentRequest(
                filter: {{
                    request_id: {{ _eq: "{escaped}" }},
                    agent_did: {{ _eq: "{agent_did}" }},
                    requester_did: {{ _eq: "{requester_did}" }}
                }},
                limit: 1
            ) {{
                agent_did
                behavior_id
                content
                lifecycle_state
                failure_reason
                temperature
                top_p
                top_k
                seed
                max_tokens
                max_total_tokens
                metadata
            }}
        }}"#
    );
    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("fetch_request({request_id}) failed: {:?}", resp.errors);
    }
    let row = resp
        .data
        .as_ref()
        .and_then(|d| d.get("AgentRequest"))
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .ok_or_else(|| anyhow::anyhow!("request {request_id} not found"))?;
    Ok(StaleRequestView {
        agent_did: row
            .get("agent_did")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        behavior_id: row
            .get("behavior_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(String::from),
        content: row
            .get("content")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        lifecycle_state: row
            .get("lifecycle_state")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        failure_reason: row
            .get("failure_reason")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        temperature: row.get("temperature").and_then(|v| v.as_f64()),
        top_p: row.get("top_p").and_then(|v| v.as_f64()),
        top_k: row.get("top_k").and_then(|v| v.as_i64()),
        seed: row.get("seed").and_then(|v| v.as_i64()),
        max_tokens: row.get("max_tokens").and_then(|v| v.as_i64()),
        max_total_tokens: row.get("max_total_tokens").and_then(|v| v.as_i64()),
        metadata: row
            .get("metadata")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(String::from),
    })
}

struct RetryLineage {
    doc_id: String,
    retry_root_request: Option<String>,
}

async fn fetch_retry_lineage(
    node: &EmbeddedNode,
    request_id: &str,
    agent_did: &str,
    requester_did: &str,
) -> Result<RetryLineage> {
    let escaped = escape_graphql_string(request_id);
    let agent_did = escape_graphql_string(agent_did);
    let requester_did = escape_graphql_string(requester_did);
    let query = format!(
        r#"query {{
            AgentRequest(
                filter: {{
                    request_id: {{ _eq: "{escaped}" }},
                    agent_did: {{ _eq: "{agent_did}" }},
                    requester_did: {{ _eq: "{requester_did}" }}
                }},
                limit: 2
            ) {{
                _docID
                retry_root_request
            }}
        }}"#
    );
    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!(
            "fetch_retry_lineage({request_id}) failed: {:?}",
            resp.errors
        );
    }
    let rows = resp
        .data
        .as_ref()
        .and_then(|d| d.get("AgentRequest"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    match rows.len() {
        0 => bail!("retry parent request not found"),
        1 => {}
        count => bail!("retry parent request_id is ambiguous across {count} documents"),
    }
    let row = rows
        .into_iter()
        .next()
        .context("retry parent request absent")?;
    Ok(RetryLineage {
        doc_id: row
            .get("_docID")
            .and_then(Value::as_str)
            .context("retry parent request has no _docID")?
            .to_string(),
        retry_root_request: normalize_optional_string(
            row.get("retry_root_request").and_then(Value::as_str),
        )
        .map(str::to_string),
    })
}

#[cfg(test)]
#[path = "../../../../../gents/src/lean_vocab_test/support.rs"]
mod lean_vocab_test;

#[cfg(test)]
mod tests;
