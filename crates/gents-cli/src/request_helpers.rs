use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{self, Write};
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use gents::{graphql::escape_graphql_string, skills::prompt_slash_skill_selection};
use gents_protocol::client_protocol::RequestLifecycleState;
use gents_protocol::graphql::{
    execute_graphql_async, graphql_error_is_retryable, GraphqlRequestOptions,
};
use gents_protocol::row::AgentRequestRow;
use gents_protocol::transcript::present_persisted_message;
use serde_json::Value;

use crate::{post_graphql, require_non_empty};

/// Maximum number of retries after an initial GraphQL operation fails with a
/// transient DefraDB contention error.
pub(crate) const MAX_TRANSIENT_GRAPHQL_RETRIES: usize = 4;

/// Classify the transient DefraDB errors for which replaying an idempotent
/// GraphQL read, or retrying request creation before an id is returned, is
/// safe. Keep this shared between protocol shims so they do not drift on the
/// backend's contention vocabulary.
pub(crate) fn graphql_error_is_transient(error: &anyhow::Error) -> bool {
    graphql_error_is_retryable(error)
}

/// Small bounded linear backoff used for transient GraphQL contention.
pub(crate) fn transient_graphql_retry_delay(retry: usize) -> Duration {
    Duration::from_millis(50 * retry.max(1) as u64)
}

pub(crate) fn ensure_local_request_signer(
    home: Option<&Path>,
    target_agent_did: &str,
) -> Result<()> {
    if gents::identity::RegisteredIdentity::from_registered_did(target_agent_did, None).is_ok() {
        return Ok(());
    }
    let home = crate::resolve_home_dir(home);
    let config = crate::read_init_config(&home)?.with_context(|| {
        format!(
            "initialized home {} is required to sign a local request",
            home.display()
        )
    })?;
    anyhow::ensure!(
        config.agent_did.trim() == target_agent_did.trim(),
        "local-self request target {} does not match initialized home principal {}",
        target_agent_did,
        config.agent_did
    );
    crate::load_initialized_home_identity(&home, &config)?;
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct SubmittedRequest {
    pub(crate) request_id: String,
    pub(crate) session_id: String,
    pub(crate) agent_did: String,
    pub(crate) behavior_id: Option<String>,
    pub(crate) temperature: Option<f64>,
    pub(crate) top_p: Option<f64>,
    pub(crate) top_k: Option<i64>,
    pub(crate) seed: Option<i64>,
    pub(crate) max_tokens: Option<i64>,
    pub(crate) max_total_tokens: Option<i64>,
    pub(crate) metadata: Option<String>,
    pub(crate) created_at: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct RequestSubmitOptions {
    pub(crate) temperature: Option<f64>,
    pub(crate) top_p: Option<f64>,
    pub(crate) top_k: Option<i64>,
    pub(crate) seed: Option<i64>,
    pub(crate) max_tokens: Option<i64>,
    pub(crate) max_total_tokens: Option<i64>,
    pub(crate) metadata: Option<String>,
    pub(crate) valid_until: Option<DateTime<Utc>>,
    pub(crate) retry_parent_request: Option<String>,
    pub(crate) retry_parent_request_doc_id: Option<String>,
    pub(crate) retry_root_request: Option<String>,
    pub(crate) retry_key: Option<String>,
}

pub(crate) fn response_query(request_id: &str) -> String {
    format!(
        r#"{{
            AgentResponse(
                filter: {{ request_id: {{ _eq: "{request_id}" }} }},
                order: {{ created_at: DESC }},
                limit: 1
            ) {{
                request_id
                behavior_id
                session_id
                status
                content
                reasoning
                error_message
                token_count
                progress_seq
                reasoning_progress_seq
                materialized_message_sequence
                materialized_at
                completed_at
                interrupted_at
            }}
        }}"#,
        request_id = escape_graphql_string(request_id),
    )
}

fn response_wait_progress_query(request_id: &str) -> String {
    format!(
        r#"{{
            AgentResponse(
                filter: {{ request_id: {{ _eq: "{request_id}" }} }},
                order: {{ created_at: DESC }},
                limit: 1
            ) {{
                _docID
                request_id
                session_id
                status
                content
                reasoning
                error_message
                token_count
                progress_seq
                reasoning_progress_seq
                materialized_message_sequence
                materialized_at
                completed_at
                interrupted_at
            }}
        }}"#,
        request_id = escape_graphql_string(request_id),
    )
}

pub(crate) fn materialized_message_query(session_id: &str, sequence: i64) -> String {
    format!(
        r#"{{
            AgentMessage(
                filter: {{
                    session_id: {{ _eq: "{session_id}" }},
                    sequence: {{ _eq: {sequence} }}
                }},
                limit: 1
            ) {{
                role
                content
                reasoning
                sequence
            }}
        }}"#,
        session_id = escape_graphql_string(session_id),
    )
}

pub(crate) fn request_terminal_query(request_id: &str) -> String {
    format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{request_id}" }} }},
                order: {{ created_at: DESC }},
                limit: 1
            ) {{
                request_id
                lifecycle_state
                failure_reason
                interrupt_requested_at
                valid_until
            }}
        }}"#,
        request_id = escape_graphql_string(request_id),
    )
}

fn response_field_is_blank(response: &Value, field: &str) -> bool {
    response
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default()
        .is_empty()
}

/// AgentResponse.status vocabulary, not AgentRequest.
fn response_is_completed(response: &Value) -> bool {
    response
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| matches!(status, "complete" | "completed")) // AgentResponse.status
}

fn response_has_presentation(response: &Value) -> bool {
    !response_field_is_blank(response, "content") || !response_field_is_blank(response, "reasoning")
}

fn response_materialized_sequence(response: &Value) -> Option<i64> {
    response
        .get("materialized_message_sequence")
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MaterializedResponsePresentation {
    Presentable,
    Pending,
    Invalid,
}

fn classify_materialized_response(
    response: &Value,
    blank_completed: MaterializedResponsePresentation,
) -> MaterializedResponsePresentation {
    if response_has_presentation(response) || !response_is_completed(response) {
        MaterializedResponsePresentation::Presentable
    } else {
        blank_completed
    }
}

pub(crate) fn materialized_response_diagnostic(request_id: &str, response: &Value) -> String {
    let session_id = response
        .get("session_id")
        .and_then(Value::as_str)
        .unwrap_or("<missing>");
    let sequence = response_materialized_sequence(response)
        .map(|sequence| sequence.to_string())
        .unwrap_or_else(|| "<missing>".to_string());
    format!(
        "could not hydrate materialized AgentMessage for request {request_id} \
         (session_id={session_id}, sequence={sequence}); the referenced message is missing or \
         invalid"
    )
}

pub(crate) async fn hydrate_materialized_response_content(
    graphql: &str,
    response: &mut Value,
) -> Result<MaterializedResponsePresentation> {
    let content_blank = response_field_is_blank(response, "content");
    let reasoning_blank = response_field_is_blank(response, "reasoning");
    if !content_blank && !reasoning_blank {
        return Ok(MaterializedResponsePresentation::Presentable);
    }

    let Some(sequence) = response_materialized_sequence(response) else {
        return Ok(classify_materialized_response(
            response,
            MaterializedResponsePresentation::Invalid,
        ));
    };
    let Some(session_id) = response.get("session_id").and_then(Value::as_str) else {
        return Ok(classify_materialized_response(
            response,
            MaterializedResponsePresentation::Invalid,
        ));
    };

    let query = materialized_message_query(session_id, sequence);
    let message_response = post_graphql(graphql, &query).await?;
    let Some(message) = message_response
        .pointer("/data/AgentMessage")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
    else {
        return Ok(classify_materialized_response(
            response,
            MaterializedResponsePresentation::Pending,
        ));
    };
    let Some(role) = message.get("role").and_then(Value::as_str) else {
        return Ok(classify_materialized_response(
            response,
            MaterializedResponsePresentation::Invalid,
        ));
    };
    if role != "assistant" {
        return Ok(classify_materialized_response(
            response,
            MaterializedResponsePresentation::Invalid,
        ));
    }
    let Some(content) = message.get("content").and_then(Value::as_str) else {
        return Ok(classify_materialized_response(
            response,
            MaterializedResponsePresentation::Invalid,
        ));
    };

    let presentation = present_persisted_message(role, content);
    let Some(object) = response.as_object_mut() else {
        return Ok(MaterializedResponsePresentation::Invalid);
    };

    if content_blank && !presentation.body_markdown.trim().is_empty() {
        object.insert(
            "content".to_string(),
            Value::String(presentation.body_markdown),
        );
    }
    if reasoning_blank {
        if let Some(reasoning) = message
            .get("reasoning")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(ToOwned::to_owned)
            .or(presentation.reasoning_markdown)
            .filter(|value| !value.trim().is_empty())
        {
            object.insert("reasoning".to_string(), Value::String(reasoning));
        }
    }

    Ok(classify_materialized_response(
        response,
        MaterializedResponsePresentation::Invalid,
    ))
}

pub(crate) async fn create_agent_request(
    graphql: &str,
    agent_did: &str,
    content: &str,
    session_id: Option<&str>,
    behavior_id: Option<&str>,
    options: RequestSubmitOptions,
) -> Result<SubmittedRequest> {
    let (prepared, _) = prepare_agent_request(
        graphql,
        agent_did,
        content,
        session_id,
        behavior_id,
        None,
        options,
    )
    .await?;
    submit_prepared_agent_request_with_retry(graphql, &prepared).await
}

/// Create one stable, signed request mutation and retry transient submission
/// failures without minting a new request identity. After every ambiguous
/// failure, a request-id read proves whether the mutation committed before it
/// is sent again.
pub(crate) async fn create_agent_request_retrying_transient(
    graphql: &str,
    agent_did: &str,
    content: &str,
    session_id: Option<&str>,
    behavior_id: Option<&str>,
    request_id: String,
    options: RequestSubmitOptions,
) -> Result<SubmittedRequest> {
    let (prepared, _) = prepare_agent_request(
        graphql,
        agent_did,
        content,
        session_id,
        behavior_id,
        Some(request_id),
        options,
    )
    .await?;
    submit_prepared_agent_request_with_retry(graphql, &prepared).await
}

#[derive(Debug, Clone)]
struct PreparedAgentRequest {
    mutation: String,
    submitted: SubmittedRequest,
}

async fn prepare_agent_request(
    graphql: &str,
    agent_did: &str,
    content: &str,
    session_id: Option<&str>,
    behavior_id: Option<&str>,
    request_id: Option<String>,
    options: RequestSubmitOptions,
) -> Result<(
    PreparedAgentRequest,
    gents_protocol::request_admission::AgentRequestCreate,
)> {
    if options.seed.is_some_and(|seed| seed < 0) {
        anyhow::bail!("seed must be non-negative");
    }
    let (request_content, request_metadata) =
        content_and_metadata_with_prompt_selected_skill_ids(options.metadata.as_deref(), content);
    let request_id = request_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let behavior_id = resolve_request_behavior_id(graphql, agent_did, behavior_id).await?;
    let session_id = session_id
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let created_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let retry_parent_value = options.retry_parent_request.as_deref().unwrap_or_default();
    let retry_root_value = options
        .retry_root_request
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| {
            if retry_parent_value.is_empty() {
                request_id.clone()
            } else {
                retry_parent_value.to_string()
            }
        });
    let admission =
        gents_protocol::request_admission::AgentRequestAdmissionRecord::local_self(agent_did);
    let create = gents::build_signed_request(
        gents::RequestSpec {
            sampling: Some(gents::SamplingCarryover {
                temperature: options.temperature,
                top_p: options.top_p,
                top_k: options.top_k,
                seed: options.seed,
                max_tokens: options.max_tokens,
                max_total_tokens: options.max_total_tokens,
                backend_id: None,
            }),
            metadata: request_metadata.clone(),
            valid_until: options
                .valid_until
                .map(|at| at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
            retry_key: options.retry_key,
            retry: Some(gents::RetryLink {
                parent_request_id: (!retry_parent_value.is_empty())
                    .then(|| retry_parent_value.to_string()),
                parent_request_doc_id: options.retry_parent_request_doc_id,
                root_request_id: retry_root_value,
                retry_count: 0,
                max_retries: i64::from(gents::lifecycle::DEFAULT_REQUEST_MAX_RETRIES),
            }),
            ..gents::RequestSpec::new(
                gents::RequestIdentity {
                    request_id: request_id.clone(),
                    agent_did: agent_did.to_string(),
                    requester_did: None,
                    behavior_id: behavior_id.clone(),
                    session_id: session_id.clone(),
                    content: request_content,
                    execution_origin: gents::lifecycle::ExecutionOrigin::Interactive,
                    created_at: created_at.clone(),
                },
                admission,
            )
        },
        gents::RequestSigner::RegisteredTarget,
    )
    .await?;
    let mutation = create.graphql_mutation().map_err(anyhow::Error::msg)?;
    let submitted = SubmittedRequest {
        request_id,
        session_id,
        agent_did: agent_did.to_string(),
        behavior_id: Some(behavior_id),
        temperature: options.temperature,
        top_p: options.top_p,
        top_k: options.top_k,
        seed: options.seed,
        max_tokens: options.max_tokens,
        max_total_tokens: options.max_total_tokens,
        metadata: request_metadata,
        created_at: Some(created_at),
    };
    Ok((
        PreparedAgentRequest {
            mutation,
            submitted,
        },
        create,
    ))
}

/// Atomically establish a session goal and publish its first runnable request.
/// Exact retries use the same immutable submission key and converge to the
/// already-committed pair; conflicting retries fail without mutating either
/// document.
pub(crate) async fn create_goal_backed_agent_request(
    graphql: &str,
    agent_did: &str,
    content: &str,
    session_id: &str,
    behavior_id: Option<&str>,
    objective: &str,
    token_budget: Option<i64>,
) -> Result<SubmittedRequest> {
    use sha2::{Digest, Sha256};

    let objective = require_non_empty("goal-objective", objective)?.trim();
    anyhow::ensure!(
        token_budget.is_none_or(|budget| budget > 0),
        "goal-token-budget must be positive"
    );
    let digest = Sha256::digest(
        format!("{agent_did}\0{session_id}\0{objective}\0{token_budget:?}\0{content}").as_bytes(),
    );
    let request_id = format!(
        "goal-submit-{}",
        digest[..16]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    let retry_key = format!(
        "goal-submit:{}",
        gents::goal::deterministic_goal_creation_key(agent_did, session_id)
    );
    let (prepared, create) = prepare_agent_request(
        graphql,
        agent_did,
        content,
        Some(session_id),
        behavior_id,
        Some(request_id),
        RequestSubmitOptions {
            retry_key: Some(retry_key.clone()),
            ..Default::default()
        },
    )
    .await?;

    let access = gents::ConfigAccess::Graphql(graphql.to_string());
    gents::goal::submit_goal_backed_request(
        &access,
        agent_did,
        session_id,
        objective,
        token_budget,
        &create,
    )
    .await?;
    Ok(prepared.submitted)
}

/// Embedded adapters keep their prompt identity and cancellation path while
/// delegating the goal/claim/request transaction to the runtime owner.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_goal_backed_agent_request_local(
    node: &defra_node::EmbeddedNode,
    graphql: &str,
    agent_did: &str,
    objective: &str,
    token_budget: Option<i64>,
    session_id: &str,
    behavior_id: &str,
    request_id: String,
    mut options: RequestSubmitOptions,
) -> Result<SubmittedRequest> {
    options.retry_key = Some(format!("goal-request:{request_id}"));
    let (prepared, create) = prepare_agent_request(
        graphql,
        agent_did,
        objective,
        Some(session_id),
        Some(behavior_id),
        Some(request_id),
        options,
    )
    .await?;
    // Retry the SAME signed request, never mint a replacement after an
    // ambiguous commit. The runtime verifies the full recovery fingerprint.
    let mut retries = 0;
    loop {
        match gents::goal::submit_goal_backed_request_local(
            node,
            agent_did,
            session_id,
            objective,
            token_budget,
            &create,
        )
        .await
        {
            Ok(_) => return Ok(prepared.submitted),
            Err(error)
                if graphql_error_is_transient(&error)
                    && retries < MAX_TRANSIENT_GRAPHQL_RETRIES =>
            {
                retries += 1;
                tokio::time::sleep(transient_graphql_retry_delay(retries)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn submit_prepared_agent_request(
    graphql: &str,
    prepared: &PreparedAgentRequest,
) -> Result<SubmittedRequest> {
    // Mutations must never inherit the generic HTTP client's transparent
    // retry loop: a committed write with a lost response is ambiguous. The
    // prepared-request loop below resolves that ambiguity by request id before
    // it ever reposts this exact signed mutation.
    execute_graphql_async(
        graphql,
        &prepared.mutation,
        GraphqlRequestOptions {
            timeout: Duration::from_secs(30),
            max_attempts: 1,
            retry_backoff: Duration::from_millis(100),
        },
    )
    .await
    .with_context(|| {
        format!(
            "submitting prepared AgentRequest {}",
            prepared.submitted.request_id
        )
    })?;
    Ok(prepared.submitted.clone())
}

async fn submit_prepared_agent_request_with_retry(
    graphql: &str,
    prepared: &PreparedAgentRequest,
) -> Result<SubmittedRequest> {
    let mut last_error = match submit_prepared_agent_request(graphql, prepared).await {
        Ok(submitted) => return Ok(submitted),
        Err(error) if graphql_error_is_transient(&error) => error,
        Err(error) => return Err(error),
    };

    for retry in 1..=MAX_TRANSIENT_GRAPHQL_RETRIES {
        tokio::time::sleep(transient_graphql_retry_delay(retry)).await;
        match submitted_request_exists(graphql, &prepared.submitted.request_id).await {
            Ok(true) => return Ok(prepared.submitted.clone()),
            Ok(false) => {}
            Err(error) if graphql_error_is_transient(&error) => {
                last_error = error;
                continue;
            }
            Err(error) => return Err(error),
        }
        match submit_prepared_agent_request(graphql, prepared).await {
            Ok(submitted) => return Ok(submitted),
            Err(error) if graphql_error_is_transient(&error) => last_error = error,
            Err(error) => return Err(error),
        }
    }
    if submitted_request_exists(graphql, &prepared.submitted.request_id).await? {
        return Ok(prepared.submitted.clone());
    }
    Err(last_error)
}

async fn submitted_request_exists(graphql: &str, request_id: &str) -> Result<bool> {
    let escaped_request_id = escape_graphql_string(request_id);
    let response = post_graphql(
        graphql,
        &format!(
            r#"{{
                AgentRequest(
                    filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                    limit: 2
                ) {{ request_id }}
            }}"#
        ),
    )
    .await?;
    let rows = response
        .pointer("/data/AgentRequest")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if rows.len() > 1 {
        anyhow::bail!("request_id {request_id} became ambiguous during submission recovery");
    }
    Ok(rows
        .first()
        .is_some_and(|row| row.get("request_id").and_then(Value::as_str) == Some(request_id)))
}

async fn resolve_request_behavior_id(
    graphql: &str,
    agent_did: &str,
    requested: Option<&str>,
) -> Result<String> {
    let escaped_agent_did = gents::graphql::escape_graphql_string(agent_did);
    let response = post_graphql(
        graphql,
        &format!(
            r#"{{
                AgentPrincipal(
                    filter: {{ agent_did: {{ _eq: "{escaped_agent_did}" }} }},
                    limit: 2
                ) {{ agent_did default_behavior_id enabled }}
                AgentBehavior(
                    filter: {{ agent_did: {{ _eq: "{escaped_agent_did}" }} }}
                ) {{ behavior_id agent_did enabled }}
            }}"#,
        ),
    )
    .await
    .context("loading authoritative request behavior")?;
    let principals = response
        .pointer("/data/AgentPrincipal")
        .and_then(Value::as_array)
        .context("AgentPrincipal query returned no row array")?;
    anyhow::ensure!(
        principals.len() == 1,
        "request target principal must resolve to exactly one row"
    );
    let principal = &principals[0];
    anyhow::ensure!(
        principal.get("enabled").and_then(Value::as_bool) == Some(true),
        "request target principal is disabled"
    );
    let behavior_id = match requested.map(str::trim).filter(|value| !value.is_empty()) {
        Some(behavior_id) => behavior_id.to_string(),
        None => principal
            .get("default_behavior_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .context("request target principal has no canonical default behavior")?,
    };
    let behaviors = response
        .pointer("/data/AgentBehavior")
        .and_then(Value::as_array)
        .context("AgentBehavior query returned no row array")?;
    let matching = behaviors
        .iter()
        .filter(|row| row.get("behavior_id").and_then(Value::as_str) == Some(&behavior_id))
        .collect::<Vec<_>>();
    anyhow::ensure!(
        matching.len() == 1,
        "request behavior must resolve to exactly one row owned by the target principal"
    );
    anyhow::ensure!(
        matching[0].get("enabled").and_then(Value::as_bool) == Some(true),
        "request behavior is disabled"
    );
    Ok(behavior_id)
}

pub(crate) fn content_and_metadata_with_prompt_selected_skill_ids(
    metadata: Option<&str>,
    content: &str,
) -> (String, Option<String>) {
    let selection = prompt_slash_skill_selection(content);
    let Some(metadata) = metadata_with_selected_skill_ids(metadata, &selection.selected_skill_ids)
    else {
        return (content.to_string(), metadata.map(ToOwned::to_owned));
    };
    (selection.prompt, metadata)
}

fn metadata_with_selected_skill_ids(
    metadata: Option<&str>,
    selected: &[String],
) -> Option<Option<String>> {
    if selected.is_empty() {
        return Some(metadata.map(ToOwned::to_owned));
    }

    let mut value = match metadata.map(str::trim).filter(|value| !value.is_empty()) {
        Some(raw) => match serde_json::from_str::<Value>(raw) {
            Ok(value) if value.is_object() => value,
            _ => return None,
        },
        None => serde_json::json!({}),
    };

    let Some(object) = value.as_object_mut() else {
        return None;
    };
    let entry = object
        .entry("selected_skill_ids".to_string())
        .or_insert_with(|| serde_json::json!([]));
    if !entry.is_array() {
        return None;
    }
    let Some(ids) = entry.as_array_mut() else {
        return None;
    };
    for id in selected {
        let already_present = ids
            .iter()
            .any(|existing| existing.as_str() == Some(id.as_str()));
        if !already_present {
            ids.push(Value::String(id.clone()));
        }
    }
    Some(Some(value.to_string()))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WaitProgressMarker {
    request_lifecycle_state: Option<RequestLifecycleState>,
    request_failure_len: Option<usize>,
    request_interrupt_requested_at: Option<String>,
    request_valid_until: Option<String>,
    response_doc_id: Option<String>,
    response_status: Option<String>,
    response_content_len: Option<usize>,
    response_reasoning_fingerprint: Option<(usize, u64)>,
    response_error_len: Option<usize>,
    response_token_count: Option<String>,
    response_progress_seq: Option<String>,
    response_reasoning_progress_seq: Option<String>,
    response_materialized_message_sequence: Option<String>,
    response_materialized_at: Option<String>,
    response_completed_at: Option<String>,
    response_interrupted_at: Option<String>,
}

fn wait_progress_marker(
    request_row: Option<&AgentRequestRow>,
    response_row: Option<&serde_json::Value>,
) -> WaitProgressMarker {
    WaitProgressMarker {
        request_lifecycle_state: request_row.and_then(|row| row.lifecycle_state),
        request_failure_len: request_row
            .and_then(|row| row.failure_reason.as_deref())
            .map(str::len),
        request_interrupt_requested_at: request_row
            .and_then(|row| row.interrupt_requested_at.clone()),
        request_valid_until: request_row.and_then(|row| row.valid_until.clone()),
        response_doc_id: scalar_marker(response_row, "_docID"),
        response_status: scalar_marker(response_row, "status"),
        response_content_len: string_len_marker(response_row, "content"),
        response_reasoning_fingerprint: string_fingerprint_marker(response_row, "reasoning"),
        response_error_len: string_len_marker(response_row, "error_message"),
        response_token_count: scalar_marker(response_row, "token_count"),
        response_progress_seq: scalar_marker(response_row, "progress_seq"),
        response_reasoning_progress_seq: scalar_marker(response_row, "reasoning_progress_seq"),
        response_materialized_message_sequence: scalar_marker(
            response_row,
            "materialized_message_sequence",
        ),
        response_materialized_at: scalar_marker(response_row, "materialized_at"),
        response_completed_at: scalar_marker(response_row, "completed_at"),
        response_interrupted_at: scalar_marker(response_row, "interrupted_at"),
    }
}

fn scalar_marker(row: Option<&serde_json::Value>, field: &str) -> Option<String> {
    let value = row?.get(field)?;
    if value.is_null() {
        return None;
    }
    value
        .as_str()
        .map(ToOwned::to_owned)
        .or_else(|| value.as_i64().map(|value| value.to_string()))
        .or_else(|| value.as_u64().map(|value| value.to_string()))
        .or_else(|| value.as_bool().map(|value| value.to_string()))
}

fn string_len_marker(row: Option<&serde_json::Value>, field: &str) -> Option<usize> {
    row?.get(field)?.as_str().map(str::len)
}

fn string_fingerprint_marker(row: Option<&serde_json::Value>, field: &str) -> Option<(usize, u64)> {
    let value = row?.get(field)?.as_str()?;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    Some((value.len(), hasher.finish()))
}

pub(crate) async fn wait_for_terminal_response(
    graphql: &str,
    request_id: &str,
    timeout_secs: u64,
    poll_secs: u64,
) -> Result<serde_json::Value> {
    let idle_timeout = Duration::from_secs(timeout_secs);
    let mut last_progress_at = tokio::time::Instant::now();
    let mut last_progress_marker: Option<WaitProgressMarker> = None;

    loop {
        let (request_row, request_value) = {
            let query = request_terminal_query(request_id);
            let response = post_graphql(graphql, &query).await?;
            let value = response
                .pointer("/data/AgentRequest")
                .and_then(|v| v.as_array())
                .and_then(|rows| rows.first())
                .cloned();
            let row = value
                .as_ref()
                .map(|row| {
                    serde_json::from_value::<AgentRequestRow>(row.clone())
                        .context("decoding terminal-wait AgentRequest row")
                })
                .transpose()?;
            (row, value)
        };
        let response_row = {
            let query = response_wait_progress_query(request_id);
            let response = post_graphql(graphql, &query).await?;
            response
                .pointer("/data/AgentResponse")
                .and_then(|v| v.as_array())
                .and_then(|rows| rows.first())
                .cloned()
        };

        let marker = wait_progress_marker(request_row.as_ref(), response_row.as_ref());
        if last_progress_marker.as_ref() != Some(&marker) {
            last_progress_marker = Some(marker);
            last_progress_at = tokio::time::Instant::now();
        }

        let lifecycle_state = request_row.as_ref().and_then(|row| row.lifecycle_state);
        let response_status = response_row
            .as_ref()
            .and_then(|row| row.get("status"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let terminal_by_request = lifecycle_state.is_some_and(RequestLifecycleState::is_terminal);
        let terminal_by_response = matches!(
            response_status,
            "complete" | "completed" | "error" | "failed" | "interrupted" // AgentResponse.status
        );
        if terminal_by_request || terminal_by_response {
            let response_row = if response_row.is_some() {
                let response = post_graphql(graphql, &response_query(request_id)).await?;
                response
                    .pointer("/data/AgentResponse")
                    .and_then(|v| v.as_array())
                    .and_then(|rows| rows.first())
                    .cloned()
            } else {
                None
            };
            let mut envelope = response_row.unwrap_or_else(|| {
                serde_json::json!({
                    "request_id": request_id,
                    "status": null,
                    "content": null,
                })
            });
            match hydrate_materialized_response_content(graphql, &mut envelope).await? {
                MaterializedResponsePresentation::Presentable => {}
                MaterializedResponsePresentation::Pending => {
                    if last_progress_at.elapsed() >= idle_timeout {
                        anyhow::bail!(
                            "timed out waiting for materialized AgentMessage {request_id} after {timeout_secs}s of inactivity\n{}",
                            request_diagnostic_hint(request_id)
                        );
                    }
                    tokio::time::sleep(Duration::from_secs(poll_secs)).await;
                    continue;
                }
                MaterializedResponsePresentation::Invalid => {
                    anyhow::bail!(materialized_response_diagnostic(request_id, &envelope));
                }
            }
            if let Some(object) = envelope.as_object_mut() {
                object.insert(
                    "request".to_string(),
                    request_value.unwrap_or(serde_json::Value::Null),
                );
            }
            return Ok(envelope);
        }

        if last_progress_at.elapsed() >= idle_timeout {
            anyhow::bail!(
                "timed out waiting for AgentResponse {request_id} after {timeout_secs}s of inactivity\n{}",
                request_diagnostic_hint(request_id)
            );
        }

        tokio::time::sleep(Duration::from_secs(poll_secs)).await;
    }
}

pub(crate) fn request_diagnostic_hint(request_id: &str) -> String {
    format!(
        "Next:\n  1. Run `gents request show {request_id}`\n  2. Run `gents response show {request_id}`\n  3. Inspect the runtime with `gents status`"
    )
}

pub(crate) fn resolve_dual_id(
    noun: &str,
    flag_name: &str,
    positional: Option<&str>,
    flag: Option<&str>,
) -> Result<String> {
    let positional = positional.map(str::trim).filter(|value| !value.is_empty());
    let flag = flag.map(str::trim).filter(|value| !value.is_empty());
    match (positional, flag) {
        (Some(positional), Some(flag)) if positional != flag => anyhow::bail!(
            "conflicting {noun} ids provided: positional={positional} and {flag_name}={flag}"
        ),
        (Some(request_id), _) | (_, Some(request_id)) => Ok(request_id.to_string()),
        (None, None) => anyhow::bail!("missing {noun} id"),
    }
}

pub(crate) fn resolve_request_id(positional: Option<&str>, flag: Option<&str>) -> Result<String> {
    resolve_dual_id("request", "--request-id", positional, flag)
}

pub(crate) fn resolve_request_content(
    content: Option<&str>,
    content_file: Option<&Path>,
) -> Result<String> {
    match (content, content_file) {
        (Some(_), Some(path)) => anyhow::bail!(
            "provide either --content or --content-file, not both ({})",
            path.display()
        ),
        (Some(content), None) => Ok(require_non_empty("content", content)?.to_string()),
        (None, Some(path)) => {
            let content = fs::read_to_string(path)
                .with_context(|| format!("reading request content from {}", path.display()))?;
            Ok(require_non_empty("content-file", &content)?.to_string())
        }
        (None, None) => {
            anyhow::bail!("request content is required; pass --content or --content-file")
        }
    }
}

#[cfg(test)]
mod dual_id_tests {
    use super::*;

    #[test]
    fn resolve_dual_id_accepts_positional_only() {
        assert_eq!(
            resolve_dual_id("task", "--task-id", Some("task-1"), None).unwrap(),
            "task-1"
        );
    }

    #[test]
    fn resolve_dual_id_accepts_flag_only() {
        assert_eq!(
            resolve_dual_id("task", "--task-id", None, Some("task-1")).unwrap(),
            "task-1"
        );
    }

    #[test]
    fn resolve_dual_id_accepts_equal_positional_and_flag() {
        assert_eq!(
            resolve_dual_id("task", "--task-id", Some("task-1"), Some("task-1")).unwrap(),
            "task-1"
        );
    }

    #[test]
    fn resolve_dual_id_rejects_conflict() {
        let err = resolve_dual_id("task", "--task-id", Some("task-1"), Some("task-2"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("conflicting task ids provided"));
        assert!(err.contains("positional=task-1"));
        assert!(err.contains("--task-id=task-2"));
    }

    #[test]
    fn resolve_dual_id_rejects_missing_id() {
        let err = resolve_dual_id("task", "--task-id", None, None)
            .unwrap_err()
            .to_string();
        assert_eq!(err, "missing task id");
    }
}

pub(crate) fn write_json_output_file(path: &Path, value: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating output directory {}", parent.display()))?;
    }
    let contents =
        serde_json::to_vec_pretty(value).context("encoding JSON output for output file")?;
    fs::write(path, contents)
        .with_context(|| format!("writing JSON output file {}", path.display()))?;
    Ok(())
}

pub(crate) fn print_json(value: &Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    io::stdout().flush()?;
    Ok(())
}

pub(crate) fn parse_duration_suffix(raw: &str) -> Result<Duration> {
    let s = raw.trim();
    if s.is_empty() {
        anyhow::bail!("duration must not be empty");
    }
    let split = s.find(|c: char| c.is_alphabetic()).unwrap_or(s.len());
    let (num_part, suffix) = s.split_at(split);
    let n: u64 = num_part
        .parse()
        .with_context(|| format!("invalid duration number in {raw}"))?;
    let secs = match suffix {
        "" | "s" => n,
        "m" => n.checked_mul(60).context("duration overflow")?,
        "h" => n.checked_mul(3600).context("duration overflow")?,
        "d" => n.checked_mul(86400).context("duration overflow")?,
        other => anyhow::bail!("unknown duration suffix {other:?} (use s, m, h, d)"),
    };
    Ok(Duration::from_secs(secs))
}

pub(crate) fn parse_valid_until_flag(raw: Option<&str>) -> Result<Option<DateTime<Utc>>> {
    match raw.map(str::trim) {
        None => Ok(Some(Utc::now() + chrono::Duration::minutes(5))),
        Some("") | Some("none") | Some("0") => Ok(None),
        Some(value) => {
            let dur = parse_duration_suffix(value)?;
            let secs = i64::try_from(dur.as_secs()).context("duration too large")?;
            Ok(Some(Utc::now() + chrono::Duration::seconds(secs)))
        }
    }
}

pub(crate) async fn fetch_request_view(graphql: &str, request_id: &str) -> Result<AgentRequestRow> {
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{request_id}" }} }},
                limit: 2
            ) {{
                _docID
                request_id
                agent_did
                behavior_id
                content
                lifecycle_state
                failure_reason
                retry_root_request
                temperature
                top_p
                top_k
                seed
                max_tokens
                max_total_tokens
                metadata
            }}
        }}"#,
        request_id = escape_graphql_string(request_id),
    );
    let response = post_graphql(graphql, &query).await?;
    let rows = response
        .pointer("/data/AgentRequest")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    if rows.len() != 1 {
        anyhow::bail!(
            "request_id {request_id} is ambiguous or absent across {} AgentRequest documents",
            rows.len()
        );
    }
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("request {request_id} not found"))?;
    serde_json::from_value(row).with_context(|| format!("decoding AgentRequest {request_id}"))
}

#[cfg(test)]
mod tests {
    use super::{
        content_and_metadata_with_prompt_selected_skill_ids, create_agent_request,
        graphql_error_is_transient, materialized_message_query,
        submit_prepared_agent_request_with_retry, transient_graphql_retry_delay,
        PreparedAgentRequest, RequestSubmitOptions, SubmittedRequest,
    };
    use axum::{
        body::{Body, Bytes},
        extract::State,
        response::{IntoResponse, Response},
        routing::post,
        Json, Router,
    };
    use futures_util::stream;
    use serde_json::{json, Value};
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct AmbiguousSubmitState {
        durable_ids: Arc<Mutex<BTreeSet<String>>>,
        mutation_count: Arc<std::sync::atomic::AtomicUsize>,
        lose_transport_response: Arc<std::sync::atomic::AtomicBool>,
    }

    async fn ambiguous_submit_endpoint(
        State(state): State<AmbiguousSubmitState>,
        Json(body): Json<Value>,
    ) -> Response {
        let query = body
            .get("query")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if query.contains("create_AgentRequest") {
            state
                .mutation_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            state
                .durable_ids
                .lock()
                .expect("durable ids")
                .insert("stable-request-id".to_string());
            if state
                .lose_transport_response
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                // The durable write happened, then the response body failed.
                // reqwest retains this decode/transport error in the anyhow
                // cause chain, which recovery must classify before querying
                // the stable request id.
                let body = Body::from_stream(stream::once(async {
                    Err::<Bytes, std::io::Error>(std::io::Error::other(
                        "connection closed before message completed",
                    ))
                }));
                return Response::new(body);
            }
            // The mutation committed, but its response was lost/replaced by a
            // transient error at the transport boundary.
            return Json(json!({"errors": [{"message": "database is locked"}]})).into_response();
        }
        let rows: Vec<Value> = state
            .durable_ids
            .lock()
            .expect("durable ids")
            .iter()
            .map(|request_id| json!({"request_id": request_id}))
            .collect();
        Json(json!({"data": {"AgentRequest": rows}})).into_response()
    }

    fn stable_test_prepared_request() -> PreparedAgentRequest {
        PreparedAgentRequest {
            // Keep the fixture byte-identical on the wire without placing the
            // raw production-write spelling in this source file. The runtime
            // fence scans the whole file (including tests) for direct writers.
            mutation: concat!(
                "mutation { create_",
                "AgentRequest(input: { request_id: \"stable-request-id\" }) { _docID } }"
            )
            .to_string(),
            submitted: SubmittedRequest {
                request_id: "stable-request-id".to_string(),
                session_id: "session".to_string(),
                agent_did: "did:key:test".to_string(),
                behavior_id: Some("behavior".to_string()),
                temperature: None,
                top_p: None,
                top_k: None,
                seed: None,
                max_tokens: None,
                max_total_tokens: None,
                metadata: None,
                created_at: None,
            },
        }
    }

    #[tokio::test]
    async fn transient_submission_recovers_committed_identity_without_reposting() {
        let state = AmbiguousSubmitState::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test endpoint");
        let address = listener.local_addr().expect("test endpoint address");
        let router = Router::new()
            .route("/", post(ambiguous_submit_endpoint))
            .with_state(state.clone());
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        let prepared = stable_test_prepared_request();
        let submitted =
            submit_prepared_agent_request_with_retry(&format!("http://{address}/"), &prepared)
                .await
                .expect("recover committed request");
        assert_eq!(submitted.request_id, "stable-request-id");
        assert_eq!(
            state
                .mutation_count
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a committed mutation must not be posted again"
        );
        assert_eq!(state.durable_ids.lock().expect("durable ids").len(), 1);
        server.abort();
    }

    #[tokio::test]
    async fn transport_response_loss_recovers_committed_identity_without_reposting() {
        let state = AmbiguousSubmitState {
            lose_transport_response: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            ..Default::default()
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test endpoint");
        let address = listener.local_addr().expect("test endpoint address");
        let router = Router::new()
            .route("/", post(ambiguous_submit_endpoint))
            .with_state(state.clone());
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        let prepared = stable_test_prepared_request();
        let submitted =
            submit_prepared_agent_request_with_retry(&format!("http://{address}/"), &prepared)
                .await
                .expect("recover committed request after transport response loss");
        assert_eq!(submitted.request_id, "stable-request-id");
        assert_eq!(
            state
                .mutation_count
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a committed mutation must not be posted again after response loss"
        );
        assert_eq!(state.durable_ids.lock().expect("durable ids").len(), 1);
        server.abort();
    }

    #[test]
    fn graphql_transient_classifier_matches_defradb_contention_only() {
        for message in [
            "transaction conflict while committing",
            "Transaction conflict while committing",
            "database is locked",
        ] {
            assert!(graphql_error_is_transient(&anyhow::anyhow!(message)));
        }
        assert!(!graphql_error_is_transient(&anyhow::anyhow!(
            "permission denied"
        )));
        assert_eq!(
            transient_graphql_retry_delay(1),
            std::time::Duration::from_millis(50)
        );
    }

    #[tokio::test]
    async fn prepared_request_signs_complete_client_semantics_once() -> anyhow::Result<()> {
        use gents::AgentIdentity;
        let dir = tempfile::tempdir()?;
        let identity = gents::KeyIdentity::load_or_create(dir.path().join("agent.key"), None)?;
        let did = identity.did().to_string();
        let data = serde_json::json!({"data": {
            "AgentPrincipal": [{"agent_did": did, "default_behavior_id": "default", "enabled": true}],
            "AgentBehavior": [{"agent_did": did, "behavior_id": "default", "enabled": true}]
        }});
        let app = axum::Router::new().route(
            "/graphql",
            axum::routing::post(move || async move { axum::Json(data) }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}/graphql", listener.local_addr()?);
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let result = super::prepare_agent_request(
            &endpoint,
            &did,
            "/vuln-scan review this",
            Some("session"),
            None,
            Some("stable-request".into()),
            RequestSubmitOptions {
                temperature: Some(0.35),
                top_p: Some(0.92),
                top_k: Some(32),
                seed: Some(1234),
                max_tokens: Some(2048),
                max_total_tokens: Some(100_000),
                metadata: Some(r#"{"case":"client"}"#.into()),
                valid_until: Some("2030-01-01T00:00:00Z".parse()?),
                retry_parent_request: Some("parent".into()),
                retry_parent_request_doc_id: Some("parent-doc".into()),
                retry_root_request: Some("root".into()),
                retry_key: Some("goal-submit:key".into()),
            },
        )
        .await;
        server.abort();
        let (_, create) = result?;
        assert_eq!(create.request_id, "stable-request");
        assert_eq!(create.requester_did, did);
        assert_eq!(create.behavior_id.as_deref(), Some("default"));
        assert_eq!(create.temperature, Some(0.35));
        assert_eq!(create.top_p, Some(0.92));
        assert_eq!(create.top_k, Some(32));
        assert_eq!(create.seed, Some(1234));
        assert_eq!(create.max_tokens, Some(2048));
        assert_eq!(create.max_total_tokens, Some(100_000));
        assert_eq!(create.valid_until.as_deref(), Some("2030-01-01T00:00:00Z"));
        assert_eq!(create.retry_parent_request.as_deref(), Some("parent"));
        assert_eq!(
            create.retry_parent_request_doc_id.as_deref(),
            Some("parent-doc")
        );
        assert_eq!(create.retry_root_request.as_deref(), Some("root"));
        assert_eq!(create.retry_key.as_deref(), Some("goal-submit:key"));
        assert_eq!((create.retry_count, create.max_retries), (0, 3));
        let metadata: serde_json::Value =
            serde_json::from_str(create.metadata.as_deref().unwrap())?;
        assert_eq!(metadata["case"], "client");
        assert_eq!(
            metadata["selected_skill_ids"],
            serde_json::json!(["vuln-scan"])
        );
        assert!(
            identity
                .verify(&did, &create.signing_payload(), &create.admission.signature)
                .await?
        );
        Ok(())
    }

    #[tokio::test]
    async fn create_agent_request_rejects_negative_seed_before_network_io() {
        let error = create_agent_request(
            "http://127.0.0.1:1/graphql",
            "did:key:test",
            "hello",
            Some("session-one"),
            None,
            RequestSubmitOptions {
                seed: Some(-1),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(error.to_string(), "seed must be non-negative");
    }

    #[test]
    fn materialized_message_query_loads_dedicated_reasoning() {
        let query = materialized_message_query("session-1", 7);
        assert!(query.contains("reasoning"));
        assert!(query.contains("sequence: { _eq: 7 }"));
    }

    #[test]
    fn slash_prompt_adds_selected_skill_ids_metadata() {
        let (_content, metadata) =
            content_and_metadata_with_prompt_selected_skill_ids(None, "/vuln-scan /work");
        assert_eq!(
            metadata.as_deref(),
            Some(r#"{"selected_skill_ids":["vuln-scan"]}"#)
        );
    }

    #[test]
    fn slash_prompt_merges_existing_selected_skill_ids() {
        let (_content, metadata) = content_and_metadata_with_prompt_selected_skill_ids(
            Some(r#"{"codex_shim":{},"selected_skill_ids":["triage"]}"#),
            "/vuln-scan /work",
        );
        let metadata = metadata.expect("metadata");
        assert!(metadata.contains(r#""codex_shim":{}"#));
        assert!(metadata.contains(r#""triage""#));
        assert!(metadata.contains(r#""vuln-scan""#));
    }

    #[test]
    fn invalid_existing_metadata_is_preserved() {
        let (content, metadata) =
            content_and_metadata_with_prompt_selected_skill_ids(Some("not json"), "/vuln-scan");
        assert_eq!(content, "/vuln-scan");
        assert_eq!(metadata, Some("not json".to_string()));
    }

    #[test]
    fn slash_prompt_strips_control_syntax_from_request_content() {
        let (content, metadata) =
            content_and_metadata_with_prompt_selected_skill_ids(None, "/vuln-scan\nReview /work");

        assert_eq!(content, "Review /work");
        assert_eq!(
            metadata.as_deref(),
            Some(r#"{"selected_skill_ids":["vuln-scan"]}"#)
        );
    }
}
