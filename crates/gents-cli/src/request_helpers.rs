use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{self, Write};
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use gents::{
    config_client::ConfigAccess, graphql::escape_graphql_string,
    skills::prompt_slash_skill_selection,
};
use gents_protocol::transcript::present_persisted_message;
use serde_json::Value;

use crate::{optional_f64_field, optional_i64_field, post_graphql, require_non_empty};

#[derive(Debug, Clone)]
pub(crate) struct SubmittedRequest {
    pub(crate) request_id: String,
    pub(crate) session_id: String,
    pub(crate) agent_did: String,
    pub(crate) behavior_id: Option<String>,
    pub(crate) temperature: Option<f64>,
    pub(crate) top_p: Option<f64>,
    pub(crate) top_k: Option<i64>,
    pub(crate) max_tokens: Option<i64>,
    pub(crate) metadata: Option<String>,
    pub(crate) created_at: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct RequestSubmitOptions {
    pub(crate) temperature: Option<f64>,
    pub(crate) top_p: Option<f64>,
    pub(crate) top_k: Option<i64>,
    pub(crate) max_tokens: Option<i64>,
    pub(crate) metadata: Option<String>,
    pub(crate) valid_until: Option<DateTime<Utc>>,
    pub(crate) retry_parent_request: Option<String>,
    pub(crate) retry_root_request: Option<String>,
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

pub(crate) fn is_terminal_lifecycle_state(state: &str) -> bool {
    matches!(
        state,
        "completed" | "failed" | "superseded" | "dead" | "interrupted"
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

fn response_materialized_sequence(response: &Value) -> Option<i64> {
    response
        .get("materialized_message_sequence")
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
        })
}

pub(crate) async fn hydrate_materialized_response_content(
    graphql: &str,
    response: &mut Value,
) -> Result<bool> {
    let content_blank = response_field_is_blank(response, "content");
    let reasoning_blank = response_field_is_blank(response, "reasoning");
    if !content_blank && !reasoning_blank {
        return Ok(true);
    }

    let Some(sequence) = response_materialized_sequence(response) else {
        return Ok(!content_blank || !reasoning_blank);
    };
    let Some(session_id) = response.get("session_id").and_then(Value::as_str) else {
        return Ok(!content_blank || !reasoning_blank);
    };

    let query = materialized_message_query(session_id, sequence);
    let message_response = post_graphql(graphql, &query).await?;
    let Some(message) = message_response
        .pointer("/data/AgentMessage")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
    else {
        return Ok(false);
    };
    let Some(role) = message.get("role").and_then(Value::as_str) else {
        return Ok(false);
    };
    let Some(content) = message.get("content").and_then(Value::as_str) else {
        return Ok(false);
    };

    let presentation = present_persisted_message(role, content);
    let Some(object) = response.as_object_mut() else {
        return Ok(false);
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

    Ok(true)
}

pub(crate) async fn create_agent_request(
    graphql: &str,
    agent_did: &str,
    content: &str,
    session_id: Option<&str>,
    behavior_id: Option<&str>,
    options: RequestSubmitOptions,
) -> Result<SubmittedRequest> {
    let source_author_did = ConfigAccess::Graphql(graphql.to_string())
        .node_identity_did()
        .await
        .context("creating an AgentRequest requires a signed database endpoint")?;
    let (request_content, request_metadata) =
        content_and_metadata_with_prompt_selected_skill_ids(options.metadata.as_deref(), content);
    let request_id = uuid::Uuid::new_v4().to_string();
    let session_id = session_id
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let created_at = chrono::Utc::now().to_rfc3339();
    let behavior_field = behavior_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            format!(
                r#"
                behavior_id: "{}","#,
                escape_graphql_string(value)
            )
        })
        .unwrap_or_default();
    let valid_until_literal = options.valid_until.map(|at| {
        format!(
            r#"valid_until: "{}""#,
            escape_graphql_string(&at.to_rfc3339())
        )
    });
    let request_override_fields = vec![
        optional_f64_field("temperature", options.temperature),
        optional_f64_field("top_p", options.top_p),
        optional_i64_field("top_k", options.top_k),
        optional_i64_field("max_tokens", options.max_tokens),
        request_metadata
            .as_ref()
            .map(|metadata| format!(r#"metadata: "{}""#, escape_graphql_string(metadata))),
        valid_until_literal,
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(",\n                ");
    let request_override_fields = if request_override_fields.is_empty() {
        String::new()
    } else {
        format!("{request_override_fields},\n                ")
    };
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
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{request_id}",
                agent_did: "{agent_did}",
                source_author_did: "{source_author_did}",
                requester_did: "{agent_did}",
                {behavior_field}
                session_id: "{session_id}",
                retry_parent_request: "{retry_parent}",
                retry_root_request: "{retry_root}",
                superseded_by_request: "",
                content: "{content}",
                {request_override_fields}status: "pending",
                lifecycle_state: "pending",
                backend_id: "",
                execution_origin: "interactive",
                failure_reason: "",
                created_at: "{created_at}",
                retry_count: 0,
                max_retries: 3
            }}) {{ _docID }}
        }}"#,
        request_id = escape_graphql_string(&request_id),
        agent_did = escape_graphql_string(agent_did),
        source_author_did = escape_graphql_string(&source_author_did),
        behavior_field = behavior_field,
        session_id = escape_graphql_string(&session_id),
        retry_parent = escape_graphql_string(retry_parent_value),
        retry_root = escape_graphql_string(&retry_root_value),
        content = escape_graphql_string(&request_content),
        request_override_fields = request_override_fields,
    );
    post_graphql(graphql, &mutation).await?;

    Ok(SubmittedRequest {
        request_id,
        session_id,
        agent_did: agent_did.to_string(),
        behavior_id: behavior_id
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
        temperature: options.temperature,
        top_p: options.top_p,
        top_k: options.top_k,
        max_tokens: options.max_tokens,
        metadata: request_metadata,
        created_at: Some(created_at),
    })
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
    request_lifecycle_state: Option<String>,
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
    request_row: Option<&serde_json::Value>,
    response_row: Option<&serde_json::Value>,
) -> WaitProgressMarker {
    WaitProgressMarker {
        request_lifecycle_state: scalar_marker(request_row, "lifecycle_state"),
        request_failure_len: string_len_marker(request_row, "failure_reason"),
        request_interrupt_requested_at: scalar_marker(request_row, "interrupt_requested_at"),
        request_valid_until: scalar_marker(request_row, "valid_until"),
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
        let request_row = {
            let query = request_terminal_query(request_id);
            let response = post_graphql(graphql, &query).await?;
            response
                .pointer("/data/AgentRequest")
                .and_then(|v| v.as_array())
                .and_then(|rows| rows.first())
                .cloned()
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

        let lifecycle_state = request_row
            .as_ref()
            .and_then(|row| row.get("lifecycle_state"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let response_status = response_row
            .as_ref()
            .and_then(|row| row.get("status"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let should_wait_for_materialized_content =
            matches!(response_status, "complete" | "completed")
                && response_row.as_ref().is_some_and(|row| {
                    response_field_is_blank(row, "content")
                        && response_materialized_sequence(row).is_some()
                });

        let terminal_by_request = is_terminal_lifecycle_state(lifecycle_state);
        let terminal_by_response = matches!(response_status, "complete" | "completed" | "error");
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
            let hydrated = hydrate_materialized_response_content(graphql, &mut envelope).await?;
            if should_wait_for_materialized_content && !hydrated {
                if last_progress_at.elapsed() >= idle_timeout {
                    anyhow::bail!(
                        "timed out waiting for materialized AgentMessage {request_id} after {timeout_secs}s of inactivity\n{}",
                        request_diagnostic_hint(request_id)
                    );
                }
                tokio::time::sleep(Duration::from_secs(poll_secs)).await;
                continue;
            }
            if let Some(object) = envelope.as_object_mut() {
                object.insert(
                    "request".to_string(),
                    request_row.unwrap_or(serde_json::Value::Null),
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

#[derive(Debug, Clone)]
pub(crate) struct StaleRequestView {
    pub(crate) agent_did: String,
    pub(crate) behavior_id: Option<String>,
    pub(crate) content: String,
    pub(crate) lifecycle_state: String,
    pub(crate) failure_reason: String,
    pub(crate) retry_root_request: Option<String>,
    pub(crate) temperature: Option<f64>,
    pub(crate) top_p: Option<f64>,
    pub(crate) top_k: Option<i64>,
    pub(crate) max_tokens: Option<i64>,
    pub(crate) metadata: Option<String>,
}

pub(crate) async fn fetch_request_view(
    graphql: &str,
    request_id: &str,
) -> Result<StaleRequestView> {
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{request_id}" }} }},
                limit: 1
            ) {{
                agent_did
                behavior_id
                content
                lifecycle_state
                failure_reason
                retry_root_request
                temperature
                top_p
                top_k
                max_tokens
                metadata
            }}
        }}"#,
        request_id = escape_graphql_string(request_id),
    );
    let response = post_graphql(graphql, &query).await?;
    let row = response
        .pointer("/data/AgentRequest")
        .and_then(|v| v.as_array())
        .and_then(|rows| rows.first())
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("request {request_id} not found"))?;
    let as_string = |key: &str| {
        row.get(key)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    };
    let as_optional = |key: &str| {
        row.get(key)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(String::from)
    };
    let as_optional_f64 = |key: &str| row.get(key).and_then(|v| v.as_f64());
    let as_optional_i64 = |key: &str| row.get(key).and_then(|v| v.as_i64());
    Ok(StaleRequestView {
        agent_did: as_string("agent_did"),
        behavior_id: as_optional("behavior_id"),
        content: as_string("content"),
        lifecycle_state: as_string("lifecycle_state"),
        failure_reason: as_string("failure_reason"),
        retry_root_request: as_optional("retry_root_request"),
        temperature: as_optional_f64("temperature"),
        top_p: as_optional_f64("top_p"),
        top_k: as_optional_i64("top_k"),
        max_tokens: as_optional_i64("max_tokens"),
        metadata: as_optional("metadata"),
    })
}

#[cfg(test)]
mod tests {
    use super::{content_and_metadata_with_prompt_selected_skill_ids, materialized_message_query};

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
