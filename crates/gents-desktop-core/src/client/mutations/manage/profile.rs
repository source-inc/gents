use anyhow::{bail, Result};
use defra_node::EmbeddedNode;
use gents_protocol::row::InferenceProfileRow;
use serde_json::Value;

use super::super::graphql::{
    escape_graphql_string, execute_mutation, graphql_optional_bool_field,
    graphql_optional_float_field, graphql_optional_int_field, graphql_optional_int_list_field,
    graphql_string_field, join_fields, normalize_required,
};

pub async fn upsert_inference_profile(
    node: &EmbeddedNode,
    row: &InferenceProfileRow,
) -> Result<()> {
    let profile_id = normalize_required("profile_id", &row.profile_id)?;
    if row
        .stream_liveness_timeout_secs
        .is_some_and(|value| value <= 0)
    {
        anyhow::bail!("stream_liveness_timeout_secs must be positive");
    }
    if row.seed.is_some_and(|value| value < 0) {
        anyhow::bail!("seed must be non-negative");
    }
    if row.reasoning_effort.as_deref().is_some_and(|value| {
        !matches!(
            value,
            "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max" | "ultra"
        )
    }) {
        anyhow::bail!(
            "reasoning_effort must be one of: none, minimal, low, medium, high, xhigh, max, ultra"
        );
    }

    let add_fields = [
        Some(format!(
            r#"profile_id: "{}""#,
            escape_graphql_string(profile_id)
        )),
        Some(graphql_string_field(
            "display_name",
            row.display_name.as_deref(),
        )),
        Some(graphql_optional_int_field(
            "context_window",
            row.context_window,
        )),
        Some(graphql_optional_int_field(
            "max_output_tokens",
            row.max_output_tokens,
        )),
        Some(graphql_optional_int_field("max_turns", row.max_turns)),
        Some(graphql_optional_float_field("temperature", row.temperature)),
        Some(graphql_optional_float_field("top_p", row.top_p)),
        Some(graphql_optional_int_field("top_k", row.top_k)),
        Some(graphql_optional_int_field("seed", row.seed)),
        Some(graphql_optional_float_field("min_p", row.min_p)),
        Some(graphql_optional_float_field(
            "frequency_penalty",
            row.frequency_penalty,
        )),
        Some(graphql_optional_float_field(
            "presence_penalty",
            row.presence_penalty,
        )),
        Some(graphql_optional_float_field(
            "repetition_penalty",
            row.repetition_penalty,
        )),
        Some(graphql_string_field(
            "reasoning_effort",
            row.reasoning_effort.as_deref(),
        )),
        Some(graphql_optional_int_field(
            "stream_batch_ms",
            row.stream_batch_ms,
        )),
        Some(graphql_optional_int_field(
            "stream_liveness_timeout_secs",
            row.stream_liveness_timeout_secs,
        )),
        Some(graphql_optional_int_field(
            "deadline_duration_secs",
            row.deadline_duration_secs,
        )),
        Some(graphql_optional_int_field(
            "retry_max_transport",
            row.retry_max_transport,
        )),
        Some(graphql_optional_int_list_field(
            "retry_backoff_ms",
            row.retry_backoff_ms.as_deref(),
        )),
        Some(graphql_optional_int_field(
            "retry_max_resample",
            row.retry_max_resample,
        )),
        Some(graphql_optional_bool_field(
            "retry_allow_repair",
            row.retry_allow_repair,
        )),
        Some(graphql_optional_int_field(
            "retry_interactive_max",
            row.retry_interactive_max,
        )),
    ];
    let update_fields = [
        Some(graphql_string_field(
            "display_name",
            row.display_name.as_deref(),
        )),
        Some(graphql_optional_int_field(
            "context_window",
            row.context_window,
        )),
        Some(graphql_optional_int_field(
            "max_output_tokens",
            row.max_output_tokens,
        )),
        Some(graphql_optional_int_field("max_turns", row.max_turns)),
        Some(graphql_optional_float_field("temperature", row.temperature)),
        Some(graphql_optional_float_field("top_p", row.top_p)),
        Some(graphql_optional_int_field("top_k", row.top_k)),
        Some(graphql_optional_int_field("seed", row.seed)),
        Some(graphql_optional_float_field("min_p", row.min_p)),
        Some(graphql_optional_float_field(
            "frequency_penalty",
            row.frequency_penalty,
        )),
        Some(graphql_optional_float_field(
            "presence_penalty",
            row.presence_penalty,
        )),
        Some(graphql_optional_float_field(
            "repetition_penalty",
            row.repetition_penalty,
        )),
        Some(graphql_string_field(
            "reasoning_effort",
            row.reasoning_effort.as_deref(),
        )),
        Some(graphql_optional_int_field(
            "stream_batch_ms",
            row.stream_batch_ms,
        )),
        Some(graphql_optional_int_field(
            "stream_liveness_timeout_secs",
            row.stream_liveness_timeout_secs,
        )),
        Some(graphql_optional_int_field(
            "deadline_duration_secs",
            row.deadline_duration_secs,
        )),
        Some(graphql_optional_int_field(
            "retry_max_transport",
            row.retry_max_transport,
        )),
        Some(graphql_optional_int_list_field(
            "retry_backoff_ms",
            row.retry_backoff_ms.as_deref(),
        )),
        Some(graphql_optional_int_field(
            "retry_max_resample",
            row.retry_max_resample,
        )),
        Some(graphql_optional_bool_field(
            "retry_allow_repair",
            row.retry_allow_repair,
        )),
        Some(graphql_optional_int_field(
            "retry_interactive_max",
            row.retry_interactive_max,
        )),
    ];

    let mutation = format!(
        r#"mutation {{
            upsert_InferenceProfile(
                filter: {{ profile_id: {{ _eq: "{profile_id}" }} }},
                add: {{
                    {add_fields}
                }},
                update: {{
                    {update_fields}
                }}
            ) {{ _docID }}
        }}"#,
        profile_id = escape_graphql_string(profile_id),
        add_fields = join_fields(&add_fields),
        update_fields = join_fields(&update_fields),
    );
    execute_mutation(node, &mutation, "upsert_inference_profile").await
}

pub async fn delete_inference_profile(node: &EmbeddedNode, profile_id: &str) -> Result<usize> {
    let mutation = build_delete_inference_profile_mutation(profile_id)?;
    let response = node.execute(&mutation).await;
    if response.has_errors() {
        bail!(
            "delete_inference_profile failed: {}",
            response
                .errors
                .iter()
                .map(|error| error.message.as_str())
                .collect::<Vec<_>>()
                .join("; ")
        );
    }
    Ok(response
        .data
        .as_ref()
        .and_then(|data| data.get("delete_InferenceProfile"))
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0))
}

fn build_delete_inference_profile_mutation(profile_id: &str) -> Result<String> {
    let profile_id = normalize_required("profile_id", profile_id)?;
    let profile_id = escape_graphql_string(profile_id);
    Ok(format!(
        r#"mutation {{
            delete_InferenceProfile(
                filter: {{ profile_id: {{ _eq: "{profile_id}" }} }}
            ) {{ _docID }}
        }}"#
    ))
}
