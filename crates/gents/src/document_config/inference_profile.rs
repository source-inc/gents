use anyhow::Result;
use defra_node::EmbeddedNode;
use serde::{Deserialize, Serialize};

use super::graphql_fields;
use super::serde_helpers;
use crate::config::{
    DEFAULT_CONTEXT_WINDOW, DEFAULT_DEADLINE_DURATION_SECS, DEFAULT_MAX_OUTPUT_TOKENS,
    DEFAULT_MAX_TURNS, DEFAULT_STREAM_BATCH_MS, DEFAULT_STREAM_LIVENESS_TIMEOUT_SECS,
};
use crate::config_client::mint_recreate_identity_timestamp;
use crate::graphql::escape_graphql_string;
use crate::retry::execute_graphql_with_conflict_retry;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct InferenceProfile {
    pub profile_id: String,
    pub display_name: Option<String>,
    pub context_window: Option<i64>,
    pub max_output_tokens: Option<i64>,
    pub max_turns: Option<i64>,
    pub temperature: Option<f64>,
    /// Sampling knobs beyond temperature (#649). `None` inherits the served
    /// model's `generation_config.json`; `Some` pins the value explicitly.
    pub top_p: Option<f64>,
    pub top_k: Option<i64>,
    pub min_p: Option<f64>,
    pub frequency_penalty: Option<f64>,
    pub presence_penalty: Option<f64>,
    pub repetition_penalty: Option<f64>,
    pub reasoning_effort: Option<String>,
    pub stream_batch_ms: Option<i64>,
    pub stream_liveness_timeout_secs: Option<i64>,
    pub deadline_duration_secs: Option<i64>,
    pub retry_max_transport: Option<i64>,
    pub retry_backoff_ms: Option<Vec<i64>>,
    pub retry_max_resample: Option<i64>,
    pub retry_allow_repair: Option<bool>,
    pub retry_interactive_max: Option<i64>,
}

const INFERENCE_PROFILE_FIELDS: &str = r#"
                _docID
                profile_id
                display_name
                context_window
                max_output_tokens
                max_turns
                temperature
                top_p
                top_k
                min_p
                frequency_penalty
                presence_penalty
                repetition_penalty
                reasoning_effort
                stream_batch_ms
                stream_liveness_timeout_secs
                deadline_duration_secs
                retry_max_transport
                retry_backoff_ms
                retry_max_resample
                retry_allow_repair
                retry_interactive_max
"#;

const DEFAULT_INFERENCE_PROFILE_LABEL: &str = "Default";

pub fn default_inference_profile_id_for_behavior(behavior_id: &str) -> String {
    format!("{behavior_id}-profile")
}

pub(super) fn default_inference_profile_for_behavior(behavior_id: &str) -> InferenceProfile {
    InferenceProfile {
        profile_id: default_inference_profile_id_for_behavior(behavior_id),
        display_name: Some(DEFAULT_INFERENCE_PROFILE_LABEL.to_string()),
        context_window: Some(DEFAULT_CONTEXT_WINDOW as i64),
        max_output_tokens: Some(DEFAULT_MAX_OUTPUT_TOKENS as i64),
        max_turns: Some(DEFAULT_MAX_TURNS as i64),
        temperature: Some(0.0),
        // Unset: inherit whatever the served model's generation_config.json
        // specifies. The default profile must not silently impose sampling the
        // operator never asked for.
        top_p: None,
        top_k: None,
        min_p: None,
        frequency_penalty: None,
        presence_penalty: None,
        repetition_penalty: None,
        reasoning_effort: None,
        stream_batch_ms: Some(DEFAULT_STREAM_BATCH_MS as i64),
        stream_liveness_timeout_secs: Some(DEFAULT_STREAM_LIVENESS_TIMEOUT_SECS as i64),
        deadline_duration_secs: Some(DEFAULT_DEADLINE_DURATION_SECS as i64),
        retry_max_transport: None,
        retry_backoff_ms: None,
        retry_max_resample: None,
        retry_allow_repair: None,
        retry_interactive_max: None,
    }
}

pub(super) async fn create_default_inference_profile(
    node: &EmbeddedNode,
    behavior_id: &str,
) -> Result<InferenceProfile> {
    let profile = default_inference_profile_for_behavior(behavior_id);
    upsert_inference_profile(node, &profile).await?;
    Ok(profile)
}

pub async fn load_inference_profile(
    node: &EmbeddedNode,
    profile_id: &str,
) -> Result<Option<InferenceProfile>> {
    Ok(load_inference_profile_record(node, profile_id)
        .await?
        .map(|(_, profile)| profile))
}

pub(crate) async fn load_inference_profile_record(
    node: &EmbeddedNode,
    profile_id: &str,
) -> Result<Option<(String, InferenceProfile)>> {
    let escaped_profile_id = escape_graphql_string(profile_id);
    let query = format!(
        r#"{{
            InferenceProfile(
                filter: {{ profile_id: {{ _eq: "{escaped_profile_id}" }} }},
                limit: 1
            ) {{
                _docID
                profile_id
                display_name
                context_window
                max_output_tokens
                max_turns
                temperature
                top_p
                top_k
                min_p
                frequency_penalty
                presence_penalty
                repetition_penalty
                reasoning_effort
                stream_batch_ms
                stream_liveness_timeout_secs
                deadline_duration_secs
                retry_max_transport
                retry_backoff_ms
                retry_max_resample
                retry_allow_repair
                retry_interactive_max
            }}
        }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("query InferenceProfile failed: {:?}", resp.errors);
    }

    Ok(serde_helpers::first_row_with_doc_id(
        resp.data.as_ref(),
        "InferenceProfile",
    ))
}

pub(crate) async fn load_inference_profile_by_doc_id(
    node: &EmbeddedNode,
    doc_id: &str,
) -> Result<Option<(String, InferenceProfile)>> {
    let escaped_doc_id = escape_graphql_string(doc_id);
    let query = format!(
        r#"{{
            InferenceProfile(
                filter: {{ _docID: {{ _eq: "{escaped_doc_id}" }} }},
                limit: 1
            ) {{
                _docID
                profile_id
                display_name
                context_window
                max_output_tokens
                max_turns
                temperature
                top_p
                top_k
                min_p
                frequency_penalty
                presence_penalty
                repetition_penalty
                reasoning_effort
                stream_batch_ms
                stream_liveness_timeout_secs
                deadline_duration_secs
                retry_max_transport
                retry_backoff_ms
                retry_max_resample
                retry_allow_repair
                retry_interactive_max
            }}
        }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("query InferenceProfile by _docID failed: {:?}", resp.errors);
    }

    Ok(serde_helpers::first_row_with_doc_id(
        resp.data.as_ref(),
        "InferenceProfile",
    ))
}

pub(crate) async fn load_inference_profile_at_cid(
    node: &EmbeddedNode,
    composite_commit_cid: &str,
) -> Result<Option<(String, InferenceProfile)>> {
    let escaped_cid = escape_graphql_string(composite_commit_cid);
    let query = format!(
        r#"{{
            InferenceProfile(cid: ["{escaped_cid}"]) {{{INFERENCE_PROFILE_FIELDS}}}
        }}"#
    );
    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("query InferenceProfile at CID failed: {:?}", resp.errors);
    }
    Ok(serde_helpers::first_row_with_doc_id(
        resp.data.as_ref(),
        "InferenceProfile",
    ))
}

pub async fn list_inference_profile_records(
    node: &EmbeddedNode,
) -> Result<Vec<(String, InferenceProfile)>> {
    let query = r#"{
            InferenceProfile(order: { profile_id: ASC }) {
                _docID
                profile_id
                display_name
                context_window
                max_output_tokens
                max_turns
                temperature
                top_p
                top_k
                min_p
                frequency_penalty
                presence_penalty
                repetition_penalty
                reasoning_effort
                stream_batch_ms
                stream_liveness_timeout_secs
                deadline_duration_secs
                retry_max_transport
                retry_backoff_ms
                retry_max_resample
                retry_allow_repair
                retry_interactive_max
            }
        }"#;

    let resp = node.execute(query).await;
    if resp.has_errors() {
        anyhow::bail!("list InferenceProfile failed: {:?}", resp.errors);
    }

    Ok(serde_helpers::rows_with_doc_id(
        resp.data.as_ref(),
        "InferenceProfile",
    ))
}

pub async fn upsert_inference_profile(
    node: &EmbeddedNode,
    profile: &InferenceProfile,
) -> Result<()> {
    let mutation = upsert_inference_profile_mutation(profile);

    let resp =
        execute_graphql_with_conflict_retry(node, &mutation, "upsert InferenceProfile").await;
    if resp.has_errors() {
        anyhow::bail!("upsert InferenceProfile failed: {:?}", resp.errors);
    }
    Ok(())
}

pub(crate) fn upsert_inference_profile_mutation(profile: &InferenceProfile) -> String {
    let escaped_profile_id = escape_graphql_string(&profile.profile_id);

    let add_fields = vec![
        Some(format!(r#"profile_id: "{escaped_profile_id}""#)),
        graphql_fields::graphql_string_field("display_name", profile.display_name.as_deref()),
        graphql_fields::graphql_optional_int_field("context_window", profile.context_window),
        graphql_fields::graphql_optional_int_field("max_output_tokens", profile.max_output_tokens),
        graphql_fields::graphql_optional_int_field("max_turns", profile.max_turns),
        graphql_fields::graphql_optional_float_field("temperature", profile.temperature),
        graphql_fields::graphql_optional_float_field("top_p", profile.top_p),
        graphql_fields::graphql_optional_int_field("top_k", profile.top_k),
        graphql_fields::graphql_optional_float_field("min_p", profile.min_p),
        graphql_fields::graphql_optional_float_field(
            "frequency_penalty",
            profile.frequency_penalty,
        ),
        graphql_fields::graphql_optional_float_field("presence_penalty", profile.presence_penalty),
        graphql_fields::graphql_optional_float_field(
            "repetition_penalty",
            profile.repetition_penalty,
        ),
        graphql_fields::graphql_string_field(
            "reasoning_effort",
            profile.reasoning_effort.as_deref(),
        ),
        graphql_fields::graphql_optional_int_field("stream_batch_ms", profile.stream_batch_ms),
        graphql_fields::graphql_optional_int_field(
            "stream_liveness_timeout_secs",
            profile.stream_liveness_timeout_secs,
        ),
        graphql_fields::graphql_optional_int_field(
            "deadline_duration_secs",
            profile.deadline_duration_secs,
        ),
        graphql_fields::graphql_optional_int_field(
            "retry_max_transport",
            profile.retry_max_transport,
        ),
        graphql_fields::graphql_int_list_field(
            "retry_backoff_ms",
            profile.retry_backoff_ms.as_deref(),
        ),
        graphql_fields::graphql_optional_int_field(
            "retry_max_resample",
            profile.retry_max_resample,
        ),
        graphql_fields::graphql_optional_bool_field(
            "retry_allow_repair",
            profile.retry_allow_repair,
        ),
        graphql_fields::graphql_optional_int_field(
            "retry_interactive_max",
            profile.retry_interactive_max,
        ),
        Some(format!(
            r#"updated_at: "{}""#,
            escape_graphql_string(&mint_recreate_identity_timestamp())
        )),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(",\n                    ");

    let update_fields = vec![
        graphql_fields::graphql_string_field("display_name", profile.display_name.as_deref()),
        graphql_fields::graphql_optional_int_field("context_window", profile.context_window),
        graphql_fields::graphql_optional_int_field("max_output_tokens", profile.max_output_tokens),
        graphql_fields::graphql_optional_int_field("max_turns", profile.max_turns),
        graphql_fields::graphql_optional_float_field("temperature", profile.temperature),
        graphql_fields::graphql_optional_float_field("top_p", profile.top_p),
        graphql_fields::graphql_optional_int_field("top_k", profile.top_k),
        graphql_fields::graphql_optional_float_field("min_p", profile.min_p),
        graphql_fields::graphql_optional_float_field(
            "frequency_penalty",
            profile.frequency_penalty,
        ),
        graphql_fields::graphql_optional_float_field("presence_penalty", profile.presence_penalty),
        graphql_fields::graphql_optional_float_field(
            "repetition_penalty",
            profile.repetition_penalty,
        ),
        graphql_fields::graphql_string_field(
            "reasoning_effort",
            profile.reasoning_effort.as_deref(),
        ),
        graphql_fields::graphql_optional_int_field("stream_batch_ms", profile.stream_batch_ms),
        graphql_fields::graphql_optional_int_field(
            "stream_liveness_timeout_secs",
            profile.stream_liveness_timeout_secs,
        ),
        graphql_fields::graphql_optional_int_field(
            "deadline_duration_secs",
            profile.deadline_duration_secs,
        ),
        graphql_fields::graphql_optional_int_field(
            "retry_max_transport",
            profile.retry_max_transport,
        ),
        graphql_fields::graphql_int_list_field(
            "retry_backoff_ms",
            profile.retry_backoff_ms.as_deref(),
        ),
        graphql_fields::graphql_optional_int_field(
            "retry_max_resample",
            profile.retry_max_resample,
        ),
        graphql_fields::graphql_optional_bool_field(
            "retry_allow_repair",
            profile.retry_allow_repair,
        ),
        graphql_fields::graphql_optional_int_field(
            "retry_interactive_max",
            profile.retry_interactive_max,
        ),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(",\n                    ");

    format!(
        r#"mutation {{
            upsert_InferenceProfile(
                filter: {{ profile_id: {{ _eq: "{escaped_profile_id}" }} }},
                add: {{
                    {add_fields}
                }},
                update: {{
                    {update_fields}
                }}
            ) {{ _docID }}
        }}"#
    )
}
