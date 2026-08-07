use anyhow::Result;
use gents::graphql::escape_graphql_string;
use serde_json::json;

use crate::cli::*;
use crate::config_writes::mint_recreate_identity_timestamp;
use crate::extract_mutation_doc_id;
use crate::optional_bool_field;
use crate::optional_f64_field;
use crate::optional_i64_field;
use crate::optional_i64_list_field;
use crate::optional_string_field;
use crate::post_graphql;
use crate::print_json;

pub(super) async fn inference_profile_set(args: InferenceProfileUpsertArgs) -> Result<()> {
    if args
        .stream_liveness_timeout_secs
        .is_some_and(|value| value <= 0)
    {
        anyhow::bail!("stream_liveness_timeout_secs must be positive");
    }
    if args
        .top_p
        .is_some_and(|value| !(0.0..=1.0).contains(&value))
    {
        anyhow::bail!("top_p must be within [0, 1]");
    }
    if args
        .min_p
        .is_some_and(|value| !(0.0..=1.0).contains(&value))
    {
        anyhow::bail!("min_p must be within [0, 1]");
    }
    if args.top_k.is_some_and(|value| value <= 0) {
        anyhow::bail!("top_k must be positive");
    }
    if args.seed.is_some_and(|value| value < 0) {
        anyhow::bail!("seed must be non-negative");
    }
    if args.repetition_penalty.is_some_and(|value| value <= 0.0) {
        anyhow::bail!("repetition_penalty must be positive");
    }
    for (name, value) in [
        ("frequency_penalty", args.frequency_penalty),
        ("presence_penalty", args.presence_penalty),
    ] {
        if value.is_some_and(|value| !(-2.0..=2.0).contains(&value)) {
            anyhow::bail!("{name} must be within [-2, 2]");
        }
    }

    let add_fields = vec![
        Some(format!(
            r#"profile_id: "{}""#,
            escape_graphql_string(&args.profile_id)
        )),
        optional_string_field("display_name", args.display_name.as_deref()),
        optional_i64_field("context_window", args.context_window),
        optional_i64_field("max_output_tokens", args.max_output_tokens),
        optional_i64_field("max_turns", args.max_turns),
        optional_f64_field("temperature", args.temperature),
        optional_f64_field("top_p", args.top_p),
        optional_i64_field("top_k", args.top_k),
        optional_i64_field("seed", args.seed),
        optional_f64_field("min_p", args.min_p),
        optional_f64_field("frequency_penalty", args.frequency_penalty),
        optional_f64_field("presence_penalty", args.presence_penalty),
        optional_f64_field("repetition_penalty", args.repetition_penalty),
        optional_string_field("reasoning_effort", args.reasoning_effort.as_deref()),
        optional_i64_field("stream_batch_ms", args.stream_batch_ms),
        optional_i64_field(
            "stream_liveness_timeout_secs",
            args.stream_liveness_timeout_secs,
        ),
        optional_i64_field("deadline_duration_secs", args.deadline_duration_secs),
        optional_i64_field("retry_max_transport", args.retry_max_transport),
        optional_i64_list_field("retry_backoff_ms", args.retry_backoff_ms.as_deref()),
        optional_i64_field("retry_max_resample", args.retry_max_resample),
        optional_bool_field("retry_allow_repair", args.retry_allow_repair),
        optional_i64_field("retry_interactive_max", args.retry_interactive_max),
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
        optional_string_field("display_name", args.display_name.as_deref()),
        optional_i64_field("context_window", args.context_window),
        optional_i64_field("max_output_tokens", args.max_output_tokens),
        optional_i64_field("max_turns", args.max_turns),
        optional_f64_field("temperature", args.temperature),
        optional_f64_field("top_p", args.top_p),
        optional_i64_field("top_k", args.top_k),
        optional_i64_field("seed", args.seed),
        optional_f64_field("min_p", args.min_p),
        optional_f64_field("frequency_penalty", args.frequency_penalty),
        optional_f64_field("presence_penalty", args.presence_penalty),
        optional_f64_field("repetition_penalty", args.repetition_penalty),
        optional_string_field("reasoning_effort", args.reasoning_effort.as_deref()),
        optional_i64_field("stream_batch_ms", args.stream_batch_ms),
        optional_i64_field(
            "stream_liveness_timeout_secs",
            args.stream_liveness_timeout_secs,
        ),
        optional_i64_field("deadline_duration_secs", args.deadline_duration_secs),
        optional_i64_field("retry_max_transport", args.retry_max_transport),
        optional_i64_list_field("retry_backoff_ms", args.retry_backoff_ms.as_deref()),
        optional_i64_field("retry_max_resample", args.retry_max_resample),
        optional_bool_field("retry_allow_repair", args.retry_allow_repair),
        optional_i64_field("retry_interactive_max", args.retry_interactive_max),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(",\n                    ");
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
        profile_id = escape_graphql_string(&args.profile_id),
        add_fields = add_fields,
        update_fields = update_fields,
    );
    let response = post_graphql(&args.graphql, &mutation).await?;
    let doc_id = extract_mutation_doc_id(&response, "InferenceProfile")?;
    let output = json!({
        "doc_id": doc_id,
        "profile_id": args.profile_id,
        "display_name": args.display_name,
        "context_window": args.context_window,
        "max_output_tokens": args.max_output_tokens,
        "max_turns": args.max_turns,
        "temperature": args.temperature,
        "top_p": args.top_p,
        "top_k": args.top_k,
        "seed": args.seed,
        "min_p": args.min_p,
        "frequency_penalty": args.frequency_penalty,
        "presence_penalty": args.presence_penalty,
        "repetition_penalty": args.repetition_penalty,
        "reasoning_effort": args.reasoning_effort,
        "stream_batch_ms": args.stream_batch_ms,
        "stream_liveness_timeout_secs": args.stream_liveness_timeout_secs,
        "deadline_duration_secs": args.deadline_duration_secs,
        "retry_max_transport": args.retry_max_transport,
        "retry_backoff_ms": args.retry_backoff_ms,
        "retry_max_resample": args.retry_max_resample,
        "retry_allow_repair": args.retry_allow_repair,
        "retry_interactive_max": args.retry_interactive_max,
    });
    print_json(&output)?;
    Ok(())
}
