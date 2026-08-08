use anyhow::Result;
use gents::defra_node::{EmbeddedNode, QueryRequest};
use gents::retry::{
    defradb_conflict_retry_backoff, execute_graphql_with_conflict_retry,
    is_defradb_transaction_conflict_text, DEFRA_DB_CONFLICT_MAX_RETRIES,
};
use gents_protocol::transcript::present_persisted_message;
use serde_json::{json, Value};

use super::progress::response_field_is_blank;
use crate::materialized_message_query;

/// All shim reads and auto-committed writes go through the runtime's bounded
/// DefraDB conflict retry (#440): right after startup the runtime's own
/// reconciliation writes are still in flight, and an unretried auto-commit
/// surfaces a raw `transaction conflict` to the Codex client (#933).
pub(super) async fn query_node_json(node: &EmbeddedNode, query: &str) -> Result<Value> {
    let response = execute_graphql_with_conflict_retry(node, query, "codex shim store").await;
    if response.has_errors() {
        anyhow::bail!("GENTS Codex shim query failed: {:?}", response.errors);
    }
    Ok(json!({
        "data": response.data.unwrap_or_else(|| json!({})),
    }))
}

/// Run a write mutation inside a transaction and commit it. A bare
/// auto-committed single mutation (`node.execute`) does NOT emit the DefraDB
/// `Update` event the runtime control watcher reconciles on, but a transaction
/// COMMIT does. Routing skill enable/disable writes through here lets a running
/// agent pick up Codex-driven toggles without a restart — matching the
/// `config skill` CLI path (#340). Mirrors the Local arm of
/// `gents::config_client::txn::ConfigApplyTxn`, plus a bounded conflict retry
/// that arm does not have (#933): a conflicted cycle commits nothing, so a
/// successful call still emits exactly one Update event.
pub(super) async fn execute_committed(node: &EmbeddedNode, mutation: &str) -> Result<Value> {
    let mut retry_index = 0;
    loop {
        match execute_committed_once(node, mutation).await {
            Ok(value) => return Ok(value),
            Err(error)
                if retry_index < DEFRA_DB_CONFLICT_MAX_RETRIES
                    && is_defradb_transaction_conflict_text(&format!("{error:#}")) =>
            {
                let backoff = defradb_conflict_retry_backoff(retry_index);
                retry_index += 1;
                tracing::warn!(
                    retry_count = retry_index,
                    max_retries = DEFRA_DB_CONFLICT_MAX_RETRIES,
                    backoff_ms = backoff.as_millis() as u64,
                    error = %error,
                    "retrying Codex shim committed mutation after transaction conflict"
                );
                tokio::time::sleep(backoff).await;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn execute_committed_once(node: &EmbeddedNode, mutation: &str) -> Result<Value> {
    let handle = node
        .runner()
        .begin_txn(false)
        .await
        .map_err(|error| anyhow::anyhow!("begin_txn: {error}"))?;
    let request = QueryRequest::new(mutation);
    let response = node.execute_request_in_txn(request, &handle).await;
    if response.has_errors() {
        let _ = node.runner().rollback_txn(&handle).await;
        anyhow::bail!("GENTS Codex shim mutation failed: {:?}", response.errors);
    }
    node.runner()
        .commit_txn(&handle)
        .await
        .map_err(|error| anyhow::anyhow!("commit_txn: {error}"))?;
    Ok(json!({
        "data": response.data.unwrap_or_else(|| json!({})),
    }))
}

pub(super) async fn hydrate_materialized_response_content(
    node: &EmbeddedNode,
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

    let message_response =
        query_node_json(node, &materialized_message_query(session_id, sequence)).await?;
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

fn response_materialized_sequence(response: &Value) -> Option<i64> {
    response
        .get("materialized_message_sequence")
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
        })
}
