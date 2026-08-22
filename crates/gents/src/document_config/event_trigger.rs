use anyhow::Result;
use defra_node::EmbeddedNode;
use serde::{Deserialize, Serialize};

use super::serde_helpers::{first_row_with_doc_id, rows_with_doc_id};
use crate::graphql::{escape_graphql_string, graphql_mutation_with_transaction_retry};

/// Runtime-owned EventTrigger fields the trigger engine writes back after a
/// fire attempt.
///
/// Each field is optional so callers can update a subset — the helper only
/// emits GraphQL input entries for the fields that are `Some`, leaving
/// apply-owned fields (`enabled`, `task_id`, `source_collection`,
/// `event_kind`, `filter`, `concurrency`) untouched. `fire_count_delta`
/// expresses the desired increment (typically `+1` on a successful fire); the
/// helper performs a read-then-write because DefraDB does not currently expose
/// atomic increments. Racing writes may undercount, which is acceptable for
/// PR 2 (fire_count is bookkeeping, not a correctness-critical counter).
#[allow(dead_code)]
#[derive(Debug, Default, Clone)]
pub(crate) struct EventTriggerRuntimeUpdate {
    pub(crate) last_attempt_at: Option<String>,
    pub(crate) last_fired_source_doc_id: Option<String>,
    pub(crate) last_status: Option<String>,
    pub(crate) last_error: Option<String>,
    pub(crate) fire_count_delta: Option<i64>,
}

/// Update the runtime-owned fields on an `EventTrigger` document identified by
/// its apply-owned `trigger_id`.
///
/// Only writes fields present in `updates`; apply-owned fields (`enabled`,
/// `task_id`, `source_collection`, `event_kind`, `filter`, `concurrency`) are
/// never touched. Returns `Ok` even when the trigger doc is missing — the
/// caller is assumed to have raced a delete from apply, which the reconcile
/// path will resolve.
///
/// `fire_count_delta` triggers a read-then-write: the current `fire_count` is
/// loaded, the delta added, and the new value written. DefraDB does not
/// expose atomic increments today, so racing concurrent updates may
/// undercount; this is acceptable for the EventTrigger `fire_count` field per
/// the event-driven-tasks PR 2 plan.
#[allow(dead_code)]
pub(crate) async fn update_event_trigger_runtime_fields(
    node: &EmbeddedNode,
    trigger_id: &str,
    updates: EventTriggerRuntimeUpdate,
) -> Result<()> {
    // Short-circuit: nothing to write.
    if updates.last_attempt_at.is_none()
        && updates.last_fired_source_doc_id.is_none()
        && updates.last_status.is_none()
        && updates.last_error.is_none()
        && updates.fire_count_delta.is_none()
    {
        return Ok(());
    }

    // Resolve the current fire_count if we need to increment it. Also use this
    // to detect whether the trigger doc still exists (idempotent behavior on
    // a deleted trigger).
    let current_fire_count = if updates.fire_count_delta.is_some() {
        let escaped_trigger_id = escape_graphql_string(trigger_id);
        let query = format!(
            r#"{{
                EventTrigger(
                    filter: {{ trigger_id: {{ _eq: "{escaped_trigger_id}" }} }},
                    limit: 1
                ) {{
                    fire_count
                }}
            }}"#
        );
        let resp = node.execute(&query).await;
        if resp.has_errors() {
            anyhow::bail!(
                "query EventTrigger fire_count for runtime update failed: {:?}",
                resp.errors
            );
        }
        let rows = resp
            .data
            .as_ref()
            .and_then(|data| data.get("EventTrigger"))
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        if rows.is_empty() {
            // EventTrigger doc disappeared; nothing to update.
            tracing::info!(
                trigger_id,
                "EventTrigger doc missing during runtime update; skipping"
            );
            return Ok(());
        }
        rows.first()
            .and_then(|row| row.get("fire_count"))
            .and_then(|value| value.as_i64())
            .unwrap_or(0)
    } else {
        0
    };

    // Build the input literal with only the requested fields so apply-owned
    // fields are never overwritten.
    let mut entries: Vec<String> = Vec::new();
    if let Some(v) = updates.last_attempt_at.as_ref() {
        entries.push(format!("last_attempt_at: \"{}\"", escape_graphql_string(v)));
    }
    if let Some(v) = updates.last_fired_source_doc_id.as_ref() {
        entries.push(format!(
            "last_fired_source_doc_id: \"{}\"",
            escape_graphql_string(v)
        ));
    }
    if let Some(v) = updates.last_status.as_ref() {
        entries.push(format!("last_status: \"{}\"", escape_graphql_string(v)));
    }
    if let Some(v) = updates.last_error.as_ref() {
        entries.push(format!("last_error: \"{}\"", escape_graphql_string(v)));
    }
    if let Some(delta) = updates.fire_count_delta {
        let new_fire_count = current_fire_count.saturating_add(delta);
        entries.push(format!("fire_count: {new_fire_count}"));
    }
    let input_literal = format!("{{ {} }}", entries.join(", "));

    let escaped_trigger_id = escape_graphql_string(trigger_id);
    // Use a filter-based mutation so we key on the apply-owned trigger_id and
    // don't need to resolve the _docID separately. DefraDB matches at most one
    // trigger (trigger_id is unique) so this updates the single target doc.
    let mutation = format!(
        r#"mutation {{
            update_EventTrigger(
                filter: {{ trigger_id: {{ _eq: "{escaped_trigger_id}" }} }},
                input: {input_literal}
            ) {{ _docID }}
        }}"#
    );

    graphql_mutation_with_transaction_retry(node, &mutation, "update EventTrigger runtime fields")
        .await?;

    Ok(())
}

/// Description of an event-driven trigger for a task.
///
/// Mirrors the `EventTrigger` GraphQL schema in
/// `crates/gents-schemas/schemas/agent/event_trigger.graphql`. Includes
/// both apply-owned fields (`trigger_id`, `task_id`, `source_collection`,
/// `event_kind`, `filter`, `enabled`, `concurrency`, `created_at`,
/// `updated_at`) and runtime-owned fields (`last_attempt_at`,
/// `last_fired_source_doc_id`, `last_status`, `last_error`, `fire_count`)
/// because `DocumentRuntimeView` is a DB-read view.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct EventTrigger {
    pub(crate) trigger_id: String,
    #[serde(default)]
    pub(crate) task_id: Option<String>,
    #[serde(default)]
    pub(crate) source_collection: Option<String>,
    #[serde(default)]
    pub(crate) event_kind: Option<String>,
    #[serde(default)]
    pub(crate) filter: Option<String>,
    #[serde(default)]
    pub(crate) enabled: Option<bool>,
    #[serde(default)]
    pub(crate) concurrency: Option<String>,
    #[serde(default)]
    pub(crate) correlation_field: Option<String>,
    #[serde(default)]
    pub(crate) fire_mode: Option<String>,
    #[serde(default)]
    pub(crate) expected_count: Option<i64>,
    #[serde(default)]
    pub(crate) expected_count_field: Option<String>,
    #[serde(default)]
    pub(crate) group_timeout_secs: Option<i64>,
    #[serde(default)]
    pub(crate) group_min_count: Option<i64>,
    #[serde(default)]
    pub(crate) workspace_authority: Option<String>,
    #[serde(default)]
    pub(crate) created_at: Option<String>,
    #[serde(default)]
    pub(crate) updated_at: Option<String>,
    // runtime-owned:
    #[serde(default)]
    pub(crate) last_attempt_at: Option<String>,
    #[serde(default)]
    pub(crate) last_fired_source_doc_id: Option<String>,
    #[serde(default)]
    pub(crate) last_status: Option<String>,
    #[serde(default)]
    pub(crate) last_error: Option<String>,
    #[serde(default)]
    pub(crate) fire_count: Option<i64>,
}

/// List every `EventTrigger` document in the node, returning
/// `(doc_id, event_trigger)` pairs.
///
/// EventTriggers are addressed by a globally unique `trigger_id` (see
/// `event_trigger.graphql`), so this helper is not scoped by `agent_did`.
#[allow(dead_code)]
pub(crate) async fn list_event_trigger_records(
    node: &EmbeddedNode,
) -> Result<Vec<(String, EventTrigger)>> {
    let query = r#"{
            EventTrigger(order: { trigger_id: ASC }) {
                _docID
                trigger_id
                task_id
                source_collection
                event_kind
                filter
                enabled
                concurrency
                correlation_field
                fire_mode
                expected_count
                expected_count_field
                group_timeout_secs
                group_min_count
                workspace_authority
                created_at
                updated_at
                last_attempt_at
                last_fired_source_doc_id
                last_status
                last_error
                fire_count
            }
        }"#;

    let resp = node.execute(query).await;
    if resp.has_errors() {
        anyhow::bail!("list EventTrigger failed: {:?}", resp.errors);
    }

    Ok(rows_with_doc_id(resp.data.as_ref(), "EventTrigger"))
}

/// Load a single `EventTrigger` document by its DefraDB `_docID`.
///
/// Used by the control watcher's update-dispatch path to classify an updated
/// document by collection when only the `_docID` is known.
#[allow(dead_code)]
pub(crate) async fn load_event_trigger_by_doc_id(
    node: &EmbeddedNode,
    doc_id: &str,
) -> Result<Option<(String, EventTrigger)>> {
    let escaped_doc_id = escape_graphql_string(doc_id);
    let query = format!(
        r#"{{
            EventTrigger(
                filter: {{ _docID: {{ _eq: "{escaped_doc_id}" }} }},
                limit: 1
            ) {{
                _docID
                trigger_id
                task_id
                source_collection
                event_kind
                filter
                enabled
                concurrency
                correlation_field
                fire_mode
                expected_count
                expected_count_field
                group_timeout_secs
                group_min_count
                workspace_authority
                created_at
                updated_at
                last_attempt_at
                last_fired_source_doc_id
                last_status
                last_error
                fire_count
            }}
        }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("query EventTrigger by _docID failed: {:?}", resp.errors);
    }

    Ok(first_row_with_doc_id(resp.data.as_ref(), "EventTrigger"))
}
