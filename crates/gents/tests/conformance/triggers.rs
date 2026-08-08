//! Task 30 — conformance tests for the trigger engine + EventTrigger lifecycle.
//!
//! # Scope
//!
//! These tests lock down the **externally-observable** conformance contract
//! between the trigger engine, the `EventSource`, the
//! `ProductionMaterializer`, and DefraDB. Full engine-level e2e coverage for
//! the event pipeline (source-doc create → materialized `AgentRequest` with
//! lineage) is handled by `tests/event_trigger_e2e.rs` (PR 2 Task 24). The
//! cases here pin the corresponding conformance surface:
//!
//! * `fires_on_matching_source_doc_create` — a filter-less EventTrigger
//!   materializes an `AgentRequest` with `caused_by_trigger_kind = "event"`
//!   and the rendered template in `content`.
//! * `does_not_fire_when_source_doc_fails_filter` — an EventTrigger gated by a
//!   `kind == "signup"` filter does NOT fire for `kind: "other"` source docs,
//!   and the runtime bookkeeping on the trigger row stays null.
//! * `enabled_false_does_not_fire` — `enabled: false` triggers never
//!   materialize requests, even for matching source docs.
//! * `backfill_is_forward_only` — pre-existing source docs are NEVER replayed
//!   when a trigger becomes active; only NEW doc-create events fire.
//! * `subscription_reconciles_on_generation_bump` — re-pointing a trigger from
//!   collection A to collection B bumps `active_generation` and only B-side
//!   writes fire afterwards.
//! * `serial_skips_when_prior_active_runtime` — the engine's gating query sees an
//!   active runtime `(trigger_id, "event")` tuple and the serial trigger skips.
//! * `latest_only_supersedes_prior_fire` — the supersede mutation the engine
//!   would run transitions the in-flight event-kind request to
//!   `superseded`, and a new materialize lands with the same lineage.
//! * `template_render_failure_records_error_status` — an event-kind render
//!   failure writes `last_status = "error"` / `last_error = ...` on the
//!   EventTrigger doc without materializing a request.
//! * `two_triggers_same_source_collection_each_evaluate_filter_independently`
//!   — two triggers on the same `source_collection` apply their own filters
//!   independently; only the trigger whose filter matches fires.
//!
//! # Pragmatic split: engine semantics vs. persistence/operational surfaces
//!
//! The pure trigger-engine branch matrix is pinned in-crate by
//! `trigger_engine::tests::trigger_engine_dispatch_matches_lean_generated_contract_cases`,
//! which consumes finite cases emitted by
//! `Proofs/Conformance/Triggers/Contracts.lean`. That Lean-generated contract
//! covers manual dispatch, schedule/event reachability, tuple-sensitive serial
//! gating, latest-only supersession, parallel bypass of in-flight gates, and
//! lineage shape without depending on wall-clock debounce.
//!
//! Cases 6, 7, 8 remain asserted here at the persistence-layer contract (seed
//! an in-flight `AgentRequest` with the right lineage tuple + simulate the
//! exact mutation / writeback the production materializer/source produces).
//! They are still valuable because they pin the DefraDB query/mutation shape
//! the engine delegates to at runtime, but they are no longer the only
//! correctness oracle for serial/latest-only trigger behavior.
//!
//! Cases 1, 2, 3, 4, 5, 9 boot a real `Gents` so the EventSource loop
//! actually observes DefraDB events; these are the tests where the
//! externally-observable behavior *only* exists if the live subscription +
//! filter + materialize chain runs end to end.

use std::sync::Arc;
use std::time::Duration;

use gents::defra_node::EmbeddedNode;
use gents::graphql::escape_graphql_string;
use gents::lifecycle::{ExecutionOrigin, RequestLifecycle, TriggerLineage};
use gents::{AgentIdentity, DocumentRuntimeOptions, Gents, ToolCeiling};
use serde_json::Value;

use crate::support::fixtures::{bind_default_behavior_backend, test_identity};
use crate::support::mock_endpoint::MockModelEndpoint;
use crate::support::snapshots::{fetch_runtime_snapshot, RuntimeSnapshot};
use crate::support::{AGENT_NAME, BACKEND_ID, DEADLINE_SECS};
use crate::{
    signed_materializer_agent_did, signed_materializer_test_db,
    signed_materializer_test_db as test_db,
};

const RUNTIME_SNAPSHOT_WAIT: Duration = Duration::from_secs(60);

async fn register_webhook_event_schema(node: &EmbeddedNode) {
    let sdl = r#"
        type WebhookEvent {
            external_id: String
            payload: String
            kind: String @index
        }
    "#;
    node.add_schema(sdl)
        .await
        .expect("add_schema for WebhookEvent");
}

async fn register_audit_event_schema(node: &EmbeddedNode) {
    let sdl = r#"
        type AuditEvent {
            external_id: String
            payload: String
            kind: String @index
        }
    "#;
    node.add_schema(sdl)
        .await
        .expect("add_schema for AuditEvent");
}

async fn create_task(node: &EmbeddedNode, task_id: &str, behavior_id: &str, prompt_template: &str) {
    let escaped_task_id = escape_graphql_string(task_id);
    let escaped_behavior_id = escape_graphql_string(behavior_id);
    let escaped_prompt_template = escape_graphql_string(prompt_template);
    let mutation = format!(
        r#"mutation {{
            create_Task(input: {{
                task_id: "{escaped_task_id}",
                name: "{escaped_task_id}",
                behavior_id: "{escaped_behavior_id}",
                prompt_template: "{escaped_prompt_template}",
                enabled: true
            }}) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(!resp.has_errors(), "create Task failed: {:?}", resp.errors);
}

#[allow(clippy::too_many_arguments)]
async fn create_event_trigger(
    node: &EmbeddedNode,
    trigger_id: &str,
    task_id: &str,
    source_collection: &str,
    event_kind: &str,
    filter: Option<&str>,
    enabled: bool,
    concurrency: &str,
) {
    let escaped_trigger_id = escape_graphql_string(trigger_id);
    let escaped_task_id = escape_graphql_string(task_id);
    let escaped_source_collection = escape_graphql_string(source_collection);
    let escaped_event_kind = escape_graphql_string(event_kind);
    let escaped_concurrency = escape_graphql_string(concurrency);
    let filter_entry = match filter {
        Some(f) => format!(", filter: \"{}\"", escape_graphql_string(f)),
        None => String::new(),
    };
    let mutation = format!(
        r#"mutation {{
            create_EventTrigger(input: {{
                trigger_id: "{escaped_trigger_id}",
                task_id: "{escaped_task_id}",
                source_collection: "{escaped_source_collection}",
                event_kind: "{escaped_event_kind}",
                enabled: {enabled},
                concurrency: "{escaped_concurrency}",
                fire_count: 0{filter_entry}
            }}) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "create EventTrigger failed: {:?}",
        resp.errors
    );
}

/// Update the apply-owned `source_collection` field on an EventTrigger (used
/// by the generation-bump reconciliation test).
///
/// DefraDB rejects `update_EventTrigger` mutations on a doc whose existing
/// `last_attempt_at` (a `DateTime` scalar) is not restated in the input — it
/// appears to re-validate scalar DateTime fields during the round-trip and
/// trips on the `String(...)` vs `Scalar(DateTime)` mismatch. Mirror the
/// workaround PR 1's `schedule_writeback_errored` helper takes: read the
/// current `last_attempt_at` and restate it in the update input if present.
async fn update_event_trigger_source_collection(
    node: &EmbeddedNode,
    trigger_id: &str,
    new_source_collection: &str,
) {
    let escaped_trigger_id = escape_graphql_string(trigger_id);
    let escaped_new = escape_graphql_string(new_source_collection);

    let read_query = format!(
        r#"{{
            EventTrigger(
                filter: {{ trigger_id: {{ _eq: "{escaped_trigger_id}" }} }},
                limit: 1
            ) {{ last_attempt_at }}
        }}"#
    );
    let read_resp = node.execute(&read_query).await;
    assert!(
        !read_resp.has_errors(),
        "read EventTrigger last_attempt_at failed: {:?}",
        read_resp.errors
    );
    let last_attempt_at = read_resp
        .data
        .as_ref()
        .and_then(|d| d.get("EventTrigger"))
        .and_then(|v| v.as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("last_attempt_at"))
        .and_then(|v| v.as_str())
        .map(str::to_owned);

    let last_attempt_entry = match last_attempt_at.as_deref() {
        Some(v) => format!(", last_attempt_at: \"{}\"", escape_graphql_string(v)),
        None => String::new(),
    };
    let mutation = format!(
        r#"mutation {{
            update_EventTrigger(
                filter: {{ trigger_id: {{ _eq: "{escaped_trigger_id}" }} }},
                input: {{ source_collection: "{escaped_new}"{last_attempt_entry} }}
            ) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "update EventTrigger source_collection failed: {:?}",
        resp.errors
    );
}

async fn write_webhook_event(node: &EmbeddedNode, external_id: &str, kind: &str) -> String {
    write_dynamic_event(node, "WebhookEvent", external_id, kind).await
}

async fn write_dynamic_event(
    node: &EmbeddedNode,
    collection: &str,
    external_id: &str,
    kind: &str,
) -> String {
    let escaped_external_id = escape_graphql_string(external_id);
    let escaped_kind = escape_graphql_string(kind);
    let mutation = format!(
        r#"mutation {{
            add_{collection}(input: {{
                external_id: "{escaped_external_id}",
                payload: "{{}}",
                kind: "{escaped_kind}"
            }}) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "add_{collection} failed: {:?}",
        resp.errors
    );
    let data = resp
        .data
        .as_ref()
        .unwrap_or_else(|| panic!("add_{collection} response missing data"));
    let field = data
        .get(format!("add_{collection}"))
        .or_else(|| data.get(format!("create_{collection}")))
        .unwrap_or_else(|| panic!("add_/create_{collection} key missing; data={data:?}"));
    field
        .get("_docID")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            field
                .as_array()
                .and_then(|rows| rows.first())
                .and_then(|row| row.get("_docID"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| panic!("{collection} mutation returned no _docID: {field}"))
}

async fn count_agent_requests_for_trigger(
    node: &EmbeddedNode,
    trigger_id: &str,
    trigger_kind: &str,
) -> usize {
    let escaped_trigger_id = escape_graphql_string(trigger_id);
    let escaped_trigger_kind = escape_graphql_string(trigger_kind);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{
                    caused_by_trigger_id: {{ _eq: "{escaped_trigger_id}" }},
                    caused_by_trigger_kind: {{ _eq: "{escaped_trigger_kind}" }}
                }}
            ) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&query).await;
    assert!(
        !resp.has_errors(),
        "count AgentRequest by trigger failed: {:?}",
        resp.errors
    );
    resp.data
        .as_ref()
        .and_then(|d| d.get("AgentRequest"))
        .and_then(|v| v.as_array())
        .map(|rows| rows.len())
        .unwrap_or(0)
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
struct EventTriggerRow {
    fire_count: Option<i64>,
    last_status: Option<String>,
    last_error: Option<String>,
    last_fired_source_doc_id: Option<String>,
    enabled: Option<bool>,
    source_collection: Option<String>,
    event_kind: Option<String>,
    concurrency: Option<String>,
    task_id: Option<String>,
}

async fn fetch_event_trigger_row(node: &EmbeddedNode, trigger_id: &str) -> Option<EventTriggerRow> {
    let escaped_trigger_id = escape_graphql_string(trigger_id);
    let query = format!(
        r#"{{
            EventTrigger(
                filter: {{ trigger_id: {{ _eq: "{escaped_trigger_id}" }} }},
                limit: 1
            ) {{
                fire_count
                last_status
                last_error
                last_fired_source_doc_id
                enabled
                source_collection
                event_kind
                concurrency
                task_id
            }}
        }}"#
    );
    let resp = node.execute(&query).await;
    assert!(
        !resp.has_errors(),
        "fetch EventTrigger row failed: {:?}",
        resp.errors
    );
    let row = resp
        .data
        .as_ref()
        .and_then(|d| d.get("EventTrigger"))
        .and_then(|v| v.as_array())
        .and_then(|rows| rows.first())
        .cloned()?;
    Some(EventTriggerRow {
        fire_count: row.get("fire_count").and_then(|v| v.as_i64()),
        last_status: row
            .get("last_status")
            .and_then(|v| v.as_str())
            .map(str::to_owned),
        last_error: row
            .get("last_error")
            .and_then(|v| v.as_str())
            .map(str::to_owned),
        last_fired_source_doc_id: row
            .get("last_fired_source_doc_id")
            .and_then(|v| v.as_str())
            .map(str::to_owned),
        enabled: row.get("enabled").and_then(|v| v.as_bool()),
        source_collection: row
            .get("source_collection")
            .and_then(|v| v.as_str())
            .map(str::to_owned),
        event_kind: row
            .get("event_kind")
            .and_then(|v| v.as_str())
            .map(str::to_owned),
        concurrency: row
            .get("concurrency")
            .and_then(|v| v.as_str())
            .map(str::to_owned),
        task_id: row
            .get("task_id")
            .and_then(|v| v.as_str())
            .map(str::to_owned),
    })
}

async fn has_active_runtime_request_for_trigger(
    node: &EmbeddedNode,
    agent_did: &str,
    trigger_id: &str,
    trigger_kind: &str,
) -> bool {
    let escaped_agent_did = escape_graphql_string(agent_did);
    let escaped_trigger_id = escape_graphql_string(trigger_id);
    let escaped_trigger_kind = escape_graphql_string(trigger_kind);
    let query = format!(
        r#"query {{
            AgentRequest(
                filter: {{
                    agent_did: {{ _eq: "{escaped_agent_did}" }},
                    caused_by_trigger_id: {{ _eq: "{escaped_trigger_id}" }},
                    caused_by_trigger_kind: {{ _eq: "{escaped_trigger_kind}" }},
                    lifecycle_state: {{ _in: ["pending", "claimed", "processing"] }}
                }},
                limit: 1
            ) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&query).await;
    assert!(
        !resp.has_errors(),
        "has_active_runtime_request_for_trigger failed: {:?}",
        resp.errors
    );
    resp.data
        .as_ref()
        .and_then(|d| d.get("AgentRequest"))
        .and_then(|v| v.as_array())
        .map(|rows| !rows.is_empty())
        .unwrap_or(false)
}

async fn supersede_active_runtime_requests_for_trigger(
    node: &EmbeddedNode,
    agent_did: &str,
    trigger_id: &str,
    trigger_kind: &str,
) -> usize {
    let escaped_agent_did = escape_graphql_string(agent_did);
    let escaped_trigger_id = escape_graphql_string(trigger_id);
    let escaped_trigger_kind = escape_graphql_string(trigger_kind);
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{
                    agent_did: {{ _eq: "{escaped_agent_did}" }},
                    caused_by_trigger_id: {{ _eq: "{escaped_trigger_id}" }},
                    caused_by_trigger_kind: {{ _eq: "{escaped_trigger_kind}" }},
                    lifecycle_state: {{ _in: ["pending", "claimed", "processing"] }}
                }},
                input: {{
                    status: "superseded",
                    lifecycle_state: "superseded"
                }}
            ) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "supersede_active_runtime_requests_for_trigger failed: {:?}",
        resp.errors
    );
    resp.data
        .as_ref()
        .and_then(|d| d.get("update_AgentRequest"))
        .and_then(|v| v.as_array())
        .map(|rows| rows.len())
        .unwrap_or(0)
}

async fn fetch_request_state(node: &EmbeddedNode, request_id: &str) -> Option<(String, String)> {
    let escaped_request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                limit: 1
            ) {{
                lifecycle_state
                status
            }}
        }}"#
    );
    let resp = node.execute(&query).await;
    assert!(
        !resp.has_errors(),
        "fetch_request_state failed: {:?}",
        resp.errors
    );
    resp.data
        .as_ref()
        .and_then(|d| d.get("AgentRequest"))
        .and_then(|v| v.as_array())
        .and_then(|rows| rows.first())
        .cloned()
        .map(|row| {
            (
                row.get("lifecycle_state")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_owned(),
                row.get("status")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_owned(),
            )
        })
}

struct BootedAgent {
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    handle: tokio::task::JoinHandle<anyhow::Result<()>>,
    _endpoint: MockModelEndpoint,
    agent_did: String,
    default_behavior_id: String,
}

impl BootedAgent {
    async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        match tokio::time::timeout(Duration::from_secs(5), self.handle).await {
            Ok(join_result) => {
                let _ = join_result;
            }
            Err(_) => panic!("agent did not shut down within 5s"),
        }
    }
}

async fn boot_agent(db: &crate::support::TestDb, test_name: &str, backend_id: &str) -> BootedAgent {
    let identity: Arc<dyn AgentIdentity> = Arc::new(test_identity(test_name));
    let mock_endpoint = MockModelEndpoint::start("default").unwrap();
    bind_default_behavior_backend(
        db.node.as_ref(),
        identity.did(),
        backend_id,
        mock_endpoint.endpoint(),
    )
    .await;
    let agent = Gents::from_default_behavior_documents(
        db.node.clone(),
        identity,
        DocumentRuntimeOptions {
            tool_ceiling: ToolCeiling::meta_only(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let agent_did = agent.agent_did().to_string();
    let default_behavior_id = agent.default_behavior_id().to_string();

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = tokio::spawn(agent.run(shutdown_rx));

    wait_for_runtime_snapshot(db.node.as_ref(), &agent_did, |snapshot| {
        snapshot.process_state == "ready"
            && snapshot.reconcile_phase == "idle"
            && snapshot.active_generation >= 1
            && snapshot.runnable_behavior_count >= 1
    })
    .await;

    BootedAgent {
        shutdown_tx,
        handle,
        _endpoint: mock_endpoint,
        agent_did,
        default_behavior_id,
    }
}

async fn wait_for_runtime_snapshot<F>(
    node: &EmbeddedNode,
    agent_did: &str,
    predicate: F,
) -> RuntimeSnapshot
where
    F: Fn(&RuntimeSnapshot) -> bool,
{
    let deadline = tokio::time::Instant::now() + RUNTIME_SNAPSHOT_WAIT;
    let mut last_snapshot = None;
    let mut sleep = Duration::from_millis(50);
    loop {
        if let Some(snapshot) = fetch_runtime_snapshot(node, agent_did).await {
            if predicate(&snapshot) {
                return snapshot;
            }
            last_snapshot = Some(snapshot);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out after {:?} waiting for runtime snapshot for {agent_did}; last_snapshot={last_snapshot:?}",
            RUNTIME_SNAPSHOT_WAIT,
        );
        tokio::time::sleep(sleep).await;
        sleep = (sleep * 2).min(Duration::from_millis(250));
    }
}

async fn wait_for_request_count(
    node: &EmbeddedNode,
    trigger_id: &str,
    expected: usize,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let count = count_agent_requests_for_trigger(node, trigger_id, "event").await;
        if count == expected {
            return;
        }
        if count > expected {
            panic!("over-fire for trigger_id={trigger_id}: expected {expected}, got {count}");
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for request_count({trigger_id}) == {expected}; got {count}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn assert_no_request_within(node: &EmbeddedNode, trigger_id: &str, settle: Duration) {
    tokio::time::sleep(settle).await;
    let count = count_agent_requests_for_trigger(node, trigger_id, "event").await;
    assert_eq!(
        count, 0,
        "expected no AgentRequest for trigger_id={trigger_id} but got {count}"
    );
}

async fn wait_for_last_status(
    node: &EmbeddedNode,
    trigger_id: &str,
    desired: &str,
    timeout: Duration,
) -> EventTriggerRow {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(row) = fetch_event_trigger_row(node, trigger_id).await {
            if row.last_status.as_deref() == Some(desired) {
                return row;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "timed out waiting for EventTrigger({trigger_id}).last_status = {desired:?}; \
                 got {:?}",
                fetch_event_trigger_row(node, trigger_id).await
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[path = "triggers_cases/concurrency_persistence.rs"]
mod concurrency_persistence;
#[path = "triggers_cases/event_source_cases.rs"]
mod event_source_cases;
