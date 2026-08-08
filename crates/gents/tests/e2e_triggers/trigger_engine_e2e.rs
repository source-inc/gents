//! Task 39 Step 1 — external-crate integration test for the TriggerEngine +
//! ScheduleSource pipeline.
//!
//! The full engine-level end-to-end assertion (a due Schedule driving
//! materialization of an `AgentRequest` with populated trigger lineage) lives
//! inside `crates/gents/src/trigger_engine/tests.rs` because the engine,
//! source, and materializer types are crate-private (`pub(crate)`). From the
//! outside, we can't construct an `ActiveRuntimeSnapshot` with a loaded
//! behavior without going through the full `Gents` bootstrap; doing that
//! here would effectively duplicate `tests/schedule_snapshot_reconcile.rs`.
//!
//! Instead, this file asserts the externally-observable end of the
//! pipeline: when `materialize_claimed_with_execution_binding` is called with
//! a populated `TriggerLineage`, the `AgentRequest` that lands in DefraDB
//! carries the correct `caused_by_trigger_id` / `caused_by_trigger_kind`
//! fields and the correct `execution_origin = scheduled`. This is the same
//! persistence boundary `ProductionMaterializer` wraps at runtime, so a
//! regression here is a regression in the engine's materialization surface
//! regardless of which concrete materializer is wired in.

use std::sync::Arc;

use gents::graphql::escape_graphql_string;
use gents::lifecycle::{ExecutionOrigin, RequestLifecycle, TriggerLineage};
use gents::AgentIdentity;

use crate::support::fixtures::test_identity;
use crate::support::{test_db_with_identity, AGENT_NAME, BACKEND_ID, DEADLINE_SECS};

#[tokio::test]
async fn scheduled_fire_persists_trigger_lineage_on_agent_request() {
    let identity: Arc<dyn AgentIdentity> = Arc::new(test_identity("trigger-engine-e2e-lineage"));
    let agent_did = identity.did().to_string();
    let db = test_db_with_identity("trigger-engine-e2e-lineage", identity).await;

    let lineage = TriggerLineage {
        trigger_id: Some("sched-e2e".to_string()),
        trigger_kind: Some("schedule".to_string()),
    };

    let lifecycle = RequestLifecycle::materialize_claimed_with_execution_binding(
        db.node.clone(),
        AGENT_NAME,
        &agent_did,
        "rendered prompt from trigger engine",
        DEADLINE_SECS,
        ExecutionOrigin::Scheduled,
        BACKEND_ID,
        lineage,
    )
    .await
    .expect("materialize claimed with lineage should succeed");

    let request_id = escape_graphql_string(&lifecycle.request().request_id);
    let query = format!(
        r#"query {{
            AgentRequest(filter: {{ request_id: {{ _eq: "{request_id}" }} }}) {{
                caused_by_trigger_id
                caused_by_trigger_kind
                execution_origin
                lifecycle_state
            }}
        }}"#
    );
    let resp = db.node.execute(&query).await;
    assert!(
        !resp.has_errors(),
        "AgentRequest query errored: {:?}",
        resp.errors
    );

    let row = resp
        .data
        .as_ref()
        .and_then(|d| d.get("AgentRequest"))
        .and_then(|v| v.as_array())
        .and_then(|rows| rows.first())
        .cloned()
        .expect("expected exactly one AgentRequest for the materialized fire");

    assert_eq!(
        row.get("caused_by_trigger_id").and_then(|v| v.as_str()),
        Some("sched-e2e"),
        "caused_by_trigger_id missing or wrong: {row}"
    );
    assert_eq!(
        row.get("caused_by_trigger_kind").and_then(|v| v.as_str()),
        Some("schedule"),
        "caused_by_trigger_kind missing or wrong: {row}"
    );
    assert_eq!(
        row.get("execution_origin").and_then(|v| v.as_str()),
        Some("scheduled"),
        "execution_origin should be scheduled for trigger-driven fires: {row}"
    );
    assert_eq!(
        row.get("lifecycle_state").and_then(|v| v.as_str()),
        Some("claimed"),
        "materialize_claimed_with_execution_binding should persist lifecycle_state=claimed: {row}"
    );
    assert!(
        lifecycle.execution_provenance().is_some(),
        "materialization must return the cryptographically verified source/claim chain"
    );
}
