use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use gents::goal::{load_canonical_goal, set_goal, update_goal_fields, GoalStatus};
use gents::{ActiveRuntimeSnapshot, GoalSource, TriggerSource, UpdateSubscriptionSource};
use serde::Deserialize;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

mod support;

use support::mock_subscription::MockUpdateSubscriptionSource;
use support::{
    create_request, create_response_with_content_and_status, set_request_lifecycle_state, test_db,
    TestDb, AGENT_DID,
};

const SESSION: &str = "goal-session";
const RESCAN: Duration = Duration::from_millis(20);

fn snapshot() -> Arc<ActiveRuntimeSnapshot> {
    Arc::new(ActiveRuntimeSnapshot {
        generation: 1,
        principal: None,
        local_did: AGENT_DID.to_string(),
        paired_peer_dids: HashSet::new(),
        default_behavior_id: support::AGENT_NAME.to_string(),
        behaviors: HashMap::new(),
        config_provenance_scope: gents::rendered_request::ConfigProvenanceScope::StaticOrOneShot,
        behavior_config_provenance: HashMap::new(),
        tool_surfaces: HashMap::new(),
        backend_admission_configs: HashMap::new(),
        unavailable_behaviors: HashMap::new(),
        active_schedules: HashMap::new(),
        unavailable_schedules: HashSet::new(),
        active_event_triggers: HashMap::new(),
        unavailable_event_triggers: HashSet::new(),
        active_tasks: HashMap::new(),
        dispatchers: HashMap::new(),
        behavior_executor_capacities: HashMap::new(),
        behavior_executor_queue_capacities: HashMap::new(),
    })
}

fn source(db: &TestDb) -> (GoalSource, watch::Sender<Arc<ActiveRuntimeSnapshot>>) {
    let (tx, rx) = watch::channel(snapshot());
    let subscriptions: Arc<dyn UpdateSubscriptionSource> =
        Arc::new(MockUpdateSubscriptionSource::new());
    (
        GoalSource::with_subscription_source(
            subscriptions,
            rx,
            db.node.clone(),
            CancellationToken::new(),
        )
        .with_rescan_interval(RESCAN),
        tx,
    )
}

async fn seed_completed_request(db: &TestDb, request_id: &str) -> String {
    let doc_id = create_request(
        db.node.as_ref(),
        request_id,
        SESSION,
        "completed",
        "2026-07-15T00:00:00Z",
    )
    .await;
    create_response_with_content_and_status(
        db.node.as_ref(),
        &format!("response-{request_id}"),
        request_id,
        SESSION,
        "durable progress",
        "complete",
    )
    .await;
    doc_id
}

#[derive(Debug, Deserialize)]
struct ChildRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    request_id: String,
    session_id: String,
    caused_by_parent_request_id: Option<String>,
    caused_by_trigger_id: Option<String>,
    caused_by_trigger_kind: Option<String>,
    metadata: Option<String>,
    lifecycle_state: Option<String>,
}

async fn goal_children(db: &TestDb) -> Vec<ChildRow> {
    let response = db
        .node
        .execute(
            r#"{
                AgentRequest(filter: { caused_by_trigger_kind: { _eq: "goal" } }) {
                    _docID request_id session_id caused_by_parent_request_id
                    caused_by_trigger_id caused_by_trigger_kind metadata lifecycle_state
                }
            }"#,
        )
        .await;
    assert!(
        !response.has_errors(),
        "query children: {:?}",
        response.errors
    );
    serde_json::from_value(
        response
            .data
            .and_then(|data| data.get("AgentRequest").cloned())
            .unwrap_or_default(),
    )
    .expect("decode goal children")
}

#[tokio::test]
async fn completed_request_materializes_exactly_one_same_session_goal_child() {
    let db = test_db("goal-exactly-once").await;
    seed_completed_request(&db, "parent-complete").await;
    let goal = set_goal(
        db.node.as_ref(),
        AGENT_DID,
        SESSION,
        Some("Finish the durable objective"),
        Some(GoalStatus::Active),
        None,
    )
    .await
    .expect("set goal");

    let (mut source, _snapshot_tx) = source(&db);
    let intent = tokio::time::timeout(Duration::from_secs(2), source.next_fire())
        .await
        .expect("goal source timed out")
        .expect("goal continuation intent");
    assert!(intent.pre_materialized_request_id.is_some());

    let children = goal_children(&db).await;
    assert_eq!(children.len(), 1);
    let child = &children[0];
    assert_eq!(child.session_id, SESSION);
    assert_eq!(
        child.caused_by_parent_request_id.as_deref(),
        Some("parent-complete")
    );
    assert_eq!(
        child.caused_by_trigger_id.as_deref(),
        Some(goal.goal_id.as_str())
    );
    assert_eq!(child.caused_by_trigger_kind.as_deref(), Some("goal"));

    assert!(
        tokio::time::timeout(Duration::from_millis(150), source.next_fire())
            .await
            .is_err(),
        "a pending child must suppress duplicate continuation"
    );
    assert_eq!(goal_children(&db).await.len(), 1);
}

#[tokio::test]
async fn any_newer_active_request_blocks_goal_continuation_for_the_whole_session() {
    let db = test_db("goal-session-idle").await;
    seed_completed_request(&db, "older-complete").await;
    create_request(
        db.node.as_ref(),
        "newer-manual",
        SESSION,
        "pending",
        "2026-07-15T00:01:00Z",
    )
    .await;
    set_goal(
        db.node.as_ref(),
        AGENT_DID,
        SESSION,
        Some("Wait for session idleness"),
        Some(GoalStatus::Active),
        None,
    )
    .await
    .expect("set goal");

    let (mut source, _snapshot_tx) = source(&db);
    assert!(
        tokio::time::timeout(Duration::from_millis(150), source.next_fire())
            .await
            .is_err()
    );
    assert!(goal_children(&db).await.is_empty());
}

#[tokio::test]
async fn interrupted_terminal_pauses_instead_of_self_continuing() {
    let db = test_db("goal-interrupted").await;
    create_request(
        db.node.as_ref(),
        "parent-interrupted",
        SESSION,
        "interrupted",
        "2026-07-15T00:00:00Z",
    )
    .await;
    set_goal(
        db.node.as_ref(),
        AGENT_DID,
        SESSION,
        Some("Respect human interruption"),
        Some(GoalStatus::Active),
        None,
    )
    .await
    .expect("set goal");

    let (mut source, _snapshot_tx) = source(&db);
    assert!(
        tokio::time::timeout(Duration::from_millis(150), source.next_fire())
            .await
            .is_err()
    );
    let goal = load_canonical_goal(db.node.as_ref(), AGENT_DID, SESSION)
        .await
        .expect("load goal")
        .expect("goal exists");
    assert_eq!(goal.parsed_status(), Some(GoalStatus::Paused));
    assert!(goal_children(&db).await.is_empty());
}

#[tokio::test]
async fn token_budget_materializes_one_wrapup_and_never_repeats_it() {
    let db = test_db("goal-budget-wrapup").await;
    seed_completed_request(&db, "parent-budget").await;
    let usage = r#"mutation {
        add_InferenceCall(input: {
            call_id: "goal-budget-call",
            runtime_instance_id: "goal-test",
            request_id: "parent-budget",
            call_seq: 1,
            backend_id: "backend-test",
            behavior_id: "test",
            agent_did: "did:test:test",
            call_kind: "inference",
            attempt: 1,
            call_state: "completed",
            queued_at: "2026-07-15T00:00:00Z",
            started_at: "2026-07-15T00:00:00Z",
            ended_at: "2026-07-15T00:00:01Z",
            priority: 0,
            queue_depth_at_enqueue: 0,
            controller_generation: 1,
            backend_config_fingerprint: "goal-test",
            prompt_tokens: 100,
            completion_tokens: 5,
            cached_input_tokens: 90
        }) { _docID }
    }"#;
    let response = db.node.execute(usage).await;
    assert!(!response.has_errors(), "seed usage: {:?}", response.errors);
    set_goal(
        db.node.as_ref(),
        AGENT_DID,
        SESSION,
        Some("Stop at the durable budget"),
        Some(GoalStatus::Active),
        Some(Some(10)),
    )
    .await
    .expect("set goal");

    let (mut source, _snapshot_tx) = source(&db);
    tokio::time::timeout(Duration::from_secs(2), source.next_fire())
        .await
        .expect("goal source timed out")
        .expect("wrapup intent");
    let children = goal_children(&db).await;
    assert_eq!(children.len(), 1);
    let metadata: serde_json::Value =
        serde_json::from_str(children[0].metadata.as_deref().expect("goal metadata"))
            .expect("valid goal metadata");
    assert_eq!(
        metadata.pointer("/goal/wrapup"),
        Some(&serde_json::json!(true))
    );

    set_request_lifecycle_state(db.node.as_ref(), &children[0].doc_id, "completed").await;
    create_response_with_content_and_status(
        db.node.as_ref(),
        "budget-wrapup-response",
        &children[0].request_id,
        SESSION,
        "final durable wrapup",
        "complete",
    )
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), source.next_fire())
            .await
            .is_err()
    );
    let goal = load_canonical_goal(db.node.as_ref(), AGENT_DID, SESSION)
        .await
        .expect("load goal")
        .expect("goal exists");
    assert_eq!(goal.parsed_status(), Some(GoalStatus::BudgetLimited));
    assert_eq!(goal.tokens_used, Some(15));
    assert_eq!(goal.wrapup_requested, Some(true));
    assert_eq!(goal.wrapup_completed, Some(true));
    assert_eq!(goal_children(&db).await.len(), 1);
}

#[tokio::test]
async fn resume_resets_blocked_audit_identity_and_count() {
    let db = test_db("goal-resume-audit-reset").await;
    let goal = set_goal(
        db.node.as_ref(),
        AGENT_DID,
        SESSION,
        Some("Resume with a fresh blocked audit"),
        Some(GoalStatus::Active),
        None,
    )
    .await
    .expect("set goal");
    update_goal_fields(
        db.node.as_ref(),
        &goal,
        r#"status: "blocked", consecutive_blocked_audits: 3, last_blocked_request_id: "request-3", last_blocked_reason: "needs approval", active_started_at: null"#,
    )
    .await
    .expect("seed blocked audit");

    let resumed = set_goal(
        db.node.as_ref(),
        AGENT_DID,
        SESSION,
        None,
        Some(GoalStatus::Active),
        None,
    )
    .await
    .expect("resume goal");
    assert_eq!(resumed.parsed_status(), Some(GoalStatus::Active));
    assert_eq!(resumed.consecutive_blocked_audits, Some(0));
    assert_eq!(resumed.last_blocked_request_id, None);
    assert_eq!(resumed.last_blocked_reason, None);
}

#[tokio::test]
async fn provider_usage_limit_moves_active_goal_to_usage_limited() {
    let db = test_db("goal-provider-usage-limit").await;
    create_request(
        db.node.as_ref(),
        "usage-limited-request",
        SESSION,
        "error",
        "2026-07-15T00:00:00Z",
    )
    .await;
    let response = db
        .node
        .execute(
            r#"mutation {
                add_InferenceCall(input: {
                    call_id: "usage-limited-call",
                    request_id: "usage-limited-request",
                    call_seq: 1,
                    attempt: 1,
                    call_state: "failed",
                    failure_reason: "provider insufficient_quota: credit balance exhausted"
                }) { _docID }
            }"#,
        )
        .await;
    assert!(
        !response.has_errors(),
        "seed usage limit: {:?}",
        response.errors
    );
    set_goal(
        db.node.as_ref(),
        AGENT_DID,
        SESSION,
        Some("Stop on provider quota exhaustion"),
        Some(GoalStatus::Active),
        None,
    )
    .await
    .expect("set goal");

    let (mut source, _snapshot_tx) = source(&db);
    assert!(
        tokio::time::timeout(Duration::from_millis(200), source.next_fire())
            .await
            .is_err()
    );
    let goal = load_canonical_goal(db.node.as_ref(), AGENT_DID, SESSION)
        .await
        .expect("load goal")
        .expect("goal exists");
    assert_eq!(goal.parsed_status(), Some(GoalStatus::UsageLimited));
    assert!(goal
        .last_failure
        .as_deref()
        .is_some_and(|reason| reason.contains("insufficient_quota")));
}

#[tokio::test]
async fn failed_wrapup_retries_twice_then_is_durably_abandoned() {
    let db = test_db("goal-wrapup-retry-bound").await;
    seed_completed_request(&db, "parent-wrapup-retry").await;
    set_goal(
        db.node.as_ref(),
        AGENT_DID,
        SESSION,
        Some("Bound failed wrap-up retries"),
        Some(GoalStatus::Active),
        Some(Some(1)),
    )
    .await
    .expect("set goal");
    let response = db
        .node
        .execute(
            r#"mutation {
                add_InferenceCall(input: {
                    call_id: "wrapup-budget-call",
                    request_id: "parent-wrapup-retry",
                    call_seq: 1,
                    attempt: 1,
                    call_state: "completed",
                    prompt_tokens: 2,
                    completion_tokens: 0,
                    cached_input_tokens: 0
                }) { _docID }
            }"#,
        )
        .await;
    assert!(
        !response.has_errors(),
        "seed wrapup usage: {:?}",
        response.errors
    );

    let (mut source, _snapshot_tx) = source(&db);
    for expected_children in 1..=3 {
        tokio::time::timeout(Duration::from_secs(2), source.next_fire())
            .await
            .expect("goal source timed out")
            .expect("wrapup or retry intent");
        let children = goal_children(&db).await;
        assert_eq!(children.len(), expected_children);
        let child = children
            .iter()
            .find(|child| child.lifecycle_state.as_deref() == Some("pending"))
            .expect("new pending wrap-up child");
        set_request_lifecycle_state(db.node.as_ref(), &child.doc_id, "failed").await;
    }

    assert!(
        tokio::time::timeout(Duration::from_millis(250), source.next_fire())
            .await
            .is_err(),
        "bounded failed wrap-up must not spawn a fourth child"
    );
    let goal = load_canonical_goal(db.node.as_ref(), AGENT_DID, SESSION)
        .await
        .expect("load goal")
        .expect("goal exists");
    assert_eq!(goal.parsed_status(), Some(GoalStatus::BudgetLimited));
    assert_eq!(goal.infrastructure_retry_count, Some(2));
    assert_eq!(goal.wrapup_completed, Some(true));
    assert!(goal
        .last_failure
        .as_deref()
        .is_some_and(|reason| reason.contains("after 2 retries")));
    assert_eq!(goal_children(&db).await.len(), 3);
}
