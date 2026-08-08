use super::*;

fn resolved_task_for_test(task_id: &str, behavior_id: &str, prompt_template: &str) -> ResolvedTask {
    ResolvedTask {
        task_id: task_id.to_string(),
        name: None,
        behavior_id: behavior_id.to_string(),
        prompt_template: prompt_template.to_string(),
        output_schema_ref: None,
    }
}

/// Build an `ActiveRuntimeSnapshot` with a single active task and no other
/// live state. Mirrors `snapshot_with_schedules`. Used by the manual-fire
/// tests that need `snapshot.active_tasks()` to resolve the intent's task.
fn snapshot_with_active_task(task: ResolvedTask) -> Arc<ActiveRuntimeSnapshot> {
    let mut tasks = HashMap::new();
    tasks.insert(task.task_id.clone(), task);
    let resolved = ResolvedRuntimeSnapshot::from_parts_with_admission_configs(
        "general".to_string(),
        vec![integration_test_behavior("general")],
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
    )
    .with_tasks(tasks)
    .with_principal(stub_principal());
    Arc::new(resolved.activate(1, HashMap::new()))
}

#[tokio::test]
async fn manual_source_run_task_now_yields_intent_with_args_vars() {
    let snapshot = snapshot_with_active_task(resolved_task_for_test(
        "greet-user",
        "behavior-1",
        "hello {{ args.name }}",
    ));
    let cancel = CancellationToken::new();
    let (mut source, handle) = ManualSource::new(cancel.clone());

    let pull = tokio::spawn(async move { source.next_fire().await });

    let _result_rx = handle
        .run_task_now(
            snapshot.as_ref(),
            "greet-user",
            serde_json::json!({"name": "Amy"}),
        )
        .await
        .unwrap();

    let intent = pull.await.unwrap().expect("next_fire returned None");
    assert_eq!(intent.trigger_kind, TriggerKind::Manual);
    assert_eq!(intent.trigger_id, None);
    assert_eq!(intent.concurrency, ConcurrencyMode::Parallel);
    assert_eq!(
        intent.args_vars.as_ref().and_then(|v| v["name"].as_str()),
        Some("Amy"),
    );
    assert_eq!(intent.task.task_id, "greet-user");
    assert_eq!(intent.event_vars["trigger_kind"].as_str(), Some("manual"));
    assert!(intent.doc_vars.is_none());
}

#[tokio::test]
async fn manual_source_run_task_now_rejects_unknown_task() {
    let snapshot =
        snapshot_with_active_task(resolved_task_for_test("other-task", "behavior-1", "x"));
    let (_source, handle) = ManualSource::new(CancellationToken::new());
    let err = handle
        .run_task_now(snapshot.as_ref(), "missing", serde_json::json!({}))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("not in the active snapshot"),
        "expected 'not in the active snapshot' in error, got: {err}"
    );
}

#[tokio::test]
async fn manual_source_next_fire_returns_none_after_cancel() {
    let cancel = CancellationToken::new();
    let (mut source, _handle) = ManualSource::new(cancel.clone());

    // Cancel immediately.
    cancel.cancel();

    let result = tokio::time::timeout(std::time::Duration::from_millis(200), source.next_fire())
        .await
        .expect("timed out waiting for cancelled next_fire");
    assert!(result.is_none());
}

/// Task 5 pinning: `ProductionMaterializer::materialize` must accept a
/// `TriggerKind::Manual` intent and persist an `AgentRequest` whose lineage
/// tuple is `(caused_by_trigger_id = null, caused_by_trigger_kind =
/// "manual")` with `execution_origin = "interactive"` (operator-initiated).
///
/// This protects two spec invariants at the materialization boundary:
///   * `TriggerKind::as_str()` is the authoritative source for the persisted
///     `caused_by_trigger_kind` field — no hard-coded "schedule"/"event".
///   * Manual fires map to `ExecutionOrigin::Interactive`, not `Scheduled`;
///     schedule and event fires keep `Scheduled`.
#[tokio::test]
async fn production_materializer_accepts_manual_lineage_end_to_end() {
    let node = signed_test_node("manual-materializer-node").await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();

    // Snapshot: behavior "general" loaded (with backend_id), no active
    // schedules (Manual doesn't consult them).
    let behavior = integration_test_behavior("general");
    let snapshot = snapshot_with_behavior_and_schedules(behavior, HashMap::new());
    let (_tx, rx) = watch::channel(snapshot);

    let materializer = ProductionMaterializer::new(node.clone(), rx);
    let task = resolved_task_for_test("task-manual", "general", "manual body");

    let request_id = materializer
        .materialize(&task, None, TriggerKind::Manual, "manual body")
        .await
        .expect("Manual materialize should succeed");

    let escaped_request_id = escape_graphql_string(&request_id);
    let query = format!(
        r#"query {{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                limit: 1
            ) {{
                caused_by_trigger_id
                caused_by_trigger_kind
                execution_origin
                status
                lifecycle_state
                content
            }}
        }}"#
    );
    let resp = node.execute(&query).await;
    assert!(
        !resp.has_errors(),
        "AgentRequest read-back errored: {:?}",
        resp.errors
    );
    let row = resp
        .data
        .as_ref()
        .and_then(|d| d.get("AgentRequest"))
        .and_then(|arr| arr.as_array())
        .and_then(|arr| arr.first())
        .expect("expected one AgentRequest row for the materialized Manual fire");
    assert!(
        row.get("caused_by_trigger_id")
            .and_then(|v| v.as_str())
            .is_none(),
        "Manual fires carry no trigger id; expected null caused_by_trigger_id: {row}"
    );
    assert_eq!(
        row.get("caused_by_trigger_kind").and_then(|v| v.as_str()),
        Some("manual"),
        "Manual lineage must serialize via TriggerKind::as_str() = \"manual\": {row}"
    );
    assert_eq!(
        row.get("execution_origin").and_then(|v| v.as_str()),
        Some("interactive"),
        "Manual fires map to ExecutionOrigin::Interactive per spec: {row}"
    );
    assert_eq!(
        row.get("status").and_then(|v| v.as_str()),
        Some("pending"),
        "Production materializer should enqueue Manual fires for normal intake: {row}"
    );
    assert_eq!(
        row.get("lifecycle_state").and_then(|v| v.as_str()),
        Some("pending"),
        "Production materializer should leave Manual fires pending until daemon claim: {row}"
    );
    assert_eq!(
        row.get("content").and_then(|v| v.as_str()),
        Some("manual body"),
        "rendered prompt should land verbatim in AgentRequest.content: {row}"
    );
}

#[tokio::test]
async fn production_materializer_rejects_manual_lineage_with_trigger_id() {
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
    let snapshot = snapshot_with_schedules(HashMap::new());
    let (_tx, rx) = watch::channel(snapshot);
    let materializer = ProductionMaterializer::new(node, rx);
    let task = resolved_task_for_test("task-manual", "general", "manual body");

    let err = materializer
        .materialize(
            &task,
            Some("manual-must-not-have-id"),
            TriggerKind::Manual,
            "manual body",
        )
        .await
        .expect_err("Manual materialize with trigger_id must fail before persistence");

    assert!(
        err.to_string().contains("must not carry trigger_id"),
        "unexpected manual lineage validation error: {err}"
    );
}

/// Task 6 pinning: `TriggerEngine::dispatch` must pass `TriggerKind::Manual`
/// intents through without consulting `active_schedules()` /
/// `active_event_triggers()` (no enabled-gate rejection for operator
/// fires), render the prompt template against `args_vars`, and invoke the
/// materializer exactly once with `(trigger_id = None, trigger_kind =
/// Manual, rendered = "hello Amy")`.
#[tokio::test]
async fn dispatch_manual_intent_renders_with_args_and_materializes() {
    // Snapshot carries the active task but NO active schedules / event
    // triggers. A Schedule/Event intent would be gated off here; Manual
    // must not be.
    let task = resolved_task_for_test("greet-user", "general", "hello {{ args.name }}");
    let snapshot = snapshot_with_active_task(task.clone());
    let (_tx, rx) = watch::channel(snapshot);
    let materializer = SpyMaterializer::new();
    let engine = TriggerEngine::new(rx, materializer.clone());

    let intent = FireIntent {
        trigger_id: None,
        trigger_kind: TriggerKind::Manual,
        task,
        concurrency: ConcurrencyMode::Parallel,
        event_vars: serde_json::json!({}),
        doc_vars: None,
        args_vars: Some(serde_json::json!({"name": "Amy"})),
        pre_materialized_request_id: None,
        materialization_request_id: None,
        on_result: Box::new(|_| {}),
    };

    let result = engine.dispatch(intent).await;

    match result {
        FireResult::Fired { request_id } => assert_eq!(
            request_id, "req-0",
            "spy materializer hands back sequentially-numbered ids starting at req-0"
        ),
        other => panic!("expected Fired for Manual intent (bypasses enabled-gate), got {other:?}"),
    }

    let calls = materializer.calls();
    assert_eq!(
        calls.len(),
        1,
        "exactly one materialize call expected for Manual dispatch"
    );
    let (trigger_id, kind, rendered) = &calls[0];
    assert!(
        trigger_id.is_none(),
        "Manual intents carry trigger_id = None; got {trigger_id:?}"
    );
    assert_eq!(*kind, TriggerKind::Manual);
    assert_eq!(
        rendered, "hello Amy",
        "dispatch must render the `args.name` template against args_vars"
    );
}

#[tokio::test]
async fn dispatch_rejects_manual_intent_with_trigger_id() {
    let task = resolved_task_for_test("greet-user", "general", "hello");
    let snapshot = snapshot_with_active_task(task.clone());
    let (_tx, rx) = watch::channel(snapshot);
    let materializer = SpyMaterializer::new();
    let engine = TriggerEngine::new(rx, materializer.clone());

    let result_captured: Arc<Mutex<Option<FireResult>>> = Arc::new(Mutex::new(None));
    let capture = result_captured.clone();
    let intent = FireIntent {
        trigger_id: Some("manual-must-not-have-id".to_string()),
        trigger_kind: TriggerKind::Manual,
        task,
        concurrency: ConcurrencyMode::Parallel,
        event_vars: serde_json::json!({}),
        doc_vars: None,
        args_vars: Some(serde_json::json!({})),
        pre_materialized_request_id: None,
        materialization_request_id: None,
        on_result: Box::new(move |r| {
            *capture.lock().unwrap() = Some(r);
        }),
    };

    let result = engine.dispatch(intent).await;

    match result {
        FireResult::Errored { error } => assert!(
            error.contains("must not carry trigger_id"),
            "unexpected manual well-formedness error: {error}"
        ),
        other => panic!("expected Errored for malformed Manual intent, got {other:?}"),
    }
    assert!(
        materializer.calls().is_empty(),
        "malformed Manual intent must not reach the materializer"
    );
    assert!(
        matches!(
            result_captured.lock().unwrap().as_ref(),
            Some(FireResult::Errored { error }) if error.contains("must not carry trigger_id")
        ),
        "on_result should receive the same malformed Manual error"
    );
}
