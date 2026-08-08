use super::*;

#[tokio::test]
async fn dispatch_skips_when_schedule_not_in_active_schedules() {
    // Snapshot has NO active schedules — the incoming FireIntent's trigger_id
    // is therefore treated as disabled.
    let snapshot = snapshot_with_schedules(HashMap::new());
    let (_tx, rx) = watch::channel(snapshot);
    let materializer = SpyMaterializer::new();
    let engine = TriggerEngine::new(rx, materializer.clone());

    let intent = FireIntent {
        trigger_id: Some("sched-1".to_string()),
        trigger_kind: TriggerKind::Schedule,
        task: resolved_task("anything"),
        concurrency: ConcurrencyMode::Serial,
        event_vars: serde_json::json!({}),
        doc_vars: None,
        args_vars: None,
        pre_materialized_request_id: None,
        materialization_request_id: None,
        on_result: Box::new(|_| {}),
    };

    let result = engine.dispatch(intent).await;

    match result {
        FireResult::Skipped { reason } => assert_eq!(reason, "trigger disabled"),
        other => panic!("expected Skipped {{ reason: \"trigger disabled\" }}, got {other:?}"),
    }
    assert!(
        materializer.calls().is_empty(),
        "materializer should not be called when the trigger is disabled"
    );
}

#[tokio::test]
async fn dispatch_reports_pre_materialized_request_without_materializer_call() {
    let snapshot = snapshot_with_schedules(HashMap::new());
    let (_tx, rx) = watch::channel(snapshot);
    let materializer = SpyMaterializer::new();
    let engine = TriggerEngine::new(rx, materializer.clone());

    let intent = FireIntent {
        trigger_id: None,
        trigger_kind: TriggerKind::Manual,
        task: resolved_task("ignored"),
        concurrency: ConcurrencyMode::Parallel,
        event_vars: serde_json::json!({"trigger_kind": "subagent"}),
        doc_vars: None,
        args_vars: None,
        pre_materialized_request_id: Some("child-pre-materialized".to_string()),
        materialization_request_id: None,
        on_result: Box::new(|_| {}),
    };

    let result = engine.dispatch(intent).await;

    match result {
        FireResult::Fired { request_id } => {
            assert_eq!(request_id, "child-pre-materialized");
        }
        other => panic!("expected Fired for pre-materialized intent, got {other:?}"),
    }
    assert!(
        materializer.calls().is_empty(),
        "pre-materialized dispatch must not create a second AgentRequest"
    );
}

#[tokio::test]
async fn dispatch_renders_and_materializes_when_schedule_active() {
    let task = resolved_task("fired at {{ event.fired_at }}");
    let schedule = resolved_schedule("sched-1", task.clone());
    let snapshot = snapshot_with_schedules(HashMap::from([("sched-1".to_string(), schedule)]));
    let (_tx, rx) = watch::channel(snapshot);
    let materializer = SpyMaterializer::new();
    let engine = TriggerEngine::new(rx, materializer.clone());

    let intent = FireIntent {
        trigger_id: Some("sched-1".to_string()),
        trigger_kind: TriggerKind::Schedule,
        task,
        concurrency: ConcurrencyMode::Serial,
        event_vars: serde_json::json!({"fired_at": "2026-04-21T00:00:00Z"}),
        doc_vars: None,
        args_vars: None,
        pre_materialized_request_id: None,
        materialization_request_id: None,
        on_result: Box::new(|_| {}),
    };

    let result = engine.dispatch(intent).await;

    match result {
        FireResult::Fired { request_id } => assert_eq!(request_id, "req-0"),
        other => panic!("expected Fired, got {other:?}"),
    }
    let calls = materializer.calls();
    assert_eq!(calls.len(), 1, "exactly one materialize call expected");
    let (trigger_id, kind, rendered) = &calls[0];
    assert_eq!(trigger_id.as_deref(), Some("sched-1"));
    assert_eq!(*kind, TriggerKind::Schedule);
    assert_eq!(rendered, "fired at 2026-04-21T00:00:00Z");
}

#[tokio::test]
async fn dispatch_parallel_materializes_every_intent() {
    // Two fires for the same trigger with `Parallel` concurrency. Both should
    // materialize unconditionally — the in-flight check is bypassed.
    let task = resolved_task("tick");
    let schedule = resolved_schedule("sched-1", task.clone());
    let snapshot = snapshot_with_schedules(HashMap::from([("sched-1".to_string(), schedule)]));
    let (_tx, rx) = watch::channel(snapshot);
    let materializer = SpyMaterializer::new();
    let engine = TriggerEngine::new(rx, materializer.clone());

    let intent1 = FireIntent {
        trigger_id: Some("sched-1".to_string()),
        trigger_kind: TriggerKind::Schedule,
        task: task.clone(),
        concurrency: ConcurrencyMode::Parallel,
        event_vars: serde_json::json!({}),
        doc_vars: None,
        args_vars: None,
        pre_materialized_request_id: None,
        materialization_request_id: None,
        on_result: Box::new(|_| {}),
    };
    let intent2 = FireIntent {
        trigger_id: Some("sched-1".to_string()),
        trigger_kind: TriggerKind::Schedule,
        task,
        concurrency: ConcurrencyMode::Parallel,
        event_vars: serde_json::json!({}),
        doc_vars: None,
        args_vars: None,
        pre_materialized_request_id: None,
        materialization_request_id: None,
        on_result: Box::new(|_| {}),
    };

    let r1 = engine.dispatch(intent1).await;
    let r2 = engine.dispatch(intent2).await;

    assert!(
        matches!(r1, FireResult::Fired { .. }),
        "first parallel dispatch should Fire, got {r1:?}"
    );
    assert!(
        matches!(r2, FireResult::Fired { .. }),
        "second parallel dispatch should Fire, got {r2:?}"
    );
    assert_eq!(
        materializer.calls().len(),
        2,
        "both parallel fires should materialize"
    );
}

#[tokio::test]
async fn dispatch_serial_materializes_when_no_inflight() {
    // Serial mode with no in-flight request for the trigger — should fire.
    let task = resolved_task("tick");
    let schedule = resolved_schedule("sched-1", task.clone());
    let snapshot = snapshot_with_schedules(HashMap::from([("sched-1".to_string(), schedule)]));
    let (_tx, rx) = watch::channel(snapshot);
    let materializer = SpyMaterializer::new();
    let engine = TriggerEngine::new(rx, materializer.clone());

    let intent = FireIntent {
        trigger_id: Some("sched-1".to_string()),
        trigger_kind: TriggerKind::Schedule,
        task,
        concurrency: ConcurrencyMode::Serial,
        event_vars: serde_json::json!({}),
        doc_vars: None,
        args_vars: None,
        pre_materialized_request_id: None,
        materialization_request_id: None,
        on_result: Box::new(|_| {}),
    };

    let result = engine.dispatch(intent).await;

    assert!(
        matches!(result, FireResult::Fired { .. }),
        "serial dispatch with no in-flight should Fire, got {result:?}"
    );
    assert_eq!(
        materializer.calls().len(),
        1,
        "serial dispatch with no in-flight should materialize once"
    );
}

#[tokio::test]
async fn dispatch_serial_skips_when_inflight_exists() {
    // Serial mode with an in-flight request pre-populated for
    // (sched-1, Schedule). Dispatch should Skip and not materialize.
    let task = resolved_task("tick");
    let schedule = resolved_schedule("sched-1", task.clone());
    let snapshot = snapshot_with_schedules(HashMap::from([("sched-1".to_string(), schedule)]));
    let (_tx, rx) = watch::channel(snapshot);
    let materializer = SpyMaterializer::new();
    materializer.mark_nonterminal("sched-1", TriggerKind::Schedule);
    let engine = TriggerEngine::new(rx, materializer.clone());

    let intent = FireIntent {
        trigger_id: Some("sched-1".to_string()),
        trigger_kind: TriggerKind::Schedule,
        task,
        concurrency: ConcurrencyMode::Serial,
        event_vars: serde_json::json!({}),
        doc_vars: None,
        args_vars: None,
        pre_materialized_request_id: None,
        materialization_request_id: None,
        on_result: Box::new(|_| {}),
    };

    let result = engine.dispatch(intent).await;

    match result {
        FireResult::Skipped { reason } => {
            assert_eq!(reason, "serial: prior fire still in-flight");
        }
        other => panic!(
            "expected Skipped {{ reason: \"serial: prior fire still in-flight\" }}, got {other:?}"
        ),
    }
    assert!(
        materializer.calls().is_empty(),
        "serial dispatch with in-flight should not materialize"
    );
}

#[tokio::test]
async fn dispatch_latest_only_supersedes_prior_and_fires_new() {
    // LatestOnly with a pre-existing in-flight request for (sched-1, Schedule).
    // Dispatch should: (1) supersede the prior request, (2) materialize the
    // new fire, (3) return Fired.
    let task = resolved_task("tick");
    let schedule = resolved_schedule("sched-1", task.clone());
    let snapshot = snapshot_with_schedules(HashMap::from([("sched-1".to_string(), schedule)]));
    let (_tx, rx) = watch::channel(snapshot);
    let materializer = SpyMaterializer::new();
    materializer.mark_nonterminal("sched-1", TriggerKind::Schedule);
    let engine = TriggerEngine::new(rx, materializer.clone());

    let intent = FireIntent {
        trigger_id: Some("sched-1".to_string()),
        trigger_kind: TriggerKind::Schedule,
        task,
        concurrency: ConcurrencyMode::LatestOnly,
        event_vars: serde_json::json!({}),
        doc_vars: None,
        args_vars: None,
        pre_materialized_request_id: None,
        materialization_request_id: None,
        on_result: Box::new(|_| {}),
    };

    let result = engine.dispatch(intent).await;

    assert!(
        matches!(result, FireResult::Fired { .. }),
        "latest_only dispatch should Fire after superseding prior, got {result:?}"
    );
    let supersede_calls = materializer.supersede_calls();
    assert_eq!(
        supersede_calls,
        vec![("sched-1".to_string(), TriggerKind::Schedule)],
        "exactly one supersede call for (sched-1, Schedule) expected"
    );
    let calls = materializer.calls();
    assert_eq!(
        calls.len(),
        1,
        "exactly one materialize call after supersede expected"
    );
    let (trigger_id, kind, _rendered) = &calls[0];
    assert_eq!(trigger_id.as_deref(), Some("sched-1"));
    assert_eq!(*kind, TriggerKind::Schedule);
}

#[tokio::test]
async fn dispatch_latest_only_lock_blocks_second_supersede_until_first_materialize_finishes() {
    let task = resolved_task("tick");
    let schedule = resolved_schedule("sched-1", task.clone());
    let snapshot = snapshot_with_schedules(HashMap::from([("sched-1".to_string(), schedule)]));
    let (_tx, rx) = watch::channel(snapshot);
    let materializer = SpyMaterializer::new();
    let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    materializer.set_materialize_gate(entered_tx, release.clone());
    let engine = Arc::new(TriggerEngine::new(rx, materializer.clone()));

    let make_intent = || FireIntent {
        trigger_id: Some("sched-1".to_string()),
        trigger_kind: TriggerKind::Schedule,
        task: task.clone(),
        concurrency: ConcurrencyMode::LatestOnly,
        event_vars: serde_json::json!({}),
        doc_vars: None,
        args_vars: None,
        pre_materialized_request_id: None,
        materialization_request_id: None,
        on_result: Box::new(|_| {}),
    };

    let engine1 = engine.clone();
    let first_intent = make_intent();
    let first = tokio::spawn(async move { engine1.dispatch(first_intent).await });
    entered_rx
        .recv()
        .await
        .expect("first LatestOnly dispatch should enter materialize gate");
    assert_eq!(
        materializer.supersede_calls(),
        vec![("sched-1".to_string(), TriggerKind::Schedule)],
        "first LatestOnly dispatch should supersede before materializing"
    );

    let second = engine.dispatch(make_intent());
    tokio::pin!(second);
    std::future::poll_fn(|cx| match second.as_mut().poll(cx) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(result) => panic!(
            "second LatestOnly dispatch completed while the first held the per-trigger lock: {result:?}"
        ),
    })
    .await;

    assert_eq!(
        materializer.supersede_calls().len(),
        1,
        "second LatestOnly dispatch must block on the per-trigger lock before superseding"
    );
    assert!(
        entered_rx.try_recv().is_err(),
        "second LatestOnly dispatch must not enter materialize while first is gated"
    );

    release.notify_waiters();
    let first_result = first.await.unwrap();
    assert!(
        matches!(first_result, FireResult::Fired { .. }),
        "first LatestOnly dispatch should finish after release, got {first_result:?}"
    );

    std::future::poll_fn(|cx| match second.as_mut().poll(cx) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(result) => panic!(
            "second LatestOnly dispatch completed before its materialize gate was released: {result:?}"
        ),
    })
    .await;
    entered_rx
        .try_recv()
        .expect("second dispatch should enter materialize after the first releases the lock");
    release.notify_waiters();
    let second_result = second.await;
    assert!(
        matches!(second_result, FireResult::Fired { .. }),
        "second LatestOnly dispatch should fire after the first releases, got {second_result:?}"
    );
    assert_eq!(
        materializer.supersede_calls(),
        vec![
            ("sched-1".to_string(), TriggerKind::Schedule),
            ("sched-1".to_string(), TriggerKind::Schedule),
        ],
        "the second supersede must occur only after the first materialize completes"
    );
    assert_eq!(
        materializer.calls().len(),
        2,
        "both LatestOnly dispatches should materialize after serialized critical sections"
    );
}

#[tokio::test]
async fn dispatch_errors_and_skips_materialize_on_template_render_failure() {
    // Template references `event.missing_field`, but the intent's event_vars
    // has no such key. With strict-undefined semantics, rendering must fail,
    // and dispatch must return Errored (with a "template:" prefix), skip the
    // materializer entirely, and invoke `on_result` with the same Errored
    // value so the upstream source can write back `last_status = "error"`.
    let task = resolved_task("{{ event.missing_field }}");
    let schedule = resolved_schedule("sched-1", task.clone());
    let snapshot = snapshot_with_schedules(HashMap::from([("sched-1".to_string(), schedule)]));
    let (_tx, rx) = watch::channel(snapshot);
    let materializer = SpyMaterializer::new();
    let engine = TriggerEngine::new(rx, materializer.clone());

    let result_captured: Arc<Mutex<Option<FireResult>>> = Arc::new(Mutex::new(None));
    let capture = result_captured.clone();

    let intent = FireIntent {
        trigger_id: Some("sched-1".to_string()),
        trigger_kind: TriggerKind::Schedule,
        task,
        concurrency: ConcurrencyMode::Serial,
        event_vars: serde_json::json!({}),
        doc_vars: None,
        args_vars: None,
        pre_materialized_request_id: None,
        materialization_request_id: None,
        on_result: Box::new(move |r| {
            *capture.lock().unwrap() = Some(r);
        }),
    };

    let result = engine.dispatch(intent).await;

    match result.clone() {
        FireResult::Errored { error } => assert!(
            error.starts_with("template:"),
            "expected template-render error, got: {error}"
        ),
        other => panic!("expected Errored, got {other:?}"),
    }

    assert!(
        materializer.calls().is_empty(),
        "no materialize call should have been made on render failure"
    );
    assert!(
        materializer.supersede_calls().is_empty(),
        "no supersede call should have been made on render failure"
    );

    let captured = result_captured.lock().unwrap().clone();
    match captured {
        Some(FireResult::Errored { error }) => assert!(
            error.starts_with("template:"),
            "expected callback Errored with template prefix, got: {error}"
        ),
        other => panic!("expected callback Errored, got {other:?}"),
    }
}

#[tokio::test]
async fn dispatch_latest_only_serializes_parallel_fires() {
    // Two LatestOnly dispatches for the same trigger fired in parallel. With
    // a materialize delay of ~60ms, the per-trigger lock must serialize them:
    // the second dispatch cannot enter its supersede+materialize critical
    // section until the first completes, so total wall-clock elapsed is at
    // least 2 * delay.
    let task = resolved_task("tick");
    let schedule = resolved_schedule("sched-1", task.clone());
    let snapshot = snapshot_with_schedules(HashMap::from([("sched-1".to_string(), schedule)]));
    let (_tx, rx) = watch::channel(snapshot);
    let materializer = SpyMaterializer::new();
    let delay = Duration::from_millis(60);
    materializer.set_materialize_delay(delay);
    let engine = Arc::new(TriggerEngine::new(rx, materializer.clone()));

    let make_intent = || FireIntent {
        trigger_id: Some("sched-1".to_string()),
        trigger_kind: TriggerKind::Schedule,
        task: task.clone(),
        concurrency: ConcurrencyMode::LatestOnly,
        event_vars: serde_json::json!({}),
        doc_vars: None,
        args_vars: None,
        pre_materialized_request_id: None,
        materialization_request_id: None,
        on_result: Box::new(|_| {}),
    };

    let start = Instant::now();
    let engine1 = engine.clone();
    let engine2 = engine.clone();
    let intent1 = make_intent();
    let intent2 = make_intent();
    let h1 = tokio::spawn(async move { engine1.dispatch(intent1).await });
    let h2 = tokio::spawn(async move { engine2.dispatch(intent2).await });
    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();
    let elapsed = start.elapsed();

    assert!(
        matches!(r1, FireResult::Fired { .. }),
        "first parallel LatestOnly dispatch should Fire, got {r1:?}"
    );
    assert!(
        matches!(r2, FireResult::Fired { .. }),
        "second parallel LatestOnly dispatch should Fire, got {r2:?}"
    );
    assert_eq!(
        materializer.calls().len(),
        2,
        "both LatestOnly fires should materialize"
    );
    assert_eq!(
        materializer.supersede_calls().len(),
        2,
        "each LatestOnly fire runs a supersede call inside its critical section"
    );
    // If the two fires had run concurrently, total elapsed would be ~= delay.
    // With per-trigger serialization, elapsed must be >= 2 * delay. Allow a
    // small slack below 2x to tolerate sleep-granularity jitter on loaded CI.
    let min_expected = delay * 2 - Duration::from_millis(10);
    assert!(
        elapsed >= min_expected,
        "expected elapsed >= {min_expected:?} (2x delay, minus slack) proving \
         per-trigger serialization, got {elapsed:?}"
    );
}

#[tokio::test]
async fn dispatch_scopes_concurrency_by_the_behaviors_agent_did() {
    // #605: the trigger tuple is only unique per agent. Both the serial gate
    // and LatestOnly supersede must receive the DID of the fire's behavior so
    // replicated foreign requests for the same trigger id can never gate (or
    // be superseded by) this agent's fires.
    let task = resolved_task("tick");
    let schedule = resolved_schedule("sched-did", task.clone());
    let snapshot = snapshot_with_schedules(HashMap::from([("sched-did".to_string(), schedule)]));
    let expected_did = snapshot
        .behavior("general")
        .expect("test snapshot resolves the general behavior")
        .agent_did()
        .to_string();
    assert!(!expected_did.is_empty());
    let (_tx, rx) = watch::channel(snapshot);
    let materializer = SpyMaterializer::new();
    let engine = TriggerEngine::new(rx, materializer.clone());

    let serial = FireIntent {
        trigger_id: Some("sched-did".to_string()),
        trigger_kind: TriggerKind::Schedule,
        task: task.clone(),
        concurrency: ConcurrencyMode::Serial,
        event_vars: serde_json::json!({}),
        doc_vars: None,
        args_vars: None,
        pre_materialized_request_id: None,
        materialization_request_id: None,
        on_result: Box::new(|_| {}),
    };
    assert!(matches!(
        engine.dispatch(serial).await,
        FireResult::Fired { .. }
    ));
    assert_eq!(materializer.gate_dids(), vec![expected_did.clone()]);

    let latest_only = FireIntent {
        trigger_id: Some("sched-did".to_string()),
        trigger_kind: TriggerKind::Schedule,
        task,
        concurrency: ConcurrencyMode::LatestOnly,
        event_vars: serde_json::json!({}),
        doc_vars: None,
        args_vars: None,
        pre_materialized_request_id: None,
        materialization_request_id: None,
        on_result: Box::new(|_| {}),
    };
    assert!(matches!(
        engine.dispatch(latest_only).await,
        FireResult::Fired { .. }
    ));
    assert_eq!(materializer.supersede_dids(), vec![expected_did]);
}
