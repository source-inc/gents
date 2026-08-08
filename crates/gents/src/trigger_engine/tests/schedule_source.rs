use chrono::Timelike;

use super::*;

/// Create a `Schedule` document with an explicit `next_run_at`. Used by
/// `ScheduleSource::next_fire` tests to seed a due (or not-yet-due) schedule
/// without going through the full reconcile/apply pipeline.
async fn create_schedule_with_next_run_at(
    node: &defra_node::EmbeddedNode,
    schedule_id: &str,
    task_id: &str,
    next_run_at: &str,
    concurrency: &str,
) {
    let escaped_schedule_id = escape_graphql_string(schedule_id);
    let escaped_task_id = escape_graphql_string(task_id);
    let escaped_next_run_at = escape_graphql_string(next_run_at);
    let escaped_concurrency = escape_graphql_string(concurrency);
    let mutation = format!(
        r#"mutation {{
            create_Schedule(input: {{
                schedule_id: "{escaped_schedule_id}",
                task_id: "{escaped_task_id}",
                interval_secs: 60,
                enabled: true,
                concurrency: "{escaped_concurrency}",
                next_run_at: "{escaped_next_run_at}"
            }}) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create Schedule failed: {:?}",
        response.errors
    );
}

#[tokio::test]
async fn schedule_source_next_fire_emits_intent_when_schedule_is_due() {
    // Seed a Schedule document with `next_run_at` 1s in the past, build a
    // snapshot that marks the same schedule active, and assert that
    // `ScheduleSource::next_fire` yields a matching `FireIntent` within 2
    // seconds. Also exercises the event_vars shape (fired_at, trigger_id,
    // trigger_kind) the downstream materializer will see.
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
    ensure_runtime_schemas(node.as_ref()).await.unwrap();

    let past = (Utc::now() - ChronoDuration::seconds(1)).to_rfc3339();
    create_schedule_with_next_run_at(node.as_ref(), "sched-1", "task-1", &past, "serial").await;

    let task = ResolvedTask {
        task_id: "task-1".to_string(),
        name: None,
        behavior_id: "general".to_string(),
        prompt_template: "hi".to_string(),
        output_schema_ref: None,
    };
    let snapshot = snapshot_with_schedules(HashMap::from([(
        "sched-1".to_string(),
        resolved_schedule("sched-1", task),
    )]));
    let (_tx, rx) = watch::channel(snapshot);
    let cancel = CancellationToken::new();
    let mut source = ScheduleSource::new(rx, node.clone(), cancel.clone())
        .with_tick_every(Duration::from_millis(50));

    let intent = tokio::time::timeout(Duration::from_secs(2), source.next_fire())
        .await
        .expect("next_fire timed out")
        .expect("next_fire returned None");

    assert_eq!(intent.trigger_id.as_deref(), Some("sched-1"));
    assert_eq!(intent.trigger_kind, TriggerKind::Schedule);
    assert_eq!(intent.concurrency, ConcurrencyMode::Serial);
    assert_eq!(intent.task.task_id, "task-1");
    assert!(intent.doc_vars.is_none());
    assert!(intent.args_vars.is_none());

    let ev = &intent.event_vars;
    assert_eq!(ev["trigger_id"].as_str(), Some("sched-1"));
    assert_eq!(ev["trigger_kind"].as_str(), Some("schedule"));
    assert!(
        ev["fired_at"].is_string(),
        "fired_at should be a string, got {:?}",
        ev["fired_at"]
    );
}

/// After a successful fire, the callback advances `next_run_at += interval`,
/// writes `last_attempt_at`, sets `last_status = "fired"`, and bumps
/// `fire_count` by 1. After a skipped fire on the same schedule (with a fresh
/// intent generated from the already-advanced next_run_at), `last_status` must
/// flip to `"skipped"`, `next_run_at` still advances, and `fire_count` stays
/// put. Apply-owned fields (`interval_secs`, `enabled`, `task_id`,
/// `concurrency`) must be untouched across both writes.
#[tokio::test]
async fn schedule_source_on_result_writes_runtime_fields_on_fired_and_skipped() {
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
    ensure_runtime_schemas(node.as_ref()).await.unwrap();

    // Seed a Schedule that is already due (next_run_at 1s in the past) so
    // next_fire() will immediately yield an intent.
    let initial_next_run_at = (Utc::now() - ChronoDuration::seconds(1)).to_rfc3339();
    create_schedule_with_next_run_at(
        node.as_ref(),
        "sched-1",
        "task-1",
        &initial_next_run_at,
        "serial",
    )
    .await;

    let task = ResolvedTask {
        task_id: "task-1".to_string(),
        name: None,
        behavior_id: "general".to_string(),
        prompt_template: "hi".to_string(),
        output_schema_ref: None,
    };
    let schedule = resolved_schedule("sched-1", task);
    let interval_secs = match schedule.cadence {
        ScheduleCadence::Interval { interval_secs } => interval_secs,
        ScheduleCadence::Cron { .. } => panic!("test helper should build an interval schedule"),
    };
    let snapshot = snapshot_with_schedules(HashMap::from([("sched-1".to_string(), schedule)]));
    let (_tx, rx) = watch::channel(snapshot);
    let cancel = CancellationToken::new();
    let mut source = ScheduleSource::new(rx, node.clone(), cancel.clone())
        .with_tick_every(Duration::from_millis(50));

    // ---- Fired case ----
    let intent = tokio::time::timeout(Duration::from_secs(2), source.next_fire())
        .await
        .expect("next_fire timed out (fired)")
        .expect("next_fire returned None (fired)");
    // Dispatch a synthetic Fired result into the callback. The callback spawns
    // a background write, so poll the DB until it lands (bounded retry).
    (intent.on_result)(FireResult::Fired {
        request_id: "req-0".to_string(),
    });
    let expected_next_run_at_fired = (DateTime::parse_from_rfc3339(&initial_next_run_at)
        .unwrap()
        .with_timezone(&Utc)
        + ChronoDuration::seconds(interval_secs))
    .to_rfc3339();
    let mut fired_schedule = None;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let records = list_schedule_records(node.as_ref()).await.unwrap();
        let (_doc_id, sched) = records
            .iter()
            .find(|(_d, s)| s.schedule_id == "sched-1")
            .cloned()
            .expect("Schedule doc disappeared");
        if sched.last_status.as_deref() == Some("fired") {
            fired_schedule = Some(sched);
            break;
        }
    }
    let fired = fired_schedule.expect("Schedule.last_status never became \"fired\"");
    assert_eq!(fired.last_status.as_deref(), Some("fired"));
    assert_eq!(fired.fire_count, Some(1));
    // Compare as parsed DateTimes truncated to second precision rather
    // than raw RFC3339 strings. Chrono's default `to_rfc3339()` emits
    // microsecond precision with a `+00:00` offset; DefraDB persists and
    // the runtime writeback normalizes to `Z` with second precision so
    // the DateTime scalar round-trips cleanly. The parse+truncate dance
    // makes the assertion robust to both axes of textual drift while
    // still proving the instant advanced by exactly one interval.
    let fired_next = fired
        .next_run_at
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc).timestamp());
    let expected_next_fired = DateTime::parse_from_rfc3339(&expected_next_run_at_fired)
        .unwrap()
        .with_timezone(&Utc)
        .timestamp();
    assert_eq!(fired_next, Some(expected_next_fired));
    assert!(
        fired.last_attempt_at.is_some(),
        "last_attempt_at should be set after a fire"
    );
    // Apply-owned fields must not be clobbered by the runtime writeback.
    assert_eq!(fired.interval_secs, Some(60));
    assert!(fired.enabled);
    assert_eq!(fired.task_id.as_deref(), Some("task-1"));
    assert_eq!(fired.concurrency.as_deref(), Some("serial"));

    // ---- Skipped case ----
    // Rewind next_run_at into the past again so the source will yield another
    // intent on the next tick. The new intent's on_result snapshot should
    // advance relative to the *new* next_run_at we just persisted.
    //
    // Use `Z`/second-precision form so the written value matches what the
    // runtime writeback produced. DefraDB's update path re-validates every
    // existing DateTime field against the schema on every partial update,
    // and rejects the whole mutation when any existing DateTime differs
    // from its canonical form (see `schedule_conformance.rs` for the same
    // quirk). We restate `last_attempt_at` using its post-writeback value
    // so this rewind mutation passes that revalidation.
    let rewound_next_run_at =
        (Utc::now() - ChronoDuration::seconds(1)).to_rfc3339_opts(SecondsFormat::Secs, true);
    let escaped_schedule_id = escape_graphql_string("sched-1");
    let escaped_rewound = escape_graphql_string(&rewound_next_run_at);
    let preserved_last_attempt = fired
        .last_attempt_at
        .as_deref()
        .expect("last_attempt_at must be set after the fired writeback")
        .to_string();
    let escaped_preserved_last_attempt = escape_graphql_string(&preserved_last_attempt);
    let mutation = format!(
        r#"mutation {{
            update_Schedule(
                filter: {{ schedule_id: {{ _eq: "{escaped_schedule_id}" }} }},
                input: {{
                    next_run_at: "{escaped_rewound}",
                    last_attempt_at: "{escaped_preserved_last_attempt}"
                }}
            ) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "rewind mutation failed: {:?}",
        resp.errors
    );

    let intent2 = tokio::time::timeout(Duration::from_secs(2), source.next_fire())
        .await
        .expect("next_fire timed out (skipped)")
        .expect("next_fire returned None (skipped)");
    (intent2.on_result)(FireResult::Skipped {
        reason: "serial: prior fire still in-flight".to_string(),
    });
    let expected_next_run_at_skipped = (DateTime::parse_from_rfc3339(&rewound_next_run_at)
        .unwrap()
        .with_timezone(&Utc)
        + ChronoDuration::seconds(interval_secs))
    .to_rfc3339();
    let mut skipped_schedule = None;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let records = list_schedule_records(node.as_ref()).await.unwrap();
        let (_doc_id, sched) = records
            .iter()
            .find(|(_d, s)| s.schedule_id == "sched-1")
            .cloned()
            .expect("Schedule doc disappeared");
        if sched.last_status.as_deref() == Some("skipped") {
            skipped_schedule = Some(sched);
            break;
        }
    }
    let skipped = skipped_schedule.expect("Schedule.last_status never became \"skipped\"");
    assert_eq!(skipped.last_status.as_deref(), Some("skipped"));
    // fire_count MUST NOT advance on skip.
    assert_eq!(skipped.fire_count, Some(1));
    // See the fired-case comment above: parse+truncate both sides so
    // offset-suffix (`Z` vs `+00:00`) and subsecond-precision drift don't
    // flake the test.
    let skipped_next = skipped
        .next_run_at
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc).timestamp());
    let expected_next_skipped = DateTime::parse_from_rfc3339(&expected_next_run_at_skipped)
        .unwrap()
        .with_timezone(&Utc)
        .timestamp();
    assert_eq!(skipped_next, Some(expected_next_skipped));
    // Apply-owned fields still intact.
    assert_eq!(skipped.interval_secs, Some(60));
    assert!(skipped.enabled);
    assert_eq!(skipped.task_id.as_deref(), Some("task-1"));
    assert_eq!(skipped.concurrency.as_deref(), Some("serial"));
}

/// Cancelling the `CancellationToken` before polling `next_fire` must short-
/// circuit the tick-sleep and return `None` promptly — much faster than the
/// configured `tick_every`. This is the graceful-shutdown path the engine
/// relies on: on cancel the source is expected to drain back to `None` so the
/// outer loop can tear it down.
#[tokio::test]
async fn schedule_source_next_fire_honors_cancellation_token() {
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
    ensure_runtime_schemas(node.as_ref()).await.unwrap();

    // No schedules are needed: cancellation must be observed before the tick
    // body ever runs the snapshot scan.
    let snapshot = snapshot_with_schedules(HashMap::new());
    let (_tx, rx) = watch::channel(snapshot);
    let cancel = CancellationToken::new();
    // Deliberately use a long tick so any wall-clock elapsed below 1s is
    // strong evidence the select arm fired on cancel, not on sleep expiry.
    let mut source = ScheduleSource::new(rx, node.clone(), cancel.clone())
        .with_tick_every(Duration::from_secs(30));

    // Cancel before calling next_fire so the select!'s cancel arm is
    // immediately ready.
    cancel.cancel();

    let start = Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(2), source.next_fire())
        .await
        .expect("next_fire did not return within 2s after cancel");
    let elapsed = start.elapsed();

    assert!(
        result.is_none(),
        "expected None after cancel, got Some(intent) with trigger_id={:?}",
        result.as_ref().and_then(|i| i.trigger_id.as_deref())
    );
    assert!(
        elapsed < Duration::from_secs(1),
        "next_fire should return promptly on cancel, took {elapsed:?}"
    );
}

/// Task 39 Step 1: end-to-end assertion that a due Schedule in the active
/// snapshot drives the `TriggerEngine` + `ScheduleSource` +
/// `ProductionMaterializer` pipeline to enqueue an `AgentRequest` carrying
/// `caused_by_trigger_id = <schedule_id>` and `caused_by_trigger_kind =
/// "schedule"` within a bounded wait.
///
/// Runs against a real `EmbeddedNode` because the ProductionMaterializer
/// writes via DefraDB — there is no in-memory shortcut. The test does not
/// assert execution (no inference is wired here); it only asserts the
/// enqueue boundary that Task 39 is restoring under the engine.
#[tokio::test]
async fn trigger_engine_enqueues_agent_request_for_due_schedule_e2e() {
    let node = signed_test_node("schedule-materializer-node").await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();

    // Seed a Schedule whose next_run_at is 1s in the past — the ScheduleSource
    // will emit an intent on its next tick.
    let past = (Utc::now() - ChronoDuration::seconds(1)).to_rfc3339();
    create_schedule_with_next_run_at(node.as_ref(), "sched-e2e", "task-e2e", &past, "serial").await;

    // Build the snapshot: one behavior loaded ("general"), one active
    // schedule pointing at a task bound to that behavior.
    let behavior = integration_test_behavior("general");
    let task = ResolvedTask {
        task_id: "task-e2e".to_string(),
        name: Some("Mini Host Health".to_string()),
        behavior_id: behavior.behavior_id.clone(),
        prompt_template: "integration fire".to_string(),
        output_schema_ref: None,
    };
    let schedule = ResolvedSchedule {
        schedule_id: "sched-e2e".to_string(),
        task_id: task.task_id.clone(),
        task,
        cadence: ScheduleCadence::Interval { interval_secs: 60 },
        enabled: true,
        concurrency: ConcurrencyMode::Serial,
    };
    let snapshot = snapshot_with_behavior_and_schedules(
        behavior,
        HashMap::from([("sched-e2e".to_string(), schedule)]),
    );
    let (_tx, rx) = watch::channel(snapshot);

    // Wire engine + source + materializer with the same watch::Receiver.
    let cancel = CancellationToken::new();
    let materializer: Arc<dyn MaterializerHandle> =
        Arc::new(ProductionMaterializer::new(node.clone(), rx.clone()));
    let source: Box<dyn TriggerSource> = Box::new(
        ScheduleSource::new(rx.clone(), node.clone(), cancel.clone())
            .with_tick_every(Duration::from_millis(50)),
    );
    let engine = TriggerEngine::new(rx, materializer);
    let engine_cancel = cancel.clone();
    let engine_handle = tokio::spawn(async move {
        engine.run(vec![source], engine_cancel).await;
    });

    // Poll the DB for an AgentRequest with the lineage tuple. Bounded retry;
    // 50ms * 80 = 4s total, well within the "within N seconds" ask.
    let mut observed = None;
    for _ in 0..80 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let query = r#"query {
            AgentRequest(filter: {
                caused_by_trigger_id: { _eq: "sched-e2e" },
                caused_by_trigger_kind: { _eq: "schedule" }
            }) {
                _docID
                caused_by_trigger_id
                caused_by_trigger_kind
                lifecycle_state
                execution_origin
                session_id
                content
            }
        }"#;
        let resp = node.execute(query).await;
        assert!(
            !resp.has_errors(),
            "AgentRequest query errored: {:?}",
            resp.errors
        );
        let rows = resp
            .data
            .as_ref()
            .and_then(|d| d.get("AgentRequest"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        if !rows.is_empty() {
            observed = rows.into_iter().next();
            break;
        }
    }

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(2), engine_handle).await;

    let row = observed.expect(
        "no AgentRequest with caused_by_trigger_id=sched-e2e observed within 4s; \
         expected the TriggerEngine + ScheduleSource pipeline to have materialized one",
    );
    assert_eq!(
        row.get("caused_by_trigger_id").and_then(|v| v.as_str()),
        Some("sched-e2e"),
        "persisted request is missing caused_by_trigger_id lineage: {row}"
    );
    assert_eq!(
        row.get("caused_by_trigger_kind").and_then(|v| v.as_str()),
        Some("schedule"),
        "persisted request is missing caused_by_trigger_kind lineage: {row}"
    );
    assert_eq!(
        row.get("execution_origin").and_then(|v| v.as_str()),
        Some("scheduled"),
        "trigger-driven fire should set execution_origin=scheduled: {row}"
    );
    assert_eq!(
        row.get("lifecycle_state").and_then(|v| v.as_str()),
        Some("pending"),
        "ProductionMaterializer should enqueue pending requests for watcher/router execution: {row}"
    );
    assert_eq!(
        row.get("content").and_then(|v| v.as_str()),
        Some("integration fire"),
        "rendered prompt template should land in AgentRequest.content: {row}"
    );

    let session_id = row
        .get("session_id")
        .and_then(|v| v.as_str())
        .expect("materialized request should have session_id");
    let conversation_query = format!(
        r#"{{
            AgentConversation(
                filter: {{ session_id: {{ _eq: "{}" }} }},
                limit: 1
            ) {{
                title
                title_source
            }}
        }}"#,
        escape_graphql_string(session_id)
    );
    let conversation_resp = node.execute(&conversation_query).await;
    assert!(
        !conversation_resp.has_errors(),
        "AgentConversation query errored: {:?}",
        conversation_resp.errors
    );
    let conversation = conversation_resp
        .data
        .as_ref()
        .and_then(|d| d.get("AgentConversation"))
        .and_then(|v| v.as_array())
        .and_then(|rows| rows.first())
        .expect("task materialization should seed AgentConversation title");
    assert_eq!(
        conversation.get("title_source").and_then(|v| v.as_str()),
        Some("task")
    );
    assert!(
        conversation
            .get("title")
            .and_then(|v| v.as_str())
            .is_some_and(|title| title.starts_with("mini-host-health-20")),
        "task conversation title should use task name plus timestamp: {conversation}"
    );
}

/// Regression for Finding 2: Schedules created with a null `next_run_at`
/// (the normal case for apply-path/desktop writes, which write only
/// apply-owned fields) must still fire. Before the fix, `ScheduleSource`
/// skipped null-`next_run_at` schedules forever, so tasks configured via
/// the CLI or desktop never ran.
///
/// Expected behavior: the runtime seeds `next_run_at = now` on the
/// first-seen tick for the schedule, treats the same tick as due, and
/// yields a `FireIntent` within a bounded wait (a couple of ticks).
#[tokio::test]
async fn schedule_source_seeds_null_next_run_at_and_fires_on_first_tick() {
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
    ensure_runtime_schemas(node.as_ref()).await.unwrap();

    // Create a Schedule doc WITHOUT next_run_at — mirrors what the
    // CLI/desktop apply writers do (they never touch runtime-owned
    // fields). Before Finding 2 was fixed, this schedule would sit
    // inert forever because ScheduleSource treated null next_run_at as
    // "not due, skip."
    let escaped_schedule_id = escape_graphql_string("sched-null");
    let escaped_task_id = escape_graphql_string("task-null");
    let mutation = format!(
        r#"mutation {{
            create_Schedule(input: {{
                schedule_id: "{escaped_schedule_id}",
                task_id: "{escaped_task_id}",
                interval_secs: 60,
                enabled: true,
                concurrency: "serial"
            }}) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create Schedule without next_run_at failed: {:?}",
        response.errors
    );

    // Sanity check: the doc really has a null next_run_at right now.
    let precondition = load_schedule_next_run_at(node.as_ref(), "sched-null")
        .await
        .unwrap();
    assert!(
        precondition.is_none(),
        "precondition: created Schedule should have null next_run_at, got {precondition:?}"
    );

    let task = ResolvedTask {
        task_id: "task-null".to_string(),
        name: None,
        behavior_id: "general".to_string(),
        prompt_template: "hi".to_string(),
        output_schema_ref: None,
    };
    let snapshot = snapshot_with_schedules(HashMap::from([(
        "sched-null".to_string(),
        resolved_schedule("sched-null", task),
    )]));
    let (_tx, rx) = watch::channel(snapshot);
    let cancel = CancellationToken::new();
    let mut source = ScheduleSource::new(rx, node.clone(), cancel.clone())
        .with_tick_every(Duration::from_millis(50));

    // With the fix: first tick seeds next_run_at = now, treats as due,
    // yields intent. Without the fix: null is treated as "not due" on
    // every tick and we'd time out.
    let started = Instant::now();
    let intent = tokio::time::timeout(Duration::from_secs(2), source.next_fire())
        .await
        .expect(
            "next_fire did not yield a FireIntent within 2s for a schedule with null \
             next_run_at; the engine must seed next_run_at on first-seen (Finding 2)",
        )
        .expect("next_fire returned None for a schedule with null next_run_at");
    let elapsed = started.elapsed();

    assert_eq!(intent.trigger_id.as_deref(), Some("sched-null"));
    assert_eq!(intent.trigger_kind, TriggerKind::Schedule);
    // Upper bound is loose: we only need "much less than the 60s
    // interval_secs" to prove first-tick seeding, not exact latency.
    assert!(
        elapsed < Duration::from_secs(2),
        "first-tick fire should land within a couple of ticks, took {elapsed:?}"
    );

    // The DB should now carry a non-null next_run_at — either the raw
    // seed (if on_result hasn't run) or the advanced value (if it has).
    // Either proves seeding happened.
    let after_seed = load_schedule_next_run_at(node.as_ref(), "sched-null")
        .await
        .unwrap();
    assert!(
        after_seed.is_some(),
        "Schedule.next_run_at should no longer be null after first-seen seeding"
    );
}

#[tokio::test]
async fn schedule_source_seeds_cron_next_run_at_without_immediate_fire() {
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
    ensure_runtime_schemas(node.as_ref()).await.unwrap();

    let mutation = r#"mutation {
        create_Schedule(input: {
            schedule_id: "sched-cron-null",
            task_id: "task-cron",
            cron: "30 0 * * *",
            timezone: "America/Los_Angeles",
            missed_run_policy: "latest_only",
            enabled: true,
            concurrency: "serial"
        }) { _docID }
    }"#;
    let response = node.execute(mutation).await;
    assert!(
        !response.has_errors(),
        "create cron Schedule without next_run_at failed: {:?}",
        response.errors
    );

    let task = ResolvedTask {
        task_id: "task-cron".to_string(),
        name: None,
        behavior_id: "general".to_string(),
        prompt_template: "hi".to_string(),
        output_schema_ref: None,
    };
    let schedule = ResolvedSchedule {
        schedule_id: "sched-cron-null".to_string(),
        task_id: task.task_id.clone(),
        task,
        cadence: ScheduleCadence::Cron {
            expression: "30 0 * * *".to_string(),
            timezone: "America/Los_Angeles".to_string(),
            missed_run_policy: crate::schedule_cron::CronMissedRunPolicy::LatestOnly,
        },
        enabled: true,
        concurrency: ConcurrencyMode::Serial,
    };
    let snapshot =
        snapshot_with_schedules(HashMap::from([("sched-cron-null".to_string(), schedule)]));
    let (_tx, rx) = watch::channel(snapshot);
    let cancel = CancellationToken::new();
    let mut source = ScheduleSource::new(rx, node.clone(), cancel.clone())
        .with_tick_every(Duration::from_millis(50));
    let started_at = Utc::now();

    let handle = tokio::spawn(async move { source.next_fire().await });
    let seeded = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match load_schedule_next_run_at(node.as_ref(), "sched-cron-null")
                .await
                .unwrap()
            {
                Some(value) => break value,
                None => {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            }
        }
    })
    .await
    .expect("cron schedule should seed next_run_at within 2s");

    cancel.cancel();
    assert!(
        handle.await.unwrap().is_none(),
        "cron schedule should stay idle after seeding a future next_run_at"
    );

    let parsed = DateTime::parse_from_rfc3339(&seeded)
        .unwrap()
        .with_timezone(&Utc);
    assert!(
        parsed > started_at,
        "cron first seed should be in the future, got {seeded}"
    );
    let timezone = crate::schedule_cron::parse_timezone("America/Los_Angeles").unwrap();
    let local = parsed.with_timezone(&timezone);
    assert_eq!(local.hour(), 0);
    assert_eq!(local.minute(), 30);
    let started_local_date = started_at.with_timezone(&timezone).date_naive();
    let next_local_date = started_local_date
        .succ_opt()
        .expect("test date should have a next day");
    assert!(
        local.date_naive() == started_local_date || local.date_naive() == next_local_date,
        "daily cron should seed to today or tomorrow in local time, got {local}"
    );
}

/// Regression for Finding 1: `ScheduleSource::next_fire` must NOT return
/// `None` on an idle tick. The engine's outer loop interprets `None` as
/// source exhaustion and breaks out — a premature `None` here (e.g. from
/// "no schedules are due right now") kills the schedule driver forever.
///
/// We drive the source with an empty active-schedule set and a short
/// tick, poll `next_fire` for >200ms (4+ ticks of 50ms), then cancel and
/// confirm that (a) we didn't get a spurious `Some(intent)` in that
/// window, and (b) after cancel the future completes with `None` within
/// a bounded wait. Before the fix, this test would observe `None`
/// arriving long before the cancel and fail the timeout-before-cancel
/// check.
#[tokio::test]
async fn schedule_source_next_fire_survives_idle_ticks_until_cancelled() {
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
    ensure_runtime_schemas(node.as_ref()).await.unwrap();

    // No active schedules: every tick will be an "idle" tick where the
    // snapshot scan finds nothing to fire. Before Finding 1 was fixed,
    // the first such tick would fall off the end of the function and
    // return `None`, ending the source.
    let snapshot = snapshot_with_schedules(HashMap::new());
    let (_tx, rx) = watch::channel(snapshot);
    let cancel = CancellationToken::new();
    let mut source = ScheduleSource::new(rx, node.clone(), cancel.clone())
        .with_tick_every(Duration::from_millis(50));

    // Race next_fire against a 250ms sleep followed by cancel. next_fire
    // must NOT finish before the cancel — if it does, the source
    // prematurely exited its internal loop. After cancel, it must finish
    // promptly with None.
    let cancel_clone = cancel.clone();
    let canceller = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        cancel_clone.cancel();
    });

    let started = Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(3), source.next_fire())
        .await
        .expect("next_fire did not return within 3s after cancel");
    let elapsed_until_return = started.elapsed();
    let _ = canceller.await;

    assert!(
        result.is_none(),
        "idle source should only return None after cancel, got Some(intent)",
    );
    assert!(
        elapsed_until_return >= Duration::from_millis(240),
        "next_fire returned before cancel fired at ~250ms (elapsed={elapsed_until_return:?}); \
         this means the source treated an idle tick as exhaustion and returned None early"
    );
}
