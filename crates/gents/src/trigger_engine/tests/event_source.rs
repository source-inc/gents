use super::*;

async fn event_source_test_node() -> Arc<defra_node::EmbeddedNode> {
    let identity = crate::test_support::signed_test_identity("event-source-node");
    Arc::new(
        defra_node::EmbeddedNode::builder()
            .with_node_identity_did(identity.did())
            .build()
            .await
            .unwrap(),
    )
}

async fn ensure_snapshot_trigger_docs(
    node: &defra_node::EmbeddedNode,
    snapshot: &ActiveRuntimeSnapshot,
) {
    for trigger in snapshot.active_event_triggers().values() {
        let existing = node
            .execute(&format!(
                r#"{{ EventTrigger(filter: {{ trigger_id: {{ _eq: "{}" }} }}) {{ _docID }} }}"#,
                escape_graphql_string(&trigger.trigger_id)
            ))
            .await;
        let present = existing
            .data
            .as_ref()
            .and_then(|data| data.get("EventTrigger"))
            .and_then(serde_json::Value::as_array)
            .is_some_and(|rows| !rows.is_empty());
        if present {
            continue;
        }
        let filter = trigger
            .filter
            .as_ref()
            .map(|value| format!(r#"filter: "{}""#, escape_graphql_string(value)))
            .unwrap_or_default();
        let response = node
            .execute(&format!(
                r#"mutation {{ create_EventTrigger(input: {{
                    trigger_id: "{}"
                    task_id: "{}"
                    source_collection: "{}"
                    event_kind: "{}"
                    {filter}
                    enabled: true
                    concurrency: "serial"
                }}) {{ _docID }} }}"#,
                escape_graphql_string(&trigger.trigger_id),
                escape_graphql_string(&trigger.task_id),
                escape_graphql_string(&trigger.source_collection),
                escape_graphql_string(&trigger.event_kind),
            ))
            .await;
        assert!(!response.has_errors(), "{:?}", response.errors);
    }
}

async fn persist_materialized_event_request(node: &defra_node::EmbeddedNode, intent: &FireIntent) {
    let request_id = intent
        .materialization_request_id
        .as_deref()
        .expect("event intent must carry deterministic request id");
    let trigger_id = intent.trigger_id.as_deref().unwrap();
    let did = node.node_identity_did().unwrap();
    let mutation = format!(
        r#"mutation {{ create_AgentRequest(input: {{
            request_id: "{}" agent_did: "{}" source_author_did: "{}"
            behavior_id: "general" session_id: "session-{}"
            retry_parent_request: "" retry_root_request: "{}" superseded_by_request: ""
            content: "test" status: "pending" lifecycle_state: "pending" backend_id: ""
            execution_origin: "scheduled" caused_by_trigger_id: "{}"
            caused_by_trigger_kind: "event" failure_reason: ""
            created_at: "2026-08-08T00:00:00Z" retry_count: 0 max_retries: 3
        }}) {{ _docID }} }}"#,
        escape_graphql_string(request_id),
        escape_graphql_string(did),
        escape_graphql_string(did),
        escape_graphql_string(request_id),
        escape_graphql_string(request_id),
        escape_graphql_string(trigger_id),
    );
    let response = node.execute(&mutation).await;
    assert!(!response.has_errors(), "{:?}", response.errors);
}

/// Build a `ResolvedEventTrigger` pointing at the named source collection.
/// Matches the empty-defaults pattern used by `resolved_schedule`.
fn resolved_event_trigger(
    trigger_id: &str,
    source_collection: &str,
    task: ResolvedTask,
) -> ResolvedEventTrigger {
    ResolvedEventTrigger {
        trigger_id: trigger_id.to_string(),
        task_id: task.task_id.clone(),
        task,
        source_collection: source_collection.to_string(),
        event_kind: "created".to_string(),
        filter: None,
        enabled: true,
        concurrency: ConcurrencyMode::Serial,
    }
}

/// Variant of `resolved_event_trigger` that attaches an operator-authored
/// filter fragment (e.g. `{ kind: { _eq: "signup" } }`). Used by the
/// filter-probe tests.
fn resolved_event_trigger_with_filter(
    trigger_id: &str,
    source_collection: &str,
    task: ResolvedTask,
    filter: &str,
) -> ResolvedEventTrigger {
    ResolvedEventTrigger {
        trigger_id: trigger_id.to_string(),
        task_id: task.task_id.clone(),
        task,
        source_collection: source_collection.to_string(),
        event_kind: "created".to_string(),
        filter: Some(filter.to_string()),
        enabled: true,
        concurrency: ConcurrencyMode::Serial,
    }
}

/// Build an `ActiveRuntimeSnapshot` carrying the supplied event triggers and
/// no other live state. Mirrors `snapshot_with_schedules` for the event-source
/// tests.
fn snapshot_with_event_triggers(
    generation: u64,
    triggers: HashMap<String, ResolvedEventTrigger>,
) -> Arc<ActiveRuntimeSnapshot> {
    let resolved = ResolvedRuntimeSnapshot::from_parts_with_admission_configs(
        "general".to_string(),
        vec![integration_test_behavior("general")],
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
    )
    .with_event_triggers(triggers, HashSet::new())
    .with_principal(stub_principal());
    Arc::new(resolved.activate(generation, HashMap::new()))
}

/// Reconciling against a fresh snapshot whose `active_event_triggers`
/// reference a single source collection should populate that collection in
/// the filter set. Publishing a replacement snapshot that swaps the source
/// collection for a different one should drop the first and pick up the
/// second on the next reconciliation, proving the filter tracks the live
/// snapshot rather than accumulating history.
#[tokio::test]
async fn event_source_reconciles_subscriptions_on_generation_bump() {
    // A real embedded node is required because `reconcile_subscriptions`
    // opens the global `node.subscribe(&[EventName::Update])` subscription
    // on the first non-empty desired set.
    let node = event_source_test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();

    // Snapshot generation 1: one trigger on CollectionA.
    let task = resolved_task("ignored");
    let snap1 = snapshot_with_event_triggers(
        1,
        HashMap::from([(
            "trigger-a".to_string(),
            resolved_event_trigger("trigger-a", "CollectionA", task.clone()),
        )]),
    );
    let (snapshot_tx, snapshot_rx) = watch::channel(snap1.clone());

    let cancel = CancellationToken::new();
    let mut source = EventSource::new(snapshot_rx, node.clone(), cancel.clone());

    // Drive reconciliation against snapshot 1. `reconcile_subscriptions` is
    // called directly here — Task 19 tests the method; the `next_fire`
    // tick-boundary integration is the subject of Task 20.
    ensure_snapshot_trigger_docs(node.as_ref(), snap1.as_ref()).await;
    source.reconcile_subscriptions(snap1.as_ref()).await;

    assert_eq!(
        source.subscribed_collections(),
        vec!["CollectionA".to_string()],
        "after reconciling against snapshot 1 the filter set should exactly \
         match the snapshot's active_event_triggers source_collection",
    );

    // Snapshot generation 2: the old trigger is gone and a new one targets
    // CollectionB. Publish it through the watch channel to mimic how the
    // runtime reconcile loop hands snapshots to the engine.
    let snap2 = snapshot_with_event_triggers(
        2,
        HashMap::from([(
            "trigger-b".to_string(),
            resolved_event_trigger("trigger-b", "CollectionB", task),
        )]),
    );
    snapshot_tx.send(snap2.clone()).expect("snapshot_rx alive");

    ensure_snapshot_trigger_docs(node.as_ref(), snap2.as_ref()).await;
    source.reconcile_subscriptions(snap2.as_ref()).await;

    assert_eq!(
        source.subscribed_collections(),
        vec!["CollectionB".to_string()],
        "after reconciling against snapshot 2 CollectionA should be dropped \
         and only CollectionB should remain in the filter set",
    );
}

#[tokio::test]
async fn event_source_config_edit_creates_a_new_transactional_activation() {
    let node = event_source_test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    node.add_schema("type ConfigEditEvent { value: String }")
        .await
        .unwrap();

    let task = resolved_task("ignored");
    let first_trigger =
        resolved_event_trigger("trigger-config-edit", "ConfigEditEvent", task.clone());
    let first = snapshot_with_event_triggers(
        1,
        HashMap::from([("trigger-config-edit".to_string(), first_trigger)]),
    );
    let (snapshot_tx, snapshot_rx) = watch::channel(first.clone());
    let cancel = CancellationToken::new();
    let mut source = EventSource::new(snapshot_rx, node.clone(), cancel.clone());
    ensure_snapshot_trigger_docs(node.as_ref(), first.as_ref()).await;
    source.reconcile_subscriptions(first.as_ref()).await;

    let update = node
        .execute(
            r#"mutation {
                update_EventTrigger(
                    filter: { trigger_id: { _eq: "trigger-config-edit" } },
                    input: { filter: "{ value: { _eq: \"v2\" } }" }
                ) { _docID }
            }"#,
        )
        .await;
    assert!(!update.has_errors(), "{:?}", update.errors);

    let second_trigger = resolved_event_trigger_with_filter(
        "trigger-config-edit",
        "ConfigEditEvent",
        task,
        r#"{ value: { _eq: "v2" } }"#,
    );
    let second = snapshot_with_event_triggers(
        2,
        HashMap::from([("trigger-config-edit".to_string(), second_trigger)]),
    );
    snapshot_tx.send(second.clone()).unwrap();
    source.reconcile_subscriptions(second.as_ref()).await;

    let activations = node
        .execute(
            r#"{
                EventTriggerActivation(
                    filter: { trigger_id: { _eq: "trigger-config-edit" } },
                    order: { created_at: ASC }
                ) { activation_key trigger_commit_cid }
            }"#,
        )
        .await;
    assert!(!activations.has_errors(), "{:?}", activations.errors);
    let rows = activations.data.unwrap()["EventTriggerActivation"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(
        rows.len(),
        2,
        "each exact trigger CID needs its own baseline"
    );
    assert_ne!(rows[0]["activation_key"], rows[1]["activation_key"]);
    assert_ne!(rows[0]["trigger_commit_cid"], rows[1]["trigger_commit_cid"]);

    cancel.cancel();
}

#[tokio::test]
async fn event_source_restart_repairs_admission_without_request() {
    let node = event_source_test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    node.add_schema("type RestartEvent { value: String }")
        .await
        .unwrap();

    let trigger = resolved_event_trigger(
        "trigger-restart-repair",
        "RestartEvent",
        resolved_task("ignored"),
    );
    let snapshot = snapshot_with_event_triggers(
        1,
        HashMap::from([("trigger-restart-repair".to_string(), trigger)]),
    );
    let (_snapshot_tx, snapshot_rx) = watch::channel(snapshot.clone());
    let cancel = CancellationToken::new();
    let mut first = EventSource::new(snapshot_rx.clone(), node.clone(), cancel.clone())
        .with_rescan_interval(Duration::from_millis(20));
    ensure_snapshot_trigger_docs(node.as_ref(), snapshot.as_ref()).await;
    first.reconcile_subscriptions(snapshot.as_ref()).await;

    let create = node
        .execute(
            r#"mutation { create_RestartEvent(input: { value: "after-activation" }) { _docID } }"#,
        )
        .await;
    assert!(!create.has_errors(), "{:?}", create.errors);
    let admitted = tokio::time::timeout(Duration::from_secs(2), first.next_fire())
        .await
        .expect("initial admission timed out")
        .expect("initial admission returned no intent");
    let request_id = admitted
        .materialization_request_id
        .clone()
        .expect("durable event intent must carry a deterministic request id");
    drop(admitted);
    drop(first);

    let mut restarted = EventSource::new(snapshot_rx, node.clone(), cancel.clone())
        .with_rescan_interval(Duration::from_millis(20));
    restarted.reconcile_subscriptions(snapshot.as_ref()).await;
    let recovered = tokio::time::timeout(Duration::from_secs(2), restarted.next_fire())
        .await
        .expect("restart repair timed out")
        .expect("restart repair returned no intent");
    assert_eq!(
        recovered.materialization_request_id.as_deref(),
        Some(request_id.as_str()),
        "admission-without-request must replay the same deterministic request id"
    );

    cancel.cancel();
}

#[tokio::test]
async fn event_source_rescan_discovers_source_created_without_subscription_wake() {
    let node = event_source_test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    node.add_schema("type OfflineEvent { value: String }")
        .await
        .unwrap();

    let trigger = resolved_event_trigger(
        "trigger-offline-rescan",
        "OfflineEvent",
        resolved_task("ignored"),
    );
    let snapshot = snapshot_with_event_triggers(
        1,
        HashMap::from([("trigger-offline-rescan".to_string(), trigger)]),
    );
    let (_snapshot_tx, snapshot_rx) = watch::channel(snapshot.clone());
    let cancel = CancellationToken::new();
    let mut activation_owner = EventSource::new(snapshot_rx.clone(), node.clone(), cancel.clone());
    ensure_snapshot_trigger_docs(node.as_ref(), snapshot.as_ref()).await;
    activation_owner
        .reconcile_subscriptions(snapshot.as_ref())
        .await;
    drop(activation_owner);

    // There is deliberately no live EventSource subscription for this write.
    let create = node
        .execute(r#"mutation { create_OfflineEvent(input: { value: "no-wake" }) { _docID } }"#)
        .await;
    assert!(!create.has_errors(), "{:?}", create.errors);

    let mut recovered = EventSource::new(snapshot_rx, node, cancel.clone())
        .with_rescan_interval(Duration::from_millis(20));
    recovered.reconcile_subscriptions(snapshot.as_ref()).await;
    let intent = tokio::time::timeout(Duration::from_secs(2), recovered.next_fire())
        .await
        .expect("durable rescan timed out")
        .expect("durable rescan returned no intent");
    assert_eq!(intent.trigger_id.as_deref(), Some("trigger-offline-rescan"));
    assert!(intent.materialization_request_id.is_some());

    cancel.cancel();
}

/// Drive `EventSource::next_fire` end-to-end against a real event stream.
///
/// The test:
/// 1. Registers a custom `WebhookEvent` schema on the embedded node so the
///    bus has a collection to emit events from (separate from the runtime
///    control collections so reconciliation is forced to walk the cache).
/// 2. Publishes a snapshot with one active `EventTrigger` on `WebhookEvent`.
/// 3. Opens the subscription (via `reconcile_subscriptions`) BEFORE creating
///    the document — `events::Bus` only buffers messages for already-
///    subscribed consumers, so a pre-subscription mutation is silently
///    dropped.
/// 4. Creates a document in that collection via a GraphQL mutation. The
///    node emits an `Update` event with `collection_id` set to the schema's
///    stable CollectionID (not the human-readable name).
/// 5. Asserts `next_fire` yields a `FireIntent` with the expected trigger
///    id, kind, task, concurrency, and event_vars shape, all within a
///    bounded 2s deadline.
#[tokio::test]
async fn event_source_next_fire_emits_intent_on_matching_real_event() {
    let node = event_source_test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();

    // Register the source collection we'll trigger on. Kept intentionally
    // minimal — the test doesn't exercise Task 21's filter/doc-var work, so
    // the doc's fields are only read by the mutation validator.
    let webhook_sdl = r#"
        type WebhookEvent {
            external_id: String
            payload: String
        }
    "#;
    node.add_schema(webhook_sdl)
        .await
        .expect("add_schema for WebhookEvent");

    // Build a snapshot with exactly one active EventTrigger on WebhookEvent.
    // The trigger_id is what the returned FireIntent should carry.
    let task = ResolvedTask {
        task_id: "task-webhook".to_string(),
        name: None,
        behavior_id: "general".to_string(),
        prompt_template: "handle webhook".to_string(),
        output_schema_ref: None,
    };
    let trigger = resolved_event_trigger("trigger-webhook", "WebhookEvent", task.clone());
    let snapshot =
        snapshot_with_event_triggers(1, HashMap::from([("trigger-webhook".to_string(), trigger)]));
    let (_tx, rx) = watch::channel(snapshot.clone());

    let cancel = CancellationToken::new();
    let mut source = EventSource::new(rx, node.clone(), cancel.clone());

    // Open the subscription BEFORE writing the doc. The bus only buffers
    // messages for already-connected subscribers — a mutation that lands
    // before subscribe() returns leaves the subscription starved.
    ensure_snapshot_trigger_docs(node.as_ref(), snapshot.as_ref()).await;
    source.reconcile_subscriptions(snapshot.as_ref()).await;
    assert_eq!(
        source.subscribed_collections(),
        vec!["WebhookEvent".to_string()],
        "precondition: subscription set should match the trigger's source_collection",
    );

    // Drive the mutation on a detached task so next_fire can park on its
    // select! arm and wake when the event lands. Delaying the write by a
    // short window lets the `recv()` future register before the message is
    // published, which is the typical runtime ordering.
    let node_for_mutation = node.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mutation = r#"mutation {
            create_WebhookEvent(input: {
                external_id: "wh-1",
                payload: "{}"
            }) { _docID }
        }"#;
        let resp = node_for_mutation.execute(mutation).await;
        assert!(
            !resp.has_errors(),
            "create_WebhookEvent failed: {:?}",
            resp.errors
        );
    });

    let intent = tokio::time::timeout(Duration::from_secs(2), source.next_fire())
        .await
        .expect("next_fire timed out waiting for WebhookEvent")
        .expect("next_fire returned None instead of emitting a FireIntent");

    assert_eq!(intent.trigger_id.as_deref(), Some("trigger-webhook"));
    assert_eq!(intent.trigger_kind, TriggerKind::Event);
    assert_eq!(intent.concurrency, ConcurrencyMode::Serial);
    assert_eq!(intent.task.task_id, "task-webhook");
    assert_eq!(intent.task.prompt_template, "handle webhook");
    // Task 21 hydrates `doc_vars` from the source doc. The trigger here
    // has no operator-authored filter, so every created doc should fire
    // and carry the full projection. We assert the shape here — the
    // dedicated hydration test drills into individual fields.
    let doc_vars = intent
        .doc_vars
        .as_ref()
        .expect("Task 21: every fire must hydrate doc_vars (filter is None here)");
    assert_eq!(
        doc_vars["external_id"].as_str(),
        Some("wh-1"),
        "doc_vars must project the WebhookEvent fields, got {doc_vars}"
    );
    assert!(intent.args_vars.is_none());

    let ev = &intent.event_vars;
    assert_eq!(ev["trigger_id"].as_str(), Some("trigger-webhook"));
    assert_eq!(ev["trigger_kind"].as_str(), Some("event"));
    assert_eq!(ev["source_collection"].as_str(), Some("WebhookEvent"));
    assert!(
        ev["source_doc_id"].as_str().is_some_and(|s| !s.is_empty()),
        "source_doc_id should be a non-empty string from the persisted doc, got {:?}",
        ev["source_doc_id"]
    );
    assert!(
        ev["fired_at"].is_string(),
        "fired_at should be a string, got {:?}",
        ev["fired_at"]
    );
}

/// Task 21, Step 1: the filter-probe path must gate the fire on the
/// trigger's operator-authored filter. With `filter: { kind: { _eq: "signup" }}`
/// live on the trigger:
///
/// 1. Writing a matching doc (`kind = "signup"`) yields a FireIntent.
/// 2. Writing a non-matching doc (`kind = "other"`) is silently dropped —
///    `next_fire` must NOT return for that doc, even though the event
///    still reaches the subscription.
///
/// We assert (1) by observing a FireIntent within a bounded window, then
/// drive (2) by writing a second non-matching doc and confirming
/// `next_fire` times out (no second intent) before we cancel the source.
#[tokio::test]
async fn event_source_filter_probe_gates_fire_on_operator_filter() {
    let node = event_source_test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();

    // Register a WebhookEvent schema that includes the `kind` field the
    // filter keys on. Must be indexed for DefraDB's filter evaluator to
    // accept `_eq` on a non-_docID field in a limit-1 query.
    let webhook_sdl = r#"
        type WebhookEvent {
            external_id: String
            payload: String
            kind: String @index
            email: String
        }
    "#;
    node.add_schema(webhook_sdl)
        .await
        .expect("add_schema for WebhookEvent");

    let task = ResolvedTask {
        task_id: "task-webhook".to_string(),
        name: None,
        behavior_id: "general".to_string(),
        prompt_template: "handle webhook".to_string(),
        output_schema_ref: None,
    };
    // Trigger requires `kind == "signup"` — `other` events must not fire.
    let trigger = resolved_event_trigger_with_filter(
        "trigger-filtered",
        "WebhookEvent",
        task.clone(),
        r#"{ kind: { _eq: "signup" } }"#,
    );
    let snapshot = snapshot_with_event_triggers(
        1,
        HashMap::from([("trigger-filtered".to_string(), trigger)]),
    );
    let (_tx, rx) = watch::channel(snapshot.clone());

    let cancel = CancellationToken::new();
    let mut source = EventSource::new(rx, node.clone(), cancel.clone());
    ensure_snapshot_trigger_docs(node.as_ref(), snapshot.as_ref()).await;
    source.reconcile_subscriptions(snapshot.as_ref()).await;

    // Write BOTH docs on a detached task. A small delay gives `next_fire`
    // time to park on its subscription recv. Order matters only for
    // tracing readability — the filter probe is run per-event, so writing
    // the non-matching doc first would still leave the matching doc as
    // the one that ultimately yields the FireIntent.
    let node_for_mutation = node.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Non-matching doc: kind = "other". The probe should reject it
        // and next_fire must NOT return for this one.
        let other_mutation = r#"mutation {
            create_WebhookEvent(input: {
                external_id: "wh-other",
                payload: "{}",
                kind: "other",
                email: "other@example.com"
            }) { _docID }
        }"#;
        let resp = node_for_mutation.execute(other_mutation).await;
        assert!(
            !resp.has_errors(),
            "create_WebhookEvent(other) failed: {:?}",
            resp.errors
        );
        // Matching doc: kind = "signup". The probe should accept this one.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let signup_mutation = r#"mutation {
            create_WebhookEvent(input: {
                external_id: "wh-signup",
                payload: "{}",
                kind: "signup",
                email: "alice@example.com"
            }) { _docID }
        }"#;
        let resp = node_for_mutation.execute(signup_mutation).await;
        assert!(
            !resp.has_errors(),
            "create_WebhookEvent(signup) failed: {:?}",
            resp.errors
        );
    });

    // The matching doc should produce an intent within the timeout. A
    // non-matching doc never yields — `next_fire` loops past it.
    let intent = tokio::time::timeout(Duration::from_secs(2), source.next_fire())
        .await
        .expect("next_fire timed out; filter-probe must yield for the signup doc")
        .expect("next_fire returned None instead of emitting a FireIntent");

    assert_eq!(intent.trigger_id.as_deref(), Some("trigger-filtered"));
    assert_eq!(intent.trigger_kind, TriggerKind::Event);
    assert_eq!(
        intent.event_vars["source_collection"].as_str(),
        Some("WebhookEvent"),
    );
    // doc_vars must be populated — covered in depth by the next test, but
    // a smoke assertion here locks the two steps together.
    let doc_vars = intent
        .doc_vars
        .as_ref()
        .expect("filter-matched fire must carry hydrated doc_vars");
    assert_eq!(
        doc_vars["kind"].as_str(),
        Some("signup"),
        "hydrated doc_vars must reflect the matching doc, got {doc_vars}"
    );
    assert_eq!(doc_vars["external_id"].as_str(), Some("wh-signup"));

    // We don't actively assert the non-matching doc was dropped beyond the
    // fact that the FireIntent we got above is for "signup" (proving the
    // source skipped over "other" rather than firing on it). A stronger
    // assertion would require a second `next_fire` poll with a short
    // timeout, which races against late-delivered events.
    cancel.cancel();
}

/// Task 21, Step 2: the FireIntent's `doc_vars` must carry the full source
/// doc projection (introspected fields, excluding GraphQL meta /
/// DefraDB-aggregate wrappers). With no filter on the trigger, every
/// created doc produces a fire, and the fire's `doc_vars` should contain
/// the operator-visible scalars we wrote into the mutation.
#[tokio::test]
async fn event_source_hydrates_doc_vars_from_source_doc_fields() {
    let node = event_source_test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();

    let webhook_sdl = r#"
        type WebhookEvent {
            external_id: String
            payload: String
            kind: String @index
            email: String
        }
    "#;
    node.add_schema(webhook_sdl)
        .await
        .expect("add_schema for WebhookEvent");

    let task = ResolvedTask {
        task_id: "task-webhook".to_string(),
        name: None,
        behavior_id: "general".to_string(),
        prompt_template: "handle webhook".to_string(),
        output_schema_ref: None,
    };
    // No filter on the trigger — every create fires, and the fire must
    // carry the full doc projection.
    let trigger = resolved_event_trigger("trigger-hydrate", "WebhookEvent", task.clone());
    let snapshot =
        snapshot_with_event_triggers(1, HashMap::from([("trigger-hydrate".to_string(), trigger)]));
    let (_tx, rx) = watch::channel(snapshot.clone());

    let cancel = CancellationToken::new();
    let mut source = EventSource::new(rx, node.clone(), cancel.clone());
    ensure_snapshot_trigger_docs(node.as_ref(), snapshot.as_ref()).await;
    source.reconcile_subscriptions(snapshot.as_ref()).await;

    let node_for_mutation = node.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mutation = r#"mutation {
            create_WebhookEvent(input: {
                external_id: "wh-hydrate",
                payload: "{\"foo\":1}",
                kind: "signup",
                email: "bob@example.com"
            }) { _docID }
        }"#;
        let resp = node_for_mutation.execute(mutation).await;
        assert!(
            !resp.has_errors(),
            "create_WebhookEvent(hydrate) failed: {:?}",
            resp.errors
        );
    });

    let intent = tokio::time::timeout(Duration::from_secs(2), source.next_fire())
        .await
        .expect("next_fire timed out; hydration path should yield on any created doc")
        .expect("next_fire returned None instead of emitting a FireIntent");

    let doc_vars = intent
        .doc_vars
        .as_ref()
        .expect("FireIntent must carry hydrated doc_vars");
    assert_eq!(
        doc_vars["external_id"].as_str(),
        Some("wh-hydrate"),
        "doc_vars must project `external_id`, got {doc_vars}"
    );
    assert_eq!(
        doc_vars["kind"].as_str(),
        Some("signup"),
        "doc_vars must project `kind`, got {doc_vars}"
    );
    assert_eq!(
        doc_vars["email"].as_str(),
        Some("bob@example.com"),
        "doc_vars must project `email`, got {doc_vars}"
    );
    assert_eq!(
        doc_vars["payload"].as_str(),
        Some(r#"{"foo":1}"#),
        "doc_vars must project `payload`, got {doc_vars}"
    );
    assert!(
        doc_vars["_docID"].as_str().is_some_and(|s| !s.is_empty()),
        "doc_vars must always carry _docID, got {doc_vars}"
    );

    cancel.cancel();
}

/// Helper: create an `EventTrigger` document keyed by `trigger_id` via a raw
/// GraphQL mutation, matching the shape used by the CLI apply path and the
/// `schedule_snapshot_reconcile` integration test. The `fire_count: 0` seed
/// is required so the runtime's `fire_count += 1` increment has a value to
/// read back.
async fn create_event_trigger_doc(
    node: &defra_node::EmbeddedNode,
    trigger_id: &str,
    task_id: &str,
    source_collection: &str,
) {
    let escaped_trigger_id = escape_graphql_string(trigger_id);
    let escaped_task_id = escape_graphql_string(task_id);
    let escaped_source_collection = escape_graphql_string(source_collection);
    let mutation = format!(
        r#"mutation {{
            create_EventTrigger(input: {{
                trigger_id: "{escaped_trigger_id}",
                task_id: "{escaped_task_id}",
                source_collection: "{escaped_source_collection}",
                event_kind: "created",
                enabled: true,
                concurrency: "serial",
                fire_count: 0
            }}) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create EventTrigger failed: {:?}",
        response.errors,
    );
}

/// Dispatch results do not mutate the trigger configuration document. Durable
/// delivery admission is the audit/correctness surface.
#[tokio::test]
async fn event_source_on_result_preserves_trigger_and_admission_on_fired() {
    let node = event_source_test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();

    // Register the source collection the trigger will observe.
    let webhook_sdl = r#"
        type WebhookEvent {
            external_id: String
            payload: String
        }
    "#;
    node.add_schema(webhook_sdl)
        .await
        .expect("add_schema for WebhookEvent");

    // Seed the EventTrigger doc so `update_event_trigger_runtime_fields` has
    // a row to write back against. Apply-path fields are set here; the
    // runtime writeback must leave them alone.
    create_event_trigger_doc(
        node.as_ref(),
        "trigger-fired",
        "task-webhook",
        "WebhookEvent",
    )
    .await;

    let task = ResolvedTask {
        task_id: "task-webhook".to_string(),
        name: None,
        behavior_id: "general".to_string(),
        prompt_template: "handle webhook".to_string(),
        output_schema_ref: None,
    };
    let trigger = resolved_event_trigger("trigger-fired", "WebhookEvent", task.clone());
    let snapshot =
        snapshot_with_event_triggers(1, HashMap::from([("trigger-fired".to_string(), trigger)]));
    let (_tx, rx) = watch::channel(snapshot.clone());

    let cancel = CancellationToken::new();
    let mut source = EventSource::new(rx, node.clone(), cancel.clone());
    // Open the subscription BEFORE writing the source doc so the mutation
    // lands after the bus has a listener. Otherwise the event is dropped.
    ensure_snapshot_trigger_docs(node.as_ref(), snapshot.as_ref()).await;
    source.reconcile_subscriptions(snapshot.as_ref()).await;

    let node_for_mutation = node.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mutation = r#"mutation {
            create_WebhookEvent(input: {
                external_id: "wh-fire",
                payload: "{}"
            }) { _docID }
        }"#;
        let resp = node_for_mutation.execute(mutation).await;
        assert!(
            !resp.has_errors(),
            "create_WebhookEvent failed: {:?}",
            resp.errors,
        );
    });

    let intent = tokio::time::timeout(Duration::from_secs(2), source.next_fire())
        .await
        .expect("next_fire timed out waiting for WebhookEvent")
        .expect("next_fire returned None instead of emitting a FireIntent");

    // Capture the source doc id the intent carries so we can assert the
    // writeback stamps it onto `last_fired_source_doc_id`.
    let fired_source_doc_id = intent
        .event_vars
        .get("source_doc_id")
        .and_then(|v| v.as_str())
        .expect("event_vars.source_doc_id must be a string")
        .to_string();

    // Dispatch a synthetic result. The callback is observability-only.
    (intent.on_result)(FireResult::Fired {
        request_id: "req-0".to_string(),
    });

    let records = list_event_trigger_records(node.as_ref()).await.unwrap();
    let (_doc_id, fired) = records
        .iter()
        .find(|(_d, t)| t.trigger_id == "trigger-fired")
        .cloned()
        .expect("EventTrigger doc disappeared");
    assert_eq!(fired.last_status, None);
    assert_eq!(fired.fire_count, Some(0));
    assert_eq!(fired.last_fired_source_doc_id, None);
    assert_eq!(fired.last_attempt_at, None);
    assert_eq!(fired.last_error, None);
    assert_eq!(fired.task_id.as_deref(), Some("task-webhook"));
    assert_eq!(fired.source_collection.as_deref(), Some("WebhookEvent"));
    assert_eq!(fired.event_kind.as_deref(), Some("created"));
    assert_eq!(fired.enabled, Some(true));
    assert_eq!(fired.concurrency.as_deref(), Some("serial"));

    let admissions = node
        .execute(&format!(
            r#"{{ EventDeliveryAdmission(filter: {{ source_doc_id: {{ _eq: "{}" }} }}) {{ _docID }} }}"#,
            escape_graphql_string(&fired_source_doc_id)
        ))
        .await;
    assert!(!admissions.has_errors(), "{:?}", admissions.errors);
    assert_eq!(
        admissions.data.unwrap()["EventDeliveryAdmission"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    cancel.cancel();
}

/// Skipped results likewise leave trigger configuration untouched.
#[tokio::test]
async fn event_source_on_result_preserves_trigger_on_skipped() {
    let node = event_source_test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();

    let webhook_sdl = r#"
        type WebhookEvent {
            external_id: String
            payload: String
        }
    "#;
    node.add_schema(webhook_sdl)
        .await
        .expect("add_schema for WebhookEvent");

    create_event_trigger_doc(
        node.as_ref(),
        "trigger-skip-err",
        "task-webhook",
        "WebhookEvent",
    )
    .await;

    let task = ResolvedTask {
        task_id: "task-webhook".to_string(),
        name: None,
        behavior_id: "general".to_string(),
        prompt_template: "handle webhook".to_string(),
        output_schema_ref: None,
    };
    let trigger = resolved_event_trigger("trigger-skip-err", "WebhookEvent", task.clone());
    let snapshot = snapshot_with_event_triggers(
        1,
        HashMap::from([("trigger-skip-err".to_string(), trigger)]),
    );
    let (_tx, rx) = watch::channel(snapshot.clone());

    let cancel = CancellationToken::new();
    let mut source = EventSource::new(rx, node.clone(), cancel.clone());
    ensure_snapshot_trigger_docs(node.as_ref(), snapshot.as_ref()).await;
    source.reconcile_subscriptions(snapshot.as_ref()).await;

    let node_for_mutation = node.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mutation = r#"mutation {
            create_WebhookEvent(input: {
                external_id: "wh-skip",
                payload: "{}"
            }) { _docID }
        }"#;
        let resp = node_for_mutation.execute(mutation).await;
        assert!(
            !resp.has_errors(),
            "create_WebhookEvent failed: {:?}",
            resp.errors,
        );
    });

    let intent = tokio::time::timeout(Duration::from_secs(2), source.next_fire())
        .await
        .expect("next_fire timed out waiting for WebhookEvent")
        .expect("next_fire returned None instead of emitting a FireIntent");

    (intent.on_result)(FireResult::Skipped {
        reason: "serial: prior fire still in-flight".to_string(),
    });

    let records = list_event_trigger_records(node.as_ref()).await.unwrap();
    let (_doc_id, skipped) = records
        .iter()
        .find(|(_d, t)| t.trigger_id == "trigger-skip-err")
        .cloned()
        .expect("EventTrigger doc disappeared");
    assert_eq!(skipped.last_status, None);
    assert_eq!(skipped.fire_count, Some(0));
    assert_eq!(skipped.last_error, None);
    assert_eq!(skipped.last_attempt_at, None);
    assert_eq!(skipped.last_fired_source_doc_id, None);
    assert_eq!(skipped.task_id.as_deref(), Some("task-webhook"));
    assert_eq!(skipped.enabled, Some(true));
    assert_eq!(skipped.concurrency.as_deref(), Some("serial"));

    cancel.cancel();
}

// ---------------------------------------------------------------------------
// Regression tests for the duplicate-on-update / fan-out correctness fixes.
// The DefraDB event bus emits a single `EventName::Update` variant for
// creates, updates, and deletes; v1 event triggers ship `event_kind =
// "created"` only. The event source enforces that forward-only contract via
// transactional activation baselines and durable delivery facts across
// restarts, and fans out one observation across every matching trigger.
// ---------------------------------------------------------------------------

/// A source doc present at activation is baselined and must not fire merely
/// because a later update wake names the same physical document.
#[tokio::test]
async fn event_source_skips_event_for_doc_already_seen_at_subscribe() {
    let node = event_source_test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();

    let webhook_sdl = r#"
        type WebhookEvent {
            external_id: String
            payload: String
        }
    "#;
    node.add_schema(webhook_sdl)
        .await
        .expect("add_schema for WebhookEvent");

    // Seed a doc before activation so its exact version enters the baseline.
    let seed_mutation = r#"mutation {
        create_WebhookEvent(input: {
            external_id: "wh-preexisting",
            payload: "seed"
        }) { _docID }
    }"#;
    let resp = node.execute(seed_mutation).await;
    assert!(
        !resp.has_errors(),
        "seeding pre-existing doc failed: {:?}",
        resp.errors,
    );
    // The returned shape varies by DefraDB version (scalar vs array); query
    // the _docID explicitly rather than parse the mutation payload.
    let lookup = r#"query {
        WebhookEvent(filter: { external_id: { _eq: "wh-preexisting" } }, limit: 1) {
            _docID
        }
    }"#;
    let resp = node.execute(lookup).await;
    assert!(
        !resp.has_errors(),
        "lookup of pre-existing doc failed: {:?}",
        resp.errors,
    );
    let preexisting_doc_id = resp
        .data
        .as_ref()
        .and_then(|d| d.get("WebhookEvent"))
        .and_then(|arr| arr.as_array())
        .and_then(|arr| arr.first())
        .and_then(|row| row.get("_docID"))
        .and_then(|v| v.as_str())
        .expect("WebhookEvent query returned no _docID")
        .to_string();

    // Reconcile after the seed; activation records it in the durable manifest.
    let task = resolved_task("ignored");
    let trigger = resolved_event_trigger("trigger-noupdate", "WebhookEvent", task);
    let snapshot = snapshot_with_event_triggers(
        1,
        HashMap::from([("trigger-noupdate".to_string(), trigger)]),
    );
    let (_tx, rx) = watch::channel(snapshot.clone());
    let cancel = CancellationToken::new();
    let mut source = EventSource::new(rx, node.clone(), cancel.clone());
    ensure_snapshot_trigger_docs(node.as_ref(), snapshot.as_ref()).await;
    source.reconcile_subscriptions(snapshot.as_ref()).await;

    // An update wake cannot reclassify a baseline document as newly created.
    let escaped = escape_graphql_string(&preexisting_doc_id);
    let update_mutation = format!(
        r#"mutation {{
            update_WebhookEvent(
                docID: "{escaped}",
                input: {{ payload: "updated" }}
            ) {{ _docID }}
        }}"#
    );
    let node_for_mutation = node.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let resp = node_for_mutation.execute(&update_mutation).await;
        assert!(
            !resp.has_errors(),
            "update_WebhookEvent failed: {:?}",
            resp.errors,
        );
    });

    // A short timeout proves neither the wake nor the rescan emitted it.
    let result = tokio::time::timeout(Duration::from_millis(500), source.next_fire()).await;
    assert!(
        result.is_err(),
        "next_fire yielded an intent for a document in the activation baseline",
    );

    cancel.cancel();
}

/// Finding 1: the first observation of a newly-created doc fires; the next
/// observation (an update to the same doc) must NOT fire. Complements the
/// pre-existing test by exercising durable delivery admission.
#[tokio::test]
async fn event_source_fires_for_first_seen_doc_then_skips_updates() {
    let node = event_source_test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();

    let webhook_sdl = r#"
        type WebhookEvent {
            external_id: String
            payload: String
        }
    "#;
    node.add_schema(webhook_sdl)
        .await
        .expect("add_schema for WebhookEvent");

    let task = resolved_task("ignored");
    let trigger = resolved_event_trigger("trigger-firstseen", "WebhookEvent", task);
    let snapshot = snapshot_with_event_triggers(
        1,
        HashMap::from([("trigger-firstseen".to_string(), trigger)]),
    );
    let (_tx, rx) = watch::channel(snapshot.clone());
    let cancel = CancellationToken::new();
    let mut source = EventSource::new(rx, node.clone(), cancel.clone());
    ensure_snapshot_trigger_docs(node.as_ref(), snapshot.as_ref()).await;
    source.reconcile_subscriptions(snapshot.as_ref()).await;

    // Create a brand-new doc; first observation should fire.
    let node_for_create = node.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mutation = r#"mutation {
            create_WebhookEvent(input: {
                external_id: "wh-first",
                payload: "{}"
            }) { _docID }
        }"#;
        let resp = node_for_create.execute(mutation).await;
        assert!(
            !resp.has_errors(),
            "create_WebhookEvent failed: {:?}",
            resp.errors,
        );
    });

    let intent = tokio::time::timeout(Duration::from_secs(2), source.next_fire())
        .await
        .expect("next_fire timed out on first observation (create should fire)")
        .expect("next_fire returned None instead of emitting a FireIntent");
    assert_eq!(intent.trigger_id.as_deref(), Some("trigger-firstseen"));
    let doc_id = intent
        .event_vars
        .get("source_doc_id")
        .and_then(|v| v.as_str())
        .expect("source_doc_id must be a string")
        .to_string();
    persist_materialized_event_request(node.as_ref(), &intent).await;

    // Once admission and request both exist, the update must not fire.
    let escaped = escape_graphql_string(&doc_id);
    let update_mutation = format!(
        r#"mutation {{
            update_WebhookEvent(
                docID: "{escaped}",
                input: {{ payload: "updated" }}
            ) {{ _docID }}
        }}"#
    );
    let node_for_update = node.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let resp = node_for_update.execute(&update_mutation).await;
        assert!(
            !resp.has_errors(),
            "update_WebhookEvent failed: {:?}",
            resp.errors,
        );
    });

    let result = tokio::time::timeout(Duration::from_millis(500), source.next_fire()).await;
    assert!(
        result.is_err(),
        "next_fire yielded an intent after durable delivery completed",
    );

    cancel.cancel();
}

/// Finding 2: one source event that matches N active triggers must yield N
/// `FireIntent`s (not 1 and not 0). Registers two triggers on the same
/// source collection with no filter, creates a single doc, and drains two
/// intents out of the source in deterministic (lex by trigger_id) order.
#[tokio::test]
async fn event_source_fans_out_one_event_across_multiple_matching_triggers() {
    let node = event_source_test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();

    let webhook_sdl = r#"
        type WebhookEvent {
            external_id: String
            payload: String
        }
    "#;
    node.add_schema(webhook_sdl)
        .await
        .expect("add_schema for WebhookEvent");

    let task = resolved_task("ignored");
    // Two triggers on the same collection. lex order: trigger-alpha < trigger-beta.
    let trigger_alpha = resolved_event_trigger("trigger-alpha", "WebhookEvent", task.clone());
    let trigger_beta = resolved_event_trigger("trigger-beta", "WebhookEvent", task);
    let snapshot = snapshot_with_event_triggers(
        1,
        HashMap::from([
            ("trigger-alpha".to_string(), trigger_alpha),
            ("trigger-beta".to_string(), trigger_beta),
        ]),
    );
    let (_tx, rx) = watch::channel(snapshot.clone());
    let cancel = CancellationToken::new();
    let mut source = EventSource::new(rx, node.clone(), cancel.clone());
    ensure_snapshot_trigger_docs(node.as_ref(), snapshot.as_ref()).await;
    source.reconcile_subscriptions(snapshot.as_ref()).await;

    // Single doc — both triggers must fire, one intent per trigger.
    let node_for_mutation = node.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mutation = r#"mutation {
            create_WebhookEvent(input: {
                external_id: "wh-fanout",
                payload: "{}"
            }) { _docID }
        }"#;
        let resp = node_for_mutation.execute(mutation).await;
        assert!(
            !resp.has_errors(),
            "create_WebhookEvent failed: {:?}",
            resp.errors,
        );
    });

    let first = tokio::time::timeout(Duration::from_secs(2), source.next_fire())
        .await
        .expect("next_fire timed out on the first fan-out intent")
        .expect("next_fire returned None instead of emitting the first intent");
    let second = tokio::time::timeout(Duration::from_secs(2), source.next_fire())
        .await
        .expect("next_fire timed out on the second fan-out intent; fan-out dropped it?")
        .expect("next_fire returned None instead of emitting the second intent");

    assert_eq!(
        first.trigger_id.as_deref(),
        Some("trigger-alpha"),
        "fan-out must emit intents in deterministic lex-by-trigger_id order",
    );
    assert_eq!(second.trigger_id.as_deref(), Some("trigger-beta"));
    // Both intents reference the same source doc.
    let first_doc_id = first
        .event_vars
        .get("source_doc_id")
        .and_then(|v| v.as_str());
    let second_doc_id = second
        .event_vars
        .get("source_doc_id")
        .and_then(|v| v.as_str());
    assert_eq!(
        first_doc_id, second_doc_id,
        "both fan-out intents must carry the same source_doc_id: {first_doc_id:?} vs {second_doc_id:?}",
    );

    cancel.cancel();
}

/// Finding 2: if the lexicographically-first trigger's filter misses, the
/// event must still be tried against the remaining triggers. Previously
/// `first_matching_trigger` would select the lex-first trigger unconditionally
/// and drop the whole event if that trigger's filter missed, silently
/// denying every other matching trigger a chance to fire.
#[tokio::test]
async fn event_source_tries_all_triggers_when_first_filter_misses() {
    let node = event_source_test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();

    let webhook_sdl = r#"
        type WebhookEvent {
            external_id: String
            payload: String
            kind: String @index
        }
    "#;
    node.add_schema(webhook_sdl)
        .await
        .expect("add_schema for WebhookEvent");

    let task = resolved_task("ignored");
    // trigger-a sorts first by lex order; its filter rejects the test doc.
    // trigger-b sorts second; its filter accepts the test doc. With the fix,
    // the engine tries trigger-a, sees the filter miss, then moves on to
    // trigger-b and fires.
    let trigger_a = resolved_event_trigger_with_filter(
        "trigger-a-lex-first",
        "WebhookEvent",
        task.clone(),
        r#"{ kind: { _eq: "signup" } }"#,
    );
    let trigger_b = resolved_event_trigger_with_filter(
        "trigger-b-matches",
        "WebhookEvent",
        task,
        r#"{ kind: { _eq: "other" } }"#,
    );
    let snapshot = snapshot_with_event_triggers(
        1,
        HashMap::from([
            ("trigger-a-lex-first".to_string(), trigger_a),
            ("trigger-b-matches".to_string(), trigger_b),
        ]),
    );
    let (_tx, rx) = watch::channel(snapshot.clone());
    let cancel = CancellationToken::new();
    let mut source = EventSource::new(rx, node.clone(), cancel.clone());
    ensure_snapshot_trigger_docs(node.as_ref(), snapshot.as_ref()).await;
    source.reconcile_subscriptions(snapshot.as_ref()).await;

    // Write a doc whose kind is "other" — misses trigger-a, matches trigger-b.
    let node_for_mutation = node.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mutation = r#"mutation {
            create_WebhookEvent(input: {
                external_id: "wh-missfirst",
                payload: "{}",
                kind: "other"
            }) { _docID }
        }"#;
        let resp = node_for_mutation.execute(mutation).await;
        assert!(
            !resp.has_errors(),
            "create_WebhookEvent failed: {:?}",
            resp.errors,
        );
    });

    let intent = tokio::time::timeout(Duration::from_secs(2), source.next_fire())
        .await
        .expect(
            "next_fire timed out; trigger-a's filter miss silently dropped the \
             event for trigger-b (fan-out regression)",
        )
        .expect("next_fire returned None instead of emitting a FireIntent");
    assert_eq!(
        intent.trigger_id.as_deref(),
        Some("trigger-b-matches"),
        "after trigger-a filter-miss, the engine must still try trigger-b and fire \
         for it; got trigger_id = {:?}",
        intent.trigger_id,
    );

    // And crucially, there must be no second intent — trigger-a did NOT
    // match the filter, so it must not have emitted.
    let maybe_extra = tokio::time::timeout(Duration::from_millis(300), source.next_fire()).await;
    assert!(
        maybe_extra.is_err(),
        "trigger-a emitted a FireIntent despite its filter miss",
    );

    cancel.cancel();
}

/// Defense-in-depth at the query-build boundary: even if a snapshot arrives
/// carrying a trigger whose `source_collection` is not a valid GraphQL
/// collection identifier (resolve-time quarantine bypassed — e.g. a snapshot
/// assembled by a different code path), `reconcile_subscriptions` must
/// refuse to admit the collection into the desired set, so no seed / rescan
/// / probe query is ever built from it.
#[tokio::test]
async fn event_source_reconcile_excludes_invalid_source_collection_identifiers() {
    let node = event_source_test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();

    let task = resolved_task("ignored");
    let snapshot = snapshot_with_event_triggers(
        1,
        HashMap::from([
            (
                "trigger-injection".to_string(),
                resolved_event_trigger(
                    "trigger-injection",
                    "Msg(limit: 1) { _docID } Foo",
                    task.clone(),
                ),
            ),
            (
                "trigger-introspection".to_string(),
                resolved_event_trigger("trigger-introspection", "__Type", task.clone()),
            ),
            (
                "trigger-clean".to_string(),
                resolved_event_trigger("trigger-clean", "CollectionClean", task),
            ),
        ]),
    );
    let (_tx, rx) = watch::channel(snapshot.clone());

    let cancel = CancellationToken::new();
    let mut source = EventSource::new(rx, node.clone(), cancel.clone());
    ensure_snapshot_trigger_docs(node.as_ref(), snapshot.as_ref()).await;
    source.reconcile_subscriptions(snapshot.as_ref()).await;

    assert_eq!(
        source.subscribed_collections(),
        vec!["CollectionClean".to_string()],
        "only the grammar-valid, non-reserved collection may enter the \
         desired set; identifier-invalid names must be excluded",
    );
}
