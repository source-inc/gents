use super::*;

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
        fire_mode: crate::runtime_snapshot::EventTriggerFireMode::PerDocument,
        correlation_field: None,
        expected_count: None,
        expected_count_field: None,
        group_timeout_secs: None,
        group_min_count: 1,
        workspace_authority: None,
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
        fire_mode: crate::runtime_snapshot::EventTriggerFireMode::PerDocument,
        correlation_field: None,
        expected_count: None,
        expected_count_field: None,
        group_timeout_secs: None,
        group_min_count: 1,
        workspace_authority: None,
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
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
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

    source.reconcile_subscriptions(snap2.as_ref()).await;

    assert_eq!(
        source.subscribed_collections(),
        vec!["CollectionB".to_string()],
        "after reconciling against snapshot 2 CollectionA should be dropped \
         and only CollectionB should remain in the filter set",
    );
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
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
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

#[tokio::test]
async fn per_group_startup_recovery_uses_filtered_membership_and_deterministic_scope() {
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    node.add_schema(
        r#"type GroupMember {
            run_id: String
            expected_total: Int
            kind: String
            value: String
        }"#,
    )
    .await
    .expect("group source schema");

    for (run_id, expected, kind, value) in [
        ("run-a", 2, "include", "second"),
        ("run-a", 2, "exclude", "must-not-count"),
        ("run-b", 2, "include", "partial"),
        ("run-a", 2, "include", "first"),
    ] {
        let mutation = format!(
            r#"mutation {{ create_GroupMember(input: {{
                run_id: "{run_id}", expected_total: {expected}, kind: "{kind}", value: "{value}"
            }}) {{ _docID }} }}"#
        );
        let response = node.execute(&mutation).await;
        assert!(!response.has_errors(), "{:#?}", response.errors);
    }

    let task = ResolvedTask {
        task_id: "group-task".into(),
        name: None,
        behavior_id: "general".into(),
        prompt_template: "{{ group.correlation_value }} {{ group.count }}".into(),
        output_schema_ref: None,
    };
    let trigger = ResolvedEventTrigger {
        fire_mode: crate::runtime_snapshot::EventTriggerFireMode::PerGroup,
        correlation_field: Some("run_id".into()),
        expected_count_field: Some("expected_total".into()),
        filter: Some(r#"{ kind: { _eq: "include" } }"#.into()),
        ..resolved_event_trigger("group-trigger", "GroupMember", task)
    };
    let snapshot =
        snapshot_with_event_triggers(1, HashMap::from([("group-trigger".to_string(), trigger)]));
    let (_tx, rx) = watch::channel(snapshot.clone());
    let mut source = EventSource::new(rx, node, CancellationToken::new());

    source.reconcile_subscriptions(snapshot.as_ref()).await;
    let intent = tokio::time::timeout(Duration::from_secs(2), source.next_fire())
        .await
        .expect("startup recovery timeout")
        .expect("complete group must be recovered");
    assert_eq!(intent.correlation.as_deref(), Some("run-a"));
    assert_eq!(intent.event_vars["correlation"], "run-a");
    let group = intent.group_vars.expect("group scope");
    assert_eq!(group["count"], 2);
    assert_eq!(group["complete"], true);
    let docs = group["docs"].as_array().expect("group docs");
    assert_eq!(docs.len(), 2);
    assert!(docs.iter().all(|doc| doc["kind"] == "include"));
    let doc_ids = docs
        .iter()
        .map(|doc| doc["_docID"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(doc_ids.windows(2).all(|pair| pair[0] < pair[1]));
}

#[tokio::test]
async fn per_group_timeout_uses_durable_first_seen_clock() {
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    node.add_schema(
        r#"type DurableGroupMember {
            run_id: String
            value: String
        }"#,
    )
    .await
    .expect("group source schema");
    let response = node
        .execute(
            r#"mutation {
                create_DurableGroupMember(input: { run_id: "run-old", value: "partial" }) {
                    _docID
                }
            }"#,
        )
        .await;
    assert!(!response.has_errors(), "{:#?}", response.errors);

    let task = resolved_task("{{ group.correlation_value }} {{ group.complete }}");
    let trigger = ResolvedEventTrigger {
        fire_mode: crate::runtime_snapshot::EventTriggerFireMode::PerGroup,
        correlation_field: Some("run_id".into()),
        expected_count: Some(2),
        group_timeout_secs: Some(60),
        group_min_count: 1,
        ..resolved_event_trigger("durable-group-trigger", "DurableGroupMember", task)
    };
    let (group_key, trigger_config_key) = EventSource::group_state_keys(&trigger, "run-old");
    let first_seen_at =
        (Utc::now() - ChronoDuration::seconds(120)).to_rfc3339_opts(SecondsFormat::Millis, true);
    let mutation = format!(
        r#"mutation {{
            create_EventTriggerGroupState(input: {{
                group_key: "{}"
                trigger_id: "durable-group-trigger"
                correlation: "run-old"
                trigger_config_key: "{}"
                first_seen_at: "{}"
            }}) {{ _docID }}
        }}"#,
        escape_graphql_string(&group_key),
        escape_graphql_string(&trigger_config_key),
        escape_graphql_string(&first_seen_at),
    );
    let response = node.execute(&mutation).await;
    assert!(!response.has_errors(), "{:#?}", response.errors);

    let snapshot = snapshot_with_event_triggers(
        1,
        HashMap::from([("durable-group-trigger".to_string(), trigger)]),
    );
    let (_tx, rx) = watch::channel(snapshot.clone());
    let mut source = EventSource::new(rx, node, CancellationToken::new());

    source.reconcile_subscriptions(snapshot.as_ref()).await;
    let intent = tokio::time::timeout(Duration::from_secs(2), source.next_fire())
        .await
        .expect("durably elapsed timeout recovery")
        .expect("partial group above the floor must fire");
    assert_eq!(intent.correlation.as_deref(), Some("run-old"));
    assert_eq!(intent.group_vars.as_ref().unwrap()["complete"], false);
}

#[tokio::test]
async fn correlation_populated_after_create_remains_eligible_for_delivery() {
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    node.add_schema(
        r#"type DeferredCorrelationMember {
            run_id: String @index
            value: String
        }"#,
    )
    .await
    .expect("deferred correlation source schema");

    let response = node
        .execute(
            r#"mutation {
                create_DeferredCorrelationMember(input: { value: "created-first" }) {
                    _docID
                }
            }"#,
        )
        .await;
    assert!(!response.has_errors(), "{:#?}", response.errors);
    let lookup = node
        .execute(r#"query { DeferredCorrelationMember { _docID } }"#)
        .await;
    assert!(!lookup.has_errors(), "{:#?}", lookup.errors);
    let doc_id = lookup
        .data
        .as_ref()
        .and_then(|data| data.get("DeferredCorrelationMember"))
        .and_then(serde_json::Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("_docID"))
        .and_then(serde_json::Value::as_str)
        .expect("created document id")
        .to_string();

    let trigger = ResolvedEventTrigger {
        correlation_field: Some("run_id".into()),
        ..resolved_event_trigger(
            "deferred-correlation-trigger",
            "DeferredCorrelationMember",
            resolved_task("{{ event.correlation }}"),
        )
    };
    let snapshot = snapshot_with_event_triggers(
        1,
        HashMap::from([("deferred-correlation-trigger".to_string(), trigger)]),
    );
    let (_tx, rx) = watch::channel(snapshot.clone());
    let mut source = EventSource::new(rx, node.clone(), CancellationToken::new());

    // Startup sees the row, but must not consume it while its declared
    // correlation is absent. A producer may populate that field in a
    // follow-up mutation.
    source.reconcile_subscriptions(snapshot.as_ref()).await;
    let mutation = format!(
        r#"mutation {{
            update_DeferredCorrelationMember(
                docID: "{}",
                input: {{ run_id: "run-late" }}
            ) {{ _docID }}
        }}"#,
        escape_graphql_string(&doc_id),
    );
    let response = node.execute(&mutation).await;
    assert!(!response.has_errors(), "{:#?}", response.errors);

    let intent = tokio::time::timeout(Duration::from_secs(2), source.next_fire())
        .await
        .expect("follow-up correlation update timed out")
        .expect("follow-up correlation update must deliver the created document");
    assert_eq!(intent.correlation.as_deref(), Some("run-late"));
    assert_eq!(intent.event_vars["source_doc_id"], doc_id);
}

#[tokio::test]
async fn missing_correlation_defers_only_that_trigger_on_a_shared_document() {
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    node.add_schema(
        r#"type MixedCorrelationMember {
            run_id: String @index
            value: String
        }"#,
    )
    .await
    .expect("mixed correlation source schema");

    let ready = resolved_event_trigger(
        "trigger-a-ready",
        "MixedCorrelationMember",
        resolved_task("ready"),
    );
    let pending = ResolvedEventTrigger {
        correlation_field: Some("run_id".into()),
        ..resolved_event_trigger(
            "trigger-z-pending",
            "MixedCorrelationMember",
            resolved_task("{{ event.correlation }}"),
        )
    };
    let snapshot = snapshot_with_event_triggers(
        1,
        HashMap::from([
            (ready.trigger_id.clone(), ready),
            (pending.trigger_id.clone(), pending),
        ]),
    );
    let (_tx, rx) = watch::channel(snapshot.clone());
    let mut source = EventSource::new(rx, node.clone(), CancellationToken::new());
    source.reconcile_subscriptions(snapshot.as_ref()).await;

    let response = node
        .execute(
            r#"mutation {
                create_MixedCorrelationMember(input: { value: "created-first" }) {
                    _docID
                }
            }"#,
        )
        .await;
    assert!(!response.has_errors(), "{:#?}", response.errors);
    let lookup = node
        .execute(r#"query { MixedCorrelationMember { _docID } }"#)
        .await;
    assert!(!lookup.has_errors(), "{:#?}", lookup.errors);
    let doc_id = lookup.data.as_ref().unwrap()["MixedCorrelationMember"][0]["_docID"]
        .as_str()
        .expect("created document id")
        .to_string();

    let first = tokio::time::timeout(Duration::from_secs(2), source.next_fire())
        .await
        .expect("ready sibling delivery timed out")
        .expect("ready sibling must not be blocked by missing correlation");
    assert_eq!(first.trigger_id.as_deref(), Some("trigger-a-ready"));

    let mutation = format!(
        r#"mutation {{
            update_MixedCorrelationMember(
                docID: "{}",
                input: {{ run_id: "run-late" }}
            ) {{ _docID }}
        }}"#,
        escape_graphql_string(&doc_id),
    );
    let response = node.execute(&mutation).await;
    assert!(!response.has_errors(), "{:#?}", response.errors);

    let second = tokio::time::timeout(Duration::from_secs(2), source.next_fire())
        .await
        .expect("deferred sibling delivery timed out")
        .expect("deferred sibling must become eligible when correlation arrives");
    assert_eq!(second.trigger_id.as_deref(), Some("trigger-z-pending"));
    assert_eq!(second.correlation.as_deref(), Some("run-late"));
}

#[tokio::test]
async fn startup_seed_defers_only_the_incomplete_trigger_on_a_shared_document() {
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    node.add_schema(
        r#"type SeededMixedCorrelationMember {
            run_id: String @index
            value: String
        }"#,
    )
    .await
    .expect("seeded mixed correlation source schema");
    let response = node
        .execute(
            r#"mutation {
                create_SeededMixedCorrelationMember(input: { value: "pre-existing" }) {
                    _docID
                }
            }"#,
        )
        .await;
    assert!(!response.has_errors(), "{:#?}", response.errors);
    let lookup = node
        .execute(r#"query { SeededMixedCorrelationMember { _docID } }"#)
        .await;
    assert!(!lookup.has_errors(), "{:#?}", lookup.errors);
    let doc_id = lookup.data.as_ref().unwrap()["SeededMixedCorrelationMember"][0]["_docID"]
        .as_str()
        .expect("created document id")
        .to_string();

    let ready = resolved_event_trigger(
        "trigger-a-ready",
        "SeededMixedCorrelationMember",
        resolved_task("ready"),
    );
    let pending = ResolvedEventTrigger {
        correlation_field: Some("run_id".into()),
        ..resolved_event_trigger(
            "trigger-z-pending",
            "SeededMixedCorrelationMember",
            resolved_task("{{ event.correlation }}"),
        )
    };
    let snapshot = snapshot_with_event_triggers(
        1,
        HashMap::from([
            (ready.trigger_id.clone(), ready),
            (pending.trigger_id.clone(), pending),
        ]),
    );
    let (_tx, rx) = watch::channel(snapshot.clone());
    let mut source = EventSource::new(rx, node.clone(), CancellationToken::new());
    source.reconcile_subscriptions(snapshot.as_ref()).await;

    let mutation = format!(
        r#"mutation {{
            update_SeededMixedCorrelationMember(
                docID: "{}",
                input: {{ run_id: "run-seeded-late" }}
            ) {{ _docID }}
        }}"#,
        escape_graphql_string(&doc_id),
    );
    let response = node.execute(&mutation).await;
    assert!(!response.has_errors(), "{:#?}", response.errors);

    let intent = tokio::time::timeout(Duration::from_secs(2), source.next_fire())
        .await
        .expect("deferred seeded delivery timed out")
        .expect("only the correlation-incomplete trigger should remain eligible");
    assert_eq!(intent.trigger_id.as_deref(), Some("trigger-z-pending"));
    assert_eq!(intent.correlation.as_deref(), Some("run-seeded-late"));
}

#[tokio::test]
async fn invalid_group_is_durably_quiesced_and_pruned_after_restart() {
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    node.add_schema(
        r#"type QuiescedGroupMember {
            run_id: String @index
            value: String
        }"#,
    )
    .await
    .expect("quiesced group source schema");
    for value in ["one", "two"] {
        let mutation = format!(
            r#"mutation {{
                create_QuiescedGroupMember(input: {{ run_id: "run-overfull", value: "{value}" }}) {{
                    _docID
                }}
            }}"#,
        );
        let response = node.execute(&mutation).await;
        assert!(!response.has_errors(), "{:#?}", response.errors);
    }

    let trigger = ResolvedEventTrigger {
        fire_mode: crate::runtime_snapshot::EventTriggerFireMode::PerGroup,
        correlation_field: Some("run_id".into()),
        expected_count: Some(1),
        ..resolved_event_trigger(
            "quiesced-group-trigger",
            "QuiescedGroupMember",
            resolved_task("{{ group.count }}"),
        )
    };
    let snapshot = snapshot_with_event_triggers(
        1,
        HashMap::from([("quiesced-group-trigger".to_string(), trigger)]),
    );
    let (_tx, rx) = watch::channel(snapshot.clone());
    let mut first_source = EventSource::new(rx, node.clone(), CancellationToken::new());
    first_source
        .reconcile_subscriptions(snapshot.as_ref())
        .await;
    assert_eq!(first_source.group_membership_query_count(), 1);

    let response = node
        .execute(
            r#"query {
                EventTriggerGroupState(
                    filter: { trigger_id: { _eq: "quiesced-group-trigger" } }
                ) { quiesced_at quiesced_reason }
            }"#,
        )
        .await;
    assert!(!response.has_errors(), "{:#?}", response.errors);
    let row = response
        .data
        .as_ref()
        .and_then(|data| data.get("EventTriggerGroupState"))
        .and_then(serde_json::Value::as_array)
        .and_then(|rows| rows.first())
        .expect("durable group state");
    assert!(row.get("quiesced_at").is_some_and(|value| !value.is_null()));
    assert!(row["quiesced_reason"]
        .as_str()
        .is_some_and(|reason| reason.contains("exceeds expected count")));

    // A fresh source has no process-local timer state. Its bounded startup
    // sweep must prune the durable quiescence marker before issuing another
    // full membership query for the permanently invalid group.
    let (_tx, rx) = watch::channel(snapshot.clone());
    let mut restarted_source = EventSource::new(rx, node, CancellationToken::new());
    restarted_source
        .reconcile_subscriptions(snapshot.as_ref())
        .await;
    assert_eq!(restarted_source.group_membership_query_count(), 0);
}

#[test]
fn group_state_identity_changes_only_with_membership_definition() {
    let task = resolved_task("first prompt");
    let mut trigger = ResolvedEventTrigger {
        fire_mode: crate::runtime_snapshot::EventTriggerFireMode::PerGroup,
        correlation_field: Some("run_id".into()),
        expected_count: Some(2),
        ..resolved_event_trigger("group-trigger", "GroupMember", task)
    };
    let initial = EventSource::group_state_keys(&trigger, "run-a");

    trigger.task.prompt_template = "changed prompt".into();
    trigger.expected_count = Some(3);
    assert_eq!(
        EventSource::group_state_keys(&trigger, "run-a"),
        initial,
        "task and policy changes must not restart a group's first-seen clock",
    );

    trigger.filter = Some(r#"{ kind: { _eq: "include" } }"#.into());
    assert_ne!(
        EventSource::group_state_keys(&trigger, "run-a"),
        initial,
        "membership filter changes must use fresh recovery state",
    );
}

#[tokio::test]
async fn generation_bump_recovers_only_when_group_membership_definition_changes() {
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    node.add_schema(
        r#"type RecoveryFingerprintMember {
            run_id: String
            kind: String
        }"#,
    )
    .await
    .expect("group source schema");

    let task = resolved_task("first prompt");
    let trigger = ResolvedEventTrigger {
        fire_mode: crate::runtime_snapshot::EventTriggerFireMode::PerGroup,
        correlation_field: Some("run_id".into()),
        expected_count: Some(2),
        ..resolved_event_trigger("fingerprint-trigger", "RecoveryFingerprintMember", task)
    };
    let second_trigger = ResolvedEventTrigger {
        trigger_id: "fingerprint-trigger-2".into(),
        ..trigger.clone()
    };
    let snapshot_one = snapshot_with_event_triggers(
        1,
        HashMap::from([
            ("fingerprint-trigger".to_string(), trigger.clone()),
            ("fingerprint-trigger-2".to_string(), second_trigger.clone()),
        ]),
    );
    let (_tx, rx) = watch::channel(snapshot_one.clone());
    let mut source = EventSource::new(rx, node, CancellationToken::new());
    source.reconcile_subscriptions(snapshot_one.as_ref()).await;
    assert_eq!(source.group_recovery_page_query_count(), 1);

    let mut task_only_change = trigger.clone();
    task_only_change.task.prompt_template = "second prompt".into();
    let snapshot_two = snapshot_with_event_triggers(
        2,
        HashMap::from([
            ("fingerprint-trigger".to_string(), task_only_change),
            ("fingerprint-trigger-2".to_string(), second_trigger.clone()),
        ]),
    );
    source.reconcile_subscriptions(snapshot_two.as_ref()).await;
    assert_eq!(
        source.group_recovery_page_query_count(),
        1,
        "an unrelated snapshot generation bump must not launch recovery I/O",
    );

    let mut membership_change = trigger;
    membership_change.filter = Some(r#"{ kind: { _eq: "include" } }"#.into());
    let snapshot_three = snapshot_with_event_triggers(
        3,
        HashMap::from([
            ("fingerprint-trigger".to_string(), membership_change),
            ("fingerprint-trigger-2".to_string(), second_trigger),
        ]),
    );
    source
        .reconcile_subscriptions(snapshot_three.as_ref())
        .await;
    assert_eq!(
        source.group_recovery_page_query_count(),
        2,
        "a membership-definition change must restart bounded recovery",
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
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
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
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
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

/// Task 22: a Fired result dispatched through the `on_result` callback must
/// write the runtime-owned bookkeeping fields back onto the `EventTrigger`
/// document: `last_status = "fired"`, `fire_count += 1`,
/// `last_fired_source_doc_id` set to the source doc id that caused the fire,
/// and `last_attempt_at` populated. Apply-owned fields (`enabled`, `task_id`,
/// `source_collection`, `event_kind`, `concurrency`) must be untouched.
#[tokio::test]
async fn event_source_on_result_writes_runtime_fields_on_fired() {
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
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

    // Dispatch a synthetic Fired result into the callback. The callback
    // spawns a background write, so poll the DB until it lands (bounded
    // retry). This mirrors the ScheduleSource Fired test pattern.
    (intent.on_result)(FireResult::Fired {
        request_id: "req-0".to_string(),
    });

    let mut fired_trigger = None;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let records = list_event_trigger_records(node.as_ref()).await.unwrap();
        let (_doc_id, trig) = records
            .iter()
            .find(|(_d, t)| t.trigger_id == "trigger-fired")
            .cloned()
            .expect("EventTrigger doc disappeared");
        if trig.last_status.as_deref() == Some("fired") {
            fired_trigger = Some(trig);
            break;
        }
    }
    let fired = fired_trigger.expect("EventTrigger.last_status never became \"fired\"");
    assert_eq!(fired.last_status.as_deref(), Some("fired"));
    assert_eq!(fired.fire_count, Some(1));
    assert_eq!(
        fired.last_fired_source_doc_id.as_deref(),
        Some(fired_source_doc_id.as_str()),
        "last_fired_source_doc_id should match the source doc id carried \
         by the intent",
    );
    assert!(
        fired.last_attempt_at.is_some(),
        "last_attempt_at should be set after a fire",
    );
    assert_eq!(
        fired.last_error, None,
        "last_error must be cleared on a successful fire",
    );
    // Apply-owned fields must not be clobbered by the runtime writeback.
    assert_eq!(fired.task_id.as_deref(), Some("task-webhook"));
    assert_eq!(fired.source_collection.as_deref(), Some("WebhookEvent"));
    assert_eq!(fired.event_kind.as_deref(), Some("created"));
    assert_eq!(fired.enabled, Some(true));
    assert_eq!(fired.concurrency.as_deref(), Some("serial"));

    cancel.cancel();
}

/// Task 22: a Skipped result writes `last_status = "skipped"` and records
/// the skip reason in `last_error` without advancing `fire_count`. A
/// subsequent Errored result flips `last_status` to `"error"` and replaces
/// `last_error` with the failure string. Both writes must go through a
/// single source instance (and a single intent) to exercise the callback
/// directly without re-driving `next_fire` for each phase — per the spec,
/// the callback is a pure synthesizer of runtime-field updates from a
/// `FireResult` value.
#[tokio::test]
async fn event_source_on_result_writes_runtime_fields_on_skipped_or_errored() {
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
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

    // ---- Skipped phase ----
    // The callback is a `FnOnce` closure so we can only invoke it once per
    // intent. To drive two phases in one test we'd need two intents. The
    // simpler path: invoke with Skipped here, then synthesize a second
    // writeback by calling `spawn_runtime_field_write` directly for the
    // Errored case below. That mirrors exactly what the intent closure does
    // internally, and keeps the test focused on the writeback shape.
    let trigger_id = "trigger-skip-err".to_string();
    let source_doc_id = intent
        .event_vars
        .get("source_doc_id")
        .and_then(|v| v.as_str())
        .expect("event_vars.source_doc_id must be a string")
        .to_string();

    (intent.on_result)(FireResult::Skipped {
        reason: "serial: prior fire still in-flight".to_string(),
    });

    let mut skipped_trigger = None;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let records = list_event_trigger_records(node.as_ref()).await.unwrap();
        let (_doc_id, trig) = records
            .iter()
            .find(|(_d, t)| t.trigger_id == "trigger-skip-err")
            .cloned()
            .expect("EventTrigger doc disappeared");
        if trig.last_status.as_deref() == Some("skipped") {
            skipped_trigger = Some(trig);
            break;
        }
    }
    let skipped = skipped_trigger.expect("EventTrigger.last_status never became \"skipped\"");
    assert_eq!(skipped.last_status.as_deref(), Some("skipped"));
    // fire_count MUST NOT advance on skip.
    assert_eq!(skipped.fire_count, Some(0));
    assert_eq!(
        skipped.last_error.as_deref(),
        Some("serial: prior fire still in-flight"),
        "last_error should carry the skip reason for operator visibility",
    );
    assert!(
        skipped.last_attempt_at.is_some(),
        "last_attempt_at should be set on a skip",
    );
    assert_eq!(
        skipped.last_fired_source_doc_id.as_deref(),
        Some(source_doc_id.as_str()),
        "last_fired_source_doc_id should record the candidate even on skip",
    );
    // Apply-owned fields intact.
    assert_eq!(skipped.task_id.as_deref(), Some("task-webhook"));
    assert_eq!(skipped.enabled, Some(true));
    assert_eq!(skipped.concurrency.as_deref(), Some("serial"));

    // ---- Errored phase ----
    // Drive the same writeback path with an Errored result. The helper is
    // an inherent `fn` on EventSource so we can call it directly — this is
    // exactly the path the `on_result` closure takes internally.
    EventSource::spawn_runtime_field_write(
        node.clone(),
        trigger_id.clone(),
        source_doc_id.clone(),
        FireResult::Errored {
            error: "materializer failed: backend timeout".to_string(),
        },
    );

    let mut errored_trigger = None;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let records = list_event_trigger_records(node.as_ref()).await.unwrap();
        let (_doc_id, trig) = records
            .iter()
            .find(|(_d, t)| t.trigger_id == "trigger-skip-err")
            .cloned()
            .expect("EventTrigger doc disappeared");
        if trig.last_status.as_deref() == Some("error") {
            errored_trigger = Some(trig);
            break;
        }
    }
    let errored = errored_trigger.expect("EventTrigger.last_status never became \"error\"");
    assert_eq!(errored.last_status.as_deref(), Some("error"));
    // fire_count MUST still not advance on error.
    assert_eq!(errored.fire_count, Some(0));
    assert_eq!(
        errored.last_error.as_deref(),
        Some("materializer failed: backend timeout"),
        "last_error should carry the failure string on Errored",
    );
    // Apply-owned fields intact.
    assert_eq!(errored.task_id.as_deref(), Some("task-webhook"));
    assert_eq!(errored.enabled, Some(true));
    assert_eq!(errored.concurrency.as_deref(), Some("serial"));

    cancel.cancel();
}

// ---------------------------------------------------------------------------
// Regression tests for the duplicate-on-update / fan-out correctness fixes.
// The DefraDB event bus emits a single `EventName::Update` variant for
// creates, updates, and deletes; v1 event triggers ship `event_kind =
// "created"` only. The event source enforces that forward-only contract via
// a first-seen gate seeded at subscription open, and fans out a single
// observation across every matching trigger.
// ---------------------------------------------------------------------------

/// Finding 1: a pre-existing source doc whose first observation arrives
/// AFTER the subscription opens must NOT fire — the seed populated by
/// `reconcile_subscriptions` registers it as already-seen. This is the
/// "don't fire on update" half of the forward-only semantic.
#[tokio::test]
async fn event_source_skips_event_for_doc_already_seen_at_subscribe() {
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
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

    // Seed a doc BEFORE the trigger + subscription exist. The first-seen
    // seed query at reconcile time will pick this doc up and mark it as
    // already-observed so any subsequent Update for it is treated as an
    // update (and dropped under v1 semantics).
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

    // Open the trigger + subscription AFTER the seed doc exists. Reconcile
    // should run the seed query and capture `preexisting_doc_id` into
    // seen_docs so the next Update is treated as a non-first observation.
    let task = resolved_task("ignored");
    let trigger = resolved_event_trigger("trigger-noupdate", "WebhookEvent", task);
    let snapshot = snapshot_with_event_triggers(
        1,
        HashMap::from([("trigger-noupdate".to_string(), trigger)]),
    );
    let (_tx, rx) = watch::channel(snapshot.clone());
    let cancel = CancellationToken::new();
    let mut source = EventSource::new(rx, node.clone(), cancel.clone());
    source.reconcile_subscriptions(snapshot.as_ref()).await;

    // Now drive an UPDATE to the pre-existing doc. Events flow, but the
    // first-seen gate should drop this one — it's a non-first observation.
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

    // next_fire MUST time out — the update was suppressed by the first-seen
    // gate. A short window is sufficient because the event bus round-trip
    // is milliseconds; anything above that window would mean we got a fire.
    let result = tokio::time::timeout(Duration::from_millis(500), source.next_fire()).await;
    assert!(
        result.is_err(),
        "next_fire yielded an intent for a pre-seeded doc's update; seed seen_docs \
         did not suppress the non-first observation",
    );

    cancel.cancel();
}

/// Finding 1: the first observation of a newly-created doc fires; the next
/// observation (an update to the same doc) must NOT fire. Complements the
/// pre-existing test by exercising the runtime-maintained first-seen set
/// rather than the seed.
#[tokio::test]
async fn event_source_fires_for_first_seen_doc_then_skips_updates() {
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
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

    // Update the same doc. Second observation; the first-seen set records
    // the doc, so the update must not fire.
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
        "next_fire yielded an intent for a doc's update; first-seen gate failed to \
         suppress the second observation",
    );

    cancel.cancel();
}

/// Finding 2: one source event that matches N active triggers must yield N
/// `FireIntent`s (not 1 and not 0). Registers two triggers on the same
/// source collection with no filter, creates a single doc, and drains two
/// intents out of the source in deterministic (lex by trigger_id) order.
#[tokio::test]
async fn event_source_fans_out_one_event_across_multiple_matching_triggers() {
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
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
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
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
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
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
    source.reconcile_subscriptions(snapshot.as_ref()).await;

    assert_eq!(
        source.subscribed_collections(),
        vec!["CollectionClean".to_string()],
        "only the grammar-valid, non-reserved collection may enter the \
         desired set; identifier-invalid names must be excluded",
    );
}
