use super::*;

async fn materializer_with_node() -> (Arc<defra_node::EmbeddedNode>, ProductionMaterializer) {
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let snapshot =
        snapshot_with_behavior_and_schedules(integration_test_behavior("general"), HashMap::new());
    let (_snapshot_tx, snapshot_rx) = watch::channel(snapshot);
    let materializer = ProductionMaterializer::new(node.clone(), snapshot_rx);
    (node, materializer)
}

async fn create_request(
    node: &defra_node::EmbeddedNode,
    request_id: &str,
    agent_did: &str,
    lifecycle_state: &str,
    trigger_id: &str,
    trigger_kind: TriggerKind,
    correlation: &str,
) {
    let request_id = escape_graphql_string(request_id);
    let agent_did = escape_graphql_string(agent_did);
    let lifecycle_state = escape_graphql_string(lifecycle_state);
    let trigger_id = escape_graphql_string(trigger_id);
    let correlation = escape_graphql_string(correlation);
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{request_id}",
                agent_did: "{agent_did}",
                content: "production marker test",
                status: "{lifecycle_state}",
                lifecycle_state: "{lifecycle_state}",
                caused_by_trigger_id: "{trigger_id}",
                caused_by_trigger_kind: "{trigger_kind}",
                caused_by_correlation: "{correlation}"
            }}) {{ _docID }}
        }}"#,
        trigger_kind = trigger_kind.as_str(),
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "creating AgentRequest {request_id} failed: {:?}",
        response.errors
    );
}

/// The AgentRequest lineage tuple is the durable at-most-once marker for a
/// correlated group. Exercise the exact production GraphQL query against a
/// terminal request so this test also proves that every lifecycle state is a
/// marker, not only requests which remain active.
#[tokio::test]
async fn durable_group_marker_matches_all_four_lineage_discriminators() {
    let (node, materializer) = materializer_with_node().await;
    let agent_did = "did:key:z-marker-owner";
    let trigger_id = "review-\"verify";
    let correlation = "run-\"42";
    create_request(
        node.as_ref(),
        "marker-completed",
        agent_did,
        "completed",
        trigger_id,
        TriggerKind::Event,
        correlation,
    )
    .await;

    assert!(materializer
        .has_materialized_group_request(agent_did, trigger_id, TriggerKind::Event, correlation,)
        .await
        .unwrap());
    assert!(!materializer
        .has_materialized_group_request(
            "did:key:z-other-owner",
            trigger_id,
            TriggerKind::Event,
            correlation,
        )
        .await
        .unwrap());
    assert!(!materializer
        .has_materialized_group_request(agent_did, "review-other", TriggerKind::Event, correlation,)
        .await
        .unwrap());
    assert!(!materializer
        .has_materialized_group_request(agent_did, trigger_id, TriggerKind::Schedule, correlation,)
        .await
        .unwrap());
    assert!(!materializer
        .has_materialized_group_request(agent_did, trigger_id, TriggerKind::Event, "run-other",)
        .await
        .unwrap());
}

#[tokio::test]
async fn materializer_skips_workspace_bound_request_for_other_deployment() {
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let snapshot =
        snapshot_with_behavior_and_schedules(integration_test_behavior("general"), HashMap::new());
    let (_snapshot_tx, snapshot_rx) = watch::channel(snapshot);
    let materializer =
        ProductionMaterializer::new(node, snapshot_rx).with_local_deployment_id("deploy-replica");
    let task = ResolvedTask {
        task_id: "task-ws".to_string(),
        name: None,
        behavior_id: "general".to_string(),
        prompt_template: "patch".to_string(),
        output_schema_ref: None,
    };
    let context = serde_json::json!({
        "version": 1,
        "source_fields": {
            "workspace_id": "ws-1",
            "workspace_authority": "readWrite",
            "workspace_owner_deployment_id": "deploy-owner"
        }
    })
    .to_string();
    let error = materializer
        .materialize(
            &task,
            Some("trigger-ws"),
            TriggerKind::Event,
            Some("src-1"),
            Some("corr-1"),
            Some(&context),
            "prompt",
        )
        .await
        .expect_err("replica must not enqueue workspace-bound work");
    assert!(
        error
            .downcast_ref::<crate::trigger_engine::MaterializeSkip>()
            .is_some(),
        "{error}"
    );
    match crate::trigger_engine::fire_result_from_materialize(Err(error)) {
        FireResult::Skipped { reason } => {
            assert!(reason.contains("another deployment"), "{reason}");
        }
        other => panic!("expected Skipped, got {other:?}"),
    }
}

/// Per-group Serial/LatestOnly gates include correlation; per-document gates
/// deliberately omit it and remain trigger-wide. Prove both query shapes and
/// the matching supersede mutation against persisted rows.
#[tokio::test]
async fn active_gate_and_supersede_honor_optional_correlation_scope() {
    let (node, materializer) = materializer_with_node().await;
    let agent_did = "did:key:z-concurrency-owner";
    let trigger_id = "review-verify";
    for (request_id, correlation) in [("active-a", "run-a"), ("active-b", "run-b")] {
        create_request(
            node.as_ref(),
            request_id,
            agent_did,
            "pending",
            trigger_id,
            TriggerKind::Event,
            correlation,
        )
        .await;
    }

    assert!(materializer
        .has_active_runtime_request_for_trigger(
            agent_did,
            trigger_id,
            TriggerKind::Event,
            Some("run-a"),
        )
        .await
        .unwrap());
    assert!(!materializer
        .has_active_runtime_request_for_trigger(
            agent_did,
            trigger_id,
            TriggerKind::Event,
            Some("run-missing"),
        )
        .await
        .unwrap());
    assert!(
        materializer
            .has_active_runtime_request_for_trigger(
                agent_did,
                trigger_id,
                TriggerKind::Event,
                None,
            )
            .await
            .unwrap(),
        "omitting correlation must preserve trigger-wide per-document gating"
    );

    assert_eq!(
        materializer
            .supersede_active_runtime_requests_for_trigger(
                agent_did,
                trigger_id,
                TriggerKind::Event,
                Some("run-a"),
            )
            .await
            .unwrap(),
        1
    );
    assert!(!materializer
        .has_active_runtime_request_for_trigger(
            agent_did,
            trigger_id,
            TriggerKind::Event,
            Some("run-a"),
        )
        .await
        .unwrap());
    assert!(
        materializer
            .has_active_runtime_request_for_trigger(
                agent_did,
                trigger_id,
                TriggerKind::Event,
                Some("run-b"),
            )
            .await
            .unwrap(),
        "correlated supersede must leave sibling groups active"
    );
    assert_eq!(
        materializer
            .supersede_active_runtime_requests_for_trigger(
                agent_did,
                trigger_id,
                TriggerKind::Event,
                None,
            )
            .await
            .unwrap(),
        1,
        "omitting correlation must supersede all remaining trigger-wide rows"
    );
}
