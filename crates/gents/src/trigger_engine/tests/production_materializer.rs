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

fn workspace_writer_task() -> ResolvedTask {
    ResolvedTask {
        task_id: "task-ws".to_string(),
        name: None,
        behavior_id: "general".to_string(),
        prompt_template: "patch".to_string(),
        output_schema_ref: None,
    }
}

fn writer_context(workspace_id: &str, owner_field: &str, owner: &str) -> String {
    let mut source_fields = serde_json::Map::new();
    source_fields.insert(
        "workspace_id".into(),
        serde_json::Value::String(workspace_id.into()),
    );
    source_fields.insert(
        "workspace_authority".into(),
        serde_json::Value::String("readWrite".into()),
    );
    source_fields.insert(owner_field.into(), serde_json::Value::String(owner.into()));
    serde_json::json!({
        "version": 1,
        "source_fields": source_fields
    })
    .to_string()
}

async fn insert_ready_workspace(node: &defra_node::EmbeddedNode, workspace_id: &str, owner: &str) {
    let mutation = crate::workspace::isolated_workspace_upsert_mutation(
        &crate::workspace::IsolatedWorkspaceDoc {
            workspace_id: workspace_id.into(),
            work_unit_id: "unit-1".into(),
            repository_id: "repo-1".into(),
            base_sha: "abc".into(),
            branch: "topic".into(),
            creation_policy: "alwaysCreate".into(),
            adapter: "git_worktree".into(),
            owner_deployment_id: owner.into(),
            writer_principal: "did:key:writer".into(),
            integrator_principal: "did:key:integrator".into(),
            instruction_manifest: "{}".into(),
            seal_hash: None,
            lifecycle_state: "ready".into(),
            caused_by_invocation_id: "inv-1".into(),
            caused_by_correlation: "corr-1".into(),
        },
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "creating IsolatedWorkspace failed: {:?}",
        response.errors
    );
}

async fn workspace_requests(
    node: &defra_node::EmbeddedNode,
    workspace_id: &str,
) -> Vec<serde_json::Value> {
    let query = format!(
        r#"{{
            AgentRequest(filter: {{ workspace_id: {{ _eq: "{id}" }} }}) {{
                request_id
                lifecycle_state
                workspace_owner_deployment_id
                workspace_authority
            }}
        }}"#,
        id = escape_graphql_string(workspace_id),
    );
    let response = node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "querying workspace requests failed: {:?}",
        response.errors
    );
    response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default()
}

#[tokio::test]
async fn materializer_skips_callback_result_owner_deployment_id_on_replica() {
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let snapshot =
        snapshot_with_behavior_and_schedules(integration_test_behavior("general"), HashMap::new());
    let (_snapshot_tx, snapshot_rx) = watch::channel(snapshot);
    let materializer =
        ProductionMaterializer::new(node, snapshot_rx).with_local_deployment_id("deploy-replica");
    let context = writer_context("ws-1", "owner_deployment_id", "deploy-owner");
    let error = materializer
        .materialize(
            &workspace_writer_task(),
            Some("trigger-ws"),
            TriggerKind::Event,
            Some("src-1"),
            Some("corr-1"),
            Some(&context),
            "prompt",
        )
        .await
        .expect_err("replica must skip CallbackResult owner_deployment_id");
    assert!(
        error
            .downcast_ref::<crate::trigger_engine::MaterializeSkip>()
            .is_some(),
        "{error}"
    );
}

#[tokio::test]
async fn materializer_stamps_owner_when_trigger_context_omits_it() {
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    insert_ready_workspace(node.as_ref(), "ws-stamp", "deploy-owner").await;
    let snapshot =
        snapshot_with_behavior_and_schedules(integration_test_behavior("general"), HashMap::new());
    let (_snapshot_tx, snapshot_rx) = watch::channel(snapshot);
    let materializer = ProductionMaterializer::new(node.clone(), snapshot_rx)
        .with_local_deployment_id("deploy-owner");
    let context = serde_json::json!({
        "version": 1,
        "source_fields": {
            "workspace_id": "ws-stamp",
            "workspace_authority": "readWrite"
        }
    })
    .to_string();
    let request_id = materializer
        .materialize(
            &workspace_writer_task(),
            Some("trigger-stamp"),
            TriggerKind::Event,
            Some("src-stamp"),
            Some("corr-stamp"),
            Some(&context),
            "prompt",
        )
        .await
        .expect("owner host stamps IsolatedWorkspace owner");
    let rows = workspace_requests(node.as_ref(), "ws-stamp").await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["request_id"].as_str(), Some(request_id.as_str()));
    assert_eq!(
        rows[0]["workspace_owner_deployment_id"].as_str(),
        Some("deploy-owner")
    );
}

#[tokio::test]
async fn unique_read_write_denial_does_not_leave_claimable_request() {
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    insert_ready_workspace(node.as_ref(), "ws-rw", "deploy-owner").await;
    let snapshot =
        snapshot_with_behavior_and_schedules(integration_test_behavior("general"), HashMap::new());
    let (_snapshot_tx, snapshot_rx) = watch::channel(snapshot);
    let materializer = ProductionMaterializer::new(node.clone(), snapshot_rx)
        .with_local_deployment_id("deploy-owner");
    let context = writer_context("ws-rw", "workspace_owner_deployment_id", "deploy-owner");
    let first = materializer
        .materialize(
            &workspace_writer_task(),
            Some("trigger-rw-1"),
            TriggerKind::Event,
            Some("src-rw-1"),
            Some("corr-rw-1"),
            Some(&context),
            "prompt",
        )
        .await
        .expect("first writer");
    let error = materializer
        .materialize(
            &workspace_writer_task(),
            Some("trigger-rw-2"),
            TriggerKind::Event,
            Some("src-rw-2"),
            Some("corr-rw-2"),
            Some(&context),
            "prompt",
        )
        .await
        .expect_err("second writer must not enqueue");
    assert!(
        error.to_string().contains("unique Active ReadWrite"),
        "{error:#}"
    );
    let rows = workspace_requests(node.as_ref(), "ws-rw").await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["request_id"].as_str(), Some(first.as_str()));
    assert_eq!(rows[0]["lifecycle_state"].as_str(), Some("pending"));
}
