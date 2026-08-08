use super::support::*;
use super::*;

async fn wait_for_request_state(
    node: &defra_node::EmbeddedNode,
    doc_id: &str,
    expected_status: &str,
) {
    let escaped_doc_id = escape_graphql_string(doc_id);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let query = format!(
            r#"{{
                AgentRequest(filter: {{ _docID: {{ _eq: "{escaped_doc_id}" }} }}, limit: 1) {{
                    status
                }}
            }}"#
        );
        let response = node.execute(&query).await;
        assert!(
            !response.has_errors(),
            "AgentRequest query failed: {:?}",
            response.errors
        );
        let row = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(Value::as_array)
            .and_then(|rows| rows.first())
            .cloned()
            .expect("AgentRequest row");
        let status = row
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if status == expected_status {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for AgentRequest {} to reach status={}, last row={:?}",
            doc_id,
            expected_status,
            row
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn fetch_backend_probe_row(
    node: &defra_node::EmbeddedNode,
    backend_id: &str,
) -> (String, Option<String>) {
    let escaped_backend_id = escape_graphql_string(backend_id);
    let query = format!(
        r#"{{
            InferenceBackend(filter: {{ backend_id: {{ _eq: "{escaped_backend_id}" }} }}, limit: 1) {{
                probe_status
                last_probe
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "InferenceBackend probe row query failed: {:?}",
        response.errors
    );
    let row = response
        .data
        .as_ref()
        .and_then(|data| data.get("InferenceBackend"))
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .cloned()
        .expect("InferenceBackend row");
    let probe_status = row
        .get("probe_status")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let last_probe = row
        .get("last_probe")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    (probe_status, last_probe)
}

async fn wait_for_backend_probe_status(
    node: &defra_node::EmbeddedNode,
    backend_id: &str,
    expected_status: &str,
) -> (String, Option<String>) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let row = fetch_backend_probe_row(node, backend_id).await;
        if row.0 == expected_status {
            return row;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for InferenceBackend {backend_id} to reach \
             probe_status={expected_status}, last row={row:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn run_agent_starts_when_startup_probe_cannot_validate_model() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("startup-probe-rejects-model"));
    let mock_endpoint = MockModelEndpoint::start("different-model").unwrap();
    bind_default_behavior_backend_with_capacity_and_probe_status(
        node.as_ref(),
        identity.did(),
        "backend-startup-probe",
        mock_endpoint.endpoint(),
        1,
        "unknown",
    )
    .await;
    let observer = Arc::new(RecordingObserver::default());
    let agent = crate::Gents::from_default_behavior_documents(
        node.clone(),
        identity.clone(),
        crate::agent::DocumentRuntimeOptions {
            tool_ceiling: ToolCeiling::meta_only(),
            process_state_observer: Some(observer.clone()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle = tokio::spawn(agent.run(shutdown_rx));

    wait_for_runtime_process_state(node.as_ref(), identity.did(), "ready").await;
    let status = fetch_runtime_status(node.as_ref(), identity.did()).await;
    assert_eq!(status.process_state, "ready");
    assert_eq!(status.reconcile_phase, "idle");
    assert_eq!(status.active_generation, 1);
    assert_eq!(status.last_reconcile_result, "startup");
    assert!(status.last_reconcile_error.is_empty());
    let (probe_status, last_probe) =
        wait_for_backend_probe_status(node.as_ref(), "backend-startup-probe", "healthy").await;
    assert_eq!(probe_status, "healthy");
    assert!(
        last_probe.is_some(),
        "startup unknown -> healthy promotion must stamp document last_probe"
    );

    let _ = shutdown_tx.send(true);
    handle
        .await
        .expect("agent task should join")
        .expect("agent run should return ok");

    let observed = observer
        .states
        .lock()
        .expect("recording observer mutex poisoned")
        .clone();
    assert_eq!(
        observed,
        vec![
            crate::agent::ProcessLifecycleState::Recovering,
            crate::agent::ProcessLifecycleState::Ready,
            crate::agent::ProcessLifecycleState::ShuttingDown,
            crate::agent::ProcessLifecycleState::Shutdown,
        ]
    );
}

#[tokio::test]
async fn run_agent_fails_when_all_behaviors_are_unavailable_due_to_invalid_config() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("startup-invalid-config"));
    bind_default_behavior_backend(
        node.as_ref(),
        identity.did(),
        "backend-invalid-config",
        "http://127.0.0.1:9/v1",
    )
    .await;
    let default_behavior_id = crate::default_behavior_id_for_agent(identity.did());
    let mut default_behavior = crate::load_agent_behavior(node.as_ref(), &default_behavior_id)
        .await
        .unwrap()
        .expect("default behavior document");
    default_behavior.tool_selection_id = Some("missing-tool-selection".to_string());
    crate::upsert_agent_behavior(node.as_ref(), &default_behavior)
        .await
        .unwrap();

    let agent = crate::Gents::from_default_behavior_documents(
        node.clone(),
        identity.clone(),
        crate::agent::DocumentRuntimeOptions {
            tool_ceiling: ToolCeiling::meta_only(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(agent.behaviors().is_empty());
    assert_eq!(agent.unavailable_behaviors().len(), 1);
    let unavailable_reason = agent
        .unavailable_behaviors()
        .get(&default_behavior_id)
        .expect("default behavior should be unavailable");
    assert!(
        unavailable_reason.contains("references missing tool selection missing-tool-selection"),
        "unexpected unavailable reason: {unavailable_reason}"
    );

    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let error = agent
        .run(shutdown_rx)
        .await
        .expect_err("startup should fail for structurally invalid config");
    let error_text = format!("{error:#}");
    assert!(
        error_text.contains("no runnable behaviors at startup due to invalid configuration"),
        "unexpected startup error: {error_text}"
    );
    assert!(
        error_text.contains("references missing tool selection missing-tool-selection"),
        "unexpected startup error: {error_text}"
    );

    let status = fetch_runtime_status(node.as_ref(), identity.did()).await;
    assert_eq!(status.process_state, "recovering");
    assert_eq!(status.reconcile_phase, "idle");
    assert_eq!(status.active_generation, 0);
    assert_eq!(status.runnable_behavior_count, 0);
    assert_eq!(status.unavailable_behavior_count, 0);
    assert_eq!(status.last_reconcile_result, "error");
    assert!(status
        .last_reconcile_error
        .contains("references missing tool selection missing-tool-selection"));
}

#[tokio::test]
async fn run_agent_starts_with_all_behaviors_unavailable_and_rejects_requests_at_runtime() {
    let identity = Arc::new(test_identity("startup-all-unavailable"));
    let node = test_node_with_identity(identity.as_ref()).await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    bind_default_behavior_backend_with_capacity_and_probe_status(
        node.as_ref(),
        identity.did(),
        "backend-unavailable",
        "http://127.0.0.1:9/v1",
        1,
        "unknown",
    )
    .await;
    let agent = crate::Gents::from_default_behavior_documents(
        node.clone(),
        identity.clone(),
        crate::agent::DocumentRuntimeOptions {
            tool_ceiling: ToolCeiling::meta_only(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(agent.behaviors().is_empty());
    assert_eq!(agent.unavailable_behaviors().len(), 1);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle = tokio::spawn(agent.run(shutdown_rx));

    wait_for_runtime_process_state(node.as_ref(), identity.did(), "ready").await;
    let status = fetch_runtime_status(node.as_ref(), identity.did()).await;
    assert_eq!(status.process_state, "ready");
    assert_eq!(status.reconcile_phase, "idle");
    assert_eq!(status.active_generation, 1);
    assert_eq!(status.runnable_behavior_count, 0);
    assert_eq!(status.unavailable_behavior_count, 1);
    assert_eq!(status.last_reconcile_result, "startup");
    assert!(status.last_reconcile_error.is_empty());

    let request_doc_id = create_agent_request(
        node.as_ref(),
        identity.did(),
        "req-unavailable-runtime",
        "session-unavailable-runtime",
        "hello",
    )
    .await;
    wait_for_request_state(node.as_ref(), &request_doc_id, "error").await;

    let request_query = format!(
        r#"{{
            AgentRequest(filter: {{ _docID: {{ _eq: "{}" }} }}, limit: 1) {{
                failure_reason
            }}
        }}"#,
        escape_graphql_string(&request_doc_id),
    );
    let request_response = node.execute(&request_query).await;
    assert!(
        !request_response.has_errors(),
        "AgentRequest failure query failed: {:?}",
        request_response.errors
    );
    let failure_reason = request_response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("failure_reason"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    assert!(
        failure_reason.contains("backend backend-unavailable is unavailable"),
        "unexpected failure reason: {failure_reason}"
    );

    let _ = shutdown_tx.send(true);
    handle
        .await
        .expect("agent task should join")
        .expect("agent run should return ok");
}

#[tokio::test]
async fn run_agent_recovers_backend_availability_without_restart() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("startup-backend-recovers"));
    bind_default_behavior_backend_with_capacity_and_probe_status(
        node.as_ref(),
        identity.did(),
        "backend-recovers",
        "http://127.0.0.1:9/v1",
        1,
        "unknown",
    )
    .await;
    let agent = crate::Gents::from_default_behavior_documents(
        node.clone(),
        identity.clone(),
        crate::agent::DocumentRuntimeOptions {
            tool_ceiling: ToolCeiling::meta_only(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(agent.behaviors().is_empty());
    assert_eq!(agent.unavailable_behaviors().len(), 1);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle = tokio::spawn(agent.run(shutdown_rx));

    wait_for_runtime_process_state(node.as_ref(), identity.did(), "ready").await;
    let startup_status = fetch_runtime_status(node.as_ref(), identity.did()).await;
    assert_eq!(startup_status.active_generation, 1);
    assert_eq!(startup_status.runnable_behavior_count, 0);
    assert_eq!(startup_status.unavailable_behavior_count, 1);
    assert_eq!(startup_status.last_reconcile_result, "startup");

    update_backend_probe_status(node.as_ref(), "backend-recovers", "healthy").await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        let status = fetch_runtime_status(node.as_ref(), identity.did()).await;
        if status.process_state == "ready"
            && status.active_generation >= 2
            && status.runnable_behavior_count == 1
            && status.unavailable_behavior_count == 0
            && status.last_reconcile_result == "applied"
            && status.last_reconcile_error.is_empty()
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for runtime to recover backend availability; last status: {:?}",
            status
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let _ = shutdown_tx.send(true);
    handle
        .await
        .expect("agent task should join")
        .expect("agent run should return ok");
}

#[tokio::test]
async fn run_agent_shutdown_is_prompt_while_request_waits_for_backend_capacity() {
    let identity = Arc::new(test_identity("shutdown-waiting-request"));
    let node = test_node_with_identity(identity.as_ref()).await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let mock_endpoint = MockModelEndpoint::start_blocking_chat("default").unwrap();
    bind_default_behavior_backend_with_capacity(
        node.as_ref(),
        identity.did(),
        "backend-blocked",
        mock_endpoint.endpoint(),
        1,
    )
    .await;
    let agent = crate::Gents::builder()
        .node(node.clone())
        .identity(identity.clone())
        .default_behavior_id("general")
        .tool_ceiling(ToolCeiling::meta_only())
        .behavior("general")
        .backend_id("backend-blocked")
        .model_name("default")
        .done()
        .behavior("code")
        .backend_id("backend-blocked")
        .model_name("default")
        .done()
        .build()
        .await
        .unwrap();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle = tokio::spawn(agent.run(shutdown_rx));

    wait_for_runtime_process_state(node.as_ref(), identity.did(), "ready").await;

    let first_request_doc_id = create_agent_request_for_behavior(
        node.as_ref(),
        identity.did(),
        Some("general"),
        "req-shutdown-running",
        "session-shutdown-running",
        "hello",
    )
    .await;
    wait_for_request_state(node.as_ref(), &first_request_doc_id, "processing").await;
    // The property under test starts when the first request owns the sole
    // backend permit. Waiting for the mock server to observe bytes adds the
    // unrelated synchronous capture write and HTTP scheduler to test setup;
    // under the full parallel suite that made this capacity test time out
    // before it reached its actual assertion (#1060).
    wait_for_inference_call_state(node.as_ref(), "req-shutdown-running", "running").await;

    let queued_request_doc_id = create_agent_request_for_behavior(
        node.as_ref(),
        identity.did(),
        Some("code"),
        "req-shutdown-waiting",
        "session-shutdown-waiting",
        "hello",
    )
    .await;
    wait_for_request_state(node.as_ref(), &queued_request_doc_id, "processing").await;
    wait_for_inference_call_state(node.as_ref(), "req-shutdown-waiting", "queued").await;

    let _ = shutdown_tx.send(true);
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("agent shutdown should not wait for backend deadline")
        .expect("agent task should join")
        .expect("agent run should return ok");

    wait_for_request_state(node.as_ref(), &first_request_doc_id, "error").await;
    wait_for_request_state(node.as_ref(), &queued_request_doc_id, "error").await;
}
