use super::*;

fn root_parent(session_id: &str) -> AgentRequest {
    let mut parent = parent_request(session_id);
    parent.subagent_depth = 0;
    parent.caused_by_parent_request_id = None;
    parent.caused_by_parent_request_doc_id = None;
    parent.caused_by_parent_tool_call_id = None;
    parent.caused_by_parent_tool_call_doc_id = None;
    parent
}

fn background_hints(parent: &AgentRequest) -> QueueHints {
    QueueHints {
        source: QueueSource::BackgroundCompletion,
        policy: QueuePolicy::Coalesce,
        key: Some(format!("background_completion:{}", parent.session_id)),
        queued_after_request_id: Some(parent.request_id.clone()),
        interrupted_request_id: None,
    }
}

fn wake_agent_request(
    parent: &AgentRequest,
    doc_id: &str,
    request_id: &str,
    hints: &QueueHints,
) -> AgentRequest {
    AgentRequest {
        doc_id: doc_id.to_string(),
        request_id: request_id.to_string(),
        agent_did: parent.agent_did.clone(),
        requester_did: parent.requester_did.clone(),
        behavior_id: parent.behavior_id.clone(),
        session_id: parent.session_id.clone(),
        content: "review notifications".to_string(),
        temperature: None,
        top_p: None,
        top_k: None,
        seed: None,
        max_tokens: None,
        max_total_tokens: None,
        metadata: Some(queue_metadata_json(hints)),
        execution_origin: Some("scheduled".to_string()),
        caused_by_correlation: None,
        caused_by_trigger_context: None,
        created_at: chrono::Utc::now().to_rfc3339(),
        deadline: None,
        subagent_depth: 0,
        caused_by_parent_request_id: Some(parent.request_id.clone()),
        caused_by_parent_request_doc_id: Some(parent.doc_id.clone()),
        caused_by_parent_tool_call_id: None,
        caused_by_parent_tool_call_doc_id: None,
        caused_by_trigger_id: None,
        caused_by_trigger_kind: None,
        caused_by_source_doc_id: None,
        workspace_id: None,
        workspace_authority: None,
        workspace_owner_deployment_id: None,
        workspace_seal_hash: None,
    }
}

#[tokio::test]
async fn notification_is_atomically_bound_to_coalesced_wake() {
    let db = test_db("atomic-background-notification").await;
    let parent = root_parent("atomic-background-session");
    let first = enqueue_background_completion_with_message(
        &db.node,
        &parent,
        "first notification",
        "background-completion-notification:first:tool",
        "review notifications",
        background_hints(&parent),
    )
    .await
    .unwrap();
    let second = enqueue_background_completion_with_message(
        &db.node,
        &parent,
        "second notification",
        "background-completion-notification:second:tool",
        "review notifications",
        background_hints(&parent),
    )
    .await
    .unwrap();
    assert!(first.created_request);
    assert!(!second.created_request);
    assert_eq!(first.request.doc_id, second.request.doc_id);

    let wake_query = format!(
        r#"{{
            AgentRequest(filter: {{ _docID: {{ _eq: "{}" }} }}) {{
                caused_by_parent_request_id
                caused_by_parent_request_doc_id
            }}
        }}"#,
        escape_graphql_string(&first.request.doc_id)
    );
    let wake_response = db.node.execute(&wake_query).await;
    assert!(
        !wake_response.has_errors(),
        "wake query: {:?}",
        wake_response.errors
    );
    let wake = &wake_response.data.as_ref().unwrap()["AgentRequest"][0];
    assert_eq!(
        wake["caused_by_parent_request_id"].as_str(),
        Some(parent.request_id.as_str())
    );
    assert_eq!(
        wake["caused_by_parent_request_doc_id"].as_str(),
        Some(parent.doc_id.as_str())
    );

    let query = format!(
        r#"{{
            AgentMessage(
                filter: {{ session_id: {{ _eq: "{}" }} }},
                order: {{ sequence: ASC }}
            ) {{ request_id request_doc_id }}
        }}"#,
        escape_graphql_string(&parent.session_id)
    );
    let response = db.node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "message query: {:?}",
        response.errors
    );
    let rows = response.data.as_ref().unwrap()["AgentMessage"]
        .as_array()
        .unwrap();
    assert_eq!(rows.len(), 2);
    for row in rows {
        assert_eq!(
            row["request_id"].as_str(),
            Some(first.request.request_id.as_str())
        );
        assert_eq!(
            row["request_doc_id"].as_str(),
            Some(first.request.doc_id.as_str())
        );
    }
}

#[tokio::test]
async fn concurrent_notifications_converge_to_one_pending_wake() {
    let db = test_db("atomic-background-race").await;
    let parent = root_parent("atomic-background-race-session");
    let first = enqueue_background_completion_with_message(
        &db.node,
        &parent,
        "first concurrent notification",
        "background-completion-notification:race-first:tool",
        "review notifications",
        background_hints(&parent),
    );
    let second = enqueue_background_completion_with_message(
        &db.node,
        &parent,
        "second concurrent notification",
        "background-completion-notification:race-second:tool",
        "review notifications",
        background_hints(&parent),
    );
    let (first, second) = tokio::join!(first, second);
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first.request.doc_id, second.request.doc_id);
    assert_eq!(
        [first.created_request, second.created_request]
            .into_iter()
            .filter(|created| *created)
            .count(),
        1
    );

    let request_query = format!(
        r#"{{
            AgentRequest(filter: {{ session_id: {{ _eq: "{}" }} }}) {{
                _docID request_id status lifecycle_state
            }}
        }}"#,
        escape_graphql_string(&parent.session_id)
    );
    let request_response = db.node.execute(&request_query).await;
    assert!(
        !request_response.has_errors(),
        "pending wake query: {:?}",
        request_response.errors
    );
    let requests = request_response.data.as_ref().unwrap()["AgentRequest"]
        .as_array()
        .unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|row| row["status"] == "pending" && row["lifecycle_state"] == "pending")
            .count(),
        1
    );

    let message_query = format!(
        r#"{{
            AgentMessage(
                filter: {{ session_id: {{ _eq: "{}" }} }},
                order: {{ sequence: ASC }}
            ) {{ request_id request_doc_id }}
        }}"#,
        escape_graphql_string(&parent.session_id)
    );
    let message_response = db.node.execute(&message_query).await;
    assert!(
        !message_response.has_errors(),
        "message query: {:?}",
        message_response.errors
    );
    let rows = message_response.data.as_ref().unwrap()["AgentMessage"]
        .as_array()
        .unwrap();
    assert_eq!(rows.len(), 2);
    let actual = rows
        .iter()
        .map(|row| {
            (
                row["request_id"].as_str().unwrap().to_string(),
                row["request_doc_id"].as_str().unwrap().to_string(),
            )
        })
        .collect::<std::collections::BTreeSet<_>>();
    let persisted_bindings = requests
        .into_iter()
        .map(|row| {
            (
                row["request_id"].as_str().unwrap().to_string(),
                row["_docID"].as_str().unwrap().to_string(),
            )
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert!(actual.is_subset(&persisted_bindings));
}

#[tokio::test]
async fn restart_before_claim_preserves_pending_input_until_the_wake_completes() {
    let TestDb { node, _tempdir } = test_db("background-restart-before-claim").await;
    let node = std::sync::Arc::new(node);
    let parent = root_parent("background-restart-before-claim-session");
    let hints = background_hints(&parent);
    let enqueued = enqueue_background_completion_with_message(
        node.as_ref(),
        &parent,
        "restart-safe notification",
        "background-completion-notification:restart-before-claim:tool",
        "review notifications",
        hints.clone(),
    )
    .await
    .unwrap();

    let recovery = crate::RequestLifecycle::recover_all(node.as_ref(), TEST_AGENT_DID)
        .await
        .unwrap();
    assert_eq!(recovery.background_wakes_redriven, 0);
    let before = crate::load_background_completion_diagnostics(
        &crate::config_client::ConfigAccess::Local(node.clone()),
        TEST_AGENT_DID,
    )
    .await
    .unwrap();
    assert_eq!(before.pending_notifications, 1);
    assert_eq!(before.acknowledged_notifications, 0);
    assert_eq!(before.epochs[0].state, "pending");

    let request = wake_agent_request(
        &parent,
        &enqueued.request.doc_id,
        &enqueued.request.request_id,
        &hints,
    );
    let mut lifecycle = crate::RequestLifecycle::new_with_execution_binding(
        node.clone(),
        TEST_BEHAVIOR_ID,
        TEST_AGENT_DID,
        request,
        60,
        ExecutionOrigin::Scheduled,
        "backend-test",
    );
    assert_eq!(
        lifecycle.claim_with_identity().await.unwrap(),
        crate::lifecycle::ClaimOutcome::Claimed
    );
    lifecycle.complete().await.unwrap();

    let after = crate::load_background_completion_diagnostics(
        &crate::config_client::ConfigAccess::Local(node),
        TEST_AGENT_DID,
    )
    .await
    .unwrap();
    assert_eq!(after.pending_notifications, 0);
    assert_eq!(after.acknowledged_notifications, 1);
    assert_eq!(after.epochs[0].state, "acknowledged");
}

#[tokio::test]
async fn persisted_response_repair_makes_acknowledgement_restart_atomic() {
    let TestDb { node, _tempdir } = test_db("background-response-repair-ack").await;
    let node = std::sync::Arc::new(node);
    let parent = root_parent("background-response-repair-ack-session");
    let hints = background_hints(&parent);
    let enqueued = enqueue_background_completion_with_message(
        node.as_ref(),
        &parent,
        "response-persisted notification",
        "background-completion-notification:response-persisted:tool",
        "review notifications",
        hints.clone(),
    )
    .await
    .unwrap();
    let request = wake_agent_request(
        &parent,
        &enqueued.request.doc_id,
        &enqueued.request.request_id,
        &hints,
    );
    let mut lifecycle = crate::RequestLifecycle::new_with_execution_binding(
        node.clone(),
        TEST_BEHAVIOR_ID,
        TEST_AGENT_DID,
        request,
        60,
        ExecutionOrigin::Scheduled,
        "backend-test",
    );
    assert_eq!(
        lifecycle.claim_with_identity().await.unwrap(),
        crate::lifecycle::ClaimOutcome::Claimed
    );
    lifecycle.begin_execution().await.unwrap();

    let response = node
        .execute(&format!(
            r#"mutation {{
                create_AgentResponse(input: {{
                    response_key: "{}", request_id: "{}", request_doc_id: "{}",
                    agent_did: "{TEST_AGENT_DID}", behavior_id: "{TEST_BEHAVIOR_ID}",
                    session_id: "{}", content: "integrated notification",
                    status: "complete", token_count: 1, progress_seq: 1,
                    created_at: "2026-08-12T00:00:00Z",
                    completed_at: "2026-08-12T00:00:01Z"
                }}) {{ _docID }}
            }}"#,
            escape_graphql_string(&enqueued.request.request_id),
            escape_graphql_string(&enqueued.request.request_id),
            escape_graphql_string(&enqueued.request.doc_id),
            escape_graphql_string(&parent.session_id),
        ))
        .await;
    assert!(
        !response.has_errors(),
        "persist terminal response: {:?}",
        response.errors
    );

    let first_repair =
        crate::RequestLifecycle::repair_terminal_requests(node.as_ref(), TEST_AGENT_DID)
            .await
            .unwrap();
    assert_eq!(first_repair.repaired, 1);
    let first = crate::load_background_completion_diagnostics(
        &crate::config_client::ConfigAccess::Local(node.clone()),
        TEST_AGENT_DID,
    )
    .await
    .unwrap();
    assert_eq!(first.pending_notifications, 0);
    assert_eq!(first.acknowledged_notifications, 1);
    assert_eq!(first.epochs[0].state, "acknowledged");

    let second_repair =
        crate::RequestLifecycle::repair_terminal_requests(node.as_ref(), TEST_AGENT_DID)
            .await
            .unwrap();
    assert_eq!(second_repair.repaired, 0);
    let second = crate::load_background_completion_diagnostics(
        &crate::config_client::ConfigAccess::Local(node),
        TEST_AGENT_DID,
    )
    .await
    .unwrap();
    assert_eq!(
        second, first,
        "acknowledgement projection must be restart-idempotent"
    );
}

#[tokio::test]
async fn successor_acknowledges_input_left_by_a_failed_active_wake() {
    let TestDb { node, _tempdir } = test_db("background-successor-ack").await;
    let node = std::sync::Arc::new(node);
    let parent = root_parent("background-successor-ack-session");
    let hints = background_hints(&parent);
    let first = enqueue_background_completion_with_message(
        node.as_ref(),
        &parent,
        "first notification",
        "background-completion-notification:successor-first:tool",
        "review notifications",
        hints.clone(),
    )
    .await
    .unwrap();
    let first_request = wake_agent_request(
        &parent,
        &first.request.doc_id,
        &first.request.request_id,
        &hints,
    );
    let mut first_lifecycle = crate::RequestLifecycle::new_with_execution_binding(
        node.clone(),
        TEST_BEHAVIOR_ID,
        TEST_AGENT_DID,
        first_request,
        60,
        ExecutionOrigin::Scheduled,
        "backend-test",
    );
    assert_eq!(
        first_lifecycle.claim_with_identity().await.unwrap(),
        crate::lifecycle::ClaimOutcome::Claimed
    );

    let second = enqueue_background_completion_with_message(
        node.as_ref(),
        &parent,
        "second notification",
        "background-completion-notification:successor-second:tool",
        "review notifications",
        hints.clone(),
    )
    .await
    .unwrap();
    assert!(second.created_request);
    assert_ne!(first.request.doc_id, second.request.doc_id);
    first_lifecycle
        .fail_with_reason("injected provider failure")
        .await
        .unwrap();

    let second_request = wake_agent_request(
        &parent,
        &second.request.doc_id,
        &second.request.request_id,
        &hints,
    );
    let mut second_lifecycle = crate::RequestLifecycle::new_with_execution_binding(
        node.clone(),
        TEST_BEHAVIOR_ID,
        TEST_AGENT_DID,
        second_request,
        60,
        ExecutionOrigin::Scheduled,
        "backend-test",
    );
    assert_eq!(
        second_lifecycle.claim_with_identity().await.unwrap(),
        crate::lifecycle::ClaimOutcome::Claimed
    );
    second_lifecycle.complete().await.unwrap();

    let access = crate::config_client::ConfigAccess::Local(node);
    let diagnostics = crate::load_background_completion_diagnostics(&access, TEST_AGENT_DID)
        .await
        .unwrap();
    assert_eq!(diagnostics.pending_notifications, 0);
    assert_eq!(diagnostics.acknowledged_notifications, 2);
    assert_eq!(diagnostics.stranded_notifications, 0);
    let first_epoch = diagnostics
        .epochs
        .iter()
        .find(|epoch| epoch.root_request_id == first.request.request_id)
        .unwrap();
    assert_eq!(first_epoch.state, "acknowledged_by_successor");
    let timeline = crate::run_timeline_fetch::load_run_timeline(&access, &first.request.request_id)
        .await
        .unwrap();
    assert_eq!(timeline.background_completions.len(), 2);
    assert!(timeline.background_completion_diagnostics_error.is_none());
    assert!(timeline.descendant_edges.is_empty());
    assert!(timeline
        .descendant_graph_diagnostics_error
        .as_deref()
        .is_some_and(|error| error.contains("points to missing parent")));
}
