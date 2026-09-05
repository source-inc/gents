use super::*;
use crate::lifecycle::RequestTerminalOutcome;

fn root_parent(agent_did: &str, session_id: &str) -> AgentRequest {
    let mut parent = parent_request(agent_did, session_id);
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
        created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        deadline: None,
        execution_generation: None,
        execution_lease_expires_at: None,
        execution_progress_seq: 0,
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
    let parent = root_parent(db.agent_did(), "atomic-background-session");
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
    let parent = root_parent(db.agent_did(), "atomic-background-race-session");
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
                _docID request_id lifecycle_state
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
        requests.len(),
        1,
        "the collision key must prevent duplicate wakes"
    );
    assert_eq!(
        requests
            .iter()
            .filter(|row| row["lifecycle_state"] == "pending")
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
    assert_eq!(actual, persisted_bindings);
}

#[tokio::test]
async fn duplicate_notification_key_recovers_its_original_wake_binding() {
    let db = test_db("atomic-background-idempotent-notification").await;
    let parent = root_parent(
        db.agent_did(),
        "atomic-background-idempotent-notification-session",
    );
    let message_key = "background-completion-notification:idempotent:tool";
    let first = enqueue_background_completion_with_message(
        &db.node,
        &parent,
        "idempotent notification",
        message_key,
        "review notifications",
        background_hints(&parent),
    )
    .await
    .unwrap();
    let retry = enqueue_background_completion_with_message(
        &db.node,
        &parent,
        "idempotent notification",
        message_key,
        "review notifications",
        background_hints(&parent),
    )
    .await
    .unwrap();

    assert_eq!(retry.request.doc_id, first.request.doc_id);
    assert_eq!(retry.request.request_id, first.request.request_id);
    assert_eq!(retry.request.session_id, first.request.session_id);
    assert_eq!(retry.message_sequence, first.message_sequence);
    assert!(!retry.created_request);

    let conflict = enqueue_background_completion_with_message(
        &db.node,
        &parent,
        "changed notification",
        message_key,
        "review notifications",
        background_hints(&parent),
    )
    .await
    .unwrap_err();
    assert!(
        conflict
            .to_string()
            .contains("conflicts with its persisted binding"),
        "unexpected conflict: {conflict:#}"
    );
}

#[tokio::test]
async fn many_concurrent_notifications_publish_one_wake_identity() {
    let db = test_db("atomic-background-many-race").await;
    let parent = root_parent(db.agent_did(), "atomic-background-many-race-session");
    let enqueued = futures::future::join_all((0..16).map(|index| {
        let node = db.node.clone();
        let parent = parent.clone();
        async move {
            enqueue_background_completion_with_message(
                node.as_ref(),
                &parent,
                &format!("concurrent notification {index}"),
                &format!("background-completion-notification:many-race-{index}:tool"),
                "review notifications",
                background_hints(&parent),
            )
            .await
            .unwrap()
        }
    }))
    .await;

    let wake_doc_id = &enqueued[0].request.doc_id;
    assert!(enqueued
        .iter()
        .all(|result| result.request.doc_id == *wake_doc_id));
    assert_eq!(
        enqueued
            .iter()
            .filter(|result| result.created_request)
            .count(),
        1
    );
}

#[tokio::test]
async fn restart_before_claim_preserves_pending_input_until_the_wake_completes() {
    let db = test_db("background-restart-before-claim").await;
    let node = db.node.clone();
    let parent = root_parent(db.agent_did(), "background-restart-before-claim-session");
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

    let recovery = crate::RequestLifecycle::recover_all(node.as_ref(), db.agent_did())
        .await
        .unwrap();
    assert_eq!(recovery.background_wakes_redriven, 0);
    let before = crate::load_background_completion_diagnostics(
        &crate::config_client::ConfigAccess::Local(node.clone()),
        db.agent_did(),
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
        db.agent_did(),
        request,
        60,
        ExecutionOrigin::Scheduled,
        "backend-test",
    );
    assert_eq!(
        lifecycle.claim_with_identity().await.unwrap(),
        crate::lifecycle::ClaimOutcome::Claimed
    );
    let writer = crate::streaming::DefraStreamWriter::new(
        node.clone(),
        db.agent_did(),
        std::time::Duration::ZERO,
    );
    lifecycle.begin_owned_execution(&writer).await.unwrap();
    lifecycle
        .terminalize_owned_without_stream(RequestTerminalOutcome::Completed, None)
        .await
        .unwrap();

    let after = crate::load_background_completion_diagnostics(
        &crate::config_client::ConfigAccess::Local(node),
        db.agent_did(),
    )
    .await
    .unwrap();
    assert_eq!(after.pending_notifications, 0);
    assert_eq!(after.acknowledged_notifications, 1);
    assert_eq!(after.epochs[0].state, "acknowledged");
}

#[tokio::test]
async fn persisted_response_repair_makes_acknowledgement_restart_atomic() {
    let db = test_db("background-response-repair-ack").await;
    let node = db.node.clone();
    let parent = root_parent(db.agent_did(), "background-response-repair-ack-session");
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
        db.agent_did(),
        request,
        60,
        ExecutionOrigin::Scheduled,
        "backend-test",
    );
    assert_eq!(
        lifecycle.claim_with_identity().await.unwrap(),
        crate::lifecycle::ClaimOutcome::Claimed
    );
    let writer = crate::streaming::DefraStreamWriter::new(
        node.clone(),
        db.agent_did(),
        std::time::Duration::ZERO,
    );
    let response_doc_id = lifecycle.begin_owned_execution(&writer).await.unwrap();

    // Simulate an interrupted older terminal write: the response committed,
    // while the owning request remains processing with an expired lease.
    let response = node
        .execute(&format!(
            r#"mutation {{
        update_AgentResponse(filter: {{ _docID: {{ _eq: "{}" }} }}, input: {{
            content: "integrated notification", status: "complete", token_count: 1,
            progress_seq: 1, completed_at: "2026-08-12T00:00:01Z"
        }}) {{ _docID }}
        update_AgentRequest(filter: {{ _docID: {{ _eq: "{}" }} }}, input: {{
            execution_lease_expires_at: "2000-01-01T00:00:00Z"
        }}) {{ _docID }}
    }}"#,
            escape_graphql_string(&response_doc_id),
            escape_graphql_string(&enqueued.request.doc_id)
        ))
        .await;
    assert!(
        !response.has_errors(),
        "persist interrupted terminal write: {:?}",
        response.errors
    );

    let first_repair =
        crate::RequestLifecycle::repair_terminal_requests(node.as_ref(), db.agent_did())
            .await
            .unwrap();
    assert_eq!(first_repair.repaired, 1);
    let first = crate::load_background_completion_diagnostics(
        &crate::config_client::ConfigAccess::Local(node.clone()),
        db.agent_did(),
    )
    .await
    .unwrap();
    assert_eq!(first.pending_notifications, 0);
    assert_eq!(first.acknowledged_notifications, 1);
    assert_eq!(first.epochs[0].state, "acknowledged");

    let second_repair =
        crate::RequestLifecycle::repair_terminal_requests(node.as_ref(), db.agent_did())
            .await
            .unwrap();
    assert_eq!(second_repair.repaired, 0);
    let second = crate::load_background_completion_diagnostics(
        &crate::config_client::ConfigAccess::Local(node),
        db.agent_did(),
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
    let db = test_db("background-successor-ack").await;
    let node = db.node.clone();
    let parent = root_parent(db.agent_did(), "background-successor-ack-session");
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
        db.agent_did(),
        first_request,
        60,
        ExecutionOrigin::Scheduled,
        "backend-test",
    );
    assert_eq!(
        first_lifecycle.claim_with_identity().await.unwrap(),
        crate::lifecycle::ClaimOutcome::Claimed
    );
    crate::session::append_message_with_requester_did(
        node.as_ref(),
        &parent.session_id,
        &parent.agent_did,
        parent.requester_did.as_deref(),
        "user",
        "unrelated foreground input",
        None,
        None,
        None,
    )
    .await
    .unwrap();

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
    let generation_query = format!(
        r#"{{
            AgentRequest(filter: {{ session_id: {{ _eq: "{}" }} }}) {{ retry_key }}
        }}"#,
        escape_graphql_string(&parent.session_id)
    );
    let generation_response = node.execute(&generation_query).await;
    assert!(
        !generation_response.has_errors(),
        "generation query: {:?}",
        generation_response.errors
    );
    let mut retry_keys = generation_response.data.as_ref().unwrap()["AgentRequest"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|row| row["retry_key"].as_str())
        .filter(|key| key.starts_with("background-completion:"))
        .collect::<Vec<_>>();
    retry_keys.sort_unstable();
    assert_eq!(retry_keys.len(), 2);
    assert!(retry_keys[0].ends_with(":00000000000000000000"));
    assert!(retry_keys[1].ends_with(":00000000000000000001"));
    assert_eq!(
        retry_keys[0].rsplit_once(':').unwrap().0,
        retry_keys[1].rsplit_once(':').unwrap().0,
        "unrelated transcript writes must not advance the queue-local generation"
    );
    first_lifecycle
        .terminalize_owned_without_stream(
            RequestTerminalOutcome::Failed,
            Some("injected provider failure"),
        )
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
        db.agent_did(),
        second_request,
        60,
        ExecutionOrigin::Scheduled,
        "backend-test",
    );
    assert_eq!(
        second_lifecycle.claim_with_identity().await.unwrap(),
        crate::lifecycle::ClaimOutcome::Claimed
    );
    let writer = crate::streaming::DefraStreamWriter::new(
        node.clone(),
        db.agent_did(),
        std::time::Duration::ZERO,
    );
    second_lifecycle
        .begin_owned_execution(&writer)
        .await
        .unwrap();
    second_lifecycle
        .terminalize_owned_without_stream(RequestTerminalOutcome::Completed, None)
        .await
        .unwrap();

    let access = crate::config_client::ConfigAccess::Local(node);
    let diagnostics = crate::load_background_completion_diagnostics(&access, db.agent_did())
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
