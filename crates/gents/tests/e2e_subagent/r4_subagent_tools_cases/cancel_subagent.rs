use super::*;

#[tokio::test]
async fn cancel_subagent_cancels_bridge_active_descendants_and_owned_queue() {
    let fixture =
        setup_spawn_fixture("cancel_subagent_active", vec![CHILD_BEHAVIOR_ID], 0, true).await;
    let db = &fixture.db;
    let hook = fixture.hook.clone();
    let session_id = fixture.session_id.clone();
    let parent_deadline = fixture.parent_deadline;
    let agent_did = fixture.agent_did.clone();
    let spawn_args = json!({
        "name": CHILD_BEHAVIOR_ID,
        "prompt": "background child for cancel_subagent",
        "await_mode": "background"
    })
    .to_string();

    let spawn_action = hook
        .on_tool_call(
            "spawn_subagent",
            Some("model-call-cancel-spawn".to_string()),
            "internal-cancel-spawn",
            &spawn_args,
        )
        .await;
    let spawn_receipt = skip_reason_json(spawn_action);
    let child_request_id = spawn_receipt["child_request_id"]
        .as_str()
        .expect("child_request_id")
        .to_string();
    let child_session_id = wait_for_child_session_id(db.node.as_ref(), &child_request_id).await;
    update_request_state(
        db.node.as_ref(),
        &child_request_id,
        "processing",
        "processing",
    )
    .await;

    let automated_request_id = "cancel-subagent-active-auto-queue";
    create_child_session_queued_request(
        db.node.as_ref(),
        &agent_did,
        automated_request_id,
        &child_session_id,
        "scheduled",
        &queue_metadata(
            "background_completion",
            "coalesce",
            Some("background_completion:cancel-subagent-active"),
            Some(&child_request_id),
        ),
    )
    .await;
    let steering_request_id = "cancel-subagent-active-steering-queue";
    create_child_session_queued_request(
        db.node.as_ref(),
        &agent_did,
        steering_request_id,
        &child_session_id,
        "interactive",
        &queue_metadata("steering", "append", None, Some(&child_request_id)),
    )
    .await;
    let user_request_id = "cancel-subagent-active-user-queue";
    create_child_session_queued_request(
        db.node.as_ref(),
        &agent_did,
        user_request_id,
        &child_session_id,
        "interactive",
        &queue_metadata("user", "append", None, Some(&child_request_id)),
    )
    .await;

    let grandchild_request_id = "cancel-subagent-active-grandchild";
    let mut descendant_bridge = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        child_request_id.clone(),
        child_session_id.clone(),
        "did:test:test".to_string(),
        "internal-cancel-descendant".to_string(),
        1,
        "spawn_subagent".to_string(),
        "{}".to_string(),
        parent_deadline,
        AwaitMode::Background,
        CancelPolicy::Cascade,
        grandchild_request_id.to_string(),
        agent_did.clone(),
    );
    descendant_bridge.start_running().await.unwrap();
    let _grandchild_session_id = create_subagent_request_with_request_id_for_test(
        db.node.as_ref(),
        grandchild_request_id.to_string(),
        child_request_id.clone(),
        "internal-cancel-descendant".to_string(),
        1,
        agent_did.clone(),
        CHILD_BEHAVIOR_ID.to_string(),
        "grandchild prompt".to_string(),
        Some(parent_deadline - chrono::Duration::minutes(1)),
    )
    .await
    .unwrap();

    let collision_action = hook
        .on_tool_call(
            "bash",
            None,
            "internal-cancel-descendant",
            "{\"cmd\":\"still running\"}",
        )
        .await;
    assert!(matches!(collision_action, ToolCallHookAction::Continue));

    let cancel_args = json!({
        "child_request_id": child_request_id.clone(),
        "reason": "parent no longer needs this work"
    })
    .to_string();
    let action = hook
        .on_tool_call(
            "cancel_subagent",
            Some("model-call-cancel".to_string()),
            "internal-cancel-tool",
            &cancel_args,
        )
        .await;
    let result = skip_reason_json(action);
    assert_eq!(result["ok"], true);
    assert_eq!(result["status"], "cancelled");
    assert_eq!(result["child_request_id"], child_request_id);
    assert_eq!(result["child_session_id"], child_session_id);
    assert_eq!(result["active_interrupted"], true);
    assert_eq!(result["descendants_cancelled"], 1);
    assert_eq!(result["queued_drained"], 2);

    let root_bridge = fetch_tool_call(db.node.as_ref(), &session_id, "internal-cancel-spawn").await;
    assert_eq!(root_bridge.lifecycle_state.as_deref(), Some("cancelled"));
    assert_eq!(root_bridge.cancel_cause.as_deref(), Some("userCancelled"));
    let descendant = fetch_tool_call(
        db.node.as_ref(),
        &child_session_id,
        "internal-cancel-descendant",
    )
    .await;
    assert_eq!(descendant.lifecycle_state.as_deref(), Some("cancelled"));
    assert_eq!(descendant.cancel_cause.as_deref(), Some("userCancelled"));
    let parent_collision =
        fetch_tool_call(db.node.as_ref(), &session_id, "internal-cancel-descendant").await;
    assert_eq!(
        parent_collision.lifecycle_state.as_deref(),
        Some("running"),
        "descendant cancellation must not consume same-id parent-session lifecycle state"
    );
    assert!(
        fetch_interrupt_requested_at(db.node.as_ref(), &child_request_id)
            .await
            .unwrap()
            .is_some(),
        "cancel_subagent should interrupt the child request"
    );
    assert!(
        fetch_interrupt_requested_at(db.node.as_ref(), grandchild_request_id)
            .await
            .unwrap()
            .is_some(),
        "cancel_subagent should cascade to live descendant subagents"
    );

    let automated = fetch_child_request(db.node.as_ref(), automated_request_id).await;
    assert_eq!(automated.status.as_deref(), Some("interrupted"));
    assert_eq!(automated.lifecycle_state.as_deref(), Some("interrupted"));
    assert!(automated
        .failure_reason
        .as_deref()
        .is_some_and(|reason| reason.contains("parent no longer needs this work")));
    let steering = fetch_child_request(db.node.as_ref(), steering_request_id).await;
    assert_eq!(steering.status.as_deref(), Some("interrupted"));
    assert_eq!(steering.lifecycle_state.as_deref(), Some("interrupted"));
    let user = fetch_child_request(db.node.as_ref(), user_request_id).await;
    assert_eq!(user.status.as_deref(), Some("pending"));
    assert_eq!(user.lifecycle_state.as_deref(), Some("pending"));
    assert_eq!(
        count_tool_calls_by_name(db.node.as_ref(), &session_id, "cancel_subagent").await,
        0
    );
}

#[tokio::test]
async fn cancel_subagent_rejects_unlinked_child_without_lifecycle_row() {
    let fixture = setup_spawn_fixture(
        "cancel_subagent_unlinked_child",
        vec![CHILD_BEHAVIOR_ID],
        0,
        true,
    )
    .await;
    let db = &fixture.db;
    let hook = fixture.hook.clone();
    let session_id = fixture.session_id.clone();
    let cancel_args = json!({ "child_request_id": "not-this-parents-child" }).to_string();

    let action = hook
        .on_tool_call(
            "cancel_subagent",
            Some("model-call-cancel-denied".to_string()),
            "internal-cancel-denied",
            &cancel_args,
        )
        .await;
    let error = skip_reason_json(action);
    assert_eq!(error["ok"], false);
    assert_eq!(error["failure_class"], "service_unavailable");
    assert_eq!(error["tool_name"], "cancel_subagent");
    assert_eq!(error["path"], "/child_request_id");
    assert_eq!(
        count_tool_calls_by_name(db.node.as_ref(), &session_id, "cancel_subagent").await,
        0
    );
}

#[tokio::test]
async fn cancel_subagent_explains_unmaterialized_child_bridge() {
    let fixture = setup_spawn_fixture(
        "cancel_subagent_unmaterialized",
        vec![CHILD_BEHAVIOR_ID],
        0,
        true,
    )
    .await;
    let db = &fixture.db;
    let hook = fixture.hook.clone();

    let child_request_id = "cancel-unmat-child";
    let bridge_tool_call_id = "cancel-unmat-bridge";
    let args = json!({
        "name": "remote-coder",
        "agent_did": "did:key:z6MkRemoteUnclaimed",
        "behavior_id": "remote-coder-behavior",
        "prompt": "cross-deployment work",
        "await_mode": "background"
    })
    .to_string();
    let mut lifecycle = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        fixture.request_id.clone(),
        fixture.session_id.clone(),
        fixture.agent_did.clone(),
        bridge_tool_call_id.to_string(),
        1,
        "spawn_subagent".to_string(),
        args,
        fixture.parent_deadline,
        AwaitMode::Background,
        CancelPolicy::Cascade,
        child_request_id.to_string(),
        "did:key:z6MkRemoteUnclaimed".to_string(),
    );
    lifecycle.start_running().await.unwrap();

    let action = hook
        .on_tool_call(
            "cancel_subagent",
            Some("model-call-cancel-unmat".to_string()),
            "internal-cancel-unmat",
            &json!({ "child_request_id": child_request_id }).to_string(),
        )
        .await;
    let error = skip_reason_json(action);
    assert_eq!(error["ok"], false);
    assert_eq!(error["failure_class"], "service_unavailable");
    assert_eq!(error["retryable"], true);
    let message = error["message"].as_str().expect("message");
    assert!(
        message.contains("has no materialized row yet"),
        "explains materialization: {message}"
    );
    assert!(
        message.contains(bridge_tool_call_id),
        "names the bridge: {message}"
    );
}
