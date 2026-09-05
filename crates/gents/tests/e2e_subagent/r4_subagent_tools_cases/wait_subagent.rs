use super::*;

#[tokio::test]
async fn wait_subagent_waits_on_existing_bridge_without_lifecycle_row() {
    let fixture = setup_spawn_fixture(
        "wait_subagent_existing_bridge",
        vec![CHILD_BEHAVIOR_ID],
        0,
        true,
    )
    .await;
    let db = &fixture.db;
    let hook = fixture.hook.clone();
    let session_id = fixture.session_id.clone();
    let agent_did = fixture.agent_did.clone();
    let spawn_args = json!({
        "name": CHILD_BEHAVIOR_ID,
        "prompt": "background child for wait_subagent",
        "await_mode": "background"
    })
    .to_string();

    let spawn_action = hook
        .on_tool_call(
            "spawn_subagent",
            Some("model-call-wait-spawn".to_string()),
            "internal-wait-spawn",
            &spawn_args,
        )
        .await;
    let spawn_receipt = skip_reason_json(spawn_action);
    assert_eq!(spawn_receipt["ok"], true);
    assert_eq!(spawn_receipt["await_mode"], "background");
    let child_request_id = spawn_receipt["child_request_id"]
        .as_str()
        .expect("child_request_id")
        .to_string();
    let child_session_id = wait_for_child_session_id(db.node.as_ref(), &child_request_id).await;

    let hook_for_wait = hook.clone();
    let wait_args = json!({ "child_request_id": child_request_id }).to_string();
    let wait_handle = tokio::spawn(async move {
        hook_for_wait
            .on_tool_call(
                "wait_subagent",
                Some("model-call-wait".to_string()),
                "internal-wait-tool",
                &wait_args,
            )
            .await
    });

    let foregrounded_bridge = wait_for_tool_call_await_mode(
        db.node.as_ref(),
        &session_id,
        "internal-wait-spawn",
        "foreground",
    )
    .await;
    assert_eq!(
        foregrounded_bridge.await_mode.as_deref(),
        Some("foreground")
    );
    assert_eq!(
        count_tool_calls_by_name(db.node.as_ref(), &session_id, "wait_subagent").await,
        0
    );

    persist_child_completion(
        db.node.as_ref(),
        &agent_did,
        &child_request_id,
        &child_session_id,
        "wait_subagent final answer",
    )
    .await;

    let action = tokio::time::timeout(Duration::from_secs(5), wait_handle)
        .await
        .expect("wait_subagent should complete after child completion")
        .expect("wait_subagent task should not panic");
    let result = skip_reason_json(action);
    assert_eq!(result["ok"], true);
    assert_eq!(result["await_mode"], "foreground");
    assert_eq!(result["status"], "completed");
    assert_eq!(result["final_response"], "wait_subagent final answer");

    let completed_bridge =
        fetch_tool_call(db.node.as_ref(), &session_id, "internal-wait-spawn").await;
    assert_eq!(
        completed_bridge.lifecycle_state.as_deref(),
        Some("completed")
    );
    assert_eq!(
        completed_bridge.result.as_deref(),
        Some("wait_subagent final answer")
    );
    assert_eq!(
        count_tool_calls_by_name(db.node.as_ref(), &session_id, "wait_subagent").await,
        0
    );
}

#[tokio::test]
async fn wait_subagent_maps_child_terminal_failures_without_lifecycle_row() {
    let cases = [
        (
            "failed",
            "failed",
            "failed",
            Some("child failed reason"),
            "child failed reason",
        ),
        (
            "dead",
            "dead",
            "failed",
            None,
            "child request reached the dead terminal state",
        ),
        (
            "interrupted",
            "interrupted",
            "cancelled",
            None,
            "child request was interrupted",
        ),
        (
            "superseded",
            "superseded",
            "failed",
            None,
            "child request was superseded",
        ),
    ];

    for (
        child_state,
        expected_status,
        expected_tool_state,
        failure_reason,
        expected_error_reason,
    ) in cases
    {
        let test_name = format!("wait_subagent_terminal_{child_state}");
        let internal_call_id = format!("internal-wait-terminal-spawn-{child_state}");
        let fixture = setup_spawn_fixture(&test_name, vec![CHILD_BEHAVIOR_ID], 0, true).await;
        let db = &fixture.db;
        let hook = fixture.hook.clone();
        let session_id = fixture.session_id.clone();
        let spawn_args = json!({
            "name": CHILD_BEHAVIOR_ID,
            "prompt": format!("background child terminal {child_state}"),
            "await_mode": "background"
        })
        .to_string();

        let spawn_action = hook
            .on_tool_call(
                "spawn_subagent",
                Some(format!("model-call-wait-terminal-spawn-{child_state}")),
                &internal_call_id,
                &spawn_args,
            )
            .await;
        let spawn_receipt = skip_reason_json(spawn_action);
        assert_eq!(spawn_receipt["ok"], true);
        assert_eq!(spawn_receipt["await_mode"], "background");
        let child_request_id = spawn_receipt["child_request_id"]
            .as_str()
            .expect("child_request_id")
            .to_string();
        let background_bridge =
            fetch_tool_call(db.node.as_ref(), &session_id, &internal_call_id).await;
        assert_eq!(
            background_bridge.await_mode.as_deref(),
            Some("background"),
            "spawn_subagent should persist a background bridge before wait_subagent starts"
        );
        assert_eq!(
            background_bridge.lifecycle_state.as_deref(),
            Some("running")
        );

        wait_for_child_session_id(db.node.as_ref(), &child_request_id).await;

        let hook_for_wait = hook.clone();
        let wait_args = json!({ "child_request_id": child_request_id }).to_string();
        let wait_handle = tokio::spawn(async move {
            hook_for_wait
                .on_tool_call(
                    "wait_subagent",
                    Some(format!("model-call-wait-terminal-{child_state}")),
                    "internal-wait-terminal",
                    &wait_args,
                )
                .await
        });

        let foregrounded_bridge = wait_for_tool_call_await_mode(
            db.node.as_ref(),
            &session_id,
            &internal_call_id,
            "foreground",
        )
        .await;
        assert_eq!(
            foregrounded_bridge.lifecycle_state.as_deref(),
            Some("running")
        );

        persist_child_terminal(
            db.node.as_ref(),
            &child_request_id,
            child_state,
            failure_reason,
        )
        .await;

        let action = tokio::time::timeout(Duration::from_secs(5), wait_handle)
            .await
            .expect("wait_subagent should complete after child terminal")
            .expect("wait_subagent task should not panic");
        let result = skip_reason_json(action);
        assert_eq!(result["ok"], false);
        assert_eq!(result["await_mode"], "foreground");
        assert_eq!(result["status"], expected_status);
        assert_eq!(result["error"]["reason"], expected_error_reason);
        assert_eq!(result["error"]["failure_class"], "external");

        let bridge = fetch_tool_call(db.node.as_ref(), &session_id, &internal_call_id).await;
        assert_eq!(
            bridge.lifecycle_state.as_deref(),
            Some(expected_tool_state),
            "unexpected bridge state for child terminal {child_state}"
        );
        if let Some(reason) = failure_reason {
            assert_eq!(bridge.result.as_deref(), Some(reason));
            assert_eq!(bridge.tool_failure_class.as_deref(), Some("external"));
        }
        assert_eq!(
            count_tool_calls_by_name(db.node.as_ref(), &session_id, "wait_subagent").await,
            0
        );
    }
}

#[tokio::test]
async fn wait_subagent_rejects_unlinked_child_without_lifecycle_row() {
    let fixture = setup_spawn_fixture(
        "wait_subagent_unlinked_child",
        vec![CHILD_BEHAVIOR_ID],
        0,
        true,
    )
    .await;
    let db = &fixture.db;
    let hook = fixture.hook.clone();
    let session_id = fixture.session_id.clone();
    let wait_args = json!({ "child_request_id": "not-this-parents-child" }).to_string();

    let action = hook
        .on_tool_call(
            "wait_subagent",
            Some("model-call-wait-denied".to_string()),
            "internal-wait-denied",
            &wait_args,
        )
        .await;
    let error = skip_reason_json(action);
    assert_eq!(error["ok"], false);
    assert_eq!(error["failure_class"], "service_unavailable");
    assert_eq!(error["tool_name"], "wait_subagent");
    assert_eq!(error["path"], "/child_request_id");
    assert_eq!(
        count_tool_calls_by_name(db.node.as_ref(), &session_id, "wait_subagent").await,
        0
    );
}

#[tokio::test]
async fn wait_subagent_explains_unmaterialized_child_bridge() {
    let fixture = setup_spawn_fixture(
        "wait_subagent_unmaterialized",
        vec![CHILD_BEHAVIOR_ID],
        0,
        true,
    )
    .await;
    let db = &fixture.db;
    let hook = fixture.hook.clone();

    let child_request_id = "wait-unmat-child";
    let bridge_tool_call_id = "wait-unmat-bridge";
    let args = json!({
        "name": "remote-coder",
        "agent_did": "did:key:z6MkRemoteUnclaimed",
        "behavior_id": "remote-coder-behavior",
        "prompt": "cross-deployment work",
        "await_mode": "background",
        "parent_subagent_depth": 0
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
    )
    .with_request_doc_id(Some(
        crate::support::exact_request_doc_id(db.node.as_ref(), &fixture.request_id).await,
    ));
    lifecycle.start_running().await.unwrap();

    let action = hook
        .on_tool_call(
            "wait_subagent",
            Some("model-call-wait-unmat".to_string()),
            "internal-wait-unmat",
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

#[tokio::test]
async fn corrupt_materialized_child_is_nonretryable_and_remains_listed() {
    let fixture = setup_spawn_fixture(
        "wait_subagent_corrupt_child",
        vec![CHILD_BEHAVIOR_ID],
        0,
        true,
    )
    .await;
    let db = &fixture.db;
    let hook = fixture.hook.clone();

    let child_request_id = "wait-corrupt-child";
    let child_session_id = "wait-corrupt-child-session";
    let bridge_tool_call_id = "wait-corrupt-bridge";
    let parent_request_doc_id =
        crate::support::exact_request_doc_id(db.node.as_ref(), &fixture.request_id).await;
    let parent_request_id = escape_graphql_string(&fixture.request_id);
    let parent_request_doc_id = escape_graphql_string(&parent_request_doc_id);
    let session_id = escape_graphql_string(&fixture.session_id);
    let agent_did = escape_graphql_string(&fixture.agent_did);
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{child_request_id}",
                agent_did: "{agent_did}",
                behavior_id: "",
                session_id: "{child_session_id}",
                retry_parent_request: "",
                retry_root_request: "{child_request_id}",
                superseded_by_request: "",
                content: "corrupt child",
                lifecycle_state: "processing",
                backend_id: "",
                execution_origin: "interactive",
                metadata: "",
                failure_reason: "",
                created_at: "2026-07-01T00:00:00Z",
                deadline: "2026-07-01T00:10:00Z",
                retry_count: 0,
                max_retries: 3,
                subagent_depth: 1,
                caused_by_parent_request_id: "{parent_request_id}",
                caused_by_parent_tool_call_id: "{bridge_tool_call_id}"
            }}) {{ _docID }}
            create_AgentToolCall(input: {{
                tool_call_key: "{session_id}:{bridge_tool_call_id}",
                request_id: "{parent_request_id}",
                request_doc_id: "{parent_request_doc_id}",
                agent_did: "{agent_did}",
                session_id: "{session_id}",
                message_sequence: 1,
                tool_name: "spawn_subagent",
                tool_call_id: "{bridge_tool_call_id}",
                args: "{{}}",
                result: "",
                status: "running",
                lifecycle_state: "running",
                started_at: "2026-07-01T00:00:00Z",
                deadline_at: "2026-07-01T00:10:00Z",
                await_mode: "background",
                cancel_policy: "cascade",
                child_request_id: "{child_request_id}"
            }}) {{ _docID }}
        }}"#
    );
    let response = db.node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create corrupt child edge failed: {:?}",
        response.errors
    );

    let action = hook
        .on_tool_call(
            "wait_subagent",
            Some("model-call-wait-corrupt".to_string()),
            "internal-wait-corrupt",
            &json!({ "child_request_id": child_request_id }).to_string(),
        )
        .await;
    let error = skip_reason_json(action);
    assert_eq!(error["ok"], false);
    assert_eq!(error["failure_class"], "service_unavailable");
    assert_eq!(error["retryable"], false);
    let message = error["message"].as_str().expect("message");
    assert!(
        message.contains("does not corroborate") && message.contains(bridge_tool_call_id),
        "rejects corrupt physical provenance: {message}"
    );
    assert!(
        !message.contains("has no materialized row yet"),
        "must not be masked as materialization lag: {message}"
    );

    let cancel_action = hook
        .on_tool_call(
            "cancel_subagent",
            Some("model-call-cancel-corrupt".to_string()),
            "internal-cancel-corrupt",
            &json!({ "child_request_id": child_request_id }).to_string(),
        )
        .await;
    let cancel_error = skip_reason_json(cancel_action);
    assert_eq!(cancel_error["ok"], false);
    assert_eq!(cancel_error["failure_class"], "service_unavailable");
    assert_eq!(cancel_error["retryable"], false);
    assert!(cancel_error["message"]
        .as_str()
        .is_some_and(|message| message.contains("does not corroborate")));

    let steer_action = hook
        .on_tool_call(
            "steer_subagent",
            Some("model-call-steer-corrupt".to_string()),
            "internal-steer-corrupt",
            &json!({
                "child_request_id": child_request_id,
                "message": "do not retry rejected lineage"
            })
            .to_string(),
        )
        .await;
    let steer_error = skip_reason_json(steer_action);
    assert_eq!(steer_error["ok"], false);
    assert_eq!(steer_error["failure_class"], "service_unavailable");
    assert_eq!(steer_error["retryable"], false);
    assert!(steer_error["message"]
        .as_str()
        .is_some_and(|message| message.contains("does not corroborate")));

    let list_action = hook
        .on_tool_call(
            "list_subagents",
            Some("model-call-list-corrupt".to_string()),
            "internal-list-corrupt",
            "{}",
        )
        .await;
    let list_result = skip_reason_json(list_action);
    let entries = list_result["entries"].as_array().expect("list entries");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["child_request_id"], child_request_id);
    assert_eq!(entries[0]["status"], "pending_child_authorization");
}

#[tokio::test]
async fn wait_subagent_from_resumed_hook_cascades_parent_interrupt() {
    assert_resumed_wait_cascades_callers_interrupt(false).await;
}

#[tokio::test]
async fn wait_subagent_from_later_turn_observes_current_callers_interrupt() {
    assert_resumed_wait_cascades_callers_interrupt(true).await;
}

async fn assert_resumed_wait_cascades_callers_interrupt(later_turn: bool) {
    let fixture = setup_spawn_fixture(
        "wait_subagent_resumed_interrupt",
        vec![CHILD_BEHAVIOR_ID],
        0,
        true,
    )
    .await;
    let db = &fixture.db;
    let hook = fixture.hook.clone();
    let session_id = fixture.session_id.clone();
    let request_id = fixture.request_id.clone();
    let parent_deadline = fixture.parent_deadline;
    let agent_did = fixture.agent_did.clone();
    let spawn_args = json!({
        "name": CHILD_BEHAVIOR_ID,
        "prompt": "background child for resumed wait cancellation",
        "await_mode": "background"
    })
    .to_string();

    let spawn_action = hook
        .on_tool_call(
            "spawn_subagent",
            Some("model-call-wait-resume-spawn".to_string()),
            "internal-wait-resume-spawn",
            &spawn_args,
        )
        .await;
    let spawn_receipt = skip_reason_json(spawn_action);
    let child_request_id = spawn_receipt["child_request_id"]
        .as_str()
        .expect("child_request_id")
        .to_string();
    wait_for_child_session_id(db.node.as_ref(), &child_request_id).await;

    let waiting_request_id = if later_turn {
        update_request_state(db.node.as_ref(), &request_id, "completed").await;
        let caller = "later-waiting-request".to_owned();
        create_parent_request(
            db.node.as_ref(),
            &agent_did,
            &caller,
            &session_id,
            0,
            parent_deadline,
        )
        .await;
        caller
    } else {
        request_id.clone()
    };

    let resumed_hook = DefraSessionHook::resume_with_identity_policy(
        db.node.clone(),
        &session_id,
        PARENT_BEHAVIOR_ID,
        &agent_did,
        FailurePolicy::default(),
    )
    .await
    .unwrap();
    resumed_hook
        .set_active_request_lineage(Some(waiting_request_id.clone()), None)
        .await
        .expect("bind persisted request lineage");
    resumed_hook
        .set_request_deadline_at(Some(parent_deadline))
        .await;

    let hook_for_wait = resumed_hook.clone();
    let wait_args = json!({ "child_request_id": child_request_id }).to_string();
    let wait_handle = tokio::spawn(async move {
        hook_for_wait
            .on_tool_call(
                "wait_subagent",
                Some("model-call-wait-resume".to_string()),
                "internal-wait-resume",
                &wait_args,
            )
            .await
    });

    let foregrounded_bridge = wait_for_tool_call_await_mode(
        db.node.as_ref(),
        &session_id,
        "internal-wait-resume-spawn",
        "foreground",
    )
    .await;
    assert_eq!(
        foregrounded_bridge.await_mode.as_deref(),
        Some("foreground")
    );

    interrupt_request(db.node.as_ref(), &waiting_request_id)
        .await
        .unwrap();

    let action = tokio::time::timeout(Duration::from_secs(5), wait_handle)
        .await
        .expect("resumed wait_subagent should unblock after parent interrupt")
        .expect("resumed wait_subagent task should not panic");
    let result = skip_reason_json(action);
    assert_eq!(result["ok"], false);
    assert_eq!(result["await_mode"], "foreground");
    assert_eq!(result["status"], "interrupted");

    let cancelled_bridge =
        fetch_tool_call(db.node.as_ref(), &session_id, "internal-wait-resume-spawn").await;
    assert_eq!(
        cancelled_bridge.lifecycle_state.as_deref(),
        Some("cancelled")
    );
    let child_interrupt = fetch_interrupt_requested_at(db.node.as_ref(), &child_request_id)
        .await
        .unwrap();
    assert!(
        child_interrupt.is_some(),
        "wait_subagent cancellation should cascade to the child request"
    );
    if later_turn {
        assert!(
            fetch_interrupt_requested_at(db.node.as_ref(), &request_id)
                .await
                .unwrap()
                .is_none(),
            "the original spawning request was never interrupted"
        );
    }
    assert_eq!(
        count_tool_calls_by_name(db.node.as_ref(), &session_id, "wait_subagent").await,
        0
    );
}

#[tokio::test]
async fn wait_subagent_returns_background_receipt_when_bridge_is_backgrounded() {
    let fixture = setup_spawn_fixture(
        "wait_subagent_backgrounded",
        vec![CHILD_BEHAVIOR_ID],
        0,
        true,
    )
    .await;
    let db = &fixture.db;
    let hook = fixture.hook.clone();
    let session_id = fixture.session_id.clone();
    let spawn_args = json!({
        "name": CHILD_BEHAVIOR_ID,
        "prompt": "background child for wait backgrounding",
        "await_mode": "background"
    })
    .to_string();

    let spawn_action = hook
        .on_tool_call(
            "spawn_subagent",
            Some("model-call-wait-bg-spawn".to_string()),
            "internal-wait-bg-spawn",
            &spawn_args,
        )
        .await;
    let spawn_receipt = skip_reason_json(spawn_action);
    let child_request_id = spawn_receipt["child_request_id"]
        .as_str()
        .expect("child_request_id")
        .to_string();
    wait_for_child_session_id(db.node.as_ref(), &child_request_id).await;

    let hook_for_wait = hook.clone();
    let wait_args = json!({ "child_request_id": child_request_id }).to_string();
    let wait_handle = tokio::spawn(async move {
        hook_for_wait
            .on_tool_call(
                "wait_subagent",
                Some("model-call-wait-bg".to_string()),
                "internal-wait-bg",
                &wait_args,
            )
            .await
    });

    wait_for_tool_call_await_mode(
        db.node.as_ref(),
        &session_id,
        "internal-wait-bg-spawn",
        "foreground",
    )
    .await;
    let mut lifecycle =
        ToolCallLifecycle::load(db.node.clone(), &session_id, "internal-wait-bg-spawn")
            .await
            .unwrap()
            .expect("wait_subagent should foreground the original bridge");
    lifecycle.background().await.unwrap();

    let action = tokio::time::timeout(Duration::from_secs(5), wait_handle)
        .await
        .expect("wait_subagent should unblock after bridge backgrounding")
        .expect("wait_subagent task should not panic");
    let result = skip_reason_json(action);
    assert_eq!(result["ok"], true);
    assert_eq!(result["await_mode"], "background");
    assert_eq!(result["status"], "running");
    assert_eq!(result["backgrounded"], true);
    assert_eq!(
        count_tool_calls_by_name(db.node.as_ref(), &session_id, "wait_subagent").await,
        0
    );
}
