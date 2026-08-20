use super::*;

const GHOST_BEHAVIOR_ID: &str = "r4-ghost-child";

async fn setup_ghost_behavior_fixture(test_name: &str) -> SpawnFixture {
    let db = test_db(test_name).await;
    let agent_did = format!("did:test:r4-{test_name}");

    upsert_tool_selection(
        db.node.as_ref(),
        &ToolSelectionDocument {
            selection_id: "r4-parent-tools".to_string(),
            agent_did: agent_did.clone(),
            subagent_targets: Some(vec![gents::subagent_target_entry(
                GHOST_BEHAVIOR_ID,
                &agent_did,
                GHOST_BEHAVIOR_ID,
                None,
            )]),
            subagent_spawn_enabled: Some(true),
            subagent_background_enabled: Some(true),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    upsert_agent_behavior(
        db.node.as_ref(),
        &AgentBehaviorDocument {
            behavior_id: PARENT_BEHAVIOR_ID.to_string(),
            agent_did: agent_did.clone(),
            display_name: Some("R4 parent (ghost test)".to_string()),
            description: None,
            summary: None,
            system_prompt: None,
            request_context_template: None,
            backend_id: None,
            model_name: None,
            tool_selection_id: Some("r4-parent-tools".to_string()),
            inference_profile_id: None,
            compaction_strategy: None,
            compaction_threshold: None,
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            enabled: true,
            created_at: Some("2026-05-12T00:00:00Z".to_string()),
        },
    )
    .await
    .unwrap();

    let source = spawn_subagent_source(
        db.node.clone(),
        &agent_did,
        PARENT_BEHAVIOR_ID,
        PARENT_BEHAVIOR_ID,
    );

    let session_id = format!("{test_name}-session");
    let request_id = format!("{test_name}-parent");
    let parent_deadline = chrono::Utc::now() + chrono::Duration::minutes(5);
    create_parent_request(
        db.node.as_ref(),
        &agent_did,
        &request_id,
        &session_id,
        0,
        parent_deadline,
    )
    .await;
    crate::support::create_agent_session(
        db.node.as_ref(),
        &session_id,
        PARENT_BEHAVIOR_ID,
        "2026-05-13T00:00:00Z",
    )
    .await;

    let hook = DefraSessionHook::resume_with_identity_policy(
        db.node.clone(),
        &session_id,
        PARENT_BEHAVIOR_ID,
        &agent_did,
        FailurePolicy::default(),
    )
    .await
    .unwrap();
    hook.set_active_request_id(Some(request_id.clone())).await;
    hook.set_request_deadline_at(Some(parent_deadline)).await;

    SpawnFixture {
        db,
        hook,
        session_id,
        request_id,
        parent_deadline,
        agent_did,
        _source: source,
    }
}

#[tokio::test]
async fn spawn_subagent_rejects_local_target_whose_behavior_was_deleted() {
    let fixture = setup_ghost_behavior_fixture("spawn_subagent_ghost_behavior").await;
    let db = &fixture.db;
    let hook = fixture.hook.clone();
    let session_id = fixture.session_id.clone();
    let args = json!({
        "name": GHOST_BEHAVIOR_ID,
        "prompt": "should not spawn orphan",
        "await_mode": "background"
    })
    .to_string();

    let action = hook
        .on_tool_call("spawn_subagent", None, "internal-spawn-ghost", &args)
        .await;
    let error = skip_reason_json(action);
    assert_eq!(error["ok"], false, "spawn must be rejected");
    assert_eq!(
        error["failure_class"], "tool_not_allowed",
        "failure_class must be tool_not_allowed"
    );
    assert!(
        error["message"]
            .as_str()
            .unwrap_or("")
            .contains("no longer exists"),
        "message must mention the behavior no longer exists"
    );

    let tool = fetch_tool_call(db.node.as_ref(), &session_id, "internal-spawn-ghost").await;
    assert_eq!(
        tool.lifecycle_state.as_deref(),
        Some("failed"),
        "tool call must be in failed state"
    );
    assert_eq!(
        tool.tool_failure_class.as_deref(),
        Some("serviceUnavailable"),
        "failure class must be serviceUnavailable"
    );
    assert!(
        child_request_for_tool(db.node.as_ref(), "internal-spawn-ghost")
            .await
            .is_none(),
        "must not write an orphan child AgentRequest"
    );
}

#[tokio::test]
async fn spawn_subagent_skip_payload_is_persisted_to_transcript() {
    let fixture = setup_spawn_fixture(
        "spawn_subagent_skip_transcript",
        vec![CHILD_BEHAVIOR_ID],
        0,
        true,
    )
    .await;
    let db = &fixture.db;
    let hook = fixture.hook.clone();
    let session_id = fixture.session_id.clone();
    let parent_deadline = fixture.parent_deadline;
    let args = json!({
        "name": CHILD_BEHAVIOR_ID,
        "prompt": "child prompt for transcript",
        "await_mode": "background",
        "deadline": (parent_deadline - chrono::Duration::minutes(1)).to_rfc3339()
    })
    .to_string();

    let action = hook
        .on_tool_call(
            "spawn_subagent",
            Some("model-call-transcript".to_string()),
            "internal-spawn-transcript",
            &args,
        )
        .await;
    let ToolCallHookAction::Skip { reason } = action else {
        panic!("expected Skip action");
    };
    let child_request_id = serde_json::from_str::<Value>(&reason).unwrap()["child_request_id"]
        .as_str()
        .unwrap()
        .to_string();

    hook.persist_message(&Message::Assistant {
        id: None,
        content: vec![AssistantContent::ToolCall(ToolCall {
            id: "internal-spawn-transcript".to_string(),
            call_id: Some("model-call-transcript".to_string()),
            function: ToolFunction {
                name: "spawn_subagent".to_string(),
                arguments: serde_json::from_str(&args).unwrap(),
            },
            signature: None,
            additional_params: None,
        })],
    })
    .await
    .unwrap();
    hook.persist_stream_tool_result_message(
        &ToolResult {
            id: "internal-spawn-transcript".to_string(),
            call_id: Some("model-call-transcript".to_string()),
            content: vec![ToolResultContent::Text(Text {
                text: reason.clone(),
            })],
        },
        "internal-spawn-transcript",
    )
    .await
    .unwrap();

    let history = load_history(db.node.as_ref(), &session_id).await.unwrap();
    assert!(history.iter().any(|message| {
        matches!(
            message,
            Message::User { content }
                if matches!(content.first().expect("non-empty content"), UserContent::ToolResult(tool_result)
                    if matches!(tool_result.content.first().expect("non-empty content"), ToolResultContent::Text(Text { text })
                        if text.contains(&child_request_id)
                            && text.contains("\"await_mode\": \"background\"")))
        )
    }));
}

#[tokio::test]
async fn spawn_subagent_rejects_unauthorized_target_without_child_request() {
    let fixture = setup_spawn_fixture(
        "spawn_subagent_unauthorized",
        vec!["different-child"],
        0,
        true,
    )
    .await;
    let db = &fixture.db;
    let hook = fixture.hook.clone();
    let session_id = fixture.session_id.clone();
    let args = json!({
        "name": CHILD_BEHAVIOR_ID,
        "prompt": "should not spawn",
        "await_mode": "background"
    })
    .to_string();

    let action = hook
        .on_tool_call("spawn_subagent", None, "internal-spawn-denied", &args)
        .await;
    let error = skip_reason_json(action);
    assert_eq!(error["ok"], false);
    assert_eq!(error["failure_class"], "tool_not_allowed");
    assert_eq!(error["requested_tool_name"], CHILD_BEHAVIOR_ID);

    let tool = fetch_tool_call(db.node.as_ref(), &session_id, "internal-spawn-denied").await;
    assert_eq!(tool.lifecycle_state.as_deref(), Some("failed"));
    assert_eq!(
        tool.tool_failure_class.as_deref(),
        Some("serviceUnavailable")
    );
    assert!(tool
        .result
        .as_deref()
        .is_some_and(|result| result.contains("\"tool_not_allowed\"")));
    assert!(
        child_request_for_tool(db.node.as_ref(), "internal-spawn-denied")
            .await
            .is_none()
    );
}

#[tokio::test]
async fn spawn_subagent_rejects_when_spawn_disabled_without_child_request() {
    let fixture = setup_spawn_fixture_with_flags(
        "spawn_subagent_spawn_disabled",
        vec![CHILD_BEHAVIOR_ID],
        0,
        false,
        true,
    )
    .await;
    let db = &fixture.db;
    let hook = fixture.hook.clone();
    let session_id = fixture.session_id.clone();
    let args = json!({
        "name": CHILD_BEHAVIOR_ID,
        "prompt": "should not spawn",
        "await_mode": "background"
    })
    .to_string();

    let action = hook
        .on_tool_call("spawn_subagent", None, "internal-spawn-disabled", &args)
        .await;
    let error = skip_reason_json(action);
    assert_eq!(error["failure_class"], "tool_not_allowed");

    let tool = fetch_tool_call(db.node.as_ref(), &session_id, "internal-spawn-disabled").await;
    assert_eq!(tool.lifecycle_state.as_deref(), Some("failed"));
    assert_eq!(
        tool.tool_failure_class.as_deref(),
        Some("serviceUnavailable")
    );
    assert!(
        child_request_for_tool(db.node.as_ref(), "internal-spawn-disabled")
            .await
            .is_none()
    );
}

#[tokio::test]
async fn spawn_subagent_rejects_background_when_background_disabled_without_child_request() {
    let fixture = setup_spawn_fixture(
        "spawn_subagent_background_disabled",
        vec![CHILD_BEHAVIOR_ID],
        0,
        false,
    )
    .await;
    let db = &fixture.db;
    let hook = fixture.hook.clone();
    let session_id = fixture.session_id.clone();
    let args = json!({
        "name": CHILD_BEHAVIOR_ID,
        "prompt": "should not spawn in background",
        "await_mode": "background"
    })
    .to_string();

    let action = hook
        .on_tool_call("spawn_subagent", None, "internal-spawn-bg-disabled", &args)
        .await;
    let error = skip_reason_json(action);
    assert_eq!(error["failure_class"], "tool_not_allowed");
    assert_eq!(error["requested_tool_name"], "background");

    let tool = fetch_tool_call(db.node.as_ref(), &session_id, "internal-spawn-bg-disabled").await;
    assert_eq!(tool.lifecycle_state.as_deref(), Some("failed"));
    assert_eq!(
        tool.tool_failure_class.as_deref(),
        Some("serviceUnavailable")
    );
    assert!(
        child_request_for_tool(db.node.as_ref(), "internal-spawn-bg-disabled")
            .await
            .is_none()
    );
}

#[tokio::test]
async fn spawn_subagent_rejects_deadline_after_parent_without_child_request() {
    let fixture =
        setup_spawn_fixture("spawn_subagent_deadline", vec![CHILD_BEHAVIOR_ID], 0, true).await;
    let db = &fixture.db;
    let hook = fixture.hook.clone();
    let session_id = fixture.session_id.clone();
    let parent_deadline = fixture.parent_deadline;
    let args = json!({
        "name": CHILD_BEHAVIOR_ID,
        "prompt": "deadline too late",
        "await_mode": "background",
        "deadline": (parent_deadline + chrono::Duration::seconds(1)).to_rfc3339()
    })
    .to_string();

    let action = hook
        .on_tool_call("spawn_subagent", None, "internal-spawn-deadline", &args)
        .await;
    let error = skip_reason_json(action);
    assert_eq!(error["failure_class"], "invalid_tool_arguments");
    assert_eq!(error["path"], "/deadline");

    let tool = fetch_tool_call(db.node.as_ref(), &session_id, "internal-spawn-deadline").await;
    assert_eq!(tool.lifecycle_state.as_deref(), Some("failed"));
    assert_eq!(tool.tool_failure_class.as_deref(), Some("argumentInvalid"));
    assert!(
        child_request_for_tool(db.node.as_ref(), "internal-spawn-deadline")
            .await
            .is_none()
    );
}

#[tokio::test]
async fn spawn_subagent_rejects_depth_ceiling_without_child_request() {
    let fixture = setup_spawn_fixture(
        "spawn_subagent_depth",
        vec![CHILD_BEHAVIOR_ID],
        MAX_SUBAGENT_DEPTH,
        true,
    )
    .await;
    let db = &fixture.db;
    let hook = fixture.hook.clone();
    let session_id = fixture.session_id.clone();
    let args = json!({
        "name": CHILD_BEHAVIOR_ID,
        "prompt": "too deep",
        "await_mode": "background"
    })
    .to_string();

    let action = hook
        .on_tool_call("spawn_subagent", None, "internal-spawn-depth", &args)
        .await;
    let error = skip_reason_json(action);
    assert_eq!(error["ok"], false);
    assert_eq!(error["failure_class"], "invalid_tool_arguments");
    assert_eq!(error["code"], "subagent_depth_exceeded");
    assert_eq!(error["parent_subagent_depth"], json!(MAX_SUBAGENT_DEPTH));
    assert_eq!(error["max_subagent_depth"], json!(MAX_SUBAGENT_DEPTH));

    let tool = fetch_tool_call(db.node.as_ref(), &session_id, "internal-spawn-depth").await;
    assert_eq!(tool.lifecycle_state.as_deref(), Some("failed"));
    assert_eq!(tool.tool_failure_class.as_deref(), Some("argumentInvalid"));
    assert!(
        child_request_for_tool(db.node.as_ref(), "internal-spawn-depth")
            .await
            .is_none()
    );
}
