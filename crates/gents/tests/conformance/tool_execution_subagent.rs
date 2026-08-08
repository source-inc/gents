use crate::support::test_db;
#[allow(unused_imports)]
use gents::tool_call_lifecycle::{
    create_subagent_request, AwaitMode, CancelCause, CancelPolicy, CascadeIntent, ChildTerminal,
    FailureClass, IllegalToolCallTransition, ToolCallLifecycle, MAX_SUBAGENT_DEPTH,
};

fn test_deadline() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now() + chrono::Duration::minutes(5)
}

async fn make_completed_request(
    node: &gents::defra_node::EmbeddedNode,
    request_id: &str,
    parent_request_id: Option<&str>,
    parent_tool_call_id: Option<&str>,
    final_message: &str,
) -> anyhow::Result<()> {
    let depth: u32 = if parent_request_id.is_some() { 1 } else { 0 };
    let parent_req_field = parent_request_id
        .map(|id| {
            let escaped = gents::graphql::escape_graphql_string(id);
            format!(r#"caused_by_parent_request_id: "{escaped}","#)
        })
        .unwrap_or_default();
    let parent_tc_field = parent_tool_call_id
        .map(|id| {
            let escaped = gents::graphql::escape_graphql_string(id);
            format!(r#"caused_by_parent_tool_call_id: "{escaped}","#)
        })
        .unwrap_or_default();
    let rid = gents::graphql::escape_graphql_string(request_id);
    let content = gents::graphql::escape_graphql_string(final_message);
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let now_escaped = gents::graphql::escape_graphql_string(&now);
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{rid}",
                agent_did: "{agent_did}",
                behavior_id: "test",
                session_id: "{rid}",
                retry_parent_request: "",
                retry_root_request: "{rid}",
                superseded_by_request: "",
                content: "{content}",
                status: "completed",
                lifecycle_state: "completed",
                backend_id: "",
                execution_origin: "interactive",
                failure_reason: "",
                created_at: "{now_escaped}",
                retry_count: 0,
                max_retries: {max_retries},
                subagent_depth: {depth},
                {prf}
                {ptc}
            }}) {{ _docID }}
        }}"#,
        agent_did = crate::support::AGENT_DID,
        max_retries = gents::lifecycle::DEFAULT_REQUEST_MAX_RETRIES,
        prf = parent_req_field,
        ptc = parent_tc_field,
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "make_completed_request failed: {:?}",
        resp.errors
    );
    Ok(())
}

async fn make_terminal_request(
    node: &gents::defra_node::EmbeddedNode,
    request_id: &str,
    parent_request_id: Option<&str>,
    parent_tool_call_id: Option<&str>,
    state: &str,
) -> anyhow::Result<()> {
    let depth: u32 = if parent_request_id.is_some() { 1 } else { 0 };
    let parent_req_field = parent_request_id
        .map(|id| {
            let escaped = gents::graphql::escape_graphql_string(id);
            format!(r#"caused_by_parent_request_id: "{escaped}","#)
        })
        .unwrap_or_default();
    let parent_tc_field = parent_tool_call_id
        .map(|id| {
            let escaped = gents::graphql::escape_graphql_string(id);
            format!(r#"caused_by_parent_tool_call_id: "{escaped}","#)
        })
        .unwrap_or_default();
    let rid = gents::graphql::escape_graphql_string(request_id);
    let state_escaped = gents::graphql::escape_graphql_string(state);
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let now_escaped = gents::graphql::escape_graphql_string(&now);
    let legacy_status = match state {
        "completed" => "completed",
        "superseded" => "superseded",
        "failed" | "dead" | "interrupted" => "error",
        other => other,
    };
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{rid}",
                agent_did: "{agent_did}",
                behavior_id: "test",
                session_id: "{rid}",
                retry_parent_request: "",
                retry_root_request: "{rid}",
                superseded_by_request: "",
                content: "",
                status: "{legacy_status}",
                lifecycle_state: "{state_escaped}",
                backend_id: "",
                execution_origin: "interactive",
                failure_reason: "",
                created_at: "{now_escaped}",
                retry_count: 0,
                max_retries: {max_retries},
                subagent_depth: {depth},
                {prf}
                {ptc}
            }}) {{ _docID }}
        }}"#,
        agent_did = crate::support::AGENT_DID,
        max_retries = gents::lifecycle::DEFAULT_REQUEST_MAX_RETRIES,
        prf = parent_req_field,
        ptc = parent_tc_field,
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "make_terminal_request({state}) failed: {:?}",
        resp.errors
    );
    Ok(())
}

async fn fetch_tool_call_state_and_result(
    node: &gents::defra_node::EmbeddedNode,
    tool_call_id: &str,
) -> (String, String) {
    let tool_call_id = gents::graphql::escape_graphql_string(tool_call_id);
    let query = format!(
        r#"{{ AgentToolCall(filter: {{ tool_call_id: {{ _eq: "{tool_call_id}" }} }}) {{
            lifecycle_state
            result
        }} }}"#
    );
    let resp = node.execute(&query).await;
    assert!(!resp.has_errors(), "query failed: {:?}", resp.errors);
    let data = resp.data.expect("data");
    let rows = data["AgentToolCall"].as_array().expect("array");
    assert_eq!(rows.len(), 1, "expected one AgentToolCall row");
    (
        rows[0]["lifecycle_state"].as_str().unwrap().to_string(),
        rows[0]["result"].as_str().unwrap().to_string(),
    )
}

async fn fetch_tool_call_await_mode(
    node: &gents::defra_node::EmbeddedNode,
    tool_call_id: &str,
) -> String {
    let tool_call_id = gents::graphql::escape_graphql_string(tool_call_id);
    let query = format!(
        r#"{{ AgentToolCall(filter: {{ tool_call_id: {{ _eq: "{tool_call_id}" }} }}) {{
            await_mode
        }} }}"#
    );
    let resp = node.execute(&query).await;
    assert!(!resp.has_errors(), "query failed: {:?}", resp.errors);
    let data = resp.data.expect("data");
    let rows = data["AgentToolCall"].as_array().expect("array");
    assert_eq!(rows.len(), 1, "expected one AgentToolCall row");
    rows[0]["await_mode"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn integration_load_round_trip_preserves_subagent_fields() {
    let db = test_db("tc-sa-load-rt-1").await;

    let mut lc = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        "req-load-rt-1".to_string(),
        "sess-load-rt-1".to_string(),
        "did:test:test".to_string(),
        "tc-load-rt-1".to_string(),
        3,
        "spawn_subagent".to_string(),
        r#"{"target":"amy-code"}"#.to_string(),
        test_deadline(),
        AwaitMode::Background,
        CancelPolicy::Detach,
        "child-req-load-rt-1".to_string(),
        "did:test:target".to_string(),
    );
    lc.start_running().await.unwrap();

    let loaded = ToolCallLifecycle::load(db.node.clone(), "sess-load-rt-1", "tc-load-rt-1")
        .await
        .unwrap()
        .expect("row must exist after start_running");

    drop(loaded);

    let resp = db
        .node
        .execute(
            r#"{ AgentToolCall(filter: { tool_call_id: { _eq: "tc-load-rt-1" } }) {
            await_mode
            cancel_policy
            child_request_id
        } }"#,
        )
        .await;
    assert!(!resp.has_errors(), "query failed: {:?}", resp.errors);
    let data = resp.data.expect("data");
    let rows = data["AgentToolCall"].as_array().expect("array");
    assert_eq!(rows.len(), 1, "expected one AgentToolCall row");
    let row = &rows[0];

    assert_eq!(
        row["await_mode"].as_str(),
        Some("background"),
        "await_mode must be persisted as 'background' after start_running with AwaitMode::Background"
    );
    assert_eq!(
        row["cancel_policy"].as_str(),
        Some("detach"),
        "cancel_policy must be persisted as 'detach' after start_running with CancelPolicy::Detach"
    );
    assert_eq!(
        row["child_request_id"].as_str(),
        Some("child-req-load-rt-1"),
        "child_request_id must be persisted and readable after start_running"
    );
}

#[tokio::test]
async fn integration_load_round_trip_foreground_cascade_also_preserved() {
    let db = test_db("tc-sa-load-rt-2").await;

    let mut lc = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        "req-load-rt-2".to_string(),
        "sess-load-rt-2".to_string(),
        "did:test:test".to_string(),
        "tc-load-rt-2".to_string(),
        1,
        "spawn_subagent".to_string(),
        "{}".to_string(),
        test_deadline(),
        AwaitMode::Foreground,
        CancelPolicy::Cascade,
        "child-req-load-rt-2".to_string(),
        "did:test:target".to_string(),
    );
    lc.start_running().await.unwrap();

    let loaded = ToolCallLifecycle::load(db.node.clone(), "sess-load-rt-2", "tc-load-rt-2")
        .await
        .unwrap()
        .expect("row must exist after start_running");
    drop(loaded);

    let resp = db
        .node
        .execute(
            r#"{ AgentToolCall(filter: { tool_call_id: { _eq: "tc-load-rt-2" } }) {
            await_mode
            cancel_policy
            child_request_id
        } }"#,
        )
        .await;
    assert!(!resp.has_errors(), "query failed: {:?}", resp.errors);
    let data = resp.data.expect("data");
    let rows = data["AgentToolCall"].as_array().expect("array");
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row["await_mode"].as_str(), Some("foreground"));
    assert_eq!(row["cancel_policy"].as_str(), Some("cascade"));
    assert_eq!(
        row["child_request_id"].as_str(),
        Some("child-req-load-rt-2")
    );
}

#[tokio::test]
async fn integration_load_round_trip_native_tool_has_no_child_request_id() {
    let db = test_db("tc-sa-load-rt-3").await;

    let mut lc = ToolCallLifecycle::new(
        db.node.clone(),
        "req-load-rt-3".to_string(),
        "sess-load-rt-3".to_string(),
        "did:test:test".to_string(),
        "tc-load-rt-3".to_string(),
        0,
        "echo".to_string(),
        "{}".to_string(),
        test_deadline(),
    );
    lc.start_running().await.unwrap();

    let loaded = ToolCallLifecycle::load(db.node.clone(), "sess-load-rt-3", "tc-load-rt-3")
        .await
        .unwrap()
        .expect("row must exist after start_running");
    drop(loaded);

    let resp = db
        .node
        .execute(
            r#"{ AgentToolCall(filter: { tool_call_id: { _eq: "tc-load-rt-3" } }) {
            await_mode
            cancel_policy
            child_request_id
        } }"#,
        )
        .await;
    assert!(!resp.has_errors(), "query failed: {:?}", resp.errors);
    let data = resp.data.expect("data");
    let rows = data["AgentToolCall"].as_array().expect("array");
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert!(
        row["child_request_id"].is_null(),
        "native tool must have child_request_id=null in DB, got: {:?}",
        row["child_request_id"]
    );
}

#[tokio::test]
async fn integration_background_then_foreground_persists_round_trip() {
    let db = test_db("tc-sa-int-1").await;

    let mut lc = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        "req-int-1".to_string(),
        "sess-int-1".to_string(),
        "did:test:test".to_string(),
        "tc-int-1".to_string(),
        1,
        "spawn_subagent".to_string(),
        "{}".to_string(),
        test_deadline(),
        AwaitMode::Foreground,
        CancelPolicy::Cascade,
        "child-req-int-1".to_string(),
        "did:test:target".to_string(),
    );
    lc.start_running().await.unwrap();

    lc.background().await.unwrap();
    let resp = db
        .node
        .execute(
            r#"{ AgentToolCall(filter: { tool_call_id: { _eq: "tc-int-1" } }) { await_mode } }"#,
        )
        .await;
    assert!(!resp.has_errors(), "query failed: {:?}", resp.errors);
    let data = resp.data.expect("data");
    let rows = data["AgentToolCall"].as_array().expect("array");
    assert_eq!(rows.len(), 1, "expected one row");
    assert_eq!(
        rows[0]["await_mode"].as_str(),
        Some("background"),
        "await_mode should be persisted as 'background' after background()"
    );

    lc.foreground().await.unwrap();
    let resp = db
        .node
        .execute(
            r#"{ AgentToolCall(filter: { tool_call_id: { _eq: "tc-int-1" } }) { await_mode } }"#,
        )
        .await;
    assert!(!resp.has_errors(), "query failed: {:?}", resp.errors);
    let data = resp.data.expect("data");
    let rows = data["AgentToolCall"].as_array().expect("array");
    assert_eq!(rows.len(), 1, "expected one row");
    assert_eq!(
        rows[0]["await_mode"].as_str(),
        Some("foreground"),
        "await_mode should be persisted as 'foreground' after foreground()"
    );

    let err = lc.foreground().await.unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<IllegalToolCallTransition>(),
            Some(IllegalToolCallTransition::ModeAlreadyForeground)
        ),
        "expected ModeAlreadyForeground on second foreground() call, got: {err:?}"
    );
}

#[tokio::test]
async fn integration_mode_flips_tolerate_stale_same_target_owner() {
    let db = test_db("tc-sa-mode-cas-1").await;

    let mut lc = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        "req-mode-cas-1".to_string(),
        "sess-mode-cas-1".to_string(),
        "did:test:test".to_string(),
        "tc-mode-cas-1".to_string(),
        1,
        "spawn_subagent".to_string(),
        "{}".to_string(),
        test_deadline(),
        AwaitMode::Foreground,
        CancelPolicy::Cascade,
        "child-req-mode-cas-1".to_string(),
        "did:test:target".to_string(),
    );
    lc.start_running().await.unwrap();

    let mut stale_foreground =
        ToolCallLifecycle::load(db.node.clone(), "sess-mode-cas-1", "tc-mode-cas-1")
            .await
            .unwrap()
            .expect("stale foreground owner should load");
    lc.background().await.unwrap();
    stale_foreground.background().await.unwrap();
    assert_eq!(
        fetch_tool_call_await_mode(&db.node, "tc-mode-cas-1").await,
        "background"
    );

    let mut stale_background =
        ToolCallLifecycle::load(db.node.clone(), "sess-mode-cas-1", "tc-mode-cas-1")
            .await
            .unwrap()
            .expect("stale background owner should load");
    lc.foreground().await.unwrap();
    stale_background.foreground().await.unwrap();
    assert_eq!(
        fetch_tool_call_await_mode(&db.node, "tc-mode-cas-1").await,
        "foreground"
    );
}

#[tokio::test]
async fn integration_detach_one_way_persists() {
    let db = test_db("tc-sa-det-1").await;

    let mut lc = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        "req-det-1".to_string(),
        "sess-det-1".to_string(),
        "did:test:test".to_string(),
        "tc-det-1".to_string(),
        1,
        "spawn_subagent".to_string(),
        "{}".to_string(),
        test_deadline(),
        AwaitMode::Foreground,
        CancelPolicy::Cascade,
        "child-req-det-1".to_string(),
        "did:test:target".to_string(),
    );
    lc.start_running().await.unwrap();

    lc.detach().await.unwrap();

    let resp = db
        .node
        .execute(
            r#"{ AgentToolCall(filter: { tool_call_id: { _eq: "tc-det-1" } }) {
                cancel_policy
                child_request_id
                request_id
            } }"#,
        )
        .await;
    assert!(!resp.has_errors(), "query failed: {:?}", resp.errors);
    let data = resp.data.expect("data");
    let rows = data["AgentToolCall"].as_array().expect("array");
    assert_eq!(rows.len(), 1, "expected one row");
    let row = &rows[0];

    assert_eq!(
        row["cancel_policy"].as_str(),
        Some("detach"),
        "cancel_policy should be persisted as 'detach' after detach()"
    );
    assert_eq!(
        row["child_request_id"].as_str(),
        Some("child-req-det-1"),
        "a detached tool must persist a non-null child_request_id \
         (Persistent: childRequestId.isSome)"
    );
    assert_eq!(
        row["request_id"].as_str(),
        Some("req-det-1"),
        "a detached tool must stay linked to its parent request \
         (Persistent: requestId = parent request id)"
    );

    let err = lc.detach().await.unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<IllegalToolCallTransition>(),
            Some(IllegalToolCallTransition::PolicyAlreadyDetach)
        ),
        "expected PolicyAlreadyDetach on second detach() call, got: {err:?}"
    );
}

#[tokio::test]
async fn integration_detach_rejects_native_tool() {
    let db = test_db("tc-sa-det-native-1").await;

    let mut lc = ToolCallLifecycle::new(
        db.node.clone(),
        "req-det-native-1".to_string(),
        "sess-det-native-1".to_string(),
        "did:test:test".to_string(),
        "tc-det-native-1".to_string(),
        0,
        "echo".to_string(),
        "{}".to_string(),
        test_deadline(),
    );
    lc.start_running().await.unwrap();

    let err = lc.detach().await.unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<IllegalToolCallTransition>(),
            Some(IllegalToolCallTransition::DetachRequiresChildLink)
        ),
        "expected DetachRequiresChildLink when detaching a native tool, got: {err:?}"
    );

    let resp = db
        .node
        .execute(
            r#"{ AgentToolCall(filter: { tool_call_id: { _eq: "tc-det-native-1" } }) {
                cancel_policy
                child_request_id
            } }"#,
        )
        .await;
    assert!(!resp.has_errors(), "query failed: {:?}", resp.errors);
    let data = resp.data.expect("data");
    let rows = data["AgentToolCall"].as_array().expect("array");
    assert_eq!(rows.len(), 1);
    assert_ne!(
        rows[0]["cancel_policy"].as_str(),
        Some("detach"),
        "a rejected detach must not persist cancel_policy=detach"
    );
    assert!(
        rows[0]["child_request_id"].is_null(),
        "native tool must remain child-less after a rejected detach"
    );
}

#[tokio::test]
async fn test_make_completed_request_creates_row() {
    let db = test_db("tc-sa-sanity-1").await;

    make_completed_request(&db.node, "req-sanity-1", None, None, "all done")
        .await
        .unwrap();

    let query = r#"{
        AgentRequest(filter: { request_id: { _eq: "req-sanity-1" } }) {
            request_id
            lifecycle_state
            subagent_depth
        }
    }"#;
    let resp = db.node.execute(query).await;
    assert!(!resp.has_errors(), "query failed: {:?}", resp.errors);

    let data = resp.data.expect("data");
    let rows = data["AgentRequest"].as_array().expect("AgentRequest array");
    assert_eq!(rows.len(), 1, "expected exactly one AgentRequest row");

    let row = &rows[0];
    assert_eq!(
        row["lifecycle_state"].as_str(),
        Some("completed"),
        "expected lifecycle_state to be 'completed', got: {:?}",
        row["lifecycle_state"]
    );
    assert_eq!(
        row["subagent_depth"].as_i64(),
        Some(0),
        "top-level request should have subagent_depth 0"
    );
}

#[tokio::test]
async fn test_make_completed_request_child_sets_depth_and_parent_fields() {
    let db = test_db("tc-sa-sanity-2").await;

    make_completed_request(
        &db.node,
        "req-sanity-child-1",
        Some("req-parent-1"),
        Some("tc-parent-1"),
        "child done",
    )
    .await
    .unwrap();

    let query = r#"{
        AgentRequest(filter: { request_id: { _eq: "req-sanity-child-1" } }) {
            request_id
            lifecycle_state
            subagent_depth
            caused_by_parent_request_id
            caused_by_parent_tool_call_id
        }
    }"#;
    let resp = db.node.execute(query).await;
    assert!(!resp.has_errors(), "query failed: {:?}", resp.errors);

    let data = resp.data.expect("data");
    let rows = data["AgentRequest"].as_array().expect("AgentRequest array");
    assert_eq!(rows.len(), 1, "expected one row");

    let row = &rows[0];
    assert_eq!(
        row["lifecycle_state"].as_str(),
        Some("completed"),
        "lifecycle_state should be completed"
    );
    assert_eq!(
        row["subagent_depth"].as_i64(),
        Some(1),
        "child request should have subagent_depth 1"
    );
    assert_eq!(
        row["caused_by_parent_request_id"].as_str(),
        Some("req-parent-1"),
        "parent request id should be set"
    );
    assert_eq!(
        row["caused_by_parent_tool_call_id"].as_str(),
        Some("tc-parent-1"),
        "parent tool call id should be set"
    );
}

#[tokio::test]
async fn integration_bridge_complete_with_real_child() {
    let db = test_db("tc-sa-bc-1").await;

    let child_request_id = "child-bc-1";
    make_completed_request(
        &db.node,
        child_request_id,
        Some("parent-req-bc1"),
        Some("parent-tc-bc1"),
        "child final assistant message",
    )
    .await
    .unwrap();

    let mut bridge = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        "parent-req-bc1".to_string(),
        "sess-bc1".to_string(),
        "did:test:test".to_string(),
        "tc-bc1".to_string(),
        1,
        "spawn_subagent".to_string(),
        "{}".to_string(),
        test_deadline(),
        AwaitMode::Foreground,
        CancelPolicy::Cascade,
        child_request_id.to_string(),
        "did:test:target".to_string(),
    );
    bridge.start_running().await.unwrap();

    let projected_output = "child final assistant message".to_string();
    let transitioned = bridge
        .bridge_complete(projected_output.clone())
        .await
        .unwrap();
    assert!(
        transitioned,
        "bridge_complete should win the running compare"
    );

    let resp = db
        .node
        .execute(
            r#"{ AgentToolCall(filter: { tool_call_id: { _eq: "tc-bc1" } }) {
            lifecycle_state
            result
            child_request_id
        } }"#,
        )
        .await;
    assert!(!resp.has_errors(), "query failed: {:?}", resp.errors);

    let data = resp.data.expect("data");
    let rows = data["AgentToolCall"].as_array().expect("array");
    assert_eq!(rows.len(), 1, "expected one AgentToolCall row");

    let row = &rows[0];
    assert_eq!(
        row["lifecycle_state"].as_str(),
        Some("completed"),
        "lifecycle_state should be 'completed' after bridge_complete"
    );
    assert_eq!(
        row["result"].as_str(),
        Some("child final assistant message"),
        "result should be the projected child output"
    );
    assert_eq!(
        row["child_request_id"].as_str(),
        Some(child_request_id),
        "child_request_id should be persisted from start_running"
    );
}

#[tokio::test]
async fn integration_bridge_complete_does_not_overwrite_externally_terminal_bridge() {
    let db = test_db("tc-sa-bc-cas-1").await;
    let child_request_id = "child-bc-cas-1";
    make_completed_request(
        &db.node,
        child_request_id,
        Some("parent-req-bc-cas1"),
        Some("parent-tc-bc-cas1"),
        "child final assistant message",
    )
    .await
    .unwrap();

    let mut bridge = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        "parent-req-bc-cas1".to_string(),
        "sess-bc-cas1".to_string(),
        "did:test:test".to_string(),
        "tc-bc-cas1".to_string(),
        1,
        "spawn_subagent".to_string(),
        "{}".to_string(),
        test_deadline(),
        AwaitMode::Foreground,
        CancelPolicy::Cascade,
        child_request_id.to_string(),
        "did:test:target".to_string(),
    );
    bridge.start_running().await.unwrap();
    let mut external = ToolCallLifecycle::load(db.node.clone(), "sess-bc-cas1", "tc-bc-cas1")
        .await
        .unwrap()
        .expect("external owner should reload running bridge");
    external
        .cancel_during_run(CancelCause::Interrupted)
        .await
        .unwrap();

    let transitioned = bridge
        .bridge_complete("late child answer".to_string())
        .await
        .unwrap();
    assert!(
        !transitioned,
        "bridge_complete should lose the persisted running compare"
    );
    let (state, result) = fetch_tool_call_state_and_result(&db.node, "tc-bc-cas1").await;
    assert_eq!(state, "cancelled");
    assert_eq!(result, "tool call cancelled");
}

async fn run_bridge_failure_case(
    name_suffix: &str,
    terminal_state: &str,
    child_terminal: ChildTerminal,
    expected_lifecycle_state: &str,
) {
    let db = test_db(&format!("tc-sa-bf-{name_suffix}")).await;
    let session = format!("sess-bf-{name_suffix}");
    let tc_id = format!("tc-bf-{name_suffix}");
    let child_id = format!("child-req-bf-{name_suffix}");
    let parent_req = format!("parent-req-bf-{name_suffix}");
    let parent_tc = format!("parent-tc-bf-{name_suffix}");

    make_terminal_request(
        &db.node,
        &child_id,
        Some(&parent_req),
        Some(&parent_tc),
        terminal_state,
    )
    .await
    .unwrap();

    let mut bridge = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        parent_req.clone(),
        session.clone(),
        "did:test:test".to_string(),
        tc_id.clone(),
        1,
        "spawn_subagent".to_string(),
        "{}".to_string(),
        test_deadline(),
        AwaitMode::Foreground,
        CancelPolicy::Cascade,
        child_id,
        "did:test:target".to_string(),
    );
    bridge.start_running().await.unwrap();
    let transitioned = bridge.bridge_failure(child_terminal).await.unwrap();
    assert!(
        transitioned,
        "bridge_failure should win the running compare for {terminal_state}"
    );

    let tc_id_escaped = gents::graphql::escape_graphql_string(&tc_id);
    let query = format!(
        r#"{{ AgentToolCall(filter: {{ tool_call_id: {{ _eq: "{tc_id_escaped}" }} }}) {{ lifecycle_state }} }}"#
    );
    let resp = db.node.execute(&query).await;
    assert!(!resp.has_errors(), "query failed: {:?}", resp.errors);

    let data = resp.data.expect("data");
    let rows = data["AgentToolCall"].as_array().expect("array");
    assert_eq!(
        rows.len(),
        1,
        "expected one AgentToolCall row for {terminal_state}"
    );
    assert_eq!(
        rows[0]["lifecycle_state"].as_str(),
        Some(expected_lifecycle_state),
        "Expected lifecycle_state '{}' for child terminal '{}', got: {:?}",
        expected_lifecycle_state,
        terminal_state,
        rows[0]["lifecycle_state"]
    );
}

#[tokio::test]
async fn integration_bridge_failure_does_not_overwrite_externally_terminal_bridge() {
    let db = test_db("tc-sa-bf-cas-1").await;
    let child_id = "child-req-bf-cas-1";
    make_terminal_request(
        &db.node,
        child_id,
        Some("parent-req-bf-cas1"),
        Some("parent-tc-bf-cas1"),
        "dead",
    )
    .await
    .unwrap();

    let mut bridge = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        "parent-req-bf-cas1".to_string(),
        "sess-bf-cas1".to_string(),
        "did:test:test".to_string(),
        "tc-bf-cas1".to_string(),
        1,
        "spawn_subagent".to_string(),
        "{}".to_string(),
        test_deadline(),
        AwaitMode::Foreground,
        CancelPolicy::Cascade,
        child_id.to_string(),
        "did:test:target".to_string(),
    );
    bridge.start_running().await.unwrap();
    let mut external = ToolCallLifecycle::load(db.node.clone(), "sess-bf-cas1", "tc-bf-cas1")
        .await
        .unwrap()
        .expect("external owner should reload running bridge");
    assert!(
        external
            .bridge_complete("already completed".to_string())
            .await
            .unwrap(),
        "external owner should terminalize the bridge"
    );

    let transitioned = bridge.bridge_failure(ChildTerminal::Dead).await.unwrap();
    assert!(
        !transitioned,
        "bridge_failure should lose the persisted running compare"
    );
    let (state, result) = fetch_tool_call_state_and_result(&db.node, "tc-bf-cas1").await;
    assert_eq!(state, "completed");
    assert_eq!(result, "already completed");
}

#[tokio::test]
async fn integration_bridge_failure_failed_projects_to_failed() {
    run_bridge_failure_case(
        "failed",
        "failed",
        ChildTerminal::Failed {
            reason: "child failed".to_string(),
            failure_class: FailureClass::External,
        },
        "failed",
    )
    .await;
}

#[tokio::test]
async fn integration_bridge_failure_dead_projects_to_failed() {
    run_bridge_failure_case("dead", "dead", ChildTerminal::Dead, "failed").await;
}

#[tokio::test]
async fn integration_bridge_failure_interrupted_projects_to_cancelled() {
    run_bridge_failure_case(
        "interrupted",
        "interrupted",
        ChildTerminal::Interrupted,
        "cancelled",
    )
    .await;
}

#[tokio::test]
async fn integration_bridge_failure_superseded_projects_to_failed() {
    run_bridge_failure_case(
        "superseded",
        "superseded",
        ChildTerminal::Superseded,
        "failed",
    )
    .await;
}

#[tokio::test]
async fn integration_cascade_intent_for_cascade_subagent_returns_some() {
    let db = test_db("tc-sa-cas-1").await;
    let mut lc = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        "req-cas-1".to_string(),
        "sess-cas-1".to_string(),
        "did:test:test".to_string(),
        "tc-cas-1".to_string(),
        1,
        "spawn_subagent".to_string(),
        "{}".to_string(),
        test_deadline(),
        AwaitMode::Foreground,
        CancelPolicy::Cascade,
        "child-req-cas-1".to_string(),
        "did:test:target".to_string(),
    );
    lc.start_running().await.unwrap();
    lc.cancel_during_run(CancelCause::Interrupted)
        .await
        .unwrap();
    let intent = lc.bridge_cancel_cascade().await.unwrap();
    assert!(intent.is_some());
    assert_eq!(intent.unwrap().child_request_id, "child-req-cas-1");
}

#[tokio::test]
async fn integration_cascade_intent_for_detached_subagent_returns_none() {
    let db = test_db("tc-sa-cas-2").await;
    let mut lc = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        "req-cas-2".to_string(),
        "sess-cas-2".to_string(),
        "did:test:test".to_string(),
        "tc-cas-2".to_string(),
        1,
        "spawn_subagent".to_string(),
        "{}".to_string(),
        test_deadline(),
        AwaitMode::Foreground,
        CancelPolicy::Detach,
        "child-req-cas-2".to_string(),
        "did:test:target".to_string(),
    );
    lc.start_running().await.unwrap();
    lc.cancel_during_run(CancelCause::Interrupted)
        .await
        .unwrap();
    let intent = lc.bridge_cancel_cascade().await.unwrap();
    assert!(intent.is_none(), "Detach policy returns None");
}

#[tokio::test]
async fn integration_cascade_intent_for_native_returns_none() {
    let db = test_db("tc-sa-cas-3").await;
    let mut lc = ToolCallLifecycle::new(
        db.node.clone(),
        "req-cas-3".to_string(),
        "sess-cas-3".to_string(),
        "did:test:test".to_string(),
        "tc-cas-3".to_string(),
        1,
        "echo".to_string(),
        "{}".to_string(),
        test_deadline(),
    );
    lc.start_running().await.unwrap();
    lc.cancel_during_run(CancelCause::Interrupted)
        .await
        .unwrap();
    let intent = lc.bridge_cancel_cascade().await.unwrap();
    assert!(
        intent.is_none(),
        "Native tool (no child_request_id) returns None"
    );
}

#[tokio::test]
async fn test_make_terminal_request_all_states() {
    let db = test_db("tc-sa-sanity-3").await;

    for (idx, state) in ChildTerminal::ALL_KIND.iter().enumerate() {
        let rid = format!("req-terminal-{idx}");
        make_terminal_request(
            &db.node,
            &rid,
            Some("req-parent-x"),
            Some("tc-parent-x"),
            state,
        )
        .await
        .unwrap_or_else(|e| panic!("make_terminal_request({state}) failed: {e}"));

        let rid_escaped = gents::graphql::escape_graphql_string(&rid);
        let query = format!(
            r#"{{
                AgentRequest(filter: {{ request_id: {{ _eq: "{rid_escaped}" }} }}) {{
                    lifecycle_state
                }}
            }}"#
        );
        let resp = db.node.execute(&query).await;
        assert!(
            !resp.has_errors(),
            "query failed for state {state}: {:?}",
            resp.errors
        );

        let data = resp.data.expect("data");
        let rows = data["AgentRequest"].as_array().expect("array");
        assert_eq!(rows.len(), 1, "expected one row for state {state}");
        assert_eq!(
            rows[0]["lifecycle_state"].as_str(),
            Some(*state),
            "expected lifecycle_state={state}"
        );
    }
}

// ---------------------------------------------------------------------------
// Bucket 3 / Task 26 — migration round-trip default-population behavior
// ---------------------------------------------------------------------------
//
// Approach (a) per Task 26: the standard `test_db` already registers the v3
// schema directly (no patch needed because the SDL ships with the v3 fields).
// We insert a minimal AgentToolCall row with only the v1/v2 fields and verify
// what the schema does for the unset v3 fields (`await_mode`, `cancel_policy`,
// `child_request_id`, `request_id`).
//
// The point of the test is to lock in the actual observed behavior so that any
// future drift — whether DefraDB starts materializing schema defaults on insert,
// or vice-versa — is caught by a deliberate signal rather than silent
// regressions in dependent code.

#[tokio::test]
async fn integration_v3_schema_defaults_populate_correctly() {
    let db = test_db("tc-sa-mig-1").await;

    let mutation = r#"mutation {
        create_AgentToolCall(input: {
            tool_call_key: "mig-test-1",
            session_id: "mig-sess-1",
            message_sequence: 1,
            tool_name: "echo",
            tool_call_id: "mig-tc-1",
            args: "{}",
            lifecycle_state: "running"
        }) { _docID }
    }"#;
    let resp = db.node.execute(mutation).await;
    assert!(
        !resp.has_errors(),
        "create_AgentToolCall (minimal) failed: {:?}",
        resp.errors
    );

    let query = r#"{
        AgentToolCall(filter: { tool_call_key: { _eq: "mig-test-1" } }) {
            tool_call_key
            lifecycle_state
            await_mode
            cancel_policy
            child_request_id
            request_id
        }
    }"#;
    let resp = db.node.execute(query).await;
    assert!(!resp.has_errors(), "query failed: {:?}", resp.errors);

    let data = resp.data.expect("data");
    let rows = data["AgentToolCall"].as_array().expect("array");
    assert_eq!(rows.len(), 1, "expected exactly one row");
    let row = &rows[0];

    eprintln!("v3 schema default-population observed behavior: {row}");

    assert_eq!(row["tool_call_key"].as_str(), Some("mig-test-1"));
    assert_eq!(row["lifecycle_state"].as_str(), Some("running"));

    // Lock in the observed behavior for the v3 fields.
    //
    // DefraDB's GraphQL @branchable schema does not materialize schema-level
    // defaults onto directly-inserted rows: the SDL only declares each new
    // field as a nullable `String`. The lens transform (registered by
    // `ensure_subagent_extensions_migrations` in the daemon path) is what
    // populates the v3 defaults onto pre-existing v2 rows on read. New rows
    // inserted directly without these fields therefore observe Null for each
    // unset v3 field.
    //
    // If a future change starts materializing defaults on insert, this test
    // will fail — at which point the assertion should be flipped to the new
    // observed values and a comment added explaining the change.
    assert!(
        row["await_mode"].is_null(),
        "directly-inserted v3 row: await_mode expected null, got: {:?}",
        row["await_mode"]
    );
    assert!(
        row["cancel_policy"].is_null(),
        "directly-inserted v3 row: cancel_policy expected null, got: {:?}",
        row["cancel_policy"]
    );
    assert!(
        row["child_request_id"].is_null(),
        "directly-inserted v3 row: child_request_id expected null, got: {:?}",
        row["child_request_id"]
    );
    assert!(
        row["request_id"].is_null(),
        "directly-inserted v3 row: request_id expected null, got: {:?}",
        row["request_id"]
    );
}

#[tokio::test]
async fn integration_create_subagent_request_at_max_depth_succeeds() {
    let db = crate::signed_materializer_test_db("tc-sa-csr-1").await;
    crate::support::create_request(
        &db.node,
        "parent-req-csr-1",
        "parent-sess-csr-1",
        "processing",
        &chrono::Utc::now().to_rfc3339(),
    )
    .await;

    let new_id = create_subagent_request(
        &db.node,
        "parent-req-csr-1".to_string(),
        "parent-tc-csr-1".to_string(),
        MAX_SUBAGENT_DEPTH - 1,
        crate::support::AGENT_DID.to_string(),
        "behavior-csr-1".to_string(),
        "csr test prompt".to_string(),
        None,
    )
    .await
    .expect("create_subagent_request at MAX-1 should succeed");

    assert!(!new_id.is_empty(), "expected non-empty request_id");
    let new_id_escaped = gents::graphql::escape_graphql_string(&new_id);
    let query = format!(
        r#"{{
            AgentRequest(filter: {{ request_id: {{ _eq: "{new_id_escaped}" }} }}) {{
                request_id
                lifecycle_state
                subagent_depth
                caused_by_parent_request_id
                caused_by_parent_tool_call_id
                caused_by_trigger_id
                caused_by_trigger_kind
            }}
        }}"#
    );
    let resp = db.node.execute(&query).await;
    assert!(!resp.has_errors(), "query failed: {:?}", resp.errors);
    let data = resp.data.expect("data");
    let rows = data["AgentRequest"].as_array().expect("array");
    assert_eq!(rows.len(), 1, "expected exactly one row");
    let row = &rows[0];
    assert_eq!(row["lifecycle_state"].as_str(), Some("pending"));
    assert_eq!(
        row["subagent_depth"].as_i64(),
        Some(i64::from(MAX_SUBAGENT_DEPTH)),
        "child depth should be parent_depth + 1 = MAX_SUBAGENT_DEPTH"
    );
    assert_eq!(
        row["caused_by_parent_request_id"].as_str(),
        Some("parent-req-csr-1")
    );
    assert_eq!(
        row["caused_by_parent_tool_call_id"].as_str(),
        Some("parent-tc-csr-1")
    );
    assert_eq!(
        row["caused_by_trigger_id"].as_str(),
        Some("parent-tc-csr-1")
    );
    assert_eq!(row["caused_by_trigger_kind"].as_str(), Some("subagent"));
}

#[tokio::test]
async fn integration_create_subagent_request_above_max_depth_fails() {
    let db = test_db("tc-sa-csr-2").await;
    let err = create_subagent_request(
        &db.node,
        "parent-req-csr-2".to_string(),
        "parent-tc-csr-2".to_string(),
        MAX_SUBAGENT_DEPTH,
        crate::support::AGENT_DID.to_string(),
        "behavior-csr-2".to_string(),
        "csr test prompt".to_string(),
        None,
    )
    .await
    .expect_err("should reject parent_depth == MAX_SUBAGENT_DEPTH");
    assert!(
        matches!(
            err.downcast_ref::<IllegalToolCallTransition>(),
            Some(IllegalToolCallTransition::SubagentDepthExceeded)
        ),
        "expected SubagentDepthExceeded, got: {err:?}"
    );
}

#[tokio::test]
async fn integration_create_subagent_request_empty_parent_fields_fails() {
    let db = test_db("tc-sa-csr-3").await;

    let err = create_subagent_request(
        &db.node,
        "".to_string(),
        "parent-tc".to_string(),
        0,
        crate::support::AGENT_DID.to_string(),
        "behavior".to_string(),
        "prompt".to_string(),
        None,
    )
    .await
    .expect_err("empty parent_request_id should fail");
    assert!(
        matches!(
            err.downcast_ref::<IllegalToolCallTransition>(),
            Some(IllegalToolCallTransition::ParentLinkageIncoherent)
        ),
        "expected ParentLinkageIncoherent for empty parent_request_id, got: {err:?}"
    );

    let err = create_subagent_request(
        &db.node,
        "parent-req".to_string(),
        "".to_string(),
        0,
        crate::support::AGENT_DID.to_string(),
        "behavior".to_string(),
        "prompt".to_string(),
        None,
    )
    .await
    .expect_err("empty parent_tool_call_id should fail");
    assert!(
        matches!(
            err.downcast_ref::<IllegalToolCallTransition>(),
            Some(IllegalToolCallTransition::ParentLinkageIncoherent)
        ),
        "expected ParentLinkageIncoherent for empty parent_tool_call_id, got: {err:?}"
    );
}
