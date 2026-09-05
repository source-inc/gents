use chrono::{Duration, Utc};
use gents::graphql::escape_graphql_string;
use gents::tool_call_lifecycle::{
    create_subagent_request_with_request_id,
    create_subagent_request_with_trusted_parent_request_id, AwaitMode, CancelPolicy,
    ToolCallLifecycle,
};
use gents::{
    resolve_descendant_edge, resolve_descendant_graph, resolve_session_descendant_edge,
    resolve_session_descendant_graph, DescendantControlAuthority, DescendantGraphAccess,
    DescendantMaterializationState, DescendantQuery,
};

async fn create_request(
    node: &gents::defra_node::EmbeddedNode,
    request_id: &str,
    session_id: &str,
    agent_did: &str,
    behavior_id: &str,
) -> String {
    create_request_with_requester(node, request_id, session_id, agent_did, behavior_id, None).await
}

async fn create_request_with_requester(
    node: &gents::defra_node::EmbeddedNode,
    request_id: &str,
    session_id: &str,
    agent_did: &str,
    behavior_id: &str,
    requester: Option<&str>,
) -> String {
    let requester = requester
        .map(|did| format!("requester_did: \"{}\",", escape_graphql_string(did)))
        .unwrap_or_default();
    let request_id = escape_graphql_string(request_id);
    let session_id = escape_graphql_string(session_id);
    let agent_did = escape_graphql_string(agent_did);
    let behavior_id = escape_graphql_string(behavior_id);
    let now = Utc::now().to_rfc3339();
    let response = node
        .execute(&format!(
            r#"mutation {{
                create_AgentRequest(input: {{
                    request_id: "{request_id}",
                    agent_did: "{agent_did}",
                    {requester}
                    behavior_id: "{behavior_id}",
                    session_id: "{session_id}",
                    retry_parent_request: "",
                    retry_root_request: "{request_id}",
                    superseded_by_request: "",
                    content: "descendant graph root",
                    lifecycle_state: "processing",
                    backend_id: "",
                    execution_origin: "interactive",
                    metadata: "",
                    failure_reason: "",
                    created_at: "{now}",
                    retry_count: 0,
                    max_retries: 3,
                    subagent_depth: 0
                }}) {{ _docID }}
            }}"#
        ))
        .await;
    assert!(!response.has_errors(), "{:?}", response.errors);
    crate::support::exact_request_doc_id(node, &request_id).await
}

async fn create_nested_bridge(
    node: &gents::defra_node::EmbeddedNode,
    parent_request_doc_id: &str,
    parent_session_id: &str,
    requester_did: &str,
    worker_did: &str,
) -> String {
    let args = escape_graphql_string(r#"{"name":"reviewer","behavior_id":"reviewer"}"#);
    let now = Utc::now().to_rfc3339();
    let deadline = (Utc::now() + Duration::minutes(5)).to_rfc3339();
    let response = node
        .execute(&format!(
            r#"mutation {{
                create_AgentToolCall(input: {{
                    tool_call_key: "{parent_session_id}:call-reviewer",
                    request_id: "request-worker",
                    request_doc_id: "{parent_request_doc_id}",
                    session_id: "{parent_session_id}",
                    agent_did: "{worker_did}",
                    requester_did: "{requester_did}",
                    message_sequence: 1,
                    tool_name: "spawn_subagent",
                    tool_call_id: "call-reviewer",
                    args: "{args}",
                    result: "",
                    status: "called",
                    lifecycle_state: "running",
                    started_at: "{now}",
                    deadline_at: "{deadline}",
                    child_request_id: "request-reviewer",
                    spawn_target_did: "{worker_did}",
                    await_mode: "foreground",
                    cancel_policy: "cascade",
                    selected_service_id: null,
                    selected_tool_name: null,
                    tool_failure_class: null,
                    latency_ms: null
                }}) {{ _docID }}
            }}"#,
            parent_request_doc_id = escape_graphql_string(parent_request_doc_id),
            parent_session_id = escape_graphql_string(parent_session_id),
            requester_did = escape_graphql_string(requester_did),
            worker_did = escape_graphql_string(worker_did),
        ))
        .await;
    assert!(!response.has_errors(), "{:?}", response.errors);
    let response = node
        .execute(
            r#"{ AgentToolCall(filter: { tool_call_id: { _eq: "call-reviewer" } }, limit: 1) { _docID } }"#,
        )
        .await;
    assert!(!response.has_errors(), "{:?}", response.errors);
    response.data.expect("bridge query data")["AgentToolCall"][0]["_docID"]
        .as_str()
        .expect("reviewer bridge doc")
        .to_string()
}

#[tokio::test]
async fn canonical_graph_preserves_pending_remote_terminal_nested_and_paging_edges() {
    let db = crate::support::test_db("canonical-descendant-graph").await;
    let root_id = "graph-root";
    let root_session = "graph-root-session";
    let root_did = "did:test:coordinator";
    let worker_did = db.node_identity.did().to_string();
    let root_doc =
        create_request(db.node.as_ref(), root_id, root_session, root_did, "default").await;

    let deadline = Utc::now() + Duration::minutes(5);
    let mut worker_bridge = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        root_id.to_string(),
        root_session.to_string(),
        root_did.to_string(),
        "call-worker".to_string(),
        1,
        "spawn_subagent".to_string(),
        r#"{"name":"fast-worker","behavior_id":"fast-worker"}"#.to_string(),
        deadline,
        AwaitMode::Background,
        CancelPolicy::Cascade,
        "request-worker".to_string(),
        worker_did.clone(),
    )
    .with_request_doc_id(Some(root_doc.clone()));
    worker_bridge.start_running().await.unwrap();
    let worker_bridge_doc = worker_bridge.doc_id().unwrap().to_string();

    let pending = resolve_descendant_graph(
        DescendantGraphAccess::Local(db.node.as_ref()),
        &DescendantQuery::direct(root_id),
    )
    .await
    .unwrap();
    assert_eq!(pending.edges.len(), 1);
    assert_eq!(
        pending.edges[0].materialization_state,
        DescendantMaterializationState::AwaitingChild
    );
    assert_eq!(pending.edges[0].child_request_id, "request-worker");
    let pending_exact = resolve_descendant_edge(
        DescendantGraphAccess::Local(db.node.as_ref()),
        root_id,
        "request-worker",
    )
    .await
    .unwrap()
    .expect("pending exact edge");
    assert_eq!(pending_exact.depth, 1);
    assert!(!pending_exact.readable());
    assert!(pending_exact.retryable());
    let pending_cursor = pending.edges[0].cursor.clone();

    create_subagent_request_with_trusted_parent_request_id(
        db.node.as_ref(),
        "request-worker".to_string(),
        root_id.to_string(),
        root_doc.clone(),
        "call-worker".to_string(),
        worker_bridge_doc.clone(),
        0,
        worker_did.clone(),
        "fast-worker".to_string(),
        "work".to_string(),
        Some(deadline),
        root_did.to_string(),
    )
    .await
    .unwrap();
    let worker_doc = crate::support::exact_request_doc_id(db.node.as_ref(), "request-worker").await;

    let materialized = resolve_descendant_graph(
        DescendantGraphAccess::Local(db.node.as_ref()),
        &DescendantQuery::direct(root_id),
    )
    .await
    .unwrap();
    assert_eq!(materialized.edges[0].cursor, pending_cursor);
    assert!(materialized.edges[0].readable());
    assert_eq!(
        materialized.edges[0].behavior_id.as_deref(),
        Some("fast-worker")
    );
    assert_eq!(
        materialized.edges[0].materialization_state,
        DescendantMaterializationState::MaterializedRemote
    );

    // A new user request has no caused_by_parent lineage, but owns the same
    // conversation. Model-facing tools must retain access without rewriting
    // the original parent or fabricating a continuation edge.
    create_request(
        db.node.as_ref(),
        "later-user-turn",
        root_session,
        root_did,
        "other-behavior",
    )
    .await;
    let access = DescendantGraphAccess::Local(db.node.as_ref());
    let later =
        resolve_session_descendant_graph(access, &DescendantQuery::direct("later-user-turn"))
            .await
            .unwrap();
    assert_eq!(later.edges.len(), 1);
    assert_eq!(later.edges[0].immediate_parent_request_id, root_id);
    assert!(later.edges[0].controllable());
    let listed = gents::__test_internals::handle_list_subagents(
        db.node.as_ref(),
        "later-user-turn",
        serde_json::from_value(serde_json::json!({"status":"all"})).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(listed.entries.len(), 1);
    assert!(gents::__test_internals::handle_read_subagent(
        db.node.as_ref(),
        "later-user-turn",
        serde_json::from_value(serde_json::json!({"child_request_id":"request-worker"})).unwrap()
    )
    .await
    .unwrap()
    .is_some());
    assert!(matches!(
        gents::__test_internals::load_steer_subagent_target(
            db.node.as_ref(),
            "later-user-turn",
            "request-worker"
        )
        .await
        .unwrap(),
        gents::__test_internals::SteerSubagentTarget::Found(_)
    ));

    for (id, session, agent, requester) in [
        ("other-session", "foreign", root_did, None),
        ("other-agent", root_session, "did:test:foreign", None),
        (
            "other-requester",
            root_session,
            root_did,
            Some("did:test:foreign"),
        ),
        ("blank-requester", root_session, root_did, Some("")),
    ] {
        create_request_with_requester(db.node.as_ref(), id, session, agent, "default", requester)
            .await;
        assert!(
            resolve_session_descendant_graph(access, &DescendantQuery::all(id))
                .await
                .unwrap()
                .edges
                .is_empty()
        );
        assert!(
            resolve_session_descendant_edge(access, id, "request-worker")
                .await
                .unwrap()
                .is_none()
        );
    }

    let worker_session = materialized.edges[0]
        .child_session_id
        .clone()
        .expect("worker session");
    let reviewer_bridge_doc = create_nested_bridge(
        db.node.as_ref(),
        &worker_doc,
        &worker_session,
        &worker_did,
        &worker_did,
    )
    .await;
    create_subagent_request_with_request_id(
        db.node.as_ref(),
        "request-reviewer".to_string(),
        "request-worker".to_string(),
        worker_doc,
        "call-reviewer".to_string(),
        reviewer_bridge_doc,
        1,
        worker_did,
        "reviewer".to_string(),
        "review".to_string(),
        Some(deadline),
    )
    .await
    .unwrap();

    worker_bridge
        .bridge_complete("durable worker result".to_string())
        .await
        .unwrap();

    let all = resolve_descendant_graph(
        DescendantGraphAccess::Local(db.node.as_ref()),
        &DescendantQuery::all(root_id),
    )
    .await
    .unwrap();
    assert_eq!(all.edges.len(), 2);
    assert!(all.edges[0].terminal_result_ref.is_some());
    assert_eq!(all.edges[1].depth, 2);
    assert_eq!(
        all.edges[1].control_authority,
        DescendantControlAuthority::VisibilityOnly
    );

    let direct_exact = resolve_descendant_edge(
        DescendantGraphAccess::Local(db.node.as_ref()),
        root_id,
        "request-worker",
    )
    .await
    .unwrap()
    .expect("direct exact edge");
    assert_eq!(direct_exact.depth, 1);
    assert!(direct_exact.readable());
    let nested_exact = resolve_descendant_edge(
        DescendantGraphAccess::Local(db.node.as_ref()),
        root_id,
        "request-reviewer",
    )
    .await
    .unwrap()
    .expect("nested exact edge");
    assert_eq!(nested_exact.depth, 2);
    assert!(nested_exact.readable());
    assert_eq!(
        nested_exact.control_authority,
        DescendantControlAuthority::VisibilityOnly
    );
    assert!(resolve_descendant_edge(
        DescendantGraphAccess::Local(db.node.as_ref()),
        root_id,
        "unrelated-child",
    )
    .await
    .unwrap()
    .is_none());
    assert!(
        resolve_descendant_edge(
            DescendantGraphAccess::Local(db.node.as_ref()),
            "request-reviewer",
            "request-worker",
        )
        .await
        .unwrap()
        .is_none(),
        "a descendant root cannot enumerate an ancestor edge"
    );

    let first = resolve_descendant_graph(
        DescendantGraphAccess::Local(db.node.as_ref()),
        &DescendantQuery {
            limit: 1,
            ..DescendantQuery::all(root_id)
        },
    )
    .await
    .unwrap();
    assert!(first.has_more);
    let second = resolve_descendant_graph(
        DescendantGraphAccess::Local(db.node.as_ref()),
        &DescendantQuery {
            after: first.next_cursor,
            limit: 1,
            ..DescendantQuery::all(root_id)
        },
    )
    .await
    .unwrap();
    assert_eq!(second.edges[0].child_request_id, "request-reviewer");
    let later_nested =
        resolve_session_descendant_edge(access, "later-user-turn", "request-reviewer")
            .await
            .unwrap()
            .unwrap();
    assert!(later_nested.readable());
    assert!(
        !later_nested.controllable(),
        "session access does not grant ancestor control"
    );
    let later_all =
        resolve_session_descendant_graph(access, &DescendantQuery::all("later-user-turn"))
            .await
            .unwrap();
    assert_eq!(later_all.edges.len(), 2);

    // A later turn can spawn another child: enumeration is the union of the
    // owning requests, with a stable cursor that survives a new user turn.
    let later_doc = crate::support::exact_request_doc_id(db.node.as_ref(), "later-user-turn").await;
    let mut later_bridge = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        "later-user-turn".into(),
        root_session.into(),
        root_did.into(),
        "later-worker-call".into(),
        1,
        "spawn_subagent".into(),
        r#"{"name":"later-worker"}"#.into(),
        deadline,
        AwaitMode::Background,
        CancelPolicy::Cascade,
        "later-child".into(),
        root_did.into(),
    )
    .with_request_doc_id(Some(later_doc));
    later_bridge.start_running().await.unwrap();
    create_request(
        db.node.as_ref(),
        "third-user-turn",
        root_session,
        root_did,
        "default",
    )
    .await;
    let page1 = resolve_session_descendant_graph(
        access,
        &DescendantQuery {
            limit: 1,
            ..DescendantQuery::all("later-user-turn")
        },
    )
    .await
    .unwrap();
    let page2 = resolve_session_descendant_graph(
        access,
        &DescendantQuery {
            after: page1.next_cursor.clone(),
            limit: 100,
            ..DescendantQuery::all("third-user-turn")
        },
    )
    .await
    .unwrap();
    assert!(page1.has_more);
    assert_eq!(page1.edges.len() + page2.edges.len(), 3);
    assert!(page2
        .edges
        .iter()
        .all(|edge| edge.cursor != page1.edges[0].cursor));
}

#[tokio::test]
async fn running_page_cursor_survives_anchor_becoming_terminal() {
    let db = crate::support::test_db("descendant-running-cursor-terminal-transition").await;
    let root_id = "cursor-root";
    let root_session = "cursor-root-session";
    let root_did = "did:test:cursor-root";
    let root_doc =
        create_request(db.node.as_ref(), root_id, root_session, root_did, "default").await;
    let deadline = Utc::now() + Duration::minutes(5);

    let mut first = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        root_id.to_string(),
        root_session.to_string(),
        root_did.to_string(),
        "cursor-call-a".to_string(),
        1,
        "spawn_subagent".to_string(),
        r#"{"name":"worker-a","behavior_id":"worker-a"}"#.to_string(),
        deadline,
        AwaitMode::Background,
        CancelPolicy::Cascade,
        "cursor-child-a".to_string(),
        root_did.to_string(),
    )
    .with_request_doc_id(Some(root_doc.clone()));
    first.start_running().await.unwrap();

    let mut second = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        root_id.to_string(),
        root_session.to_string(),
        root_did.to_string(),
        "cursor-call-b".to_string(),
        2,
        "spawn_subagent".to_string(),
        r#"{"name":"worker-b","behavior_id":"worker-b"}"#.to_string(),
        deadline,
        AwaitMode::Background,
        CancelPolicy::Cascade,
        "cursor-child-b".to_string(),
        root_did.to_string(),
    )
    .with_request_doc_id(Some(root_doc));
    second.start_running().await.unwrap();

    let first_page = resolve_descendant_graph(
        DescendantGraphAccess::Local(db.node.as_ref()),
        &DescendantQuery {
            limit: 1,
            include_terminal: false,
            ..DescendantQuery::direct(root_id)
        },
    )
    .await
    .unwrap();
    assert_eq!(first_page.edges.len(), 1);
    assert!(first_page.has_more);
    let anchor_child = first_page.edges[0].child_request_id.clone();
    let after = first_page.next_cursor.expect("running page cursor");

    let terminalized = match anchor_child.as_str() {
        "cursor-child-a" => first
            .bridge_complete("worker a complete".to_string())
            .await
            .unwrap(),
        "cursor-child-b" => second
            .bridge_complete("worker b complete".to_string())
            .await
            .unwrap(),
        other => panic!("unexpected cursor anchor {other}"),
    };
    assert!(terminalized);

    let second_page = resolve_descendant_graph(
        DescendantGraphAccess::Local(db.node.as_ref()),
        &DescendantQuery {
            after: Some(after),
            limit: 1,
            include_terminal: false,
            ..DescendantQuery::direct(root_id)
        },
    )
    .await
    .expect("terminal transition must not invalidate the stable cursor anchor");
    assert_eq!(second_page.edges.len(), 1);
    assert_ne!(second_page.edges[0].child_request_id, anchor_child);
    assert!(!second_page.has_more);
}
