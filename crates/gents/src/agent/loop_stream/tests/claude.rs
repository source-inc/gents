/// Two Messages turns through the owned loop on SSE fixtures: `tool_use echo`
/// with streamed arguments → gents executes echo → `tool_result` continuation
/// → text `done`. Asserts the persisted `AgentToolCall.args` carries the
/// streamed arguments (defect C1, live-confirmed by write request #8).
#[tokio::test]
async fn claude_messages_tool_round_trip_through_owned_loop() {
    use crate::claude_messages::{
        install_messages_sse_fixtures, lock_fixtures_for_test, sse_fixture_final_text,
        sse_fixture_tool_use,
    };
    use crate::claude_subscription::{ClaudeSubscriptionClient, StaticBearer};
    use crate::rendered_request::scope::{ambient_arming_sink, scope_request, test_scope};
    use crate::rendered_request::{
        CaptureScopeKind, RenderedRequestCaptureSink, RenderedRequestContext,
    };
    use rig::client::CompletionClient;

    let _guard = lock_fixtures_for_test();
    install_messages_sse_fixtures(vec![
        sse_fixture_tool_use("toolu_1", "echo", "{\"text\":\"hi\"}"),
        sse_fixture_final_text("done"),
    ]);
    let (node, hook) = test_hook().await;

    let sink: RenderedRequestCaptureSink = Arc::new(|_| Box::pin(async { Ok(()) }));
    let scope = test_scope(
        RenderedRequestContext {
            request_doc_id: "doc-loop-claude".to_string(),
            request_commit_cid: "bafy-request-commit".to_string(),
            request_id: "req-loop-claude".to_string(),
            agent_did: "did:key:agent".to_string(),
            requester_did: String::new(),
            behavior_id: "general".to_string(),
            session_id: "session-loop-claude".to_string(),
            model_name: "claude-sonnet-5".to_string(),
        },
        sink,
    );
    let mut loop_config = config(4);
    loop_config.on_rendered_request = Some(ambient_arming_sink(CaptureScopeKind::Inference));
    // Fixture-only: a refusing bearer keeps an extra turn off the network.
    let model =
        ClaudeSubscriptionClient::with_bearer(Arc::new(StaticBearer::failing("no credential")))
            .completion_model("claude-sonnet-5");
    let tools: Arc<Vec<Box<dyn ToolDyn>>> = Arc::new(vec![echo_tool()]);

    let (tool_results, final_text) = scope_request(scope, async move {
        let stream = run_loop_stream(
            model,
            Some(hook),
            Message::user("use the echo tool"),
            Vec::new(),
            tools,
            loop_config,
        );
        futures::pin_mut!(stream);
        let mut tool_results = Vec::new();
        let mut final_text = None;
        while let Some(item) = stream.next().await {
            match item.expect("loop item should be Ok") {
                LoopStreamItem::Item(MultiTurnStreamItem::StreamUserItem(
                    StreamedUserContent::ToolResult { tool_result, .. },
                )) => {
                    tool_results.push(
                        tool_result_text(&crate::llm::rig_compat::from_rig_tool_result_content(
                            &tool_result.content.first(),
                        ))
                        .to_string(),
                    );
                }
                LoopStreamItem::Item(MultiTurnStreamItem::FinalResponse(final_response)) => {
                    final_text = Some(final_response.response().to_string());
                }
                _ => {}
            }
        }
        (tool_results, final_text)
    })
    .await;

    assert_eq!(tool_results, vec!["ECHOED".to_string()]);
    assert_eq!(final_text.as_deref(), Some("done"));

    let resp = node
        .execute("query { AgentToolCall { tool_name args lifecycle_state result } }")
        .await;
    assert!(
        !resp.has_errors(),
        "AgentToolCall query failed: {:?}",
        resp.errors
    );
    let rows = resp
        .data
        .as_ref()
        .and_then(|data| data.get("AgentToolCall"))
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    let echo = rows
        .iter()
        .find(|row| row.get("tool_name").and_then(|value| value.as_str()) == Some("echo"))
        .unwrap_or_else(|| panic!("expected an echo AgentToolCall; rows: {rows:?}"));
    assert_eq!(echo["lifecycle_state"], "completed");
    assert!(
        echo["result"]
            .as_str()
            .is_some_and(|result| result.contains("ECHOED")),
        "{echo}"
    );
    let args: serde_json::Value =
        serde_json::from_str(echo["args"].as_str().expect("args string")).expect("args json");
    assert_eq!(
        args,
        serde_json::json!({"text": "hi"}),
        "streamed arguments must persist"
    );
}
