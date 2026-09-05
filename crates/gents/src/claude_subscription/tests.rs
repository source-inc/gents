use rig::completion::ToolDefinition;

use super::*;
use crate::llm::message::Message;
use crate::oauth_credential::test_support::{seed_credential, test_node};

fn request_from_native(
    preamble: Option<&str>,
    history: Vec<Message>,
    tools: Vec<ToolDefinition>,
) -> CompletionRequest {
    let rig_history = crate::llm::rig_compat::to_rig_messages(&history);
    CompletionRequest {
        model: None,
        preamble: preamble.map(str::to_string),
        chat_history: OneOrMany::many(rig_history).expect("at least one row"),
        documents: Vec::new(),
        tools,
        temperature: None,
        max_tokens: Some(128),
        tool_choice: None,
        additional_params: None,
        output_schema: None,
    }
}

/// Text-only request: empty surface, no preamble.
fn ping_request() -> CompletionRequest {
    request_from_native(None, vec![Message::user("ping")], Vec::new())
}

fn echo_tool_request() -> CompletionRequest {
    request_from_native(
        None,
        vec![Message::user("use echo")],
        vec![ToolDefinition {
            name: "echo".into(),
            description: "echo".into(),
            parameters: serde_json::json!({"type":"object","properties":{}}),
        }],
    )
}

/// Fixture-only client: the bearer refuses, so a turn that outruns the
/// fixture queue fails at the token read instead of reaching the network.
fn test_client() -> ClaudeSubscriptionClient<StaticBearer> {
    ClaudeSubscriptionClient::with_bearer(Arc::new(StaticBearer::failing("no credential")))
}

#[tokio::test]
async fn fixture_stream_maps_tool_use_without_touching_the_bearer() {
    let _guard = crate::claude_messages::lock_fixtures_for_test();
    crate::claude_messages::install_messages_sse_fixtures(vec![
        crate::claude_messages::sse_fixture_tool_use("toolu_1", "echo", "{\"text\":\"hi\"}"),
    ]);
    let client = test_client();
    let model = client.completion_model("claude-sonnet-5");
    let mut stream = model.stream(echo_tool_request()).await.expect("stream");
    use rig::streaming::StreamedAssistantContent;
    let mut calls = Vec::new();
    let mut usage = None;
    while let Some(item) = stream.next().await {
        match item.expect("chunk") {
            StreamedAssistantContent::ToolCall { tool_call, .. } => {
                calls.push((
                    tool_call.id,
                    tool_call.function.name,
                    tool_call.function.arguments,
                ));
            }
            StreamedAssistantContent::Final(final_response) => {
                usage = final_response.token_usage();
                break;
            }
            other => panic!("unexpected chunk: {other:?}"),
        }
    }
    assert_eq!(
        calls,
        vec![(
            "toolu_1".to_string(),
            "echo".to_string(),
            serde_json::json!({"text": "hi"})
        )]
    );
    let usage = usage.expect("usage from message_delta");
    assert_eq!((usage.input_tokens, usage.output_tokens), (10, 5));
    assert_eq!(
        client.bearer.bearer_calls(),
        0,
        "fixtures never consult the bearer"
    );
}

#[tokio::test]
async fn messages_http_fixture_streams_text_turn_on_empty_surface() {
    let _guard = crate::claude_messages::lock_fixtures_for_test();
    crate::claude_messages::install_messages_sse_fixtures(vec![
        crate::claude_messages::sse_fixture_final_text("pong"),
    ]);
    let model = test_client().completion_model("claude-sonnet-5");
    let response = model.completion(ping_request()).await.expect("text turn");
    let text = response
        .choice
        .iter()
        .filter_map(|content| match content {
            rig::completion::AssistantContent::Text(text) => Some(text.text.clone()),
            _ => None,
        })
        .collect::<String>();
    assert_eq!(text, "pong");
}

#[tokio::test]
async fn live_path_without_fixture_asks_the_bearer_and_fails_closed_on_bearer_error() {
    let _guard = crate::claude_messages::lock_fixtures_for_test();
    let client =
        ClaudeSubscriptionClient::with_bearer(Arc::new(StaticBearer::failing("no credential")));
    let model = client.completion_model("claude-sonnet-5");
    let err = model
        .completion(ping_request())
        .await
        .expect_err("bearer error");
    assert!(
        err.to_string()
            .contains("Claude Messages bearer: no credential"),
        "{err}"
    );
    assert_eq!(client.bearer.bearer_calls(), 1);
}

/// The queue serves one body per call; once drained the live path runs and
/// asks the bearer, which here refuses so no network is touched.
#[tokio::test]
async fn fixture_queue_serves_one_body_per_call_then_asks_the_bearer() {
    let _guard = crate::claude_messages::lock_fixtures_for_test();
    crate::claude_messages::install_messages_sse_fixtures(vec![
        crate::claude_messages::sse_fixture_final_text("one"),
    ]);
    let client =
        ClaudeSubscriptionClient::with_bearer(Arc::new(StaticBearer::failing("no credential")));
    let model = client.completion_model("claude-sonnet-5");
    model
        .completion(ping_request())
        .await
        .expect("first call served");
    assert_eq!(client.bearer.bearer_calls(), 0);
    let err = model
        .completion(ping_request())
        .await
        .expect_err("queue drained → live bearer read");
    assert!(
        err.to_string()
            .contains("Claude Messages bearer: no credential"),
        "{err}"
    );
    assert_eq!(client.bearer.bearer_calls(), 1);
}

#[tokio::test]
async fn build_without_a_credential_fails_closed_with_the_login_hint() {
    let node = test_node().await;
    let err = ClaudeSubscriptionClient::build(Arc::new(node), "did:key:z6MkNobody")
        .await
        .expect_err("missing");
    assert!(
        err.to_string()
            .contains("gents claude-login --agent-did did:key:z6MkNobody"),
        "{err:#}"
    );
}

#[tokio::test]
async fn build_with_a_seeded_credential_yields_a_shared_bearer() {
    let node = Arc::new(test_node().await);
    seed_credential(
        &node,
        "did:key:z6MkSeeded",
        crate::claude_oauth::CLAUDE_OAUTH_PROVIDER,
        chrono::Utc::now() + chrono::Duration::hours(8),
    )
    .await;
    let client = ClaudeSubscriptionClient::build(node.clone(), "did:key:z6MkSeeded")
        .await
        .expect("client");
    let again = ClaudeSubscriptionClient::build(node, "did:key:z6MkSeeded")
        .await
        .expect("client");
    assert!(
        Arc::ptr_eq(&client.bearer, &again.bearer),
        "one bearer per credential id"
    );
    assert_eq!(
        client.bearer.current_bearer().await.expect("token"),
        "access-TEST"
    );
}

/// Spec §6: a stale credential triggers exactly one refresh before the send.
/// Drives the real `OAuthRefreshKind::Claude` dispatch and the env-override
/// wrapper: the rotated token comes back as the bearer, and the second read
/// is served from cache (the one-shot server is gone by then, so a second
/// refresh would fail to connect).
#[tokio::test]
async fn stale_credential_refreshes_once_through_the_claude_token_endpoint() {
    let node = Arc::new(test_node().await);
    seed_credential(
        &node,
        "did:key:z6MkStaleRefresh",
        crate::claude_oauth::CLAUDE_OAUTH_PROVIDER,
        chrono::Utc::now() - chrono::Duration::minutes(1),
    )
    .await;
    let (url, handle) = crate::oauth_credential::test_support::one_shot_token_server(
        200,
        r#"{"access_token":"access-NEW","refresh_token":"refresh-NEW","expires_in":28800}"#,
    )
    .await;
    // Process-global, read by the wrapper at refresh time; no other lib test
    // refreshes a Claude credential, so nothing else observes it.
    std::env::set_var(
        crate::claude_oauth::CLAUDE_OAUTH_TOKEN_URL_OVERRIDE_ENV,
        &url,
    );
    let client = ClaudeSubscriptionClient::build(node, "did:key:z6MkStaleRefresh")
        .await
        .expect("client");
    let bearer = client.bearer.current_bearer().await;
    std::env::remove_var(crate::claude_oauth::CLAUDE_OAUTH_TOKEN_URL_OVERRIDE_ENV);
    assert_eq!(bearer.expect("token"), "access-NEW");

    let request = handle.await.expect("server");
    let body: serde_json::Value =
        serde_json::from_str(request.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    assert_eq!(body["grant_type"], "refresh_token");
    assert_eq!(body["refresh_token"], "refresh-TEST");
    assert_eq!(
        body["client_id"],
        crate::claude_oauth::CLAUDE_OAUTH_CLIENT_ID
    );

    assert_eq!(
        client.bearer.current_bearer().await.expect("cached"),
        "access-NEW",
        "a second read must not refresh again"
    );
}

#[test]
fn kind_round_trips_and_is_agent_scoped() {
    assert_eq!(
        crate::BackendProviderKind::parse_optional(Some("ClaudeCliSubscription")).unwrap(),
        crate::BackendProviderKind::ClaudeCliSubscription
    );
    assert!(crate::BackendProviderKind::parse_optional(Some("claude-cli-subscription")).is_err());
    assert_eq!(
        crate::BackendProviderKind::ClaudeCliSubscription.as_str(),
        "ClaudeCliSubscription"
    );
    assert!(crate::BackendProviderKind::ClaudeCliSubscription.is_agent_scoped_oauth());
}
