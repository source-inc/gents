use std::collections::HashSet;

use bytes::Bytes;
use futures::StreamExt;
use rig::completion::{CompletionRequest, ToolDefinition};
use rig::http_client;
use rig::streaming::RawStreamingChoice;
use serde_json::{json, Value};

use super::*;
use crate::llm::message::{
    AssistantContent, Message, ToolCall, ToolFunction, ToolResultContent, UserContent,
};

/// Lean `ClaudeMap.identity` and Rust `CLAUDE_CODE_IDENTITY` are the same
/// bytes; the body-cases fence checks `system[0]` against the witness, so
/// this pins the constant even for a witness set with no rows.
#[test]
fn identity_matches_lean_body_witness_head() {
    let cases = crate::lean_vocab_test::lean_prompt_assembly_claude_body_cases();
    assert!(!cases.is_empty());
    for case in cases {
        assert_eq!(case.system[0], CLAUDE_CODE_IDENTITY, "{}", case.name);
    }
}

fn request_from_native(
    preamble: Option<&str>,
    history: Vec<Message>,
    tools: Vec<ToolDefinition>,
) -> CompletionRequest {
    let rig_history = crate::llm::rig_compat::to_rig_messages(&history);
    CompletionRequest {
        model: None,
        preamble: preamble.map(str::to_string),
        chat_history: rig::OneOrMany::many(rig_history).expect("at least one row"),
        documents: Vec::new(),
        tools,
        temperature: None,
        max_tokens: Some(128),
        tool_choice: None,
        additional_params: None,
        output_schema: None,
    }
}

fn echo_tool() -> ToolDefinition {
    ToolDefinition {
        name: "echo".into(),
        description: "echo".into(),
        parameters: json!({"type":"object","properties":{}}),
    }
}

fn echo_request() -> CompletionRequest {
    request_from_native(
        Some("You are helpful."),
        vec![Message::user("use echo")],
        vec![echo_tool()],
    )
}

fn request_with_system_rows() -> CompletionRequest {
    request_from_native(
        Some("You are helpful."),
        vec![
            Message::System {
                content: "workspace context".into(),
            },
            Message::user("use echo"),
        ],
        vec![echo_tool()],
    )
}

#[test]
fn messages_body_includes_gents_tools_and_system() {
    let body = build_messages_body("claude-sonnet-5", &echo_request());
    assert_eq!(body["model"], "claude-sonnet-5");
    assert_eq!(body["stream"], true);
    assert_eq!(body["max_tokens"], 128);
    assert_eq!(body["tools"][0]["name"], "echo");
    assert_eq!(body["system"][1]["text"], "You are helpful.");
    assert_eq!(body["messages"][0]["role"], "user");
    assert_messages_body_has_no_sampling(&body);
}

/// Messages HTTP leads `system` with the Claude Code identity block and
/// keeps the Gents preamble intact after it. Order matters: the identity
/// is a wire-level prefix, not a rewrite of what the loop assembled.
#[test]
fn messages_body_system_leads_with_claude_code_identity_then_preamble() {
    let body = build_messages_body("claude-sonnet-5", &echo_request());
    let system = body["system"].as_array().expect("system array");
    assert_eq!(system.len(), 2, "{body}");
    assert_eq!(system[0]["type"], "text");
    assert_eq!(system[0]["text"], CLAUDE_CODE_IDENTITY);
    assert_eq!(system[1]["type"], "text");
    assert_eq!(system[1]["text"], "You are helpful.");
}

#[test]
fn messages_body_system_is_identity_only_without_preamble() {
    for preamble in [None, Some(String::new()), Some("   ".to_string())] {
        let mut request = echo_request();
        request.preamble = preamble;
        let body = build_messages_body("claude-sonnet-5", &request);
        let system = body["system"].as_array().expect("system array");
        assert_eq!(system.len(), 1, "{body}");
        assert_eq!(system[0]["text"], CLAUDE_CODE_IDENTITY);
    }
}

#[test]
fn messages_body_routes_system_rows_after_identity_and_preamble() {
    let body = build_messages_body("claude-sonnet-5", &request_with_system_rows());
    let system = body["system"].as_array().expect("system");
    assert_eq!(system.len(), 3, "{body}");
    assert_eq!(system[0]["text"], CLAUDE_CODE_IDENTITY);
    assert_eq!(system[1]["text"], "You are helpful.");
    assert_eq!(system[2]["text"], "workspace context");
    assert_eq!(body["messages"].as_array().expect("messages").len(), 1);
    assert_eq!(body["messages"][0]["role"], "user");
}

#[test]
fn messages_body_marks_two_cache_breakpoints() {
    let body = build_messages_body("claude-sonnet-5", &request_with_system_rows());
    let system = body["system"].as_array().expect("system");
    assert_eq!(system.last().unwrap()["cache_control"]["type"], "ephemeral");
    assert!(system[0].get("cache_control").is_none());
    let last_message = body["messages"].as_array().unwrap().last().unwrap().clone();
    let last_block = last_message["content"]
        .as_array()
        .unwrap()
        .last()
        .unwrap()
        .clone();
    assert_eq!(last_block["cache_control"]["type"], "ephemeral");
}

#[test]
fn messages_body_never_carries_a_system_prefixed_user_block() {
    let body = build_messages_body("claude-sonnet-5", &request_with_system_rows());
    let leaked = body["messages"].as_array().unwrap().iter().any(|m| {
        m["content"].as_array().unwrap().iter().any(|b| {
            b["text"]
                .as_str()
                .is_some_and(|t| t.starts_with("system: "))
        })
    });
    assert!(!leaked, "{body}");
}

#[test]
fn messages_body_omits_sampling_even_when_request_sets_it() {
    let mut request = echo_request();
    request.temperature = Some(0.7);
    request.additional_params = Some(json!({
        "temperature": 0.2,
        "top_p": 0.9,
        "top_k": 40,
        "seed": 1,
        "min_p": 0.05,
        "frequency_penalty": 0.1,
        "presence_penalty": 0.1,
    }));
    let body = build_messages_body("claude-sonnet-5", &request);
    assert_messages_body_has_no_sampling(&body);
    assert_eq!(body["tools"][0]["name"], "echo");
}

/// Keys the Messages body may carry. Sampling (`temperature`, `top_p`,
/// `top_k`) and `additional_params` stay off the wire: live `claude-sonnet-5`
/// returns 400 "`temperature` is deprecated for this model".
const MESSAGES_BODY_ALLOWED_KEYS: &[&str] = &[
    "model",
    "max_tokens",
    "stream",
    "messages",
    "tools",
    "system",
];

fn assert_messages_body_has_no_sampling(body: &Value) {
    let keys: Vec<&String> = body.as_object().expect("object body").keys().collect();
    assert!(
        keys.iter()
            .all(|key| MESSAGES_BODY_ALLOWED_KEYS.contains(&key.as_str())),
        "unexpected Messages key: {body}"
    );
    for forbidden in [
        "temperature",
        "top_p",
        "top_k",
        "seed",
        "min_p",
        "frequency_penalty",
        "presence_penalty",
    ] {
        assert!(
            body.get(forbidden).is_none(),
            "{forbidden} must not appear on Claude Messages: {body}"
        );
    }
}

#[test]
fn messages_body_threads_tool_result() {
    let request = request_from_native(
        None,
        vec![
            Message::Assistant {
                id: None,
                content: vec![AssistantContent::ToolCall(ToolCall::new(
                    "toolu_1".into(),
                    ToolFunction::new("echo".into(), json!({})),
                ))],
            },
            Message::User {
                content: vec![UserContent::tool_result(
                    "toolu_1",
                    vec![ToolResultContent::text("ECHOED")],
                )],
            },
        ],
        vec![echo_tool()],
    );
    let body = build_messages_body("claude-sonnet-5", &request);
    assert_eq!(body["messages"][0]["content"][0]["type"], "tool_use");
    assert_eq!(body["messages"][1]["content"][0]["type"], "tool_result");
    assert_eq!(body["messages"][1]["content"][0]["content"], "ECHOED");
}

/// Lean `ClaudeMap.toolsField`: the wire never carries `tools: []`.
#[test]
fn messages_body_omits_tools_key_when_surface_is_empty() {
    let mut request = echo_request();
    request.tools.clear();
    let body = build_messages_body("claude-sonnet-5", &request);
    assert!(body.get("tools").is_none(), "{body}");
    let with_tools = build_messages_body("claude-sonnet-5", &echo_request());
    assert_eq!(with_tools["tools"][0]["name"], "echo");
    assert_eq!(with_tools["tools"].as_array().map(Vec::len), Some(1));
}

#[test]
fn sse_maps_echo_and_rejects_bash() {
    let sse = r#"
event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":"echo","input":{}}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: message_stop
data: {"type":"message_stop"}
"#;
    let surface = HashSet::from(["echo".to_string()]);
    let events = parse_messages_sse(sse, &surface).expect("map");
    assert!(matches!(
        &events[0],
        RawStreamingChoice::ToolCall(call) if call.name == "echo" && call.id == "toolu_1"
    ));

    let bash = sse.replace("echo", "Bash");
    let err = parse_messages_sse(&bash, &surface).expect_err("Bash");
    assert!(err.to_string().contains("Bash"), "{err}");
}

/// A `tool_use` block with an explicit start input and any number of
/// `input_json_delta` fragments; `sse_fixture_tool_use` covers the one-delta
/// `input: {}` shape Anthropic actually sends.
fn sse_tool_use_block(id: &str, name: &str, start_input: &str, deltas: &[&str]) -> String {
    let mut sse = format!(
        "event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{{\"type\":\"tool_use\",\"id\":\"{id}\",\"name\":\"{name}\",\"input\":{start_input}}}}}\n\n"
    );
    for partial in deltas {
        let escaped = serde_json::to_string(partial).expect("escape partial_json");
        sse.push_str(&format!(
            "event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"type\":\"input_json_delta\",\"partial_json\":{escaped}}}}}\n\n"
        ));
    }
    sse.push_str(
        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    );
    sse
}

fn tool_call_arguments(events: &[RawStreamingChoice<ClaudeStreamResponse>]) -> Value {
    match &events[0] {
        RawStreamingChoice::ToolCall(call) => call.arguments.clone(),
        other => panic!("expected ToolCall first, got {other:?}"),
    }
}

/// C1: Anthropic sends `input: {}` on `content_block_start` and streams the
/// real arguments as `input_json_delta` fragments. The deltas are the
/// arguments; the start input is ignored once any delta arrives.
#[test]
fn sse_tool_use_deltas_yield_exact_arguments() {
    let sse = sse_tool_use_block("toolu_1", "echo", "{}", &["{\"text\":", " \"hi\"}"]);
    let surface = HashSet::from(["echo".to_string()]);
    let events = parse_messages_sse(&sse, &surface).expect("parse");
    assert_eq!(tool_call_arguments(&events), json!({"text": "hi"}));

    let single = sse_fixture_tool_use("toolu_1", "echo", "{\"text\":\"hi\"}");
    let events = parse_messages_sse(&single, &surface).expect("parse fixture");
    assert_eq!(tool_call_arguments(&events), json!({"text": "hi"}));
}

#[test]
fn sse_tool_use_without_deltas_uses_start_input() {
    let sse = sse_tool_use_block("toolu_1", "echo", "{\"text\":\"hi\"}", &[]);
    let surface = HashSet::from(["echo".to_string()]);
    let events = parse_messages_sse(&sse, &surface).expect("parse");
    assert_eq!(tool_call_arguments(&events), json!({"text": "hi"}));
}

#[test]
fn sse_tool_use_with_no_input_at_all_is_empty_object() {
    let sse = sse_tool_use_block("toolu_1", "echo", "{}", &[]);
    let surface = HashSet::from(["echo".to_string()]);
    let events = parse_messages_sse(&sse, &surface).expect("parse");
    assert_eq!(tool_call_arguments(&events), json!({}));
}

#[test]
fn sse_tool_use_with_unparseable_input_fails_closed() {
    let sse = sse_fixture_tool_use("toolu_1", "echo", "{\"text\":");
    let surface = HashSet::from(["echo".to_string()]);
    let err = parse_messages_sse(&sse, &surface).expect_err("truncated json");
    assert!(
        err.to_string().contains("fail-closed: malformed tool_use"),
        "{err}"
    );
}

#[test]
fn sse_duplicate_tool_use_id_fails_closed() {
    let mut sse = sse_tool_use_block("toolu_1", "echo", "{}", &["{}"]);
    sse.push_str(&sse_tool_use_block("toolu_1", "echo", "{}", &["{}"]));
    let surface = HashSet::from(["echo".to_string()]);
    let err = parse_messages_sse(&sse, &surface).expect_err("duplicate id");
    assert!(
        err.to_string()
            .contains("fail-closed: duplicate tool_use id toolu_1"),
        "{err}"
    );
}

#[test]
fn sse_overlapping_tool_use_block_fails_closed() {
    let first = sse_tool_use_block("toolu_1", "echo", "{}", &[]);
    let (start, _stop) = first.split_once("event: content_block_stop").expect("stop");
    let mut sse = start.to_string();
    sse.push_str(&sse_tool_use_block("toolu_2", "echo", "{}", &[]));
    let surface = HashSet::from(["echo".to_string()]);
    let err = parse_messages_sse(&sse, &surface).expect_err("overlap");
    assert!(
        err.to_string()
            .contains("fail-closed: overlapping tool_use block toolu_2"),
        "{err}"
    );
}

/// A stream that ends with `message_stop` but never sent `content_block_stop`
/// still yields the open tool call, and it precedes the single `FinalResponse`.
#[test]
fn sse_message_stop_flushes_pending_tool_before_final() {
    let surface = HashSet::from(["echo".to_string()]);
    let mut state = MessagesSseState::new(surface);
    let mut events = Vec::new();
    let block = sse_tool_use_block("toolu_1", "echo", "{}", &["{\"text\":\"hi\"}"]);
    let without_stop = block
        .split_once("event: content_block_stop")
        .expect("block ends with content_block_stop")
        .0
        .to_string();
    for line in without_stop.lines() {
        events.extend(state.push_line(line).expect("push"));
    }
    assert!(events.is_empty(), "{events:?}");
    for line in "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n".lines() {
        events.extend(state.push_line(line).expect("push"));
    }
    assert!(
        matches!(
            &events[..],
            [RawStreamingChoice::ToolCall(call), RawStreamingChoice::FinalResponse(_)]
                if call.arguments == json!({"text": "hi"})
        ),
        "{events:?}"
    );
    let trailing = state.finish().expect("finish");
    assert!(trailing.is_empty(), "no second FinalResponse: {trailing:?}");
}

#[test]
fn push_line_yields_text_before_the_body_ends() {
    let mut state = MessagesSseState::new(HashSet::new());
    assert!(state
        .push_line("event: content_block_delta")
        .unwrap()
        .is_empty());
    let events = state
        .push_line(
            r#"data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hel"}}"#,
        )
        .unwrap();
    assert!(
        events.is_empty(),
        "a data line is not complete until the blank line"
    );
    let events = state.push_line("").unwrap();
    assert!(matches!(&events[..], [RawStreamingChoice::Message(t)] if t == "hel"));
    let events = state.finish().unwrap();
    assert!(matches!(
        &events[..],
        [RawStreamingChoice::FinalResponse(_)]
    ));
}

#[test]
fn sse_error_event_becomes_provider_error() {
    let sse = "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n";
    let err = parse_messages_sse(sse, &HashSet::new()).expect_err("error event");
    let msg = err.to_string();
    assert!(
        msg.contains("overloaded_error") && msg.contains("Overloaded"),
        "{msg}"
    );
}

/// Stable projection of a stream event for equality: rig mints a fresh
/// `internal_call_id` per `RawStreamingToolCall`, so `Debug` output of two
/// parses of the same bytes never matches.
fn event_key(event: &RawStreamingChoice<ClaudeStreamResponse>) -> String {
    match event {
        RawStreamingChoice::Message(text) => format!("text:{text}"),
        RawStreamingChoice::ToolCall(call) => {
            format!("tool:{}:{}:{}", call.id, call.name, call.arguments)
        }
        RawStreamingChoice::FinalResponse(response) => format!("final:{:?}", response.usage),
        other => format!("{other:?}"),
    }
}

#[tokio::test]
async fn chunk_boundaries_do_not_change_the_event_sequence() {
    let sse = format!(
        "{}{}",
        sse_fixture_text("hello world"),
        sse_fixture_tool_use("toolu_1", "echo", "{\"text\":\"hi\"}")
    );
    let surface = HashSet::from(["echo".to_string()]);
    let whole: Vec<String> = parse_messages_sse(&sse, &surface)
        .unwrap()
        .iter()
        .map(event_key)
        .collect();
    for chunk_len in [1usize, 3, 7, 64, 4096] {
        let chunks: Vec<Result<Bytes, http_client::Error>> = sse
            .as_bytes()
            .chunks(chunk_len)
            .map(|c| Ok(Bytes::copy_from_slice(c)))
            .collect();
        let body: http_client::sse::BoxedStream = Box::pin(futures::stream::iter(chunks));
        let events: Vec<String> = stream_sse_body(body, MessagesSseState::new(surface.clone()))
            .map(|e| event_key(&e.expect("event")))
            .collect()
            .await;
        assert_eq!(events, whole, "chunk_len={chunk_len}");
    }
}

#[tokio::test]
async fn first_text_event_is_observable_before_the_body_is_exhausted() {
    let (tx, rx) = futures::channel::mpsc::unbounded::<Result<Bytes, http_client::Error>>();
    let body: http_client::sse::BoxedStream = Box::pin(rx);
    let mut events = Box::pin(stream_sse_body(body, MessagesSseState::new(HashSet::new())));
    tx.unbounded_send(Ok(Bytes::from(sse_fixture_text("first"))))
        .unwrap();
    let first = events.next().await.expect("event").expect("ok");
    assert!(matches!(first, RawStreamingChoice::Message(ref t) if t == "first"));
    tx.unbounded_send(Ok(Bytes::from_static(
        b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
    )))
    .unwrap();
    drop(tx);
    let rest: Vec<_> = events.collect().await;
    assert!(matches!(
        rest.last().unwrap().as_ref().unwrap(),
        RawStreamingChoice::FinalResponse(_)
    ));
}

/// Live 4xx diagnosability: status, `request-id`, and a bounded body prefix;
/// nothing from the request side.
#[test]
fn non_success_error_carries_status_request_id_and_bounded_body_prefix() {
    let err = non_success_error(
        reqwest::StatusCode::BAD_REQUEST,
        Some("req_123"),
        &body_prefix(b"  {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"temperature\"}}\n"),
    );
    let text = err.to_string();
    assert!(text.contains("HTTP 400 Bad Request"), "{text}");
    assert!(text.contains("(request-id req_123)"), "{text}");
    assert!(
        text.ends_with(
            " body={\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"temperature\"}}"
        ),
        "{text}"
    );

    let long = vec![b'x'; NON_SUCCESS_BODY_PREFIX_BYTES * 3];
    assert_eq!(body_prefix(&long).len(), NON_SUCCESS_BODY_PREFIX_BYTES);

    let bare = non_success_error(reqwest::StatusCode::TOO_MANY_REQUESTS, None, "").to_string();
    assert!(bare.ends_with("(request-id -)"), "{bare}");
    assert!(!bare.contains("body="), "{bare}");
}

/// The wire sends the bearer as `authorization: Bearer <token>`, and a 401
/// from the transport invalidates the bearer exactly once; that request
/// fails and the next request refreshes.
#[tokio::test]
async fn transport_401_invalidates_the_bearer_once() {
    let _guard = lock_fixtures_for_test();
    let (url, handle) = crate::oauth_credential::test_support::one_shot_token_server(
        401,
        r#"{"type":"error","error":{"type":"authentication_error","message":"bad token"}}"#,
    )
    .await;
    let bearer = crate::claude_subscription::StaticBearer::new("access-STALE");
    let err = stream_messages_at(
        &url,
        "claude-sonnet-5",
        &echo_request(),
        HashSet::new(),
        &bearer,
        &ReqwestClient::new(),
    )
    .await
    .err()
    .expect("401");
    assert!(err.to_string().contains("401"), "{err}");
    assert_eq!(bearer.invalidations(), 1);
    let request = handle.await.expect("request");
    assert!(
        request
            .lines()
            .any(|line| line.eq_ignore_ascii_case("authorization: Bearer access-STALE")),
        "{request}"
    );
}
