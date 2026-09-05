use std::future::Future;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::StreamExt;
use rig::client::CompletionClient;
use rig::completion::{CompletionModel, CompletionRequest};
use rig::http_client::{
    self, HttpClientExt, LazyBody, MultipartForm, Request, Response, StreamingResponse,
};
use rig::one_or_many::OneOrMany;
use rig::wasm_compat::WasmCompatSend;

use super::*;
use crate::llm::message::{AssistantContent, Message, Reasoning};

fn core_request(reasoning: &str) -> CompletionRequest {
    let messages = vec![
        Message::Assistant {
            id: None,
            content: vec![AssistantContent::Reasoning(
                Reasoning::new(reasoning).with_id("rs_test".to_string()),
            )],
        },
        Message::user("visible prompt"),
    ];
    CompletionRequest {
        model: None,
        preamble: Some("system preamble".to_string()),
        chat_history: OneOrMany::many(crate::llm::rig_compat::to_rig_messages(&messages))
            .expect("non-empty history"),
        documents: vec![rig::completion::Document {
            id: "document-1".to_string(),
            text: "provider-visible document payload".to_string(),
            additional_props: std::collections::HashMap::from([(
                "source".to_string(),
                "provider-input-test".to_string(),
            )]),
        }],
        tools: vec![rig::completion::ToolDefinition {
            name: "lookup".to_string(),
            description: "Look up a test value".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {"key": {"type": "string"}},
                "required": ["key"]
            }),
        }],
        temperature: Some(0.2),
        max_tokens: Some(128),
        tool_choice: Some(rig::message::ToolChoice::Auto),
        additional_params: Some(serde_json::json!({
            "top_p": 0.9,
            "parallel_tool_calls": false
        })),
        output_schema: Some(
            serde_json::from_value(serde_json::json!({
                "title": "ProviderInputAnswer",
                "type": "object",
                "properties": {"answer": {"type": "string"}},
                "required": ["answer"]
            }))
            .expect("output schema"),
        ),
    }
}

#[derive(Debug)]
struct StaticBearer;

impl crate::oauth_credential::BearerSource for StaticBearer {
    async fn current_bearer(&self) -> anyhow::Result<String> {
        Ok("test-oauth-token".to_string())
    }

    async fn invalidate(&self) {}
}

#[derive(Clone, Debug, Default)]
struct WireCapturingClient {
    bodies: Arc<Mutex<Vec<Value>>>,
}

impl HttpClientExt for WireCapturingClient {
    fn send<T, U>(
        &self,
        req: Request<T>,
    ) -> impl Future<Output = http_client::Result<Response<LazyBody<U>>>> + WasmCompatSend + 'static
    where
        T: Into<Bytes> + WasmCompatSend,
        U: From<Bytes> + WasmCompatSend + 'static,
    {
        let bodies = self.bodies.clone();
        let body = req.into_body().into();
        async move {
            bodies
                .lock()
                .unwrap()
                .push(serde_json::from_slice(&body).expect("captured JSON body"));
            let body: LazyBody<U> = Box::pin(async { Ok(U::from(Bytes::from_static(b"{}"))) });
            Ok(Response::builder().status(200).body(body)?)
        }
    }

    fn send_multipart<U>(
        &self,
        _req: Request<MultipartForm>,
    ) -> impl Future<Output = http_client::Result<Response<LazyBody<U>>>> + WasmCompatSend + 'static
    where
        U: From<Bytes> + WasmCompatSend + 'static,
    {
        async move {
            let body: LazyBody<U> = Box::pin(async { Ok(U::from(Bytes::from_static(b"{}"))) });
            Ok(Response::builder().status(200).body(body)?)
        }
    }

    fn send_streaming<T>(
        &self,
        req: Request<T>,
    ) -> impl Future<Output = http_client::Result<StreamingResponse>> + WasmCompatSend
    where
        T: Into<Bytes>,
    {
        let bodies = self.bodies.clone();
        let body = req.into_body().into();
        async move {
            bodies
                .lock()
                .unwrap()
                .push(serde_json::from_slice(&body).expect("captured JSON body"));
            let stream: rig::http_client::sse::BoxedStream =
                Box::pin(futures::stream::empty::<http_client::Result<Bytes>>());
            Ok(Response::builder().status(200).body(stream)?)
        }
    }
}

#[tokio::test]
async fn chat_projection_matches_actual_rig_wire_body_and_omits_reasoning() {
    let counter = ProviderInputCounter::new(
        BackendProviderKind::OpenAiCompatible,
        OpenAiWireApi::ChatCompletions,
        "test-model",
    );
    let request = core_request(&"private-reasoning-sentinel".repeat(128 * 1024));
    let projected_body = counter.project_body(&request).expect("provider body");
    let mut actual = serde_json::to_value(
        rig::providers::openai::completion::CompletionRequest::try_from((
            "test-model".to_string(),
            request,
        ))
        .expect("Rig Chat DTO"),
    )
    .expect("serialize Rig Chat DTO");
    set_streaming_fields(&mut actual, true);

    assert_eq!(projected_body, actual);
    assert!(!projected_body
        .to_string()
        .contains("private-reasoning-sentinel"));

    let transport = WireCapturingClient::default();
    let bodies = transport.bodies.clone();
    let client = crate::inference_http::build_openai_chat_completions_client(
        "test-key",
        "http://provider.invalid",
        transport,
    )
    .expect("OpenAI Chat client");
    let model = client.completion_model("test-model");
    if let Ok(mut response) = model
        .stream(core_request(
            &"private-reasoning-sentinel".repeat(128 * 1024),
        ))
        .await
    {
        let _ = response.next().await;
    }
    assert_eq!(
        projected_body,
        bodies.lock().unwrap().pop().expect("captured wire body")
    );
}

#[test]
fn megabytes_of_chat_history_reasoning_do_not_change_provider_accounting() {
    let counter = ProviderInputCounter::new(
        BackendProviderKind::OpenAiCompatible,
        OpenAiWireApi::ChatCompletions,
        "test-model",
    );
    let small = counter
        .project_request(&core_request("small hidden thought"))
        .expect("small projection");
    let huge = counter
        .project_request(&core_request(&"huge hidden thought".repeat(200_000)))
        .expect("huge projection");
    let small_body = counter
        .project_body(&core_request("small hidden thought"))
        .expect("small provider body");
    let huge_body = counter
        .project_body(&core_request(&"huge hidden thought".repeat(200_000)))
        .expect("huge provider body");

    assert_eq!(small_body, huge_body);
    assert_eq!(small.estimated_input_tokens, huge.estimated_input_tokens);
    assert_eq!(
        small.components.estimated_input_tokens(),
        small.estimated_input_tokens,
        "diagnostic components must partition the authoritative total"
    );
    assert!(
        small.components.documents > 0,
        "provider-normalized documents must remain visible in diagnostics"
    );
    assert!(small.components.tool_schemas > 0);
}

#[test]
fn reasoning_only_chat_history_has_zero_provider_visible_message_cost() {
    let counter = ProviderInputCounter::new(
        BackendProviderKind::OpenAiCompatible,
        OpenAiWireApi::ChatCompletions,
        "test-model",
    );
    let reasoning_only = vec![Message::Assistant {
        id: None,
        content: vec![AssistantContent::Reasoning(
            Reasoning::new(&"hidden".repeat(500_000)).with_id("rs_only".to_string()),
        )],
    }];

    assert_eq!(
        counter.estimate_message_request(&reasoning_only).unwrap(),
        0
    );
}

#[test]
fn component_partition_stays_exact_across_json_remainders() {
    let counter = ProviderInputCounter::new(
        BackendProviderKind::OpenAiCompatible,
        OpenAiWireApi::ChatCompletions,
        "test-model",
    );
    for length in 0..16 {
        let mut request = core_request("hidden");
        request.additional_params = Some(serde_json::json!({"remainder": "x".repeat(length)}));
        let projection = counter.project_request(&request).unwrap();
        assert_eq!(
            projection.components.estimated_input_tokens(),
            projection.estimated_input_tokens,
            "component partition drifted for a {length}-byte payload"
        );
    }
}

#[test]
fn responses_projection_matches_rig_then_production_normalization() {
    let counter = ProviderInputCounter::new(
        BackendProviderKind::OpenAiCompatible,
        OpenAiWireApi::Responses,
        "test-model",
    );
    assert_eq!(
        counter.profile(),
        ProviderInputProfile::OpenAiResponsesNormalized
    );
    let request = core_request("provider-visible reasoning");
    let projected_body = counter.project_body(&request).expect("provider body");
    let dto = rig::providers::openai::responses_api::CompletionRequest::try_from((
        "test-model".to_string(),
        request,
    ))
    .expect("Rig Responses DTO");
    let mut actual = serde_json::to_value(dto).expect("serialize Rig Responses DTO");
    set_streaming_fields(&mut actual, false);
    crate::llm::responses_normalize::normalize_responses_assistant_items(&mut actual);

    assert_eq!(projected_body, actual);
    assert!(projected_body
        .to_string()
        .contains("provider-visible reasoning"));
}

#[tokio::test]
async fn normalized_responses_projection_matches_the_terminal_wire_body() {
    let terminal = WireCapturingClient::default();
    let bodies = terminal.bodies.clone();
    let transport = crate::inference_http::ResponsesNormalizingHttpClient::new(terminal);
    let client = crate::inference_http::build_openai_responses_client(
        "test-key",
        "http://provider.invalid",
        transport,
        rig::http_client::HeaderMap::default(),
    )
    .expect("OpenAI Responses client");
    let model = client.completion_model("test-model");
    let request = core_request("provider-visible reasoning");
    let counter = ProviderInputCounter::new(
        BackendProviderKind::OpenAiCompatible,
        OpenAiWireApi::Responses,
        "test-model",
    );
    let projected_body = counter.project_body(&request).expect("provider body");

    if let Ok(mut response) = model.stream(request).await {
        let _ = response.next().await;
    }
    assert_eq!(
        projected_body,
        bodies.lock().unwrap().pop().expect("captured wire body")
    );
}

#[tokio::test]
async fn codex_responses_projection_matches_the_terminal_rewritten_body() {
    let terminal = WireCapturingClient::default();
    let bodies = terminal.bodies.clone();
    let transport =
        crate::chatgpt_codex::ChatGptCodexHttpClient::with_inner(Arc::new(StaticBearer), terminal);
    let client = crate::inference_http::build_openai_responses_client(
        "managed-test-key",
        "http://provider.invalid",
        transport,
        rig::http_client::HeaderMap::default(),
    )
    .expect("Codex Responses client");
    let model = client.completion_model("test-model");
    let request = core_request("codex-visible reasoning");
    let counter = ProviderInputCounter::new(
        BackendProviderKind::ChatGptCodex,
        OpenAiWireApi::Responses,
        "test-model",
    );
    let projected_body = counter.project_body(&request).expect("provider body");

    if let Ok(mut response) = model.stream(request).await {
        let _ = response.next().await;
    }
    assert_eq!(
        projected_body,
        bodies.lock().unwrap().pop().expect("captured wire body")
    );
}

#[tokio::test]
async fn xai_responses_projection_matches_the_terminal_rewritten_body() {
    let terminal = WireCapturingClient::default();
    let bodies = terminal.bodies.clone();
    let transport =
        crate::xai_grok_oauth::XaiGrokOAuthHttpClient::with_inner(Arc::new(StaticBearer), terminal);
    let client = crate::inference_http::build_openai_responses_client(
        "managed-test-key",
        "http://provider.invalid",
        transport,
        rig::http_client::HeaderMap::default(),
    )
    .expect("xAI Responses client");
    let model = client.completion_model("test-model");
    let request = core_request("xai-visible reasoning");
    let counter = ProviderInputCounter::new(
        BackendProviderKind::XaiGrokOAuth,
        OpenAiWireApi::Responses,
        "test-model",
    );
    let projected_body = counter.project_body(&request).expect("provider body");

    if let Ok(mut response) = model.stream(request).await {
        let _ = response.next().await;
    }
    assert_eq!(
        projected_body,
        bodies.lock().unwrap().pop().expect("captured wire body")
    );
}

#[tokio::test]
async fn openrouter_projection_matches_the_actual_rig_wire_body() {
    let transport = WireCapturingClient::default();
    let bodies = transport.bodies.clone();
    let client = rig::providers::openrouter::Client::builder()
        .api_key("test-key")
        .base_url("http://provider.invalid")
        .http_client(transport)
        .build()
        .expect("OpenRouter client");
    let model = client.completion_model("test-model");
    let request = core_request("openrouter-visible-reasoning");
    let counter = ProviderInputCounter::new(
        BackendProviderKind::OpenRouter,
        OpenAiWireApi::ChatCompletions,
        "test-model",
    );
    let projected_body = counter.project_body(&request).expect("provider body");

    if let Ok(mut response) = model.stream(request).await {
        let _ = response.next().await;
    }
    let actual = bodies.lock().unwrap().pop().expect("captured wire body");
    assert_eq!(projected_body, actual);
    assert!(actual.to_string().contains("openrouter-visible-reasoning"));
}

/// The Messages wire sends `build_messages_body` verbatim (no rig DTO, no
/// rewrite), so the projection is that body; `openai_wire_api` is ignored.
#[test]
fn claude_messages_projection_is_the_messages_body_regardless_of_wire() {
    let request = core_request("claude-visible reasoning");
    for wire in [OpenAiWireApi::ChatCompletions, OpenAiWireApi::Responses] {
        let counter = ProviderInputCounter::new(
            BackendProviderKind::ClaudeCliSubscription,
            wire,
            "claude-sonnet-5",
        );
        assert_eq!(counter.profile(), ProviderInputProfile::ClaudeMessages);
        assert_eq!(
            counter.project_body(&request).expect("provider body"),
            crate::claude_messages::build_messages_body("claude-sonnet-5", &request)
        );
    }
    let projection = ProviderInputCounter::new(
        BackendProviderKind::ClaudeCliSubscription,
        OpenAiWireApi::ChatCompletions,
        "claude-sonnet-5",
    )
    .project_request(&request)
    .expect("projection");
    assert_eq!(
        projection.estimator,
        "claude_messages_wire_json_bytes_div_4_v1"
    );
    assert!(projection.components.messages > 0);
    assert!(projection.components.tool_schemas > 0);
    assert_eq!(projection.components.documents, 0);
}
