use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::llm::message::{AssistantContent, ToolResultContent, UserContent};
use crate::llm::tool::{BoxFuture, ToolDefinition, ToolDyn, ToolError};
use futures::{stream, Stream, StreamExt};
use rig::completion::{CompletionError, CompletionModel, CompletionRequest, CompletionResponse};

use crate::llm::message::Message;
use rig::streaming::{
    RawStreamingChoice, RawStreamingToolCall, StreamedAssistantContent, StreamedUserContent,
    StreamingCompletionResponse,
};
use tokio::sync::Mutex;

use super::*;
use crate::ensure_schemas;
use crate::hook::{DefraSessionHook, FailurePolicy};
use crate::test_support::first_content;

enum ScriptedCall {
    Turn(Vec<RawStreamingChoice<()>>),
    FailStream(CompletionError),
    TurnWithMidStreamError(Vec<RawStreamingChoice<()>>, CompletionError),
}

/// A `CompletionModel` whose `stream` replays one scripted call: each
/// `stream()` pops the next [`ScriptedCall`] from the queue, letting a test
/// drive multi-turn loops and provider failures without a provider. Once the
/// queue is empty it yields a bare final response so the loop terminates.
#[derive(Clone)]
struct ScriptedModel {
    calls: Arc<Mutex<VecDeque<ScriptedCall>>>,
    /// `chat_history` of every request the loop sent, in order (converted to
    /// native at the capture boundary) — lets a test assert how the loop
    /// threaded prior turns back to the provider.
    seen_histories: Arc<Mutex<Vec<Vec<Message>>>>,
    /// Advertised tool names (`request.tools`) of every request the loop sent,
    /// in order — lets a test assert the toolset is attached on every turn.
    seen_tools: Arc<Mutex<Vec<Vec<String>>>>,
    /// Effective per-turn output caps after the provider-input budget clamp.
    seen_max_tokens: Arc<Mutex<Vec<Option<u64>>>>,
    /// When set, every turn's stream yields its scripted chunks then hangs
    /// (never reaches EOF), simulating a provider that stalls mid-turn.
    stall_after_chunks: bool,
}

impl ScriptedModel {
    fn new(chunks: Vec<RawStreamingChoice<()>>) -> Self {
        Self::new_turns(vec![chunks])
    }

    fn new_turns(turns: Vec<Vec<RawStreamingChoice<()>>>) -> Self {
        Self::new_calls(turns.into_iter().map(ScriptedCall::Turn).collect())
    }

    fn new_calls(calls: Vec<ScriptedCall>) -> Self {
        Self {
            calls: Arc::new(Mutex::new(calls.into())),
            seen_histories: Arc::new(Mutex::new(Vec::new())),
            seen_tools: Arc::new(Mutex::new(Vec::new())),
            seen_max_tokens: Arc::new(Mutex::new(Vec::new())),
            stall_after_chunks: false,
        }
    }

    /// A single turn that emits `chunks` then stalls forever instead of ending.
    fn new_stalling(chunks: Vec<RawStreamingChoice<()>>) -> Self {
        let mut model = Self::new_calls(vec![ScriptedCall::Turn(chunks)]);
        model.stall_after_chunks = true;
        model
    }

    async fn seen_histories(&self) -> Vec<Vec<Message>> {
        self.seen_histories.lock().await.clone()
    }

    async fn seen_tools(&self) -> Vec<Vec<String>> {
        self.seen_tools.lock().await.clone()
    }

    async fn seen_max_tokens(&self) -> Vec<Option<u64>> {
        self.seen_max_tokens.lock().await.clone()
    }
}

#[allow(refining_impl_trait)]
impl CompletionModel for ScriptedModel {
    type Response = ();
    type StreamingResponse = ();
    type Client = ();

    fn make(_: &Self::Client, _: impl Into<String>) -> Self {
        Self::new_turns(Vec::new())
    }

    async fn completion(
        &self,
        _request: CompletionRequest,
    ) -> Result<CompletionResponse<Self::Response>, CompletionError> {
        Err(CompletionError::ProviderError(
            "completion is unused in loop_stream tests".to_string(),
        ))
    }

    async fn stream(
        &self,
        _request: CompletionRequest,
    ) -> Result<StreamingCompletionResponse<Self::StreamingResponse>, CompletionError> {
        self.seen_histories.lock().await.push(
            _request
                .chat_history
                .iter()
                .map(crate::llm::rig_compat::from_rig_message)
                .collect(),
        );
        self.seen_tools.lock().await.push(
            _request
                .tools
                .iter()
                .map(|tool| tool.name.clone())
                .collect(),
        );
        self.seen_max_tokens.lock().await.push(_request.max_tokens);
        let call = self
            .calls
            .lock()
            .await
            .pop_front()
            .unwrap_or_else(|| ScriptedCall::Turn(vec![RawStreamingChoice::FinalResponse(())]));
        let items: Vec<Result<RawStreamingChoice<()>, CompletionError>> = match call {
            ScriptedCall::Turn(chunks) => chunks.into_iter().map(Ok).collect(),
            ScriptedCall::FailStream(error) => return Err(error),
            ScriptedCall::TurnWithMidStreamError(chunks, error) => {
                let mut items: Vec<Result<RawStreamingChoice<()>, CompletionError>> =
                    chunks.into_iter().map(Ok).collect();
                items.push(Err(error));
                items
            }
        };
        let inner: rig::streaming::StreamingResult<()> = if self.stall_after_chunks {
            Box::pin(stream::iter(items).chain(stream::pending()))
        } else {
            Box::pin(stream::iter(items))
        };
        Ok(StreamingCompletionResponse::stream(inner))
    }
}

/// A trivial tool that echoes a fixed output, for dispatch tests.
struct EchoTool {
    name: String,
    output: String,
}

impl ToolDyn for EchoTool {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn definition<'a>(&'a self, _prompt: String) -> BoxFuture<'a, ToolDefinition> {
        Box::pin(async move {
            ToolDefinition {
                name: self.name.clone(),
                description: "echo".to_string(),
                parameters: serde_json::json!({"type": "object"}),
            }
        })
    }

    fn call<'a>(&'a self, _args: String) -> BoxFuture<'a, Result<String, ToolError>> {
        Box::pin(async move { Ok(self.output.clone()) })
    }
}

struct CountingTool {
    name: String,
    output: String,
    calls: Arc<AtomicUsize>,
}

impl ToolDyn for CountingTool {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn definition<'a>(&'a self, _prompt: String) -> BoxFuture<'a, ToolDefinition> {
        Box::pin(async move {
            ToolDefinition {
                name: self.name.clone(),
                description: "counting".to_string(),
                parameters: serde_json::json!({"type": "object"}),
            }
        })
    }

    fn call<'a>(&'a self, _args: String) -> BoxFuture<'a, Result<String, ToolError>> {
        let calls = self.calls.clone();
        let output = self.output.clone();
        Box::pin(async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(output)
        })
    }
}

/// A tool whose call always returns a fixed string (used for large/managed
/// outputs); name defaults to "echo".
struct FixedTool {
    name: String,
    output: String,
}

impl ToolDyn for FixedTool {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn definition<'a>(&'a self, _prompt: String) -> BoxFuture<'a, ToolDefinition> {
        Box::pin(async move {
            ToolDefinition {
                name: self.name.clone(),
                description: "fixed".to_string(),
                parameters: serde_json::json!({"type": "object"}),
            }
        })
    }

    fn call<'a>(&'a self, _args: String) -> BoxFuture<'a, Result<String, ToolError>> {
        Box::pin(async move { Ok(self.output.clone()) })
    }
}

/// Records the prompt/rag string handed to `definition`, for the rag-text test.
struct RecordingDefinitionTool {
    seen_prompt: Arc<Mutex<Option<String>>>,
}

impl ToolDyn for RecordingDefinitionTool {
    fn name(&self) -> String {
        "record".to_string()
    }

    fn definition<'a>(&'a self, prompt: String) -> BoxFuture<'a, ToolDefinition> {
        let seen = self.seen_prompt.clone();
        Box::pin(async move {
            *seen.lock().await = Some(prompt);
            ToolDefinition {
                name: "record".to_string(),
                description: "record".to_string(),
                parameters: serde_json::json!({"type": "object"}),
            }
        })
    }

    fn call<'a>(&'a self, _args: String) -> BoxFuture<'a, Result<String, ToolError>> {
        Box::pin(async move { Ok("ok".to_string()) })
    }
}

fn echo_tool() -> Box<dyn ToolDyn> {
    Box::new(EchoTool {
        name: "echo".to_string(),
        output: "ECHOED".to_string(),
    })
}

/// Script one tool-calling turn that invokes `echo`.
fn echo_tool_turn() -> Vec<RawStreamingChoice<()>> {
    vec![
        RawStreamingChoice::ToolCall(RawStreamingToolCall::new(
            "call-1".to_string(),
            "echo".to_string(),
            serde_json::json!({}),
        )),
        RawStreamingChoice::FinalResponse(()),
    ]
}

/// Set the request context that on_tool_call/on_tool_result require. The
/// session itself is created by the generator's per-turn on_completion_call.
async fn ready_hook_for(hook: &DefraSessionHook) {
    hook.set_active_request_id(Some(uuid::Uuid::new_v4().to_string()))
        .await;
    hook.set_request_deadline_at(Some(chrono::Utc::now() + chrono::Duration::seconds(60)))
        .await;
}

fn tool_result_text(content: &ToolResultContent) -> &str {
    match content {
        ToolResultContent::Text(text) => text.text.as_str(),
        ToolResultContent::Image(_) => "",
    }
}

fn config(max_turns: usize) -> LoopConfig {
    LoopConfig {
        preamble: None,
        context_message: None,
        temperature: None,
        max_tokens: None,
        additional_params: None,
        structured_output: None,
        tool_choice: None,
        on_rendered_request: None,
        turn_compactor: None,
        context_window: crate::config::DEFAULT_CONTEXT_WINDOW,
        compaction_threshold: crate::config::DEFAULT_COMPACTION_THRESHOLD,
        retry_policy: crate::agent::completion_retry::CompletionRetryPolicy::scheduled_default(),
        deadline: None,
        max_turns,
    }
}

#[derive(Debug)]
struct AttemptEvent {
    turn: usize,
    attempt: u32,
    will_retry: bool,
    backoff: Duration,
}

#[derive(Debug, Default)]
struct CollectedScriptedStream {
    attempts: Vec<AttemptEvent>,
    text_chunks: Vec<String>,
    tool_results: Vec<String>,
    retractions: Vec<(usize, u32)>,
    final_text: Option<String>,
    error: Option<String>,
}

async fn collect_scripted_stream<S>(stream: S) -> CollectedScriptedStream
where
    S: Stream<Item = Result<LoopStreamItem<()>, StreamingError>>,
{
    futures::pin_mut!(stream);
    let mut collected = CollectedScriptedStream::default();

    loop {
        match tokio::time::timeout(Duration::from_millis(1), stream.next()).await {
            Ok(Some(Ok(LoopStreamItem::AttemptFailed {
                turn,
                attempt,
                error: _,
                will_retry,
                backoff,
            }))) => collected.attempts.push(AttemptEvent {
                turn,
                attempt,
                will_retry,
                backoff,
            }),
            Ok(Some(Ok(LoopStreamItem::TurnRetracted { turn, attempt, .. }))) => {
                collected.retractions.push((turn, attempt));
            }
            Ok(Some(Ok(LoopStreamItem::Item(MultiTurnStreamItem::StreamAssistantItem(
                StreamedAssistantContent::Text(text),
            ))))) => {
                collected.text_chunks.push(text.text);
            }
            Ok(Some(Ok(LoopStreamItem::Item(MultiTurnStreamItem::StreamUserItem(
                StreamedUserContent::ToolResult { tool_result, .. },
            ))))) => {
                collected.tool_results.push(
                    tool_result_text(&crate::llm::rig_compat::from_rig_tool_result_content(
                        &tool_result.content.first(),
                    ))
                    .to_string(),
                );
            }
            Ok(Some(Ok(LoopStreamItem::Item(MultiTurnStreamItem::FinalResponse(
                final_response,
            ))))) => {
                collected.final_text = Some(final_response.response().to_string());
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(error))) => {
                collected.error = Some(format!("{error:?}"));
                break;
            }
            Ok(None) => break,
            Err(_) => {
                tokio::time::advance(Duration::from_secs(300)).await;
            }
        }
    }

    collected
}

fn transient_provider_error(label: &str) -> CompletionError {
    CompletionError::ProviderError(format!("status code 503: {label}"))
}

fn permanent_provider_error() -> CompletionError {
    CompletionError::ProviderError("status code 400: duplicate field max_tokens".to_string())
}

fn parse_400_text(tag: &str) -> String {
    format!("BadRequestError: Expecting value [{tag}]: line 1 column 28 (char 27)")
}

fn parse_400_error(tag: &str) -> CompletionError {
    CompletionError::ProviderError(parse_400_text(tag))
}

fn assert_duration_in_range(delay: Duration, low_ms: u64, high_ms: u64) {
    let actual_ms = delay.as_millis() as u64;
    assert!(
        actual_ms >= low_ms && actual_ms <= high_ms,
        "expected duration in [{low_ms}, {high_ms}]ms, got {actual_ms}ms"
    );
}

fn history_has_control_char_tool_arg(history: &[Message]) -> bool {
    history.iter().any(|message| match message {
        Message::Assistant { content, .. } => content.iter().any(|item| match item {
            AssistantContent::ToolCall(tool_call) => {
                json_value_has_control_char(&tool_call.function.arguments)
            }
            _ => false,
        }),
        _ => false,
    })
}

fn history_has_tool_call(history: &[Message], tool_name: &str) -> bool {
    history.iter().any(|message| match message {
        Message::Assistant { content, .. } => content.iter().any(|item| {
            matches!(
                item,
                AssistantContent::ToolCall(tool_call)
                    if tool_call.function.name == tool_name
            )
        }),
        _ => false,
    })
}

fn history_has_tool_result_text(history: &[Message], expected: &str) -> bool {
    history.iter().any(|message| match message {
        Message::User { content } => content.iter().any(|item| {
            matches!(
                item,
                UserContent::ToolResult(result)
                    if tool_result_text(first_content(&result.content)) == expected
            )
        }),
        _ => false,
    })
}

fn json_value_has_control_char(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(text) => text.chars().any(char::is_control),
        serde_json::Value::Array(values) => values.iter().any(json_value_has_control_char),
        serde_json::Value::Object(map) => map.values().any(json_value_has_control_char),
        _ => false,
    }
}

async fn test_hook() -> (
    Arc<defra_node::EmbeddedNode>,
    DefraSessionHook,
    crate::test_support::SignedTestIdentity,
) {
    let data_path =
        std::env::temp_dir().join(format!("agent-loop-stream-{}", uuid::Uuid::new_v4()));
    let identity = crate::test_support::signed_test_identity("agent-loop-stream-identity");
    let node = Arc::new(
        defra_node::EmbeddedNode::builder()
            .data_path(&data_path)
            .with_node_identity_did(identity.did())
            .build()
            .await
            .unwrap(),
    );
    ensure_schemas(&node).await.unwrap();
    let hook = DefraSessionHook::with_identity(
        node.clone(),
        "general",
        identity.did(),
        FailurePolicy::default(),
    );
    (node, hook, identity)
}

#[tokio::test]
async fn single_turn_no_tools_yields_text_then_final() {
    let (_node, hook, _identity) = test_hook().await;
    let model = ScriptedModel::new(vec![
        RawStreamingChoice::Message("Hello ".to_string()),
        RawStreamingChoice::Message("world".to_string()),
        RawStreamingChoice::FinalResponse(()),
    ]);

    let stream = run_loop_stream(
        model,
        Some(hook),
        Message::user("hi"),
        Vec::new(),
        Arc::new(Vec::new()),
        config(0),
    );
    futures::pin_mut!(stream);

    let mut texts = Vec::new();
    let mut final_text = None;
    while let Some(item) = stream.next().await {
        match item.expect("loop item should be Ok") {
            LoopStreamItem::Item(MultiTurnStreamItem::StreamAssistantItem(
                StreamedAssistantContent::Text(text),
            )) => {
                texts.push(text.text);
            }
            LoopStreamItem::Item(MultiTurnStreamItem::FinalResponse(final_response)) => {
                final_text = Some(final_response.response().to_string());
            }
            _ => {}
        }
    }

    assert_eq!(texts, vec!["Hello ".to_string(), "world".to_string()]);
    assert_eq!(final_text.as_deref(), Some("Hello world"));
}

#[tokio::test]
async fn rendered_request_sink_runs_before_provider_stream() {
    let (_node, hook, _identity) = test_hook().await;
    let model = ScriptedModel::new(vec![
        RawStreamingChoice::Message("unreached".to_string()),
        RawStreamingChoice::FinalResponse(()),
    ]);
    let captures = Arc::new(Mutex::new(Vec::new()));
    let captures_for_sink = captures.clone();
    let mut loop_config = config(0);
    loop_config.on_rendered_request =
        Some(Arc::new(move |turn_index, attempt, request, _trace| {
            let captures = captures_for_sink.clone();
            Box::pin(async move {
                captures
                    .lock()
                    .await
                    .push((turn_index, attempt, request.chat_history.len()));
                Err(anyhow::anyhow!("capture failed"))
            })
        }));

    let stream = run_loop_stream(
        model.clone(),
        Some(hook),
        Message::user("hi"),
        Vec::new(),
        Arc::new(Vec::new()),
        loop_config,
    );
    futures::pin_mut!(stream);

    let item = stream
        .next()
        .await
        .expect("stream should yield the sink error");
    let error = item.expect_err("capture failure should abort the provider call");
    assert!(
        format!("{error:?}").contains("capturing rendered completion request failed"),
        "unexpected error: {error:?}"
    );
    assert_eq!(captures.lock().await.as_slice(), &[(0, 0, 1)]);
    assert!(
        model.seen_histories().await.is_empty(),
        "provider stream must not start after capture failure"
    );
}

/// The durable `RenderedRequest` table, as `Proofs.RenderedCapture` models it:
/// a partial map from the five-component capture key to the opaque canonical
/// request stored under it.
type CaptureKey = (u64, u64, u64, usize, u32);

type ConfigSourceRef = (String, Option<u64>, u64, u64, u64);

#[derive(Clone, Debug, PartialEq, Eq)]
struct CanonicalCapture {
    provider_body: u64,
    config_scope: String,
    config: Option<Vec<ConfigSourceRef>>,
}

fn config_bundle(
    present: bool,
    sources: &[crate::lean_vocab_test::LeanRenderedConfigSourceRef],
) -> Option<Vec<ConfigSourceRef>> {
    present.then(|| {
        sources
            .iter()
            .map(|source| {
                (
                    source.source_class.clone(),
                    source.logical_id,
                    source.doc_id,
                    source.composite_commit_cid,
                    source.signer_did,
                )
            })
            .collect()
    })
}

fn config_bundle_is_complete(config: &Option<Vec<ConfigSourceRef>>) -> bool {
    const REQUIRED_CLASSES: [&str; 4] = [
        "principal",
        "behavior",
        "inference_backend",
        "inference_profile",
    ];
    let Some(sources) = config else {
        return false;
    };
    if sources.len() < REQUIRED_CLASSES.len() {
        return false;
    }
    let exact = |source: &ConfigSourceRef| source.2 != 0 && source.3 != 0 && source.4 != 0;
    if !sources
        .iter()
        .take(REQUIRED_CLASSES.len())
        .zip(REQUIRED_CLASSES)
        .all(|(source, expected)| source.0 == expected && source.1.is_none() && exact(source))
    {
        return false;
    }

    let mut index = REQUIRED_CLASSES.len();
    if sources
        .get(index)
        .is_some_and(|source| source.0 == "tool_selection")
    {
        let tool = &sources[index];
        if tool.1.is_some() || !exact(tool) {
            return false;
        }
        index += 1;
    }

    let mut previous_skill_id = 0;
    sources[index..].iter().all(|skill| {
        let Some(logical_id) = skill.1 else {
            return false;
        };
        let canonical = skill.0 == "skill" && exact(skill) && previous_skill_id < logical_id;
        previous_skill_id = logical_id;
        canonical
    })
}

fn config_bundle_is_admitted(scope: &str, config: &Option<Vec<ConfigSourceRef>>) -> bool {
    match scope {
        "reconciled_document_runtime" => config_bundle_is_complete(config),
        "static_or_one_shot" => config.is_none() || config_bundle_is_complete(config),
        _ => false,
    }
}

/// `RenderedCapture.capture`, mirrored. This is deliberately the *only* hand
/// written thing in the fence below, and it is the shape PR2's
/// `rendered_request::sink` has to implement: missing key writes, identical
/// canonical value succeeds without a write, conflicting canonical value is an
/// integrity error. Everything else the test asserts is generated.
fn mirror_capture(
    store: &mut std::collections::HashMap<CaptureKey, CanonicalCapture>,
    key: CaptureKey,
    request: CanonicalCapture,
) -> &'static str {
    if !config_bundle_is_admitted(&request.config_scope, &request.config) {
        return "rejected";
    }
    match store.get(&key) {
        None => {
            store.insert(key, request);
            "fresh"
        }
        Some(stored) if stored == &request => "idempotent",
        Some(_) => "rejected",
    }
}

/// Persist-before-send, driven end to end through the real owned loop.
///
/// The Lean model (`Proofs/RenderedCapture.lean`) proves that `sent` is
/// unreachable from `assembled` without an intervening successful capture of
/// the same `(key, canonical request)`, and that a rejected capture makes
/// `sent` unreachable permanently. This test is the fence that keeps
/// `run_loop_stream` honest about it: for every generated row, a sink that
/// answers exactly as `RenderedCapture.capture` does must let the provider
/// observe exactly `provider_requests_observed` requests — one when the fact is
/// durable, zero when it is not.
///
/// The seam under test is `crates/gents/src/agent/loop_stream.rs:297-307`:
/// `on_rendered_request` runs immediately before `model.stream`, and its error
/// is mapped to a terminal completion error rather than being logged and
/// stepped over. A fail-open regression there — swallowing the sink error,
/// moving the call after `model.stream`, or making the sink optional at the
/// dispatch site — flips `provider_requests_observed` on the rejected row and
/// fails here.
#[tokio::test(start_paused = true)]
async fn generated_rendered_capture_cases_fence_persist_before_send() {
    let cases = crate::lean_vocab_test::lean_rendered_capture_cases();
    assert!(!cases.is_empty(), "Lean emitted no rendered-capture cases");

    for case in cases {
        let key: CaptureKey = (
            case.agent_did,
            case.session_id,
            case.request_id,
            case.turn_index,
            case.attempt,
        );
        let mut seeded = std::collections::HashMap::new();
        if let Some(prior) = case.prior_binding {
            seeded.insert(
                key,
                CanonicalCapture {
                    provider_body: prior,
                    config_scope: case.config_scope.clone(),
                    config: config_bundle(case.prior_config_present, &case.prior_config_sources),
                },
            );
        }
        let store = Arc::new(Mutex::new(seeded));
        let outcomes = Arc::new(Mutex::new(Vec::<&'static str>::new()));

        let store_for_sink = store.clone();
        let outcomes_for_sink = outcomes.clone();
        let request_value = CanonicalCapture {
            provider_body: case.request,
            config_scope: case.config_scope.clone(),
            config: config_bundle(case.config_present, &case.config_sources),
        };
        let mut loop_config = config(0);
        loop_config.on_rendered_request =
            Some(Arc::new(move |_turn_index, _attempt, _request, _trace| {
                let store = store_for_sink.clone();
                let outcomes = outcomes_for_sink.clone();
                let request_value = request_value.clone();
                Box::pin(async move {
                    let outcome = mirror_capture(&mut *store.lock().await, key, request_value);
                    outcomes.lock().await.push(outcome);
                    if outcome == "rejected" {
                        Err(anyhow::anyhow!(
                            "capture key already names a different canonical request"
                        ))
                    } else {
                        Ok(())
                    }
                })
            }));

        let model = ScriptedModel::new(vec![
            RawStreamingChoice::Message("ok".to_string()),
            RawStreamingChoice::FinalResponse(()),
        ]);
        let stream = run_loop_stream(
            model.clone(),
            None,
            Message::user("hi"),
            Vec::new(),
            Arc::new(Vec::new()),
            loop_config,
        );
        let collected = collect_scripted_stream(stream).await;

        assert_eq!(
            outcomes.lock().await.as_slice(),
            &[case.capture_outcome.as_str()],
            "{}: the sink decision drifted from RenderedCapture.capture",
            case.name
        );
        let expected_durable = case.durable_after.map(|provider_body| CanonicalCapture {
            provider_body,
            config_scope: case.config_scope.clone(),
            config: config_bundle(case.durable_config_present, &case.durable_config_sources),
        });
        assert_eq!(
            store.lock().await.get(&key).cloned(),
            expected_durable,
            "{}: the durable canonical binding drifted from the Lean model",
            case.name
        );
        assert_eq!(
            model.seen_histories().await.len(),
            case.provider_requests_observed,
            "{}: the provider observed a different number of requests than the \
             modeled trace permits (expected final stage {})",
            case.name,
            case.final_stage
        );

        if case.send_permitted {
            assert_eq!(case.final_stage, "sent");
            assert!(
                collected.error.is_none(),
                "{}: a durable capture must not fail the turn: {:?}",
                case.name,
                collected.error
            );
        } else {
            assert_eq!(case.final_stage, "assembled");
            assert!(
                !case.capture_durable,
                "{}: a row may not refuse the send while claiming durability",
                case.name
            );
            let error = collected
                .error
                .as_deref()
                .unwrap_or_else(|| panic!("{}: capture failure must be terminal", case.name));
            assert!(
                error.contains("capturing rendered completion request failed"),
                "{}: unexpected terminal error: {error}",
                case.name
            );
        }
    }
}

#[test]
fn generated_turn_budget_cases_drive_every_completion_dispatch() {
    let cases = crate::lean_vocab_test::lean_prompt_assembly_turn_budget_cases();
    assert!(
        !cases.is_empty(),
        "Lean emitted no owned-loop turn budget cases"
    );

    for case in cases {
        let threshold = case.threshold_basis_points as f64 / 10_000.0;
        assert_eq!(
            crate::compaction::threshold_budget(case.context_window, threshold),
            case.configured_threshold_budget,
            "{}: configured threshold drifted from Lean",
            case.name
        );
        assert_eq!(
            crate::compaction::effective_input_budget(
                case.context_window,
                case.max_output_tokens,
                threshold,
            ),
            case.effective_input_budget,
            "{}: effective input budget drifted from Lean",
            case.name
        );
        let actual_output = case
            .turn_input_tokens
            .iter()
            .map(|tokens| {
                crate::compaction::effective_output_budget(
                    *tokens,
                    case.context_window,
                    case.max_output_tokens,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            actual_output, case.turn_output_tokens,
            "{}: per-turn output clamp drifted from Lean",
            case.name
        );
        let actual = case
            .turn_input_tokens
            .iter()
            .map(|tokens| {
                crate::compaction::input_exceeds_budget(
                    *tokens,
                    case.context_window,
                    case.max_output_tokens,
                    threshold,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            actual, case.turn_should_compact,
            "{}: a later completion turn bypassed the Lean dispatch gate",
            case.name
        );
    }
}

#[tokio::test]
async fn completion_output_ceiling_is_clamped_to_remaining_context() {
    let model = ScriptedModel::new(vec![
        RawStreamingChoice::Message("done".to_string()),
        RawStreamingChoice::FinalResponse(()),
    ]);
    let prompt = Message::user("fit the output dynamically");
    let mut loop_config = config(0);
    loop_config.max_tokens = Some(1_000);
    loop_config.compaction_threshold = 1.0;

    let request = build_request(&model, prompt.clone(), &[], &[], &[], &loop_config)
        .await
        .expect("request should build");
    let input_tokens = completion_request_input_estimate(&request);
    loop_config.context_window = input_tokens + 250;

    let stream = run_loop_stream(
        model.clone(),
        None,
        prompt,
        Vec::new(),
        Arc::new(Vec::new()),
        loop_config,
    );
    let collected = collect_scripted_stream(stream).await;

    assert_eq!(collected.error, None);
    assert_eq!(model.seen_max_tokens().await, vec![Some(250)]);
}

#[tokio::test]
async fn later_completion_turn_is_compacted_before_provider_dispatch() {
    let model = ScriptedModel::new_turns(vec![
        echo_tool_turn(),
        vec![
            RawStreamingChoice::Message("done".to_string()),
            RawStreamingChoice::FinalResponse(()),
        ],
    ]);
    let tools: Arc<Vec<Box<dyn ToolDyn>>> = Arc::new(vec![Box::new(EchoTool {
        name: "echo".to_string(),
        output: "r".repeat(12_000),
    })]);
    let prompt = Message::user("p".repeat(20_000));
    let mut loop_config = config(2);
    loop_config.max_tokens = Some(100);
    loop_config.compaction_threshold = 1.0;

    let first_request = build_request(
        &model,
        prompt.clone(),
        &[],
        &[],
        tools.as_slice(),
        &loop_config,
    )
    .await
    .expect("first request should build");
    let first_tokens = completion_request_input_estimate(&first_request);
    loop_config.context_window = first_tokens + 100 + 100;

    let compactions = Arc::new(AtomicUsize::new(0));
    let compactions_for_callback = compactions.clone();
    let keep_recent_target = Arc::new(AtomicUsize::new(usize::MAX));
    let keep_recent_target_for_callback = keep_recent_target.clone();
    loop_config.turn_compactor = Some(Arc::new(move |messages, target| {
        let compactions = compactions_for_callback.clone();
        let keep_recent_target = keep_recent_target_for_callback.clone();
        Box::pin(async move {
            compactions.fetch_add(1, Ordering::SeqCst);
            keep_recent_target.store(target, Ordering::SeqCst);
            let keep_from = messages.len().saturating_sub(2);
            let mut compacted = vec![Message::user(
                "<system-reminder>compacted earlier turn</system-reminder>",
            )];
            compacted.extend(messages.into_iter().skip(keep_from));
            Ok(compacted)
        })
    }));

    let stream = run_loop_stream(model.clone(), None, prompt, Vec::new(), tools, loop_config);
    let collected = collect_scripted_stream(stream).await;

    assert_eq!(collected.error, None);
    assert_eq!(collected.final_text.as_deref(), Some("done"));
    assert_eq!(
        compactions.load(Ordering::SeqCst),
        1,
        "the safe entry turn must dispatch directly and the grown second turn must compact"
    );
    assert!(
        keep_recent_target.load(Ordering::SeqCst) < 20_000,
        "the per-turn target must reserve room for static request layers and the summary"
    );
    let histories = model.seen_histories().await;
    assert_eq!(histories.len(), 2);
    assert!(
        histories[1].iter().any(|message| message
            .rag_text()
            .is_some_and(|text| { text.contains("compacted earlier turn") })),
        "the second provider request must use the compacted provider view"
    );
}

#[tokio::test(start_paused = true)]
async fn pre_stream_transport_failure_retries_and_succeeds() {
    let model = ScriptedModel::new_calls(vec![
        ScriptedCall::FailStream(transient_provider_error("first")),
        ScriptedCall::Turn(vec![
            RawStreamingChoice::Message("recovered".to_string()),
            RawStreamingChoice::FinalResponse(()),
        ]),
    ]);

    let stream = run_loop_stream(
        model.clone(),
        None,
        Message::user("hi"),
        Vec::new(),
        Arc::new(Vec::new()),
        config(0),
    );
    let collected = collect_scripted_stream(stream).await;

    assert_eq!(collected.final_text.as_deref(), Some("recovered"));
    assert_eq!(collected.error, None);
    assert_eq!(collected.attempts.len(), 1);
    assert_eq!(collected.attempts[0].turn, 0);
    assert_eq!(collected.attempts[0].attempt, 0);
    assert!(collected.attempts[0].will_retry);
    assert_duration_in_range(collected.attempts[0].backoff, 3_750, 6_250);

    let histories = model.seen_histories().await;
    assert_eq!(
        histories.len(),
        2,
        "one failed attempt plus one successful retry"
    );
    assert_eq!(
        histories[0], histories[1],
        "transport retry must reissue the identical provider request"
    );
}

#[tokio::test(start_paused = true)]
async fn transport_ladder_exhaustion_fails_with_last_error() {
    let model = ScriptedModel::new_calls(vec![
        ScriptedCall::FailStream(transient_provider_error("still down 1")),
        ScriptedCall::FailStream(transient_provider_error("still down 2")),
        ScriptedCall::FailStream(transient_provider_error("still down 3")),
        ScriptedCall::FailStream(transient_provider_error("still down 4")),
    ]);

    let stream = run_loop_stream(
        model,
        None,
        Message::user("hi"),
        Vec::new(),
        Arc::new(Vec::new()),
        config(0),
    );
    let collected = collect_scripted_stream(stream).await;

    assert_eq!(collected.final_text, None);
    assert_eq!(collected.attempts.len(), 4);
    assert!(collected.attempts[..3]
        .iter()
        .all(|attempt| attempt.will_retry));
    assert!(!collected.attempts[3].will_retry);
    assert_eq!(collected.attempts[3].attempt, 3);
    assert_eq!(collected.attempts[3].backoff, Duration::ZERO);
    let error = collected
        .error
        .expect("retry exhaustion should end in error");
    assert!(
        error.contains("completion retry budget exhausted") && error.contains("still down 4"),
        "terminal error must include budget exhaustion and the last provider error; got {error}"
    );
}

#[tokio::test(start_paused = true)]
async fn three_minute_outage_recovers_within_ladder() {
    let model = ScriptedModel::new_calls(vec![
        ScriptedCall::FailStream(transient_provider_error("outage 1")),
        ScriptedCall::FailStream(transient_provider_error("outage 2")),
        ScriptedCall::FailStream(transient_provider_error("outage 3")),
        ScriptedCall::Turn(vec![
            RawStreamingChoice::Message("back".to_string()),
            RawStreamingChoice::FinalResponse(()),
        ]),
    ]);

    let stream = run_loop_stream(
        model,
        None,
        Message::user("hi"),
        Vec::new(),
        Arc::new(Vec::new()),
        config(0),
    );
    let collected = collect_scripted_stream(stream).await;

    assert_eq!(collected.final_text.as_deref(), Some("back"));
    assert_eq!(collected.error, None);
    assert_eq!(collected.attempts.len(), 3);
    assert!(collected.attempts.iter().all(|attempt| attempt.will_retry));
    let total_backoff = collected
        .attempts
        .iter()
        .fold(Duration::ZERO, |total, attempt| total + attempt.backoff);
    assert_duration_in_range(total_backoff, 116_250, 193_750);
}

#[tokio::test(start_paused = true)]
async fn parse_400_resamples_once_then_repairs_on_identical_error() {
    let poison = format!("bad{}value", '\u{0007}');
    let model = ScriptedModel::new_calls(vec![
        ScriptedCall::Turn(vec![
            RawStreamingChoice::ToolCall(RawStreamingToolCall::new(
                "call-1".to_string(),
                "echo".to_string(),
                serde_json::json!({ "note": poison }),
            )),
            RawStreamingChoice::FinalResponse(()),
        ]),
        ScriptedCall::FailStream(parse_400_error("same")),
        ScriptedCall::FailStream(parse_400_error("same")),
        ScriptedCall::Turn(vec![
            RawStreamingChoice::Message("repaired".to_string()),
            RawStreamingChoice::FinalResponse(()),
        ]),
    ]);
    let mut loop_config = config(4);
    loop_config.context_message = Some(Message::user("<context>\nrepair-test\n</context>"));

    let stream = run_loop_stream(
        model.clone(),
        None,
        Message::user("use the echo tool"),
        Vec::new(),
        Arc::new(vec![echo_tool()]),
        loop_config,
    );
    let collected = collect_scripted_stream(stream).await;

    assert_eq!(collected.final_text.as_deref(), Some("repaired"));
    assert_eq!(collected.error, None);
    assert_eq!(collected.attempts.len(), 2);
    assert!(collected.attempts.iter().all(|attempt| attempt.will_retry));
    assert_duration_in_range(collected.attempts[0].backoff, 3_750, 6_250);
    assert_eq!(collected.attempts[1].backoff, Duration::ZERO);

    let histories = model.seen_histories().await;
    assert_eq!(
        histories.len(),
        4,
        "tool turn, parse failure, resample, and repaired retry"
    );
    assert_eq!(
        histories[1], histories[2],
        "first parse-400 retry must resample the same provider request"
    );
    assert!(
        history_has_control_char_tool_arg(&histories[1]),
        "dirty tool arguments should be present before repair: {:?}",
        histories[1]
    );
    assert!(
        !history_has_control_char_tool_arg(&histories[3]),
        "repair must sanitize provider-bound tool arguments: {:?}",
        histories[3]
    );
    assert!(
        histories[3].iter().any(is_request_context_message),
        "repair must preserve the current request context: {:?}",
        histories[3]
    );
    assert_provider_request_invariants(4, &histories[3]);
}

/// The capture seam must hand the sink the loop's own `attempt` counter and its
/// own build path, one row per provider attempt.
///
/// Two things are fenced here that nothing else fences:
///
/// * `attempt` is part of the capture key. `RenderedCapture.attempt_distinguishes_facts`
///   is proven in Lean, but a mutation probe that replaced the loop's `attempt`
///   with a literal `0` compiled and failed no test — the existing sink tests
///   either rebuild the key from the Lean case or only ever observe attempt 0.
///   Retries here must arrive as distinct attempts within one turn.
/// * `AssemblyBuildPath` must flip to `Repair` exactly on the attempt that the
///   `PreStreamDirective::Repair` branch rebuilt with `build_request`. That
///   attempt skips `clamp_request_output_budget`, so a reconstructor that
///   assumes the budgeted path would produce a different `max_tokens` and a
///   false mismatch.
#[tokio::test(start_paused = true)]
async fn capture_seam_reports_distinct_attempts_and_the_repair_build_path() {
    let poison = format!("bad{}value", '\u{0007}');
    let model = ScriptedModel::new_calls(vec![
        ScriptedCall::Turn(vec![
            RawStreamingChoice::ToolCall(RawStreamingToolCall::new(
                "call-1".to_string(),
                "echo".to_string(),
                serde_json::json!({ "note": poison }),
            )),
            RawStreamingChoice::FinalResponse(()),
        ]),
        ScriptedCall::FailStream(parse_400_error("same")),
        ScriptedCall::FailStream(parse_400_error("same")),
        ScriptedCall::Turn(vec![
            RawStreamingChoice::Message("repaired".to_string()),
            RawStreamingChoice::FinalResponse(()),
        ]),
    ]);

    let captures: Arc<Mutex<Vec<(usize, u32, AssemblyBuildPath, AssemblyTrace)>>> =
        Arc::new(Mutex::new(Vec::new()));
    let captures_for_sink = captures.clone();
    let mut loop_config = config(4);
    loop_config.on_rendered_request =
        Some(Arc::new(move |turn_index, attempt, _request, trace| {
            let captures = captures_for_sink.clone();
            Box::pin(async move {
                captures
                    .lock()
                    .await
                    .push((turn_index, attempt, trace.build_path, trace));
                Ok(())
            })
        }));

    let stream = run_loop_stream(
        model.clone(),
        None,
        Message::user("use the echo tool"),
        Vec::new(),
        Arc::new(vec![echo_tool()]),
        loop_config,
    );
    let collected = collect_scripted_stream(stream).await;
    assert_eq!(collected.error, None);
    assert_eq!(collected.final_text.as_deref(), Some("repaired"));

    let captures = captures.lock().await;
    let observed = captures
        .iter()
        .map(|(turn, attempt, path, _)| (*turn, *attempt, *path))
        .collect::<Vec<_>>();
    assert_eq!(
        observed,
        vec![
            (0, 0, AssemblyBuildPath::Budgeted),
            (1, 0, AssemblyBuildPath::Budgeted),
            (1, 1, AssemblyBuildPath::Budgeted),
            (1, 2, AssemblyBuildPath::Repair),
        ],
        "one capture per provider attempt, with the loop's own attempt counter"
    );
    assert_eq!(
        captures.len(),
        model.seen_histories().await.len(),
        "every provider request must have exactly one capture"
    );

    // Leak 2: the exact tool-result content threaded to the model. Persistence
    // re-derives this text from `AgentToolCall.result` through a different
    // truncation mode and limit set, so the trace is the only place the bytes
    // the model actually saw survive.
    let repaired_trace = &captures.last().expect("a repaired capture").3;
    let threaded = repaired_trace
        .threaded_tool_results
        .iter()
        .find(|result| result.tool_call_id == "call-1")
        .expect("the echo call's threaded result");
    assert_eq!(
        threaded.content,
        vec![ToolResultContent::text("ECHOED")],
        "the trace must carry the threaded tool-result content verbatim"
    );
    assert!(
        repaired_trace.effective_message_count > threaded.message_index,
        "overlay positions must fit the reconstructible native list"
    );
    assert!(
        repaired_trace.effective_messages.is_some(),
        "a repaired attempt rewrote the message vectors in place, so the durable transcript no \
         longer reproduces them and the full native list is the only oracle"
    );

    let first_turn_trace = &captures.first().expect("a first capture").3;
    assert!(
        first_turn_trace.effective_messages.is_none(),
        "a turn before any repair must not duplicate the full transcript"
    );
}

/// `repair_provider_input` rewrites `history` and `new_messages` in place, and
/// both outlive the turn loop, so every turn *after* a repair is assembled from
/// messages no `AgentMessage` row reproduces. `build_path` resets per turn and
/// would report `Budgeted` for those turns, so the ephemeral marker is what
/// stops a reconstructor trusting a list it cannot rebuild.
///
/// The sibling test above repairs on the loop's final turn, so it cannot see
/// this carry-over at all.
#[tokio::test(start_paused = true)]
async fn a_turn_after_a_repair_still_carries_the_effective_message_list() {
    let poison = format!("bad{}value", '\u{0007}');
    let model = ScriptedModel::new_calls(vec![
        // Turn 0 — a tool call carrying the argument that will need repair.
        ScriptedCall::Turn(vec![
            RawStreamingChoice::ToolCall(RawStreamingToolCall::new(
                "call-1".to_string(),
                "echo".to_string(),
                serde_json::json!({ "note": poison }),
            )),
            RawStreamingChoice::FinalResponse(()),
        ]),
        // Turn 1 — rejected twice, then repaired and answered with another tool
        // call so the loop runs at least one more turn.
        ScriptedCall::FailStream(parse_400_error("same")),
        ScriptedCall::FailStream(parse_400_error("same")),
        ScriptedCall::Turn(vec![
            RawStreamingChoice::ToolCall(RawStreamingToolCall::new(
                "call-2".to_string(),
                "echo".to_string(),
                serde_json::json!({ "note": "clean" }),
            )),
            RawStreamingChoice::FinalResponse(()),
        ]),
        // Turn 2 — the turn after the repair. This is the one under test.
        ScriptedCall::Turn(vec![
            RawStreamingChoice::Message("after repair".to_string()),
            RawStreamingChoice::FinalResponse(()),
        ]),
    ]);

    let captures: Arc<Mutex<Vec<(usize, u32, AssemblyBuildPath, AssemblyTrace)>>> =
        Arc::new(Mutex::new(Vec::new()));
    let captures_for_sink = captures.clone();
    let mut loop_config = config(4);
    loop_config.on_rendered_request =
        Some(Arc::new(move |turn_index, attempt, _request, trace| {
            let captures = captures_for_sink.clone();
            Box::pin(async move {
                captures
                    .lock()
                    .await
                    .push((turn_index, attempt, trace.build_path, trace));
                Ok(())
            })
        }));

    let stream = run_loop_stream(
        model.clone(),
        None,
        Message::user("use the echo tool"),
        Vec::new(),
        Arc::new(vec![echo_tool()]),
        loop_config,
    );
    let collected = collect_scripted_stream(stream).await;
    assert_eq!(collected.error, None);
    assert_eq!(collected.final_text.as_deref(), Some("after repair"));

    let captures = captures.lock().await;
    let repair_position = captures
        .iter()
        .position(|(_, _, path, _)| *path == AssemblyBuildPath::Repair)
        .expect("the scripted 400s must have produced a repair");
    assert!(
        repair_position + 1 < captures.len(),
        "the script must run at least one turn after the repair; got {:?}",
        captures
            .iter()
            .map(|(turn, attempt, path, _)| (*turn, *attempt, *path))
            .collect::<Vec<_>>()
    );

    for (turn, attempt, _, trace) in captures.iter().skip(repair_position) {
        assert!(
            trace.effective_messages.is_some(),
            "turn {turn} attempt {attempt} was assembled from repaired vectors, so its effective \
             message list must be carried rather than left for a reconstructor to rebuild"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn capture_trace_retains_ephemeral_request_context() {
    let model = ScriptedModel::new(vec![
        RawStreamingChoice::Message("done".to_string()),
        RawStreamingChoice::FinalResponse(()),
    ]);
    let traces: Arc<Mutex<Vec<AssemblyTrace>>> = Arc::new(Mutex::new(Vec::new()));
    let traces_for_sink = Arc::clone(&traces);
    let mut loop_config = config(0);
    loop_config.context_message = Some(Message::user(
        "<context>\nrendered-at-2026-08-07T00:00:00Z\n</context>",
    ));
    loop_config.on_rendered_request = Some(Arc::new(move |_, _, _, trace| {
        let traces = Arc::clone(&traces_for_sink);
        Box::pin(async move {
            traces.lock().await.push(trace);
            Ok(())
        })
    }));

    let collected = collect_scripted_stream(run_loop_stream(
        model,
        None,
        Message::user("hi"),
        Vec::new(),
        Arc::new(Vec::new()),
        loop_config,
    ))
    .await;
    assert_eq!(collected.error, None);

    let traces = traces.lock().await;
    let effective = traces[0]
        .effective_messages
        .as_ref()
        .expect("dynamic request context requires the native oracle");
    assert!(effective.iter().any(is_request_context_message));
}

/// The sibling of the test above for the *other* repair branch.
///
/// `PreStreamDirective::Repair` is handled in two places: once where
/// `model.stream` itself returns `Err`, and once where the first poll of the
/// returned stream fails. Both rebuild with `build_request` and both must
/// report `Repair`. `ScriptedCall::FailStream` only reaches the first;
/// `TurnWithMidStreamError(vec![], …)` reaches the second.
#[tokio::test(start_paused = true)]
async fn capture_seam_reports_the_repair_build_path_from_the_first_poll_branch() {
    let model = ScriptedModel::new_calls(vec![
        ScriptedCall::TurnWithMidStreamError(Vec::new(), parse_400_error("same")),
        ScriptedCall::TurnWithMidStreamError(Vec::new(), parse_400_error("same")),
        ScriptedCall::Turn(vec![
            RawStreamingChoice::Message("repaired".to_string()),
            RawStreamingChoice::FinalResponse(()),
        ]),
    ]);

    let captures: Arc<Mutex<Vec<(usize, u32, AssemblyBuildPath)>>> =
        Arc::new(Mutex::new(Vec::new()));
    let captures_for_sink = captures.clone();
    let mut loop_config = config(0);
    loop_config.on_rendered_request =
        Some(Arc::new(move |turn_index, attempt, _request, trace| {
            let captures = captures_for_sink.clone();
            Box::pin(async move {
                captures
                    .lock()
                    .await
                    .push((turn_index, attempt, trace.build_path));
                Ok(())
            })
        }));

    let stream = run_loop_stream(
        model.clone(),
        None,
        Message::user("hi"),
        Vec::new(),
        Arc::new(Vec::new()),
        loop_config,
    );
    let collected = collect_scripted_stream(stream).await;
    assert_eq!(collected.error, None);
    assert_eq!(collected.final_text.as_deref(), Some("repaired"));

    assert_eq!(
        captures.lock().await.as_slice(),
        &[
            (0, 0, AssemblyBuildPath::Budgeted),
            (0, 1, AssemblyBuildPath::Budgeted),
            (0, 2, AssemblyBuildPath::Repair),
        ]
    );
}

/// The mis-wired-transport backstop, which nothing else exercises.
///
/// The transport is what claims an armed capture and writes the row. A provider
/// stack assembled without `RenderedRequestCapturingHttpClient` — a new
/// `BackendProviderKind`, a wrapper inserted below the capture seam, a builder
/// that forgets it — still streams perfectly well; the only observable trace is
/// that the arm is still pending when the first stream item arrives. Deleting
/// the check at that point would otherwise pass the entire suite while every
/// turn on that backend went uncaptured.
///
/// `ScriptedModel` stands in for exactly that mis-wiring: it answers the loop
/// without ever claiming the pending capture.
#[tokio::test(start_paused = true)]
async fn a_provider_response_with_the_capture_still_armed_fails_the_turn() {
    use crate::rendered_request::scope::{scope_request, test_scope, CaptureScopeKind};
    use crate::rendered_request::{RenderedRequestCaptureSink, RenderedRequestContext};

    let model = ScriptedModel::new(vec![
        RawStreamingChoice::Message("uncaptured".to_string()),
        RawStreamingChoice::FinalResponse(()),
    ]);

    let context = RenderedRequestContext {
        request_doc_id: "doc-1".to_string(),
        request_provenance: Some(crate::document_version::test_request_execution_provenance(
            "doc-1",
            "did:key:agent",
        )),
        inference_call_provenance_scope:
            crate::rendered_request::InferenceCallProvenanceScope::StaticOrTest,
        transcript_snapshot: Vec::new(),
        config_provenance_scope: crate::rendered_request::ConfigProvenanceScope::StaticOrOneShot,
        config_provenance: None,
        request_id: "req-1".to_string(),
        agent_did: "did:key:agent".to_string(),
        requester_did: String::new(),
        behavior_id: "general".to_string(),
        session_id: "session-1".to_string(),
        model_name: "model".to_string(),
    };
    let sink: RenderedRequestCaptureSink = Arc::new(|_| {
        Box::pin(async { Ok(crate::rendered_request::test_static_rendered_request_version()) })
    });
    let scope = test_scope(context, sink);

    let mut loop_config = config(0);
    // The production arming sink: it arms the ambient scope and leaves the
    // write to the transport, which in this stack does not exist.
    loop_config.on_rendered_request = Some(crate::rendered_request::scope::ambient_arming_sink(
        CaptureScopeKind::Inference,
    ));

    let collected = scope_request(scope, async {
        let stream = run_loop_stream(
            model.clone(),
            None,
            Message::user("hi"),
            Vec::new(),
            Arc::new(Vec::new()),
            loop_config,
        );
        collect_scripted_stream(stream).await
    })
    .await;

    let error = collected
        .error
        .as_deref()
        .expect("a response with no durable capture must terminate the turn");
    assert!(
        error.contains("missing its capturing transport"),
        "the failure must name the mis-wired stack: {error}"
    );
    assert_eq!(
        collected.final_text, None,
        "no turn may complete on a provider response nothing captured"
    );
}

/// The same backstop must fire when the provider returns EOF without yielding
/// an item; otherwise the item-level check is never reached and the loop can
/// misclassify an uncaptured send as an ordinary empty completion.
#[tokio::test(start_paused = true)]
async fn an_empty_provider_stream_with_the_capture_still_armed_fails_the_turn() {
    use crate::rendered_request::scope::{scope_request, test_scope, CaptureScopeKind};
    use crate::rendered_request::{RenderedRequestCaptureSink, RenderedRequestContext};

    let model = ScriptedModel::new(Vec::new());
    let context = RenderedRequestContext {
        request_doc_id: "doc-empty".to_string(),
        request_provenance: Some(crate::document_version::test_request_execution_provenance(
            "doc-empty",
            "did:key:agent",
        )),
        inference_call_provenance_scope:
            crate::rendered_request::InferenceCallProvenanceScope::StaticOrTest,
        transcript_snapshot: Vec::new(),
        config_provenance_scope: crate::rendered_request::ConfigProvenanceScope::StaticOrOneShot,
        config_provenance: None,
        request_id: "req-empty".to_string(),
        agent_did: "did:key:agent".to_string(),
        requester_did: String::new(),
        behavior_id: "general".to_string(),
        session_id: "session-empty".to_string(),
        model_name: "model".to_string(),
    };
    let sink: RenderedRequestCaptureSink = Arc::new(|_| {
        Box::pin(async { Ok(crate::rendered_request::test_static_rendered_request_version()) })
    });
    let scope = test_scope(context, sink);
    let mut loop_config = config(0);
    loop_config.on_rendered_request = Some(crate::rendered_request::scope::ambient_arming_sink(
        CaptureScopeKind::Inference,
    ));

    let collected = scope_request(scope, async {
        collect_scripted_stream(run_loop_stream(
            model,
            None,
            Message::user("hi"),
            Vec::new(),
            Arc::new(Vec::new()),
            loop_config,
        ))
        .await
    })
    .await;

    let error = collected
        .error
        .as_deref()
        .expect("an empty uncaptured response must terminate the turn");
    assert!(error.contains("missing its capturing transport"), "{error}");
    assert_eq!(collected.final_text, None);
}

#[tokio::test(start_paused = true)]
async fn first_stream_poll_parse_400_uses_pre_stream_retry_policy() {
    let model = ScriptedModel::new_calls(vec![
        ScriptedCall::TurnWithMidStreamError(Vec::new(), parse_400_error("same")),
        ScriptedCall::TurnWithMidStreamError(Vec::new(), parse_400_error("same")),
        ScriptedCall::Turn(vec![
            RawStreamingChoice::Message("repaired".to_string()),
            RawStreamingChoice::FinalResponse(()),
        ]),
    ]);

    let stream = run_loop_stream(
        model.clone(),
        None,
        Message::user("hi"),
        Vec::new(),
        Arc::new(Vec::new()),
        config(0),
    );
    let collected = collect_scripted_stream(stream).await;

    assert_eq!(collected.final_text.as_deref(), Some("repaired"));
    assert_eq!(collected.error, None);
    assert_eq!(collected.attempts.len(), 2);
    assert!(collected.attempts.iter().all(|attempt| attempt.will_retry));
    assert_duration_in_range(collected.attempts[0].backoff, 3_750, 6_250);
    assert_eq!(collected.attempts[1].backoff, Duration::ZERO);

    let histories = model.seen_histories().await;
    assert_eq!(
        histories.len(),
        3,
        "first-poll parse failure, resample, and repaired retry"
    );
    assert_eq!(
        histories[0], histories[1],
        "first parse-400 retry must resample the same provider request"
    );
}

#[tokio::test(start_paused = true)]
async fn permanent_400_fails_immediately() {
    let model =
        ScriptedModel::new_calls(vec![ScriptedCall::FailStream(permanent_provider_error())]);

    let stream = run_loop_stream(
        model,
        None,
        Message::user("hi"),
        Vec::new(),
        Arc::new(Vec::new()),
        config(0),
    );
    let collected = collect_scripted_stream(stream).await;

    assert_eq!(collected.final_text, None);
    assert_eq!(collected.attempts.len(), 1);
    assert!(!collected.attempts[0].will_retry);
    assert_eq!(collected.attempts[0].backoff, Duration::ZERO);
    let error = collected.error.expect("permanent 400 should fail");
    assert!(
        error.contains("duplicate field max_tokens")
            && !error.contains("completion retry budget exhausted"),
        "permanent 400 should not be retried or wrapped as budget exhaustion; got {error}"
    );
}

#[tokio::test(start_paused = true)]
async fn deadline_fail_fast_pre_sleep() {
    let model = ScriptedModel::new_calls(vec![ScriptedCall::FailStream(transient_provider_error(
        "too late",
    ))]);
    let mut loop_config = config(0);
    loop_config.retry_policy = crate::agent::completion_retry::CompletionRetryPolicy {
        transport_backoff: vec![Duration::from_secs(30)],
        max_resample: 0,
        allow_repair: false,
    };
    loop_config.deadline = Some(chrono::Utc::now() + chrono::Duration::seconds(10));

    let stream = run_loop_stream(
        model,
        None,
        Message::user("hi"),
        Vec::new(),
        Arc::new(Vec::new()),
        loop_config,
    );
    futures::pin_mut!(stream);
    let started_at = tokio::time::Instant::now();

    let first = stream
        .next()
        .await
        .expect("deadline failure should yield attempt event")
        .expect("attempt event should be Ok");
    match first {
        LoopStreamItem::AttemptFailed {
            attempt,
            will_retry,
            backoff,
            ..
        } => {
            assert_eq!(attempt, 0);
            assert!(!will_retry);
            assert_eq!(backoff, Duration::ZERO);
        }
        other => panic!("expected AttemptFailed, got {other:?}"),
    }

    let second = stream
        .next()
        .await
        .expect("deadline failure should yield terminal error");
    assert!(
        second.is_err(),
        "expected terminal deadline error: {second:?}"
    );
    assert_eq!(
        tokio::time::Instant::now(),
        started_at,
        "deadline fail-fast must not sleep before failing"
    );
}

#[tokio::test(start_paused = true)]
async fn retry_reissues_same_request() {
    let model = ScriptedModel::new_calls(vec![
        ScriptedCall::FailStream(transient_provider_error("reset")),
        ScriptedCall::Turn(vec![
            RawStreamingChoice::Message("ok".to_string()),
            RawStreamingChoice::FinalResponse(()),
        ]),
    ]);

    let stream = run_loop_stream(
        model.clone(),
        None,
        Message::user("use the tool"),
        Vec::new(),
        Arc::new(vec![echo_tool()]),
        config(1),
    );
    let collected = collect_scripted_stream(stream).await;

    assert_eq!(collected.final_text.as_deref(), Some("ok"));
    assert_eq!(collected.error, None);
    let histories = model.seen_histories().await;
    let tools = model.seen_tools().await;
    assert_eq!(histories.len(), 2);
    assert_eq!(tools.len(), 2);
    assert_eq!(histories[0], histories[1]);
    assert_eq!(tools[0], tools[1]);
}

#[tokio::test(start_paused = true)]
async fn mid_stream_decode_error_without_effects_retracts_and_resamples() {
    let model = ScriptedModel::new_calls(vec![
        ScriptedCall::TurnWithMidStreamError(
            vec![RawStreamingChoice::Message("Hel".to_string())],
            transient_provider_error("decode"),
        ),
        ScriptedCall::Turn(vec![
            RawStreamingChoice::Message("Hello ".to_string()),
            RawStreamingChoice::Message("world".to_string()),
            RawStreamingChoice::FinalResponse(()),
        ]),
    ]);

    let stream = run_loop_stream(
        model.clone(),
        None,
        Message::user("hi"),
        Vec::new(),
        Arc::new(Vec::new()),
        config(0),
    );
    let collected = collect_scripted_stream(stream).await;

    assert_eq!(
        collected.text_chunks,
        vec!["Hel".to_string(), "Hello ".to_string(), "world".to_string()]
    );
    assert_eq!(collected.retractions, vec![(0, 0)]);
    assert_eq!(collected.final_text.as_deref(), Some("Hello world"));
    assert_eq!(collected.error, None);

    let histories = model.seen_histories().await;
    assert_eq!(histories.len(), 2);
    assert_eq!(
        histories[0], histories[1],
        "mid-stream retraction must reissue the same turn request"
    );
}

#[tokio::test(start_paused = true)]
async fn reasoning_only_completion_retracts_and_resamples() {
    let model = ScriptedModel::new_calls(vec![
        ScriptedCall::Turn(vec![
            RawStreamingChoice::ReasoningDelta {
                id: None,
                reasoning: "still thinking".to_string(),
            },
            RawStreamingChoice::FinalResponse(()),
        ]),
        ScriptedCall::Turn(vec![
            RawStreamingChoice::Message("finished answer".to_string()),
            RawStreamingChoice::FinalResponse(()),
        ]),
    ]);

    let stream = run_loop_stream(
        model.clone(),
        None,
        Message::user("solve this"),
        Vec::new(),
        Arc::new(Vec::new()),
        config(0),
    );
    let collected = collect_scripted_stream(stream).await;

    assert_eq!(collected.retractions, vec![(0, 0)]);
    assert_eq!(collected.final_text.as_deref(), Some("finished answer"));
    assert_eq!(collected.error, None);
    assert_eq!(
        model.seen_histories().await.len(),
        2,
        "the reasoning-only turn must be resampled as the same provider turn"
    );
}

#[tokio::test(start_paused = true)]
async fn mid_stream_failure_after_tool_ran_closes_turn_and_continues() {
    let calls = Arc::new(AtomicUsize::new(0));
    let model = ScriptedModel::new_calls(vec![
        ScriptedCall::TurnWithMidStreamError(
            vec![RawStreamingChoice::ToolCall(RawStreamingToolCall::new(
                "call-1".to_string(),
                "echo".to_string(),
                serde_json::json!({}),
            ))],
            transient_provider_error("decode after tool"),
        ),
        ScriptedCall::Turn(vec![
            RawStreamingChoice::Message("done".to_string()),
            RawStreamingChoice::FinalResponse(()),
        ]),
    ]);
    let tools: Vec<Box<dyn ToolDyn>> = vec![Box::new(CountingTool {
        name: "echo".to_string(),
        output: "ECHOED".to_string(),
        calls: calls.clone(),
    })];

    let stream = run_loop_stream(
        model.clone(),
        None,
        Message::user("use the echo tool"),
        Vec::new(),
        Arc::new(tools),
        config(4),
    );
    let collected = collect_scripted_stream(stream).await;

    assert_eq!(collected.tool_results, vec!["ECHOED".to_string()]);
    assert_eq!(collected.final_text.as_deref(), Some("done"));
    assert_eq!(collected.error, None);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(collected.attempts.len(), 1);
    assert_eq!(collected.attempts[0].turn, 0);
    assert_eq!(collected.attempts[0].attempt, 0);
    assert!(collected.attempts[0].will_retry);
    assert_duration_in_range(collected.attempts[0].backoff, 3_750, 6_250);

    let histories = model.seen_histories().await;
    assert_eq!(
        histories.len(),
        2,
        "effectful mid-stream failure should close the turn then continue"
    );
    assert!(
        history_has_tool_call(&histories[1], "echo"),
        "continued request must include the assistant tool call: {:?}",
        histories[1]
    );
    assert!(
        history_has_tool_result_text(&histories[1], "ECHOED"),
        "continued request must include the tool result: {:?}",
        histories[1]
    );
}

#[tokio::test(start_paused = true)]
async fn mid_stream_failure_after_tool_budget_exhausted_fails() {
    let calls = Arc::new(AtomicUsize::new(0));
    let model = ScriptedModel::new_calls(vec![ScriptedCall::TurnWithMidStreamError(
        vec![RawStreamingChoice::ToolCall(RawStreamingToolCall::new(
            "call-1".to_string(),
            "echo".to_string(),
            serde_json::json!({}),
        ))],
        transient_provider_error("decode after tool"),
    )]);
    let tools: Vec<Box<dyn ToolDyn>> = vec![Box::new(CountingTool {
        name: "echo".to_string(),
        output: "ECHOED".to_string(),
        calls: calls.clone(),
    })];
    let mut loop_config = config(4);
    loop_config.retry_policy = crate::agent::completion_retry::CompletionRetryPolicy {
        transport_backoff: Vec::new(),
        max_resample: 0,
        allow_repair: false,
    };

    let stream = run_loop_stream(
        model,
        None,
        Message::user("use the echo tool"),
        Vec::new(),
        Arc::new(tools),
        loop_config,
    );
    let collected = collect_scripted_stream(stream).await;

    assert_eq!(collected.final_text, None);
    assert_eq!(collected.tool_results, Vec::<String>::new());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let error = collected
        .error
        .expect("effectful retry exhaustion should fail");
    assert!(
        error.contains("completion retry budget exhausted")
            && error.contains("transport retry budget exhausted"),
        "terminal error must report exhausted effectful retry budget; got {error}"
    );
}

#[tokio::test]
async fn context_message_is_sent_before_prompt() {
    let model = ScriptedModel::new(vec![
        RawStreamingChoice::Message("ok".to_string()),
        RawStreamingChoice::FinalResponse(()),
    ]);
    let mut cfg = config(0);
    cfg.context_message = Some(Message::user(
        "<context>\nnow=2026-06-15T00:00:00Z\n</context>",
    ));

    let stream = run_loop_stream(
        model.clone(),
        None,
        Message::user("actual prompt"),
        Vec::new(),
        Arc::new(Vec::new()),
        cfg,
    );
    futures::pin_mut!(stream);
    while stream.next().await.is_some() {}

    let histories = model.seen_histories().await;
    assert_eq!(histories.len(), 1);
    assert!(matches!(
        &histories[0][0],
        Message::User { content }
            if matches!(first_content(content), UserContent::Text(text) if text.text.starts_with("<context>"))
    ));
}

#[tokio::test]
async fn tool_call_turn_executes_threads_result_and_completes() {
    let (node, hook, _identity) = test_hook().await;
    ready_hook_for(&hook).await;
    let prompt = Message::user("use the echo tool");

    // Turn 1: the model calls `echo`. Turn 2: it answers with text.
    let model = ScriptedModel::new_turns(vec![
        vec![
            RawStreamingChoice::ToolCall(RawStreamingToolCall::new(
                "call-1".to_string(),
                "echo".to_string(),
                serde_json::json!({}),
            )),
            RawStreamingChoice::FinalResponse(()),
        ],
        vec![
            RawStreamingChoice::Message("done".to_string()),
            RawStreamingChoice::FinalResponse(()),
        ],
    ]);
    let tools: Vec<Box<dyn ToolDyn>> = vec![Box::new(EchoTool {
        name: "echo".to_string(),
        output: "ECHOED".to_string(),
    })];

    let stream = run_loop_stream(
        model,
        Some(hook),
        prompt,
        Vec::new(),
        Arc::new(tools),
        config(4),
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

    // The tool ran, its (bounded) result was threaded/yielded, and the loop
    // reached a text response on the next turn.
    assert_eq!(tool_results, vec!["ECHOED".to_string()]);
    assert_eq!(final_text.as_deref(), Some("done"));

    // The generator drove the tool-call lifecycle directly: on_tool_call started
    // it and on_tool_result completed it with the result. (The tool-result
    // *message* persistence is split with StreamProcessor — exercised once the
    // generator is wired into the consumer in step 3 — so it is not asserted
    // here against the standalone generator.)
    let resp = node
        .execute("query { AgentToolCall { tool_name lifecycle_state result } }")
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
    assert!(
        rows.iter().any(|row| {
            row.get("tool_name").and_then(|value| value.as_str()) == Some("echo")
                && row.get("lifecycle_state").and_then(|value| value.as_str()) == Some("completed")
                && row
                    .get("result")
                    .and_then(|value| value.as_str())
                    .is_some_and(|result| result.contains("ECHOED"))
        }),
        "expected a completed echo tool call recording the result; rows: {rows:?}"
    );
}

#[tokio::test]
async fn tool_executes_before_provider_stalls_mid_stream() {
    // P2 regression: a provider that emits a tool call then stalls before EOF
    // must still have its tool executed. Rig runs each tool inline as its
    // ToolCall arrives, so the lifecycle / AgentToolCall row exists before the
    // stall; the daemon liveness timeout then has something to cancel. The old
    // design collected tool calls and dispatched only after the stream drained,
    // so a mid-stream stall left the tool unrun with nothing to mark.
    let (node, hook, _identity) = test_hook().await;
    ready_hook_for(&hook).await;
    let prompt = Message::user("use the echo tool then stall");

    // One turn: emit a tool call, then hang (no FinalResponse, no EOF).
    let model = ScriptedModel::new_stalling(vec![RawStreamingChoice::ToolCall(
        RawStreamingToolCall::new(
            "call-1".to_string(),
            "echo".to_string(),
            serde_json::json!({}),
        ),
    )]);
    let tools: Vec<Box<dyn ToolDyn>> = vec![Box::new(EchoTool {
        name: "echo".to_string(),
        output: "ECHOED".to_string(),
    })];

    let stream = run_loop_stream(
        model,
        Some(hook),
        prompt,
        Vec::new(),
        Arc::new(tools),
        config(4),
    );
    futures::pin_mut!(stream);

    // Item 1 is the tool call. Resuming the stream then runs the tool inline and
    // afterwards blocks forever on the stalled provider — so the second poll
    // never returns, but the tool executes before that block. Bound it.
    let first = stream.next().await.expect("should yield the tool call");
    assert!(
        matches!(
            first,
            Ok(LoopStreamItem::Item(
                MultiTurnStreamItem::StreamAssistantItem(StreamedAssistantContent::ToolCall { .. })
            ))
        ),
        "first item should be the tool call; got {first:?}"
    );
    let _ = tokio::time::timeout(tokio::time::Duration::from_secs(3), stream.next()).await;

    // Despite the stall, the tool ran to completion (its row exists, recorded).
    let resp = node
        .execute("query { AgentToolCall { tool_name lifecycle_state result } }")
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
    assert!(
        rows.iter().any(|row| {
            row.get("tool_name").and_then(|value| value.as_str()) == Some("echo")
                && row
                    .get("result")
                    .and_then(|value| value.as_str())
                    .is_some_and(|result| result.contains("ECHOED"))
        }),
        "tool must execute inline before the provider stall; rows: {rows:?}"
    );
}

#[tokio::test]
async fn tool_definition_receives_prompt_rag_text() {
    // P3/compat: tool definitions must be built with the prompt's rag text (rig
    // parity), not String::new(), so prompt-aware tools keep the task context.
    let (_node, hook, _identity) = test_hook().await;
    ready_hook_for(&hook).await;
    let seen = Arc::new(Mutex::new(None));
    let tool: Box<dyn ToolDyn> = Box::new(RecordingDefinitionTool {
        seen_prompt: seen.clone(),
    });

    // A single text-only turn; the tool is never called, but its definition is
    // still requested when the request is built.
    let model = ScriptedModel::new(vec![
        RawStreamingChoice::Message("hi".to_string()),
        RawStreamingChoice::FinalResponse(()),
    ]);
    let stream = run_loop_stream(
        model,
        Some(hook),
        Message::user("teach me rust"),
        Vec::new(),
        Arc::new(vec![tool]),
        config(1),
    );
    futures::pin_mut!(stream);
    while stream.next().await.is_some() {}

    assert_eq!(
        seen.lock().await.as_deref(),
        Some("teach me rust"),
        "tool definition should receive the prompt's rag text, not an empty string"
    );
}

#[tokio::test]
async fn exceeding_max_turns_terminates_with_error() {
    let (_node, hook, _identity) = test_hook().await;
    let prompt = Message::user("loop");
    ready_hook_for(&hook).await;

    // max_turns = 0 permits one tool round-trip (2 completions, matching rig);
    // a model that keeps calling tools is blocked on the completion past the cap
    // and surfaces a max-turns error.
    let model = ScriptedModel::new_turns(vec![echo_tool_turn(), echo_tool_turn()]);
    let stream = run_loop_stream(
        model,
        Some(hook),
        prompt,
        Vec::new(),
        Arc::new(vec![echo_tool()]),
        config(0),
    );
    futures::pin_mut!(stream);

    let mut items = Vec::new();
    while let Some(item) = stream.next().await {
        items.push(item);
    }

    let last = items.last().expect("stream should yield at least one item");
    assert!(last.is_err(), "expected a terminal error; got {last:?}");
    // Permanent `StreamingError::Prompt(MaxTurnsError)` (rig's variant), not a
    // retryable `Completion(ResponseError)` — turn exhaustion must not retry.
    let error = last.as_ref().err().unwrap();
    assert!(
        matches!(
            error,
            rig::agent::StreamingError::Prompt(prompt_error)
                if matches!(**prompt_error, rig::completion::PromptError::MaxTurnsError { .. })
        ),
        "expected a max-turns Prompt error; got {last:?}"
    );
    // And it must classify as a permanent failure: retrying turn exhaustion would
    // re-run the loop (and its tools) to no purpose.
    assert!(
        !crate::error::classify_completion_error(error).is_retryable(),
        "max-turns exhaustion must be non-retryable; got {last:?}"
    );
    // The Harbor adapter (scripts/harbor/run_gents.sh) classifies budget
    // exhaustion by matching the persisted error message's exact prefix:
    // `agent stream failed: ` (agent/daemon/inference.rs) followed by this
    // display. If rig's wording changes, MaxTurn trials silently revert to
    // Harbor infrastructure exceptions instead of verifier-scored attempts.
    assert!(
        error.to_string().starts_with("PromptError: MaxTurnError: "),
        "max-turns error display must start with the anchored Harbor prefix; got {error}"
    );
}

#[tokio::test]
async fn managed_terminal_tool_result_terminates_loop() {
    let (_node, hook, _identity) = test_hook().await;
    let prompt = Message::user("run the slow tool");
    ready_hook_for(&hook).await;

    // With the typed outcome channel a tool CANNOT fabricate a managed
    // terminal: run the loop with an already-expired request deadline so the
    // dispatcher's own envelope produces `ToolOutcome::TimedOut`, and
    // on_tool_result terminates the loop.
    let tools: Vec<Box<dyn ToolDyn>> = vec![Box::new(FixedTool {
        name: "echo".to_string(),
        output: "unreachable".to_string(),
    })];
    let model = ScriptedModel::new_turns(vec![echo_tool_turn()]);
    let stream = run_loop_stream(
        model,
        Some(hook),
        prompt,
        Vec::new(),
        Arc::new(tools),
        config(4),
    );
    futures::pin_mut!(stream);

    // The daemon installs the tool runtime scope around stream polling; an
    // already-expired deadline makes the dispatcher's envelope resolve the
    // tool call to `ToolOutcome::TimedOut`.
    let items = crate::tool_call_lifecycle::runtime::scope_request_tool_execution(
        Some(chrono::Utc::now() - chrono::Duration::seconds(1)),
        tokio_util::sync::CancellationToken::new(),
        async {
            let mut items = Vec::new();
            while let Some(item) = stream.next().await {
                items.push(item);
            }
            items
        },
    )
    .await;

    let last = items.last().expect("stream should yield at least one item");
    assert!(last.is_err(), "expected a terminal error; got {last:?}");
    assert!(
        format!("{:?}", last.as_ref().err().unwrap()).contains("deadline"),
        "expected a deadline/timeout terminate; got {last:?}"
    );
}

#[tokio::test]
async fn threaded_assistant_turn_carries_provider_message_id() {
    // P2a regression: the in-loop assistant message threaded back to the provider
    // must carry the provider message id (OpenAI Responses / ChatGPT Codex
    // follow-up requests reference prior `msg_` ids). Turn 1 emits a MessageId
    // plus a tool call; the tool result drives turn 2, whose request history must
    // contain the assistant tool-call message tagged with that id.
    let (_node, hook, _identity) = test_hook().await;
    ready_hook_for(&hook).await;

    let model = ScriptedModel::new_turns(vec![
        vec![
            RawStreamingChoice::MessageId("msg_abc123".to_string()),
            RawStreamingChoice::ToolCall(RawStreamingToolCall::new(
                "call-1".to_string(),
                "echo".to_string(),
                serde_json::json!({}),
            )),
            RawStreamingChoice::FinalResponse(()),
        ],
        vec![
            RawStreamingChoice::Message("done".to_string()),
            RawStreamingChoice::FinalResponse(()),
        ],
    ]);

    let stream = run_loop_stream(
        model.clone(),
        Some(hook),
        Message::user("go"),
        Vec::new(),
        Arc::new(vec![echo_tool()]),
        config(4),
    );
    futures::pin_mut!(stream);
    while stream.next().await.is_some() {}

    let histories = model.seen_histories().await;
    assert_eq!(
        histories.len(),
        2,
        "expected two completion turns; got {histories:?}"
    );
    let assistant_id = histories[1].iter().find_map(|message| match message {
        Message::Assistant { id, .. } => Some(id.clone()),
        _ => None,
    });
    assert_eq!(
        assistant_id,
        Some(Some("msg_abc123".to_string())),
        "threaded assistant turn must carry the provider message id; history: {:?}",
        histories[1]
    );
}

#[tokio::test]
async fn toolset_is_attached_to_every_completion_request_in_the_loop() {
    // Regression for the CLI tool-loop test: rig's Agent re-sent the full tool
    // list on every turn; the owned loop must too. The follow-up request after a
    // tool result is folded in (turn 2) must still advertise the toolset, or the
    // provider sees a tool-result conversation with no tools.
    let (_node, hook, _identity) = test_hook().await;
    ready_hook_for(&hook).await;

    // Turn 1: the model calls `echo`. Turn 2: it answers with text.
    let model = ScriptedModel::new_turns(vec![
        echo_tool_turn(),
        vec![
            RawStreamingChoice::Message("done".to_string()),
            RawStreamingChoice::FinalResponse(()),
        ],
    ]);
    let stream = run_loop_stream(
        model.clone(),
        Some(hook),
        Message::user("use the echo tool"),
        Vec::new(),
        Arc::new(vec![echo_tool()]),
        config(4),
    );
    futures::pin_mut!(stream);
    while stream.next().await.is_some() {}

    let seen_tools = model.seen_tools().await;
    assert_eq!(
        seen_tools.len(),
        2,
        "expected two completion turns; got {seen_tools:?}"
    );
    for (turn, tools) in seen_tools.iter().enumerate() {
        assert!(
            tools.contains(&"echo".to_string()),
            "completion request for turn {} must advertise the toolset; got {seen_tools:?}",
            turn + 1
        );
    }
}

/// Provider-request invariants: what every completion request the loop emits
/// must satisfy, independent of the scenario that produced it. Mirrors the
/// provider-side contract that `sanitize_history_for_provider` enforces for
/// loaded history — the loop must satisfy it by construction for the messages
/// it threads itself.
fn assert_provider_request_invariants(turn: usize, history: &[Message]) {
    let mut pending_call_keys: Vec<String> = Vec::new();
    for message in history {
        match message {
            Message::Assistant { content, .. } => {
                assert!(
                    pending_call_keys.is_empty(),
                    "turn {turn}: assistant message before prior turn's tool calls were resolved"
                );
                // Ordering: no text or reasoning after a tool call.
                let mut seen_tool_call = false;
                for item in content.iter() {
                    match item {
                        AssistantContent::ToolCall(tool_call) => {
                            seen_tool_call = true;
                            pending_call_keys.push(
                                tool_call
                                    .call_id
                                    .clone()
                                    .unwrap_or_else(|| tool_call.id.clone()),
                            );
                        }
                        _ => assert!(
                            !seen_tool_call,
                            "turn {turn}: assistant content after a tool call (providers reject)"
                        ),
                    }
                }
            }
            Message::User { content } => {
                let has_tool_results = content
                    .iter()
                    .any(|item| matches!(item, UserContent::ToolResult(_)));
                assert!(
                    has_tool_results || pending_call_keys.is_empty(),
                    "turn {turn}: ordinary user content before prior tool calls were resolved"
                );
                for item in content.iter() {
                    if let UserContent::ToolResult(tool_result) = item {
                        let key = tool_result
                            .call_id
                            .clone()
                            .unwrap_or_else(|| tool_result.id.clone());
                        let position = pending_call_keys.iter().position(|call| call == &key);
                        assert!(
                            position.is_some(),
                            "turn {turn}: tool result '{key}' without a preceding tool call"
                        );
                        pending_call_keys.remove(position.unwrap());
                    }
                }
            }
            Message::System { .. } => assert!(
                pending_call_keys.is_empty(),
                "turn {turn}: system message before prior tool calls were resolved"
            ),
        }
    }
    assert!(
        pending_call_keys.is_empty(),
        "turn {turn}: unpaired tool calls reached the provider: {pending_call_keys:?}"
    );
}

#[tokio::test]
async fn every_request_in_a_tool_loop_satisfies_provider_invariants() {
    // Conformance guard for the loop's own threading: across a multi-tool,
    // multi-turn run, every request's history must pair calls with results and
    // keep assistant content provider-ordered — by construction, no sanitizer.
    let (_node, hook, _identity) = test_hook().await;
    ready_hook_for(&hook).await;

    let model = ScriptedModel::new_turns(vec![
        // Turn 1: text + reasoning + two tool calls in one assistant turn.
        vec![
            RawStreamingChoice::Message("let me check".to_string()),
            RawStreamingChoice::ToolCall(RawStreamingToolCall::new(
                "call-1".to_string(),
                "echo".to_string(),
                serde_json::json!({}),
            )),
            RawStreamingChoice::ToolCall(RawStreamingToolCall::new(
                "call-2".to_string(),
                "echo".to_string(),
                serde_json::json!({}),
            )),
            RawStreamingChoice::FinalResponse(()),
        ],
        // Turn 2: one more tool call.
        echo_tool_turn(),
        // Turn 3: final text.
        vec![
            RawStreamingChoice::Message("done".to_string()),
            RawStreamingChoice::FinalResponse(()),
        ],
    ]);

    let stream = run_loop_stream(
        model.clone(),
        Some(hook),
        Message::user("run the tools"),
        Vec::new(),
        Arc::new(vec![echo_tool()]),
        config(4),
    );
    futures::pin_mut!(stream);
    while stream.next().await.is_some() {}

    let histories = model.seen_histories().await;
    assert_eq!(
        histories.len(),
        3,
        "expected three completion turns; got {histories:?}"
    );
    for (index, history) in histories.iter().enumerate() {
        assert_provider_request_invariants(index + 1, history);
    }
}

#[tokio::test]
async fn dirty_caller_history_is_sanitized_at_loop_entry() {
    // Chokepoint guarantee: EVERY owned-loop consumer (daemon, oneshot,
    // compaction summarize, title, subagent children) sends provider-valid
    // history because the loop sanitizes the caller-provided history at entry
    // — no call site can forget the sanitizer. Feed a dirty history (unpaired
    // call, orphaned result, text-after-call ordering) and assert the request
    // on the wire satisfies the provider invariants.
    let (_node, hook, _identity) = test_hook().await;
    ready_hook_for(&hook).await;

    let unpaired_call = crate::llm::message::ToolCall {
        id: "call-unpaired".to_string(),
        call_id: Some("call-unpaired".to_string()),
        function: crate::llm::message::ToolFunction {
            name: "echo".to_string(),
            arguments: serde_json::json!({}),
        },
        signature: None,
        additional_params: None,
    };
    let paired_call = crate::llm::message::ToolCall {
        id: "call-paired".to_string(),
        call_id: Some("call-paired".to_string()),
        function: crate::llm::message::ToolFunction {
            name: "echo".to_string(),
            arguments: serde_json::json!({}),
        },
        signature: None,
        additional_params: None,
    };
    let dirty_history = vec![
        // Orphaned result: its call was compacted away.
        Message::User {
            content: vec![UserContent::tool_result(
                "call-gone".to_string(),
                vec![crate::llm::message::ToolResultContent::text("orphaned")],
            )],
        },
        // Misordered assistant turn (text AFTER calls) with one unpaired call.
        Message::Assistant {
            id: None,
            content: vec![
                AssistantContent::ToolCall(paired_call),
                AssistantContent::ToolCall(unpaired_call),
                AssistantContent::Text(crate::llm::message::Text {
                    text: "stale ordering".to_string(),
                }),
            ],
        },
        Message::User {
            content: vec![UserContent::tool_result(
                "call-paired".to_string(),
                vec![crate::llm::message::ToolResultContent::text("ok")],
            )],
        },
    ];

    let model = ScriptedModel::new(vec![
        RawStreamingChoice::Message("hi".to_string()),
        RawStreamingChoice::FinalResponse(()),
    ]);
    let stream = run_loop_stream(
        model.clone(),
        Some(hook),
        Message::user("continue"),
        dirty_history,
        Arc::new(Vec::new()),
        config(1),
    );
    futures::pin_mut!(stream);
    while stream.next().await.is_some() {}

    let histories = model.seen_histories().await;
    assert_eq!(histories.len(), 1);
    assert_provider_request_invariants(1, &histories[0]);
    // The unpaired call is gone but the paired exchange survives.
    let kept_calls: Vec<String> = histories[0]
        .iter()
        .filter_map(|message| match message {
            Message::Assistant { content, .. } => Some(
                content
                    .iter()
                    .filter_map(|item| match item {
                        AssistantContent::ToolCall(call) => Some(call.id.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .flatten()
        .collect();
    assert_eq!(
        kept_calls,
        vec!["call-paired".to_string()],
        "unpaired call dropped, paired call kept; history: {:?}",
        histories[0]
    );
}

#[tokio::test]
async fn oversized_tool_result_is_bounded_before_threading() {
    let (_node, hook, _identity) = test_hook().await;
    let prompt = Message::user("read the big thing");
    ready_hook_for(&hook).await;

    // A tool returning far more than the default limits: the model-facing
    // (threaded/yielded) result must be bounded, while on_tool_result still
    // receives the full output for spill (#401 closed natively).
    let big_line = "x".repeat(200);
    let big_output = std::iter::repeat(big_line)
        .take(10_000)
        .collect::<Vec<_>>()
        .join("\n");
    let full_len = big_output.len();
    let tools: Vec<Box<dyn ToolDyn>> = vec![Box::new(FixedTool {
        name: "echo".to_string(),
        output: big_output,
    })];
    let model = ScriptedModel::new_turns(vec![
        echo_tool_turn(),
        vec![
            RawStreamingChoice::Message("ok".to_string()),
            RawStreamingChoice::FinalResponse(()),
        ],
    ]);
    let stream = run_loop_stream(
        model,
        Some(hook),
        prompt,
        Vec::new(),
        Arc::new(tools),
        config(4),
    );
    futures::pin_mut!(stream);

    let mut bounded_len = None;
    while let Some(item) = stream.next().await {
        if let LoopStreamItem::Item(MultiTurnStreamItem::StreamUserItem(
            StreamedUserContent::ToolResult { tool_result, .. },
        )) = item.expect("loop item should be Ok")
        {
            bounded_len = Some(
                tool_result_text(&crate::llm::rig_compat::from_rig_tool_result_content(
                    &tool_result.content.first(),
                ))
                .len(),
            );
        }
    }

    let bounded_len = bounded_len.expect("a tool result should have been yielded");
    assert!(
        bounded_len < full_len,
        "expected the threaded result to be bounded: bounded={bounded_len} full={full_len}"
    );
    assert!(bounded_len > 0, "bounded result should be non-empty");
}

#[tokio::test]
async fn run_loop_to_text_persists_assistant_reply() {
    // Regression: one-shot (run_loop_to_text) must persist the assistant reply,
    // not just the user prompt.
    let (node, hook, _identity) = test_hook().await;
    ready_hook_for(&hook).await;
    let model = ScriptedModel::new(vec![
        RawStreamingChoice::Message("the answer".to_string()),
        RawStreamingChoice::FinalResponse(()),
    ]);

    let reply = run_loop_to_text(
        model,
        Some(hook.clone()),
        Message::user("the question"),
        Vec::new(),
        Arc::new(Vec::new()),
        config(0),
    )
    .await
    .expect("run_loop_to_text should succeed");
    assert_eq!(reply, "the answer");

    let session_id = hook.session_id().await.expect("session id");
    let history = crate::session::load_history(&node, &session_id)
        .await
        .unwrap();
    assert!(
        history.iter().any(|message| matches!(message,
            Message::Assistant { content, .. }
                if content.iter().any(|c| matches!(c, AssistantContent::Text(text)
                    if text.text == "the answer")))),
        "one-shot must persist the assistant reply; history: {history:?}"
    );
}

#[tokio::test]
async fn run_loop_to_text_persists_tool_using_transcript() {
    // Regression: for tool-using one-shots, both the assistant tool-call turn and
    // the tool-result message must be persisted (tool-result persistence gates on
    // the assistant turn being persisted first).
    let (node, hook, _identity) = test_hook().await;
    ready_hook_for(&hook).await;
    let model = ScriptedModel::new_turns(vec![
        echo_tool_turn(),
        vec![
            RawStreamingChoice::Message("done".to_string()),
            RawStreamingChoice::FinalResponse(()),
        ],
    ]);

    let reply = run_loop_to_text(
        model,
        Some(hook.clone()),
        Message::user("use the echo tool"),
        Vec::new(),
        Arc::new(vec![echo_tool()]),
        config(4),
    )
    .await
    .expect("run_loop_to_text should succeed");
    assert_eq!(reply, "done");

    let session_id = hook.session_id().await.expect("session id");
    let history = crate::session::load_history(&node, &session_id)
        .await
        .unwrap();
    assert!(
        history.iter().any(|message| matches!(message,
            Message::User { content }
                if content.iter().any(|c| matches!(c, UserContent::ToolResult(result)
                    if tool_result_text(first_content(&result.content)) == "ECHOED")))),
        "tool-using one-shot must persist the tool-result message; history: {history:?}"
    );
    assert!(
        history.iter().any(|message| matches!(message,
            Message::Assistant { content, .. }
                if content.iter().any(|c| matches!(c, AssistantContent::Text(text)
                    if text.text == "done")))),
        "tool-using one-shot must persist the final assistant reply; history: {history:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn run_loop_to_text_retract_persists_only_the_resample() {
    // #648 HIGH: the one-shot consumer must reset its accumulator on
    // TurnRetracted (mirroring StreamProcessor). Without the reset, the
    // retracted partial ("Based on") concatenates with the resample and the
    // durable assistant message becomes "Based onThe answer is 42" — corrupting
    // the transcript that feeds future history and training capture, even though
    // the returned string is correct. This fences that exact regression.
    let (node, hook, _identity) = test_hook().await;
    ready_hook_for(&hook).await;
    let model = ScriptedModel::new_calls(vec![
        ScriptedCall::TurnWithMidStreamError(
            vec![RawStreamingChoice::Message("Based on".to_string())],
            transient_provider_error("decode"),
        ),
        ScriptedCall::Turn(vec![
            RawStreamingChoice::Message("The answer is 42".to_string()),
            RawStreamingChoice::FinalResponse(()),
        ]),
    ]);

    let reply = run_loop_to_text(
        model,
        Some(hook.clone()),
        Message::user("hi"),
        Vec::new(),
        Arc::new(Vec::new()),
        config(0),
    )
    .await
    .expect("run_loop_to_text should succeed after a mid-stream retract");
    assert_eq!(reply, "The answer is 42");

    let session_id = hook.session_id().await.expect("session id");
    let history = crate::session::load_history(&node, &session_id)
        .await
        .unwrap();
    let assistant_texts: Vec<String> = history
        .iter()
        .filter_map(|message| match message {
            Message::Assistant { content, .. } => Some(content),
            _ => None,
        })
        .flatten()
        .filter_map(|content| match content {
            AssistantContent::Text(text) => Some(text.text.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        assistant_texts,
        vec!["The answer is 42".to_string()],
        "retract must discard the partial; persisted assistant text: {assistant_texts:?}"
    );
}

#[test]
fn value_to_json_string_passes_strings_through_unquoted() {
    assert_eq!(
        value_to_json_string(&serde_json::json!("plain")),
        "plain".to_string()
    );
    assert_eq!(
        value_to_json_string(&serde_json::json!({"path": "x"})),
        r#"{"path":"x"}"#.to_string()
    );
}

#[test]
fn deadline_remaining_is_zero_when_past() {
    let past = chrono::Utc::now() - chrono::Duration::seconds(5);
    assert_eq!(
        super::deadline_remaining(Some(past)),
        Some(std::time::Duration::ZERO)
    );
    assert_eq!(super::deadline_remaining(None), None);
}

#[test]
fn assembles_context_immediately_before_prompt() {
    // Fences Lean `PromptAssembly.Template.assembleWithContext_tail`: when a
    // per-request context message is present, the assembly ends with exactly
    // [contextPreamble, prompt] — context immediately precedes the prompt.
    let context = Message::user("<context>\nseat: x\n</context>");
    let prompt = Message::user("hello");

    let with_context = super::assemble_new_messages(Some(context.clone()), prompt.clone());
    assert_eq!(with_context.len(), 2);
    assert!(super::is_request_context_message(&with_context[0]));
    assert_eq!(with_context[1], prompt);
    // Context is the immediately-preceding entry before the prompt.
    assert_eq!(&with_context[with_context.len() - 2], &context);

    // Without a context message, the prompt is the sole (last) entry.
    let without = super::assemble_new_messages(None, prompt.clone());
    assert_eq!(without, vec![prompt]);
}

#[test]
fn is_request_context_message_only_matches_context_user_text() {
    assert!(super::is_request_context_message(&Message::user(
        "<context>\nx\n</context>"
    )));
    assert!(!super::is_request_context_message(&Message::user(
        "an ordinary prompt"
    )));
    assert!(!super::is_request_context_message(&Message::assistant(
        "hi"
    )));
}

#[tokio::test]
async fn dispatch_tool_calls_known_tool_and_reports_unknown() {
    // No tool runtime scope is active in this unit test, so dispatch_tool takes
    // the unscoped path: look up by name and call directly.
    let tools: Vec<Box<dyn ToolDyn>> = vec![Box::new(EchoTool {
        name: "echo".to_string(),
        output: "ECHOED".to_string(),
    })];

    assert_eq!(
        super::dispatch_tool(&tools, "echo", "{}".to_string(), None).await,
        crate::tool_call_lifecycle::ToolOutcome::Completed("ECHOED".to_string())
    );
    // An unresolved tool name is a dispatch FAILURE carried as typed data.
    // Classifying it `Completed` would durably record a hallucinated tool name
    // as a successful call (fenced end-to-end by
    // `hook::tests::hook_maps_unknown_tool_dispatch_to_failed_lifecycle`).
    let unknown = super::dispatch_tool(&tools, "missing", "{}".to_string(), None).await;
    match &unknown {
        crate::tool_call_lifecycle::ToolOutcome::Failed {
            denial: None, text, ..
        } => {
            assert_eq!(text, "error: unknown tool 'missing'");
        }
        other => panic!("unknown tool must classify as a dispatch failure, got {other:?}"),
    }
    // The model still sees exactly the text it always saw.
    assert_eq!(unknown.model_facing_text(), "error: unknown tool 'missing'");
}

#[tokio::test]
async fn dispatch_tool_types_unparseable_args_as_argument_invalid() {
    use crate::llm::tool::{Tool, ToolDefinition};

    // A tool whose Args require fields the (valid-JSON) call omits, so the real
    // parse seam raises UnparseableArgs.
    struct StrictArgsTool;
    #[derive(Debug, thiserror::Error)]
    #[error("strict tool error")]
    struct StrictToolError;
    #[derive(serde::Deserialize)]
    struct StrictArgs {
        #[allow(dead_code)]
        body: String,
        #[allow(dead_code)]
        findings: Vec<String>,
    }
    impl Tool for StrictArgsTool {
        const NAME: &'static str = "strict";
        type Error = StrictToolError;
        type Args = StrictArgs;
        type Output = String;
        async fn definition(&self, _prompt: String) -> ToolDefinition {
            ToolDefinition {
                name: Self::NAME.to_string(),
                description: String::new(),
                parameters: serde_json::json!({"type": "object"}),
            }
        }
        async fn call(&self, _args: Self::Args) -> Result<Self::Output, Self::Error> {
            Ok("ran".to_string())
        }
    }

    let tools: Vec<Box<dyn ToolDyn>> = vec![Box::new(StrictArgsTool)];
    // Truncated mid-string: escape-only repair cannot complete it, so it stays
    // UnparseableArgs and dispatch types it `Failed(ArgumentInvalid)` carrying
    // the model-facing notice — not the tool output.
    let result =
        super::dispatch_tool(&tools, "strict", r#"{"body":"cut off"#.to_string(), None).await;
    match &result {
        crate::tool_call_lifecycle::ToolOutcome::Failed {
            class,
            denial: None,
            text,
        } => {
            assert_eq!(
                *class,
                crate::tool_call_lifecycle::FailureClass::ArgumentInvalid
            );
            assert!(
                !text.contains("ran") && text.contains("token limit"),
                "the notice must replace the tool output and guide the model to shorten, got: {text}"
            );
        }
        other => panic!("unparseable args must classify ArgumentInvalid, got {other:?}"),
    }
}

/// Loop-level fence: an unparseable-args tool call (a) does NOT run the tool,
/// (b) surfaces a clean notice to the model (the internal marker stripped) so it
/// can re-emit corrected arguments next turn, and (c) terminalizes the started
/// `AgentToolCall` as `failed`/`argumentInvalid` via `on_tool_result`. This
/// preserves the tool-call liveness invariant (Lean
/// `ToolExecution.live_call_reaches_terminal`, T5: the started call reaches a
/// terminal state) using the proven `Running → Failed` edge with the existing
/// `FailureClass::ArgumentInvalid`.
#[tokio::test]
async fn unparseable_tool_args_notify_model_and_terminalize_failed() {
    use crate::llm::tool::{Tool, ToolDefinition};

    struct StrictArgsTool;
    #[derive(Debug, thiserror::Error)]
    #[error("strict tool error")]
    struct StrictToolError;
    #[derive(serde::Deserialize)]
    struct StrictArgs {
        #[allow(dead_code)]
        report_type: String,
        #[allow(dead_code)]
        findings: Vec<String>,
    }
    impl Tool for StrictArgsTool {
        const NAME: &'static str = "post_status";
        type Error = StrictToolError;
        type Args = StrictArgs;
        type Output = String;
        async fn definition(&self, _prompt: String) -> ToolDefinition {
            ToolDefinition {
                name: Self::NAME.to_string(),
                description: String::new(),
                parameters: serde_json::json!({"type": "object"}),
            }
        }
        async fn call(&self, _args: Self::Args) -> Result<Self::Output, Self::Error> {
            // Must NOT run: the args never deserialize.
            panic!("the tool must not run on unparseable arguments");
        }
    }

    let (node, hook, _identity) = test_hook().await;
    ready_hook_for(&hook).await;

    // Valid JSON, but missing the required `findings` field: a Malformed parse
    // failure that no repair can recover into the typed args.
    let model = ScriptedModel::new_turns(vec![
        vec![
            RawStreamingChoice::ToolCall(RawStreamingToolCall::new(
                "call-1".to_string(),
                "post_status".to_string(),
                serde_json::json!({ "report_type": "steward" }),
            )),
            RawStreamingChoice::FinalResponse(()),
        ],
        vec![
            RawStreamingChoice::Message("ok".to_string()),
            RawStreamingChoice::FinalResponse(()),
        ],
    ]);
    let tools: Vec<Box<dyn ToolDyn>> = vec![Box::new(StrictArgsTool)];

    let stream = run_loop_stream(
        model,
        Some(hook),
        Message::user("post a status report"),
        Vec::new(),
        Arc::new(tools),
        config(4),
    );
    futures::pin_mut!(stream);

    // The model is notified via a tool result (no error ends the stream); it sees
    // the clean notice and answers on the next turn.
    let mut tool_results = Vec::new();
    while let Some(item) = stream.next().await {
        if let LoopStreamItem::Item(MultiTurnStreamItem::StreamUserItem(
            StreamedUserContent::ToolResult { tool_result, .. },
        )) = item.expect("loop must not fail; unparseable args are notified, not raised")
        {
            tool_results.push(
                tool_result_text(&crate::llm::rig_compat::from_rig_tool_result_content(
                    &tool_result.content.first(),
                ))
                .to_string(),
            );
        }
    }
    assert!(
        tool_results
            .iter()
            .any(|r| r.contains("could not be parsed")),
        "the model must be notified with a clean parse-failure notice, got: {tool_results:?}"
    );
    assert!(
        !tool_results
            .iter()
            .any(|r| r.contains("__gents_tool_lifecycle__")),
        "the internal marker must never leak to the model, got: {tool_results:?}"
    );

    // T5: the started call terminalized failed(argumentInvalid) — via on_tool_result
    // stripping the marker and forcing ArgumentInvalid — instead of dangling in `running`.
    let resp = node
        .execute("query { AgentToolCall { tool_name lifecycle_state tool_failure_class } }")
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
    assert!(
        rows.iter().any(|row| {
            row.get("tool_name").and_then(|v| v.as_str()) == Some("post_status")
                && row.get("lifecycle_state").and_then(|v| v.as_str()) == Some("failed")
                && row.get("tool_failure_class").and_then(|v| v.as_str()) == Some("argumentInvalid")
        }),
        "the started tool call must terminalize failed/argumentInvalid, got rows: {rows:?}"
    );
}

/// #589/#590 incident regression, salvageable half: the model emits the exact
/// production corrupt-arguments payload (leaked `</think`, stray CJK, nested
/// Hermes fragment, literal newlines, duplicated keys). The escape-only repair
/// salvages the intended object, so (a) the typed tool RUNS with the intended
/// `tool_name`, (b) the durable `AgentMessage` history carries object-shaped
/// arguments — never the raw corrupt string that jammed Amy's session — and
/// (c) the next provider request sees object-shaped arguments.
#[tokio::test]
async fn corrupt_589_tool_args_salvage_runs_and_history_stays_object_shaped() {
    use crate::llm::tool::{Tool, ToolDefinition};

    struct DescribeTool;
    #[derive(Debug, thiserror::Error)]
    #[error("describe tool error")]
    struct DescribeToolError;
    #[derive(serde::Deserialize)]
    struct DescribeArgs {
        tool_name: String,
    }
    impl Tool for DescribeTool {
        const NAME: &'static str = "describe_tool";
        type Error = DescribeToolError;
        type Args = DescribeArgs;
        type Output = String;
        async fn definition(&self, _prompt: String) -> ToolDefinition {
            ToolDefinition {
                name: Self::NAME.to_string(),
                description: String::new(),
                parameters: serde_json::json!({"type": "object"}),
            }
        }
        async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
            Ok(format!("described:{}", args.tool_name))
        }
    }

    let (node, hook, _identity) = test_hook().await;
    ready_hook_for(&hook).await;

    // The wire parser could not parse the corrupt bytes, so rig carries them as
    // a raw Value::String — exactly the shape persisted in the production store.
    let poison = serde_json::Value::String(crate::test_support::CORRUPT_TOOL_ARGS_589.to_string());
    let model = ScriptedModel::new_turns(vec![
        vec![
            RawStreamingChoice::ToolCall(RawStreamingToolCall::new(
                "call-1".to_string(),
                "describe_tool".to_string(),
                poison,
            )),
            RawStreamingChoice::FinalResponse(()),
        ],
        vec![
            RawStreamingChoice::Message("done".to_string()),
            RawStreamingChoice::FinalResponse(()),
        ],
    ]);
    let tools: Vec<Box<dyn ToolDyn>> = vec![Box::new(DescribeTool)];

    let stream = run_loop_stream(
        model.clone(),
        Some(hook),
        Message::user("describe list_hosts"),
        Vec::new(),
        Arc::new(tools),
        config(4),
    );
    futures::pin_mut!(stream);

    let mut tool_results = Vec::new();
    while let Some(item) = stream.next().await {
        if let LoopStreamItem::Item(MultiTurnStreamItem::StreamUserItem(
            StreamedUserContent::ToolResult { tool_result, .. },
        )) = item.expect("loop item should be Ok")
        {
            tool_results.push(
                tool_result_text(&crate::llm::rig_compat::from_rig_tool_result_content(
                    &tool_result.content.first(),
                ))
                .to_string(),
            );
        }
    }

    // (a) The intended call ran: salvage recovered `tool_name: list_hosts`.
    assert_eq!(
        tool_results,
        vec!["described:list_hosts".to_string()],
        "the salvageable #589 payload must run the intended call, not waste a turn"
    );

    // (b) The next provider request carries object-shaped arguments. (The
    // durable AgentMessage fence lives in the StreamProcessor harness —
    // `stream_processor::tests::corrupt_tool_call_arguments_persist_object_shaped`
    // — since the bare generator does not persist assistant turns.)
    let histories = model.seen_histories().await;
    assert_eq!(histories.len(), 2);
    assert_all_history_tool_args_object_shaped(&histories[1]);
    drop(node);
}

/// #590 incident regression, non-salvageable half: arguments that are valid
/// JSON but not an object (the `"[]"` reproduction). The call must fail
/// `argumentInvalid` with the model notified (never run), and neither the
/// durable history nor the next provider request may carry the non-object.
#[tokio::test]
async fn nonobject_tool_args_never_reach_durable_history_or_provider() {
    use crate::llm::tool::{Tool, ToolDefinition};

    struct StrictTool;
    #[derive(Debug, thiserror::Error)]
    #[error("strict tool error")]
    struct StrictToolError;
    #[derive(serde::Deserialize)]
    struct StrictArgs {
        #[allow(dead_code)]
        tool_name: String,
    }
    impl Tool for StrictTool {
        const NAME: &'static str = "describe_tool";
        type Error = StrictToolError;
        type Args = StrictArgs;
        type Output = String;
        async fn definition(&self, _prompt: String) -> ToolDefinition {
            ToolDefinition {
                name: Self::NAME.to_string(),
                description: String::new(),
                parameters: serde_json::json!({"type": "object"}),
            }
        }
        async fn call(&self, _args: Self::Args) -> Result<Self::Output, Self::Error> {
            panic!("the tool must not run on non-object arguments");
        }
    }

    let (node, hook, _identity) = test_hook().await;
    ready_hook_for(&hook).await;

    let model = ScriptedModel::new_turns(vec![
        vec![
            RawStreamingChoice::ToolCall(RawStreamingToolCall::new(
                "call-1".to_string(),
                "describe_tool".to_string(),
                serde_json::json!([]),
            )),
            RawStreamingChoice::FinalResponse(()),
        ],
        vec![
            RawStreamingChoice::Message("ok".to_string()),
            RawStreamingChoice::FinalResponse(()),
        ],
    ]);
    let tools: Vec<Box<dyn ToolDyn>> = vec![Box::new(StrictTool)];

    let stream = run_loop_stream(
        model.clone(),
        Some(hook),
        Message::user("describe"),
        Vec::new(),
        Arc::new(tools),
        config(4),
    );
    futures::pin_mut!(stream);
    while let Some(item) = stream.next().await {
        item.expect("loop must not fail; non-object args are notified, not raised");
    }

    // The started call terminalized failed/argumentInvalid — never a live
    // completed call carrying poison (#589's persist gate).
    let resp = node
        .execute("query { AgentToolCall { tool_name lifecycle_state tool_failure_class } }")
        .await;
    assert!(!resp.has_errors(), "query failed: {:?}", resp.errors);
    let rows = resp
        .data
        .as_ref()
        .and_then(|data| data.get("AgentToolCall"))
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        rows.iter().any(|row| {
            row.get("tool_name").and_then(|v| v.as_str()) == Some("describe_tool")
                && row.get("lifecycle_state").and_then(|v| v.as_str()) == Some("failed")
                && row.get("tool_failure_class").and_then(|v| v.as_str()) == Some("argumentInvalid")
        }),
        "a non-object-args call must terminalize failed/argumentInvalid, got: {rows:?}"
    );

    // The next provider request carries object-shaped args — the [] never
    // re-egresses.
    let histories = model.seen_histories().await;
    assert_eq!(histories.len(), 2);
    assert_all_history_tool_args_object_shaped(&histories[1]);
}

/// Every tool call inside a (native) history must carry object-shaped
/// arguments — the provider-render precondition (#590).
fn assert_all_history_tool_args_object_shaped(history: &[Message]) {
    for message in history {
        if let Message::Assistant { content, .. } = message {
            for item in content {
                if let AssistantContent::ToolCall(tool_call) = item {
                    assert!(
                        tool_call.function.arguments.is_object(),
                        "non-object tool-call arguments reached the provider: {:?}",
                        tool_call.function.arguments
                    );
                }
            }
        }
    }
}

/// #652: the repair pass must sanitize the LOADED HISTORY, not just the
/// run-threaded messages.
///
/// The motivating failure (the vLLM parse-signature 400) originates from
/// `json.loads` of tool-call arguments in the INPUT TRANSCRIPT — i.e. exactly
/// the history the repair pass used to skip. With the poison in history and
/// none in the new messages, repair used to re-issue a byte-identical poisoned
/// request and fail the same way: the fence described a transform that did not
/// exist.
#[tokio::test(start_paused = true)]
async fn repair_sanitizes_poisoned_tool_args_in_loaded_history() {
    let poison = format!("bad{}value", '\u{0007}');

    // The poison lives ONLY in the loaded conversation history — a prior turn's
    // persisted tool call. The current run produces nothing dirty.
    let poisoned_call = crate::llm::message::ToolCall {
        id: "call-historic".to_string(),
        call_id: Some("call-historic".to_string()),
        function: crate::llm::message::ToolFunction {
            name: "echo".to_string(),
            arguments: serde_json::json!({ "note": poison }),
        },
        signature: None,
        additional_params: None,
    };
    let history = vec![
        Message::user("earlier"),
        Message::Assistant {
            id: None,
            content: vec![AssistantContent::ToolCall(poisoned_call)],
        },
        Message::User {
            content: vec![UserContent::tool_result(
                "call-historic",
                vec![ToolResultContent::text("ok")],
            )],
        },
    ];
    assert!(
        history_has_control_char_tool_arg(&history),
        "the fixture must actually carry poisoned history"
    );

    // Two identical parse-400s: resample once, then repair.
    let model = ScriptedModel::new_calls(vec![
        ScriptedCall::FailStream(parse_400_error("same")),
        ScriptedCall::FailStream(parse_400_error("same")),
        ScriptedCall::Turn(vec![
            RawStreamingChoice::Message("repaired".to_string()),
            RawStreamingChoice::FinalResponse(()),
        ]),
    ]);

    let stream = run_loop_stream(
        model.clone(),
        None,
        Message::user("hi"),
        history,
        Arc::new(Vec::new()),
        config(0),
    );
    let collected = collect_scripted_stream(stream).await;

    assert_eq!(collected.final_text.as_deref(), Some("repaired"));
    assert_eq!(collected.error, None);

    let histories = model.seen_histories().await;
    assert_eq!(
        histories.len(),
        3,
        "parse failure, resample, and the repaired retry"
    );
    assert!(
        history_has_control_char_tool_arg(&histories[0]),
        "the poisoned history must reach the provider before repair: {:?}",
        histories[0]
    );
    assert!(
        !history_has_control_char_tool_arg(&histories[2]),
        "repair must sanitize tool arguments in the LOADED HISTORY, not only \
         the run-threaded messages — otherwise it re-issues the same poisoned \
         input and fails identically (#652): {:?}",
        histories[2]
    );
}

// ---------------------------------------------------------------------------
// Generated PromptAssembly contract consumers.
//
// These live in the crate rather than in `tests/conformance/prompt_assembly.rs`
// because they drive `pub(crate)` production entry points: `assemble_new_messages`
// and `repair_provider_input`. The sanitize family, whose entry point is public,
// is fenced from the integration test.
// ---------------------------------------------------------------------------

/// Text of a single-item user text message, for slot classification.
fn sole_user_text(message: &Message) -> String {
    match message {
        Message::User { content } => match content.as_slice() {
            [UserContent::Text(text)] => text.text.clone(),
            other => panic!("layer fence built an unexpected user message: {other:?}"),
        },
        other => panic!("layer fence built an unexpected message: {other:?}"),
    }
}

/// Name the `PromptAssembly.Slot` a message occupies.
fn classify_slot(message: &Message, is_last: bool, conversation_index: &mut usize) -> String {
    if super::is_request_context_message(message) {
        return "contextPreamble".to_string();
    }
    if is_last {
        return "prompt".to_string();
    }
    let text = sole_user_text(message);
    if let Some(body) = text.strip_prefix("<system-reminder>\n") {
        if body.starts_with("Continuation checkpoints from earlier conversation") {
            return "summaryReminder".to_string();
        }
        if let Some(rest) = body.strip_prefix("skill-") {
            let digits = rest
                .chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>();
            return format!("skillReminder:{digits}");
        }
    }
    let slot = format!("conversation:{conversation_index}");
    *conversation_index += 1;
    slot
}

/// Fences the fixed layer order of the assembled request against Lean
/// `PromptAssembly.Template.assembleWithContext`, whose `assembleWithContext_tail`
/// theorem pins the tail as `[contextPreamble, prompt]`.
///
/// The summary/conversation layers come from the production
/// `LayeredPromptBuilder::build`, and the tail from the production
/// `assemble_new_messages`. The skill-reminder prepend is *mirrored* from
/// `agent/daemon/request.rs` rather than driven, because it happens inline in
/// that function's async request flow; the reminders themselves are built by the
/// production `LayeredPromptBuilder::system_reminder`.
#[tokio::test]
async fn generated_layer_cases_pin_the_assembled_request_order() {
    use crate::lean_vocab_test::lean_prompt_assembly_layer_cases;
    use crate::prompt::{LayeredPromptBuilder, PromptBuilder};

    let cases = lean_prompt_assembly_layer_cases();
    assert!(
        !cases.is_empty(),
        "Lean emitted no PromptAssembly layer cases"
    );

    for case in cases {
        let builder = LayeredPromptBuilder::for_behavior(
            "system prompt",
            "fence",
            &["bash"],
            false,
            100_000,
            8_192,
            &[],
        );

        let conversation = (0..case.conversation_len)
            .map(|index| Message::user(format!("conversation-{index}")))
            .collect::<Vec<_>>();
        let summaries = (0..case.summary_count)
            .map(|index| format!("summary-{index}"))
            .collect::<Vec<_>>();
        let skill_reminders = (0..case.skill_count)
            .map(|index| LayeredPromptBuilder::system_reminder(&format!("skill-{index}")))
            .collect::<Vec<_>>();

        let built = builder
            .build(&conversation, &summaries)
            .await
            .expect("build layered prompt");

        let mut assembled = skill_reminders;
        assembled.extend(built.messages);
        assembled.extend(super::assemble_new_messages(
            Some(Message::user("<context>\nnow: t\n</context>")),
            Message::user("prompt"),
        ));

        // The preamble is a field on the completion request, not a message.
        assert!(
            !builder.preamble().is_empty(),
            "the preamble slot must be carried by the system-prompt field"
        );
        let mut slots = vec!["preamble".to_string()];
        let mut conversation_index = 0usize;
        let assembled_len = assembled.len();
        for (position, message) in assembled.iter().enumerate() {
            slots.push(classify_slot(
                message,
                position + 1 == assembled_len,
                &mut conversation_index,
            ));
        }

        assert_eq!(
            slots, case.slots,
            "assembled layer order drifted from the Lean model on case {:?}",
            case.name
        );
    }
}

/// Concrete tool-call arguments denoting each abstract `PromptAssembly.ToolArgs`
/// shape the contract emits.
fn repair_vector(name: &str) -> serde_json::Value {
    match name {
        // A `raw` payload is one whose string leaves still carry literal
        // newlines — exactly what the leaf sanitizer rewrites.
        "object:raw" => serde_json::json!({"k": "line\nbreak"}),
        "object:empty" => serde_json::json!({}),
        "object:sanitized" => serde_json::json!({"k": "no break"}),
        "str:object:raw" => serde_json::Value::String("{\"k\": \"line\\nbreak\"}".to_string()),
        "str:unparsed" => serde_json::Value::String("not json at all".to_string()),
        "array" => serde_json::json!([1, 2]),
        "scalar" => serde_json::json!(123),
        "null" => serde_json::Value::Null,
        other => panic!("generated repair case names an unmodeled shape: {other}"),
    }
}

/// Project repaired arguments back onto the abstract shape, mirroring the
/// `Payload` abstraction in the contract: `empty` is `{}`, `raw` still carries a
/// literal newline in some string leaf, `sanitized` does not.
fn repair_shape(value: &serde_json::Value) -> String {
    let serde_json::Value::Object(map) = value else {
        panic!("repair must always yield an object, got {value:?}");
    };
    if map.is_empty() {
        return "object:empty".to_string();
    }
    if has_raw_leaf(value) {
        "object:raw".to_string()
    } else {
        "object:sanitized".to_string()
    }
}

fn has_raw_leaf(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(text) => text.contains('\n'),
        serde_json::Value::Array(values) => values.iter().any(has_raw_leaf),
        serde_json::Value::Object(map) => map.values().any(has_raw_leaf),
        _ => false,
    }
}

fn repaired_arguments(arguments: serde_json::Value) -> serde_json::Value {
    // `repair_provider_input` re-sanitizes after rewriting arguments, so the
    // call must be paired or the whole turn is (correctly) dropped.
    let mut history = vec![
        Message::Assistant {
            id: None,
            content: vec![AssistantContent::ToolCall(crate::llm::message::ToolCall {
                id: "call-1".to_string(),
                call_id: Some("call-1".to_string()),
                function: crate::llm::message::ToolFunction {
                    name: "echo".to_string(),
                    arguments,
                },
                signature: None,
                additional_params: None,
            })],
        },
        Message::User {
            content: vec![UserContent::ToolResult(crate::llm::message::ToolResult {
                id: "call-1".to_string(),
                call_id: Some("call-1".to_string()),
                content: vec![ToolResultContent::Text(crate::llm::message::Text {
                    text: "call-1-result".to_string(),
                })],
            })],
        },
    ];
    let mut new_messages = Vec::new();
    super::repair_provider_input(&mut history, &mut new_messages);
    let [Message::Assistant { content, .. }, Message::User { .. }] = history.as_slice() else {
        panic!("repair must rewrite payloads only, never rows: {history:?}");
    };
    let [AssistantContent::ToolCall(tool_call)] = content.as_slice() else {
        panic!("repair dropped the tool call: {content:?}");
    };
    tool_call.function.arguments.clone()
}

/// Fences Lean `PromptAssembly.repairArgs` — `repair_is_payload_only` (repair
/// rewrites argument payloads only, never rows, roles, call ids, or ordering)
/// and `repair_idempotent` (a second pass is a no-op).
#[test]
fn generated_repair_cases_drive_tool_argument_repair() {
    use crate::lean_vocab_test::lean_prompt_assembly_repair_cases;

    let cases = lean_prompt_assembly_repair_cases();
    assert!(
        !cases.is_empty(),
        "Lean emitted no PromptAssembly repair cases"
    );

    for case in cases {
        let input = repair_vector(&case.input);
        let once = repaired_arguments(input.clone());
        assert_eq!(
            repair_shape(&once),
            case.expected,
            "repair disagrees with the Lean model on case {:?}",
            case.name
        );

        let twice = repaired_arguments(once.clone());
        assert_eq!(
            repair_shape(&twice),
            case.expected_twice,
            "repair is not idempotent on case {:?}",
            case.name
        );
        assert_eq!(
            twice, once,
            "repair_idempotent: a second pass must not change the payload ({:?})",
            case.name
        );

        // `repair_is_payload_only`: object inputs keep their shape, and repair
        // never rewrites anything outside the payload.
        if case.payload_only {
            assert!(
                input.is_object(),
                "the contract marks {:?} payload-only, so its input must be an object",
                case.name
            );
        }
    }
}
