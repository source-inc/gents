//! Claude subscription over Anthropic Messages HTTP — the only wire.
//!
//! Every turn (tool-capable or not) is `POST /v1/messages` with the agent's
//! `OAuthCredential` bearer, `system[0]` = [`CLAUDE_CODE_IDENTITY`], Gents
//! preamble and `Message::System` rows after it, and `tools` only when the
//! surface is non-empty. The SSE body is parsed incrementally by
//! [`MessagesSseState`]; `tool_use` blocks map onto the gents surface or fail
//! closed. Lean model: `Proofs/PromptAssembly/ClaudeMap.lean` (system
//! assembly, accumulation).

use std::collections::HashSet;
#[cfg(test)]
use std::collections::VecDeque;
use std::fmt;
#[cfg(test)]
use std::sync::{Mutex, OnceLock};

use bytes::Bytes;
use futures::StreamExt;
use rig::completion::{CompletionError, CompletionRequest, ToolDefinition};
use rig::http_client::{
    self, HeaderValue, HttpClientExt, LazyBody, MultipartForm, Request, ReqwestClient, Response,
    StreamingResponse,
};
use rig::streaming::{RawStreamingChoice, RawStreamingToolCall};
use rig::wasm_compat::WasmCompatSend;
use serde_json::{json, Value};
use thiserror::Error;

use crate::claude_subscription::ClaudeStreamResponse;
use crate::llm::message::{AssistantContent, Message, ToolResultContent, UserContent};
use crate::oauth_credential::BearerSource;
use crate::rendered_request::RenderedRequestCapturingHttpClient;

pub const MESSAGES_URI: &str = "https://api.anthropic.com/v1/messages";
pub(crate) const ANTHROPIC_VERSION: &str = "2023-06-01";
pub(crate) const OAUTH_BETA: &str = "oauth-2025-04-20";
const DEFAULT_MAX_TOKENS: u64 = 4096;

/// First `system` block. The subscription token was minted for Claude Code;
/// without this identity the same token 429s on every model (write request #7).
/// Lean: `ClaudeMap.identity`.
pub const CLAUDE_CODE_IDENTITY: &str = "You are Claude Code, Anthropic's official CLI for Claude.";

/// Fail-closed outcomes of the Messages tool-block parser. Display strings are
/// matched by the conformance drivers; keep them stable.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum MessagesParseError {
    #[error("fail-closed: tool_use observed ({names})")]
    ToolUse { names: String },
    #[error("fail-closed: duplicate tool_use id {id}")]
    DuplicateToolUseId { id: String },
    #[error("fail-closed: malformed tool_use: {message}")]
    MalformedToolUse { message: String },
    #[error("fail-closed: overlapping tool_use block {id}")]
    OverlappingToolUse { id: String },
}

impl From<MessagesParseError> for CompletionError {
    fn from(error: MessagesParseError) -> Self {
        CompletionError::ProviderError(error.to_string())
    }
}

#[cfg(test)]
fn messages_sse_fixture_queue() -> &'static Mutex<VecDeque<String>> {
    static QUEUE: OnceLock<Mutex<VecDeque<String>>> = OnceLock::new();
    QUEUE.get_or_init(|| Mutex::new(VecDeque::new()))
}

/// Test-only: SSE bodies served instead of the network, one per
/// `stream_messages` call, in order. Cleared by `lock_fixtures_for_test`.
/// Compiled out of non-test builds so no linking crate can bypass the network
/// or the write gate.
#[cfg(test)]
pub(crate) fn install_messages_sse_fixtures(bodies: Vec<String>) {
    *messages_sse_fixture_queue()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner()) = bodies.into_iter().collect();
}

#[cfg(test)]
fn take_messages_sse_fixture() -> Option<String> {
    messages_sse_fixture_queue()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .pop_front()
}

/// Test-only: serializes tests that use the fixture queue and clears it.
#[cfg(test)]
pub(crate) fn lock_fixtures_for_test() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    let guard = LOCK.lock().unwrap_or_else(|poison| poison.into_inner());
    install_messages_sse_fixtures(Vec::new());
    guard
}

/// Anthropic Messages JSON body from a rig `CompletionRequest`. The history
/// crosses the converter seam once (`rig_compat::from_rig_message`) and the
/// body is assembled over the native message family.
pub fn build_messages_body(model: &str, request: &CompletionRequest) -> Value {
    let history: Vec<Message> = request
        .chat_history
        .iter()
        .map(crate::llm::rig_compat::from_rig_message)
        .collect();
    build_messages_body_native(
        model,
        request.preamble.as_deref(),
        request.max_tokens,
        &history,
        &request.tools,
    )
}

/// Body assembly over the native message family (no rig vocabulary).
///
/// Lean: `systemBlocks`, `splitSystem`, `toolsField`. Two `cache_control`
/// breakpoints: the last `system` block (identity + preamble + System rows +
/// tools prefix) and the last content block of the last message (moving
/// breakpoint across tool_result turns).
pub fn build_messages_body_native(
    model: &str,
    preamble: Option<&str>,
    max_tokens: Option<u64>,
    history: &[Message],
    tools: &[ToolDefinition],
) -> Value {
    let mut system: Vec<Value> = vec![json!({ "type": "text", "text": CLAUDE_CODE_IDENTITY })];
    if let Some(preamble) = preamble.map(str::trim).filter(|value| !value.is_empty()) {
        system.push(json!({ "type": "text", "text": preamble }));
    }
    for row in system_rows(history) {
        system.push(json!({ "type": "text", "text": row }));
    }
    mark_ephemeral(system.last_mut());

    let mut messages = anthropic_messages(history);
    if let Some(last) = messages.last_mut() {
        if let Some(blocks) = last.get_mut("content").and_then(Value::as_array_mut) {
            mark_ephemeral(blocks.last_mut());
        }
    }

    let tools: Vec<Value> = tools
        .iter()
        .map(|tool| {
            json!({
                "name": tool.name,
                "description": tool.description,
                "input_schema": tool.parameters,
            })
        })
        .collect();

    let mut body = json!({
        "model": model,
        "max_tokens": max_tokens.unwrap_or(DEFAULT_MAX_TOKENS),
        "stream": true,
        "system": system,
        "messages": messages,
    });
    if !tools.is_empty() {
        body["tools"] = Value::Array(tools);
    }
    // No sampling keys: live claude-sonnet-5 400s on `temperature` / `top_p`
    // / `top_k`; `additional_params` carries those and is not merged.
    body
}

fn mark_ephemeral(block: Option<&mut Value>) {
    if let Some(Value::Object(map)) = block {
        map.insert("cache_control".to_string(), json!({ "type": "ephemeral" }));
    }
}

/// `Message::System` rows in transcript order (Lean `splitSystem`).
fn system_rows(history: &[Message]) -> Vec<String> {
    history
        .iter()
        .filter_map(|message| match message {
            Message::System { content } if !content.trim().is_empty() => Some(content.clone()),
            _ => None,
        })
        .collect()
}

fn anthropic_messages(history: &[Message]) -> Vec<Value> {
    let mut out = Vec::new();
    for message in history {
        match message {
            Message::User { content } => {
                let mut blocks = Vec::new();
                for block in content {
                    match block {
                        UserContent::Text(text) if !text.text.is_empty() => {
                            blocks.push(json!({"type": "text", "text": text.text}));
                        }
                        UserContent::ToolResult(result) => {
                            let body: String = result
                                .content
                                .iter()
                                .filter_map(|item| match item {
                                    ToolResultContent::Text(text) => Some(text.text.as_str()),
                                    _ => None,
                                })
                                .collect::<Vec<_>>()
                                .join("");
                            blocks.push(json!({
                                "type": "tool_result",
                                "tool_use_id": result.id,
                                "content": body,
                            }));
                        }
                        _ => {}
                    }
                }
                if !blocks.is_empty() {
                    out.push(json!({"role": "user", "content": blocks}));
                }
            }
            Message::Assistant { content, .. } => {
                let mut blocks = Vec::new();
                for block in content {
                    match block {
                        AssistantContent::Text(text) if !text.text.is_empty() => {
                            blocks.push(json!({"type": "text", "text": text.text}));
                        }
                        AssistantContent::ToolCall(call) => {
                            blocks.push(json!({
                                "type": "tool_use",
                                "id": call.id,
                                "name": call.function.name,
                                "input": call.function.arguments,
                            }));
                        }
                        _ => {}
                    }
                }
                if !blocks.is_empty() {
                    out.push(json!({"role": "assistant", "content": blocks}));
                }
            }
            // Rows were lifted into `system` by `system_rows`.
            Message::System { .. } => {}
        }
    }
    out
}

/// Incremental SSE parser for one Messages response.
///
/// Feed lines with [`push_line`]; each completed event (terminated by a blank
/// line) may yield zero or more `RawStreamingChoice`s. [`finish`] flushes an
/// unterminated `tool_use` block and guarantees exactly one `FinalResponse`.
/// Lean: `ClaudeMap.runStream` (`step` / `flush`).
///
/// [`push_line`]: MessagesSseState::push_line
/// [`finish`]: MessagesSseState::finish
pub struct MessagesSseState {
    surface: HashSet<String>,
    pending: Option<PendingTool>,
    seen_ids: HashSet<String>,
    usage: Option<rig::completion::Usage>,
    data: String,
    finished: bool,
    /// `request-id` response header, carried into stream-error messages only.
    request_id: Option<String>,
}

impl MessagesSseState {
    pub fn new(surface: HashSet<String>) -> Self {
        Self {
            surface,
            pending: None,
            seen_ids: HashSet::new(),
            usage: None,
            data: String::new(),
            finished: false,
            request_id: None,
        }
    }

    pub fn with_request_id(mut self, request_id: Option<String>) -> Self {
        self.request_id = request_id;
        self
    }

    /// One SSE line without its trailing newline. `data:` lines accumulate;
    /// a blank line dispatches the accumulated payload.
    pub fn push_line(
        &mut self,
        line: &str,
    ) -> Result<Vec<RawStreamingChoice<ClaudeStreamResponse>>, CompletionError> {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if let Some(rest) = line.strip_prefix("data:") {
            if !self.data.is_empty() {
                self.data.push('\n');
            }
            self.data.push_str(rest.trim());
            return Ok(Vec::new());
        }
        if !line.is_empty() {
            // `event:`, `id:`, comments — the payload's own `type` is authoritative.
            return Ok(Vec::new());
        }
        self.dispatch_pending_data()
    }

    /// End of body: flush an open block and emit `FinalResponse` if none seen.
    pub fn finish(
        mut self,
    ) -> Result<Vec<RawStreamingChoice<ClaudeStreamResponse>>, CompletionError> {
        let mut events = self.dispatch_pending_data()?;
        if let Some(tool) = self.pending.take() {
            events.push(mapped_tool_call(tool, &self.surface, &mut self.seen_ids)?);
        }
        if !self.finished {
            self.finished = true;
            events.push(RawStreamingChoice::FinalResponse(ClaudeStreamResponse {
                usage: self.usage,
            }));
        }
        Ok(events)
    }

    fn dispatch_pending_data(
        &mut self,
    ) -> Result<Vec<RawStreamingChoice<ClaudeStreamResponse>>, CompletionError> {
        if self.data.is_empty() {
            return Ok(Vec::new());
        }
        let raw = std::mem::take(&mut self.data);
        let payload: Value = match serde_json::from_str(&raw) {
            Ok(value) => value,
            Err(_) => {
                tracing::debug!(
                    len = raw.len(),
                    "claude messages: ignoring non-JSON SSE payload"
                );
                return Ok(Vec::new());
            }
        };
        self.handle_payload(&payload)
    }

    fn handle_payload(
        &mut self,
        payload: &Value,
    ) -> Result<Vec<RawStreamingChoice<ClaudeStreamResponse>>, CompletionError> {
        let mut events = Vec::new();
        let Some(kind) = payload.get("type").and_then(Value::as_str) else {
            return Ok(events);
        };
        match kind {
            "content_block_start" => {
                let Some(block) = payload.get("content_block") else {
                    return Ok(events);
                };
                if block.get("type").and_then(Value::as_str) != Some("tool_use") {
                    return Ok(events);
                }
                let id = block
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                if self.pending.is_some() {
                    return Err(MessagesParseError::OverlappingToolUse { id }.into());
                }
                let name = block
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let start_input = match block.get("input") {
                    Some(Value::Object(_)) => Some(block["input"].to_string()),
                    _ => None,
                };
                self.pending = Some(PendingTool {
                    id,
                    name,
                    start_input,
                    deltas: String::new(),
                });
            }
            "content_block_delta" => {
                let Some(delta) = payload.get("delta") else {
                    return Ok(events);
                };
                match delta.get("type").and_then(Value::as_str) {
                    Some("text_delta") => {
                        if let Some(text) = delta.get("text").and_then(Value::as_str) {
                            if !text.is_empty() {
                                events.push(RawStreamingChoice::Message(text.to_string()));
                            }
                        }
                    }
                    Some("input_json_delta") => {
                        if let (Some(tool), Some(partial)) = (
                            self.pending.as_mut(),
                            delta.get("partial_json").and_then(Value::as_str),
                        ) {
                            tool.deltas.push_str(partial);
                        }
                    }
                    _ => {}
                }
            }
            "content_block_stop" => {
                if let Some(tool) = self.pending.take() {
                    events.push(mapped_tool_call(tool, &self.surface, &mut self.seen_ids)?);
                }
            }
            "message_delta" => {
                if let Some(value) = payload.get("usage") {
                    self.usage = Some(usage_from_sse(value));
                }
            }
            "message_stop" => {
                // A malformed stream may end without `content_block_stop`;
                // the open block still precedes the final response.
                if let Some(tool) = self.pending.take() {
                    events.push(mapped_tool_call(tool, &self.surface, &mut self.seen_ids)?);
                }
                if !self.finished {
                    self.finished = true;
                    events.push(RawStreamingChoice::FinalResponse(ClaudeStreamResponse {
                        usage: self.usage,
                    }));
                }
            }
            "error" => {
                let error = payload.get("error").cloned().unwrap_or(Value::Null);
                let error_type = error.get("type").and_then(Value::as_str).unwrap_or("error");
                let message = error.get("message").and_then(Value::as_str).unwrap_or("");
                let request_id = self.request_id.as_deref().unwrap_or("-");
                return Err(CompletionError::ProviderError(format!(
                    "Claude Messages stream error {error_type}: {message} (request-id {request_id})"
                )));
            }
            _ => {}
        }
        Ok(events)
    }
}

/// All-lines wrapper over [`MessagesSseState`] for tests and the conformance
/// drivers.
pub fn parse_messages_sse(
    sse: &str,
    surface: &HashSet<String>,
) -> Result<Vec<RawStreamingChoice<ClaudeStreamResponse>>, CompletionError> {
    let mut state = MessagesSseState::new(surface.clone());
    let mut events = Vec::new();
    for line in sse.lines() {
        events.extend(state.push_line(line)?);
    }
    events.extend(state.finish()?);
    Ok(events)
}

struct PendingTool {
    id: String,
    name: String,
    /// `content_block.input` from `content_block_start`, serialized. Anthropic
    /// sends `{}` here and streams the real arguments as deltas.
    start_input: Option<String>,
    /// Concatenated `input_json_delta.partial_json` fragments, in order.
    deltas: String,
}

impl PendingTool {
    /// Lean `ClaudeMap.accumulate`: deltas win when any arrived; otherwise the
    /// start input; otherwise `{}`.
    fn arguments_json(&self) -> String {
        if !self.deltas.is_empty() {
            self.deltas.clone()
        } else {
            self.start_input.clone().unwrap_or_else(|| "{}".to_string())
        }
    }
}

fn mapped_tool_call(
    tool: PendingTool,
    surface: &HashSet<String>,
    seen_ids: &mut HashSet<String>,
) -> Result<RawStreamingChoice<ClaudeStreamResponse>, CompletionError> {
    if tool.id.trim().is_empty() || tool.name.trim().is_empty() {
        return Err(MessagesParseError::MalformedToolUse {
            message: "missing id or name".to_string(),
        }
        .into());
    }
    if !seen_ids.insert(tool.id.clone()) {
        return Err(MessagesParseError::DuplicateToolUseId { id: tool.id }.into());
    }
    if surface.is_empty() || !surface.contains(&tool.name) {
        return Err(MessagesParseError::ToolUse { names: tool.name }.into());
    }
    let raw = tool.arguments_json();
    let input: Value =
        serde_json::from_str(&raw).map_err(|error| MessagesParseError::MalformedToolUse {
            message: format!("tool_use {} input is not JSON: {error}", tool.id),
        })?;
    Ok(RawStreamingChoice::ToolCall(RawStreamingToolCall::new(
        tool.id, tool.name, input,
    )))
}

fn usage_from_sse(value: &Value) -> rig::completion::Usage {
    let input_tokens = value
        .get("input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let output_tokens = value
        .get("output_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    rig::completion::Usage {
        input_tokens,
        output_tokens,
        total_tokens: input_tokens.saturating_add(output_tokens),
        cached_input_tokens: value
            .get("cache_read_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cache_creation_input_tokens: value
            .get("cache_creation_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
    }
}

/// Incremental line-splitter over a response body.
pub(crate) fn stream_sse_body(
    body: http_client::sse::BoxedStream,
    state: MessagesSseState,
) -> impl futures::Stream<Item = Result<RawStreamingChoice<ClaudeStreamResponse>, CompletionError>>
{
    async_stream::stream! {
        let mut body = body;
        let mut state = state;
        let mut buffer: Vec<u8> = Vec::new();
        while let Some(chunk) = body.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    yield Err(CompletionError::ProviderError(format!("Claude Messages body: {error}")));
                    return;
                }
            };
            buffer.extend_from_slice(&chunk);
            while let Some(newline) = buffer.iter().position(|byte| *byte == b'\n') {
                let line: Vec<u8> = buffer.drain(..=newline).collect();
                let line = String::from_utf8_lossy(&line[..line.len() - 1]).into_owned();
                match state.push_line(&line) {
                    Ok(events) => {
                        for event in events {
                            yield Ok(event);
                        }
                    }
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                }
            }
        }
        if !buffer.is_empty() {
            let line = String::from_utf8_lossy(&buffer).into_owned();
            match state.push_line(&line) {
                Ok(events) => for event in events { yield Ok(event); },
                Err(error) => { yield Err(error); return; }
            }
        }
        match state.finish() {
            Ok(events) => for event in events { yield Ok(event); },
            Err(error) => yield Err(error),
        }
    }
}

pub async fn stream_messages<S: BearerSource>(
    model: &str,
    request: &CompletionRequest,
    surface: HashSet<String>,
    bearer: &S,
    http: &ReqwestClient,
) -> Result<
    impl futures::Stream<Item = Result<RawStreamingChoice<ClaudeStreamResponse>, CompletionError>>,
    CompletionError,
> {
    stream_messages_at(MESSAGES_URI, model, request, surface, bearer, http).await
}

/// `stream_messages` against an explicit URI (tests point it at a local
/// server). On a transport 401 the bearer is invalidated once before the
/// error is returned; that request fails and the next request refreshes.
pub(crate) async fn stream_messages_at<S: BearerSource>(
    uri: &str,
    model: &str,
    request: &CompletionRequest,
    surface: HashSet<String>,
    bearer: &S,
    http: &ReqwestClient,
) -> Result<
    impl futures::Stream<Item = Result<RawStreamingChoice<ClaudeStreamResponse>, CompletionError>>,
    CompletionError,
> {
    #[cfg(test)]
    let fixture = take_messages_sse_fixture();
    #[cfg(not(test))]
    let fixture: Option<String> = None;
    let body = build_messages_body(model, request);
    let body_bytes = serde_json::to_vec(&body).map_err(|error| {
        CompletionError::ProviderError(format!("encode Claude Messages body: {error}"))
    })?;

    let mut builder = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .header("anthropic-version", ANTHROPIC_VERSION)
        .header("anthropic-beta", OAUTH_BETA);
    if fixture.is_none() {
        let token = bearer.current_bearer().await.map_err(|error| {
            CompletionError::ProviderError(format!("Claude Messages bearer: {error}"))
        })?;
        let mut authorization =
            HeaderValue::from_str(&format!("Bearer {token}")).map_err(|error| {
                CompletionError::ProviderError(format!("Claude Messages auth header: {error}"))
            })?;
        authorization.set_sensitive(true);
        builder = builder.header("authorization", authorization);
        tracing::info!(
            model = %model,
            "live Claude Messages HTTP send (this process bills the Claude subscription)"
        );
    }
    let http_request = builder.body(Bytes::from(body_bytes)).map_err(|error| {
        CompletionError::ProviderError(format!("Claude Messages request: {error}"))
    })?;

    let client = RenderedRequestCapturingHttpClient::new(MessagesTransport {
        fixture,
        live: http.clone(),
    });
    let response = match client.send_streaming(http_request).await {
        Ok(response) => response,
        // rig's reqwest transport pre-checks the status and hands back the
        // body text; bound it the same way as the streamed path.
        Err(http_client::Error::InvalidStatusCodeWithMessage(status, message)) => {
            if status.as_u16() == 401 {
                bearer.invalidate().await;
            }
            return Err(non_success_error(
                status,
                None,
                &body_prefix(message.as_bytes()),
            ));
        }
        Err(error) => return Err(error.into()),
    };
    let status = response.status();
    let request_id = response
        .headers()
        .get("request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    // Reachable only for non-reqwest transports (e.g. the fixture): rig's
    // reqwest client pre-checks the status and errors above.
    if !status.is_success() {
        let prefix = read_body_prefix(response.into_body()).await;
        return Err(non_success_error(status, request_id.as_deref(), &prefix));
    }
    Ok(stream_sse_body(
        response.into_body(),
        MessagesSseState::new(surface).with_request_id(request_id),
    ))
}

/// Longest response-body prefix kept in a non-2xx error.
const NON_SUCCESS_BODY_PREFIX_BYTES: usize = 512;

/// Non-2xx Messages response as a `ProviderError`. Carries the status, the
/// `request-id` header and a bounded body prefix (Anthropic's
/// `error.type`/`error.message`); never request headers.
fn non_success_error(
    status: reqwest::StatusCode,
    request_id: Option<&str>,
    body_prefix: &str,
) -> CompletionError {
    let mut message = format!(
        "Claude Messages HTTP {status} (request-id {})",
        request_id.unwrap_or("-")
    );
    if !body_prefix.is_empty() {
        message.push_str(" body=");
        message.push_str(body_prefix);
    }
    CompletionError::ProviderError(message)
}

/// Up to [`NON_SUCCESS_BODY_PREFIX_BYTES`] of `bytes`, lossily decoded and
/// trimmed.
fn body_prefix(bytes: &[u8]) -> String {
    let end = bytes.len().min(NON_SUCCESS_BODY_PREFIX_BYTES);
    String::from_utf8_lossy(&bytes[..end]).trim().to_string()
}

/// Drain at most the prefix budget from a non-2xx body; read errors end the
/// prefix early rather than failing the (already failed) request.
async fn read_body_prefix(mut body: http_client::sse::BoxedStream) -> String {
    let mut bytes: Vec<u8> = Vec::new();
    while bytes.len() < NON_SUCCESS_BODY_PREFIX_BYTES {
        match body.next().await {
            Some(Ok(chunk)) => bytes.extend_from_slice(&chunk),
            Some(Err(_)) | None => break,
        }
    }
    body_prefix(&bytes)
}

/// Serves a queued SSE fixture or forwards to the shared client. Sits
/// behind `RenderedRequestCapturingHttpClient` so persist-before-send runs
/// for fixtures too.
#[derive(Clone)]
struct MessagesTransport {
    fixture: Option<String>,
    live: ReqwestClient,
}

impl HttpClientExt for MessagesTransport {
    fn send<T, U>(
        &self,
        req: Request<T>,
    ) -> impl std::future::Future<Output = http_client::Result<Response<LazyBody<U>>>>
           + WasmCompatSend
           + 'static
    where
        T: Into<Bytes> + WasmCompatSend,
        U: From<Bytes>,
        U: WasmCompatSend + 'static,
    {
        let inner = self.live.clone();
        let fixture = self.fixture.clone();
        let (parts, body) = req.into_parts();
        let body: Bytes = body.into();
        async move {
            if fixture.is_some() {
                let body: LazyBody<U> = Box::pin(async { Ok(U::from(Bytes::from_static(b"{}"))) });
                return Ok(Response::builder().status(200).body(body)?);
            }
            let req = Request::from_parts(parts, body);
            HttpClientExt::send::<Bytes, U>(&inner, req).await
        }
    }

    fn send_multipart<U>(
        &self,
        req: Request<MultipartForm>,
    ) -> impl std::future::Future<Output = http_client::Result<Response<LazyBody<U>>>>
           + WasmCompatSend
           + 'static
    where
        U: From<Bytes>,
        U: WasmCompatSend + 'static,
    {
        let inner = self.live.clone();
        async move { HttpClientExt::send_multipart::<U>(&inner, req).await }
    }

    fn send_streaming<T>(
        &self,
        req: Request<T>,
    ) -> impl std::future::Future<Output = http_client::Result<StreamingResponse>> + WasmCompatSend
    where
        T: Into<Bytes>,
    {
        let inner = self.live.clone();
        let fixture = self.fixture.clone();
        let (parts, body) = req.into_parts();
        let body: Bytes = body.into();
        async move {
            if let Some(sse) = fixture {
                let stream: http_client::sse::BoxedStream =
                    Box::pin(futures::stream::iter([Ok(Bytes::from(sse))]));
                return Ok(Response::builder().status(200).body(stream)?);
            }
            let req = Request::from_parts(parts, body);
            HttpClientExt::send_streaming(&inner, req).await
        }
    }
}

impl fmt::Debug for MessagesTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MessagesTransport")
            .field("fixture", &self.fixture.is_some())
            .finish_non_exhaustive()
    }
}

/// Test-only SSE body: one text block (`content_block_start` + one
/// `text_delta` + `content_block_stop`). No `message_stop`, so callers can
/// append their own terminator.
#[cfg(test)]
pub(crate) fn sse_fixture_text(text: &str) -> String {
    let text = serde_json::to_string(text).expect("escape");
    format!(
        "event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{{\"type\":\"text\",\"text\":\"\"}}}}\n\n\
         event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"type\":\"text_delta\",\"text\":{text}}}}}\n\n\
         event: content_block_stop\ndata: {{\"type\":\"content_block_stop\",\"index\":0}}\n\n"
    )
}

/// Test-only SSE body: one `tool_use` block with `input: {}` at start, one
/// `input_json_delta` carrying `partial_json`, then `message_delta` (usage
/// 10/5) and `message_stop`.
#[cfg(test)]
pub(crate) fn sse_fixture_tool_use(id: &str, name: &str, partial_json: &str) -> String {
    let partial = serde_json::to_string(partial_json).expect("escape");
    format!(
        "event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{{\"type\":\"tool_use\",\"id\":\"{id}\",\"name\":\"{name}\",\"input\":{{}}}}}}\n\n\
         event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"type\":\"input_json_delta\",\"partial_json\":{partial}}}}}\n\n\
         event: content_block_stop\ndata: {{\"type\":\"content_block_stop\",\"index\":0}}\n\n\
         event: message_delta\ndata: {{\"type\":\"message_delta\",\"delta\":{{\"stop_reason\":\"tool_use\"}},\"usage\":{{\"input_tokens\":10,\"output_tokens\":5}}}}\n\n\
         event: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n"
    )
}

/// Test-only SSE body: [`sse_fixture_text`] terminated by `message_delta`
/// (usage 12/3) and `message_stop`.
#[cfg(test)]
pub(crate) fn sse_fixture_final_text(text: &str) -> String {
    format!(
        "{}event: message_delta\ndata: {{\"type\":\"message_delta\",\"delta\":{{\"stop_reason\":\"end_turn\"}},\"usage\":{{\"input_tokens\":12,\"output_tokens\":3}}}}\n\nevent: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n",
        sse_fixture_text(text)
    )
}

#[cfg(test)]
#[path = "claude_messages/tests.rs"]
mod tests;
