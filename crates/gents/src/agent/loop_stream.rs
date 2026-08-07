//! Owned multi-turn completion→tool loop (issue #400, decision D6).
//!
//! This replaces rig's `Agent::stream_prompt` *producer* with our own stream
//! generator, while keeping rig as the provider/streaming *client*
//! (`CompletionModel::stream`, the `Message` family, and the streaming decode
//! types). The generator mirrors rig's `agent::prompt_request::streaming::send`:
//! build a request from the running message history, stream one completion,
//! accumulate assistant content, and — when the turn produced tool calls —
//! execute them, thread their results back into the history, and loop. When a
//! turn produces no tool calls, it yields a terminal `FinalResponse`.
//!
//! The generator yields a native `LoopStreamItem` envelope around rig's
//! `MultiTurnStreamItem`, keeping provider payloads at the rig boundary while
//! giving the runtime a place to carry retry-control events.
//!
//! Tool side-effects (lifecycle tracking, truncation/spill, persistence) are
//! NOT reimplemented here: the generator calls the existing
//! `DefraSessionHook::on_tool_call` / `on_tool_result` methods directly (the
//! former `PromptHook` callbacks). The generator owns only the orchestration:
//! request construction, turn iteration, deadline/cancellation-aware dispatch,
//! native result bounding, and message threading. Because the bounded result is
//! threaded into the conversation by construction, the in-loop truncation gap
//! (#401) is closed natively without the recorder shim.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;

use crate::agent::completion_retry::{
    CompletionRetryPolicy, CompletionRetryState, MidStreamDirective, PreStreamDirective,
};
use crate::error::InferenceError;
use crate::llm::message::{
    AssistantContent, Message, ToolCall, ToolResult, ToolResultContent, UserContent,
};
use crate::llm::rig_compat;
use crate::llm::{HookAction, ToolCallHookAction};
use crate::rendered_request::{AssemblyBuildPath, AssemblyTrace};
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use rig::agent::{MultiTurnStreamItem, StreamingError};
use rig::completion::{
    CompletionError, CompletionModel, CompletionRequest, GetTokenUsage, PromptError, Usage,
};

use crate::llm::tool::ToolDyn;
use crate::llm::ToolChoice;
use rig::streaming::{StreamedAssistantContent, StreamedUserContent};

use super::stream_processor::AssistantTurnAccumulator;
use crate::hook::DefraSessionHook;
use crate::tool_call_lifecycle::runtime::{
    current_tool_runtime_context, deadline_remaining,
    scope_request_tool_execution_with_workspace_and_live_output, ToolOutcome,
};
use crate::truncation::{tool_result_truncation_mode, truncate_text, TruncationLimits};

#[cfg(test)]
mod tests;

/// `(turn_index, attempt, request, assembly_trace)`.
///
/// The trace rides alongside the request because the assembled
/// `CompletionRequest` is the *output* of prompt assembly and cannot explain
/// its own inputs: the provider-assigned assistant message ids, the exact
/// threaded tool-result content, the post-compaction message list, and which
/// builder produced it are all in-memory facts that die with the loop. See
/// `crate::rendered_request::AssemblyTrace`.
pub(crate) type RenderedRequestSink = Arc<
    dyn Fn(
            usize,
            u32,
            CompletionRequest,
            AssemblyTrace,
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
        + Send
        + Sync,
>;

pub(crate) type TurnCompactor = Arc<
    dyn Fn(
            Vec<Message>,
            usize,
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<Message>>> + Send>>
        + Send
        + Sync,
>;

/// A typed-output contract carried through the owned completion loop.
///
/// Rig owns the provider schema transport. Gents keeps ownership of the loop
/// so schema validation participates in its deadline-aware, formally modelled
/// retract-and-resample lifecycle instead of bypassing persistence and hooks
/// through `rig::Agent::prompt_typed`.
#[derive(Clone)]
pub(crate) struct StructuredOutputConfig {
    schema: schemars::Schema,
    validate: Arc<dyn Fn(&str) -> Result<(), String> + Send + Sync>,
}

impl StructuredOutputConfig {
    fn for_type<T>() -> Self
    where
        T: DeserializeOwned + schemars::JsonSchema + 'static,
    {
        Self {
            schema: schemars::schema_for!(T),
            validate: Arc::new(|raw| {
                serde_json::from_str::<T>(raw)
                    .map(|_| ())
                    .map_err(|error| {
                        format!(
                            "{error}; raw_output_preview={}; finish_metadata=unavailable_at_rig_streaming_boundary",
                            bounded_structured_output_preview(raw)
                        )
                    })
            }),
        }
    }
}

fn bounded_structured_output_preview(raw: &str) -> String {
    const MAX_PREVIEW_BYTES: usize = 192;
    let mut cut = raw.len().min(MAX_PREVIEW_BYTES);
    while !raw.is_char_boundary(cut) {
        cut -= 1;
    }
    let suffix = if cut < raw.len() { "…" } else { "" };
    serde_json::to_string(&format!("{}{suffix}", &raw[..cut]))
        .unwrap_or_else(|_| "\"<unavailable>\"".to_string())
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum LoopStreamItem<R> {
    Item(MultiTurnStreamItem<R>),
    TurnRetracted {
        turn: usize,
        attempt: u32,
        /// Backoff the generator sleeps before the resample. Carried so the
        /// daemon can extend the next poll's liveness budget by it, exactly as
        /// for `AttemptFailed` — otherwise a retract backoff longer than the
        /// liveness timeout is misread as a dead stream (#648).
        backoff: std::time::Duration,
    },
    AttemptFailed {
        turn: usize,
        attempt: u32,
        error: InferenceError,
        will_retry: bool,
        backoff: std::time::Duration,
    },
}

#[derive(Clone)]
pub(crate) struct LoopConfig {
    pub(crate) preamble: Option<String>,
    pub(crate) context_message: Option<Message>,
    pub(crate) temperature: Option<f64>,
    pub(crate) max_tokens: Option<u64>,
    /// One request-scoped ledger shared by the owned inference loop and every
    /// nested provider call it admits (notably compaction). `None` preserves
    /// the unbounded interactive behavior.
    pub(crate) aggregate_token_budget: Option<AggregateTokenBudget>,
    pub(crate) additional_params: Option<serde_json::Value>,
    pub(crate) structured_output: Option<StructuredOutputConfig>,
    pub(crate) tool_choice: Option<ToolChoice>,
    pub(crate) on_rendered_request: Option<RenderedRequestSink>,
    /// Ephemeral provider-view compaction used between completion turns. The
    /// durable transcript remains permissive; this callback only narrows the
    /// in-memory input immediately before provider dispatch.
    pub(crate) turn_compactor: Option<TurnCompactor>,
    pub(crate) context_window: usize,
    pub(crate) compaction_threshold: f64,
    pub(crate) retry_policy: CompletionRetryPolicy,
    pub(crate) deadline: Option<DateTime<Utc>>,
    pub(crate) max_turns: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggregateTokenCharge {
    Missing,
    Within,
    Exhausted,
    Overrun,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggregatePostChargeAction {
    Continue,
    Succeed,
    Fail,
}

fn aggregate_post_charge_action(
    charge: AggregateTokenCharge,
    terminal_valid: bool,
) -> AggregatePostChargeAction {
    match charge {
        AggregateTokenCharge::Missing | AggregateTokenCharge::Overrun => {
            AggregatePostChargeAction::Fail
        }
        AggregateTokenCharge::Within if terminal_valid => AggregatePostChargeAction::Succeed,
        AggregateTokenCharge::Within => AggregatePostChargeAction::Continue,
        AggregateTokenCharge::Exhausted if terminal_valid => AggregatePostChargeAction::Succeed,
        AggregateTokenCharge::Exhausted => AggregatePostChargeAction::Fail,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AggregateTokenLedger {
    limit: u64,
    used: u64,
}

pub(crate) const AGGREGATE_TOKEN_BUDGET_EXHAUSTED_PREFIX: &str =
    "aggregate_token_budget_exhausted: ";

/// Recover only the typed request-budget failure from an anyhow context
/// chain. Matching the underlying `StreamingError` rather than arbitrary text
/// prevents provider/model content from forging Harbor's scoreable outcome.
pub(crate) fn aggregate_token_budget_exhaustion_message(error: &anyhow::Error) -> Option<String> {
    error.chain().find_map(|cause| {
        let streaming_error = cause.downcast_ref::<StreamingError>()?;
        let StreamingError::Completion(CompletionError::ProviderError(reason)) = streaming_error
        else {
            return None;
        };
        reason
            .starts_with(AGGREGATE_TOKEN_BUDGET_EXHAUSTED_PREFIX)
            .then(|| reason.clone())
    })
}

/// Cloneable handle to the single monotone ledger for one durable request.
///
/// Nested compaction runs execute their own owned loop, so carrying only the
/// numeric limit would mint a fresh allowance. Sharing this handle makes all
/// provider calls compete for and charge the same request-wide budget.
#[derive(Debug, Clone)]
pub(crate) struct AggregateTokenBudget {
    ledger: Arc<Mutex<AggregateTokenLedger>>,
}

impl AggregateTokenBudget {
    pub(crate) fn new(limit: u64) -> Self {
        Self {
            ledger: Arc::new(Mutex::new(AggregateTokenLedger::new(limit))),
        }
    }

    fn snapshot(&self) -> Result<AggregateTokenLedger, StreamingError> {
        self.ledger.lock().map(|ledger| *ledger).map_err(|_| {
            StreamingError::Completion(CompletionError::ProviderError(
                "aggregate_token_ledger_unavailable: request budget lock was poisoned".to_string(),
            ))
        })
    }

    fn charge_reported(
        &self,
        usage: Option<Usage>,
    ) -> Result<(AggregateTokenCharge, AggregateTokenLedger), StreamingError> {
        let mut ledger = self.ledger.lock().map_err(|_| {
            StreamingError::Completion(CompletionError::ProviderError(
                "aggregate_token_ledger_unavailable: request budget lock was poisoned".to_string(),
            ))
        })?;
        let charge = ledger.charge_reported(usage);
        Ok((charge, *ledger))
    }
}

impl AggregateTokenLedger {
    fn new(limit: u64) -> Self {
        Self { limit, used: 0 }
    }

    fn remaining(self) -> u64 {
        self.limit.saturating_sub(self.used)
    }

    fn effective_output_tokens(self, input_tokens: u64, configured_max: u64) -> u64 {
        configured_max.min(self.remaining().saturating_sub(input_tokens))
    }

    fn can_dispatch(self, input_tokens: u64, configured_max: u64) -> bool {
        self.effective_output_tokens(input_tokens, configured_max) > 0
    }

    fn charge_reported(&mut self, usage: Option<Usage>) -> AggregateTokenCharge {
        let Some(usage) = usage else {
            return AggregateTokenCharge::Missing;
        };
        let charged = charged_usage_total(usage);
        if charged == 0 {
            return AggregateTokenCharge::Missing;
        }
        self.used = self.used.saturating_add(charged);
        match self.used.cmp(&self.limit) {
            std::cmp::Ordering::Less => AggregateTokenCharge::Within,
            std::cmp::Ordering::Equal => AggregateTokenCharge::Exhausted,
            std::cmp::Ordering::Greater => AggregateTokenCharge::Overrun,
        }
    }
}

fn charged_usage_total(usage: Usage) -> u64 {
    usage
        .total_tokens
        .max(usage.input_tokens.saturating_add(usage.output_tokens))
}

fn add_usage_saturating(aggregate: &mut Usage, usage: Usage) {
    aggregate.input_tokens = aggregate.input_tokens.saturating_add(usage.input_tokens);
    aggregate.output_tokens = aggregate.output_tokens.saturating_add(usage.output_tokens);
    aggregate.total_tokens = aggregate.total_tokens.saturating_add(usage.total_tokens);
    aggregate.cached_input_tokens = aggregate
        .cached_input_tokens
        .saturating_add(usage.cached_input_tokens);
    aggregate.cache_creation_input_tokens = aggregate
        .cache_creation_input_tokens
        .saturating_add(usage.cache_creation_input_tokens);
}

/// Assemble the per-request message tail: the optional `<context>` message rides
/// immediately before the prompt, which is always last (rig prompt semantics).
///
/// This mirrors Lean `PromptAssembly.Template.assembleWithContext`, whose
/// `assembleWithContext_tail` theorem fixes the order as `... contextPreamble,
/// prompt`. Fenced by `tests` (`assembles_context_immediately_before_prompt`);
/// reordering here breaks that test and contradicts the proof.
pub(crate) fn assemble_new_messages(
    context_message: Option<Message>,
    prompt: Message,
) -> Vec<Message> {
    let mut new_messages: Vec<Message> = Vec::with_capacity(2);
    if let Some(context_message) = context_message {
        new_messages.push(context_message);
    }
    new_messages.push(prompt);
    new_messages
}

pub(crate) fn is_request_context_message(message: &Message) -> bool {
    let Message::User { content } = message else {
        return false;
    };
    let [UserContent::Text(text)] = content.as_slice() else {
        return false;
    };
    let trimmed = text.text.trim();
    trimmed.starts_with("<context>") && trimmed.ends_with("</context>")
}

pub(crate) fn run_loop_stream<M>(
    model: M,
    hook: Option<DefraSessionHook>,
    prompt: Message,
    history: Vec<Message>,
    tools: Arc<Vec<Box<dyn ToolDyn>>>,
    config: LoopConfig,
) -> impl Stream<Item = Result<LoopStreamItem<M::StreamingResponse>, StreamingError>>
where
    M: CompletionModel + 'static,
    M::StreamingResponse: 'static,
{
    try_stream! {
        let history = crate::compaction::sanitize_history_for_provider(history);
        // #497: prior requests' per-request `<context>` messages are durably
        // persisted (training capture), but must NOT be replayed to the provider:
        // they carry stale `ctx.now` / collection summaries and would accumulate
        // unboundedly across a multi-request session, inflating tokens and
        // presenting stale context as current. Strip them from the provider-bound
        // history; the CURRENT request's context rides in `new_messages` below.
        // Persistence is untouched (it already happened upstream).
        // `mut` because the completion-retry Repair directive rewrites the
        // assembled input in place — loaded history included (#652).
        let mut history: Vec<Message> = history
            .into_iter()
            .filter(|message| !is_request_context_message(message))
            .collect();
        // The running set of messages produced this request. The last element
        // is always the "prompt" for the next turn (rig semantics): initially
        // the user message, later the trailing tool-result user message. The
        // optional per-request context message rides immediately before the
        // prompt (mirrors Lean `PromptAssembly.Template.assembleWithContext`).
        let mut new_messages: Vec<Message> =
            assemble_new_messages(config.context_message.clone(), prompt);
        let mut aggregated_usage = Usage::new();
        let aggregate_token_budget = config.aggregate_token_budget.clone();
        let mut current_turn: usize = 0;
        let mut retry = CompletionRetryState::new(config.retry_policy.clone());
        // Three in-memory-only transforms can enter these vectors: the rendered
        // request context, a model-generated per-turn compaction summary, and a
        // repair rewrite. Once any of them lands, this and every later turn must
        // retain the full native list.
        //
        // The request context *is* persisted (`prompt_hook.rs:28-30`), so the
        // reason to retain it is not absence — it is that persistence runs under
        // a configurable `FailurePolicy::FailOpen`, so that row may legitimately
        // be missing while the request still ships. Capture must not depend on a
        // subsystem whose failure mode is tolerant.
        let mut effective_messages_are_ephemeral = config.context_message.is_some();

        'turns: loop {
            if current_turn > config.max_turns + 1 {
                let prompt = new_messages
                    .last()
                    .cloned()
                    .expect("new_messages always retains at least the initial prompt");
                let chat_history = rig_compat::to_rig_messages(&error_chat_history(
                    &history,
                    &new_messages[..new_messages.len() - 1],
                ));
                Err(StreamingError::Prompt(Box::new(PromptError::MaxTurnsError {
                    max_turns: config.max_turns,
                    chat_history: Box::new(chat_history),
                    prompt: Box::new(rig_compat::to_rig_message(&prompt)),
                })))?;
            }
            current_turn += 1;

            let turn_index = current_turn - 1;
            let (mut request, compacted_this_turn) = build_budgeted_request(
                &model,
                &mut history,
                &mut new_messages,
                tools.as_slice(),
                &config,
                turn_index,
            )
            .await?;
            effective_messages_are_ephemeral |= compacted_this_turn;

            let current_prompt = new_messages
                .last()
                .cloned()
                .expect("new_messages always retains at least the initial prompt");
            let prior = &new_messages[..new_messages.len() - 1];

            if let Some(hook) = hook.as_ref() {
                let history_snapshot: Vec<Message> =
                    history.iter().chain(prior.iter()).cloned().collect();
                if let HookAction::Terminate { reason } =
                    hook.on_completion_call_with_context(
                        &current_prompt,
                        &history_snapshot,
                        (current_turn == 1)
                            .then_some(config.context_message.as_ref())
                            .flatten(),
                    ).await
                {
                    Err(StreamingError::Prompt(Box::new(PromptError::PromptCancelled {
                        chat_history: rig_compat::to_rig_messages(&error_chat_history(
                            &history,
                            &new_messages,
                        )),
                        reason,
                    })))?;
                }
            }

            let mut attempt = 0_u32;
            // Which builder produced the `request` currently in hand. Flipped
            // by the Repair directive below, which calls `build_request`
            // directly and therefore never applies the output clamp. This is
            // not recoverable from the transcript, so it rides in the trace.
            let mut build_path = AssemblyBuildPath::Budgeted;
            'attempts: loop {
                let mut stream = loop {
                    // Repair and retry paths can rebuild or reuse the request.
                    // Re-apply both clamps at the one provider-dispatch
                    // chokepoint so no attempt escapes either budget.
                    clamp_request_output_budget(&mut request, &config);
                    clamp_request_aggregate_token_budget(
                        &mut request,
                        aggregate_token_budget.as_ref(),
                    )?;
                    if let Some(on_rendered_request) = config.on_rendered_request.as_ref() {
                        // `history ++ new_messages` is the effective provider
                        // message list: post sanitization, post request-context
                        // filtering, and post any per-turn compaction (which
                        // rewrote both vectors in place).
                        let effective_messages =
                            history.iter().chain(new_messages.iter()).cloned().collect();
                        let assembly_trace = if effective_messages_are_ephemeral {
                            AssemblyTrace::from_effective_messages(build_path, effective_messages)
                        } else {
                            AssemblyTrace::from_reconstructible_messages(
                                build_path,
                                effective_messages,
                            )
                        };
                        on_rendered_request(turn_index, attempt, request.clone(), assembly_trace)
                            .await
                            .map_err(|error| {
                                StreamingError::Completion(CompletionError::ProviderError(format!(
                                    "capturing rendered completion request failed: {error:#}"
                                )))
                            })?;
                    }

                    match model.stream(request.clone()).await {
                        Ok(stream) => break stream,
                        Err(completion_error) => {
                            let streaming_error = StreamingError::Completion(completion_error);
                            let classified = crate::error::classify_completion_error(&streaming_error);
                            let error_text = streaming_error.to_string();
                            match retry.on_pre_stream_failure(
                                &classified,
                                &error_text,
                                Utc::now(),
                                config.deadline,
                            ) {
                                PreStreamDirective::RetryAfter { delay, kind } => {
                                    yield LoopStreamItem::AttemptFailed {
                                        turn: turn_index,
                                        attempt,
                                        error: classified,
                                        will_retry: true,
                                        backoff: delay,
                                    };
                                    tracing::warn!(
                                        turn = turn_index,
                                        attempt,
                                        kind = ?kind,
                                        delay_ms = delay.as_millis() as u64,
                                        error = %error_text,
                                        "retrying completion after transient failure"
                                    );
                                    tokio::time::sleep(delay).await;
                                    attempt += 1;
                                }
                                PreStreamDirective::Repair => {
                                    retry.mark_repair_used();
                                    yield LoopStreamItem::AttemptFailed {
                                        turn: turn_index,
                                        attempt,
                                        error: classified,
                                        will_retry: true,
                                        backoff: std::time::Duration::ZERO,
                                    };
                                    repair_provider_input(&mut history, &mut new_messages);
                                    let repaired_prompt = new_messages
                                        .last()
                                        .cloned()
                                        .expect("new_messages remains non-empty after repair");
                                    let repaired_prior = &new_messages[..new_messages.len() - 1];
                                    // `build_request`, not `build_budgeted_request`:
                                    // no output clamp is applied to a repaired
                                    // attempt.
                                    request = build_request(
                                        &model,
                                        repaired_prompt,
                                        &history,
                                        repaired_prior,
                                        tools.as_slice(),
                                        &config,
                                    )
                                    .await?;
                                    build_path = AssemblyBuildPath::Repair;
                                    // Repair rewrites `history` and `new_messages` in place, and
                                    // both are declared outside `'turns`. The durable transcript is
                                    // never rewritten to match, so every later turn is assembled
                                    // from messages no `AgentMessage` row reproduces. Mark the
                                    // effective list ephemeral for the rest of the request, not just
                                    // this turn — `build_path` resets per turn and would otherwise
                                    // report `Budgeted` for a turn whose input repair had altered.
                                    effective_messages_are_ephemeral = true;
                                    attempt += 1;
                                }
                                PreStreamDirective::Fail { reason } => {
                                    let terminal_reason =
                                        terminal_pre_stream_retry_reason(&classified, attempt, reason);
                                    yield LoopStreamItem::AttemptFailed {
                                        turn: turn_index,
                                        attempt,
                                        error: classified,
                                        will_retry: false,
                                        backoff: std::time::Duration::ZERO,
                                    };
                                    Err(StreamingError::Completion(
                                        CompletionError::ProviderError(terminal_reason),
                                    ))?;
                                    unreachable!("Err(..)? above ends the stream");
                                }
                            }
                        }
                    }
                };

            // Accumulate assistant content twice over: `accumulator` builds the
            // assistant message we thread back into `new_messages` for the next
            // turn (reasoning/tool-call/text ordering handled there), while the
            // yielded items drive the consumer's own accumulation/persistence.
            // `pending_results` holds each tool call's bounded result, executed
            // inline as its ToolCall arrives (see below) and threaded/yielded only
            // once the turn's stream has drained.
            let mut accumulator = AssistantTurnAccumulator::default();
            let mut pending_results: Vec<(ToolCall, String, String)> = Vec::new();
            let mut turn_text = String::new();
            let mut saw_stream_item = false;
            let mut saw_final_usage_event = false;
            let mut aggregate_budget_exhausted = false;
            let mut aggregate_usage_failure = None::<String>;

            while let Some(item) = stream.next().await {
                let item = match item {
                    Ok(item) => {
                        if !saw_stream_item {
                            // A provider response arrived while this attempt's
                            // capture was still waiting to be claimed, which
                            // means the send did not travel through the
                            // capturing transport. That is a mis-wired client
                            // stack, and the only honest response is to stop:
                            // silently continuing would produce a turn whose
                            // provider input is not durable anywhere.
                            ensure_rendered_request_was_captured(turn_index, attempt)?;
                        }
                        saw_stream_item = true;
                        item
                    }
                    Err(completion_error) if pending_results.is_empty() => {
                        let streaming_error = StreamingError::Completion(completion_error);
                        let classified = crate::error::classify_completion_error(&streaming_error);
                        let error_text = streaming_error.to_string();
                        if !saw_stream_item {
                            match retry.on_pre_stream_failure(
                                &classified,
                                &error_text,
                                Utc::now(),
                                config.deadline,
                            ) {
                                PreStreamDirective::RetryAfter { delay, kind } => {
                                    yield LoopStreamItem::AttemptFailed {
                                        turn: turn_index,
                                        attempt,
                                        error: classified,
                                        will_retry: true,
                                        backoff: delay,
                                    };
                                    tracing::warn!(
                                        turn = turn_index,
                                        attempt,
                                        kind = ?kind,
                                        delay_ms = delay.as_millis() as u64,
                                        error = %error_text,
                                        "retrying completion after first stream item failed"
                                    );
                                    tokio::time::sleep(delay).await;
                                    attempt += 1;
                                    continue 'attempts;
                                }
                                PreStreamDirective::Repair => {
                                    retry.mark_repair_used();
                                    yield LoopStreamItem::AttemptFailed {
                                        turn: turn_index,
                                        attempt,
                                        error: classified,
                                        will_retry: true,
                                        backoff: std::time::Duration::ZERO,
                                    };
                                    repair_provider_input(&mut history, &mut new_messages);
                                    let repaired_prompt = new_messages.last().cloned().expect(
                                        "new_messages remains non-empty after repair",
                                    );
                                    let repaired_prior = &new_messages[..new_messages.len() - 1];
                                    // `build_request`, not `build_budgeted_request`:
                                    // no output clamp is applied to a repaired
                                    // attempt.
                                    request = build_request(
                                        &model,
                                        repaired_prompt,
                                        &history,
                                        repaired_prior,
                                        tools.as_slice(),
                                        &config,
                                    )
                                    .await?;
                                    build_path = AssemblyBuildPath::Repair;
                                    // See the sibling repair arm above: repair mutates the
                                    // request-scoped message vectors, so the effective list stays
                                    // ephemeral for every later turn of this request.
                                    effective_messages_are_ephemeral = true;
                                    attempt += 1;
                                    continue 'attempts;
                                }
                                PreStreamDirective::Fail { reason } => {
                                    let terminal_reason = terminal_pre_stream_retry_reason(
                                        &classified,
                                        attempt,
                                        reason,
                                    );
                                    yield LoopStreamItem::AttemptFailed {
                                        turn: turn_index,
                                        attempt,
                                        error: classified,
                                        will_retry: false,
                                        backoff: std::time::Duration::ZERO,
                                    };
                                    Err(StreamingError::Completion(
                                        CompletionError::ProviderError(terminal_reason),
                                    ))?;
                                    unreachable!("Err(..)? above ends the stream");
                                }
                            }
                        }
                        if let Some(budget) = aggregate_token_budget.as_ref() {
                            let ledger = budget.snapshot()?;
                            Err(StreamingError::Completion(
                                CompletionError::ProviderError(format!(
                                    "aggregate_token_usage_missing: limit={}, used={}; \
                                     provider stream failed after emitting content without a \
                                     final usage event",
                                    ledger.limit, ledger.used,
                                )),
                            ))?;
                            unreachable!("Err(..)? above ends the stream");
                        }
                        match retry.on_mid_stream_failure(false, Utc::now(), config.deadline) {
                            MidStreamDirective::RetractAndResample { delay } => {
                                yield LoopStreamItem::TurnRetracted {
                                    turn: turn_index,
                                    attempt,
                                    backoff: delay,
                                };
                                tracing::warn!(
                                    turn = turn_index,
                                    attempt,
                                    delay_ms = delay.as_millis() as u64,
                                    error = %error_text,
                                    "retracting partial completion turn after mid-stream failure"
                                );
                                tokio::time::sleep(delay).await;
                                attempt += 1;
                                continue 'attempts;
                            }
                            MidStreamDirective::CloseAndContinue { .. } => {
                                unreachable!(
                                    "no-effect mid-stream failure cannot close and continue"
                                );
                            }
                            MidStreamDirective::Fail { reason } => {
                                let terminal_reason =
                                    terminal_pre_stream_retry_reason(&classified, attempt, reason);
                                Err(StreamingError::Completion(
                                    CompletionError::ProviderError(terminal_reason),
                                ))?;
                                unreachable!("Err(..)? above ends the stream");
                            }
                        }
                    }
                    Err(completion_error) => {
                        if let Some(budget) = aggregate_token_budget.as_ref() {
                            for item in close_streaming_turn(
                                &mut new_messages,
                                &mut accumulator,
                                stream.message_id.clone(),
                                pending_results,
                            ) {
                                yield item;
                            }
                            let ledger = budget.snapshot()?;
                            Err(StreamingError::Completion(
                                CompletionError::ProviderError(format!(
                                    "aggregate_token_usage_missing: limit={}, used={}; \
                                     provider stream failed after tool effects without a final \
                                     usage event",
                                    ledger.limit, ledger.used,
                                )),
                            ))?;
                            unreachable!("Err(..)? above ends the stream");
                        }
                        let streaming_error = StreamingError::Completion(completion_error);
                        let classified = crate::error::classify_completion_error(&streaming_error);
                        let error_text = streaming_error.to_string();
                        match retry.on_mid_stream_failure(true, Utc::now(), config.deadline) {
                            MidStreamDirective::CloseAndContinue { delay } => {
                                for item in close_streaming_turn(
                                    &mut new_messages,
                                    &mut accumulator,
                                    stream.message_id.clone(),
                                    pending_results,
                                ) {
                                    yield item;
                                }
                                yield LoopStreamItem::AttemptFailed {
                                    turn: turn_index,
                                    attempt,
                                    error: classified,
                                    will_retry: true,
                                    backoff: delay,
                                };
                                tracing::warn!(
                                    turn = turn_index,
                                    attempt,
                                    delay_ms = delay.as_millis() as u64,
                                    error = %error_text,
                                    "closing completion turn after mid-stream failure with tool effects"
                                );
                                tokio::time::sleep(delay).await;
                                continue 'turns;
                            }
                            MidStreamDirective::RetractAndResample { .. } => {
                                unreachable!(
                                    "effectful mid-stream failure cannot retract and resample"
                                );
                            }
                            MidStreamDirective::Fail { reason } => {
                                let terminal_reason =
                                    terminal_pre_stream_retry_reason(&classified, attempt, reason);
                                Err(StreamingError::Completion(
                                    CompletionError::ProviderError(terminal_reason),
                                ))?;
                                unreachable!("Err(..)? above ends the stream");
                            }
                        }
                    }
                };

                match item {
                    StreamedAssistantContent::Text(text) => {
                        turn_text.push_str(&text.text);
                        accumulator.push_text(&text.text);
                        yield LoopStreamItem::Item(MultiTurnStreamItem::StreamAssistantItem(StreamedAssistantContent::Text(text)));
                    }
                    StreamedAssistantContent::Reasoning(reasoning) => {
                        accumulator.push_reasoning(rig_compat::from_rig_reasoning(&reasoning));
                        yield LoopStreamItem::Item(MultiTurnStreamItem::StreamAssistantItem(StreamedAssistantContent::Reasoning(reasoning)));
                    }
                    StreamedAssistantContent::ReasoningDelta { id, reasoning } => {
                        accumulator.push_reasoning_delta(id.clone(), &reasoning);
                        yield LoopStreamItem::Item(MultiTurnStreamItem::StreamAssistantItem(StreamedAssistantContent::ReasoningDelta { id, reasoning }));
                    }
                    StreamedAssistantContent::ToolCall { tool_call, internal_call_id } => {
                        accumulator.push_tool_call(rig_compat::from_rig_tool_call(&tool_call));
                        yield LoopStreamItem::Item(MultiTurnStreamItem::StreamAssistantItem(
                            StreamedAssistantContent::ToolCall {
                                tool_call: tool_call.clone(),
                                internal_call_id: internal_call_id.clone(),
                            },
                        ));

                        let tool_name = tool_call.function.name.clone();
                        let tool_args = value_to_json_string(&tool_call.function.arguments);

                        let call_action = match hook.as_ref() {
                            Some(hook) => {
                                hook.on_tool_call(
                                    &tool_name,
                                    tool_call.call_id.clone(),
                                    &internal_call_id,
                                    &tool_args,
                                )
                                .await
                            }
                            None => ToolCallHookAction::Continue,
                        };

                        let bounded_result = match call_action {
                            ToolCallHookAction::Terminate { reason } => {
                                Err(StreamingError::Prompt(Box::new(
                                    PromptError::PromptCancelled {
                                        chat_history: rig_compat::to_rig_messages(&error_chat_history(
                                            &history,
                                            &new_messages,
                                        )),
                                        reason,
                                    },
                                )))?;
                                unreachable!("Err(..)? above ends the stream");
                            }
                            ToolCallHookAction::Skip { reason } => {
                                reason
                            }
                            _ => {
                                let live_output = match hook.as_ref() {
                                    Some(hook) => Some(
                                        hook.foreground_live_output_writer(&internal_call_id)
                                            .await,
                                    ),
                                    None => None,
                                };
                                let outcome = dispatch_tool(
                                    tools.as_slice(),
                                    &tool_name,
                                    tool_args.clone(),
                                    live_output,
                                )
                                .await;

                                if let Some(hook) = hook.as_ref() {
                                    let result_action = hook
                                        .on_tool_result(
                                            &tool_name,
                                            tool_call.call_id.clone(),
                                            &internal_call_id,
                                            &tool_args,
                                            &outcome,
                                        )
                                        .await;
                                    if let HookAction::Terminate { reason } = result_action {
                                        Err(StreamingError::Prompt(Box::new(
                                            PromptError::PromptCancelled {
                                                chat_history: rig_compat::to_rig_messages(&error_chat_history(
                                                    &history,
                                                    &new_messages,
                                                )),
                                                reason,
                                            },
                                        )))?;
                                    }
                                }
                                // The typed outcome's model-facing accessor is
                                // the only text that may thread to the model.
                                let (bounded, _, _) = truncate_text(
                                    outcome.model_facing_text(),
                                    tool_result_truncation_mode(&tool_name),
                                    &TruncationLimits::default(),
                                );
                                bounded
                            }
                        };

                        pending_results.push((rig_compat::from_rig_tool_call(&tool_call), internal_call_id, bounded_result));
                    }
                    StreamedAssistantContent::ToolCallDelta { .. } => {
                    }
                    StreamedAssistantContent::Final(raw) => {
                        saw_final_usage_event = true;
                        let usage = raw.token_usage();
                        if let Some(usage) = usage {
                            add_usage_saturating(&mut aggregated_usage, usage);
                        }
                        if let Some(budget) = aggregate_token_budget.as_ref() {
                            let (charge, ledger) = budget.charge_reported(usage)?;
                            match charge {
                                AggregateTokenCharge::Missing => {
                                    aggregate_usage_failure = Some(format!(
                                        "aggregate_token_usage_missing: limit={}, used={}; \
                                         provider completed without a non-zero usage report",
                                        ledger.limit, ledger.used,
                                    ));
                                }
                                AggregateTokenCharge::Within => {}
                                AggregateTokenCharge::Exhausted => {
                                    aggregate_budget_exhausted = true;
                                }
                                AggregateTokenCharge::Overrun => {
                                    aggregate_usage_failure = Some(format!(
                                        "aggregate_token_budget_overrun: limit={}, observed_used={}",
                                        ledger.limit, ledger.used,
                                    ));
                                }
                            }
                        }
                    }
                }
            }

            // An empty stream never enters the item branch above. It is still
            // proof that `model.stream` dispatched, so it must not reach the
            // ordinary no-output retry path while this attempt remains armed.
            if !saw_stream_item {
                ensure_rendered_request_was_captured(turn_index, attempt)?;
            }

            if aggregate_token_budget.is_some() && !saw_final_usage_event {
                let ledger = aggregate_token_budget
                    .as_ref()
                    .expect("configured aggregate token budget remains present")
                    .snapshot()?;
                aggregate_usage_failure = Some(format!(
                    "aggregate_token_usage_missing: limit={}, used={}; \
                     provider stream ended without a final usage event",
                    ledger.limit, ledger.used,
                ));
            }

            if let Some(reason) = aggregate_usage_failure {
                for item in close_streaming_turn(
                    &mut new_messages,
                    &mut accumulator,
                    stream.message_id.clone(),
                    pending_results,
                ) {
                    yield item;
                }
                Err(StreamingError::Completion(CompletionError::ProviderError(reason)))?;
                unreachable!("Err(..)? above ends the stream");
            }

            let structured_output_error = if pending_results.is_empty() {
                config
                    .structured_output
                    .as_ref()
                    .and_then(|output| (output.validate)(&turn_text).err())
            } else {
                None
            };
            let terminal_valid = pending_results.is_empty()
                && !turn_text.trim().is_empty()
                && structured_output_error.is_none();
            if aggregate_budget_exhausted
                && aggregate_post_charge_action(
                    AggregateTokenCharge::Exhausted,
                    terminal_valid,
                )
                    == AggregatePostChargeAction::Fail
            {
                for item in close_streaming_turn(
                    &mut new_messages,
                    &mut accumulator,
                    stream.message_id.clone(),
                    pending_results,
                ) {
                    yield item;
                }
                let ledger = aggregate_token_budget
                    .as_ref()
                    .expect("exhaustion requires a configured aggregate token budget")
                    .snapshot()?;
                let contract_detail = structured_output_error
                    .as_deref()
                    .map(|error| format!("; terminal output did not satisfy the structured contract: {error}"))
                    .unwrap_or_default();
                Err(StreamingError::Completion(CompletionError::ProviderError(format!(
                    "{AGGREGATE_TOKEN_BUDGET_EXHAUSTED_PREFIX}limit={}, used={} after provider call{}",
                    ledger.limit, ledger.used, contract_detail,
                ))))?;
                unreachable!("Err(..)? above ends the stream");
            }

            if pending_results.is_empty() && turn_text.trim().is_empty() {
                // A provider can finish a turn after emitting only reasoning.
                // Known DeepSeek V4 Flash failure shapes include stopping
                // without closing the reasoning block and exhausting the
                // output-token allowance. Rig does not currently retain the
                // provider finish reason, so classify only the invariant we
                // can observe here: this is not a usable terminal answer, and
                // no tool effect has run. Model it as the existing no-effect
                // mid-stream failure: retract the streamed reasoning and
                // resample the same provider request, bounded by the configured
                // transport retry ladder. This is exactly
                // CompletionRetry.retract, so no durable side effect is replayed.
                match retry.on_mid_stream_failure(false, Utc::now(), config.deadline) {
                    MidStreamDirective::RetractAndResample { delay } => {
                        yield LoopStreamItem::TurnRetracted {
                            turn: turn_index,
                            attempt,
                            backoff: delay,
                        };
                        tracing::warn!(
                            turn = turn_index,
                            attempt,
                            delay_ms = delay.as_millis() as u64,
                            "retracting completion turn with no visible output"
                        );
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                        continue 'attempts;
                    }
                    MidStreamDirective::CloseAndContinue { .. } => {
                        unreachable!(
                            "no-output completion without tool effects cannot close and continue"
                        );
                    }
                    MidStreamDirective::Fail { reason } => {
                        Err(StreamingError::Completion(
                            CompletionError::ProviderError(format!(
                                "completion produced no visible output: {reason}; \
                                 raw_output_preview=\"\"; \
                                 finish_metadata=unavailable_at_rig_streaming_boundary"
                            )),
                        ))?;
                        unreachable!("Err(..)? above ends the stream");
                    }
                }
            }

            if let Some(error) = structured_output_error {
                // The provider completed normally, but the result does not
                // satisfy the typed contract Rig sent. No tool effect has run,
                // so this is the same proven CompletionRetry.retract transition
                // as an interrupted or empty no-effect turn: discard all
                // streamed content and resample the identical request.
                match retry.on_mid_stream_failure(false, Utc::now(), config.deadline) {
                    MidStreamDirective::RetractAndResample { delay } => {
                        yield LoopStreamItem::TurnRetracted {
                            turn: turn_index,
                            attempt,
                            backoff: delay,
                        };
                        tracing::warn!(
                            turn = turn_index,
                            attempt,
                            delay_ms = delay.as_millis() as u64,
                            error = %error,
                            "retracting completion turn after structured-output validation failure"
                        );
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                        continue 'attempts;
                    }
                    MidStreamDirective::CloseAndContinue { .. } => {
                        unreachable!(
                            "invalid structured output without tool effects cannot close and continue"
                        );
                    }
                    MidStreamDirective::Fail { reason } => {
                        Err(StreamingError::Completion(
                            CompletionError::ProviderError(format!(
                                "structured-output validation failed: {error}; {reason}"
                            )),
                        ))?;
                        unreachable!("Err(..)? above ends the stream");
                    }
                }
            }

            if pending_results.is_empty() {
                yield LoopStreamItem::Item(MultiTurnStreamItem::final_response(&turn_text, aggregated_usage));
                break 'turns;
            }

            for item in close_streaming_turn(
                &mut new_messages,
                &mut accumulator,
                stream.message_id.clone(),
                pending_results,
            ) {
                yield item;
            }
            break 'attempts;
        }
        }
    }
}

fn close_streaming_turn<R>(
    new_messages: &mut Vec<Message>,
    accumulator: &mut AssistantTurnAccumulator,
    message_id: Option<String>,
    pending_results: Vec<(ToolCall, String, String)>,
) -> Vec<LoopStreamItem<R>> {
    // Thread the assistant turn (text + reasoning + tool calls) ahead of its
    // tool results, matching rig's history ordering. Carry the provider
    // message id (captured into `stream.message_id` from the stream's
    // `MessageId` event) onto the threaded message — rig threads this same id,
    // and OpenAI Responses / ChatGPT Codex follow-up requests reference prior
    // `msg_` ids, so dropping it breaks them.
    if let Some(mut assistant_message) = accumulator.take_message() {
        if let Message::Assistant { id, .. } = &mut assistant_message {
            *id = message_id;
        }
        new_messages.push(assistant_message);
    }

    pending_results
        .into_iter()
        .map(|(tool_call, internal_call_id, bounded_result)| {
            let content = ToolResultContent::from_tool_output(bounded_result);
            let user_content = match tool_call.call_id.clone() {
                Some(call_id) => UserContent::tool_result_with_call_id(
                    tool_call.id.clone(),
                    call_id,
                    content.clone(),
                ),
                None => UserContent::tool_result(tool_call.id.clone(), content.clone()),
            };
            new_messages.push(Message::User {
                content: vec![user_content],
            });

            let tool_result = ToolResult {
                id: tool_call.id.clone(),
                call_id: tool_call.call_id.clone(),
                content,
            };
            LoopStreamItem::Item(MultiTurnStreamItem::StreamUserItem(
                StreamedUserContent::ToolResult {
                    tool_result: rig_compat::to_rig_tool_result(&tool_result),
                    internal_call_id,
                },
            ))
        })
        .collect()
}

fn terminal_pre_stream_retry_reason(
    classified: &InferenceError,
    attempt: u32,
    reason: String,
) -> String {
    if !classified.is_retryable() {
        reason
    } else {
        format!(
            "completion retry budget exhausted after {} attempts: {reason}; last error: {classified}",
            attempt + 1
        )
    }
}

/// Repair the ASSEMBLED provider input — loaded history and run-threaded
/// messages alike (#652).
///
/// This runs only after the provider has already REJECTED the request (the
/// completion-retry `Repair` directive). It is deliberately more aggressive
/// than the egress normalizer: on top of the shape coercion it runs a LOSSY
/// leaf sanitizer over every JSON string in a tool call's arguments. That
/// lossiness is exactly why it cannot live at egress — it would corrupt
/// legitimate multi-line tool arguments on every request.
///
/// It used to rewrite only `new_messages`. But the motivating failure (the vLLM
/// parse-signature 400) originates from tool-call arguments in the INPUT
/// TRANSCRIPT — i.e. the loaded history it skipped — so repair re-issued the
/// same poisoned input and failed identically. The fence described a transform
/// that did not exist.
///
/// Widening it to history is licensed by `PromptAssembly.repair_is_payload_only`
/// (repair rewrites argument payloads only — never rows, roles, call ids, or
/// ordering, so the row-granular assembly theorems T1–T5 hold verbatim) and by
/// `PromptAssembly.repair_idempotent` (a second pass is a no-op, so re-entering
/// the path cannot keep re-escaping its own escapes).
pub(crate) fn repair_provider_input(history: &mut Vec<Message>, new_messages: &mut Vec<Message>) {
    repair_messages(history);
    repair_messages(new_messages);
    *history = crate::compaction::sanitize_history_for_provider(std::mem::take(history));
    *new_messages = crate::compaction::sanitize_history_for_provider(std::mem::take(new_messages));
}

fn repair_messages(messages: &mut [Message]) {
    for message in messages.iter_mut() {
        let Message::Assistant { content, .. } = message else {
            continue;
        };
        for item in content {
            let AssistantContent::ToolCall(tool_call) = item else {
                continue;
            };
            let mut repaired = crate::llm::tool::normalize_tool_call_arguments(
                "repair",
                &tool_call.function.name,
                &tool_call.function.arguments,
            );
            sanitize_json_string_leaves(&mut repaired);
            tool_call.function.arguments = repaired;
        }
    }
}

fn sanitize_json_string_leaves(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                sanitize_json_string_leaves(value);
            }
        }
        serde_json::Value::Object(map) => {
            for value in map.values_mut() {
                sanitize_json_string_leaves(value);
            }
        }
        serde_json::Value::String(text) => {
            *text = sanitize_provider_arg_string(text);
        }
        _ => {}
    }
}

fn sanitize_provider_arg_string(text: &str) -> String {
    let mut sanitized = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '\n' => sanitized.push_str("\\n"),
            '\t' => sanitized.push_str("\\t"),
            ch if ch.is_control() => {}
            ch => sanitized.push(ch),
        }
    }
    sanitized
}

pub(crate) async fn run_loop_to_text<M>(
    model: M,
    hook: Option<DefraSessionHook>,
    prompt: Message,
    history: Vec<Message>,
    tools: Arc<Vec<Box<dyn ToolDyn>>>,
    config: LoopConfig,
) -> anyhow::Result<String>
where
    M: CompletionModel + 'static,
    M::StreamingResponse: 'static,
{
    let stream = run_loop_stream(model, hook.clone(), prompt, history, tools, config);
    futures::pin_mut!(stream);
    let mut accumulator = AssistantTurnAccumulator::default();
    let mut final_text = String::new();
    let mut last_attempt_error: Option<InferenceError> = None;

    while let Some(item) = stream.next().await {
        let item = item.map_err(|error| {
            let error = anyhow::Error::new(error);
            match last_attempt_error.as_ref() {
                Some(last_error) => error.context(format!(
                    "one-shot loop stream error after retry failure ({last_error})"
                )),
                None => error.context("one-shot loop stream error"),
            }
        })?;
        match item {
            LoopStreamItem::TurnRetracted { .. } => {
                accumulator = AssistantTurnAccumulator::default();
                continue;
            }
            LoopStreamItem::AttemptFailed { error, .. } => {
                last_attempt_error = Some(error);
                continue;
            }
            LoopStreamItem::Item(item) => match item {
                MultiTurnStreamItem::StreamAssistantItem(content) => match content {
                    StreamedAssistantContent::Text(text) => accumulator.push_text(&text.text),
                    StreamedAssistantContent::Reasoning(reasoning) => {
                        accumulator.push_reasoning(rig_compat::from_rig_reasoning(&reasoning))
                    }
                    StreamedAssistantContent::ReasoningDelta { id, reasoning } => {
                        accumulator.push_reasoning_delta(id, &reasoning)
                    }
                    StreamedAssistantContent::ToolCall {
                        tool_call,
                        internal_call_id,
                    } => {
                        if let Some(hook) = hook.as_ref() {
                            hook.register_stream_tool_call_identity(
                                &internal_call_id,
                                &tool_call.id,
                                tool_call.call_id.as_deref(),
                            )
                            .await;
                        }
                        accumulator.push_tool_call(rig_compat::from_rig_tool_call(&tool_call));
                    }
                    _ => {}
                },
                MultiTurnStreamItem::StreamUserItem(StreamedUserContent::ToolResult {
                    tool_result,
                    internal_call_id,
                }) => {
                    if let Some(hook) = hook.as_ref() {
                        if let Some(message) = accumulator.take_message() {
                            hook.apply_persistence_policy(
                                hook.persist_message(&message).await.map(|_| ()),
                                "persist one-shot assistant turn",
                            )?;
                        }
                        hook.apply_persistence_policy(
                            hook.persist_stream_tool_result_message(
                                &rig_compat::from_rig_tool_result(&tool_result),
                                &internal_call_id,
                            )
                            .await,
                            "persist one-shot tool result",
                        )?;
                    }
                }
                MultiTurnStreamItem::FinalResponse(final_response) => {
                    accumulator.reconcile_text(final_response.response());
                    if let Some(hook) = hook.as_ref() {
                        if let Some(message) = accumulator.take_message() {
                            hook.apply_persistence_policy(
                                hook.persist_message(&message).await.map(|_| ()),
                                "persist one-shot final assistant turn",
                            )?;
                        }
                    }
                    final_text = final_response.response().to_string();
                }
                _ => {}
            },
        }
    }
    Ok(final_text)
}

/// Runs a typed completion without surrendering the runtime's owned-loop
/// chokepoint to Rig's `Agent` orchestration. Rig's schema is attached to every
/// provider request, while the owned loop validates before accepting a final
/// turn and applies its normal bounded recovery policy on malformed output.
pub(crate) async fn run_loop_to_typed<M, T>(
    model: M,
    hook: Option<DefraSessionHook>,
    prompt: Message,
    history: Vec<Message>,
    tools: Arc<Vec<Box<dyn ToolDyn>>>,
    mut config: LoopConfig,
) -> anyhow::Result<T>
where
    M: CompletionModel + 'static,
    M::StreamingResponse: 'static,
    T: DeserializeOwned + schemars::JsonSchema + 'static,
{
    config.structured_output = Some(StructuredOutputConfig::for_type::<T>());
    let raw = run_loop_to_text(model, hook, prompt, history, tools, config).await?;
    serde_json::from_str(&raw).map_err(|error| {
        anyhow::anyhow!(
            "decoding validated structured output as {} failed: {error}",
            std::any::type_name::<T>()
        )
    })
}

fn error_chat_history(history: &[Message], new_messages: &[Message]) -> Vec<Message> {
    history.iter().chain(new_messages.iter()).cloned().collect()
}

fn current_rag_text(prompt: &Message, history: &[Message], prior: &[Message]) -> String {
    if let Some(text) = prompt.rag_text() {
        return text;
    }
    history
        .iter()
        .chain(prior.iter())
        .rev()
        .find_map(Message::rag_text)
        .unwrap_or_default()
}

fn value_to_json_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(string) => string.clone(),
        other => other.to_string(),
    }
}

pub(crate) async fn dispatch_tool(
    tools: &[Box<dyn ToolDyn>],
    name: &str,
    args: String,
    live_output: Option<crate::background_tools::LiveToolOutputWriter>,
) -> ToolOutcome {
    let Some(tool) = tools.iter().find(|tool| tool.name() == name) else {
        // An unresolved tool name is a dispatch FAILURE, not tool output.
        // Models hallucinate tool names and stale surfaces outlive their
        // tools, so this is a routine path, not an exotic one; classifying it
        // `Completed` would reproduce the durability bug the typed channel
        // exists to close (#400/D6). The detail text is unchanged so the model
        // sees what it always saw.
        return ToolOutcome::from_tool_call_error(&format!("error: unknown tool '{name}'"));
    };

    let Some(scope) = current_tool_runtime_context() else {
        return ToolOutcome::from_dispatch(name, tool.call(args).await);
    };

    if deadline_remaining(scope.deadline_at).is_some_and(|remaining| remaining.is_zero()) {
        return ToolOutcome::TimedOut {
            deadline_at: scope.deadline_at,
        };
    }

    let deadline_at = scope.deadline_at;
    let mut deadline = Box::pin(async move {
        match deadline_remaining(deadline_at) {
            Some(remaining) => tokio::time::sleep(remaining).await,
            None => std::future::pending::<()>().await,
        }
    });

    let call = scope_request_tool_execution_with_workspace_and_live_output(
        scope.deadline_at,
        scope.cancellation_token.clone(),
        scope.workspace_cwd.clone(),
        live_output,
        tool.call(args),
    );
    tokio::select! {
        biased;
        _ = scope.cancellation_token.cancelled() => ToolOutcome::Cancelled,
        _ = &mut deadline => ToolOutcome::TimedOut { deadline_at: scope.deadline_at },
        result = call => ToolOutcome::from_dispatch(name, result),
    }
}

fn serialized_token_estimate<T: serde::Serialize + ?Sized>(value: &T) -> usize {
    serde_json::to_string(value)
        .map(|json| crate::compaction::estimate_tokens(&json))
        .unwrap_or_default()
}

/// Estimate the complete provider input represented by Rig's rendered request,
/// including static tool schemas. The production profile deliberately leaves
/// tokenizer headroom because this estimator is approximate; its job here is to
/// apply that conservative profile to every turn's assembled request, in
/// `build_budgeted_request`. A mid-turn `Repair` rebuild is not re-estimated:
/// repair only normalizes tool arguments and drops orphaned pairs, so it cannot
/// grow the input past the budget already cleared for that turn.
fn completion_request_input_estimate(request: &CompletionRequest) -> usize {
    serialized_token_estimate(&request.chat_history)
        .saturating_add(serialized_token_estimate(&request.documents))
        .saturating_add(serialized_token_estimate(&request.tools))
        .saturating_add(serialized_token_estimate(&request.additional_params))
        .saturating_add(serialized_token_estimate(&request.output_schema))
}

/// Treat the configured output value as a ceiling and fit each completion to
/// the context remaining after its fully assembled provider input. Compaction
/// protects the configured input threshold; this clamp independently preserves
/// `input + output <= context` on every dispatch.
fn clamp_request_output_budget(request: &mut CompletionRequest, config: &LoopConfig) {
    let Some(configured_max) = request.max_tokens else {
        return;
    };
    let input_tokens = completion_request_input_estimate(request);
    let configured_max = usize::try_from(configured_max).unwrap_or(usize::MAX);
    let effective_max = crate::compaction::effective_output_budget(
        input_tokens,
        config.context_window,
        configured_max,
    );
    if effective_max < configured_max {
        tracing::debug!(
            input_tokens,
            context_window = config.context_window,
            configured_max_output_tokens = configured_max,
            effective_max_output_tokens = effective_max,
            "clamped completion output to remaining provider context"
        );
    }
    request.max_tokens = u64::try_from(effective_max).ok();
}

/// Apply the request-wide token ledger immediately before every provider
/// dispatch. The input estimate is the same complete rendered-request estimate
/// fenced by `PromptAssembly.Budget`; provider tokenization remains an external
/// boundary, so the post-call usage report is checked independently.
fn clamp_request_aggregate_token_budget(
    request: &mut CompletionRequest,
    budget: Option<&AggregateTokenBudget>,
) -> Result<(), StreamingError> {
    let Some(budget) = budget else {
        return Ok(());
    };
    let ledger = budget.snapshot()?;
    let input_tokens =
        u64::try_from(completion_request_input_estimate(request)).unwrap_or(u64::MAX);
    let configured_max = request.max_tokens.unwrap_or(u64::MAX);
    let effective_max = ledger.effective_output_tokens(input_tokens, configured_max);
    if !ledger.can_dispatch(input_tokens, configured_max) {
        return Err(StreamingError::Completion(CompletionError::ProviderError(
            format!(
                "{AGGREGATE_TOKEN_BUDGET_EXHAUSTED_PREFIX}limit={}, used={}, \
                 estimated_input_tokens={input_tokens}, remaining={}",
                ledger.limit,
                ledger.used,
                ledger.remaining(),
            ),
        )));
    }
    if effective_max < configured_max {
        tracing::debug!(
            token_limit = ledger.limit,
            tokens_used = ledger.used,
            input_tokens,
            configured_max_output_tokens = configured_max,
            effective_max_output_tokens = effective_max,
            "clamped completion output to the request-wide token budget"
        );
    }
    request.max_tokens = Some(effective_max);
    Ok(())
}

fn compactable_message_estimate(messages: &[Message]) -> usize {
    let rig_messages = messages
        .iter()
        .map(rig_compat::to_rig_message)
        .collect::<Vec<_>>();
    serialized_token_estimate(&rig_messages)
}

/// Keep enough room for both the non-compactable request layers (preamble,
/// tool schemas, provider parameters) and the summary inserted by the
/// compactor. The post-compaction dispatch guard remains authoritative if a
/// pathological summary or a single oversized current prompt still does not
/// fit.
fn turn_keep_recent_target(
    request: &CompletionRequest,
    provider_messages: &[Message],
    config: &LoopConfig,
) -> usize {
    let effective_budget = crate::compaction::effective_input_budget(
        config.context_window,
        config
            .max_tokens
            .and_then(|tokens| usize::try_from(tokens).ok())
            .unwrap_or_default(),
        config.compaction_threshold,
    );
    let total_input = completion_request_input_estimate(request);
    let compactable_input = compactable_message_estimate(provider_messages);
    let static_input = total_input.saturating_sub(compactable_input);
    let message_budget = effective_budget.saturating_sub(static_input);

    // Summaries vary with the model and history. Reserve one quarter of the
    // compactable-message budget for the summary and serialization drift.
    message_budget.saturating_mul(3) / 4
}

fn completion_request_exceeds_budget(request: &CompletionRequest, config: &LoopConfig) -> bool {
    let max_output_tokens = config
        .max_tokens
        .and_then(|tokens| usize::try_from(tokens).ok())
        .unwrap_or_default();
    crate::compaction::input_exceeds_budget(
        completion_request_input_estimate(request),
        config.context_window,
        max_output_tokens,
        config.compaction_threshold,
    )
}

fn ensure_rendered_request_was_captured(
    turn_index: usize,
    attempt: u32,
) -> Result<(), StreamingError> {
    if crate::rendered_request::scope::pending_is_armed() {
        return Err(StreamingError::Completion(CompletionError::ProviderError(
            format!(
                "provider response for turn {turn_index} attempt {attempt} \
                 arrived without a durable rendered-request capture; the \
                 completion client is missing its capturing transport"
            ),
        )));
    }
    Ok(())
}

async fn build_budgeted_request<M: CompletionModel>(
    model: &M,
    history: &mut Vec<Message>,
    new_messages: &mut Vec<Message>,
    tools: &[Box<dyn ToolDyn>],
    config: &LoopConfig,
    turn_index: usize,
) -> Result<(CompletionRequest, bool), StreamingError> {
    let current_prompt = new_messages
        .last()
        .cloned()
        .expect("new_messages always retains at least the initial prompt");
    let prior = &new_messages[..new_messages.len() - 1];
    let mut request = build_request(model, current_prompt, history, prior, tools, config).await?;
    clamp_request_output_budget(&mut request, config);

    let Some(compactor) = config.turn_compactor.as_ref() else {
        return Ok((request, false));
    };
    if !completion_request_exceeds_budget(&request, config) {
        return Ok((request, false));
    }

    let provider_messages = history
        .iter()
        .chain(new_messages.iter())
        .cloned()
        .collect::<Vec<_>>();
    let before_tokens = completion_request_input_estimate(&request);
    let keep_recent_target = turn_keep_recent_target(&request, &provider_messages, config);
    let mut compacted = compactor(provider_messages, keep_recent_target)
        .await
        .map_err(|error| {
            aggregate_token_budget_exhaustion_message(&error).map_or_else(
                || {
                    StreamingError::Completion(CompletionError::ProviderError(format!(
                        "per-turn provider-input compaction failed: {error:#}"
                    )))
                },
                |reason| StreamingError::Completion(CompletionError::ProviderError(reason)),
            )
        })?;
    let compacted_prompt = compacted.pop().ok_or_else(|| {
        StreamingError::Completion(CompletionError::ProviderError(
            "per-turn provider-input compaction returned no prompt".to_string(),
        ))
    })?;
    *history = compacted;
    *new_messages = vec![compacted_prompt.clone()];

    let mut rebuilt = build_request(model, compacted_prompt, history, &[], tools, config).await?;
    clamp_request_output_budget(&mut rebuilt, config);
    let after_tokens = completion_request_input_estimate(&rebuilt);
    tracing::info!(
        turn = turn_index,
        before_tokens,
        after_tokens,
        keep_recent_target,
        context_window = config.context_window,
        max_output_tokens = config.max_tokens.unwrap_or_default(),
        "compacted provider input before completion dispatch"
    );

    if completion_request_exceeds_budget(&rebuilt, config) {
        let effective_budget = crate::compaction::effective_input_budget(
            config.context_window,
            config
                .max_tokens
                .and_then(|tokens| usize::try_from(tokens).ok())
                .unwrap_or_default(),
            config.compaction_threshold,
        );
        return Err(StreamingError::Completion(CompletionError::ProviderError(
            format!(
                "per-turn provider input remains over budget after compaction: \
                 estimated_input_tokens={after_tokens}, effective_input_budget={effective_budget}"
            ),
        )));
    }

    Ok((rebuilt, true))
}

async fn build_request<M: CompletionModel>(
    model: &M,
    prompt: Message,
    history: &[Message],
    prior: &[Message],
    tools: &[Box<dyn ToolDyn>],
    config: &LoopConfig,
) -> Result<CompletionRequest, StreamingError> {
    let rag_text = current_rag_text(&prompt, history, prior);
    let mut tool_defs = Vec::with_capacity(tools.len());
    for tool in tools {
        let native = tool.definition(rag_text.clone()).await;
        tool_defs.push(crate::llm::rig_compat::to_rig_tool_definition(&native));
    }

    let chat_history: Vec<rig::completion::Message> = config
        .preamble
        .as_ref()
        .map(|preamble| rig::completion::Message::system(preamble.clone()))
        .into_iter()
        .chain(history.iter().map(rig_compat::to_rig_message))
        .chain(prior.iter().map(rig_compat::to_rig_message))
        .collect();

    let mut builder = model
        .completion_request(rig_compat::to_rig_message(&prompt))
        .messages(chat_history)
        .temperature_opt(config.temperature)
        .max_tokens_opt(config.max_tokens)
        .additional_params_opt(config.additional_params.clone())
        .output_schema_opt(
            config
                .structured_output
                .as_ref()
                .map(|output| output.schema.clone()),
        )
        .tools(tool_defs);

    if let Some(tool_choice) = &config.tool_choice {
        builder = builder.tool_choice(crate::llm::rig_compat::to_rig_tool_choice(tool_choice));
    }

    Ok(builder.build())
}
