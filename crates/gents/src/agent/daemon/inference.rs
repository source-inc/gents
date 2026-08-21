use std::future::IntoFuture;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use tracing::Instrument;

use super::{BehaviorDaemon, HandleRequestOutcome};
use crate::admission::{self, CallKind};
use crate::compaction::{CompactionOptions, Compactor};
use crate::config::AgentBehavior;
use crate::hook::DefraSessionHook;
use crate::llm::message::Message;
use crate::streaming::{StreamStatus, StreamWriter};
use crate::watcher::AgentRequest;

type RequestDeadline = Option<DateTime<Utc>>;

fn terminal_response_has_visible_output(streamed_text: &str, final_text: Option<&str>) -> bool {
    !streamed_text.trim().is_empty() || final_text.is_some_and(|text| !text.trim().is_empty())
}

fn request_deadline_remaining(deadline: RequestDeadline) -> Option<Duration> {
    let deadline = deadline?;
    let now = Utc::now();
    if now >= deadline {
        return Some(Duration::ZERO);
    }
    Some((deadline - now).to_std().unwrap_or(Duration::ZERO))
}

fn request_deadline_error(deadline: RequestDeadline, context: &str) -> anyhow::Error {
    match deadline {
        Some(deadline) => anyhow!(
            "request deadline exceeded while {}; deadline={}",
            context,
            deadline.to_rfc3339()
        ),
        None => anyhow!("request deadline exceeded while {}", context),
    }
}

fn ensure_request_deadline_open(deadline: RequestDeadline, context: &str) -> Result<()> {
    if request_deadline_remaining(deadline).is_some_and(|remaining| remaining.is_zero()) {
        return Err(request_deadline_error(deadline, context));
    }
    Ok(())
}

fn render_request_context_message(
    node: &defra_node::EmbeddedNode,
    behavior: &AgentBehavior,
    request: &AgentRequest,
    frozen_instruction_manifest: Option<&str>,
) -> Result<Option<Message>> {
    let template_body = match behavior.request_context_template.as_deref() {
        Some(template) if !template.trim().is_empty() => {
            let mut ctx = serde_json::Map::new();
            ctx.insert(
                "now".to_string(),
                serde_json::json!(Utc::now().to_rfc3339()),
            );
            if template.contains("collection_summary") {
                ctx.insert(
                    "collection_summary".to_string(),
                    serde_json::json!(crate::template::collection_summary(node)?),
                );
            }

            let rendered = crate::template::render_request_context_template(
                template,
                serde_json::json!({
                    "node_did": behavior.agent_did(),
                    "behavior_id": behavior.behavior_id.as_str(),
                }),
                serde_json::Value::Object(ctx),
                &crate::template::catalog::default_catalog(),
            )
            .map_err(|error| anyhow!("request_context_template render failed: {error}"))?;
            tracing::debug!(
                request_id = %request.request_id,
                behavior_id = %behavior.behavior_id,
                "rendered request context template"
            );
            Some(rendered)
        }
        _ => None,
    };
    Ok(assemble_request_context_message(
        template_body,
        frozen_instruction_manifest,
    ))
}

fn assemble_request_context_message(
    template_body: Option<String>,
    frozen_instruction_manifest: Option<&str>,
) -> Option<Message> {
    let instruction_body =
        frozen_instruction_manifest.and_then(crate::workspace::instruction_context_section);
    match (template_body, instruction_body) {
        (None, None) => None,
        (template, instructions) => {
            let mut body = String::new();
            if let Some(template) = template {
                body.push_str(&template);
            }
            if let Some(instructions) = instructions {
                if !body.is_empty() {
                    body.push_str("\n\n");
                }
                body.push_str(&instructions);
            }
            Some(Message::user(format!("<context>\n{body}\n</context>")))
        }
    }
}

async fn await_with_request_deadline<F, T>(
    deadline: RequestDeadline,
    future: F,
    context: &str,
) -> Result<T>
where
    F: IntoFuture<Output = T>,
{
    let future = future.into_future();
    match request_deadline_remaining(deadline) {
        None => Ok(future.await),
        Some(remaining) if remaining.is_zero() => Err(request_deadline_error(deadline, context)),
        Some(remaining) => tokio::time::timeout(remaining, future)
            .await
            .map_err(|_| request_deadline_error(deadline, context)),
    }
}

impl<M: rig::completion::CompletionModel + 'static> BehaviorDaemon<M> {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run_inference(
        &mut self,
        request: &crate::watcher::AgentRequest,
        doc_id: &str,
        history: &[crate::llm::message::Message],
        lifecycle: &mut crate::lifecycle::RequestLifecycle,
        shutdown: &mut tokio::sync::watch::Receiver<bool>,
        interrupt_rx: &mut tokio::sync::watch::Receiver<Option<crate::interrupt::InterruptIntent>>,
        request_token: &tokio_util::sync::CancellationToken,
        aggregate_token_budget: Option<crate::agent::loop_stream::AggregateTokenBudget>,
        effective_seed: Option<i64>,
        workspace: crate::tool_call_lifecycle::runtime::ToolWorkspaceScope,
        frozen_instruction_manifest: Option<String>,
    ) -> Result<HandleRequestOutcome> {
        let request_deadline = lifecycle.claimed_deadline_at();
        let trigger_context = crate::lifecycle::TriggerExecutionContext::parse(
            request.caused_by_trigger_context.as_deref(),
        )?;
        let trigger_correlation = request.caused_by_correlation.clone();
        let deadline_at = request
            .deadline
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("")
            .to_string();
        let has_deadline = !deadline_at.is_empty();
        let workspace_cwd_set = workspace.workspace_cwd.is_some();

        let request_context_message = render_request_context_message(
            self.node.as_ref(),
            &self.behavior,
            request,
            frozen_instruction_manifest.as_deref(),
        )?;

        ensure_request_deadline_open(request_deadline, "starting inference")?;
        if *shutdown.borrow() {
            return Err(anyhow!("shutdown requested during inference"));
        }
        if interrupt_rx.borrow().is_some() {
            request_token.cancel();
            return Err(anyhow!("request interrupted during inference"));
        }

        let attempt_index = 1_i64;
        let request_id = request.request_id.clone();
        let session_id = request.session_id.clone();
        let behavior_id = self.behavior.behavior_id.clone();
        let backend_id = lifecycle.backend_id().to_string();
        let model_name = self.behavior.model_name.clone();
        // The rendered-request capture scope is installed by `handle_request`,
        // outside every completion loop the request contains — including the
        // pre-request compaction summarizer, which runs before this function is
        // ever called. Do not install one here: a second scope would restart
        // the per-kind label sequence and hand the inference loop a label the
        // summarizer's scope had already used.
        let inference = Box::pin(async {
                let hook = DefraSessionHook::resume_or_create_with_identity_policy(
                    self.node.clone(),
                    &request.session_id,
                    &self.behavior.behavior_id,
                    self.behavior.agent_did(),
                    self.hook_failure_policy,
                )
                .await?
                .with_background_tool_registry(self.background_tool_registry.clone())
                .with_background_execution_registry(self.background_execution_registry.clone());
                hook.set_active_request_binding(
                    Some(request.request_id.clone()),
                    Some(request.doc_id.clone()),
                    request.requester_did.clone(),
                )
                .await;
                hook.set_request_deadline_at(request_deadline).await;
                hook.set_approval_required_tools(self.approval_required_tools.as_ref().clone())
                    .await;
                let persistence_hook = hook.clone();

                let model = (*self.model).clone();
                let mut loop_config = crate::completion_factory::loop_config_for_request(
                    &self.behavior,
                    self.preamble.clone(),
                    request,
                    aggregate_token_budget.clone(),
                    self.loop_tools.len(),
                )?;
                loop_config.deadline = request_deadline;
                let active_obligations = crate::agent::output_obligation::active_for_request(
                    self.output_obligations.as_ref(),
                    request.has_automated_trigger_lineage(),
                );
                if !active_obligations.is_empty() {
                    loop_config.output_obligation_gate = Some(
                        crate::agent::output_obligation::OutputObligationGate::new(
                            self.node.clone(),
                            request.doc_id.clone(),
                            active_obligations,
                        ),
                    );
                }
                let turn_compactor = self.compactor.clone();
                let turn_context_window = self.behavior.context_window;
                let turn_compaction_options = self.compaction_options_for_request(
                    request_deadline,
                    aggregate_token_budget,
                    effective_seed,
                );
                let turn_node = self.node.clone();
                let turn_request = request.clone();
                let turn_request_commit_cid = lifecycle
                    .request_commit_cid()
                    .context("claimed request has no exact commit CID for per-turn reduction")?
                    .to_string();
                let turn_compactor_callback = move |
                    compaction_request: crate::agent::loop_stream::TurnCompactionRequest,
                | -> std::pin::Pin<
                    Box<
                        dyn std::future::Future<
                                Output = anyhow::Result<
                                    crate::agent::loop_stream::TurnCompactionOutcome,
                                >,
                            > + Send,
                    >,
                > {
                    let compactor = turn_compactor.clone();
                    let mut options: CompactionOptions = turn_compaction_options.clone();
                    options.keep_recent_tokens =
                        options.keep_recent_tokens.min(compaction_request.keep_recent_target);
                    let node = turn_node.clone();
                    let request = turn_request.clone();
                    let request_commit_cid = turn_request_commit_cid.clone();
                    Box::pin(async move {
                        if crate::session::session_has_other_live_response(
                            node.as_ref(),
                            &request.session_id,
                            Some(&request.request_id),
                        )
                        .await?
                        {
                            anyhow::bail!(
                                "per-turn compaction refused while another response in the \
                                 session is streaming"
                            );
                        }
                        if !crate::compaction::has_unique_call_ids(&compaction_request.messages) {
                            anyhow::bail!(
                                "per-turn compaction refused because tool-call ids are not unique"
                            );
                        }
                        let source_boundary =
                            crate::provider_context_reduction::capture_source_boundary(
                                node.as_ref(),
                                &request.session_id,
                                &request.doc_id,
                                &request_commit_cid,
                            )
                            .await?;
                        let source_messages = compaction_request.messages;
                        let (result, producer_join) = admission::scope_call_with_join(
                            CallKind::Compaction,
                            1,
                            compactor.compact(
                                source_messages.clone(),
                                turn_context_window,
                                &options,
                            ),
                        )
                        .await;
                        let result = result?;
                        let producer_call = producer_join
                            .filter(|join| matches!(join.call_kind, CallKind::Compaction))
                            .map(|join| crate::provider_context_reduction::ProducerCallRef {
                                call_id: join.call_id,
                                call_seq: join.call_seq,
                            });
                        let (normalized_source, _) =
                            crate::compaction::provider_view(source_messages);
                        let split = usize::try_from(result.messages_compacted)
                            .unwrap_or(usize::MAX)
                            .min(normalized_source.len());
                        let (compacted_prefix, retained_suffix) = normalized_source.split_at(split);
                        if retained_suffix != result.messages.as_slice() {
                            anyhow::bail!(
                                "per-turn compaction retained suffix does not match its exact source split"
                            );
                        }

                        let summary = durable_reduction_summary(result.summary, split)?;
                        let mut provider_messages = result.messages;
                        if !summary.is_empty() {
                            provider_messages.insert(
                                0,
                                crate::prompt::LayeredPromptBuilder::system_reminder(
                                    &crate::prompt::continuation_checkpoint_reminder(&summary),
                                ),
                            );
                        }
                        let reduction_index = compaction_request.prior_reduction_keys.len() + 1;
                        let row = crate::provider_context_reduction::persist(
                            node.as_ref(),
                            crate::provider_context_reduction::NewProviderContextReduction {
                                agent_did: &request.agent_did,
                                requester_did: request.requester_did.as_deref(),
                                session_id: &request.session_id,
                                request_id: &request.request_id,
                                request_doc_id: &request.doc_id,
                                request_commit_cid: &request_commit_cid,
                                reduction_index,
                                turn_index: compaction_request.turn_index,
                                parent_reduction_key: compaction_request
                                    .prior_reduction_keys
                                    .last()
                                    .map(String::as_str),
                                producer_call: producer_call.as_ref(),
                                source_boundary: &source_boundary,
                                compacted_prefix,
                                retained_suffix,
                                checkpoint_messages: &provider_messages,
                                summary: &summary,
                                original_tokens: result.original_token_estimate,
                                compacted_tokens: result.compacted_token_estimate,
                            },
                        )
                        .await?;
                        Ok(crate::agent::loop_stream::TurnCompactionOutcome {
                            messages: provider_messages,
                            reduction_key: row.reduction_key,
                        })
                    })
                };
                loop_config.turn_compactor =
                    Some(std::sync::Arc::new(turn_compactor_callback));
                loop_config.context_message = request_context_message.clone();
                let restored = crate::provider_context_reduction::load_unconsumed_for_request(
                    self.node.as_ref(),
                    &request.doc_id,
                )
                .await?;
                let (loop_history, loop_prompt) = if let Some((row, lineage_keys)) = restored {
                    let mut messages = row.checkpoint_messages()?;
                    let prompt = messages.pop().context(
                        "durable provider-context checkpoint has no current prompt",
                    )?;
                    loop_config.context_message = None;
                    loop_config.active_reduction_keys = row.active_reduction_keys();
                    loop_config.reduction_chain_keys = lineage_keys;
                    loop_config.initial_turn_index = usize::try_from(row.turn_index)
                        .context("durable provider-context checkpoint has invalid turn index")?;
                    tracing::info!(
                        request_id = %request.request_id,
                        reduction_key = %row.reduction_key,
                        reduction_index = row.reduction_index,
                        "restored unconsumed durable provider-context checkpoint"
                    );
                    (messages, prompt)
                } else {
                    loop_config.reduction_chain_keys =
                        crate::provider_context_reduction::load_for_request(
                            self.node.as_ref(),
                            &request.doc_id,
                        )
                        .await?
                        .into_iter()
                        .map(|row| row.reduction_key)
                        .collect();
                    (
                        history.to_vec(),
                        crate::llm::message::Message::user(request.content.clone()),
                    )
                };
                let loop_tools = self.loop_tools.clone();
                let inference_token = request_token.child_token();
                let inference_token_for_start = inference_token.clone();
                let terminal_failure_reason = admission::terminal_failure_reason_observer();
                let hook_for_start_interrupt = persistence_hook.clone();
                let mut stream = admission::scope_call_with_token_and_failure_reason(
                    CallKind::Inference,
                    attempt_index,
                    inference_token.clone(),
                    terminal_failure_reason.clone(),
                    async {
                        tokio::select! {
                            biased;
                            _ = shutdown.changed() => {
                                Err(anyhow!("shutdown requested before inference stream started"))
                            }
                            _ = interrupt_rx.changed() => {
                                request_token.cancel();
                                inference_token_for_start.cancel();
                                if let Err(error) = hook_for_start_interrupt.cancel_in_flight_tool_calls().await {
                                    tracing::warn!(
                                        request_id = %request_id,
                                        session_id = %session_id,
                                        error = %error,
                                        "failed to cancel in-flight tool calls before inference stream started"
                                    );
                                }
                                Err(anyhow!("request interrupted during inference"))
                            }
                            stream = std::future::ready(Box::pin(crate::agent::loop_stream::run_loop_stream(
                                model,
                                Some(hook),
                                loop_prompt,
                                loop_history,
                                loop_tools,
                                loop_config,
                            ))) => Ok(stream)
                        }
                    },
                )
                .await?;

                admission::scope_call_with_token_and_failure_reason(
                    CallKind::Inference,
                    attempt_index,
                    inference_token.clone(),
                    terminal_failure_reason.clone(),
                    async {
                        let liveness_timeout = self.behavior.stream_liveness_timeout;

                        let mut processor = crate::agent::stream_processor::StreamProcessor::new(
                            &persistence_hook,
                            &self.stream_writer,
                            lifecycle,
                            doc_id,
                        );
                        let mut stream_error = None;
                        // A retry's backoff sleep runs *inside* the loop
                        // generator, spanning the next `stream.next()` poll. The
                        // liveness timeout wraps that poll, so a backoff longer
                        // than `liveness_timeout` would otherwise be misread as a
                        // dead stream and turned into a spurious terminal
                        // "stream liveness timeout", defeating the retry (#648).
                        // Carry the pending backoff forward and add it to the
                        // next poll's liveness budget.
                        let mut pending_backoff = std::time::Duration::ZERO;

                        loop {
                            let item = match tokio::select! {
                                biased;
                                _ = shutdown.changed() => {
                                    return Err(anyhow!("shutdown requested during inference stream"));
                                }
                                _ = interrupt_rx.changed() => {
                                    request_token.cancel();
                                    inference_token.cancel();
                                    if let Err(error) =
                                        persistence_hook.cancel_in_flight_tool_calls().await
                                    {
                                        tracing::warn!(
                                            request_id = %request_id,
                                            session_id = %session_id,
                                            error = %error,
                                            "failed to cancel in-flight tool calls during request interrupt"
                                        );
                                    }
                                    if let Err(error) = processor
                                        .persist_partial_turn("persist interrupted assistant turn")
                                        .await
                                    {
                                        tracing::warn!(
                                            request_id = %request_id,
                                            session_id = %session_id,
                                            error = %error,
                                            "failed to persist interrupted assistant turn before terminal transition"
                                        );
                                    }
                                    if let Err(error) = persistence_hook
                                        .backfill_completed_tool_results()
                                        .await
                                    {
                                        tracing::warn!(
                                            request_id = %request_id,
                                            session_id = %session_id,
                                            error = %error,
                                            "failed to backfill completed tool-result messages on interrupt"
                                        );
                                    }
                                    return Err(anyhow!("request interrupted during inference"));
                                }
                                result = await_with_request_deadline(
                                    request_deadline,
                                    crate::tool_call_lifecycle::runtime::scope_request_tool_execution_with_trigger_context(
                                        request_deadline,
                                        request_token.clone(),
                                        None,
                                        None,
                                        Some(session_id.clone()),
                                        trigger_correlation.clone(),
                                        trigger_context.source_fields.clone(),
                                        false,
                                        tokio::time::timeout(
                                            liveness_timeout.saturating_add(pending_backoff),
                                            stream.next(),
                                        ),
                                    ),
                                    "waiting for inference stream item",
                                ) => {
                                    match result {
                                        Ok(item) => item,
                                        Err(error) => {
                                            if let Err(sweep_error) =
                                                persistence_hook.timeout_expired_tool_calls().await
                                            {
                                                tracing::warn!(
                                                    request_id = %request_id,
                                                    session_id = %session_id,
                                                    error = %sweep_error,
                                                    "failed to sweep expired in-flight tool calls after request deadline"
                                                );
                                            }
                                            return Err(error);
                                        }
                                    }
                                }
                            } {
                                Ok(Some(item)) => item,
                                Ok(None) => break,
                                Err(_) => {
                                    if let Err(error) = persistence_hook
                                        .fail_in_flight_tool_calls(
                                            "stream liveness timeout while tool call was running",
                                            crate::tool_call_lifecycle::FailureClass::External,
                                        )
                                        .await
                                    {
                                        tracing::warn!(
                                            request_id = %request_id,
                                            session_id = %session_id,
                                            error = %error,
                                            "failed to mark in-flight tool calls failed after stream liveness timeout"
                                        );
                                    }
                                    let timeout_reason = format!(
                                        "stream liveness timeout: no data received for {}s",
                                        liveness_timeout.as_secs()
                                    );
                                    admission::set_terminal_failure_reason(
                                        &terminal_failure_reason,
                                        timeout_reason.clone(),
                                    );
                                    stream_error = Some(rig::agent::StreamingError::Completion(
                                        rig::completion::CompletionError::ProviderError(
                                            timeout_reason,
                                        ),
                                    ));
                                    break;
                                }
                            };
                            // The generator sleeps this backoff before its next
                            // yield, so extend the *next* poll's liveness budget
                            // by it (reset to zero for any non-retry item).
                            pending_backoff = match &item {
                                Ok(crate::agent::loop_stream::LoopStreamItem::AttemptFailed {
                                    backoff,
                                    will_retry: true,
                                    ..
                                })
                                | Ok(crate::agent::loop_stream::LoopStreamItem::TurnRetracted {
                                    backoff,
                                    ..
                                }) => *backoff,
                                _ => std::time::Duration::ZERO,
                            };
                            match processor.process_item(item).await {
                                Ok(crate::agent::stream_processor::StreamAction::Continue) => {}
                                Ok(crate::agent::stream_processor::StreamAction::Done) => break,
                                Ok(crate::agent::stream_processor::StreamAction::Error(error)) => {
                                    stream_error = Some(error);
                                    break;
                                }
                                Err(error) => return Err(error),
                            }
                        }

                        if let Some(error) = stream_error {
                            let _ = processor
                                .persist_partial_turn("persist errored assistant turn")
                                .await?;
                            if let Err(error) = persistence_hook
                                .backfill_completed_tool_results()
                                .await
                            {
                                tracing::warn!(
                                    request_id = %request_id,
                                    session_id = %session_id,
                                    error = %error,
                                    "failed to backfill completed tool-result messages after stream error"
                                );
                            }

                            let error_reason = format!("agent stream failed: {}", error);
                            self.stream_writer
                                .finalize_error(doc_id, &error_reason)
                                .await?;

                            return Ok(HandleRequestOutcome::FailedAfterResponse(anyhow!(
                                error_reason
                            )));
                        }

                        let mut streamed_text = std::mem::take(&mut processor.streamed_text);
                        let final_text = processor.final_text.take();

                        if let Some(text) = final_text.as_deref() {
                            if streamed_text.is_empty() {
                                let _ = self.stream_writer.write_tokens(doc_id, text).await?;
                                streamed_text.push_str(text);
                            } else if let Some(remainder) = text.strip_prefix(&streamed_text) {
                                if !remainder.is_empty() {
                                    let _ =
                                        self.stream_writer.write_tokens(doc_id, remainder).await?;
                                    streamed_text.push_str(remainder);
                                }
                            }
                        }

                        if !terminal_response_has_visible_output(
                            &streamed_text,
                            final_text.as_deref(),
                        ) {
                            let error_reason =
                                "agent stream completed without producing any visible response content";
                            self.stream_writer
                                .finalize_error(doc_id, error_reason)
                                .await?;

                            return Ok(HandleRequestOutcome::FailedAfterResponse(anyhow!(
                                error_reason
                            )));
                        }

                        ensure_request_deadline_open(
                            request_deadline,
                            "finalizing inference response",
                        )?;
                        self.stream_writer
                            .finalize(doc_id, StreamStatus::Complete)
                            .await?;

                        Ok(HandleRequestOutcome::Completed)
                    },
                )
                .await
            }
            .instrument(tracing::info_span!(
                "inference.attempt",
                request_id = %request_id,
                session_id = %session_id,
                agent_did = %request.agent_did,
                behavior_id = %behavior_id,
                backend_id = %backend_id,
                model_name = %model_name,
                deadline_at = %deadline_at,
                has_deadline,
                subagent_depth = request.subagent_depth,
                is_subagent = request.subagent_depth > 0
                    || request.caused_by_parent_request_id.is_some()
                    || request.caused_by_parent_tool_call_id.is_some(),
                workspace_cwd_set,
                attempt = attempt_index,
                retry_attempt = false,
            )));

        // The capture scope installed by `handle_request` spans both the
        // stream's construction and its drain loop: the SSE transports connect
        // lazily on first poll, so the HTTP send that the capturing transport
        // intercepts usually happens during polling.
        let outcome = crate::tool_call_lifecycle::runtime::scope_request_tool_execution_with_workspace_overlay(
            request_deadline,
            request_token.clone(),
            workspace,
            None,
            Some(session_id.clone()),
            trigger_correlation.clone(),
            trigger_context.source_fields.clone(),
            false,
            inference,
        )
        .await?;

        Ok(outcome)
    }

    pub(super) async fn write_error_response(
        &self,
        request: &crate::watcher::AgentRequest,
        behavior_id: &str,
        error: &anyhow::Error,
    ) -> Result<()> {
        let doc_id = self
            .stream_writer
            .begin_with_requester_did(
                &request.session_id,
                &request.request_id,
                Some(&request.doc_id),
                behavior_id,
                request.requester_did.as_deref(),
            )
            .await?;
        let error_reason = error.to_string();
        let error_text = format!("Error: {}", error_reason);
        let _ = self
            .stream_writer
            .write_tokens(&doc_id, &error_text)
            .await?;
        self.stream_writer
            .finalize_error(&doc_id, &error_reason)
            .await?;
        Ok(())
    }
}

fn durable_reduction_summary(
    summary: Option<String>,
    compacted_prefix_len: usize,
) -> Result<String> {
    let summary = summary.map(|summary| summary.trim().to_string());
    if compacted_prefix_len > 0 && summary.as_deref().is_none_or(str::is_empty) {
        anyhow::bail!("per-turn compaction removed a provider prefix without a non-empty summary");
    }
    Ok(summary.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::{
        assemble_request_context_message, await_with_request_deadline, durable_reduction_summary,
        ensure_request_deadline_open, request_deadline_remaining,
        terminal_response_has_visible_output, BehaviorDaemon,
    };
    use crate::agent::completion_retry::CompletionRetryProfileFields;
    use crate::agent::runtime::StartupBarrier;
    use crate::backend_provider::BackendProviderKind;
    use crate::compaction::CompactionStrategy;
    use crate::config::{AgentBehavior, SamplingConfig};
    use crate::hook::{BackgroundExecutionRegistry, BackgroundToolRegistry, FailurePolicy};
    use crate::identity::{AgentIdentity, AgentPrincipal, KeyIdentity};
    use crate::llm::tool::ToolDyn;
    use crate::prompt::LayeredPromptBuilder;
    use crate::tool_surface::BehaviorToolConfig;
    use crate::watcher::AgentRequest;
    use futures::stream;
    use rig::completion::{
        CompletionError, CompletionModel, CompletionRequest, CompletionResponse,
    };
    use rig::streaming::{RawStreamingChoice, StreamingCompletionResponse};
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn durable_reduction_requires_a_nonempty_summary_for_a_removed_prefix() {
        assert!(durable_reduction_summary(Some("   \n".to_string()), 1).is_err());
        assert!(durable_reduction_summary(None, 1).is_err());
        assert_eq!(
            durable_reduction_summary(Some("  checkpoint  ".to_string()), 1).unwrap(),
            "checkpoint"
        );
        assert_eq!(durable_reduction_summary(None, 0).unwrap(), "");
    }

    #[derive(Clone)]
    struct RoutedReplyModel;

    #[allow(refining_impl_trait)]
    impl CompletionModel for RoutedReplyModel {
        type Response = ();
        type StreamingResponse = ();
        type Client = ();

        fn make(_: &Self::Client, _: impl Into<String>) -> Self {
            Self
        }

        async fn completion(
            &self,
            _request: CompletionRequest,
        ) -> Result<CompletionResponse<Self::Response>, CompletionError> {
            Err(CompletionError::ProviderError(
                "completion is unused in daemon lineage test".to_string(),
            ))
        }

        async fn stream(
            &self,
            _request: CompletionRequest,
        ) -> Result<StreamingCompletionResponse<Self::StreamingResponse>, CompletionError> {
            let items = vec![
                Ok(RawStreamingChoice::Message("routed reply".to_string())),
                Ok(RawStreamingChoice::FinalResponse(())),
            ];
            let inner: rig::streaming::StreamingResult<()> = Box::pin(stream::iter(items));
            Ok(StreamingCompletionResponse::stream(inner))
        }
    }

    fn test_behavior() -> Arc<AgentBehavior> {
        let identity: Arc<dyn AgentIdentity> = Arc::new(
            KeyIdentity::load_or_create(
                std::env::temp_dir().join(format!("daemon-lineage-{}.key", uuid::Uuid::new_v4())),
                None,
            )
            .expect("test identity"),
        );
        let principal = Arc::new(AgentPrincipal {
            agent_did: identity.did().to_string(),
            identity,
            default_behavior_id: "general".to_string(),
            display_name: None,
            enabled: true,
        });

        Arc::new(AgentBehavior {
            behavior_id: "general".to_string(),
            principal,
            backend_id: Some("backend-general".to_string()),
            backend_provider_kind: BackendProviderKind::OpenAiCompatible,
            openai_wire_api: crate::OpenAiWireApi::ChatCompletions,
            backend_endpoint: "http://127.0.0.1:8999/v1".to_string(),
            backend_api_key: None,
            backend_api_key_env_var: None,
            model_name: "scripted".to_string(),
            context_window: 8_192,
            max_output_tokens: 1_024,
            max_turns: 2,
            system_prompt: "system".to_string(),
            request_context_template: None,
            tools: BehaviorToolConfig::meta_only(),
            compaction_threshold: 0.75,
            compaction_strategy: CompactionStrategy::StripThenSummarize,
            stream_batch_ms: 0,
            stream_liveness_timeout: Duration::from_secs(5),
            deadline_duration: Duration::from_secs(30),
            completion_retry: CompletionRetryProfileFields::default(),
            sampling: SamplingConfig::default(),
            skills: Vec::new(),
        })
    }

    async fn create_routed_request(
        node: &defra_node::EmbeddedNode,
        behavior: &AgentBehavior,
        requester_did: &str,
    ) -> AgentRequest {
        let request_id = uuid::Uuid::new_v4().to_string();
        let session_id = uuid::Uuid::new_v4().to_string();
        let created_at = chrono::Utc::now().to_rfc3339();
        let escaped_request_id = crate::graphql::escape_graphql_string(&request_id);
        let escaped_session_id = crate::graphql::escape_graphql_string(&session_id);
        let escaped_agent_did = crate::graphql::escape_graphql_string(behavior.agent_did());
        let escaped_requester_did = crate::graphql::escape_graphql_string(requester_did);
        let mutation = format!(
            r#"mutation {{
                create_AgentRequest(input: {{
                    request_id: "{escaped_request_id}",
                    agent_did: "{escaped_agent_did}",
                    requester_did: "{escaped_requester_did}",
                    behavior_id: "general",
                    session_id: "{escaped_session_id}",
                    retry_parent_request: "",
                    retry_root_request: "{escaped_request_id}",
                    superseded_by_request: "",
                    content: "route this reply",
                    status: "pending",
                    lifecycle_state: "pending",
                    backend_id: "backend-general",
                    execution_origin: "interactive",
                    failure_reason: "",
                    created_at: "{created_at}",
                    retry_count: 0,
                    max_retries: 3,
                    subagent_depth: 1,
                    caused_by_parent_request_id: "parent-request",
                    caused_by_parent_request_doc_id: "parent-request-doc",
                    caused_by_parent_tool_call_id: "parent-tool-call",
                    caused_by_parent_tool_call_doc_id: "parent-tool-call-doc"
                }}) {{ _docID }}
            }}"#
        );
        let response = node.execute(&mutation).await;
        assert!(
            !response.has_errors(),
            "create routed AgentRequest failed: {:?}",
            response.errors
        );
        let doc_id = response
            .data
            .as_ref()
            .and_then(|data| data.get("create_AgentRequest"))
            .and_then(|value| value.get("_docID"))
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned);
        let doc_id = match doc_id {
            Some(doc_id) => doc_id,
            None => {
                let query = format!(
                    r#"{{
                        AgentRequest(
                            filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                            limit: 1
                        ) {{ _docID }}
                    }}"#
                );
                let response = node.execute(&query).await;
                assert!(
                    !response.has_errors(),
                    "query created AgentRequest failed: {:?}",
                    response.errors
                );
                response
                    .data
                    .as_ref()
                    .and_then(|data| data.get("AgentRequest"))
                    .and_then(serde_json::Value::as_array)
                    .and_then(|rows| rows.first())
                    .and_then(|row| row.get("_docID"))
                    .and_then(serde_json::Value::as_str)
                    .expect("created request _docID")
                    .to_string()
            }
        };

        AgentRequest {
            doc_id,
            request_id,
            agent_did: behavior.agent_did().to_string(),
            requester_did: Some(requester_did.to_string()),
            behavior_id: Some(behavior.behavior_id.clone()),
            session_id,
            content: "route this reply".to_string(),
            temperature: None,
            top_p: None,
            top_k: None,
            seed: None,
            max_tokens: None,
            max_total_tokens: None,
            metadata: None,
            execution_origin: Some("interactive".to_string()),
            created_at,
            deadline: None,
            subagent_depth: 1,
            caused_by_parent_request_id: Some("parent-request".to_string()),
            caused_by_parent_request_doc_id: Some("parent-request-doc".to_string()),
            caused_by_parent_tool_call_id: Some("parent-tool-call".to_string()),
            caused_by_parent_tool_call_doc_id: Some("parent-tool-call-doc".to_string()),
            caused_by_trigger_id: None,
            caused_by_trigger_kind: None,
            caused_by_source_doc_id: None,
            caused_by_correlation: None,
            caused_by_trigger_context: None,
            workspace_id: None,
            workspace_authority: None,
            workspace_owner_deployment_id: None,
            workspace_seal_hash: None,
        }
    }

    #[test]
    fn render_request_context_message_uses_frozen_agents_not_live_tree() {
        let manifest = crate::workspace::InstructionManifest::new(
            "abc",
            vec![crate::workspace::InstructionFile::from_bytes(
                "AGENTS.md",
                b"frozen-base-instructions\n",
            )],
        );
        let message = assemble_request_context_message(None, Some(&manifest.to_json_string()))
            .expect("context");
        let encoded = serde_json::to_string(&message).expect("serialize");
        assert!(encoded.contains("frozen-base-instructions"));
        assert!(!encoded.contains("live-writer-instructions"));
        assert!(encoded.contains("<context>"));
    }

    #[test]
    fn terminal_response_requires_visible_output() {
        assert!(!terminal_response_has_visible_output("", None));
        assert!(!terminal_response_has_visible_output("   ", Some("")));
        assert!(!terminal_response_has_visible_output("", Some("   ")));
        assert!(terminal_response_has_visible_output("hello", None));
        assert!(terminal_response_has_visible_output("", Some("hello")));
    }

    #[test]
    fn request_deadline_remaining_reports_expired_deadline() {
        let deadline = chrono::Utc::now() - chrono::Duration::milliseconds(1);

        assert_eq!(
            request_deadline_remaining(Some(deadline)),
            Some(Duration::ZERO)
        );
        assert!(ensure_request_deadline_open(Some(deadline), "test").is_err());
    }

    #[tokio::test]
    async fn await_with_request_deadline_bounds_waits() {
        let deadline = chrono::Utc::now() + chrono::Duration::milliseconds(10);

        let result = await_with_request_deadline(
            Some(deadline),
            tokio::time::sleep(Duration::from_secs(5)),
            "test wait",
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn await_with_request_deadline_allows_fast_work() {
        let deadline = chrono::Utc::now() + chrono::Duration::seconds(1);

        let result = await_with_request_deadline(Some(deadline), async { 42 }, "test wait")
            .await
            .expect("fast work should finish before deadline");

        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn daemon_request_path_stamps_requester_lineage_on_hook_messages() {
        let data_path =
            std::env::temp_dir().join(format!("daemon-requester-lineage-{}", uuid::Uuid::new_v4()));
        let node = Arc::new(
            defra_node::EmbeddedNode::builder()
                .data_path(&data_path)
                .build()
                .await
                .expect("embedded node"),
        );
        crate::ensure_runtime_schemas(node.as_ref())
            .await
            .expect("runtime schemas");

        let requester_did = "did:test:coordinator";
        let behavior = test_behavior();
        let request = create_routed_request(node.as_ref(), &behavior, requester_did).await;
        let prompt_builder = LayeredPromptBuilder::for_behavior(
            &behavior.system_prompt,
            &behavior.behavior_id,
            &[],
            false,
            behavior.context_window,
            behavior.max_output_tokens,
            &[],
        );
        let preamble = prompt_builder.preamble().to_string();
        let loop_tools: Arc<Vec<Box<dyn ToolDyn>>> = Arc::new(Vec::new());
        let mut daemon = BehaviorDaemon::new(
            node.clone(),
            behavior,
            Arc::new(RoutedReplyModel),
            preamble,
            loop_tools,
            prompt_builder,
            FailurePolicy::default(),
            None,
            BackgroundToolRegistry::default(),
            BackgroundExecutionRegistry::default(),
            Arc::new(StartupBarrier::ready_for_test()),
            Arc::new(crate::startup_readiness::StartupDemotions::new()),
        );
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        daemon.process_request(request.clone(), shutdown_rx).await;

        let escaped_session_id = crate::graphql::escape_graphql_string(&request.session_id);
        let query = format!(
            r#"{{
                AgentMessage(
                    filter: {{ session_id: {{ _eq: "{escaped_session_id}" }} }}
                ) {{
                    role
                    content
                    requester_did
                }}
            }}"#
        );
        let response = node.execute(&query).await;
        assert!(
            !response.has_errors(),
            "query routed AgentMessage failed: {:?}",
            response.errors
        );
        let rows = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentMessage"))
            .and_then(serde_json::Value::as_array)
            .expect("AgentMessage rows");
        assert!(
            rows.iter().any(|row| {
                row.get("role").and_then(serde_json::Value::as_str) == Some("assistant")
                    && row
                        .get("content")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|content| content.contains("routed reply"))
                    && row.get("requester_did").and_then(serde_json::Value::as_str)
                        == Some(requester_did)
            }),
            "daemon-persisted assistant message must carry requester lineage; rows={rows:?}"
        );

        node.shutdown().await;
        let _ = std::fs::remove_dir_all(data_path);
    }
}
