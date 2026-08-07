use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use rig::completion::CompletionModel;
use tokio::sync::{mpsc, Mutex};
use tracing::Instrument;

mod inference;
mod request;
mod title;

use super::runtime::StartupBarrier;
use crate::compaction::{CompactionOptions, DefraCompactor};
use crate::config::AgentBehavior;
use crate::hook::FailurePolicy;
use crate::lifecycle::{ClaimOutcome, RequestLifecycle};
use crate::prompt::LayeredPromptBuilder;
use crate::runtime_trace::{
    record_current_claim_outcome, record_current_failure_class, record_current_request_outcome,
    RequestTraceAttrs,
};
use crate::streaming::DefraStreamWriter;
use crate::watcher::AgentRequest;

async fn finalize_request_failure(
    lifecycle: &mut RequestLifecycle,
    reason: &str,
    request_id: &str,
) {
    if let Err(error) = lifecycle.fail_with_reason(reason).await {
        tracing::error!(
            request_id,
            error = %error,
            "failed to transition request to failed after bounded retries; durable repair remains pending"
        );
    }
}

pub(super) struct BehaviorDaemon<M: CompletionModel> {
    node: Arc<defra_node::EmbeddedNode>,
    behavior: Arc<AgentBehavior>,
    model: Arc<M>,
    preamble: String,
    loop_tools: Arc<Vec<Box<dyn crate::llm::tool::ToolDyn>>>,
    prompt_builder: LayeredPromptBuilder,
    stream_writer: DefraStreamWriter,
    compactor: DefraCompactor<M>,
    compaction_options: CompactionOptions,
    hook_failure_policy: FailurePolicy,
    rendered_request_capture_factory:
        Option<crate::rendered_request::RenderedRequestCaptureFactory>,
    background_tool_registry: crate::hook::BackgroundToolRegistry,
    background_execution_registry: crate::hook::BackgroundExecutionRegistry,
    approval_required_tools: Arc<Vec<String>>,
    startup_barrier: Arc<StartupBarrier>,
    startup_demotions: Arc<crate::startup_readiness::StartupDemotions>,
}

enum HandleRequestOutcome {
    Completed,
    FailedAfterResponse(anyhow::Error),
    Interrupted,
}

impl<M: CompletionModel + 'static> BehaviorDaemon<M> {
    pub(super) fn new(
        node: Arc<defra_node::EmbeddedNode>,
        behavior: Arc<AgentBehavior>,
        model: Arc<M>,
        preamble: String,
        loop_tools: Arc<Vec<Box<dyn crate::llm::tool::ToolDyn>>>,
        prompt_builder: LayeredPromptBuilder,
        hook_failure_policy: FailurePolicy,
        rendered_request_capture_factory: Option<
            crate::rendered_request::RenderedRequestCaptureFactory,
        >,
        background_tool_registry: crate::hook::BackgroundToolRegistry,
        background_execution_registry: crate::hook::BackgroundExecutionRegistry,
        startup_barrier: Arc<StartupBarrier>,
        startup_demotions: Arc<crate::startup_readiness::StartupDemotions>,
    ) -> Self {
        let stream_writer = DefraStreamWriter::new(
            node.clone(),
            behavior.agent_did(),
            Duration::from_millis(behavior.stream_batch_ms),
        );
        let mut compaction_config = crate::completion_factory::loop_config(
            behavior.as_ref(),
            preamble.clone(),
            0,
            crate::rendered_request::CaptureScopeKind::Compaction,
        );
        compaction_config.max_turns = 0;
        let compactor = DefraCompactor::new(model.clone(), compaction_config);
        let compaction_options = CompactionOptions {
            threshold: behavior.compaction_threshold,
            strategy: behavior.compaction_strategy.clone(),
            ..Default::default()
        };

        Self {
            node,
            behavior,
            model,
            preamble,
            loop_tools,
            prompt_builder,
            stream_writer,
            compactor,
            compaction_options,
            hook_failure_policy,
            rendered_request_capture_factory,
            background_tool_registry,
            background_execution_registry,
            approval_required_tools: Arc::new(Vec::new()),
            startup_barrier,
            startup_demotions,
        }
    }

    /// Request-scoped compaction options: the daemon-lifetime knobs plus the
    /// claimed deadline of the request this compaction serves. The deadline is
    /// a required argument so no call site can omit it — the compactor's
    /// stored config is daemon-lifetime and carries no deadline, so this is
    /// the only path by which compaction recovery becomes deadline-aware
    /// (#1016). Both entry points force summarization: they fire only after
    /// the caller has established the assembled input is over budget.
    pub(super) fn compaction_options_for_request(
        &self,
        deadline: Option<chrono::DateTime<chrono::Utc>>,
        aggregate_token_budget: Option<crate::agent::loop_stream::AggregateTokenBudget>,
    ) -> CompactionOptions {
        CompactionOptions {
            force_summarize: true,
            deadline,
            aggregate_token_budget,
            ..self.compaction_options.clone()
        }
    }

    pub(super) fn with_approval_required_tools(mut self, tools: Vec<String>) -> Self {
        self.approval_required_tools = Arc::new(tools);
        self
    }

    pub(super) async fn run(
        &mut self,
        request_rx: Arc<Mutex<mpsc::Receiver<AgentRequest>>>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        tracing::info!(
            behavior_id = %self.behavior.behavior_id,
            did = %self.behavior.agent_did(),
            model = %self.behavior.model_name,
            context_window = self.behavior.context_window,
            "gents behavior started"
        );

        self.startup_barrier
            .mark_behavior_ready(&self.behavior.behavior_id)
            .await;
        // A successful start supersedes any demotion that raced it: the
        // behavior is serving, so the ledger entry (which would make the
        // router reject its requests) must not survive.
        self.startup_demotions.clear(&self.behavior.behavior_id);
        tracing::info!(
            behavior_id = %self.behavior.behavior_id,
            did = %self.behavior.agent_did(),
            "gents behavior executor online"
        );

        loop {
            let request = tokio::select! {
                biased;

                _ = shutdown.changed() => {
                    tracing::info!(behavior_id = %self.behavior.behavior_id, "shutdown signal received");
                    return Ok(());
                }

                req = async {
                    let mut receiver = request_rx.lock().await;
                    receiver.recv().await
                } => {
                    match req {
                        Some(req) => req,
                        None => return Ok(()),
                    }
                }
            };

            let trace_attrs = RequestTraceAttrs::from_request(&request);
            let behavior_id = self.behavior.behavior_id.clone();
            let backend_id = self.behavior.backend_id.clone().unwrap_or_default();
            let execution_origin = crate::lifecycle::ExecutionOrigin::from_persisted(
                request.execution_origin.as_deref(),
            );

            self.process_request(request, shutdown.clone())
                .instrument(tracing::info_span!(
                    "agent.request",
                    request_doc_id = %trace_attrs.request_doc_id,
                    request_id = %trace_attrs.request_id,
                    session_id = %trace_attrs.session_id,
                    agent_did = %trace_attrs.agent_did,
                    behavior_id = %behavior_id,
                    requested_behavior_id = %trace_attrs.requested_behavior_id,
                    backend_id = %backend_id,
                    execution_origin = %execution_origin.as_str(),
                    persisted_execution_origin = %trace_attrs.execution_origin,
                    deadline_at = %trace_attrs.deadline_at,
                    has_deadline = trace_attrs.has_deadline,
                    subagent_depth = trace_attrs.subagent_depth,
                    is_subagent = trace_attrs.is_subagent,
                    parent_request_id = %trace_attrs.parent_request_id,
                    parent_tool_call_id = %trace_attrs.parent_tool_call_id,
                    selected_skill_count = trace_attrs.selected_skill_count,
                    workspace_cwd_set = trace_attrs.workspace_cwd_set,
                    claim_outcome = tracing::field::Empty,
                    request_outcome = tracing::field::Empty,
                    failure_class = tracing::field::Empty,
                ))
                .await;
        }
    }

    async fn process_request(
        &mut self,
        request: AgentRequest,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        let mut lifecycle = RequestLifecycle::new_with_execution_binding(
            self.node.clone(),
            &self.behavior.behavior_id,
            self.behavior.agent_did(),
            request.clone(),
            self.behavior.deadline_duration.as_secs(),
            crate::lifecycle::ExecutionOrigin::from_persisted(request.execution_origin.as_deref()),
            self.behavior.backend_id.clone().unwrap_or_default(),
        );

        let claim_result = lifecycle
            .claim_with_identity()
            .instrument(tracing::info_span!(
                "request.claim",
                request_id = %request.request_id,
                session_id = %request.session_id,
                agent_did = %request.agent_did,
                behavior_id = %self.behavior.behavior_id,
            ))
            .await;

        match claim_result {
            Ok(ClaimOutcome::Claimed) => {
                record_current_claim_outcome("claimed");
            }
            Ok(ClaimOutcome::Queued) => {
                record_current_claim_outcome("queued");
                record_current_request_outcome("queued");
                tracing::info!(
                    behavior_id = %self.behavior.behavior_id,
                    request_id = %request.request_id,
                    session_id = %request.session_id,
                    "request queued behind an earlier same-session request"
                );
                return;
            }
            Ok(ClaimOutcome::Interrupted) => {
                record_current_claim_outcome("interrupted");
                record_current_request_outcome("interrupted_pre_claim");
                tracing::info!(
                    behavior_id = %self.behavior.behavior_id,
                    request_id = %request.request_id,
                    session_id = %request.session_id,
                    cancellation_source = "pre_claim",
                    "request interrupted before claim"
                );
                return;
            }
            Ok(ClaimOutcome::Expired) => {
                record_current_claim_outcome("expired");
                record_current_request_outcome("expired_pre_claim");
                tracing::info!(
                    behavior_id = %self.behavior.behavior_id,
                    request_id = %request.request_id,
                    session_id = %request.session_id,
                    cancellation_source = "stale_ttl",
                    "request expired (valid_until passed) before claim; marked dead"
                );
                return;
            }
            Err(error) => {
                record_current_claim_outcome("error");
                record_current_request_outcome("claim_error");
                record_current_failure_class(&error);
                tracing::warn!(
                    behavior_id = %self.behavior.behavior_id,
                    request_id = %request.request_id,
                    error = %error,
                    "failed to claim request"
                );
                return;
            }
        }

        if let Some(requested_behavior_id) = request
            .behavior_id
            .as_deref()
            .map(str::trim)
            .filter(|behavior_id| !behavior_id.is_empty())
        {
            if requested_behavior_id != self.behavior.behavior_id {
                let error = anyhow::anyhow!(
                    "request targets behavior {} but runtime is serving behavior {}",
                    requested_behavior_id,
                    self.behavior.behavior_id
                );
                record_current_request_outcome("rejected_behavior_mismatch");
                record_current_failure_class(&error);
                tracing::warn!(
                    behavior_id = %self.behavior.behavior_id,
                    request_id = %request.request_id,
                    session_id = %request.session_id,
                    requested_behavior_id = %requested_behavior_id,
                    "rejecting request for unroutable behavior"
                );
                let response_exists = lifecycle.response_exists().await.unwrap_or(false);
                let response_written = if response_exists {
                    self.stream_writer
                        .finalize_existing_request_error(&request.request_id, &error.to_string())
                        .await
                } else {
                    self.write_error_response(&request, lifecycle.behavior_id(), &error)
                        .await
                        .map(|_| true)
                };
                if let Err(stream_error) = response_written {
                    tracing::error!(
                        behavior_id = %self.behavior.behavior_id,
                        error = %stream_error,
                        "failed to write behavior-mismatch response"
                    );
                }
                finalize_request_failure(&mut lifecycle, &error.to_string(), &request.request_id)
                    .await;
                return;
            }
        }

        if let Err(error) = lifecycle
            .prepare_session_with_identity()
            .instrument(tracing::info_span!(
                "request.prepare_session",
                request_id = %request.request_id,
                session_id = %request.session_id,
                agent_did = %request.agent_did,
                behavior_id = %lifecycle.behavior_id(),
            ))
            .await
        {
            record_current_request_outcome("session_prepare_error");
            record_current_failure_class(&error);
            tracing::error!(
                behavior_id = %self.behavior.behavior_id,
                request_id = %request.request_id,
                session_id = %request.session_id,
                behavior_id = %lifecycle.behavior_id(),
                error = %error,
                "failed to prepare behavior-pinned session"
            );
            let response_exists = lifecycle.response_exists().await.unwrap_or(false);
            let response_written = if response_exists {
                self.stream_writer
                    .finalize_existing_request_error(&request.request_id, &error.to_string())
                    .await
            } else {
                self.write_error_response(&request, lifecycle.behavior_id(), &error)
                    .await
                    .map(|_| true)
            };
            if let Err(stream_error) = response_written {
                tracing::error!(
                    behavior_id = %self.behavior.behavior_id,
                    error = %stream_error,
                    "failed to write session-preparation response"
                );
            }
            finalize_request_failure(&mut lifecycle, &error.to_string(), &request.request_id).await;
            return;
        }

        let (interrupt_tx, interrupt_rx) =
            tokio::sync::watch::channel::<Option<crate::interrupt::InterruptIntent>>(None);
        let observer = crate::interrupt::spawn_request_interrupt_observer(
            self.node.clone(),
            request.doc_id.clone(),
            interrupt_tx,
            shutdown.clone(),
        );

        let result = self
            .handle_request(&mut lifecycle, shutdown, interrupt_rx)
            .await;
        observer.abort();

        match result {
            Ok(HandleRequestOutcome::Completed) => {
                record_current_request_outcome("completed");
                if let Err(error) = lifecycle.complete().await {
                    record_current_failure_class(&error);
                    tracing::error!(
                        request_id = %request.request_id,
                        error = %error,
                        "failed to persist completed request after bounded retries; durable repair remains pending"
                    );
                }
            }
            Ok(HandleRequestOutcome::Interrupted) => {
                record_current_request_outcome("interrupted");
                tracing::info!(
                    behavior_id = %self.behavior.behavior_id,
                    request_id = %request.request_id,
                    session_id = %request.session_id,
                    cancellation_source = "mid_flight",
                    "request interrupted mid-flight"
                );
            }
            Ok(HandleRequestOutcome::FailedAfterResponse(error)) => {
                record_current_request_outcome("failed_after_response");
                record_current_failure_class(&error);
                tracing::error!(
                    behavior_id = %self.behavior.behavior_id,
                    request_id = %request.request_id,
                    error = %error,
                    "request failed after response started"
                );
                finalize_request_failure(&mut lifecycle, &error.to_string(), &request.request_id)
                    .await;
            }
            Err(error) => {
                record_current_request_outcome("failed");
                record_current_failure_class(&error);
                tracing::error!(
                    behavior_id = %self.behavior.behavior_id,
                    request_id = %request.request_id,
                    error = %error,
                    "request handling failed"
                );
                let response_exists = lifecycle.response_exists().await.unwrap_or(false);
                let response_written = if response_exists {
                    self.stream_writer
                        .finalize_existing_request_error(&request.request_id, &error.to_string())
                        .await
                } else {
                    self.write_error_response(&request, lifecycle.behavior_id(), &error)
                        .await
                        .map(|_| true)
                };
                if let Err(stream_error) = response_written {
                    tracing::error!(
                        behavior_id = %self.behavior.behavior_id,
                        error = %stream_error,
                        "failed to write error response"
                    );
                }
                finalize_request_failure(&mut lifecycle, &error.to_string(), &request.request_id)
                    .await;
            }
        }
    }
}
