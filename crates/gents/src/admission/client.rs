use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rig::client::CompletionClient;
use rig::completion::{CompletionError, CompletionModel, CompletionRequest, CompletionResponse};
use rig::streaming::StreamingCompletionResponse;
use tokio_util::sync::CancellationToken;

use super::controller::PendingCallMetadata;
use super::provenance::RunningInferenceCallProvenance;
use super::stream_guard::hold_stream_guard_with_running_call;
use super::AdmissionRegistry;
use crate::watcher::AgentRequest;

const CANCELLED_BY_INTERRUPT_MSG: &str = "inference cancelled by request interrupt";

#[derive(Clone)]
pub(crate) struct AdmittedCompletionClient<C> {
    inner: C,
    admission: AdmissionRegistry,
}

impl<C> AdmittedCompletionClient<C> {
    pub(crate) fn new(inner: C, admission: AdmissionRegistry) -> Self {
        Self { inner, admission }
    }
}

impl<C> CompletionClient for AdmittedCompletionClient<C>
where
    C: CompletionClient,
    C::CompletionModel: 'static,
    <C::CompletionModel as CompletionModel>::Response: 'static,
    <C::CompletionModel as CompletionModel>::StreamingResponse: 'static,
{
    type CompletionModel = AdmittedCompletionModel<C::CompletionModel>;
}

#[derive(Clone)]
pub(crate) struct AdmittedCompletionModel<M> {
    inner: M,
    admission: AdmissionRegistry,
}

impl<M> CompletionModel for AdmittedCompletionModel<M>
where
    M: CompletionModel + 'static,
    M::Response: 'static,
    M::StreamingResponse: 'static,
{
    type Response = M::Response;
    type StreamingResponse = M::StreamingResponse;
    type Client = AdmittedCompletionClient<M::Client>;

    fn make(client: &Self::Client, model: impl Into<String>) -> Self {
        Self {
            inner: M::make(&client.inner, model),
            admission: client.admission.clone(),
        }
    }

    async fn completion(
        &self,
        request: CompletionRequest,
    ) -> Result<CompletionResponse<Self::Response>, CompletionError> {
        let mut permit = self.admission.acquire_current_call().await?;
        let running_call = permit.running_call_provenance().clone();
        let token = current_context().ok().and_then(|c| c.inference_token);
        match token {
            Some(token) => {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => {
                        permit.mark_interrupted();
                        Err(CompletionError::ProviderError(CANCELLED_BY_INTERRUPT_MSG.into()))
                    }
                    result = scope_running_call(running_call, self.inner.completion(request)) => match result {
                        Ok(response) => {
                            permit.require_rendered_request_binding().await?;
                            permit.finish_success(Some(response.usage)).await;
                            Ok(response)
                        }
                        Err(error) => {
                            permit.finish_failure(&error.to_string()).await;
                            Err(error)
                        }
                    }
                }
            }
            None => match scope_running_call(running_call, self.inner.completion(request)).await {
                Ok(response) => {
                    permit.require_rendered_request_binding().await?;
                    permit.finish_success(Some(response.usage)).await;
                    Ok(response)
                }
                Err(error) => {
                    permit.finish_failure(&error.to_string()).await;
                    Err(error)
                }
            },
        }
    }

    async fn stream(
        &self,
        request: CompletionRequest,
    ) -> Result<StreamingCompletionResponse<Self::StreamingResponse>, CompletionError> {
        let mut permit = self.admission.acquire_current_call().await?;
        let running_call = permit.running_call_provenance().clone();
        let token = current_context().ok().and_then(|c| c.inference_token);
        match token {
            Some(token) => {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => {
                        permit.mark_interrupted();
                        Err(CompletionError::ProviderError(CANCELLED_BY_INTERRUPT_MSG.into()))
                    }
                    result = scope_running_call(running_call.clone(), self.inner.stream(request)) => match result {
                        Ok(stream) => {
                            Ok(hold_stream_guard_with_running_call(stream, permit, running_call))
                        }
                        Err(error) => {
                            permit.finish_failure(&error.to_string()).await;
                            Err(error)
                        }
                    }
                }
            }
            None => {
                match scope_running_call(running_call.clone(), self.inner.stream(request)).await {
                    Ok(stream) => Ok(hold_stream_guard_with_running_call(
                        stream,
                        permit,
                        running_call,
                    )),
                    Err(error) => {
                        permit.finish_failure(&error.to_string()).await;
                        Err(error)
                    }
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CallKind {
    Inference,
    Compaction,
    #[cfg_attr(not(test), allow(dead_code))]
    Scheduled,
    OneOff,
}

impl CallKind {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Inference => "inference",
            Self::Compaction => "compaction",
            Self::Scheduled => "scheduled",
            Self::OneOff => "oneoff",
        }
    }
}

#[derive(Clone)]
pub(crate) struct AdmissionCallContext {
    pub(super) request_id: String,
    pub(super) backend_id: String,
    pub(super) behavior_id: String,
    pub(super) agent_did: String,
    pub(super) session_id: String,
    pub(super) call_kind: CallKind,
    pub(super) attempt: i64,
    pub(super) call_seq: Arc<AtomicU64>,
    pub(super) inference_token: Option<CancellationToken>,
    pub(super) terminal_failure_reason: Option<TerminalFailureReasonObserver>,
}

impl AdmissionCallContext {
    pub(crate) fn for_oneoff(
        request_id: impl Into<String>,
        session_id: impl Into<String>,
        behavior_id: impl Into<String>,
        backend_id: impl Into<String>,
        agent_did: impl Into<String>,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            backend_id: backend_id.into(),
            behavior_id: behavior_id.into(),
            agent_did: agent_did.into(),
            session_id: session_id.into(),
            call_kind: CallKind::OneOff,
            attempt: 1,
            call_seq: Arc::new(AtomicU64::new(0)),
            inference_token: None,
            terminal_failure_reason: None,
        }
    }

    pub(crate) fn for_request(
        request: &AgentRequest,
        behavior_id: impl Into<String>,
        backend_id: impl Into<String>,
    ) -> Self {
        Self {
            request_id: request.request_id.clone(),
            backend_id: backend_id.into(),
            behavior_id: behavior_id.into(),
            agent_did: request.agent_did.clone(),
            session_id: request.session_id.clone(),
            call_kind: CallKind::Inference,
            attempt: 1,
            call_seq: Arc::new(AtomicU64::new(0)),
            inference_token: None,
            terminal_failure_reason: None,
        }
    }

    pub(super) fn next_call(&self, runtime_instance_id: &str) -> PendingCallMetadata {
        let call_seq = self.call_seq.fetch_add(1, Ordering::SeqCst) + 1;
        PendingCallMetadata {
            call_id: uuid::Uuid::new_v4().to_string(),
            runtime_instance_id: runtime_instance_id.to_string(),
            request_id: self.request_id.clone(),
            call_seq,
            backend_id: self.backend_id.clone(),
            behavior_id: self.behavior_id.clone(),
            agent_did: self.agent_did.clone(),
            call_kind: self.call_kind,
            attempt: self.attempt,
        }
    }
}

tokio::task_local! {
    static ADMISSION_CALL_CONTEXT: AdmissionCallContext;
    static RUNNING_INFERENCE_CALL_PROVENANCE: RunningInferenceCallProvenance;
}

async fn scope_running_call<T>(
    provenance: RunningInferenceCallProvenance,
    future: impl Future<Output = T>,
) -> T {
    RUNNING_INFERENCE_CALL_PROVENANCE
        .scope(provenance.clone(), async move {
            provenance.mark_provider_path_entered();
            future.await
        })
        .await
}

/// Exact signed running-call V1 visible only while the admitted provider
/// future is constructing and sending its request.
pub(crate) fn current_running_call_provenance() -> Option<RunningInferenceCallProvenance> {
    RUNNING_INFERENCE_CALL_PROVENANCE
        .try_with(Clone::clone)
        .ok()
}

/// Poll a lazy provider stream inside the same running-call scope used to
/// construct it. Some rig providers do not construct the HTTP request until
/// the returned stream is first polled, so scoping only `CompletionModel::stream`
/// would leave the innermost capture transport without the exact call V1.
pub(super) fn scope_running_call_poll<T>(
    provenance: &RunningInferenceCallProvenance,
    poll: impl FnOnce() -> T,
) -> T {
    provenance.mark_provider_path_entered();
    RUNNING_INFERENCE_CALL_PROVENANCE.sync_scope(provenance.clone(), poll)
}

pub(crate) async fn scope_request<T>(
    context: AdmissionCallContext,
    future: impl Future<Output = T>,
) -> T {
    ADMISSION_CALL_CONTEXT.scope(context, future).await
}

pub(crate) async fn scope_call<T>(
    call_kind: CallKind,
    attempt: i64,
    future: impl Future<Output = T>,
) -> T {
    let mut context = current_context().expect("admission call scope requires request context");
    context.call_kind = call_kind;
    context.attempt = attempt;
    ADMISSION_CALL_CONTEXT.scope(context, future).await
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn scope_call_with_token<T>(
    call_kind: CallKind,
    attempt: i64,
    token: CancellationToken,
    future: impl Future<Output = T>,
) -> T {
    let mut context = current_context().expect("admission call scope requires request context");
    context.call_kind = call_kind;
    context.attempt = attempt;
    context.inference_token = Some(token);
    ADMISSION_CALL_CONTEXT.scope(context, future).await
}

pub(crate) type TerminalFailureReasonObserver = Arc<Mutex<Option<String>>>;

pub(crate) fn terminal_failure_reason_observer() -> TerminalFailureReasonObserver {
    Arc::new(Mutex::new(None))
}

pub(crate) fn set_terminal_failure_reason(
    observer: &TerminalFailureReasonObserver,
    reason: impl Into<String>,
) {
    let reason = reason.into();
    match observer.lock() {
        Ok(mut slot) => *slot = Some(reason),
        Err(poisoned) => *poisoned.into_inner() = Some(reason),
    }
}

pub(crate) async fn scope_call_with_token_and_failure_reason<T>(
    call_kind: CallKind,
    attempt: i64,
    token: CancellationToken,
    terminal_failure_reason: TerminalFailureReasonObserver,
    future: impl Future<Output = T>,
) -> T {
    let mut context = current_context().expect("admission call scope requires request context");
    context.call_kind = call_kind;
    context.attempt = attempt;
    context.inference_token = Some(token);
    context.terminal_failure_reason = Some(terminal_failure_reason);
    ADMISSION_CALL_CONTEXT.scope(context, future).await
}

pub(super) fn current_context() -> Result<AdmissionCallContext, CompletionError> {
    ADMISSION_CALL_CONTEXT
        .try_with(Clone::clone)
        .map_err(|_| CompletionError::ProviderError("missing inference admission context".into()))
}

pub(crate) fn current_session_id() -> Option<String> {
    ADMISSION_CALL_CONTEXT
        .try_with(|context| context.session_id.clone())
        .ok()
}
