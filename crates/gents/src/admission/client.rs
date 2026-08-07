use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rig::client::CompletionClient;
use rig::completion::{CompletionError, CompletionModel, CompletionRequest, CompletionResponse};
use rig::streaming::StreamingCompletionResponse;
use tokio_util::sync::CancellationToken;

use super::controller::PendingCallMetadata;
use super::stream_guard::hold_stream_guard;
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
        let token = current_context().ok().and_then(|c| c.inference_token);
        match token {
            Some(token) => {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => {
                        permit.mark_interrupted();
                        Err(CompletionError::ProviderError(CANCELLED_BY_INTERRUPT_MSG.into()))
                    }
                    result = self.inner.completion(request) => match result {
                        Ok(response) => {
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
            None => match self.inner.completion(request).await {
                Ok(response) => {
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
        let token = current_context().ok().and_then(|c| c.inference_token);
        match token {
            Some(token) => {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => {
                        permit.mark_interrupted();
                        Err(CompletionError::ProviderError(CANCELLED_BY_INTERRUPT_MSG.into()))
                    }
                    result = self.inner.stream(request) => match result {
                        Ok(stream) => Ok(hold_stream_guard(stream, permit)),
                        Err(error) => {
                            permit.finish_failure(&error.to_string()).await;
                            Err(error)
                        }
                    }
                }
            }
            None => match self.inner.stream(request).await {
                Ok(stream) => Ok(hold_stream_guard(stream, permit)),
                Err(error) => {
                    permit.finish_failure(&error.to_string()).await;
                    Err(error)
                }
            },
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

/// The identity of the admission call currently in flight on this task:
/// exactly what `next_call` minted for it. This is what makes the
/// capture-to-`InferenceCall` join exact instead of ordinal — an admission
/// rejection consumes no `call_seq` and stores no join.
#[derive(Clone, Debug)]
pub(crate) struct CurrentCallJoin {
    pub(crate) call_id: String,
    pub(crate) call_seq: i64,
    pub(crate) call_kind: CallKind,
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
    /// Written by `next_call`, read by `current_call_join()` at the transport
    /// seam. Shared (`Arc`) across the request's call scopes — `scope_call`
    /// clones the context, and the clone must observe the same slot the
    /// admitted call writes.
    pub(super) current_call: Arc<Mutex<Option<CurrentCallJoin>>>,
    pub(super) inference_token: Option<CancellationToken>,
    pub(super) terminal_failure_reason: Option<TerminalFailureReasonObserver>,
}

impl AdmissionCallContext {
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
            current_call: Arc::new(Mutex::new(None)),
            inference_token: None,
            terminal_failure_reason: None,
        }
    }

    pub(super) fn next_call(&self, runtime_instance_id: &str) -> PendingCallMetadata {
        let call_seq = self.call_seq.fetch_add(1, Ordering::SeqCst) + 1;
        let metadata = PendingCallMetadata {
            call_id: uuid::Uuid::new_v4().to_string(),
            runtime_instance_id: runtime_instance_id.to_string(),
            request_id: self.request_id.clone(),
            call_seq,
            backend_id: self.backend_id.clone(),
            behavior_id: self.behavior_id.clone(),
            agent_did: self.agent_did.clone(),
            call_kind: self.call_kind,
            attempt: self.attempt,
        };
        let join = CurrentCallJoin {
            call_id: metadata.call_id.clone(),
            call_seq: i64::try_from(call_seq).unwrap_or(i64::MAX),
            call_kind: self.call_kind,
        };
        match self.current_call.lock() {
            Ok(mut slot) => *slot = Some(join),
            Err(poisoned) => *poisoned.into_inner() = Some(join),
        }
        metadata
    }
}

tokio::task_local! {
    static ADMISSION_CALL_CONTEXT: AdmissionCallContext;
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

/// The admission identity of the call currently in flight on this task, if
/// one has been admitted. `None` outside an admission scope (one-shot runs)
/// and before the first `next_call` of a scope.
pub(crate) fn current_call_join() -> Option<CurrentCallJoin> {
    ADMISSION_CALL_CONTEXT
        .try_with(|context| match context.current_call.lock() {
            Ok(slot) => slot.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        })
        .ok()
        .flatten()
}
