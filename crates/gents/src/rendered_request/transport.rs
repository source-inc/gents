//! The capture seam: the last `HttpClientExt` before the network client.
//!
//! ## Why here
//!
//! Four provider kinds, three of which edit the body after rig has serialized
//! it:
//!
//! | stack (outermost → innermost) | rewrite below the assembled request |
//! |---|---|
//! | `ChatGptCodexHttpClient` → *capture* → reqwest | hoists the first system text into `instructions` and strips system items from `input`, sets `store:false`/`stream:true`, deletes `max_output_tokens`/`temperature`/`top_p`, forces `strict:false` on every tool |
//! | `XaiGrokOAuthHttpClient` → *capture* → reqwest | injects `store:false` |
//! | `SessionTaggingHttpClient` → `ResponsesNormalizingHttpClient` → *capture* → reqwest | rewrites prior assistant items into typed, id-bearing, annotated Responses items |
//! | `SessionTaggingHttpClient` → *capture* → reqwest (Chat Completions), openrouter → *capture* → reqwest | none |
//!
//! Installing the capture *innermost* means it observes the body after every
//! one of those edits and immediately before the network client receives it, so
//! `request_json` is what the provider was sent rather than what the loop
//! intended. It also means there is no code path to the provider that skips
//! capture — the alternative, capturing in the loop, leaves three of four
//! stacks describing a request nobody sent.
//!
//! ## Fail-closed
//!
//! When a capture is armed and this client is about to post a completion body,
//! the durable write happens *first*. If it fails — malformed body, DTO build
//! error, DefraDB rejection, or an integrity conflict on the capture key — the
//! send returns an error and the inner client is never called. `Proofs/
//! RenderedCapture.lean`'s `capture_failure_blocks_send` is the property; this
//! is the only place the implementation can honour it, because this is the only
//! place that owns both the bytes and the decision to send them.
//!
//! The contract is stated positively, which matters. "Capture succeeded" is one
//! premise; "no capture was armed, so forward" is a *different* premise, and it
//! is the one under which a whole completion loop — the pre-request compaction
//! summarizer — once reached the provider with no durable row and no warning.
//! So inside a capture scope, a completion body with nothing armed and nothing
//! previously claimed is refused outright: a call nothing can name is a call
//! this transport will not make. Only two things forward without a row: a
//! non-completion path (`/models`, `/key`, multipart), and a send outside any
//! scope, which means no request is running in this task at all.
//!
//! One armed attempt may legitimately produce several outbound bodies — rig's
//! SSE event source reconnects by re-posting the request. Once the first body
//! is durable, an exact resend reuses that result; changed bytes go back through
//! the sink and become the integrity error they should be.

use std::fmt;
use std::future::Future;

use bytes::Bytes;
use rig::http_client::{
    self, HttpClientExt, LazyBody, MultipartForm, Request, ReqwestClient, Response,
    StreamingResponse,
};
use rig::wasm_compat::WasmCompatSend;
use sha2::{Digest, Sha256};

use super::scope::{self, CaptureClaim, PendingCapture, RequestCaptureScope};
use super::RenderedRequestSource;

/// Transport wrapper that persists the outbound completion body before it is
/// sent. Install it as the innermost wrapper of every provider stack.
#[derive(Clone, Debug, Default)]
pub struct RenderedRequestCapturingHttpClient<H = ReqwestClient> {
    inner: H,
}

impl<H> RenderedRequestCapturingHttpClient<H> {
    pub fn new(inner: H) -> Self {
        Self { inner }
    }
}

/// What a send should do with the request it is holding.
enum CaptureDecision {
    /// Not a completion body, or no capture scope installed at all. Forward
    /// unchanged.
    Forward,
    /// Capture first; forward only on success.
    Capture {
        scope: std::sync::Arc<RequestCaptureScope>,
        pending: PendingCapture,
        source: RenderedRequestSource,
        running_call: Option<crate::admission::RunningInferenceCallProvenance>,
        durable_body_fingerprint: Option<[u8; 32]>,
        /// Scheme and authority of the URI this body was actually posted to,
        /// e.g. `https://api.openai.com`. Observed at the seam rather than read
        /// from configuration, because a backend document can be edited between
        /// reconcile and send — this says where the bytes went, not where they
        /// were meant to go. `None` when the URI carries no authority.
        ///
        /// Deliberately not the full URI: the path is already implied by
        /// `source`, and query strings on some providers carry credentials.
        provider_endpoint: Option<String>,
    },
    /// A completion body inside a request that armed nothing. Refuse.
    Refuse {
        scope: std::sync::Arc<RequestCaptureScope>,
        path: String,
    },
}

/// Scheme and authority only — never the path or query. The path is implied by
/// `RenderedRequestSource`, and some providers put credentials in the query.
fn provider_endpoint_of(scheme: Option<&str>, authority: Option<&str>) -> Option<String> {
    let authority = authority?;
    Some(match scheme {
        Some(scheme) => format!("{scheme}://{authority}"),
        None => authority.to_string(),
    })
}

fn decide(path: &str, provider_endpoint: Option<String>) -> CaptureDecision {
    let Some(source) = RenderedRequestSource::for_request_path(path) else {
        return CaptureDecision::Forward;
    };
    // No scope at all means no request is running in this task — a CLI probe or
    // a library embedding. Inside a scope the answer is never "forward
    // silently": either a capture names this body, or nothing can, and the
    // latter is a refusal.
    let Some((scope, claim)) = scope::claim_pending() else {
        return CaptureDecision::Forward;
    };
    match claim {
        CaptureClaim::Armed(pending) => CaptureDecision::Capture {
            scope,
            pending,
            source,
            running_call: crate::admission::current_running_call_provenance(),
            durable_body_fingerprint: None,
            provider_endpoint,
        },
        CaptureClaim::Resend {
            pending,
            durable_body_fingerprint,
        } => CaptureDecision::Capture {
            scope,
            pending,
            source,
            running_call: crate::admission::current_running_call_provenance(),
            durable_body_fingerprint,
            provider_endpoint,
        },
        CaptureClaim::Unexplained => CaptureDecision::Refuse {
            scope,
            path: path.to_string(),
        },
    }
}

/// Persist the body, or produce the transport error that refuses the send.
async fn capture_or_refuse(decision: CaptureDecision, body: &Bytes) -> http_client::Result<()> {
    let (scope, pending, source, running_call, durable_body_fingerprint, provider_endpoint) =
        match decision {
            CaptureDecision::Forward => return Ok(()),
            CaptureDecision::Refuse { scope, path } => {
                tracing::error!(
                    request_id = %scope.context().request_id,
                    session_id = %scope.context().session_id,
                    path = %path,
                    "refusing provider send: a completion body reached the transport with no armed \
                     rendered-request capture"
                );
                return Err(http_client::Error::Instance(
                    anyhow::anyhow!(scope::unexplained_send_message(scope.context(), &path)).into(),
                ));
            }
            CaptureDecision::Capture {
                scope,
                pending,
                source,
                running_call,
                durable_body_fingerprint,
                provider_endpoint,
            } => (
                scope,
                pending,
                source,
                running_call,
                durable_body_fingerprint,
                provider_endpoint,
            ),
        };

    let mut hasher = Sha256::new();
    hasher.update(match source {
        RenderedRequestSource::OpenAiResponses => [0],
        RenderedRequestSource::OpenAiChatCompletions => [1],
    });
    hasher.update(body.as_ref());
    let body_fingerprint: [u8; 32] = hasher.finalize().into();
    if durable_body_fingerprint == Some(body_fingerprint) {
        tracing::debug!(
            capture_scope = %pending.capture_scope,
            turn_index = pending.turn_index,
            attempt = pending.attempt,
            "reusing durable rendered-request capture for byte-identical transport resend"
        );
        return Ok(());
    }

    let capture_scope = pending.capture_scope.clone();
    let turn_index = pending.turn_index;
    let attempt = pending.attempt;
    let claimed = pending.clone();
    match scope::capture_body(
        scope.as_ref(),
        pending,
        source,
        running_call,
        provider_endpoint,
        body.as_ref(),
    )
    .await
    {
        Ok(()) => {
            scope.mark_claimed_durable(&claimed, body_fingerprint);
            Ok(())
        }
        Err((stage, error)) => {
            // Never log the payload: this record exists precisely because the
            // body is the whole conversation.
            tracing::error!(
                capture_scope = %capture_scope,
                request_id = %scope.context().request_id,
                session_id = %scope.context().session_id,
                turn_index,
                attempt,
                stage = stage.as_str(),
                error = %format!("{error:#}"),
                "refusing provider send: rendered-request capture failed"
            );
            Err(http_client::Error::Instance(
                anyhow::anyhow!(scope::capture_failure_message(
                    stage,
                    &capture_scope,
                    turn_index,
                    attempt,
                    &error,
                ))
                .into(),
            ))
        }
    }
}

impl<H> HttpClientExt for RenderedRequestCapturingHttpClient<H>
where
    H: Clone + HttpClientExt + fmt::Debug + 'static,
{
    fn send<T, U>(
        &self,
        req: Request<T>,
    ) -> impl Future<Output = http_client::Result<Response<LazyBody<U>>>> + WasmCompatSend + 'static
    where
        T: Into<Bytes> + WasmCompatSend,
        U: From<Bytes>,
        U: WasmCompatSend + 'static,
    {
        let inner = self.inner.clone();
        let (parts, body) = req.into_parts();
        let body: Bytes = body.into();
        // The decision — and therefore the `take_pending` that consumes the
        // arm — has to happen here, in the caller's task, not inside the
        // returned future: `HttpClientExt::send` returns a `'static` future
        // that the provider client may poll from anywhere, and a task-local is
        // not visible from a task that did not install it.
        let decision = decide(
            parts.uri.path(),
            provider_endpoint_of(
                parts.uri.scheme_str(),
                parts.uri.authority().map(|authority| authority.as_str()),
            ),
        );
        async move {
            capture_or_refuse(decision, &body).await?;
            let req = Request::from_parts(parts, body);
            HttpClientExt::send::<Bytes, U>(&inner, req).await
        }
    }

    fn send_multipart<U>(
        &self,
        req: Request<MultipartForm>,
    ) -> impl Future<Output = http_client::Result<Response<LazyBody<U>>>> + WasmCompatSend + 'static
    where
        U: From<Bytes>,
        U: WasmCompatSend + 'static,
    {
        // No provider in this runtime posts a completion as multipart; audio
        // and file uploads travel this way. Forward untouched, and in
        // particular do not consume an armed capture.
        let inner = self.inner.clone();
        async move { HttpClientExt::send_multipart(&inner, req).await }
    }

    fn send_streaming<T>(
        &self,
        req: Request<T>,
    ) -> impl Future<Output = http_client::Result<StreamingResponse>> + WasmCompatSend
    where
        T: Into<Bytes>,
    {
        let inner = self.inner.clone();
        let (parts, body) = req.into_parts();
        let body: Bytes = body.into();
        let decision = decide(
            parts.uri.path(),
            provider_endpoint_of(
                parts.uri.scheme_str(),
                parts.uri.authority().map(|authority| authority.as_str()),
            ),
        );
        async move {
            capture_or_refuse(decision, &body).await?;
            let req = Request::from_parts(parts, body);
            HttpClientExt::send_streaming(&inner, req).await
        }
    }
}

/// Terminal transport for capture-seam tests: counts and records every body it
/// is asked to send, and answers with an empty success. A capture failure must
/// leave `sends` at zero.
///
/// It lives outside `mod tests` because the provider wrappers that must sit
/// *above* the capture seam — ChatGPT Codex, xAI Grok — assemble their real
/// stack over it in their own modules to prove the row describes the rewritten
/// body rather than the one rig serialized.
#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub(crate) struct CountingInner {
    pub(crate) sends: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub(crate) bodies: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
}

#[cfg(test)]
impl CountingInner {
    pub(crate) fn send_count(&self) -> usize {
        self.sends.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn bodies(&self) -> Vec<serde_json::Value> {
        self.bodies.lock().expect("bodies").clone()
    }
}

#[cfg(test)]
mod counting_inner_impl {
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use super::*;

    impl HttpClientExt for CountingInner {
        fn send<T, U>(
            &self,
            req: Request<T>,
        ) -> impl Future<Output = http_client::Result<Response<LazyBody<U>>>> + WasmCompatSend + 'static
        where
            T: Into<Bytes> + WasmCompatSend,
            U: From<Bytes> + WasmCompatSend + 'static,
        {
            let sends = Arc::clone(&self.sends);
            let bodies = Arc::clone(&self.bodies);
            let body: Bytes = req.into_body().into();
            async move {
                sends.fetch_add(1, Ordering::SeqCst);
                if let Ok(value) = serde_json::from_slice(&body) {
                    bodies.lock().expect("bodies").push(value);
                }
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
            let sends = Arc::clone(&self.sends);
            async move {
                sends.fetch_add(1, Ordering::SeqCst);
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
            let sends = Arc::clone(&self.sends);
            let bodies = Arc::clone(&self.bodies);
            let body: Bytes = req.into_body().into();
            async move {
                sends.fetch_add(1, Ordering::SeqCst);
                if let Ok(value) = serde_json::from_slice(&body) {
                    bodies.lock().expect("bodies").push(value);
                }
                let stream: rig::http_client::sse::BoxedStream =
                    Box::pin(futures::stream::empty::<http_client::Result<Bytes>>());
                Ok(Response::builder().status(200).body(stream)?)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Mutex};

    use serde_json::json;

    use super::*;
    use crate::rendered_request::scope::{arm, scope_request, test_scope, CaptureScopeKind};
    use crate::rendered_request::{
        AssemblyBuildPath, AssemblyTrace, RenderedCompletionRequest, RenderedRequestCaptureSink,
        RenderedRequestContext,
    };

    fn context() -> RenderedRequestContext {
        RenderedRequestContext {
            request_doc_id: "doc-1".to_string(),
            request_provenance: Some(crate::document_version::test_request_execution_provenance(
                "doc-1",
                "did:key:agent",
            )),
            inference_call_provenance_scope:
                crate::rendered_request::InferenceCallProvenanceScope::StaticOrTest,
            transcript_snapshot: Vec::new(),
            config_provenance_scope:
                crate::rendered_request::ConfigProvenanceScope::StaticOrOneShot,
            config_provenance: None,
            request_id: "req-1".to_string(),
            agent_did: "did:key:agent".to_string(),
            requester_did: "did:key:requester".to_string(),
            behavior_id: "behavior".to_string(),
            session_id: "session".to_string(),
            model_name: "configured-model".to_string(),
        }
    }

    fn recording_sink() -> (
        RenderedRequestCaptureSink,
        Arc<Mutex<Vec<RenderedCompletionRequest>>>,
    ) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink_seen = Arc::clone(&seen);
        let sink: RenderedRequestCaptureSink = Arc::new(move |rendered| {
            let seen = Arc::clone(&sink_seen);
            Box::pin(async move {
                seen.lock().expect("seen").push(rendered);
                Ok(crate::rendered_request::test_static_rendered_request_version())
            })
        });
        (sink, seen)
    }

    fn failing_sink() -> RenderedRequestCaptureSink {
        Arc::new(|_| Box::pin(async { anyhow::bail!("injected capture failure") }))
    }

    fn trace() -> AssemblyTrace {
        AssemblyTrace::from_effective_messages(AssemblyBuildPath::Budgeted, Vec::new())
    }

    fn responses_request(body: serde_json::Value) -> Request<Bytes> {
        Request::builder()
            .method("POST")
            .uri("https://example.test/v1/responses")
            .body(Bytes::from(serde_json::to_vec(&body).expect("body")))
            .expect("request")
    }

    #[tokio::test]
    async fn captures_the_body_the_inner_client_receives() {
        let inner = CountingInner::default();
        let client = RenderedRequestCapturingHttpClient::new(inner.clone());
        let (sink, seen) = recording_sink();
        let scope = test_scope(context(), sink);

        scope_request(scope, async {
            arm(CaptureScopeKind::Inference, 3, 1, trace()).expect("armed");
            let _ = HttpClientExt::send_streaming(
                &client,
                responses_request(json!({
                    "model": "wire-model",
                    "instructions": "hoisted",
                    "input": [{"role": "user", "content": "hi"}],
                    "store": false,
                    "stream": true,
                    "tools": [{"type": "function", "name": "read", "strict": false}],
                })),
            )
            .await
            .expect("send");
        })
        .await;

        let seen = seen.lock().expect("seen");
        assert_eq!(seen.len(), 1);
        let rendered = &seen[0];
        assert_eq!(rendered.turn_index, 3);
        assert_eq!(rendered.attempt, 1);
        assert_eq!(rendered.capture_scope, "inference.1");
        assert_eq!(rendered.source, RenderedRequestSource::OpenAiResponses);
        // The Codex rewrite is visible in the row because the row *is* the
        // rewritten body.
        assert_eq!(rendered.request_json["instructions"], "hoisted");
        assert_eq!(rendered.request_json["store"], false);
        assert_eq!(rendered.tools_json[0]["strict"], false);
        assert_eq!(rendered.messages_json[0]["role"], "user");
        // The model column reports the model the provider was actually asked
        // for, not the behavior's configured name.
        assert_eq!(rendered.model_name, "wire-model");
        assert_eq!(
            rendered.sampling_json["max_tokens"],
            serde_json::Value::Null
        );

        assert_eq!(
            *inner.bodies.lock().expect("bodies").first().unwrap(),
            rendered.request_json,
            "the captured payload and the forwarded payload must be the same bytes"
        );
    }

    /// The fail-closed property. A sink error must terminate the send with the
    /// inner transport untouched.
    #[tokio::test]
    async fn a_failed_capture_issues_no_http_call() {
        let inner = CountingInner::default();
        let client = RenderedRequestCapturingHttpClient::new(inner.clone());
        let scope = test_scope(context(), failing_sink());

        let error = scope_request(scope, async {
            arm(CaptureScopeKind::Inference, 0, 0, trace()).expect("armed");
            HttpClientExt::send_streaming(&client, responses_request(json!({"input": []})))
                .await
                .err()
                .expect("send must fail when capture fails")
        })
        .await;

        assert!(
            error.to_string().contains("was not issued"),
            "error should say the call was refused: {error}"
        );
        assert_eq!(
            inner.sends.load(Ordering::SeqCst),
            0,
            "no HTTP request may be issued when the capture fails"
        );
    }

    #[tokio::test]
    async fn an_unparseable_body_fails_closed_too() {
        let inner = CountingInner::default();
        let client = RenderedRequestCapturingHttpClient::new(inner.clone());
        let (sink, seen) = recording_sink();
        let scope = test_scope(context(), sink);

        let error = scope_request(scope, async {
            arm(CaptureScopeKind::Inference, 0, 0, trace()).expect("armed");
            let req = Request::builder()
                .method("POST")
                .uri("https://example.test/v1/chat/completions")
                .body(Bytes::from_static(b"not json"))
                .expect("request");
            HttpClientExt::send_streaming(&client, req)
                .await
                .err()
                .expect("send must fail")
        })
        .await;

        assert!(error.to_string().contains("decode_body"), "{error}");
        assert_eq!(inner.sends.load(Ordering::SeqCst), 0);
        assert!(seen.lock().expect("seen").is_empty());
    }

    /// A `/models` listing inside an armed scope must not consume the turn's
    /// identity, and must not be captured as if it were a completion.
    #[tokio::test]
    async fn non_completion_paths_pass_through_without_consuming_the_arm() {
        let inner = CountingInner::default();
        let client = RenderedRequestCapturingHttpClient::new(inner.clone());
        let (sink, seen) = recording_sink();
        let scope = test_scope(context(), sink);

        scope_request(scope, async {
            arm(CaptureScopeKind::Inference, 0, 0, trace()).expect("armed");
            let req = Request::builder()
                .method("GET")
                .uri("https://example.test/v1/models")
                .body(Bytes::from_static(b"{}"))
                .expect("request");
            let _ = HttpClientExt::send::<Bytes, Bytes>(&client, req)
                .await
                .expect("models listing succeeds");
            assert!(
                crate::rendered_request::scope::pending_is_armed(),
                "a models listing must leave the completion's capture armed"
            );
        })
        .await;

        assert_eq!(inner.sends.load(Ordering::SeqCst), 1);
        assert!(seen.lock().expect("seen").is_empty());
    }

    /// Outside a request scope — CLI probes, model listings at startup — the
    /// wrapper is transparent.
    #[tokio::test]
    async fn without_a_scope_the_client_is_transparent() {
        let inner = CountingInner::default();
        let client = RenderedRequestCapturingHttpClient::new(inner.clone());

        let _ = HttpClientExt::send_streaming(&client, responses_request(json!({"input": []})))
            .await
            .expect("send");

        assert_eq!(inner.sends.load(Ordering::SeqCst), 1);
    }

    /// The positive half of fail-closed, and the fence over the defect that
    /// motivated it: a completion loop that runs inside a request without
    /// arming a capture (the pre-request compaction summarizer did exactly
    /// this, because its scope did not span it) must not reach the provider.
    /// The old contract forwarded this body silently.
    #[tokio::test]
    async fn a_completion_inside_a_scope_that_never_armed_is_refused() {
        let inner = CountingInner::default();
        let client = RenderedRequestCapturingHttpClient::new(inner.clone());
        let (sink, seen) = recording_sink();
        let scope = test_scope(context(), sink);

        let error = scope_request(scope, async {
            HttpClientExt::send_streaming(
                &client,
                responses_request(json!({"input": [{"role": "user", "content": "hi"}]})),
            )
            .await
            .err()
            .expect("an unarmed completion inside a request must be refused")
        })
        .await;

        assert!(
            error
                .to_string()
                .contains("no armed rendered-request capture"),
            "the refusal must name its cause: {error}"
        );
        assert_eq!(
            inner.sends.load(Ordering::SeqCst),
            0,
            "no HTTP request may be issued for a completion nothing can name"
        );
        assert!(seen.lock().expect("seen").is_empty());
    }

    /// rig's SSE event source reconnects by re-posting the same request. That
    /// second body has no arm of its own, but it is not unexplained: it belongs
    /// to the attempt already claimed. Once the first write is durable, an
    /// exact resend reuses it instead of paying for another synchronous write.
    #[tokio::test]
    async fn a_transport_resend_reuses_the_durable_capture_rather_than_being_refused() {
        let inner = CountingInner::default();
        let client = RenderedRequestCapturingHttpClient::new(inner.clone());
        let (sink, seen) = recording_sink();
        let scope = test_scope(context(), sink);

        scope_request(scope, async {
            arm(CaptureScopeKind::Inference, 2, 1, trace()).expect("armed");
            let body = json!({"model": "m", "input": [{"role": "user", "content": "hi"}]});
            let _ = HttpClientExt::send_streaming(&client, responses_request(body.clone()))
                .await
                .expect("first send");
            let _ = HttpClientExt::send_streaming(&client, responses_request(body))
                .await
                .expect("reconnect send");
        })
        .await;

        let seen = seen.lock().expect("seen");
        assert_eq!(
            seen.len(),
            1,
            "an identical resend must not rewrite DefraDB"
        );
        assert_eq!(
            (seen[0].turn_index, seen[0].attempt),
            (2, 1),
            "a resend keeps the coordinates of the attempt it re-posts"
        );
        assert_eq!(inner.sends.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_changed_transport_resend_still_reaches_the_integrity_sink() {
        let inner = CountingInner::default();
        let client = RenderedRequestCapturingHttpClient::new(inner.clone());
        let (sink, seen) = recording_sink();
        let scope = test_scope(context(), sink);

        scope_request(scope, async {
            arm(CaptureScopeKind::Inference, 2, 1, trace()).expect("armed");
            let _ = HttpClientExt::send_streaming(
                &client,
                responses_request(json!({"input": [{"role": "user", "content": "first"}]})),
            )
            .await
            .expect("first send");
            let _ = HttpClientExt::send_streaming(
                &client,
                responses_request(json!({"input": [{"role": "user", "content": "changed"}]})),
            )
            .await
            .expect("recording sink accepts changed test body");
        })
        .await;

        assert_eq!(seen.lock().expect("seen").len(), 2);
        assert_eq!(inner.sends.load(Ordering::SeqCst), 2);
    }

    /// Chat Completions bodies name their message list `messages`, and the
    /// derived views have to follow the wire shape actually posted.
    #[tokio::test]
    async fn chat_completions_bodies_index_the_messages_field() {
        let inner = CountingInner::default();
        let client = RenderedRequestCapturingHttpClient::new(inner);
        let (sink, seen) = recording_sink();
        let scope = test_scope(context(), sink);

        scope_request(scope, async {
            arm(CaptureScopeKind::Inference, 0, 0, trace()).expect("armed");
            let req = Request::builder()
                .method("POST")
                .uri("https://example.test/v1/chat/completions")
                .body(Bytes::from(
                    serde_json::to_vec(&json!({
                        "model": "m",
                        "messages": [{"role": "user", "content": "hi"}],
                        "max_tokens": 512,
                        "temperature": 0.2,
                    }))
                    .expect("body"),
                ))
                .expect("request");
            let _ = HttpClientExt::send::<Bytes, Bytes>(&client, req)
                .await
                .expect("send");
        })
        .await;

        let seen = seen.lock().expect("seen");
        assert_eq!(seen[0].source, RenderedRequestSource::OpenAiChatCompletions);
        assert_eq!(seen[0].messages_json[0]["role"], "user");
        assert_eq!(seen[0].sampling_json["max_tokens"], 512);
        assert_eq!(seen[0].sampling_json["temperature"], 0.2);
    }
}
