use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::StreamExt;
use rig::completion::CompletionResponse;
use rig::streaming::{RawStreamingChoice, StreamingCompletionResponse};

use super::hold_stream_guard;

struct DropProbe {
    drops: Arc<AtomicUsize>,
}

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl super::StreamGuardLifecycle for DropProbe {}

struct CancelBeforePollProbe {
    drops: Arc<AtomicUsize>,
}

impl Drop for CancelBeforePollProbe {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl super::StreamGuardLifecycle for CancelBeforePollProbe {
    fn cancel_before_poll(&mut self) -> bool {
        true
    }
}

#[tokio::test]
async fn holds_guard_until_stream_eof_and_preserves_final_response_metadata() {
    let drops = Arc::new(AtomicUsize::new(0));
    let inner = StreamingCompletionResponse::stream(Box::pin(futures::stream::iter(vec![
        Ok(RawStreamingChoice::Message("hello".to_string())),
        Ok(RawStreamingChoice::MessageId("msg_123".to_string())),
        Ok(RawStreamingChoice::FinalResponse(())),
    ])));
    let mut guarded = hold_stream_guard(
        inner,
        DropProbe {
            drops: drops.clone(),
        },
    );

    assert_eq!(drops.load(Ordering::SeqCst), 0);
    while guarded.next().await.is_some() {
        assert_eq!(drops.load(Ordering::SeqCst), 0);
    }

    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let completed: CompletionResponse<Option<()>> = guarded.into();
    assert_eq!(completed.raw_response, Some(()));
    assert_eq!(completed.message_id.as_deref(), Some("msg_123"));
}

#[tokio::test]
async fn drops_guard_when_caller_drops_stream_before_eof() {
    let drops = Arc::new(AtomicUsize::new(0));
    let inner: StreamingCompletionResponse<()> =
        StreamingCompletionResponse::stream(Box::pin(futures::stream::pending()));
    let guarded = hold_stream_guard(
        inner,
        DropProbe {
            drops: drops.clone(),
        },
    );

    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(guarded);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn drops_guard_when_inner_stream_errors() {
    let drops = Arc::new(AtomicUsize::new(0));
    let inner: StreamingCompletionResponse<()> =
        StreamingCompletionResponse::stream(Box::pin(futures::stream::iter(vec![Err(
            rig::completion::CompletionError::ProviderError("boom".to_string()),
        )])));
    let mut guarded = hold_stream_guard(
        inner,
        DropProbe {
            drops: drops.clone(),
        },
    );

    let item = guarded.next().await.expect("error item");
    assert!(item.is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancellation_before_first_poll_never_polls_the_provider_stream() {
    let polls = Arc::new(AtomicUsize::new(0));
    let polls_for_stream = polls.clone();
    let inner: StreamingCompletionResponse<()> =
        StreamingCompletionResponse::stream(Box::pin(futures::stream::poll_fn(move |_| {
            polls_for_stream.fetch_add(1, Ordering::SeqCst);
            std::task::Poll::Pending
        })));
    let drops = Arc::new(AtomicUsize::new(0));
    let mut guarded = super::hold_stream_guard(
        inner,
        CancelBeforePollProbe {
            drops: drops.clone(),
        },
    );

    let error = guarded
        .next()
        .await
        .expect("cancellation error")
        .expect_err("cancelled stream must fail before polling provider");
    assert!(error.to_string().contains("cancelled"));
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
