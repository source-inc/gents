mod binding;
mod conversation;
mod request;

pub use conversation::rename_conversation;
// Re-export the shared interrupt helper from `gents` so the desktop
// client and the runtime share a single GraphQL implementation. Keeping this
// behind the `chat::` module path preserves the existing public surface.
// `fetch_interrupt_requested_at` is reachable directly via `gents::` for
// the conformance test; desktop code only uses `interrupt_request`.
pub use gents::interrupt_request;
pub use request::{
    resend_request, retry_request, submit_request, SubmitRequestOptions, SubmittedRequest,
};

#[cfg(test)]
mod tests;
