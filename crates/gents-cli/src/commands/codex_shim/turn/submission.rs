use anyhow::Result;

use super::super::ShimState;
use crate::{create_agent_request_retrying_transient, RequestSubmitOptions, SubmittedRequest};

pub(super) async fn create_agent_request_with_retry(
    state: &ShimState,
    content: &str,
    session_id: Option<&str>,
    options: RequestSubmitOptions,
) -> Result<SubmittedRequest> {
    let request_id = uuid::Uuid::new_v4().to_string();
    let result = create_agent_request_retrying_transient(
        state.graphql.as_ref(),
        state.agent_did.as_ref(),
        content,
        session_id,
        Some(state.behavior_id.as_ref()),
        request_id.clone(),
        options,
    )
    .await;
    if result.is_err() {
        // The final failure may still be an ambiguous lost response. The shim
        // owns the embedded node and the stable id, so best-effort interruption
        // closes that leak without risking a different request generation.
        if let Err(error) = gents::interrupt_request(state.node.as_ref(), &request_id).await {
            tracing::debug!(
                %error,
                request_id,
                "Codex shim found no committed request to interrupt after submission failure"
            );
        }
    }
    result
}
