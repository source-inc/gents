use anyhow::{Context, Result};
use defra_node::{EmbeddedNode, ExecuteRetryPolicy, QueryRequest, QueryResponse};
use identity::Did;

use super::controller::InferenceCallRecord;

/// Resolve the future ACP actor for an inference-call operation and ensure the
/// node that will author mutations is the declared agent deployment.
///
/// Query identity is not signature evidence. Every admitted call version is
/// independently checked with `verified_block_signer_did` before use.
pub(super) fn call_identity(node: &EmbeddedNode, call: &InferenceCallRecord) -> Result<Did> {
    let node_did = node.node_identity_did().ok_or_else(|| {
        anyhow::anyhow!(
            "InferenceCall {} requires a configured DefraDB node signing identity",
            call.call_id
        )
    })?;
    if node_did != call.agent_did {
        anyhow::bail!(
            "InferenceCall {} agent DID {} does not match node signing identity {}",
            call.call_id,
            call.agent_did,
            node_did
        );
    }
    Did::new(call.agent_did.as_str())
        .with_context(|| format!("parsing InferenceCall {} agent DID", call.call_id))
}

pub(super) async fn execute_for_call(
    node: &EmbeddedNode,
    call: &InferenceCallRecord,
    graphql: impl Into<String>,
) -> Result<QueryResponse> {
    let identity = call_identity(node, call)?;
    Ok(node
        .execute_request_with_retry(
            QueryRequest::new(graphql.into()).with_identity(Some(identity)),
            ExecuteRetryPolicy::default(),
        )
        .await)
}
