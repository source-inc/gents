use anyhow::Result;
use defra_node::EmbeddedNode;

use super::documents::{update_invocation, CallbackInvocationDoc};
use super::{
    LIFECYCLE_CLAIMED, LIFECYCLE_DENIED, LIFECYCLE_FAILED, LIFECYCLE_PENDING, LIFECYCLE_RUNNING,
    LIFECYCLE_SUCCEEDED,
};

pub fn invocation_is_claimable(
    local_deployment_id: &str,
    invocation: &CallbackInvocationDoc,
) -> bool {
    invocation.owner_deployment_id == local_deployment_id
        && matches!(
            invocation.lifecycle_state.as_str(),
            LIFECYCLE_PENDING | LIFECYCLE_CLAIMED | LIFECYCLE_RUNNING
        )
}

pub fn invocation_is_terminal(state: &str) -> bool {
    matches!(
        state,
        LIFECYCLE_SUCCEEDED | LIFECYCLE_FAILED | LIFECYCLE_DENIED
    )
}

/// Claim is unique per (owner_deployment_id, invocation_id). Replicas see the
/// row and must not claim.
pub async fn claim_invocation(
    node: &EmbeddedNode,
    local_deployment_id: &str,
    invocation: &CallbackInvocationDoc,
) -> Result<Option<CallbackInvocationDoc>> {
    if invocation.owner_deployment_id != local_deployment_id {
        return Ok(None);
    }
    if invocation_is_terminal(&invocation.lifecycle_state) {
        return Ok(None);
    }
    if matches!(
        invocation.lifecycle_state.as_str(),
        LIFECYCLE_CLAIMED | LIFECYCLE_RUNNING
    ) {
        return Ok(Some(invocation.clone()));
    }
    if invocation.lifecycle_state != LIFECYCLE_PENDING {
        return Ok(None);
    }

    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let mut claimed = invocation.clone();
    claimed.lifecycle_state = LIFECYCLE_CLAIMED.to_string();
    claimed.claimed_at = Some(now);
    claimed.attempts = Some(claimed.attempts.unwrap_or(0).saturating_add(1));
    if update_invocation(node, &claimed, Some(LIFECYCLE_PENDING)).await? {
        return Ok(Some(claimed));
    }
    let current = super::documents::load_invocation(node, &invocation.invocation_id).await?;
    Ok(current.filter(|row| invocation_is_claimable(local_deployment_id, row)))
}
