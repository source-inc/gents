//! Current context is a persisted inference observation, not token spend.
use anyhow::{Context, Result};
use defra_node::EmbeddedNode;
use gents::graphql::{ensure_no_errors, escape_graphql_string};
use gents_protocol::row::AgentRequestRow;

pub(super) type ContextOrder = (chrono::DateTime<chrono::Utc>, i64, String);

pub(super) struct ContextSample {
    pub order: ContextOrder,
    pub used: u64,
}

/// The turn/descendant owner already authorized this request's projection.
/// Resolve its exact identity again for the runtime accounting boundary.
pub(super) async fn load(
    node: &EmbeddedNode,
    session_id: &str,
    request_id: &str,
) -> Result<Option<ContextSample>> {
    let response = node
        .execute(&format!(
            r#"{{ AgentRequest(filter: {{request_id: {{_eq: "{}"}}}}, limit: 2) {{
        request_id session_id agent_did requester_did
    }} }}"#,
            escape_graphql_string(request_id)
        ))
        .await;
    ensure_no_errors(&response, "Grok context owner")?;
    let owners: Vec<AgentRequestRow> = serde_json::from_value(
        response
            .data
            .as_ref()
            .and_then(|v| v.get("AgentRequest"))
            .cloned()
            .context("missing context owner rows")?,
    )?;
    let [owner] = owners.as_slice() else {
        return Ok(None);
    };
    if owner.session_id.as_deref() != Some(session_id) {
        return Ok(None);
    }
    let Some(agent) = owner.agent_did.as_deref() else {
        return Ok(None);
    };
    let Some(observation) = gents::toolset::load_request_context_observation(
        node,
        agent,
        owner.requester_did.as_deref(),
        session_id,
        request_id,
    )
    .await?
    else {
        return Ok(None);
    };
    let context = observation.context;
    // Without a real dispatch timestamp we cannot order this observation
    // against another request's active context. Do not fabricate one.
    let Some(queued) = context
        .queued_at
        .as_deref()
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map(|time| time.with_timezone(&chrono::Utc))
    else {
        return Ok(None);
    };
    Ok(Some(ContextSample {
        order: (queued, context.call_sequence, context.call_id),
        used: (context.accounting.estimated_input_tokens as u64)
            .saturating_add(observation.completion_tokens.unwrap_or(0)),
    }))
}
