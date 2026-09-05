//! Prompt-free UI details derived from an authorized durable provider capture.
use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionContextDetails {
    pub system_prompt_tokens: u64,
    pub message_tokens: u64,
    pub message_count: u64,
    pub tool_definitions_count: u64,
    pub tool_call_count: u64,
}

pub async fn load_session_context_details(
    node: &EmbeddedNode,
    agent: &str,
    requester: Option<&str>,
    session: &str,
    context: &super::super::context_budget::LastRequestContextSnapshot,
) -> Result<Option<SessionContextDetails>> {
    anyhow::ensure!(
        !agent.trim().is_empty() && !session.trim().is_empty(),
        "context details require an exact session scope"
    );
    let requester = requester
        .map(|did| format!("\"{}\"", escape_graphql_string(did)))
        .unwrap_or_else(|| "null".into());
    let response = node
        .execute(&format!(
            r#"{{ AgentRequest(filter: {{
        agent_did: {{_eq: "{}"}}, requester_did: {{_eq: {requester}}},
        session_id: {{_eq: "{}"}}, request_id: {{_eq: "{}"}}
    }}, limit: 2) {{_docID}} }}"#,
            escape_graphql_string(agent),
            escape_graphql_string(session),
            escape_graphql_string(&context.request_id)
        ))
        .await;
    crate::graphql::ensure_no_errors(&response, "context detail ownership")?;
    let owners = response
        .data
        .as_ref()
        .and_then(|data| data["AgentRequest"].as_array())
        .context("missing context detail owners")?;
    let [owner] = owners.as_slice() else {
        return Ok(None);
    };
    let doc = owner["_docID"]
        .as_str()
        .context("context detail owner lacks physical identity")?;
    let response = node
        .execute(&format!(
            r#"{{ RenderedRequest(filter: {{
        request_doc_id: {{_eq: "{}"}}, agent_did: {{_eq: "{}"}},
        session_id: {{_eq: "{}"}}, capture_scope: {{_like: "inference.%"}},
        turn_index: {{_eq: {}}}, attempt: {{_eq: {}}}
    }}, limit: 2) {{capture_version source request_json provenance_json}} }}"#,
            escape_graphql_string(doc),
            escape_graphql_string(agent),
            escape_graphql_string(session),
            context.accounting.turn_index,
            context.accounting.attempt
        ))
        .await;
    crate::graphql::ensure_no_errors(&response, "context detail capture")?;
    let captures = response
        .data
        .as_ref()
        .and_then(|data| data["RenderedRequest"].as_array())
        .context("missing context detail captures")?;
    let [capture] = captures.as_slice() else {
        return Ok(None);
    };
    if capture["capture_version"].as_u64()
        != Some(gents_protocol::rendered_request::CAPTURE_VERSION as u64)
    {
        return Ok(None);
    }
    let provenance: Value = serde_json::from_str(
        capture["provenance_json"]
            .as_str()
            .context("missing capture provenance")?,
    )?;
    if provenance["admission"]["call_id"].as_str() != Some(context.call_id.as_str()) {
        return Ok(None);
    }
    // Other provider shapes require their own audited decomposition.
    if capture["source"].as_str() != Some("openai_chat_completions")
        || context.accounting.estimator != "openai_chat_wire_json_bytes_div_4_v1"
        || context.accounting.components.documents != 0
    {
        return Ok(None);
    }
    let body: Value = serde_json::from_str(
        capture["request_json"]
            .as_str()
            .context("missing captured provider request")?,
    )?;
    chat_details(&body, context.accounting.components.messages as u64).map(Some)
}

fn chat_details(body: &Value, message_tokens: u64) -> Result<SessionContextDetails> {
    let messages = body["messages"]
        .as_array()
        .context("captured chat messages are not an array")?;
    let mut system = Vec::new();
    let mut message_count = 0_u64;
    let mut tool_call_count = 0_u64;
    for message in messages {
        if matches!(message["role"].as_str(), Some("system" | "developer")) {
            system.push(message.clone());
        } else {
            message_count += 1;
        }
        if let Some(calls) = message.get("tool_calls") {
            if !calls.is_null() {
                tool_call_count += calls
                    .as_array()
                    .context("captured tool calls are not an array")?
                    .len() as u64;
            }
        }
    }
    // Same byte estimator as the accounting owner; assign array-framing
    // rounding remainder to messages so this partition stays additive.
    let system_prompt_tokens = if system.is_empty() {
        0
    } else {
        crate::provider_input::estimate_json(&json!(system))? as u64
    };
    let message_tokens = message_tokens
        .checked_sub(system_prompt_tokens)
        .context("system estimate exceeds captured message accounting")?;
    let tool_definitions_count = match body.get("tools") {
        None | Some(Value::Null) => 0,
        Some(tools) => tools
            .as_array()
            .context("captured tools are not an array")?
            .len() as u64,
    };
    Ok(SessionContextDetails {
        system_prompt_tokens,
        message_tokens,
        message_count,
        tool_definitions_count,
        tool_call_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn chat_details_partition_messages_and_count_actual_provider_tools() {
        let body = json!({"messages":[
            {"role":"system","content":"system instruction"},
            {"role":"developer","content":"developer instruction"},
            {"role":"user","content":"hello"},
            {"role":"assistant","tool_calls":[{"id":"a"},{"id":"b"}]},
            {"role":"tool","content":"done"}
        ],"tools":[{"type":"function"}]});
        let total = crate::provider_input::estimate_json(&body["messages"]).unwrap() as u64;
        let details = chat_details(&body, total).unwrap();
        assert!(details.system_prompt_tokens > 0);
        assert_eq!(details.system_prompt_tokens + details.message_tokens, total);
        assert_eq!(details.message_count, 3);
        assert_eq!(details.tool_call_count, 2);
        assert_eq!(details.tool_definitions_count, 1);
        assert!(chat_details(&body, 0).is_err());
        assert!(chat_details(&json!({"messages":[],"tools":{}}), 0).is_err());
    }
}
