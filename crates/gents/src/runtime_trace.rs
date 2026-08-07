use std::collections::HashMap;

use opentelemetry::global;
use serde_json::Value;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::watcher::AgentRequest;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RequestTraceAttrs {
    pub(crate) request_doc_id: String,
    pub(crate) request_id: String,
    pub(crate) agent_did: String,
    pub(crate) session_id: String,
    pub(crate) requested_behavior_id: String,
    pub(crate) execution_origin: String,
    pub(crate) deadline_at: String,
    pub(crate) has_deadline: bool,
    pub(crate) subagent_depth: u32,
    pub(crate) is_subagent: bool,
    pub(crate) parent_request_id: String,
    pub(crate) parent_tool_call_id: String,
    pub(crate) selected_skill_count: usize,
    pub(crate) workspace_cwd_set: bool,
}

impl RequestTraceAttrs {
    pub(crate) fn from_request(request: &AgentRequest) -> Self {
        let metadata = request
            .metadata
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .and_then(|value| serde_json::from_str::<Value>(value).ok());

        Self {
            request_doc_id: request.doc_id.clone(),
            request_id: request.request_id.clone(),
            agent_did: request.agent_did.clone(),
            session_id: request.session_id.clone(),
            requested_behavior_id: clean_optional(request.behavior_id.as_deref()),
            execution_origin: clean_optional(request.execution_origin.as_deref()),
            deadline_at: clean_optional(request.deadline.as_deref()),
            has_deadline: request
                .deadline
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty()),
            subagent_depth: request.subagent_depth,
            is_subagent: request.subagent_depth > 0
                || request.caused_by_parent_request_id.is_some()
                || request.caused_by_parent_tool_call_id.is_some(),
            parent_request_id: clean_optional(request.caused_by_parent_request_id.as_deref()),
            parent_tool_call_id: clean_optional(request.caused_by_parent_tool_call_id.as_deref()),
            selected_skill_count: metadata
                .as_ref()
                .and_then(|value| value.get("selected_skill_ids"))
                .and_then(Value::as_array)
                .map_or(0, Vec::len),
            workspace_cwd_set: metadata
                .as_ref()
                .and_then(|value| {
                    value
                        .pointer("/codex_shim/cwd")
                        .or_else(|| value.get("workspace_cwd"))
                })
                .and_then(Value::as_str)
                .is_some_and(|value| !value.trim().is_empty()),
        }
    }
}

pub(crate) fn record_current_request_outcome(outcome: &'static str) {
    tracing::Span::current().record("request_outcome", outcome);
}

pub(crate) fn record_current_claim_outcome(outcome: &'static str) {
    tracing::Span::current().record("claim_outcome", outcome);
}

pub(crate) fn record_current_failure_class(error: &anyhow::Error) {
    let error_text = error.to_string();
    let failure_class = crate::trace_export::analyze_request_failure(Some(&error_text))
        .map(|class| class.as_str())
        .unwrap_or("external");
    tracing::Span::current().record("failure_class", failure_class);
}

pub(crate) fn current_trace_context_headers() -> HashMap<String, String> {
    let context = tracing::Span::current().context();
    let mut headers = HashMap::new();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut headers);
    });
    headers.retain(|name, value| !name.trim().is_empty() && !value.trim().is_empty());
    headers
}

fn clean_optional(value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(metadata: Option<&str>) -> AgentRequest {
        AgentRequest {
            doc_id: "doc-1".to_string(),
            request_id: "req-1".to_string(),
            agent_did: "did:key:agent".to_string(),
            requester_did: None,
            behavior_id: Some("behavior-a".to_string()),
            session_id: "session-1".to_string(),
            content: "do not put this in telemetry".to_string(),
            temperature: None,
            top_p: None,
            top_k: None,
            seed: None,
            max_tokens: None,
            metadata: metadata.map(ToOwned::to_owned),
            execution_origin: Some("manual".to_string()),
            created_at: "2026-06-04T00:00:00Z".to_string(),
            deadline: Some("2026-06-04T00:05:00Z".to_string()),
            subagent_depth: 1,
            caused_by_parent_request_id: Some("parent-req".to_string()),
            caused_by_parent_tool_call_id: Some("parent-tool".to_string()),
        }
    }

    #[test]
    fn request_trace_attrs_summarize_metadata_without_payloads() {
        let attrs = RequestTraceAttrs::from_request(&request(Some(
            r#"{
                "selected_skill_ids": ["rust", "ops"],
                "codex_shim": { "cwd": "/workspace" },
                "prompt": "do not capture me"
            }"#,
        )));

        assert_eq!(attrs.request_doc_id, "doc-1");
        assert_eq!(attrs.request_id, "req-1");
        assert_eq!(attrs.agent_did, "did:key:agent");
        assert_eq!(attrs.session_id, "session-1");
        assert_eq!(attrs.selected_skill_count, 2);
        assert!(attrs.workspace_cwd_set);
        assert!(attrs.is_subagent);
        assert_eq!(attrs.parent_request_id, "parent-req");
        assert_eq!(attrs.parent_tool_call_id, "parent-tool");
    }

    #[test]
    fn request_trace_attrs_tolerate_missing_or_bad_metadata() {
        let attrs = RequestTraceAttrs::from_request(&request(Some("not-json")));

        assert_eq!(attrs.selected_skill_count, 0);
        assert!(!attrs.workspace_cwd_set);
    }
}
