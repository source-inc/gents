//! Row fetch for [`crate::run_timeline`]: loads the persisted documents a
//! request's timeline is reconstructed from, over either transport
//! ([`ConfigAccess::Graphql`] or [`ConfigAccess::Local`]). Lifted from the
//! CLI `trace` command so the desktop client shares one fetcher.

use std::collections::BTreeSet;

use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::config_client::ConfigAccess;
use crate::graphql::escape_graphql_string;
use crate::run_timeline::{
    build_run_timeline, RunTimeline, RunTimelineRows, TimelineConversationRow,
    TimelineInferenceCallRow, TimelineMessageRow, TimelineRenderedRequestRow, TimelineRequestRow,
    TimelineResponseRow, TimelineSessionRow, TimelineToolCallRow,
};
use gents_protocol::graphql::graphql_rows_from_response;

pub async fn load_run_timeline(access: &ConfigAccess, request_id: &str) -> Result<RunTimeline> {
    Ok(build_run_timeline(
        load_run_timeline_rows(access, request_id).await?,
    ))
}

pub async fn load_run_timeline_rows(
    access: &ConfigAccess,
    request_id: &str,
) -> Result<RunTimelineRows> {
    let request = load_timeline_request_by_id(access, request_id).await?;
    let root_session_id = request.session_id.clone();

    let mut requests = match root_session_id.as_deref() {
        Some(session_id) => load_timeline_requests_for_session(access, session_id).await?,
        None => Vec::new(),
    };
    merge_timeline_request(&mut requests, request.clone());
    for child in load_timeline_child_requests(access, &request.request_id).await? {
        merge_timeline_request(&mut requests, child);
    }

    let session_ids = timeline_session_ids(&requests);
    let mut messages = Vec::new();
    let mut tool_calls = Vec::new();
    let mut responses = Vec::new();
    for session_id in &session_ids {
        messages.extend(load_timeline_messages_for_session(access, session_id).await?);
        tool_calls.extend(load_timeline_tool_calls_for_session(access, session_id).await?);
        responses.extend(load_timeline_responses_for_session(access, session_id).await?);
    }
    if session_ids.is_empty() || root_session_id.is_none() {
        responses.extend(load_timeline_responses_for_request(access, &request.request_id).await?);
    }
    let mut inference_calls = Vec::new();
    for request_id in timeline_request_ids(&requests) {
        inference_calls
            .extend(load_timeline_inference_calls_for_request(access, &request_id).await?);
    }
    let mut rendered_requests = Vec::new();
    for session_id in &session_ids {
        rendered_requests
            .extend(load_timeline_rendered_requests_for_session(access, session_id).await?);
    }
    if session_ids.is_empty() || root_session_id.is_none() {
        rendered_requests
            .extend(load_timeline_rendered_requests_for_request(access, &request.request_id).await?);
    }

    let session = match root_session_id.as_deref() {
        Some(session_id) => load_timeline_session(access, session_id).await?,
        None => None,
    };
    let conversation = match root_session_id.as_deref() {
        Some(session_id) => load_timeline_conversation(access, session_id).await?,
        None => None,
    };

    Ok(RunTimelineRows {
        request,
        session,
        conversation,
        requests,
        messages,
        tool_calls,
        inference_calls,
        responses,
        rendered_requests,
    })
}

async fn load_timeline_request_by_id(
    access: &ConfigAccess,
    request_id: &str,
) -> Result<TimelineRequestRow> {
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{}" }} }},
                order: {{ created_at: DESC }},
                limit: 1
            ) {{
                _docID
                request_id
                agent_did
                behavior_id
                session_id
                content
                metadata
                status
                lifecycle_state
                backend_id
                failure_reason
                created_at
                retry_count
                interrupt_requested_at
                caused_by_parent_request_id
                caused_by_parent_tool_call_id
                execution_origin
                caused_by_trigger_id
                caused_by_trigger_kind
            }}
        }}"#,
        escape_graphql_string(request_id)
    );
    load_rows::<TimelineRequestRow>(access, "AgentRequest", &query)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("request {request_id} not found"))
}

async fn load_timeline_requests_for_session(
    access: &ConfigAccess,
    session_id: &str,
) -> Result<Vec<TimelineRequestRow>> {
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ session_id: {{ _eq: "{}" }} }},
                order: {{ created_at: ASC }}
            ) {{
                _docID
                request_id
                agent_did
                behavior_id
                session_id
                content
                metadata
                status
                lifecycle_state
                backend_id
                failure_reason
                created_at
                retry_count
                interrupt_requested_at
                caused_by_parent_request_id
                caused_by_parent_tool_call_id
                execution_origin
                caused_by_trigger_id
                caused_by_trigger_kind
            }}
        }}"#,
        escape_graphql_string(session_id)
    );
    load_rows(access, "AgentRequest", &query).await
}

async fn load_timeline_child_requests(
    access: &ConfigAccess,
    parent_request_id: &str,
) -> Result<Vec<TimelineRequestRow>> {
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ caused_by_parent_request_id: {{ _eq: "{}" }} }},
                order: {{ created_at: ASC }}
            ) {{
                _docID
                request_id
                agent_did
                behavior_id
                session_id
                content
                metadata
                status
                lifecycle_state
                backend_id
                failure_reason
                created_at
                retry_count
                interrupt_requested_at
                caused_by_parent_request_id
                caused_by_parent_tool_call_id
                execution_origin
                caused_by_trigger_id
                caused_by_trigger_kind
            }}
        }}"#,
        escape_graphql_string(parent_request_id)
    );
    load_rows(access, "AgentRequest", &query).await
}

async fn load_timeline_messages_for_session(
    access: &ConfigAccess,
    session_id: &str,
) -> Result<Vec<TimelineMessageRow>> {
    let query = format!(
        r#"{{
            AgentMessage(
                filter: {{ session_id: {{ _eq: "{}" }} }},
                order: {{ sequence: ASC }}
            ) {{
                _docID
                session_id
                request_id
                sequence
                role
                content
                timestamp
            }}
        }}"#,
        escape_graphql_string(session_id)
    );
    match load_rows(access, "AgentMessage", &query).await {
        Ok(rows) => Ok(rows),
        Err(error) if error.to_string().contains("request_id") => {
            let fallback_query = format!(
                r#"{{
                    AgentMessage(
                        filter: {{ session_id: {{ _eq: "{}" }} }},
                        order: {{ sequence: ASC }}
                    ) {{
                        _docID
                        session_id
                        sequence
                        role
                        content
                        timestamp
                    }}
                }}"#,
                escape_graphql_string(session_id)
            );
            load_rows(access, "AgentMessage", &fallback_query).await
        }
        Err(error) => Err(error),
    }
}

async fn load_timeline_tool_calls_for_session(
    access: &ConfigAccess,
    session_id: &str,
) -> Result<Vec<TimelineToolCallRow>> {
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{ session_id: {{ _eq: "{}" }} }},
                order: {{ started_at: ASC }}
            ) {{
                _docID
                request_id
                session_id
                message_sequence
                tool_name
                tool_call_id
                args
                result
                status
                lifecycle_state
                started_at
                deadline_at
                completed_at
                selected_service_id
                selected_tool_name
                tool_failure_class
                denial_reason
                denied_argv
                denied_command
                denied_argument
                denied_subcommand
                denied_prefix
                policy_mode
                policy_network
                latency_ms
                await_mode
                cancel_policy
                cancel_cause
                child_request_id
            }}
        }}"#,
        escape_graphql_string(session_id)
    );
    load_rows(access, "AgentToolCall", &query).await
}

async fn load_timeline_responses_for_session(
    access: &ConfigAccess,
    session_id: &str,
) -> Result<Vec<TimelineResponseRow>> {
    let query = format!(
        r#"{{
            AgentResponse(
                filter: {{ session_id: {{ _eq: "{}" }} }},
                order: {{ created_at: ASC }}
            ) {{
                _docID
                request_id
                agent_did
                behavior_id
                session_id
                content
                reasoning
                status
                error_message
                token_count
                progress_seq
                materialized_message_sequence
                materialized_at
                created_at
                completed_at
                interrupted_at
            }}
        }}"#,
        escape_graphql_string(session_id)
    );
    load_rows(access, "AgentResponse", &query).await
}

async fn load_timeline_responses_for_request(
    access: &ConfigAccess,
    request_id: &str,
) -> Result<Vec<TimelineResponseRow>> {
    let query = format!(
        r#"{{
            AgentResponse(
                filter: {{ request_id: {{ _eq: "{}" }} }},
                order: {{ created_at: ASC }}
            ) {{
                _docID
                request_id
                agent_did
                behavior_id
                session_id
                content
                reasoning
                status
                error_message
                token_count
                progress_seq
                materialized_message_sequence
                materialized_at
                created_at
                completed_at
                interrupted_at
            }}
        }}"#,
        escape_graphql_string(request_id)
    );
    load_rows(access, "AgentResponse", &query).await
}

async fn load_timeline_inference_calls_for_request(
    access: &ConfigAccess,
    request_id: &str,
) -> Result<Vec<TimelineInferenceCallRow>> {
    let query = format!(
        r#"{{
            InferenceCall(
                filter: {{ request_id: {{ _eq: "{}" }} }},
                order: {{ call_seq: ASC }}
            ) {{
                _docID
                call_id
                request_id
                call_seq
                attempt
                call_state
                failure_reason
                queued_at
                started_at
                ended_at
                backend_id
                call_kind
                prompt_tokens
                completion_tokens
                cached_input_tokens
            }}
        }}"#,
        escape_graphql_string(request_id)
    );
    load_rows(access, "InferenceCall", &query).await
}

/// The rendered-request capture rows for one session, metadata columns only.
/// `request_json` is deliberately never selected here — see
/// `TimelineRenderedRequestRow`. Pre-#1059 databases have no `RenderedRequest`
/// collection; `load_rows` reports that as an empty section, not a failed
/// timeline.
async fn load_timeline_rendered_requests_for_session(
    access: &ConfigAccess,
    session_id: &str,
) -> Result<Vec<TimelineRenderedRequestRow>> {
    let query = format!(
        r#"{{
            RenderedRequest(
                filter: {{ session_id: {{ _eq: "{}" }} }},
                order: {{ created_at: ASC }}
            ) {{
                _docID
                capture_key
                request_doc_id
                request_id
                session_id
                capture_scope
                turn_index
                attempt
                capture_version
                model_name
                source
                prompt_hash
                tools_hash
                provenance_json
                created_at
            }}
        }}"#,
        escape_graphql_string(session_id)
    );
    load_rows(access, "RenderedRequest", &query).await
}

async fn load_timeline_rendered_requests_for_request(
    access: &ConfigAccess,
    request_id: &str,
) -> Result<Vec<TimelineRenderedRequestRow>> {
    let query = format!(
        r#"{{
            RenderedRequest(
                filter: {{ request_id: {{ _eq: "{}" }} }},
                order: {{ created_at: ASC }}
            ) {{
                _docID
                capture_key
                request_doc_id
                request_id
                session_id
                capture_scope
                turn_index
                attempt
                capture_version
                model_name
                source
                prompt_hash
                tools_hash
                provenance_json
                created_at
            }}
        }}"#,
        escape_graphql_string(request_id)
    );
    load_rows(access, "RenderedRequest", &query).await
}

async fn load_timeline_session(
    access: &ConfigAccess,
    session_id: &str,
) -> Result<Option<TimelineSessionRow>> {
    let query = format!(
        r#"{{
            AgentSession(
                filter: {{ session_id: {{ _eq: "{}" }} }},
                limit: 1
            ) {{
                _docID
                session_id
                agent_name
                behavior_id
                started
                ended
                status
            }}
        }}"#,
        escape_graphql_string(session_id)
    );
    Ok(
        load_rows::<TimelineSessionRow>(access, "AgentSession", &query)
            .await?
            .into_iter()
            .next(),
    )
}

async fn load_timeline_conversation(
    access: &ConfigAccess,
    session_id: &str,
) -> Result<Option<TimelineConversationRow>> {
    let query = format!(
        r#"{{
            AgentConversation(
                filter: {{ session_id: {{ _eq: "{}" }} }},
                limit: 1
            ) {{
                _docID
                session_id
                agent_name
                agent_did
                behavior_id
                title
                title_source
                preview_text
                status
                created_at
                updated_at
                latest_request_id
                forked_from_session_id
            }}
        }}"#,
        escape_graphql_string(session_id)
    );
    Ok(
        load_rows::<TimelineConversationRow>(access, "AgentConversation", &query)
            .await?
            .into_iter()
            .next(),
    )
}

fn merge_timeline_request(rows: &mut Vec<TimelineRequestRow>, request: TimelineRequestRow) {
    if !rows.iter().any(|row| row.request_id == request.request_id) {
        rows.push(request);
    }
}

fn timeline_session_ids(requests: &[TimelineRequestRow]) -> Vec<String> {
    requests
        .iter()
        .filter_map(|request| {
            request
                .session_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn timeline_request_ids(requests: &[TimelineRequestRow]) -> Vec<String> {
    requests
        .iter()
        .filter_map(|request| {
            let request_id = request.request_id.trim();
            (!request_id.is_empty()).then_some(request_id.to_string())
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

async fn load_rows<T>(access: &ConfigAccess, collection: &str, query: &str) -> Result<Vec<T>>
where
    T: DeserializeOwned,
{
    rows_or_empty_if_collection_missing(access, collection, query)
        .await?
        .into_iter()
        .map(serde_json::from_value)
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("decoding {collection} rows"))
}

async fn rows_or_empty_if_collection_missing(
    access: &ConfigAccess,
    collection_name: &str,
    query: &str,
) -> Result<Vec<Value>> {
    let rows = match access.execute(query).await {
        Ok(response) => Ok(graphql_rows_from_response(&response, collection_name)),
        Err(error) => Err(error),
    };
    match rows {
        Ok(rows) => Ok(rows),
        Err(error)
            if {
                let message = error.to_string();
                message.contains(collection_name)
                    && (message.contains("collection not found")
                        || message.contains("Cannot query field"))
            } =>
        {
            Ok(Vec::new())
        }
        Err(error) => Err(error),
    }
}
