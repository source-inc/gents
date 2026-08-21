use super::*;

pub(super) async fn load_timeline_request_by_id(
    access: &ConfigAccess,
    request_id: &str,
) -> Result<TimelineRequestRow> {
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{}" }} }},
                order: {{ created_at: DESC }},
                limit: 2
            ) {{
                _docID
                request_id
                agent_did
                behavior_id
                session_id
                content
                seed
                max_total_tokens
                metadata
                status
                lifecycle_state
                backend_id
                failure_reason
                created_at
                retry_count
                interrupt_requested_at
                caused_by_trigger_id
                caused_by_trigger_kind
                caused_by_correlation
                caused_by_trigger_context
                caused_by_source_doc_id
                caused_by_parent_request_id
                caused_by_parent_request_doc_id
                caused_by_parent_tool_call_id
                caused_by_parent_tool_call_doc_id
                workspace_id
                workspace_authority
                workspace_owner_deployment_id
                workspace_seal_hash
                execution_origin
            }}
        }}"#,
        escape_graphql_string(request_id)
    );
    let mut rows = load_rows::<TimelineRequestRow>(access, "AgentRequest", &query).await?;
    match rows.len() {
        0 => Err(anyhow::anyhow!("request {request_id} not found")),
        1 => Ok(rows.remove(0)),
        count => anyhow::bail!(
            "request_id {request_id} is ambiguous across {count} AgentRequest documents"
        ),
    }
}

pub(super) async fn load_timeline_requests_for_session(
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
                seed
                max_total_tokens
                metadata
                status
                lifecycle_state
                backend_id
                failure_reason
                created_at
                retry_count
                interrupt_requested_at
                caused_by_trigger_id
                caused_by_trigger_kind
                caused_by_correlation
                caused_by_trigger_context
                caused_by_source_doc_id
                caused_by_parent_request_id
                caused_by_parent_request_doc_id
                caused_by_parent_tool_call_id
                caused_by_parent_tool_call_doc_id
                workspace_id
                workspace_authority
                workspace_owner_deployment_id
                workspace_seal_hash
                execution_origin
            }}
        }}"#,
        escape_graphql_string(session_id)
    );
    load_rows(access, "AgentRequest", &query).await
}

pub(super) async fn load_timeline_child_requests(
    access: &ConfigAccess,
    parent_request_doc_id: &str,
) -> Result<Vec<TimelineRequestRow>> {
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ caused_by_parent_request_doc_id: {{ _eq: "{}" }} }},
                order: {{ created_at: ASC }}
            ) {{
                _docID
                request_id
                agent_did
                behavior_id
                session_id
                content
                seed
                max_total_tokens
                metadata
                status
                lifecycle_state
                backend_id
                failure_reason
                created_at
                retry_count
                interrupt_requested_at
                caused_by_trigger_id
                caused_by_trigger_kind
                caused_by_correlation
                caused_by_trigger_context
                caused_by_source_doc_id
                caused_by_parent_request_id
                caused_by_parent_request_doc_id
                caused_by_parent_tool_call_id
                caused_by_parent_tool_call_doc_id
                workspace_id
                workspace_authority
                workspace_owner_deployment_id
                workspace_seal_hash
                execution_origin
            }}
        }}"#,
        escape_graphql_string(parent_request_doc_id)
    );
    load_rows(access, "AgentRequest", &query).await
}
