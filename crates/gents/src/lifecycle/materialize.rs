use super::*;

#[derive(Debug, Clone)]
pub struct EnqueuedAgentRequest {
    pub doc_id: String,
    pub request_id: String,
    pub session_id: String,
}

fn trigger_lineage_graphql_fields(trigger_lineage: &TriggerLineage) -> Result<String> {
    let trigger_kind = trigger_lineage
        .trigger_kind
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let source_doc_id = trigger_lineage.source_doc_id.as_deref().map(str::trim);
    match (trigger_kind, source_doc_id) {
        (Some("event"), Some(value)) if !value.is_empty() => {}
        (Some("event"), _) => anyhow::bail!("Event trigger lineage requires source_doc_id"),
        (_, Some(_)) => anyhow::bail!("Only Event trigger lineage may carry source_doc_id"),
        _ => {}
    }

    let caused_by_trigger_id_field = trigger_lineage
        .trigger_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            format!(
                r#"
                    caused_by_trigger_id: "{}","#,
                escape_graphql_string(value)
            )
        })
        .unwrap_or_default();
    let caused_by_trigger_kind_field = trigger_lineage
        .trigger_kind
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            format!(
                r#"
                    caused_by_trigger_kind: "{}","#,
                escape_graphql_string(value)
            )
        })
        .unwrap_or_default();
    let caused_by_source_doc_id_field = source_doc_id
        .map(|value| {
            format!(
                r#"
                    caused_by_source_doc_id: "{}","#,
                escape_graphql_string(value)
            )
        })
        .unwrap_or_default();
    let caused_by_correlation_field = trigger_lineage
        .correlation
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            format!(
                r#"
                    caused_by_correlation: "{}","#,
                escape_graphql_string(value)
            )
        })
        .unwrap_or_default();
    let caused_by_trigger_context_field = trigger_lineage
        .trigger_context
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            crate::lifecycle::TriggerExecutionContext::parse(Some(value))?;
            Ok::<_, anyhow::Error>(format!(
                r#"
                    caused_by_trigger_context: "{}","#,
                escape_graphql_string(value)
            ))
        })
        .transpose()?
        .unwrap_or_default();
    Ok(format!(
        "{caused_by_trigger_id_field}{caused_by_trigger_kind_field}{caused_by_source_doc_id_field}{caused_by_correlation_field}{caused_by_trigger_context_field}"
    ))
}

fn workspace_lineage_graphql_fields(workspace: Option<&WorkspaceLineage>) -> String {
    let Some(workspace) = workspace else {
        return String::new();
    };
    let mut fields = String::new();
    let mut push = |name: &str, value: Option<&str>| {
        if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
            fields.push_str(&format!(
                r#"
                    {name}: "{}","#,
                escape_graphql_string(value)
            ));
        }
    };
    push("workspace_id", workspace.workspace_id.as_deref());
    push(
        "workspace_authority",
        workspace.workspace_authority.as_deref(),
    );
    push(
        "workspace_owner_deployment_id",
        workspace.workspace_owner_deployment_id.as_deref(),
    );
    push(
        "workspace_seal_hash",
        workspace.workspace_seal_hash.as_deref(),
    );
    fields
}

async fn resolve_created_agent_request_doc_id(
    node: &EmbeddedNode,
    mutation_response: &defra_node::QueryResponse,
    mutation_field: &str,
    escaped_request_id: &str,
    lookup_error: &str,
    missing_doc_id_error: &str,
) -> Result<String> {
    if let Some(doc_id) = extract_single_doc_id(mutation_response, mutation_field) {
        return Ok(doc_id);
    }

    let query = format!(
        r#"{{ AgentRequest(filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }}, limit: 2) {{ _docID }} }}"#
    );
    let query_resp = node.execute(&query).await;
    if query_resp.has_errors() {
        anyhow::bail!("{lookup_error}: {:?}", query_resp.errors);
    }

    let rows = query_resp
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    if rows.len() != 1 {
        anyhow::bail!(
            "{missing_doc_id_error}: request_id lookup returned {} documents",
            rows.len()
        );
    }
    rows.first()
        .and_then(|row| row.get("_docID"))
        .and_then(|doc_id| doc_id.as_str())
        .ok_or_else(|| anyhow::anyhow!("{missing_doc_id_error}"))
        .map(str::to_string)
}

pub(crate) async fn write_pending_agent_request_with_lineage_and_conversation_title(
    node: &EmbeddedNode,
    agent_did: &str,
    behavior_id: &str,
    content: &str,
    execution_origin: ExecutionOrigin,
    trigger_lineage: TriggerLineage,
    conversation_title: Option<&str>,
) -> Result<EnqueuedAgentRequest> {
    write_pending_agent_request_with_lineage_workspace_and_conversation_title(
        node,
        agent_did,
        behavior_id,
        content,
        execution_origin,
        trigger_lineage,
        conversation_title,
        None,
    )
    .await
}

pub(crate) async fn write_pending_agent_request_with_lineage_workspace_and_conversation_title(
    node: &EmbeddedNode,
    agent_did: &str,
    behavior_id: &str,
    content: &str,
    execution_origin: ExecutionOrigin,
    trigger_lineage: TriggerLineage,
    conversation_title: Option<&str>,
    workspace_lineage: Option<&WorkspaceLineage>,
) -> Result<EnqueuedAgentRequest> {
    if trigger_lineage.trigger_kind.as_deref() == Some("manual")
        && trigger_lineage.trigger_id.is_some()
    {
        anyhow::bail!("Manual trigger enqueue must not carry trigger_id");
    }
    if let Some(workspace) = workspace_lineage {
        workspace.require_authority_if_workspace_id()?;
    }

    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let request_id = uuid::Uuid::new_v4().to_string();
    let session_id = uuid::Uuid::new_v4().to_string();

    let escaped_request_id = escape_graphql_string(&request_id);
    let escaped_agent_did = escape_graphql_string(agent_did);
    let escaped_behavior_id = escape_graphql_string(behavior_id);
    let escaped_session_id = escape_graphql_string(&session_id);
    let prompt_selection = crate::skills::prompt_slash_skill_selection(content);
    let content = prompt_selection.prompt.as_str();
    let escaped_content = escape_graphql_string(content);
    let escaped_created_at = escape_graphql_string(&now);
    let execution_origin = execution_origin.as_str();
    let lineage_fields = trigger_lineage_graphql_fields(&trigger_lineage)?;
    let workspace_fields = workspace_lineage_graphql_fields(workspace_lineage);
    let metadata_field = if prompt_selection.selected_skill_ids.is_empty() {
        String::new()
    } else {
        let metadata = serde_json::json!({
            "selected_skill_ids": prompt_selection.selected_skill_ids,
        })
        .to_string();
        format!(
            r#"
                metadata: "{}","#,
            escape_graphql_string(&metadata)
        )
    };
    let conversation_title = conversation_title.and_then(|title| {
        let title = title.trim();
        (!title.is_empty()).then(|| title.to_string())
    });

    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{escaped_request_id}",
                agent_did: "{escaped_agent_did}",
                behavior_id: "{escaped_behavior_id}",
                session_id: "{escaped_session_id}",
                retry_parent_request: "",
                retry_root_request: "{escaped_request_id}",
                superseded_by_request: "",
                content: "{escaped_content}",{metadata_field}
                status: "pending",
                lifecycle_state: "pending",
                backend_id: "",
                execution_origin: "{execution_origin}",{lineage_fields}{workspace_fields}
                failure_reason: "",
                created_at: "{escaped_created_at}",
                retry_count: 0,
                max_retries: {max_retries}
            }}) {{ _docID }}
        }}"#,
        max_retries = DEFAULT_REQUEST_MAX_RETRIES,
    );

    // A trigger fire is not replayable: `event_kind: created` is first-seen, so
    // dropping this create on a transient conflict loses the stage for good.
    let response = crate::graphql::graphql_mutation_with_transaction_retry(
        node,
        &mutation,
        "materialize_pending_agent_request",
    )
    .await?;

    let doc_id = resolve_created_agent_request_doc_id(
        node,
        &response,
        "create_AgentRequest",
        &escaped_request_id,
        "querying created pending AgentRequest doc id failed",
        "pending AgentRequest create returned no _docID",
    )
    .await?;

    if let Some(title) = conversation_title {
        let seed_result = async {
            session::ensure_session_with_behavior_id(
                node,
                &session_id,
                behavior_id,
                agent_did,
                behavior_id,
            )
            .await?;
            session::upsert_conversation_from_request_with_identity_and_title(
                node,
                &session_id,
                behavior_id,
                agent_did,
                behavior_id,
                &request_id,
                content,
                "pending",
                None,
                Some((&title, session::CONVERSATION_TITLE_SOURCE_TASK)),
            )
            .await
        }
        .await;

        if let Err(error) = seed_result {
            tracing::warn!(
                request_id = %request_id,
                session_id = %session_id,
                title = %title,
                error = %error,
                "failed to seed task conversation title"
            );
        }
    }

    Ok(EnqueuedAgentRequest {
        doc_id,
        request_id,
        session_id,
    })
}

impl RequestLifecycle {
    pub fn new_with_agent_did(
        node: Arc<EmbeddedNode>,
        agent_name: &str,
        agent_did: &str,
        request: AgentRequest,
        deadline_duration_secs: u64,
    ) -> Self {
        Self::new_with_execution_binding(
            node,
            agent_name,
            agent_did,
            request,
            deadline_duration_secs,
            ExecutionOrigin::Interactive,
            "",
        )
    }

    pub fn new_with_execution_binding(
        node: Arc<EmbeddedNode>,
        agent_name: &str,
        agent_did: &str,
        request: AgentRequest,
        deadline_duration_secs: u64,
        execution_origin: ExecutionOrigin,
        backend_id: impl Into<String>,
    ) -> Self {
        let behavior_id = resolve_behavior_id(agent_name, request.behavior_id.as_deref());
        Self {
            node,
            agent_name: agent_name.to_string(),
            agent_did: agent_did.to_string(),
            behavior_id,
            execution_origin,
            backend_id: backend_id.into(),
            failure_reason: None,
            request,
            request_commit_cid: None,
            response_doc_id: None,
            progress_seq: 0,
            deadline_duration_secs,
            claimed_deadline_at: None,
            background_completion_input_through_sequence: None,
            state: LocalLifecycleState::Pending,
            valid_until_at_claim: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn materialize_claimed_with_execution_binding(
        node: Arc<EmbeddedNode>,
        agent_name: &str,
        agent_did: &str,
        content: &str,
        deadline_duration_secs: u64,
        execution_origin: ExecutionOrigin,
        backend_id: impl Into<String>,
        trigger_lineage: TriggerLineage,
    ) -> Result<Self> {
        let backend_id = backend_id.into();
        let behavior_id = agent_name.to_string();
        let request_id = uuid::Uuid::new_v4().to_string();
        let session_id = uuid::Uuid::new_v4().to_string();
        let now = chrono::Utc::now();
        let created_at = now.to_rfc3339();
        let claimed_at = created_at.clone();
        let deadline_at = now + chrono::Duration::seconds(deadline_duration_secs as i64);
        let deadline = deadline_at.to_rfc3339();
        let lineage_fields = trigger_lineage_graphql_fields(&trigger_lineage)?;

        session::create_session_with_behavior_id(
            node.as_ref(),
            &session_id,
            agent_name,
            agent_did,
            &behavior_id,
        )
        .await?;

        let escaped_request_id = escape_graphql_string(&request_id);
        let escaped_agent_did = escape_graphql_string(agent_did);
        let escaped_behavior_id = escape_graphql_string(&behavior_id);
        let escaped_session_id = escape_graphql_string(&session_id);
        let escaped_content = escape_graphql_string(content);
        let escaped_backend_id = escape_graphql_string(&backend_id);
        let escaped_retry_root_request = graphql_retry_root_request(None, &request_id);
        let escaped_created_at = escape_graphql_string(&created_at);
        let escaped_claimed_at = escape_graphql_string(&claimed_at);
        let escaped_deadline = escape_graphql_string(&deadline);
        let execution_origin_str = execution_origin.as_str();
        let mutation = format!(
            r#"mutation {{
                add_AgentRequest(input: {{
                    request_id: "{escaped_request_id}",
                    agent_did: "{escaped_agent_did}",
                    behavior_id: "{escaped_behavior_id}",
                    session_id: "{escaped_session_id}",
                    retry_parent_request: "",
                    retry_root_request: "{escaped_retry_root_request}",
                    superseded_by_request: "",
                    content: "{escaped_content}",
                    status: "processing",
                    lifecycle_state: "{lifecycle_state}",
                    backend_id: "{escaped_backend_id}",
                    execution_origin: "{execution_origin_str}",{lineage_fields}
                    failure_reason: "",
                    created_at: "{escaped_created_at}",
                    claimed_at: "{escaped_claimed_at}",
                    deadline: "{escaped_deadline}",
                    retry_count: 0,
                    max_retries: {max_retries}
                }}) {{
                    _docID
                    _version {{ cid height fieldName }}
                }}
            }}"#,
            lifecycle_state = PersistedLifecycleState::Claimed.as_str(),
            max_retries = DEFAULT_REQUEST_MAX_RETRIES,
        );

        let resp = crate::graphql::graphql_mutation_with_transaction_retry(
            node.as_ref(),
            &mutation,
            "materialize_claimed_agent_request",
        )
        .await?;

        let doc_id = resolve_created_agent_request_doc_id(
            node.as_ref(),
            &resp,
            "add_AgentRequest",
            &escaped_request_id,
            "querying created AgentRequest doc id failed",
            "add_AgentRequest returned no _docID",
        )
        .await?;
        let request_commit_cid =
            crate::graphql::mutation_composite_version(&resp, "add_AgentRequest")?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "materialized AgentRequest {doc_id} returned no composite version"
                    )
                })?
                .cid;

        let request = AgentRequest {
            doc_id,
            request_id: request_id.clone(),
            agent_did: agent_did.to_string(),
            requester_did: None,
            behavior_id: Some(behavior_id.clone()),
            session_id: session_id.clone(),
            content: content.to_string(),
            temperature: None,
            top_p: None,
            top_k: None,
            seed: None,
            max_tokens: None,
            max_total_tokens: None,
            metadata: None,
            execution_origin: Some(execution_origin_str.to_string()),
            created_at,
            deadline: Some(deadline),
            subagent_depth: 0,
            caused_by_parent_request_id: None,
            caused_by_parent_request_doc_id: None,
            caused_by_parent_tool_call_id: None,
            caused_by_parent_tool_call_doc_id: None,
            caused_by_trigger_id: None,
            caused_by_trigger_kind: None,
            caused_by_source_doc_id: None,
            caused_by_correlation: None,
            caused_by_trigger_context: None,
            workspace_id: None,
            workspace_authority: None,
            workspace_owner_deployment_id: None,
            workspace_seal_hash: None,
        };

        session::upsert_conversation_from_request_with_identity(
            node.as_ref(),
            &session_id,
            agent_name,
            agent_did,
            &behavior_id,
            &request_id,
            content,
            "processing",
        )
        .await?;

        Ok(Self {
            node,
            agent_name: agent_name.to_string(),
            agent_did: agent_did.to_string(),
            behavior_id,
            execution_origin,
            backend_id,
            failure_reason: None,
            request,
            request_commit_cid: Some(request_commit_cid),
            response_doc_id: None,
            progress_seq: 0,
            deadline_duration_secs,
            claimed_deadline_at: Some(deadline_at),
            background_completion_input_through_sequence: None,
            state: LocalLifecycleState::Claimed,
            valid_until_at_claim: None,
        })
    }

    pub fn request(&self) -> &AgentRequest {
        &self.request
    }

    pub fn response_doc_id(&self) -> Option<&str> {
        self.response_doc_id.as_deref()
    }

    pub fn backend_id(&self) -> &str {
        &self.backend_id
    }

    pub fn behavior_id(&self) -> &str {
        &self.behavior_id
    }

    pub async fn prepare_session_with_identity(&self) -> Result<()> {
        self.ensure_state(&[LocalLifecycleState::Claimed], "prepare_session")?;
        session::ensure_session_with_behavior_id_and_requester_did(
            &self.node,
            &self.request.session_id,
            &self.agent_name,
            &self.agent_did,
            &self.behavior_id,
            self.request.requester_did.as_deref(),
        )
        .await?;
        session::upsert_conversation_from_request_with_identity_and_requester_did(
            &self.node,
            &self.request.session_id,
            &self.agent_name,
            &self.agent_did,
            &self.behavior_id,
            &self.request.request_id,
            &self.request.content,
            "processing",
            self.request.requester_did.as_deref(),
        )
        .await
    }
}
