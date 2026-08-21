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
    if trigger_lineage.trigger_kind.as_deref() == Some("manual")
        && trigger_lineage.trigger_id.is_some()
    {
        anyhow::bail!("Manual trigger enqueue must not carry trigger_id");
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
    let conversation_title = conversation_title.and_then(|title| {
        let title = title.trim();
        (!title.is_empty()).then(|| title.to_string())
    });
    let mut metadata = serde_json::Map::new();
    if !prompt_selection.selected_skill_ids.is_empty() {
        metadata.insert(
            "selected_skill_ids".to_string(),
            serde_json::json!(prompt_selection.selected_skill_ids),
        );
    }
    if let Some(title) = conversation_title.as_deref() {
        metadata.insert(
            "conversation_title".to_string(),
            serde_json::Value::String(title.to_string()),
        );
    }
    let metadata_field = if metadata.is_empty() {
        String::new()
    } else {
        let metadata = serde_json::Value::Object(metadata).to_string();
        format!(
            r#"
                metadata: "{}","#,
            escape_graphql_string(&metadata)
        )
    };
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
                execution_origin: "{execution_origin}",{lineage_fields}
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

        let request = AgentRequest {
            doc_id: String::new(),
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
            created_at: created_at.clone(),
            deadline: Some(deadline.clone()),
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
        };
        let projection = request_session_projection_mutation(
            &request,
            agent_name,
            agent_did,
            &behavior_id,
            &claimed_at,
        );
        let resp =
            materialize_claimed_request_with_projection(node.as_ref(), &mutation, &projection)
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

        let request = AgentRequest { doc_id, ..request };

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
}

fn conversation_title_from_metadata(metadata: Option<&str>) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(metadata?)
        .ok()?
        .get("conversation_title")?
        .as_str()
        .map(str::trim)
        .filter(|title| !title.is_empty())
        .map(str::to_string)
}

pub(super) fn request_session_projection_mutation(
    request: &AgentRequest,
    agent_name: &str,
    agent_did: &str,
    behavior_id: &str,
    started: &str,
) -> String {
    let session_field = session::request_session_projection_field(
        &request.session_id,
        agent_name,
        agent_did,
        behavior_id,
        request.requester_did.as_deref(),
        started,
    );
    let title = conversation_title_from_metadata(request.metadata.as_deref());
    let conversation_field = session::request_conversation_projection_field(
        &request.session_id,
        agent_name,
        agent_did,
        behavior_id,
        &request.request_id,
        &request.content,
        "processing",
        request.requester_did.as_deref(),
        title
            .as_deref()
            .map(|title| (title, session::CONVERSATION_TITLE_SOURCE_TASK)),
        started,
    );
    format!("mutation {{ {session_field} {conversation_field} }}")
}

async fn materialize_claimed_request_with_projection(
    node: &EmbeddedNode,
    request_mutation: &str,
    projection_mutation: &str,
) -> Result<defra_node::QueryResponse> {
    let mut last_error = None;
    for retry_index in 0..=crate::graphql::DEFRA_DB_CONFLICT_MAX_RETRIES {
        let txn = crate::config_client::ConfigApplyTxn::begin_local(node, None).await?;
        let attempt = async {
            let created = txn.execute_local_response(request_mutation).await?;
            if !created
                .data
                .as_ref()
                .and_then(|data| data.get("add_AgentRequest"))
                .is_some_and(response_has_documents)
            {
                anyhow::bail!("materialized AgentRequest mutation returned no document");
            }
            txn.execute_local_response(projection_mutation).await?;
            Ok::<_, anyhow::Error>(created)
        }
        .await;
        let result = match attempt {
            Ok(created) => txn.commit().await.map(|()| created),
            Err(error) => {
                let _ = txn.discard().await;
                Err(error)
            }
        };
        match result {
            Ok(created) => return Ok(created),
            Err(error)
                if retry_index < crate::graphql::DEFRA_DB_CONFLICT_MAX_RETRIES
                    && crate::graphql::is_defradb_transaction_conflict_text(
                        &error.to_string().to_ascii_lowercase(),
                    ) =>
            {
                last_error = Some(error);
                tokio::time::sleep(crate::graphql::defradb_conflict_retry_backoff(retry_index))
                    .await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error
        .unwrap_or_else(|| anyhow::anyhow!("materialize claimed request transaction exhausted")))
}
