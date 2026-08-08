use super::*;

#[derive(Debug, Clone)]
pub struct EnqueuedAgentRequest {
    pub doc_id: String,
    pub request_id: String,
    pub session_id: String,
}

fn trigger_lineage_graphql_fields(trigger_lineage: &TriggerLineage) -> String {
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
    format!("{caused_by_trigger_id_field}{caused_by_trigger_kind_field}")
}

async fn resolve_created_agent_request_doc_id(
    node: &EmbeddedNode,
    identity: identity::Did,
    _mutation_response: &defra_node::QueryResponse,
    _mutation_field: &str,
    escaped_request_id: &str,
    lookup_error: &str,
    missing_doc_id_error: &str,
) -> Result<String> {
    let query = format!(
        r#"{{ AgentRequest(filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }}) {{ _docID }} }}"#
    );
    let query_resp = node
        .execute_request_with_retry(
            defra_node::QueryRequest::new(query).with_identity(Some(identity)),
            defra_node::ExecuteRetryPolicy::default(),
        )
        .await;
    if query_resp.has_errors() {
        anyhow::bail!("{lookup_error}: {:?}", query_resp.errors);
    }

    let rows = query_resp
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|value| value.as_array())
        .ok_or_else(|| anyhow::anyhow!("{missing_doc_id_error}"))?;
    match rows.as_slice() {
        [row] => row
            .get("_docID")
            .and_then(|doc_id| doc_id.as_str())
            .ok_or_else(|| anyhow::anyhow!("{missing_doc_id_error}"))
            .map(str::to_string),
        [] => anyhow::bail!("{missing_doc_id_error}"),
        rows => anyhow::bail!(
            "AgentRequest request_id lookup observed {} logical twins",
            rows.len()
        ),
    }
}

pub(crate) async fn write_pending_agent_request_with_lineage_and_conversation_title(
    node: &EmbeddedNode,
    agent_did: &str,
    behavior_id: &str,
    content: &str,
    execution_origin: ExecutionOrigin,
    trigger_lineage: TriggerLineage,
    conversation_title: Option<&str>,
    requested_request_id: Option<&str>,
) -> Result<EnqueuedAgentRequest> {
    if trigger_lineage.trigger_kind.as_deref() == Some("manual")
        && trigger_lineage.trigger_id.is_some()
    {
        anyhow::bail!("Manual trigger enqueue must not carry trigger_id");
    }

    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let request_id = requested_request_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let session_id = uuid::Uuid::new_v4().to_string();

    let escaped_request_id = escape_graphql_string(&request_id);
    let escaped_agent_did = escape_graphql_string(agent_did);
    let source_author_did = node.node_identity_did().ok_or_else(|| {
        anyhow::anyhow!("pending AgentRequest creation requires a configured node signing identity")
    })?;
    let escaped_source_author_did = escape_graphql_string(source_author_did);
    let escaped_behavior_id = escape_graphql_string(behavior_id);
    let escaped_session_id = escape_graphql_string(&session_id);
    let prompt_selection = crate::skills::prompt_slash_skill_selection(content);
    let content = prompt_selection.prompt.as_str();
    let escaped_content = escape_graphql_string(content);
    let query_identity = identity::Did::new(source_author_did)?;

    if requested_request_id.is_some() {
        let existing_query = format!(
            r#"{{ AgentRequest(filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }}) {{
                _docID request_id agent_did behavior_id session_id content
                caused_by_trigger_id caused_by_trigger_kind
            }} }}"#
        );
        let existing = node
            .execute_request_with_retry(
                defra_node::QueryRequest::new(existing_query)
                    .with_identity(Some(query_identity.clone())),
                defra_node::ExecuteRetryPolicy::default(),
            )
            .await;
        if existing.has_errors() {
            anyhow::bail!(
                "query deterministic AgentRequest failed: {:?}",
                existing.errors
            );
        }
        let rows = existing
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| anyhow::anyhow!("deterministic AgentRequest query returned no rows"))?;
        match rows.as_slice() {
            [row] => {
                let matches = row.get("request_id").and_then(serde_json::Value::as_str)
                    == Some(request_id.as_str())
                    && row.get("agent_did").and_then(serde_json::Value::as_str) == Some(agent_did)
                    && row.get("behavior_id").and_then(serde_json::Value::as_str)
                        == Some(behavior_id)
                    && row.get("content").and_then(serde_json::Value::as_str) == Some(content)
                    && row
                        .get("caused_by_trigger_id")
                        .and_then(serde_json::Value::as_str)
                        == trigger_lineage.trigger_id.as_deref()
                    && row
                        .get("caused_by_trigger_kind")
                        .and_then(serde_json::Value::as_str)
                        == trigger_lineage.trigger_kind.as_deref();
                if !matches {
                    anyhow::bail!("deterministic AgentRequest {request_id} conflicts with the requested materialization");
                }
                return Ok(EnqueuedAgentRequest {
                    doc_id: row
                        .get("_docID")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| anyhow::anyhow!("deterministic AgentRequest has no _docID"))?
                        .to_owned(),
                    request_id,
                    session_id: row
                        .get("session_id")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| {
                            anyhow::anyhow!("deterministic AgentRequest has no session_id")
                        })?
                        .to_owned(),
                });
            }
            [] => {}
            rows => anyhow::bail!(
                "deterministic AgentRequest {request_id} has {} visible logical twins",
                rows.len()
            ),
        }
    }
    let escaped_created_at = escape_graphql_string(&now);
    let execution_origin = execution_origin.as_str();
    let lineage_fields = trigger_lineage_graphql_fields(&trigger_lineage);
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
                source_author_did: "{escaped_source_author_did}",
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

    let response = node
        .execute_request_with_retry(
            defra_node::QueryRequest::new(mutation).with_identity(Some(query_identity.clone())),
            defra_node::ExecuteRetryPolicy::default(),
        )
        .await;
    if response.has_errors() {
        anyhow::bail!(
            "create pending AgentRequest with trigger lineage failed: {:?}",
            response.errors
        );
    }

    let doc_id = resolve_created_agent_request_doc_id(
        node,
        query_identity,
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
            request_version: None,
            execution_provenance: None,
            response_doc_id: None,
            progress_seq: 0,
            deadline_duration_secs,
            claimed_deadline_at: None,
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
        let enqueued = write_pending_agent_request_with_lineage_and_conversation_title(
            node.as_ref(),
            agent_did,
            &behavior_id,
            content,
            execution_origin,
            trigger_lineage,
            None,
            None,
        )
        .await?;
        let request = crate::watcher::DefraWatcher::new(Arc::clone(&node), agent_did)
            .try_fetch_request(&enqueued.doc_id)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "new pending AgentRequest {} was not claimable",
                    enqueued.request_id
                )
            })?;
        let mut lifecycle = Self::new_with_execution_binding(
            node,
            agent_name,
            agent_did,
            request,
            deadline_duration_secs,
            execution_origin,
            backend_id,
        );
        match lifecycle.claim_with_identity().await? {
            ClaimOutcome::Claimed => {}
            outcome => anyhow::bail!(
                "new pending AgentRequest {} was not claimed: {outcome:?}",
                enqueued.request_id
            ),
        }
        lifecycle.prepare_session_with_identity().await?;
        Ok(lifecycle)
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
        self.require_execution_provenance("prepare the execution session")?;
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
