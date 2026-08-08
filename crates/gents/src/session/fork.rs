use anyhow::{Context, Result};
use async_trait::async_trait;
use defra_node::{EmbeddedNode, ExecuteRetryPolicy, QueryRequest};
use gents_protocol::graphql::{execute_graphql_async, GraphqlRequestOptions};
use identity::Did;
use serde_json::Value;
use std::collections::{HashMap, HashSet};

use super::retry::log_mutation_timing;
use super::{CompactionFactRef, CompactionSourceManifest};
use crate::graphql::escape_graphql_string;
use crate::lifecycle::active_runtime_lifecycle_state_graphql_list;
use crate::retry::{
    defradb_conflict_retry_backoff, is_defradb_transaction_conflict_text,
    DEFRA_DB_CONFLICT_MAX_RETRIES,
};

#[derive(Debug, Clone)]
pub struct GraphqlExecuteResponse {
    pub data: Option<Value>,
    pub errors: Vec<Value>,
}

impl GraphqlExecuteResponse {
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    fn from_http_value(value: Value) -> Self {
        let data = value.get("data").cloned();
        let errors = value
            .get("errors")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        Self { data, errors }
    }

    fn from_embedded(response: defra_node::QueryResponse) -> Self {
        let errors = response
            .errors
            .into_iter()
            .map(|error| {
                serde_json::to_value(error)
                    .unwrap_or_else(|_| Value::String("GraphQL error".to_string()))
            })
            .collect();
        Self {
            data: response.data,
            errors,
        }
    }
}

#[async_trait]
pub trait GraphqlExecutor: Send + Sync {
    async fn execute_graphql(&self, query: &str) -> Result<GraphqlExecuteResponse>;

    async fn verified_signer_did(&self, cid: &str, projected: Option<&str>) -> Result<String>;

    fn node_identity_did(&self) -> Option<&str> {
        None
    }
}

#[async_trait]
impl GraphqlExecutor for EmbeddedNode {
    async fn execute_graphql(&self, query: &str) -> Result<GraphqlExecuteResponse> {
        let identity = EmbeddedNode::node_identity_did(self)
            .map(Did::new)
            .transpose()
            .context("parsing fork node query identity")?;
        Ok(GraphqlExecuteResponse::from_embedded(
            self.execute_request_with_retry(
                QueryRequest::new(query).with_identity(identity),
                ExecuteRetryPolicy::default(),
            )
            .await,
        ))
    }

    async fn verified_signer_did(&self, cid: &str, _projected: Option<&str>) -> Result<String> {
        self.verified_block_signer_did(cid).await
    }

    fn node_identity_did(&self) -> Option<&str> {
        EmbeddedNode::node_identity_did(self)
    }
}

#[derive(Debug, Clone)]
pub struct HttpGraphqlExecutor {
    endpoint: String,
    options: GraphqlRequestOptions,
}

impl HttpGraphqlExecutor {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            options: GraphqlRequestOptions::default(),
        }
    }

    pub fn with_options(endpoint: impl Into<String>, options: GraphqlRequestOptions) -> Self {
        Self {
            endpoint: endpoint.into(),
            options,
        }
    }
}

#[async_trait]
impl GraphqlExecutor for HttpGraphqlExecutor {
    async fn execute_graphql(&self, query: &str) -> Result<GraphqlExecuteResponse> {
        let value = execute_graphql_async(&self.endpoint, query, self.options).await?;
        Ok(GraphqlExecuteResponse::from_http_value(value))
    }

    async fn verified_signer_did(&self, cid: &str, projected: Option<&str>) -> Result<String> {
        projected
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow::anyhow!("remote commit {cid} has no verified signer projection"))
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
struct ForkCommitParent {
    cid: String,
    #[serde(rename = "fieldName")]
    field_name: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct ForkCommitSignature {
    identity: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct ForkCommitRow {
    cid: String,
    #[serde(default)]
    heads: Vec<ForkCommitParent>,
    signature: Option<ForkCommitSignature>,
}

async fn exact_current_ref(
    executor: &(impl GraphqlExecutor + ?Sized),
    collection: &str,
    doc_id: &str,
) -> Result<crate::SignedDocumentVersionRef> {
    let response = executor
        .execute_graphql(&format!(
            r#"{{ _commits(docID: ["{}"], filter: {{ fieldName: {{ _eq: "_C" }} }}) {{ cid heads {{ cid fieldName }} signature {{ identity }} }} }}"#,
            escape_graphql_string(doc_id)
        ))
        .await?;
    if response.has_errors() {
        anyhow::bail!(
            "loading exact {collection} commit evidence failed: {}",
            render_graphql_errors(&response)
        );
    }
    let commits: Vec<ForkCommitRow> = serde_json::from_value(
        response
            .data
            .as_ref()
            .and_then(|data| data.get("_commits"))
            .cloned()
            .unwrap_or_default(),
    )?;
    let nested = commits
        .iter()
        .flat_map(|commit| commit.heads.iter())
        .filter(|head| head.field_name.as_deref() == Some("_C"))
        .map(|head| head.cid.as_str())
        .collect::<HashSet<_>>();
    let current = commits
        .iter()
        .filter(|commit| !nested.contains(commit.cid.as_str()))
        .collect::<Vec<_>>();
    let [current] = current.as_slice() else {
        anyhow::bail!(
            "{collection} {doc_id} has {} current composite heads",
            current.len()
        );
    };
    let signer = executor
        .verified_signer_did(
            &current.cid,
            current
                .signature
                .as_ref()
                .map(|signature| signature.identity.as_str()),
        )
        .await?;
    Ok(crate::SignedDocumentVersionRef::new(
        crate::DocumentVersionRef::new(doc_id, &current.cid),
        signer,
    ))
}

async fn exact_snapshot(
    executor: &(impl GraphqlExecutor + ?Sized),
    collection: &str,
    source: &crate::SignedDocumentVersionRef,
    fields: &str,
) -> Result<Value> {
    let verified = executor
        .verified_signer_did(
            &source.version.composite_commit_cid,
            Some(&source.signer_did),
        )
        .await?;
    if verified != source.signer_did {
        anyhow::bail!("{collection} exact source signer does not verify");
    }
    let response = executor
        .execute_graphql(&format!(
            r#"{{ {collection}(cid: ["{}"]) {{ _docID {fields} }} }}"#,
            escape_graphql_string(&source.version.composite_commit_cid)
        ))
        .await?;
    if response.has_errors() {
        anyhow::bail!(
            "loading exact {collection} source failed: {}",
            render_graphql_errors(&response)
        );
    }
    let rows = graphql_rows(&response, collection);
    match rows.as_slice() {
        [row]
            if row.get("_docID").and_then(Value::as_str)
                == Some(source.version.doc_id.as_str()) =>
        {
            Ok(row.clone())
        }
        rows => anyhow::bail!(
            "{collection} exact source reconstructed {} rows or a different document",
            rows.len()
        ),
    }
}

fn fork_source_fields(source: &crate::SignedDocumentVersionRef) -> String {
    format!(
        r#"fork_source_doc_id: "{}",
            fork_source_composite_commit_cid: "{}",
            fork_source_signer_did: "{}","#,
        escape_graphql_string(&source.version.doc_id),
        escape_graphql_string(&source.version.composite_commit_cid),
        escape_graphql_string(&source.signer_did),
    )
}

async fn verify_child_ref(
    executor: &(impl GraphqlExecutor + ?Sized),
    collection: &str,
    doc_id: &str,
    node_did: Option<&str>,
) -> Result<crate::SignedDocumentVersionRef> {
    let child = exact_current_ref(executor, collection, doc_id).await?;
    if node_did.is_some_and(|node_did| child.signer_did != node_did) {
        anyhow::bail!(
            "forked {collection} signer {} does not match node identity {}",
            child.signer_did,
            node_did.unwrap_or_default()
        );
    }
    Ok(child)
}

fn mutation_doc_id(response: &GraphqlExecuteResponse, field: &str) -> Result<String> {
    let data = response
        .data
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("{field} returned no mutation data"))?;
    // DefraDB currently exposes `create_Foo` in the schema but serializes the
    // response under its underlying `add_Foo` resolver name. Accept both
    // spellings while still requiring one physical result row.
    let add_field = field
        .strip_prefix("create_")
        .map(|collection| format!("add_{collection}"));
    let value = data
        .get(field)
        .or_else(|| add_field.as_deref().and_then(|field| data.get(field)))
        .ok_or_else(|| anyhow::anyhow!("{field} returned no mutation payload: data={}", data))?;
    if let Some(doc_id) = value.get("_docID").and_then(Value::as_str) {
        return Ok(doc_id.to_owned());
    }
    let rows = value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("{field} returned an unknown mutation payload shape"))?;
    match rows.as_slice() {
        [row] => row
            .get("_docID")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow::anyhow!("{field} returned no physical _docID")),
        rows => anyhow::bail!(
            "{field} returned {} physical rows; expected exactly one",
            rows.len()
        ),
    }
}

fn reject_logical_twins(rows: &[Value], field: &str, label: &str) -> Result<()> {
    let mut seen = HashSet::new();
    for row in rows {
        let key = row
            .get(field)
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("{label} row missing {field}"))?;
        if !seen.insert(key) {
            anyhow::bail!("{label} contains replicated logical twins for {field}={key}");
        }
    }
    Ok(())
}

async fn require_child_key_absent(
    executor: &(impl GraphqlExecutor + ?Sized),
    collection: &str,
    field: &str,
    value: &str,
) -> Result<()> {
    let response = executor
        .execute_graphql(&format!(
            r#"{{ {collection}(filter: {{ {field}: {{ _eq: "{}" }} }}) {{ _docID }} }}"#,
            escape_graphql_string(value)
        ))
        .await?;
    if response.has_errors() {
        anyhow::bail!(
            "enumerating fork target {collection} failed: {}",
            render_graphql_errors(&response)
        );
    }
    let rows = graphql_rows(&response, collection);
    if !rows.is_empty() {
        anyhow::bail!(
            "fork target {collection} logical key has {} pre-existing physical rows",
            rows.len()
        );
    }
    Ok(())
}

async fn require_sole_child_key(
    executor: &(impl GraphqlExecutor + ?Sized),
    collection: &str,
    field: &str,
    value: &str,
    expected_doc_id: &str,
) -> Result<()> {
    let response = executor
        .execute_graphql(&format!(
            r#"{{ {collection}(filter: {{ {field}: {{ _eq: "{}" }} }}) {{ _docID }} }}"#,
            escape_graphql_string(value)
        ))
        .await?;
    if response.has_errors() {
        anyhow::bail!(
            "re-enumerating fork target {collection} failed: {}",
            render_graphql_errors(&response)
        );
    }
    let rows = graphql_rows(&response, collection);
    match rows.as_slice() {
        [row]
            if row.get("_docID").and_then(Value::as_str) == Some(expected_doc_id) => Ok(()),
        rows => anyhow::bail!(
            "fork target {collection} logical key resolved to {} physical twins or another document",
            rows.len()
        ),
    }
}

fn optional_exact_ref(
    row: &Value,
    doc_field: &str,
    cid_field: &str,
    signer_field: &str,
    label: &str,
) -> Result<Option<crate::SignedDocumentVersionRef>> {
    let doc_id = row.get(doc_field).and_then(Value::as_str);
    let cid = row.get(cid_field).and_then(Value::as_str);
    let signer = row.get(signer_field).and_then(Value::as_str);
    match (doc_id, cid, signer) {
        (None, None, None) => Ok(None),
        (Some(doc_id), Some(cid), Some(signer))
            if !doc_id.trim().is_empty() && !cid.trim().is_empty() && !signer.trim().is_empty() =>
        {
            Ok(Some(crate::SignedDocumentVersionRef::new(
                crate::DocumentVersionRef::new(doc_id, cid),
                signer,
            )))
        }
        _ => anyhow::bail!("{label} exact source reference is partial or empty"),
    }
}

async fn attach_child_tool_fact(
    executor: &(impl GraphqlExecutor + ?Sized),
    child_call_doc_id: &str,
    kind: &str,
    fact: &crate::SignedDocumentVersionRef,
) -> Result<()> {
    let (doc_field, cid_field, signer_field) = match kind {
        "result" => (
            "result_doc_id",
            "result_composite_commit_cid",
            "result_signer_did",
        ),
        "approval" => (
            "approval_doc_id",
            "approval_composite_commit_cid",
            "approval_signer_did",
        ),
        _ => anyhow::bail!("unsupported child tool fact kind {kind}"),
    };
    let mutation = format!(
        r#"mutation {{ update_AgentToolCall(filter: {{ _docID: {{ _eq: "{}" }} }}, input: {{ {doc_field}: "{}", {cid_field}: "{}", {signer_field}: "{}" }}) {{ _docID }} }}"#,
        escape_graphql_string(child_call_doc_id),
        escape_graphql_string(&fact.version.doc_id),
        escape_graphql_string(&fact.version.composite_commit_cid),
        escape_graphql_string(&fact.signer_did),
    );
    let response =
        execute_mutation_with_retry(executor, &mutation, &format!("fork::attach_child_{kind}"))
            .await?;
    let updated = response
        .data
        .as_ref()
        .and_then(|data| data.get("update_AgentToolCall"))
        .is_some_and(|value| match value {
            Value::Object(row) => row.get("_docID").is_some(),
            Value::Array(rows) => rows.len() == 1 && rows[0].get("_docID").is_some(),
            _ => false,
        });
    if !updated {
        anyhow::bail!("attaching child {kind} did not update exactly one call row");
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct ForkParentConversation {
    behavior_id: Option<String>,
    agent_did: Option<String>,
    agent_name: Option<String>,
    requester_did: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ForkParams<'a> {
    pub source_session_id: &'a str,
    pub fork_at_user_turn: u32,
    pub caller_agent_did: &'a str,
    pub target_behavior_id: Option<&'a str>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ForkOutcome {
    pub session_id: String,
    pub copied_messages: u32,
    pub copied_tool_calls: u32,
    pub copied_tool_results: u32,
    pub copied_tool_approvals: u32,
    pub copied_compaction_entries: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum ForkError {
    #[error("fork source not found: session_id={0}")]
    ForkSourceNotFound(String),
    #[error("fork source's agent_did does not match caller")]
    ForkNotSameAgent,
    #[error("fork source has an active runtime AgentRequest and is busy")]
    ForkSourceBusy,
    #[error("fork_at_user_turn={0} is out of range (parent has only {1} user messages)")]
    ForkAtUserTurnOutOfRange(u32, u32),
    #[error("target behavior not found: {0}")]
    ForkBehaviorNotFound(String),
    #[error("target behavior {0} is not owned by principal {1}")]
    ForkBehaviorNotOwnedByPrincipal(String, String),
    #[error("fork copy step failed: {0}")]
    ForkCopyFailed(#[from] anyhow::Error),
}

async fn load_parent_conversation(
    executor: &(impl GraphqlExecutor + ?Sized),
    source_session_id: &str,
) -> Result<Option<ForkParentConversation>> {
    let escaped = escape_graphql_string(source_session_id);
    let query = format!(
        r#"{{
            AgentConversation(
                filter: {{
                    session_id: {{ _eq: "{escaped}" }}
                }}
            ) {{
                _docID
                behavior_id
                agent_did
                agent_name
                requester_did
            }}
        }}"#
    );
    let resp = executor.execute_graphql(&query).await?;
    if resp.has_errors() {
        anyhow::bail!(
            "loading conversation document for session_id={}: {}",
            source_session_id,
            render_graphql_errors(&resp)
        );
    }

    let rows = graphql_rows(&resp, "AgentConversation");
    let row = match rows.as_slice() {
        [] => return Ok(None),
        [row] => row,
        rows => anyhow::bail!(
            "source session resolves to {} physical AgentConversation twins",
            rows.len()
        ),
    };
    Ok(Some(ForkParentConversation {
        behavior_id: row
            .get("behavior_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        agent_did: row
            .get("agent_did")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        agent_name: row
            .get("agent_name")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        requester_did: row
            .get("requester_did")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
    }))
}

async fn verify_source_idle(
    executor: &(impl GraphqlExecutor + ?Sized),
    source_session_id: &str,
) -> Result<bool> {
    let escaped = escape_graphql_string(source_session_id);
    let active_runtime_states = active_runtime_lifecycle_state_graphql_list();
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{
                    session_id: {{ _eq: "{escaped}" }},
                    lifecycle_state: {{ _in: {active_runtime_states} }}
                }},
                limit: 1
            ) {{ request_id }}
        }}"#
    );
    let resp = executor.execute_graphql(&query).await?;
    if resp.has_errors() {
        anyhow::bail!(
            "verify_source_idle query failed: {}",
            render_graphql_errors(&resp)
        );
    }
    let rows = graphql_rows(&resp, "AgentRequest");
    Ok(rows.is_empty())
}

pub async fn fork(node: &EmbeddedNode, params: ForkParams<'_>) -> Result<ForkOutcome, ForkError> {
    fork_with_executor(node, params).await
}

pub async fn fork_via_http(
    graphql_endpoint: &str,
    params: ForkParams<'_>,
) -> Result<ForkOutcome, ForkError> {
    let executor = HttpGraphqlExecutor::new(graphql_endpoint);
    fork_with_executor(&executor, params).await
}

async fn fork_with_executor(
    executor: &(impl GraphqlExecutor + ?Sized),
    params: ForkParams<'_>,
) -> Result<ForkOutcome, ForkError> {
    let parent = load_parent_conversation(executor, params.source_session_id)
        .await
        .map_err(ForkError::ForkCopyFailed)?
        .ok_or_else(|| ForkError::ForkSourceNotFound(params.source_session_id.to_string()))?;

    let parent_agent_did = parent.agent_did.as_deref().unwrap_or("");
    if parent_agent_did.is_empty() {
        return Err(ForkError::ForkSourceNotFound(
            params.source_session_id.to_string(),
        ));
    }
    if parent_agent_did != params.caller_agent_did {
        return Err(ForkError::ForkNotSameAgent);
    }
    let expected_node_did = executor.node_identity_did();

    if !verify_source_idle(executor, params.source_session_id)
        .await
        .map_err(ForkError::ForkCopyFailed)?
    {
        return Err(ForkError::ForkSourceBusy);
    }

    let (cut_seq, cut_ts) =
        match compute_cut(executor, params.source_session_id, params.fork_at_user_turn)
            .await
            .map_err(ForkError::ForkCopyFailed)?
        {
            Ok((seq, ts)) => (seq, ts),
            Err(total_user_msgs) => {
                return Err(ForkError::ForkAtUserTurnOutOfRange(
                    params.fork_at_user_turn,
                    total_user_msgs,
                ));
            }
        };

    let resolved_behavior_id = if let Some(target) = params.target_behavior_id {
        if let Some(err) = resolve_target_behavior(executor, target, parent_agent_did)
            .await
            .map_err(ForkError::ForkCopyFailed)?
        {
            return Err(err);
        }
        target.to_string()
    } else {
        parent.behavior_id.clone().unwrap_or_default()
    };

    let child_session_id = uuid::Uuid::new_v4().to_string();
    let parent_agent_name = parent.agent_name.as_deref().unwrap_or("");
    let (child_conversation_doc_id, node_did) = create_child_session_and_conversation(
        executor,
        &child_session_id,
        &resolved_behavior_id,
        params.source_session_id,
        params.fork_at_user_turn,
        parent_agent_did,
        parent_agent_name,
        parent.requester_did.as_deref(),
        expected_node_did,
    )
    .await
    .map_err(ForkError::ForkCopyFailed)?;

    let copied_messages = copy_messages(
        executor,
        params.source_session_id,
        &child_session_id,
        parent_agent_did,
        parent.requester_did.as_deref(),
        &node_did,
        cut_seq,
    )
    .await
    .map_err(ForkError::ForkCopyFailed)?;

    let copied_tool_calls = copy_tool_calls(
        executor,
        params.source_session_id,
        &child_session_id,
        parent_agent_did,
        parent.requester_did.as_deref(),
        &node_did,
        cut_seq,
    )
    .await
    .map_err(ForkError::ForkCopyFailed)?;

    let copied_tool_results = copy_tool_results(
        executor,
        &child_session_id,
        parent_agent_did,
        parent.requester_did.as_deref(),
        &child_conversation_doc_id,
        &node_did,
        &copied_tool_calls,
    )
    .await
    .map_err(ForkError::ForkCopyFailed)?;

    let copied_tool_approvals = copy_tool_approvals(
        executor,
        &child_session_id,
        parent_agent_did,
        parent.requester_did.as_deref(),
        &node_did,
        &copied_tool_calls,
    )
    .await
    .map_err(ForkError::ForkCopyFailed)?;

    let copied_compaction_entries = copy_compaction_entries(
        executor,
        params.source_session_id,
        &child_session_id,
        parent_agent_did,
        parent.requester_did.as_deref(),
        &node_did,
        &resolved_behavior_id,
        &copied_messages,
        &cut_ts,
    )
    .await
    .map_err(ForkError::ForkCopyFailed)?;

    Ok(ForkOutcome {
        session_id: child_session_id,
        copied_messages: copied_messages.len() as u32,
        copied_tool_calls: copied_tool_calls.len() as u32,
        copied_tool_results,
        copied_tool_approvals,
        copied_compaction_entries,
    })
}

async fn resolve_target_behavior(
    executor: &(impl GraphqlExecutor + ?Sized),
    target_behavior_id: &str,
    parent_agent_did: &str,
) -> Result<Option<ForkError>> {
    let escaped = escape_graphql_string(target_behavior_id);
    let query = format!(
        r#"{{
            AgentBehavior(filter: {{ behavior_id: {{ _eq: "{escaped}" }} }}, limit: 1) {{ agent_did }}
        }}"#
    );
    let resp = executor.execute_graphql(&query).await?;
    if resp.has_errors() {
        anyhow::bail!(
            "resolve_target_behavior query failed: {}",
            render_graphql_errors(&resp)
        );
    }
    let rows = graphql_rows(&resp, "AgentBehavior");
    if rows.is_empty() {
        return Ok(Some(ForkError::ForkBehaviorNotFound(
            target_behavior_id.to_string(),
        )));
    }
    let behavior_did = rows[0]
        .get("agent_did")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if behavior_did != parent_agent_did {
        return Ok(Some(ForkError::ForkBehaviorNotOwnedByPrincipal(
            target_behavior_id.to_string(),
            parent_agent_did.to_string(),
        )));
    }
    Ok(None)
}

async fn compute_cut(
    executor: &(impl GraphqlExecutor + ?Sized),
    source_session_id: &str,
    fork_at_user_turn: u32,
) -> Result<std::result::Result<(u32, String), u32>> {
    let escaped = escape_graphql_string(source_session_id);
    let query = format!(
        r#"{{
            AgentMessage(
                filter: {{
                    session_id: {{ _eq: "{escaped}" }},
                    role: {{ _eq: "user" }}
                }},
                order: {{ sequence: ASC }}
            ) {{ sequence timestamp }}
        }}"#
    );
    let resp = executor.execute_graphql(&query).await?;
    if resp.has_errors() {
        anyhow::bail!("compute_cut query failed: {}", render_graphql_errors(&resp));
    }
    let rows = graphql_rows(&resp, "AgentMessage");
    let total_user_msgs = rows.len() as u32;
    if fork_at_user_turn > total_user_msgs {
        return Ok(Err(total_user_msgs));
    }
    if fork_at_user_turn == total_user_msgs {
        return compute_end_cut(executor, source_session_id).await.map(Ok);
    }
    let row = &rows[fork_at_user_turn as usize];
    let seq = row
        .get("sequence")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow::anyhow!("sequence missing"))? as u32;
    let ts = row
        .get("timestamp")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("timestamp missing"))?
        .to_string();
    Ok(Ok((seq, ts)))
}

async fn compute_end_cut(
    executor: &(impl GraphqlExecutor + ?Sized),
    source_session_id: &str,
) -> Result<(u32, String)> {
    let escaped = escape_graphql_string(source_session_id);
    let query = format!(
        r#"{{
            AgentMessage(
                filter: {{ session_id: {{ _eq: "{escaped}" }} }},
                order: {{ sequence: DESC }},
                limit: 1
            ) {{ sequence }}
        }}"#
    );
    let resp = executor.execute_graphql(&query).await?;
    if resp.has_errors() {
        anyhow::bail!(
            "compute_end_cut query failed: {}",
            render_graphql_errors(&resp)
        );
    }
    let max_sequence = graphql_rows(&resp, "AgentMessage")
        .first()
        .and_then(|row| row.get("sequence"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let cut_seq = u32::try_from(max_sequence.saturating_add(1))
        .context("message sequence exceeds u32 during fork end cut")?;
    Ok((cut_seq, "9999-12-31T23:59:59Z".to_string()))
}

#[derive(Debug, Clone)]
struct ForkedMessage {
    source: crate::SignedDocumentVersionRef,
    child: crate::SignedDocumentVersionRef,
    sequence: u32,
}

async fn copy_messages(
    executor: &(impl GraphqlExecutor + ?Sized),
    source_session_id: &str,
    child_session_id: &str,
    agent_did: &str,
    requester_did: Option<&str>,
    node_did: &str,
    cut_seq: u32,
) -> Result<Vec<ForkedMessage>> {
    let escaped_source = escape_graphql_string(source_session_id);
    let query = format!(
        r#"{{
            AgentMessage(
                filter: {{
                    session_id: {{ _eq: "{escaped_source}" }},
                    sequence: {{ _lt: {cut_seq} }}
                }},
                order: {{ sequence: ASC }}
            ) {{ _docID message_key }}
        }}"#
    );
    let resp = executor.execute_graphql(&query).await?;
    if resp.has_errors() {
        anyhow::bail!(
            "copy_messages query failed: {}",
            render_graphql_errors(&resp)
        );
    }
    let rows = graphql_rows(&resp, "AgentMessage");
    reject_logical_twins(&rows, "message_key", "fork source AgentMessage")?;
    let mut copied = Vec::with_capacity(rows.len());
    for row in &rows {
        let source_doc_id = row
            .get("_docID")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("source AgentMessage missing _docID"))?;
        let source_ref = exact_current_ref(executor, "AgentMessage", source_doc_id).await?;
        let row = exact_snapshot(
            executor,
            "AgentMessage",
            &source_ref,
            "message_key session_id agent_did requester_did request_id request_doc_id sequence role content reasoning timestamp",
        )
        .await?;
        if row.get("session_id").and_then(Value::as_str) != Some(source_session_id) {
            anyhow::bail!("exact AgentMessage source belongs to a different session");
        }
        let sequence = row
            .get("sequence")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| anyhow::anyhow!("sequence missing"))?;
        let role = row.get("role").and_then(|v| v.as_str()).unwrap_or("");
        let content = row.get("content").and_then(|v| v.as_str()).unwrap_or("");
        let reasoning = row.get("reasoning").and_then(|v| v.as_str()).unwrap_or("");
        let timestamp = row.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        let request_id = row.get("request_id").and_then(Value::as_str);
        let request_doc_id = row.get("request_doc_id").and_then(Value::as_str);
        let message_key = format!("{child_session_id}:{sequence}");
        require_child_key_absent(executor, "AgentMessage", "message_key", &message_key).await?;
        let requester_did_field = crate::session::requester_did_create_field(requester_did);
        let source_fields = fork_source_fields(&source_ref);
        let mutation = format!(
            r#"mutation {{ create_AgentMessage(input: {{
                    message_key: "{message_key_escaped}",
                    session_id: "{child_session_escaped}",
                    agent_did: "{agent_did_escaped}",
                    {requester_did_field}
                    request_id: {request_id},
                    request_doc_id: {request_doc_id},
                    sequence: {sequence},
                    role: "{role_escaped}",
                    content: "{content_escaped}",
                    reasoning: "{reasoning_escaped}",
                    timestamp: "{timestamp_escaped}",
                    {source_fields}
                }}) {{ _docID }} }}"#,
            message_key_escaped = escape_graphql_string(&message_key),
            child_session_escaped = escape_graphql_string(child_session_id),
            agent_did_escaped = escape_graphql_string(agent_did),
            request_id = nullable_string_literal(request_id),
            request_doc_id = nullable_string_literal(request_doc_id),
            role_escaped = escape_graphql_string(role),
            content_escaped = escape_graphql_string(content),
            reasoning_escaped = escape_graphql_string(reasoning),
            timestamp_escaped = escape_graphql_string(timestamp),
        );
        let response =
            execute_mutation_with_retry(executor, &mutation, "fork::copy_message").await?;
        let doc_id = mutation_doc_id(&response, "create_AgentMessage")?;
        let child = verify_child_ref(executor, "AgentMessage", &doc_id, Some(node_did)).await?;
        require_sole_child_key(
            executor,
            "AgentMessage",
            "message_key",
            &message_key,
            &doc_id,
        )
        .await?;
        copied.push(ForkedMessage {
            source: source_ref,
            child,
            sequence: u32::try_from(sequence).context("forked message sequence exceeds u32")?,
        });
    }
    Ok(copied)
}

#[derive(Debug, Clone)]
struct ForkedToolCall {
    source: crate::SignedDocumentVersionRef,
    child: crate::SignedDocumentVersionRef,
    source_row: Value,
}

async fn copy_tool_calls(
    executor: &(impl GraphqlExecutor + ?Sized),
    source_session_id: &str,
    child_session_id: &str,
    agent_did: &str,
    requester_did: Option<&str>,
    node_did: &str,
    cut_seq: u32,
) -> Result<Vec<ForkedToolCall>> {
    let escaped_source = escape_graphql_string(source_session_id);
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{
                    session_id: {{ _eq: "{escaped_source}" }},
                    message_sequence: {{ _lt: {cut_seq} }}
                }},
                order: {{ message_sequence: ASC }}
            ) {{ _docID tool_call_key }}
        }}"#
    );
    let resp = executor.execute_graphql(&query).await?;
    if resp.has_errors() {
        anyhow::bail!(
            "copy_tool_calls query failed: {}",
            render_graphql_errors(&resp)
        );
    }
    let rows = graphql_rows(&resp, "AgentToolCall");
    reject_logical_twins(&rows, "tool_call_key", "fork source AgentToolCall")?;
    let mut copied = Vec::with_capacity(rows.len());
    for row in &rows {
        let source_doc_id = row
            .get("_docID")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("source AgentToolCall missing _docID"))?;
        let source = exact_current_ref(executor, "AgentToolCall", source_doc_id).await?;
        let row = exact_snapshot(
            executor,
            "AgentToolCall",
            &source,
            "tool_call_key request_id session_id agent_did requester_did message_sequence tool_name tool_call_id args result result_doc_id result_composite_commit_cid result_signer_did approval_doc_id approval_composite_commit_cid approval_signer_did status lifecycle_state started_at deadline_at completed_at selected_service_id selected_tool_name tool_failure_class denial_reason denied_argv denied_command denied_argument denied_subcommand denied_prefix policy_mode policy_network latency_ms await_mode cancel_policy cancel_cause child_request_id",
        )
        .await?;
        if row.get("session_id").and_then(Value::as_str) != Some(source_session_id) {
            anyhow::bail!("exact AgentToolCall source belongs to a different session");
        }
        let message_sequence = row
            .get("message_sequence")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| anyhow::anyhow!("message_sequence missing"))?;
        let tool_name = row.get("tool_name").and_then(|v| v.as_str()).unwrap_or("");
        let tool_call_id = row
            .get("tool_call_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let args = row.get("args").and_then(|v| v.as_str()).unwrap_or("");
        let result = row.get("result").and_then(|v| v.as_str()).unwrap_or("");
        let status = row.get("status").and_then(|v| v.as_str()).unwrap_or("");
        let lifecycle_state = row.get("lifecycle_state").and_then(|v| v.as_str());
        let started_at = row.get("started_at").and_then(|v| v.as_str()).unwrap_or("");
        let deadline_at = row.get("deadline_at").and_then(Value::as_str);
        let completed_at = row.get("completed_at").and_then(Value::as_str);
        let selected_service_id = row.get("selected_service_id").and_then(|v| v.as_str());
        let selected_tool_name = row.get("selected_tool_name").and_then(|v| v.as_str());
        let tool_failure_class = row.get("tool_failure_class").and_then(|v| v.as_str());
        let denial_reason = row.get("denial_reason").and_then(|v| v.as_str());
        let denied_argv = row.get("denied_argv").and_then(json_string_array);
        let denied_command = row.get("denied_command").and_then(|v| v.as_str());
        let denied_argument = row.get("denied_argument").and_then(|v| v.as_str());
        let denied_subcommand = row.get("denied_subcommand").and_then(|v| v.as_str());
        let denied_prefix = row.get("denied_prefix").and_then(json_string_array);
        let policy_mode = row.get("policy_mode").and_then(|v| v.as_str());
        let policy_network = row.get("policy_network").and_then(|v| v.as_str());
        let cancel_cause = row.get("cancel_cause").and_then(|v| v.as_str());
        let latency_ms = row.get("latency_ms").and_then(json_i64);
        let await_mode = row.get("await_mode").and_then(Value::as_str);
        let cancel_policy = row.get("cancel_policy").and_then(Value::as_str);
        let child_request_id = row.get("child_request_id").and_then(Value::as_str);
        let request_id = row.get("request_id").and_then(Value::as_str);
        let tool_call_key = format!("{child_session_id}:{tool_call_id}");
        require_child_key_absent(executor, "AgentToolCall", "tool_call_key", &tool_call_key)
            .await?;
        let requester_did_field = crate::session::requester_did_create_field(requester_did);
        let source_fields = fork_source_fields(&source);
        let mutation = format!(
            r#"mutation {{ create_AgentToolCall(input: {{
                    tool_call_key: "{tool_call_key_escaped}",
                    request_id: {request_id},
                    session_id: "{child_session_escaped}",
                    agent_did: "{agent_did_escaped}",
                    {requester_did_field}
                    message_sequence: {message_sequence},
                    tool_name: "{tool_name_escaped}",
                    tool_call_id: "{tool_call_id_escaped}",
                    args: "{args_escaped}",
                    result: "{result_escaped}",
                    status: "{status_escaped}",
                    lifecycle_state: {lifecycle_state},
                    started_at: {started_at},
                    deadline_at: {deadline_at},
                    completed_at: {completed_at},
                    selected_service_id: {selected_service_id},
                    selected_tool_name: {selected_tool_name},
                    tool_failure_class: {tool_failure_class},
                    denial_reason: {denial_reason},
                    denied_argv: {denied_argv},
                    denied_command: {denied_command},
                    denied_argument: {denied_argument},
                    denied_subcommand: {denied_subcommand},
                    denied_prefix: {denied_prefix},
                    policy_mode: {policy_mode},
                    policy_network: {policy_network},
                    cancel_cause: {cancel_cause},
                    latency_ms: {latency_ms},
                    await_mode: {await_mode},
                    cancel_policy: {cancel_policy},
                    child_request_id: {child_request_id},
                    {source_fields}
                }}) {{ _docID }} }}"#,
            tool_call_key_escaped = escape_graphql_string(&tool_call_key),
            request_id = nullable_string_literal(request_id),
            child_session_escaped = escape_graphql_string(child_session_id),
            agent_did_escaped = escape_graphql_string(agent_did),
            tool_name_escaped = escape_graphql_string(tool_name),
            tool_call_id_escaped = escape_graphql_string(tool_call_id),
            args_escaped = escape_graphql_string(args),
            result_escaped = escape_graphql_string(result),
            status_escaped = escape_graphql_string(status),
            lifecycle_state = nullable_string_literal(lifecycle_state),
            started_at = nullable_string_literal((!started_at.is_empty()).then_some(started_at)),
            deadline_at = nullable_string_literal(deadline_at),
            completed_at = nullable_string_literal(completed_at),
            selected_service_id = nullable_string_literal(selected_service_id),
            selected_tool_name = nullable_string_literal(selected_tool_name),
            tool_failure_class = nullable_string_literal(tool_failure_class),
            denial_reason = nullable_string_literal(denial_reason),
            denied_argv = nullable_string_array_literal(denied_argv.as_deref()),
            denied_command = nullable_string_literal(denied_command),
            denied_argument = nullable_string_literal(denied_argument),
            denied_subcommand = nullable_string_literal(denied_subcommand),
            denied_prefix = nullable_string_array_literal(denied_prefix.as_deref()),
            policy_mode = nullable_string_literal(policy_mode),
            policy_network = nullable_string_literal(policy_network),
            cancel_cause = nullable_string_literal(cancel_cause),
            latency_ms = nullable_i64_literal(latency_ms),
            await_mode = nullable_string_literal(await_mode),
            cancel_policy = nullable_string_literal(cancel_policy),
            child_request_id = nullable_string_literal(child_request_id),
        );
        let response =
            execute_mutation_with_retry(executor, &mutation, "fork::copy_tool_call").await?;
        let doc_id = mutation_doc_id(&response, "create_AgentToolCall")?;
        let child = verify_child_ref(executor, "AgentToolCall", &doc_id, Some(node_did)).await?;
        require_sole_child_key(
            executor,
            "AgentToolCall",
            "tool_call_key",
            &tool_call_key,
            &doc_id,
        )
        .await?;
        copied.push(ForkedToolCall {
            source,
            child,
            source_row: row,
        });
    }
    Ok(copied)
}

async fn copy_tool_results(
    executor: &(impl GraphqlExecutor + ?Sized),
    child_session_id: &str,
    child_agent_did: &str,
    child_requester_did: Option<&str>,
    child_conversation_doc_id: &str,
    node_did: &str,
    calls: &[ForkedToolCall],
) -> Result<u32> {
    let mut copied = 0u32;
    for call in calls {
        let Some(source) = optional_exact_ref(
            &call.source_row,
            "result_doc_id",
            "result_composite_commit_cid",
            "result_signer_did",
            "fork source AgentToolResult",
        )?
        else {
            continue;
        };
        let row = exact_snapshot(
            executor,
            "AgentToolResult",
            &source,
            "result_key tool_call_key tool_call_doc_id tool_call_composite_commit_cid tool_call_signer_did agent_did requester_did session_id tool_name tool_input output_text model_output_truncated truncation_metadata conversation_doc_id created_at discarded_because_interrupted",
        )
        .await?;
        if row.get("tool_call_doc_id").and_then(Value::as_str)
            != Some(call.source.version.doc_id.as_str())
        {
            anyhow::bail!("source AgentToolResult points to a different physical source call");
        }
        let source_parent = crate::SignedDocumentVersionRef::new(
            crate::DocumentVersionRef::new(
                row.get("tool_call_doc_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                row.get("tool_call_composite_commit_cid")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            ),
            row.get("tool_call_signer_did")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        );
        exact_snapshot(
            executor,
            "AgentToolCall",
            &source_parent,
            "tool_call_key session_id",
        )
        .await
        .context("verifying exact historical source call for forked result")?;
        let tool_name = row.get("tool_name").and_then(|v| v.as_str()).unwrap_or("");
        let tool_input = row.get("tool_input").and_then(|v| v.as_str()).unwrap_or("");
        let output_text = row
            .get("output_text")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let truncated = row
            .get("model_output_truncated")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let truncation_metadata = row
            .get("truncation_metadata")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let created_at = row.get("created_at").and_then(|v| v.as_str()).unwrap_or("");
        let discarded = row
            .get("discarded_because_interrupted")
            .and_then(Value::as_bool);
        let tool_call_id = call
            .source_row
            .get("tool_call_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let tool_call_key = format!("{child_session_id}:{tool_call_id}");
        let result_key = call.child.version.doc_id.clone();
        require_child_key_absent(executor, "AgentToolResult", "result_key", &result_key).await?;
        let requester_did_field = crate::session::requester_did_create_field(child_requester_did);
        let source_fields = fork_source_fields(&source);
        let mutation = format!(
            r#"mutation {{ create_AgentToolResult(input: {{
                    result_key: "{result_key_escaped}",
                    tool_call_key: "{tool_call_key_escaped}",
                    tool_call_doc_id: "{child_call_doc_id}",
                    tool_call_composite_commit_cid: "{child_call_cid}",
                    tool_call_signer_did: "{child_call_signer}",
                    agent_did: "{child_agent_did_escaped}",
                    {requester_did_field}
                    session_id: "{child_session_escaped}",
                    tool_name: "{tool_name_escaped}",
                    tool_input: "{tool_input_escaped}",
                    output_text: "{output_text_escaped}",
                    model_output_truncated: {truncated},
                    truncation_metadata: "{truncation_metadata_escaped}",
                    conversation_doc_id: "{child_conversation_doc_id}",
                    created_at: "{created_at_escaped}",
                    discarded_because_interrupted: {discarded},
                    {source_fields}
                }}) {{ _docID }} }}"#,
            result_key_escaped = escape_graphql_string(&result_key),
            tool_call_key_escaped = escape_graphql_string(&tool_call_key),
            child_call_doc_id = escape_graphql_string(&call.child.version.doc_id),
            child_call_cid = escape_graphql_string(&call.child.version.composite_commit_cid),
            child_call_signer = escape_graphql_string(&call.child.signer_did),
            child_agent_did_escaped = escape_graphql_string(child_agent_did),
            child_session_escaped = escape_graphql_string(child_session_id),
            tool_name_escaped = escape_graphql_string(tool_name),
            tool_input_escaped = escape_graphql_string(tool_input),
            output_text_escaped = escape_graphql_string(output_text),
            truncation_metadata_escaped = escape_graphql_string(truncation_metadata),
            child_conversation_doc_id = escape_graphql_string(child_conversation_doc_id),
            created_at_escaped = escape_graphql_string(created_at),
            discarded = discarded.unwrap_or(false),
        );
        let response =
            execute_mutation_with_retry(executor, &mutation, "fork::copy_tool_result").await?;
        let doc_id = mutation_doc_id(&response, "create_AgentToolResult")?;
        let child_result =
            verify_child_ref(executor, "AgentToolResult", &doc_id, Some(node_did)).await?;
        require_sole_child_key(
            executor,
            "AgentToolResult",
            "result_key",
            &result_key,
            &doc_id,
        )
        .await?;
        attach_child_tool_fact(
            executor,
            &call.child.version.doc_id,
            "result",
            &child_result,
        )
        .await?;
        copied += 1;
    }
    Ok(copied)
}

async fn copy_tool_approvals(
    executor: &(impl GraphqlExecutor + ?Sized),
    child_session_id: &str,
    child_agent_did: &str,
    child_requester_did: Option<&str>,
    node_did: &str,
    calls: &[ForkedToolCall],
) -> Result<u32> {
    let mut copied = 0u32;
    for call in calls {
        let Some(source) = optional_exact_ref(
            &call.source_row,
            "approval_doc_id",
            "approval_composite_commit_cid",
            "approval_signer_did",
            "fork source AgentToolApproval",
        )?
        else {
            continue;
        };
        let row = exact_snapshot(
            executor,
            "AgentToolApproval",
            &source,
            "approval_id approval_key tool_call_id tool_call_key tool_call_doc_id tool_call_composite_commit_cid tool_call_signer_did request_id session_id agent_did requester_did decision approver_did reason created_at",
        )
        .await?;
        if row.get("tool_call_doc_id").and_then(Value::as_str)
            != Some(call.source.version.doc_id.as_str())
        {
            anyhow::bail!("source AgentToolApproval points to a different physical source call");
        }
        let source_parent = crate::SignedDocumentVersionRef::new(
            crate::DocumentVersionRef::new(
                row.get("tool_call_doc_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                row.get("tool_call_composite_commit_cid")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            ),
            row.get("tool_call_signer_did")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        );
        exact_snapshot(
            executor,
            "AgentToolCall",
            &source_parent,
            "tool_call_key session_id",
        )
        .await
        .context("verifying exact historical source call for forked approval")?;

        let tool_call_id = call
            .source_row
            .get("tool_call_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let tool_call_key = format!("{child_session_id}:{tool_call_id}");
        let approval_key = call.child.version.doc_id.clone();
        let approval_id = format!("approval-{}", call.child.version.doc_id);
        require_child_key_absent(executor, "AgentToolApproval", "approval_key", &approval_key)
            .await?;
        let requester_did_field = crate::session::requester_did_create_field(child_requester_did);
        let source_fields = fork_source_fields(&source);
        let mutation = format!(
            r#"mutation {{ create_AgentToolApproval(input: {{
                    approval_id: "{}",
                    approval_key: "{}",
                    tool_call_id: "{}",
                    tool_call_key: "{}",
                    tool_call_doc_id: "{}",
                    tool_call_composite_commit_cid: "{}",
                    tool_call_signer_did: "{}",
                    request_id: {},
                    session_id: "{}",
                    agent_did: "{}",
                    {requester_did_field}
                    decision: "{}",
                    approver_did: "{}",
                    reason: {},
                    created_at: "{}",
                    {source_fields}
                }}) {{ _docID }} }}"#,
            escape_graphql_string(&approval_id),
            escape_graphql_string(&approval_key),
            escape_graphql_string(tool_call_id),
            escape_graphql_string(&tool_call_key),
            escape_graphql_string(&call.child.version.doc_id),
            escape_graphql_string(&call.child.version.composite_commit_cid),
            escape_graphql_string(&call.child.signer_did),
            nullable_string_literal(row.get("request_id").and_then(Value::as_str)),
            escape_graphql_string(child_session_id),
            escape_graphql_string(child_agent_did),
            escape_graphql_string(
                row.get("decision")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
            ),
            escape_graphql_string(node_did),
            nullable_string_literal(row.get("reason").and_then(Value::as_str)),
            escape_graphql_string(
                row.get("created_at")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
            ),
        );
        let response =
            execute_mutation_with_retry(executor, &mutation, "fork::copy_tool_approval").await?;
        let doc_id = mutation_doc_id(&response, "create_AgentToolApproval")?;
        let child_approval =
            verify_child_ref(executor, "AgentToolApproval", &doc_id, Some(node_did)).await?;
        require_sole_child_key(
            executor,
            "AgentToolApproval",
            "approval_key",
            &approval_key,
            &doc_id,
        )
        .await?;
        attach_child_tool_fact(
            executor,
            &call.child.version.doc_id,
            "approval",
            &child_approval,
        )
        .await?;
        copied += 1;
    }
    Ok(copied)
}

async fn copy_compaction_entries(
    executor: &(impl GraphqlExecutor + ?Sized),
    source_session_id: &str,
    child_session_id: &str,
    agent_did: &str,
    requester_did: Option<&str>,
    node_did: &str,
    behavior_id: &str,
    messages: &[ForkedMessage],
    cut_ts: &str,
) -> Result<u32> {
    let escaped_source = escape_graphql_string(source_session_id);
    let escaped_cut_ts = escape_graphql_string(cut_ts);
    let query = format!(
        r#"{{
            CompactionEntry(
                filter: {{
                    session_id: {{ _eq: "{escaped_source}" }},
                    created_at: {{ _lt: "{escaped_cut_ts}" }}
                }},
                order: {{ sequence: ASC }}
            ) {{ _docID compaction_key }}
        }}"#
    );
    let resp = executor.execute_graphql(&query).await?;
    if resp.has_errors() {
        anyhow::bail!(
            "copy_compaction_entries query failed: {}",
            render_graphql_errors(&resp)
        );
    }
    let rows = graphql_rows(&resp, "CompactionEntry");
    reject_logical_twins(&rows, "compaction_key", "fork source CompactionEntry")?;
    let mut copied = 0u32;
    let message_map = messages
        .iter()
        .map(|message| (message.source.version.doc_id.as_str(), message))
        .collect::<HashMap<_, _>>();
    let mut compaction_map: HashMap<String, (crate::SignedDocumentVersionRef, CompactionFactRef)> =
        HashMap::new();
    for row in &rows {
        let source_doc_id = row
            .get("_docID")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("source CompactionEntry missing _docID"))?;
        let source = exact_current_ref(executor, "CompactionEntry", source_doc_id).await?;
        let row = exact_snapshot(
            executor,
            "CompactionEntry",
            &source,
            "compaction_key session_id agent_did requester_did sequence summary files_read files_modified messages_compacted original_tokens compacted_tokens source_manifest_version source_manifest_json created_at",
        )
        .await?;
        if row.get("session_id").and_then(Value::as_str) != Some(source_session_id) {
            anyhow::bail!("exact CompactionEntry source belongs to a different session");
        }
        let sequence = row
            .get("sequence")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| anyhow::anyhow!("compaction sequence missing"))?;
        let summary = row.get("summary").and_then(|v| v.as_str()).unwrap_or("");
        let files_read = row
            .get("files_read")
            .and_then(|v| v.as_str())
            .unwrap_or("[]");
        let files_modified = row
            .get("files_modified")
            .and_then(|v| v.as_str())
            .unwrap_or("[]");
        let messages_compacted = row
            .get("messages_compacted")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let original_tokens = row
            .get("original_tokens")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let compacted_tokens = row
            .get("compacted_tokens")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let source_manifest_version = row
            .get("source_manifest_version")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow::anyhow!("source compaction manifest version missing"))?;
        let source_manifest_json = row
            .get("source_manifest_json")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow::anyhow!("source compaction manifest missing"))?;
        let mut manifest: CompactionSourceManifest = serde_json::from_str(source_manifest_json)
            .context("decode exact source compaction manifest")?;
        manifest
            .validate(source_session_id, agent_did)
            .context("validate exact source compaction manifest")?;
        if manifest.behavior_id != behavior_id {
            anyhow::bail!(
                "cannot fork CompactionEntry across behavior change {} -> {behavior_id}",
                manifest.behavior_id
            );
        }
        manifest.session_id = child_session_id.to_string();
        for fact in &mut manifest.transcript_snapshot {
            let child = message_map.get(fact.doc_id.as_str()).ok_or_else(|| {
                anyhow::anyhow!(
                    "compaction source message {} is outside the exact fork cut",
                    fact.doc_id
                )
            })?;
            if child.source.version.composite_commit_cid != fact.composite_commit_cid
                || child.source.signer_did != fact.signer_did
                || child.sequence != fact.sequence
            {
                anyhow::bail!("compaction source message exact ref changed during fork");
            }
            fact.doc_id = child.child.version.doc_id.clone();
            fact.composite_commit_cid = child.child.version.composite_commit_cid.clone();
            fact.signer_did = child.child.signer_did.clone();
        }
        for fact in &mut manifest.prior_compactions {
            let (source_ref, child) =
                compaction_map
                    .get(&fact.source.version.doc_id)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "prior compaction {} was not minted earlier in the child session",
                            fact.source.version.doc_id
                        )
                    })?;
            if source_ref.version.composite_commit_cid != fact.source.version.composite_commit_cid
                || source_ref.signer_did != fact.source.signer_did
                || child.sequence != fact.sequence
            {
                anyhow::bail!("prior compaction exact ref changed during fork");
            }
            *fact = child.clone();
        }
        manifest
            .validate(child_session_id, agent_did)
            .context("validate child compaction manifest")?;
        let child_manifest_json =
            crate::rendered_request::canonical_json_string(&serde_json::to_value(&manifest)?)?;
        let created_at = row.get("created_at").and_then(|v| v.as_str()).unwrap_or("");
        let compaction_key = format!("{child_session_id}:{sequence}");
        require_child_key_absent(
            executor,
            "CompactionEntry",
            "compaction_key",
            &compaction_key,
        )
        .await?;
        let requester_did_field = crate::session::requester_did_create_field(requester_did);
        let source_fields = fork_source_fields(&source);
        let mutation = format!(
            r#"mutation {{ create_CompactionEntry(input: {{
                    compaction_key: "{compaction_key_escaped}",
                    session_id: "{child_session_escaped}",
                    agent_did: "{agent_did_escaped}",
                    {requester_did_field}
                    sequence: {sequence},
                    summary: "{summary_escaped}",
                    files_read: "{files_read_escaped}",
                    files_modified: "{files_modified_escaped}",
                    messages_compacted: {messages_compacted},
                    original_tokens: {original_tokens},
                    compacted_tokens: {compacted_tokens},
                    source_manifest_version: {source_manifest_version},
                    source_manifest_json: "{source_manifest_json_escaped}",
                    created_at: "{created_at_escaped}",
                    {source_fields}
                }}) {{ _docID }} }}"#,
            compaction_key_escaped = escape_graphql_string(&compaction_key),
            child_session_escaped = escape_graphql_string(child_session_id),
            agent_did_escaped = escape_graphql_string(agent_did),
            summary_escaped = escape_graphql_string(summary),
            files_read_escaped = escape_graphql_string(files_read),
            files_modified_escaped = escape_graphql_string(files_modified),
            source_manifest_json_escaped = escape_graphql_string(&child_manifest_json),
            created_at_escaped = escape_graphql_string(created_at),
        );
        let response =
            execute_mutation_with_retry(executor, &mutation, "fork::copy_compaction_entry").await?;
        let doc_id = mutation_doc_id(&response, "create_CompactionEntry")?;
        let child = verify_child_ref(executor, "CompactionEntry", &doc_id, Some(node_did)).await?;
        require_sole_child_key(
            executor,
            "CompactionEntry",
            "compaction_key",
            &compaction_key,
            &doc_id,
        )
        .await?;
        compaction_map.insert(
            source.version.doc_id.clone(),
            (
                source,
                CompactionFactRef {
                    sequence: u32::try_from(sequence)
                        .context("forked compaction sequence exceeds u32")?,
                    source: child,
                },
            ),
        );
        copied += 1;
    }
    Ok(copied)
}

async fn create_child_session_and_conversation(
    executor: &(impl GraphqlExecutor + ?Sized),
    child_session_id: &str,
    behavior_id: &str,
    source_session_id: &str,
    fork_at_user_turn: u32,
    parent_agent_did: &str,
    parent_agent_name: &str,
    requester_did: Option<&str>,
    expected_node_did: Option<&str>,
) -> Result<(String, String)> {
    let now = chrono::Utc::now().to_rfc3339();
    let child_session_escaped = escape_graphql_string(child_session_id);
    let behavior_id_escaped = escape_graphql_string(behavior_id);
    let forked_from_escaped = escape_graphql_string(source_session_id);
    let now_escaped = escape_graphql_string(&now);
    let agent_did_escaped = escape_graphql_string(parent_agent_did);
    let agent_name_escaped = escape_graphql_string(parent_agent_name);
    let requester_did_field = crate::session::requester_did_create_field(requester_did);

    let session_mutation = format!(
        r#"mutation {{
            create_AgentSession(input: {{
                session_id: "{child_session_escaped}",
                agent_name: "{agent_name_escaped}",
                agent_did: "{agent_did_escaped}",
                {requester_did_field}
                behavior_id: "{behavior_id_escaped}",
                started: "{now_escaped}",
                status: "active"
            }}) {{ _docID }}
        }}"#
    );
    let session =
        execute_mutation_with_retry(executor, &session_mutation, "fork::create_session").await?;
    let session_doc_id = mutation_doc_id(&session, "create_AgentSession")?;
    let session_ref =
        verify_child_ref(executor, "AgentSession", &session_doc_id, expected_node_did).await?;
    let node_did = session_ref.signer_did;

    let conv_mutation = format!(
        r#"mutation {{
            create_AgentConversation(input: {{
                session_id: "{child_session_escaped}",
                agent_name: "{agent_name_escaped}",
                agent_did: "{agent_did_escaped}",
                {requester_did_field}
                behavior_id: "{behavior_id_escaped}",
                title: "Forked conversation",
                preview_text: "",
                status: "active",
                created_at: "{now_escaped}",
                updated_at: "{now_escaped}",
                latest_request_id: "",
                forked_from_session_id: "{forked_from_escaped}",
                fork_at_user_turn: {fork_at_user_turn},
                forked_at: "{now_escaped}"
            }}) {{ _docID }}
        }}"#
    );
    let conversation =
        execute_mutation_with_retry(executor, &conv_mutation, "fork::create_conversation").await?;
    let conversation_doc_id = mutation_doc_id(&conversation, "create_AgentConversation")?;
    verify_child_ref(
        executor,
        "AgentConversation",
        &conversation_doc_id,
        Some(&node_did),
    )
    .await?;
    Ok((conversation_doc_id, node_did))
}

async fn execute_mutation_with_retry(
    executor: &(impl GraphqlExecutor + ?Sized),
    mutation: &str,
    operation: &str,
) -> Result<GraphqlExecuteResponse> {
    let mut last_resp = None;
    let mut last_error = None;
    for attempt in 0..=DEFRA_DB_CONFLICT_MAX_RETRIES {
        if attempt > 0 {
            let backoff = defradb_conflict_retry_backoff(attempt - 1);
            tracing::warn!(
                operation = %operation,
                attempt = attempt,
                backoff_ms = backoff.as_millis() as u64,
                "retrying mutation"
            );
            tokio::time::sleep(backoff).await;
        }

        let started = std::time::Instant::now();
        let resp = executor.execute_graphql(mutation).await;
        let elapsed = started.elapsed();
        log_mutation_timing(operation, elapsed);

        match resp {
            Ok(resp) if !resp.has_errors() => return Ok(resp),
            Ok(resp) => {
                let retryable = is_defradb_transaction_conflict_text(&render_graphql_errors(&resp));
                tracing::warn!(
                    operation = %operation,
                    attempt = attempt,
                    errors = %render_graphql_errors(&resp),
                    elapsed_ms = elapsed.as_millis() as u64,
                    "mutation failed"
                );
                if retryable && attempt < DEFRA_DB_CONFLICT_MAX_RETRIES {
                    last_resp = Some(resp);
                    continue;
                }
                anyhow::bail!("{operation} failed: {}", render_graphql_errors(&resp));
            }
            Err(error) => {
                tracing::warn!(
                    operation = %operation,
                    attempt = attempt,
                    error = %error,
                    elapsed_ms = elapsed.as_millis() as u64,
                    "mutation transport failed"
                );
                if attempt < DEFRA_DB_CONFLICT_MAX_RETRIES {
                    last_error = Some(error);
                    continue;
                }
                return Err(error);
            }
        }
    }

    if let Some(resp) = last_resp {
        anyhow::bail!(
            "{operation} failed after {DEFRA_DB_CONFLICT_MAX_RETRIES} retries: {}",
            render_graphql_errors(&resp)
        );
    }
    Err(last_error
        .unwrap_or_else(|| anyhow::anyhow!("{operation} failed without GraphQL response")))
}

fn graphql_rows(response: &GraphqlExecuteResponse, collection_name: &str) -> Vec<Value> {
    response
        .data
        .as_ref()
        .and_then(|data| data.get(collection_name))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn render_graphql_errors(response: &GraphqlExecuteResponse) -> String {
    Value::Array(response.errors.clone()).to_string()
}

fn nullable_string_literal(value: Option<&str>) -> String {
    value
        .map(|value| format!(r#""{}""#, escape_graphql_string(value)))
        .unwrap_or_else(|| "null".to_string())
}

fn nullable_string_array_literal(value: Option<&[String]>) -> String {
    value
        .map(|values| {
            let values = values
                .iter()
                .map(|value| format!(r#""{}""#, escape_graphql_string(value)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("[{values}]")
        })
        .unwrap_or_else(|| "null".to_string())
}

fn nullable_i64_literal(value: Option<i64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_string())
}

fn json_i64(value: &serde_json::Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
}

fn json_string_array(value: &serde_json::Value) -> Option<Vec<String>> {
    Some(
        value
            .as_array()?
            .iter()
            .filter_map(|value| value.as_str().map(ToOwned::to_owned))
            .collect(),
    )
}
