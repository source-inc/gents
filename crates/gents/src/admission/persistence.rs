use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use defra_node::EmbeddedNode;
use rig::completion::{CompletionError, Usage};

use super::controller::InferenceCallRecord;
use crate::graphql::escape_graphql_string;
use crate::{
    document_version::verified_current_signed_document_version_with_identity,
    SignedDocumentVersionRef,
};

const EXACT_TRANSITION_ATTEMPTS: usize = 3;

enum ExactTransitionResult {
    Complete,
    RetryExpectedState,
}

enum ExactTransitionFacts {
    Running {
        started_at: String,
    },
    Terminal {
        ended_at: String,
        failure_reason: Option<String>,
        prompt_tokens: Option<u64>,
        completion_tokens: Option<u64>,
        cached_input_tokens: Option<u64>,
        running_call: Option<SignedDocumentVersionRef>,
        rendered_request: Option<SignedDocumentVersionRef>,
    },
}

impl ExactTransitionFacts {
    fn matches(&self, row: &serde_json::Value) -> bool {
        match self {
            Self::Running { started_at } => {
                row.get("started_at").and_then(serde_json::Value::as_str)
                    == Some(started_at.as_str())
            }
            Self::Terminal {
                ended_at,
                failure_reason,
                prompt_tokens,
                completion_tokens,
                cached_input_tokens,
                running_call,
                rendered_request,
            } => {
                row.get("ended_at").and_then(serde_json::Value::as_str) == Some(ended_at.as_str())
                    && row
                        .get("failure_reason")
                        .and_then(serde_json::Value::as_str)
                        == failure_reason.as_deref()
                    && row.get("prompt_tokens").and_then(serde_json::Value::as_u64)
                        == *prompt_tokens
                    && row
                        .get("completion_tokens")
                        .and_then(serde_json::Value::as_u64)
                        == *completion_tokens
                    && row
                        .get("cached_input_tokens")
                        .and_then(serde_json::Value::as_u64)
                        == *cached_input_tokens
                    && match (running_call, rendered_request) {
                        (Some(call), Some(render)) => {
                            string_field(row, "rendered_from_call_commit_cid")
                                == Some(call.version.composite_commit_cid.as_str())
                                && string_field(row, "rendered_request_doc_id")
                                    == Some(render.version.doc_id.as_str())
                                && string_field(row, "rendered_request_composite_commit_cid")
                                    == Some(render.version.composite_commit_cid.as_str())
                                && string_field(row, "rendered_request_signer_did")
                                    == Some(render.signer_did.as_str())
                        }
                        (None, None) => true,
                        _ => false,
                    }
            }
        }
    }
}

fn string_field<'a>(row: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    row.get(field).and_then(serde_json::Value::as_str)
}

pub(super) fn spawn_persistence<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(future);
    }
}

pub(super) fn completion_persistence_error(error: anyhow::Error) -> CompletionError {
    CompletionError::ProviderError(format!("persisting InferenceCall failed: {error:#}"))
}

fn extract_inference_call_doc_id(data: Option<&serde_json::Value>) -> Result<String> {
    data.and_then(|data| data.get("add_InferenceCall"))
        .and_then(|value| {
            value
                .get("_docID")
                .and_then(|doc_id| doc_id.as_str())
                .or_else(|| {
                    value
                        .as_array()
                        .and_then(|rows| rows.first())
                        .and_then(|row| row.get("_docID"))
                        .and_then(|doc_id| doc_id.as_str())
                })
        })
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow::anyhow!("add_InferenceCall returned no _docID"))
}

async fn resolve_running_call_version(
    node: &EmbeddedNode,
    doc_id: &str,
    call: &InferenceCallRecord,
) -> Result<SignedDocumentVersionRef> {
    let identity = super::query::call_identity(node, call)?;
    let version = verified_current_signed_document_version_with_identity(
        node,
        "InferenceCall",
        doc_id,
        Some(identity),
    )
    .await?;
    if version.signer_did != call.agent_did {
        anyhow::bail!(
            "InferenceCall {} running version {} signer {} does not match agent DID {}",
            call.call_id,
            version.version.composite_commit_cid,
            version.signer_did,
            call.agent_did
        );
    }

    let cid = escape_graphql_string(&version.version.composite_commit_cid);
    let response = super::query::execute_for_call(
        node,
        call,
        format!(
            r#"{{
                InferenceCall(cid: ["{cid}"]) {{
                    _docID
                    call_id
                    runtime_instance_id
                    request_id
                    agent_did
                    controller_generation
                    call_state
                    rendered_from_call_commit_cid
                    rendered_request_doc_id
                    rendered_request_composite_commit_cid
                    rendered_request_signer_did
                }}
            }}"#
        ),
    )
    .await?;
    if response.has_errors() {
        anyhow::bail!(
            "reloading exact running InferenceCall {} version {} failed: {:?}",
            call.call_id,
            version.version.composite_commit_cid,
            response.errors
        );
    }
    let rows = response
        .data
        .as_ref()
        .and_then(|data| data.get("InferenceCall"))
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let [row] = rows.as_slice() else {
        anyhow::bail!(
            "exact running InferenceCall {} version {} resolved {} rows",
            call.call_id,
            version.version.composite_commit_cid,
            rows.len()
        );
    };
    let identity_matches = string_field(row, "_docID") == Some(doc_id)
        && string_field(row, "call_id") == Some(call.call_id.as_str())
        && string_field(row, "runtime_instance_id") == Some(call.runtime_instance_id.as_str())
        && string_field(row, "request_id") == Some(call.request_id.as_str())
        && string_field(row, "agent_did") == Some(call.agent_did.as_str())
        && row
            .get("controller_generation")
            .and_then(serde_json::Value::as_u64)
            == Some(call.controller_generation);
    let unbound = [
        "rendered_from_call_commit_cid",
        "rendered_request_doc_id",
        "rendered_request_composite_commit_cid",
        "rendered_request_signer_did",
    ]
    .into_iter()
    .all(|field| row.get(field).is_none_or(serde_json::Value::is_null));
    if !identity_matches || string_field(row, "call_state") != Some("running") || !unbound {
        anyhow::bail!(
            "InferenceCall {} exact version {} is not its intended unbound running V1",
            call.call_id,
            version.version.composite_commit_cid
        );
    }
    Ok(version)
}

pub(super) async fn persist_rendered_request_binding(
    node: Arc<EmbeddedNode>,
    call: &InferenceCallRecord,
    running_call: &SignedDocumentVersionRef,
    rendered_request: &SignedDocumentVersionRef,
) -> Result<()> {
    if running_call.version.doc_id.trim().is_empty()
        || running_call.version.composite_commit_cid.trim().is_empty()
        || rendered_request.version.doc_id.trim().is_empty()
        || rendered_request
            .version
            .composite_commit_cid
            .trim()
            .is_empty()
    {
        anyhow::bail!(
            "InferenceCall {} render binding requires exact document versions",
            call.call_id
        );
    }
    if running_call.signer_did != call.agent_did || rendered_request.signer_did != call.agent_did {
        anyhow::bail!(
            "InferenceCall {} render binding signers must equal agent DID {}",
            call.call_id,
            call.agent_did
        );
    }
    let current =
        resolve_running_call_version(node.as_ref(), &running_call.version.doc_id, call).await?;
    if &current != running_call {
        anyhow::bail!(
            "InferenceCall {} running V1 changed before RenderedRequest binding",
            call.call_id
        );
    }

    let mutation = update_render_binding_mutation(call, running_call, rendered_request);
    let response = super::query::execute_for_call(node.as_ref(), call, mutation).await?;
    if response.has_errors() {
        anyhow::bail!(
            "binding InferenceCall {} to RenderedRequest failed: {:?}",
            call.call_id,
            response.errors
        );
    }
    let ids = mutation_doc_ids(response.data.as_ref(), "update_InferenceCall");
    if !ids.is_empty() && ids.as_slice() != [running_call.version.doc_id.as_str()] {
        anyhow::bail!(
            "binding InferenceCall {} returned unexpected document ids {ids:?}",
            call.call_id
        );
    }
    // A zero-row acknowledgement can mean either CAS loss or response loss.
    // Exact V2 reload distinguishes an idempotent same binding from a
    // conflicting writer and is therefore safe for both cases.
    verify_current_render_binding(node.as_ref(), call, running_call, rendered_request).await
}

pub(super) async fn persist_call_queued(
    node: Arc<EmbeddedNode>,
    call: &InferenceCallRecord,
) -> Result<String> {
    let now = chrono::Utc::now().to_rfc3339();
    let mutation = add_call_mutation(call, "queued", None, Some(&now), None, None, None);
    let resp = super::query::execute_for_call(node.as_ref(), call, mutation).await?;
    if resp.has_errors() {
        anyhow::bail!("persisting queued InferenceCall failed: {:?}", resp.errors);
    }
    let doc_id = extract_inference_call_doc_id(resp.data.as_ref())?;
    if let Err(twin_error) = verify_no_logical_call_twin(node.as_ref(), &doc_id, call).await {
        let cleanup = persist_existing_call_terminal(
            node,
            &doc_id,
            call,
            "queued",
            "cancelled",
            Some("Cancelled"),
            None,
        )
        .await;
        if let Err(cleanup_error) = cleanup {
            anyhow::bail!(
                "{twin_error:#}; quarantining newly created queued _docID={doc_id} also failed: {cleanup_error:#}"
            );
        }
        return Err(twin_error);
    }
    Ok(doc_id)
}

pub(super) async fn persist_call_started(
    node: Arc<EmbeddedNode>,
    call: &InferenceCallRecord,
) -> Result<SignedDocumentVersionRef, CompletionError> {
    let now = chrono::Utc::now().to_rfc3339();
    let mutation = add_call_mutation(call, "running", None, Some(&now), Some(&now), None, None);
    let resp = super::query::execute_for_call(node.as_ref(), call, mutation)
        .await
        .map_err(completion_persistence_error)?;
    if resp.has_errors() {
        return Err(CompletionError::ProviderError(format!(
            "persisting running InferenceCall failed: {:?}",
            resp.errors
        )));
    }
    let doc_id =
        extract_inference_call_doc_id(resp.data.as_ref()).map_err(completion_persistence_error)?;
    if let Err(twin_error) = verify_no_logical_call_twin(node.as_ref(), &doc_id, call).await {
        let cleanup = persist_existing_call_terminal(
            node,
            &doc_id,
            call,
            "running",
            "failed",
            Some("StreamDroppedBeforeTerminalResponse"),
            None,
        )
        .await;
        let error = match cleanup {
            Ok(()) => twin_error,
            Err(cleanup_error) => anyhow::anyhow!(
                "{twin_error:#}; quarantining newly created running _docID={doc_id} also failed: {cleanup_error:#}"
            ),
        };
        return Err(completion_persistence_error(error));
    }
    resolve_running_call_version(node.as_ref(), &doc_id, call)
        .await
        .map_err(completion_persistence_error)
}

pub(super) async fn persist_existing_call_running(
    node: Arc<EmbeddedNode>,
    doc_id: &str,
    call: &InferenceCallRecord,
) -> Result<SignedDocumentVersionRef> {
    verify_no_logical_call_twin(node.as_ref(), doc_id, call).await?;
    let now = chrono::Utc::now().to_rfc3339();
    let mutation = update_call_running_mutation(doc_id, call, &now);
    let facts = ExactTransitionFacts::Running { started_at: now };
    execute_exact_transition_with_retry(
        node.clone(),
        &mutation,
        "persist existing running InferenceCall",
        doc_id,
        call,
        "queued",
        "running",
        &facts,
    )
    .await?;
    resolve_running_call_version(node.as_ref(), doc_id, call).await
}

pub(super) async fn persist_terminal_call(
    node: Arc<EmbeddedNode>,
    call: InferenceCallRecord,
    call_state: &str,
    failure_reason: Option<&str>,
    usage: Option<Usage>,
) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    let mutation = add_call_mutation(
        &call,
        call_state,
        failure_reason,
        Some(&now),
        None,
        Some(&now),
        usage,
    );
    let resp = super::query::execute_for_call(node.as_ref(), &call, mutation).await?;
    if resp.has_errors() {
        anyhow::bail!(
            "persisting terminal InferenceCall failed: {:?}",
            resp.errors
        );
    }
    Ok(())
}

pub(super) async fn persist_existing_call_terminal(
    node: Arc<EmbeddedNode>,
    doc_id: &str,
    call: &InferenceCallRecord,
    expected_call_state: &str,
    call_state: &str,
    failure_reason: Option<&str>,
    usage: Option<Usage>,
) -> Result<()> {
    persist_existing_call_terminal_with_render(
        node,
        doc_id,
        call,
        expected_call_state,
        call_state,
        failure_reason,
        usage,
        None,
        None,
    )
    .await
}

pub(super) async fn persist_existing_call_terminal_with_render(
    node: Arc<EmbeddedNode>,
    doc_id: &str,
    call: &InferenceCallRecord,
    expected_call_state: &str,
    call_state: &str,
    failure_reason: Option<&str>,
    usage: Option<Usage>,
    running_call: Option<&SignedDocumentVersionRef>,
    rendered_request: Option<&SignedDocumentVersionRef>,
) -> Result<()> {
    if running_call.is_some() != rendered_request.is_some() {
        anyhow::bail!(
            "InferenceCall {} terminal render provenance is incomplete",
            call.call_id
        );
    }
    let now = chrono::Utc::now().to_rfc3339();
    let facts = ExactTransitionFacts::Terminal {
        ended_at: now.clone(),
        failure_reason: failure_reason.map(str::to_owned),
        prompt_tokens: usage.map(|usage| usage.input_tokens),
        completion_tokens: usage.map(|usage| usage.output_tokens),
        cached_input_tokens: usage.map(|usage| usage.cached_input_tokens),
        running_call: running_call.cloned(),
        rendered_request: rendered_request.cloned(),
    };
    let mutation = update_call_terminal_mutation(
        doc_id,
        call,
        expected_call_state,
        call_state,
        failure_reason,
        &now,
        usage,
        running_call,
        rendered_request,
    );
    execute_exact_transition_with_retry(
        node,
        &mutation,
        "persist existing terminal InferenceCall",
        doc_id,
        call,
        expected_call_state,
        call_state,
        &facts,
    )
    .await
}

async fn execute_exact_transition_with_retry(
    node: Arc<EmbeddedNode>,
    mutation: &str,
    operation: &str,
    doc_id: &str,
    call: &InferenceCallRecord,
    expected_call_state: &str,
    target_call_state: &str,
    facts: &ExactTransitionFacts,
) -> Result<()> {
    for attempt in 1..=EXACT_TRANSITION_ATTEMPTS {
        let response = super::query::execute_for_call(node.as_ref(), call, mutation).await?;
        if response.has_errors() {
            anyhow::bail!("{operation} failed: {:?}", response.errors);
        }
        match verify_exact_transition(
            node.as_ref(),
            response.data.as_ref(),
            doc_id,
            call,
            expected_call_state,
            target_call_state,
            facts,
        )
        .await?
        {
            ExactTransitionResult::Complete => return Ok(()),
            ExactTransitionResult::RetryExpectedState if attempt < EXACT_TRANSITION_ATTEMPTS => {
                tokio::task::yield_now().await;
            }
            ExactTransitionResult::RetryExpectedState => {
                anyhow::bail!(
                    "InferenceCall exact transition remained in expected state after {EXACT_TRANSITION_ATTEMPTS} attempts: _docID={doc_id} call_id={} expected_state={expected_call_state} target_state={target_call_state}",
                    call.call_id
                );
            }
        }
    }
    unreachable!("bounded exact-transition loop always returns")
}

fn add_call_mutation(
    call: &InferenceCallRecord,
    call_state: &str,
    failure_reason: Option<&str>,
    queued_at: Option<&str>,
    started_at: Option<&str>,
    ended_at: Option<&str>,
    usage: Option<Usage>,
) -> String {
    let queued_at = optional_graphql_string("queued_at", queued_at);
    let started_at = optional_graphql_string("started_at", started_at);
    let ended_at = optional_graphql_string("ended_at", ended_at);
    let failure_reason = optional_graphql_string("failure_reason", failure_reason);
    let (prompt_tokens, completion_tokens, cached_input_tokens) = usage_fields(usage);
    format!(
        r#"mutation {{
            add_InferenceCall(input: {{
                call_id: "{call_id}",
                runtime_instance_id: "{runtime_instance_id}",
                request_id: "{request_id}",
                call_seq: {call_seq},
                backend_id: "{backend_id}",
                behavior_id: "{behavior_id}",
                agent_did: "{agent_did}",
                call_kind: "{call_kind}",
                attempt: {attempt},
                call_state: "{call_state}",
                {failure_reason}
                {queued_at}
                {started_at}
                {ended_at}
                priority: 0,
                queue_depth_at_enqueue: {queue_depth_at_enqueue},
                controller_generation: {controller_generation},
                backend_config_fingerprint: "{backend_config_fingerprint}"
                {prompt_tokens}
                {completion_tokens}
                {cached_input_tokens}
            }}) {{ _docID }}
        }}"#,
        call_id = escape_graphql_string(&call.call_id),
        runtime_instance_id = escape_graphql_string(&call.runtime_instance_id),
        request_id = escape_graphql_string(&call.request_id),
        call_seq = call.call_seq,
        backend_id = escape_graphql_string(&call.backend_id),
        behavior_id = escape_graphql_string(&call.behavior_id),
        agent_did = escape_graphql_string(&call.agent_did),
        call_kind = call.call_kind.as_str(),
        attempt = call.attempt,
        call_state = call_state,
        failure_reason = failure_reason,
        queued_at = queued_at,
        started_at = started_at,
        ended_at = ended_at,
        queue_depth_at_enqueue = call.queue_depth_at_enqueue,
        controller_generation = call.controller_generation,
        backend_config_fingerprint = escape_graphql_string(&call.backend_config_fingerprint),
        prompt_tokens = prompt_tokens,
        completion_tokens = completion_tokens,
        cached_input_tokens = cached_input_tokens,
    )
}

fn update_call_running_mutation(
    doc_id: &str,
    call: &InferenceCallRecord,
    started_at: &str,
) -> String {
    format!(
        r#"mutation {{
            update_InferenceCall(
                filter: {{
                    _docID: {{ _eq: "{doc_id}" }},
                    call_id: {{ _eq: "{call_id}" }},
                    runtime_instance_id: {{ _eq: "{runtime_instance_id}" }},
                    request_id: {{ _eq: "{request_id}" }},
                    agent_did: {{ _eq: "{agent_did}" }},
                    controller_generation: {{ _eq: {controller_generation} }},
                    call_state: {{ _eq: "queued" }}
                }},
                input: {{
                    call_state: "running",
                    started_at: "{started_at}"
                }}
            ) {{ _docID }}
        }}"#,
        doc_id = escape_graphql_string(doc_id),
        call_id = escape_graphql_string(&call.call_id),
        runtime_instance_id = escape_graphql_string(&call.runtime_instance_id),
        request_id = escape_graphql_string(&call.request_id),
        agent_did = escape_graphql_string(&call.agent_did),
        controller_generation = call.controller_generation,
        started_at = escape_graphql_string(started_at),
    )
}

fn update_render_binding_mutation(
    call: &InferenceCallRecord,
    running_call: &SignedDocumentVersionRef,
    rendered_request: &SignedDocumentVersionRef,
) -> String {
    format!(
        r#"mutation {{
            update_InferenceCall(
                filter: {{
                    _docID: {{ _eq: "{doc_id}" }},
                    call_id: {{ _eq: "{call_id}" }},
                    runtime_instance_id: {{ _eq: "{runtime_instance_id}" }},
                    request_id: {{ _eq: "{request_id}" }},
                    agent_did: {{ _eq: "{agent_did}" }},
                    controller_generation: {{ _eq: {controller_generation} }},
                    call_state: {{ _eq: "running" }},
                    rendered_from_call_commit_cid: {{ _eq: null }},
                    rendered_request_doc_id: {{ _eq: null }},
                    rendered_request_composite_commit_cid: {{ _eq: null }},
                    rendered_request_signer_did: {{ _eq: null }}
                }},
                input: {{
                    rendered_from_call_commit_cid: "{call_cid}",
                    rendered_request_doc_id: "{render_doc_id}",
                    rendered_request_composite_commit_cid: "{render_cid}",
                    rendered_request_signer_did: "{render_signer}"
                }}
            ) {{ _docID }}
        }}"#,
        doc_id = escape_graphql_string(&running_call.version.doc_id),
        call_id = escape_graphql_string(&call.call_id),
        runtime_instance_id = escape_graphql_string(&call.runtime_instance_id),
        request_id = escape_graphql_string(&call.request_id),
        agent_did = escape_graphql_string(&call.agent_did),
        controller_generation = call.controller_generation,
        call_cid = escape_graphql_string(&running_call.version.composite_commit_cid),
        render_doc_id = escape_graphql_string(&rendered_request.version.doc_id),
        render_cid = escape_graphql_string(&rendered_request.version.composite_commit_cid),
        render_signer = escape_graphql_string(&rendered_request.signer_did),
    )
}

async fn verify_current_render_binding(
    node: &EmbeddedNode,
    call: &InferenceCallRecord,
    running_call: &SignedDocumentVersionRef,
    rendered_request: &SignedDocumentVersionRef,
) -> Result<()> {
    let identity = super::query::call_identity(node, call)?;
    let bound = verified_current_signed_document_version_with_identity(
        node,
        "InferenceCall",
        &running_call.version.doc_id,
        Some(identity),
    )
    .await?;
    if bound.signer_did != call.agent_did {
        anyhow::bail!(
            "InferenceCall {} bound V2 signer does not match agent DID",
            call.call_id
        );
    }
    let cid = escape_graphql_string(&bound.version.composite_commit_cid);
    let response = super::query::execute_for_call(
        node,
        call,
        format!(
            r#"{{ InferenceCall(cid: ["{cid}"]) {{
                _docID call_id call_state rendered_from_call_commit_cid rendered_request_doc_id
                rendered_request_composite_commit_cid rendered_request_signer_did
            }} }}"#
        ),
    )
    .await?;
    if response.has_errors() {
        anyhow::bail!(
            "reloading bound InferenceCall {} failed: {:?}",
            call.call_id,
            response.errors
        );
    }
    let rows = response
        .data
        .as_ref()
        .and_then(|data| data.get("InferenceCall"))
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let [row] = rows.as_slice() else {
        anyhow::bail!(
            "bound InferenceCall {} exact reload returned {} rows",
            call.call_id,
            rows.len()
        );
    };
    let matches = string_field(row, "_docID") == Some(running_call.version.doc_id.as_str())
        && string_field(row, "call_id") == Some(call.call_id.as_str())
        && string_field(row, "call_state") == Some("running")
        && string_field(row, "rendered_from_call_commit_cid")
            == Some(running_call.version.composite_commit_cid.as_str())
        && string_field(row, "rendered_request_doc_id")
            == Some(rendered_request.version.doc_id.as_str())
        && string_field(row, "rendered_request_composite_commit_cid")
            == Some(rendered_request.version.composite_commit_cid.as_str())
        && string_field(row, "rendered_request_signer_did")
            == Some(rendered_request.signer_did.as_str());
    if !matches {
        anyhow::bail!(
            "InferenceCall {} bound V2 facts conflict with requested RenderedRequest",
            call.call_id
        );
    }
    Ok(())
}

fn update_call_terminal_mutation(
    doc_id: &str,
    call: &InferenceCallRecord,
    expected_call_state: &str,
    call_state: &str,
    failure_reason: Option<&str>,
    ended_at: &str,
    usage: Option<Usage>,
    running_call: Option<&SignedDocumentVersionRef>,
    rendered_request: Option<&SignedDocumentVersionRef>,
) -> String {
    let failure_reason = optional_graphql_string("failure_reason", failure_reason);
    let (prompt_tokens, completion_tokens, cached_input_tokens) = usage_fields(usage);
    let render_filters = match (running_call, rendered_request) {
        (Some(call), Some(render)) => format!(
            r#"
                    rendered_from_call_commit_cid: {{ _eq: "{}" }},
                    rendered_request_doc_id: {{ _eq: "{}" }},
                    rendered_request_composite_commit_cid: {{ _eq: "{}" }},
                    rendered_request_signer_did: {{ _eq: "{}" }},"#,
            escape_graphql_string(&call.version.composite_commit_cid),
            escape_graphql_string(&render.version.doc_id),
            escape_graphql_string(&render.version.composite_commit_cid),
            escape_graphql_string(&render.signer_did),
        ),
        _ => String::new(),
    };
    format!(
        r#"mutation {{
            update_InferenceCall(
                filter: {{
                    _docID: {{ _eq: "{doc_id}" }},
                    call_id: {{ _eq: "{call_id}" }},
                    runtime_instance_id: {{ _eq: "{runtime_instance_id}" }},
                    request_id: {{ _eq: "{request_id}" }},
                    agent_did: {{ _eq: "{agent_did}" }},
                    controller_generation: {{ _eq: {controller_generation} }},
                    {render_filters}
                    call_state: {{ _eq: "{expected_call_state}" }}
                }},
                input: {{
                    call_state: "{call_state}",
                    {failure_reason}
                    ended_at: "{ended_at}"
                    {prompt_tokens}
                    {completion_tokens}
                    {cached_input_tokens}
                }}
            ) {{ _docID }}
        }}"#,
        doc_id = escape_graphql_string(doc_id),
        call_id = escape_graphql_string(&call.call_id),
        runtime_instance_id = escape_graphql_string(&call.runtime_instance_id),
        request_id = escape_graphql_string(&call.request_id),
        agent_did = escape_graphql_string(&call.agent_did),
        controller_generation = call.controller_generation,
        render_filters = render_filters,
        expected_call_state = escape_graphql_string(expected_call_state),
        call_state = call_state,
        failure_reason = failure_reason,
        ended_at = escape_graphql_string(ended_at),
        prompt_tokens = prompt_tokens,
        completion_tokens = completion_tokens,
        cached_input_tokens = cached_input_tokens,
    )
}

async fn verify_exact_transition(
    node: &EmbeddedNode,
    data: Option<&serde_json::Value>,
    doc_id: &str,
    call: &InferenceCallRecord,
    expected_call_state: &str,
    target_call_state: &str,
    facts: &ExactTransitionFacts,
) -> Result<ExactTransitionResult> {
    let returned_doc_ids = mutation_doc_ids(data, "update_InferenceCall");
    if returned_doc_ids.as_slice() == [doc_id] {
        return Ok(ExactTransitionResult::Complete);
    }
    if !returned_doc_ids.is_empty() {
        anyhow::bail!(
            "InferenceCall exact transition returned unexpected document ids: _docID={doc_id} call_id={} expected_state={expected_call_state} target_state={target_call_state} returned_doc_ids={returned_doc_ids:?}",
            call.call_id
        );
    }

    let escaped_doc_id = escape_graphql_string(doc_id);
    let query = format!(
        r#"{{
            InferenceCall(filter: {{ _docID: {{ _eq: "{escaped_doc_id}" }} }}) {{
                _docID
                call_id
                runtime_instance_id
                request_id
                agent_did
                controller_generation
                call_state
                started_at
                ended_at
                failure_reason
                prompt_tokens
                completion_tokens
                cached_input_tokens
                rendered_from_call_commit_cid
                rendered_request_doc_id
                rendered_request_composite_commit_cid
                rendered_request_signer_did
            }}
        }}"#
    );
    let current = super::query::execute_for_call(node, call, query).await?;
    if current.has_errors() {
        anyhow::bail!(
            "InferenceCall transition returned document ids {returned_doc_ids:?}, then exact reload failed for _docID={doc_id}: {:?}",
            current.errors
        );
    }
    let observed = current
        .data
        .as_ref()
        .and_then(|data| data.get("InferenceCall"))
        .and_then(serde_json::Value::as_array)
        .and_then(|rows| rows.first());
    let Some(observed) = observed else {
        anyhow::bail!(
            "InferenceCall exact transition matched no document and exact reload found no row: _docID={doc_id} call_id={} expected_state={expected_call_state} target_state={target_call_state}",
            call.call_id
        );
    };
    let observed_call_id = observed.get("call_id").and_then(serde_json::Value::as_str);
    let observed_call_state = observed
        .get("call_state")
        .and_then(serde_json::Value::as_str);
    let observed_identity_matches = observed_call_id == Some(call.call_id.as_str())
        && observed
            .get("runtime_instance_id")
            .and_then(serde_json::Value::as_str)
            == Some(call.runtime_instance_id.as_str())
        && observed
            .get("request_id")
            .and_then(serde_json::Value::as_str)
            == Some(call.request_id.as_str())
        && observed
            .get("agent_did")
            .and_then(serde_json::Value::as_str)
            == Some(call.agent_did.as_str())
        && observed
            .get("controller_generation")
            .and_then(serde_json::Value::as_u64)
            == Some(call.controller_generation);
    if observed_identity_matches {
        if observed_call_state == Some(target_call_state) {
            if facts.matches(observed) {
                return Ok(ExactTransitionResult::Complete);
            }
            anyhow::bail!(
                "InferenceCall exact transition reached target state with conflicting facts: _docID={doc_id} call_id={} target_state={target_call_state}",
                call.call_id
            );
        }
        if observed_call_state == Some(expected_call_state) {
            return Ok(ExactTransitionResult::RetryExpectedState);
        }
    }
    anyhow::bail!(
        "InferenceCall exact transition conflict: _docID={doc_id} call_id={} expected_state={expected_call_state} target_state={target_call_state} returned_doc_ids={returned_doc_ids:?} observed_call_id={observed_call_id:?} observed_state={observed_call_state:?}",
        call.call_id
    )
}

async fn verify_no_logical_call_twin(
    node: &EmbeddedNode,
    doc_id: &str,
    call: &InferenceCallRecord,
) -> Result<()> {
    let call_id = &call.call_id;
    let escaped_call_id = escape_graphql_string(call_id);
    let query = format!(
        r#"{{
            InferenceCall(filter: {{ call_id: {{ _eq: "{escaped_call_id}" }} }}) {{ _docID }}
        }}"#
    );
    let response = super::query::execute_for_call(node, call, query).await?;
    if response.has_errors() {
        anyhow::bail!(
            "checking InferenceCall logical uniqueness for call_id={call_id}: {:?}",
            response.errors
        );
    }
    let doc_ids = response
        .data
        .as_ref()
        .and_then(|data| data.get("InferenceCall"))
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|row| row.get("_docID").and_then(serde_json::Value::as_str))
        .collect::<Vec<_>>();
    if doc_ids.as_slice() == [doc_id] {
        return Ok(());
    }
    anyhow::bail!(
        "InferenceCall logical identity conflict for call_id={call_id}: created _docID={doc_id}, visible _docIDs={doc_ids:?}"
    )
}

fn mutation_doc_ids(data: Option<&serde_json::Value>, field: &str) -> Vec<String> {
    let Some(value) = data.and_then(|data| data.get(field)) else {
        return Vec::new();
    };
    if let Some(doc_id) = value.get("_docID").and_then(serde_json::Value::as_str) {
        return vec![doc_id.to_owned()];
    }
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|row| {
            row.get("_docID")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .collect()
}

fn optional_graphql_string(field: &str, value: Option<&str>) -> String {
    value
        .map(|value| format!(r#"{field}: "{}","#, escape_graphql_string(value)))
        .unwrap_or_default()
}

fn usage_fields(usage: Option<Usage>) -> (String, String, String) {
    match usage {
        Some(usage) => (
            format!("prompt_tokens: {},", usage.input_tokens),
            format!("completion_tokens: {},", usage.output_tokens),
            format!("cached_input_tokens: {},", usage.cached_input_tokens),
        ),
        None => (String::new(), String::new(), String::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admission::CallKind;
    use crate::schema::ensure_schemas;
    use std::sync::OnceLock;

    fn identity() -> &'static crate::test_support::SignedTestIdentity {
        static IDENTITY: OnceLock<crate::test_support::SignedTestIdentity> = OnceLock::new();
        IDENTITY.get_or_init(|| {
            crate::test_support::signed_test_identity("gents-admission-persistence-tests")
        })
    }

    async fn test_node() -> Arc<EmbeddedNode> {
        let node = Arc::new(
            EmbeddedNode::builder()
                .with_node_identity_did(identity().did())
                .build()
                .await
                .unwrap(),
        );
        ensure_schemas(node.as_ref()).await.unwrap();
        node
    }

    fn call(call_id: &str, request_id: &str) -> InferenceCallRecord {
        InferenceCallRecord {
            call_id: call_id.to_owned(),
            runtime_instance_id: "runtime-exact-target-test".to_owned(),
            request_id: request_id.to_owned(),
            call_seq: 1,
            backend_id: "backend-exact-target-test".to_owned(),
            behavior_id: "default".to_owned(),
            agent_did: identity().did().to_owned(),
            call_kind: CallKind::Inference,
            attempt: 1,
            queue_depth_at_enqueue: 0,
            controller_generation: 1,
            backend_config_fingerprint: "exact-target-test".to_owned(),
        }
    }

    async fn call_identity_and_state(node: &EmbeddedNode, doc_id: &str) -> (String, String) {
        let doc_id = escape_graphql_string(doc_id);
        let response = node
            .execute(&format!(
                r#"{{
                    InferenceCall(filter: {{ _docID: {{ _eq: "{doc_id}" }} }}) {{
                        call_id
                        call_state
                    }}
                }}"#
            ))
            .await;
        assert!(!response.has_errors(), "{:?}", response.errors);
        let row = response
            .data
            .as_ref()
            .and_then(|data| data.get("InferenceCall"))
            .and_then(serde_json::Value::as_array)
            .and_then(|rows| rows.first())
            .expect("exact InferenceCall row");
        (
            row["call_id"].as_str().unwrap().to_owned(),
            row["call_state"].as_str().unwrap().to_owned(),
        )
    }

    #[test]
    fn render_binding_is_compare_and_set_from_unbound_running_v1() {
        let call = call("call-render-cas", "request-render-cas");
        let running = SignedDocumentVersionRef::new(
            crate::DocumentVersionRef::new("call-doc", "call-v1"),
            call.agent_did.clone(),
        );
        let rendered = SignedDocumentVersionRef::new(
            crate::DocumentVersionRef::new("render-doc", "render-v1"),
            call.agent_did.clone(),
        );
        let mutation = update_render_binding_mutation(&call, &running, &rendered);

        for field in [
            "rendered_from_call_commit_cid",
            "rendered_request_doc_id",
            "rendered_request_composite_commit_cid",
            "rendered_request_signer_did",
        ] {
            assert!(
                mutation.contains(&format!(r#"{field}: {{ _eq: null }}"#)),
                "binding must CAS nullable {field}: {mutation}"
            );
        }
        assert!(mutation.contains(r#"rendered_from_call_commit_cid: "call-v1""#));
        assert!(mutation.contains(r#"rendered_request_doc_id: "render-doc""#));
        assert!(mutation.contains(r#"rendered_request_composite_commit_cid: "render-v1""#));
        assert!(mutation.contains(&format!(
            r#"rendered_request_signer_did: "{}""#,
            call.agent_did
        )));
    }

    #[tokio::test]
    async fn existing_transitions_reject_sibling_and_stale_document_mutation() {
        let node = test_node().await;
        let first = call("call-exact-first", "request-exact-first");
        let sibling = call("call-exact-sibling", "request-exact-sibling");
        let first_doc_id = persist_call_queued(node.clone(), &first).await.unwrap();
        let sibling_doc_id = persist_call_queued(node.clone(), &sibling).await.unwrap();

        persist_existing_call_running(node.clone(), &first_doc_id, &first)
            .await
            .unwrap();
        let sibling_error = persist_existing_call_running(node.clone(), &sibling_doc_id, &first)
            .await
            .unwrap_err();
        assert!(
            sibling_error
                .to_string()
                .contains("InferenceCall logical identity conflict"),
            "{sibling_error:#}"
        );
        assert_eq!(
            call_identity_and_state(node.as_ref(), &sibling_doc_id).await,
            (sibling.call_id.clone(), "queued".to_owned())
        );

        persist_existing_call_terminal(
            node.clone(),
            &first_doc_id,
            &first,
            "running",
            "completed",
            None,
            None,
        )
        .await
        .unwrap();
        let stale_error = persist_existing_call_terminal(
            node.clone(),
            &first_doc_id,
            &first,
            "running",
            "failed",
            Some("StreamDroppedBeforeTerminalResponse"),
            None,
        )
        .await
        .unwrap_err();
        assert!(
            stale_error
                .to_string()
                .contains("observed_state=Some(\"completed\")"),
            "{stale_error:#}"
        );
        assert_eq!(
            call_identity_and_state(node.as_ref(), &first_doc_id).await,
            (first.call_id, "completed".to_owned())
        );
    }
}
