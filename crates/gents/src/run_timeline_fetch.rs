//! Row fetch for [`crate::run_timeline`]: loads the persisted documents a
//! request's timeline is reconstructed from, over either transport
//! ([`ConfigAccess::Graphql`] or [`ConfigAccess::Local`]). Lifted from the
//! CLI `trace` command so the desktop client shares one fetcher.

use std::collections::{BTreeSet, HashSet};

use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::config_client::ConfigAccess;
use crate::graphql::escape_graphql_string;
use crate::run_timeline::{
    build_run_timeline, RunTimeline, RunTimelineRows, TimelineConversationRow,
    TimelineInferenceCallRow, TimelineMessageRow, TimelineRequestRow, TimelineResponseRow,
    TimelineSessionRow, TimelineToolApprovalFact, TimelineToolCallRow, TimelineToolResultFact,
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
        let mut session_tool_calls =
            load_timeline_tool_calls_for_session(access, session_id).await?;
        attach_exact_tool_facts(access, session_id, &mut session_tool_calls).await?;
        tool_calls.extend(session_tool_calls);
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
                result_doc_id
                result_composite_commit_cid
                result_signer_did
                approval_doc_id
                approval_composite_commit_cid
                approval_signer_did
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

#[derive(serde::Deserialize)]
struct TimelineResultFactRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    tool_call_doc_id: String,
    tool_call_composite_commit_cid: String,
    tool_call_signer_did: String,
    output_text: String,
}

#[derive(serde::Deserialize)]
struct TimelineApprovalFactRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    tool_call_doc_id: String,
    tool_call_composite_commit_cid: String,
    tool_call_signer_did: String,
    approver_did: String,
    decision: String,
    reason: Option<String>,
}

async fn exact_current_ref(
    access: &ConfigAccess,
    collection: &str,
    doc_id: &str,
) -> Result<crate::SignedDocumentVersionRef> {
    if let ConfigAccess::Local(node) = access {
        return crate::document_version::verified_current_signed_document_version(
            node, collection, doc_id,
        )
        .await;
    }
    #[derive(serde::Deserialize)]
    struct Parent {
        cid: String,
        #[serde(rename = "fieldName")]
        field_name: Option<String>,
    }
    #[derive(serde::Deserialize)]
    struct Signature {
        identity: String,
    }
    #[derive(serde::Deserialize)]
    struct Commit {
        cid: String,
        heads: Vec<Parent>,
        signature: Option<Signature>,
    }
    let query = format!(
        r#"{{ _commits(docID: ["{}"], filter: {{ fieldName: {{ _eq: "_C" }} }}) {{ cid heads {{ cid fieldName }} signature {{ identity }} }} }}"#,
        escape_graphql_string(doc_id)
    );
    let rows: Vec<Commit> = serde_json::from_value(
        access
            .execute(&query)
            .await?
            .get("data")
            .and_then(|data| data.get("_commits"))
            .cloned()
            .unwrap_or_default(),
    )?;
    let nested = rows
        .iter()
        .flat_map(|row| row.heads.iter())
        .filter(|head| head.field_name.as_deref() == Some("_C"))
        .map(|head| head.cid.as_str())
        .collect::<HashSet<_>>();
    let current = rows
        .iter()
        .filter(|row| !nested.contains(row.cid.as_str()))
        .collect::<Vec<_>>();
    let [current] = current.as_slice() else {
        anyhow::bail!("{collection} {doc_id} has {} current heads", current.len());
    };
    let signer = current
        .signature
        .as_ref()
        .map(|signature| signature.identity.trim())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("{collection} {doc_id} has no commit signer"))?;
    Ok(crate::SignedDocumentVersionRef::new(
        crate::DocumentVersionRef::new(doc_id, &current.cid),
        signer,
    ))
}

fn complete_edge_doc_id<'a>(
    doc_id: Option<&'a str>,
    composite_commit_cid: Option<&str>,
    signer_did: Option<&str>,
    label: &str,
) -> Result<Option<&'a str>> {
    match (doc_id, composite_commit_cid, signer_did) {
        (None, None, None) => Ok(None),
        (Some(doc_id), Some(cid), Some(signer))
            if !doc_id.trim().is_empty() && !cid.trim().is_empty() && !signer.trim().is_empty() =>
        {
            Ok(Some(doc_id))
        }
        _ => anyhow::bail!("{label} exact reference is partial or empty"),
    }
}

async fn verify_historical_tool_call_ref(
    access: &ConfigAccess,
    source: &crate::SignedDocumentVersionRef,
) -> Result<()> {
    let escaped_cid = escape_graphql_string(&source.version.composite_commit_cid);
    let response = access
        .execute(&format!(
            r#"{{ AgentToolCall(cid: ["{escaped_cid}"]) {{ _docID }} }}"#
        ))
        .await?;
    let rows = response
        .get("data")
        .and_then(|data| data.get("AgentToolCall"))
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("exact AgentToolCall snapshot returned no rows"))?;
    match rows.as_slice() {
        [row]
            if row.get("_docID").and_then(Value::as_str)
                == Some(source.version.doc_id.as_str()) => {}
        rows => anyhow::bail!(
            "exact AgentToolCall commit reconstructed {} rows or a different physical document",
            rows.len()
        ),
    }

    let signer = match access {
        ConfigAccess::Local(node) => node
            .verified_block_signer_did(&source.version.composite_commit_cid)
            .await
            .context("cryptographically verify historical AgentToolCall commit")?,
        ConfigAccess::Graphql(_) => {
            let evidence = access
                .execute(&format!(
                    r#"{{ _commits(cid: ["{escaped_cid}"]) {{ cid signature {{ identity }} }} }}"#
                ))
                .await?;
            let rows = evidence
                .get("data")
                .and_then(|data| data.get("_commits"))
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    anyhow::anyhow!("exact AgentToolCall commit returned no evidence")
                })?;
            let [row] = rows.as_slice() else {
                anyhow::bail!(
                    "exact AgentToolCall commit resolved to {} evidence rows",
                    rows.len()
                );
            };
            if row.get("cid").and_then(Value::as_str)
                != Some(source.version.composite_commit_cid.as_str())
            {
                anyhow::bail!("AgentToolCall commit evidence returned a different CID");
            }
            row.get("signature")
                .and_then(|signature| signature.get("identity"))
                .and_then(Value::as_str)
                .filter(|identity| !identity.trim().is_empty())
                .ok_or_else(|| anyhow::anyhow!("AgentToolCall commit evidence has no signer"))?
                .to_string()
        }
    };
    if signer != source.signer_did {
        anyhow::bail!("historical AgentToolCall signer does not match the pinned fact edge");
    }
    Ok(())
}

async fn attach_exact_tool_facts(
    access: &ConfigAccess,
    session_id: &str,
    calls: &mut [TimelineToolCallRow],
) -> Result<()> {
    let result_query = format!(
        r#"{{ AgentToolResult(filter: {{ session_id: {{ _eq: "{}" }} }}) {{ _docID tool_call_doc_id tool_call_composite_commit_cid tool_call_signer_did output_text }} }}"#,
        escape_graphql_string(session_id)
    );
    let results: Vec<TimelineResultFactRow> =
        load_rows(access, "AgentToolResult", &result_query).await?;
    let approval_query = format!(
        r#"{{ AgentToolApproval(filter: {{ session_id: {{ _eq: "{}" }} }}) {{ _docID tool_call_doc_id tool_call_composite_commit_cid tool_call_signer_did approver_did decision reason }} }}"#,
        escape_graphql_string(session_id)
    );
    let approvals: Vec<TimelineApprovalFactRow> =
        load_rows(access, "AgentToolApproval", &approval_query).await?;

    for call in calls {
        if let Some(result_doc_id) = complete_edge_doc_id(
            call.result_doc_id.as_deref(),
            call.result_composite_commit_cid.as_deref(),
            call.result_signer_did.as_deref(),
            "AgentToolCall result",
        )? {
            let matching = results
                .iter()
                .filter(|row| row.doc_id == result_doc_id)
                .collect::<Vec<_>>();
            let [row] = matching.as_slice() else {
                anyhow::bail!(
                    "exact result ref resolved to {} physical rows",
                    matching.len()
                );
            };
            let call_doc_id = call.doc_id.as_deref().unwrap_or_default();
            if row.tool_call_doc_id != call_doc_id {
                anyhow::bail!("result fact points to a different physical AgentToolCall");
            }
            let exact = exact_current_ref(access, "AgentToolResult", result_doc_id).await?;
            if call.result_composite_commit_cid.as_deref()
                != Some(exact.version.composite_commit_cid.as_str())
                || call.result_signer_did.as_deref() != Some(exact.signer_did.as_str())
            {
                anyhow::bail!("AgentToolCall result edge does not match exact signed result fact");
            }
            verify_historical_tool_call_ref(
                access,
                &crate::SignedDocumentVersionRef::new(
                    crate::DocumentVersionRef::new(
                        &row.tool_call_doc_id,
                        &row.tool_call_composite_commit_cid,
                    ),
                    &row.tool_call_signer_did,
                ),
            )
            .await?;
            call.result_fact = Some(TimelineToolResultFact {
                doc_id: exact.version.doc_id,
                composite_commit_cid: exact.version.composite_commit_cid,
                signer_did: exact.signer_did,
                tool_call_doc_id: row.tool_call_doc_id.clone(),
                tool_call_composite_commit_cid: row.tool_call_composite_commit_cid.clone(),
                tool_call_signer_did: row.tool_call_signer_did.clone(),
                output_text: row.output_text.clone(),
            });
        }
        if let Some(approval_doc_id) = complete_edge_doc_id(
            call.approval_doc_id.as_deref(),
            call.approval_composite_commit_cid.as_deref(),
            call.approval_signer_did.as_deref(),
            "AgentToolCall approval",
        )? {
            let matching = approvals
                .iter()
                .filter(|row| row.doc_id == approval_doc_id)
                .collect::<Vec<_>>();
            let [row] = matching.as_slice() else {
                anyhow::bail!(
                    "exact approval ref resolved to {} physical rows",
                    matching.len()
                );
            };
            if row.tool_call_doc_id != call.doc_id.as_deref().unwrap_or_default() {
                anyhow::bail!("approval fact points to a different physical AgentToolCall");
            }
            let exact = exact_current_ref(access, "AgentToolApproval", approval_doc_id).await?;
            if call.approval_composite_commit_cid.as_deref()
                != Some(exact.version.composite_commit_cid.as_str())
                || call.approval_signer_did.as_deref() != Some(exact.signer_did.as_str())
                || row.approver_did != exact.signer_did
            {
                anyhow::bail!(
                    "AgentToolCall approval edge does not match exact signed approval fact"
                );
            }
            verify_historical_tool_call_ref(
                access,
                &crate::SignedDocumentVersionRef::new(
                    crate::DocumentVersionRef::new(
                        &row.tool_call_doc_id,
                        &row.tool_call_composite_commit_cid,
                    ),
                    &row.tool_call_signer_did,
                ),
            )
            .await?;
            call.approval_fact = Some(TimelineToolApprovalFact {
                doc_id: exact.version.doc_id,
                composite_commit_cid: exact.version.composite_commit_cid,
                signer_did: exact.signer_did,
                tool_call_doc_id: row.tool_call_doc_id.clone(),
                tool_call_composite_commit_cid: row.tool_call_composite_commit_cid.clone(),
                tool_call_signer_did: row.tool_call_signer_did.clone(),
                decision: row.decision.clone(),
                reason: row.reason.clone(),
            });
        }
    }
    Ok(())
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
            }}
        }}"#,
        escape_graphql_string(request_id)
    );
    load_rows(access, "InferenceCall", &query).await
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
