use anyhow::{bail, Context, Result};
use serde::Deserialize;

use super::*;
use crate::client::{ClientCore, ClientCoreOptions, DesktopPaths};

use super::lean_vocab_test::{
    assert_lean_transition_is_legal, lean_contract_snapshot, LeanSessionRecoveryCase,
};

const RECOVERY_AGENT_DID: &str = "did:test:amy";
const RECOVERY_BEHAVIOR_ID: &str = "amy-code";

#[derive(Debug)]
struct RecoveryPreState {
    session_id: String,
    failed_request_id: String,
    existing_request_id: Option<String>,
    pre_latest_request_id: String,
    parent: AgentRequestRow,
}

#[derive(Debug)]
struct ForcedRequestState {
    status: &'static str,
    lifecycle_state: String,
    retry_count: i64,
    max_retries: i64,
    deadline: String,
    backend_id: String,
    execution_origin: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RetryRequestIdInjection {
    new_request_id: String,
}

#[derive(Debug, Deserialize)]
struct RecoveryRequestRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    request_id: String,
    agent_did: String,
    behavior_id: String,
    session_id: String,
    content: String,
    temperature: Option<f64>,
    top_p: Option<f64>,
    top_k: Option<i64>,
    seed: Option<i64>,
    max_tokens: Option<i64>,
    max_total_tokens: Option<i64>,
    metadata: Option<String>,
    status: String,
    lifecycle_state: String,
    backend_id: String,
    execution_origin: String,
    retry_root_request: String,
    retry_parent_request: String,
    retry_parent_request_doc_id: Option<String>,
    retry_count: i64,
    max_retries: i64,
}

#[derive(Debug, Deserialize)]
struct RecoveryConversationRow {
    latest_request_id: String,
}

#[test]
fn prepare_prompt_submission_strips_skill_selector_and_records_metadata() -> Result<()> {
    let (content, options) = prepare_prompt_submission(
        "/triage\ninspect the failure",
        SubmitRequestOptions::default(),
    )?;

    assert_eq!(content, "inspect the failure");
    assert_eq!(
        options.metadata.as_deref(),
        Some(r#"{"selected_skill_ids":["triage"]}"#)
    );
    Ok(())
}

#[tokio::test]
async fn submit_request_rejects_negative_seed_before_store_access() -> Result<()> {
    let node = defra_node::EmbeddedNode::builder().build().await?;
    let error = submit_request(
        &node,
        &ClientStore::default(),
        "session-one",
        "did:key:agent",
        "did:key:requester",
        "hello",
        None,
        SubmitRequestOptions {
            seed: Some(-1),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.to_string(), "seed must be non-negative");
    node.shutdown().await;
    Ok(())
}

#[test]
fn retry_key_and_mutation_are_scoped_to_exact_parent_document() {
    assert_ne!(
        retry_successor_key("parent-doc-a"),
        retry_successor_key("parent-doc-b")
    );
    let field = build_add_agent_request_field(
        "request",
        "retry-request",
        "did:test:agent",
        "did:test:requester",
        "behavior",
        "session",
        "logical-parent",
        Some("parent-doc-a"),
        "logical-root",
        "retry content",
        "2026-08-09T00:00:00Z",
        1,
        3,
        "backend",
        "interactive",
        "",
    );
    assert!(field.contains(r#"retry_parent_request_doc_id: "parent-doc-a""#));
}

#[test]
fn prepare_prompt_submission_merges_selected_skills_with_metadata() -> Result<()> {
    let (content, options) = prepare_prompt_submission(
        "/skill review inspect",
        SubmitRequestOptions {
            metadata: Some(r#"{"queue":{"source":"manual"}}"#.to_string()),
            ..SubmitRequestOptions::default()
        },
    )?;

    let metadata: serde_json::Value = serde_json::from_str(options.metadata.as_deref().unwrap())?;
    assert_eq!(content, "inspect");
    assert_eq!(metadata["queue"]["source"], "manual");
    assert_eq!(metadata["selected_skill_ids"][0], "review");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_session_recovery_cases_drive_desktop_retry_request() -> Result<()> {
    let cases = &lean_contract_snapshot().session_recovery_cases;
    let legal_count = cases.iter().filter(|case| case.legal).count();
    let illegal_count = cases.len() - legal_count;
    assert_eq!(
        (legal_count, illegal_count),
        (2, 16),
        "Lean SessionRecovery case split changed; update this desktop driver before bumping"
    );

    let tempdir = tempfile::tempdir()?;
    let core = ClientCore::start_with_paths_and_options(
        DesktopPaths::from_root(tempdir.path()),
        ClientCoreOptions::local_only(),
    )
    .await?;

    let result = async {
        for case in cases {
            assert_eq!(case.action.as_str(), "reissueFailed");
            if case.legal {
                assert_lean_transition_is_legal(
                    "SessionRecovery",
                    &case.pre_latest_state,
                    &case.post_latest_state,
                );
            } else {
                assert!(
                    case.post_latest_state.is_empty(),
                    "illegal Lean case {} must not carry a post latest state",
                    case.name
                );
            }

            drive_session_recovery_case_with_core(&core, case)
                .await
                .with_context(|| format!("driving Lean SessionRecovery case {}", case.name))?;
        }

        Ok::<(), anyhow::Error>(())
    }
    .await;
    let shutdown = core.shutdown().await;
    result?;
    shutdown?;
    Ok(())
}

async fn drive_session_recovery_case_with_core(
    core: &ClientCore,
    case: &LeanSessionRecoveryCase,
) -> Result<()> {
    let pre = seed_session_recovery_pre_state(core, case).await?;
    let pre_count = request_count_for_session_for_test(core.node(), &pre.session_id).await?;
    assert_eq!(
        pre_count, case.pre_request_count,
        "pre request count must match Lean witness for {}",
        case.name
    );
    assert_eq!(
        latest_request_id_for_session_for_test(core.node(), &pre.session_id).await?,
        pre.pre_latest_request_id,
        "pre latest request role must match Lean witness for {}",
        case.name
    );
    if case.pre_latest_exists {
        assert_eq!(
            fetch_request_row_for_test(core.node(), &pre.pre_latest_request_id)
                .await?
                .lifecycle_state,
            case.pre_latest_state,
            "pre latest request state must match Lean witness for {}",
            case.name
        );
    } else {
        assert_eq!(
            request_count_by_id_for_test(core.node(), &pre.pre_latest_request_id).await?,
            0,
            "missing latest request must be absent for {}",
            case.name
        );
    }
    if case.pre_failed_exists {
        assert_eq!(
            pre.parent.lifecycle_state.as_deref(),
            Some(case.pre_failed_state.as_str()),
            "pre failed request state must match Lean witness for {}",
            case.name
        );
    }
    assert_eq!(
        pre.parent.retry_count,
        Some(case.pre_retry_count as i64),
        "pre retry_count must match Lean witness for {}",
        case.name
    );
    assert_eq!(
        pre.parent.max_retries,
        Some(case.max_retries as i64),
        "pre max_retries must match Lean witness for {}",
        case.name
    );
    assert_eq!(
        pre.parent.backend_id.as_deref(),
        Some(case.pre_backend.as_str()),
        "pre backend_id must match Lean witness for {}",
        case.name
    );
    assert_eq!(
        pre.parent.execution_origin.as_deref(),
        Some(case.pre_origin.as_str()),
        "pre execution_origin must match Lean witness for {}",
        case.name
    );

    let injected_new_request_id = injected_new_request_id(case, &pre)?;
    let result = match injected_new_request_id.clone() {
        Some(injection) => {
            retry_request_with_id_injection_for_test(core, &pre.parent, injection).await
        }
        None => core.retry_request(&pre.parent).await,
    };

    if case.legal {
        let submitted = result?;
        assert_legal_session_recovery_post_state(core, case, &pre, &submitted.request_id).await
    } else {
        assert_illegal_session_recovery_post_state(
            core,
            case,
            &pre,
            result.unwrap_err().to_string(),
            injected_new_request_id
                .as_ref()
                .map(|injection| injection.new_request_id.as_str()),
        )
        .await
    }
}

async fn seed_session_recovery_pre_state(
    core: &ClientCore,
    case: &LeanSessionRecoveryCase,
) -> Result<RecoveryPreState> {
    let created = core
        .create_conversation(RECOVERY_AGENT_DID, Some(RECOVERY_BEHAVIOR_ID))
        .await?;
    let failed_is_latest = case.pre_latest_id == case.failed_id;
    let should_seed_failed =
        case.pre_request_ids.contains(&case.failed_id) || !case.pre_failed_exists;
    let should_seed_existing = case
        .pre_request_ids
        .iter()
        .any(|request_id| *request_id != case.failed_id);

    let mut failed = None;
    let mut existing = None;
    if failed_is_latest {
        if should_seed_existing {
            existing = Some(
                submit_recovery_seed_request(core, &created.session_id, case, "existing").await?,
            );
        }
        if should_seed_failed {
            failed = Some(
                submit_recovery_seed_request(core, &created.session_id, case, "failed").await?,
            );
        }
    } else {
        if should_seed_failed {
            failed = Some(
                submit_recovery_seed_request(core, &created.session_id, case, "failed").await?,
            );
        }
        if should_seed_existing {
            existing = Some(
                submit_recovery_seed_request(core, &created.session_id, case, "latest").await?,
            );
        }
    }

    let failed_request_id = failed
        .as_ref()
        .map(|request| request.request_id.clone())
        .unwrap_or_else(|| format!("missing-failed-{}", case.name));
    let existing_request_id = existing.as_ref().map(|request| request.request_id.clone());
    let pre_latest_request_id = if case.pre_latest_exists {
        if failed_is_latest {
            failed_request_id.clone()
        } else {
            existing_request_id.clone().with_context(|| {
                format!(
                    "Lean case {} expected an existing latest request",
                    case.name
                )
            })?
        }
    } else {
        failed_request_id.clone()
    };

    let expected_parent_status = retry_parent_status_for_case(case);
    if let Some(failed) = failed.as_ref() {
        force_request_state_for_test(
            core.node(),
            &failed.request_id,
            &forced_retry_parent_state(case),
        )
        .await?;
    }
    if let Some(existing) = existing.as_ref() {
        if !failed_is_latest {
            force_request_state_for_test(
                core.node(),
                &existing.request_id,
                &forced_latest_request_state(case),
            )
            .await?;
        }
    }
    if !case.pre_failed_exists {
        delete_request_by_id_for_test(core.node(), &failed_request_id).await?;
    }
    core.refresh_store().await?;

    let parent = if case.pre_failed_exists {
        let parent = request_from_store_for_test(core, &failed_request_id)?;
        assert_eq!(
            parent.lifecycle_state.as_deref(),
            Some(case.pre_failed_state.as_str()),
            "seeded retry parent lifecycle must match Lean witness for {}",
            case.name
        );
        assert_eq!(
            parent.status.as_deref(),
            Some(expected_parent_status),
            "seeded retry parent admission/status must match Lean witness for {}",
            case.name
        );
        assert_eq!(
            parent.backend_id.as_deref(),
            Some(case.pre_backend.as_str()),
            "seeded retry parent backend_id did not refresh into the desktop store for {}",
            case.name
        );
        assert_eq!(
            parent.execution_origin.as_deref(),
            Some(case.pre_origin.as_str()),
            "seeded retry parent execution_origin did not refresh into the desktop store for {}",
            case.name
        );
        parent
    } else {
        synthetic_missing_retry_parent(case, &created.session_id, &failed_request_id)
    };

    Ok(RecoveryPreState {
        session_id: created.session_id,
        failed_request_id,
        existing_request_id,
        pre_latest_request_id,
        parent,
    })
}

async fn submit_recovery_seed_request(
    core: &ClientCore,
    session_id: &str,
    case: &LeanSessionRecoveryCase,
    role: &str,
) -> Result<SubmittedRequest> {
    core.submit_request(
        session_id,
        RECOVERY_AGENT_DID,
        &format!("{role} request for {}", case.name),
        None,
    )
    .await
}

fn synthetic_missing_retry_parent(
    case: &LeanSessionRecoveryCase,
    session_id: &str,
    request_id: &str,
) -> AgentRequestRow {
    AgentRequestRow {
        request_id: request_id.to_string(),
        agent_did: Some(RECOVERY_AGENT_DID.to_string()),
        requester_did: None,
        behavior_id: Some(RECOVERY_BEHAVIOR_ID.to_string()),
        session_id: Some(session_id.to_string()),
        retry_parent_request: Some(String::new()),
        retry_root_request: Some(request_id.to_string()),
        superseded_by_request: Some(String::new()),
        content: Some(format!("missing request for {}", case.name)),
        temperature: None,
        top_p: None,
        top_k: None,
        seed: None,
        max_tokens: None,
        max_total_tokens: None,
        metadata: None,
        status: Some(retry_parent_status_for_case(case).to_string()),
        lifecycle_state: Some(case.pre_failed_state.clone()),
        backend_id: Some(case.pre_backend.clone()),
        execution_origin: Some(case.pre_origin.clone()),
        caused_by_trigger_id: None,
        caused_by_trigger_kind: None,
        caused_by_correlation: None,
        caused_by_trigger_context: None,
        caused_by_parent_request_id: None,
        failure_reason: Some(String::new()),
        terminalized_at: None,
        terminal_redrive_attempts: None,
        created_at: Some(chrono::Utc::now().to_rfc3339()),
        claimed_at: None,
        deadline: Some(recovery_deadline_for_case(case)),
        retry_count: Some(case.pre_retry_count as i64),
        max_retries: Some(case.max_retries as i64),
        interrupt_requested_at: None,
        valid_until: None,
        workspace_id: None,
        workspace_authority: None,
        workspace_owner_deployment_id: None,
        workspace_seal_hash: None,
    }
}

#[derive(Debug, Deserialize)]
struct DocIdForTest {
    #[serde(rename = "_docID")]
    doc_id: String,
}

async fn delete_request_by_id_for_test(node: &EmbeddedNode, request_id: &str) -> Result<()> {
    let escaped_request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"{{
                AgentRequest(
                    filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                    limit: 1
                ) {{ _docID }}
            }}"#
    );
    let row: DocIdForTest = query_single_for_test(node, &query, "AgentRequest").await?;
    let escaped_doc_id = escape_graphql_string(&row.doc_id);
    let mutation = format!(
        r#"mutation {{
                delete_AgentRequest(docID: "{escaped_doc_id}") {{ _docID }}
            }}"#
    );
    let resp = node.execute(&mutation).await;
    if resp.has_errors() {
        bail!("delete request {request_id} failed: {:?}", resp.errors);
    }
    Ok(())
}

async fn assert_legal_session_recovery_post_state(
    core: &ClientCore,
    case: &LeanSessionRecoveryCase,
    pre: &RecoveryPreState,
    new_request_id: &str,
) -> Result<()> {
    assert_eq!(case.pre_request_count + 1, case.post_request_count);
    assert_eq!(
        request_count_for_session_for_test(core.node(), &pre.session_id).await?,
        case.post_request_count,
        "post request count must match Lean witness for {}",
        case.name
    );
    assert_eq!(
        latest_request_id_for_session_for_test(core.node(), &pre.session_id).await?,
        new_request_id,
        "new request must become latest for {}",
        case.name
    );
    assert_eq!(
        core.store().focused_request_id(),
        Some(new_request_id.to_string())
    );

    let new_request = fetch_request_row_for_test(core.node(), new_request_id).await?;
    assert_eq!(new_request.request_id, new_request_id);
    assert_eq!(new_request.session_id, pre.session_id);
    assert_eq!(new_request.agent_did, RECOVERY_AGENT_DID);
    assert_eq!(new_request.behavior_id, RECOVERY_BEHAVIOR_ID);
    assert_eq!(
        new_request.content,
        pre.parent.content.as_deref().unwrap_or_default()
    );
    assert_eq!(new_request.status, "pending");
    assert_eq!(new_request.lifecycle_state, case.post_new_state);
    assert_eq!(new_request.retry_parent_request, pre.failed_request_id);
    assert_eq!(new_request.retry_root_request, pre.failed_request_id);
    assert_eq!(new_request.retry_count, case.post_retry_count as i64);
    assert_eq!(new_request.max_retries, case.max_retries as i64);
    if case.origin_preserved {
        assert_eq!(new_request.execution_origin, case.post_new_origin);
    }
    if case.backend_preserved {
        assert_eq!(new_request.backend_id, case.post_new_backend);
    }

    let failed_request = fetch_request_row_for_test(core.node(), &pre.failed_request_id).await?;
    assert_eq!(failed_request.lifecycle_state, case.post_failed_state);
    assert_eq!(failed_request.status, retry_parent_status_for_case(case));
    assert_eq!(failed_request.retry_count, case.pre_retry_count as i64);
    assert_eq!(failed_request.backend_id, case.pre_backend);
    assert_eq!(failed_request.execution_origin, case.pre_origin);
    assert_eq!(
        request_count_by_id_for_test(core.node(), &pre.failed_request_id).await?,
        if case.old_request_retained { 1 } else { 0 },
        "old failed request retention must match Lean witness for {}",
        case.name
    );
    assert_eq!(
        request_count_by_id_for_test(core.node(), new_request_id).await?,
        if case.new_request_inserted { 1 } else { 0 },
        "new request insertion must match Lean witness for {}",
        case.name
    );
    assert!(case.pre_failed_is_latest);
    assert!(!case.post_failed_is_latest);
    assert!(case.post_new_is_latest);

    Ok(())
}

async fn assert_illegal_session_recovery_post_state(
    core: &ClientCore,
    case: &LeanSessionRecoveryCase,
    pre: &RecoveryPreState,
    err: String,
    injected_new_request_id: Option<&str>,
) -> Result<()> {
    let expected = expected_illegal_guard_fragment(case);
    assert!(
        err.contains(expected),
        "illegal case {} should fail guard containing {expected:?}, got: {err}",
        case.name
    );
    assert_eq!(
        request_count_for_session_for_test(core.node(), &pre.session_id).await?,
        case.pre_request_count,
        "illegal case {} must not insert a retry request",
        case.name
    );
    assert_eq!(
        latest_request_id_for_session_for_test(core.node(), &pre.session_id).await?,
        pre.pre_latest_request_id,
        "illegal case {} must not change latest request",
        case.name
    );
    if let Some(request_id) = injected_new_request_id {
        assert_eq!(
            request_count_by_id_for_test(core.node(), request_id).await?,
            1,
            "duplicate-id guard for {} must not add another colliding row",
            case.name
        );
    }

    Ok(())
}

async fn retry_request_with_id_injection_for_test(
    core: &ClientCore,
    parent: &AgentRequestRow,
    injection: RetryRequestIdInjection,
) -> Result<SubmittedRequest> {
    let snapshot = core.store().snapshot();
    let submitted = retry_request_with_request_id(
        core.node(),
        snapshot.as_ref(),
        parent,
        core.principal().did(),
        injection.new_request_id,
    )
    .await?;
    core.store()
        .set_focused_request_id(Some(submitted.request_id.clone()));
    core.refresh_store().await?;
    Ok(submitted)
}

fn forced_retry_parent_state(case: &LeanSessionRecoveryCase) -> ForcedRequestState {
    ForcedRequestState {
        status: retry_parent_status_for_case(case),
        lifecycle_state: case.pre_failed_state.clone(),
        retry_count: case.pre_retry_count as i64,
        max_retries: case.max_retries as i64,
        deadline: recovery_deadline_for_case(case),
        backend_id: case.pre_backend.clone(),
        execution_origin: case.pre_origin.clone(),
    }
}

fn recovery_deadline_for_case(case: &LeanSessionRecoveryCase) -> String {
    let deadline = if case.pre_deadline_exceeded {
        chrono::Utc::now() - chrono::Duration::seconds(5)
    } else {
        chrono::Utc::now() + chrono::Duration::minutes(5)
    };
    deadline.to_rfc3339()
}

fn forced_latest_request_state(case: &LeanSessionRecoveryCase) -> ForcedRequestState {
    ForcedRequestState {
        status: status_for_lifecycle_state(&case.pre_latest_state),
        lifecycle_state: case.pre_latest_state.clone(),
        retry_count: 0,
        max_retries: case.max_retries as i64,
        deadline: (chrono::Utc::now() + chrono::Duration::minutes(5)).to_rfc3339(),
        backend_id: case.pre_backend.clone(),
        execution_origin: case.pre_origin.clone(),
    }
}

fn retry_parent_status_for_case(case: &LeanSessionRecoveryCase) -> &'static str {
    if case.pre_failed_state != "failed" {
        status_for_lifecycle_state(&case.pre_failed_state)
    } else if case.pre_failed_admission == "released" {
        "error"
    } else {
        "processing"
    }
}

fn status_for_lifecycle_state(lifecycle_state: &str) -> &'static str {
    match lifecycle_state {
        "failed" => "error",
        "pending" => "pending",
        "completed" => "completed",
        "superseded" => "superseded",
        "dead" => "dead",
        "interrupted" => "interrupted",
        _ => "processing",
    }
}

fn injected_new_request_id(
    case: &LeanSessionRecoveryCase,
    pre: &RecoveryPreState,
) -> Result<Option<RetryRequestIdInjection>> {
    if !case.pre_new_request_exists {
        return Ok(None);
    }

    let new_request_id = if case.new_id == case.failed_id {
        pre.failed_request_id.clone()
    } else {
        pre.existing_request_id.clone().with_context(|| {
            format!(
                "Lean case {} needs an existing non-failed request id for new_id={}",
                case.name, case.new_id
            )
        })?
    };

    Ok(Some(RetryRequestIdInjection { new_request_id }))
}

fn expected_illegal_guard_fragment(case: &LeanSessionRecoveryCase) -> &'static str {
    // Generated cases assert the first surfaced denial in this production
    // guard order, so future multi-violation cases should choose the same
    // precedence deliberately.
    if !case.pre_failed_exists {
        "not found"
    } else if case.pre_failed_state != "failed" || case.pre_failed_admission != "released" {
        "failed/error"
    } else if case.pre_origin != "interactive" {
        "must be interactive"
    } else if case.pre_retry_count >= case.max_retries {
        "exhausted retry budget"
    } else if case.pre_deadline_exceeded {
        "deadline is closed"
    } else if !case.pre_failed_is_latest {
        "must be latest"
    } else if case.pre_new_request_exists {
        "already exists"
    } else {
        panic!("unhandled illegal SessionRecovery case: {}", case.name);
    }
}

fn request_from_store_for_test(core: &ClientCore, request_id: &str) -> Result<AgentRequestRow> {
    core.store()
        .snapshot()
        .requests
        .iter()
        .find(|row| row.request_id == request_id)
        .cloned()
        .with_context(|| format!("expected request {request_id} in desktop store"))
}

async fn fetch_request_row_for_test(
    node: &EmbeddedNode,
    request_id: &str,
) -> Result<RecoveryRequestRow> {
    let escaped_request_id = escape_graphql_string(request_id);
    query_single_for_test(
            node,
            &format!(
                r#"{{
                    AgentRequest(filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }}, limit: 1) {{
                        _docID
                        request_id
                        agent_did
                        behavior_id
                        session_id
                        content
                        temperature
                        top_p
                        top_k
                        seed
                        max_tokens
                        max_total_tokens
                        metadata
                        status
                        lifecycle_state
                        backend_id
                        execution_origin
                        retry_root_request
                        retry_parent_request
                        retry_parent_request_doc_id
                        retry_count
                        max_retries
                    }}
                }}"#
            ),
            "AgentRequest",
        )
        .await
}

async fn latest_request_id_for_session_for_test(
    node: &EmbeddedNode,
    session_id: &str,
) -> Result<String> {
    let escaped_session_id = escape_graphql_string(session_id);
    let conversation: RecoveryConversationRow = query_single_for_test(
            node,
            &format!(
                r#"{{
                    AgentConversation(filter: {{ session_id: {{ _eq: "{escaped_session_id}" }} }}, limit: 1) {{
                        latest_request_id
                    }}
                }}"#
            ),
            "AgentConversation",
        )
        .await?;
    Ok(conversation.latest_request_id)
}

async fn request_count_for_session_for_test(
    node: &EmbeddedNode,
    session_id: &str,
) -> Result<usize> {
    let escaped_session_id = escape_graphql_string(session_id);
    query_row_count_for_test(
        node,
        &format!(
            r#"{{
                    AgentRequest(filter: {{ session_id: {{ _eq: "{escaped_session_id}" }} }}) {{
                        request_id
                    }}
                }}"#
        ),
        "AgentRequest",
    )
    .await
}

async fn request_count_by_id_for_test(node: &EmbeddedNode, request_id: &str) -> Result<usize> {
    let escaped_request_id = escape_graphql_string(request_id);
    query_row_count_for_test(
        node,
        &format!(
            r#"{{
                    AgentRequest(filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }}) {{
                        _docID
                    }}
                }}"#
        ),
        "AgentRequest",
    )
    .await
}

async fn request_count_by_retry_parent_for_test(
    node: &EmbeddedNode,
    request_id: &str,
) -> Result<usize> {
    let escaped_request_id = escape_graphql_string(request_id);
    query_row_count_for_test(
        node,
        &format!(
            r#"{{
                    AgentRequest(
                        filter: {{ retry_parent_request: {{ _eq: "{escaped_request_id}" }} }}
                    ) {{ _docID }}
                }}"#
        ),
        "AgentRequest",
    )
    .await
}

async fn query_single_for_test<T>(node: &EmbeddedNode, query: &str, root: &str) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let response = node.execute(query).await;
    if response.has_errors() {
        bail!(
            "query {root} failed: {}",
            response
                .errors
                .iter()
                .map(|error| error.message.as_str())
                .collect::<Vec<_>>()
                .join("; ")
        );
    }

    let row = response
        .data
        .as_ref()
        .and_then(|data| data.get(root))
        .and_then(|rows| rows.as_array())
        .and_then(|rows| rows.first())
        .cloned()
        .with_context(|| format!("missing row for {root}"))?;
    Ok(serde_json::from_value(row)?)
}

async fn query_row_count_for_test(node: &EmbeddedNode, query: &str, root: &str) -> Result<usize> {
    let response = node.execute(query).await;
    if response.has_errors() {
        bail!(
            "query {root} count failed: {}",
            response
                .errors
                .iter()
                .map(|error| error.message.as_str())
                .collect::<Vec<_>>()
                .join("; ")
        );
    }

    Ok(response
        .data
        .as_ref()
        .and_then(|data| data.get(root))
        .and_then(|rows| rows.as_array())
        .map(Vec::len)
        .unwrap_or_default())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn desktop_chat_seed_rows_are_scoped_to_the_requester_principal() -> Result<()> {
    let tempdir = tempfile::tempdir()?;
    let core = ClientCore::start_with_paths_and_options(
        DesktopPaths::from_root(tempdir.path()),
        ClientCoreOptions::local_only(),
    )
    .await?;
    let requester_did = core.principal().did().to_string();
    let agent_did = "did:test:amy";
    let created = core
        .create_conversation(agent_did, Some("amy-code"))
        .await?;
    let submitted = core
        .submit_request(
            &created.session_id,
            agent_did,
            "requester route regression",
            None,
        )
        .await?;

    let session_id = escape_graphql_string(&created.session_id);
    let request_id = escape_graphql_string(&submitted.request_id);
    let response = core
            .node()
            .execute(&format!(
                r#"{{
                    AgentRequest(filter: {{ request_id: {{ _eq: "{request_id}" }} }}, limit: 1) {{
                        agent_did
                        requester_did
                    }}
                    AgentSession(filter: {{ session_id: {{ _eq: "{session_id}" }} }}, limit: 1) {{
                        agent_did
                        requester_did
                    }}
                    AgentConversation(filter: {{ session_id: {{ _eq: "{session_id}" }} }}, limit: 1) {{
                        agent_did
                        requester_did
                    }}
                }}"#
            ))
            .await;
    if response.has_errors() {
        bail!(
            "querying desktop requester routes failed: {:?}",
            response.errors
        );
    }
    let data = response.data.context("requester route query data")?;
    for collection in ["AgentRequest", "AgentSession", "AgentConversation"] {
        let row = data
            .get(collection)
            .and_then(serde_json::Value::as_array)
            .and_then(|rows| rows.first())
            .with_context(|| format!("missing {collection} row"))?;
        assert_eq!(
            row.get("agent_did").and_then(serde_json::Value::as_str),
            Some(agent_did),
            "{collection} must retain the remote agent owner"
        );
        assert_eq!(
            row.get("requester_did").and_then(serde_json::Value::as_str),
            Some(requester_did.as_str()),
            "{collection} must carry the immutable desktop requester route"
        );
    }

    core.shutdown().await?;
    Ok(())
}

async fn force_request_state_for_test(
    node: &EmbeddedNode,
    request_id: &str,
    state: &ForcedRequestState,
) -> Result<()> {
    let escaped_request_id = escape_graphql_string(request_id);
    let escaped_status = escape_graphql_string(state.status);
    let escaped_lifecycle_state = escape_graphql_string(&state.lifecycle_state);
    let escaped_deadline = escape_graphql_string(&state.deadline);
    let escaped_backend_id = escape_graphql_string(&state.backend_id);
    let escaped_execution_origin = escape_graphql_string(&state.execution_origin);
    let retry_count = state.retry_count;
    let max_retries = state.max_retries;
    let mutation = format!(
        r#"mutation {{
                update_AgentRequest(
                    filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                    input: {{
                        status: "{escaped_status}",
                        lifecycle_state: "{escaped_lifecycle_state}",
                        retry_count: {retry_count},
                        max_retries: {max_retries},
                        deadline: "{escaped_deadline}",
                        backend_id: "{escaped_backend_id}",
                        execution_origin: "{escaped_execution_origin}"
                    }}
                ) {{ _docID }}
            }}"#
    );
    execute_mutation(node, &mutation, "force_request_state_for_test").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_request_with_injected_id_rejects_duplicate_new_request_id() -> Result<()> {
    let tempdir = tempfile::tempdir()?;
    let core = ClientCore::start_with_paths_and_options(
        DesktopPaths::from_root(tempdir.path()),
        ClientCoreOptions::local_only(),
    )
    .await?;

    let created = core
        .create_conversation("did:test:amy", Some("amy-code"))
        .await?;
    let original = core
        .submit_request(&created.session_id, "did:test:amy", "first attempt", None)
        .await?;
    let mut parent = core
        .store()
        .snapshot()
        .requests
        .iter()
        .find(|row| row.request_id == original.request_id)
        .cloned()
        .context("expected submitted parent request in desktop store")?;

    let deadline = Utc::now() + chrono::Duration::minutes(5);
    force_retry_parent_eligible_for_test(
        core.node(),
        &original.request_id,
        1,
        i64::from(DEFAULT_REQUEST_MAX_RETRIES),
        &deadline.to_rfc3339(),
    )
    .await?;
    parent.status = Some("error".to_string());
    parent.lifecycle_state = Some("failed".to_string());
    parent.deadline = Some(deadline.to_rfc3339());
    parent.retry_count = Some(1);
    parent.max_retries = Some(i64::from(DEFAULT_REQUEST_MAX_RETRIES));

    let duplicate_request_id = "duplicate-retry-request-id";
    seed_duplicate_request_id_for_test(
        core.node(),
        duplicate_request_id,
        &created.session_id,
        "did:test:amy",
        "amy-code",
    )
    .await?;
    assert_eq!(
        request_count_by_id_for_test(core.node(), duplicate_request_id).await?,
        1
    );

    let snapshot = core.store().snapshot();
    let err = retry_request_with_request_id(
        core.node(),
        snapshot.as_ref(),
        &parent,
        core.principal().did(),
        duplicate_request_id.to_string(),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        err.contains("already exists"),
        "duplicate new request id must be rejected before retry insert: {err}"
    );
    assert_eq!(
        request_count_by_id_for_test(core.node(), duplicate_request_id).await?,
        1,
        "failed duplicate retry must not add another row with the colliding request_id"
    );

    core.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_request_preserves_parent_overrides_and_metadata() -> Result<()> {
    let tempdir = tempfile::tempdir()?;
    let core = ClientCore::start_with_paths_and_options(
        DesktopPaths::from_root(tempdir.path()),
        ClientCoreOptions::local_only(),
    )
    .await?;

    let created = core
        .create_conversation("did:test:amy", Some("amy-code"))
        .await?;
    let metadata = r#"{"eval":"amygdala","case":"retry"}"#.to_string();
    let original = core
        .submit_request_with_options(
            &created.session_id,
            "did:test:amy",
            "retry should preserve overrides",
            None,
            SubmitRequestOptions {
                temperature: Some(0.35),
                top_p: Some(0.92),
                top_k: Some(32),
                seed: Some(1234),
                max_tokens: Some(2048),
                max_total_tokens: Some(100_000),
                metadata: Some(metadata.clone()),
                ..SubmitRequestOptions::default()
            },
        )
        .await?;
    let deadline = Utc::now() + chrono::Duration::minutes(5);
    force_retry_parent_eligible_for_test(
        core.node(),
        &original.request_id,
        1,
        i64::from(DEFAULT_REQUEST_MAX_RETRIES),
        &deadline.to_rfc3339(),
    )
    .await?;
    core.refresh_store().await?;

    let parent = request_from_store_for_test(&core, &original.request_id)?;
    assert_eq!(parent.temperature, Some(0.35));
    assert_eq!(parent.top_p, Some(0.92));
    assert_eq!(parent.top_k, Some(32));
    assert_eq!(parent.seed, Some(1234));
    assert_eq!(parent.max_tokens, Some(2048));
    assert_eq!(parent.max_total_tokens, Some(100_000));
    assert_eq!(parent.metadata.as_deref(), Some(metadata.as_str()));

    let submitted = core.retry_request(&parent).await?;
    let retried = fetch_request_row_for_test(core.node(), &submitted.request_id).await?;
    let original_row = fetch_request_row_for_test(core.node(), &original.request_id).await?;
    assert_eq!(retried.retry_parent_request, original.request_id);
    assert_eq!(
        retried.retry_parent_request_doc_id.as_deref(),
        Some(original_row.doc_id.as_str())
    );
    assert_eq!(retried.retry_root_request, original.request_id);
    assert_eq!(retried.temperature, Some(0.35));
    assert_eq!(retried.top_p, Some(0.92));
    assert_eq!(retried.top_k, Some(32));
    assert_eq!(retried.seed, Some(1234));
    assert_eq!(retried.max_tokens, Some(2048));
    assert_eq!(retried.max_total_tokens, Some(100_000));
    assert_eq!(retried.metadata.as_deref(), Some(metadata.as_str()));

    core.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_retry_claims_return_one_durable_successor() -> Result<()> {
    let tempdir = tempfile::tempdir()?;
    let core = ClientCore::start_with_paths_and_options(
        DesktopPaths::from_root(tempdir.path()),
        ClientCoreOptions::local_only(),
    )
    .await?;

    let created = core
        .create_conversation("did:test:amy", Some("amy-code"))
        .await?;
    let original = core
        .submit_request(
            &created.session_id,
            "did:test:amy",
            "retry exactly once",
            None,
        )
        .await?;
    let deadline = Utc::now() + chrono::Duration::minutes(5);
    force_retry_parent_eligible_for_test(
        core.node(),
        &original.request_id,
        0,
        i64::from(DEFAULT_REQUEST_MAX_RETRIES),
        &deadline.to_rfc3339(),
    )
    .await?;
    core.refresh_store().await?;
    let parent = request_from_store_for_test(&core, &original.request_id)?;

    let (left, right) = tokio::join!(core.retry_request(&parent), core.retry_request(&parent));
    let left = left?;
    let right = right?;

    assert_eq!(
        left.request_id, right.request_id,
        "concurrent retry intents must converge on the claimed successor"
    );
    assert_eq!(
        request_count_by_retry_parent_for_test(core.node(), &original.request_id).await?,
        1,
        "only one executable child may be created for a retry parent"
    );
    assert_eq!(
        latest_request_id_for_session_for_test(core.node(), &created.session_id).await?,
        left.request_id
    );

    core.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_uses_effective_latest_when_conversation_points_at_legacy_wake() -> Result<()> {
    let tempdir = tempfile::tempdir()?;
    let core = ClientCore::start_with_paths_and_options(
        DesktopPaths::from_root(tempdir.path()),
        ClientCoreOptions::local_only(),
    )
    .await?;

    let created = core
        .create_conversation("did:test:amy", Some("amy-code"))
        .await?;
    let original = core
        .submit_request(
            &created.session_id,
            "did:test:amy",
            "retry after legacy wake",
            None,
        )
        .await?;
    let deadline = Utc::now() + chrono::Duration::minutes(5);
    force_retry_parent_eligible_for_test(
        core.node(),
        &original.request_id,
        0,
        i64::from(DEFAULT_REQUEST_MAX_RETRIES),
        &deadline.to_rfc3339(),
    )
    .await?;

    let session_id = escape_graphql_string(&created.session_id);
    let now = Utc::now().to_rfc3339();
    let metadata = escape_graphql_string(
        r#"{"queue":{"source":"background_completion","policy":"coalesce","key":"child-1","queued_after_request_id":null}}"#,
    );
    let mutation = format!(
        r#"mutation {{
                wake: create_AgentRequest(input: {{
                    request_id: "legacy-wake",
                    agent_did: "did:test:amy",
                    behavior_id: "amy-code",
                    session_id: "{session_id}",
                    content: "legacy wake",
                    metadata: "{metadata}",
                    status: "pending",
                    lifecycle_state: "pending",
                    execution_origin: "scheduled",
                    created_at: "{now}",
                    retry_count: 0,
                    max_retries: 3
                }}) {{ _docID }}
                conversation: update_AgentConversation(
                    filter: {{ session_id: {{ _eq: "{session_id}" }} }},
                    input: {{
                        latest_request_id: "legacy-wake",
                        updated_at: "{now}"
                    }}
                ) {{ _docID }}
            }}"#
    );
    execute_mutation(core.node(), &mutation, "seed_legacy_wake_for_retry").await?;
    core.refresh_store().await?;

    let parent = request_from_store_for_test(&core, &original.request_id)?;
    let submitted = core.retry_request(&parent).await?;
    let retried = fetch_request_row_for_test(core.node(), &submitted.request_id).await?;
    assert_eq!(retried.retry_parent_request, original.request_id);
    assert_eq!(retried.execution_origin, "interactive");

    core.shutdown().await?;
    Ok(())
}

async fn force_retry_parent_eligible_for_test(
    node: &EmbeddedNode,
    request_id: &str,
    retry_count: i64,
    max_retries: i64,
    deadline: &str,
) -> Result<()> {
    let escaped_request_id = escape_graphql_string(request_id);
    let escaped_deadline = escape_graphql_string(deadline);
    let mutation = format!(
        r#"mutation {{
                update_AgentRequest(
                    filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                    input: {{
                        status: "error",
                        lifecycle_state: "failed",
                        retry_count: {retry_count},
                        max_retries: {max_retries},
                        deadline: "{escaped_deadline}"
                    }}
                ) {{ _docID }}
            }}"#
    );
    execute_mutation(node, &mutation, "force_retry_parent_eligible_for_test").await
}

async fn seed_duplicate_request_id_for_test(
    node: &EmbeddedNode,
    request_id: &str,
    session_id: &str,
    agent_did: &str,
    behavior_id: &str,
) -> Result<()> {
    let created_at = Utc::now().to_rfc3339();
    let request_field = build_add_agent_request_field(
        "duplicate",
        request_id,
        agent_did,
        "did:test:desktop",
        behavior_id,
        session_id,
        "",
        None,
        "",
        "existing duplicate request id occupant",
        &created_at,
        0,
        i64::from(DEFAULT_REQUEST_MAX_RETRIES),
        "",
        "interactive",
        "",
    );
    let mutation = format!("mutation {{\n{request_field}\n}}");
    execute_mutation(node, &mutation, "seed_duplicate_request_id_for_test").await
}
