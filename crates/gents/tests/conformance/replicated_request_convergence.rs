use super::*;

use gents::__test_internals::{
    drain_automated_wakeups, reconcile_coalesced_pending_request, QueueSource,
};
use gents::{DefraWatcher, Watcher, TERMINAL_REDRIVE_CAP};

const CONVERGENCE_CREATED_AT: &str = "2026-03-23T00:00:00Z";
const OWNER_DID: &str = AGENT_DID;
const FOREIGN_DID: &str = "did:test:foreign-owner";
const REQUESTER_DID: &str = "did:test:requester";

#[derive(Debug, Deserialize)]
struct ConvergenceRow {
    status: String,
    lifecycle_state: String,
    agent_did: String,
    failure_reason: Option<String>,
    terminalized_at: Option<String>,
    terminal_redrive_attempts: Option<i64>,
}

async fn create_owned_request(
    node: &EmbeddedNode,
    request_id: &str,
    session_id: &str,
    agent_did: &str,
    status: &str,
    lifecycle_state: &str,
) -> String {
    create_owned_request_with_times(
        node,
        request_id,
        session_id,
        agent_did,
        status,
        lifecycle_state,
        CONVERGENCE_CREATED_AT,
        CONVERGENCE_CREATED_AT,
    )
    .await
}

async fn seed_owned_request_projection(node: &EmbeddedNode, session_id: &str, request_id: &str) {
    create_agent_session(node, session_id, AGENT_NAME, CONVERGENCE_CREATED_AT).await;
    upsert_conversation(node, session_id, request_id, "hello", "processing").await;
}

#[allow(clippy::too_many_arguments)]
async fn create_owned_request_with_times(
    node: &EmbeddedNode,
    request_id: &str,
    session_id: &str,
    agent_did: &str,
    status: &str,
    lifecycle_state: &str,
    created_at: &str,
    terminalized_at: &str,
) -> String {
    create_owned_request_with_times_and_requester(
        node,
        request_id,
        session_id,
        agent_did,
        status,
        lifecycle_state,
        created_at,
        terminalized_at,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn create_routed_owned_request_with_times(
    node: &EmbeddedNode,
    request_id: &str,
    session_id: &str,
    agent_did: &str,
    status: &str,
    lifecycle_state: &str,
    created_at: &str,
    terminalized_at: &str,
) -> String {
    create_owned_request_with_times_and_requester(
        node,
        request_id,
        session_id,
        agent_did,
        status,
        lifecycle_state,
        created_at,
        terminalized_at,
        Some(REQUESTER_DID),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn create_owned_request_with_times_and_requester(
    node: &EmbeddedNode,
    request_id: &str,
    session_id: &str,
    agent_did: &str,
    status: &str,
    lifecycle_state: &str,
    created_at: &str,
    terminalized_at: &str,
    requester_did: Option<&str>,
) -> String {
    let is_terminal = matches!(
        lifecycle_state,
        "completed" | "failed" | "superseded" | "dead" | "interrupted"
    );
    let escaped_request_id = escape_graphql_string(request_id);
    let escaped_session_id = escape_graphql_string(session_id);
    let escaped_agent_did = escape_graphql_string(agent_did);
    let escaped_status = escape_graphql_string(status);
    let escaped_lifecycle_state = escape_graphql_string(lifecycle_state);
    let escaped_created_at = escape_graphql_string(created_at);
    let escaped_terminalized_at = escape_graphql_string(terminalized_at);
    let requester_field = requester_did
        .map(|did| format!("requester_did: \"{}\",", escape_graphql_string(did)))
        .unwrap_or_default();
    let terminal_fields = if is_terminal {
        format!(", terminalized_at: \"{escaped_terminalized_at}\", terminal_redrive_attempts: 0")
    } else {
        String::new()
    };
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{escaped_request_id}",
                agent_did: "{escaped_agent_did}",
                {requester_field}
                behavior_id: "{AGENT_NAME}",
                session_id: "{escaped_session_id}",
                retry_parent_request: "",
                retry_root_request: "{escaped_request_id}",
                superseded_by_request: "",
                content: "hello",
                status: "{escaped_status}",
                lifecycle_state: "{escaped_lifecycle_state}",
                backend_id: "",
                execution_origin: "interactive",
                created_at: "{escaped_created_at}",
                retry_count: 0,
                max_retries: {max_retries}
                {terminal_fields}
            }}) {{ _docID }}
        }}"#,
        max_retries = gents::lifecycle::DEFAULT_REQUEST_MAX_RETRIES,
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "create owned request failed: {:?}",
        resp.errors
    );

    let query = format!(
        r#"{{
            AgentRequest(filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }}, limit: 1) {{
                _docID
            }}
        }}"#
    );
    first_row::<support::DocIdRow>(&node.execute(&query).await, "AgentRequest").doc_id
}

#[derive(Debug, Deserialize)]
struct QueueConvergenceRow {
    status: String,
    lifecycle_state: String,
    agent_did: String,
    superseded_by_request: Option<String>,
}

fn coalesce_wakeup_metadata(session_id: &str) -> String {
    format!(
        r#"{{"queue":{{"source":"background_completion","policy":"coalesce","key":"background_completion:{session_id}","queued_after_request_id":null}}}}"#
    )
}

async fn create_queue_request(
    node: &EmbeddedNode,
    request_id: &str,
    session_id: &str,
    agent_did: &str,
    execution_origin: &str,
    metadata: &str,
    created_at: &str,
) -> String {
    let escaped_request_id = escape_graphql_string(request_id);
    let escaped_session_id = escape_graphql_string(session_id);
    let escaped_agent_did = escape_graphql_string(agent_did);
    let escaped_execution_origin = escape_graphql_string(execution_origin);
    let escaped_metadata = escape_graphql_string(metadata);
    let escaped_created_at = escape_graphql_string(created_at);
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{escaped_request_id}",
                agent_did: "{escaped_agent_did}",
                behavior_id: "{AGENT_NAME}",
                session_id: "{escaped_session_id}",
                retry_parent_request: "",
                retry_root_request: "{escaped_request_id}",
                superseded_by_request: "",
                content: "wake up",
                metadata: "{escaped_metadata}",
                status: "pending",
                lifecycle_state: "pending",
                backend_id: "",
                execution_origin: "{escaped_execution_origin}",
                created_at: "{escaped_created_at}",
                retry_count: 0,
                max_retries: {max_retries}
            }}) {{ _docID }}
        }}"#,
        max_retries = gents::lifecycle::DEFAULT_REQUEST_MAX_RETRIES,
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "create queue request failed: {:?}",
        resp.errors
    );

    let query = format!(
        r#"{{
            AgentRequest(filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }}, limit: 1) {{
                _docID
            }}
        }}"#
    );
    first_row::<support::DocIdRow>(&node.execute(&query).await, "AgentRequest").doc_id
}

async fn fetch_queue_convergence_row(node: &EmbeddedNode, request_id: &str) -> QueueConvergenceRow {
    let escaped_request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            AgentRequest(filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }}, limit: 1) {{
                status
                lifecycle_state
                agent_did
                superseded_by_request
            }}
        }}"#
    );
    first_row(&node.execute(&query).await, "AgentRequest")
}

async fn fetch_convergence_row(node: &EmbeddedNode, request_id: &str) -> ConvergenceRow {
    let escaped_request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            AgentRequest(filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }}, limit: 1) {{
                status
                lifecycle_state
                agent_did
                failure_reason
                terminalized_at
                terminal_redrive_attempts
            }}
        }}"#
    );
    first_row(&node.execute(&query).await, "AgentRequest")
}

async fn composite_commit_count(node: &EmbeddedNode, doc_id: &str) -> usize {
    let escaped_doc_id = escape_graphql_string(doc_id);
    let query = format!(
        r#"query {{
            _commits(
                docID: ["{escaped_doc_id}"],
                filter: {{ fieldName: {{ _eq: "_C" }} }}
            ) {{ cid }}
        }}"#
    );
    let response = node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "query composite request commits failed: {:?}",
        response.errors
    );
    response
        .data
        .as_ref()
        .and_then(|data| data.get("_commits"))
        .and_then(serde_json::Value::as_array)
        .map_or(0, Vec::len)
}

pub(super) async fn single_claimer_watcher_never_claims_foreign_replica() {
    let db = test_db("convergence-single-claimer").await;

    let foreign_doc = create_owned_request(
        &db.node,
        "convergence-foreign-req",
        "convergence-foreign-session",
        FOREIGN_DID,
        "pending",
        "pending",
    )
    .await;

    let mut watcher = DefraWatcher::new(db.node.clone(), OWNER_DID);

    assert!(
        watcher
            .try_fetch_request(&foreign_doc)
            .await
            .unwrap()
            .is_none(),
        "foreign replica must not be claimable via try_fetch_request"
    );

    let scanned = tokio::time::timeout(Duration::from_millis(750), watcher.next_request()).await;
    assert!(
        scanned.is_err(),
        "watcher scan (next_request) must never yield a foreign replica, got {scanned:?}"
    );

    let own_doc = create_owned_request(
        &db.node,
        "convergence-own-req",
        "convergence-own-session",
        OWNER_DID,
        "pending",
        "pending",
    )
    .await;
    let claimed = watcher.try_fetch_request(&own_doc).await.unwrap();
    let claimed = claimed.expect("own pending request must be claimable (guards a vacuous filter)");
    assert_eq!(
        claimed.agent_did, OWNER_DID,
        "the claimable request must be the owner's own"
    );
}

pub(super) async fn terminal_convergence_redrive_reasserts_unconverged_terminal() {
    let db = test_db("convergence-terminal-redrive").await;

    create_routed_owned_request_with_times(
        &db.node,
        "convergence-owned-failed",
        "convergence-owned-failed-session",
        OWNER_DID,
        "error",
        "failed",
        CONVERGENCE_CREATED_AT,
        CONVERGENCE_CREATED_AT,
    )
    .await;
    create_routed_owned_request_with_times(
        &db.node,
        "convergence-foreign-failed",
        "convergence-foreign-failed-session",
        FOREIGN_DID,
        "error",
        "failed",
        CONVERGENCE_CREATED_AT,
        CONVERGENCE_CREATED_AT,
    )
    .await;
    create_owned_request(
        &db.node,
        "convergence-owned-processing",
        "convergence-owned-processing-session",
        OWNER_DID,
        "processing",
        "processing",
    )
    .await;
    create_owned_request(
        &db.node,
        "convergence-owned-local-terminal",
        "convergence-owned-local-terminal-session",
        OWNER_DID,
        "completed",
        "completed",
    )
    .await;

    let first = RequestLifecycle::redrive_terminal_convergence(&db.node, OWNER_DID)
        .await
        .unwrap();
    assert_eq!(
        first.scanned, 1,
        "only the owned terminal request is a candidate (foreign + active excluded)"
    );
    assert_eq!(
        first.reasserted, 1,
        "owner must re-assert its one unconverged terminal request"
    );
    assert!(!first.is_noop());

    let owned = fetch_convergence_row(&db.node, "convergence-owned-failed").await;
    assert_eq!(owned.status, "error");
    assert_eq!(owned.lifecycle_state, "failed");
    assert_eq!(owned.terminal_redrive_attempts, Some(1));
    assert!(owned.terminalized_at.is_some());
    let foreign = fetch_convergence_row(&db.node, "convergence-foreign-failed").await;
    assert_eq!(foreign.agent_did, FOREIGN_DID);
    assert_eq!(foreign.status, "error");

    for _ in 1..TERMINAL_REDRIVE_CAP {
        let more = RequestLifecycle::redrive_terminal_convergence(&db.node, OWNER_DID)
            .await
            .unwrap();
        assert_eq!(more.reasserted, 1, "each re-assert under the cap counts");
    }
    let exhausted = RequestLifecycle::redrive_terminal_convergence(&db.node, OWNER_DID)
        .await
        .unwrap();
    assert!(
        exhausted.is_noop(),
        "re-drive must self-terminate after {TERMINAL_REDRIVE_CAP} re-asserts, got {exhausted:?}"
    );

    let foreign_run = RequestLifecycle::redrive_terminal_convergence(&db.node, FOREIGN_DID)
        .await
        .unwrap();
    assert_eq!(
        foreign_run.scanned, 1,
        "the foreign owner's own terminal replica is its sole candidate"
    );
    assert_eq!(foreign_run.reasserted, 1);
}

pub(super) async fn terminal_redrive_window_advances_past_sixty_four_rows() {
    let db = test_db("convergence-terminal-window").await;

    for index in 0..65 {
        create_routed_owned_request_with_times(
            &db.node,
            &format!("convergence-newer-{index:02}"),
            &format!("convergence-newer-session-{index:02}"),
            OWNER_DID,
            "completed",
            "completed",
            "2026-03-23T00:00:00Z",
            "2026-03-23T00:00:01Z",
        )
        .await;
    }
    create_routed_owned_request_with_times(
        &db.node,
        "convergence-old-late-terminal",
        "convergence-old-late-terminal-session",
        OWNER_DID,
        "completed",
        "completed",
        "2020-01-01T00:00:00Z",
        "2026-03-23T00:00:02Z",
    )
    .await;

    for _ in 0..TERMINAL_REDRIVE_CAP {
        let report = RequestLifecycle::redrive_terminal_convergence(&db.node, OWNER_DID)
            .await
            .unwrap();
        assert_eq!(report.scanned, 64);
        assert_eq!(report.reasserted, 64);
        let old = fetch_convergence_row(&db.node, "convergence-old-late-terminal").await;
        assert_eq!(old.terminal_redrive_attempts, Some(0));
    }

    let advanced = RequestLifecycle::redrive_terminal_convergence(&db.node, OWNER_DID)
        .await
        .unwrap();
    assert_eq!(advanced.scanned, 2);
    assert_eq!(advanced.reasserted, 2);
    let old = fetch_convergence_row(&db.node, "convergence-old-late-terminal").await;
    assert_eq!(old.terminal_redrive_attempts, Some(1));
}

pub(super) async fn durable_response_repairs_request_after_terminal_write_gap() {
    let db = test_db("convergence-terminal-repair").await;
    let request_id = "convergence-terminal-repair-request";
    let session_id = "convergence-terminal-repair-session";
    let request_doc_id = create_owned_request(
        &db.node,
        request_id,
        session_id,
        OWNER_DID,
        "processing",
        "processing",
    )
    .await;
    seed_owned_request_projection(&db.node, session_id, request_id).await;
    let request_commits_before_repair = composite_commit_count(&db.node, &request_doc_id).await;
    let response_doc_id =
        create_response_with_status(&db.node, request_id, request_id, session_id, "error").await;
    let escaped_response_doc_id = escape_graphql_string(&response_doc_id);
    let mutation = format!(
        r#"mutation {{
            update_AgentResponse(
                filter: {{ _docID: {{ _eq: "{escaped_response_doc_id}" }} }},
                input: {{ error_message: "provider failed durably" }}
            ) {{ _docID }}
        }}"#
    );
    let response = db.node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "seed terminal reason: {:?}",
        response.errors
    );

    let repaired = RequestLifecycle::repair_terminal_requests(&db.node, OWNER_DID)
        .await
        .unwrap();
    assert_eq!(repaired.repaired, 1);
    assert_eq!(repaired.failed, 0);
    assert_eq!(
        composite_commit_count(&db.node, &request_doc_id).await,
        request_commits_before_repair + 1,
        "one logical terminal repair must add one composite request commit"
    );
    let row = fetch_convergence_row(&db.node, request_id).await;
    assert_eq!(row.status, "error");
    assert_eq!(row.lifecycle_state, "failed");
    assert_eq!(
        row.failure_reason.as_deref(),
        Some("provider failed durably")
    );
    assert!(row.terminalized_at.is_some());
    assert_eq!(row.terminal_redrive_attempts, Some(0));

    let duplicate = RequestLifecycle::repair_terminal_requests(&db.node, OWNER_DID)
        .await
        .unwrap();
    assert_eq!(duplicate.repaired, 0, "duplicate observation is idempotent");

    let provider_request_id = "convergence-provider-interrupted-message";
    let provider_session_id = "convergence-provider-interrupted-session";
    create_owned_request(
        &db.node,
        provider_request_id,
        provider_session_id,
        OWNER_DID,
        "processing",
        "processing",
    )
    .await;
    seed_owned_request_projection(&db.node, provider_session_id, provider_request_id).await;
    let provider_response_doc_id = create_response_with_status(
        &db.node,
        provider_request_id,
        provider_request_id,
        provider_session_id,
        "error",
    )
    .await;
    let escaped_provider_response_doc_id = escape_graphql_string(&provider_response_doc_id);
    let response = db
        .node
        .execute(&format!(
            r#"mutation {{
                update_AgentResponse(
                    filter: {{ _docID: {{ _eq: "{escaped_provider_response_doc_id}" }} }},
                    input: {{ error_message: "interrupted" }}
                ) {{ _docID }}
            }}"#
        ))
        .await;
    assert!(
        !response.has_errors(),
        "seed provider interrupted message: {:?}",
        response.errors
    );

    let provider_repair = RequestLifecycle::repair_terminal_requests(&db.node, OWNER_DID)
        .await
        .unwrap();
    assert_eq!(provider_repair.repaired, 1);
    let provider_row = fetch_convergence_row(&db.node, provider_request_id).await;
    assert_eq!(provider_row.status, "error");
    assert_eq!(provider_row.lifecycle_state, "failed");
    assert_eq!(provider_row.failure_reason.as_deref(), Some("interrupted"));

    let runtime_interrupt_request_id = "convergence-runtime-interrupt-stamp";
    let runtime_interrupt_session_id = "convergence-runtime-interrupt-session";
    create_owned_request(
        &db.node,
        runtime_interrupt_request_id,
        runtime_interrupt_session_id,
        OWNER_DID,
        "processing",
        "processing",
    )
    .await;
    seed_owned_request_projection(
        &db.node,
        runtime_interrupt_session_id,
        runtime_interrupt_request_id,
    )
    .await;
    let runtime_interrupt_response_doc_id = create_response_with_status(
        &db.node,
        runtime_interrupt_request_id,
        runtime_interrupt_request_id,
        runtime_interrupt_session_id,
        "error",
    )
    .await;
    let escaped_runtime_interrupt_response_doc_id =
        escape_graphql_string(&runtime_interrupt_response_doc_id);
    let response = db
        .node
        .execute(&format!(
            r#"mutation {{
                update_AgentResponse(
                    filter: {{ _docID: {{ _eq: "{escaped_runtime_interrupt_response_doc_id}" }} }},
                    input: {{
                        error_message: "interrupted",
                        interrupted_at: "2026-07-10T00:00:00Z"
                    }}
                ) {{ _docID }}
            }}"#
        ))
        .await;
    assert!(
        !response.has_errors(),
        "seed runtime interrupt stamp: {:?}",
        response.errors
    );

    let runtime_interrupt_repair = RequestLifecycle::repair_terminal_requests(&db.node, OWNER_DID)
        .await
        .unwrap();
    assert_eq!(runtime_interrupt_repair.repaired, 1);
    let runtime_interrupt_row = fetch_convergence_row(&db.node, runtime_interrupt_request_id).await;
    assert_eq!(runtime_interrupt_row.status, "interrupted");
    assert_eq!(runtime_interrupt_row.lifecycle_state, "interrupted");
    assert_eq!(
        runtime_interrupt_row.failure_reason.as_deref(),
        Some("interrupted")
    );
}

pub(super) async fn recover_stuck_requests_recovers_claimed_lifecycle_state() {
    let db = test_db("convergence-recover-claimed").await;

    create_owned_request(
        &db.node,
        "convergence-stuck-claimed",
        "convergence-stuck-claimed-session",
        OWNER_DID,
        "claimed",
        "claimed",
    )
    .await;
    seed_owned_request_projection(
        &db.node,
        "convergence-stuck-claimed-session",
        "convergence-stuck-claimed",
    )
    .await;
    create_response_with_status(
        &db.node,
        "convergence-stuck-claimed",
        "convergence-stuck-claimed",
        "convergence-stuck-claimed-session",
        "error",
    )
    .await;

    let report = RequestLifecycle::recover_all(&db.node, OWNER_DID)
        .await
        .unwrap();
    assert_eq!(
        report.requests_recovered, 1,
        "a claimed own-request must be recovered (Lean requestRecoveryStale = claimed ∨ processing)"
    );

    let row = fetch_convergence_row(&db.node, "convergence-stuck-claimed").await;
    assert!(
        matches!(row.lifecycle_state.as_str(), "failed" | "completed"),
        "recovered request must be terminal, got {}",
        row.lifecycle_state
    );
}

pub(super) async fn reconcile_coalesce_never_supersedes_foreign_replica() {
    let db = test_db("convergence-coalesce-foreign").await;
    let session_id = "convergence-coalesce-foreign-session";
    let metadata = coalesce_wakeup_metadata(session_id);
    let key = format!("background_completion:{session_id}");

    create_queue_request(
        &db.node,
        "convergence-coalesce-owner-survivor",
        session_id,
        OWNER_DID,
        "scheduled",
        &metadata,
        "2026-03-23T00:00:00Z",
    )
    .await;
    create_queue_request(
        &db.node,
        "convergence-coalesce-owner-duplicate",
        session_id,
        OWNER_DID,
        "scheduled",
        &metadata,
        "2026-03-23T00:00:01Z",
    )
    .await;
    create_queue_request(
        &db.node,
        "convergence-coalesce-foreign",
        session_id,
        FOREIGN_DID,
        "scheduled",
        &metadata,
        "2026-03-23T00:00:02Z",
    )
    .await;

    let survivor = reconcile_coalesced_pending_request(
        &db.node,
        session_id,
        OWNER_DID,
        QueueSource::BackgroundCompletion,
        &key,
    )
    .await
    .unwrap()
    .expect("owner survivor");
    assert_eq!(
        survivor.request_id, "convergence-coalesce-owner-survivor",
        "the earliest owner row is the coalesce survivor"
    );

    let owner_survivor =
        fetch_queue_convergence_row(&db.node, "convergence-coalesce-owner-survivor").await;
    assert_eq!(owner_survivor.status, "pending");
    assert_eq!(owner_survivor.lifecycle_state, "pending");

    let owner_duplicate =
        fetch_queue_convergence_row(&db.node, "convergence-coalesce-owner-duplicate").await;
    assert_eq!(owner_duplicate.status, "superseded");
    assert_eq!(owner_duplicate.lifecycle_state, "superseded");
    assert_eq!(
        owner_duplicate.superseded_by_request.as_deref(),
        Some("convergence-coalesce-owner-survivor"),
    );

    let foreign = fetch_queue_convergence_row(&db.node, "convergence-coalesce-foreign").await;
    assert_eq!(
        foreign.agent_did, FOREIGN_DID,
        "foreign replica ownership unchanged"
    );
    assert_eq!(
        foreign.status, "pending",
        "foreign replica must not be superseded by the owner's coalesce reconcile"
    );
    assert_eq!(foreign.lifecycle_state, "pending");
    assert_eq!(
        foreign.superseded_by_request.as_deref().unwrap_or(""),
        "",
        "foreign replica must carry no supersede pointer"
    );
}

pub(super) async fn drain_wakeups_never_interrupts_foreign_replica() {
    let db = test_db("convergence-drain-foreign").await;
    let session_id = "convergence-drain-foreign-session";
    let metadata = coalesce_wakeup_metadata(session_id);

    create_queue_request(
        &db.node,
        "convergence-drain-owner",
        session_id,
        OWNER_DID,
        "scheduled",
        &metadata,
        "2026-03-23T00:00:00Z",
    )
    .await;
    create_queue_request(
        &db.node,
        "convergence-drain-foreign",
        session_id,
        FOREIGN_DID,
        "scheduled",
        &metadata,
        "2026-03-23T00:00:01Z",
    )
    .await;

    let drained = drain_automated_wakeups(
        &db.node,
        session_id,
        OWNER_DID,
        "automated wake-up drained because active request was interrupted",
    )
    .await
    .unwrap();
    assert_eq!(
        drained, 1,
        "exactly the owner's own automated wake-up is drained"
    );

    let owner = fetch_queue_convergence_row(&db.node, "convergence-drain-owner").await;
    assert_eq!(owner.status, "interrupted");
    assert_eq!(owner.lifecycle_state, "interrupted");

    let foreign = fetch_queue_convergence_row(&db.node, "convergence-drain-foreign").await;
    assert_eq!(foreign.agent_did, FOREIGN_DID);
    assert_eq!(
        foreign.status, "pending",
        "foreign replica must not be interrupted by the owner's wake-up drain"
    );
    assert_eq!(foreign.lifecycle_state, "pending");
}
