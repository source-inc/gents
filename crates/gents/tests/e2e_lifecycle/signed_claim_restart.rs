use std::sync::Arc;
use std::time::{Duration, Instant};

use gents::graphql::escape_graphql_string;
use gents::lifecycle::ClaimOutcome;
use gents::watcher::AgentRequest;
use gents::{AgentIdentity as _, KeyIdentity, RequestLifecycle};
use serde::Deserialize;

use crate::support::{test_db_with_identity, TestDb};

const BEHAVIOR_ID: &str = "general";

#[derive(Deserialize)]
struct StatusRow {
    status: String,
    lifecycle_state: Option<String>,
}

async fn create_signed_request(
    db: &TestDb,
    request_id: &str,
    session_id: &str,
    agent_did: &str,
    source_author_did: &str,
) -> AgentRequest {
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{}",
                agent_did: "{}",
                source_author_did: "{}",
                behavior_id: "{BEHAVIOR_ID}",
                session_id: "{}",
                retry_parent_request: "",
                retry_root_request: "{}",
                superseded_by_request: "",
                content: "restart-safe signed request",
                status: "pending",
                lifecycle_state: "pending",
                backend_id: "",
                execution_origin: "interactive",
                created_at: "2026-08-07T00:00:00Z",
                retry_count: 0,
                max_retries: 3,
                subagent_depth: 0
            }}) {{ _docID }}
        }}"#,
        escape_graphql_string(request_id),
        escape_graphql_string(agent_did),
        escape_graphql_string(source_author_did),
        escape_graphql_string(session_id),
        escape_graphql_string(request_id),
    );
    let response = db.node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create signed request failed: {:?}",
        response.errors
    );
    let query = format!(
        r#"query {{
            AgentRequest(filter: {{ request_id: {{ _eq: "{}" }} }}) {{ _docID }}
        }}"#,
        escape_graphql_string(request_id),
    );
    let response = db.node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "created request lookup failed: {:?}",
        response.errors
    );
    let doc_id = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(serde_json::Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("_docID"))
        .and_then(serde_json::Value::as_str)
        .expect("created AgentRequest doc id")
        .to_string();
    AgentRequest {
        doc_id,
        request_id: request_id.to_string(),
        agent_did: agent_did.to_string(),
        requester_did: None,
        behavior_id: Some(BEHAVIOR_ID.to_string()),
        session_id: session_id.to_string(),
        content: "restart-safe signed request".to_string(),
        temperature: None,
        top_p: None,
        top_k: None,
        max_tokens: None,
        metadata: None,
        execution_origin: Some("interactive".to_string()),
        created_at: "2026-08-07T00:00:00Z".to_string(),
        deadline: None,
        subagent_depth: 0,
        caused_by_parent_request_id: None,
        caused_by_parent_tool_call_id: None,
    }
}

async fn status(db: &TestDb, doc_id: &str) -> StatusRow {
    let query = format!(
        r#"query {{
            AgentRequest(filter: {{ _docID: {{ _eq: "{}" }} }}) {{
                status
                lifecycle_state
            }}
        }}"#,
        escape_graphql_string(doc_id),
    );
    let response = db.node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "status query failed: {:?}",
        response.errors
    );
    serde_json::from_value(
        response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(serde_json::Value::as_array)
            .and_then(|rows| rows.first())
            .cloned()
            .expect("AgentRequest status row"),
    )
    .unwrap()
}

async fn composite_commit_count(db: &TestDb, doc_id: &str) -> usize {
    let query = format!(
        r#"query {{
            _commits(
                docID: ["{}"],
                filter: {{ fieldName: {{ _eq: "_C" }} }}
            ) {{ cid }}
        }}"#,
        escape_graphql_string(doc_id),
    );
    let response = db.node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "commit query failed: {:?}",
        response.errors
    );
    response
        .data
        .as_ref()
        .and_then(|data| data.get("_commits"))
        .and_then(serde_json::Value::as_array)
        .map_or(0, Vec::len)
}

async fn crash_and_reopen(db: &mut TestDb) {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        match db.simulate_process_crash().await {
            Ok(()) => return,
            Err(error)
                if error.to_string().contains("is locked by another process")
                    && Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(error) => panic!("persistent DefraDB reopen failed: {error}"),
        }
    }
}

#[tokio::test]
async fn signed_claim_commit_and_rollback_remain_fenced_across_restart() {
    let key_dir = tempfile::tempdir().unwrap();
    let identity = Arc::new(
        KeyIdentity::load_or_create(key_dir.path().join("node.key"), None)
            .expect("signed test identity"),
    );
    let agent_did = identity.did().to_string();
    let mut db = test_db_with_identity("signed-claim-restart", identity).await;

    let committed = create_signed_request(
        &db,
        "restart-committed-request",
        "restart-committed-session",
        &agent_did,
        &agent_did,
    )
    .await;
    let mut lifecycle = RequestLifecycle::new_with_agent_did(
        db.node.clone(),
        BEHAVIOR_ID,
        &agent_did,
        committed.clone(),
        300,
    );
    assert_eq!(
        lifecycle.claim_with_identity().await.unwrap(),
        ClaimOutcome::Claimed
    );
    let committed_count = composite_commit_count(&db, &committed.doc_id).await;
    drop(lifecycle);

    crash_and_reopen(&mut db).await;
    let row = status(&db, &committed.doc_id).await;
    assert_eq!(row.status, "processing");
    assert_eq!(row.lifecycle_state.as_deref(), Some("claimed"));
    let mut replay = RequestLifecycle::new_with_agent_did(
        db.node.clone(),
        BEHAVIOR_ID,
        &agent_did,
        committed.clone(),
        300,
    );
    replay
        .claim_with_identity()
        .await
        .expect_err("a committed signed claim must not be admitted twice after restart");
    drop(replay);
    assert_eq!(
        composite_commit_count(&db, &committed.doc_id).await,
        committed_count,
        "replay after restart must not write another commit"
    );

    let rejected = create_signed_request(
        &db,
        "restart-rejected-request",
        "restart-rejected-session",
        &agent_did,
        "did:key:zDeclaredButNotSigner",
    )
    .await;
    let rejected_count = composite_commit_count(&db, &rejected.doc_id).await;
    let mut rejected_lifecycle = RequestLifecycle::new_with_agent_did(
        db.node.clone(),
        BEHAVIOR_ID,
        &agent_did,
        rejected.clone(),
        300,
    );
    rejected_lifecycle
        .claim_with_identity()
        .await
        .expect_err("declared-author mismatch must roll back");
    drop(rejected_lifecycle);
    assert_eq!(
        composite_commit_count(&db, &rejected.doc_id).await,
        rejected_count,
        "rejected claim must roll back before restart"
    );

    crash_and_reopen(&mut db).await;
    let row = status(&db, &rejected.doc_id).await;
    assert_eq!(row.status, "pending");
    assert_eq!(row.lifecycle_state.as_deref(), Some("pending"));
    assert_eq!(
        composite_commit_count(&db, &rejected.doc_id).await,
        rejected_count,
        "rolled-back claim must remain absent after restart"
    );

    let row = status(&db, &committed.doc_id).await;
    assert_eq!(row.status, "processing");
    assert_eq!(row.lifecycle_state.as_deref(), Some("claimed"));
}
