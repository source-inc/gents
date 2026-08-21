use std::time::Duration;

use anyhow::{bail, Context, Result};
use gents_desktop_core::client::{
    ClientCore, ClientCoreOptions, DesktopPaths, SubmitRequestOptions,
};
use serde::Deserialize;
use tokio::time::{sleep, Instant};
use uuid::Uuid;

#[derive(Debug, Deserialize)]
struct RequestRow {
    request_id: String,
    agent_did: String,
    behavior_id: String,
    session_id: String,
    content: String,
    status: String,
    lifecycle_state: String,
    backend_id: String,
    execution_origin: String,
    retry_root_request: String,
    retry_parent_request: String,
    retry_count: i64,
    max_retries: i64,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn submit_request_does_not_create_runtime_projections() -> Result<()> {
    let tempdir = tempfile::tempdir()?;
    let core = ClientCore::start_with_paths_and_options(
        DesktopPaths::from_root(tempdir.path()),
        ClientCoreOptions::local_only(),
    )
    .await?;

    let session_id = Uuid::new_v4().to_string();
    core.submit_request(&session_id, "did:test:amy", "hello", Some("amy-code"))
        .await?;
    let response = core
        .node()
        .execute(&format!(
            r#"{{
                AgentSession(filter: {{ session_id: {{ _eq: "{session_id}" }} }}) {{ _docID }}
                AgentConversation(filter: {{ session_id: {{ _eq: "{session_id}" }} }}) {{ _docID }}
            }}"#
        ))
        .await;
    assert!(
        !response.has_errors(),
        "projection query failed: {:?}",
        response.errors
    );
    let data = response.data.context("projection query data")?;
    for collection in ["AgentSession", "AgentConversation"] {
        assert!(
            data.get(collection)
                .and_then(serde_json::Value::as_array)
                .is_some_and(Vec::is_empty),
            "client submission must not create {collection}"
        );
    }
    core.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn submit_request_writes_request_as_the_only_durable_input() -> Result<()> {
    let tempdir = tempfile::tempdir()?;
    let core = ClientCore::start_with_paths_and_options(
        DesktopPaths::from_root(tempdir.path()),
        ClientCoreOptions::local_only(),
    )
    .await?;

    let session_id = Uuid::new_v4().to_string();
    let submitted = core
        .submit_request(
            &session_id,
            "did:test:amy",
            "  hello   there\noperator  ",
            None,
        )
        .await?;

    let request: RequestRow = query_single(
        core.node(),
        &format!(
            r#"{{
                AgentRequest(filter: {{ request_id: {{ _eq: "{}" }} }}, limit: 1) {{
                    request_id
                    agent_did
                    behavior_id
                    session_id
                    content
                    status
                    lifecycle_state
                    backend_id
                    execution_origin
                    retry_root_request
                    retry_parent_request
                    retry_count
                    max_retries
                }}
            }}"#,
            submitted.request_id
        ),
        "AgentRequest",
    )
    .await?;
    assert_eq!(request.request_id, submitted.request_id);
    assert_eq!(request.agent_did, "did:test:amy");
    assert_eq!(request.behavior_id, "did:test:amy:default");
    assert_eq!(request.session_id, session_id);
    assert_eq!(request.content, "hello   there\noperator");
    assert_eq!(request.status, "pending");
    assert_eq!(request.lifecycle_state, "pending");
    assert!(request.backend_id.is_empty());
    assert_eq!(request.execution_origin, "interactive");
    assert_eq!(request.retry_root_request, submitted.request_id);
    assert!(request.retry_parent_request.is_empty());
    assert_eq!(request.retry_count, 0);
    assert_eq!(request.max_retries, 3);

    assert_eq!(
        core.store().focused_request_id(),
        Some(submitted.request_id.clone())
    );
    assert_eq!(core.store().snapshot().requests.len(), 1);
    core.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resend_preserves_request_overrides_and_metadata() -> Result<()> {
    let tempdir = tempfile::tempdir()?;
    let core = ClientCore::start_with_paths_and_options(
        DesktopPaths::from_root(tempdir.path()),
        ClientCoreOptions::local_only(),
    )
    .await?;

    let session_id = Uuid::new_v4().to_string();

    let metadata_value = r#"{"key":"preserve-me"}"#.to_string();
    let options = SubmitRequestOptions {
        temperature: Some(0.7),
        top_p: Some(0.95),
        top_k: Some(40),
        max_tokens: Some(1234),
        max_total_tokens: Some(10_000),
        metadata: Some(metadata_value.clone()),
        ..SubmitRequestOptions::default()
    };
    let original = core
        .submit_request_with_options(
            &session_id,
            "did:test:amy",
            "please preserve my overrides",
            None,
            options,
        )
        .await?;

    let force_stale = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{ request_id: {{ _eq: "{request_id}" }} }},
                input: {{ lifecycle_state: "dead", failure_reason: "Stale" }}
            ) {{ _docID }}
        }}"#,
        request_id = original.request_id,
    );
    let resp = core.node().execute(&force_stale).await;
    assert!(
        !resp.has_errors(),
        "forcing stale state failed: {:?}",
        resp.errors
    );

    let resent = core.resend_request(&original.request_id).await?;

    let new_row: RequestWithOverridesRow = query_single(
        core.node(),
        &format!(
            r#"{{
                AgentRequest(filter: {{ request_id: {{ _eq: "{}" }} }}, limit: 1) {{
                    request_id
                    session_id
                    retry_parent_request
                    retry_root_request
                    temperature
                    top_p
                    top_k
                    max_tokens
                    max_total_tokens
                    metadata
                }}
            }}"#,
            resent.request_id
        ),
        "AgentRequest",
    )
    .await?;
    assert_eq!(new_row.request_id, resent.request_id);
    assert_eq!(new_row.session_id, resent.session_id);
    assert_ne!(new_row.session_id, original.session_id);
    assert_eq!(new_row.retry_parent_request, original.request_id);
    assert_eq!(new_row.retry_root_request, original.request_id);
    assert_eq!(new_row.temperature, Some(0.7));
    assert_eq!(new_row.top_p, Some(0.95));
    assert_eq!(new_row.top_k, Some(40));
    assert_eq!(new_row.max_tokens, Some(1234));
    assert_eq!(new_row.max_total_tokens, Some(10_000));
    assert_eq!(new_row.metadata.as_deref(), Some(metadata_value.as_str()));

    core.shutdown().await?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct RequestWithOverridesRow {
    request_id: String,
    session_id: String,
    retry_parent_request: String,
    retry_root_request: String,
    temperature: Option<f64>,
    top_p: Option<f64>,
    top_k: Option<i64>,
    max_tokens: Option<i64>,
    max_total_tokens: Option<i64>,
    metadata: Option<String>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_and_remove_peer_persists_peer_directory() -> Result<()> {
    let tempdir_a = tempfile::tempdir()?;
    let tempdir_b = tempfile::tempdir()?;
    let remote = ClientCore::start_with_paths_and_options(
        DesktopPaths::from_root(tempdir_a.path()),
        ClientCoreOptions::local_only(),
    )
    .await?;
    let local = ClientCore::start_with_paths_and_options(
        DesktopPaths::from_root(tempdir_b.path()),
        ClientCoreOptions::local_only(),
    )
    .await?;

    let addr = wait_for_connectable_iroh_addr(&remote).await?;
    let added = local
        .add_peer(
            "Workshop Bay",
            &addr,
            "did:test:workshop",
            Some("http://127.0.0.1:1/api/v0/graphql"),
            None,
        )
        .await?;

    assert_eq!(added.label, "Workshop Bay");
    assert_eq!(added.addr, addr);
    assert!(added.connected);
    assert_eq!(local.configured_peer_count(), 1);
    assert_eq!(local.dialed_peer_count(), 1);
    assert_eq!(local.peer_records().await.len(), 1);
    assert_eq!(
        peer_pairing_desired_count(local.node(), &added.peer_id).await?,
        1
    );

    let removed = local.remove_peer(&added.peer_id).await?;

    assert_eq!(removed.peer_id, added.peer_id);
    assert!(!removed.connected);
    assert!(removed.warning.is_none());
    assert!(local.peer_records().await.is_empty());
    assert_eq!(local.configured_peer_count(), 0);
    assert_eq!(local.dialed_peer_count(), 0);
    assert!(local.p2p().connected_peers().await?.is_empty());
    assert!(local.p2p().get_replicators().await?.is_empty());
    assert_eq!(
        peer_pairing_desired_count(local.node(), &added.peer_id).await?,
        0
    );
    local.shutdown().await?;
    remote.shutdown().await?;
    Ok(())
}

async fn peer_pairing_desired_count(
    node: &defra_node::EmbeddedNode,
    peer_id: &str,
) -> Result<usize> {
    let peer_id = gents_protocol::graphql::escape_graphql_string(peer_id);
    let response = node
        .execute(&format!(
            r#"query {{
                PeerPairingDesired(filter: {{ peer_id: {{ _eq: "{peer_id}" }} }}) {{
                    _docID
                }}
            }}"#
        ))
        .await;
    if response.has_errors() {
        bail!("query PeerPairingDesired failed: {:?}", response.errors);
    }
    Ok(response
        .data
        .as_ref()
        .and_then(|data| data.get("PeerPairingDesired"))
        .and_then(|rows| rows.as_array())
        .map(Vec::len)
        .unwrap_or_default())
}

async fn query_single<T>(node: &defra_node::EmbeddedNode, query: &str, root: &str) -> Result<T>
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

async fn wait_for_connectable_iroh_addr(core: &ClientCore) -> Result<String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let addrs = core.p2p().listen_addresses().await?;
        if let Some(addr) = addrs
            .into_iter()
            .find(|addr| addr.contains("/p2p/") || addr.starts_with("endpoint"))
        {
            return Ok(addr);
        }
        if Instant::now() >= deadline {
            bail!("timed out waiting for desktop listen address");
        }
        sleep(Duration::from_millis(100)).await;
    }
}
