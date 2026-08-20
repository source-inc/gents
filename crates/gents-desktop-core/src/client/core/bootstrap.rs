use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use defra_node::{EmbeddedNode, NodeBuilder, P2PConfig, StorageBackend};
use defra_p2p_adapter::P2POperations as P2POps;
use p2p::iroh::parse_public_peer_addr;
use tokio::sync::{mpsc, watch};
use tokio::time::{sleep, Instant};

use super::super::observe::{spawn_observer_with_selection, ObservedStore};
use super::super::paths::DesktopPaths;
use super::super::peer_directory::{PeerDirectory, PeerRecord};
use super::super::principal_identity::PrincipalIdentity;
use super::super::query::load_full_snapshot_with_peer_records;
use super::super::schema::{
    ensure_runtime_schemas, index_collection_names, subscribe_all_collections,
    subscribed_collection_names,
};
use super::bearer_pairing::{
    current_local_endpoint, install_bearer_replicator_for_record, is_bearer_peer,
    publish_local_endpoint,
};
use super::p2p_ops::{
    p2p_add_replicator, p2p_connect_peer, p2p_connected_peers, p2p_listen_addresses,
    p2p_local_peer_id, p2p_sync_branchable_collection,
};
use super::supervisor::spawn_p2p_supervisor_task;
use super::{
    ClientCore, ClientCoreOptions, ClientPeerStatus, P2PHealth, BOOTSTRAP_OPERATION_BACKOFF,
    BOOTSTRAP_OPERATION_TIMEOUT,
};

impl ClientCore {
    pub async fn start() -> Result<Self> {
        let paths = DesktopPaths::discover()?;
        Self::start_with_paths(paths).await
    }

    pub async fn start_with_paths(paths: DesktopPaths) -> Result<Self> {
        Self::start_with_paths_and_options(paths, ClientCoreOptions::default()).await
    }

    pub async fn start_with_paths_and_options(
        paths: DesktopPaths,
        options: ClientCoreOptions,
    ) -> Result<Self> {
        paths.ensure_root_dirs().await?;
        gents::storage_backend::reject_legacy_rocksdb_store(paths.node_data_dir())?;

        let principal = PrincipalIdentity::load_or_create(&paths).await?;
        let node = Arc::new(
            NodeBuilder::default()
                .data_path(paths.node_data_dir())
                .with_storage_backend(StorageBackend::Lark)
                .with_p2p(desktop_p2p_config(&paths, &options))
                .with_node_identity_did(principal.did())
                .build()
                .await
                .context("starting embedded desktop node")?,
        );

        let peer_directory = Arc::new(tokio::sync::RwLock::new(
            PeerDirectory::load(paths.peer_directory_path()).await?,
        ));
        ensure_runtime_schemas(node.as_ref()).await?;
        ensure_desktop_schema_migrations(Arc::clone(&node)).await?;
        subscribe_all_collections(node.as_ref()).await?;

        let observer_subscription = node.subscribe(&[defra_node::EventName::Update]);

        let (selected_agent_did, _) = watch::channel::<Option<String>>(None);

        let initial_snapshot = {
            let records = peer_directory.read().await.records().to_vec();
            load_full_snapshot_with_peer_records(node.as_ref(), &records, principal.did()).await?
        };
        let (store, _store_updates) = ObservedStore::new(initial_snapshot);
        let observer = spawn_observer_with_selection(
            Arc::clone(&node),
            Arc::clone(&store),
            Arc::clone(&peer_directory),
            principal.did().to_string(),
            observer_subscription,
            selected_agent_did.subscribe(),
        );

        let p2p = node
            .p2p_arc()
            .context("desktop node started without P2P support")?;
        let local_peer_id = p2p_local_peer_id(&p2p)
            .await
            .context("reading desktop P2P peer id")?;
        let listen_addresses = p2p_listen_addresses(&p2p)
            .await
            .context("reading desktop P2P listen addresses")?;

        let (peer_statuses, bootstrap_errors) = {
            let records = peer_directory.read().await.records().to_vec();
            bootstrap_saved_peers(node.as_ref(), &p2p, &records, &options, &principal).await
        };
        let peer_statuses = Arc::new(std::sync::RwLock::new(peer_statuses));
        let (p2p_health, _p2p_health_rx) = watch::channel(P2PHealth::default());
        let initial_health = super::supervisor::probe_p2p_health(&p2p, &P2PHealth::default()).await;
        p2p_health.send_replace(initial_health);
        let (p2p_control, p2p_control_rx) = mpsc::channel(8);
        let p2p_supervisor = spawn_p2p_supervisor_task(
            Arc::clone(&node),
            Arc::clone(&p2p),
            Arc::clone(&peer_directory),
            Arc::clone(&peer_statuses),
            p2p_health.clone(),
            p2p_control_rx,
            Arc::new(principal.clone()),
            options.install_replicators_on_bootstrap,
        );
        Ok(Self {
            paths,
            options,
            principal,
            node,
            p2p,
            peer_directory,
            store,
            observer: tokio::sync::Mutex::new(Some(observer)),
            peer_statuses,
            p2p_supervisor: tokio::sync::Mutex::new(Some(p2p_supervisor)),
            p2p_health,
            selected_agent_did,
            last_loaded_for: tokio::sync::Mutex::new(std::collections::HashMap::new()),
            request_patch_signatures: tokio::sync::Mutex::new(std::collections::HashMap::new()),
            p2p_control: tokio::sync::Mutex::new(Some(p2p_control)),
            last_mutation_error: std::sync::RwLock::new(None),
            local_peer_id,
            listen_addresses,
            bootstrap_errors,
        })
    }
}

fn desktop_p2p_config(paths: &DesktopPaths, options: &ClientCoreOptions) -> P2PConfig {
    P2PConfig {
        port: options.port,
        bind_addr: options.bind_addr,
        relay_mode: options.relay_mode.clone(),
        discovery: options.discovery.clone(),
        max_concurrent_multipath_paths: None,
        secret_key_path: Some(paths.iroh_secret_key_path().to_path_buf()),
        load_persisted_collections: options.load_persisted_collections,
        max_concurrent_dag_fetches: options.max_concurrent_dag_fetches,
        max_concurrent_push_tasks: options.max_concurrent_push_tasks,
        rate_limit_burst: options.rate_limit_burst,
        rate_limit_rate: options.rate_limit_rate,
        max_doc_sync_request_doc_ids: p2p::sync::DEFAULT_MAX_DOC_SYNC_REQUEST_DOC_IDS,
        max_pending_dags: options.max_pending_dags,
    }
}

async fn ensure_desktop_schema_migrations(node: Arc<EmbeddedNode>) -> Result<()> {
    gents::migration::ensure_all_runtime_migrations(node)
        .await
        .context("ensure desktop runtime schema migrations")?;
    Ok(())
}

pub(super) async fn bootstrap_saved_peers(
    node: &EmbeddedNode,
    p2p: &Arc<dyn P2POps>,
    records: &[PeerRecord],
    options: &ClientCoreOptions,
    actor: &PrincipalIdentity,
) -> (Vec<ClientPeerStatus>, Vec<String>) {
    let mut statuses = Vec::with_capacity(records.len());
    let mut errors = Vec::new();

    for record in records {
        let mut status = ClientPeerStatus {
            peer_id: record.peer_id.clone(),
            label: record.label.clone(),
            agent_did: record.agent_did.clone(),
            addr: record.addr.clone(),
            dial_succeeded: false,
            last_error: None,
            pairing: Vec::new(),
        };

        match connect_peer_with_retry(p2p, &record.addr, &record.label).await {
            Ok(()) => {
                status.dial_succeeded = true;

                if options.install_replicators_on_bootstrap {
                    let replicator_result = if is_bearer_peer(record) {
                        if let Err(error) = publish_local_endpoint(node, p2p, actor).await {
                            let message = format!(
                                "peer {} signed endpoint refresh failed: {}",
                                record.label, error
                            );
                            status.last_error = Some(message.clone());
                            errors.push(message);
                            statuses.push(status);
                            continue;
                        }
                        install_bearer_replicator_for_record(p2p, record, actor.did()).await
                    } else {
                        add_replicator_with_retry(
                            p2p,
                            subscribed_collection_names()
                                .into_iter()
                                .map(str::to_owned)
                                .collect(),
                            &record.addr,
                            &record.label,
                        )
                        .await
                    };
                    if let Err(error) = replicator_result {
                        let message = format!(
                            "peer {} replicator bootstrap failed: {}",
                            record.label, error
                        );
                        status.last_error = Some(message.clone());
                        errors.push(message);
                    }
                }

                if options.install_replicators_on_bootstrap && !is_bearer_peer(record) {
                    match configure_local_runtime_pairing(node, p2p, actor, record).await {
                        Ok(()) => {}
                        Err(error) => {
                            let message = format!(
                                "peer {} local runtime pairing failed: {}",
                                record.label, error
                            );
                            status.last_error = Some(message.clone());
                            errors.push(message);
                        }
                    }
                }
            }
            Err(error) => {
                let message = format!("peer {} dial failed: {}", record.label, error);
                status.last_error = Some(message.clone());
                errors.push(message);
            }
        }

        statuses.push(status);
    }

    (statuses, errors)
}

pub(super) async fn configure_local_runtime_pairing(
    node: &EmbeddedNode,
    p2p: &Arc<dyn P2POps>,
    actor: &PrincipalIdentity,
    record: &PeerRecord,
) -> Result<()> {
    let local_endpoint = current_local_endpoint(p2p, actor)
        .await
        .context("resolving requester P2P endpoint for reciprocal pairing")?;
    write_peer_pairing_desired(node, record, actor.did(), &local_endpoint.address).await
}

pub(super) async fn write_peer_pairing_desired(
    node: &EmbeddedNode,
    record: &PeerRecord,
    requester_did: &str,
    requester_addr: &str,
) -> Result<()> {
    use gents::agent::p2p_reconcile::templates::MACHINE_TEMPLATE;
    use gents_protocol::graphql::escape_graphql_string;

    let peer_id = escape_graphql_string(&record.peer_id);
    // This row is consumed by the remote runtime. Its peer identity and
    // replicator address therefore describe this requester, not the remote
    // deployment represented by `record`.
    let agent_did = escape_graphql_string(requester_did);
    let template = escape_graphql_string(MACHINE_TEMPLATE);
    let replicator_addr = escape_graphql_string(requester_addr);
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let now = escape_graphql_string(&now);

    let query = format!(
        r#"query {{ PeerPairingDesired(filter: {{ peer_id: {{ _eq: "{peer_id}" }} }}) {{ _docID created_at }} }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query PeerPairingDesired failed: {}",
            response
                .errors
                .iter()
                .map(|error| error.message.as_str())
                .collect::<Vec<_>>()
                .join("; ")
        );
    }
    let existing_row = response
        .data
        .as_ref()
        .and_then(|data| data.get("PeerPairingDesired"))
        .and_then(|rows| rows.as_array())
        .and_then(|rows| rows.first())
        .cloned();

    let mutation = if let Some(existing_row) = existing_row {
        let created_at = existing_row
            .get("created_at")
            .and_then(|value| value.as_str())
            .map(escape_graphql_string)
            .unwrap_or_else(|| now.clone());
        format!(
            r#"mutation {{ update_PeerPairingDesired(
                filter: {{ peer_id: {{ _eq: "{peer_id}" }} }},
                input: {{
                    collections: null,
                    template: "{template}",
                    replicator_addresses: ["{replicator_addr}"],
                    agent_did: "{agent_did}",
                    created_at: "{created_at}",
                    profiles: null,
                    updated_at: "{now}"
                }}
            ) {{ _docID }} }}"#
        )
    } else {
        format!(
            r#"mutation {{ create_PeerPairingDesired(input: {{
                peer_id: "{peer_id}",
                agent_did: "{agent_did}",
                collections: null,
                template: "{template}",
                replicator_addresses: ["{replicator_addr}"],
                profiles: null,
                created_at: "{now}",
                updated_at: "{now}"
            }}) {{ _docID }} }}"#
        )
    };

    let response = node.execute(&mutation).await;
    if response.has_errors() {
        anyhow::bail!(
            "write PeerPairingDesired failed: {}",
            response
                .errors
                .iter()
                .map(|error| error.message.as_str())
                .collect::<Vec<_>>()
                .join("; ")
        );
    }
    Ok(())
}

pub(super) async fn connect_peer_with_retry(
    p2p: &Arc<dyn P2POps>,
    addr: &str,
    label: &str,
) -> Result<()> {
    connect_peer_with_retry_until(p2p, addr, label, BOOTSTRAP_OPERATION_TIMEOUT).await
}

pub(super) async fn force_connect_peer_with_retry(
    p2p: &Arc<dyn P2POps>,
    addr: &str,
    label: &str,
) -> Result<()> {
    force_connect_peer_with_retry_until(p2p, addr, label, BOOTSTRAP_OPERATION_TIMEOUT).await
}

pub(super) async fn connect_peer_with_retry_until(
    p2p: &Arc<dyn P2POps>,
    addr: &str,
    label: &str,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let expected_peer_id = parse_public_peer_addr(addr)
        .ok()
        .map(|(peer_id, _)| peer_id.to_string());

    loop {
        if let Some(peer_id) = expected_peer_id.as_deref() {
            if is_connected_peer(p2p, peer_id).await? {
                return Ok(());
            }
        }

        match p2p_connect_peer(p2p, addr).await {
            Ok(()) => {
                if let Some(peer_id) = expected_peer_id.as_deref() {
                    wait_for_connected_peer(p2p, peer_id, deadline, label).await?;
                }
                return Ok(());
            }
            Err(error) => {
                if let Some(peer_id) = expected_peer_id.as_deref() {
                    if is_connected_peer(p2p, peer_id).await? {
                        return Ok(());
                    }
                }
                if Instant::now() >= deadline {
                    anyhow::bail!("timed out connecting bootstrap peer {label} at {addr}: {error}");
                }
                sleep(BOOTSTRAP_OPERATION_BACKOFF).await;
            }
        }
    }
}

pub(super) async fn force_connect_peer_with_retry_until(
    p2p: &Arc<dyn P2POps>,
    addr: &str,
    label: &str,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let expected_peer_id = parse_public_peer_addr(addr)
        .ok()
        .map(|(peer_id, _)| peer_id.to_string());

    loop {
        match p2p_connect_peer(p2p, addr).await {
            Ok(()) => {
                if let Some(peer_id) = expected_peer_id.as_deref() {
                    wait_for_connected_peer(p2p, peer_id, deadline, label).await?;
                }
                return Ok(());
            }
            Err(error) => {
                if let Some(peer_id) = expected_peer_id.as_deref() {
                    if is_connected_peer(p2p, peer_id).await? {
                        return Ok(());
                    }
                }
                if Instant::now() >= deadline {
                    anyhow::bail!(
                        "timed out force-connecting bootstrap peer {label} at {addr}: {error}"
                    );
                }
                sleep(BOOTSTRAP_OPERATION_BACKOFF).await;
            }
        }
    }
}

pub(super) async fn add_replicator_with_retry(
    p2p: &Arc<dyn P2POps>,
    collections: Vec<String>,
    addr: &str,
    label: &str,
) -> Result<()> {
    add_replicator_with_retry_until(p2p, collections, addr, label, BOOTSTRAP_OPERATION_TIMEOUT)
        .await
}

pub(super) async fn add_replicator_with_retry_until(
    p2p: &Arc<dyn P2POps>,
    collections: Vec<String>,
    addr: &str,
    label: &str,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        match p2p_add_replicator(p2p, collections.clone(), addr).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                if Instant::now() >= deadline {
                    anyhow::bail!(
                        "timed out installing bootstrap replicator for peer {label} at {addr}: {error}"
                    );
                }
                sleep(BOOTSTRAP_OPERATION_BACKOFF).await;
            }
        }
    }
}

pub(super) async fn request_index_sync(
    node: &EmbeddedNode,
    p2p: &Arc<dyn P2POps>,
) -> Result<Vec<String>> {
    if p2p_connected_peers(p2p).await?.is_empty() {
        anyhow::bail!("no connected peers available for session index request");
    }

    let [conversation_name, session_name] = index_collection_names();
    let resolve_id = |collection_name| -> Result<String> {
        let collection = node
            .get_collection(collection_name)
            .map_err(|error| {
                anyhow::anyhow!("loading collection id for {collection_name}: {error}")
            })?
            .ok_or_else(|| anyhow::anyhow!("collection {collection_name} not found"))?;
        Ok(collection.collection_id)
    };
    let conversation_id = resolve_id(conversation_name)?;
    let session_id = resolve_id(session_name)?;

    tokio::try_join!(
        p2p_sync_branchable_collection(p2p, &conversation_id),
        p2p_sync_branchable_collection(p2p, &session_id),
    )?;

    Ok(vec![
        conversation_name.to_string(),
        session_name.to_string(),
    ])
}

pub(super) async fn is_connected_peer(p2p: &Arc<dyn P2POps>, peer_id: &str) -> Result<bool> {
    let peers = p2p_connected_peers(p2p).await?;
    Ok(peers.iter().any(|peer| {
        parse_public_peer_addr(peer)
            .map(|(parsed_peer_id, _)| parsed_peer_id.as_str() == peer_id)
            .unwrap_or_else(|_| peer.contains(peer_id))
    }))
}

async fn wait_for_connected_peer(
    p2p: &Arc<dyn P2POps>,
    peer_id: &str,
    deadline: Instant,
    label: &str,
) -> Result<()> {
    loop {
        if is_connected_peer(p2p, peer_id).await? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for bootstrap peer {peer_id} to connect for {label}");
        }
        sleep(Duration::from_millis(100)).await;
    }
}

pub(super) fn normalize_required<'a>(field: &str, value: &'a str) -> Result<&'a str> {
    let trimmed = value.trim();
    (!trimmed.is_empty())
        .then_some(trimmed)
        .with_context(|| format!("{field} must not be empty"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn desktop_p2p_config_uses_pending_dag_option() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let paths = DesktopPaths::from_root(tempdir.path().to_path_buf());
        let options = ClientCoreOptions {
            max_pending_dags: 77,
            ..ClientCoreOptions::local_only()
        };

        let config = desktop_p2p_config(&paths, &options);

        assert_eq!(config.max_pending_dags, 77);
        assert_eq!(
            config.max_doc_sync_request_doc_ids,
            p2p::sync::DEFAULT_MAX_DOC_SYNC_REQUEST_DOC_IDS
        );
    }
}
