#[path = "snapshot/projection.rs"]
pub mod projection;
#[path = "snapshot/timeline.rs"]
mod timeline;
#[path = "snapshot/tool_presentation.rs"]
mod tool_presentation;

use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use gents_desktop_core::client::{ClientCore, DesktopPaths, P2PHealth, PeerDirectory};

use super::state::ResolvedBridgePolicy;
use super::types::{
    normalize_optional, DesktopBootstrapSummary, DesktopClientSnapshot, P2PHealthView,
    SavedPeerView,
};
use projection::{project_bootstrap_summary, project_client_snapshot, SnapshotGrants};

#[derive(Debug, serde::Deserialize)]
struct StoredInitConfigView {
    #[serde(default)]
    agent_name: Option<String>,
    #[serde(default)]
    agent_did: Option<String>,
    #[serde(default)]
    tool_ceiling: Option<String>,
    #[serde(default)]
    tool_root: Option<String>,
}

fn to_health_view(health: &P2PHealth) -> P2PHealthView {
    P2PHealthView {
        status: health.status_label().to_string(),
        connected_peer_count: health.connected_peer_count,
        replicator_count: health.replicator_count,
        consecutive_failures: health.consecutive_failures,
        last_error: health.last_error.clone(),
        last_ok_at: system_time_rfc3339(health.last_ok_at),
        last_failure_at: system_time_rfc3339(health.last_failure_at),
    }
}

fn system_time_rfc3339(value: Option<SystemTime>) -> Option<String> {
    value.map(|value| DateTime::<Utc>::from(value).to_rfc3339())
}

pub async fn build_bootstrap_summary() -> Result<DesktopBootstrapSummary, String> {
    let agent_home = gents_desktop_core::local_runtime::default_agent_home()
        .map_err(|error| error.to_string())?;
    let desktop_paths = DesktopPaths::discover().map_err(|error| error.to_string())?;
    let full = build_bootstrap_summary_raw(&desktop_paths, Some(agent_home.as_path())).await?;
    Ok(project_bootstrap_summary(full, SnapshotGrants::all()))
}

pub async fn build_bootstrap_summary_for_policy(
    policy: &ResolvedBridgePolicy,
) -> Result<DesktopBootstrapSummary, String> {
    let full =
        build_bootstrap_summary_raw(&policy.desktop_paths, policy.agent_home.as_deref()).await?;
    Ok(project_bootstrap_summary(full, policy.snapshot_grants))
}

async fn build_bootstrap_summary_raw(
    desktop_paths: &DesktopPaths,
    agent_home: Option<&Path>,
) -> Result<DesktopBootstrapSummary, String> {
    let peer_directory = PeerDirectory::load(desktop_paths.peer_directory_path())
        .await
        .map_err(|error| error.to_string())?;
    let init = agent_home.and_then(read_stored_init_config);
    let agent_home_display = agent_home
        .map(|p| p.display().to_string())
        .unwrap_or_default();
    let agent_home_exists = agent_home.map(|p| p.exists()).unwrap_or(false);

    Ok(DesktopBootstrapSummary {
        default_agent_home: agent_home_display,
        init_agent_name: init
            .as_ref()
            .and_then(|config| normalize_optional(config.agent_name.as_deref())),
        init_agent_did: init
            .as_ref()
            .and_then(|config| normalize_optional(config.agent_did.as_deref())),
        init_tool_ceiling: init
            .as_ref()
            .and_then(|config| normalize_optional(config.tool_ceiling.as_deref())),
        init_tool_root: init
            .as_ref()
            .and_then(|config| normalize_optional(config.tool_root.as_deref())),
        desktop_home: desktop_paths.root().display().to_string(),
        peer_directory_path: desktop_paths.peer_directory_path().display().to_string(),
        node_data_dir: desktop_paths.node_data_dir().display().to_string(),
        log_file_path: desktop_paths.log_file_path().display().to_string(),
        agent_home_exists,
        desktop_home_exists: desktop_paths.root().exists(),
        peer_directory_exists: desktop_paths.peer_directory_path().exists(),
        saved_peers: peer_directory
            .records()
            .iter()
            .map(|peer| SavedPeerView {
                peer_id: peer.peer_id.clone(),
                label: peer.label.clone(),
                agent_did: peer.agent_did.clone(),
                addr: peer.addr.clone(),
                source: peer.source.clone(),
                graphql: peer.graphql.clone(),
            })
            .collect(),
    })
}

fn read_stored_init_config(agent_home: &std::path::Path) -> Option<StoredInitConfigView> {
    let bytes = std::fs::read(agent_home.join("init.json")).ok()?;
    serde_json::from_slice(&bytes).ok()
}

#[path = "snapshot/runtime_tasks.rs"]
mod runtime_tasks;
#[cfg(test)]
use runtime_tasks::{
    conversation_task_tag, recent_runs_for_task_views, request_backed_conversation_summaries,
    retain_latest_conversation_summaries, task_run_history,
};
use runtime_tasks::{request_matches_agent, source_matches_agent};

#[path = "snapshot/runtime.rs"]
mod runtime;
pub use runtime::build_runtime_snapshot;

#[path = "snapshot/operations_signature.rs"]
pub mod operations_signature;

#[path = "snapshot/operations_snapshot.rs"]
pub mod operations_snapshot;
pub use operations_signature::{
    compute_preview_signature, PreviewSignatureInput, PreviewSignatureRow,
};

#[path = "snapshot/subagent_tree.rs"]
pub mod subagent_tree;

#[path = "snapshot/session.rs"]
mod session;
#[cfg(test)]
pub use session::build_session_snapshot_from_store;
pub use session::build_session_snapshot_from_store_for_agent;

pub async fn build_client_snapshot_with_grants(
    core: Option<&Arc<ClientCore>>,
    policy: Option<&ResolvedBridgePolicy>,
    grants: SnapshotGrants,
) -> Result<DesktopClientSnapshot, String> {
    let bootstrap = match policy {
        Some(policy) => {
            build_bootstrap_summary_raw(&policy.desktop_paths, policy.agent_home.as_deref()).await?
        }
        None => {
            let agent_home = gents_desktop_core::local_runtime::default_agent_home()
                .map_err(|error| error.to_string())?;
            let desktop_paths = DesktopPaths::discover().map_err(|error| error.to_string())?;
            build_bootstrap_summary_raw(&desktop_paths, Some(agent_home.as_path())).await?
        }
    };
    let client = match core {
        Some(core) => Some(build_runtime_snapshot(core.as_ref()).await),
        None => None,
    };
    Ok(project_client_snapshot(
        DesktopClientSnapshot { bootstrap, client },
        grants,
    ))
}

#[cfg(test)]
#[path = "snapshot/tests.rs"]
mod tests;
