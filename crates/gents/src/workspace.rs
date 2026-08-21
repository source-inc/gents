//! Isolated-workspace overlay: load identity + local placement, bind the
//! request-scoped tool root, and fail closed when the authority cannot run.

use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use defra_node::EmbeddedNode;
use serde::Deserialize;

use crate::graphql::{escape_graphql_string, first_row, graphql_with_transaction_retry, rows};
use crate::tool_surface::{resolve_configured_tool_root, FileToolMode};
use crate::toolset::{workspace_write_sandbox_enforced, WorkspaceAuthority};
use crate::watcher::AgentRequest;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IsolatedWorkspaceRecord {
    pub workspace_id: String,
    pub owner_deployment_id: String,
    pub lifecycle_state: String,
    pub seal_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkspacePlacementRecord {
    pub workspace_id: String,
    pub deployment_id: String,
    pub host_path: String,
    pub observed_tree_hash: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct WorkspaceOverlay {
    pub root: PathBuf,
    pub cwd: PathBuf,
    pub authority: WorkspaceAuthority,
}

pub(crate) struct WorkspaceBindInput<'a> {
    pub workspace_id: &'a str,
    pub authority: WorkspaceAuthority,
    pub owner_deployment_id: &'a str,
    pub seal_hash: Option<&'a str>,
    pub request_cwd: Option<&'a Path>,
    pub local_deployment_id: &'a str,
    pub operator_tool_root: Option<&'a Path>,
    pub enabled_workspace_roots: &'a [PathBuf],
    pub workspace_write_sandbox_enforced: bool,
}

pub(crate) fn workspace_authority_file_mode(authority: WorkspaceAuthority) -> FileToolMode {
    match authority {
        WorkspaceAuthority::ReadWrite => FileToolMode::ReadWrite,
        WorkspaceAuthority::ReadOnly | WorkspaceAuthority::Integrate => FileToolMode::ReadOnly,
    }
}

/// Unbound requests (`workspace_id` none/blank) stay on the behavior tool root.
#[inline(never)]
pub(crate) async fn resolve_request_workspace_overlay(
    node: &EmbeddedNode,
    request: &AgentRequest,
    operator_tool_root: Option<&Path>,
) -> Result<Option<WorkspaceOverlay>> {
    let Some(workspace_id) = optional_id(request.workspace_id.as_deref()) else {
        return Ok(None);
    };
    let authority = match request.workspace_authority.as_deref().map(str::trim) {
        Some(value) if !value.is_empty() => WorkspaceAuthority::parse(value)?,
        _ => bail!("workspace-bound request {workspace_id} is missing workspace_authority"),
    };

    let owner_deployment_id = optional_id(request.workspace_owner_deployment_id.as_deref())
        .ok_or_else(|| {
            anyhow!(
                "workspace-bound request {workspace_id} is missing workspace_owner_deployment_id"
            )
        })?;
    let workspace = load_isolated_workspace(node, workspace_id)
        .await?
        .ok_or_else(|| anyhow!("isolated workspace {workspace_id} not found"))?;
    let local_deployment_id = load_local_deployment_id(node).await?;
    let placement = load_workspace_placement(node, workspace_id, &local_deployment_id)
        .await?
        .ok_or_else(|| {
            anyhow!("workspace placement for {workspace_id} not found on this deployment")
        })?;
    let enabled_workspace_roots = load_enabled_workspace_roots(node).await?;
    let request_cwd = request_workspace_cwd(request);
    bind_workspace_overlay(
        &workspace,
        &placement,
        WorkspaceBindInput {
            workspace_id,
            authority,
            owner_deployment_id,
            seal_hash: optional_id(request.workspace_seal_hash.as_deref()),
            request_cwd: request_cwd.as_deref(),
            local_deployment_id: &local_deployment_id,
            operator_tool_root,
            enabled_workspace_roots: &enabled_workspace_roots,
            workspace_write_sandbox_enforced: workspace_write_sandbox_enforced(),
        },
    )
    .map(Some)
}

pub(crate) fn bind_workspace_overlay(
    workspace: &IsolatedWorkspaceRecord,
    placement: &WorkspacePlacementRecord,
    input: WorkspaceBindInput<'_>,
) -> Result<WorkspaceOverlay> {
    if workspace.workspace_id != input.workspace_id {
        bail!(
            "isolated workspace id {} does not match request {}",
            workspace.workspace_id,
            input.workspace_id
        );
    }
    if placement.workspace_id != input.workspace_id {
        bail!(
            "workspace placement id {} does not match request {}",
            placement.workspace_id,
            input.workspace_id
        );
    }
    if !input
        .authority
        .bindable_lifecycle_state(&workspace.lifecycle_state)
    {
        bail!(
            "isolated workspace {} in state {} is not bindable for authority {}",
            input.workspace_id,
            workspace.lifecycle_state,
            input.authority.as_str()
        );
    }
    let requested_owner = require_deployment_id(
        Some(input.owner_deployment_id),
        "request workspace_owner_deployment_id",
    )?;
    let local = require_deployment_id(
        Some(input.local_deployment_id),
        "local HostDeployment.deployment_id",
    )?;
    if workspace.owner_deployment_id.trim() != placement.deployment_id.trim() {
        bail!(
            "workspace placement deployment_id {} does not match owner_deployment_id {}",
            placement.deployment_id,
            workspace.owner_deployment_id
        );
    }
    if requested_owner != workspace.owner_deployment_id.trim() {
        bail!(
            "request workspace_owner_deployment_id {requested_owner} does not match workspace owner {}",
            workspace.owner_deployment_id
        );
    }
    if requested_owner != local {
        bail!("request workspace_owner_deployment_id {requested_owner} is not this host {local}");
    }
    if local != placement.deployment_id.trim() {
        bail!(
            "workspace placement for {} is owned by deployment {}, not this host {}",
            input.workspace_id,
            placement.deployment_id,
            local
        );
    }

    if matches!(input.authority, WorkspaceAuthority::ReadWrite)
        && !input.workspace_write_sandbox_enforced
    {
        bail!(
            "ReadWrite workspace binding requires an enforceable WorkspaceWrite sandbox on this host"
        );
    }

    if crate::toolset::normalize_workspace_lifecycle_state(&workspace.lifecycle_state)
        == Some("sealed")
    {
        let workspace_hash = optional_id(workspace.seal_hash.as_deref()).ok_or_else(|| {
            anyhow!(
                "sealed workspace {} is missing seal_hash",
                input.workspace_id
            )
        })?;
        let request_hash = input.seal_hash.ok_or_else(|| {
            anyhow!(
                "sealed workspace {} requires request.workspace_seal_hash",
                input.workspace_id
            )
        })?;
        if request_hash != workspace_hash {
            bail!(
                "request workspace_seal_hash {request_hash} does not match workspace seal_hash {workspace_hash}"
            );
        }
        let observed = optional_id(placement.observed_tree_hash.as_deref()).ok_or_else(|| {
            anyhow!(
                "sealed workspace {} is missing placement observed_tree_hash",
                input.workspace_id
            )
        })?;
        if observed != workspace_hash {
            bail!(
                "placement observed_tree_hash {observed} does not match workspace seal_hash {workspace_hash}"
            );
        }
    }

    let root = canonicalize_placement_path(&placement.host_path)?;
    require_under_ceiling(
        &root,
        input.operator_tool_root,
        input.enabled_workspace_roots,
    )?;
    let cwd = match input.request_cwd {
        Some(cwd) => {
            let canonical = std::fs::canonicalize(cwd)
                .with_context(|| format!("canonicalizing request cwd {}", cwd.display()))?;
            if !canonical.is_dir() || !canonical.starts_with(&root) {
                bail!(
                    "request cwd {} is not a directory under workspace root {}",
                    canonical.display(),
                    root.display()
                );
            }
            canonical
        }
        None => root.clone(),
    };

    Ok(WorkspaceOverlay {
        root,
        cwd,
        authority: input.authority,
    })
}

fn canonicalize_placement_path(host_path: &str) -> Result<PathBuf> {
    let path = Path::new(host_path.trim());
    if !path.is_absolute() {
        bail!("workspace placement host_path must be absolute: {host_path}");
    }
    if !path.is_dir() {
        bail!(
            "workspace placement host_path is not a directory: {}",
            path.display()
        );
    }
    std::fs::canonicalize(path)
        .with_context(|| format!("canonicalizing workspace placement {}", path.display()))
}

fn require_under_ceiling(
    path: &Path,
    operator_tool_root: Option<&Path>,
    enabled_workspace_roots: &[PathBuf],
) -> Result<()> {
    let operator = operator_tool_root
        .map(resolve_configured_tool_root)
        .transpose()?;
    if operator.is_none() && enabled_workspace_roots.is_empty() {
        bail!(
            "workspace placement {} requires an operator tool-root or enabled WorkspaceRoot ceiling",
            path.display()
        );
    }
    if let Some(ceiling) = operator.as_deref() {
        if !path.starts_with(ceiling) {
            bail!(
                "workspace placement {} escapes operator tool root {}",
                path.display(),
                ceiling.display()
            );
        }
    }
    if !enabled_workspace_roots.is_empty()
        && !enabled_workspace_roots
            .iter()
            .any(|root| path.starts_with(root))
    {
        bail!(
            "workspace placement {} is not under an enabled WorkspaceRoot",
            path.display()
        );
    }
    Ok(())
}

pub(super) fn optional_id(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn require_deployment_id<'a>(value: Option<&'a str>, what: &str) -> Result<&'a str> {
    optional_id(value).ok_or_else(|| anyhow!("{what} is missing"))
}

pub(crate) fn request_workspace_cwd(request: &AgentRequest) -> Option<PathBuf> {
    let metadata = request.metadata.as_deref()?.trim();
    if metadata.is_empty() {
        return None;
    }
    let value = serde_json::from_str::<serde_json::Value>(metadata).ok()?;
    value
        .pointer("/codex_shim/cwd")
        .or_else(|| value.get("workspace_cwd"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

#[derive(Deserialize)]
struct IsolatedWorkspaceRow {
    workspace_id: Option<String>,
    owner_deployment_id: Option<String>,
    lifecycle_state: Option<String>,
    seal_hash: Option<String>,
}

#[derive(Deserialize)]
struct WorkspacePlacementRow {
    workspace_id: Option<String>,
    deployment_id: Option<String>,
    host_path: Option<String>,
    observed_tree_hash: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct HostDeploymentRow {
    pub(super) deployment_id: Option<String>,
}

#[derive(Deserialize)]
struct WorkspaceRootRow {
    root_path: Option<String>,
    enabled: Option<bool>,
}

async fn load_isolated_workspace(
    node: &EmbeddedNode,
    workspace_id: &str,
) -> Result<Option<IsolatedWorkspaceRecord>> {
    let escaped = escape_graphql_string(workspace_id);
    let query = format!(
        r#"{{
            IsolatedWorkspace(
                filter: {{ workspace_id: {{ _eq: "{escaped}" }} }},
                limit: 1
            ) {{
                workspace_id
                owner_deployment_id
                lifecycle_state
                seal_hash
            }}
        }}"#
    );
    let response = graphql_with_transaction_retry(node, &query, "load IsolatedWorkspace").await?;
    let Some(row) = first_row::<IsolatedWorkspaceRow>(&response, "IsolatedWorkspace")? else {
        return Ok(None);
    };
    let workspace_id = optional_id(row.workspace_id.as_deref())
        .ok_or_else(|| anyhow!("IsolatedWorkspace is missing workspace_id"))?
        .to_string();
    let owner_deployment_id = optional_id(row.owner_deployment_id.as_deref())
        .ok_or_else(|| anyhow!("IsolatedWorkspace {workspace_id} is missing owner_deployment_id"))?
        .to_string();
    let lifecycle_state = optional_id(row.lifecycle_state.as_deref())
        .ok_or_else(|| anyhow!("IsolatedWorkspace {workspace_id} is missing lifecycle_state"))?
        .to_string();
    Ok(Some(IsolatedWorkspaceRecord {
        workspace_id,
        owner_deployment_id,
        lifecycle_state,
        seal_hash: optional_id(row.seal_hash.as_deref()).map(str::to_string),
    }))
}

async fn load_workspace_placement(
    node: &EmbeddedNode,
    workspace_id: &str,
    local_deployment_id: &str,
) -> Result<Option<WorkspacePlacementRecord>> {
    let escaped_workspace = escape_graphql_string(workspace_id);
    let escaped_deployment = escape_graphql_string(local_deployment_id);
    let query = format!(
        r#"{{
            WorkspacePlacement(
                filter: {{
                    workspace_id: {{ _eq: "{escaped_workspace}" }},
                    deployment_id: {{ _eq: "{escaped_deployment}" }}
                }},
                limit: 1
            ) {{
                workspace_id
                deployment_id
                host_path
                observed_tree_hash
            }}
        }}"#
    );
    let response = graphql_with_transaction_retry(node, &query, "load WorkspacePlacement").await?;
    let Some(row) = first_row::<WorkspacePlacementRow>(&response, "WorkspacePlacement")? else {
        return Ok(None);
    };
    let workspace_id = optional_id(row.workspace_id.as_deref())
        .ok_or_else(|| anyhow!("WorkspacePlacement is missing workspace_id"))?
        .to_string();
    let deployment_id = optional_id(row.deployment_id.as_deref())
        .ok_or_else(|| anyhow!("WorkspacePlacement {workspace_id} is missing deployment_id"))?
        .to_string();
    let host_path = optional_id(row.host_path.as_deref())
        .ok_or_else(|| anyhow!("WorkspacePlacement {workspace_id} is missing host_path"))?
        .to_string();
    Ok(Some(WorkspacePlacementRecord {
        workspace_id,
        deployment_id,
        host_path,
        observed_tree_hash: optional_id(row.observed_tree_hash.as_deref()).map(str::to_string),
    }))
}

async fn load_local_deployment_id(node: &EmbeddedNode) -> Result<String> {
    let query = r#"{
        HostDeployment(limit: 2) {
            deployment_id
        }
    }"#;
    let response = graphql_with_transaction_retry(node, query, "load HostDeployment").await?;
    let rows = rows::<HostDeploymentRow>(&response, "HostDeployment")?;
    local_deployment_id_from_rows(rows)
}

pub(super) fn local_deployment_id_from_rows(rows: Vec<HostDeploymentRow>) -> Result<String> {
    if rows.len() > 1 {
        bail!("multiple HostDeployment rows; deployment_id is ambiguous");
    }
    let row = rows.into_iter().next().ok_or_else(|| {
        anyhow!("HostDeployment is missing; cannot bind a workspace on this host")
    })?;
    optional_id(row.deployment_id.as_deref())
        .map(str::to_string)
        .ok_or_else(|| anyhow!("HostDeployment is missing deployment_id"))
}

async fn load_enabled_workspace_roots(node: &EmbeddedNode) -> Result<Vec<PathBuf>> {
    let query = r#"{
        WorkspaceRoot {
            root_path
            enabled
        }
    }"#;
    let response = graphql_with_transaction_retry(node, query, "load WorkspaceRoot").await?;
    let mut roots = Vec::new();
    for row in rows::<WorkspaceRootRow>(&response, "WorkspaceRoot")? {
        if !row.enabled.unwrap_or(false) {
            continue;
        }
        let Some(root_path) = optional_id(row.root_path.as_deref()) else {
            continue;
        };
        match resolve_configured_tool_root(Path::new(root_path)) {
            Ok(root) => roots.push(root),
            Err(error) => {
                tracing::warn!(
                    root_path,
                    error = %error,
                    "dropping enabled WorkspaceRoot that failed to resolve"
                );
            }
        }
    }
    Ok(roots)
}
