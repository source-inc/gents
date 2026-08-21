use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path};

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const CAP_CREATE_WORKSPACE: &str = "create_workspace";
pub const CAP_OBSERVE_DIRTY_BASE: &str = "observe_dirty_base";
pub const CAP_CLONE_ARTIFACTS: &str = "clone_artifacts";
pub(crate) const ACTION_PLAN_ABI: u32 = 1;
pub const DEFAULT_MAKE_WORKTREE_ARTIFACTS: &[&str] = &["target/", "crates/gents/proofs/.lake"];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ActionPlan {
    pub abi: u32,
    pub actions: Vec<HostAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type")]
pub enum HostAction {
    #[serde(rename = "create_workspace")]
    CreateWorkspace(CreateWorkspaceAction),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CreationPolicy {
    GitWorktreeDiff,
    IsolatedClone,
}

impl CreationPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GitWorktreeDiff => "git_worktree_diff",
            Self::IsolatedClone => "isolated_clone",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceAdapterKind {
    MakeWorktree,
    GitWorktree,
}

impl WorkspaceAdapterKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MakeWorktree => "make_worktree",
            Self::GitWorktree => "git_worktree",
        }
    }

    pub fn clones_artifacts(self) -> bool {
        matches!(self, Self::MakeWorktree)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CreateWorkspaceAction {
    pub workspace_id: String,
    pub work_unit_id: String,
    pub repository_id: String,
    pub base_sha: String,
    pub branch: String,
    #[serde(default = "default_creation_policy")]
    pub creation_policy: CreationPolicy,
    #[serde(default = "default_adapter")]
    pub adapter: WorkspaceAdapterKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clone_artifacts: Option<Vec<String>>,
}

fn default_creation_policy() -> CreationPolicy {
    CreationPolicy::GitWorktreeDiff
}

fn default_adapter() -> WorkspaceAdapterKind {
    WorkspaceAdapterKind::MakeWorktree
}

/// Builtin ActionPlan emitter: structured fields → the same ABI WASM will use.
pub fn emit_create_workspace_plan(action: CreateWorkspaceAction) -> ActionPlan {
    ActionPlan {
        abi: ACTION_PLAN_ABI,
        actions: vec![HostAction::CreateWorkspace(action)],
    }
}

/// Sorted-key JSON for a validated ActionPlan. Rejects NaN/Inf and host paths.
pub(crate) fn action_plan_canonical_json(plan: &ActionPlan) -> Result<String, String> {
    let value = serde_json::to_value(plan).map_err(|error| error.to_string())?;
    reject_illegal_plan_value(&value)?;
    canonical_json_string(&value)
}

/// Parse ActionPlan JSON. Unknown action types, extra fields, NaN, and host
/// paths deny the entire plan.
pub(crate) fn parse_action_plan_json(raw: &str) -> Result<ActionPlan, String> {
    let value: Value =
        serde_json::from_str(raw).map_err(|error| format!("ActionPlan is not JSON: {error}"))?;
    reject_illegal_plan_value(&value)?;
    serde_json::from_value(value).map_err(|error| {
        let text = error.to_string();
        if text.contains("unknown variant") {
            format!("unknown ActionPlan action type: {text}")
        } else {
            format!("ActionPlan schema rejected: {text}")
        }
    })
}

/// Sorted object keys, no NaN/Inf. Used for module_id and planner input.
pub(crate) fn canonical_json_string(value: &Value) -> Result<String, String> {
    reject_non_finite_numbers(value)?;
    serde_json::to_string(&sort_json(value)).map_err(|error| error.to_string())
}

fn reject_non_finite_numbers(value: &Value) -> Result<(), String> {
    match value {
        Value::Number(number) if number.as_f64().is_some_and(|float| !float.is_finite()) => {
            Err("JSON must not contain NaN or Infinity".into())
        }
        Value::Array(items) => {
            for item in items {
                reject_non_finite_numbers(item)?;
            }
            Ok(())
        }
        Value::Object(map) => {
            for child in map.values() {
                reject_non_finite_numbers(child)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn sort_json(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(sort_json).collect()),
        Value::Object(map) => {
            let sorted = map
                .iter()
                .map(|(key, child)| (key.clone(), sort_json(child)))
                .collect::<BTreeMap<_, _>>();
            Value::Object(sorted.into_iter().collect())
        }
        other => other.clone(),
    }
}

fn reject_illegal_plan_value(value: &Value) -> Result<(), String> {
    match value {
        Value::Null | Value::Bool(_) => Ok(()),
        Value::Number(number) => {
            if number.as_f64().is_some_and(|float| !float.is_finite()) {
                Err("ActionPlan JSON must not contain NaN or Infinity".into())
            } else {
                Ok(())
            }
        }
        Value::String(text) => {
            if looks_like_host_path(text) {
                Err(format!("ActionPlan must not contain host path `{text}`"))
            } else {
                Ok(())
            }
        }
        Value::Array(items) => {
            for item in items {
                reject_illegal_plan_value(item)?;
            }
            Ok(())
        }
        Value::Object(map) => {
            for (key, child) in map {
                if key.eq_ignore_ascii_case("host_path") {
                    return Err("ActionPlan must not contain host_path".into());
                }
                reject_illegal_plan_value(child)?;
            }
            Ok(())
        }
    }
}

impl ActionPlan {
    pub(crate) fn validate_against(&self, capabilities: &BTreeSet<String>) -> Result<()> {
        if self.abi != ACTION_PLAN_ABI {
            bail!("unsupported ActionPlan abi {}", self.abi);
        }
        if self.actions.is_empty() {
            bail!("ActionPlan has no actions");
        }
        for action in &self.actions {
            action.validate_against(capabilities)?;
        }
        Ok(())
    }
}

impl HostAction {
    pub(crate) fn validate_against(&self, capabilities: &BTreeSet<String>) -> Result<()> {
        match self {
            Self::CreateWorkspace(action) => action.validate_against(capabilities),
        }
    }
}

impl CreateWorkspaceAction {
    pub(crate) fn validate_against(&self, capabilities: &BTreeSet<String>) -> Result<()> {
        require_non_empty("workspace_id", &self.workspace_id)?;
        require_non_empty("work_unit_id", &self.work_unit_id)?;
        require_non_empty("repository_id", &self.repository_id)?;
        require_non_empty("base_sha", &self.base_sha)?;
        require_non_empty("branch", &self.branch)?;
        require_capability(capabilities, CAP_CREATE_WORKSPACE)?;
        require_capability(capabilities, CAP_OBSERVE_DIRTY_BASE)?;
        if !matches!(self.creation_policy, CreationPolicy::GitWorktreeDiff) {
            bail!(
                "creation_policy {} is not implemented in v1 (only git_worktree_diff)",
                self.creation_policy.as_str()
            );
        }
        let artifacts = self.effective_clone_artifacts();
        if !artifacts.is_empty() {
            if !self.adapter.clones_artifacts() {
                bail!(
                    "adapter {} cannot clone artifacts; use make_worktree",
                    self.adapter.as_str()
                );
            }
            require_capability(capabilities, CAP_CLONE_ARTIFACTS)?;
            for artifact in &artifacts {
                assert_relative_artifact(artifact)?;
            }
        }
        Ok(())
    }

    pub(crate) fn effective_clone_artifacts(&self) -> Vec<String> {
        if !self.adapter.clones_artifacts() {
            return Vec::new();
        }
        match &self.clone_artifacts {
            None => DEFAULT_MAKE_WORKTREE_ARTIFACTS
                .iter()
                .map(|path| (*path).to_string())
                .collect(),
            Some(paths) => paths.clone(),
        }
    }

    pub(crate) fn identity(&self) -> super::LogicalWorkspaceIdentity {
        super::LogicalWorkspaceIdentity {
            workspace_id: self.workspace_id.clone(),
            work_unit_id: self.work_unit_id.clone(),
            repository_id: self.repository_id.clone(),
            base_sha: self.base_sha.clone(),
            branch: self.branch.clone(),
        }
    }
}

fn require_non_empty(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field} must be non-empty");
    }
    Ok(())
}

fn require_capability(capabilities: &BTreeSet<String>, cap: &str) -> Result<()> {
    if capabilities.contains(cap) {
        Ok(())
    } else {
        bail!("missing capability {cap}")
    }
}

/// Platform-independent host-path shape: leading `/`, `drive:`, UNC, `file:`,
/// `~`, or `..` components. Used so replicated ActionPlan JSON cannot carry
/// host-shaped strings even when this process is not on Windows.
pub(crate) fn looks_like_host_path(text: &str) -> bool {
    let text = text.trim();
    if text.is_empty() {
        return false;
    }
    if text.starts_with('/') || text.starts_with('\\') || text.starts_with('~') {
        return true;
    }
    let lower = text.to_ascii_lowercase();
    if lower.starts_with("file:") {
        return true;
    }
    let bytes = text.as_bytes();
    if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
        if bytes.len() == 2 || bytes[2] == b'\\' || bytes[2] == b'/' {
            return true;
        }
    }
    Path::new(text)
        .components()
        .any(|component| matches!(component, Component::ParentDir))
}

fn assert_relative_artifact(path: &str) -> Result<()> {
    let path = Path::new(path);
    if path.is_absolute() || looks_like_host_path(&path.to_string_lossy()) {
        bail!(
            "clone artifact must be a relative path, got {}",
            path.display()
        );
    }
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        bail!(
            "clone artifact must not contain '..', got {}",
            path.display()
        );
    }
    Ok(())
}
