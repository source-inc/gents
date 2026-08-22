use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};

use super::action_plan::{ActionPlan, CreateWorkspaceAction, HostAction};
use super::adapter::{
    artifacts_complete, clone_artifacts, observe_dirty_base, observe_effect, observed_tree_hash,
    provision, resolve_base_sha, write_identity, ObservedEffect,
};
use super::documents::{
    new_isolated_workspace, IsolatedWorkspaceDoc, ProvisioningObservation, WorkspaceDocuments,
    WorkspacePlacementDoc, ADAPTER_VERSION, LIFECYCLE_PROVISION_FAILED, LIFECYCLE_READY,
};
use super::journal::{self, ActionJournalEntry, ActionJournalState};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalWorkspaceIdentity {
    pub workspace_id: String,
    pub work_unit_id: String,
    pub repository_id: String,
    pub base_sha: String,
    pub branch: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryPlacementRef {
    pub repository_id: String,
    pub deployment_id: String,
    pub host_path: PathBuf,
    pub enabled: bool,
}

pub struct HostExecutorContext<'a> {
    pub deployment_id: String,
    pub repository: RepositoryPlacementRef,
    pub ceiling: Option<&'a Path>,
    pub capabilities: BTreeSet<String>,
    pub writer_principal: String,
    pub integrator_principal: String,
    pub caused_by_invocation_id: String,
    pub caused_by_correlation: String,
    pub documents: &'a mut dyn WorkspaceDocuments,
}

#[derive(Debug)]
pub enum HostExecuteError {
    Denied {
        reason: String,
    },
    Failed {
        reason: String,
        identity_mismatch: bool,
        outcome: Option<CreateWorkspaceOutcome>,
    },
}

impl std::fmt::Display for HostExecuteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Denied { reason } => write!(f, "action plan denied: {reason}"),
            Self::Failed { reason, .. } => write!(f, "create_workspace failed: {reason}"),
        }
    }
}

impl std::error::Error for HostExecuteError {}

impl HostExecuteError {
    fn denied(reason: impl Into<String>) -> Self {
        Self::Denied {
            reason: reason.into(),
        }
    }

    fn failed(
        reason: impl Into<String>,
        identity_mismatch: bool,
        outcome: Option<CreateWorkspaceOutcome>,
    ) -> Self {
        Self::Failed {
            reason: reason.into(),
            identity_mismatch,
            outcome,
        }
    }

    pub fn identity_mismatch(&self) -> bool {
        matches!(
            self,
            Self::Failed {
                identity_mismatch: true,
                ..
            }
        )
    }

    pub fn outcome(&self) -> Option<&CreateWorkspaceOutcome> {
        match self {
            Self::Failed { outcome, .. } => outcome.as_ref(),
            Self::Denied { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateWorkspaceOutcome {
    pub workspace: IsolatedWorkspaceDoc,
    pub placement: WorkspacePlacementDoc,
}

/// Host-chosen dest: deterministic child of the source-checkout parent from
/// `(workspace_id, branch)`, rejected if it escapes the operator ceiling.
pub fn workspace_host_path(
    repository_checkout: &Path,
    workspace_id: &str,
    branch: &str,
    ceiling: Option<&Path>,
) -> Result<PathBuf> {
    let checkout = if repository_checkout.exists() {
        std::fs::canonicalize(repository_checkout).with_context(|| {
            format!(
                "canonicalizing repository checkout {}",
                repository_checkout.display()
            )
        })?
    } else {
        bail!(
            "repository checkout {} does not exist",
            repository_checkout.display()
        );
    };
    let parent = checkout.parent().ok_or_else(|| {
        anyhow!(
            "repository checkout {} has no parent for worktree placement",
            checkout.display()
        )
    })?;
    let dest = parent.join(dest_name(workspace_id, branch));
    if dest == checkout {
        bail!("workspace destination collides with the source checkout");
    }
    if let Some(ceiling) = ceiling {
        let ceiling = if ceiling.exists() {
            std::fs::canonicalize(ceiling)
                .with_context(|| format!("canonicalizing ceiling {}", ceiling.display()))?
        } else {
            lexical_absolute(ceiling)
        };
        if !dest.starts_with(&ceiling) {
            bail!(
                "workspace destination {} escapes operator ceiling {}",
                dest.display(),
                ceiling.display()
            );
        }
    }
    Ok(dest)
}

pub fn execute_create_workspace_plan(
    plan: &ActionPlan,
    journal: &mut Vec<ActionJournalEntry>,
    ctx: &mut HostExecutorContext<'_>,
) -> Result<CreateWorkspaceOutcome, HostExecuteError> {
    if !journal::action_journal_prefix_legal(journal) {
        return Err(HostExecuteError::denied("illegal action journal prefix"));
    }
    plan.validate_against(&ctx.capabilities)
        .map_err(|err| HostExecuteError::denied(err.to_string()))?;
    if plan.actions.len() != 1 {
        return Err(HostExecuteError::denied(
            "PR 4 host executor only runs a single create_workspace action",
        ));
    }
    let action = match &plan.actions[0] {
        HostAction::CreateWorkspace(action) => action,
    };
    if ctx.repository.repository_id != action.repository_id {
        return Err(HostExecuteError::denied(format!(
            "RepositoryPlacement {} does not match plan repository_id {}",
            ctx.repository.repository_id, action.repository_id
        )));
    }
    if !ctx.repository.enabled {
        return Err(HostExecuteError::denied(format!(
            "RepositoryPlacement {} is disabled",
            ctx.repository.repository_id
        )));
    }
    if ctx.repository.deployment_id != ctx.deployment_id {
        return Err(HostExecuteError::denied(
            "RepositoryPlacement.deployment_id does not match this host deployment_id",
        ));
    }

    if journal::current_state(journal, 0).is_none() {
        journal::advance(journal, 0, ActionJournalState::Validated);
    }

    create_workspace_action(action, journal, ctx)
}

fn create_workspace_action(
    action: &CreateWorkspaceAction,
    journal: &mut Vec<ActionJournalEntry>,
    ctx: &mut HostExecutorContext<'_>,
) -> Result<CreateWorkspaceOutcome, HostExecuteError> {
    let source = ctx.repository.host_path.clone();
    let dest = workspace_host_path(&source, &action.workspace_id, &action.branch, ctx.ceiling)
        .map_err(|err| HostExecuteError::denied(err.to_string()))?;

    let resolved_base = resolve_base_sha(&source, &action.base_sha).map_err(|err| {
        HostExecuteError::denied(format!(
            "base_sha {} is not a commit: {err}",
            action.base_sha
        ))
    })?;
    let mut identity = action.identity();
    identity.base_sha = resolved_base.clone();

    let artifacts = action.effective_clone_artifacts();
    let state = journal::current_state(journal, 0).unwrap_or(ActionJournalState::Validated);

    if matches!(state, ActionJournalState::ResultDocsWritten) {
        return load_written_docs(&identity.workspace_id, ctx.documents)
            .map_err(|err| HostExecuteError::failed(err.to_string(), false, None));
    }

    if matches!(state, ActionJournalState::Validated) {
        journal::advance(journal, 0, ActionJournalState::Executing);
    }

    // Recovery from Executing always observes; never blindly re-runs create.
    let observed = observe_effect(&source, &dest, &identity, &resolved_base, &artifacts)
        .map_err(|err| HostExecuteError::failed(err.to_string(), false, None))?;

    let (observation, tree_hash, dirty_base) = match observed {
        ObservedEffect::Absent
            if matches!(
                journal::current_state(journal, 0),
                Some(ActionJournalState::Executing)
            ) =>
        {
            match provision(&source, &dest, action, &resolved_base) {
                Ok(observation) => {
                    let tree_hash = observed_tree_hash(&dest).unwrap_or_default();
                    let dirty_base =
                        observe_dirty_base(&source).unwrap_or(super::adapter::DirtyBase {
                            dirty: false,
                            summary: String::new(),
                        });
                    (observation, tree_hash, dirty_base)
                }
                Err(err) => {
                    let dirty_base =
                        observe_dirty_base(&source).unwrap_or(super::adapter::DirtyBase {
                            dirty: false,
                            summary: String::new(),
                        });
                    journal::advance(journal, 0, ActionJournalState::EffectObserved);
                    let outcome = persist_docs(
                        action,
                        &identity,
                        ctx,
                        &dest,
                        ProvisioningObservation {
                            path_exists: dest.exists(),
                            ..ProvisioningObservation::default()
                        },
                        String::new(),
                        &dirty_base,
                        LIFECYCLE_PROVISION_FAILED,
                    )
                    .map_err(|persist_err| {
                        HostExecuteError::failed(
                            format!("{err}; also failed writing result docs: {persist_err}"),
                            false,
                            None,
                        )
                    })?;
                    journal::advance(journal, 0, ActionJournalState::ResultDocsWritten);
                    return Err(HostExecuteError::failed(
                        err.to_string(),
                        false,
                        Some(outcome),
                    ));
                }
            }
        }
        ObservedEffect::Absent => {
            return Err(HostExecuteError::failed(
                "journal is past Executing but destination is absent",
                false,
                None,
            ));
        }
        ObservedEffect::Match {
            mut observation,
            tree_hash,
            dirty_base,
        } => {
            let _ = write_identity(&dest, &identity);
            if action.adapter.clones_artifacts() {
                if let Err(err) = clone_artifacts(&source, &dest, &artifacts) {
                    journal::advance(journal, 0, ActionJournalState::EffectObserved);
                    let outcome = persist_docs(
                        action,
                        &identity,
                        ctx,
                        &dest,
                        observation,
                        tree_hash,
                        &dirty_base,
                        LIFECYCLE_PROVISION_FAILED,
                    )
                    .map_err(|persist_err| {
                        HostExecuteError::failed(
                            format!("{err}; also failed writing result docs: {persist_err}"),
                            false,
                            None,
                        )
                    })?;
                    journal::advance(journal, 0, ActionJournalState::ResultDocsWritten);
                    return Err(HostExecuteError::failed(
                        err.to_string(),
                        false,
                        Some(outcome),
                    ));
                }
                observation.artifacts_cloned = artifacts_complete(&source, &dest, &artifacts);
            }
            (observation, tree_hash, dirty_base)
        }
        ObservedEffect::Mismatch {
            reason,
            observation,
            dirty_base,
        } => {
            journal::advance(journal, 0, ActionJournalState::EffectObserved);
            let outcome = persist_docs(
                action,
                &identity,
                ctx,
                &dest,
                observation,
                String::new(),
                &dirty_base,
                LIFECYCLE_PROVISION_FAILED,
            )
            .map_err(|err| HostExecuteError::failed(err.to_string(), true, None))?;
            journal::advance(journal, 0, ActionJournalState::ResultDocsWritten);
            return Err(HostExecuteError::failed(reason, true, Some(outcome)));
        }
    };

    journal::advance(journal, 0, ActionJournalState::EffectObserved);
    let outcome = persist_docs(
        action,
        &identity,
        ctx,
        &dest,
        observation,
        tree_hash,
        &dirty_base,
        LIFECYCLE_READY,
    )
    .map_err(|err| HostExecuteError::failed(err.to_string(), false, None))?;
    journal::advance(journal, 0, ActionJournalState::ResultDocsWritten);
    Ok(outcome)
}

fn persist_docs(
    action: &CreateWorkspaceAction,
    identity: &LogicalWorkspaceIdentity,
    ctx: &mut HostExecutorContext<'_>,
    dest: &Path,
    observation: ProvisioningObservation,
    tree_hash: String,
    dirty_base: &super::adapter::DirtyBase,
    lifecycle_state: &str,
) -> Result<CreateWorkspaceOutcome> {
    if let Some(existing) = ctx
        .documents
        .load_isolated_workspace(&identity.workspace_id)?
    {
        if existing.identity() != *identity {
            bail!(
                "IsolatedWorkspace {} already exists with a different identity; leaving it in place",
                identity.workspace_id
            );
        }
        if existing.lifecycle_state == LIFECYCLE_READY && lifecycle_state != LIFECYCLE_READY {
            bail!(
                "refusing to overwrite Ready IsolatedWorkspace {}",
                identity.workspace_id
            );
        }
        // provisioning → ready | provisionFailed only. ProvisionFailed is terminal.
        if existing.lifecycle_state == LIFECYCLE_PROVISION_FAILED
            && lifecycle_state != LIFECYCLE_PROVISION_FAILED
        {
            bail!(
                "IsolatedWorkspace {} is provisionFailed; refusing {lifecycle_state} without cleanup",
                identity.workspace_id
            );
        }
    }

    let workspace = new_isolated_workspace(
        identity,
        action.creation_policy,
        action.adapter,
        &ctx.deployment_id,
        &ctx.writer_principal,
        &ctx.integrator_principal,
        &ctx.caused_by_invocation_id,
        &ctx.caused_by_correlation,
        lifecycle_state,
    );
    let host_path = dest
        .to_str()
        .ok_or_else(|| anyhow!("workspace host_path is not valid UTF-8"))?
        .to_string();
    let placement = WorkspacePlacementDoc {
        workspace_id: identity.workspace_id.clone(),
        deployment_id: ctx.deployment_id.clone(),
        host_path,
        repository_placement_id: ctx.repository.repository_id.clone(),
        adapter: action.adapter.as_str().to_string(),
        adapter_version: ADAPTER_VERSION.to_string(),
        dirty_base: dirty_base.dirty,
        dirty_base_summary: dirty_base.summary.clone(),
        provisioning_state: observation.to_json_string(),
        observed_tree_hash: tree_hash,
    };
    ctx.documents.write_isolated_workspace(workspace.clone())?;
    ctx.documents.write_placement(placement.clone())?;
    Ok(CreateWorkspaceOutcome {
        workspace,
        placement,
    })
}

fn load_written_docs(
    workspace_id: &str,
    documents: &dyn WorkspaceDocuments,
) -> Result<CreateWorkspaceOutcome> {
    let workspace = documents
        .load_isolated_workspace(workspace_id)?
        .ok_or_else(|| {
            anyhow!("journal ResultDocsWritten but IsolatedWorkspace {workspace_id} missing")
        })?;
    let placement = documents.load_placement(workspace_id)?.ok_or_else(|| {
        anyhow!("journal ResultDocsWritten but WorkspacePlacement {workspace_id} missing")
    })?;
    Ok(CreateWorkspaceOutcome {
        workspace,
        placement,
    })
}

fn dest_name(workspace_id: &str, branch: &str) -> String {
    format!(
        "gents-ws-{}-{}",
        sanitize_fs(workspace_id),
        slug_branch(branch)
    )
}

fn sanitize_fs(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('-');
        }
    }
    if out.is_empty() {
        "workspace".to_string()
    } else {
        out
    }
}

fn slug_branch(branch: &str) -> String {
    let stripped = branch
        .strip_prefix("fix/")
        .or_else(|| branch.strip_prefix("feat/"))
        .or_else(|| branch.strip_prefix("feature/"))
        .or_else(|| branch.strip_prefix("docs/"))
        .or_else(|| branch.strip_prefix("perf/"))
        .or_else(|| branch.strip_prefix("chore/"))
        .or_else(|| branch.strip_prefix("agent/"))
        .unwrap_or(branch);
    sanitize_fs(&stripped.replace('/', "-"))
}

fn lexical_absolute(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                normalized.pop();
            }
            Component::CurDir => {}
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}
