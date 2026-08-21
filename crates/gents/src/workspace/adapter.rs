use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};

use super::action_plan::CreateWorkspaceAction;
use super::documents::ProvisioningObservation;
use super::LogicalWorkspaceIdentity;

const IDENTITY_FILE_NAME: &str = "gents-workspace-identity.json";
const DIRTY_BASE_SUMMARY_LIMIT: usize = 2048;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ObservedEffect {
    Absent,
    Match {
        observation: ProvisioningObservation,
        tree_hash: String,
        dirty_base: DirtyBase,
    },
    Mismatch {
        reason: String,
        observation: ProvisioningObservation,
        dirty_base: DirtyBase,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirtyBase {
    pub dirty: bool,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RecordedIdentity {
    workspace_id: String,
    work_unit_id: String,
    repository_id: String,
    base_sha: String,
    branch: String,
}

impl From<&LogicalWorkspaceIdentity> for RecordedIdentity {
    fn from(identity: &LogicalWorkspaceIdentity) -> Self {
        Self {
            workspace_id: identity.workspace_id.clone(),
            work_unit_id: identity.work_unit_id.clone(),
            repository_id: identity.repository_id.clone(),
            base_sha: identity.base_sha.clone(),
            branch: identity.branch.clone(),
        }
    }
}

pub(crate) fn observe_dirty_base(source: &Path) -> Result<DirtyBase> {
    let porcelain = git_output(source, &["status", "--porcelain=v1"])?;
    let dirty = !porcelain.trim().is_empty();
    let summary = if porcelain.len() > DIRTY_BASE_SUMMARY_LIMIT {
        porcelain[..DIRTY_BASE_SUMMARY_LIMIT].to_string()
    } else {
        porcelain
    };
    Ok(DirtyBase { dirty, summary })
}

pub(crate) fn observe_effect(
    source: &Path,
    dest: &Path,
    identity: &LogicalWorkspaceIdentity,
    resolved_base: &str,
    artifacts: &[String],
) -> Result<ObservedEffect> {
    let dirty_base = observe_dirty_base(source)?;
    if !dest.exists() {
        return Ok(ObservedEffect::Absent);
    }
    let mut observation = ProvisioningObservation {
        path_exists: true,
        ..ProvisioningObservation::default()
    };
    observation.worktree_registered = is_worktree_of(source, dest)?;
    observation.identity_recorded = identity_path(dest).ok().is_some_and(|path| path.is_file());
    observation.artifacts_cloned = artifacts.iter().any(|rel| dest.join(rel).exists());

    match match_existing(source, dest, identity, resolved_base) {
        Ok(tree_hash) => Ok(ObservedEffect::Match {
            observation,
            tree_hash,
            dirty_base,
        }),
        Err(reason) => Ok(ObservedEffect::Mismatch {
            reason,
            observation,
            dirty_base,
        }),
    }
}

/// Create the worktree (and optional artifact clones). Never deletes `dest`.
pub(crate) fn provision(
    source: &Path,
    dest: &Path,
    action: &CreateWorkspaceAction,
    resolved_base: &str,
) -> Result<ProvisioningObservation> {
    add_worktree(source, dest, &action.branch, resolved_base)?;
    write_identity(dest, &action.identity())?;
    let mut observation = ProvisioningObservation {
        path_exists: dest.exists(),
        worktree_registered: true,
        identity_recorded: true,
        artifacts_cloned: false,
    };
    if action.adapter.clones_artifacts() {
        observation.artifacts_cloned =
            clone_artifacts(source, dest, &action.effective_clone_artifacts())?;
    }
    Ok(observation)
}

pub(crate) fn write_identity(dest: &Path, identity: &LogicalWorkspaceIdentity) -> Result<()> {
    let path = identity_path(dest)?;
    let recorded = RecordedIdentity::from(identity);
    let json = serde_json::to_vec_pretty(&recorded).context("serializing workspace identity")?;
    fs::write(&path, json)
        .with_context(|| format!("writing workspace identity {}", path.display()))?;
    Ok(())
}

pub(crate) fn observed_tree_hash(dest: &Path) -> Result<String> {
    git_output(dest, &["rev-parse", "HEAD^{tree}"])
}

pub(crate) fn resolve_base_sha(source: &Path, base_sha: &str) -> Result<String> {
    git_output(
        source,
        &["rev-parse", "--verify", &format!("{base_sha}^{{commit}}")],
    )
}

fn match_existing(
    source: &Path,
    dest: &Path,
    identity: &LogicalWorkspaceIdentity,
    resolved_base: &str,
) -> Result<String, String> {
    if let Some(recorded) = read_identity(dest).map_err(|err| err.to_string())? {
        if recorded != RecordedIdentity::from(identity) {
            return Err(format!(
                "existing target identity mismatch at {}",
                dest.display()
            ));
        }
    } else {
        if !is_worktree_of(source, dest).map_err(|err| err.to_string())? {
            return Err(format!(
                "existing path {} is not a worktree of the source repository",
                dest.display()
            ));
        }
        let head = git_output(dest, &["rev-parse", "HEAD"]).map_err(|err| err.to_string())?;
        let branch = git_output(dest, &["rev-parse", "--abbrev-ref", "HEAD"])
            .map_err(|err| err.to_string())?;
        if head != resolved_base || branch != identity.branch {
            return Err(format!(
                "existing worktree at {} does not match base_sha/branch",
                dest.display()
            ));
        }
        write_identity(dest, identity).map_err(|err| err.to_string())?;
    }
    observed_tree_hash(dest).map_err(|err| err.to_string())
}

fn read_identity(dest: &Path) -> Result<Option<RecordedIdentity>> {
    let path = match identity_path(dest) {
        Ok(path) => path,
        Err(_) => return Ok(None),
    };
    if !path.is_file() {
        return Ok(None);
    }
    let bytes = fs::read(&path)
        .with_context(|| format!("reading workspace identity {}", path.display()))?;
    let recorded = serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing workspace identity {}", path.display()))?;
    Ok(Some(recorded))
}

fn identity_path(dest: &Path) -> Result<PathBuf> {
    let git_dir = git_output(dest, &["rev-parse", "--absolute-git-dir"])?;
    Ok(PathBuf::from(git_dir).join(IDENTITY_FILE_NAME))
}

fn is_worktree_of(source: &Path, dest: &Path) -> Result<bool> {
    if !dest.exists() {
        return Ok(false);
    }
    let dest_common = match git_output(dest, &["rev-parse", "--absolute-git-common-dir"]) {
        Ok(value) => value,
        Err(_) => return Ok(false),
    };
    let source_common = git_output(source, &["rev-parse", "--absolute-git-common-dir"])?;
    let dest_common =
        fs::canonicalize(&dest_common).unwrap_or_else(|_| PathBuf::from(&dest_common));
    let source_common =
        fs::canonicalize(&source_common).unwrap_or_else(|_| PathBuf::from(&source_common));
    Ok(dest_common == source_common)
}

fn add_worktree(source: &Path, dest: &Path, branch: &str, base_sha: &str) -> Result<()> {
    if dest.exists() {
        bail!(
            "refusing to create worktree: destination {} already exists",
            dest.display()
        );
    }
    let branch_ref = format!("refs/heads/{branch}");
    if git_ok(source, &["show-ref", "--verify", "--quiet", &branch_ref]) {
        let branch_sha = git_output(source, &["rev-parse", &branch_ref])?;
        if branch_sha != base_sha {
            bail!(
                "branch {branch} exists at {branch_sha}, not requested base {base_sha}; not moving the branch"
            );
        }
        git_run(
            source,
            &["worktree", "add", "--", &dest.display().to_string(), branch],
        )?;
    } else {
        git_run(
            source,
            &[
                "worktree",
                "add",
                "-b",
                branch,
                "--",
                &dest.display().to_string(),
                base_sha,
            ],
        )?;
    }
    Ok(())
}

fn clone_artifacts(source: &Path, dest: &Path, artifacts: &[String]) -> Result<bool> {
    let mut cloned_any = false;
    for relative in artifacts {
        let rel = relative.trim_end_matches('/');
        let src_dir = source.join(rel);
        if !src_dir.is_dir() {
            tracing::info!(
                artifact = %rel,
                "skip: no artifact directory to clone"
            );
            continue;
        }
        let dst_dir = dest.join(rel);
        if dst_dir.exists() {
            cloned_any = true;
            continue;
        }
        if let Some(parent) = dst_dir.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating artifact parent {}", parent.display()))?;
        }
        clone_dir(&src_dir, &dst_dir, rel)?;
        if rel == "target" {
            let _ = fs::remove_dir_all(dst_dir.join("debug").join("incremental"));
            let _ = fs::remove_dir_all(dst_dir.join("release").join("incremental"));
        }
        cloned_any = true;
    }
    Ok(cloned_any)
}

fn clone_dir(src: &Path, dst: &Path, label: &str) -> Result<()> {
    tracing::info!(label, "cloning artifact directory");
    let clonefile = Command::new("cp").args(["-Rc"]).arg(src).arg(dst).status();
    match clonefile {
        Ok(status) if status.success() => return Ok(()),
        Ok(_) | Err(_) => {
            let _ = fs::remove_dir_all(dst);
        }
    }
    let copied = Command::new("cp")
        .args(["-R"])
        .arg(src)
        .arg(dst)
        .status()
        .with_context(|| format!("copying {label}"))?;
    if copied.success() {
        Ok(())
    } else {
        let _ = fs::remove_dir_all(dst);
        Err(anyhow!("plain copy of {label} failed"))
    }
}

pub(crate) fn git_run(cwd: &Path, args: &[&str]) -> Result<()> {
    let output = git_command(cwd, args)
        .output()
        .with_context(|| format!("running git {} in {}", args.join(" "), cwd.display()))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(anyhow!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

fn git_ok(cwd: &Path, args: &[&str]) -> bool {
    git_command(cwd, args)
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn git_output(cwd: &Path, args: &[&str]) -> Result<String> {
    let output = git_command(cwd, args)
        .output()
        .with_context(|| format!("running git {} in {}", args.join(" "), cwd.display()))?;
    if !output.status.success() {
        bail!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn git_command(cwd: &Path, args: &[&str]) -> Command {
    let mut cmd = Command::new("git");
    cmd.args(args).current_dir(cwd);
    cmd.env_remove("GIT_DIR");
    cmd.env_remove("GIT_WORK_TREE");
    cmd.env_remove("GIT_COMMON_DIR");
    cmd.env_remove("GIT_INDEX_FILE");
    cmd.env("GIT_TERMINAL_PROMPT", "0");
    cmd
}
