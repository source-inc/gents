use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};

use super::action_plan::CreateWorkspaceAction;
use super::documents::ProvisioningObservation;
use super::instructions::{InstructionFile, InstructionManifest, DEFAULT_INSTRUCTION_PATHS};
use super::LogicalWorkspaceIdentity;

const IDENTITY_FILE_NAME: &str = "gents-workspace-identity.json";
pub(crate) const DIRTY_BASE_SUMMARY_LIMIT: usize = 2048;

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
    Ok(DirtyBase {
        dirty,
        summary: bound_dirty_base_summary(&porcelain),
    })
}

/// Truncate porcelain at a UTF-8 char boundary so a multibyte filename
/// cannot panic CreateWorkspace before EffectObserved.
pub(crate) fn bound_dirty_base_summary(porcelain: &str) -> String {
    if porcelain.len() <= DIRTY_BASE_SUMMARY_LIMIT {
        return porcelain.to_string();
    }
    let mut end = DIRTY_BASE_SUMMARY_LIMIT;
    while end > 0 && !porcelain.is_char_boundary(end) {
        end -= 1;
    }
    porcelain[..end].to_string()
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
    observation.artifacts_cloned = artifacts_complete(source, dest, artifacts);

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
        clone_artifacts(source, dest, &action.effective_clone_artifacts())?;
        observation.artifacts_cloned =
            artifacts_complete(source, dest, &action.effective_clone_artifacts());
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

/// Working-tree identity for seal: temp-index `write-tree`, including
/// uncommitted writer edits and excluding gitignored paths.
pub(crate) fn working_tree_hash(dest: &Path) -> Result<String> {
    Ok(capture_seal_snapshot(dest)?.tree_hash)
}

pub(crate) struct SealSnapshot {
    pub tree_hash: String,
    pub diff: Vec<u8>,
    pub changed_files: Vec<String>,
}

pub(crate) fn capture_seal_snapshot(dest: &Path) -> Result<SealSnapshot> {
    let tmp = tempfile::Builder::new()
        .prefix("gents-ws-index")
        .tempdir()
        .context("creating temporary git index for seal")?;
    let index = tmp.path().join("index");
    git_run_with_index(dest, &index, &["add", "-A", "--"])?;
    let tree_hash = git_output_with_index(dest, &index, &["write-tree"])?;
    let diff =
        git_output_bytes_with_index(dest, &index, &["diff", "--binary", "--cached", "HEAD"])?;
    let names = git_output_with_index(dest, &index, &["diff", "--name-only", "--cached", "HEAD"])?;
    let changed_files = names
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect();
    Ok(SealSnapshot {
        tree_hash,
        diff,
        changed_files,
    })
}

/// Read instruction files from `base_sha` blobs. Never reads the live worktree.
/// Missing paths are omitted; any other `git show` failure fails closed.
pub(crate) fn capture_instruction_manifest(repo: &Path, base_sha: &str) -> Result<String> {
    let mut files = Vec::new();
    for path in DEFAULT_INSTRUCTION_PATHS {
        match git_show_blob(repo, base_sha, path)? {
            Some(bytes) => files.push(InstructionFile::from_bytes(path, &bytes)),
            None => continue,
        }
    }
    Ok(InstructionManifest::new(base_sha, files).to_json_string())
}

fn git_show_blob(repo: &Path, base_sha: &str, path: &str) -> Result<Option<Vec<u8>>> {
    let spec = format!("{base_sha}:{path}");
    let output = git_command(repo, &["show", &spec])
        .output()
        .with_context(|| format!("git show {spec} in {}", repo.display()))?;
    if output.status.success() {
        return Ok(Some(output.stdout));
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    if git_blob_missing(&stderr) {
        return Ok(None);
    }
    bail!("git show {spec} failed: {}", stderr.trim())
}

fn git_blob_missing(stderr: &str) -> bool {
    let lower = stderr.to_ascii_lowercase();
    lower.contains("does not exist") || lower.contains("exists on disk, but not in")
}

const SEAL_FILE_NAME: &str = "gents-workspace-seal.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RecordedSeal {
    seal_hash: String,
    base_sha: String,
}

pub(crate) struct IntegrateEffect {
    pub head_sha: String,
    pub changed_files: Vec<String>,
    pub diff: Vec<u8>,
    /// Set when a new commit object was created but trunk `HEAD` was not moved.
    pub pending_head: bool,
}

pub(crate) fn integrate_commit_message(seal_hash: &str) -> String {
    format!("gents: integrate workspace seal {seal_hash}")
}

/// Apply the sealed worktree diff onto an isolated index and `commit-tree`.
/// Does **not** move trunk `HEAD` — the caller writes a durable receipt first.
pub(crate) fn prepare_integrate_commit(
    trunk: &Path,
    worktree: &Path,
    seal_hash: &str,
    base_sha: &str,
) -> Result<IntegrateEffect> {
    let trunk = fs::canonicalize(trunk)
        .with_context(|| format!("canonicalizing trunk {}", trunk.display()))?;
    let worktree = fs::canonicalize(worktree)
        .with_context(|| format!("canonicalizing worktree {}", worktree.display()))?;
    if trunk == worktree {
        bail!("refusing to integrate: trunk/source checkout is the worker workspace");
    }
    if !is_worktree_of(&trunk, &worktree)? {
        bail!(
            "refusing to integrate: {} is not a worktree of the source checkout",
            worktree.display()
        );
    }
    if let Some(existing) = observe_integrate_commit(&trunk, seal_hash)? {
        let snapshot = capture_seal_snapshot(&worktree)?;
        if snapshot.tree_hash != seal_hash {
            bail!(
                "live tree hash {} does not match workspace seal_hash {seal_hash}",
                snapshot.tree_hash
            );
        }
        let head = git_output(&trunk, &["rev-parse", "HEAD"])?;
        return Ok(IntegrateEffect {
            pending_head: head == existing,
            head_sha: existing,
            changed_files: snapshot.changed_files,
            diff: snapshot.diff,
        });
    }
    let snapshot = capture_seal_snapshot(&worktree)?;
    if snapshot.tree_hash != seal_hash {
        bail!(
            "live tree hash {} does not match workspace seal_hash {seal_hash}",
            snapshot.tree_hash
        );
    }
    if diff_is_empty(&snapshot.diff) {
        return empty_diff_effect(&trunk, &worktree, seal_hash, base_sha, snapshot);
    }
    refuse_overlapping_dirty_trunk(&trunk, &snapshot.changed_files)?;
    let commit = commit_tree_from_isolated_index(&trunk, &snapshot.diff, seal_hash)?;
    Ok(IntegrateEffect {
        head_sha: commit,
        changed_files: snapshot.changed_files,
        diff: snapshot.diff,
        pending_head: true,
    })
}

fn empty_diff_effect(
    trunk: &Path,
    worktree: &Path,
    seal_hash: &str,
    base_sha: &str,
    snapshot: SealSnapshot,
) -> Result<IntegrateEffect> {
    let trunk_tree = git_output(trunk, &["rev-parse", "HEAD^{tree}"])?;
    let worktree_head = git_output(worktree, &["rev-parse", "HEAD"])?;
    if trunk_tree == seal_hash || worktree_head == base_sha {
        let head_sha = git_output(trunk, &["rev-parse", "HEAD"])?;
        return Ok(IntegrateEffect {
            head_sha,
            changed_files: snapshot.changed_files,
            diff: snapshot.diff,
            pending_head: false,
        });
    }
    bail!(
        "empty sealed diff but trunk tree {trunk_tree} does not match seal_hash {seal_hash} and worktree HEAD {worktree_head} is not base_sha {base_sha}"
    )
}

fn diff_is_empty(diff: &[u8]) -> bool {
    std::str::from_utf8(diff)
        .map(|text| text.trim().is_empty())
        .unwrap_or(false)
}

/// Point trunk `HEAD` at a commit created by [`prepare_integrate_commit`]
/// without rewriting the default index (operator staged files stay put).
///
/// `update-ref` is CAS from the pending commit's parent so later trunk
/// commits are not rewound. `HEAD == commit` still applies path updates
/// so a crash between `update-ref` and the last path converges.
pub(crate) fn advance_trunk_to_integrate_commit(trunk: &Path, commit: &str) -> Result<()> {
    let trunk = fs::canonicalize(trunk)
        .with_context(|| format!("canonicalizing trunk {}", trunk.display()))?;
    let head = git_output(&trunk, &["rev-parse", "HEAD"])?;
    if head == commit {
        let subject = git_output(&trunk, &["log", "-1", "--format=%s", commit])?;
        if !subject.starts_with("gents: integrate workspace seal ") {
            return Ok(());
        }
        return apply_integrate_commit_paths(&trunk, commit);
    }
    if git_ok(&trunk, &["merge-base", "--is-ancestor", commit, &head]) {
        return Ok(());
    }
    let parent = git_output(&trunk, &["rev-parse", &format!("{commit}^")])?;
    if head != parent {
        bail!(
            "refusing to move trunk HEAD at {} to integrate commit {commit}: HEAD {head} is not the commit's parent {parent}",
            trunk.display()
        );
    }
    git_run(&trunk, &["update-ref", "HEAD", commit, &parent]).with_context(|| {
        format!(
            "moving trunk HEAD at {} to integrate commit {commit}",
            trunk.display()
        )
    })?;
    apply_integrate_commit_paths(&trunk, commit)
}

fn apply_integrate_commit_paths(trunk: &Path, commit: &str) -> Result<()> {
    let parent = git_output(trunk, &["rev-parse", &format!("{commit}^")])?;
    let name_status = git_output(
        trunk,
        &["diff", "--name-status", "--no-renames", &parent, commit],
    )?;
    apply_integrate_name_status(trunk, commit, &name_status)
}

fn apply_integrate_name_status(trunk: &Path, commit: &str, name_status: &str) -> Result<()> {
    for line in name_status
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let Some((status, rest)) = line.split_once('\t') else {
            bail!("unrecognized git name-status line: {line}");
        };
        let code = status.chars().next().unwrap_or('\0');
        match code {
            'A' | 'M' | 'T' => checkout_from_commit(trunk, commit, rest)?,
            'D' => remove_from_trunk(trunk, rest)?,
            'R' | 'C' => {
                let Some((old, new)) = rest.split_once('\t') else {
                    bail!("rename/copy name-status missing dest: {line}");
                };
                if code == 'R' {
                    remove_from_trunk(trunk, old)?;
                }
                checkout_from_commit(trunk, commit, new)?;
            }
            _ => bail!("unsupported git name-status {status} in {line}"),
        }
    }
    Ok(())
}

fn checkout_from_commit(trunk: &Path, commit: &str, path: &str) -> Result<()> {
    git_run(trunk, &["checkout", commit, "--", path]).with_context(|| {
        format!(
            "checking out sealed path {path} onto trunk {}",
            trunk.display()
        )
    })
}

fn remove_from_trunk(trunk: &Path, path: &str) -> Result<()> {
    git_run(trunk, &["rm", "-f", "--ignore-unmatch", "--", path])
        .with_context(|| format!("removing sealed path {path} from trunk {}", trunk.display()))
}

pub(crate) fn observe_integrate_commit(trunk: &Path, seal_hash: &str) -> Result<Option<String>> {
    let expected = integrate_commit_message(seal_hash);
    let log = git_output(trunk, &["log", "--format=%H%x00%s", "-32"])?;
    for line in log.split('\n') {
        let Some((hash, subject)) = line.split_once('\u{0}') else {
            continue;
        };
        if subject == expected {
            return Ok(Some(hash.to_string()));
        }
    }
    Ok(None)
}

pub(crate) fn commit_exists(repo: &Path, sha: &str) -> bool {
    git_ok(repo, &["cat-file", "-t", sha])
}

fn refuse_overlapping_dirty_trunk(trunk: &Path, changed_files: &[String]) -> Result<()> {
    let porcelain = git_output(trunk, &["status", "--porcelain=v1"])?;
    if porcelain.trim().is_empty() {
        return Ok(());
    }
    for path in porcelain_paths(&porcelain) {
        for part in path.split(" -> ") {
            if changed_files.iter().any(|file| file == part) {
                bail!(
                    "refusing to integrate: trunk has uncommitted changes overlapping sealed path {part}"
                );
            }
        }
    }
    Ok(())
}

/// `git_output` trims the buffer, so an unstaged line ` M path` becomes `M path`.
fn porcelain_paths(porcelain: &str) -> impl Iterator<Item = &str> {
    porcelain.lines().filter_map(|line| {
        let line = line.trim_end();
        if line.is_empty() {
            return None;
        }
        let path = if line.len() >= 3 && line.as_bytes()[2] == b' ' {
            line[3..].trim()
        } else if line.len() >= 2 && line.as_bytes()[1] == b' ' {
            line[2..].trim()
        } else {
            return None;
        };
        if path.is_empty() {
            None
        } else {
            Some(path)
        }
    })
}

fn commit_tree_from_isolated_index(trunk: &Path, diff: &[u8], seal_hash: &str) -> Result<String> {
    let tmp = tempfile::Builder::new()
        .prefix("gents-integrate-index")
        .tempdir()
        .context("creating temporary git index for integrate")?;
    let index = tmp.path().join("index");
    git_run_with_index(trunk, &index, &["read-tree", "HEAD"])?;
    git_apply_cached(trunk, &index, diff).with_context(|| {
        format!(
            "git apply --cached on isolated index in {}",
            trunk.display()
        )
    })?;
    let tree = git_output_with_index(trunk, &index, &["write-tree"])?;
    let parent = git_output(trunk, &["rev-parse", "HEAD"])?;
    let message = integrate_commit_message(seal_hash);
    let mut cmd = git_command_with_index(
        trunk,
        &index,
        &["commit-tree", &tree, "-p", &parent, "-m", &message],
    );
    cmd.env("GIT_AUTHOR_NAME", "gents-integrator");
    cmd.env("GIT_AUTHOR_EMAIL", "gents-integrator@local");
    cmd.env("GIT_COMMITTER_NAME", "gents-integrator");
    cmd.env("GIT_COMMITTER_EMAIL", "gents-integrator@local");
    git_output_inner(cmd, trunk, &["commit-tree"])
}

/// Remove the worker tree. Never deletes the source checkout. Idempotent when
/// `dest` is already gone. Not called implicitly on request terminal.
///
/// `expected` is the host-chosen dest from `(workspace_id, branch)`. Mismatch,
/// ancestor-of-source, and non-sibling leftovers refuse `remove_dir_all`.
pub(crate) fn cleanup_workspace_tree(
    source: &Path,
    dest: &Path,
    expected: &Path,
    ceiling: Option<&Path>,
) -> Result<()> {
    let source = if source.exists() {
        fs::canonicalize(source)
            .with_context(|| format!("canonicalizing source {}", source.display()))?
    } else {
        source.to_path_buf()
    };
    let expected = if expected.exists() {
        fs::canonicalize(expected).unwrap_or_else(|_| expected.to_path_buf())
    } else {
        expected.to_path_buf()
    };
    if let Some(ceiling) = ceiling {
        let ceiling = if ceiling.exists() {
            fs::canonicalize(ceiling)
                .with_context(|| format!("canonicalizing ceiling {}", ceiling.display()))?
        } else {
            ceiling.to_path_buf()
        };
        if !expected.starts_with(&ceiling) {
            bail!(
                "cleanup dest {} escapes operator ceiling {}",
                expected.display(),
                ceiling.display()
            );
        }
    }
    if source.starts_with(&expected) {
        bail!(
            "refusing to cleanup: dest {} is an ancestor of the source checkout",
            expected.display()
        );
    }
    if expected == source {
        bail!("refusing to cleanup: destination is the source checkout");
    }
    if dest.exists() {
        let dest = fs::canonicalize(dest)
            .with_context(|| format!("canonicalizing workspace dest {}", dest.display()))?;
        if dest != expected {
            bail!(
                "cleanup dest {} does not match host-chosen path {}",
                dest.display(),
                expected.display()
            );
        }
    } else if dest != expected {
        bail!(
            "cleanup dest {} does not match host-chosen path {}",
            dest.display(),
            expected.display()
        );
    }
    if !expected.exists() {
        let _ = git_run(&source, &["worktree", "prune"]);
        return Ok(());
    }
    if expected == source {
        bail!("refusing to cleanup: destination is the source checkout");
    }
    let dest_str = expected.display().to_string();
    let remove = git_run(&source, &["worktree", "remove", "--force", &dest_str]);
    if expected.exists() {
        if is_worktree_of(&source, &expected)? {
            return Err(remove.err().unwrap_or_else(|| {
                anyhow!("git worktree remove left {} in place", expected.display())
            }));
        }
        // ProvisionFailed leftover at the exact host-chosen dest: not a worktree.
        fs::remove_dir_all(&expected)
            .with_context(|| format!("removing leftover workspace {}", expected.display()))?;
        let _ = git_run(&source, &["worktree", "prune"]);
    }
    Ok(())
}

fn git_apply_cached(cwd: &Path, index: &Path, diff: &[u8]) -> Result<()> {
    let mut cmd = git_command_with_index(
        cwd,
        index,
        &["apply", "--cached", "--whitespace=nowarn", "-"],
    );
    cmd.stdin(Stdio::piped());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    let mut child = cmd
        .spawn()
        .with_context(|| format!("spawning git apply --cached in {}", cwd.display()))?;
    {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("git apply stdin closed"))?;
        stdin.write_all(diff)?;
        if !diff.ends_with(b"\n") {
            stdin.write_all(b"\n")?;
        }
    }
    let output = child
        .wait_with_output()
        .with_context(|| format!("waiting for git apply in {}", cwd.display()))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(anyhow!(
            "git apply failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

pub(crate) fn write_seal_marker(dest: &Path, seal_hash: &str, base_sha: &str) -> Result<()> {
    let path = seal_marker_path(dest)?;
    let recorded = RecordedSeal {
        seal_hash: seal_hash.to_string(),
        base_sha: base_sha.to_string(),
    };
    let json = serde_json::to_vec_pretty(&recorded).context("serializing workspace seal")?;
    fs::write(&path, json).with_context(|| format!("writing workspace seal {}", path.display()))?;
    Ok(())
}

fn seal_marker_path(dest: &Path) -> Result<PathBuf> {
    let git_dir = git_output(dest, &["rev-parse", "--absolute-git-dir"])?;
    Ok(PathBuf::from(git_dir).join(SEAL_FILE_NAME))
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

pub(crate) fn is_worktree_of(source: &Path, dest: &Path) -> Result<bool> {
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

/// Resume-safe: copies missing dest dirs, leaves existing dest dirs in place.
pub(crate) fn clone_artifacts(source: &Path, dest: &Path, artifacts: &[String]) -> Result<()> {
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
    }
    Ok(())
}

pub(crate) fn artifacts_complete(source: &Path, dest: &Path, artifacts: &[String]) -> bool {
    artifacts.iter().all(|relative| {
        let rel = relative.trim_end_matches('/');
        !source.join(rel).is_dir() || dest.join(rel).exists()
    })
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
    git_run_inner(git_command(cwd, args), cwd, args)
}

fn git_run_with_index(cwd: &Path, index: &Path, args: &[&str]) -> Result<()> {
    git_run_inner(git_command_with_index(cwd, index, args), cwd, args)
}

fn git_run_inner(mut cmd: Command, cwd: &Path, args: &[&str]) -> Result<()> {
    let output = cmd
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
    git_output_inner(git_command(cwd, args), cwd, args)
}

pub(crate) fn absolute_git_dir(repo: &Path) -> Result<PathBuf> {
    Ok(PathBuf::from(git_output(
        repo,
        &["rev-parse", "--absolute-git-dir"],
    )?))
}

fn git_output_with_index(cwd: &Path, index: &Path, args: &[&str]) -> Result<String> {
    git_output_inner(git_command_with_index(cwd, index, args), cwd, args)
}

fn git_output_bytes_with_index(cwd: &Path, index: &Path, args: &[&str]) -> Result<Vec<u8>> {
    git_output_bytes_inner(git_command_with_index(cwd, index, args), cwd, args)
}

fn git_output_inner(cmd: Command, cwd: &Path, args: &[&str]) -> Result<String> {
    let bytes = git_output_bytes_inner(cmd, cwd, args)?;
    Ok(String::from_utf8_lossy(&bytes).trim().to_string())
}

fn git_output_bytes_inner(mut cmd: Command, cwd: &Path, args: &[&str]) -> Result<Vec<u8>> {
    let output = cmd
        .output()
        .with_context(|| format!("running git {} in {}", args.join(" "), cwd.display()))?;
    if !output.status.success() {
        bail!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output.stdout)
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

fn git_command_with_index(cwd: &Path, index: &Path, args: &[&str]) -> Command {
    let mut cmd = git_command(cwd, args);
    cmd.env("GIT_INDEX_FILE", index);
    cmd
}
