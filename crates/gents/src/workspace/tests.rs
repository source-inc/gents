use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;

use crate::toolset::{validate_command_policy, CommandExecutionMode, CommandExecutionPolicy};

use super::*;

fn capabilities() -> BTreeSet<String> {
    [
        CAP_CREATE_WORKSPACE,
        CAP_OBSERVE_DIRTY_BASE,
        CAP_CLONE_ARTIFACTS,
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn git_worktree_caps() -> BTreeSet<String> {
    [CAP_CREATE_WORKSPACE, CAP_OBSERVE_DIRTY_BASE]
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn git(cwd: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_COMMON_DIR")
        .env_remove("GIT_INDEX_FILE")
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

struct Fixture {
    _root: TempDir,
    repo: PathBuf,
    base_sha: String,
}

impl Fixture {
    fn new() -> Self {
        let root = TempDir::new().expect("tempdir");
        let repo = root.path().join("repo");
        fs::create_dir_all(&repo).unwrap();
        git(&repo, &["init", "-b", "main"]);
        git(&repo, &["config", "user.email", "ws@example.com"]);
        git(&repo, &["config", "user.name", "Workspace Test"]);
        fs::write(repo.join("README.md"), "hello\n").unwrap();
        git(&repo, &["add", "README.md"]);
        git(&repo, &["commit", "-m", "init"]);
        let base_sha = git(&repo, &["rev-parse", "HEAD"]);
        Self {
            _root: root,
            repo,
            base_sha,
        }
    }

    fn parent(&self) -> &Path {
        self.repo.parent().expect("repo parent")
    }

    fn action(
        &self,
        workspace_id: &str,
        work_unit_id: &str,
        branch: &str,
    ) -> CreateWorkspaceAction {
        CreateWorkspaceAction {
            workspace_id: workspace_id.to_string(),
            work_unit_id: work_unit_id.to_string(),
            repository_id: "repo-1".to_string(),
            base_sha: self.base_sha.clone(),
            branch: branch.to_string(),
            creation_policy: CreationPolicy::GitWorktreeDiff,
            adapter: WorkspaceAdapterKind::GitWorktree,
            clone_artifacts: None,
        }
    }

    fn ctx<'a>(
        &'a self,
        docs: &'a mut MemoryWorkspaceDocuments,
        caps: BTreeSet<String>,
    ) -> HostExecutorContext<'a> {
        HostExecutorContext {
            deployment_id: "deploy-1".to_string(),
            repository: RepositoryPlacementRef {
                repository_id: "repo-1".to_string(),
                deployment_id: "deploy-1".to_string(),
                host_path: self.repo.clone(),
                enabled: true,
            },
            ceiling: Some(self.parent()),
            capabilities: caps,
            writer_principal: "did:key:zWriter".to_string(),
            integrator_principal: "did:key:zIntegrator".to_string(),
            caused_by_invocation_id: "inv-1".to_string(),
            caused_by_correlation: "corr-1".to_string(),
            documents: docs,
        }
    }
}

fn git_worktree_diff_policy() -> CommandExecutionPolicy {
    CommandExecutionPolicy::write_capable()
        .with_mode(CommandExecutionMode::WorkspaceWrite)
        .with_git_worktree_diff()
}

#[test]
fn builtin_emitter_omits_absolute_destination() {
    let action = CreateWorkspaceAction {
        workspace_id: "ws-1".into(),
        work_unit_id: "unit-1".into(),
        repository_id: "repo-1".into(),
        base_sha: "abc".into(),
        branch: "topic".into(),
        creation_policy: CreationPolicy::GitWorktreeDiff,
        adapter: WorkspaceAdapterKind::MakeWorktree,
        clone_artifacts: None,
    };
    let plan: ActionPlan = emit_create_workspace_plan(action);
    assert!(action_journal_prefix_legal(&[]));
    assert_eq!(
        DEFAULT_MAKE_WORKTREE_ARTIFACTS,
        ["target/", "crates/gents/proofs/.lake"]
    );
    let json = serde_json::to_value(&plan).unwrap();
    let encoded = json.to_string();
    assert!(!encoded.contains("host_path"));
    assert!(!encoded.contains("/tmp"));
    assert_eq!(json["abi"], 1);
    assert_eq!(json["actions"][0]["type"], "create_workspace");
    assert!(serde_json::from_value::<HostAction>(serde_json::json!({
        "type": "create_workspace",
        "workspace_id": "ws-1",
        "work_unit_id": "unit-1",
        "repository_id": "repo-1",
        "base_sha": "abc",
        "branch": "topic",
        "host_path": "/tmp/evil"
    }))
    .is_err());
}

#[test]
fn isolated_workspace_mutation_has_no_host_path() {
    let doc = IsolatedWorkspaceDoc {
        workspace_id: "ws-1".into(),
        work_unit_id: "unit-1".into(),
        repository_id: "repo-1".into(),
        base_sha: "abc".into(),
        branch: "topic".into(),
        creation_policy: "git_worktree_diff".into(),
        adapter: "git_worktree".into(),
        owner_deployment_id: "deploy-1".into(),
        writer_principal: "did:key:zW".into(),
        integrator_principal: "did:key:zI".into(),
        instruction_manifest: "{}".into(),
        seal_hash: None,
        lifecycle_state: "ready".into(),
        caused_by_invocation_id: "inv-1".into(),
        caused_by_correlation: "corr-1".into(),
    };
    let mutation = isolated_workspace_create_mutation(&doc);
    assert!(!mutation.contains("host_path"));
    assert!(mutation.contains("seal_hash: null"));
    let placement = WorkspacePlacementDoc {
        workspace_id: "ws-1".into(),
        deployment_id: "deploy-1".into(),
        host_path: "/tmp/ws".into(),
        repository_placement_id: "repo-1".into(),
        adapter: "git_worktree".into(),
        adapter_version: "gents-workspace-adapter/1".into(),
        dirty_base: false,
        dirty_base_summary: String::new(),
        provisioning_state: "{}".into(),
        observed_tree_hash: "tree".into(),
    };
    let placement_mutation =
        workspace_placement_upsert_mutation(&placement, "2026-08-21T00:00:00Z");
    assert!(placement_mutation.contains("host_path:"));
    assert!(!placement_mutation.contains("[]"));
}

#[test]
fn create_workspace_is_idempotent_when_identity_matches() {
    let fx = Fixture::new();
    let mut docs = MemoryWorkspaceDocuments::default();
    let action = fx.action("ws-match", "unit-1", "topic-match");
    let plan = emit_create_workspace_plan(action);
    let mut journal = Vec::new();
    let first = execute_create_workspace_plan(
        &plan,
        &mut journal,
        &mut fx.ctx(&mut docs, git_worktree_caps()),
    )
    .expect("first provision");
    assert_eq!(first.workspace.lifecycle_state, "ready");
    let observation: ProvisioningObservation =
        serde_json::from_str(&first.placement.provisioning_state).unwrap();
    assert!(observation.path_exists);
    assert!(observation.worktree_registered);
    assert!(Path::new(&first.placement.host_path)
        .join("README.md")
        .is_file());
    assert_eq!(
        journal.last().map(|entry| entry.state),
        Some(ActionJournalState::ResultDocsWritten)
    );

    let mut journal2 = Vec::new();
    let second = execute_create_workspace_plan(
        &plan,
        &mut journal2,
        &mut fx.ctx(&mut docs, git_worktree_caps()),
    )
    .expect("idempotent provision");
    assert_eq!(second.workspace.lifecycle_state, "ready");
    assert_eq!(second.placement.host_path, first.placement.host_path);
    assert_eq!(
        docs.workspaces.len(),
        1,
        "idempotent retry must not mint a second IsolatedWorkspace"
    );
}

#[test]
fn existing_target_mismatch_does_not_overwrite_or_cleanup() {
    let fx = Fixture::new();
    let dest = fx.parent().join("foreign");
    fs::create_dir_all(&dest).unwrap();
    fs::write(dest.join("keep-me.txt"), "untouched\n").unwrap();

    let mut docs = MemoryWorkspaceDocuments::default();
    let action = fx.action("foreign", "unit-1", "topic-foreign");
    // Force dest to be the pre-existing foreign directory.
    let planned = workspace_host_path(
        &fx.repo,
        &action.workspace_id,
        &action.branch,
        Some(fx.parent()),
    )
    .unwrap();
    fs::rename(&dest, &planned).unwrap();
    fs::write(planned.join("keep-me.txt"), "untouched\n").unwrap();

    let plan = emit_create_workspace_plan(action);
    let mut journal = Vec::new();
    let err = execute_create_workspace_plan(
        &plan,
        &mut journal,
        &mut fx.ctx(&mut docs, git_worktree_caps()),
    )
    .expect_err("mismatch must fail");
    assert!(err.identity_mismatch(), "{err}");
    assert_eq!(
        err.outcome()
            .map(|outcome| outcome.workspace.lifecycle_state.as_str()),
        Some("provisionFailed")
    );
    assert_eq!(
        fs::read_to_string(planned.join("keep-me.txt")).unwrap(),
        "untouched\n"
    );
    assert!(
        !planned.join("README.md").exists(),
        "mismatch must not check out the repo over the leftover"
    );
    let stored = docs
        .load_isolated_workspace("foreign")
        .unwrap()
        .expect("ProvisionFailed row");
    assert_eq!(stored.lifecycle_state, "provisionFailed");
    assert!(!stored.dirty_base_on_replicated_row());
}

#[test]
fn matching_ready_workspace_is_not_overwritten_by_identity_mismatch_retry() {
    let fx = Fixture::new();
    let mut docs = MemoryWorkspaceDocuments::default();
    let original = fx.action("ws-stable", "unit-1", "topic-stable");
    let plan = emit_create_workspace_plan(original.clone());
    let mut journal = Vec::new();
    let first = execute_create_workspace_plan(
        &plan,
        &mut journal,
        &mut fx.ctx(&mut docs, git_worktree_caps()),
    )
    .expect("provision");
    let dest = PathBuf::from(&first.placement.host_path);
    let readme = fs::read_to_string(dest.join("README.md")).unwrap();

    let mut mismatched = original;
    mismatched.work_unit_id = "unit-OTHER".into();
    let plan = emit_create_workspace_plan(mismatched);
    let mut journal = Vec::new();
    let err = execute_create_workspace_plan(
        &plan,
        &mut journal,
        &mut fx.ctx(&mut docs, git_worktree_caps()),
    )
    .expect_err("identity mismatch");
    assert!(err.identity_mismatch(), "{err}");
    assert_eq!(fs::read_to_string(dest.join("README.md")).unwrap(), readme);
    let stored = docs.load_isolated_workspace("ws-stable").unwrap().unwrap();
    assert_eq!(stored.lifecycle_state, "ready");
    assert_eq!(stored.work_unit_id, "unit-1");
}

#[test]
fn recover_from_executing_observes_existing_effect() {
    let fx = Fixture::new();
    let mut docs = MemoryWorkspaceDocuments::default();
    let action = fx.action("ws-recover", "unit-1", "topic-recover");
    let plan = emit_create_workspace_plan(action);
    let mut journal = Vec::new();
    let first = execute_create_workspace_plan(
        &plan,
        &mut journal,
        &mut fx.ctx(&mut docs, git_worktree_caps()),
    )
    .expect("provision");

    docs.workspaces.clear();
    docs.placements.clear();
    let mut journal = vec![ActionJournalEntry::new(0, ActionJournalState::Executing)];
    let recovered = execute_create_workspace_plan(
        &plan,
        &mut journal,
        &mut fx.ctx(&mut docs, git_worktree_caps()),
    )
    .expect("recover from Executing");
    assert_eq!(recovered.workspace.lifecycle_state, "ready");
    assert_eq!(recovered.placement.host_path, first.placement.host_path);
    assert_eq!(
        journal.last().map(|entry| entry.state),
        Some(ActionJournalState::ResultDocsWritten)
    );
}

#[test]
fn dirty_base_is_recorded_on_placement_not_copied() {
    let fx = Fixture::new();
    fs::write(fx.repo.join("dirty.txt"), "only in source\n").unwrap();
    let mut docs = MemoryWorkspaceDocuments::default();
    let action = fx.action("ws-dirty", "unit-1", "topic-dirty");
    let plan = emit_create_workspace_plan(action);
    let mut journal = Vec::new();
    let outcome = execute_create_workspace_plan(
        &plan,
        &mut journal,
        &mut fx.ctx(&mut docs, git_worktree_caps()),
    )
    .expect("provision");
    assert!(outcome.placement.dirty_base);
    assert!(outcome.placement.dirty_base_summary.contains("dirty.txt"));
    let dest = PathBuf::from(&outcome.placement.host_path);
    assert!(
        !dest.join("dirty.txt").exists(),
        "git_worktree must not copy dirty source files"
    );
    assert!(dest.join("README.md").is_file());
    let isolated = docs.load_isolated_workspace("ws-dirty").unwrap().unwrap();
    assert!(!isolated.dirty_base_on_replicated_row());
}

#[test]
fn make_worktree_clones_artifacts_git_worktree_does_not() {
    let fx = Fixture::new();
    fs::create_dir_all(fx.repo.join("target")).unwrap();
    fs::write(fx.repo.join("target").join("cache.bin"), "warm").unwrap();
    fs::create_dir_all(fx.repo.join("crates/gents/proofs/.lake")).unwrap();
    fs::write(
        fx.repo.join("crates/gents/proofs/.lake").join("pkg"),
        "mathlib",
    )
    .unwrap();

    let mut docs = MemoryWorkspaceDocuments::default();
    let mut make = fx.action("ws-make", "unit-1", "topic-make");
    make.adapter = WorkspaceAdapterKind::MakeWorktree;
    let plan = emit_create_workspace_plan(make);
    let mut journal = Vec::new();
    let make_out =
        execute_create_workspace_plan(&plan, &mut journal, &mut fx.ctx(&mut docs, capabilities()))
            .expect("make_worktree");
    let make_dest = PathBuf::from(&make_out.placement.host_path);
    assert_eq!(
        fs::read_to_string(make_dest.join("target").join("cache.bin")).unwrap(),
        "warm"
    );
    assert_eq!(
        fs::read_to_string(make_dest.join("crates/gents/proofs/.lake").join("pkg")).unwrap(),
        "mathlib"
    );

    let mut docs = MemoryWorkspaceDocuments::default();
    let git_only = fx.action("ws-git", "unit-1", "topic-git");
    let plan = emit_create_workspace_plan(git_only);
    let mut journal = Vec::new();
    let git_out = execute_create_workspace_plan(
        &plan,
        &mut journal,
        &mut fx.ctx(&mut docs, git_worktree_caps()),
    )
    .expect("git_worktree");
    let git_dest = PathBuf::from(&git_out.placement.host_path);
    assert!(!git_dest.join("target").exists());
    assert!(!git_dest.join("crates/gents/proofs/.lake").exists());
}

#[test]
fn destination_escaping_ceiling_is_denied() {
    let fx = Fixture::new();
    let mut docs = MemoryWorkspaceDocuments::default();
    let action = fx.action("ws-escape", "unit-1", "topic-escape");
    let plan = emit_create_workspace_plan(action);
    let mut journal = Vec::new();
    let mut ctx = fx.ctx(&mut docs, git_worktree_caps());
    ctx.ceiling = Some(&fx.repo);
    let err =
        execute_create_workspace_plan(&plan, &mut journal, &mut ctx).expect_err("ceiling escape");
    assert!(matches!(err, HostExecuteError::Denied { .. }), "{err}");
    assert!(docs.workspaces.is_empty());
}

#[test]
fn git_worktree_diff_denies_metadata_writes_allows_reads() {
    let policy = git_worktree_diff_policy();
    for sub in [
        "add",
        "commit",
        "merge",
        "rebase",
        "push",
        "update-ref",
        "symbolic-ref",
    ] {
        let err = validate_command_policy("git", &[sub.to_string()], &policy).unwrap_err();
        let payload = err.to_string();
        assert!(
            payload.contains("gitMetadataWriteDenied") || payload.contains("git_worktree_diff"),
            "expected git metadata denial for {sub}, got {payload}"
        );
    }
    for (command, args) in [
        ("git", vec!["status"]),
        ("git", vec!["diff"]),
        ("git", vec!["log"]),
        ("git", vec!["rev-parse", "HEAD"]),
    ] {
        let argv: Vec<String> = args.iter().map(|arg| (*arg).to_string()).collect();
        validate_command_policy(command, &argv, &policy)
            .unwrap_or_else(|err| panic!("{command} {argv:?} should be allowed: {err}"));
    }

    let err = validate_command_policy(
        "/bin/sh",
        &["-lc".into(), "git commit -am 'x'".into()],
        &policy,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("gitMetadataWriteDenied")
            || err.to_string().contains("git_worktree_diff")
            || err.to_string().contains("commit"),
        "{err}"
    );

    let unrestricted =
        CommandExecutionPolicy::write_capable().with_mode(CommandExecutionMode::WorkspaceWrite);
    validate_command_policy("git", &[String::from("commit")], &unrestricted)
        .expect("without git_worktree_diff, WorkspaceWrite still allows git commit at argv layer");
}

trait IsolatedWorkspaceDocExt {
    fn dirty_base_on_replicated_row(&self) -> bool;
}

impl IsolatedWorkspaceDocExt for IsolatedWorkspaceDoc {
    fn dirty_base_on_replicated_row(&self) -> bool {
        let encoded = serde_json::to_string(self).unwrap();
        encoded.contains("dirty_base") || encoded.contains("host_path")
    }
}
