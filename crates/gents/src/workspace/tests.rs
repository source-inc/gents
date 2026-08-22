use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;

use crate::toolset::{
    validate_command_policy, CommandConstraints, CommandExecutionMode, CommandExecutionPolicy,
    CommandNetworkMode,
};

use super::adapter::{bound_dirty_base_summary, DIRTY_BASE_SUMMARY_LIMIT};
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

fn seal_caps() -> BTreeSet<String> {
    [CAP_SEAL_WORKSPACE]
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

    fn commit(&mut self, rel: &str, content: &str) {
        fs::write(self.repo.join(rel), content).unwrap();
        git(&self.repo, &["add", rel]);
        git(&self.repo, &["commit", "-m", rel]);
        self.base_sha = git(&self.repo, &["rev-parse", "HEAD"]);
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
    assert!(
        serde_json::from_value::<ActionPlan>(serde_json::json!({
            "abi": 1,
            "actions": [{
                "type": "create_workspace",
                "workspace_id": "ws-1",
                "work_unit_id": "unit-1",
                "repository_id": "repo-1",
                "base_sha": "abc",
                "branch": "topic"
            }],
            "host_path": "/tmp/evil"
        }))
        .is_err(),
        "destination on the plan root must be Denied, not dropped"
    );
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
    let mutation = isolated_workspace_upsert_mutation(&doc);
    assert!(!mutation.contains("host_path"));
    assert!(mutation.contains("upsert_IsolatedWorkspace"));
    assert!(!mutation.contains("create_IsolatedWorkspace"));
    assert!(mutation.contains("seal_hash: null"));
    assert!(mutation.contains("instruction_manifest:"));
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
    let receipt = WorkspaceReceiptDoc {
        receipt_id: "receipt-writer-ws-1-req-1".into(),
        workspace_id: "ws-1".into(),
        produced_by_request_id: "req-1".into(),
        produced_by_request_doc_id: "doc-1".into(),
        kind: "writer".into(),
        base_sha: "abc".into(),
        seal_hash: "tree".into(),
        head_sha: None,
        changed_files: None,
        diff_artifact: None,
        checks_run: None,
        unresolved_conflicts: None,
        integration_instructions: None,
    };
    let receipt_mutation = workspace_receipt_create_mutation(&receipt);
    assert!(receipt_mutation.contains("upsert_WorkspaceReceipt"));
    assert!(receipt_mutation.contains("changed_files: null"));
    assert!(!receipt_mutation.contains("[]"));
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

    for script in [
        "git --exec-path=/tmp/evil status",
        "git --git-dir=/tmp/evil status",
        "git --work-tree=/tmp/evil diff",
    ] {
        let err = validate_command_policy("/bin/sh", &["-lc".into(), script.into()], &policy)
            .unwrap_err();
        assert!(
            err.to_string().contains("gitMetadataWriteDenied")
                || err.to_string().contains("git_worktree_diff")
                || err.to_string().contains("exec-path")
                || err.to_string().contains("git-dir")
                || err.to_string().contains("work-tree"),
            "script {script:?} should be denied, got {err}"
        );
    }

    let constraints = CommandConstraints {
        allowed_argv_prefixes: Vec::new(),
        forbidden_argv_prefixes: Vec::new(),
        network_mode: CommandNetworkMode::Inherit,
        execution_mode: CommandExecutionMode::WorkspaceWrite,
        sandbox: CommandExecutionMode::WorkspaceWrite,
        deny_all_argv: false,
        deny_git_metadata_writes: true,
    };
    assert!(constraints.to_spawn_policy().deny_git_metadata_writes());
}

#[test]
fn dirty_base_summary_truncates_on_char_boundary() {
    let cjk = "文".repeat(700);
    assert!(cjk.len() > DIRTY_BASE_SUMMARY_LIMIT);
    assert!(!cjk.is_char_boundary(DIRTY_BASE_SUMMARY_LIMIT));
    let summary = bound_dirty_base_summary(&cjk);
    assert!(summary.len() <= DIRTY_BASE_SUMMARY_LIMIT);
    assert!(cjk.is_char_boundary(summary.len()));
}

#[test]
fn dirty_base_observation_survives_multibyte_porcelain() {
    let fx = Fixture::new();
    let name = "文".repeat(80);
    for i in 0..16 {
        fs::write(fx.repo.join(format!("{name}-{i}")), "x").unwrap();
    }
    let mut docs = MemoryWorkspaceDocuments::default();
    let action = fx.action("ws-cjk", "unit-1", "topic-cjk");
    let plan = emit_create_workspace_plan(action);
    let mut journal = Vec::new();
    let outcome = execute_create_workspace_plan(
        &plan,
        &mut journal,
        &mut fx.ctx(&mut docs, git_worktree_caps()),
    )
    .expect("multibyte dirty porcelain must not panic");
    assert!(outcome.placement.dirty_base);
    assert!(outcome.placement.dirty_base_summary.len() <= DIRTY_BASE_SUMMARY_LIMIT);
}

#[test]
fn make_worktree_resumes_missing_artifact_dirs_on_match() {
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
    let mut make = fx.action("ws-resume", "unit-1", "topic-resume");
    make.adapter = WorkspaceAdapterKind::MakeWorktree;
    let plan = emit_create_workspace_plan(make);
    let mut journal = Vec::new();
    let first =
        execute_create_workspace_plan(&plan, &mut journal, &mut fx.ctx(&mut docs, capabilities()))
            .expect("make_worktree");
    let dest = PathBuf::from(&first.placement.host_path);
    fs::remove_dir_all(dest.join("crates/gents/proofs/.lake")).unwrap();
    assert!(!dest.join("crates/gents/proofs/.lake").exists());

    let mut journal = Vec::new();
    execute_create_workspace_plan(&plan, &mut journal, &mut fx.ctx(&mut docs, capabilities()))
        .expect("resume clone");
    assert_eq!(
        fs::read_to_string(dest.join("crates/gents/proofs/.lake").join("pkg")).unwrap(),
        "mathlib"
    );
    assert_eq!(
        fs::read_to_string(dest.join("target").join("cache.bin")).unwrap(),
        "warm"
    );
}

#[test]
fn provision_failed_is_terminal_and_does_not_become_ready() {
    let fx = Fixture::new();
    let mut docs = MemoryWorkspaceDocuments::default();
    let action = fx.action("ws-failed", "unit-1", "topic-failed");
    let plan = emit_create_workspace_plan(action);
    let mut journal = Vec::new();
    execute_create_workspace_plan(
        &plan,
        &mut journal,
        &mut fx.ctx(&mut docs, git_worktree_caps()),
    )
    .expect("provision");
    docs.workspaces
        .get_mut("ws-failed")
        .expect("row")
        .lifecycle_state = "provisionFailed".to_string();

    let mut journal = Vec::new();
    let err = execute_create_workspace_plan(
        &plan,
        &mut journal,
        &mut fx.ctx(&mut docs, git_worktree_caps()),
    )
    .expect_err("provisionFailed must not become ready");
    assert!(err.to_string().contains("provisionFailed"), "{err}");
    let stored = docs.load_isolated_workspace("ws-failed").unwrap().unwrap();
    assert_eq!(stored.lifecycle_state, "provisionFailed");
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

fn seal_writer(
    fx: &Fixture,
    docs: &mut MemoryWorkspaceDocuments,
    workspace_id: &str,
    request_id: &str,
) -> SealWorkspaceOutcome {
    let plan = emit_seal_workspace_plan(SealWorkspaceAction {
        workspace_id: workspace_id.to_string(),
        produced_by_request_id: request_id.to_string(),
        produced_by_request_doc_id: format!("{request_id}-doc"),
    });
    let mut journal = Vec::new();
    execute_seal_workspace_plan(&plan, &mut journal, &mut fx.ctx(docs, seal_caps())).expect("seal")
}

#[test]
fn writer_seal_persists_receipt_and_forbids_read_write() {
    let fx = Fixture::new();
    let mut docs = MemoryWorkspaceDocuments::default();
    let action = fx.action("ws-seal", "unit-1", "topic-seal");
    let plan = emit_create_workspace_plan(action);
    let mut journal = Vec::new();
    let created = execute_create_workspace_plan(
        &plan,
        &mut journal,
        &mut fx.ctx(&mut docs, git_worktree_caps()),
    )
    .expect("provision");
    let dest = PathBuf::from(&created.placement.host_path);
    fs::write(dest.join("patch.rs"), "fn patch() {}\n").unwrap();

    let mut writer = super::binding::new_binding(
        "ws-seal",
        "req-writer",
        "req-writer-doc",
        crate::toolset::WorkspaceAuthority::ReadWrite,
        "deploy-1",
        None,
    );
    docs.write_binding(writer.clone()).unwrap();

    let sealed = seal_writer(&fx, &mut docs, "ws-seal", "req-writer");
    assert_eq!(sealed.workspace.lifecycle_state, "sealed");
    assert!(sealed.workspace.seal_hash.is_some());
    assert_eq!(
        sealed.placement.observed_tree_hash,
        sealed.workspace.seal_hash.clone().unwrap()
    );
    assert_eq!(sealed.receipt.kind, "writer");
    assert_eq!(sealed.receipt.produced_by_request_id, "req-writer");
    assert!(sealed
        .receipt
        .changed_files
        .as_deref()
        .is_some_and(|files| files.contains("patch.rs")));
    writer = docs
        .load_bindings("ws-seal")
        .unwrap()
        .into_iter()
        .find(|binding| binding.request_id == "req-writer")
        .unwrap();
    assert_eq!(writer.lifecycle_state, "released");

    let err = super::binding::admit_workspace_binding(
        "ws-seal",
        &sealed.workspace.lifecycle_state,
        sealed.workspace.seal_hash.as_deref(),
        &docs.load_bindings("ws-seal").unwrap(),
        super::binding::new_binding(
            "ws-seal",
            "req-writer-2",
            "req-writer-2-doc",
            crate::toolset::WorkspaceAuthority::ReadWrite,
            "deploy-1",
            sealed.workspace.seal_hash.as_deref(),
        ),
        false,
    )
    .unwrap_err();
    assert!(err.to_string().contains("not bindable"), "{err:#}");
}

#[test]
fn concurrent_read_only_after_seal_with_matching_hash() {
    let fx = Fixture::new();
    let mut docs = MemoryWorkspaceDocuments::default();
    let action = fx.action("ws-ro", "unit-1", "topic-ro");
    let plan = emit_create_workspace_plan(action);
    let mut journal = Vec::new();
    execute_create_workspace_plan(
        &plan,
        &mut journal,
        &mut fx.ctx(&mut docs, git_worktree_caps()),
    )
    .expect("provision");
    let sealed = seal_writer(&fx, &mut docs, "ws-ro", "req-writer");
    let hash = sealed.workspace.seal_hash.clone().unwrap();

    let first = super::binding::admit_workspace_binding(
        "ws-ro",
        "sealed",
        Some(&hash),
        &[],
        super::binding::new_binding(
            "ws-ro",
            "req-review-a",
            "doc-a",
            crate::toolset::WorkspaceAuthority::ReadOnly,
            "deploy-1",
            Some(&hash),
        ),
        false,
    )
    .unwrap();
    let super::binding::AdmitBinding::Create { binding: a, .. } = first else {
        panic!("expected create");
    };
    let second = super::binding::admit_workspace_binding(
        "ws-ro",
        "sealed",
        Some(&hash),
        std::slice::from_ref(&a),
        super::binding::new_binding(
            "ws-ro",
            "req-review-b",
            "doc-b",
            crate::toolset::WorkspaceAuthority::ReadOnly,
            "deploy-1",
            Some(&hash),
        ),
        false,
    )
    .unwrap();
    let super::binding::AdmitBinding::Create { binding: b, .. } = second else {
        panic!("expected concurrent create");
    };
    assert_eq!(a.seal_hash.as_deref(), Some(hash.as_str()));
    assert_eq!(b.seal_hash.as_deref(), Some(hash.as_str()));
    assert!(a.is_active());
    assert!(b.is_active());
}

#[test]
fn seal_drift_fails_closed() {
    let fx = Fixture::new();
    let mut docs = MemoryWorkspaceDocuments::default();
    let action = fx.action("ws-drift", "unit-1", "topic-drift");
    let plan = emit_create_workspace_plan(action);
    let mut journal = Vec::new();
    let created = execute_create_workspace_plan(
        &plan,
        &mut journal,
        &mut fx.ctx(&mut docs, git_worktree_caps()),
    )
    .expect("provision");
    let dest = PathBuf::from(&created.placement.host_path);
    let sealed = seal_writer(&fx, &mut docs, "ws-drift", "req-writer");
    let hash = sealed.workspace.seal_hash.clone().unwrap();
    fs::write(dest.join("after-seal.txt"), "mutated after review\n").unwrap();
    let live = super::adapter::working_tree_hash(&dest).unwrap();
    assert_ne!(live, hash);

    let mut workspace = super::IsolatedWorkspaceRecord {
        workspace_id: "ws-drift".into(),
        owner_deployment_id: "deploy-1".into(),
        lifecycle_state: "sealed".into(),
        seal_hash: Some(hash.clone()),
        instruction_manifest: sealed.workspace.instruction_manifest.clone(),
    };
    let mut placed = super::WorkspacePlacementRecord {
        workspace_id: "ws-drift".into(),
        deployment_id: "deploy-1".into(),
        host_path: dest.to_string_lossy().into_owned(),
        observed_tree_hash: Some(hash.clone()),
    };
    let error = bind_workspace_overlay(
        &workspace,
        &placed,
        WorkspaceBindInput {
            workspace_id: "ws-drift",
            authority: crate::toolset::WorkspaceAuthority::ReadOnly,
            owner_deployment_id: "deploy-1",
            seal_hash: Some(&hash),
            request_cwd: None,
            local_deployment_id: "deploy-1",
            operator_tool_root: Some(fx.parent()),
            enabled_workspace_roots: &[],
            workspace_write_sandbox_enforced: false,
            live_tree_hash: Some(&live),
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("live tree hash"), "{error:#}");
    workspace.seal_hash = Some(hash.clone());
    placed.observed_tree_hash = Some("stale".into());
    let stored = bind_workspace_overlay(
        &workspace,
        &placed,
        WorkspaceBindInput {
            workspace_id: "ws-drift",
            authority: crate::toolset::WorkspaceAuthority::ReadOnly,
            owner_deployment_id: "deploy-1",
            seal_hash: Some(&hash),
            request_cwd: None,
            local_deployment_id: "deploy-1",
            operator_tool_root: Some(fx.parent()),
            enabled_workspace_roots: &[],
            workspace_write_sandbox_enforced: false,
            live_tree_hash: Some(&hash),
        },
    )
    .unwrap_err();
    assert!(
        stored
            .to_string()
            .contains("observed_tree_hash stale does not match"),
        "{stored:#}"
    );
}

#[test]
fn frozen_agents_md_is_used_instead_of_live_writer_tree() {
    let mut fx = Fixture::new();
    fx.commit("AGENTS.md", "frozen-base-instructions\n");
    let mut docs = MemoryWorkspaceDocuments::default();
    let action = fx.action("ws-agents", "unit-1", "topic-agents");
    let plan = emit_create_workspace_plan(action);
    let mut journal = Vec::new();
    let created = execute_create_workspace_plan(
        &plan,
        &mut journal,
        &mut fx.ctx(&mut docs, git_worktree_caps()),
    )
    .expect("provision");
    let dest = PathBuf::from(&created.placement.host_path);
    fs::write(dest.join("AGENTS.md"), "live-writer-instructions\n").unwrap();

    let manifest = InstructionManifest::parse(&created.workspace.instruction_manifest)
        .expect("instruction_manifest");
    assert_eq!(manifest.base_sha, fx.base_sha);
    let agents = manifest
        .files
        .iter()
        .find(|file| file.path == "AGENTS.md")
        .expect("AGENTS.md from base_sha");
    assert!(agents.text.contains("frozen-base-instructions"));
    assert!(!agents.text.contains("live-writer-instructions"));

    let section = instruction_context_section(&created.workspace.instruction_manifest).unwrap();
    assert!(section.contains("frozen-base-instructions"));
    assert!(!section.contains("live-writer-instructions"));
    let live = fs::read_to_string(dest.join("AGENTS.md")).unwrap();
    assert!(live.contains("live-writer-instructions"));

    let sealed = seal_writer(&fx, &mut docs, "ws-agents", "req-writer");
    let sealed_manifest = InstructionManifest::parse(&sealed.workspace.instruction_manifest)
        .expect("sealed manifest");
    let sealed_agents = sealed_manifest
        .files
        .iter()
        .find(|file| file.path == "AGENTS.md")
        .expect("AGENTS.md still frozen");
    assert!(sealed_agents.text.contains("frozen-base-instructions"));
    assert!(!sealed_agents.text.contains("live-writer-instructions"));
}

#[test]
fn already_sealed_repairs_placement_hash_and_receipt() {
    let fx = Fixture::new();
    let mut docs = MemoryWorkspaceDocuments::default();
    let action = fx.action("ws-repair", "unit-1", "topic-repair");
    let plan = emit_create_workspace_plan(action);
    let mut journal = Vec::new();
    let created = execute_create_workspace_plan(
        &plan,
        &mut journal,
        &mut fx.ctx(&mut docs, git_worktree_caps()),
    )
    .expect("provision");
    let dest = PathBuf::from(&created.placement.host_path);
    fs::write(dest.join("patch.rs"), "fn patch() {}\n").unwrap();
    let sealed = seal_writer(&fx, &mut docs, "ws-repair", "req-writer");
    let hash = sealed.workspace.seal_hash.clone().unwrap();
    docs.placements
        .get_mut("ws-repair")
        .unwrap()
        .observed_tree_hash = created.placement.observed_tree_hash.clone();
    docs.receipts.clear();

    let repaired = seal_writer(&fx, &mut docs, "ws-repair", "req-writer");
    assert_eq!(repaired.workspace.lifecycle_state, "sealed");
    assert_eq!(repaired.placement.observed_tree_hash, hash);
    assert_eq!(repaired.receipt.produced_by_request_id, "req-writer");
    assert_eq!(repaired.receipt.seal_hash, hash);
}

#[test]
fn instruction_manifest_fails_closed_on_git_show_error() {
    let fx = Fixture::new();
    let err = super::adapter::capture_instruction_manifest(&fx.repo, "not-a-commit")
        .expect_err("invalid base_sha must not become empty files");
    assert!(err.to_string().contains("git show"), "{err:#}");
}

#[test]
fn seal_workspace_plan_omits_host_path() {
    let plan = emit_seal_workspace_plan(SealWorkspaceAction {
        workspace_id: "ws-1".into(),
        produced_by_request_id: "req-1".into(),
        produced_by_request_doc_id: "doc-1".into(),
    });
    let json = serde_json::to_value(&plan).unwrap();
    assert_eq!(json["actions"][0]["type"], "seal_workspace");
    assert!(json["actions"][0].get("host_path").is_none());
    assert!(serde_json::from_value::<HostAction>(serde_json::json!({
        "type": "seal_workspace",
        "workspace_id": "ws-1",
        "produced_by_request_id": "req-1",
        "produced_by_request_doc_id": "doc-1",
        "host_path": "/tmp/evil"
    }))
    .is_err());
}
