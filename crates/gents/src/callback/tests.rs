use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use serde_json::json;
use tempfile::TempDir;

use crate::ensure_runtime_schemas;
use crate::watcher::workspace_bound_request_claimable;
use crate::workspace::{
    action_journal_prefix_legal, emit_create_workspace_plan, execute_create_workspace_plan,
    ActionJournalEntry, ActionJournalState, CreateWorkspaceAction, CreationPolicy,
    HostExecutorContext, MemoryWorkspaceDocuments, RepositoryPlacementRef, WorkspaceAdapterKind,
    WorkspaceDocuments, CAP_CREATE_WORKSPACE, CAP_OBSERVE_DIRTY_BASE,
};

use super::claim::invocation_is_claimable;
use super::documents::{
    strip_secret_fields, validate_callback_binding, CallbackBindingDoc, CallbackInvocationDoc,
};
use super::host::ensure_local_host_deployment;
use super::run::{
    can_emit_callback_result, can_start_executing, emit_plan_from_source, resolve_action_plan,
};
use super::{BUILTIN_CREATE_WORKSPACE, LIFECYCLE_PENDING, LIFECYCLE_RUNNING, LIFECYCLE_SUCCEEDED};

fn binding() -> CallbackBindingDoc {
    CallbackBindingDoc {
        binding_id: "bind-1".into(),
        source_collection: "WorkUnit".into(),
        event_kind: "created".into(),
        filter: None,
        source_fields: Some(r#"["work_unit_id","repository_id","base_sha","branch"]"#.into()),
        module_id: None,
        builtin_emitter: Some(BUILTIN_CREATE_WORKSPACE.into()),
        principal_did: "did:key:zWriter".into(),
        capability_set: Some(
            r#"["create_workspace","observe_dirty_base","clone_artifacts"]"#.into(),
        ),
        retry_policy: None,
        owner_deployment_id: "deploy-1".into(),
        enabled: Some(true),
    }
}

#[test]
fn journal_prefix_blocks_action_n_plus_one_until_result_docs_written() {
    let illegal = vec![
        ActionJournalEntry::new(0, ActionJournalState::Validated),
        ActionJournalEntry::new(1, ActionJournalState::Executing),
    ];
    assert!(!action_journal_prefix_legal(&illegal));
    assert!(!can_start_executing(&illegal, 1));

    let legal = vec![
        ActionJournalEntry::new(0, ActionJournalState::ResultDocsWritten),
        ActionJournalEntry::new(1, ActionJournalState::Executing),
    ];
    assert!(action_journal_prefix_legal(&legal));
    assert!(can_start_executing(&legal, 1));
    assert!(can_start_executing(&[], 0));
}

#[test]
fn callback_result_requires_succeeded_complete_journal_and_docs() {
    let journal = vec![ActionJournalEntry::new(
        0,
        ActionJournalState::ResultDocsWritten,
    )];
    let workspace = crate::workspace::IsolatedWorkspaceDoc {
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
    let placement = crate::workspace::WorkspacePlacementDoc {
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
    assert!(can_emit_callback_result(
        LIFECYCLE_RUNNING,
        &journal,
        Some(&workspace),
        Some(&placement)
    ));
    assert!(!can_emit_callback_result(
        LIFECYCLE_SUCCEEDED,
        &[],
        Some(&workspace),
        Some(&placement)
    ));
    assert!(!can_emit_callback_result(
        LIFECYCLE_SUCCEEDED,
        &journal,
        None,
        Some(&placement)
    ));
    assert!(!can_emit_callback_result(
        LIFECYCLE_SUCCEEDED,
        &journal,
        Some(&workspace),
        None
    ));
    assert!(can_emit_callback_result(
        LIFECYCLE_SUCCEEDED,
        &journal,
        Some(&workspace),
        Some(&placement)
    ));
}

#[test]
fn non_owner_does_not_claim_invocation_or_workspace_request() {
    let invocation = CallbackInvocationDoc {
        invocation_id: "inv-1".into(),
        owner_deployment_id: "deploy-owner".into(),
        binding_id: "bind-1".into(),
        source_collection: "WorkUnit".into(),
        source_doc_id: "doc-1".into(),
        source_version: Some("created".into()),
        idempotency_key: "bind-1:doc-1:created".into(),
        lifecycle_state: LIFECYCLE_PENDING.into(),
        attempts: Some(0),
        action_plan: None,
        action_journal: None,
        error: None,
        claimed_at: None,
        created_at: None,
    };
    assert!(!invocation_is_claimable("deploy-replica", &invocation));
    assert!(invocation_is_claimable("deploy-owner", &invocation));

    assert!(workspace_bound_request_claimable(
        Some("deploy-owner"),
        None,
        None
    ));
    assert!(workspace_bound_request_claimable(
        Some("deploy-owner"),
        Some("ws-1"),
        Some("deploy-owner")
    ));
    assert!(!workspace_bound_request_claimable(
        Some("deploy-replica"),
        Some("ws-1"),
        Some("deploy-owner")
    ));
    assert!(!workspace_bound_request_claimable(
        None,
        Some("ws-1"),
        Some("deploy-owner")
    ));
}

#[test]
fn apply_rejects_secret_bearing_source_fields() {
    let mut secret = binding();
    secret.source_fields = Some(r#"["work_unit_id","api_token"]"#.into());
    assert!(validate_callback_binding(&secret).is_err());
    assert!(validate_callback_binding(&binding()).is_ok());
    let stripped = strip_secret_fields(json!({"branch": "topic", "api_token": "secret"}));
    assert_eq!(stripped["branch"], "topic");
    assert!(stripped.get("api_token").is_none());
}

#[test]
fn apply_rejects_secret_bearing_filter_fields() {
    let mut secret = binding();
    secret.filter = Some(r#"{ api_token: { _eq: "x" } }"#.into());
    let error = validate_callback_binding(&secret).unwrap_err().to_string();
    assert!(error.contains("secret-bearing"), "{error}");
    let mut literal = binding();
    literal.filter = Some(r#"{ work_unit_id: { _eq: "TOKEN" } }"#.into());
    assert!(validate_callback_binding(&literal).is_ok());
}

#[test]
fn apply_rejects_wasm_only_bindings() {
    let mut wasm = binding();
    wasm.builtin_emitter = None;
    wasm.module_id = Some("mod-1".into());
    let error = validate_callback_binding(&wasm).unwrap_err().to_string();
    assert!(error.contains("WASM planner is not implemented"), "{error}");
}

#[test]
fn recovery_reuses_stored_action_plan() {
    let source = json!({
        "work_unit_id": "unit-1",
        "repository_id": "repo-1",
        "base_sha": "abc",
        "branch": "topic",
        "workspace_id": "ws-stored"
    });
    let plan = emit_plan_from_source(&binding(), &source).unwrap();
    let mut invocation = CallbackInvocationDoc {
        invocation_id: "inv-1".into(),
        owner_deployment_id: "deploy-1".into(),
        binding_id: "bind-1".into(),
        source_collection: "WorkUnit".into(),
        source_doc_id: "doc-1".into(),
        source_version: Some("created".into()),
        idempotency_key: "bind-1:doc-1:created".into(),
        lifecycle_state: LIFECYCLE_RUNNING.into(),
        attempts: Some(1),
        action_plan: Some(serde_json::to_string(&plan).unwrap()),
        action_journal: Some(
            serde_json::to_string(&[ActionJournalEntry::new(0, ActionJournalState::Executing)])
                .unwrap(),
        ),
        error: None,
        claimed_at: None,
        created_at: None,
    };
    let mutated = json!({
        "work_unit_id": "unit-OTHER",
        "repository_id": "repo-1",
        "base_sha": "abc",
        "branch": "topic-OTHER",
        "workspace_id": "ws-mutated"
    });
    let resolved = resolve_action_plan(&invocation, &binding(), &mutated).unwrap();
    match &resolved.actions[0] {
        crate::workspace::HostAction::CreateWorkspace(action) => {
            assert_eq!(action.workspace_id, "ws-stored");
            assert_eq!(action.branch, "topic");
        }
    }
    invocation.action_plan = None;
    let missing = resolve_action_plan(&invocation, &binding(), &mutated).unwrap_err();
    assert!(missing.contains("missing stored ActionPlan"), "{missing}");
}

#[test]
fn builtin_emitter_builds_create_workspace_plan() {
    let source = json!({
        "work_unit_id": "unit-1",
        "repository_id": "repo-1",
        "base_sha": "abc",
        "branch": "topic",
        "workspace_id": "ws-1"
    });
    let plan = emit_plan_from_source(&binding(), &source).expect("plan");
    let encoded = serde_json::to_string(&plan).unwrap();
    assert!(!encoded.contains("host_path"));
    assert_eq!(plan.abi, 1);
    match &plan.actions[0] {
        crate::workspace::HostAction::CreateWorkspace(action) => {
            assert_eq!(action.workspace_id, "ws-1");
            assert_eq!(action.work_unit_id, "unit-1");
        }
    }
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
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

struct GitFixture {
    _root: TempDir,
    repo: PathBuf,
    base_sha: String,
}

impl GitFixture {
    fn new() -> Self {
        let root = TempDir::new().expect("tempdir");
        let repo = root.path().join("repo");
        fs::create_dir_all(&repo).unwrap();
        git(&repo, &["init", "-b", "main"]);
        git(&repo, &["config", "user.email", "cb@example.com"]);
        git(&repo, &["config", "user.name", "Callback Test"]);
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
}

#[test]
fn callback_result_only_after_workspace_docs_are_durable() {
    let fx = GitFixture::new();
    let mut docs = MemoryWorkspaceDocuments::default();
    let action = CreateWorkspaceAction {
        workspace_id: "ws-result".into(),
        work_unit_id: "unit-1".into(),
        repository_id: "repo-1".into(),
        base_sha: fx.base_sha.clone(),
        branch: "topic-result".into(),
        creation_policy: CreationPolicy::GitWorktreeDiff,
        adapter: WorkspaceAdapterKind::GitWorktree,
        clone_artifacts: None,
    };
    let plan = emit_create_workspace_plan(action);
    let caps: BTreeSet<String> = [CAP_CREATE_WORKSPACE, CAP_OBSERVE_DIRTY_BASE]
        .into_iter()
        .map(str::to_string)
        .collect();
    let mut journal = Vec::new();
    assert!(!can_emit_callback_result(
        LIFECYCLE_RUNNING,
        &journal,
        docs.load_isolated_workspace("ws-result").unwrap().as_ref(),
        docs.load_placement("ws-result").unwrap().as_ref(),
    ));

    let mut ctx = HostExecutorContext {
        deployment_id: "deploy-1".into(),
        repository: RepositoryPlacementRef {
            repository_id: "repo-1".into(),
            deployment_id: "deploy-1".into(),
            host_path: fx.repo.clone(),
            enabled: true,
        },
        ceiling: fx.repo.parent(),
        capabilities: caps,
        writer_principal: "did:key:zW".into(),
        integrator_principal: "did:key:zI".into(),
        caused_by_invocation_id: "inv-1".into(),
        caused_by_correlation: "corr-1".into(),
        documents: &mut docs,
    };
    let outcome = execute_create_workspace_plan(&plan, &mut journal, &mut ctx).expect("provision");
    assert_eq!(
        journal.last().map(|entry| entry.state),
        Some(ActionJournalState::ResultDocsWritten)
    );
    assert!(can_emit_callback_result(
        LIFECYCLE_RUNNING,
        &journal,
        Some(&outcome.workspace),
        Some(&outcome.placement)
    ));
    assert!(can_emit_callback_result(
        LIFECYCLE_SUCCEEDED,
        &journal,
        Some(&outcome.workspace),
        Some(&outcome.placement)
    ));
}

async fn test_node() -> Arc<defra_node::EmbeddedNode> {
    let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    node
}

#[tokio::test]
async fn host_deployment_is_stable_and_not_an_agent_did() {
    let node = test_node().await;
    let first = ensure_local_host_deployment(node.as_ref()).await.unwrap();
    let second = ensure_local_host_deployment(node.as_ref()).await.unwrap();
    assert_eq!(first, second);
    assert!(!first.starts_with("did:"));
}

#[tokio::test]
async fn first_seen_source_create_materializes_owner_invocation() {
    let node = test_node().await;
    let deployment_id = ensure_local_host_deployment(node.as_ref()).await.unwrap();
    node.add_schema(
        r#"
        type WorkUnit {
            work_unit_id: String
            repository_id: String
            base_sha: String
            branch: String
        }
        "#,
    )
    .await
    .expect("WorkUnit schema");

    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let binding_mutation = format!(
        r#"mutation {{
            create_CallbackBinding(input: {{
                binding_id: "bind-scan",
                source_collection: "WorkUnit",
                event_kind: "created",
                filter: "",
                source_fields: "[\"work_unit_id\",\"repository_id\",\"base_sha\",\"branch\"]",
                module_id: "",
                builtin_emitter: "create_workspace",
                principal_did: "did:key:zWriter",
                capability_set: "[\"create_workspace\",\"observe_dirty_base\"]",
                retry_policy: "",
                owner_deployment_id: "{owner}",
                enabled: true,
                created_at: "{now}",
                updated_at: "{now}"
            }}) {{ _docID }}
        }}"#,
        owner = crate::graphql::escape_graphql_string(&deployment_id),
        now = crate::graphql::escape_graphql_string(&now),
    );
    let response = node.execute(&binding_mutation).await;
    assert!(!response.has_errors(), "{:?}", response.errors);

    let cancel = tokio_util::sync::CancellationToken::new();
    let mut engine = super::CallbackEngine::new(node.clone(), deployment_id.clone(), None, cancel);
    engine.reconcile_bindings().await;

    let create = r#"mutation {
        create_WorkUnit(input: {
            work_unit_id: "unit-scan",
            repository_id: "repo-1",
            base_sha: "abc",
            branch: "topic"
        }) { _docID }
    }"#;
    let created = node.execute(create).await;
    assert!(!created.has_errors(), "{:?}", created.errors);
    let doc_id = crate::graphql::single_mutation_document(&created, "create_WorkUnit")
        .unwrap()
        .and_then(|row| row.get("_docID"))
        .and_then(serde_json::Value::as_str)
        .expect("WorkUnit doc id")
        .to_string();

    engine.handle_created_doc("WorkUnit", &doc_id).await;

    let query = format!(
        r#"{{
            CallbackInvocation(
                filter: {{ source_doc_id: {{ _eq: "{id}" }} }},
                limit: 1
            ) {{
                invocation_id
                owner_deployment_id
                lifecycle_state
                idempotency_key
            }}
        }}"#,
        id = crate::graphql::escape_graphql_string(&doc_id),
    );
    let response = node.execute(&query).await;
    assert!(!response.has_errors(), "{:?}", response.errors);
    let rows = response
        .data
        .as_ref()
        .and_then(|data| data.get("CallbackInvocation"))
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["owner_deployment_id"], deployment_id);
}
