use std::path::{Path, PathBuf};
use std::process::Command;

use super::*;
use gents::workspace::{
    isolated_workspace_upsert_mutation, workspace_placement_upsert_mutation, IsolatedWorkspaceDoc,
    WorkspacePlacementDoc,
};
use serde::Deserialize;
use tempfile::TempDir;

#[derive(Debug, Deserialize)]
struct ChildWorkspaceRow {
    #[allow(dead_code)]
    request_id: String,
    workspace_id: Option<String>,
    workspace_authority: Option<String>,
    workspace_owner_deployment_id: Option<String>,
    workspace_seal_hash: Option<String>,
}

#[derive(Debug, Deserialize)]
struct IsolatedWorkspaceRow {
    workspace_id: String,
    #[serde(default)]
    lifecycle_state: Option<String>,
    #[serde(default)]
    repository_id: Option<String>,
    #[serde(default)]
    branch: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WorkspacePlacementRow {
    workspace_id: String,
    host_path: Option<String>,
    deployment_id: Option<String>,
}

async fn seed_isolated_workspace(
    node: &EmbeddedNode,
    workspace_id: &str,
    owner_deployment_id: &str,
    lifecycle_state: &str,
    seal_hash: Option<&str>,
    repository_id: &str,
    base_sha: &str,
    branch: &str,
) {
    let doc = IsolatedWorkspaceDoc {
        workspace_id: workspace_id.to_string(),
        work_unit_id: format!("{workspace_id}-unit"),
        repository_id: repository_id.to_string(),
        base_sha: base_sha.to_string(),
        branch: branch.to_string(),
        creation_policy: "git_worktree_diff".to_string(),
        adapter: "git_worktree".to_string(),
        owner_deployment_id: owner_deployment_id.to_string(),
        writer_principal: "did:key:zWriter".to_string(),
        integrator_principal: "did:key:zIntegrator".to_string(),
        instruction_manifest: "{}".to_string(),
        seal_hash: seal_hash.map(str::to_string),
        lifecycle_state: lifecycle_state.to_string(),
        caused_by_invocation_id: format!("{workspace_id}-inv"),
        caused_by_correlation: format!("{workspace_id}-corr"),
    };
    let mutation = isolated_workspace_upsert_mutation(&doc);
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "upsert IsolatedWorkspace failed: {:?}",
        response.errors
    );
}

async fn seed_host_deployment(node: &EmbeddedNode, deployment_id: &str) {
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let mutation = format!(
        r#"mutation {{
            create_HostDeployment(input: {{
                deployment_id: "{id}",
                display_name: "local",
                created_at: "{now}",
                updated_at: "{now}"
            }}) {{ _docID }}
        }}"#,
        id = escape_graphql_string(deployment_id),
        now = escape_graphql_string(&now),
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create HostDeployment failed: {:?}",
        response.errors
    );
}

async fn seed_repository_placement(
    node: &EmbeddedNode,
    repository_id: &str,
    deployment_id: &str,
    host_path: &Path,
) {
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let mutation = format!(
        r#"mutation {{
            create_RepositoryPlacement(input: {{
                repository_id: "{repo}",
                deployment_id: "{deploy}",
                host_path: "{path}",
                enabled: true,
                updated_at: "{now}"
            }}) {{ _docID }}
        }}"#,
        repo = escape_graphql_string(repository_id),
        deploy = escape_graphql_string(deployment_id),
        path = escape_graphql_string(&host_path.to_string_lossy()),
        now = escape_graphql_string(&now),
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create RepositoryPlacement failed: {:?}",
        response.errors
    );
}

async fn seed_workspace_placement(
    node: &EmbeddedNode,
    workspace_id: &str,
    deployment_id: &str,
    host_path: &Path,
    repository_id: &str,
) {
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let doc = WorkspacePlacementDoc {
        workspace_id: workspace_id.to_string(),
        deployment_id: deployment_id.to_string(),
        host_path: host_path.to_string_lossy().into_owned(),
        repository_placement_id: repository_id.to_string(),
        adapter: "git_worktree".to_string(),
        adapter_version: "gents-workspace-adapter/1".to_string(),
        dirty_base: false,
        dirty_base_summary: String::new(),
        provisioning_state: "{}".to_string(),
        observed_tree_hash: String::new(),
    };
    let mutation = workspace_placement_upsert_mutation(&doc, &now);
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "upsert WorkspacePlacement failed: {:?}",
        response.errors
    );
}

async fn seed_workspace_root(node: &EmbeddedNode, root_path: &Path) {
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let mutation = format!(
        r#"mutation {{
            create_WorkspaceRoot(input: {{
                root_path: "{path}",
                display_name: "spawn-test",
                enabled: true,
                updated_at: "{now}"
            }}) {{ _docID }}
        }}"#,
        path = escape_graphql_string(&root_path.to_string_lossy()),
        now = escape_graphql_string(&now),
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create WorkspaceRoot failed: {:?}",
        response.errors
    );
}

async fn seed_local_workspace(
    node: &EmbeddedNode,
    workspace_id: &str,
    owner: &str,
    lifecycle_state: &str,
    seal_hash: Option<&str>,
    repository_id: &str,
    base_sha: &str,
    branch: &str,
    placement_path: &Path,
) {
    seed_host_deployment(node, owner).await;
    seed_isolated_workspace(
        node,
        workspace_id,
        owner,
        lifecycle_state,
        seal_hash,
        repository_id,
        base_sha,
        branch,
    )
    .await;
    seed_workspace_placement(node, workspace_id, owner, placement_path, repository_id).await;
}

async fn fetch_child_workspace(node: &EmbeddedNode, child_request_id: &str) -> ChildWorkspaceRow {
    let escaped = escape_graphql_string(child_request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped}" }} }},
                limit: 1
            ) {{
                request_id
                workspace_id
                workspace_authority
                workspace_owner_deployment_id
                workspace_seal_hash
            }}
        }}"#
    );
    first_row(&node.execute(&query).await, "AgentRequest")
}

async fn fetch_isolated_workspace(node: &EmbeddedNode, workspace_id: &str) -> IsolatedWorkspaceRow {
    let escaped = escape_graphql_string(workspace_id);
    let query = format!(
        r#"{{
            IsolatedWorkspace(
                filter: {{ workspace_id: {{ _eq: "{escaped}" }} }},
                limit: 1
            ) {{
                workspace_id
                lifecycle_state
                repository_id
                branch
            }}
        }}"#
    );
    first_row(&node.execute(&query).await, "IsolatedWorkspace")
}

async fn fetch_workspace_placement(
    node: &EmbeddedNode,
    workspace_id: &str,
) -> WorkspacePlacementRow {
    let escaped = escape_graphql_string(workspace_id);
    let query = format!(
        r#"{{
            WorkspacePlacement(
                filter: {{ workspace_id: {{ _eq: "{escaped}" }} }},
                limit: 1
            ) {{
                workspace_id
                host_path
                deployment_id
            }}
        }}"#
    );
    first_row(&node.execute(&query).await, "WorkspacePlacement")
}

async fn spawn_background_child_result(
    fixture: &SpawnFixture,
    tool_call_id: &str,
    workspace: Option<Value>,
) -> Value {
    let mut args = json!({
        "name": CHILD_BEHAVIOR_ID,
        "prompt": "workspace child prompt",
        "await_mode": "background",
    });
    if let Some(workspace) = workspace {
        args["workspace"] = workspace;
    }
    let args = args.to_string();
    let action = fixture
        .hook
        .on_tool_call("spawn_subagent", None, tool_call_id, &args)
        .await;
    skip_reason_json(action)
}

async fn spawn_background_child(
    fixture: &SpawnFixture,
    tool_call_id: &str,
    workspace: Option<Value>,
) -> ChildWorkspaceRow {
    let result = spawn_background_child_result(fixture, tool_call_id, workspace).await;
    assert_eq!(result["ok"], true, "{result}");
    let child = wait_for_child_request_for_tool(fixture.db.node.as_ref(), tool_call_id).await;
    fetch_child_workspace(fixture.db.node.as_ref(), &child.request_id).await
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

fn init_git_repo() -> (TempDir, PathBuf, String) {
    let root = TempDir::new().expect("tempdir");
    let repo = root.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "-b", "main"]);
    git(&repo, &["config", "user.email", "ws@example.com"]);
    git(&repo, &["config", "user.name", "Workspace Test"]);
    std::fs::write(repo.join("README.md"), "hello\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "-m", "init"]);
    let sha = git(&repo, &["rev-parse", "HEAD"]);
    (root, repo, sha)
}

fn placement_dir(label: &str) -> (TempDir, PathBuf) {
    let root = TempDir::new().expect("placement tempdir");
    let path = root.path().join(label);
    std::fs::create_dir_all(&path).unwrap();
    (root, path)
}

fn parent_workspace_fields(workspace_id: &str, owner: &str, authority: &str) -> String {
    format!(
        r#", workspace_id: "{id}"
                , workspace_authority: "{authority}"
                , workspace_owner_deployment_id: "{owner}""#,
        id = escape_graphql_string(workspace_id),
        authority = escape_graphql_string(authority),
        owner = escape_graphql_string(owner),
    )
}

#[tokio::test]
async fn spawn_subagent_inherit_uses_parent_authority_infimum() {
    let workspace_id = "ws-inherit-infimum";
    let owner = "deploy-inherit";
    let extra = parent_workspace_fields(workspace_id, owner, "readOnly");
    let fixture = setup_spawn_fixture_with_parent_fields(
        "spawn_ws_inherit",
        vec![CHILD_BEHAVIOR_ID],
        0,
        true,
        true,
        chrono::Utc::now() + chrono::Duration::minutes(5),
        &extra,
    )
    .await;
    let (placement_root, placement) = placement_dir("inherit");
    seed_local_workspace(
        fixture.db.node.as_ref(),
        workspace_id,
        owner,
        "ready",
        None,
        "repo-inherit",
        "abc123",
        "topic",
        &placement,
    )
    .await;

    let child =
        spawn_background_child(&fixture, "internal-spawn-inherit", Some(json!("inherit"))).await;
    assert_eq!(child.workspace_id.as_deref(), Some(workspace_id));
    assert_eq!(
        child.workspace_authority.as_deref(),
        Some("readOnly"),
        "inherit must infimum Ready/ReadWrite default with parent ReadOnly"
    );
    assert_eq!(child.workspace_owner_deployment_id.as_deref(), Some(owner));
    assert!(child
        .workspace_seal_hash
        .as_deref()
        .is_none_or(|value| value.is_empty()));

    let omitted = spawn_background_child(&fixture, "internal-spawn-inherit-default", None).await;
    assert_eq!(omitted.workspace_id.as_deref(), Some(workspace_id));
    assert_eq!(
        omitted.workspace_authority.as_deref(),
        Some("readOnly"),
        "omitted workspace must default to inherit with parent authority infimum"
    );
    let _keep = placement_root;
}

#[tokio::test]
async fn spawn_subagent_inherit_sealed_copies_seal_hash() {
    let workspace_id = "ws-inherit-sealed";
    let owner = "deploy-inherit-sealed";
    let extra = parent_workspace_fields(workspace_id, owner, "readWrite");
    let fixture = setup_spawn_fixture_with_parent_fields(
        "spawn_ws_inherit_sealed",
        vec![CHILD_BEHAVIOR_ID],
        0,
        true,
        true,
        chrono::Utc::now() + chrono::Duration::minutes(5),
        &extra,
    )
    .await;
    let (placement_root, placement) = placement_dir("inherit-sealed");
    seed_local_workspace(
        fixture.db.node.as_ref(),
        workspace_id,
        owner,
        "sealed",
        Some("seal-inherit"),
        "repo-inherit-sealed",
        "abc123",
        "topic",
        &placement,
    )
    .await;

    let child = spawn_background_child(
        &fixture,
        "internal-spawn-inherit-sealed",
        Some(json!("inherit")),
    )
    .await;
    assert_eq!(child.workspace_id.as_deref(), Some(workspace_id));
    assert_eq!(child.workspace_authority.as_deref(), Some("readOnly"));
    assert_eq!(child.workspace_seal_hash.as_deref(), Some("seal-inherit"));
    let _keep = placement_root;
}

#[tokio::test]
async fn spawn_subagent_bind_id_stamps_existing_workspace() {
    let workspace_id = "ws-bind-ready";
    let owner = "deploy-bind";
    let fixture = setup_spawn_fixture("spawn_ws_bind", vec![CHILD_BEHAVIOR_ID], 0, true).await;
    let (placement_root, placement) = placement_dir("bind");
    seed_local_workspace(
        fixture.db.node.as_ref(),
        workspace_id,
        owner,
        "ready",
        None,
        "repo-bind",
        "abc123",
        "topic",
        &placement,
    )
    .await;

    let child = spawn_background_child(
        &fixture,
        "internal-spawn-bind",
        Some(json!({ "id": workspace_id })),
    )
    .await;
    assert_eq!(child.workspace_id.as_deref(), Some(workspace_id));
    assert_eq!(child.workspace_authority.as_deref(), Some("readWrite"));
    assert_eq!(child.workspace_owner_deployment_id.as_deref(), Some(owner));
    let _keep = placement_root;
}

#[tokio::test]
async fn spawn_subagent_bind_id_infimums_parent_readonly() {
    let workspace_id = "ws-bind-readonly-parent";
    let owner = "deploy-bind-ro";
    let extra = parent_workspace_fields(workspace_id, owner, "readOnly");
    let fixture = setup_spawn_fixture_with_parent_fields(
        "spawn_ws_bind_ro",
        vec![CHILD_BEHAVIOR_ID],
        0,
        true,
        true,
        chrono::Utc::now() + chrono::Duration::minutes(5),
        &extra,
    )
    .await;
    let (placement_root, placement) = placement_dir("bind-ro");
    seed_local_workspace(
        fixture.db.node.as_ref(),
        workspace_id,
        owner,
        "ready",
        None,
        "repo-bind-ro",
        "abc123",
        "topic",
        &placement,
    )
    .await;

    let child = spawn_background_child(
        &fixture,
        "internal-spawn-bind-ro",
        Some(json!({ "id": workspace_id })),
    )
    .await;
    assert_eq!(child.workspace_id.as_deref(), Some(workspace_id));
    assert_eq!(
        child.workspace_authority.as_deref(),
        Some("readOnly"),
        "bind-id must infimum Ready/ReadWrite default with parent ReadOnly"
    );
    let _keep = placement_root;
}

#[tokio::test]
async fn spawn_subagent_bind_id_sealed_copies_seal_hash() {
    let workspace_id = "ws-bind-sealed";
    let owner = "deploy-bind-sealed";
    let fixture =
        setup_spawn_fixture("spawn_ws_bind_sealed", vec![CHILD_BEHAVIOR_ID], 0, true).await;
    let (placement_root, placement) = placement_dir("bind-sealed");
    seed_local_workspace(
        fixture.db.node.as_ref(),
        workspace_id,
        owner,
        "sealed",
        Some("seal-bind"),
        "repo-bind-sealed",
        "abc123",
        "topic",
        &placement,
    )
    .await;

    let child = spawn_background_child(
        &fixture,
        "internal-spawn-bind-sealed",
        Some(json!({ "id": workspace_id })),
    )
    .await;
    assert_eq!(child.workspace_id.as_deref(), Some(workspace_id));
    assert_eq!(child.workspace_authority.as_deref(), Some("readOnly"));
    assert_eq!(child.workspace_seal_hash.as_deref(), Some("seal-bind"));
    let _keep = placement_root;
}

#[tokio::test]
async fn spawn_subagent_provision_creates_isolated_workspace() {
    let parent_workspace_id = "ws-provision-parent";
    let owner = "deploy-provision";
    let (root, repo, sha) = init_git_repo();
    let parent_ws = root.path().join("parent-ws");
    git(
        &repo,
        &[
            "worktree",
            "add",
            "-b",
            "topic",
            "--",
            &parent_ws.to_string_lossy(),
            &sha,
        ],
    );
    let extra = parent_workspace_fields(parent_workspace_id, owner, "readWrite");
    let fixture = setup_spawn_fixture_with_parent_fields(
        "spawn_ws_provision",
        vec![CHILD_BEHAVIOR_ID],
        0,
        true,
        true,
        chrono::Utc::now() + chrono::Duration::minutes(5),
        &extra,
    )
    .await;
    seed_local_workspace(
        fixture.db.node.as_ref(),
        parent_workspace_id,
        owner,
        "ready",
        None,
        "repo-provision",
        &sha,
        "topic",
        &parent_ws,
    )
    .await;
    seed_repository_placement(fixture.db.node.as_ref(), "repo-provision", owner, &repo).await;
    seed_workspace_root(
        fixture.db.node.as_ref(),
        &std::fs::canonicalize(root.path()).unwrap(),
    )
    .await;

    let first = spawn_background_child(
        &fixture,
        "internal-spawn-provision",
        Some(json!({ "provision": { "policy": "git_worktree_diff" } })),
    )
    .await;
    let first_id = first
        .workspace_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .expect("provision must stamp workspace_id")
        .to_string();
    assert_ne!(first_id, parent_workspace_id);
    assert_eq!(first.workspace_authority.as_deref(), Some("readWrite"));
    assert_eq!(first.workspace_owner_deployment_id.as_deref(), Some(owner));

    let created = fetch_isolated_workspace(fixture.db.node.as_ref(), &first_id).await;
    assert_eq!(created.workspace_id, first_id);
    assert_eq!(created.lifecycle_state.as_deref(), Some("ready"));
    assert_eq!(created.repository_id.as_deref(), Some("repo-provision"));
    assert_ne!(created.branch.as_deref(), Some("topic"));
    assert!(
        created
            .branch
            .as_deref()
            .is_some_and(|branch| branch.contains("topic-ws-")),
        "child branch should be unique, got {:?}",
        created.branch
    );

    let placement = fetch_workspace_placement(fixture.db.node.as_ref(), &first_id).await;
    assert_eq!(placement.workspace_id, first_id);
    assert_eq!(placement.deployment_id.as_deref(), Some(owner));
    let host_path = placement
        .host_path
        .as_deref()
        .filter(|value| !value.is_empty())
        .expect("provision must persist WorkspacePlacement.host_path");
    let dest = PathBuf::from(host_path);
    assert!(dest.is_dir(), "placement dest missing: {}", dest.display());
    assert!(
        dest.starts_with(std::fs::canonicalize(root.path()).unwrap()),
        "placement {} must sit under operator WorkspaceRoot {}",
        dest.display(),
        root.path().display()
    );
    let listed = git(&repo, &["worktree", "list"]);
    assert!(
        listed.contains(&dest.to_string_lossy().into_owned())
            || listed.contains(&dest.canonicalize().unwrap().to_string_lossy().into_owned()),
        "git worktree list missing dest {dest:?}: {listed}"
    );

    let second = spawn_background_child(
        &fixture,
        "internal-spawn-provision-2",
        Some(json!({ "provision": { "policy": "git_worktree_diff" } })),
    )
    .await;
    let second_id = second
        .workspace_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .expect("second provision must stamp workspace_id")
        .to_string();
    assert_ne!(second_id, first_id);
    assert_ne!(second_id, parent_workspace_id);
    let second_created = fetch_isolated_workspace(fixture.db.node.as_ref(), &second_id).await;
    assert_ne!(second_created.branch.as_deref(), Some("topic"));
    assert_ne!(second_created.branch, created.branch);
    let second_placement = fetch_workspace_placement(fixture.db.node.as_ref(), &second_id).await;
    let second_dest = PathBuf::from(
        second_placement
            .host_path
            .as_deref()
            .expect("second placement host_path"),
    );
    assert!(second_dest.is_dir());
    let listed = git(&repo, &["worktree", "list"]);
    assert!(
        listed.contains(&second_dest.to_string_lossy().into_owned())
            || listed.contains(
                &second_dest
                    .canonicalize()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            ),
        "git worktree list missing second dest {second_dest:?}: {listed}"
    );
    let _keep = root;
}

#[tokio::test]
async fn spawn_subagent_provision_fails_closed_when_dest_escapes_operator_tool_root() {
    let parent_workspace_id = "ws-provision-ceiling";
    let owner = "deploy-provision-ceiling";
    let (root, repo, sha) = init_git_repo();
    let parent_ws = root.path().join("parent-ws");
    git(
        &repo,
        &[
            "worktree",
            "add",
            "-b",
            "topic",
            "--",
            &parent_ws.to_string_lossy(),
            &sha,
        ],
    );
    let extra = parent_workspace_fields(parent_workspace_id, owner, "readWrite");
    let mut fixture = setup_spawn_fixture_with_parent_fields(
        "spawn_ws_provision_ceiling",
        vec![CHILD_BEHAVIOR_ID],
        0,
        true,
        true,
        chrono::Utc::now() + chrono::Duration::minutes(5),
        &extra,
    )
    .await;
    seed_local_workspace(
        fixture.db.node.as_ref(),
        parent_workspace_id,
        owner,
        "ready",
        None,
        "repo-provision-ceiling",
        &sha,
        "topic",
        &parent_ws,
    )
    .await;
    seed_repository_placement(
        fixture.db.node.as_ref(),
        "repo-provision-ceiling",
        owner,
        &repo,
    )
    .await;
    seed_workspace_root(
        fixture.db.node.as_ref(),
        &std::fs::canonicalize(root.path()).unwrap(),
    )
    .await;
    fixture
        .hook
        .set_operator_tool_root(Some(std::fs::canonicalize(&repo).unwrap()));

    let tool_call_id = "internal-spawn-provision-ceiling";
    let result = spawn_background_child_result(
        &fixture,
        tool_call_id,
        Some(json!({ "provision": { "policy": "git_worktree_diff" } })),
    )
    .await;
    assert_eq!(result["ok"], false, "{result}");
    let message = result["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("ceiling") || message.contains("escapes") || message.contains("tool root"),
        "expected operator ceiling denial, got {result}"
    );

    let workspace_id = format!("spawn-ws-{tool_call_id}");
    let escaped = escape_graphql_string(&workspace_id);
    let query = format!(
        r#"{{
            IsolatedWorkspace(
                filter: {{ workspace_id: {{ _eq: "{escaped}" }} }},
                limit: 1
            ) {{ workspace_id }}
        }}"#
    );
    let created: Option<IsolatedWorkspaceRow> =
        first_optional_row(&fixture.db.node.execute(&query).await, "IsolatedWorkspace");
    assert!(
        created.is_none(),
        "provision must not persist IsolatedWorkspace after operator-ceiling denial: {created:?}"
    );
    let _keep = root;
}
