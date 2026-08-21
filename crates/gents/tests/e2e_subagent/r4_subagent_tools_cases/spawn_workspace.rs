use std::path::{Path, PathBuf};
use std::process::Command;

use super::*;
use gents::workspace::{isolated_workspace_upsert_mutation, IsolatedWorkspaceDoc};
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
    lifecycle_state: Option<String>,
    repository_id: Option<String>,
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
            }}
        }}"#
    );
    first_row(&node.execute(&query).await, "IsolatedWorkspace")
}

async fn spawn_background_child(
    fixture: &SpawnFixture,
    tool_call_id: &str,
    workspace: Option<Value>,
) -> ChildWorkspaceRow {
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
    let result = skip_reason_json(action);
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

#[tokio::test]
async fn spawn_subagent_inherit_uses_parent_authority_infimum() {
    let workspace_id = "ws-inherit-infimum";
    let owner = "deploy-inherit";
    let extra = format!(
        r#", workspace_id: "{id}"
                , workspace_authority: "readOnly"
                , workspace_owner_deployment_id: "{owner}""#,
        id = escape_graphql_string(workspace_id),
        owner = escape_graphql_string(owner),
    );
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
    seed_isolated_workspace(
        fixture.db.node.as_ref(),
        workspace_id,
        owner,
        "ready",
        None,
        "repo-inherit",
        "abc123",
        "topic",
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
}

#[tokio::test]
async fn spawn_subagent_bind_id_stamps_existing_workspace() {
    let workspace_id = "ws-bind-ready";
    let owner = "deploy-bind";
    let fixture = setup_spawn_fixture("spawn_ws_bind", vec![CHILD_BEHAVIOR_ID], 0, true).await;
    seed_host_deployment(fixture.db.node.as_ref(), owner).await;
    seed_isolated_workspace(
        fixture.db.node.as_ref(),
        workspace_id,
        owner,
        "ready",
        None,
        "repo-bind",
        "abc123",
        "topic",
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
}

#[tokio::test]
async fn spawn_subagent_provision_creates_isolated_workspace() {
    let parent_workspace_id = "ws-provision-parent";
    let owner = "deploy-provision";
    let (root, repo, sha) = init_git_repo();
    let extra = format!(
        r#", workspace_id: "{id}"
                , workspace_authority: "readWrite"
                , workspace_owner_deployment_id: "{owner}""#,
        id = escape_graphql_string(parent_workspace_id),
        owner = escape_graphql_string(owner),
    );
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
    seed_host_deployment(fixture.db.node.as_ref(), owner).await;
    seed_isolated_workspace(
        fixture.db.node.as_ref(),
        parent_workspace_id,
        owner,
        "ready",
        None,
        "repo-provision",
        &sha,
        "topic",
    )
    .await;
    seed_repository_placement(fixture.db.node.as_ref(), "repo-provision", owner, &repo).await;

    let child = spawn_background_child(
        &fixture,
        "internal-spawn-provision",
        Some(json!({ "provision": { "policy": "git_worktree_diff" } })),
    )
    .await;
    let child_workspace_id = child
        .workspace_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .expect("provision must stamp workspace_id");
    assert_ne!(child_workspace_id, parent_workspace_id);
    assert_eq!(child.workspace_authority.as_deref(), Some("readWrite"));
    assert_eq!(child.workspace_owner_deployment_id.as_deref(), Some(owner));

    let created = fetch_isolated_workspace(fixture.db.node.as_ref(), child_workspace_id).await;
    assert_eq!(created.workspace_id, child_workspace_id);
    assert_eq!(created.lifecycle_state.as_deref(), Some("ready"));
    assert_eq!(created.repository_id.as_deref(), Some("repo-provision"));
    let _keep = root;
}
