use super::{resolve_default_read_root, ToolContext, ToolError};
use crate::tool_call_lifecycle::FailureClass;
use crate::toolset::{CommandExecutionMode, CommandNetworkMode, CommandPolicyDenial, DenialReason};

#[test]
fn policy_denial_reaches_dispatch_as_typed_failure() {
    let denial = CommandPolicyDenial::new(
        DenialReason::DisabledNetworkUnenforceable,
        CommandExecutionMode::ReadOnly,
        CommandNetworkMode::Disabled,
    );
    let expected = denial.tool_error_payload();

    match ToolError::policy_denial(denial).into_dispatch_error() {
        crate::llm::tool::ToolError::ReportedFailure { class, text } => {
            assert_eq!(class, FailureClass::PolicyDenied);
            assert_eq!(text, expected);
        }
        other => panic!("expected typed policy denial, got {other:?}"),
    }
}

#[test]
fn default_read_root_prefers_current_dir() {
    let cwd = std::env::temp_dir().join("gents-cwd-root");
    let home = std::env::temp_dir().join("gents-home-root");
    let resolved = resolve_default_read_root(Some(cwd.clone()), Some(home)).unwrap();
    assert_eq!(resolved, cwd);
}

#[test]
fn default_read_root_falls_back_to_home() {
    let home = std::env::temp_dir().join("gents-home-root");
    let resolved = resolve_default_read_root(None, Some(home.clone())).unwrap();
    assert_eq!(resolved, home);
}

#[test]
fn default_read_root_errors_when_unavailable() {
    assert!(resolve_default_read_root(None, None).is_err());
}

#[test]
fn relative_paths_resolve_from_base_inside_root() {
    let root =
        std::env::temp_dir().join(format!("gents-tool-context-root-{}", uuid::Uuid::new_v4()));
    let base = root.join("workspace");
    std::fs::create_dir_all(&base).unwrap();
    std::fs::write(base.join("README.md"), "ok").unwrap();

    let context = ToolContext::new_with_base(root.clone(), Some(base.clone()), false).unwrap();
    let resolved = context.resolve_existing_file("README.md").unwrap();

    assert_eq!(
        resolved,
        std::fs::canonicalize(base.join("README.md")).unwrap()
    );
    assert_eq!(context.display_path(&resolved), "README.md");

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn base_outside_root_falls_back_to_root() {
    let root =
        std::env::temp_dir().join(format!("gents-tool-context-root-{}", uuid::Uuid::new_v4()));
    let outside = std::env::temp_dir().join(format!(
        "gents-tool-context-outside-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(root.join("README.md"), "ok").unwrap();

    let context = ToolContext::new_with_base(root.clone(), Some(outside.clone()), false).unwrap();
    let resolved = context.resolve_existing_file("README.md").unwrap();

    assert_eq!(
        resolved,
        std::fs::canonicalize(root.join("README.md")).unwrap()
    );

    let _ = std::fs::remove_dir_all(root);
    let _ = std::fs::remove_dir_all(outside);
}

#[cfg(unix)]
#[test]
fn allow_create_rejects_a_dangling_symlink_leaf() {
    use std::os::unix::fs::symlink;

    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let target = outside.path().join("not-created-yet");
    symlink(&target, root.path().join("notes.txt")).unwrap();
    let context = ToolContext::new(root.path().to_path_buf(), false).unwrap();

    let error = context
        .resolve_path_allow_create("notes.txt")
        .expect_err("a dangling symlink must not be treated as a creatable leaf");
    assert!(
        error.to_string().contains("canonicalizing path ancestor"),
        "{error:#}"
    );
}

#[tokio::test]
async fn request_runtime_workspace_overrides_static_base() {
    let root =
        std::env::temp_dir().join(format!("gents-tool-context-root-{}", uuid::Uuid::new_v4()));
    let workspace = root.join("repo");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("README.md"), "ok").unwrap();
    std::fs::write(root.join("README.md"), "wrong").unwrap();

    let context = ToolContext::new_with_base(root.clone(), Some(root.clone()), false).unwrap();
    let (resolved, display) =
        crate::tool_call_lifecycle::runtime::scope_request_tool_execution_with_workspace(
            None,
            tokio_util::sync::CancellationToken::new(),
            Some(workspace.clone()),
            async {
                let resolved = context.resolve_existing_file("README.md")?;
                let display = context.display_path(&resolved);
                anyhow::Ok((resolved, display))
            },
        )
        .await
        .unwrap();

    assert_eq!(
        resolved,
        std::fs::canonicalize(workspace.join("README.md")).unwrap()
    );
    assert_eq!(display, "README.md");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn request_runtime_workspace_root_replaces_static_root() {
    let baked =
        std::env::temp_dir().join(format!("gents-tool-context-baked-{}", uuid::Uuid::new_v4()));
    let placement = std::env::temp_dir().join(format!(
        "gents-tool-context-placement-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&baked).unwrap();
    std::fs::create_dir_all(&placement).unwrap();
    std::fs::write(baked.join("secret.txt"), "nope").unwrap();
    std::fs::write(placement.join("notes.txt"), "ok").unwrap();

    let context = ToolContext::new(baked.clone(), false).unwrap();
    let (inside, denied) =
        crate::tool_call_lifecycle::runtime::scope_request_tool_execution_with_workspace_overlay(
            None,
            tokio_util::sync::CancellationToken::new(),
            crate::tool_call_lifecycle::runtime::ToolWorkspaceScope {
                workspace_cwd: Some(placement.clone()),
                workspace_root: Some(std::fs::canonicalize(&placement).unwrap()),
                workspace_authority: Some(crate::toolset::WorkspaceAuthority::ReadWrite),
            },
            None,
            None,
            None,
            Default::default(),
            false,
            async {
                let inside = context.resolve_existing_file("notes.txt")?;
                let denied =
                    context.resolve_existing_file(baked.join("secret.txt").to_str().unwrap());
                anyhow::Ok((inside, denied.err().map(|error| error.to_string())))
            },
        )
        .await
        .unwrap();

    assert_eq!(
        inside,
        std::fs::canonicalize(placement.join("notes.txt")).unwrap()
    );
    assert!(
        denied.unwrap().contains("outside the allowed tool root"),
        "bound overlay must deny the baked behavior root"
    );

    let _ = std::fs::remove_dir_all(baked);
    let _ = std::fs::remove_dir_all(placement);
}
