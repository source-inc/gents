use std::path::PathBuf;

use crate::tool_surface::FileToolMode;
use crate::toolset::{
    apply_workspace_authority, CommandExecutionMode, CommandExecutionPolicy, WorkspaceAuthority,
};

use super::{
    bind_workspace_overlay, workspace_authority_file_mode, IsolatedWorkspaceRecord,
    WorkspaceBindInput, WorkspacePlacementRecord,
};

fn temp_tree() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let root = tempfile::tempdir().unwrap();
    let placement = root.path().join("worktrees").join("ws-1");
    std::fs::create_dir_all(&placement).unwrap();
    let canonical_root = std::fs::canonicalize(root.path()).unwrap();
    let canonical_placement = std::fs::canonicalize(&placement).unwrap();
    (root, canonical_root, canonical_placement)
}

fn ready_workspace() -> IsolatedWorkspaceRecord {
    IsolatedWorkspaceRecord {
        workspace_id: "ws-1".into(),
        owner_deployment_id: "dep-1".into(),
        lifecycle_state: "ready".into(),
        seal_hash: None,
    }
}

fn placement(host_path: &std::path::Path) -> WorkspacePlacementRecord {
    WorkspacePlacementRecord {
        workspace_id: "ws-1".into(),
        deployment_id: "dep-1".into(),
        host_path: host_path.to_string_lossy().into_owned(),
        observed_tree_hash: None,
    }
}

fn bind_input<'a>(
    authority: WorkspaceAuthority,
    operator_tool_root: Option<&'a std::path::Path>,
    enabled_workspace_roots: &'a [PathBuf],
    enforced: bool,
) -> WorkspaceBindInput<'a> {
    WorkspaceBindInput {
        workspace_id: "ws-1",
        authority,
        owner_deployment_id: Some("dep-1"),
        seal_hash: None,
        request_cwd: None,
        local_deployment_id: Some("dep-1"),
        operator_tool_root,
        enabled_workspace_roots,
        workspace_write_sandbox_enforced: enforced,
    }
}

#[test]
fn read_write_meets_unrestricted_to_workspace_write() {
    let policy =
        CommandExecutionPolicy::write_capable().with_mode(CommandExecutionMode::Unrestricted);
    let met = apply_workspace_authority(&policy, WorkspaceAuthority::ReadWrite);
    assert_eq!(met.mode, CommandExecutionMode::WorkspaceWrite);
}

#[test]
fn read_write_never_meets_to_unrestricted() {
    for behavior in [
        CommandExecutionMode::ReadOnly,
        CommandExecutionMode::WorkspaceWrite,
        CommandExecutionMode::Unrestricted,
    ] {
        let policy = CommandExecutionPolicy::write_capable().with_mode(behavior);
        let met = apply_workspace_authority(&policy, WorkspaceAuthority::ReadWrite);
        assert_ne!(met.mode, CommandExecutionMode::Unrestricted);
        assert_eq!(
            met.mode,
            behavior.meet(CommandExecutionMode::WorkspaceWrite)
        );
    }
}

#[test]
fn read_only_and_integrate_meet_command_mode_to_read_only() {
    let policy =
        CommandExecutionPolicy::write_capable().with_mode(CommandExecutionMode::Unrestricted);
    assert_eq!(
        apply_workspace_authority(&policy, WorkspaceAuthority::ReadOnly).mode,
        CommandExecutionMode::ReadOnly
    );
    assert_eq!(
        apply_workspace_authority(&policy, WorkspaceAuthority::Integrate).mode,
        CommandExecutionMode::ReadOnly
    );
}

#[test]
fn authority_file_mode_matches_spec() {
    assert_eq!(
        workspace_authority_file_mode(WorkspaceAuthority::ReadWrite),
        FileToolMode::ReadWrite
    );
    assert_eq!(
        FileToolMode::ReadWrite.meet(workspace_authority_file_mode(WorkspaceAuthority::ReadOnly)),
        FileToolMode::ReadOnly
    );
    assert_eq!(
        FileToolMode::ReadWrite.meet(workspace_authority_file_mode(WorkspaceAuthority::Integrate)),
        FileToolMode::ReadOnly
    );
}

#[test]
fn read_write_binds_ready_placement_under_operator_root() {
    let (_guard, operator, placement_path) = temp_tree();
    let overlay = bind_workspace_overlay(
        &ready_workspace(),
        &placement(&placement_path),
        bind_input(WorkspaceAuthority::ReadWrite, Some(&operator), &[], true),
    )
    .unwrap();
    assert_eq!(overlay.root, placement_path);
    assert_eq!(overlay.cwd, placement_path);
    assert_eq!(overlay.authority, WorkspaceAuthority::ReadWrite);
}

#[test]
fn read_write_fails_closed_without_workspace_write_sandbox() {
    let (_guard, operator, placement_path) = temp_tree();
    let error = bind_workspace_overlay(
        &ready_workspace(),
        &placement(&placement_path),
        bind_input(WorkspaceAuthority::ReadWrite, Some(&operator), &[], false),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("enforceable WorkspaceWrite sandbox"),
        "{error:#}"
    );
}

#[test]
fn read_write_rejects_sealed_workspace() {
    let (_guard, operator, placement_path) = temp_tree();
    let mut workspace = ready_workspace();
    workspace.lifecycle_state = "sealed".into();
    workspace.seal_hash = Some("hash-1".into());
    let error = bind_workspace_overlay(
        &workspace,
        &placement(&placement_path),
        WorkspaceBindInput {
            seal_hash: Some("hash-1"),
            ..bind_input(WorkspaceAuthority::ReadWrite, Some(&operator), &[], true)
        },
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("not bindable for authority"),
        "{error:#}"
    );
}

#[test]
fn read_only_binds_ready_and_sealed() {
    let (_guard, operator, placement_path) = temp_tree();
    bind_workspace_overlay(
        &ready_workspace(),
        &placement(&placement_path),
        bind_input(WorkspaceAuthority::ReadOnly, Some(&operator), &[], false),
    )
    .unwrap();

    let mut workspace = ready_workspace();
    workspace.lifecycle_state = "sealed".into();
    workspace.seal_hash = Some("hash-1".into());
    bind_workspace_overlay(
        &workspace,
        &placement(&placement_path),
        WorkspaceBindInput {
            seal_hash: Some("hash-1"),
            ..bind_input(WorkspaceAuthority::ReadOnly, Some(&operator), &[], false)
        },
    )
    .unwrap();
}

#[test]
fn integrate_only_binds_sealed_with_matching_hash() {
    let (_guard, operator, placement_path) = temp_tree();
    let error = bind_workspace_overlay(
        &ready_workspace(),
        &placement(&placement_path),
        bind_input(WorkspaceAuthority::Integrate, Some(&operator), &[], false),
    )
    .unwrap_err();
    assert!(error.to_string().contains("not bindable"), "{error:#}");

    let mut workspace = ready_workspace();
    workspace.lifecycle_state = "sealed".into();
    workspace.seal_hash = Some("hash-1".into());
    let mut placed = placement(&placement_path);
    placed.observed_tree_hash = Some("hash-1".into());
    bind_workspace_overlay(
        &workspace,
        &placed,
        WorkspaceBindInput {
            seal_hash: Some("hash-1"),
            ..bind_input(WorkspaceAuthority::Integrate, Some(&operator), &[], false)
        },
    )
    .unwrap();
}

#[test]
fn sealed_mismatch_and_missing_hash_fail_closed() {
    let (_guard, operator, placement_path) = temp_tree();
    let mut workspace = ready_workspace();
    workspace.lifecycle_state = "sealed".into();
    workspace.seal_hash = Some("hash-1".into());
    let error = bind_workspace_overlay(
        &workspace,
        &placement(&placement_path),
        WorkspaceBindInput {
            seal_hash: Some("hash-other"),
            ..bind_input(WorkspaceAuthority::ReadOnly, Some(&operator), &[], false)
        },
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not match workspace seal_hash"),
        "{error:#}"
    );

    let mut drifted = placement(&placement_path);
    drifted.observed_tree_hash = Some("drifted".into());
    let error = bind_workspace_overlay(
        &workspace,
        &drifted,
        WorkspaceBindInput {
            seal_hash: Some("hash-1"),
            ..bind_input(WorkspaceAuthority::ReadOnly, Some(&operator), &[], false)
        },
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("observed_tree_hash drifted does not match"),
        "{error:#}"
    );
}

#[test]
fn placement_outside_operator_root_fails_closed() {
    let (_guard, operator, _) = temp_tree();
    let outside = tempfile::tempdir().unwrap();
    let outside_path = std::fs::canonicalize(outside.path()).unwrap();
    let error = bind_workspace_overlay(
        &ready_workspace(),
        &placement(&outside_path),
        bind_input(WorkspaceAuthority::ReadOnly, Some(&operator), &[], false),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("escapes operator tool root"),
        "{error:#}"
    );
}

#[test]
fn missing_ceiling_fails_closed() {
    let (_guard, _, placement_path) = temp_tree();
    let error = bind_workspace_overlay(
        &ready_workspace(),
        &placement(&placement_path),
        bind_input(WorkspaceAuthority::ReadOnly, None, &[], false),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("operator tool-root or enabled WorkspaceRoot"),
        "{error:#}"
    );
}

#[test]
fn enabled_workspace_root_allowlist_is_required_when_present() {
    let (_guard, operator, placement_path) = temp_tree();
    let other = tempfile::tempdir().unwrap();
    let other_root = std::fs::canonicalize(other.path()).unwrap();
    let error = bind_workspace_overlay(
        &ready_workspace(),
        &placement(&placement_path),
        bind_input(
            WorkspaceAuthority::ReadOnly,
            Some(&operator),
            &[other_root],
            false,
        ),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("not under an enabled WorkspaceRoot"),
        "{error:#}"
    );
}

#[test]
fn persisted_cwd_must_stay_under_placement() {
    let (_guard, operator, placement_path) = temp_tree();
    let nested = placement_path.join("src");
    std::fs::create_dir_all(&nested).unwrap();
    let overlay = bind_workspace_overlay(
        &ready_workspace(),
        &placement(&placement_path),
        WorkspaceBindInput {
            request_cwd: Some(&nested),
            ..bind_input(WorkspaceAuthority::ReadOnly, Some(&operator), &[], false)
        },
    )
    .unwrap();
    assert_eq!(overlay.cwd, std::fs::canonicalize(&nested).unwrap());

    let outside = operator.join("other");
    std::fs::create_dir_all(&outside).unwrap();
    let error = bind_workspace_overlay(
        &ready_workspace(),
        &placement(&placement_path),
        WorkspaceBindInput {
            request_cwd: Some(&outside),
            ..bind_input(WorkspaceAuthority::ReadOnly, Some(&operator), &[], false)
        },
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("not a directory under workspace root"),
        "{error:#}"
    );
}

#[test]
fn unbound_requests_are_none() {
    assert!(WorkspaceAuthority::parse("readWrite").is_ok());
    assert!(WorkspaceAuthority::ReadWrite.allows_file_writes());
    assert!(!WorkspaceAuthority::ReadOnly.allows_file_writes());
    assert!(!WorkspaceAuthority::Integrate.allows_file_writes());
}
