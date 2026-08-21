use crate::lean_vocab_test::{
    lean_workspace_binding_cases, lean_workspace_cases, LeanWorkspaceBindingCase,
    LeanWorkspaceBindingRef,
};

fn workspace_transition_legal(from: &str, to: &str, seal_hash: Option<&str>) -> bool {
    match (from, to) {
        ("provisioning", "ready") | ("provisioning", "provisionFailed") => true,
        ("ready", "sealed") => seal_hash.is_some(),
        ("sealed", "cleaning") | ("cleaning", "cleaned") => true,
        _ => false,
    }
}

fn bindable_state(state: &str) -> bool {
    state == "ready" || state == "sealed"
}

fn owner_claimable(owner_deployment_id: &str, deployment_id: &str, workspace_state: &str) -> bool {
    owner_deployment_id == deployment_id && bindable_state(workspace_state)
}

fn read_write_ok(workspace_state: &str, binding: &LeanWorkspaceBindingRef) -> bool {
    !(binding.authority == "readWrite" && binding.state == "active") || workspace_state == "ready"
}

fn read_only_ok(
    workspace_state: &str,
    workspace_seal_hash: Option<&str>,
    binding: &LeanWorkspaceBindingRef,
) -> bool {
    if binding.authority != "readOnly" {
        return true;
    }
    bindable_state(workspace_state)
        && (workspace_state != "sealed" || binding.seal_hash.as_deref() == workspace_seal_hash)
}

fn integrate_ok(
    workspace_state: &str,
    workspace_seal_hash: Option<&str>,
    binding: &LeanWorkspaceBindingRef,
) -> bool {
    if binding.authority != "integrate" {
        return true;
    }
    workspace_state == "sealed" && binding.seal_hash.as_deref() == workspace_seal_hash
}

fn candidate_binding_legal(case: &LeanWorkspaceBindingCase) -> bool {
    let candidate = &case.candidate;
    candidate.workspace_id == case.workspace_id
        && owner_claimable(
            &case.owner_deployment_id,
            &candidate.deployment_id,
            &case.workspace_state,
        )
        && read_write_ok(&case.workspace_state, candidate)
        && read_only_ok(
            &case.workspace_state,
            case.workspace_seal_hash.as_deref(),
            candidate,
        )
        && integrate_ok(
            &case.workspace_state,
            case.workspace_seal_hash.as_deref(),
            candidate,
        )
}

fn unique_active_read_write(case: &LeanWorkspaceBindingCase) -> bool {
    case.existing
        .iter()
        .chain(std::iter::once(&case.candidate))
        .filter(|binding| {
            binding.workspace_id == case.workspace_id
                && binding.authority == "readWrite"
                && binding.state == "active"
        })
        .count()
        <= 1
}

fn git_metadata_write_ok(policy: &str, authority: &str) -> bool {
    !(policy == "git_worktree_diff" && authority == "readWrite")
}

fn command_mode_rank(mode: &str) -> u8 {
    match mode {
        "read_only" => 0,
        "workspace_write" => 1,
        "unrestricted" => 2,
        _ => 0,
    }
}

fn authority_command_mode(authority: &str) -> &'static str {
    match authority {
        "readWrite" => "workspace_write",
        _ => "read_only",
    }
}

fn authority_meet(behavior: &str, authority: &str) -> &'static str {
    let cap = authority_command_mode(authority);
    if command_mode_rank(behavior) <= command_mode_rank(cap) {
        match behavior {
            "read_only" => "read_only",
            "workspace_write" => "workspace_write",
            "unrestricted" => "unrestricted",
            other => panic!("unknown behavior command mode {other}"),
        }
    } else {
        cap
    }
}

fn authority_meet_ok(behavior: &str, authority: &str) -> bool {
    !(authority == "readWrite" && authority_meet(behavior, authority) == "unrestricted")
}

fn binding_case_legal(case: &LeanWorkspaceBindingCase) -> bool {
    candidate_binding_legal(case)
        && unique_active_read_write(case)
        && (!case.git_metadata_write
            || git_metadata_write_ok(&case.creation_policy, &case.candidate.authority))
        && authority_meet_ok(&case.behavior_command_mode, &case.candidate.authority)
}

fn assert_required_names(names: &[&str], required: &[&str], label: &str) {
    for name in required {
        assert!(
            names.iter().any(|candidate| candidate == name),
            "missing {label} case {name}"
        );
    }
}

#[test]
fn generated_workspace_cases_match_lean_predicate() {
    let cases = lean_workspace_cases();
    assert!(
        !cases.is_empty(),
        "Lean workspace_cases must include lifecycle witnesses"
    );
    let names: Vec<&str> = cases.iter().map(|case| case.name.as_str()).collect();
    assert_required_names(
        &names,
        &["provision_success", "provision_fail", "seal_requires_hash"],
        "workspace",
    );
    for case in cases {
        let actual = workspace_transition_legal(
            &case.from,
            &case.to,
            case.seal_hash.as_deref(),
        );
        assert_eq!(
            actual, case.legal,
            "workspace case {} disagreed with the Lean predicate",
            case.name
        );
    }
}

#[test]
fn generated_workspace_binding_cases_match_lean_predicate() {
    let cases = lean_workspace_binding_cases();
    assert!(
        !cases.is_empty(),
        "Lean workspace_binding_cases must include binding witnesses"
    );
    let names: Vec<&str> = cases.iter().map(|case| case.name.as_str()).collect();
    assert_required_names(
        &names,
        &[
            "provision_fail_no_bind",
            "read_write_after_sealed_illegal",
            "second_active_read_write_illegal",
            "two_active_read_only_after_seal_legal",
            "integrate_before_seal_illegal",
            "integrate_mismatched_seal_hash_illegal",
            "non_owner_deployment_cannot_claim",
            "git_worktree_diff_read_write_git_metadata_write_illegal",
            "authority_meet_read_write_not_unrestricted",
        ],
        "workspace binding",
    );
    for case in cases {
        let actual = binding_case_legal(case);
        assert_eq!(
            actual, case.legal,
            "workspace binding case {} disagreed with the Lean predicate",
            case.name
        );
    }
}
