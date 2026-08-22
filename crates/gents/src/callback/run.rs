use std::collections::BTreeSet;
use std::path::Path;

use anyhow::{anyhow, Result};
use defra_node::EmbeddedNode;
use serde_json::Value;

use crate::workspace::journal::{advance, current_state};
use crate::workspace::{
    emit_create_workspace_plan, execute_create_workspace_plan, ActionJournalEntry,
    ActionJournalState, ActionPlan, CreateWorkspaceAction, CreationPolicy, HostAction,
    HostExecuteError, HostExecutorContext, IsolatedWorkspaceDoc, MemoryWorkspaceDocuments,
    WorkspaceAdapterKind, WorkspaceDocuments, WorkspacePlacementDoc,
};

use super::claim::{claim_invocation, invocation_is_claimable};
use super::documents::{
    create_callback_result, flush_workspace_docs, load_binding, load_callback_result,
    load_memory_workspace_docs, load_repository_placement, update_invocation, CallbackBindingDoc,
    CallbackInvocationDoc, CallbackResultDoc,
};
use super::scan::fetch_source_for_invocation;
use super::{
    BUILTIN_CREATE_WORKSPACE, LIFECYCLE_CLAIMED, LIFECYCLE_DENIED, LIFECYCLE_FAILED,
    LIFECYCLE_RUNNING, LIFECYCLE_SUCCEEDED,
};

/// Action N+1 must not enter Executing until N is ResultDocsWritten.
pub fn can_start_executing(journal: &[ActionJournalEntry], index: u32) -> bool {
    crate::workspace::action_journal_prefix_legal(journal)
        && (index == 0
            || matches!(
                current_state(journal, index - 1),
                Some(ActionJournalState::ResultDocsWritten)
            ))
}

pub fn result_docs_ready(
    journal: &[ActionJournalEntry],
    workspace: Option<&IsolatedWorkspaceDoc>,
    placement: Option<&WorkspacePlacementDoc>,
) -> bool {
    !journal.is_empty()
        && crate::workspace::action_journal_prefix_legal(journal)
        && journal
            .iter()
            .all(|entry| matches!(entry.state, ActionJournalState::ResultDocsWritten))
        && workspace.is_some()
        && placement.is_some()
}

/// CallbackResult may be written from running (then succeed) or to repair
/// succeeded-without-result. The document must not exist while journal/docs
/// are incomplete.
pub fn can_emit_callback_result(
    state: &str,
    journal: &[ActionJournalEntry],
    workspace: Option<&IsolatedWorkspaceDoc>,
    placement: Option<&WorkspacePlacementDoc>,
) -> bool {
    matches!(state, LIFECYCLE_RUNNING | LIFECYCLE_SUCCEEDED)
        && result_docs_ready(journal, workspace, placement)
}

pub fn encode_journal(journal: &[ActionJournalEntry]) -> String {
    serde_json::to_string(journal).unwrap_or_else(|_| "[]".to_string())
}

pub fn decode_journal(raw: Option<&str>) -> Result<Vec<ActionJournalEntry>> {
    let Some(raw) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(Vec::new());
    };
    Ok(serde_json::from_str(raw)?)
}

pub fn emit_plan_from_source(
    binding: &CallbackBindingDoc,
    source: &Value,
) -> Result<ActionPlan, String> {
    let builtin = binding
        .builtin_emitter
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    match builtin {
        Some(BUILTIN_CREATE_WORKSPACE) => emit_create_workspace_from_source(source),
        Some(other) => Err(format!("unknown builtin_emitter `{other}`")),
        None => Err("WASM planner is not implemented".to_string()),
    }
}

fn emit_create_workspace_from_source(source: &Value) -> Result<ActionPlan, String> {
    let work_unit_id = required_string(source, "work_unit_id")?;
    let repository_id = required_string(source, "repository_id")?;
    let base_sha = required_string(source, "base_sha")?;
    let branch = required_string(source, "branch")?;
    let workspace_id =
        optional_string(source, "workspace_id").unwrap_or_else(|| work_unit_id.clone());
    let creation_policy = match optional_string(source, "creation_policy").as_deref() {
        None | Some("git_worktree_diff") => CreationPolicy::GitWorktreeDiff,
        Some(other) => {
            return Err(format!(
                "creation_policy `{other}` is not implemented in v1"
            ))
        }
    };
    let adapter = match optional_string(source, "adapter").as_deref() {
        None | Some("make_worktree") => WorkspaceAdapterKind::MakeWorktree,
        Some("git_worktree") => WorkspaceAdapterKind::GitWorktree,
        Some(other) => return Err(format!("unknown workspace adapter `{other}`")),
    };
    let clone_artifacts = source.get("clone_artifacts").and_then(|value| {
        value.as_array().map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_string))
                .collect::<Vec<_>>()
        })
    });
    Ok(emit_create_workspace_plan(CreateWorkspaceAction {
        workspace_id,
        work_unit_id,
        repository_id,
        base_sha,
        branch,
        creation_policy,
        adapter,
        clone_artifacts,
    }))
}

fn required_string(source: &Value, field: &str) -> Result<String, String> {
    optional_string(source, field).ok_or_else(|| format!("source document missing `{field}`"))
}

fn optional_string(source: &Value, field: &str) -> Option<String> {
    source
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn correlation_from_source(source: &Value) -> String {
    optional_string(source, "caused_by_correlation")
        .or_else(|| optional_string(source, "correlation"))
        .unwrap_or_default()
}

fn stored_action_plan(invocation: &CallbackInvocationDoc) -> Result<Option<ActionPlan>, String> {
    let Some(raw) = invocation
        .action_plan
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    serde_json::from_str(raw)
        .map(Some)
        .map_err(|error| format!("stored ActionPlan is invalid: {error}"))
}

pub fn resolve_action_plan(
    invocation: &CallbackInvocationDoc,
    binding: &CallbackBindingDoc,
    source: &Value,
) -> Result<ActionPlan, String> {
    if let Some(plan) = stored_action_plan(invocation)? {
        return Ok(plan);
    }
    let journal =
        decode_journal(invocation.action_journal.as_deref()).map_err(|error| error.to_string())?;
    if !journal.is_empty() {
        return Err("missing stored ActionPlan with a non-empty journal".to_string());
    }
    emit_plan_from_source(binding, source)
}

pub async fn run_owned_invocation(
    node: &EmbeddedNode,
    invocation: &CallbackInvocationDoc,
    binding: &CallbackBindingDoc,
    ceiling: Option<&Path>,
) -> Result<()> {
    if !invocation_is_claimable(&invocation.owner_deployment_id, invocation)
        && invocation.lifecycle_state != LIFECYCLE_CLAIMED
        && invocation.lifecycle_state != LIFECYCLE_RUNNING
    {
        return Ok(());
    }
    let Some(mut claimed) =
        claim_invocation(node, &invocation.owner_deployment_id, invocation).await?
    else {
        return Ok(());
    };
    persist_claimed_to_running(node, &mut claimed).await?;

    if finish_succeeded_if_docs_ready(node, &claimed).await? {
        return Ok(());
    }

    let source = fetch_source_for_invocation(node, binding, &claimed).await?;
    execute_running_invocation(node, &mut claimed, binding, &source, ceiling).await
}

async fn persist_claimed_to_running(
    node: &EmbeddedNode,
    invocation: &mut CallbackInvocationDoc,
) -> Result<()> {
    if invocation.lifecycle_state == LIFECYCLE_RUNNING {
        return Ok(());
    }
    invocation.lifecycle_state = LIFECYCLE_RUNNING.to_string();
    if update_invocation(node, invocation, Some(LIFECYCLE_CLAIMED)).await? {
        return Ok(());
    }
    let current = super::documents::load_invocation(node, &invocation.invocation_id).await?;
    match current {
        Some(row) if row.lifecycle_state == LIFECYCLE_RUNNING => {
            *invocation = row;
            Ok(())
        }
        Some(row) => anyhow::bail!(
            "CallbackInvocation {} could not persist claimed→running; state={}",
            invocation.invocation_id,
            row.lifecycle_state
        ),
        None => anyhow::bail!(
            "CallbackInvocation {} disappeared during claimed→running",
            invocation.invocation_id
        ),
    }
}

/// Testable planner + host-executor core. Persists journal/docs via the node.
async fn execute_running_invocation(
    node: &EmbeddedNode,
    invocation: &mut CallbackInvocationDoc,
    binding: &CallbackBindingDoc,
    source: &Value,
    ceiling: Option<&Path>,
) -> Result<()> {
    let mut journal = decode_journal(invocation.action_journal.as_deref())?;
    if !crate::workspace::action_journal_prefix_legal(&journal) {
        return deny(node, invocation, "illegal action journal prefix").await;
    }

    let plan = match resolve_action_plan(invocation, binding, source) {
        Ok(plan) => plan,
        Err(reason) => return deny(node, invocation, &reason).await,
    };
    invocation.action_plan = Some(serde_json::to_string(&plan)?);
    if let Err(error) = plan.validate_against(&binding.capabilities()) {
        return deny(node, invocation, &error.to_string()).await;
    }

    let HostAction::CreateWorkspace(action) = plan
        .actions
        .first()
        .ok_or_else(|| anyhow!("ActionPlan has no actions"))?
        .clone();
    if !can_start_executing(&journal, 0) {
        return deny(node, invocation, "journal prefix blocks first action").await;
    }

    let Some(repository) =
        load_repository_placement(node, &action.repository_id, &invocation.owner_deployment_id)
            .await?
    else {
        return deny(
            node,
            invocation,
            &format!(
                "RepositoryPlacement {} not found on this host",
                action.repository_id
            ),
        )
        .await;
    };

    persist_journal(node, invocation, &journal, LIFECYCLE_RUNNING, None).await?;

    if current_state(&journal, 0).is_none() {
        advance(&mut journal, 0, ActionJournalState::Validated);
        persist_journal(node, invocation, &journal, LIFECYCLE_RUNNING, None).await?;
    }
    if matches!(
        current_state(&journal, 0),
        Some(ActionJournalState::Validated)
    ) {
        // Durable Executing before the adapter so recovery observes rather than re-runs.
        advance(&mut journal, 0, ActionJournalState::Executing);
        persist_journal(node, invocation, &journal, LIFECYCLE_RUNNING, None).await?;
    }

    let mut docs = load_memory_workspace_docs(node, &action.workspace_id).await?;
    let capabilities: BTreeSet<String> = binding.capabilities();
    let correlation = correlation_from_source(source);
    let execute_result = {
        let mut ctx = HostExecutorContext {
            deployment_id: invocation.owner_deployment_id.clone(),
            repository,
            ceiling,
            capabilities,
            writer_principal: binding.principal_did.clone(),
            integrator_principal: binding.principal_did.clone(),
            caused_by_invocation_id: invocation.invocation_id.clone(),
            caused_by_correlation: correlation.clone(),
            documents: &mut docs,
        };
        execute_create_workspace_plan(&plan, &mut journal, &mut ctx)
    };
    let outcome = match execute_result {
        Ok(outcome) => outcome,
        Err(HostExecuteError::Denied { reason }) => {
            return deny(node, invocation, &reason).await;
        }
        Err(HostExecuteError::Failed {
            reason, outcome, ..
        }) => {
            if let Some(outcome) = outcome {
                let written = memory_from_outcome(&outcome);
                flush_workspace_docs(node, &written).await?;
            } else {
                flush_workspace_docs(node, &docs).await?;
            }
            persist_journal(node, invocation, &journal, LIFECYCLE_FAILED, Some(&reason)).await?;
            return Ok(());
        }
    };

    let written = memory_from_outcome(&outcome);
    flush_workspace_docs(node, &written).await?;
    persist_journal(node, invocation, &journal, LIFECYCLE_RUNNING, None).await?;
    emit_result_then_succeed(
        node,
        invocation,
        &journal,
        &outcome.workspace,
        &outcome.placement,
        Some(correlation),
    )
    .await
}

fn memory_from_outcome(
    outcome: &crate::workspace::CreateWorkspaceOutcome,
) -> MemoryWorkspaceDocuments {
    let mut docs = MemoryWorkspaceDocuments::default();
    let _ = docs.write_isolated_workspace(outcome.workspace.clone());
    let _ = docs.write_placement(outcome.placement.clone());
    docs
}

async fn persist_journal(
    node: &EmbeddedNode,
    invocation: &mut CallbackInvocationDoc,
    journal: &[ActionJournalEntry],
    state: &str,
    error: Option<&str>,
) -> Result<()> {
    invocation.action_journal = Some(encode_journal(journal));
    invocation.lifecycle_state = state.to_string();
    invocation.error = error.map(str::to_string);
    if !update_invocation(node, invocation, None).await? {
        anyhow::bail!(
            "CallbackInvocation {} persist matched no row (state={state})",
            invocation.invocation_id
        );
    }
    Ok(())
}

async fn deny(
    node: &EmbeddedNode,
    invocation: &mut CallbackInvocationDoc,
    reason: &str,
) -> Result<()> {
    invocation.action_journal = Some("[]".to_string());
    invocation.lifecycle_state = LIFECYCLE_DENIED.to_string();
    invocation.error = Some(reason.to_string());
    if !update_invocation(node, invocation, None).await? {
        anyhow::bail!(
            "CallbackInvocation {} deny persist matched no row",
            invocation.invocation_id
        );
    }
    Ok(())
}

async fn emit_result_then_succeed(
    node: &EmbeddedNode,
    invocation: &mut CallbackInvocationDoc,
    journal: &[ActionJournalEntry],
    workspace: &IsolatedWorkspaceDoc,
    placement: &WorkspacePlacementDoc,
    correlation: Option<String>,
) -> Result<()> {
    if !can_emit_callback_result(
        &invocation.lifecycle_state,
        journal,
        Some(workspace),
        Some(placement),
    ) {
        anyhow::bail!(
            "CallbackInvocation {} result docs are not ready",
            invocation.invocation_id
        );
    }
    let correlation = correlation
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| workspace.caused_by_correlation.clone());
    create_callback_result(
        node,
        &CallbackResultDoc {
            result_id: format!("res-{}", invocation.invocation_id),
            invocation_id: invocation.invocation_id.clone(),
            owner_deployment_id: invocation.owner_deployment_id.clone(),
            workspace_id: Some(workspace.workspace_id.clone()),
            caused_by_correlation: Some(correlation),
            created_at: Some(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
        },
    )
    .await?;
    persist_journal(node, invocation, journal, LIFECYCLE_SUCCEEDED, None).await
}

pub async fn recover_local_invocations(
    node: &EmbeddedNode,
    local_deployment_id: &str,
    ceiling: Option<&Path>,
) -> Result<()> {
    let invocations =
        super::documents::list_recoverable_invocations(node, local_deployment_id).await?;
    for invocation in invocations {
        if invocation.owner_deployment_id != local_deployment_id {
            continue;
        }
        if invocation.lifecycle_state == LIFECYCLE_SUCCEEDED {
            if let Err(error) = finish_succeeded_if_docs_ready(node, &invocation).await {
                tracing::warn!(
                    invocation_id = %invocation.invocation_id,
                    %error,
                    "callback succeeded-without-result repair failed"
                );
            }
            continue;
        }
        if !invocation_is_claimable(local_deployment_id, &invocation) {
            continue;
        }
        let Some(binding) = load_binding(node, &invocation.binding_id).await? else {
            tracing::warn!(
                invocation_id = %invocation.invocation_id,
                binding_id = %invocation.binding_id,
                "recovery skipped: CallbackBinding missing"
            );
            continue;
        };
        if let Err(error) = run_owned_invocation(node, &invocation, &binding, ceiling).await {
            tracing::warn!(
                invocation_id = %invocation.invocation_id,
                %error,
                "callback recovery run failed"
            );
        }
    }
    Ok(())
}

pub async fn finish_succeeded_if_docs_ready(
    node: &EmbeddedNode,
    invocation: &CallbackInvocationDoc,
) -> Result<bool> {
    if !matches!(
        invocation.lifecycle_state.as_str(),
        LIFECYCLE_RUNNING | LIFECYCLE_SUCCEEDED
    ) {
        return Ok(false);
    }
    let journal = decode_journal(invocation.action_journal.as_deref())?;
    let workspace_id = stored_action_plan(invocation)
        .map_err(anyhow::Error::msg)?
        .and_then(|plan| match plan.actions.into_iter().next() {
            Some(HostAction::CreateWorkspace(action)) => Some(action.workspace_id),
            None => None,
        });
    let Some(workspace_id) = workspace_id else {
        return Ok(false);
    };
    let docs = load_memory_workspace_docs(node, &workspace_id).await?;
    let workspace = docs.load_isolated_workspace(&workspace_id)?;
    let placement = docs.load_placement(&workspace_id)?;
    if !result_docs_ready(&journal, workspace.as_ref(), placement.as_ref()) {
        return Ok(false);
    }
    let Some(workspace) = workspace else {
        return Ok(false);
    };
    let Some(placement) = placement else {
        return Ok(false);
    };
    if load_callback_result(node, &invocation.invocation_id)
        .await?
        .is_some()
        && invocation.lifecycle_state == LIFECYCLE_SUCCEEDED
    {
        return Ok(true);
    }
    let mut current = invocation.clone();
    emit_result_then_succeed(
        node,
        &mut current,
        &journal,
        &workspace,
        &placement,
        Some(workspace.caused_by_correlation.clone()),
    )
    .await?;
    Ok(true)
}
