use anyhow::Result;
use defra_node::EmbeddedNode;

use crate::backend_registry::lookup_backend_by_doc_id;
use crate::document_config::{
    load_agent_behavior_by_doc_id, load_agent_principal_by_doc_id, load_event_trigger_by_doc_id,
    load_inference_profile_by_doc_id, load_schedule_by_doc_id, load_skill_by_doc_id,
    load_task_by_doc_id, load_tool_selection_by_doc_id,
};

use super::load::{
    load_verified_backend_by_doc_id, load_verified_behavior_by_doc_id,
    load_verified_principal_by_doc_id, load_verified_profile_by_doc_id,
    load_verified_skill_by_doc_id, load_verified_tool_selection_by_doc_id,
};
use super::snapshot::behavior_references_ready;
use super::{
    validate_subagent_targets_resolve, ControlUpdateOutcome, DocumentRecord, DocumentRuntimeView,
    UnversionedDocumentRecord,
};

pub(crate) async fn apply_control_update(
    node: &EmbeddedNode,
    agent_did: &str,
    _collection_id: &str,
    doc_id: &str,
    view: &mut DocumentRuntimeView,
) -> Result<ControlUpdateOutcome> {
    if load_agent_principal_by_doc_id(node, doc_id)
        .await?
        .is_some()
    {
        let record = load_verified_principal_by_doc_id(node, doc_id).await?;
        let principal = &record.value;
        if principal.agent_did != agent_did {
            return Ok(ControlUpdateOutcome::Irrelevant);
        }
        if view.principal.doc_id != doc_id && view.principal.value.agent_did == principal.agent_did
        {
            anyhow::bail!(
                "AgentPrincipal logical id {} resolves to duplicate documents {} and {}",
                principal.agent_did,
                view.principal.doc_id,
                doc_id
            );
        }
        let default_behavior_visible = principal
            .default_behavior_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .is_none_or(|behavior_id| view.behaviors.contains_key(behavior_id));
        view.principal = record;
        return Ok(if default_behavior_visible {
            ControlUpdateOutcome::Applied
        } else {
            ControlUpdateOutcome::PendingVisibility
        });
    }
    if view.principal.doc_id == doc_id {
        return Ok(ControlUpdateOutcome::PendingVisibility);
    }

    if load_agent_behavior_by_doc_id(node, doc_id).await?.is_some() {
        let record = load_verified_behavior_by_doc_id(node, doc_id).await?;
        let behavior = &record.value;
        if behavior.agent_did != agent_did {
            return Ok(ControlUpdateOutcome::Irrelevant);
        }
        if !behavior_references_ready(view, &behavior) {
            return Ok(ControlUpdateOutcome::PendingVisibility);
        }
        replace_verified_record(&mut view.behaviors, record)?;
        return Ok(ControlUpdateOutcome::Applied);
    }
    if view.has_behavior_doc_id(doc_id) {
        return Ok(ControlUpdateOutcome::PendingVisibility);
    }

    if load_tool_selection_by_doc_id(node, doc_id).await?.is_some() {
        let record = load_verified_tool_selection_by_doc_id(node, doc_id).await?;
        let selection = &record.value;
        if selection.agent_did != agent_did {
            return Ok(ControlUpdateOutcome::Irrelevant);
        }
        selection.validate()?;
        validate_subagent_targets_resolve(&selection, view)?;
        replace_verified_record(&mut view.tool_selections, record)?;
        return Ok(ControlUpdateOutcome::Applied);
    }
    if view.has_tool_selection_doc_id(doc_id) {
        return Ok(ControlUpdateOutcome::PendingVisibility);
    }

    if load_skill_by_doc_id(node, doc_id).await?.is_some() {
        let record = load_verified_skill_by_doc_id(node, doc_id).await?;
        let skill = &record.value;
        if skill.agent_did != agent_did {
            return Ok(ControlUpdateOutcome::Irrelevant);
        }
        replace_verified_record(&mut view.skills, record)?;
        return Ok(ControlUpdateOutcome::Applied);
    }
    if view.has_skill_doc_id(doc_id) {
        view.remove_skill_by_doc_id(doc_id);
        return Ok(ControlUpdateOutcome::Applied);
    }

    if load_inference_profile_by_doc_id(node, doc_id)
        .await?
        .is_some()
    {
        let record = load_verified_profile_by_doc_id(node, doc_id).await?;
        let profile = &record.value;
        if !view.references_profile(&profile.profile_id)
            && !view.has_inference_profile_doc_id(doc_id)
        {
            return Ok(ControlUpdateOutcome::Irrelevant);
        }
        replace_verified_record(&mut view.inference_profiles, record)?;
        return Ok(ControlUpdateOutcome::Applied);
    }
    if view.has_inference_profile_doc_id(doc_id) {
        return Ok(ControlUpdateOutcome::PendingVisibility);
    }

    if lookup_backend_by_doc_id(node, doc_id).await?.is_some() {
        let record = load_verified_backend_by_doc_id(node, doc_id).await?;
        let backend = &record.value;
        if !view.references_backend(&backend.backend_id) && !view.has_backend_doc_id(doc_id) {
            return Ok(ControlUpdateOutcome::Irrelevant);
        }
        replace_verified_record(&mut view.backends, record)?;
        return Ok(ControlUpdateOutcome::Applied);
    }
    if view.has_backend_doc_id(doc_id) {
        return Ok(ControlUpdateOutcome::PendingVisibility);
    }

    if let Some((loaded_doc_id, task)) = load_task_by_doc_id(node, doc_id).await? {
        if task.task_id.trim().is_empty() {
            return Ok(ControlUpdateOutcome::Irrelevant);
        }
        view.remove_task_by_doc_id(doc_id);
        view.tasks.insert(
            task.task_id.clone(),
            UnversionedDocumentRecord {
                doc_id: loaded_doc_id,
                value: task,
            },
        );
        return Ok(ControlUpdateOutcome::Applied);
    }
    if view.has_task_doc_id(doc_id) {
        view.remove_task_by_doc_id(doc_id);
        return Ok(ControlUpdateOutcome::Applied);
    }

    if let Some((loaded_doc_id, schedule)) = load_schedule_by_doc_id(node, doc_id).await? {
        if schedule.schedule_id.trim().is_empty() {
            return Ok(ControlUpdateOutcome::Irrelevant);
        }
        let configuration_changed = view
            .schedules
            .get(&schedule.schedule_id)
            .is_none_or(|record| !same_schedule_configuration(&record.value, &schedule));
        view.remove_schedule_by_doc_id(doc_id);
        view.schedules.insert(
            schedule.schedule_id.clone(),
            UnversionedDocumentRecord {
                doc_id: loaded_doc_id,
                value: schedule,
            },
        );
        return Ok(if configuration_changed {
            ControlUpdateOutcome::Applied
        } else {
            // ScheduleSource persists bookkeeping on the same document after
            // every fire attempt. Keep the cached document current, but do
            // not restart the configuration debounce for runtime-only fields.
            ControlUpdateOutcome::Irrelevant
        });
    }
    if view.has_schedule_doc_id(doc_id) {
        view.remove_schedule_by_doc_id(doc_id);
        return Ok(ControlUpdateOutcome::Applied);
    }

    if let Some((loaded_doc_id, trigger)) = load_event_trigger_by_doc_id(node, doc_id).await? {
        if trigger.trigger_id.trim().is_empty() {
            return Ok(ControlUpdateOutcome::Irrelevant);
        }
        view.remove_event_trigger_by_doc_id(doc_id);
        view.event_triggers.insert(
            trigger.trigger_id.clone(),
            UnversionedDocumentRecord {
                doc_id: loaded_doc_id,
                value: trigger,
            },
        );
        return Ok(ControlUpdateOutcome::Applied);
    }
    if view.has_event_trigger_doc_id(doc_id) {
        view.remove_event_trigger_by_doc_id(doc_id);
        return Ok(ControlUpdateOutcome::Applied);
    }

    if let Some(credential) =
        crate::chatgpt_codex::lookup_oauth_credential_by_doc_id(node, doc_id).await?
    {
        if credential.agent_did != agent_did {
            return Ok(ControlUpdateOutcome::Irrelevant);
        }
        let loaded_doc_id = credential
            .doc_id
            .clone()
            .unwrap_or_else(|| doc_id.to_string());
        view.remove_oauth_credential_by_doc_id(doc_id);
        view.oauth_credentials.insert(
            credential.credential_id.clone(),
            UnversionedDocumentRecord {
                doc_id: loaded_doc_id,
                value: credential,
            },
        );
        return Ok(ControlUpdateOutcome::Applied);
    }
    if view.has_oauth_credential_doc_id(doc_id) {
        view.remove_oauth_credential_by_doc_id(doc_id);
        return Ok(ControlUpdateOutcome::Applied);
    }

    Ok(ControlUpdateOutcome::Irrelevant)
}

fn replace_verified_record<T>(
    records: &mut std::collections::HashMap<String, DocumentRecord<T>>,
    record: DocumentRecord<T>,
) -> Result<()> {
    let logical_id = record.fact.logical_id.clone();
    if let Some(existing) = records.get(&logical_id) {
        if existing.doc_id != record.doc_id {
            anyhow::bail!(
                "{} logical id {} resolves to duplicate documents {} and {}",
                record.fact.collection,
                logical_id,
                existing.doc_id,
                record.doc_id
            );
        }
    }
    let previous_logical_id = records.iter().find_map(|(logical_id, existing)| {
        (existing.doc_id == record.doc_id).then_some(logical_id.clone())
    });
    if let Some(previous_logical_id) = previous_logical_id {
        records.remove(&previous_logical_id);
    }
    records.insert(logical_id, record);
    Ok(())
}

fn same_schedule_configuration(
    previous: &crate::document_config::Schedule,
    current: &crate::document_config::Schedule,
) -> bool {
    previous.schedule_id == current.schedule_id
        && previous.task_id == current.task_id
        && previous.interval_secs == current.interval_secs
        && previous.cron == current.cron
        && previous.timezone == current.timezone
        && previous.missed_run_policy == current.missed_run_policy
        && previous.enabled == current.enabled
        && previous.concurrency == current.concurrency
}
