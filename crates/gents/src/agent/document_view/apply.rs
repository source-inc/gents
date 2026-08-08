use anyhow::Result;
use defra_node::EmbeddedNode;

use crate::backend_registry::lookup_backend_by_doc_id;
use crate::document_config::{
    load_agent_behavior_by_doc_id, load_agent_principal_by_doc_id,
    load_datastore_tool_surface_by_doc_id, load_event_trigger_by_doc_id,
    load_inference_profile_by_doc_id, load_schedule_by_doc_id, load_skill_by_doc_id,
    load_task_by_doc_id, load_tool_selection_by_doc_id,
};

use super::snapshot::behavior_references_ready;
use super::{
    validate_subagent_targets_resolve, ControlUpdateOutcome, DocumentRecord, DocumentRuntimeView,
};

pub(crate) async fn apply_control_update(
    node: &EmbeddedNode,
    agent_did: &str,
    _collection_id: &str,
    doc_id: &str,
    view: &mut DocumentRuntimeView,
) -> Result<ControlUpdateOutcome> {
    if let Some((loaded_doc_id, principal)) = load_agent_principal_by_doc_id(node, doc_id).await? {
        if principal.agent_did != agent_did {
            return Ok(ControlUpdateOutcome::Irrelevant);
        }
        let default_behavior_visible = principal
            .default_behavior_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .is_none_or(|behavior_id| view.behaviors.contains_key(behavior_id));
        view.principal = DocumentRecord {
            doc_id: loaded_doc_id,
            value: principal,
        };
        return Ok(if default_behavior_visible {
            ControlUpdateOutcome::Applied
        } else {
            ControlUpdateOutcome::PendingVisibility
        });
    }
    if view.principal.doc_id == doc_id {
        return Ok(ControlUpdateOutcome::PendingVisibility);
    }

    if let Some((loaded_doc_id, behavior)) = load_agent_behavior_by_doc_id(node, doc_id).await? {
        if behavior.agent_did != agent_did {
            return Ok(ControlUpdateOutcome::Irrelevant);
        }
        if !behavior_references_ready(view, &behavior) {
            return Ok(ControlUpdateOutcome::PendingVisibility);
        }
        view.remove_behavior_by_doc_id(doc_id);
        view.behaviors.insert(
            behavior.behavior_id.clone(),
            DocumentRecord {
                doc_id: loaded_doc_id,
                value: behavior,
            },
        );
        return Ok(ControlUpdateOutcome::Applied);
    }
    if view.has_behavior_doc_id(doc_id) {
        return Ok(ControlUpdateOutcome::PendingVisibility);
    }

    if let Some((loaded_doc_id, selection)) = load_tool_selection_by_doc_id(node, doc_id).await? {
        if selection.agent_did != agent_did {
            return Ok(ControlUpdateOutcome::Irrelevant);
        }
        selection.validate()?;
        validate_subagent_targets_resolve(&selection, view)?;
        view.remove_tool_selection_by_doc_id(doc_id);
        view.tool_selections.insert(
            selection.selection_id.clone(),
            DocumentRecord {
                doc_id: loaded_doc_id,
                value: selection,
            },
        );
        return Ok(ControlUpdateOutcome::Applied);
    }
    if view.has_tool_selection_doc_id(doc_id) {
        return Ok(ControlUpdateOutcome::PendingVisibility);
    }

    if let Some((loaded_doc_id, skill)) = load_skill_by_doc_id(node, doc_id).await? {
        if skill.agent_did != agent_did {
            return Ok(ControlUpdateOutcome::Irrelevant);
        }
        view.remove_skill_by_doc_id(doc_id);
        view.skills.insert(
            skill.skill_id.clone(),
            DocumentRecord {
                doc_id: loaded_doc_id,
                value: skill,
            },
        );
        return Ok(ControlUpdateOutcome::Applied);
    }
    if view.has_skill_doc_id(doc_id) {
        view.remove_skill_by_doc_id(doc_id);
        return Ok(ControlUpdateOutcome::Applied);
    }

    if let Some((loaded_doc_id, surface)) =
        load_datastore_tool_surface_by_doc_id(node, doc_id).await?
    {
        if surface.agent_did != agent_did {
            // Ownership moved away: drop the grant now rather than serving the
            // stale copy until restart.
            if view.remove_datastore_tool_surface_by_doc_id(doc_id) {
                return Ok(ControlUpdateOutcome::Applied);
            }
            return Ok(ControlUpdateOutcome::Irrelevant);
        }
        view.remove_datastore_tool_surface_by_doc_id(doc_id);
        view.datastore_tool_surfaces.insert(
            surface.surface_id.clone(),
            DocumentRecord {
                doc_id: loaded_doc_id,
                value: surface,
            },
        );
        return Ok(ControlUpdateOutcome::Applied);
    }
    if view.has_datastore_tool_surface_doc_id(doc_id) {
        view.remove_datastore_tool_surface_by_doc_id(doc_id);
        return Ok(ControlUpdateOutcome::Applied);
    }

    if let Some((loaded_doc_id, profile)) = load_inference_profile_by_doc_id(node, doc_id).await? {
        if !view.references_profile(&profile.profile_id)
            && !view.has_inference_profile_doc_id(doc_id)
        {
            return Ok(ControlUpdateOutcome::Irrelevant);
        }
        view.remove_inference_profile_by_doc_id(doc_id);
        view.inference_profiles.insert(
            profile.profile_id.clone(),
            DocumentRecord {
                doc_id: loaded_doc_id,
                value: profile,
            },
        );
        return Ok(ControlUpdateOutcome::Applied);
    }
    if view.has_inference_profile_doc_id(doc_id) {
        return Ok(ControlUpdateOutcome::PendingVisibility);
    }

    if let Some((loaded_doc_id, backend)) = lookup_backend_by_doc_id(node, doc_id).await? {
        if !view.references_backend(&backend.backend_id) && !view.has_backend_doc_id(doc_id) {
            return Ok(ControlUpdateOutcome::Irrelevant);
        }
        view.remove_backend_by_doc_id(doc_id);
        view.backends.insert(
            backend.backend_id.clone(),
            DocumentRecord {
                doc_id: loaded_doc_id,
                value: backend,
            },
        );
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
            DocumentRecord {
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
        view.remove_schedule_by_doc_id(doc_id);
        view.schedules.insert(
            schedule.schedule_id.clone(),
            DocumentRecord {
                doc_id: loaded_doc_id,
                value: schedule,
            },
        );
        return Ok(ControlUpdateOutcome::Applied);
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
            DocumentRecord {
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
            DocumentRecord {
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
