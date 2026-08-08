mod apply;
mod load;
mod snapshot;

pub(crate) use apply::apply_control_update;
pub(crate) use load::load_document_runtime_view;
pub(crate) use snapshot::resolve_document_runtime_snapshot_from_view;

use std::collections::HashMap;

use crate::backend_registry::InferenceBackend;
use crate::chatgpt_codex::OAuthCredential;
use crate::document_config::{
    AgentBehavior, AgentPrincipal, DatastoreToolSurfaceDocument, EventTrigger, InferenceProfile,
    Schedule, SkillDocument, Task, ToolSelectionDocument, WriteToolDecl,
};

#[derive(Debug, Clone)]
pub(crate) struct DocumentRecord<T> {
    pub(crate) doc_id: String,
    pub(crate) value: T,
}

#[derive(Debug, Clone)]
pub(crate) struct DocumentRuntimeView {
    pub(crate) principal: DocumentRecord<AgentPrincipal>,
    pub(crate) behaviors: HashMap<String, DocumentRecord<AgentBehavior>>,
    pub(crate) skills: HashMap<String, DocumentRecord<SkillDocument>>,
    pub(crate) datastore_tool_surfaces:
        HashMap<String, DocumentRecord<DatastoreToolSurfaceDocument>>,
    pub(crate) tool_selections: HashMap<String, DocumentRecord<ToolSelectionDocument>>,
    pub(crate) inference_profiles: HashMap<String, DocumentRecord<InferenceProfile>>,
    pub(crate) backends: HashMap<String, DocumentRecord<InferenceBackend>>,
    pub(crate) oauth_credentials: HashMap<String, DocumentRecord<OAuthCredential>>,
    pub(crate) tasks: HashMap<String, DocumentRecord<Task>>,
    pub(crate) schedules: HashMap<String, DocumentRecord<Schedule>>,
    pub(crate) event_triggers: HashMap<String, DocumentRecord<EventTrigger>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ControlUpdateOutcome {
    Irrelevant,
    Applied,
    PendingVisibility,
}

impl DocumentRuntimeView {
    fn has_behavior_doc_id(&self, doc_id: &str) -> bool {
        self.behaviors
            .values()
            .any(|record| record.doc_id == doc_id)
    }

    fn has_tool_selection_doc_id(&self, doc_id: &str) -> bool {
        self.tool_selections
            .values()
            .any(|record| record.doc_id == doc_id)
    }

    fn has_skill_doc_id(&self, doc_id: &str) -> bool {
        self.skills.values().any(|record| record.doc_id == doc_id)
    }

    fn has_datastore_tool_surface_doc_id(&self, doc_id: &str) -> bool {
        self.datastore_tool_surfaces
            .values()
            .any(|record| record.doc_id == doc_id)
    }

    fn has_inference_profile_doc_id(&self, doc_id: &str) -> bool {
        self.inference_profiles
            .values()
            .any(|record| record.doc_id == doc_id)
    }

    fn has_backend_doc_id(&self, doc_id: &str) -> bool {
        self.backends.values().any(|record| record.doc_id == doc_id)
    }

    fn has_task_doc_id(&self, doc_id: &str) -> bool {
        self.tasks.values().any(|record| record.doc_id == doc_id)
    }

    fn has_schedule_doc_id(&self, doc_id: &str) -> bool {
        self.schedules
            .values()
            .any(|record| record.doc_id == doc_id)
    }

    fn has_event_trigger_doc_id(&self, doc_id: &str) -> bool {
        self.event_triggers
            .values()
            .any(|record| record.doc_id == doc_id)
    }

    fn remove_behavior_by_doc_id(&mut self, doc_id: &str) -> bool {
        let key = self.behaviors.iter().find_map(|(behavior_id, record)| {
            (record.doc_id == doc_id).then_some(behavior_id.clone())
        });
        key.is_some_and(|behavior_id| self.behaviors.remove(&behavior_id).is_some())
    }

    fn remove_tool_selection_by_doc_id(&mut self, doc_id: &str) -> bool {
        let key = self
            .tool_selections
            .iter()
            .find_map(|(selection_id, record)| {
                (record.doc_id == doc_id).then_some(selection_id.clone())
            });
        key.is_some_and(|selection_id| self.tool_selections.remove(&selection_id).is_some())
    }

    fn remove_skill_by_doc_id(&mut self, doc_id: &str) -> bool {
        let key = self
            .skills
            .iter()
            .find_map(|(skill_id, record)| (record.doc_id == doc_id).then_some(skill_id.clone()));
        key.is_some_and(|skill_id| self.skills.remove(&skill_id).is_some())
    }

    fn remove_datastore_tool_surface_by_doc_id(&mut self, doc_id: &str) -> bool {
        let key = self
            .datastore_tool_surfaces
            .iter()
            .find_map(|(surface_id, record)| {
                (record.doc_id == doc_id).then_some(surface_id.clone())
            });
        key.is_some_and(|surface_id| self.datastore_tool_surfaces.remove(&surface_id).is_some())
    }

    fn remove_inference_profile_by_doc_id(&mut self, doc_id: &str) -> bool {
        let key = self
            .inference_profiles
            .iter()
            .find_map(|(profile_id, record)| {
                (record.doc_id == doc_id).then_some(profile_id.clone())
            });
        key.is_some_and(|profile_id| self.inference_profiles.remove(&profile_id).is_some())
    }

    fn remove_backend_by_doc_id(&mut self, doc_id: &str) -> bool {
        let key = self.backends.iter().find_map(|(backend_id, record)| {
            (record.doc_id == doc_id).then_some(backend_id.clone())
        });
        key.is_some_and(|backend_id| self.backends.remove(&backend_id).is_some())
    }

    fn has_oauth_credential_doc_id(&self, doc_id: &str) -> bool {
        self.oauth_credentials
            .values()
            .any(|record| record.doc_id == doc_id)
    }

    fn remove_oauth_credential_by_doc_id(&mut self, doc_id: &str) -> bool {
        let key = self
            .oauth_credentials
            .iter()
            .find_map(|(credential_id, record)| {
                (record.doc_id == doc_id).then_some(credential_id.clone())
            });
        key.is_some_and(|credential_id| self.oauth_credentials.remove(&credential_id).is_some())
    }

    pub(super) fn has_enabled_oauth_credential(&self, provider: &str) -> bool {
        self.oauth_credentials
            .values()
            .any(|record| record.value.provider == provider && record.value.enabled)
    }

    fn remove_task_by_doc_id(&mut self, doc_id: &str) -> bool {
        let key = self
            .tasks
            .iter()
            .find_map(|(task_id, record)| (record.doc_id == doc_id).then_some(task_id.clone()));
        key.is_some_and(|task_id| self.tasks.remove(&task_id).is_some())
    }

    fn remove_schedule_by_doc_id(&mut self, doc_id: &str) -> bool {
        let key = self.schedules.iter().find_map(|(schedule_id, record)| {
            (record.doc_id == doc_id).then_some(schedule_id.clone())
        });
        key.is_some_and(|schedule_id| self.schedules.remove(&schedule_id).is_some())
    }

    fn remove_event_trigger_by_doc_id(&mut self, doc_id: &str) -> bool {
        let key = self.event_triggers.iter().find_map(|(trigger_id, record)| {
            (record.doc_id == doc_id).then_some(trigger_id.clone())
        });
        key.is_some_and(|trigger_id| self.event_triggers.remove(&trigger_id).is_some())
    }

    fn references_profile(&self, profile_id: &str) -> bool {
        self.behaviors.values().any(|record| {
            record
                .value
                .inference_profile_id
                .as_deref()
                .is_some_and(|id| id == profile_id)
        })
    }

    fn references_backend(&self, backend_id: &str) -> bool {
        self.behaviors.values().any(|record| {
            record
                .value
                .backend_id
                .as_deref()
                .is_some_and(|id| id == backend_id)
        })
    }

    pub(crate) fn has_unresolved_behavior_references(&self) -> bool {
        !self.pending_visibility_details().is_empty()
    }

    pub(crate) fn pending_visibility_details(&self) -> Vec<String> {
        let mut details = Vec::new();

        if let Some(default_behavior_id) = self
            .principal
            .value
            .default_behavior_id
            .as_deref()
            .and_then(snapshot::non_empty)
        {
            if !self.behaviors.contains_key(default_behavior_id) {
                details.push(format!(
                    "principal {} references missing default behavior {}",
                    self.principal.value.agent_did, default_behavior_id
                ));
            }
        }

        for record in self.behaviors.values() {
            snapshot::collect_unresolved_behavior_references(self, &record.value, &mut details);
        }

        details.sort();
        details
    }
}

fn validate_subagent_targets_resolve(
    selection: &ToolSelectionDocument,
    view: &DocumentRuntimeView,
) -> anyhow::Result<()> {
    let own_agent_did = view.principal.value.agent_did.as_str();
    for entry in selection.subagent_targets.iter().flatten() {
        let target = crate::document_config::SubagentTarget::parse(entry).map_err(|error| {
            anyhow::anyhow!(
                "ToolSelection {} subagent_targets entry {entry:?} is not a valid SubagentTarget JSON: {error}",
                selection.selection_id,
            )
        })?;
        if !target.is_structurally_valid() {
            anyhow::bail!(
                "ToolSelection {} subagent_targets entry {entry:?} has empty name/agent_did/behavior_id",
                selection.selection_id,
            );
        }
        if target.agent_did == own_agent_did && !view.behaviors.contains_key(&target.behavior_id) {
            anyhow::bail!(
                "ToolSelection {} subagent_targets entry {entry:?} names a local behavior that does not resolve to an AgentBehavior",
                selection.selection_id,
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;

/// Merge inline `write_tools` with entries from linked `DatastoreToolSurface`
/// docs. Fail-closed on missing/disabled/foreign surfaces and name collisions.
pub(crate) fn merge_write_tools_with_surfaces(
    selection: &ToolSelectionDocument,
    view: &DocumentRuntimeView,
) -> anyhow::Result<Vec<WriteToolDecl>> {
    use anyhow::{anyhow, bail};
    use std::collections::HashSet;

    let mut decls = selection.write_tools.clone().unwrap_or_default();
    let mut seen: HashSet<String> = HashSet::new();
    for decl in &decls {
        if !seen.insert(decl.tool_name.clone()) {
            bail!(
                "ToolSelection {} has duplicate write_tools tool_name {:?}",
                selection.selection_id,
                decl.tool_name
            );
        }
    }

    let surface_ids = selection
        .datastore_tool_surface_ids
        .as_deref()
        .unwrap_or(&[]);
    for surface_id in surface_ids {
        let surface_id = surface_id.trim();
        if surface_id.is_empty() {
            bail!(
                "ToolSelection {} has an empty datastore_tool_surface_ids entry",
                selection.selection_id
            );
        }
        let record = view
            .datastore_tool_surfaces
            .get(surface_id)
            .ok_or_else(|| {
                anyhow!(
                    "ToolSelection {} references missing DatastoreToolSurface {}",
                    selection.selection_id,
                    surface_id
                )
            })?;
        let surface = &record.value;
        if surface.agent_did.trim() != selection.agent_did.trim() {
            bail!(
                "ToolSelection {} references DatastoreToolSurface {} owned by a different agent",
                selection.selection_id,
                surface_id
            );
        }
        if !surface.enabled {
            bail!(
                "ToolSelection {} references disabled DatastoreToolSurface {}",
                selection.selection_id,
                surface_id
            );
        }
        for entry in surface.entries.as_deref().unwrap_or(&[]) {
            if !entry.is_well_formed() {
                bail!(
                    "DatastoreToolSurface {} has a malformed entry (tool_name/collection required)",
                    surface_id
                );
            }
            if !seen.insert(entry.tool_name.clone()) {
                bail!(
                    "duplicate write tool_name {:?} after expanding DatastoreToolSurface {} for ToolSelection {}",
                    entry.tool_name,
                    surface_id,
                    selection.selection_id
                );
            }
            decls.push(entry.clone());
        }
    }
    Ok(decls)
}
