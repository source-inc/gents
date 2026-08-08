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
    AgentBehavior, AgentPrincipal, EventTrigger, InferenceProfile, Schedule, SkillDocument, Task,
    ToolSelectionDocument,
};

#[derive(Debug, Clone)]
pub(crate) struct DocumentRecord<T> {
    pub(crate) doc_id: String,
    pub(crate) fact: crate::ConfigFactRef,
    pub(crate) value: T,
}

impl<T> DocumentRecord<T> {
    fn from_verified_fact(fact: crate::ConfigFactRef, value: T) -> anyhow::Result<Self> {
        let doc_id = fact.source.version.doc_id.clone();
        if doc_id.trim().is_empty() {
            anyhow::bail!("verified configuration document has an empty _docID");
        }
        Ok(Self {
            doc_id,
            fact,
            value,
        })
    }
}

/// Documents that participate in runtime operation but not provider
/// configuration provenance in this slice.
#[derive(Debug, Clone)]
pub(crate) struct UnversionedDocumentRecord<T> {
    pub(crate) doc_id: String,
    pub(crate) value: T,
}

#[derive(Debug, Clone)]
pub(crate) struct DocumentRuntimeView {
    pub(crate) principal: DocumentRecord<AgentPrincipal>,
    pub(crate) behaviors: HashMap<String, DocumentRecord<AgentBehavior>>,
    pub(crate) skills: HashMap<String, DocumentRecord<SkillDocument>>,
    pub(crate) tool_selections: HashMap<String, DocumentRecord<ToolSelectionDocument>>,
    pub(crate) inference_profiles: HashMap<String, DocumentRecord<InferenceProfile>>,
    pub(crate) backends: HashMap<String, DocumentRecord<InferenceBackend>>,
    pub(crate) oauth_credentials: HashMap<String, UnversionedDocumentRecord<OAuthCredential>>,
    pub(crate) tasks: HashMap<String, UnversionedDocumentRecord<Task>>,
    pub(crate) schedules: HashMap<String, UnversionedDocumentRecord<Schedule>>,
    pub(crate) event_triggers: HashMap<String, UnversionedDocumentRecord<EventTrigger>>,
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

    fn remove_skill_by_doc_id(&mut self, doc_id: &str) -> bool {
        let key = self
            .skills
            .iter()
            .find_map(|(skill_id, record)| (record.doc_id == doc_id).then_some(skill_id.clone()));
        key.is_some_and(|skill_id| self.skills.remove(&skill_id).is_some())
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
