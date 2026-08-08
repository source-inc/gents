use std::collections::BTreeMap;

use gents::apply_model::{self, DesiredFields, DocRef, LiveState, Manifest};
use gents::Collection;

use super::DesiredStateManifest;

fn doc(collection: Collection, id: &str) -> DocRef {
    DocRef {
        collection,
        id: id.to_string(),
    }
}

fn live_state_from_manifest(m: &DesiredStateManifest) -> LiveState {
    let mut desired: BTreeMap<DocRef, DesiredFields> = BTreeMap::new();

    let principal_refs = m
        .agent_principal
        .default_behavior_id
        .as_deref()
        .map(|b| vec![doc(Collection::AgentBehavior, b)])
        .unwrap_or_default();
    desired.insert(
        doc(Collection::AgentPrincipal, &m.agent_principal.agent_did),
        DesiredFields::with_refs("", principal_refs),
    );

    for b in &m.agent_behaviors {
        let mut refs = Vec::new();
        if let Some(x) = &b.backend_id {
            refs.push(doc(Collection::InferenceBackend, x));
        }
        if let Some(x) = &b.inference_profile_id {
            refs.push(doc(Collection::InferenceProfile, x));
        }
        if let Some(x) = &b.tool_selection_id {
            refs.push(doc(Collection::ToolSelection, x));
        }
        for s in &b.skill_refs {
            refs.push(doc(Collection::Skill, s));
        }
        desired.insert(
            doc(Collection::AgentBehavior, &b.behavior_id),
            DesiredFields::with_refs("", refs),
        );
    }

    for t in &m.tasks {
        desired.insert(
            doc(Collection::Task, &t.task_id),
            DesiredFields::with_refs("", vec![doc(Collection::AgentBehavior, &t.behavior_id)]),
        );
    }

    for s in &m.schedules {
        desired.insert(
            doc(Collection::Schedule, &s.schedule_id),
            DesiredFields::with_refs("", vec![doc(Collection::Task, &s.task_id)]),
        );
    }

    for e in &m.event_triggers {
        desired.insert(
            doc(Collection::EventTrigger, &e.trigger_id),
            DesiredFields::with_refs("", vec![doc(Collection::Task, &e.task_id)]),
        );
    }

    for ts in &m.tool_selections {
        let mut refs = Vec::new();
        for sid in &ts.allowed_mcp_service_ids {
            refs.push(doc(Collection::ToolServiceRegistry, sid));
        }
        for sid in &ts.datastore_tool_surface_ids {
            refs.push(doc(Collection::DatastoreToolSurface, sid));
        }
        desired.insert(
            doc(Collection::ToolSelection, &ts.selection_id),
            DesiredFields::with_refs("", refs),
        );
    }

    for binding in &m.projection_acp_bindings {
        let refs = binding
            .behavior_id
            .as_deref()
            .map(|behavior_id| vec![doc(Collection::AgentBehavior, behavior_id)])
            .unwrap_or_default();
        desired.insert(
            doc(Collection::ProjectionAcpBinding, &binding.binding_id),
            DesiredFields::with_refs("", refs),
        );
    }

    for s in &m.skills {
        desired.insert(
            doc(Collection::Skill, &s.skill_id),
            DesiredFields::opaque(""),
        );
    }
    for s in &m.datastore_tool_surfaces {
        desired.insert(
            doc(Collection::DatastoreToolSurface, &s.surface_id),
            DesiredFields::opaque(""),
        );
    }
    for b in &m.inference_backends {
        desired.insert(
            doc(Collection::InferenceBackend, &b.backend_id),
            DesiredFields::opaque(""),
        );
    }
    for p in &m.inference_profiles {
        desired.insert(
            doc(Collection::InferenceProfile, &p.profile_id),
            DesiredFields::opaque(""),
        );
    }
    for r in &m.tool_service_registries {
        desired.insert(
            doc(Collection::ToolServiceRegistry, &r.service_id),
            DesiredFields::opaque(""),
        );
    }

    LiveState {
        desired,
        live: BTreeMap::new(),
    }
}

fn manifest_from_desired(m: &DesiredStateManifest) -> Manifest {
    let mut docs: BTreeMap<DocRef, DesiredFields> = BTreeMap::new();
    docs.insert(
        doc(Collection::AgentPrincipal, &m.agent_principal.agent_did),
        DesiredFields::opaque(""),
    );
    for b in &m.agent_behaviors {
        docs.insert(
            doc(Collection::AgentBehavior, &b.behavior_id),
            DesiredFields::opaque(""),
        );
    }
    for s in &m.skills {
        docs.insert(
            doc(Collection::Skill, &s.skill_id),
            DesiredFields::opaque(""),
        );
    }
    for s in &m.datastore_tool_surfaces {
        docs.insert(
            doc(Collection::DatastoreToolSurface, &s.surface_id),
            DesiredFields::opaque(""),
        );
    }
    for ts in &m.tool_selections {
        docs.insert(
            doc(Collection::ToolSelection, &ts.selection_id),
            DesiredFields::opaque(""),
        );
    }
    for b in &m.inference_backends {
        docs.insert(
            doc(Collection::InferenceBackend, &b.backend_id),
            DesiredFields::opaque(""),
        );
    }
    for p in &m.inference_profiles {
        docs.insert(
            doc(Collection::InferenceProfile, &p.profile_id),
            DesiredFields::opaque(""),
        );
    }
    for r in &m.tool_service_registries {
        docs.insert(
            doc(Collection::ToolServiceRegistry, &r.service_id),
            DesiredFields::opaque(""),
        );
    }
    for t in &m.tasks {
        docs.insert(doc(Collection::Task, &t.task_id), DesiredFields::opaque(""));
    }
    for s in &m.schedules {
        docs.insert(
            doc(Collection::Schedule, &s.schedule_id),
            DesiredFields::opaque(""),
        );
    }
    for e in &m.event_triggers {
        docs.insert(
            doc(Collection::EventTrigger, &e.trigger_id),
            DesiredFields::opaque(""),
        );
    }
    for binding in &m.projection_acp_bindings {
        docs.insert(
            doc(Collection::ProjectionAcpBinding, &binding.binding_id),
            DesiredFields::opaque(""),
        );
    }
    Manifest { docs }
}

pub(crate) fn prune_safe_deletes(
    desired: &DesiredStateManifest,
    live: &DesiredStateManifest,
) -> Vec<DocRef> {
    let m = manifest_from_desired(desired);
    let l = live_state_from_manifest(live);
    apply_model::diff_prune(&m, &l).delete
}
