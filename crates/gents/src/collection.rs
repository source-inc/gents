//! Typed discriminator for the set of operator-controlled collections.
//!
//! Mirrors the Lean inductive `ApplyReconcile.Collection` in
//! `crates/gents/proofs/Proofs/ApplyReconcile.lean`. Any change
//! to the set of variants, their GraphQL names, or their apply-order
//! ranks must be reflected in the Lean module.

use std::fmt;

// PartialOrd/Ord derived for BTreeMap<DocRef, _> use in apply_model; ordering
// is declaration order, NOT apply_order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Collection {
    AgentPrincipal,
    AgentBehavior,
    Skill,
    DatastoreToolSurface,
    WorkspaceRoot,
    ToolSelection,
    InferenceBackend,
    InferenceProfile,
    ToolServiceRegistry,
    ProjectionAcpBinding,
    PeerPairingDesired,
    Task,
    Schedule,
    EventTrigger,
}

impl Collection {
    // NOTE: `WorkspaceRoot` is intentionally NOT a member of `ALL`. `ALL`
    // drives the full desired-state config CRUD surface (CONFIG_APPLY_ORDER,
    // DesiredStateManifest diff/load/write) — WorkspaceRoot's schema lands in
    // this task, but that CLI surface is built in a follow-up task. The
    // variant still exists on the enum (with real graphql_type/unique_field/
    // apply_order/dir_name) so exhaustive matches over `Collection` account
    // for it; it's just excluded from the CRUD-driving `ALL` set for now.
    pub const ALL: [Collection; 13] = [
        Collection::AgentPrincipal,
        Collection::AgentBehavior,
        Collection::Skill,
        Collection::DatastoreToolSurface,
        Collection::ToolSelection,
        Collection::InferenceBackend,
        Collection::InferenceProfile,
        Collection::ToolServiceRegistry,
        Collection::ProjectionAcpBinding,
        Collection::PeerPairingDesired,
        Collection::Task,
        Collection::Schedule,
        Collection::EventTrigger,
    ];

    pub fn file_name(self) -> Option<&'static str> {
        match self {
            Collection::AgentPrincipal => Some("agent-principal.json"),
            _ => None,
        }
    }

    pub fn dir_name(self) -> Option<&'static str> {
        match self {
            Collection::AgentPrincipal => None,
            Collection::AgentBehavior => Some("agent-behaviors"),
            Collection::Skill => Some("skills"),
            Collection::DatastoreToolSurface => Some("datastore-tool-surfaces"),
            Collection::WorkspaceRoot => Some("workspace-roots"),
            Collection::ToolSelection => Some("tool-selections"),
            Collection::InferenceBackend => Some("inference-backends"),
            Collection::InferenceProfile => Some("inference-profiles"),
            Collection::ToolServiceRegistry => Some("tool-services"),
            Collection::ProjectionAcpBinding => Some("projection-acp-bindings"),
            Collection::PeerPairingDesired => Some("peer-pairings"),
            Collection::Task => Some("tasks"),
            Collection::Schedule => Some("schedules"),
            Collection::EventTrigger => Some("event_triggers"),
        }
    }

    pub fn graphql_type(self) -> &'static str {
        match self {
            Collection::AgentPrincipal => "AgentPrincipal",
            Collection::AgentBehavior => "AgentBehavior",
            Collection::Skill => "Skill",
            Collection::DatastoreToolSurface => "DatastoreToolSurface",
            Collection::WorkspaceRoot => "WorkspaceRoot",
            Collection::ToolSelection => "ToolSelection",
            Collection::InferenceBackend => "InferenceBackend",
            Collection::InferenceProfile => "InferenceProfile",
            Collection::ToolServiceRegistry => "ToolServiceRegistry",
            Collection::ProjectionAcpBinding => "ProjectionAcpBinding",
            Collection::PeerPairingDesired => "PeerPairingDesired",
            Collection::Task => "Task",
            Collection::Schedule => "Schedule",
            Collection::EventTrigger => "EventTrigger",
        }
    }

    pub fn unique_field(self) -> &'static str {
        match self {
            Collection::AgentPrincipal => "agent_did",
            Collection::AgentBehavior => "behavior_id",
            Collection::Skill => "skill_id",
            Collection::DatastoreToolSurface => "surface_id",
            Collection::WorkspaceRoot => "root_path",
            Collection::ToolSelection => "selection_id",
            Collection::InferenceBackend => "backend_id",
            Collection::InferenceProfile => "profile_id",
            Collection::ToolServiceRegistry => "service_id",
            Collection::ProjectionAcpBinding => "binding_id",
            Collection::PeerPairingDesired => "peer_id",
            Collection::Task => "task_id",
            Collection::Schedule => "schedule_id",
            Collection::EventTrigger => "trigger_id",
        }
    }

    /// Apply ordering rank: lower ranks are written first so referenced
    /// documents exist before referrers. Mirrors
    /// `ApplyReconcile.Collection.applyOrder` in Lean.
    pub fn apply_order(self) -> u8 {
        match self {
            Collection::InferenceBackend
            | Collection::ToolSelection
            | Collection::InferenceProfile
            | Collection::ToolServiceRegistry
            | Collection::Skill
            | Collection::DatastoreToolSurface
            | Collection::WorkspaceRoot
            | Collection::PeerPairingDesired => 0,
            Collection::AgentBehavior => 1,
            Collection::ProjectionAcpBinding => 2,
            Collection::Task => 2,
            Collection::Schedule => 2,
            Collection::AgentPrincipal => 3,
            Collection::EventTrigger => 3,
        }
    }

    pub fn manifest_authoritative(self) -> bool {
        matches!(self, Collection::PeerPairingDesired)
    }
}

impl fmt::Display for Collection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Collection::AgentPrincipal => "agent_principal",
            Collection::AgentBehavior => "agent_behaviors",
            Collection::Skill => "skills",
            Collection::DatastoreToolSurface => "datastore_tool_surfaces",
            Collection::WorkspaceRoot => "workspace_roots",
            Collection::ToolSelection => "tool_selections",
            Collection::InferenceBackend => "inference_backends",
            Collection::InferenceProfile => "inference_profiles",
            Collection::ToolServiceRegistry => "tool_service_registries",
            Collection::ProjectionAcpBinding => "projection_acp_bindings",
            Collection::PeerPairingDesired => "peer_pairings",
            Collection::Task => "tasks",
            Collection::Schedule => "schedules",
            Collection::EventTrigger => "event_triggers",
        };
        f.write_str(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn all_collections_have_distinct_file_or_dir_names() {
        let names: BTreeSet<&str> = Collection::ALL
            .iter()
            .map(|c| {
                c.file_name()
                    .or(c.dir_name())
                    .expect("every variant has one")
            })
            .collect();
        assert_eq!(names.len(), Collection::ALL.len());
    }

    #[test]
    fn all_collections_have_distinct_graphql_types() {
        let names: BTreeSet<&str> = Collection::ALL.iter().map(|c| c.graphql_type()).collect();
        assert_eq!(names.len(), Collection::ALL.len());
    }

    #[test]
    fn all_collections_have_distinct_display_strings() {
        let names: BTreeSet<String> = Collection::ALL.iter().map(|c| c.to_string()).collect();
        assert_eq!(names.len(), Collection::ALL.len());
    }

    #[test]
    fn peer_pairings_are_the_only_manifest_authoritative_collection() {
        let authoritative = Collection::ALL
            .into_iter()
            .filter(|collection| collection.manifest_authoritative())
            .collect::<Vec<_>>();
        assert_eq!(authoritative, vec![Collection::PeerPairingDesired]);
    }

    #[test]
    fn canonical_variants_and_ranks() {
        // This list is the Rust side of the parity contract. The Lean
        // inductive `ApplyReconcile.Collection` and the
        // `ApplyReconcile.Collection.applyOrder` function in
        // crates/gents/proofs/Proofs/ApplyReconcile.lean must
        // match this sequence exactly. When you add a variant here, you
        // MUST also:
        //
        // 1. Add the variant to the Lean inductive.
        // 2. Add the variant's rank to Collection.applyOrder in Lean.
        // 3. Update the exhaustive pattern-match example at the bottom
        //    of ApplyReconcile.lean (added in Task A4 alongside this test).
        //
        // Both the Lean build and this test must stay green.
        let canonical: &[(Collection, u8, &str)] = &[
            (Collection::AgentPrincipal, 3, "AgentPrincipal"),
            (Collection::AgentBehavior, 1, "AgentBehavior"),
            (Collection::Skill, 0, "Skill"),
            (Collection::ToolSelection, 0, "ToolSelection"),
            (Collection::InferenceBackend, 0, "InferenceBackend"),
            (Collection::InferenceProfile, 0, "InferenceProfile"),
            (Collection::ToolServiceRegistry, 0, "ToolServiceRegistry"),
            (Collection::ProjectionAcpBinding, 2, "ProjectionAcpBinding"),
            (Collection::PeerPairingDesired, 0, "PeerPairingDesired"),
            (Collection::Task, 2, "Task"),
            (Collection::Schedule, 2, "Schedule"),
            (Collection::EventTrigger, 3, "EventTrigger"),
        ];

        // ALL must list every canonical variant exactly once.
        assert_eq!(Collection::ALL.len(), canonical.len());
        for (variant, _, _) in canonical.iter() {
            assert!(
                Collection::ALL.contains(variant),
                "Collection::ALL missing variant {variant:?}; \
                 see ApplyReconcile.lean parity contract"
            );
        }

        // apply_order and graphql_type must match the canonical values.
        for (variant, expected_rank, expected_type) in canonical.iter() {
            assert_eq!(
                variant.apply_order(),
                *expected_rank,
                "Collection::{variant:?}.apply_order() drifted from Lean parity contract"
            );
            assert_eq!(
                variant.graphql_type(),
                *expected_type,
                "Collection::{variant:?}.graphql_type() drifted from Lean parity contract"
            );
        }
    }

    #[test]
    fn exactly_one_of_file_or_dir_name() {
        for variant in Collection::ALL {
            let has_file = variant.file_name().is_some();
            let has_dir = variant.dir_name().is_some();
            assert!(
                has_file ^ has_dir,
                "Collection::{variant:?} must return Some from exactly one of file_name()/dir_name()"
            );
        }
    }

    #[test]
    fn apply_order_puts_referees_before_referrers() {
        assert!(
            Collection::InferenceBackend.apply_order() < Collection::AgentBehavior.apply_order()
        );
        assert!(Collection::ToolSelection.apply_order() < Collection::AgentBehavior.apply_order());
        assert!(
            Collection::InferenceProfile.apply_order() < Collection::AgentBehavior.apply_order()
        );
        assert!(Collection::AgentBehavior.apply_order() < Collection::Task.apply_order());
        assert!(Collection::AgentBehavior.apply_order() < Collection::Schedule.apply_order());
        assert!(
            Collection::AgentBehavior.apply_order()
                < Collection::ProjectionAcpBinding.apply_order()
        );
        // Rank-0 members must all agree on rank 0.
        assert_eq!(
            Collection::InferenceBackend.apply_order(),
            Collection::ToolSelection.apply_order(),
        );
        assert_eq!(
            Collection::InferenceBackend.apply_order(),
            Collection::InferenceProfile.apply_order(),
        );
        assert_eq!(
            Collection::InferenceBackend.apply_order(),
            Collection::ToolServiceRegistry.apply_order(),
        );
    }
}
