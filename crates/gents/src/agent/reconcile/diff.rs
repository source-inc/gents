use crate::runtime_snapshot::{ActiveRuntimeSnapshot, ResolvedRuntimeSnapshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SnapshotDiffCounts {
    pub(super) added: usize,
    pub(super) removed: usize,
    pub(super) updated: usize,
    pub(super) default_changed: bool,
    pub(super) unavailable_changed: bool,
}

pub(super) fn diff_counts(
    current: &ActiveRuntimeSnapshot,
    proposed: &ResolvedRuntimeSnapshot,
) -> SnapshotDiffCounts {
    let current_ids = current
        .behaviors
        .keys()
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    let proposed_ids = proposed
        .behaviors
        .keys()
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    let added = proposed_ids.difference(&current_ids).count();
    let removed = current_ids.difference(&proposed_ids).count();
    let updated = current_ids
        .intersection(&proposed_ids)
        .filter(|behavior_id| {
            let current_behavior = current
                .behaviors
                .get(*behavior_id)
                .expect("intersection must exist in current behaviors");
            let proposed_behavior = proposed
                .behaviors
                .get(*behavior_id)
                .expect("intersection must exist in proposed behaviors");
            format!("{current_behavior:?}") != format!("{proposed_behavior:?}")
                || current.config_provenance_scope != proposed.config_provenance_scope
                || current
                    .behavior_config_provenance
                    .get(*behavior_id)
                    .map(AsRef::as_ref)
                    != proposed
                        .behavior_config_provenance
                        .get(*behavior_id)
                        .map(AsRef::as_ref)
                || match (
                    current.tool_surfaces.get(*behavior_id),
                    proposed.tool_surfaces.get(*behavior_id),
                ) {
                    (Some(current_tools), Some(proposed_tools)) => {
                        format!("{current_tools:?}") != format!("{proposed_tools:?}")
                    }
                    _ => true,
                }
        })
        .count();

    SnapshotDiffCounts {
        added,
        removed,
        updated,
        default_changed: current.default_behavior_id != proposed.default_behavior_id,
        unavailable_changed: current.unavailable_behaviors != proposed.unavailable_behaviors,
    }
}
