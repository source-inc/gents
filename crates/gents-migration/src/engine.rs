//! Locate-in-chain → attach → patch-inactive → verify → activate.
//!
//! Phase A ships a zero-step chain: register baseline, enforce single-version
//! DAGs, verify expectations. Step application paths are implemented for the
//! registry enum so Phase B only adds data.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use defra_node::{CollectionVersion, EmbeddedNode};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::error::{Error, Result};
use crate::lens;
use crate::materialize;
use crate::registry::{
    BaselineCollection, DynamicRegistry, MigrationStep, Registry, DEFAULT_REGISTRY,
};
use crate::report::MigrationReport;

/// Process-wide lock: desktop and runtime paths can race within one process.
static ENSURE_LOCK: Mutex<()> = Mutex::const_new(());

/// Single schema entry point: baseline + chain + verify + materialize.
///
/// Idempotent, resumable, and cheap when current. Every host calls this on
/// every start.
pub async fn ensure_migrations(node: &EmbeddedNode) -> Result<MigrationReport> {
    ensure_migrations_with_registry(node, &DEFAULT_REGISTRY).await
}

/// Testable entry: inject a custom registry.
pub async fn ensure_migrations_with_registry(
    node: &EmbeddedNode,
    registry: &Registry<'_>,
) -> Result<MigrationReport> {
    let _guard = ENSURE_LOCK.lock().await;
    let mut report = MigrationReport::default();

    register_baseline(node, registry, &mut report).await?;
    apply_steps(node, registry, &mut report).await?;
    verify_managed_lineages(node, registry, &mut report).await?;

    report.materialization = materialize::materialize_all(node, registry).await?;
    report.warnings.extend(
        report
            .materialization
            .parked_unique_conflicts
            .iter()
            .cloned(),
    );

    info!(
        baseline_registered = report.baseline_registered,
        baseline_already_present = report.baseline_already_present,
        steps_applied = report.steps_applied,
        steps_already_current = report.steps_already_current,
        edges_repaired = report.edges_repaired,
        documents_materialized = report.materialization.documents_materialized,
        "ensure_migrations complete"
    );

    Ok(report)
}

/// Convenience for call sites that use `Arc<EmbeddedNode>` and ignore the report.
pub async fn ensure_migrations_arc(node: Arc<EmbeddedNode>) -> Result<MigrationReport> {
    ensure_migrations(node.as_ref()).await
}

/// Ensure against a heap-owned registry (pin authoring / conformance tests).
pub async fn ensure_migrations_dynamic(
    node: &EmbeddedNode,
    registry: &DynamicRegistry,
) -> Result<MigrationReport> {
    let (baseline, steps) = registry.as_registry();
    let view = Registry {
        baseline: &baseline,
        steps: &steps,
    };
    ensure_migrations_with_registry(node, &view).await
}

// ---------------------------------------------------------------------------
// Baseline
// ---------------------------------------------------------------------------

async fn register_baseline(
    node: &EmbeddedNode,
    registry: &Registry<'_>,
    report: &mut MigrationReport,
) -> Result<()> {
    for entry in registry.baseline {
        match node.add_schema(entry.sdl).await {
            Ok(()) => {
                report.baseline_registered += 1;
                debug!(collection = entry.name, "baseline schema registered");
            }
            Err(error) => {
                if error.to_string().contains("already exists") {
                    report.baseline_already_present += 1;
                    debug!(collection = entry.name, "baseline schema already present");
                } else {
                    return Err(Error::BaselineRegister {
                        collection: entry.name.to_string(),
                        source: error,
                    });
                }
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Steps (Phase B surface; Phase A steps is empty)
// ---------------------------------------------------------------------------

async fn apply_steps(
    node: &EmbeddedNode,
    registry: &Registry<'_>,
    report: &mut MigrationReport,
) -> Result<()> {
    for step in registry.steps {
        match step {
            MigrationStep::AddCollection {
                id,
                sdl,
                expected_version,
                expected_state,
            } => {
                apply_add_collection(node, id, sdl, *expected_version, expected_state, report)
                    .await?;
            }
            MigrationStep::PatchVersioned {
                id,
                collection,
                patch,
                lens: lens_spec,
                expected_version,
                expected_transform,
                expected_state,
            } => {
                apply_patch_versioned(
                    node,
                    id,
                    collection,
                    patch,
                    *lens_spec,
                    *expected_version,
                    *expected_transform,
                    expected_state,
                    report,
                )
                .await?;
            }
            MigrationStep::PatchInPlace {
                id,
                collection,
                patch,
                expected_state,
            } => {
                apply_patch_in_place(node, id, collection, patch, expected_state, report).await?;
            }
        }
    }
    Ok(())
}

async fn apply_add_collection(
    node: &EmbeddedNode,
    id: &str,
    sdl: &str,
    expected_version: Option<&str>,
    expected_state: &crate::expectation::CollectionExpectation,
    report: &mut MigrationReport,
) -> Result<()> {
    // Infer collection name from the first `type Name` in the SDL for lookups
    // after registration; callers still pin via expected_version/state.
    match node.add_schema(sdl).await {
        Ok(()) => report.steps_applied += 1,
        Err(error) if error.to_string().contains("already exists") => {
            report.steps_already_current += 1;
        }
        Err(error) => {
            return Err(Error::StepFailed {
                step: id.to_string(),
                collection: id.to_string(),
                source: error,
            });
        }
    }

    // If a pin is present, locate and verify the active version by scanning.
    if let Some(pin) = expected_version {
        let versions = node
            .get_all_collection_versions()
            .await
            .map_err(Error::Node)?;
        let Some(active) = versions
            .iter()
            .find(|v| v.version_id == pin && !v.is_placeholder)
        else {
            return Err(Error::VersionPinMismatch {
                collection: id.to_string(),
                expected: pin.to_string(),
                actual: "<missing>".into(),
            });
        };
        expected_state
            .verify(active)
            .map_err(|detail| Error::StateVerification {
                collection: id.to_string(),
                step: id.to_string(),
                detail,
            })?;
    }
    Ok(())
}

async fn apply_patch_versioned(
    node: &EmbeddedNode,
    id: &str,
    collection: &str,
    patch: &str,
    lens_spec: Option<crate::registry::LensSpec<'_>>,
    expected_version: Option<&str>,
    expected_transform: Option<&str>,
    expected_state: &crate::expectation::CollectionExpectation,
    report: &mut MigrationReport,
) -> Result<()> {
    let Some(pin) = expected_version else {
        return Err(Error::StepFailed {
            step: id.to_string(),
            collection: collection.to_string(),
            source: anyhow::anyhow!(
                "PatchVersioned steps require expected_version pin (design §8.1)"
            ),
        });
    };

    let active = node
        .get_collection(collection)
        .map_err(Error::Node)?
        .ok_or_else(|| Error::CollectionMissing {
            collection: collection.to_string(),
        })?;

    // Already at pin and active → verify + optional edge repair.
    if active.version_id == pin && active.is_active && !active.is_placeholder {
        verify_active_step(
            node,
            id,
            collection,
            pin,
            expected_transform,
            lens_spec,
            expected_state,
            report,
        )
        .await?;
        report.steps_already_current += 1;
        return Ok(());
    }

    // Locate destination state among all versions.
    let all = node
        .get_all_collection_versions()
        .await
        .map_err(Error::Node)?;
    let dest = all.iter().find(|v| v.version_id == pin);

    // A later step in the same collection may already be active. In that
    // state this destination is an inactive ancestor, not a crash-window
    // version waiting to be reactivated. Re-activating it would roll the
    // collection backward on every ensure before the later step rolls it
    // forward again.
    if let Some(dest) = dest {
        if !dest.is_active && version_descends_from(&all, &active.version_id, pin) {
            expected_state
                .verify(dest)
                .map_err(|detail| Error::StateVerification {
                    collection: collection.to_string(),
                    step: id.to_string(),
                    detail,
                })?;
            if let Some(expected_tx) = expected_transform {
                let source = dest
                    .previous_version
                    .as_ref()
                    .map(|previous| previous.source_collection_id.as_str())
                    .ok_or_else(|| Error::StateVerification {
                        collection: collection.to_string(),
                        step: id.to_string(),
                        detail: "versioned migration destination is missing its source edge"
                            .to_string(),
                    })?;
                repair_transform_if_needed(
                    node,
                    id,
                    collection,
                    source,
                    pin,
                    lens_spec,
                    expected_tx,
                    dest,
                    report,
                )
                .await?;
            }
            report.steps_already_current += 1;
            return Ok(());
        }
    }

    match dest {
        None => {
            // destination absent → attach (if lens), then patch inactive.
            if let Some(spec) = lens_spec {
                let cfg = lens::lens_config(&spec, &active.version_id, pin);
                node.set_migration(cfg)
                    .await
                    .map_err(|e| Error::StepFailed {
                        step: id.to_string(),
                        collection: collection.to_string(),
                        source: e,
                    })?;
            }
            let patched = node
                .patch_collection(collection, patch)
                .await
                .map_err(|e| Error::StepFailed {
                    step: id.to_string(),
                    collection: collection.to_string(),
                    source: e,
                })?;
            if patched.version_id != pin {
                return Err(Error::VersionPinMismatch {
                    collection: collection.to_string(),
                    expected: pin.to_string(),
                    actual: patched.version_id,
                });
            }
            expected_state
                .verify(&patched)
                .map_err(|detail| Error::StateVerification {
                    collection: collection.to_string(),
                    step: id.to_string(),
                    detail,
                })?;
            node.set_active_collection_version(pin)
                .await
                .map_err(|e| Error::StepFailed {
                    step: id.to_string(),
                    collection: collection.to_string(),
                    source: e,
                })?;
            report.steps_applied += 1;
        }
        Some(v) if v.is_placeholder => {
            // placeholder → patch inactive, verify, activate.
            let patched = node
                .patch_collection(collection, patch)
                .await
                .map_err(|e| Error::StepFailed {
                    step: id.to_string(),
                    collection: collection.to_string(),
                    source: e,
                })?;
            if patched.version_id != pin {
                return Err(Error::VersionPinMismatch {
                    collection: collection.to_string(),
                    expected: pin.to_string(),
                    actual: patched.version_id,
                });
            }
            expected_state
                .verify(&patched)
                .map_err(|detail| Error::StateVerification {
                    collection: collection.to_string(),
                    step: id.to_string(),
                    detail,
                })?;
            node.set_active_collection_version(pin)
                .await
                .map_err(|e| Error::StepFailed {
                    step: id.to_string(),
                    collection: collection.to_string(),
                    source: e,
                })?;
            report.steps_applied += 1;
        }
        Some(v) if !v.is_active => {
            // complete inactive → verify, activate.
            expected_state
                .verify(v)
                .map_err(|detail| Error::StateVerification {
                    collection: collection.to_string(),
                    step: id.to_string(),
                    detail,
                })?;
            if let Some(expected_tx) = expected_transform {
                repair_transform_if_needed(
                    node,
                    id,
                    collection,
                    &active.version_id,
                    pin,
                    lens_spec,
                    expected_tx,
                    v,
                    report,
                )
                .await?;
            }
            node.set_active_collection_version(pin)
                .await
                .map_err(|e| Error::StepFailed {
                    step: id.to_string(),
                    collection: collection.to_string(),
                    source: e,
                })?;
            report.steps_applied += 1;
        }
        Some(v) => {
            // complete active (active version_id may differ if name pointer lag)
            verify_active_step(
                node,
                id,
                collection,
                pin,
                expected_transform,
                lens_spec,
                expected_state,
                report,
            )
            .await?;
            if v.version_id == pin {
                // Post-activation repair: re-call set_active to re-trigger reindex.
                if let Err(e) = node.set_active_collection_version(pin).await {
                    warn!(
                        step = id,
                        collection,
                        error = %e,
                        "post-activation reindex repair failed"
                    );
                    report.warnings.push(format!(
                        "post-activation repair failed for {collection} step {id}: {e}"
                    ));
                }
            }
            report.steps_already_current += 1;
        }
    }

    Ok(())
}

fn version_descends_from(
    versions: &[CollectionVersion],
    descendant_version: &str,
    ancestor_version: &str,
) -> bool {
    let mut current = descendant_version;
    let mut visited = HashSet::new();
    while visited.insert(current) {
        let Some(version) = versions
            .iter()
            .find(|version| version.version_id == current)
        else {
            return false;
        };
        let Some(previous) = version.previous_version.as_ref() else {
            return false;
        };
        if previous.source_collection_id == ancestor_version {
            return true;
        }
        current = &previous.source_collection_id;
    }
    false
}

async fn verify_active_step(
    node: &EmbeddedNode,
    id: &str,
    collection: &str,
    pin: &str,
    expected_transform: Option<&str>,
    lens_spec: Option<crate::registry::LensSpec<'_>>,
    expected_state: &crate::expectation::CollectionExpectation,
    report: &mut MigrationReport,
) -> Result<()> {
    let active = node
        .get_collection(collection)
        .map_err(Error::Node)?
        .ok_or_else(|| Error::CollectionMissing {
            collection: collection.to_string(),
        })?;
    if active.version_id != pin {
        return Err(Error::VersionPinMismatch {
            collection: collection.to_string(),
            expected: pin.to_string(),
            actual: active.version_id,
        });
    }
    expected_state
        .verify(&active)
        .map_err(|detail| Error::StateVerification {
            collection: collection.to_string(),
            step: id.to_string(),
            detail,
        })?;
    if let Some(expected_tx) = expected_transform {
        let prev = active.version_id.clone();
        // Need source of this edge: previous_version.source
        if let Some(ref pv) = active.previous_version {
            let source = pv.source_collection_id.clone();
            repair_transform_if_needed(
                node,
                id,
                collection,
                &source,
                pin,
                lens_spec,
                expected_tx,
                &active,
                report,
            )
            .await?;
            let _ = prev;
        }
    }
    Ok(())
}

async fn repair_transform_if_needed(
    node: &EmbeddedNode,
    id: &str,
    collection: &str,
    source: &str,
    dest: &str,
    lens_spec: Option<crate::registry::LensSpec<'_>>,
    expected_tx: &str,
    version: &CollectionVersion,
    report: &mut MigrationReport,
) -> Result<()> {
    let current = version
        .previous_version
        .as_ref()
        .and_then(|pv| pv.transform.as_deref());
    if current == Some(expected_tx) {
        return Ok(());
    }
    let Some(spec) = lens_spec else {
        // Cannot repair without wasm bytes; surface as verification failure.
        return Err(Error::StateVerification {
            collection: collection.to_string(),
            step: id.to_string(),
            detail: format!(
                "transform missing or mismatched (have {current:?}, want {expected_tx}) and no lens bytes to repair"
            ),
        });
    };
    let cfg = lens::lens_config(&spec, source, dest);
    node.set_migration(cfg)
        .await
        .map_err(|e| Error::StepFailed {
            step: id.to_string(),
            collection: collection.to_string(),
            source: e,
        })?;
    report.edges_repaired += 1;
    info!(
        step = id,
        collection, source, dest, "repaired missing migration edge transform"
    );
    Ok(())
}

async fn apply_patch_in_place(
    node: &EmbeddedNode,
    id: &str,
    collection: &str,
    patch: &str,
    expected_state: &crate::expectation::CollectionExpectation,
    report: &mut MigrationReport,
) -> Result<()> {
    let active = node
        .get_collection(collection)
        .map_err(Error::Node)?
        .ok_or_else(|| Error::CollectionMissing {
            collection: collection.to_string(),
        })?;

    if expected_state.verify(&active).is_ok() {
        report.steps_already_current += 1;
        return Ok(());
    }

    let patched = node
        .patch_collection(collection, patch)
        .await
        .map_err(|e| Error::StepFailed {
            step: id.to_string(),
            collection: collection.to_string(),
            source: e,
        })?;
    expected_state
        .verify(&patched)
        .map_err(|detail| Error::StateVerification {
            collection: collection.to_string(),
            step: id.to_string(),
            detail,
        })?;
    report.steps_applied += 1;
    Ok(())
}

// ---------------------------------------------------------------------------
// Lineage verification (entire DAG of every managed collection)
// ---------------------------------------------------------------------------

async fn verify_managed_lineages(
    node: &EmbeddedNode,
    registry: &Registry<'_>,
    _report: &mut MigrationReport,
) -> Result<()> {
    let all_versions = node
        .get_all_collection_versions()
        .await
        .map_err(Error::Node)?;

    // Group non-placeholder versions by collection name.
    let mut by_name: HashMap<String, Vec<&CollectionVersion>> = HashMap::new();
    for v in &all_versions {
        if v.is_placeholder || v.name.is_empty() {
            continue;
        }
        by_name.entry(v.name.clone()).or_default().push(v);
    }

    let known_pins = known_pin_set(registry);
    let target_active = target_active_pins(registry);

    for entry in registry.baseline {
        verify_one_collection(entry, &by_name, &known_pins, &target_active)?;
    }

    // AddCollection steps also manage collections that may not be in baseline.
    for step in registry.steps {
        if let MigrationStep::AddCollection {
            id: _,
            sdl: _,
            expected_version,
            expected_state,
        } = step
        {
            if let Some(pin) = expected_version {
                let Some(v) = all_versions.iter().find(|v| v.version_id == *pin) else {
                    return Err(Error::VersionPinMismatch {
                        collection: step.id().to_string(),
                        expected: pin.to_string(),
                        actual: "<missing>".into(),
                    });
                };
                expected_state
                    .verify(v)
                    .map_err(|detail| Error::StateVerification {
                        collection: v.name.clone(),
                        step: step.id().to_string(),
                        detail,
                    })?;
            }
        }
    }

    Ok(())
}

fn known_pin_set(registry: &Registry<'_>) -> HashSet<String> {
    let mut pins = HashSet::new();
    for b in registry.baseline {
        if let Some(v) = b.expected_version {
            pins.insert(v.to_string());
        }
    }
    for step in registry.steps {
        match step {
            MigrationStep::AddCollection {
                expected_version: Some(v),
                ..
            }
            | MigrationStep::PatchVersioned {
                expected_version: Some(v),
                ..
            } => {
                pins.insert((*v).to_string());
            }
            _ => {}
        }
    }
    pins
}

/// Final active pin per collection: last PatchVersioned pin, else baseline root.
fn target_active_pins(registry: &Registry<'_>) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for b in registry.baseline {
        if let Some(v) = b.expected_version {
            map.insert(b.name.to_string(), v.to_string());
        }
    }
    for step in registry.steps {
        if let MigrationStep::PatchVersioned {
            collection,
            expected_version: Some(v),
            ..
        } = step
        {
            map.insert(collection.to_string(), (*v).to_string());
        }
    }
    map
}

fn verify_one_collection(
    entry: &BaselineCollection<'_>,
    by_name: &HashMap<String, Vec<&CollectionVersion>>,
    known_pins: &HashSet<String>,
    target_active: &HashMap<String, String>,
) -> Result<()> {
    let versions = by_name
        .get(entry.name)
        .ok_or_else(|| Error::CollectionMissing {
            collection: entry.name.to_string(),
        })?;

    let non_ph: Vec<&&CollectionVersion> = versions.iter().filter(|v| !v.is_placeholder).collect();

    if non_ph.is_empty() {
        return Err(Error::CollectionMissing {
            collection: entry.name.to_string(),
        });
    }

    // Multi-version DAGs are legal only when every non-placeholder version is a
    // known pin (baseline root + step destinations).
    if non_ph.len() > 1 {
        let all_known = !known_pins.is_empty()
            && non_ph
                .iter()
                .all(|v| known_pins.contains(v.version_id.as_str()));
        if !all_known {
            let ids: Vec<&str> = non_ph.iter().map(|v| v.version_id.as_str()).collect();
            for v in &non_ph {
                if !known_pins.is_empty() && !known_pins.contains(v.version_id.as_str()) {
                    return Err(Error::ForeignVersion {
                        collection: entry.name.to_string(),
                        version_id: v.version_id.clone(),
                    });
                }
            }
            return Err(Error::UnknownLineage {
                collection: entry.name.to_string(),
                versions: ids.join(", "),
            });
        }
    }

    let active = non_ph
        .iter()
        .find(|v| v.is_active)
        .or_else(|| non_ph.first())
        .copied()
        .ok_or_else(|| Error::CollectionMissing {
            collection: entry.name.to_string(),
        })?;

    // Root pin (if set) must appear in the DAG — it is not necessarily active
    // after later PatchVersioned steps.
    if let Some(root) = entry.expected_version {
        let has_root = non_ph.iter().any(|v| v.version_id == root);
        if !has_root {
            return Err(Error::UnknownLineage {
                collection: entry.name.to_string(),
                versions: non_ph
                    .iter()
                    .map(|v| v.version_id.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
            });
        }
    }

    // Active version must match the final target pin when one is known.
    if let Some(target) = target_active.get(entry.name) {
        if active.version_id != *target {
            return Err(Error::VersionPinMismatch {
                collection: entry.name.to_string(),
                expected: target.clone(),
                actual: active.version_id.clone(),
            });
        }
    }

    entry
        .expected_state
        .verify(active)
        .map_err(|detail| Error::StateVerification {
            collection: entry.name.to_string(),
            step: "baseline".into(),
            detail,
        })?;

    Ok(())
}
