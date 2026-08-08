use serde::Deserialize;

/// Generated immutable compaction source-manifest witness (#1073).
#[derive(Debug, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanCompactionSourceManifestCase {
    pub(crate) name: String,
    pub(crate) disposition: String,
    pub(crate) visible_logical_twins: usize,
    pub(crate) manifest_valid: bool,
    pub(crate) sources_current: bool,
    pub(crate) durable_rows: usize,
}
