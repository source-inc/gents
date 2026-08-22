//! Typed patch layer for agent self-configuration (#654).
//!
//! Mirrors the Lean `SelfConfig` model (`proofs/Proofs/SelfConfig/`): each
//! config collection has a declared writable field set; everything else —
//! identity/unique keys, the owner `agent_did`, runtime-owned status fields,
//! secrets, apply-managed fields — is protected. A patch is an in-memory
//! partial merge over exactly the writable fields ([`apply_patch`]), rejected
//! wholesale if it names anything outside them ([`ensure_admissible`]),
//! validated as a whole document, and committed through a
//! [`super::ConfigApplyTxn`] under the agent DID.
//!
//! Field tables and merge semantics are fenced against the Lean contract
//! snapshot by `tests/conformance/self_config.rs`:
//! - `all_fields` must mirror the bundled SDL (schema drift breaks the fence
//!   rather than silently classifying a new field);
//! - identity immutability, field containment, and reject-leaves-unchanged
//!   replay the generated Lean witness cases through this merge.

use anyhow::{bail, Result};
use serde_json::{Map, Value};

use crate::graphql::escape_graphql_string;
use gents_protocol::graphql::{extract_mutation_doc_id, graphql_input_literal};

use super::ConfigApplyTxn;

/// Write targets of the self-config surface. The `automation` tool category
/// spans `Task`/`Schedule`/`EventTrigger`; targets are per collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SelfConfigTarget {
    AgentBehavior,
    ToolSelection,
    InferenceProfile,
    InferenceBackend,
    ToolServiceRegistry,
    Task,
    Schedule,
    EventTrigger,
}

/// Self-config tool categories (`ToolSelection.self_config_categories`
/// vocabulary), in the canonical sorted order the policy scope uses.
pub const SELF_CONFIG_CATEGORIES: [&str; 7] = [
    "automation",
    "backend",
    "behavior",
    "mcp_service",
    "persona",
    "profile",
    "tools",
];

/// Categories advertised when `self_config_categories` is unset: the core
/// spine. Extensions (backend / mcp_service / automation) are opt-in.
pub const DEFAULT_SELF_CONFIG_CATEGORIES: [&str; 3] = ["behavior", "profile", "tools"];

pub const ALL_SELF_CONFIG_TARGETS: [SelfConfigTarget; 8] = [
    SelfConfigTarget::AgentBehavior,
    SelfConfigTarget::ToolSelection,
    SelfConfigTarget::InferenceProfile,
    SelfConfigTarget::InferenceBackend,
    SelfConfigTarget::ToolServiceRegistry,
    SelfConfigTarget::Task,
    SelfConfigTarget::Schedule,
    SelfConfigTarget::EventTrigger,
];

impl SelfConfigTarget {
    pub fn collection_name(self) -> &'static str {
        match self {
            Self::AgentBehavior => "AgentBehavior",
            Self::ToolSelection => "ToolSelection",
            Self::InferenceProfile => "InferenceProfile",
            Self::InferenceBackend => "InferenceBackend",
            Self::ToolServiceRegistry => "ToolServiceRegistry",
            Self::Task => "Task",
            Self::Schedule => "Schedule",
            Self::EventTrigger => "EventTrigger",
        }
    }

    pub fn from_collection_name(name: &str) -> Option<Self> {
        ALL_SELF_CONFIG_TARGETS
            .into_iter()
            .find(|target| target.collection_name() == name)
    }

    /// Unique key per collection; parity with [`crate::Collection::unique_field`].
    pub fn unique_field(self) -> &'static str {
        match self {
            Self::AgentBehavior => "behavior_id",
            Self::ToolSelection => "selection_id",
            Self::InferenceProfile => "profile_id",
            Self::InferenceBackend => "backend_id",
            Self::ToolServiceRegistry => "service_id",
            Self::Task => "task_id",
            Self::Schedule => "schedule_id",
            Self::EventTrigger => "trigger_id",
        }
    }

    /// Self-config tool category gating this target.
    pub fn category(self) -> &'static str {
        match self {
            Self::AgentBehavior => "behavior",
            Self::ToolSelection => "tools",
            Self::InferenceProfile => "profile",
            Self::InferenceBackend => "backend",
            Self::ToolServiceRegistry => "mcp_service",
            Self::Task | Self::Schedule | Self::EventTrigger => "automation",
        }
    }

    /// Full schema field list, in bundled-SDL declaration order. Fenced
    /// against both the Lean contract tables and the bundled SDL by the
    /// self-config conformance tests.
    pub fn all_fields(self) -> &'static [&'static str] {
        match self {
            Self::AgentBehavior => &[
                "behavior_id",
                "agent_did",
                "display_name",
                "description",
                "summary",
                "system_prompt",
                "request_context_template",
                "backend_id",
                "model_name",
                "tool_selection_id",
                "inference_profile_id",
                "compaction_strategy",
                "compaction_threshold",
                "enabled",
                "skill_refs",
                "skill_excludes",
                "created_at",
                "updated_at",
            ],
            Self::ToolSelection => &[
                "selection_id",
                "agent_did",
                "display_name",
                "tool_policy_version",
                "enable_file_tools",
                "file_tools_mode",
                "file_tool_root",
                "enable_bash",
                "bash_mode",
                "command_execution_policy",
                "command_allowed_argv_prefixes",
                "command_forbidden_argv_prefixes",
                "read_only_command_allowlist",
                "command_network_mode",
                "cli_tool_names",
                "enable_meta_tools",
                "allowed_mcp_service_ids",
                "delegate_to",
                "backgroundable_tool_names",
                "approval_required_tools",
                "subagent_targets",
                "subagent_spawn_enabled",
                "orchestration_enabled",
                "subagent_steering_enabled",
                "subagent_background_enabled",
                "subagent_default_await_mode",
                "subagent_allow_cross_deployment",
                "cross_deployment_spawn_timeout_seconds",
                "enable_memory",
                "enable_session_history_tool",
                "enable_context_budget",
                "enable_defra_query",
                "defra_query_collections",
                "write_tools",
                "datastore_tool_surface_ids",
                "enable_self_config",
                "self_config_categories",
                "self_config_no_lockout",
                "self_config_dry_run",
                "enable_lsp",
                "enable_graph_dsl",
                "lsp_config",
                "updated_at",
            ],
            Self::InferenceProfile => &[
                "profile_id",
                "display_name",
                "context_window",
                "max_output_tokens",
                "max_turns",
                "temperature",
                "top_p",
                "top_k",
                "seed",
                "min_p",
                "frequency_penalty",
                "presence_penalty",
                "repetition_penalty",
                "reasoning_effort",
                "stream_batch_ms",
                "stream_liveness_timeout_secs",
                "deadline_duration_secs",
                "retry_max_transport",
                "retry_backoff_ms",
                "retry_max_resample",
                "retry_allow_repair",
                "retry_interactive_max",
                "updated_at",
            ],
            Self::InferenceBackend => &[
                "backend_id",
                "name",
                "provider_kind",
                "openai_wire_api",
                "endpoint",
                "api_key",
                "api_key_env_var",
                "max_concurrent",
                "max_queue_depth",
                "enabled",
                "models",
                "last_probe",
                "probe_status",
                "updated_at",
            ],
            Self::ToolServiceRegistry => &[
                "service_id",
                "display_name",
                "description",
                "hostname",
                "tailscale_ip",
                "lan_ip",
                "mcp_port",
                "mcp_path",
                "send_agent_did",
                "status",
                "version",
                "updated_at",
            ],
            Self::Task => &[
                "task_id",
                "name",
                "description",
                "behavior_id",
                "prompt_template",
                "enabled",
                "output_schema_ref",
                "created_at",
                "updated_at",
            ],
            Self::Schedule => &[
                "schedule_id",
                "task_id",
                "interval_secs",
                "cron",
                "timezone",
                "missed_run_policy",
                "enabled",
                "concurrency",
                "next_run_at",
                "last_attempt_at",
                "last_status",
                "last_error",
                "fire_count",
                "created_at",
                "updated_at",
            ],
            Self::EventTrigger => &[
                "trigger_id",
                "task_id",
                "source_collection",
                "event_kind",
                "filter",
                "enabled",
                "concurrency",
                "correlation_field",
                "fire_mode",
                "expected_count",
                "expected_count_field",
                "group_timeout_secs",
                "group_min_count",
                "created_at",
                "updated_at",
                "last_attempt_at",
                "last_fired_source_doc_id",
                "last_status",
                "last_error",
                "fire_count",
            ],
        }
    }

    /// The writable surface. Everything else is protected: identity/unique
    /// keys and the owner `agent_did` (self-config never changes *who* the
    /// agent is), runtime-owned status (`probe_status`, `last_*`,
    /// `next_run_at`, `fire_count`), secrets (`InferenceBackend.api_key` —
    /// `api_key_env_var` is the writable non-secret reference), apply-managed
    /// or deprecated fields (`write_tools`, `delegate_to`,
    /// `tool_policy_version`), writer-stamped timestamps, and
    /// `Task.behavior_id` (the automation ownership link, pinned at create).
    ///
    /// The self-config gate fields themselves ARE writable (an agent may
    /// disable its own gate; the opt-in no-lockout guard refuses that patch).
    pub fn writable_fields(self) -> &'static [&'static str] {
        match self {
            Self::AgentBehavior => &[
                "display_name",
                "description",
                "summary",
                "system_prompt",
                "request_context_template",
                "backend_id",
                "model_name",
                "tool_selection_id",
                "inference_profile_id",
                "compaction_strategy",
                "compaction_threshold",
                "enabled",
                "skill_refs",
                "skill_excludes",
            ],
            Self::ToolSelection => &[
                "display_name",
                "enable_file_tools",
                "file_tools_mode",
                "file_tool_root",
                "enable_bash",
                "bash_mode",
                "command_execution_policy",
                "command_allowed_argv_prefixes",
                "command_forbidden_argv_prefixes",
                "read_only_command_allowlist",
                "command_network_mode",
                "cli_tool_names",
                "enable_meta_tools",
                "allowed_mcp_service_ids",
                "backgroundable_tool_names",
                "approval_required_tools",
                "subagent_targets",
                "subagent_spawn_enabled",
                "subagent_steering_enabled",
                "subagent_background_enabled",
                "subagent_default_await_mode",
                "subagent_allow_cross_deployment",
                "cross_deployment_spawn_timeout_seconds",
                "enable_memory",
                "enable_session_history_tool",
                "enable_context_budget",
                "enable_defra_query",
                "defra_query_collections",
                "enable_self_config",
                "self_config_categories",
                "self_config_no_lockout",
                "self_config_dry_run",
                "enable_lsp",
                "enable_graph_dsl",
                "lsp_config",
            ],
            Self::InferenceProfile => &[
                "display_name",
                "context_window",
                "max_output_tokens",
                "max_turns",
                "temperature",
                "top_p",
                "top_k",
                "seed",
                "min_p",
                "frequency_penalty",
                "presence_penalty",
                "repetition_penalty",
                "reasoning_effort",
                "stream_batch_ms",
                "stream_liveness_timeout_secs",
                "deadline_duration_secs",
                "retry_max_transport",
                "retry_backoff_ms",
                "retry_max_resample",
                "retry_allow_repair",
                "retry_interactive_max",
            ],
            Self::InferenceBackend => &[
                "name",
                "provider_kind",
                "openai_wire_api",
                "endpoint",
                "api_key_env_var",
                "max_concurrent",
                "max_queue_depth",
                "enabled",
                "models",
            ],
            Self::ToolServiceRegistry => &[
                "display_name",
                "description",
                "hostname",
                "tailscale_ip",
                "lan_ip",
                "mcp_port",
                "mcp_path",
                "send_agent_did",
                "status",
            ],
            Self::Task => &[
                "name",
                "description",
                "prompt_template",
                "enabled",
                "output_schema_ref",
            ],
            Self::Schedule => &[
                "task_id",
                "interval_secs",
                "cron",
                "timezone",
                "missed_run_policy",
                "enabled",
                "concurrency",
            ],
            Self::EventTrigger => &[
                "task_id",
                "source_collection",
                "event_kind",
                "filter",
                "enabled",
                "concurrency",
                "correlation_field",
                "fire_mode",
                "expected_count",
                "expected_count_field",
                "group_timeout_secs",
                "group_min_count",
            ],
        }
    }

    /// Protected fields: the complement of the writable surface, so the
    /// partition is complete and disjoint by construction (Lean
    /// `protectedFields`).
    pub fn protected_fields(self) -> Vec<&'static str> {
        let writable = self.writable_fields();
        self.all_fields()
            .iter()
            .copied()
            .filter(|field| !writable.contains(field))
            .collect()
    }

    pub fn is_writable(self, field: &str) -> bool {
        self.writable_fields().contains(&field)
    }
}

/// A self-config patch: field → `Some(value)` to set, `None` to clear.
/// Ordered; later entries win (Lean `applyPatch` fold).
pub type SelfConfigPatch = Vec<(String, Option<Value>)>;

/// Admissibility (Lean `admissible`): the typed surface rejects — rather than
/// silently drops — any patch naming a field outside the writable set, or
/// carrying a value that is not a scalar.
///
/// The value-shape rule restores the model: Lean's `FieldValue` is a
/// `String` (`proofs/Proofs/SelfConfig/Apply.lean`), while the tool schema
/// is `additionalProperties: true` and accepts arbitrary JSON. Every
/// writable column across all six targets is a scalar or a list of scalars,
/// so nothing legitimate is refused here — and an object value would reach
/// the mutation renderer, where its keys land in identifier position.
pub fn ensure_admissible(target: SelfConfigTarget, patch: &SelfConfigPatch) -> Result<()> {
    for (field, value) in patch {
        if let Some(value) = value {
            ensure_scalar_patch_value(target, field, value)?;
        }
        if !target.is_writable(field) {
            if target.all_fields().contains(&field.as_str()) {
                bail!(
                    "field {field} on {collection} is protected (identity, runtime-owned, \
                     secret, or apply-managed) and cannot be patched via self-config",
                    collection = target.collection_name(),
                );
            }
            bail!(
                "unknown field {field} for {collection}; writable fields: {writable}",
                collection = target.collection_name(),
                writable = target.writable_fields().join(", "),
            );
        }
    }
    Ok(())
}

fn ensure_scalar_patch_value(target: SelfConfigTarget, field: &str, value: &Value) -> Result<()> {
    let shape = match value {
        Value::Object(_) => "an object",
        Value::Array(items)
            if items
                .iter()
                .any(|item| !item.is_string() && !item.is_number() && !item.is_boolean()) =>
        {
            "a list with a non-scalar element"
        }
        _ => return Ok(()),
    };
    bail!(
        "field {field} on {collection} must be a scalar or a list of scalars, got {shape}",
        collection = target.collection_name(),
    )
}

/// In-memory partial merge (Lean `applyPatch`): only writable fields change;
/// a set overwrites, a clear removes. Entries outside the writable set are
/// ignored here as defense in depth below [`ensure_admissible`].
pub fn apply_patch(
    target: SelfConfigTarget,
    doc: &Map<String, Value>,
    patch: &SelfConfigPatch,
) -> Map<String, Value> {
    let mut merged = doc.clone();
    for (field, value) in patch {
        if !target.is_writable(field) {
            continue;
        }
        match value {
            Some(value) => {
                merged.insert(field.clone(), value.clone());
            }
            None => {
                merged.remove(field);
            }
        }
    }
    merged
}

/// One field-level delta of a dry-run preview or applied patch.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct FieldDelta {
    pub field: String,
    pub from: Value,
    pub to: Value,
}

/// Field-level diff between two document projections, in schema field order.
pub fn diff_docs(
    target: SelfConfigTarget,
    before: &Map<String, Value>,
    after: &Map<String, Value>,
) -> Vec<FieldDelta> {
    target
        .all_fields()
        .iter()
        .filter_map(|field| {
            let from = before.get(*field).cloned().unwrap_or(Value::Null);
            let to = after.get(*field).cloned().unwrap_or(Value::Null);
            (from != to).then(|| FieldDelta {
                field: (*field).to_string(),
                from,
                to,
            })
        })
        .collect()
}

/// Load the (single live) document for `unique_value` inside the transaction,
/// selecting every schema field. Returns `(docID, doc)` with `null` fields
/// stripped so absence and null are one state, matching the merge model.
/// `InferenceBackend.api_key` is never selected — the secret cannot round-trip
/// through the patch layer.
pub async fn read_doc_in_txn(
    txn: &ConfigApplyTxn<'_>,
    target: SelfConfigTarget,
    unique_value: &str,
) -> Result<Option<(String, Map<String, Value>)>> {
    let collection = target.collection_name();
    let unique_field = target.unique_field();
    let fields = target
        .all_fields()
        .iter()
        .copied()
        .filter(|field| !(target == SelfConfigTarget::InferenceBackend && *field == "api_key"))
        .collect::<Vec<_>>()
        .join("\n                ");
    let query = format!(
        r#"{{
            {collection}(
                filter: {{ {unique_field}: {{ _eq: "{unique_value}" }} }},
                limit: 2
            ) {{
                _docID
                {fields}
            }}
        }}"#,
        unique_value = escape_graphql_string(unique_value),
    );
    let response = txn.execute(&query).await?;
    let rows = response
        .get("data")
        .and_then(|data| data.get(collection))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if rows.len() > 1 {
        bail!("multiple live {collection} documents share {unique_field}={unique_value}");
    }
    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };
    let Value::Object(mut row) = row else {
        bail!("{collection} row is not an object");
    };
    let doc_id = row
        .remove("_docID")
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .ok_or_else(|| anyhow::anyhow!("{collection} row missing _docID"))?;
    row.retain(|_, value| !value.is_null());
    Ok(Some((doc_id, row)))
}

/// Write exactly the patched fields to an existing document inside the
/// transaction. Wire-level field containment: the mutation names only the
/// patch's writable fields, so nothing else can be touched even under races
/// with runtime writers. Clears become explicit `null`; empty lists are
/// written as `null`, never `[]` (nillable-array corruption).
pub async fn update_doc_fields_in_txn(
    txn: &ConfigApplyTxn<'_>,
    target: SelfConfigTarget,
    doc_id: &str,
    patch: &SelfConfigPatch,
    merged: &Map<String, Value>,
) -> Result<String> {
    let collection = target.collection_name();
    let mut input = Map::new();
    for (field, _) in patch {
        if !target.is_writable(field) {
            continue;
        }
        let value = merged.get(field).cloned().unwrap_or(Value::Null);
        input.insert(field.clone(), sanitize_written_value(value));
    }
    if input.is_empty() {
        return Ok(doc_id.to_string());
    }
    let input_literal = graphql_input_literal(&Value::Object(input))?;
    let mutation = format!(
        r#"mutation {{
            update_{collection}(docID: "{doc_id}", input: {input_literal}) {{ _docID }}
        }}"#,
        doc_id = escape_graphql_string(doc_id),
    );
    let response = txn.execute(&mutation).await?;
    extract_mutation_doc_id(&response, collection)
}

/// Create a document inside the transaction from a full merged projection.
/// Null values are omitted; empty lists are omitted (never `[]`).
pub async fn create_doc_in_txn(
    txn: &ConfigApplyTxn<'_>,
    target: SelfConfigTarget,
    merged: &Map<String, Value>,
) -> Result<String> {
    let collection = target.collection_name();
    let mut input = Map::new();
    for (field, value) in merged {
        if value.is_null() {
            continue;
        }
        if matches!(value, Value::Array(items) if items.is_empty()) {
            continue;
        }
        input.insert(field.clone(), value.clone());
    }
    let input_literal = graphql_input_literal(&Value::Object(input))?;
    let mutation = format!(
        r#"mutation {{
            create_{collection}(input: {input_literal}) {{ _docID }}
        }}"#,
    );
    let response = txn.execute(&mutation).await?;
    extract_mutation_doc_id(&response, collection)
}

/// Empty list values become `null` on update — an empty list literal types as
/// `JsonArray` in DefraDB and corrupts nillable array columns.
fn sanitize_written_value(value: Value) -> Value {
    match value {
        Value::Array(items) if items.is_empty() => Value::Null,
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn field_partition_is_disjoint_and_complete() {
        for target in ALL_SELF_CONFIG_TARGETS {
            let all = target.all_fields();
            let writable = target.writable_fields();
            let protected = target.protected_fields();
            for field in writable {
                assert!(
                    all.contains(field),
                    "{}: writable field {field} missing from all_fields",
                    target.collection_name()
                );
            }
            assert_eq!(
                writable.len() + protected.len(),
                all.len(),
                "{}: partition incomplete",
                target.collection_name()
            );
            assert!(
                !writable.contains(&target.unique_field()),
                "{}: unique field must be protected",
                target.collection_name()
            );
            assert!(
                !writable.contains(&"agent_did"),
                "{}: agent_did must never be writable",
                target.collection_name()
            );
        }
    }

    #[test]
    fn merge_sets_clears_and_contains() {
        let doc = json!({
            "behavior_id": "beh-1",
            "agent_did": "did:key:z6M",
            "system_prompt": "old",
            "model_name": "m-small",
        });
        let Value::Object(doc) = doc else {
            unreachable!()
        };
        let patch: SelfConfigPatch = vec![
            ("system_prompt".to_string(), Some(json!("new"))),
            ("model_name".to_string(), None),
            ("agent_did".to_string(), Some(json!("did:key:attacker"))),
        ];
        let merged = apply_patch(SelfConfigTarget::AgentBehavior, &doc, &patch);
        assert_eq!(merged.get("system_prompt"), Some(&json!("new")));
        assert!(!merged.contains_key("model_name"));
        assert_eq!(
            merged.get("agent_did"),
            Some(&json!("did:key:z6M")),
            "protected field must survive even an inadmissible entry"
        );
        assert!(ensure_admissible(SelfConfigTarget::AgentBehavior, &patch).is_err());

        let deltas = diff_docs(SelfConfigTarget::AgentBehavior, &doc, &merged);
        assert_eq!(
            deltas,
            vec![
                FieldDelta {
                    field: "system_prompt".into(),
                    from: json!("old"),
                    to: json!("new"),
                },
                FieldDelta {
                    field: "model_name".into(),
                    from: json!("m-small"),
                    to: Value::Null,
                },
            ]
        );
    }

    #[test]
    fn empty_list_writes_sanitize_to_null() {
        assert_eq!(sanitize_written_value(json!([])), Value::Null);
        assert_eq!(sanitize_written_value(json!(["a"])), json!(["a"]));
        assert_eq!(sanitize_written_value(json!("x")), json!("x"));
    }
}
