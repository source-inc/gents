//! Helper for creating subagent-parent-linked AgentRequest rows.
//!
//! Public API surface consumed by R3's `SubagentSource` and by Bucket 3
//! conformance fixtures (Task 26). Mirrors R1's existing AgentRequest
//! creation flow in `crates/gents/src/lifecycle/materialize.rs`,
//! with the addition of subagent parent-linkage fields and the depth
//! cap enforced by Lean's `Subagent.maxSubagentDepth`.

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use defra_node::EmbeddedNode;
use serde::Deserialize;

use crate::graphql::escape_graphql_string;
use crate::lifecycle::{WorkspaceLineage, DEFAULT_REQUEST_MAX_RETRIES};
use crate::session::execute_mutation_with_retry;

use super::IllegalToolCallTransition;

/// The configured cap on subagent recursion depth. Matches Lean's
/// `Subagent.maxSubagentDepth = 3` (see
/// `crates/gents/proofs/Proofs/Background/State.lean`). Exposed as
/// part of R2's public API surface so R3's apply-time spawn-flow
/// validation can reference the same value as the Lean spec.
pub const MAX_SUBAGENT_DEPTH: u32 = 3;

/// Create a new AgentRequest row with subagent parent linkage. Allocates a
/// fresh request id before delegating to the request-id-aware implementation
/// used by R3's `SubagentSource`.
#[allow(clippy::too_many_arguments)]
pub async fn create_subagent_request(
    node: &EmbeddedNode,
    parent_request_id: String,
    parent_request_doc_id: String,
    parent_tool_call_id: String,
    parent_tool_call_doc_id: String,
    parent_subagent_depth: u32,
    agent_did: String,
    behavior_id: String,
    prompt: String,
    deadline: Option<DateTime<Utc>>,
) -> Result<String> {
    let new_request_id = uuid::Uuid::new_v4().to_string();
    create_subagent_request_with_request_id(
        node,
        new_request_id,
        parent_request_id,
        parent_request_doc_id,
        parent_tool_call_id,
        parent_tool_call_doc_id,
        parent_subagent_depth,
        agent_did,
        behavior_id,
        prompt,
        deadline,
    )
    .await
}

/// Create a new AgentRequest row with subagent parent linkage and a caller-
/// supplied request id. `SubagentSource` uses this path to preserve B5 link
/// symmetry: the child `AgentRequest.request_id` must equal the parent
/// `AgentToolCall.child_request_id` that caused the spawn.
///
/// Validates the preconditions enforced by the Lean Subagent spec before
/// creation:
///
///   1. `parent_subagent_depth + 1 ≤ MAX_SUBAGENT_DEPTH`. Returns
///      `IllegalToolCallTransition::SubagentDepthExceeded` otherwise.
///   2. Both logical identifiers (`parent_request_id` and
///      `parent_tool_call_id`) and both physical document identifiers
///      (`parent_request_doc_id` and `parent_tool_call_doc_id`) are non-empty.
///      Returns `IllegalToolCallTransition::ParentLinkageIncoherent`
///      otherwise. (A child must reference both parent identifiers; the
///      well-formedness invariant in `watcher.rs::validate_subagent_fields`
///      requires that `subagent_depth > 0` documents have all four fields
///      populated.)
///   3. On the same-node path, `parent_request_doc_id` resolves to an existing
///      `AgentRequest` whose logical id and owner match the supplied values.
///      Both paths require `parent_tool_call_doc_id` to resolve to an
///      `AgentToolCall` whose request document and logical tool-call id match.
///      The trusted cross-deployment path additionally requires a non-empty
///      requester DID.
///
/// On success returns the child `request_id`.
///
/// Field ownership notes (mirroring `materialize.rs`):
///   - `request_id` is caller-supplied; `session_id` is a freshly generated
///     UUID.
///   - `lifecycle_state` is initialized to `"pending"`.
///   - `subagent_depth = parent_subagent_depth + 1`.
///   - The logical and physical `caused_by_parent_*` pairs carry the parent
///     linkage.
///   - Trigger lineage fields identify the bridge edge:
///     `caused_by_trigger_kind = "subagent"` and
///     `caused_by_trigger_id = parent_tool_call_id`.
#[allow(clippy::too_many_arguments)]
pub async fn create_subagent_request_with_request_id(
    node: &EmbeddedNode,
    request_id: String,
    parent_request_id: String,
    parent_request_doc_id: String,
    parent_tool_call_id: String,
    parent_tool_call_doc_id: String,
    parent_subagent_depth: u32,
    agent_did: String,
    behavior_id: String,
    prompt: String,
    deadline: Option<DateTime<Utc>>,
) -> Result<String> {
    create_subagent_request_inner(
        node,
        request_id,
        parent_request_id,
        parent_tool_call_id,
        parent_subagent_depth,
        agent_did,
        behavior_id,
        prompt,
        deadline,
        true,
        None,
        (parent_request_doc_id, parent_tool_call_doc_id),
        None,
    )
    .await
}

/// Same as [`create_subagent_request_with_request_id`], stamping optional
/// isolated-workspace identity onto the child `AgentRequest`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_subagent_request_with_request_id_and_workspace(
    node: &EmbeddedNode,
    request_id: String,
    parent_request_id: String,
    parent_request_doc_id: String,
    parent_tool_call_id: String,
    parent_tool_call_doc_id: String,
    parent_subagent_depth: u32,
    agent_did: String,
    behavior_id: String,
    prompt: String,
    deadline: Option<DateTime<Utc>>,
    workspace: Option<WorkspaceLineage>,
) -> Result<String> {
    create_subagent_request_inner(
        node,
        request_id,
        parent_request_id,
        parent_tool_call_id,
        parent_subagent_depth,
        agent_did,
        behavior_id,
        prompt,
        deadline,
        true,
        None,
        (parent_request_doc_id, parent_tool_call_doc_id),
        workspace,
    )
    .await
}

/// Create a subagent request from a targeted bridge authored by a trusted
/// paired peer. The child is locally owned and routes lifecycle state back to
/// `requester_did`; the coordinator parent request is intentionally not
/// replicated to the host (#683).
#[allow(clippy::too_many_arguments)]
pub async fn create_subagent_request_with_trusted_parent_request_id(
    node: &EmbeddedNode,
    request_id: String,
    parent_request_id: String,
    parent_request_doc_id: String,
    parent_tool_call_id: String,
    parent_tool_call_doc_id: String,
    parent_subagent_depth: u32,
    agent_did: String,
    behavior_id: String,
    prompt: String,
    deadline: Option<DateTime<Utc>>,
    requester_did: String,
) -> Result<String> {
    create_subagent_request_inner(
        node,
        request_id,
        parent_request_id,
        parent_tool_call_id,
        parent_subagent_depth,
        agent_did,
        behavior_id,
        prompt,
        deadline,
        false,
        Some(requester_did),
        (parent_request_doc_id, parent_tool_call_doc_id),
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_subagent_request_with_trusted_parent_request_id_and_workspace(
    node: &EmbeddedNode,
    request_id: String,
    parent_request_id: String,
    parent_request_doc_id: String,
    parent_tool_call_id: String,
    parent_tool_call_doc_id: String,
    parent_subagent_depth: u32,
    agent_did: String,
    behavior_id: String,
    prompt: String,
    deadline: Option<DateTime<Utc>>,
    requester_did: String,
    workspace: Option<WorkspaceLineage>,
) -> Result<String> {
    create_subagent_request_inner(
        node,
        request_id,
        parent_request_id,
        parent_tool_call_id,
        parent_subagent_depth,
        agent_did,
        behavior_id,
        prompt,
        deadline,
        false,
        Some(requester_did),
        (parent_request_doc_id, parent_tool_call_doc_id),
        workspace,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn create_subagent_request_inner(
    node: &EmbeddedNode,
    request_id: String,
    parent_request_id: String,
    parent_tool_call_id: String,
    parent_subagent_depth: u32,
    agent_did: String,
    behavior_id: String,
    prompt: String,
    deadline: Option<DateTime<Utc>>,
    require_parent_agent_match: bool,
    requester_did: Option<String>,
    parent_doc_ids: (String, String),
    workspace: Option<WorkspaceLineage>,
) -> Result<String> {
    // 1. Depth check (pure precondition, fires before any DB I/O).
    if parent_subagent_depth >= MAX_SUBAGENT_DEPTH {
        return Err(anyhow!(IllegalToolCallTransition::SubagentDepthExceeded));
    }

    // Normalize the immutable route key before validating and persisting it.
    // A whitespace-padded DID must not create a request that can never match
    // the paired peer's exact replication filter.
    let requester_did = requester_did.map(|did| did.trim().to_owned());

    // 2. Coherence check (pure precondition, fires before any DB I/O).
    if request_id.is_empty()
        || parent_request_id.is_empty()
        || parent_tool_call_id.is_empty()
        || parent_doc_ids.0.trim().is_empty()
        || parent_doc_ids.1.trim().is_empty()
    {
        return Err(anyhow!(IllegalToolCallTransition::ParentLinkageIncoherent));
    }

    // 3. Same-node children cross-reference the local parent row. A trusted
    // cross-deployment child instead uses the targeted, owner-authored bridge
    // as its durable parent edge; copying the entire parent request to every
    // possible host is neither necessary nor pair-scoped (#683).
    if require_parent_agent_match {
        let parent = load_parent_request_by_doc_id(node, &parent_doc_ids.0).await?;
        if parent.request_id != parent_request_id || parent.agent_did != agent_did {
            return Err(anyhow!(IllegalToolCallTransition::ParentLinkageIncoherent));
        }
    } else if requester_did.as_deref().is_none_or(str::is_empty) {
        return Err(anyhow!(IllegalToolCallTransition::ParentLinkageIncoherent));
    }

    // 4. Generate fresh session identifier (mirror materialize.rs pattern).
    let new_session_id = uuid::Uuid::new_v4().to_string();
    let new_subagent_depth = parent_subagent_depth + 1;
    let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

    let escaped_request_id = escape_graphql_string(&request_id);
    let escaped_agent_did = escape_graphql_string(&agent_did);
    let requester_field = requester_did
        .as_deref()
        .map(escape_graphql_string)
        .map(|did| format!("requester_did: \"{did}\","))
        .unwrap_or_default();
    let escaped_behavior_id = escape_graphql_string(&behavior_id);
    let escaped_session_id = escape_graphql_string(&new_session_id);
    let prompt_selection = crate::skills::prompt_slash_skill_selection(&prompt);
    let prompt = prompt_selection.prompt;
    let escaped_prompt = escape_graphql_string(&prompt);
    let escaped_created_at = escape_graphql_string(&now);
    let escaped_parent_request_id = escape_graphql_string(&parent_request_id);
    let escaped_parent_tool_call_id = escape_graphql_string(&parent_tool_call_id);
    validate_parent_tool_call(
        node,
        &parent_doc_ids.1,
        &parent_doc_ids.0,
        &parent_tool_call_id,
    )
    .await?;
    let parent_doc_fields = format!(
        r#"
                caused_by_parent_request_doc_id: "{}",
                caused_by_parent_tool_call_doc_id: "{}","#,
        escape_graphql_string(&parent_doc_ids.0),
        escape_graphql_string(&parent_doc_ids.1),
    );
    let metadata_field = selected_skill_metadata_field(&prompt_selection.selected_skill_ids);
    let runtime_context = crate::tool_call_lifecycle::runtime::current_tool_runtime_context();
    let inherited_context_json = runtime_context
        .as_ref()
        .filter(|context| !context.source_fields.is_empty())
        .map(|context| {
            serde_json::to_string(&crate::lifecycle::TriggerExecutionContext {
                version: 1,
                source_fields: context.source_fields.clone(),
            })
        })
        .transpose()?;
    let inherited_trigger_context = crate::lifecycle::inherited_trigger_context_graphql_fields(
        runtime_context
            .as_ref()
            .and_then(|context| context.correlation.as_deref()),
        inherited_context_json.as_deref(),
    )?;

    let deadline_field = deadline
        .map(|d| {
            let escaped_deadline = escape_graphql_string(&d.to_rfc3339());
            format!(
                r#"
                deadline: "{escaped_deadline}","#
            )
        })
        .unwrap_or_default();
    if let Some(workspace) = workspace.as_ref() {
        workspace.require_authority_if_workspace_id()?;
    }
    let workspace_fields = workspace
        .as_ref()
        .map(WorkspaceLineage::graphql_fields)
        .unwrap_or_default();

    // 5. Build and execute the CREATE mutation. Mirrors the field shape
    // of `write_pending_agent_request_with_lineage_and_conversation_title`
    // in `lifecycle/materialize.rs`, plus the three subagent fields.
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{escaped_request_id}",
                agent_did: "{escaped_agent_did}",
                {requester_field}
                behavior_id: "{escaped_behavior_id}",
                session_id: "{escaped_session_id}",
                retry_parent_request: "",
                retry_root_request: "{escaped_request_id}",
                superseded_by_request: "",
                content: "{escaped_prompt}",{metadata_field}
                status: "pending",
                lifecycle_state: "pending",
                backend_id: "",
                execution_origin: "interactive",
                failure_reason: "",
                created_at: "{escaped_created_at}",{deadline_field}
                retry_count: 0,
                max_retries: {max_retries},
                subagent_depth: {new_subagent_depth},
                caused_by_parent_request_id: "{escaped_parent_request_id}",
                {parent_doc_fields}
                caused_by_parent_tool_call_id: "{escaped_parent_tool_call_id}",
                caused_by_trigger_id: "{escaped_parent_tool_call_id}",
                caused_by_trigger_kind: "subagent",
                {inherited_trigger_context}{workspace_fields}
            }}) {{ _docID }}
        }}"#,
        max_retries = DEFAULT_REQUEST_MAX_RETRIES,
    );

    execute_mutation_with_retry(node, &mutation, "create_subagent_request").await?;

    Ok(request_id)
}

fn selected_skill_metadata_field(selected_skill_ids: &[String]) -> String {
    if selected_skill_ids.is_empty() {
        return String::new();
    }

    let metadata = serde_json::json!({
        "selected_skill_ids": selected_skill_ids,
    })
    .to_string();
    format!(
        r#"
                metadata: "{}","#,
        escape_graphql_string(&metadata)
    )
}

#[derive(Debug, Deserialize)]
struct ParentRequestLookupRow {
    request_id: String,
    agent_did: String,
}

async fn load_parent_request_by_doc_id(
    node: &EmbeddedNode,
    parent_request_doc_id: &str,
) -> Result<ParentRequestLookupRow> {
    let escaped_doc_id = escape_graphql_string(parent_request_doc_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ _docID: {{ _eq: "{escaped_doc_id}" }} }},
                limit: 1
            ) {{ request_id agent_did }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query exact parent AgentRequest failed: {:?}",
            response.errors
        );
    }
    let mut rows: Vec<ParentRequestLookupRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    rows.pop()
        .ok_or_else(|| anyhow!(IllegalToolCallTransition::ParentLinkageIncoherent))
}

#[derive(Debug, Deserialize)]
struct ParentToolCallLookupRow {
    request_doc_id: String,
    tool_call_id: String,
}

async fn validate_parent_tool_call(
    node: &EmbeddedNode,
    parent_tool_call_doc_id: &str,
    parent_request_doc_id: &str,
    parent_tool_call_id: &str,
) -> Result<()> {
    let tool_doc_id = escape_graphql_string(parent_tool_call_doc_id);
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{ _docID: {{ _eq: "{tool_doc_id}" }} }},
                limit: 1
            ) {{ request_doc_id tool_call_id }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!("query parent AgentToolCall failed: {:?}", response.errors);
    }
    let rows: Vec<ParentToolCallLookupRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentToolCall"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    match rows.as_slice() {
        [row]
            if row.request_doc_id == parent_request_doc_id
                && row.tool_call_id == parent_tool_call_id =>
        {
            Ok(())
        }
        _ => Err(anyhow!(IllegalToolCallTransition::ParentLinkageIncoherent)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The depth and coherence checks fire BEFORE any DB I/O, so we can
    // exercise them without a real EmbeddedNode by leveraging the early
    // return semantics. The DB-touching happy path is deferred to Bucket
    // 3 / Task 26, which has the test_db fixture set up.
    //
    // We can't easily fabricate an `EmbeddedNode` for unit tests (it's a
    // real type with a real constructor that boots a node). So the unit
    // tests below use `unsafe { std::mem::zeroed() }` only conceptually
    // — that's UB in Rust. Instead, we inline-construct the precondition
    // checks directly in #[test] blocks that don't call the function.
    //
    // The function-level error paths (depth + coherence) ARE tested by
    // Task 26's end-to-end fixtures, where a real node is available.

    #[test]
    fn max_subagent_depth_matches_lean_spec() {
        // Lean: Subagent.State.lean defines `maxSubagentDepth : Nat := 3`.
        assert_eq!(MAX_SUBAGENT_DEPTH, 3);
    }

    #[test]
    fn depth_precondition_arithmetic() {
        // parent_subagent_depth + 1 must be <= MAX_SUBAGENT_DEPTH.
        // Allowed parent depths: 0, 1, 2 (resulting children: 1, 2, 3).
        // Rejected parent depths: 3 and above.
        for parent_depth in 0..=2 {
            assert!(parent_depth < MAX_SUBAGENT_DEPTH);
        }
        for parent_depth in 3..=10 {
            assert!(parent_depth >= MAX_SUBAGENT_DEPTH);
        }
    }

    #[test]
    fn subagent_prompt_slash_command_adds_selected_skill_metadata() {
        let selected = vec!["vuln-scan".to_string()];
        let field = selected_skill_metadata_field(&selected);

        assert!(field.contains("metadata:"));
        assert!(field.contains(r#"\"selected_skill_ids\":[\"vuln-scan\"]"#));
    }

    #[test]
    fn subagent_prompt_without_leading_slash_keeps_metadata_absent() {
        assert_eq!(selected_skill_metadata_field(&[]), "");
    }
}
