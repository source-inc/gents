//! Helper for creating subagent-parent-linked AgentRequest rows.
//!
//! Public API surface consumed by R3's `SubagentSource` and by Bucket 3
//! conformance fixtures (Task 26). Mirrors R1's existing AgentRequest
//! creation flow in `crates/gents/src/lifecycle/materialize.rs`,
//! with the addition of subagent parent-linkage fields and the depth
//! cap enforced by Lean's `Subagent.maxSubagentDepth`.

use std::collections::HashSet;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use defra_node::EmbeddedNode;
use serde::Deserialize;

use crate::graphql::escape_graphql_string;
use crate::lifecycle::DEFAULT_REQUEST_MAX_RETRIES;
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
    parent_tool_call_id: String,
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
        parent_tool_call_id,
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
///   2. Both `parent_request_id` and `parent_tool_call_id` non-empty.
///      Returns `IllegalToolCallTransition::ParentLinkageIncoherent`
///      otherwise. (A child must reference both parent identifiers; the
///      well-formedness invariant in `watcher.rs::validate_subagent_fields`
///      requires that `subagent_depth > 0` documents have both fields
///      populated.)
///   3. On the same-node path, `parent_request_id` resolves to an existing
///      `AgentRequest` owned by the same `agent_did` as the child request. The
///      trusted cross-deployment path instead requires a non-empty requester
///      DID and treats the targeted bridge as the durable parent edge.
///
/// On success returns the child `request_id`.
///
/// Field ownership notes (mirroring `materialize.rs`):
///   - `request_id` is caller-supplied; `session_id` is a freshly generated
///     UUID.
///   - `lifecycle_state` is initialized to `"pending"`.
///   - `subagent_depth = parent_subagent_depth + 1`.
///   - `caused_by_parent_request_id` / `caused_by_parent_tool_call_id`
///     carry the parent linkage.
///   - Trigger lineage fields identify the bridge edge:
///     `caused_by_trigger_kind = "subagent"` and
///     `caused_by_trigger_id = parent_tool_call_id`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_subagent_request_with_request_id(
    node: &EmbeddedNode,
    request_id: String,
    parent_request_id: String,
    parent_tool_call_id: String,
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
    )
    .await
}

/// Create a subagent request from a targeted bridge authored by a trusted
/// paired peer. The child is locally owned and routes lifecycle state back to
/// `requester_did`; the coordinator parent request is intentionally not
/// replicated to the host (#683).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_subagent_request_with_trusted_parent_request_id(
    node: &EmbeddedNode,
    request_id: String,
    parent_request_id: String,
    parent_tool_call_id: String,
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
    )
    .await
}

/// Test-only access to deterministic request identifiers for conformance
/// fixtures. Release builds reject this path before database I/O.
#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub async fn create_subagent_request_with_request_id_for_test(
    node: &EmbeddedNode,
    request_id: String,
    parent_request_id: String,
    parent_tool_call_id: String,
    parent_subagent_depth: u32,
    agent_did: String,
    behavior_id: String,
    prompt: String,
    deadline: Option<DateTime<Utc>>,
) -> Result<String> {
    if !cfg!(debug_assertions) {
        anyhow::bail!("deterministic subagent request creation is unavailable outside test builds");
    }
    create_subagent_request_with_request_id(
        node,
        request_id,
        parent_request_id,
        parent_tool_call_id,
        parent_subagent_depth,
        agent_did,
        behavior_id,
        prompt,
        deadline,
    )
    .await
}

/// Test-only access to the trusted bridge constructor. Production bridge
/// ingestion remains crate-owned and cannot be invoked through the public API.
#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub async fn create_subagent_request_with_trusted_parent_request_id_for_test(
    node: &EmbeddedNode,
    request_id: String,
    parent_request_id: String,
    parent_tool_call_id: String,
    parent_subagent_depth: u32,
    agent_did: String,
    behavior_id: String,
    prompt: String,
    deadline: Option<DateTime<Utc>>,
    requester_did: String,
) -> Result<String> {
    if !cfg!(debug_assertions) {
        anyhow::bail!("trusted subagent bridge creation is unavailable outside test builds");
    }
    create_subagent_request_with_trusted_parent_request_id(
        node,
        request_id,
        parent_request_id,
        parent_tool_call_id,
        parent_subagent_depth,
        agent_did,
        behavior_id,
        prompt,
        deadline,
        requester_did,
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
    if request_id.is_empty() || parent_request_id.is_empty() || parent_tool_call_id.is_empty() {
        return Err(anyhow!(IllegalToolCallTransition::ParentLinkageIncoherent));
    }

    let source_author_did = require_node_signer_did(node)?;

    // 3. Same-node children cross-reference the local parent row. A trusted
    // cross-deployment child instead uses the targeted, owner-authored bridge
    // as its durable parent edge; copying the entire parent request to every
    // possible host is neither necessary nor pair-scoped (#683).
    if require_parent_agent_match {
        let parent_agent_did = parent_request_agent_did(node, &parent_request_id).await?;
        if parent_agent_did.as_deref() != Some(agent_did.as_str()) {
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
    let escaped_source_author_did = escape_graphql_string(&source_author_did);
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
    let metadata_field = selected_skill_metadata_field(&prompt_selection.selected_skill_ids);

    let deadline_field = deadline
        .map(|d| {
            let escaped_deadline = escape_graphql_string(&d.to_rfc3339());
            format!(
                r#"
                deadline: "{escaped_deadline}","#
            )
        })
        .unwrap_or_default();

    // 5. Build and execute the CREATE mutation. Mirrors the field shape
    // of `write_pending_agent_request_with_lineage_and_conversation_title`
    // in `lifecycle/materialize.rs`, plus the three subagent fields.
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{escaped_request_id}",
                agent_did: "{escaped_agent_did}",
                source_author_did: "{escaped_source_author_did}",
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
                caused_by_parent_tool_call_id: "{escaped_parent_tool_call_id}",
                caused_by_trigger_id: "{escaped_parent_tool_call_id}",
                caused_by_trigger_kind: "subagent"
            }}) {{ _docID }}
        }}"#,
        max_retries = DEFAULT_REQUEST_MAX_RETRIES,
    );

    execute_mutation_with_retry(node, &mutation, "create_subagent_request").await?;

    Ok(request_id)
}

fn require_node_signer_did(node: &EmbeddedNode) -> Result<String> {
    node.node_identity_did()
        .map(str::trim)
        .filter(|did| !did.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            anyhow!("create_subagent_request requires a configured DefraDB node signing identity")
        })
}

/// The bridge fields that authorize one child materialization. Callers build
/// this from the row they intend to consume; admission reloads the exact
/// current composite CID and requires that signed snapshot to match it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BridgeAdmissionSnapshot {
    pub request_id: Option<String>,
    pub agent_did: Option<String>,
    pub tool_call_id: String,
    pub tool_name: String,
    pub args: String,
    pub lifecycle_state: Option<String>,
    pub deadline_at: Option<String>,
    pub await_mode: Option<String>,
    pub cancel_policy: Option<String>,
    pub child_request_id: Option<String>,
    pub spawn_target_did: Option<String>,
    pub unclaimed_deadline_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedBridgeAdmission {
    pub composite_commit_cid: String,
    pub signer_did: String,
}

#[derive(Debug, Clone, Deserialize)]
struct BridgeCommitParent {
    cid: String,
    #[serde(rename = "fieldName")]
    field_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct BridgeCompositeCommit {
    cid: String,
    #[serde(default)]
    heads: Vec<BridgeCommitParent>,
}

#[derive(Debug, Deserialize)]
struct BridgeSnapshotRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    agent_did: Option<String>,
    tool_call_id: String,
    #[serde(default)]
    tool_name: String,
    #[serde(default)]
    args: String,
    #[serde(default)]
    lifecycle_state: Option<String>,
    #[serde(default)]
    deadline_at: Option<String>,
    #[serde(default)]
    await_mode: Option<String>,
    #[serde(default)]
    cancel_policy: Option<String>,
    #[serde(default)]
    child_request_id: Option<String>,
    #[serde(default)]
    spawn_target_did: Option<String>,
    #[serde(default)]
    unclaimed_deadline_at: Option<String>,
}

impl BridgeSnapshotRow {
    fn admission_snapshot(&self) -> BridgeAdmissionSnapshot {
        BridgeAdmissionSnapshot {
            request_id: self.request_id.clone(),
            agent_did: self.agent_did.clone(),
            tool_call_id: self.tool_call_id.clone(),
            tool_name: self.tool_name.clone(),
            args: self.args.clone(),
            lifecycle_state: self.lifecycle_state.clone(),
            deadline_at: self.deadline_at.clone(),
            await_mode: self.await_mode.clone(),
            cancel_policy: self.cancel_policy.clone(),
            child_request_id: self.child_request_id.clone(),
            spawn_target_did: self.spawn_target_did.clone(),
            unclaimed_deadline_at: self.unclaimed_deadline_at.clone(),
        }
    }
}

/// Verify that the bridge authority comes from one exact signed current head.
///
/// This deliberately does not accept an expected signer argument derived from
/// a bridge column. It cryptographically obtains the signer, binds the exact
/// snapshot's immutable `agent_did` to it, and returns that verified DID for
/// the caller to check against the local principal or paired-peer registry.
pub(crate) async fn verify_current_bridge_admission(
    node: &EmbeddedNode,
    bridge_doc_id: &str,
    expected: &BridgeAdmissionSnapshot,
) -> Result<VerifiedBridgeAdmission> {
    let escaped_doc_id = escape_graphql_string(bridge_doc_id);
    let commits_query = format!(
        r#"query {{
            _commits(
                docID: ["{escaped_doc_id}"],
                filter: {{ fieldName: {{ _eq: "_C" }} }}
            ) {{
                cid
                heads {{ cid fieldName }}
            }}
        }}"#
    );
    let response = node.execute(&commits_query).await;
    if response.has_errors() {
        anyhow::bail!(
            "querying AgentToolCall {bridge_doc_id} bridge evidence failed: {:?}",
            response.errors
        );
    }
    let commits: Vec<BridgeCompositeCommit> = response
        .data
        .as_ref()
        .and_then(|data| data.get("_commits"))
        .cloned()
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or_default();
    let nested = commits
        .iter()
        .flat_map(|commit| commit.heads.iter())
        .filter(|head| head.field_name.as_deref() == Some("_C"))
        .map(|head| head.cid.as_str())
        .collect::<HashSet<_>>();
    let current = commits
        .iter()
        .filter(|commit| !nested.contains(commit.cid.as_str()))
        .collect::<Vec<_>>();
    let current = match current.as_slice() {
        [commit] => *commit,
        [] => anyhow::bail!("AgentToolCall {bridge_doc_id} has no current composite head"),
        commits => anyhow::bail!(
            "AgentToolCall {bridge_doc_id} has {} current composite heads; refusing bridge admission",
            commits.len()
        ),
    };

    let signer_did = node
        .verified_block_signer_did(&current.cid)
        .await
        .map_err(|error| {
            anyhow!(
                "cryptographically verifying AgentToolCall {bridge_doc_id} current head {}: {error}",
                current.cid
            )
        })?;
    let escaped_cid = escape_graphql_string(&current.cid);
    let snapshot_query = format!(
        r#"query {{
            AgentToolCall(cid: ["{escaped_cid}"]) {{
                _docID
                request_id
                agent_did
                tool_call_id
                tool_name
                args
                lifecycle_state
                deadline_at
                await_mode
                cancel_policy
                child_request_id
                spawn_target_did
                unclaimed_deadline_at
            }}
        }}"#
    );
    let response = node.execute(&snapshot_query).await;
    if response.has_errors() {
        anyhow::bail!(
            "loading AgentToolCall {bridge_doc_id} exact bridge snapshot {} failed: {:?}",
            current.cid,
            response.errors
        );
    }
    let rows: Vec<BridgeSnapshotRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentToolCall"))
        .cloned()
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or_default();
    let row = match rows.as_slice() {
        [row] if row.doc_id == bridge_doc_id => row,
        [row] => anyhow::bail!(
            "bridge CID {} reconstructed AgentToolCall {}, expected {bridge_doc_id}",
            current.cid,
            row.doc_id
        ),
        rows => anyhow::bail!(
            "bridge CID {} reconstructed {} AgentToolCall documents; expected one",
            current.cid,
            rows.len()
        ),
    };
    let actual = row.admission_snapshot();
    if &actual != expected {
        anyhow::bail!(
            "AgentToolCall {bridge_doc_id} current signed snapshot {} does not match the admitted parent/tool/child evidence",
            current.cid
        );
    }
    let declared_author = actual
        .agent_did
        .as_deref()
        .map(str::trim)
        .filter(|did| !did.is_empty())
        .ok_or_else(|| anyhow!("AgentToolCall {bridge_doc_id} has no declared author DID"))?;
    if signer_did != declared_author {
        anyhow::bail!(
            "AgentToolCall {bridge_doc_id} signer {signer_did} does not match declared author {declared_author}"
        );
    }
    if actual.lifecycle_state.as_deref() != Some("running") {
        anyhow::bail!(
            "AgentToolCall {bridge_doc_id} exact signed snapshot is not a running bridge"
        );
    }
    if actual
        .request_id
        .as_deref()
        .is_none_or(|value| value.trim().is_empty())
        || actual.tool_call_id.trim().is_empty()
        || actual
            .child_request_id
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
    {
        anyhow::bail!(
            "AgentToolCall {bridge_doc_id} exact signed snapshot lacks complete parent/tool/child evidence"
        );
    }

    Ok(VerifiedBridgeAdmission {
        composite_commit_cid: current.cid.clone(),
        signer_did,
    })
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
    agent_did: String,
}

async fn parent_request_agent_did(
    node: &EmbeddedNode,
    parent_request_id: &str,
) -> Result<Option<String>> {
    let escaped_parent_request_id = escape_graphql_string(parent_request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_parent_request_id}" }} }},
                limit: 1
            ) {{ agent_did }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query parent AgentRequest for create_subagent_request failed: {:?}",
            response.errors
        );
    }
    let rows: Vec<ParentRequestLookupRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    Ok(rows.into_iter().next().map(|row| row.agent_did))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AgentIdentity;
    use serde_json::Value;
    use tempfile::TempDir;

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

    struct TestDb {
        node: EmbeddedNode,
        signer_did: Option<String>,
        _tempdir: TempDir,
    }

    async fn test_db(name: &str, signed: bool) -> TestDb {
        let tempdir = tempfile::Builder::new()
            .prefix(&format!("gents-subagent-request-{name}-"))
            .tempdir()
            .expect("tempdir");
        let signer_did = if signed {
            let identity = crate::identity::KeyIdentity::load_or_create(
                tempdir.path().join("node-identity.key"),
                None,
            )
            .expect("node identity");
            Some(identity.did().to_string())
        } else {
            None
        };
        let mut builder = EmbeddedNode::builder().data_path(tempdir.path());
        if let Some(did) = signer_did.as_deref() {
            builder = builder.with_node_identity_did(did);
        }
        let node = builder.build().await.expect("embedded node");
        crate::schema::ensure_runtime_schemas(&node)
            .await
            .expect("runtime schemas");
        TestDb {
            node,
            signer_did,
            _tempdir: tempdir,
        }
    }

    async fn request_rows(node: &EmbeddedNode, request_id: &str) -> Vec<Value> {
        let request_id = escape_graphql_string(request_id);
        let response = node
            .execute(&format!(
                r#"{{
                    AgentRequest(filter: {{ request_id: {{ _eq: "{request_id}" }} }}) {{
                        agent_did
                        source_author_did
                        requester_did
                    }}
                }}"#
            ))
            .await;
        assert!(
            !response.has_errors(),
            "AgentRequest query failed: {:?}",
            response.errors
        );
        response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
    }

    fn bridge_snapshot(author_did: &str, args: &str) -> BridgeAdmissionSnapshot {
        BridgeAdmissionSnapshot {
            request_id: Some("parent-request".to_string()),
            agent_did: Some(author_did.to_string()),
            tool_call_id: "parent-tool-call".to_string(),
            tool_name: "spawn_subagent".to_string(),
            args: args.to_string(),
            lifecycle_state: Some("running".to_string()),
            deadline_at: None,
            await_mode: Some("background".to_string()),
            cancel_policy: Some("cascade".to_string()),
            child_request_id: Some("child-request".to_string()),
            spawn_target_did: Some(author_did.to_string()),
            unclaimed_deadline_at: None,
        }
    }

    async fn create_bridge(node: &EmbeddedNode, snapshot: &BridgeAdmissionSnapshot) -> String {
        let author = escape_graphql_string(snapshot.agent_did.as_deref().unwrap());
        let args = escape_graphql_string(&snapshot.args);
        let mutation = format!(
            r#"mutation {{
                create_AgentToolCall(input: {{
                    tool_call_key: "bridge-admission-key",
                    request_id: "parent-request",
                    session_id: "parent-session",
                    agent_did: "{author}",
                    tool_name: "spawn_subagent",
                    tool_call_id: "parent-tool-call",
                    args: "{args}",
                    status: "running",
                    lifecycle_state: "running",
                    await_mode: "background",
                    cancel_policy: "cascade",
                    child_request_id: "child-request",
                    spawn_target_did: "{author}"
                }}) {{ _docID }}
            }}"#
        );
        let response = node.execute(&mutation).await;
        assert!(
            !response.has_errors(),
            "bridge create failed: {:?}",
            response.errors
        );
        let response = node
            .execute(
                r#"{
                    AgentToolCall(
                        filter: { tool_call_key: { _eq: "bridge-admission-key" } },
                        limit: 1
                    ) { _docID }
                }"#,
            )
            .await;
        assert!(
            !response.has_errors(),
            "bridge lookup failed: {:?}",
            response.errors
        );
        response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentToolCall"))
            .and_then(Value::as_array)
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("_docID"))
            .and_then(Value::as_str)
            .expect("bridge _docID")
            .to_string()
    }

    #[tokio::test]
    async fn exact_signed_bridge_admission_returns_cryptographic_author() {
        let db = test_db("bridge-admission-signed", true).await;
        let signer = db.signer_did.as_deref().unwrap();
        let snapshot = bridge_snapshot(signer, r#"{"message":"delegate"}"#);
        let doc_id = create_bridge(&db.node, &snapshot).await;

        let admitted = verify_current_bridge_admission(&db.node, &doc_id, &snapshot)
            .await
            .expect("signed exact bridge admission");
        assert_eq!(admitted.signer_did, signer);
        assert!(!admitted.composite_commit_cid.is_empty());
    }

    #[tokio::test]
    async fn bridge_admission_rejects_unsigned_or_mismatched_authorship() {
        let unsigned = test_db("bridge-admission-unsigned", false).await;
        let unsigned_snapshot = bridge_snapshot("did:key:z6MkClaimedAuthor", "{}");
        let unsigned_doc = create_bridge(&unsigned.node, &unsigned_snapshot).await;
        let error =
            verify_current_bridge_admission(&unsigned.node, &unsigned_doc, &unsigned_snapshot)
                .await
                .expect_err("unsigned bridge must be rejected");
        assert!(
            error.to_string().contains("cryptographically verifying"),
            "unexpected unsigned error: {error:#}"
        );

        let signed = test_db("bridge-admission-mismatched-author", true).await;
        let mismatched = bridge_snapshot("did:key:z6MkClaimedAuthor", "{}");
        let mismatched_doc = create_bridge(&signed.node, &mismatched).await;
        let error = verify_current_bridge_admission(&signed.node, &mismatched_doc, &mismatched)
            .await
            .expect_err("bridge signer/author mismatch must be rejected");
        assert!(
            error.to_string().contains("does not match declared author"),
            "unexpected mismatch error: {error:#}"
        );
    }

    #[tokio::test]
    async fn unsigned_subagent_request_creation_fails_before_persisting() {
        let db = test_db("unsigned-rejection", false).await;
        let request_id = "child-unsigned-rejection";
        let error = create_subagent_request_with_trusted_parent_request_id(
            &db.node,
            request_id.to_string(),
            "remote-parent-request".to_string(),
            "remote-parent-tool-call".to_string(),
            0,
            "did:key:z6MkTargetAgent".to_string(),
            "general".to_string(),
            "child prompt".to_string(),
            None,
            "did:key:z6MkInitiatingRequester".to_string(),
        )
        .await
        .expect_err("unsigned subagent request creation must fail closed");
        assert!(
            error.to_string().contains("node signing identity"),
            "unexpected error: {error:#}"
        );
        assert!(
            request_rows(&db.node, request_id).await.is_empty(),
            "fail-closed creation must not persist a poison row"
        );
    }

    #[tokio::test]
    async fn signed_subagent_request_uses_target_signer_and_preserves_requester() {
        let db = test_db("signed-success", true).await;
        let request_id = "child-signed-success";
        let target_agent = db.signer_did.as_deref().unwrap();
        let requester = "did:key:z6MkInitiatingRequester";
        let created = create_subagent_request_with_trusted_parent_request_id(
            &db.node,
            request_id.to_string(),
            "remote-parent-request".to_string(),
            "remote-parent-tool-call".to_string(),
            0,
            target_agent.to_string(),
            "general".to_string(),
            "child prompt".to_string(),
            None,
            requester.to_string(),
        )
        .await
        .expect("signed subagent request creation");
        assert_eq!(created, request_id);

        let rows = request_rows(&db.node, request_id).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["agent_did"], target_agent);
        assert_eq!(
            rows[0]["source_author_did"].as_str(),
            db.signer_did.as_deref()
        );
        assert_eq!(rows[0]["requester_did"], requester);
        assert_eq!(rows[0]["source_author_did"], rows[0]["agent_did"]);
        assert_ne!(rows[0]["source_author_did"], rows[0]["requester_did"]);
    }
}
