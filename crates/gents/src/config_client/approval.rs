//! Tool-call approval client: list held calls and write verdict documents.
//!
//! Shared by the CLI (`tools holds` / `tools approve`) and the desktop
//! bridge. An operator approves by writing an `AgentToolApproval` document —
//! same shape as every other control-plane action; the runtime's verdict
//! watcher (hook/persistence/approval.rs) notices and drives the Lean-fenced
//! approve/deny edge. A physical call admits one immutable decision fact;
//! conflicting replays and replicated physical twins fail closed.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::graphql::escape_graphql_string;

use super::ConfigAccess;

/// A tool call persisted in `awaitingApproval`, as surfaced to operators.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeldToolCall {
    #[serde(rename = "_docID")]
    pub doc_id: String,
    pub tool_call_id: String,
    pub request_id: Option<String>,
    pub session_id: Option<String>,
    pub agent_did: Option<String>,
    pub tool_name: Option<String>,
    pub args: Option<String>,
    pub deadline_at: Option<String>,
}

/// List every tool call currently held for approval, optionally scoped to one
/// agent DID.
pub async fn list_held_tool_calls(
    access: &ConfigAccess,
    agent_did: Option<&str>,
) -> Result<Vec<HeldToolCall>> {
    let agent_filter = agent_did
        .map(|did| {
            let escaped = escape_graphql_string(did);
            format!(r#", agent_did: {{ _eq: "{escaped}" }}"#)
        })
        .unwrap_or_default();
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{ lifecycle_state: {{ _eq: "awaitingApproval" }}{agent_filter} }},
                order: {{ deadline_at: ASC }}
            ) {{
                _docID
                tool_call_id
                request_id
                session_id
                agent_did
                tool_name
                args
                deadline_at
            }}
        }}"#
    );
    let response = access.execute(&query).await?;
    let rows = response
        .get("data")
        .and_then(|data| data.get("AgentToolCall"))
        .cloned()
        .unwrap_or(serde_json::Value::Array(Vec::new()));
    serde_json::from_value(rows).context("decode held AgentToolCall rows")
}

/// Verdict to record for a held tool call.
#[derive(Debug, Clone)]
pub struct ToolApprovalVerdict {
    pub tool_call_id: String,
    pub agent_did: String,
    pub request_id: Option<String>,
    /// true = approved, false = denied.
    pub approve: bool,
    pub approver_did: String,
    pub reason: Option<String>,
}

#[derive(Deserialize)]
struct ApprovalCallRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    tool_call_key: String,
    request_id: String,
    session_id: String,
    requester_did: Option<String>,
    lifecycle_state: String,
}

#[derive(Deserialize)]
struct ApprovalCommitParent {
    cid: String,
    #[serde(rename = "fieldName")]
    field_name: Option<String>,
}

#[derive(Deserialize)]
struct ApprovalCommitSignature {
    identity: String,
}

#[derive(Deserialize)]
struct ApprovalCommitRow {
    cid: String,
    #[serde(default)]
    heads: Vec<ApprovalCommitParent>,
    signature: Option<ApprovalCommitSignature>,
}

#[derive(Deserialize)]
struct ExistingApprovalRow {
    approval_id: String,
    tool_call_id: String,
    tool_call_key: String,
    tool_call_doc_id: String,
    tool_call_composite_commit_cid: String,
    tool_call_signer_did: String,
    request_id: Option<String>,
    session_id: String,
    agent_did: String,
    requester_did: Option<String>,
    decision: String,
    approver_did: String,
    reason: Option<String>,
}

async fn exact_held_call(
    access: &ConfigAccess,
    verdict: &ToolApprovalVerdict,
) -> Result<(ApprovalCallRow, String, String)> {
    let query = format!(
        r#"{{ AgentToolCall(filter: {{ tool_call_id: {{ _eq: "{}" }}, agent_did: {{ _eq: "{}" }} }}) {{ _docID tool_call_key request_id session_id requester_did lifecycle_state }} }}"#,
        escape_graphql_string(&verdict.tool_call_id),
        escape_graphql_string(&verdict.agent_did),
    );
    let rows: Vec<ApprovalCallRow> = serde_json::from_value(
        access
            .execute(&query)
            .await?
            .get("data")
            .and_then(|data| data.get("AgentToolCall"))
            .cloned()
            .unwrap_or_default(),
    )?;
    let [row] = rows.as_slice() else {
        anyhow::bail!(
            "held tool identity resolved to {} physical rows",
            rows.len()
        );
    };
    let query = format!(
        r#"{{ _commits(docID: ["{}"], filter: {{ fieldName: {{ _eq: "_C" }} }}) {{ cid heads {{ cid fieldName }} signature {{ identity }} }} }}"#,
        escape_graphql_string(&row.doc_id)
    );
    let commits: Vec<ApprovalCommitRow> = serde_json::from_value(
        access
            .execute(&query)
            .await?
            .get("data")
            .and_then(|data| data.get("_commits"))
            .cloned()
            .unwrap_or_default(),
    )?;
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
    let [current] = current.as_slice() else {
        anyhow::bail!(
            "held tool call has {} current composite heads",
            current.len()
        );
    };
    let signer = match access {
        ConfigAccess::Local(node) => node.verified_block_signer_did(&current.cid).await?,
        ConfigAccess::Graphql(_) => current
            .signature
            .as_ref()
            .map(|signature| signature.identity.trim())
            .filter(|identity| !identity.is_empty())
            .ok_or_else(|| anyhow::anyhow!("held tool call composite commit has no signer"))?
            .to_string(),
    };
    Ok((
        ApprovalCallRow {
            doc_id: row.doc_id.clone(),
            tool_call_key: row.tool_call_key.clone(),
            request_id: row.request_id.clone(),
            session_id: row.session_id.clone(),
            requester_did: row.requester_did.clone(),
            lifecycle_state: row.lifecycle_state.clone(),
        },
        current.cid.clone(),
        signer,
    ))
}

async fn verify_approval_parent(access: &ConfigAccess, row: &ExistingApprovalRow) -> Result<()> {
    let cid = escape_graphql_string(&row.tool_call_composite_commit_cid);
    let response = access
        .execute(&format!(
            r#"{{ AgentToolCall(cid: ["{cid}"]) {{ _docID tool_call_key }} }}"#
        ))
        .await?;
    let parents = response
        .get("data")
        .and_then(|data| data.get("AgentToolCall"))
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("approval parent returned no exact snapshot"))?;
    match parents.as_slice() {
        [parent]
            if parent.get("_docID").and_then(serde_json::Value::as_str)
                == Some(row.tool_call_doc_id.as_str())
                && parent
                    .get("tool_call_key")
                    .and_then(serde_json::Value::as_str)
                    == Some(row.tool_call_key.as_str()) => {}
        rows => anyhow::bail!(
            "approval parent reconstructed {} rows or a different physical call",
            rows.len()
        ),
    }
    let signer = match access {
        ConfigAccess::Local(node) => {
            node.verified_block_signer_did(&row.tool_call_composite_commit_cid)
                .await?
        }
        ConfigAccess::Graphql(_) => {
            let response = access
                .execute(&format!(
                    r#"{{ _commits(cid: ["{cid}"]) {{ cid signature {{ identity }} }} }}"#
                ))
                .await?;
            let rows = response
                .get("data")
                .and_then(|data| data.get("_commits"))
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| anyhow::anyhow!("approval parent returned no commit evidence"))?;
            let [commit] = rows.as_slice() else {
                anyhow::bail!(
                    "approval parent resolved to {} commit evidence rows",
                    rows.len()
                );
            };
            if commit.get("cid").and_then(serde_json::Value::as_str)
                != Some(row.tool_call_composite_commit_cid.as_str())
            {
                anyhow::bail!("approval parent commit evidence returned a different CID");
            }
            commit
                .get("signature")
                .and_then(|signature| signature.get("identity"))
                .and_then(serde_json::Value::as_str)
                .filter(|identity| !identity.trim().is_empty())
                .ok_or_else(|| anyhow::anyhow!("approval parent commit has no signer"))?
                .to_string()
        }
    };
    if signer != row.tool_call_signer_did {
        anyhow::bail!(
            "approval parent signer {signer} does not match pinned {}",
            row.tool_call_signer_did
        );
    }
    Ok(())
}

async fn approval_rows(
    access: &ConfigAccess,
    approval_key: &str,
) -> Result<Vec<ExistingApprovalRow>> {
    let query = format!(
        r#"{{ AgentToolApproval(filter: {{ approval_key: {{ _eq: "{}" }} }}) {{ approval_id tool_call_id tool_call_key tool_call_doc_id tool_call_composite_commit_cid tool_call_signer_did request_id session_id agent_did requester_did decision approver_did reason }} }}"#,
        escape_graphql_string(approval_key)
    );
    Ok(serde_json::from_value(
        access
            .execute(&query)
            .await?
            .get("data")
            .and_then(|data| data.get("AgentToolApproval"))
            .cloned()
            .unwrap_or_default(),
    )?)
}

/// Write the `AgentToolApproval` decision document. Returns the approval_id.
pub async fn write_tool_approval(
    access: &ConfigAccess,
    verdict: &ToolApprovalVerdict,
) -> Result<String> {
    let (call, call_cid, call_signer) = exact_held_call(access, verdict).await?;
    if verdict.request_id.as_deref() != Some(call.request_id.as_str()) {
        anyhow::bail!("approval request_id does not match the exact held call");
    }
    let approval_id = format!("approval-{}", call.doc_id);
    let approval_key = call.doc_id.clone();
    let escaped_approval_id = escape_graphql_string(&approval_id);
    let escaped_tool_call_id = escape_graphql_string(&verdict.tool_call_id);
    let escaped_agent_did = escape_graphql_string(&verdict.agent_did);
    let escaped_request_id = escape_graphql_string(&call.request_id);
    let escaped_session_id = escape_graphql_string(&call.session_id);
    let requester_did_field =
        crate::session::requester_did_create_field(call.requester_did.as_deref());
    let escaped_approver_did = escape_graphql_string(&verdict.approver_did);
    let decision = if verdict.approve {
        "approved"
    } else {
        "denied"
    };
    let reason_field = verdict
        .reason
        .as_deref()
        .map(|reason| {
            let escaped = escape_graphql_string(reason);
            format!(r#"reason: "{escaped}","#)
        })
        .unwrap_or_default();
    let created_at = chrono::Utc::now().to_rfc3339();

    let decision_matches = |row: &ExistingApprovalRow| {
        row.approval_id == approval_id
            && row.tool_call_id == verdict.tool_call_id
            && row.tool_call_key == call.tool_call_key
            && row.tool_call_doc_id == call.doc_id
            && row.request_id.as_deref() == Some(call.request_id.as_str())
            && row.session_id == call.session_id
            && row.agent_did == verdict.agent_did
            && row.requester_did == call.requester_did
            && row.decision == decision
            && row.approver_did == verdict.approver_did
            && row.reason.as_deref().unwrap_or_default()
                == verdict.reason.as_deref().unwrap_or_default()
    };
    let observe = |rows: &[ExistingApprovalRow]| -> Result<bool> {
        match rows {
            [] => Ok(false),
            [row] if decision_matches(row) => Ok(true),
            [_] => anyhow::bail!("approval replay conflicts with immutable decision fact"),
            rows => anyhow::bail!("approval logical key has {} physical twins", rows.len()),
        }
    };
    let existing = approval_rows(access, &approval_key).await?;
    if observe(&existing)? {
        verify_approval_parent(access, &existing[0]).await?;
        return Ok(approval_id);
    }
    if call.lifecycle_state != "awaitingApproval" {
        anyhow::bail!("new approval fact requires an awaitingApproval call");
    }

    let mutation = format!(
        r#"mutation {{
            create_AgentToolApproval(input: {{
                approval_id: "{escaped_approval_id}",
                approval_key: "{}",
                tool_call_id: "{escaped_tool_call_id}",
                tool_call_key: "{}",
                tool_call_doc_id: "{}",
                tool_call_composite_commit_cid: "{}",
                tool_call_signer_did: "{}",
                request_id: "{escaped_request_id}",
                session_id: "{escaped_session_id}",
                agent_did: "{escaped_agent_did}",
                {requester_did_field}
                decision: "{decision}",
                approver_did: "{escaped_approver_did}",
                {reason_field}
                created_at: "{created_at}"
            }}) {{ _docID }}
        }}"#,
        escape_graphql_string(&approval_key),
        escape_graphql_string(&call.tool_call_key),
        escape_graphql_string(&call.doc_id),
        escape_graphql_string(&call_cid),
        escape_graphql_string(&call_signer),
    );
    if let Err(error) = access
        .execute(&mutation)
        .await
        .context("create AgentToolApproval")
    {
        let existing = approval_rows(access, &approval_key).await?;
        if observe(&existing)? {
            verify_approval_parent(access, &existing[0]).await?;
            return Ok(approval_id);
        }
        return Err(error);
    }
    Ok(approval_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn write_and_list_round_trip_against_local_node() {
        let data_path =
            std::env::temp_dir().join(format!("agent-approval-client-{}", uuid::Uuid::new_v4()));
        let signing_identity =
            crate::test_support::signed_test_identity("agent-approval-client-node");
        let node = defra_node::EmbeddedNode::builder()
            .with_node_identity_did(signing_identity.did())
            .data_path(&data_path)
            .build()
            .await
            .unwrap();
        crate::ensure_schemas(&node).await.unwrap();
        let access = ConfigAccess::Local(std::sync::Arc::new(node));

        // Persist a held row shaped like the runtime's hold_for_approval.
        let deadline = (chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339();
        access
            .execute(&format!(
                r#"mutation {{
                    create_AgentToolCall(input: {{
                        tool_call_key: "session-client:call-client",
                        request_id: "req-client",
                        session_id: "session-client",
                        agent_did: "did:test:general",
                        message_sequence: 1,
                        tool_name: "guarded",
                        tool_call_id: "call-client",
                        args: "{{}}",
                        result: "",
                        status: "called",
                        lifecycle_state: "awaitingApproval",
                        started_at: null,
                        deadline_at: "{deadline}"
                    }}) {{ _docID }}
                }}"#
            ))
            .await
            .unwrap();

        let held = list_held_tool_calls(&access, Some("did:test:general"))
            .await
            .unwrap();
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].tool_call_id, "call-client");
        assert_eq!(held[0].tool_name.as_deref(), Some("guarded"));
        let call_ref = crate::document_version::verified_current_signed_document_version(
            match &access {
                ConfigAccess::Local(node) => node,
                ConfigAccess::Graphql(_) => unreachable!("test uses an embedded node"),
            },
            "AgentToolCall",
            &held[0].doc_id,
        )
        .await
        .unwrap();

        let approval_id = write_tool_approval(
            &access,
            &ToolApprovalVerdict {
                tool_call_id: "call-client".to_string(),
                agent_did: "did:test:general".to_string(),
                request_id: Some("req-client".to_string()),
                approve: false,
                approver_did: call_ref.signer_did.clone(),
                reason: Some("blocked in test".to_string()),
            },
        )
        .await
        .unwrap();
        assert_eq!(approval_id, format!("approval-{}", held[0].doc_id));

        // The call is mutable: after the decision is attached its current CID
        // advances. An identical approval replay must still validate the
        // stored historical parent snapshot and converge on the same fact.
        access
            .execute(&format!(
                r#"mutation {{ update_AgentToolCall(filter: {{ _docID: {{ _eq: "{}" }} }}, input: {{ lifecycle_state: "running" }}) {{ _docID }} }}"#,
                escape_graphql_string(&held[0].doc_id)
            ))
            .await
            .unwrap();
        let replayed = write_tool_approval(
            &access,
            &ToolApprovalVerdict {
                tool_call_id: "call-client".to_string(),
                agent_did: "did:test:general".to_string(),
                request_id: Some("req-client".to_string()),
                approve: false,
                approver_did: call_ref.signer_did,
                reason: Some("blocked in test".to_string()),
            },
        )
        .await
        .unwrap();
        assert_eq!(replayed, approval_id);

        let decision = access
            .execute(
                r#"{ AgentToolApproval(filter: { tool_call_id: { _eq: "call-client" } }) { decision reason approver_did } }"#,
            )
            .await
            .unwrap();
        let rows = decision
            .get("data")
            .and_then(|data| data.get("AgentToolApproval"))
            .and_then(|rows| rows.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("decision").and_then(|value| value.as_str()),
            Some("denied")
        );

        let _ = std::fs::remove_dir_all(&data_path);
    }
}
