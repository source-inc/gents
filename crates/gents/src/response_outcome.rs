//! Immutable terminal response facts.
//!
//! `AgentResponse` is a replaceable live projection. This module publishes the
//! correctness-bearing terminal fact by exact request and message document
//! versions before the request lifecycle is allowed to become terminal.

use anyhow::{Context as _, Result};
use defra_node::{EmbeddedNode, ExecuteRetryPolicy, QueryRequest, QueryResponse};
use identity::Did;
use serde::Deserialize;

use crate::graphql::escape_graphql_string;
use crate::{MessageFactRef, RequestExecutionProvenance, SignedDocumentVersionRef};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResponseOutcomeKind {
    Complete,
    Error,
    Interrupted,
}

impl ResponseOutcomeKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Error => "error",
            Self::Interrupted => "interrupted",
        }
    }

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "complete" => Ok(Self::Complete),
            "error" => Ok(Self::Error),
            "interrupted" => Ok(Self::Interrupted),
            other => anyhow::bail!("unknown AgentResponseOutcome kind {other:?}"),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ResponseOutcomeInput<'a> {
    pub(crate) request_id: &'a str,
    pub(crate) session_id: &'a str,
    pub(crate) agent_did: &'a str,
    pub(crate) requester_did: Option<&'a str>,
    pub(crate) behavior_id: &'a str,
    pub(crate) provenance: &'a RequestExecutionProvenance,
    pub(crate) kind: ResponseOutcomeKind,
    pub(crate) reason_code: Option<&'a str>,
    pub(crate) final_message: Option<&'a MessageFactRef>,
    pub(crate) terminalized_at: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AcceptedResponseOutcome {
    pub(crate) request_doc_id: String,
    pub(crate) request_id: String,
    pub(crate) session_id: String,
    pub(crate) provenance: RequestExecutionProvenance,
    pub(crate) outcome_signer_did: String,
    pub(crate) kind: ResponseOutcomeKind,
    pub(crate) reason_code: Option<String>,
    pub(crate) final_message: Option<MessageFactRef>,
    pub(crate) final_message_content: Option<String>,
    pub(crate) terminalized_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedResponseMessage {
    pub(crate) fact: MessageFactRef,
    pub(crate) content: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct ResponseOutcomeRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    request_doc_id: String,
    request_id: String,
    session_id: String,
    agent_did: String,
    #[serde(default)]
    requester_did: Option<String>,
    behavior_id: String,
    request_source_composite_commit_cid: String,
    request_source_signer_did: String,
    request_claim_composite_commit_cid: String,
    request_claim_signer_did: String,
    outcome_kind: String,
    #[serde(default)]
    reason_code: Option<String>,
    #[serde(default)]
    final_message_doc_id: Option<String>,
    #[serde(default)]
    final_message_composite_commit_cid: Option<String>,
    #[serde(default)]
    final_message_signer_did: Option<String>,
    #[serde(default)]
    final_message_sequence: Option<u32>,
    terminalized_at: String,
}

#[derive(Debug, Deserialize)]
struct FinalMessageRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    session_id: String,
    agent_did: String,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    request_doc_id: Option<String>,
    sequence: u32,
    role: String,
    content: String,
}

fn response_identity(node: &EmbeddedNode, agent_did: &str) -> Result<Did> {
    let node_did = node.node_identity_did().ok_or_else(|| {
        anyhow::anyhow!("AgentResponseOutcome persistence requires a DefraDB node identity")
    })?;
    if agent_did.trim().is_empty() {
        anyhow::bail!("AgentResponseOutcome requires a semantic agent DID");
    }
    Did::new(node_did).context("parsing AgentResponseOutcome node writer DID")
}

fn reader_identity(node: &EmbeddedNode) -> Result<Did> {
    let node_did = node.node_identity_did().ok_or_else(|| {
        anyhow::anyhow!("reading AgentResponseOutcome requires a DefraDB query identity")
    })?;
    Did::new(node_did).context("parsing AgentResponseOutcome reader DID")
}

async fn execute(node: &EmbeddedNode, identity: &Did, graphql: String) -> QueryResponse {
    node.execute_request_with_retry(
        QueryRequest::new(graphql).with_identity(Some(identity.clone())),
        ExecuteRetryPolicy::default(),
    )
    .await
}

fn graphql_optional_string(value: Option<&str>) -> String {
    value
        .filter(|value| !value.trim().is_empty())
        .map(|value| format!("\"{}\"", escape_graphql_string(value)))
        .unwrap_or_else(|| "null".to_string())
}

fn graphql_optional_u32(value: Option<u32>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_string())
}

fn row_fields() -> &'static str {
    r#"_docID request_doc_id request_id session_id agent_did requester_did behavior_id
       request_source_composite_commit_cid request_source_signer_did
       request_claim_composite_commit_cid request_claim_signer_did
       outcome_kind reason_code final_message_doc_id
       final_message_composite_commit_cid final_message_signer_did
       final_message_sequence terminalized_at"#
}

async fn load_rows(
    node: &EmbeddedNode,
    identity: &Did,
    request_doc_id: &str,
) -> Result<Vec<ResponseOutcomeRow>> {
    let escaped = escape_graphql_string(request_doc_id);
    let response = execute(
        node,
        identity,
        format!(
            r#"query {{
                AgentResponseOutcome(
                    filter: {{ request_doc_id: {{ _eq: "{escaped}" }} }}
                ) {{ {} }}
            }}"#,
            row_fields()
        ),
    )
    .await;
    if response.has_errors() {
        anyhow::bail!(
            "loading AgentResponseOutcome siblings for request _docID={request_doc_id}: {:?}",
            response.errors
        );
    }
    response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentResponseOutcome"))
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map(|rows| rows.unwrap_or_default())
        .context("decoding AgentResponseOutcome siblings")
}

fn row_matches(row: &ResponseOutcomeRow, desired: &ResponseOutcomeInput<'_>) -> bool {
    let final_message = desired.final_message;
    row.request_doc_id == desired.provenance.source.version.doc_id
        && row.request_id == desired.request_id
        && row.session_id == desired.session_id
        && row.agent_did == desired.agent_did
        && row.requester_did.as_deref() == desired.requester_did
        && row.behavior_id == desired.behavior_id
        && row.request_source_composite_commit_cid
            == desired.provenance.source.version.composite_commit_cid
        && row.request_source_signer_did == desired.provenance.source.signer_did
        && row.request_claim_composite_commit_cid
            == desired.provenance.claim.version.composite_commit_cid
        && row.request_claim_signer_did == desired.provenance.claim.signer_did
        && row.outcome_kind == desired.kind.as_str()
        && row.reason_code.as_deref() == desired.reason_code
        && row.final_message_doc_id.as_deref() == final_message.map(|fact| fact.doc_id.as_str())
        && row.final_message_composite_commit_cid.as_deref()
            == final_message.map(|fact| fact.composite_commit_cid.as_str())
        && row.final_message_signer_did.as_deref()
            == final_message.map(|fact| fact.signer_did.as_str())
        && row.final_message_sequence == final_message.map(|fact| fact.sequence)
        && same_rfc3339_instant(&row.terminalized_at, desired.terminalized_at)
}

fn same_rfc3339_instant(left: &str, right: &str) -> bool {
    match (
        chrono::DateTime::parse_from_rfc3339(left),
        chrono::DateTime::parse_from_rfc3339(right),
    ) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}

async fn verify_final_message(
    node: &EmbeddedNode,
    identity: &Did,
    desired: &ResponseOutcomeInput<'_>,
    fact: &MessageFactRef,
) -> Result<String> {
    let escaped_doc_id = escape_graphql_string(&fact.doc_id);
    let response = execute(
        node,
        identity,
        format!(
            r#"query {{
                AgentMessage(filter: {{ _docID: {{ _eq: "{escaped_doc_id}" }} }}) {{
                    _docID session_id agent_did request_id request_doc_id sequence role content
                }}
            }}"#
        ),
    )
    .await;
    if response.has_errors() {
        anyhow::bail!(
            "loading final AgentMessage {}: {:?}",
            fact.doc_id,
            response.errors
        );
    }
    let rows: Vec<FinalMessageRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentMessage"))
        .cloned()
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or_default();
    let [row] = rows.as_slice() else {
        anyhow::bail!(
            "final AgentMessage {} resolved to {} rows",
            fact.doc_id,
            rows.len()
        );
    };
    if row.doc_id != fact.doc_id
        || row.session_id != desired.session_id
        || row.agent_did != desired.agent_did
        || row.request_id.as_deref() != Some(desired.request_id)
        || row.request_doc_id.as_deref() != Some(desired.provenance.source.version.doc_id.as_str())
        || row.sequence != fact.sequence
        || row.role != "assistant"
    {
        anyhow::bail!(
            "final AgentMessage {} does not match response lineage",
            fact.doc_id
        );
    }
    let verified = crate::document_version::verified_current_signed_document_version_with_identity(
        node,
        "AgentMessage",
        &fact.doc_id,
        Some(identity.clone()),
    )
    .await?;
    if verified.version.composite_commit_cid != fact.composite_commit_cid
        || verified.signer_did != fact.signer_did
    {
        anyhow::bail!(
            "final AgentMessage {} exact signed version changed",
            fact.doc_id
        );
    }
    Ok(row.content.clone())
}

fn validate_input(input: &ResponseOutcomeInput<'_>) -> Result<()> {
    let request_doc_id = &input.provenance.source.version.doc_id;
    if request_doc_id.trim().is_empty()
        || input.provenance.claim.version.doc_id != *request_doc_id
        || input
            .provenance
            .source
            .version
            .composite_commit_cid
            .trim()
            .is_empty()
        || input
            .provenance
            .claim
            .version
            .composite_commit_cid
            .trim()
            .is_empty()
        || input.provenance.source.version.composite_commit_cid
            == input.provenance.claim.version.composite_commit_cid
        || input.provenance.source.signer_did.trim().is_empty()
        || input.provenance.claim.signer_did.trim().is_empty()
    {
        anyhow::bail!("AgentResponseOutcome requires exact source and claim references");
    }
    if input.request_id.trim().is_empty()
        || input.session_id.trim().is_empty()
        || input.behavior_id.trim().is_empty()
        || input.terminalized_at.trim().is_empty()
    {
        anyhow::bail!("AgentResponseOutcome requires complete logical lineage and timestamp");
    }
    match input.kind {
        ResponseOutcomeKind::Complete => {
            if input.final_message.is_none() || input.reason_code.is_some() {
                anyhow::bail!("complete AgentResponseOutcome requires one message and no reason");
            }
        }
        ResponseOutcomeKind::Error | ResponseOutcomeKind::Interrupted => {
            if input.reason_code.is_none() {
                anyhow::bail!("non-complete AgentResponseOutcome requires a typed reason");
            }
        }
    }
    Ok(())
}

/// Publish or verify the one accepted immutable outcome for an exact request.
///
/// The ordinary schema index intentionally permits replicated twins. Every
/// retry enumerates the full sibling set; identical replay is accepted, while
/// any divergent or multiple set fails closed.
pub(crate) async fn publish_response_outcome(
    node: &EmbeddedNode,
    input: ResponseOutcomeInput<'_>,
) -> Result<SignedDocumentVersionRef> {
    validate_input(&input)?;
    let identity = response_identity(node, input.agent_did)?;
    if let Some(message) = input.final_message {
        verify_final_message(node, &identity, &input, message).await?;
    }

    let existing = load_rows(node, &identity, &input.provenance.source.version.doc_id).await?;
    match existing.as_slice() {
        [row] if row_matches(row, &input) => {
            return crate::document_version::verified_current_signed_document_version_with_identity(
                node,
                "AgentResponseOutcome",
                &row.doc_id,
                Some(identity),
            )
            .await;
        }
        [] => {}
        rows => anyhow::bail!(
            "AgentResponseOutcome request _docID={} has {} conflicting visible facts",
            input.provenance.source.version.doc_id,
            rows.len()
        ),
    }

    let final_message = input.final_message;
    let mutation = format!(
        r#"mutation {{
            create_AgentResponseOutcome(input: {{
                request_doc_id: "{request_doc_id}"
                request_id: "{request_id}"
                session_id: "{session_id}"
                agent_did: "{agent_did}"
                requester_did: {requester_did}
                behavior_id: "{behavior_id}"
                request_source_composite_commit_cid: "{source_cid}"
                request_source_signer_did: "{source_signer}"
                request_claim_composite_commit_cid: "{claim_cid}"
                request_claim_signer_did: "{claim_signer}"
                outcome_kind: "{outcome_kind}"
                reason_code: {reason_code}
                final_message_doc_id: {message_doc_id}
                final_message_composite_commit_cid: {message_cid}
                final_message_signer_did: {message_signer}
                final_message_sequence: {message_sequence}
                terminalized_at: "{terminalized_at}"
            }}) {{ _docID }}
        }}"#,
        request_doc_id = escape_graphql_string(&input.provenance.source.version.doc_id),
        request_id = escape_graphql_string(input.request_id),
        session_id = escape_graphql_string(input.session_id),
        agent_did = escape_graphql_string(input.agent_did),
        requester_did = graphql_optional_string(input.requester_did),
        behavior_id = escape_graphql_string(input.behavior_id),
        source_cid = escape_graphql_string(&input.provenance.source.version.composite_commit_cid),
        source_signer = escape_graphql_string(&input.provenance.source.signer_did),
        claim_cid = escape_graphql_string(&input.provenance.claim.version.composite_commit_cid),
        claim_signer = escape_graphql_string(&input.provenance.claim.signer_did),
        outcome_kind = input.kind.as_str(),
        reason_code = graphql_optional_string(input.reason_code),
        message_doc_id = graphql_optional_string(final_message.map(|fact| fact.doc_id.as_str())),
        message_cid =
            graphql_optional_string(final_message.map(|fact| fact.composite_commit_cid.as_str())),
        message_signer =
            graphql_optional_string(final_message.map(|fact| fact.signer_did.as_str())),
        message_sequence = graphql_optional_u32(final_message.map(|fact| fact.sequence)),
        terminalized_at = escape_graphql_string(input.terminalized_at),
    );
    let response = execute(node, &identity, mutation).await;
    if response.has_errors() {
        anyhow::bail!(
            "creating AgentResponseOutcome failed: {:?}",
            response.errors
        );
    }

    let rows = load_rows(node, &identity, &input.provenance.source.version.doc_id).await?;
    let [row] = rows.as_slice() else {
        anyhow::bail!(
            "AgentResponseOutcome create produced {} visible request siblings",
            rows.len()
        );
    };
    if !row_matches(row, &input) {
        anyhow::bail!("created AgentResponseOutcome did not round-trip exact facts");
    }
    crate::document_version::verified_current_signed_document_version_with_identity(
        node,
        "AgentResponseOutcome",
        &row.doc_id,
        Some(identity),
    )
    .await
}

/// Load the complete visible sibling set and accept exactly one verified fact.
pub(crate) async fn load_accepted_response_outcome(
    node: &EmbeddedNode,
    agent_did: &str,
    request_doc_id: &str,
) -> Result<Option<AcceptedResponseOutcome>> {
    let identity = reader_identity(node)?;
    let rows = load_rows(node, &identity, request_doc_id).await?;
    let row = match rows.as_slice() {
        [] => return Ok(None),
        [row] => row,
        rows => anyhow::bail!(
            "AgentResponseOutcome request _docID={request_doc_id} has {} visible siblings",
            rows.len()
        ),
    };
    if row.agent_did != agent_did || row.request_doc_id != request_doc_id {
        anyhow::bail!("AgentResponseOutcome does not match requested owner/request document");
    }
    let provenance = RequestExecutionProvenance::new(
        SignedDocumentVersionRef::new(
            crate::DocumentVersionRef::new(
                &row.request_doc_id,
                &row.request_source_composite_commit_cid,
            ),
            &row.request_source_signer_did,
        ),
        SignedDocumentVersionRef::new(
            crate::DocumentVersionRef::new(
                &row.request_doc_id,
                &row.request_claim_composite_commit_cid,
            ),
            &row.request_claim_signer_did,
        ),
    );
    let message_fields = (
        row.final_message_doc_id.as_deref(),
        row.final_message_composite_commit_cid.as_deref(),
        row.final_message_signer_did.as_deref(),
        row.final_message_sequence,
    );
    let final_message = match message_fields {
        (None, None, None, None) => None,
        (Some(doc_id), Some(cid), Some(signer_did), Some(sequence)) => Some(MessageFactRef {
            sequence,
            doc_id: doc_id.to_string(),
            composite_commit_cid: cid.to_string(),
            signer_did: signer_did.to_string(),
        }),
        _ => anyhow::bail!("AgentResponseOutcome has a partial final-message reference"),
    };
    let kind = ResponseOutcomeKind::from_str(&row.outcome_kind)?;
    let input = ResponseOutcomeInput {
        request_id: &row.request_id,
        session_id: &row.session_id,
        agent_did: &row.agent_did,
        requester_did: row
            .requester_did
            .as_deref()
            .filter(|did| !did.trim().is_empty()),
        behavior_id: &row.behavior_id,
        provenance: &provenance,
        kind,
        reason_code: row.reason_code.as_deref(),
        final_message: final_message.as_ref(),
        terminalized_at: &row.terminalized_at,
    };
    validate_input(&input)?;
    if !row_matches(row, &input) {
        anyhow::bail!("AgentResponseOutcome exact row failed structural verification");
    }
    let final_message_content = match final_message.as_ref() {
        Some(message) => Some(verify_final_message(node, &identity, &input, message).await?),
        None => None,
    };
    let outcome_version =
        crate::document_version::verified_current_signed_document_version_with_identity(
            node,
            "AgentResponseOutcome",
            &row.doc_id,
            Some(identity),
        )
        .await?;
    // The verified signer is the DefraDB principal that authored this exact
    // outcome version. It need not equal the semantic `agent_did`: a node may
    // persist facts on behalf of the agent, with ACP authorization enforced by
    // the database policy layer.
    Ok(Some(AcceptedResponseOutcome {
        request_doc_id: row.request_doc_id.clone(),
        request_id: row.request_id.clone(),
        session_id: row.session_id.clone(),
        provenance,
        outcome_signer_did: outcome_version.signer_did,
        kind,
        reason_code: row.reason_code.clone(),
        final_message,
        final_message_content,
        terminalized_at: row.terminalized_at.clone(),
    }))
}

/// Resolve a completed response through its immutable outcome and exact signed
/// assistant-message fact. Mutable `AgentResponse` rows are deliberately not
/// consulted: they are only live projections and may lag or be replaced.
pub(crate) async fn load_verified_complete_response_message(
    node: &EmbeddedNode,
    agent_did: &str,
    request_doc_id: &str,
    request_id: &str,
    session_id: &str,
) -> Result<Option<VerifiedResponseMessage>> {
    let Some(outcome) = load_accepted_response_outcome(node, agent_did, request_doc_id).await?
    else {
        return Ok(None);
    };
    if outcome.request_id != request_id || outcome.session_id != session_id {
        anyhow::bail!(
            "AgentResponseOutcome for request _docID={request_doc_id} does not match logical request/session lineage"
        );
    }
    if outcome.kind != ResponseOutcomeKind::Complete {
        anyhow::bail!(
            "AgentResponseOutcome for completed request _docID={request_doc_id} is {}",
            outcome.kind.as_str()
        );
    }
    let (Some(fact), Some(content)) = (outcome.final_message, outcome.final_message_content) else {
        anyhow::bail!(
            "complete AgentResponseOutcome for request _docID={request_doc_id} has no verified final message"
        );
    };
    Ok(Some(VerifiedResponseMessage { fact, content }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::AgentIdentity as _;

    #[test]
    fn timestamp_comparison_accepts_equivalent_utc_spellings() {
        assert!(same_rfc3339_instant(
            "2026-08-08T00:00:00+00:00",
            "2026-08-08T00:00:00Z"
        ));
    }

    #[tokio::test]
    async fn node_writer_may_differ_from_semantic_agent() {
        let key_dir = tempfile::tempdir().unwrap();
        let node_identity =
            crate::identity::KeyIdentity::load_or_create(key_dir.path().join("node.key"), None)
                .unwrap();
        let node = EmbeddedNode::builder()
            .with_node_identity_did(node_identity.did())
            .data_path(key_dir.path().join("data"))
            .build()
            .await
            .unwrap();
        let semantic_agent = "did:key:zSemanticAgent";

        let identity = response_identity(&node, semantic_agent).unwrap();
        assert_eq!(identity.to_string(), node_identity.did());

        let provenance = RequestExecutionProvenance::new(
            SignedDocumentVersionRef::new(
                crate::DocumentVersionRef::new("request-doc", "source-cid"),
                "did:key:zSourceWriter",
            ),
            SignedDocumentVersionRef::new(
                crate::DocumentVersionRef::new("request-doc", "claim-cid"),
                node_identity.did(),
            ),
        );
        validate_input(&ResponseOutcomeInput {
            request_id: "request-id",
            session_id: "session-id",
            agent_did: semantic_agent,
            requester_did: None,
            behavior_id: "general",
            provenance: &provenance,
            kind: ResponseOutcomeKind::Error,
            reason_code: Some("daemon_restart_missing_response"),
            final_message: None,
            terminalized_at: "2026-08-08T00:00:00Z",
        })
        .unwrap();
    }
}
