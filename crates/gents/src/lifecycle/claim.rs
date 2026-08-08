use super::*;
use std::collections::{HashMap, HashSet};

use anyhow::Context as _;

#[derive(serde::Deserialize)]
struct CompositeCommitRow {
    cid: String,
    height: i64,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct CommitParentRow {
    cid: String,
    #[serde(rename = "fieldName")]
    field_name: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct CompositeHeadEvidenceRow {
    cid: String,
    #[serde(default)]
    heads: Vec<CommitParentRow>,
}

fn exactly_one_composite<'a>(
    rows: &'a [CompositeHeadEvidenceRow],
    subject: &str,
) -> Result<&'a CompositeHeadEvidenceRow> {
    match rows {
        [row] => Ok(row),
        [] => anyhow::bail!("{subject} has no current composite head"),
        rows => anyhow::bail!(
            "{subject} has {} current composite heads; refusing ambiguous provenance",
            rows.len()
        ),
    }
}

fn current_composite_heads(rows: &[CompositeHeadEvidenceRow]) -> Vec<CompositeHeadEvidenceRow> {
    let nested_composite_cids = rows
        .iter()
        .flat_map(|row| row.heads.iter())
        .filter(|head| head.field_name.as_deref() == Some("_C"))
        .map(|head| head.cid.as_str())
        .collect::<HashSet<_>>();
    rows.iter()
        .filter(|row| !nested_composite_cids.contains(row.cid.as_str()))
        .cloned()
        .collect()
}

fn require_expected_writer(
    commit_cid: &str,
    signer_did: &str,
    expected_writer_did: &str,
) -> Result<()> {
    if expected_writer_did.trim().is_empty() {
        anyhow::bail!("commit {commit_cid} has an empty expected writer DID");
    }
    if signer_did == expected_writer_did {
        Ok(())
    } else {
        anyhow::bail!(
            "commit {commit_cid} signer {signer_did} does not match expected writer {expected_writer_did}"
        )
    }
}

fn require_unique_logical_request_doc_id(
    request_id: &str,
    doc_ids: &HashSet<String>,
    expected_doc_id: &str,
) -> Result<()> {
    if doc_ids.len() == 1 && doc_ids.contains(expected_doc_id) {
        return Ok(());
    }
    anyhow::bail!(
        "AgentRequest logical request_id {request_id} resolves to {} distinct documents; expected only {expected_doc_id}",
        doc_ids.len()
    )
}

fn require_exact_source_admission(
    doc_id: &str,
    interrupt_requested_at: Option<&str>,
    valid_until: Option<&str>,
    claimed_at: &str,
) -> Result<()> {
    if interrupt_requested_at.is_some_and(|value| !value.trim().is_empty()) {
        anyhow::bail!("AgentRequest {doc_id} was interrupted before its signed claim");
    }
    let Some(valid_until) = valid_until.filter(|value| !value.trim().is_empty()) else {
        return Ok(());
    };
    let claimed_at = chrono::DateTime::parse_from_rfc3339(claimed_at)
        .with_context(|| format!("invalid signed-claim timestamp for AgentRequest {doc_id}"))?
        .with_timezone(&chrono::Utc);
    let valid_until = chrono::DateTime::parse_from_rfc3339(valid_until)
        .with_context(|| format!("invalid valid_until on exact AgentRequest {doc_id} source"))?
        .with_timezone(&chrono::Utc);
    if claimed_at > valid_until {
        anyhow::bail!("AgentRequest {doc_id} expired before its signed claim");
    }
    Ok(())
}

fn request_payload_preserved(source: &AgentRequest, claim: &AgentRequest) -> bool {
    source.doc_id == claim.doc_id
        && source.request_id == claim.request_id
        && source.agent_did == claim.agent_did
        && source.requester_did == claim.requester_did
        && source.session_id == claim.session_id
        && source.content == claim.content
        && source.temperature == claim.temperature
        && source.top_p == claim.top_p
        && source.top_k == claim.top_k
        && source.max_tokens == claim.max_tokens
        && source.metadata == claim.metadata
        && source.created_at == claim.created_at
        && source.subagent_depth == claim.subagent_depth
        && source.caused_by_parent_request_id == claim.caused_by_parent_request_id
        && source.caused_by_parent_tool_call_id == claim.caused_by_parent_tool_call_id
}

fn require_source_as_sole_composite_parent(
    claim: &CompositeHeadEvidenceRow,
    source_cid: &str,
) -> Result<()> {
    let composite_parents = claim
        .heads
        .iter()
        .filter(|head| head.field_name.as_deref() == Some("_C"))
        .map(|head| head.cid.as_str())
        .collect::<Vec<_>>();
    match composite_parents.as_slice() {
        [parent] if *parent == source_cid => Ok(()),
        [parent] => anyhow::bail!(
            "claim commit {} composite parent {parent} does not match source {source_cid}",
            claim.cid
        ),
        parents => anyhow::bail!(
            "claim commit {} has {} composite parents; expected source {source_cid} as the sole parent",
            claim.cid,
            parents.len()
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_verified_execution_provenance(
    source_commit: &CompositeHeadEvidenceRow,
    source_version: crate::DocumentVersionRef,
    source_signer_did: &str,
    expected_source_writer_did: &str,
    source_request: &AgentRequest,
    claim_commit: &CompositeHeadEvidenceRow,
    claim_version: crate::DocumentVersionRef,
    claim_signer_did: &str,
    target_agent_did: &str,
    claimed_request: &AgentRequest,
) -> Result<crate::RequestExecutionProvenance> {
    if source_version.composite_commit_cid != source_commit.cid {
        anyhow::bail!("source evidence CID does not match its exact document version");
    }
    if claim_version.composite_commit_cid != claim_commit.cid {
        anyhow::bail!("claim evidence CID does not match its exact document version");
    }
    if source_version.doc_id != claim_version.doc_id {
        anyhow::bail!("source and claim evidence refer to different documents");
    }
    if source_version.doc_id != source_request.doc_id
        || claim_version.doc_id != claimed_request.doc_id
    {
        anyhow::bail!("document version evidence does not match the reconstructed request");
    }
    if claimed_request.agent_did != target_agent_did {
        anyhow::bail!(
            "claim target {} does not match request target {}",
            target_agent_did,
            claimed_request.agent_did
        );
    }
    require_expected_writer(
        &source_commit.cid,
        source_signer_did,
        expected_source_writer_did,
    )?;
    require_expected_writer(&claim_commit.cid, claim_signer_did, target_agent_did)?;
    require_source_as_sole_composite_parent(claim_commit, &source_commit.cid)?;
    if !request_payload_preserved(source_request, claimed_request) {
        anyhow::bail!(
            "claim commit {} changed the admitted source payload",
            claim_commit.cid
        );
    }
    let claim_parent_cid = claim_commit
        .heads
        .iter()
        .find(|head| head.field_name.as_deref() == Some("_C"))
        .map(|head| head.cid.as_str());
    let ingest_outcome = super::ingest_contract::evaluate_request_ingest(
        &super::ingest_contract::RequestIngestEvidence {
            // Both signer values came from successful cryptographic block
            // verification before this function is entered.
            source_signature_valid: true,
            source_signer_did,
            expected_source_signer_did: expected_source_writer_did,
            // The atomic producer selected a pending source, established one
            // logical document, and selected one exact composite head before
            // calling the shared executable contract.
            source_claimable: true,
            logical_match_count: 1,
            source_doc_id: source_request.doc_id.as_str(),
            observed_doc_id: source_version.doc_id.as_str(),
            source_head_count: 1,
            observed_source_cid: source_commit.cid.as_str(),
            source_cid: source_version.composite_commit_cid.as_str(),
            claim_signature_valid: true,
            claim_signer_did,
            target_agent_did,
            claim_parent_cid,
            claim_payload_preserved: true,
        },
    );
    if ingest_outcome != super::ingest_contract::RequestIngestOutcome::Admitted {
        anyhow::bail!("signed request ingest rejected at {ingest_outcome:?}");
    }
    Ok(crate::RequestExecutionProvenance {
        source: crate::SignedDocumentVersionRef::new(source_version, source_signer_did),
        claim: crate::SignedDocumentVersionRef::new(claim_version, claim_signer_did),
    })
}

async fn composite_evidence_in_txn(
    node: &EmbeddedNode,
    transaction: &defra_node::TransactionHandle,
    doc_id: &str,
) -> Result<Vec<CompositeHeadEvidenceRow>> {
    let escaped_doc_id = escape_graphql_string(doc_id);
    let query = format!(
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
    let response = node
        .execute_request_in_txn(defra_node::QueryRequest::new(query), transaction)
        .await;
    if response.has_errors() {
        anyhow::bail!(
            "querying AgentRequest {doc_id} composite evidence in claim transaction failed: {:?}",
            response.errors
        );
    }
    response
        .data
        .as_ref()
        .and_then(|data| data.get("_commits"))
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map(|rows| rows.unwrap_or_default())
        .map_err(Into::into)
}

async fn current_composite_head_in_txn(
    node: &EmbeddedNode,
    transaction: &defra_node::TransactionHandle,
    doc_id: &str,
    phase: &str,
) -> Result<CompositeHeadEvidenceRow> {
    let rows = composite_evidence_in_txn(node, transaction, doc_id).await?;
    let heads = current_composite_heads(&rows);
    exactly_one_composite(&heads, &format!("AgentRequest {doc_id} {phase}")).cloned()
}

async fn require_unique_logical_request_in_txn(
    node: &EmbeddedNode,
    transaction: &defra_node::TransactionHandle,
    request_id: &str,
    expected_doc_id: &str,
) -> Result<()> {
    let escaped_request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"query {{
            AgentRequest(filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }}) {{
                _docID
            }}
        }}"#
    );
    let response = node
        .execute_request_in_txn(defra_node::QueryRequest::new(query), transaction)
        .await;
    if response.has_errors() {
        anyhow::bail!(
            "checking AgentRequest logical request_id {request_id} in claim transaction failed: {:?}",
            response.errors
        );
    }
    let doc_ids = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|row| row.get("_docID").and_then(serde_json::Value::as_str))
        .map(ToOwned::to_owned)
        .collect::<HashSet<_>>();
    require_unique_logical_request_doc_id(request_id, &doc_ids, expected_doc_id)
}

#[allow(clippy::too_many_arguments)]
async fn claim_with_verified_provenance_in_txn(
    node: &EmbeddedNode,
    transaction: &defra_node::TransactionHandle,
    doc_id: &str,
    target_agent_did: &str,
    claimed_at: &str,
    deadline: &str,
    behavior_id: &str,
    backend_id: &str,
    execution_origin: &str,
) -> Result<(
    crate::RequestExecutionProvenance,
    crate::DocumentVersionRef,
    AgentRequest,
)> {
    let source_commit = current_composite_head_in_txn(node, transaction, doc_id, "source").await?;
    let source_snapshot = crate::watcher::load_agent_request_at_cid_in_txn(
        node,
        transaction,
        &source_commit.cid,
        doc_id,
    )
    .await?
    .ok_or_else(|| {
        anyhow::anyhow!(
            "source commit {} did not reconstruct AgentRequest {doc_id}",
            source_commit.cid
        )
    })?;
    if source_snapshot.status != "pending"
        || source_snapshot.lifecycle_state.as_deref() != Some("pending")
    {
        anyhow::bail!(
            "AgentRequest {doc_id} source {} is not pending/pending",
            source_commit.cid
        );
    }
    if source_snapshot.request.agent_did != target_agent_did {
        anyhow::bail!(
            "AgentRequest {doc_id} targets {}, not claiming agent {target_agent_did}",
            source_snapshot.request.agent_did
        );
    }
    require_exact_source_admission(
        doc_id,
        source_snapshot.interrupt_requested_at.as_deref(),
        source_snapshot.valid_until.as_deref(),
        claimed_at,
    )?;
    require_unique_logical_request_in_txn(
        node,
        transaction,
        &source_snapshot.request.request_id,
        doc_id,
    )
    .await?;
    let source_signer_did = node
        .verified_block_signer_did_in_txn(&source_commit.cid, transaction)
        .await
        .with_context(|| {
            format!(
                "cryptographically verifying AgentRequest {doc_id} source {}",
                source_commit.cid
            )
        })?;
    require_expected_writer(
        &source_commit.cid,
        &source_signer_did,
        &source_snapshot.source_author_did,
    )?;

    let escaped_doc_id = escape_graphql_string(doc_id);
    let escaped_claimed_at = escape_graphql_string(claimed_at);
    let escaped_deadline = escape_graphql_string(deadline);
    let escaped_backend_id = escape_graphql_string(backend_id);
    let escaped_behavior_id = escape_graphql_string(behavior_id);
    let escaped_execution_origin = escape_graphql_string(execution_origin);
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{
                    _docID: {{ _eq: "{escaped_doc_id}" }},
                    agent_did: {{ _eq: "{}" }},
                    source_author_did: {{ _eq: "{}" }},
                    status: {{ _eq: "pending" }},
                    lifecycle_state: {{ _eq: "pending" }}
                }},
                input: {{
                    status: "processing",
                    lifecycle_state: "{}",
                    behavior_id: "{escaped_behavior_id}",
                    backend_id: "{escaped_backend_id}",
                    execution_origin: "{escaped_execution_origin}",
                    claimed_at: "{escaped_claimed_at}",
                    deadline: "{escaped_deadline}"
                }}
            ) {{ _docID }}
        }}"#,
        escape_graphql_string(target_agent_did),
        escape_graphql_string(&source_snapshot.source_author_did),
        PersistedLifecycleState::Claimed.as_str(),
    );
    let response = node
        .execute_request_in_txn(defra_node::QueryRequest::new(mutation), transaction)
        .await;
    if response.has_errors() {
        anyhow::bail!(
            "claiming AgentRequest {doc_id} in provenance transaction failed: {:?}",
            response.errors
        );
    }
    if !response
        .data
        .as_ref()
        .and_then(|data| data.get("update_AgentRequest"))
        .is_some_and(response_has_documents)
    {
        anyhow::bail!("AgentRequest {doc_id} changed before its verified claim could be written");
    }

    let claim_commit = current_composite_head_in_txn(node, transaction, doc_id, "claim").await?;
    let claim_snapshot = crate::watcher::load_agent_request_at_cid_in_txn(
        node,
        transaction,
        &claim_commit.cid,
        doc_id,
    )
    .await?
    .ok_or_else(|| {
        anyhow::anyhow!(
            "claim commit {} did not reconstruct AgentRequest {doc_id}",
            claim_commit.cid
        )
    })?;
    let request = &claim_snapshot.request;
    let matches_claim = claim_snapshot.status == "processing"
        && claim_snapshot.lifecycle_state.as_deref() == Some("claimed")
        && claim_snapshot.claimed_at.as_deref() == Some(claimed_at)
        && claim_snapshot.backend_id.as_deref().unwrap_or("") == backend_id
        && request.behavior_id.as_deref().unwrap_or("") == behavior_id
        && request.execution_origin.as_deref().unwrap_or("") == execution_origin
        && request.deadline.as_deref() == Some(deadline);
    if !matches_claim {
        anyhow::bail!(
            "AgentRequest {doc_id} claim {} does not reconstruct the exact claim markers",
            claim_commit.cid
        );
    }
    if claim_snapshot.source_author_did != source_snapshot.source_author_did {
        anyhow::bail!(
            "AgentRequest {doc_id} claim changed source_author_did from {} to {}",
            source_snapshot.source_author_did,
            claim_snapshot.source_author_did
        );
    }
    let claim_signer_did = node
        .verified_block_signer_did_in_txn(&claim_commit.cid, transaction)
        .await
        .with_context(|| {
            format!(
                "cryptographically verifying AgentRequest {doc_id} claim {}",
                claim_commit.cid
            )
        })?;
    let source_version = crate::DocumentVersionRef::new(doc_id, &source_commit.cid);
    let claim_version = crate::DocumentVersionRef::new(doc_id, &claim_commit.cid);
    let provenance = validate_verified_execution_provenance(
        &source_commit,
        source_version,
        &source_signer_did,
        &source_snapshot.source_author_did,
        &source_snapshot.request,
        &claim_commit,
        claim_version.clone(),
        &claim_signer_did,
        target_agent_did,
        request,
    )?;
    Ok((provenance, claim_version, claim_snapshot.request))
}

#[allow(clippy::too_many_arguments)]
async fn claim_with_verified_provenance(
    node: &EmbeddedNode,
    doc_id: &str,
    target_agent_did: &str,
    claimed_at: &str,
    deadline: &str,
    behavior_id: &str,
    backend_id: &str,
    execution_origin: &str,
) -> Result<(
    crate::RequestExecutionProvenance,
    crate::DocumentVersionRef,
    AgentRequest,
)> {
    let transaction = node
        .runner()
        .begin_txn(false)
        .await
        .map_err(|error| anyhow::anyhow!("begin signed-ingest transaction: {error}"))?;
    let result = claim_with_verified_provenance_in_txn(
        node,
        &transaction,
        doc_id,
        target_agent_did,
        claimed_at,
        deadline,
        behavior_id,
        backend_id,
        execution_origin,
    )
    .await;
    match result {
        Ok(value) => {
            if let Err(error) = node.runner().commit_txn(&transaction).await {
                let _ = node.runner().rollback_txn(&transaction).await;
                anyhow::bail!("commit signed-ingest transaction: {error}");
            }
            Ok(value)
        }
        Err(error) => {
            if let Err(rollback_error) = node.runner().rollback_txn(&transaction).await {
                tracing::warn!(
                    error = %rollback_error,
                    doc_id,
                    "failed to roll back rejected signed-ingest transaction"
                );
            }
            Err(error)
        }
    }
}

async fn composite_commits(node: &EmbeddedNode, doc_id: &str) -> Result<Vec<CompositeCommitRow>> {
    let escaped_doc_id = escape_graphql_string(doc_id);
    let query = format!(
        r#"query {{
            _commits(
                docID: ["{escaped_doc_id}"],
                filter: {{ fieldName: {{ _eq: "_C" }} }}
            ) {{
                cid
                height
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "querying AgentRequest {doc_id} composite commits failed: {:?}",
            response.errors
        );
    }
    response
        .data
        .as_ref()
        .and_then(|data| data.get("_commits"))
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map(|rows| rows.unwrap_or_default())
        .map_err(Into::into)
}

/// Re-verify persisted source/claim evidence before it crosses another trust
/// boundary (for example, before a rendered provider request is captured).
pub(crate) async fn verify_persisted_execution_provenance(
    node: &EmbeddedNode,
    provenance: &crate::RequestExecutionProvenance,
    request_doc_id: &str,
    target_agent_did: &str,
) -> Result<AgentRequest> {
    provenance.validate_for_request(request_doc_id, target_agent_did)?;

    let source_cid = &provenance.source.version.composite_commit_cid;
    let claim_cid = &provenance.claim.version.composite_commit_cid;
    let source_snapshot =
        crate::watcher::load_agent_request_at_cid(node, source_cid, request_doc_id)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "source commit {source_cid} does not reconstruct AgentRequest {request_doc_id}"
                )
            })?;
    let claim_snapshot = crate::watcher::load_agent_request_at_cid(node, claim_cid, request_doc_id)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "claim commit {claim_cid} does not reconstruct AgentRequest {request_doc_id}"
            )
        })?;

    if source_snapshot.status != "pending"
        || source_snapshot.lifecycle_state.as_deref() != Some("pending")
    {
        anyhow::bail!(
            "source commit {source_cid} is not an exact pending/pending admission snapshot"
        );
    }
    if claim_snapshot.status != "processing"
        || claim_snapshot.lifecycle_state.as_deref() != Some("claimed")
    {
        anyhow::bail!("claim commit {claim_cid} is not an exact processing/claimed snapshot");
    }

    let escaped_doc_id = escape_graphql_string(request_doc_id);
    let evidence_query = format!(
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
    let response = node.execute(&evidence_query).await;
    if response.has_errors() {
        anyhow::bail!(
            "querying persisted execution provenance for AgentRequest {request_doc_id} failed: {:?}",
            response.errors
        );
    }
    let evidence: Vec<CompositeHeadEvidenceRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("_commits"))
        .cloned()
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or_default();
    let source_commit = evidence
        .iter()
        .find(|row| row.cid == *source_cid)
        .ok_or_else(|| anyhow::anyhow!("source composite commit {source_cid} is not persisted"))?;
    let claim_commit = evidence
        .iter()
        .find(|row| row.cid == *claim_cid)
        .ok_or_else(|| anyhow::anyhow!("claim composite commit {claim_cid} is not persisted"))?;
    let source_signer = node.verified_block_signer_did(source_cid).await?;
    let claim_signer = node.verified_block_signer_did(claim_cid).await?;

    let verified = validate_verified_execution_provenance(
        source_commit,
        provenance.source.version.clone(),
        &source_signer,
        &source_snapshot.source_author_did,
        &source_snapshot.request,
        claim_commit,
        provenance.claim.version.clone(),
        &claim_signer,
        target_agent_did,
        &claim_snapshot.request,
    )?;
    if &verified != provenance {
        anyhow::bail!("persisted execution evidence does not match the supplied provenance");
    }
    Ok(claim_snapshot.request)
}

fn composite_ancestry_cids(
    evidence: &[CompositeHeadEvidenceRow],
    current: &CompositeHeadEvidenceRow,
) -> Result<HashSet<String>> {
    let by_cid = evidence
        .iter()
        .map(|row| (row.cid.as_str(), row))
        .collect::<HashMap<_, _>>();
    let mut ancestry = HashSet::new();
    let mut pending = vec![current.cid.as_str()];
    while let Some(cid) = pending.pop() {
        if !ancestry.insert(cid.to_string()) {
            continue;
        }
        let row = by_cid.get(cid).ok_or_else(|| {
            anyhow::anyhow!("AgentRequest commit ancestry is missing composite commit {cid}")
        })?;
        for parent in row
            .heads
            .iter()
            .filter(|parent| parent.field_name.as_deref() == Some("_C"))
        {
            if !by_cid.contains_key(parent.cid.as_str()) {
                anyhow::bail!(
                    "AgentRequest commit {} references missing composite parent {}",
                    row.cid,
                    parent.cid
                );
            }
            pending.push(parent.cid.as_str());
        }
    }
    Ok(ancestry)
}

/// Reconstruct the exact signed source/claim pair from the ancestry of the
/// request's sole current composite head.
///
/// This is the crash-recovery counterpart of the atomic claim gate. It does
/// not trust the current mutable row to remember provenance: it time-travels
/// the current commit ancestry, selects exactly one `processing/claimed`
/// snapshot, verifies its sole `_C` parent is an exact `pending/pending`
/// source, cryptographically verifies both signers, and reuses the same
/// payload-preservation validator as the original claim.
pub(crate) struct ReconstructedExecutionProvenance {
    pub(crate) provenance: crate::RequestExecutionProvenance,
    pub(crate) claimed_request: AgentRequest,
}

pub(crate) async fn reconstruct_execution_provenance_from_claim_ancestry(
    node: &EmbeddedNode,
    request_doc_id: &str,
    target_agent_did: &str,
) -> Result<ReconstructedExecutionProvenance> {
    if request_doc_id.trim().is_empty() || target_agent_did.trim().is_empty() {
        anyhow::bail!("claim-ancestry reconstruction requires a request document and target DID");
    }
    let node_did = node.node_identity_did().ok_or_else(|| {
        anyhow::anyhow!("claim-ancestry reconstruction requires a DefraDB query identity")
    })?;
    let identity = identity::Did::new(node_did).context("parsing claim-ancestry reader DID")?;
    let escaped_doc_id = escape_graphql_string(request_doc_id);
    let response = node
        .execute_request_with_retry(
            defra_node::QueryRequest::new(format!(
                r#"query {{
                    _commits(
                        docID: ["{escaped_doc_id}"],
                        filter: {{ fieldName: {{ _eq: "_C" }} }}
                    ) {{
                        cid
                        heads {{ cid fieldName }}
                    }}
                }}"#
            ))
            .with_identity(Some(identity.clone())),
            defra_node::ExecuteRetryPolicy::default(),
        )
        .await;
    if response.has_errors() {
        anyhow::bail!(
            "querying claim ancestry for AgentRequest {request_doc_id} failed: {:?}",
            response.errors
        );
    }
    let evidence: Vec<CompositeHeadEvidenceRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("_commits"))
        .cloned()
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or_default();
    let current_heads = current_composite_heads(&evidence);
    let current = exactly_one_composite(
        &current_heads,
        &format!("AgentRequest {request_doc_id} recovery head"),
    )?;
    let ancestry = composite_ancestry_cids(&evidence, current)?;

    let mut claim_cids = Vec::new();
    for cid in &ancestry {
        let Some(snapshot) = crate::watcher::load_agent_request_at_cid_with_identity(
            node,
            cid,
            request_doc_id,
            &identity,
        )
        .await?
        else {
            anyhow::bail!(
                "composite commit {cid} did not reconstruct AgentRequest {request_doc_id}"
            );
        };
        if snapshot.status == "processing" && snapshot.lifecycle_state.as_deref() == Some("claimed")
        {
            claim_cids.push(cid.clone());
        }
    }
    let claim_cid = match claim_cids.as_slice() {
        [cid] => cid,
        [] => anyhow::bail!(
            "AgentRequest {request_doc_id} current ancestry has no exact processing/claimed snapshot"
        ),
        cids => anyhow::bail!(
            "AgentRequest {request_doc_id} current ancestry has {} processing/claimed snapshots",
            cids.len()
        ),
    };
    let claim_commit = evidence
        .iter()
        .find(|row| row.cid == *claim_cid)
        .ok_or_else(|| anyhow::anyhow!("claim commit {claim_cid} disappeared from ancestry"))?;
    let composite_parents = claim_commit
        .heads
        .iter()
        .filter(|parent| parent.field_name.as_deref() == Some("_C"))
        .collect::<Vec<_>>();
    let [source_parent] = composite_parents.as_slice() else {
        anyhow::bail!(
            "claim commit {claim_cid} has {} composite parents; expected exactly one",
            composite_parents.len()
        );
    };
    let source_commit = evidence
        .iter()
        .find(|row| row.cid == source_parent.cid)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "claim commit {claim_cid} source parent {} is not persisted",
                source_parent.cid
            )
        })?;
    let source_snapshot = crate::watcher::load_agent_request_at_cid_with_identity(
        node,
        &source_commit.cid,
        request_doc_id,
        &identity,
    )
    .await?
    .ok_or_else(|| {
        anyhow::anyhow!(
            "source commit {} did not reconstruct AgentRequest {request_doc_id}",
            source_commit.cid
        )
    })?;
    let claim_snapshot = crate::watcher::load_agent_request_at_cid_with_identity(
        node,
        claim_cid,
        request_doc_id,
        &identity,
    )
    .await?
    .ok_or_else(|| {
        anyhow::anyhow!(
            "claim commit {claim_cid} did not reconstruct AgentRequest {request_doc_id}"
        )
    })?;
    if source_snapshot.status != "pending"
        || source_snapshot.lifecycle_state.as_deref() != Some("pending")
    {
        anyhow::bail!(
            "source commit {} is not an exact pending/pending admission snapshot",
            source_commit.cid
        );
    }
    let source_signer = node
        .verified_block_signer_did(&source_commit.cid)
        .await
        .with_context(|| {
            format!(
                "cryptographically verifying recovered AgentRequest source {}",
                source_commit.cid
            )
        })?;
    let claim_signer = node
        .verified_block_signer_did(claim_cid)
        .await
        .with_context(|| {
            format!("cryptographically verifying recovered AgentRequest claim {claim_cid}")
        })?;
    let provenance = validate_verified_execution_provenance(
        source_commit,
        crate::DocumentVersionRef::new(request_doc_id, &source_commit.cid),
        &source_signer,
        &source_snapshot.source_author_did,
        &source_snapshot.request,
        claim_commit,
        crate::DocumentVersionRef::new(request_doc_id, claim_cid),
        &claim_signer,
        target_agent_did,
        &claim_snapshot.request,
    )?;
    Ok(ReconstructedExecutionProvenance {
        provenance,
        claimed_request: claim_snapshot.request,
    })
}

pub(super) async fn current_composite_commit_cids(
    node: &EmbeddedNode,
    doc_id: &str,
) -> Result<HashSet<String>> {
    Ok(composite_commits(node, doc_id)
        .await?
        .into_iter()
        .map(|commit| commit.cid)
        .collect())
}

pub(super) async fn resolve_claimed_request_version(
    node: &EmbeddedNode,
    doc_id: &str,
    claimed_at: &str,
    deadline: &str,
    behavior_id: &str,
    backend_id: &str,
    execution_origin: &str,
    commits_before_claim: &HashSet<String>,
) -> Result<(crate::DocumentVersionRef, AgentRequest)> {
    let mut commits = composite_commits(node, doc_id).await?;
    commits.retain(|commit| !commits_before_claim.contains(&commit.cid));
    // The successful conditional mutation is the earliest new snapshot with
    // these claim markers. Later mutations inherit the markers, so choosing a
    // newest match would silently move the execution boundary forward.
    commits.sort_by(|left, right| {
        left.height
            .cmp(&right.height)
            .then_with(|| left.cid.cmp(&right.cid))
    });

    let mut selected: Option<(i64, crate::DocumentVersionRef, AgentRequest)> = None;
    for commit in commits {
        if selected
            .as_ref()
            .is_some_and(|(height, _, _)| commit.height > *height)
        {
            break;
        }
        let Some(snapshot) =
            crate::watcher::load_agent_request_at_cid(node, &commit.cid, doc_id).await?
        else {
            continue;
        };
        let request = &snapshot.request;
        let matches_claim = snapshot.status == "processing"
            && snapshot.lifecycle_state.as_deref() == Some("claimed")
            && snapshot.claimed_at.as_deref() == Some(claimed_at)
            && snapshot.backend_id.as_deref().unwrap_or("") == backend_id
            && request.behavior_id.as_deref().unwrap_or("") == behavior_id
            && request.execution_origin.as_deref().unwrap_or("") == execution_origin
            && request.deadline.as_deref() == Some(deadline);
        if matches_claim {
            if selected.is_some() {
                anyhow::bail!(
                    "AgentRequest {doc_id} has multiple new claim snapshots at height {}; refusing to choose a CID",
                    commit.height
                );
            }
            selected = Some((
                commit.height,
                crate::DocumentVersionRef::new(doc_id, commit.cid),
                snapshot.request,
            ));
        }
    }

    if let Some((_, version, request)) = selected {
        return Ok((version, request));
    }

    anyhow::bail!(
        "AgentRequest {doc_id} was claimed but no new composite commit reconstructs the exact claim snapshot"
    )
}

fn parse_rfc3339_utc(value: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| dt.with_timezone(&chrono::Utc))
}

async fn fetch_interrupt_and_ttl(
    node: &EmbeddedNode,
    doc_id: &str,
) -> Result<(Option<String>, Option<String>)> {
    let escaped_doc_id = escape_graphql_string(doc_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ _docID: {{ _eq: "{escaped_doc_id}" }} }},
                limit: 1
            ) {{
                interrupt_requested_at
                valid_until
            }}
        }}"#
    );
    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("fetch_interrupt_and_ttl for {doc_id}: {:?}", resp.errors);
    }
    let rows = resp
        .data
        .as_ref()
        .and_then(|d| d.get("AgentRequest"))
        .and_then(|v| v.as_array());
    let row = rows
        .and_then(|arr| arr.first())
        .ok_or_else(|| anyhow::anyhow!("AgentRequest {doc_id} not found"))?;
    let interrupt = row
        .get("interrupt_requested_at")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from);
    let valid = row
        .get("valid_until")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from);
    Ok((interrupt, valid))
}

impl RequestLifecycle {
    pub fn set_response_doc_id(&mut self, doc_id: &str) -> Result<()> {
        self.ensure_state(
            &[LocalLifecycleState::Claimed, LocalLifecycleState::Streaming],
            "set_response_doc_id",
        )?;
        self.require_execution_provenance("attach a response document")?;
        self.response_doc_id = Some(doc_id.to_string());
        self.state = LocalLifecycleState::Streaming;
        Ok(())
    }

    pub async fn claim_with_identity(&mut self) -> Result<ClaimOutcome> {
        self.claim_inner(true).await
    }

    /// Legacy unsigned claim path retained only for integration fixtures that
    /// deliberately run an identity-less embedded node. Release builds reject
    /// this path before any database I/O.
    #[doc(hidden)]
    pub async fn claim_without_identity_for_test(&mut self) -> Result<ClaimOutcome> {
        if !cfg!(debug_assertions) {
            anyhow::bail!("unsigned request claiming is unavailable outside test builds");
        }
        let outcome = self.claim_inner(false).await?;
        if outcome == ClaimOutcome::Claimed {
            self.execution_provenance =
                Some(crate::document_version::test_request_execution_provenance(
                    &self.request.doc_id,
                    &self.agent_did,
                ));
        }
        Ok(outcome)
    }

    pub async fn begin_execution(&mut self) -> Result<()> {
        self.ensure_state(&[LocalLifecycleState::Claimed], "begin_execution")?;
        self.require_execution_provenance("begin execution")?;
        self.transition_execution_view(
            "processing",
            PersistedLifecycleState::Claimed,
            "processing",
            PersistedLifecycleState::Processing,
        )
        .await
    }

    async fn transition_pending_to_interrupted(&mut self, _interrupt_at: &str) -> Result<()> {
        let doc_id = escape_graphql_string(&self.request.doc_id);
        let agent_did = escape_graphql_string(&self.request.agent_did);
        let terminalized_at = escape_graphql_string(&chrono::Utc::now().to_rfc3339());
        let mutation = format!(
            r#"mutation {{
                update_AgentRequest(
                    filter: {{
                        _docID: {{ _eq: "{doc_id}" }},
                        agent_did: {{ _eq: "{agent_did}" }},
                        status: {{ _eq: "pending" }},
                        lifecycle_state: {{ _eq: "pending" }}
                    }},
                    input: {{
                        status: "interrupted",
                        lifecycle_state: "interrupted",
                        terminalized_at: "{terminalized_at}",
                        terminal_redrive_attempts: 0
                    }}
                ) {{ _docID }}
            }}"#
        );
        let resp = crate::retry::execute_graphql_with_terminal_persistence_retry(
            &self.node,
            &mutation,
            "interrupt_before_claim",
        )
        .await?;
        if !resp
            .data
            .as_ref()
            .and_then(|data| data.get("update_AgentRequest"))
            .is_some_and(response_has_documents)
        {
            let request_view = self.request_view().await?;
            if request_view.as_ref().is_some_and(|row| {
                row.status == "interrupted" && row.lifecycle_state.as_deref() == Some("interrupted")
            }) {
                return Ok(());
            }
            anyhow::bail!(
                "request {} could not transition pending -> interrupted; current status={} lifecycle_state={}",
                self.request.request_id,
                request_view
                    .as_ref()
                    .map(|row| row.status.as_str())
                    .unwrap_or("missing"),
                request_view
                    .as_ref()
                    .and_then(|row| row.lifecycle_state.as_deref())
                    .unwrap_or("missing")
            );
        }
        Ok(())
    }

    async fn transition_pending_to_dead_stale(&mut self) -> Result<()> {
        let doc_id = escape_graphql_string(&self.request.doc_id);
        let agent_did = escape_graphql_string(&self.request.agent_did);
        let terminalized_at = escape_graphql_string(&chrono::Utc::now().to_rfc3339());
        let mutation = format!(
            r#"mutation {{
                update_AgentRequest(
                    filter: {{
                        _docID: {{ _eq: "{doc_id}" }},
                        agent_did: {{ _eq: "{agent_did}" }},
                        status: {{ _eq: "pending" }},
                        lifecycle_state: {{ _eq: "pending" }}
                    }},
                    input: {{
                        status: "dead",
                        lifecycle_state: "dead",
                        failure_reason: "Stale",
                        terminalized_at: "{terminalized_at}",
                        terminal_redrive_attempts: 0
                    }}
                ) {{ _docID }}
            }}"#
        );
        let resp = crate::retry::execute_graphql_with_terminal_persistence_retry(
            &self.node,
            &mutation,
            "expire_stale",
        )
        .await?;
        if !resp
            .data
            .as_ref()
            .and_then(|data| data.get("update_AgentRequest"))
            .is_some_and(response_has_documents)
        {
            let request_view = self.request_view().await?;
            if request_view.as_ref().is_some_and(|row| {
                row.status == "dead" && row.lifecycle_state.as_deref() == Some("dead")
            }) {
                return Ok(());
            }
            anyhow::bail!(
                "request {} could not transition pending -> dead; current status={} lifecycle_state={}",
                self.request.request_id,
                request_view
                    .as_ref()
                    .map(|row| row.status.as_str())
                    .unwrap_or("missing"),
                request_view
                    .as_ref()
                    .and_then(|row| row.lifecycle_state.as_deref())
                    .unwrap_or("missing")
            );
        }
        self.failure_reason = Some("Stale".to_string());
        Ok(())
    }

    async fn claim_inner(&mut self, explicit_did: bool) -> Result<ClaimOutcome> {
        self.ensure_state(&[LocalLifecycleState::Pending], "claim")?;
        if explicit_did {
            // Reject obvious replays before queue ordering can classify an
            // already-claimed document as merely blocked. The transactional
            // source reload below repeats this gate at the write boundary.
            let request_view = self.request_view().await?;
            let is_pending = request_view.as_ref().is_some_and(|row| {
                row.status == "pending" && row.lifecycle_state.as_deref() == Some("pending")
            });
            if !is_pending {
                anyhow::bail!(
                    "AgentRequest {} is not pending/pending and cannot be replayed",
                    self.request.doc_id
                );
            }
        }
        let (interrupt_requested_at, valid_until) =
            fetch_interrupt_and_ttl(&self.node, &self.request.doc_id).await?;

        if let Some(interrupt_at) = interrupt_requested_at {
            self.transition_pending_to_interrupted(&interrupt_at)
                .await?;
            self.state = LocalLifecycleState::Interrupted;
            return Ok(ClaimOutcome::Interrupted);
        }

        let valid_until_at_claim = match valid_until.as_deref() {
            Some(s) => {
                let dt = chrono::DateTime::parse_from_rfc3339(s)
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "invalid valid_until on request {}: {e}",
                            self.request.doc_id
                        )
                    })?
                    .with_timezone(&chrono::Utc);
                if chrono::Utc::now() > dt {
                    self.transition_pending_to_dead_stale().await?;
                    self.state = LocalLifecycleState::Dead;
                    return Ok(ClaimOutcome::Expired);
                }
                Some(dt)
            }
            None => None,
        };

        let dedup = self.check_deduplication().await?;
        if !dedup.is_earliest {
            tracing::info!(
                request_id = %self.request.request_id,
                session_id = %self.request.session_id,
                blocking_request_id = dedup.blocking_request_id.as_deref().unwrap_or(""),
                "request remains queued behind earlier same-session request"
            );
            return Ok(ClaimOutcome::Queued);
        }

        let now = chrono::Utc::now();
        let claimed_at = now.to_rfc3339();
        let synthesized_deadline_at =
            now + chrono::Duration::seconds(self.deadline_duration_secs as i64);
        let deadline_at = self
            .request
            .deadline
            .as_deref()
            .and_then(parse_rfc3339_utc)
            .unwrap_or(synthesized_deadline_at);
        let deadline = deadline_at.to_rfc3339();
        let doc_id = self.request.doc_id.clone();
        if explicit_did {
            let (execution_provenance, request_version, claimed_request) =
                claim_with_verified_provenance(
                    &self.node,
                    &doc_id,
                    &self.request.agent_did,
                    &claimed_at,
                    &deadline,
                    &self.behavior_id,
                    &self.backend_id,
                    self.execution_origin.as_str(),
                )
                .await?;

            // The transaction has committed at this point. Keep the local view
            // unchanged on every verification, mutation, or commit failure.
            self.request = claimed_request;
            self.request_version = Some(request_version);
            self.execution_provenance = Some(execution_provenance);
            self.state = LocalLifecycleState::Claimed;
            self.claimed_deadline_at = Some(deadline_at);
            self.valid_until_at_claim = valid_until_at_claim;

            tracing::debug!(
                doc_id = %doc_id,
                deadline,
                backend_id = %self.backend_id,
                execution_origin = self.execution_origin.as_str(),
                "claimed and verified signed AgentRequest provenance"
            );
            return Ok(ClaimOutcome::Claimed);
        }

        let commits_before_claim = current_composite_commit_cids(&self.node, &doc_id).await?;
        let escaped_doc_id = escape_graphql_string(&doc_id);
        let escaped_claimed_at = escape_graphql_string(&claimed_at);
        let escaped_deadline = escape_graphql_string(&deadline);
        let escaped_backend_id = escape_graphql_string(&self.backend_id);
        let escaped_behavior_id = escape_graphql_string(&self.behavior_id);
        let execution_origin = self.execution_origin.as_str();

        let mutation = format!(
            r#"mutation {{
                update_AgentRequest(
                    filter: {{
                        _docID: {{ _eq: "{escaped_doc_id}" }},
                        status: {{ _eq: "pending" }},
                        lifecycle_state: {{ _eq: "pending" }}
                    }},
                    input: {{
                        status: "processing",
                        lifecycle_state: "{lifecycle_state}",
                        behavior_id: "{escaped_behavior_id}",
                        backend_id: "{escaped_backend_id}",
                        execution_origin: "{execution_origin}",
                        claimed_at: "{escaped_claimed_at}",
                        deadline: "{escaped_deadline}"
                    }}
                ) {{ _docID }}
            }}"#,
            lifecycle_state = PersistedLifecycleState::Claimed.as_str(),
        );

        let resp =
            session::execute_mutation_with_retry(&self.node, &mutation, "claim_request").await?;

        if !resp
            .data
            .as_ref()
            .and_then(|d| d.get("update_AgentRequest"))
            .is_some_and(response_has_documents)
        {
            match self.request_status().await? {
                Some(status) if status == "processing" => {
                    tracing::debug!(doc_id = %doc_id, "claimed via post-update verification");
                }
                Some(status) => {
                    anyhow::bail!("request {} is no longer pending (status={status})", doc_id)
                }
                None => anyhow::bail!("request {} disappeared while claiming", doc_id),
            }
        } else {
            tracing::debug!(
                doc_id = %doc_id,
                deadline = %deadline,
                backend_id = %self.backend_id,
                execution_origin,
                "claimed agent request with deadline"
            );
        }

        let (request_version, claimed_request) = resolve_claimed_request_version(
            &self.node,
            &doc_id,
            &claimed_at,
            &deadline,
            &self.behavior_id,
            &self.backend_id,
            execution_origin,
            &commits_before_claim,
        )
        .await?;

        self.request = claimed_request;
        self.request_version = Some(request_version);

        self.state = LocalLifecycleState::Claimed;
        self.claimed_deadline_at = Some(deadline_at);
        self.valid_until_at_claim = valid_until_at_claim;

        Ok(ClaimOutcome::Claimed)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::identity::AgentIdentity as _;

    const TEST_AGENT_DID: &str = "did:test:claim-order-test";
    const TEST_BEHAVIOR_ID: &str = "general";
    const TEST_BACKEND_ID: &str = "backend-order";

    fn composite(cid: &str, composite_parents: &[&str]) -> CompositeHeadEvidenceRow {
        CompositeHeadEvidenceRow {
            cid: cid.to_string(),
            heads: composite_parents
                .iter()
                .map(|parent| CommitParentRow {
                    cid: (*parent).to_string(),
                    field_name: Some("_C".to_string()),
                })
                .collect(),
        }
    }

    fn provenance_request() -> AgentRequest {
        AgentRequest {
            doc_id: "doc-1".to_string(),
            request_id: "request-1".to_string(),
            agent_did: "did:key:target".to_string(),
            requester_did: Some("did:key:requester".to_string()),
            behavior_id: Some("general".to_string()),
            session_id: "session-1".to_string(),
            content: "execute this".to_string(),
            temperature: Some(0.5),
            top_p: Some(0.9),
            top_k: Some(40),
            max_tokens: Some(1000),
            metadata: Some("{}".to_string()),
            execution_origin: Some("interactive".to_string()),
            created_at: "2026-08-07T00:00:00Z".to_string(),
            deadline: None,
            subagent_depth: 0,
            caused_by_parent_request_id: None,
            caused_by_parent_tool_call_id: None,
        }
    }

    #[tokio::test]
    async fn claimed_lifecycle_cannot_transition_without_verified_execution_provenance() {
        let node = Arc::new(EmbeddedNode::builder().build().await.unwrap());
        let mut lifecycle = RequestLifecycle::new_with_execution_binding(
            node,
            TEST_BEHAVIOR_ID,
            "did:key:target",
            provenance_request(),
            30,
            ExecutionOrigin::Interactive,
            TEST_BACKEND_ID,
        );
        lifecycle.state = LocalLifecycleState::Claimed;

        assert!(lifecycle.set_response_doc_id("response-doc").is_err());
        assert!(lifecycle.begin_execution().await.is_err());
        assert!(lifecycle.complete().await.is_err());
        assert!(lifecycle.fail().await.is_err());
        assert!(lifecycle.transition_to_interrupted().await.is_err());
        assert!(lifecycle
            .record_failure_reason("no provenance")
            .await
            .is_err());
    }

    #[test]
    fn composite_graph_derives_one_current_head_by_subtracting_nested_heads() {
        let rows = vec![
            composite("source", &[]),
            composite("claim", &["source"]),
            composite("later", &["claim"]),
        ];
        let current = current_composite_heads(&rows);
        assert_eq!(current.len(), 1);
        assert_eq!(current[0].cid, "later");
        assert_eq!(
            exactly_one_composite(&current, "request").unwrap().cid,
            "later"
        );
    }

    #[test]
    fn composite_graph_rejects_missing_and_ambiguous_current_heads() {
        assert!(exactly_one_composite(&[], "request")
            .unwrap_err()
            .to_string()
            .contains("no current composite head"));

        let rows = vec![
            composite("source", &[]),
            composite("left", &["source"]),
            composite("right", &["source"]),
        ];
        let current = current_composite_heads(&rows);
        assert_eq!(current.len(), 2);
        assert!(exactly_one_composite(&current, "request")
            .unwrap_err()
            .to_string()
            .contains("ambiguous provenance"));
    }

    #[test]
    fn recovery_ancestry_walks_only_persisted_composite_parents() {
        let source = composite("source-cid", &[]);
        let claim = composite("claim-cid", &["source-cid"]);
        let processing = composite("processing-cid", &["claim-cid"]);
        let evidence = vec![source, claim, processing.clone()];
        let ancestry = composite_ancestry_cids(&evidence, &processing).unwrap();
        assert_eq!(
            ancestry,
            ["source-cid", "claim-cid", "processing-cid"]
                .into_iter()
                .map(str::to_string)
                .collect()
        );

        let incomplete = vec![composite("processing-cid", &["missing-claim-cid"])];
        let error = composite_ancestry_cids(&incomplete, &incomplete[0])
            .unwrap_err()
            .to_string();
        assert!(error.contains("missing composite parent"));
    }

    #[test]
    fn logical_request_identity_must_name_exactly_one_document() {
        let unique = HashSet::from(["doc-1".to_string()]);
        require_unique_logical_request_doc_id("request-1", &unique, "doc-1").unwrap();

        let duplicate = HashSet::from(["doc-1".to_string(), "doc-2".to_string()]);
        assert!(
            require_unique_logical_request_doc_id("request-1", &duplicate, "doc-1")
                .unwrap_err()
                .to_string()
                .contains("resolves to 2 distinct documents")
        );

        let wrong = HashSet::from(["doc-2".to_string()]);
        assert!(require_unique_logical_request_doc_id("request-1", &wrong, "doc-1").is_err());
        assert!(
            require_unique_logical_request_doc_id("request-1", &HashSet::new(), "doc-1").is_err()
        );
    }

    #[test]
    fn verified_source_and_claim_build_exact_execution_provenance() {
        let source_commit = composite("source-cid", &[]);
        let claim_commit = composite("claim-cid", &["source-cid"]);
        let source_request = provenance_request();
        let claimed_request = source_request.clone();

        let provenance = validate_verified_execution_provenance(
            &source_commit,
            crate::DocumentVersionRef::new("doc-1", "source-cid"),
            "did:key:source-author",
            "did:key:source-author",
            &source_request,
            &claim_commit,
            crate::DocumentVersionRef::new("doc-1", "claim-cid"),
            "did:key:target",
            "did:key:target",
            &claimed_request,
        )
        .unwrap();

        assert_eq!(provenance.source.version.composite_commit_cid, "source-cid");
        assert_eq!(provenance.source.signer_did, "did:key:source-author");
        assert_eq!(provenance.claim.version.composite_commit_cid, "claim-cid");
        assert_eq!(provenance.claim.signer_did, "did:key:target");
    }

    #[test]
    fn verified_provenance_fails_closed_on_signer_ancestry_and_payload_drift() {
        let source_commit = composite("source-cid", &[]);
        let valid_claim = composite("claim-cid", &["source-cid"]);
        let source_request = provenance_request();
        let source_version = || crate::DocumentVersionRef::new("doc-1", "source-cid");
        let claim_version = || crate::DocumentVersionRef::new("doc-1", "claim-cid");

        let validate = |source_signer: &str,
                        claim_signer: &str,
                        claim_commit: &CompositeHeadEvidenceRow,
                        claimed_request: &AgentRequest| {
            validate_verified_execution_provenance(
                &source_commit,
                source_version(),
                source_signer,
                "did:key:source-author",
                &source_request,
                claim_commit,
                claim_version(),
                claim_signer,
                "did:key:target",
                claimed_request,
            )
        };

        assert!(validate(
            "did:key:wrong",
            "did:key:target",
            &valid_claim,
            &source_request
        )
        .unwrap_err()
        .to_string()
        .contains("does not match expected writer"));
        assert!(validate(
            "did:key:source-author",
            "did:key:wrong",
            &valid_claim,
            &source_request
        )
        .unwrap_err()
        .to_string()
        .contains("does not match expected writer"));

        let wrong_parent = composite("claim-cid", &["other-source"]);
        assert!(validate(
            "did:key:source-author",
            "did:key:target",
            &wrong_parent,
            &source_request
        )
        .unwrap_err()
        .to_string()
        .contains("does not match source"));

        let ambiguous_parent = composite("claim-cid", &["source-cid", "other-source"]);
        assert!(validate(
            "did:key:source-author",
            "did:key:target",
            &ambiguous_parent,
            &source_request
        )
        .unwrap_err()
        .to_string()
        .contains("sole parent"));

        let mut changed_payload = source_request.clone();
        changed_payload.content = "tampered".to_string();
        assert!(validate(
            "did:key:source-author",
            "did:key:target",
            &valid_claim,
            &changed_payload
        )
        .unwrap_err()
        .to_string()
        .contains("changed the admitted source payload"));
    }

    async fn test_node() -> Arc<EmbeddedNode> {
        let node = Arc::new(EmbeddedNode::builder().build().await.unwrap());
        crate::ensure_runtime_schemas(node.as_ref()).await.unwrap();
        node
    }

    async fn signed_test_node() -> (Arc<EmbeddedNode>, String, tempfile::TempDir) {
        let key_dir = tempfile::tempdir().unwrap();
        let identity =
            crate::identity::KeyIdentity::load_or_create(key_dir.path().join("node.key"), None)
                .unwrap();
        let did = identity.did().to_string();
        let node = Arc::new(
            EmbeddedNode::builder()
                .with_node_identity_did(&did)
                .build()
                .await
                .unwrap(),
        );
        crate::ensure_runtime_schemas(node.as_ref()).await.unwrap();
        (node, did, key_dir)
    }

    async fn insert_pending_request(
        node: &EmbeddedNode,
        request_id: &str,
        session_id: &str,
        created_at: &str,
    ) -> AgentRequest {
        insert_pending_request_for_agent(node, request_id, session_id, created_at, TEST_AGENT_DID)
            .await
    }

    async fn insert_pending_request_for_agent(
        node: &EmbeddedNode,
        request_id: &str,
        session_id: &str,
        created_at: &str,
        agent_did: &str,
    ) -> AgentRequest {
        insert_pending_request_with_author(
            node, request_id, session_id, created_at, agent_did, agent_did,
        )
        .await
    }

    async fn insert_pending_request_with_author(
        node: &EmbeddedNode,
        request_id: &str,
        session_id: &str,
        created_at: &str,
        agent_did: &str,
        source_author_did: &str,
    ) -> AgentRequest {
        let escaped_request_id = escape_graphql_string(request_id);
        let escaped_session_id = escape_graphql_string(session_id);
        let escaped_created_at = escape_graphql_string(created_at);
        let escaped_agent_did = escape_graphql_string(agent_did);
        let escaped_source_author_did = escape_graphql_string(source_author_did);
        let mutation = format!(
            r#"mutation {{
                create_AgentRequest(input: {{
                    request_id: "{escaped_request_id}",
                    agent_did: "{escaped_agent_did}",
                    source_author_did: "{escaped_source_author_did}",
                    behavior_id: "{TEST_BEHAVIOR_ID}",
                    session_id: "{escaped_session_id}",
                    retry_parent_request: "",
                    retry_root_request: "{escaped_request_id}",
                    superseded_by_request: "",
                    content: "same-session request",
                    status: "pending",
                    lifecycle_state: "pending",
                    backend_id: "",
                    execution_origin: "interactive",
                    failure_reason: "",
                    created_at: "{escaped_created_at}",
                    retry_count: 0,
                    max_retries: {max_retries},
                    subagent_depth: 0
                }}) {{ _docID }}
            }}"#,
            max_retries = DEFAULT_REQUEST_MAX_RETRIES,
        );
        let response =
            session::execute_mutation_with_retry(node, &mutation, "insert_pending_request")
                .await
                .unwrap();
        let inline_doc_id = response
            .data
            .as_ref()
            .and_then(|data| data.get("create_AgentRequest"))
            .and_then(|value| {
                value
                    .get("_docID")
                    .and_then(|doc_id| doc_id.as_str())
                    .or_else(|| {
                        value
                            .as_array()
                            .and_then(|rows| rows.first())
                            .and_then(|row| row.get("_docID"))
                            .and_then(|doc_id| doc_id.as_str())
                    })
            })
            .map(ToOwned::to_owned);
        let doc_id = match inline_doc_id {
            Some(doc_id) => doc_id,
            None => lookup_request_doc_id(node, request_id)
                .await
                .expect("created AgentRequest doc id"),
        };

        AgentRequest {
            doc_id,
            request_id: request_id.to_string(),
            agent_did: agent_did.to_string(),
            requester_did: None,
            behavior_id: Some(TEST_BEHAVIOR_ID.to_string()),
            session_id: session_id.to_string(),
            content: "same-session request".to_string(),
            temperature: None,
            top_p: None,
            top_k: None,
            max_tokens: None,
            metadata: None,
            execution_origin: Some("interactive".to_string()),
            created_at: created_at.to_string(),
            deadline: None,
            subagent_depth: 0,
            caused_by_parent_request_id: None,
            caused_by_parent_tool_call_id: None,
        }
    }

    async fn lookup_request_doc_id(
        node: &EmbeddedNode,
        request_id: &str,
    ) -> anyhow::Result<String> {
        let escaped_request_id = escape_graphql_string(request_id);
        let query = format!(
            r#"{{
                AgentRequest(
                    filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                    limit: 1
                ) {{ _docID }}
            }}"#
        );
        let response = node.execute(&query).await;
        if response.has_errors() {
            anyhow::bail!("query created AgentRequest failed: {:?}", response.errors);
        }
        response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(|value| value.as_array())
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("_docID"))
            .and_then(|value| value.as_str())
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow::anyhow!("AgentRequest {request_id} not found"))
    }

    #[tokio::test]
    async fn claim_preserves_same_session_ordering() {
        let (node, agent_did, _key_dir) = signed_test_node().await;
        let first = insert_pending_request_for_agent(
            node.as_ref(),
            "same-session-request-1",
            "same-session",
            "2026-01-01T00:00:00Z",
            &agent_did,
        )
        .await;
        let second = insert_pending_request_for_agent(
            node.as_ref(),
            "same-session-request-2",
            "same-session",
            "2026-01-01T00:00:01Z",
            &agent_did,
        )
        .await;

        let mut first_lifecycle = RequestLifecycle::new_with_execution_binding(
            node.clone(),
            TEST_BEHAVIOR_ID,
            &agent_did,
            first,
            60,
            ExecutionOrigin::Interactive,
            TEST_BACKEND_ID,
        );
        let mut second_lifecycle = RequestLifecycle::new_with_execution_binding(
            node,
            TEST_BEHAVIOR_ID,
            &agent_did,
            second,
            60,
            ExecutionOrigin::Interactive,
            TEST_BACKEND_ID,
        );

        assert_eq!(
            first_lifecycle
                .claim_without_identity_for_test()
                .await
                .unwrap(),
            ClaimOutcome::Claimed
        );
        assert_eq!(
            second_lifecycle
                .claim_without_identity_for_test()
                .await
                .unwrap(),
            ClaimOutcome::Queued
        );
    }

    #[tokio::test]
    async fn claim_reloads_the_exact_composite_snapshot_it_pins() {
        let (node, agent_did, _key_dir) = signed_test_node().await;
        let stale_request = insert_pending_request_for_agent(
            node.as_ref(),
            "claim-version-request",
            "claim-version-session",
            "2026-01-01T00:00:00Z",
            &agent_did,
        )
        .await;
        let doc_id = stale_request.doc_id.clone();
        let mutation = format!(
            r#"mutation {{
                update_AgentRequest(
                    filter: {{ _docID: {{ _eq: "{}" }} }},
                    input: {{ content: "edited after watcher read" }}
                ) {{ _docID }}
            }}"#,
            escape_graphql_string(&doc_id),
        );
        let response = node.execute(&mutation).await;
        assert!(
            !response.has_errors(),
            "pre-claim edit failed: {:?}",
            response.errors
        );

        let mut lifecycle = RequestLifecycle::new_with_execution_binding(
            node.clone(),
            TEST_BEHAVIOR_ID,
            &agent_did,
            stale_request,
            60,
            ExecutionOrigin::Interactive,
            TEST_BACKEND_ID,
        );
        assert_eq!(
            lifecycle.claim_without_identity_for_test().await.unwrap(),
            ClaimOutcome::Claimed
        );

        assert_eq!(lifecycle.request().content, "edited after watcher read");
        let version = lifecycle
            .request_version()
            .expect("claim must pin an AgentRequest version");
        assert_eq!(version.doc_id, doc_id);
        assert!(!version.composite_commit_cid.is_empty());

        let snapshot = crate::watcher::load_agent_request_at_cid(
            node.as_ref(),
            &version.composite_commit_cid,
            &doc_id,
        )
        .await
        .unwrap()
        .expect("pinned claim snapshot");
        assert_eq!(snapshot.status, "processing");
        assert_eq!(snapshot.lifecycle_state.as_deref(), Some("claimed"));
        assert_eq!(snapshot.request.content, lifecycle.request().content);
    }

    #[tokio::test]
    async fn claim_version_resolution_rejects_later_marker_preserving_edits() {
        let node = test_node().await;
        let pending = insert_pending_request(
            node.as_ref(),
            "claim-version-race-request",
            "claim-version-race-session",
            "2026-01-01T00:00:00Z",
        )
        .await;
        let doc_id = pending.doc_id.clone();
        let commits_before_claim = current_composite_commit_cids(node.as_ref(), &doc_id)
            .await
            .unwrap();
        let claimed_at = "2026-01-01T00:00:01Z";
        let deadline = "2026-01-01T00:01:01Z";
        let claim = format!(
            r#"mutation {{
                update_AgentRequest(
                    filter: {{
                        _docID: {{ _eq: "{}" }},
                        status: {{ _eq: "pending" }},
                        lifecycle_state: {{ _eq: "pending" }}
                    }},
                    input: {{
                        status: "processing",
                        lifecycle_state: "claimed",
                        behavior_id: "{TEST_BEHAVIOR_ID}",
                        backend_id: "{TEST_BACKEND_ID}",
                        execution_origin: "interactive",
                        claimed_at: "{claimed_at}",
                        deadline: "{deadline}"
                    }}
                ) {{ _docID }}
            }}"#,
            escape_graphql_string(&doc_id),
        );
        let response = node.execute(&claim).await;
        assert!(
            !response.has_errors(),
            "claim failed: {:?}",
            response.errors
        );

        // This commit inherits every claim marker. A newest-first scan would
        // therefore pin this later content instead of the claim boundary.
        let edit = format!(
            r#"mutation {{
                update_AgentRequest(
                    filter: {{ _docID: {{ _eq: "{}" }} }},
                    input: {{ content: "edited after claim" }}
                ) {{ _docID }}
            }}"#,
            escape_graphql_string(&doc_id),
        );
        let response = node.execute(&edit).await;
        assert!(
            !response.has_errors(),
            "post-claim edit failed: {:?}",
            response.errors
        );

        let (version, snapshot) = resolve_claimed_request_version(
            node.as_ref(),
            &doc_id,
            claimed_at,
            deadline,
            TEST_BEHAVIOR_ID,
            TEST_BACKEND_ID,
            "interactive",
            &commits_before_claim,
        )
        .await
        .unwrap();
        assert_eq!(snapshot.content, pending.content);
        assert_ne!(snapshot.content, "edited after claim");

        let reconstructed = crate::watcher::load_agent_request_at_cid(
            node.as_ref(),
            &version.composite_commit_cid,
            &doc_id,
        )
        .await
        .unwrap()
        .expect("pinned claim snapshot");
        assert_eq!(reconstructed.request.content, pending.content);
    }

    #[tokio::test]
    async fn signed_claim_commits_verified_source_and_claim_as_one_boundary() {
        let (node, agent_did, _key_dir) = signed_test_node().await;
        let pending = insert_pending_request_for_agent(
            node.as_ref(),
            "signed-claim-request",
            "signed-claim-session",
            "2026-01-01T00:00:00Z",
            &agent_did,
        )
        .await;
        let source_request = pending.clone();
        let mut lifecycle = RequestLifecycle::new_with_execution_binding(
            node.clone(),
            TEST_BEHAVIOR_ID,
            &agent_did,
            pending,
            60,
            ExecutionOrigin::Interactive,
            TEST_BACKEND_ID,
        );

        assert_eq!(
            lifecycle.claim_with_identity().await.unwrap(),
            ClaimOutcome::Claimed
        );
        let provenance = lifecycle
            .execution_provenance()
            .expect("signed claim provenance");
        assert_eq!(provenance.source.signer_did, agent_did);
        assert_eq!(provenance.claim.signer_did, agent_did);
        assert_eq!(lifecycle.request_version(), Some(&provenance.claim.version));
        assert_ne!(
            provenance.source.version.composite_commit_cid,
            provenance.claim.version.composite_commit_cid
        );

        let commits_before_replay =
            current_composite_commit_cids(node.as_ref(), &source_request.doc_id)
                .await
                .unwrap();
        let mut replay = RequestLifecycle::new_with_execution_binding(
            node.clone(),
            TEST_BEHAVIOR_ID,
            &agent_did,
            source_request.clone(),
            60,
            ExecutionOrigin::Interactive,
            TEST_BACKEND_ID,
        );
        let error = replay
            .claim_with_identity()
            .await
            .expect_err("an already-claimed request must not be adopted as success");
        assert!(error.to_string().contains("is not pending/pending"));
        assert!(replay.execution_provenance().is_none());
        assert_eq!(replay.state, LocalLifecycleState::Pending);
        assert_eq!(
            current_composite_commit_cids(node.as_ref(), &source_request.doc_id)
                .await
                .unwrap(),
            commits_before_replay,
            "rejected replay must not create a commit"
        );
    }

    #[tokio::test]
    async fn signed_claim_rejects_duplicate_logical_request_ids_without_writing() {
        let (node, agent_did, _key_dir) = signed_test_node().await;
        let first = insert_pending_request_for_agent(
            node.as_ref(),
            "duplicate-logical-request",
            "duplicate-session-a",
            "2026-01-01T00:00:00Z",
            &agent_did,
        )
        .await;
        let second = insert_pending_request_for_agent(
            node.as_ref(),
            "duplicate-logical-request-other",
            "duplicate-session-b",
            "2026-01-01T00:00:01Z",
            &agent_did,
        )
        .await;
        assert_ne!(first.doc_id, second.doc_id);
        let mutation = format!(
            r#"mutation {{
                update_AgentRequest(
                    filter: {{ _docID: {{ _eq: "{}" }} }},
                    input: {{ request_id: "duplicate-logical-request" }}
                ) {{ _docID }}
            }}"#,
            escape_graphql_string(&second.doc_id),
        );
        let response = node.execute(&mutation).await;
        assert!(
            !response.has_errors(),
            "creating logical-ID conflict failed: {:?}",
            response.errors
        );
        let commits_before_claim = current_composite_commit_cids(node.as_ref(), &first.doc_id)
            .await
            .unwrap();
        let mut lifecycle = RequestLifecycle::new_with_execution_binding(
            node.clone(),
            TEST_BEHAVIOR_ID,
            &agent_did,
            first.clone(),
            60,
            ExecutionOrigin::Interactive,
            TEST_BACKEND_ID,
        );

        let error = lifecycle
            .claim_with_identity()
            .await
            .expect_err("duplicate logical request IDs must fail closed");
        assert!(error
            .to_string()
            .contains("resolves to 2 distinct documents"));
        assert!(lifecycle.execution_provenance().is_none());
        assert_eq!(lifecycle.state, LocalLifecycleState::Pending);
        assert_eq!(
            current_composite_commit_cids(node.as_ref(), &first.doc_id)
                .await
                .unwrap(),
            commits_before_claim,
            "duplicate rejection must not create a claim commit"
        );
    }

    #[tokio::test]
    async fn signed_claim_rejects_unsigned_source_without_writing() {
        let node = test_node().await;
        let pending = insert_pending_request(
            node.as_ref(),
            "unsigned-source-request",
            "unsigned-source-session",
            "2026-01-01T00:00:00Z",
        )
        .await;
        let commits_before_claim = current_composite_commit_cids(node.as_ref(), &pending.doc_id)
            .await
            .unwrap();
        let mut lifecycle = RequestLifecycle::new_with_execution_binding(
            node.clone(),
            TEST_BEHAVIOR_ID,
            TEST_AGENT_DID,
            pending.clone(),
            60,
            ExecutionOrigin::Interactive,
            TEST_BACKEND_ID,
        );

        let error = lifecycle
            .claim_with_identity()
            .await
            .expect_err("unsigned source must fail closed");
        assert!(error
            .to_string()
            .contains("cryptographically verifying AgentRequest"));
        assert!(lifecycle.execution_provenance().is_none());
        assert_eq!(lifecycle.state, LocalLifecycleState::Pending);
        assert_eq!(
            current_composite_commit_cids(node.as_ref(), &pending.doc_id)
                .await
                .unwrap(),
            commits_before_claim,
            "unsigned-source rejection must not create a claim commit"
        );
    }

    #[tokio::test]
    async fn signed_claim_rejects_declared_author_mismatch_without_writing() {
        let (node, agent_did, _key_dir) = signed_test_node().await;
        let pending = insert_pending_request_with_author(
            node.as_ref(),
            "author-mismatch-request",
            "author-mismatch-session",
            "2026-01-01T00:00:00Z",
            &agent_did,
            "did:key:zDeclaredButNotSigner",
        )
        .await;
        let commits_before_claim = current_composite_commit_cids(node.as_ref(), &pending.doc_id)
            .await
            .unwrap();
        let mut lifecycle = RequestLifecycle::new_with_execution_binding(
            node.clone(),
            TEST_BEHAVIOR_ID,
            &agent_did,
            pending.clone(),
            60,
            ExecutionOrigin::Interactive,
            TEST_BACKEND_ID,
        );

        let error = lifecycle
            .claim_with_identity()
            .await
            .expect_err("declared source author mismatch must fail closed");
        assert!(error.to_string().contains("does not match expected writer"));
        assert!(lifecycle.execution_provenance().is_none());
        assert_eq!(lifecycle.state, LocalLifecycleState::Pending);
        assert_eq!(
            current_composite_commit_cids(node.as_ref(), &pending.doc_id)
                .await
                .unwrap(),
            commits_before_claim,
            "author mismatch rejection must not create a claim commit"
        );
    }

    #[tokio::test]
    async fn signed_claim_transaction_rechecks_interrupt_and_expiry_on_exact_source() {
        let (node, agent_did, _key_dir) = signed_test_node().await;
        let interrupted = insert_pending_request_for_agent(
            node.as_ref(),
            "interrupted-source-request",
            "interrupted-source-session",
            "2026-01-01T00:00:00Z",
            &agent_did,
        )
        .await;
        let mutation = format!(
            r#"mutation {{
                update_AgentRequest(
                    filter: {{ _docID: {{ _eq: "{}" }} }},
                    input: {{ interrupt_requested_at: "2026-01-01T00:00:01Z" }}
                ) {{ _docID }}
            }}"#,
            escape_graphql_string(&interrupted.doc_id),
        );
        let response = node.execute(&mutation).await;
        assert!(
            !response.has_errors(),
            "interrupt failed: {:?}",
            response.errors
        );
        let interrupted_commits = current_composite_commit_cids(node.as_ref(), &interrupted.doc_id)
            .await
            .unwrap();
        let error = claim_with_verified_provenance(
            node.as_ref(),
            &interrupted.doc_id,
            &agent_did,
            "2026-01-01T00:00:02Z",
            "2026-01-01T00:01:02Z",
            TEST_BEHAVIOR_ID,
            TEST_BACKEND_ID,
            "interactive",
        )
        .await
        .expect_err("exact interrupted source must fail inside claim transaction");
        assert!(error
            .to_string()
            .contains("interrupted before its signed claim"));
        assert_eq!(
            current_composite_commit_cids(node.as_ref(), &interrupted.doc_id)
                .await
                .unwrap(),
            interrupted_commits
        );

        let expired = insert_pending_request_for_agent(
            node.as_ref(),
            "expired-source-request",
            "expired-source-session",
            "2026-01-01T00:00:00Z",
            &agent_did,
        )
        .await;
        let mutation = format!(
            r#"mutation {{
                update_AgentRequest(
                    filter: {{ _docID: {{ _eq: "{}" }} }},
                    input: {{ valid_until: "2026-01-01T00:00:01Z" }}
                ) {{ _docID }}
            }}"#,
            escape_graphql_string(&expired.doc_id),
        );
        let response = node.execute(&mutation).await;
        assert!(
            !response.has_errors(),
            "expiry setup failed: {:?}",
            response.errors
        );
        let expired_commits = current_composite_commit_cids(node.as_ref(), &expired.doc_id)
            .await
            .unwrap();
        let error = claim_with_verified_provenance(
            node.as_ref(),
            &expired.doc_id,
            &agent_did,
            "2026-01-01T00:00:02Z",
            "2026-01-01T00:01:02Z",
            TEST_BEHAVIOR_ID,
            TEST_BACKEND_ID,
            "interactive",
        )
        .await
        .expect_err("exact expired source must fail inside claim transaction");
        assert!(error
            .to_string()
            .contains("expired before its signed claim"));
        assert_eq!(
            current_composite_commit_cids(node.as_ref(), &expired.doc_id)
                .await
                .unwrap(),
            expired_commits
        );
    }
}
