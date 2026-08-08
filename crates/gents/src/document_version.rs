use std::collections::HashSet;

use anyhow::{anyhow, Result};
use defra_node::{EmbeddedNode, ExecuteRetryPolicy, QueryRequest};
use identity::Did;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
struct CommitParentRow {
    cid: String,
    #[serde(rename = "fieldName")]
    field_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct CompositeHeadEvidenceRow {
    cid: String,
    #[serde(default)]
    heads: Vec<CommitParentRow>,
}

fn sole_current_composite_head<'a>(
    rows: &'a [CompositeHeadEvidenceRow],
    collection: &str,
    doc_id: &str,
) -> Result<&'a CompositeHeadEvidenceRow> {
    let nested_composite_cids = rows
        .iter()
        .flat_map(|row| row.heads.iter())
        .filter(|head| head.field_name.as_deref() == Some("_C"))
        .map(|head| head.cid.as_str())
        .collect::<HashSet<_>>();
    let current = rows
        .iter()
        .filter(|row| !nested_composite_cids.contains(row.cid.as_str()))
        .collect::<Vec<_>>();
    match current.as_slice() {
        [current] => Ok(*current),
        [] => anyhow::bail!("{collection} {doc_id} has no current composite head"),
        current => anyhow::bail!(
            "{collection} {doc_id} has {} current composite heads; refusing ambiguous provenance",
            current.len()
        ),
    }
}

/// Resolve the sole current composite version of one DefraDB document and
/// cryptographically verify the signer of that exact commit.
///
/// This is intentionally collection-agnostic. Callers remain responsible for
/// reloading the CID and validating collection-specific facts such as lifecycle
/// state and logical ownership before treating the version as admitted input.
pub(crate) async fn verified_current_signed_document_version(
    node: &EmbeddedNode,
    collection: &str,
    doc_id: &str,
) -> Result<SignedDocumentVersionRef> {
    verified_current_signed_document_version_with_identity(node, collection, doc_id, None).await
}

/// Identity-aware variant used by correctness paths that will later be ACP
/// protected. The query identity is authorization context only; signer
/// verification below remains the authorship proof.
pub(crate) async fn verified_current_signed_document_version_with_identity(
    node: &EmbeddedNode,
    collection: &str,
    doc_id: &str,
    identity: Option<Did>,
) -> Result<SignedDocumentVersionRef> {
    let escaped_doc_id = crate::graphql::escape_graphql_string(doc_id);
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
        .execute_request_with_retry(
            QueryRequest::new(query).with_identity(identity),
            ExecuteRetryPolicy::default(),
        )
        .await;
    if response.has_errors() {
        anyhow::bail!(
            "querying {collection} {doc_id} composite evidence failed: {:?}",
            response.errors
        );
    }
    let rows = response
        .data
        .as_ref()
        .and_then(|data| data.get("_commits"))
        .cloned()
        .map(serde_json::from_value::<Vec<CompositeHeadEvidenceRow>>)
        .transpose()?
        .unwrap_or_default();
    let current = sole_current_composite_head(&rows, collection, doc_id)?;
    let signer_did = node
        .verified_block_signer_did(&current.cid)
        .await
        .map_err(|error| {
            anyhow!(
                "cryptographically verifying {collection} {doc_id} composite commit {}: {error}",
                current.cid
            )
        })?;
    if signer_did.trim().is_empty() {
        anyhow::bail!(
            "cryptographically verifying {collection} {doc_id} composite commit {} returned an empty signer DID",
            current.cid
        );
    }
    Ok(SignedDocumentVersionRef::new(
        DocumentVersionRef::new(doc_id, &current.cid),
        signer_did,
    ))
}

/// An immutable point in one DefraDB document's history.
///
/// `_docID` is the stable document identity. `composite_commit_cid` is the
/// content-addressed composite commit that reconstructs the exact snapshot the
/// runtime consumed. Neither value substitutes for the other.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentVersionRef {
    pub doc_id: String,
    pub composite_commit_cid: String,
}

impl DocumentVersionRef {
    pub(crate) fn new(doc_id: impl Into<String>, composite_commit_cid: impl Into<String>) -> Self {
        Self {
            doc_id: doc_id.into(),
            composite_commit_cid: composite_commit_cid.into(),
        }
    }
}

/// One exact DefraDB document version together with its cryptographically
/// verified commit author.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedDocumentVersionRef {
    pub version: DocumentVersionRef,
    pub signer_did: String,
}

impl SignedDocumentVersionRef {
    pub(crate) fn new(version: DocumentVersionRef, signer_did: impl Into<String>) -> Self {
        Self {
            version,
            signer_did: signer_did.into(),
        }
    }
}

/// One exact, signed configuration fact used during behavior resolution.
///
/// `collection` and `logical_id` retain the semantic edge that a bare DefraDB
/// document version cannot express. `source` is the cryptographically verified
/// immutable document snapshot from which the runtime parsed the value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigFactRef {
    pub collection: String,
    pub logical_id: String,
    pub source: SignedDocumentVersionRef,
}

impl ConfigFactRef {
    pub(crate) fn new(
        collection: impl Into<String>,
        logical_id: impl Into<String>,
        source: SignedDocumentVersionRef,
    ) -> Self {
        Self {
            collection: collection.into(),
            logical_id: logical_id.into(),
            source,
        }
    }

    fn validate(&self, expected_collection: &str) -> anyhow::Result<()> {
        if self.collection != expected_collection {
            anyhow::bail!(
                "configuration fact collection {} does not match expected {expected_collection}",
                self.collection
            );
        }
        if self.logical_id.trim().is_empty() {
            anyhow::bail!("{expected_collection} configuration fact has an empty logical id");
        }
        if self.source.version.doc_id.trim().is_empty()
            || self.source.version.composite_commit_cid.trim().is_empty()
        {
            anyhow::bail!(
                "{expected_collection} {} configuration fact requires a document id and composite commit CID",
                self.logical_id
            );
        }
        if self.source.signer_did.trim().is_empty() {
            anyhow::bail!(
                "{expected_collection} {} configuration fact requires a verified signer DID",
                self.logical_id
            );
        }
        Ok(())
    }
}

/// The exact signed document set used to resolve one behavior configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedBehaviorConfigProvenance {
    pub principal: ConfigFactRef,
    pub behavior: ConfigFactRef,
    pub inference_backend: ConfigFactRef,
    pub inference_profile: ConfigFactRef,
    pub tool_selection: Option<ConfigFactRef>,
    /// Effective skills in canonical ascending `logical_id` order.
    pub skills: Vec<ConfigFactRef>,
    pub resolution_algorithm_version: u32,
}

impl ResolvedBehaviorConfigProvenance {
    pub fn validate_for_behavior(&self, behavior_id: &str, agent_did: &str) -> anyhow::Result<()> {
        if behavior_id.trim().is_empty() {
            anyhow::bail!("behavior configuration provenance requires a behavior id");
        }
        if agent_did.trim().is_empty() {
            anyhow::bail!("behavior configuration provenance requires an agent DID");
        }
        if self.resolution_algorithm_version == 0 {
            anyhow::bail!("behavior configuration provenance requires a non-zero resolution algorithm version");
        }

        self.principal.validate("AgentPrincipal")?;
        self.behavior.validate("AgentBehavior")?;
        self.inference_backend.validate("InferenceBackend")?;
        self.inference_profile.validate("InferenceProfile")?;
        if let Some(tool_selection) = &self.tool_selection {
            tool_selection.validate("ToolSelection")?;
        }
        for skill in &self.skills {
            skill.validate("Skill")?;
        }

        if self.principal.logical_id != agent_did {
            anyhow::bail!(
                "principal provenance {} does not match agent {agent_did}",
                self.principal.logical_id
            );
        }
        if self.behavior.logical_id != behavior_id {
            anyhow::bail!(
                "behavior provenance {} does not match behavior {behavior_id}",
                self.behavior.logical_id
            );
        }
        if let Some((left, right)) = self.skills.windows(2).find_map(|pair| {
            (pair[0].logical_id >= pair[1].logical_id).then_some((&pair[0], &pair[1]))
        }) {
            anyhow::bail!(
                "skill provenance must be unique and canonically ordered; found {} before {}",
                left.logical_id,
                right.logical_id
            );
        }
        Ok(())
    }
}

/// Provenance boundary for one request execution.
///
/// `source` is the sole current composite head admitted before the claim.
/// `claim` is the exact target-agent-authored child version whose only
/// composite parent is `source`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestExecutionProvenance {
    pub source: SignedDocumentVersionRef,
    pub claim: SignedDocumentVersionRef,
}

impl RequestExecutionProvenance {
    pub(crate) fn new(source: SignedDocumentVersionRef, claim: SignedDocumentVersionRef) -> Self {
        Self { source, claim }
    }

    pub(crate) fn validate_for_request(
        &self,
        request_doc_id: &str,
        target_agent_did: &str,
    ) -> anyhow::Result<()> {
        if request_doc_id.trim().is_empty() {
            anyhow::bail!("request provenance requires a non-empty document id");
        }
        if target_agent_did.trim().is_empty() {
            anyhow::bail!("request provenance requires a non-empty target agent DID");
        }
        if self.source.version.doc_id != request_doc_id
            || self.claim.version.doc_id != request_doc_id
        {
            anyhow::bail!(
                "request provenance source and claim must both reference document {request_doc_id}"
            );
        }
        if self.source.version.composite_commit_cid.trim().is_empty()
            || self.claim.version.composite_commit_cid.trim().is_empty()
        {
            anyhow::bail!("request provenance source and claim CIDs must be non-empty");
        }
        if self.source.version.composite_commit_cid == self.claim.version.composite_commit_cid {
            anyhow::bail!("request provenance source and claim CIDs must be distinct");
        }
        if self.source.signer_did.trim().is_empty() || self.claim.signer_did.trim().is_empty() {
            anyhow::bail!("request provenance source and claim signer DIDs must be non-empty");
        }
        if self.claim.signer_did != target_agent_did {
            anyhow::bail!(
                "request provenance claim signer {} does not match target agent {}",
                self.claim.signer_did,
                target_agent_did
            );
        }
        Ok(())
    }
}

#[doc(hidden)]
pub(crate) fn test_request_execution_provenance(
    doc_id: &str,
    claim_signer_did: &str,
) -> RequestExecutionProvenance {
    RequestExecutionProvenance::new(
        SignedDocumentVersionRef::new(
            DocumentVersionRef::new(doc_id, "bafy-source-1"),
            "did:key:source",
        ),
        SignedDocumentVersionRef::new(
            DocumentVersionRef::new(doc_id, "bafy-claim-1"),
            claim_signer_did,
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn head(cid: &str, parents: &[&str]) -> CompositeHeadEvidenceRow {
        CompositeHeadEvidenceRow {
            cid: cid.to_string(),
            heads: parents
                .iter()
                .map(|cid| CommitParentRow {
                    cid: (*cid).to_string(),
                    field_name: Some("_C".to_string()),
                })
                .collect(),
        }
    }

    #[test]
    fn exact_provenance_requires_one_current_composite_head() {
        let empty = sole_current_composite_head(&[], "Skill", "doc-skill")
            .unwrap_err()
            .to_string();
        assert!(empty.contains("no current composite head"));

        let ambiguous_rows = [head("bafy-left", &[]), head("bafy-right", &[])];
        let ambiguous = sole_current_composite_head(&ambiguous_rows, "Skill", "doc-skill")
            .unwrap_err()
            .to_string();
        assert!(ambiguous.contains("2 current composite heads"));

        let linear_rows = [
            head("bafy-parent", &[]),
            head("bafy-child", &["bafy-parent"]),
        ];
        assert_eq!(
            sole_current_composite_head(&linear_rows, "Skill", "doc-skill")
                .unwrap()
                .cid,
            "bafy-child"
        );
    }

    fn fact(collection: &str, logical_id: &str) -> ConfigFactRef {
        ConfigFactRef::new(
            collection,
            logical_id,
            SignedDocumentVersionRef::new(
                DocumentVersionRef::new(format!("doc-{logical_id}"), format!("bafy-{logical_id}")),
                "did:key:zSigner",
            ),
        )
    }

    fn provenance() -> ResolvedBehaviorConfigProvenance {
        ResolvedBehaviorConfigProvenance {
            principal: fact("AgentPrincipal", "did:key:zAgent"),
            behavior: fact("AgentBehavior", "default"),
            inference_backend: fact("InferenceBackend", "backend"),
            inference_profile: fact("InferenceProfile", "profile"),
            tool_selection: Some(fact("ToolSelection", "tools")),
            skills: vec![fact("Skill", "alpha"), fact("Skill", "zeta")],
            resolution_algorithm_version: 1,
        }
    }

    #[test]
    fn resolved_behavior_config_provenance_accepts_canonical_exact_facts() {
        provenance()
            .validate_for_behavior("default", "did:key:zAgent")
            .unwrap();
    }

    #[test]
    fn resolved_behavior_config_provenance_rejects_duplicate_or_unsorted_skills() {
        let mut duplicate = provenance();
        duplicate.skills[1] = duplicate.skills[0].clone();
        assert!(duplicate
            .validate_for_behavior("default", "did:key:zAgent")
            .unwrap_err()
            .to_string()
            .contains("unique and canonically ordered"));

        let mut unsorted = provenance();
        unsorted.skills.reverse();
        assert!(unsorted
            .validate_for_behavior("default", "did:key:zAgent")
            .unwrap_err()
            .to_string()
            .contains("unique and canonically ordered"));
    }
}
