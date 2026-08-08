//! The durable writer behind the capture seam.
//!
//! `Proofs/RenderedCapture.lean` specifies this function exactly, and the three
//! outcomes are not negotiable:
//!
//! | store state for `capture_key` | outcome | write |
//! |---|---|---|
//! | unbound | `fresh` | create |
//! | bound to the identical canonical capture fact | `idempotent` | none |
//! | bound to a *different* canonical capture fact | `rejected` | none, and an error |
//!
//! `capture_rejects_rebinding` is why the third row is an error rather than an
//! update: one capture key names one provider request for the life of the
//! store. `capture_failure_blocks_send` is why an error here has to reach the
//! transport — the caller is
//! [`crate::rendered_request::transport::RenderedRequestCapturingHttpClient`],
//! which refuses the HTTP call on any error this returns.
//!
//! ## Identity
//!
//! Writes go through `EmbeddedNode::execute_request_with_retry` with the
//! agent's DID attached to the `QueryRequest`, not through the identity-less
//! `EmbeddedNode::execute`. `QueryRequest` identity is the ACP-check input; it
//! is not signature evidence. The sink separately verifies the composite
//! commit signer before returning the exact durable RenderedRequest version.
//! Attaching the DID also means owner registration works unchanged once a
//! policy can be installed (blocked on defradb.rs#1318). Construction fails unless the
//! declared agent DID is valid and matches the DefraDB node signing identity.
//! Every read and write carries that DID as the query identity, while commit
//! signature verification independently proves authorship. Missing or
//! malformed identity refuses the provider call before any capture query or
//! mutation is attempted.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use defra_node::{EmbeddedNode, ExecuteRetryPolicy, QueryRequest, QueryResponse};
use identity::Did;
use serde_json::Value;

use super::{
    canonical_json, canonical_json_string, RenderedCompletionRequest,
    RenderedRequestCaptureFactory, RenderedRequestCaptureSink, RenderedRequestContext,
};
use crate::graphql::escape_graphql_string;

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

#[derive(Debug, serde::Deserialize)]
struct InferenceCallSnapshot {
    #[serde(rename = "_docID")]
    doc_id: String,
    request_id: String,
    behavior_id: String,
    agent_did: String,
    call_state: String,
}

fn sole_current_composite_head<'a>(
    rows: &'a [CompositeHeadEvidenceRow],
    collection: &str,
    doc_id: &str,
) -> Result<&'a CompositeHeadEvidenceRow> {
    let nested = rows
        .iter()
        .flat_map(|row| row.heads.iter())
        .filter(|head| head.field_name.as_deref() == Some("_C"))
        .map(|head| head.cid.as_str())
        .collect::<HashSet<_>>();
    let current = rows
        .iter()
        .filter(|row| !nested.contains(row.cid.as_str()))
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

/// The DefraDB-backed capture sink.
#[derive(Clone)]
pub struct DefraRenderedRequestSink {
    node: Arc<EmbeddedNode>,
    identity: Did,
}

impl std::fmt::Debug for DefraRenderedRequestSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefraRenderedRequestSink")
            .field("identity", &self.identity.as_str())
            .finish()
    }
}

impl DefraRenderedRequestSink {
    pub fn new(node: Arc<EmbeddedNode>, agent_did: &str) -> Result<Self> {
        let node_did = node.node_identity_did().ok_or_else(|| {
            anyhow!("RenderedRequest capture requires a configured DefraDB node signing identity")
        })?;
        if node_did != agent_did {
            anyhow::bail!(
                "RenderedRequest capture agent DID {agent_did} does not match node signing identity {node_did}"
            );
        }
        let identity = Did::new(agent_did)
            .with_context(|| format!("parsing RenderedRequest capture agent DID {agent_did}"))?;
        Ok(Self { node, identity })
    }

    async fn execute(&self, graphql: &str, operation: &str, warn_on_error: bool) -> QueryResponse {
        let response = self
            .node
            .execute_request_with_retry(
                QueryRequest::new(graphql).with_identity(Some(self.identity.clone())),
                ExecuteRetryPolicy::default(),
            )
            .await;
        if warn_on_error && response.has_errors() {
            tracing::warn!(
                operation = %operation,
                errors = ?response.errors,
                "rendered-request capture statement failed"
            );
        }
        response
    }

    async fn composite_evidence(
        &self,
        collection: &str,
        doc_id: &str,
    ) -> Result<Vec<CompositeHeadEvidenceRow>> {
        let escaped_doc_id = escape_graphql_string(doc_id);
        let response = self
            .execute(
                &format!(
                    r#"query {{
                        _commits(
                            docID: ["{escaped_doc_id}"],
                            filter: {{ fieldName: {{ _eq: "_C" }} }}
                        ) {{
                            cid
                            heads {{ cid fieldName }}
                        }}
                    }}"#
                ),
                "rendered_request::composite_evidence",
                true,
            )
            .await;
        if response.has_errors() {
            anyhow::bail!(
                "querying {collection} {doc_id} composite evidence failed: {:?}",
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

    async fn verify_inference_call_provenance(
        &self,
        rendered: &RenderedCompletionRequest,
    ) -> Result<()> {
        if rendered.inference_call_doc_id.is_empty() {
            return Ok(());
        }
        let rows = self
            .composite_evidence("InferenceCall", &rendered.inference_call_doc_id)
            .await?;
        let current =
            sole_current_composite_head(&rows, "InferenceCall", &rendered.inference_call_doc_id)?;
        if current.cid != rendered.inference_call_composite_commit_cid {
            anyhow::bail!(
                "InferenceCall {} current composite head {} does not match captured running head {}; refusing a stale or later-head send",
                rendered.inference_call_doc_id,
                current.cid,
                rendered.inference_call_composite_commit_cid
            );
        }
        let signer_did = self
            .node
            .verified_block_signer_did(&current.cid)
            .await
            .with_context(|| {
                format!(
                    "cryptographically verifying InferenceCall {} current head {}",
                    rendered.inference_call_doc_id, current.cid
                )
            })?;
        if signer_did.trim().is_empty()
            || signer_did != rendered.inference_call_signer_did
            || signer_did != rendered.agent_did
        {
            anyhow::bail!(
                "InferenceCall {} verified signer {:?} does not match captured signer {:?} and capture agent {:?}",
                rendered.inference_call_doc_id,
                signer_did,
                rendered.inference_call_signer_did,
                rendered.agent_did
            );
        }

        let escaped_cid = escape_graphql_string(&current.cid);
        let collection = gents_protocol::schemas::INFERENCE_CALL_NAME;
        let response = self
            .execute(
                &format!(
                    r#"query {{
                        {collection}(cid: ["{escaped_cid}"]) {{
                            _docID request_id behavior_id agent_did call_state
                        }}
                    }}"#
                ),
                "rendered_request::inference_call_snapshot",
                true,
            )
            .await;
        if response.has_errors() {
            anyhow::bail!(
                "loading exact InferenceCall snapshot {} failed: {:?}",
                current.cid,
                response.errors
            );
        }
        let snapshots: Vec<InferenceCallSnapshot> = response
            .data
            .as_ref()
            .and_then(|data| data.get(collection))
            .cloned()
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or_default();
        let snapshot = match snapshots.as_slice() {
            [snapshot] => snapshot,
            snapshots => anyhow::bail!(
                "InferenceCall CID {} reconstructed {} documents, expected one",
                current.cid,
                snapshots.len()
            ),
        };
        if snapshot.doc_id != rendered.inference_call_doc_id
            || snapshot.request_id != rendered.request_id
            || snapshot.behavior_id != rendered.behavior_id
            || snapshot.agent_did != rendered.agent_did
            || snapshot.call_state != "running"
        {
            anyhow::bail!(
                "exact running InferenceCall snapshot does not match rendered-request identity"
            );
        }
        Ok(())
    }

    async fn ensure_call_has_single_render(
        &self,
        rendered: &RenderedCompletionRequest,
        require_bound: bool,
    ) -> Result<()> {
        if rendered.inference_call_doc_id.is_empty() {
            return Ok(());
        }
        let query = format!(
            r#"query {{
                {collection}(filter: {{
                    inference_call_doc_id: {{ _eq: "{doc_id}" }},
                    inference_call_composite_commit_cid: {{ _eq: "{cid}" }},
                    inference_call_signer_did: {{ _eq: "{signer}" }}
                }}, limit: 3) {{ capture_key }}
            }}"#,
            collection = RENDERED_REQUEST_COLLECTION,
            doc_id = escape_graphql_string(&rendered.inference_call_doc_id),
            cid = escape_graphql_string(&rendered.inference_call_composite_commit_cid),
            signer = escape_graphql_string(&rendered.inference_call_signer_did),
        );
        let response = self
            .execute(&query, "rendered_request::call_binding", true)
            .await;
        if response.has_errors() {
            anyhow::bail!(
                "checking RenderedRequest call binding failed: {:?}",
                response.errors
            );
        }
        let rows = response
            .data
            .as_ref()
            .and_then(|data| data.get(RENDERED_REQUEST_COLLECTION))
            .and_then(Value::as_array)
            .ok_or_else(|| {
                anyhow!("checking RenderedRequest call binding returned an unexpected shape")
            })?;
        match rows.as_slice() {
            [] if !require_bound => Ok(()),
            [row]
                if row.get("capture_key").and_then(Value::as_str)
                    == Some(rendered.capture_key.as_str()) =>
            {
                Ok(())
            }
            [] => anyhow::bail!(
                "running InferenceCall {} has no durable RenderedRequest after capture",
                rendered.inference_call_doc_id
            ),
            rows => anyhow::bail!(
                "running InferenceCall {} version {} resolves to {} RenderedRequest facts; refusing ambiguous render binding",
                rendered.inference_call_doc_id,
                rendered.inference_call_composite_commit_cid,
                rows.len()
            ),
        }
    }

    /// The immutable capture fact already stored under `capture_key`, if any.
    ///
    /// A GraphQL error is an error, never "no rows": treating a failed read as
    /// an unbound key would turn a transient DB fault into a duplicate-key
    /// create and, worse, into a silent rebinding attempt.
    async fn stored_fact(&self, capture_key: &str) -> Result<Option<Value>> {
        let query = format!(
            r#"query {{
                {collection}(filter: {{ capture_key: {{ _eq: "{capture_key}" }} }}, limit: 2) {{
                    _docID
                    request_doc_id
                    request_source_commit_cid
                    request_source_signer_did
                    request_claim_commit_cid
                    request_claim_signer_did
                    inference_call_doc_id
                    inference_call_composite_commit_cid
                    inference_call_signer_did
                    request_id
                    session_id
                    agent_did
                    requester_did
                    behavior_id
                    capture_scope
                    turn_index
                    attempt
                    capture_version
                    model_name
                    source
                    request_json
                    prompt_hash
                    tools_hash
                    provenance_json
                }}
            }}"#,
            collection = RENDERED_REQUEST_COLLECTION,
            capture_key = escape_graphql_string(capture_key),
        );
        let response = self.execute(&query, "rendered_request::lookup", true).await;
        if response.has_errors() {
            return Err(anyhow!(
                "reading RenderedRequest by capture key failed: {:?}",
                response.errors
            ));
        }
        let data = response
            .data
            .ok_or_else(|| anyhow!("reading RenderedRequest by capture key returned no data"))?;
        let rows = data
            .get(RENDERED_REQUEST_COLLECTION)
            .and_then(Value::as_array)
            .ok_or_else(|| {
                anyhow!("reading RenderedRequest by capture key returned an unexpected shape")
            })?;
        match rows.len() {
            0 => Ok(None),
            1 => Ok(Some(rows[0].clone())),
            // The unique index makes this unreachable; if it ever happens the
            // fact record is already ambiguous and must not be extended.
            count => Err(anyhow!(
                "capture key {capture_key} matched {count} RenderedRequest rows; the unique index is not enforcing"
            )),
        }
    }

    async fn verified_rendered_request_version(
        &self,
        rendered: &RenderedCompletionRequest,
    ) -> Result<crate::SignedDocumentVersionRef> {
        let stored = self
            .stored_fact(&rendered.capture_key)
            .await?
            .ok_or_else(|| {
                anyhow!("durable RenderedRequest disappeared before exact verification")
            })?;
        self.reconcile_existing(rendered, stored.clone(), "exact_verification")?;
        let doc_id = stored
            .get("_docID")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow!("stored RenderedRequest has no _docID"))?;
        let rows = self
            .composite_evidence(RENDERED_REQUEST_COLLECTION, doc_id)
            .await?;
        let current = sole_current_composite_head(&rows, RENDERED_REQUEST_COLLECTION, doc_id)?;
        let signer_did = self
            .node
            .verified_block_signer_did(&current.cid)
            .await
            .with_context(|| {
                format!(
                    "cryptographically verifying RenderedRequest {doc_id} current head {}",
                    current.cid
                )
            })?;
        if signer_did.trim().is_empty() {
            anyhow::bail!(
                "RenderedRequest {doc_id} current head {} has an empty verified signer DID",
                current.cid
            );
        }
        if signer_did != rendered.agent_did {
            anyhow::bail!(
                "RenderedRequest {doc_id} verified signer {signer_did} does not match capture agent {}",
                rendered.agent_did
            );
        }

        // Reload by the exact CID after selecting the current head. This closes
        // the read/head race: returning the head CID while comparing a prior
        // logical-key snapshot would otherwise attest to bytes we never read.
        let escaped_cid = escape_graphql_string(&current.cid);
        let query = format!(
            r#"query {{
                {collection}(cid: ["{escaped_cid}"]) {{
                    _docID
                    request_doc_id
                    request_source_commit_cid
                    request_source_signer_did
                    request_claim_commit_cid
                    request_claim_signer_did
                    inference_call_doc_id
                    inference_call_composite_commit_cid
                    inference_call_signer_did
                    request_id
                    session_id
                    agent_did
                    requester_did
                    behavior_id
                    capture_scope
                    turn_index
                    attempt
                    capture_version
                    model_name
                    source
                    request_json
                    prompt_hash
                    tools_hash
                    provenance_json
                }}
            }}"#,
            collection = RENDERED_REQUEST_COLLECTION,
        );
        let response = self
            .execute(&query, "rendered_request::exact_snapshot", true)
            .await;
        if response.has_errors() {
            anyhow::bail!(
                "loading RenderedRequest {doc_id} exact snapshot {} failed: {:?}",
                current.cid,
                response.errors
            );
        }
        let exact_rows = response
            .data
            .as_ref()
            .and_then(|data| data.get(RENDERED_REQUEST_COLLECTION))
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("loading exact RenderedRequest returned an unexpected shape"))?;
        let exact = match exact_rows.as_slice() {
            [exact] if exact.get("_docID").and_then(Value::as_str) == Some(doc_id) => exact.clone(),
            [exact] => anyhow::bail!(
                "RenderedRequest CID {} reconstructed _docID={:?}, expected {doc_id}",
                current.cid,
                exact.get("_docID")
            ),
            exact_rows => anyhow::bail!(
                "RenderedRequest CID {} reconstructed {} documents, expected one",
                current.cid,
                exact_rows.len()
            ),
        };
        self.reconcile_existing(rendered, exact, "exact_cid")?;
        Ok(crate::SignedDocumentVersionRef::new(
            crate::DocumentVersionRef::new(doc_id, &current.cid),
            signer_did,
        ))
    }

    async fn create(
        &self,
        rendered: &RenderedCompletionRequest,
        request_json: &str,
        provenance_json: &str,
    ) -> Result<()> {
        let source = serde_json::to_value(rendered.source)
            .ok()
            .and_then(|value| value.as_str().map(ToOwned::to_owned))
            .unwrap_or_else(|| "unknown".to_string());
        let mutation = format!(
            r#"mutation {{
                create_{collection}(input: {{
                    capture_key: "{capture_key}",
                    request_doc_id: "{request_doc_id}",
                    request_source_commit_cid: "{request_source_commit_cid}",
                    request_source_signer_did: "{request_source_signer_did}",
                    request_claim_commit_cid: "{request_claim_commit_cid}",
                    request_claim_signer_did: "{request_claim_signer_did}",
                    inference_call_doc_id: "{inference_call_doc_id}",
                    inference_call_composite_commit_cid: "{inference_call_composite_commit_cid}",
                    inference_call_signer_did: "{inference_call_signer_did}",
                    request_id: "{request_id}",
                    session_id: "{session_id}",
                    agent_did: "{agent_did}",
                    requester_did: "{requester_did}",
                    behavior_id: "{behavior_id}",
                    capture_scope: "{capture_scope}",
                    turn_index: {turn_index},
                    attempt: {attempt},
                    capture_version: {capture_version},
                    model_name: "{model_name}",
                    source: "{source}",
                    request_json: "{request_json}",
                    prompt_hash: "{prompt_hash}",
                    tools_hash: "{tools_hash}",
                    provenance_json: "{provenance_json}",
                    created_at: "{created_at}"
                }}) {{ _docID }}
            }}"#,
            collection = RENDERED_REQUEST_COLLECTION,
            capture_key = escape_graphql_string(&rendered.capture_key),
            request_doc_id = escape_graphql_string(&rendered.request_doc_id),
            request_source_commit_cid = escape_graphql_string(&rendered.request_source_commit_cid),
            request_source_signer_did = escape_graphql_string(&rendered.request_source_signer_did),
            request_claim_commit_cid = escape_graphql_string(&rendered.request_claim_commit_cid),
            request_claim_signer_did = escape_graphql_string(&rendered.request_claim_signer_did),
            inference_call_doc_id = escape_graphql_string(&rendered.inference_call_doc_id),
            inference_call_composite_commit_cid =
                escape_graphql_string(&rendered.inference_call_composite_commit_cid,),
            inference_call_signer_did = escape_graphql_string(&rendered.inference_call_signer_did),
            request_id = escape_graphql_string(&rendered.request_id),
            session_id = escape_graphql_string(&rendered.session_id),
            agent_did = escape_graphql_string(&rendered.agent_did),
            requester_did = escape_graphql_string(&rendered.requester_did),
            behavior_id = escape_graphql_string(&rendered.behavior_id),
            capture_scope = escape_graphql_string(&rendered.capture_scope),
            turn_index = rendered.turn_index,
            attempt = rendered.attempt,
            capture_version = rendered.capture_version,
            model_name = escape_graphql_string(&rendered.model_name),
            source = escape_graphql_string(&source),
            request_json = escape_graphql_string(request_json),
            prompt_hash = escape_graphql_string(&rendered.prompt_hash),
            tools_hash = escape_graphql_string(&rendered.tools_hash),
            provenance_json = escape_graphql_string(provenance_json),
            created_at = escape_graphql_string(&chrono::Utc::now().to_rfc3339()),
        );

        // A duplicate-key error is an expected input to reconciliation, so the
        // create itself does not warn. A genuine failure is logged by the
        // transport after the re-read below cannot establish idempotency.
        let response = self
            .execute(&mutation, "rendered_request::create", false)
            .await;
        if response.has_errors() {
            return Err(anyhow!(
                "creating RenderedRequest failed: {:?}",
                response.errors
            ));
        }
        // A mutation that returns no document wrote nothing, and "no errors" is
        // not the same as "durable". The field lookup is explicit rather than
        // handing the whole `data` object to `response_has_documents`, which
        // would answer for the envelope instead of for the mutation's result.
        // That result field is taken as the envelope's single entry rather than
        // by name: DefraDB answers a `create_RenderedRequest` mutation under the
        // key `add_RenderedRequest`, and hard-coding either spelling would turn
        // a rename into a silently unverified write.
        if !response
            .data
            .as_ref()
            .and_then(single_mutation_result)
            .is_some_and(crate::graphql::response_has_documents)
        {
            return Err(anyhow!(
                "creating RenderedRequest returned no document; the capture is not durable"
            ));
        }
        Ok(())
    }

    /// Persist one capture. See the outcome table at the top of this module.
    pub async fn capture(
        &self,
        rendered: RenderedCompletionRequest,
    ) -> Result<crate::SignedDocumentVersionRef> {
        rendered.validate_new_capture()?;
        if !rendered.request_doc_id.is_empty() {
            let provenance = crate::RequestExecutionProvenance::new(
                crate::SignedDocumentVersionRef::new(
                    crate::DocumentVersionRef::new(
                        &rendered.request_doc_id,
                        &rendered.request_source_commit_cid,
                    ),
                    &rendered.request_source_signer_did,
                ),
                crate::SignedDocumentVersionRef::new(
                    crate::DocumentVersionRef::new(
                        &rendered.request_doc_id,
                        &rendered.request_claim_commit_cid,
                    ),
                    &rendered.request_claim_signer_did,
                ),
            );
            let admitted_request = crate::lifecycle::verify_persisted_execution_provenance(
                &self.node,
                &provenance,
                &rendered.request_doc_id,
                &rendered.agent_did,
            )
            .await
            .context("re-verifying rendered-request execution provenance")?;
            let admitted_requester_did = admitted_request.requester_did.as_deref().unwrap_or("");
            let admitted_behavior_id = admitted_request.behavior_id.as_deref().unwrap_or("");
            if rendered.request_id != admitted_request.request_id
                || rendered.session_id != admitted_request.session_id
                || rendered.agent_did != admitted_request.agent_did
                || rendered.requester_did != admitted_requester_did
                || rendered.behavior_id != admitted_behavior_id
            {
                anyhow::bail!(
                    "rendered-request capture identity fields disagree with the exact admitted claim snapshot"
                );
            }
        }
        self.verify_inference_call_provenance(&rendered)
            .await
            .context("re-verifying rendered-request InferenceCall provenance")?;
        self.ensure_call_has_single_render(&rendered, false).await?;

        // Canonicalize once. The stored bytes and the complete-fact comparison
        // have to use the same representation or "identical" means nothing.
        let request_json = canonical_json_string(&rendered.request_json)
            .context("encoding rendered-request request_json")?;
        let provenance_json = canonical_json_string(&rendered.provenance_json)
            .context("encoding rendered-request provenance_json")?;

        // Create first. Fresh captures are overwhelmingly the common path, and
        // now cost one durable statement rather than a lookup plus a mutation.
        // Only re-delivery and races pay for the conflict read.
        match self
            .create(&rendered, &request_json, &provenance_json)
            .await
        {
            Ok(()) => {
                tracing::debug!(
                    capture_key = %rendered.capture_key,
                    request_id = %rendered.request_id,
                    capture_scope = %rendered.capture_scope,
                    turn_index = rendered.turn_index,
                    attempt = rendered.attempt,
                    outcome = "fresh",
                    "persisted rendered provider request"
                );
                self.ensure_call_has_single_render(&rendered, true).await?;
                self.verified_rendered_request_version(&rendered).await
            }
            Err(create_error) => {
                // A concurrent writer may have won the unique index between the
                // lookup and the create. Re-read: an identical value is still
                // an idempotent success, a different one is still an integrity
                // violation, and anything else keeps the original error.
                match self.stored_fact(&rendered.capture_key).await {
                    Ok(Some(stored)) => {
                        self.reconcile_existing(&rendered, stored, "create_conflict")?;
                        self.ensure_call_has_single_render(&rendered, true).await?;
                        self.verified_rendered_request_version(&rendered).await
                    }
                    _ => Err(create_error),
                }
            }
        }
    }

    fn reconcile_existing(
        &self,
        rendered: &RenderedCompletionRequest,
        stored: Value,
        via: &str,
    ) -> Result<()> {
        let incoming = canonical_capture_fact(rendered)?;
        let stored = canonical_stored_fact(stored)?;
        if stored == incoming {
            tracing::debug!(
                capture_key = %rendered.capture_key,
                request_id = %rendered.request_id,
                capture_scope = %rendered.capture_scope,
                turn_index = rendered.turn_index,
                attempt = rendered.attempt,
                outcome = "idempotent",
                via,
                "rendered provider request was already durable"
            );
            return Ok(());
        }

        tracing::error!(
            capture_key = %rendered.capture_key,
            request_id = %rendered.request_id,
            session_id = %rendered.session_id,
            capture_scope = %rendered.capture_scope,
            turn_index = rendered.turn_index,
            attempt = rendered.attempt,
            outcome = "rejected",
            via,
            stored_bytes = canonical_json_string(&stored).map(|value| value.len()).unwrap_or_default(),
            incoming_bytes = canonical_json_string(&incoming).map(|value| value.len()).unwrap_or_default(),
            "rendered-request capture key already names a different immutable fact"
        );
        Err(anyhow!(
            "rendered-request integrity violation: capture key {} already names a different \
             canonical capture fact; a capture key is never rebound",
            rendered.capture_key,
        ))
    }
}

/// Canonical equality surface for idempotency. `created_at` is intentionally
/// excluded: it records when the winning writer created the row, while every
/// other immutable column is part of the fact a later projection will trust.
fn canonical_capture_fact(rendered: &RenderedCompletionRequest) -> Result<Value> {
    let source =
        serde_json::to_value(rendered.source).context("encoding rendered-request source")?;
    Ok(canonical_json(&serde_json::json!({
        "request_doc_id": rendered.request_doc_id,
        "request_source_commit_cid": rendered.request_source_commit_cid,
        "request_source_signer_did": rendered.request_source_signer_did,
        "request_claim_commit_cid": rendered.request_claim_commit_cid,
        "request_claim_signer_did": rendered.request_claim_signer_did,
        "inference_call_doc_id": rendered.inference_call_doc_id,
        "inference_call_composite_commit_cid": rendered.inference_call_composite_commit_cid,
        "inference_call_signer_did": rendered.inference_call_signer_did,
        "request_id": rendered.request_id,
        "session_id": rendered.session_id,
        "agent_did": rendered.agent_did,
        "requester_did": rendered.requester_did,
        "behavior_id": rendered.behavior_id,
        "capture_scope": rendered.capture_scope,
        "turn_index": rendered.turn_index,
        "attempt": rendered.attempt,
        "capture_version": rendered.capture_version,
        "model_name": rendered.model_name,
        "source": source,
        "request_json": canonical_json(&rendered.request_json),
        "prompt_hash": rendered.prompt_hash,
        "tools_hash": rendered.tools_hash,
        "provenance_json": canonical_json(&rendered.provenance_json),
    })))
}

fn canonical_stored_fact(mut stored: Value) -> Result<Value> {
    let object = stored
        .as_object_mut()
        .ok_or_else(|| anyhow!("stored RenderedRequest fact was not an object"))?;
    object.remove("_docID");
    for field in ["request_json", "provenance_json"] {
        let encoded = object
            .get(field)
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("stored RenderedRequest {field} was not a string"))?;
        let decoded: Value = serde_json::from_str(encoded)
            .with_context(|| format!("decoding stored RenderedRequest {field}"))?;
        object.insert(field.to_string(), canonical_json(&decoded));
    }
    Ok(canonical_json(&stored))
}

const RENDERED_REQUEST_COLLECTION: &str = gents_protocol::schemas::RENDERED_REQUEST_NAME;

/// The single result field of a single-operation mutation envelope.
///
/// `None` when the envelope is not a one-entry object, which is the honest
/// answer for a response this sink does not recognise — treating it as "wrote
/// something" is the failure mode the caller is checking for.
fn single_mutation_result(data: &Value) -> Option<&Value> {
    let object = data.as_object()?;
    let mut entries = object.values();
    let first = entries.next()?;
    entries.next().is_none().then_some(first)
}

/// The production capture factory: one sink per request context, all writing
/// through the same node under the requesting agent's DID.
pub(crate) fn defra_rendered_request_capture_factory(
    node: Arc<EmbeddedNode>,
) -> RenderedRequestCaptureFactory {
    Arc::new(move |context: RenderedRequestContext| {
        match DefraRenderedRequestSink::new(Arc::clone(&node), &context.agent_did) {
            Ok(sink) => {
                let capture: RenderedRequestCaptureSink = Arc::new(move |rendered| {
                    let sink = sink.clone();
                    Box::pin(async move { sink.capture(rendered).await })
                });
                capture
            }
            Err(error) => {
                let message = format!("initializing RenderedRequest capture sink: {error:#}");
                let capture: RenderedRequestCaptureSink = Arc::new(move |_| {
                    let message = message.clone();
                    Box::pin(async move { anyhow::bail!(message) })
                });
                capture
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The collection name is interpolated as a bare GraphQL identifier, where
    /// escaping cannot defend. It is a compile-time constant from the protocol
    /// catalog, and this is the fence that keeps it a valid identifier if that
    /// catalog ever changes.
    #[test]
    fn the_collection_name_is_a_valid_graphql_identifier() {
        crate::graphql::validate_collection_identifier(RENDERED_REQUEST_COLLECTION)
            .expect("RenderedRequest must be a valid GraphQL identifier");
        assert_eq!(RENDERED_REQUEST_COLLECTION, "RenderedRequest");
    }

    /// The shape DefraDB actually answers a `create_RenderedRequest` mutation
    /// with — note the `add_` key. A create whose result cannot be found reads
    /// as "wrote nothing", so this is the difference between verifying the
    /// write and assuming it.
    #[test]
    fn a_create_envelope_yields_its_single_result_field() {
        use crate::graphql::response_has_documents;
        use serde_json::json;

        let created = json!({ "add_RenderedRequest": [{ "_docID": "bae-1" }] });
        assert!(response_has_documents(
            single_mutation_result(&created).expect("one result field")
        ));

        let wrote_nothing = json!({ "add_RenderedRequest": [] });
        assert!(!response_has_documents(
            single_mutation_result(&wrote_nothing).expect("one result field")
        ));

        // An envelope this sink does not recognise must not read as a write.
        assert!(single_mutation_result(&json!({ "a": [], "b": [] })).is_none());
        assert!(single_mutation_result(&json!([])).is_none());
        assert!(single_mutation_result(&json!({})).is_none());
    }
}
