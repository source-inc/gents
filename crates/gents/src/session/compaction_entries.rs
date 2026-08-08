use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use defra_node::{EmbeddedNode, ExecuteRetryPolicy, QueryRequest, QueryResponse};
use identity::Did;
use serde_json::Value;

use super::rows::{dedupe_paths, CompactionEntryRow};
use super::*;

const COMPACTION_FACT_ATTEMPTS: usize = 3;

impl CompactionSourceManifest {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        session_id: impl Into<String>,
        behavior_id: impl Into<String>,
        transcript_snapshot: Vec<MessageFactRef>,
        config_provenance: crate::ResolvedBehaviorConfigProvenance,
        prior_compactions: Vec<CompactionFactRef>,
        provider_view_message_count: usize,
        prior_compacted_message_count: usize,
        compactor_input_message_count: usize,
    ) -> Self {
        Self {
            manifest_version: COMPACTION_SOURCE_MANIFEST_VERSION,
            session_id: session_id.into(),
            behavior_id: behavior_id.into(),
            transcript_snapshot,
            config_provenance,
            prior_compactions,
            provider_view_message_count,
            prior_compacted_message_count,
            compactor_input_message_count,
        }
    }

    pub(crate) fn validate(
        &self,
        expected_session_id: &str,
        expected_agent_did: &str,
    ) -> Result<()> {
        if self.manifest_version != COMPACTION_SOURCE_MANIFEST_VERSION {
            anyhow::bail!(
                "unsupported CompactionEntry source manifest version {}",
                self.manifest_version
            );
        }
        if self.session_id.trim().is_empty() || self.session_id != expected_session_id {
            anyhow::bail!(
                "CompactionEntry source manifest session {:?} does not match {expected_session_id:?}",
                self.session_id
            );
        }
        self.config_provenance
            .validate_for_behavior(&self.behavior_id, expected_agent_did)
            .context("invalid CompactionEntry resolved config provenance")?;

        let mut previous_sequence = None;
        let mut doc_ids = BTreeSet::new();
        let mut cids = BTreeSet::new();
        for fact in &self.transcript_snapshot {
            require_complete_ref(
                &fact.doc_id,
                &fact.composite_commit_cid,
                &fact.signer_did,
                "AgentMessage",
            )?;
            if previous_sequence.is_some_and(|previous| fact.sequence <= previous) {
                anyhow::bail!(
                    "CompactionEntry transcript inputs are not in canonical sequence order"
                );
            }
            if !doc_ids.insert(fact.doc_id.as_str())
                || !cids.insert(fact.composite_commit_cid.as_str())
            {
                anyhow::bail!("CompactionEntry transcript inputs repeat an exact document version");
            }
            previous_sequence = Some(fact.sequence);
        }
        if self.transcript_snapshot.is_empty() {
            anyhow::bail!("CompactionEntry source manifest requires a non-empty transcript");
        }

        previous_sequence = None;
        doc_ids.clear();
        cids.clear();
        for fact in &self.prior_compactions {
            require_complete_ref(
                &fact.source.version.doc_id,
                &fact.source.version.composite_commit_cid,
                &fact.source.signer_did,
                "CompactionEntry",
            )?;
            if previous_sequence.is_some_and(|previous| fact.sequence <= previous) {
                anyhow::bail!("prior CompactionEntry refs are not in canonical sequence order");
            }
            if !doc_ids.insert(fact.source.version.doc_id.as_str())
                || !cids.insert(fact.source.version.composite_commit_cid.as_str())
            {
                anyhow::bail!("prior CompactionEntry refs repeat an exact document version");
            }
            previous_sequence = Some(fact.sequence);
        }
        if self.prior_compacted_message_count > self.provider_view_message_count {
            anyhow::bail!(
                "CompactionEntry prior compacted count exceeds the exact provider-view input"
            );
        }
        let remaining = self
            .provider_view_message_count
            .saturating_sub(self.prior_compacted_message_count);
        if self.compactor_input_message_count > remaining {
            anyhow::bail!(
                "CompactionEntry compactor input count exceeds the exact post-prefix provider view"
            );
        }
        Ok(())
    }
}

fn require_complete_ref(doc_id: &str, cid: &str, signer_did: &str, label: &str) -> Result<()> {
    if doc_id.trim().is_empty() || cid.trim().is_empty() || signer_did.trim().is_empty() {
        anyhow::bail!("{label} exact source reference is incomplete");
    }
    Ok(())
}

fn compaction_identity(node: &EmbeddedNode, agent_did: &str) -> Result<Did> {
    let node_did = node.node_identity_did().ok_or_else(|| {
        anyhow::anyhow!("CompactionEntry persistence requires a DefraDB node signing identity")
    })?;
    if node_did != agent_did {
        anyhow::bail!(
            "CompactionEntry agent DID {agent_did} does not match node signing identity {node_did}"
        );
    }
    Did::new(agent_did).context("parsing CompactionEntry agent DID")
}

async fn execute(node: &EmbeddedNode, query: String, identity: Option<Did>) -> QueryResponse {
    node.execute_request_with_retry(
        QueryRequest::new(query).with_identity(identity),
        ExecuteRetryPolicy::default(),
    )
    .await
}

fn row_fields() -> &'static str {
    r#"_docID compaction_key session_id agent_did requester_did sequence summary
       files_read files_modified messages_compacted original_tokens compacted_tokens
       source_manifest_version source_manifest_json created_at fork_source_doc_id
       fork_source_composite_commit_cid fork_source_signer_did"#
}

fn fork_source_ref(row: &CompactionEntryRow) -> Result<Option<crate::SignedDocumentVersionRef>> {
    match (
        row.fork_source_doc_id.as_deref(),
        row.fork_source_composite_commit_cid.as_deref(),
        row.fork_source_signer_did.as_deref(),
    ) {
        (None, None, None) => Ok(None),
        (Some(doc_id), Some(cid), Some(signer_did))
            if !doc_id.trim().is_empty()
                && !cid.trim().is_empty()
                && !signer_did.trim().is_empty() =>
        {
            Ok(Some(crate::SignedDocumentVersionRef::new(
                crate::DocumentVersionRef::new(doc_id, cid),
                signer_did,
            )))
        }
        _ => anyhow::bail!(
            "CompactionEntry {} has a partial or empty fork source reference",
            row.doc_id
        ),
    }
}

async fn load_rows(
    node: &EmbeddedNode,
    session_id: &str,
    identity: Option<Did>,
) -> Result<Vec<CompactionEntryRow>> {
    let escaped_session_id = escape_graphql_string(session_id);
    let query = format!(
        r#"{{
            CompactionEntry(
                filter: {{ session_id: {{ _eq: "{escaped_session_id}" }} }},
                order: {{ sequence: ASC }}
            ) {{ {} }}
        }}"#,
        row_fields()
    );
    let response = execute(node, query, identity).await;
    if response.has_errors() {
        anyhow::bail!(
            "loading CompactionEntry candidates for session_id={session_id}: {:?}",
            response.errors
        );
    }
    response
        .data
        .as_ref()
        .and_then(|data| data.get("CompactionEntry"))
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map(|rows| rows.unwrap_or_default())
        .map_err(Into::into)
}

fn reject_logical_twins(rows: &[CompactionEntryRow], session_id: &str) -> Result<()> {
    let mut keys = BTreeMap::<&str, Vec<&str>>::new();
    let mut sequences = BTreeMap::<u32, Vec<&str>>::new();
    for row in rows {
        keys.entry(&row.compaction_key)
            .or_default()
            .push(&row.doc_id);
        sequences.entry(row.sequence).or_default().push(&row.doc_id);
    }
    let key_conflicts = keys
        .into_iter()
        .filter(|(_, docs)| docs.len() > 1)
        .collect::<Vec<_>>();
    let sequence_conflicts = sequences
        .into_iter()
        .filter(|(_, docs)| docs.len() > 1)
        .collect::<Vec<_>>();
    if !key_conflicts.is_empty() || !sequence_conflicts.is_empty() {
        anyhow::bail!(
            "CompactionEntry logical fact conflict for session_id={session_id}: keys={key_conflicts:?} sequences={sequence_conflicts:?}"
        );
    }
    Ok(())
}

fn config_sources(
    provenance: &crate::ResolvedBehaviorConfigProvenance,
) -> Vec<&crate::ConfigFactRef> {
    let mut sources = vec![
        &provenance.principal,
        &provenance.behavior,
        &provenance.inference_backend,
        &provenance.inference_profile,
    ];
    if let Some(tool_selection) = provenance.tool_selection.as_ref() {
        sources.push(tool_selection);
    }
    sources.extend(provenance.skills.iter());
    sources
}

async fn verify_exact_ref(
    node: &EmbeddedNode,
    collection: &str,
    logical_field: Option<(&str, Value)>,
    source: &crate::SignedDocumentVersionRef,
    identity: Option<Did>,
    require_current: bool,
) -> Result<()> {
    require_complete_ref(
        &source.version.doc_id,
        &source.version.composite_commit_cid,
        &source.signer_did,
        collection,
    )?;
    let signer = node
        .verified_block_signer_did(&source.version.composite_commit_cid)
        .await
        .with_context(|| {
            format!(
                "cryptographically verifying {collection} {} exact source {}",
                source.version.doc_id, source.version.composite_commit_cid
            )
        })?;
    if signer != source.signer_did {
        anyhow::bail!(
            "{collection} {} exact source signer {signer} disagrees with pinned signer {}",
            source.version.doc_id,
            source.signer_did
        );
    }
    if require_current {
        let current =
            crate::document_version::verified_current_signed_document_version_with_identity(
                node,
                collection,
                &source.version.doc_id,
                identity.clone(),
            )
            .await?;
        if &current != source {
            anyhow::bail!(
                "{collection} {} changed after the compaction input snapshot was loaded",
                source.version.doc_id
            );
        }
    }

    let escaped_cid = escape_graphql_string(&source.version.composite_commit_cid);
    let logical_selection = logical_field
        .as_ref()
        .map(|(field, _)| format!(" {field}"))
        .unwrap_or_default();
    let response = execute(
        node,
        format!(r#"{{ {collection}(cid: ["{escaped_cid}"]) {{ _docID{logical_selection} }} }}"#),
        identity,
    )
    .await;
    if response.has_errors() {
        anyhow::bail!(
            "loading {collection} exact source {}: {:?}",
            source.version.composite_commit_cid,
            response.errors
        );
    }
    let rows = response
        .data
        .as_ref()
        .and_then(|data| data.get(collection))
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("loading {collection} exact source returned no rows"))?;
    let row = match rows.as_slice() {
        [row]
            if row.get("_docID").and_then(Value::as_str)
                == Some(source.version.doc_id.as_str()) =>
        {
            row
        }
        rows => anyhow::bail!(
            "{collection} exact source {} reconstructed {} documents or a different _docID",
            source.version.composite_commit_cid,
            rows.len()
        ),
    };
    if let Some((field, expected)) = logical_field {
        if row.get(field) != Some(&expected) {
            anyhow::bail!(
                "{collection} exact source {} does not bind logical field {field} to {expected}",
                source.version.composite_commit_cid
            );
        }
    }
    Ok(())
}

fn config_logical_field(collection: &str) -> Result<&'static str> {
    match collection {
        "AgentPrincipal" => Ok("agent_did"),
        "AgentBehavior" => Ok("behavior_id"),
        "InferenceBackend" => Ok("backend_id"),
        "InferenceProfile" => Ok("profile_id"),
        "ToolSelection" => Ok("selection_id"),
        "Skill" => Ok("skill_id"),
        _ => anyhow::bail!("unsupported CompactionEntry config collection {collection}"),
    }
}

async fn verify_sole_config_candidate(
    node: &EmbeddedNode,
    fact: &crate::ConfigFactRef,
    identity: Option<Did>,
) -> Result<()> {
    let field = config_logical_field(&fact.collection)?;
    let escaped_logical_id = escape_graphql_string(&fact.logical_id);
    let response = execute(
        node,
        format!(
            r#"{{ {}(filter: {{ {field}: {{ _eq: "{escaped_logical_id}" }} }}) {{ _docID }} }}"#,
            fact.collection
        ),
        identity,
    )
    .await;
    if response.has_errors() {
        anyhow::bail!(
            "enumerating {} {} logical candidates: {:?}",
            fact.collection,
            fact.logical_id,
            response.errors
        );
    }
    let rows = response
        .data
        .as_ref()
        .and_then(|data| data.get(&fact.collection))
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("config logical candidate query returned no rows"))?;
    match rows.as_slice() {
        [row]
            if row.get("_docID").and_then(Value::as_str)
                == Some(fact.source.version.doc_id.as_str()) =>
        {
            Ok(())
        }
        rows => anyhow::bail!(
            "{} {} has {} visible logical candidates or resolves to a different _docID",
            fact.collection,
            fact.logical_id,
            rows.len()
        ),
    }
}

async fn load_current_transcript_snapshot(
    node: &EmbeddedNode,
    session_id: &str,
    identity: Option<Did>,
) -> Result<Vec<MessageFactRef>> {
    let escaped_session_id = escape_graphql_string(session_id);
    let response = execute(
        node,
        format!(
            r#"{{ AgentMessage(
                filter: {{ session_id: {{ _eq: "{escaped_session_id}" }} }},
                order: {{ sequence: ASC }}
            ) {{ _docID message_key agent_did sequence }} }}"#
        ),
        identity.clone(),
    )
    .await;
    if response.has_errors() {
        anyhow::bail!(
            "reloading exact compaction transcript candidates: {:?}",
            response.errors
        );
    }
    let rows = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentMessage"))
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("exact compaction transcript query returned no rows"))?;
    let mut message_keys = BTreeSet::new();
    let mut sequences = BTreeSet::new();
    let mut snapshot = Vec::with_capacity(rows.len());
    for row in rows {
        let doc_id = row
            .get("_docID")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("AgentMessage candidate has no _docID"))?;
        let message_key = row
            .get("message_key")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("AgentMessage candidate has no message_key"))?;
        let agent_did = row
            .get("agent_did")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("AgentMessage candidate has no agent_did"))?;
        let sequence = row
            .get("sequence")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| anyhow::anyhow!("AgentMessage candidate has invalid sequence"))?;
        if !message_keys.insert(message_key) || !sequences.insert(sequence) {
            anyhow::bail!(
                "compaction transcript became ambiguous before finalization: message_key={message_key} sequence={sequence}"
            );
        }
        let source =
            crate::document_version::verified_current_signed_document_version_with_identity(
                node,
                "AgentMessage",
                doc_id,
                identity.clone(),
            )
            .await?;
        if source.signer_did != agent_did {
            anyhow::bail!(
                "AgentMessage {doc_id} signer {} does not match row agent {agent_did}",
                source.signer_did
            );
        }
        snapshot.push(MessageFactRef {
            sequence,
            doc_id: doc_id.to_owned(),
            composite_commit_cid: source.version.composite_commit_cid,
            signer_did: source.signer_did,
        });
    }
    Ok(snapshot)
}

async fn verify_manifest_sources(
    node: &EmbeddedNode,
    manifest: &CompactionSourceManifest,
    identity: Option<Did>,
    require_current: bool,
) -> Result<()> {
    for fact in &manifest.transcript_snapshot {
        let source = crate::SignedDocumentVersionRef {
            version: crate::DocumentVersionRef {
                doc_id: fact.doc_id.clone(),
                composite_commit_cid: fact.composite_commit_cid.clone(),
            },
            signer_did: fact.signer_did.clone(),
        };
        verify_exact_ref(
            node,
            "AgentMessage",
            Some(("sequence", Value::from(fact.sequence))),
            &source,
            identity.clone(),
            require_current,
        )
        .await?;
    }
    for fact in config_sources(&manifest.config_provenance) {
        if require_current {
            verify_sole_config_candidate(node, fact, identity.clone()).await?;
        }
        verify_exact_ref(
            node,
            &fact.collection,
            Some((
                config_logical_field(&fact.collection)?,
                Value::String(fact.logical_id.clone()),
            )),
            &fact.source,
            identity.clone(),
            require_current,
        )
        .await?;
    }
    for fact in &manifest.prior_compactions {
        verify_exact_ref(
            node,
            "CompactionEntry",
            Some(("sequence", Value::from(fact.sequence))),
            &fact.source,
            identity.clone(),
            require_current,
        )
        .await?;
    }
    Ok(())
}

async fn verify_compaction_row(
    node: &EmbeddedNode,
    row: &CompactionEntryRow,
    identity: Option<Did>,
) -> Result<CompactionFactRef> {
    let source = crate::document_version::verified_current_signed_document_version_with_identity(
        node,
        "CompactionEntry",
        &row.doc_id,
        identity.clone(),
    )
    .await?;
    match fork_source_ref(row)? {
        None if source.signer_did != row.agent_did => {
            anyhow::bail!(
                "ordinary CompactionEntry {} signer {} does not match agent {}",
                row.doc_id,
                source.signer_did,
                row.agent_did
            );
        }
        Some(fork_source) => {
            if fork_source.version.doc_id == row.doc_id {
                anyhow::bail!("CompactionEntry {} cannot derive from itself", row.doc_id);
            }
            verify_exact_ref(
                node,
                "CompactionEntry",
                Some(("sequence", Value::from(row.sequence))),
                &fork_source,
                identity.clone(),
                false,
            )
            .await
            .context("verifying exact fork source CompactionEntry")?;
        }
        None => {}
    }
    let escaped_cid = escape_graphql_string(&source.version.composite_commit_cid);
    let response = execute(
        node,
        format!(
            r#"{{ CompactionEntry(cid: ["{escaped_cid}"]) {{ {} }} }}"#,
            row_fields()
        ),
        identity.clone(),
    )
    .await;
    if response.has_errors() {
        anyhow::bail!(
            "loading CompactionEntry {} exact snapshot {}: {:?}",
            row.doc_id,
            source.version.composite_commit_cid,
            response.errors
        );
    }
    let exact: Vec<CompactionEntryRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("CompactionEntry"))
        .cloned()
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or_default();
    match exact.as_slice() {
        [exact] if exact == row => {}
        [exact] => anyhow::bail!(
            "CompactionEntry {} current signed snapshot does not match loaded facts: exact={exact:?}",
            row.doc_id
        ),
        rows => anyhow::bail!(
            "CompactionEntry CID {} reconstructed {} documents, expected one",
            source.version.composite_commit_cid,
            rows.len()
        ),
    }
    let entry = CompactionEntry::try_from(row.clone())?;
    entry
        .source_manifest
        .validate(&row.session_id, &row.agent_did)?;
    let canonical = crate::rendered_request::canonical_json_string(&serde_json::to_value(
        &entry.source_manifest,
    )?)?;
    if canonical != row.source_manifest_json {
        anyhow::bail!(
            "CompactionEntry {} source manifest is not canonical",
            row.doc_id
        );
    }
    verify_manifest_sources(node, &entry.source_manifest, identity, false).await?;
    Ok(CompactionFactRef {
        sequence: row.sequence,
        source,
    })
}

async fn load_compaction_entries_with_identity(
    node: &EmbeddedNode,
    session_id: &str,
    identity: Option<Did>,
) -> Result<LoadedCompactionEntries> {
    let rows = load_rows(node, session_id, identity.clone()).await?;
    reject_logical_twins(&rows, session_id)?;
    let mut entries = Vec::with_capacity(rows.len());
    let mut fact_refs = Vec::with_capacity(rows.len());
    let mut previous_sequence = None;
    for row in rows {
        if previous_sequence.is_some_and(|previous| row.sequence <= previous) {
            anyhow::bail!(
                "CompactionEntry rows for session_id={session_id} are not in canonical sequence order"
            );
        }
        let fact_ref = verify_compaction_row(node, &row, identity.clone()).await?;
        entries.push(CompactionEntry::try_from(row)?);
        fact_refs.push(fact_ref);
        previous_sequence = fact_refs.last().map(|fact| fact.sequence);
    }
    let compacted_message_count = entries
        .iter()
        .map(|entry| entry.messages_compacted as i64)
        .sum::<i64>();
    tracing::Span::current().record("compaction_entry_count", entries.len() as i64);
    tracing::Span::current().record("compacted_message_count", compacted_message_count);
    Ok(LoadedCompactionEntries { entries, fact_refs })
}

pub async fn load_compaction_entries(
    node: &EmbeddedNode,
    session_id: &str,
) -> Result<Vec<CompactionEntry>> {
    let node_did = node.node_identity_did().ok_or_else(|| {
        anyhow::anyhow!("loading CompactionEntry facts requires a DefraDB node identity")
    })?;
    let identity = Did::new(node_did).context("parsing CompactionEntry query identity")?;
    Ok(
        load_compaction_entries_with_identity(node, session_id, Some(identity))
            .await?
            .entries,
    )
}

pub(crate) async fn load_compaction_entries_for_agent(
    node: &EmbeddedNode,
    session_id: &str,
    agent_did: &str,
) -> Result<LoadedCompactionEntries> {
    let identity = compaction_identity(node, agent_did)?;
    load_compaction_entries_with_identity(node, session_id, Some(identity)).await
}

#[cfg(test)]
pub(crate) async fn create_test_config_provenance(
    node: &EmbeddedNode,
    agent_did: &str,
    behavior_id: &str,
) -> Result<crate::ResolvedBehaviorConfigProvenance> {
    let backend_id = format!("{behavior_id}-backend");
    let profile_id = format!("{behavior_id}-profile");
    let mutation = format!(
        r#"mutation {{
            principal: create_AgentPrincipal(input: {{
                agent_did: "{}"
                default_behavior_id: "{}"
                enabled: true
            }}) {{ _docID }}
            behavior: create_AgentBehavior(input: {{
                behavior_id: "{}"
                agent_did: "{}"
                backend_id: "{}"
                inference_profile_id: "{}"
                enabled: true
            }}) {{ _docID }}
            backend: create_InferenceBackend(input: {{
                backend_id: "{}"
                enabled: true
            }}) {{ _docID }}
            profile: create_InferenceProfile(input: {{
                profile_id: "{}"
            }}) {{ _docID }}
        }}"#,
        escape_graphql_string(agent_did),
        escape_graphql_string(behavior_id),
        escape_graphql_string(behavior_id),
        escape_graphql_string(agent_did),
        escape_graphql_string(&backend_id),
        escape_graphql_string(&profile_id),
        escape_graphql_string(&backend_id),
        escape_graphql_string(&profile_id),
    );
    let node_did = node
        .node_identity_did()
        .ok_or_else(|| anyhow::anyhow!("test config facts require a node identity"))?;
    let identity = Did::new(node_did).context("parsing test config node identity")?;
    let response = execute(node, mutation, Some(identity)).await;
    if response.has_errors() {
        anyhow::bail!("creating exact test config facts: {:?}", response.errors);
    }
    let data = response
        .data
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("test config mutation returned no data"))?;
    let doc_id = |alias: &str| -> Result<String> {
        data.get(alias)
            .and_then(Value::as_array)
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("_docID"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| anyhow::anyhow!("test config mutation returned no {alias} _docID"))
    };
    let sources = [
        ("AgentPrincipal", agent_did, doc_id("principal")?),
        ("AgentBehavior", behavior_id, doc_id("behavior")?),
        ("InferenceBackend", backend_id.as_str(), doc_id("backend")?),
        ("InferenceProfile", profile_id.as_str(), doc_id("profile")?),
    ];
    let mut facts = Vec::with_capacity(sources.len());
    for (collection, logical_id, doc_id) in sources {
        let source = crate::document_version::verified_current_signed_document_version(
            node, collection, &doc_id,
        )
        .await?;
        facts.push(crate::ConfigFactRef::new(collection, logical_id, source));
    }
    Ok(crate::ResolvedBehaviorConfigProvenance {
        principal: facts.remove(0),
        behavior: facts.remove(0),
        inference_backend: facts.remove(0),
        inference_profile: facts.remove(0),
        tool_selection: None,
        skills: Vec::new(),
        resolution_algorithm_version: 1,
    })
}

#[allow(clippy::too_many_arguments)]
pub async fn save_compaction_entry(
    node: &EmbeddedNode,
    session_id: &str,
    agent_did: &str,
    summary: &str,
    files_read: &[String],
    files_modified: &[String],
    messages_compacted: u32,
    original_tokens: usize,
    compacted_tokens: usize,
    source_manifest: CompactionSourceManifest,
) -> Result<CompactionEntry> {
    save_compaction_entry_with_requester_did(
        node,
        session_id,
        agent_did,
        None,
        summary,
        files_read,
        files_modified,
        messages_compacted,
        original_tokens,
        compacted_tokens,
        source_manifest,
    )
    .await
}

fn desired_matches(row: &CompactionEntryRow, desired: &CompactionEntryRow) -> bool {
    row.compaction_key == desired.compaction_key
        && row.session_id == desired.session_id
        && row.agent_did == desired.agent_did
        && row.requester_did == desired.requester_did
        && row.sequence == desired.sequence
        && row.summary == desired.summary
        && row.files_read == desired.files_read
        && row.files_modified == desired.files_modified
        && row.messages_compacted == desired.messages_compacted
        && row.original_tokens == desired.original_tokens
        && row.compacted_tokens == desired.compacted_tokens
        && row.source_manifest_version == desired.source_manifest_version
        && row.source_manifest_json == desired.source_manifest_json
        && super::history::rfc3339_instants_equal(&row.created_at, &desired.created_at)
}

fn candidate_for_desired(
    rows: Vec<CompactionEntryRow>,
    desired: &CompactionEntryRow,
) -> Result<Option<CompactionEntryRow>> {
    let candidates = rows
        .into_iter()
        .filter(|row| {
            row.compaction_key == desired.compaction_key || row.sequence == desired.sequence
        })
        .collect::<Vec<_>>();
    match candidates.as_slice() {
        [] => Ok(None),
        [row] if desired_matches(row, desired) => Ok(Some(row.clone())),
        [row] => anyhow::bail!(
            "CompactionEntry finalized fact conflict: _docID={} key={} sequence={}",
            row.doc_id,
            row.compaction_key,
            row.sequence
        ),
        rows => anyhow::bail!(
            "CompactionEntry logical fact conflict for key={} sequence={}: _docIDs={:?}",
            desired.compaction_key,
            desired.sequence,
            rows.iter()
                .map(|row| row.doc_id.as_str())
                .collect::<Vec<_>>()
        ),
    }
}

fn mutation_doc_ids(data: Option<&Value>) -> Vec<String> {
    data.and_then(Value::as_object)
        .into_iter()
        .flat_map(|object| object.values())
        .filter_map(Value::as_array)
        .flatten()
        .filter_map(|row| row.get("_docID").and_then(Value::as_str).map(str::to_owned))
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn save_compaction_entry_with_requester_did(
    node: &EmbeddedNode,
    session_id: &str,
    agent_did: &str,
    requester_did: Option<&str>,
    summary: &str,
    files_read: &[String],
    files_modified: &[String],
    messages_compacted: u32,
    original_tokens: usize,
    compacted_tokens: usize,
    source_manifest: CompactionSourceManifest,
) -> Result<CompactionEntry> {
    let identity = compaction_identity(node, agent_did)?;
    source_manifest.validate(session_id, agent_did)?;
    if messages_compacted as usize > source_manifest.compactor_input_message_count {
        anyhow::bail!("CompactionEntry compacted count exceeds its exact compactor input");
    }
    verify_manifest_sources(node, &source_manifest, Some(identity.clone()), true)
        .await
        .context("re-verifying exact CompactionEntry inputs before finalization")?;
    let current_transcript =
        load_current_transcript_snapshot(node, session_id, Some(identity.clone())).await?;
    if current_transcript != source_manifest.transcript_snapshot {
        anyhow::bail!(
            "CompactionEntry transcript snapshot changed or became ambiguous before finalization"
        );
    }

    let previous =
        load_compaction_entries_with_identity(node, session_id, Some(identity.clone())).await?;
    if previous.fact_refs != source_manifest.prior_compactions {
        anyhow::bail!("CompactionEntry prior exact snapshot changed before finalization");
    }
    let observed_prior_count = previous
        .entries
        .iter()
        .map(|entry| entry.messages_compacted as usize)
        .sum::<usize>();
    if observed_prior_count != source_manifest.prior_compacted_message_count {
        anyhow::bail!("CompactionEntry prior compacted count disagrees with exact prior facts");
    }

    let mut cumulative_files_read = previous
        .entries
        .last()
        .map(|entry| entry.files_read.clone())
        .unwrap_or_default();
    cumulative_files_read.extend(files_read.iter().cloned());
    dedupe_paths(&mut cumulative_files_read);
    let mut cumulative_files_modified = previous
        .entries
        .last()
        .map(|entry| entry.files_modified.clone())
        .unwrap_or_default();
    cumulative_files_modified.extend(files_modified.iter().cloned());
    dedupe_paths(&mut cumulative_files_modified);

    let sequence = previous
        .fact_refs
        .last()
        .map_or(1, |entry| entry.sequence + 1);
    let compaction_key = format!("{session_id}:{sequence}");
    let created_at = chrono::Utc::now().to_rfc3339();
    let source_manifest_json =
        crate::rendered_request::canonical_json_string(&serde_json::to_value(&source_manifest)?)?;
    let desired = CompactionEntryRow {
        doc_id: String::new(),
        compaction_key: compaction_key.clone(),
        session_id: session_id.to_string(),
        agent_did: agent_did.to_string(),
        requester_did: requester_did.map(str::to_owned),
        sequence,
        summary: summary.trim().to_string(),
        files_read: serde_json::to_string(&cumulative_files_read)?,
        files_modified: serde_json::to_string(&cumulative_files_modified)?,
        messages_compacted,
        original_tokens,
        compacted_tokens,
        source_manifest_version: COMPACTION_SOURCE_MANIFEST_VERSION,
        source_manifest_json,
        created_at,
        fork_source_doc_id: None,
        fork_source_composite_commit_cid: None,
        fork_source_signer_did: None,
    };

    for attempt in 1..=COMPACTION_FACT_ATTEMPTS {
        let rows = load_rows(node, session_id, Some(identity.clone())).await?;
        reject_logical_twins(&rows, session_id)?;
        if let Some(existing) = candidate_for_desired(rows, &desired)? {
            verify_compaction_row(node, &existing, Some(identity.clone())).await?;
            return CompactionEntry::try_from(existing);
        }

        let requester_did_field = super::requester_did_create_field(requester_did);
        let mutation = format!(
            r#"mutation {{
                create_CompactionEntry(input: {{
                    compaction_key: "{compaction_key}",
                    session_id: "{session_id}",
                    agent_did: "{agent_did}",
                    {requester_did_field}
                    sequence: {sequence},
                    summary: "{summary}",
                    files_read: "{files_read}",
                    files_modified: "{files_modified}",
                    messages_compacted: {messages_compacted},
                    original_tokens: {original_tokens},
                    compacted_tokens: {compacted_tokens},
                    source_manifest_version: {source_manifest_version},
                    source_manifest_json: "{source_manifest_json}",
                    created_at: "{created_at}"
                }}) {{ _docID }}
            }}"#,
            compaction_key = escape_graphql_string(&desired.compaction_key),
            session_id = escape_graphql_string(&desired.session_id),
            agent_did = escape_graphql_string(&desired.agent_did),
            summary = escape_graphql_string(&desired.summary),
            files_read = escape_graphql_string(&desired.files_read),
            files_modified = escape_graphql_string(&desired.files_modified),
            messages_compacted = desired.messages_compacted,
            original_tokens = desired.original_tokens,
            compacted_tokens = desired.compacted_tokens,
            source_manifest_version = desired.source_manifest_version,
            source_manifest_json = escape_graphql_string(&desired.source_manifest_json),
            created_at = escape_graphql_string(&desired.created_at),
        );
        let response = execute(node, mutation, Some(identity.clone())).await;
        if !response.has_errors() {
            let returned = mutation_doc_ids(response.data.as_ref());
            if returned.len() != 1 {
                anyhow::bail!("creating CompactionEntry returned unexpected _docIDs={returned:?}");
            }
            let rows = load_rows(node, session_id, Some(identity.clone())).await?;
            reject_logical_twins(&rows, session_id)?;
            let persisted = candidate_for_desired(rows, &desired)?.ok_or_else(|| {
                anyhow::anyhow!(
                    "created CompactionEntry {} was not observable by exact logical key/order",
                    returned[0]
                )
            })?;
            if persisted.doc_id != returned[0] {
                anyhow::bail!(
                    "created CompactionEntry returned _docID={} but observed {}",
                    returned[0],
                    persisted.doc_id
                );
            }
            verify_compaction_row(node, &persisted, Some(identity.clone())).await?;
            return CompactionEntry::try_from(persisted);
        }
        if attempt == COMPACTION_FACT_ATTEMPTS {
            anyhow::bail!("creating CompactionEntry failed: {:?}", response.errors);
        }
        tokio::task::yield_now().await;
    }
    unreachable!("bounded CompactionEntry persistence loop returns")
}
