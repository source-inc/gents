use super::retry::execute_query_timed;
use super::*;
use anyhow::Context as _;
use gents_protocol::transcript::decode_persisted_message;
use serde_json::Value;
use std::collections::HashSet;

/// Exact finalized transcript loaded for one provider request assembly.
///
/// `messages` and `fact_refs` have the same canonical sequence order. Each
/// reference names the signed DefraDB composite snapshot from which the
/// corresponding message was decoded.
#[derive(Clone, Debug, PartialEq)]
pub struct LoadedHistory {
    pub messages: Vec<Message>,
    pub fact_refs: Vec<MessageFactRef>,
}

/// Physical and content-addressed identity of one verified transcript fact.
///
/// `_docID` remains stable for a document; `composite_commit_cid` pins the exact
/// payload consumed during request assembly. `sequence` makes the manifest's
/// transcript snapshot order explicit rather than relying on JSON array order
/// alone.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageFactRef {
    pub sequence: u32,
    pub doc_id: String,
    pub composite_commit_cid: String,
    pub signer_did: String,
}

pub async fn load_history(node: &EmbeddedNode, session_id: &str) -> Result<Vec<Message>> {
    Ok(load_history_with_refs(node, session_id).await?.messages)
}

pub async fn load_history_with_refs(
    node: &EmbeddedNode,
    session_id: &str,
) -> Result<LoadedHistory> {
    let escaped_session_id = escape_graphql_string(session_id);
    let query = format!(
        r#"{{
            AgentMessage(
                filter: {{ session_id: {{ _eq: "{escaped_session_id}" }} }},
                order: {{ sequence: ASC }}
            ) {{
                _docID
                message_key
                session_id
                agent_did
                requester_did
                request_id
                request_doc_id
                sequence
                role
                content
                reasoning
                timestamp
            }}
        }}"#
    );

    let resp = execute_query_timed(node, &query, "load_history").await;
    if resp.has_errors() {
        anyhow::bail!(
            "loading history for session_id={}: {:?}",
            session_id,
            resp.errors
        );
    }

    let messages: Vec<MessageFactRow> =
        match resp.data.as_ref().and_then(|data| data.get("AgentMessage")) {
            Some(value) => serde_json::from_value(value.clone())?,
            None => Vec::new(),
        };

    let mut seen_doc_ids = HashSet::with_capacity(messages.len());
    let mut seen_message_keys = HashSet::with_capacity(messages.len());
    let mut seen_sequences = HashSet::with_capacity(messages.len());
    let mut history = Vec::with_capacity(messages.len());
    let mut fact_refs = Vec::with_capacity(messages.len());
    let mut previous_sequence = None;
    for msg in messages {
        if msg.doc_id.is_empty() || msg.session_id != session_id {
            anyhow::bail!(
                "provider history rejected invalid AgentMessage fact for session_id={session_id} _docID={} message_key={} sequence={}",
                msg.doc_id,
                msg.message_key,
                msg.sequence
            );
        }
        if !seen_doc_ids.insert(msg.doc_id.clone())
            || !seen_message_keys.insert(msg.message_key.clone())
            || !seen_sequences.insert(msg.sequence)
        {
            anyhow::bail!(
                "provider history rejected ambiguous AgentMessage facts for session_id={session_id}: _docID={} message_key={} sequence={}",
                msg.doc_id,
                msg.message_key,
                msg.sequence
            );
        }
        if previous_sequence.is_some_and(|previous| msg.sequence <= previous) {
            anyhow::bail!(
                "provider history rejected non-canonical AgentMessage order for session_id={session_id}: previous_sequence={previous_sequence:?} sequence={}",
                msg.sequence
            );
        }
        let fact_ref = verify_finalized_message_fact(node, &msg).await?;
        tracing::trace!(
            doc_id = %fact_ref.doc_id,
            composite_commit_cid = %fact_ref.composite_commit_cid,
            signer_did = %fact_ref.signer_did,
            "verified finalized AgentMessage provider-history fact"
        );
        history.push(decode_persisted_message(
            msg.role.as_str(),
            msg.content.as_str(),
        ));
        previous_sequence = Some(msg.sequence);
        fact_refs.push(fact_ref);
    }

    tracing::Span::current().record("history_message_count", history.len() as i64);
    tracing::debug!(session_id = %session_id, count = history.len(), "loaded history");
    Ok(LoadedHistory {
        messages: history,
        fact_refs,
    })
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn save_message(
    node: &EmbeddedNode,
    session_id: &str,
    agent_did: &str,
    sequence: u32,
    role: &str,
    content: &str,
    reasoning: Option<&str>,
) -> Result<()> {
    save_message_with_requester_did(
        node, session_id, agent_did, None, sequence, role, content, reasoning,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn save_message_with_requester_did(
    node: &EmbeddedNode,
    session_id: &str,
    agent_did: &str,
    requester_did: Option<&str>,
    sequence: u32,
    role: &str,
    content: &str,
    reasoning: Option<&str>,
) -> Result<()> {
    save_message_with_requester_did_and_request_id(
        node,
        session_id,
        agent_did,
        requester_did,
        sequence,
        role,
        content,
        reasoning,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn save_message_with_requester_did_and_request_id(
    node: &EmbeddedNode,
    session_id: &str,
    agent_did: &str,
    requester_did: Option<&str>,
    sequence: u32,
    role: &str,
    content: &str,
    reasoning: Option<&str>,
    request_id: Option<&str>,
) -> Result<()> {
    let message_key = format!("{session_id}:{sequence}");
    save_message_inner(
        node,
        session_id,
        agent_did,
        requester_did,
        sequence,
        role,
        content,
        reasoning,
        request_id,
        &message_key,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn save_message_inner(
    node: &EmbeddedNode,
    session_id: &str,
    agent_did: &str,
    requester_did: Option<&str>,
    sequence: u32,
    role: &str,
    content: &str,
    reasoning: Option<&str>,
    request_id: Option<&str>,
    message_key: &str,
) -> Result<()> {
    persist_finalized_message(
        node,
        DesiredMessageFact {
            message_key,
            session_id,
            agent_did,
            requester_did,
            request_id,
            request_doc_id: None,
            sequence,
            role,
            content,
            reasoning: reasoning.unwrap_or(""),
        },
    )
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn save_message_draft_with_requester_did(
    node: &EmbeddedNode,
    session_id: &str,
    agent_did: &str,
    requester_did: Option<&str>,
    sequence: u32,
    role: &str,
    content: &str,
    reasoning: Option<&str>,
) -> Result<()> {
    save_message_draft_with_requester_did_and_request_id(
        node,
        session_id,
        agent_did,
        requester_did,
        sequence,
        role,
        content,
        reasoning,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn save_message_draft_with_requester_did_and_request_id(
    node: &EmbeddedNode,
    session_id: &str,
    agent_did: &str,
    requester_did: Option<&str>,
    sequence: u32,
    role: &str,
    content: &str,
    reasoning: Option<&str>,
    request_id: Option<&str>,
) -> Result<()> {
    let message_key = format!("{session_id}:{sequence}");
    persist_draft_message(
        node,
        DesiredMessageFact {
            message_key: &message_key,
            session_id,
            agent_did,
            requester_did,
            request_id,
            request_doc_id: None,
            sequence,
            role,
            content,
            reasoning: reasoning.unwrap_or(""),
        },
    )
    .await
}

/// Reserve the next durable transcript position with a mutable draft.
///
/// In-flight assistant turns use this before inline tool execution. Other
/// writers must observe the reservation when allocating their own finalized
/// positions, while the assistant payload remains replaceable until its true
/// final boundary.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn append_message_draft_with_requester_did(
    node: &EmbeddedNode,
    session_id: &str,
    agent_did: &str,
    requester_did: Option<&str>,
    role: &str,
    content: &str,
    reasoning: Option<&str>,
    request_id: Option<&str>,
) -> Result<u32> {
    let mut attempts = 0;
    loop {
        attempts += 1;
        let sequence = next_append_sequence(node, session_id).await?;
        match save_message_draft_with_requester_did_and_request_id(
            node,
            session_id,
            agent_did,
            requester_did,
            sequence,
            role,
            content,
            reasoning,
            request_id,
        )
        .await
        {
            Ok(()) => return Ok(sequence),
            Err(error) if attempts < 5 => {
                tracing::debug!(
                    session_id,
                    sequence,
                    error = %error,
                    "assistant draft reservation lost a sequence race; retrying"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

const MESSAGE_FACT_ATTEMPTS: usize = 3;

#[derive(Clone, Copy)]
struct DesiredMessageFact<'a> {
    message_key: &'a str,
    session_id: &'a str,
    agent_did: &'a str,
    requester_did: Option<&'a str>,
    request_id: Option<&'a str>,
    request_doc_id: Option<&'a str>,
    sequence: u32,
    role: &'a str,
    content: &'a str,
    reasoning: &'a str,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct MessageFactRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    message_key: String,
    session_id: String,
    agent_did: String,
    #[serde(default)]
    requester_did: Option<String>,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    request_doc_id: Option<String>,
    sequence: u32,
    role: String,
    content: String,
    #[serde(default)]
    reasoning: Option<String>,
    timestamp: String,
}

#[derive(Debug, Deserialize)]
struct MessageCommitParent {
    cid: String,
    #[serde(rename = "fieldName")]
    field_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MessageCompositeCommit {
    cid: String,
    #[serde(default)]
    heads: Vec<MessageCommitParent>,
}

struct MessagePersistOutcome {
    sequence: u32,
    created: bool,
    fact_ref: MessageFactRef,
}

async fn verify_finalized_message_fact(
    node: &EmbeddedNode,
    expected: &MessageFactRow,
) -> Result<MessageFactRef> {
    let escaped_doc_id = escape_graphql_string(&expected.doc_id);
    let response = node
        .execute(&format!(
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
        .await;
    if response.has_errors() {
        anyhow::bail!(
            "querying AgentMessage {} commit evidence: {:?}",
            expected.doc_id,
            response.errors
        );
    }
    let commits: Vec<MessageCompositeCommit> = response
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
        [current] => *current,
        [] => anyhow::bail!(
            "AgentMessage {} has no current composite head",
            expected.doc_id
        ),
        current => anyhow::bail!(
            "AgentMessage {} has {} current composite heads",
            expected.doc_id,
            current.len()
        ),
    };
    let signer_did = node
        .verified_block_signer_did(&current.cid)
        .await
        .with_context(|| {
            format!(
                "cryptographically verifying AgentMessage {} current head {}",
                expected.doc_id, current.cid
            )
        })?;
    if signer_did.trim().is_empty() {
        anyhow::bail!(
            "AgentMessage {} current head {} has an empty signer DID",
            expected.doc_id,
            current.cid
        );
    }

    let escaped_cid = escape_graphql_string(&current.cid);
    let response = node
        .execute(&format!(
            r#"query {{
                AgentMessage(cid: ["{escaped_cid}"]) {{
                    _docID message_key session_id agent_did requester_did request_id request_doc_id
                    sequence role content reasoning timestamp
                }}
            }}"#
        ))
        .await;
    if response.has_errors() {
        anyhow::bail!(
            "loading AgentMessage {} exact snapshot {}: {:?}",
            expected.doc_id,
            current.cid,
            response.errors
        );
    }
    let rows: Vec<MessageFactRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentMessage"))
        .cloned()
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or_default();
    let exact = match rows.as_slice() {
        [row] if row.doc_id == expected.doc_id => row,
        [row] => anyhow::bail!(
            "AgentMessage CID {} reconstructed _docID={}, expected {}",
            current.cid,
            row.doc_id,
            expected.doc_id
        ),
        rows => anyhow::bail!(
            "AgentMessage CID {} reconstructed {} documents, expected one",
            current.cid,
            rows.len()
        ),
    };
    if !signed_snapshot_matches_finalized_fact(exact, expected) {
        anyhow::bail!(
            "AgentMessage {} current signed snapshot {} does not match finalized facts",
            expected.doc_id,
            current.cid
        );
    }
    Ok(MessageFactRef {
        sequence: expected.sequence,
        doc_id: expected.doc_id.clone(),
        composite_commit_cid: current.cid.clone(),
        signer_did,
    })
}

fn signed_snapshot_matches_finalized_fact(
    exact: &MessageFactRow,
    expected: &MessageFactRow,
) -> bool {
    exact.doc_id == expected.doc_id
        && exact.message_key == expected.message_key
        && exact.session_id == expected.session_id
        && exact.agent_did == expected.agent_did
        && exact.requester_did == expected.requester_did
        && exact.request_id == expected.request_id
        && exact.request_doc_id == expected.request_doc_id
        && exact.sequence == expected.sequence
        && exact.role == expected.role
        && exact.content == expected.content
        && exact.reasoning == expected.reasoning
        && rfc3339_instants_equal(&exact.timestamp, &expected.timestamp)
}

pub(super) fn rfc3339_instants_equal(left: &str, right: &str) -> bool {
    let Ok(left) = chrono::DateTime::parse_from_rfc3339(left) else {
        return false;
    };
    let Ok(right) = chrono::DateTime::parse_from_rfc3339(right) else {
        return false;
    };
    left == right
}

fn normalized(value: Option<&str>) -> &str {
    value.unwrap_or("")
}

fn finalized_fact_matches(row: &MessageFactRow, desired: DesiredMessageFact<'_>) -> bool {
    row.message_key == desired.message_key
        && row.session_id == desired.session_id
        && row.agent_did == desired.agent_did
        && normalized(row.requester_did.as_deref()) == normalized(desired.requester_did)
        && normalized(row.request_id.as_deref()) == normalized(desired.request_id)
        && normalized(row.request_doc_id.as_deref()) == normalized(desired.request_doc_id)
        && row.sequence == desired.sequence
        && row.role == desired.role
        && row.content == desired.content
        && normalized(row.reasoning.as_deref()) == desired.reasoning
}

fn draft_identity_matches(row: &MessageFactRow, desired: DesiredMessageFact<'_>) -> bool {
    row.message_key == desired.message_key
        && row.session_id == desired.session_id
        && row.agent_did == desired.agent_did
        && normalized(row.requester_did.as_deref()) == normalized(desired.requester_did)
        && normalized(row.request_id.as_deref()) == normalized(desired.request_id)
        && normalized(row.request_doc_id.as_deref()) == normalized(desired.request_doc_id)
        && row.sequence == desired.sequence
        && row.role == desired.role
}

async fn load_message_fact_candidates(
    node: &EmbeddedNode,
    session_id: &str,
    sequence: u32,
    message_key: &str,
) -> Result<Vec<MessageFactRow>> {
    let escaped_session_id = escape_graphql_string(session_id);
    let escaped_message_key = escape_graphql_string(message_key);
    let fields = r#"
        _docID message_key session_id agent_did requester_did request_id request_doc_id
        sequence role content reasoning timestamp
    "#;
    let query = format!(
        r#"{{
            by_key: AgentMessage(
                filter: {{ message_key: {{ _eq: "{escaped_message_key}" }} }}
            ) {{ {fields} }}
            by_order: AgentMessage(
                filter: {{
                    session_id: {{ _eq: "{escaped_session_id}" }},
                    sequence: {{ _eq: {sequence} }}
                }}
            ) {{ {fields} }}
        }}"#
    );
    let response = execute_query_timed(node, &query, "load_message_fact_candidates").await;
    if response.has_errors() {
        anyhow::bail!(
            "loading AgentMessage fact candidates session_id={session_id} sequence={sequence} message_key={message_key}: {:?}",
            response.errors
        );
    }

    let mut by_doc_id = std::collections::BTreeMap::new();
    for field in ["by_key", "by_order"] {
        let rows: Vec<MessageFactRow> = response
            .data
            .as_ref()
            .and_then(|data| data.get(field))
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()?
            .unwrap_or_default();
        for row in rows {
            by_doc_id.insert(row.doc_id.clone(), row);
        }
    }
    Ok(by_doc_id.into_values().collect())
}

/// Resolve one finalized transcript row to its exact current signed version.
///
/// This is the response-outcome handoff: callers receive the physical document
/// id, composite CID, signer, and sequence instead of reconstructing terminal
/// provenance from the logical sequence later.
pub(crate) async fn message_fact_ref_for_sequence(
    node: &EmbeddedNode,
    session_id: &str,
    sequence: u32,
    agent_did: &str,
) -> Result<MessageFactRef> {
    let node_did = node.node_identity_did().ok_or_else(|| {
        anyhow::anyhow!("resolving AgentMessage fact requires a DefraDB node identity")
    })?;
    if node_did != agent_did {
        anyhow::bail!(
            "AgentMessage fact agent DID {agent_did} does not match node identity {node_did}"
        );
    }
    let identity = identity::Did::new(agent_did).context("parsing AgentMessage query identity")?;
    let escaped_session_id = escape_graphql_string(session_id);
    let query = format!(
        r#"query {{
            AgentMessage(filter: {{
                session_id: {{ _eq: "{escaped_session_id}" }},
                sequence: {{ _eq: {sequence} }}
            }}) {{
                _docID message_key session_id agent_did requester_did request_id request_doc_id
                sequence role content reasoning timestamp
            }}
        }}"#
    );
    let response = node
        .execute_request_with_retry(
            defra_node::QueryRequest::new(query).with_identity(Some(identity.clone())),
            defra_node::ExecuteRetryPolicy::default(),
        )
        .await;
    if response.has_errors() {
        anyhow::bail!(
            "loading AgentMessage exact fact session_id={session_id} sequence={sequence}: {:?}",
            response.errors
        );
    }
    let rows: Vec<MessageFactRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentMessage"))
        .cloned()
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or_default();
    let [row] = rows.as_slice() else {
        anyhow::bail!(
            "AgentMessage session_id={session_id} sequence={sequence} resolved to {} rows",
            rows.len()
        );
    };
    if row.session_id != session_id || row.sequence != sequence || row.agent_did != agent_did {
        anyhow::bail!("AgentMessage exact fact does not match requested transcript lineage");
    }
    let version = crate::document_version::verified_current_signed_document_version_with_identity(
        node,
        "AgentMessage",
        &row.doc_id,
        Some(identity),
    )
    .await?;
    Ok(MessageFactRef {
        sequence,
        doc_id: row.doc_id.clone(),
        composite_commit_cid: version.version.composite_commit_cid,
        signer_did: version.signer_did,
    })
}

async fn load_message_draft_candidates(
    node: &EmbeddedNode,
    session_id: &str,
    sequence: u32,
    message_key: &str,
) -> Result<Vec<MessageFactRow>> {
    let escaped_session_id = escape_graphql_string(session_id);
    let escaped_message_key = escape_graphql_string(message_key);
    let fields = r#"
        _docID message_key session_id agent_did requester_did request_id request_doc_id
        sequence role content reasoning timestamp
    "#;
    let query = format!(
        r#"{{
            by_key: AgentMessageDraft(
                filter: {{ message_key: {{ _eq: "{escaped_message_key}" }} }}
            ) {{ {fields} }}
            by_order: AgentMessageDraft(
                filter: {{
                    session_id: {{ _eq: "{escaped_session_id}" }},
                    sequence: {{ _eq: {sequence} }}
                }}
            ) {{ {fields} }}
        }}"#
    );
    let response = execute_query_timed(node, &query, "load_message_draft_candidates").await;
    if response.has_errors() {
        anyhow::bail!(
            "loading AgentMessageDraft candidates session_id={session_id} sequence={sequence} message_key={message_key}: {:?}",
            response.errors
        );
    }

    let mut by_doc_id = std::collections::BTreeMap::new();
    for field in ["by_key", "by_order"] {
        let rows: Vec<MessageFactRow> = response
            .data
            .as_ref()
            .and_then(|data| data.get(field))
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()?
            .unwrap_or_default();
        for row in rows {
            by_doc_id.insert(row.doc_id.clone(), row);
        }
    }
    Ok(by_doc_id.into_values().collect())
}

fn require_one_candidate(
    candidates: Vec<MessageFactRow>,
    desired: DesiredMessageFact<'_>,
) -> Result<Option<MessageFactRow>> {
    if candidates.len() <= 1 {
        return Ok(candidates.into_iter().next());
    }
    let conflicts = candidates
        .iter()
        .map(|row| {
            format!(
                "_docID={} key={} sequence={}",
                row.doc_id, row.message_key, row.sequence
            )
        })
        .collect::<Vec<_>>();
    anyhow::bail!(
        "AgentMessage logical fact conflict for session_id={} message_key={} sequence={}: {conflicts:?}",
        desired.session_id,
        desired.message_key,
        desired.sequence
    )
}

async fn exact_request_doc_id_for_message(
    node: &EmbeddedNode,
    request_id: Option<&str>,
    agent_did: &str,
) -> Result<Option<String>> {
    let Some(request_id) = request_id.filter(|value| !value.trim().is_empty()) else {
        return Ok(None);
    };
    let node_did = node.node_identity_did().ok_or_else(|| {
        anyhow::anyhow!("request-linked AgentMessage persistence requires a node identity")
    })?;
    if node_did != agent_did {
        anyhow::bail!("AgentMessage agent DID {agent_did} does not match node identity {node_did}");
    }
    let identity = identity::Did::new(agent_did).context("parsing AgentMessage identity")?;
    let escaped_request_id = escape_graphql_string(request_id);
    let escaped_agent_did = escape_graphql_string(agent_did);
    let response = node
        .execute_request_with_retry(
            defra_node::QueryRequest::new(format!(
                r#"query {{
                    AgentRequest(filter: {{
                        request_id: {{ _eq: "{escaped_request_id}" }},
                        agent_did: {{ _eq: "{escaped_agent_did}" }}
                    }}) {{ _docID }}
                }}"#
            ))
            .with_identity(Some(identity)),
            defra_node::ExecuteRetryPolicy::default(),
        )
        .await;
    if response.has_errors() {
        anyhow::bail!(
            "resolving exact AgentRequest for AgentMessage request_id={request_id}: {:?}",
            response.errors
        );
    }
    let rows = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    match rows.as_slice() {
        [] => Ok(None),
        [row] => row
            .get("_docID")
            .and_then(Value::as_str)
            .map(|doc_id| Some(doc_id.to_string()))
            .ok_or_else(|| anyhow::anyhow!("AgentRequest exact lookup returned no _docID")),
        rows => anyhow::bail!(
            "AgentMessage request_id={request_id} resolves to {} AgentRequest documents",
            rows.len()
        ),
    }
}

async fn persist_finalized_message(
    node: &EmbeddedNode,
    desired: DesiredMessageFact<'_>,
) -> Result<MessagePersistOutcome> {
    let request_doc_id =
        exact_request_doc_id_for_message(node, desired.request_id, desired.agent_did).await?;
    let desired = DesiredMessageFact {
        request_doc_id: request_doc_id.as_deref(),
        ..desired
    };
    let mut last_create_errors = None;
    for attempt in 1..=MESSAGE_FACT_ATTEMPTS {
        let existing = require_one_candidate(
            load_message_fact_candidates(
                node,
                desired.session_id,
                desired.sequence,
                desired.message_key,
            )
            .await?,
            desired,
        )?;
        match existing {
            Some(row) => {
                if finalized_fact_matches(&row, desired) {
                    let fact_ref = verify_finalized_message_fact(node, &row).await?;
                    return Ok(MessagePersistOutcome {
                        sequence: row.sequence,
                        created: false,
                        fact_ref,
                    });
                }
                anyhow::bail!(
                    "AgentMessage finalized fact conflict: _docID={} message_key={} sequence={} existing_role={} desired_role={}",
                    row.doc_id,
                    row.message_key,
                    row.sequence,
                    row.role,
                    desired.role
                );
            }
            None => {
                let mutation = create_message_mutation(desired, "AgentMessage");
                let response = node.execute(&mutation).await;
                if !response.has_errors() {
                    let returned = mutation_doc_ids(response.data.as_ref(), "add_AgentMessage");
                    if returned.len() != 1 {
                        anyhow::bail!(
                            "creating finalized AgentMessage returned unexpected _docIDs={returned:?} data={:?}",
                            response.data
                        );
                    }
                    let persisted = require_one_candidate(
                        load_message_fact_candidates(
                            node,
                            desired.session_id,
                            desired.sequence,
                            desired.message_key,
                        )
                        .await?,
                        desired,
                    )?
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "created AgentMessage _docID={} was not observable by exact logical key/order",
                            returned[0]
                        )
                    })?;
                    if persisted.doc_id != returned[0]
                        || !finalized_fact_matches(&persisted, desired)
                    {
                        anyhow::bail!(
                            "created AgentMessage did not round-trip exact facts: returned _docID={} observed _docID={}",
                            returned[0],
                            persisted.doc_id
                        );
                    }
                    let fact_ref = verify_finalized_message_fact(node, &persisted).await?;
                    return Ok(MessagePersistOutcome {
                        sequence: desired.sequence,
                        created: true,
                        fact_ref,
                    });
                }
                last_create_errors = Some(format!("{:?}", response.errors));
            }
        }
        if attempt < MESSAGE_FACT_ATTEMPTS {
            tokio::task::yield_now().await;
        }
    }
    anyhow::bail!(
        "AgentMessage finalization did not converge after {MESSAGE_FACT_ATTEMPTS} attempts: session_id={} message_key={} sequence={} create_errors={last_create_errors:?}",
        desired.session_id,
        desired.message_key,
        desired.sequence
    )
}

async fn persist_draft_message(node: &EmbeddedNode, desired: DesiredMessageFact<'_>) -> Result<()> {
    let request_doc_id =
        exact_request_doc_id_for_message(node, desired.request_id, desired.agent_did).await?;
    let desired = DesiredMessageFact {
        request_doc_id: request_doc_id.as_deref(),
        ..desired
    };
    for attempt in 1..=MESSAGE_FACT_ATTEMPTS {
        if let Some(finalized) = require_one_candidate(
            load_message_fact_candidates(
                node,
                desired.session_id,
                desired.sequence,
                desired.message_key,
            )
            .await?,
            desired,
        )? {
            if finalized_fact_matches(&finalized, desired) {
                return Ok(());
            }
            anyhow::bail!(
                "AgentMessage finalized fact cannot be rewritten by draft: _docID={} message_key={} sequence={}",
                finalized.doc_id,
                finalized.message_key,
                finalized.sequence
            );
        }
        let existing = require_one_candidate(
            load_message_draft_candidates(
                node,
                desired.session_id,
                desired.sequence,
                desired.message_key,
            )
            .await?,
            desired,
        )?;
        match existing {
            Some(row) => {
                if !draft_identity_matches(&row, desired) {
                    anyhow::bail!(
                        "AgentMessage draft ownership/order conflict: _docID={} message_key={} sequence={}",
                        row.doc_id,
                        row.message_key,
                        row.sequence
                    );
                }
                if row.content == desired.content
                    && normalized(row.reasoning.as_deref()) == desired.reasoning
                {
                    return Ok(());
                }
                let mutation = update_draft_mutation(&row, desired);
                let response = super::retry::execute_mutation_with_retry(
                    node,
                    &mutation,
                    "update AgentMessage draft",
                )
                .await?;
                let returned = mutation_doc_ids(response.data.as_ref(), "update_AgentMessageDraft");
                if returned.as_slice() == [row.doc_id.as_str()] {
                    return Ok(());
                }
                if !returned.is_empty() {
                    anyhow::bail!(
                        "AgentMessage draft update returned unexpected _docIDs: target={} returned={returned:?}",
                        row.doc_id
                    );
                }
            }
            None => {
                let mutation = create_message_mutation(desired, "AgentMessageDraft");
                let response = node.execute(&mutation).await;
                if !response.has_errors() {
                    let returned =
                        mutation_doc_ids(response.data.as_ref(), "add_AgentMessageDraft");
                    if returned.len() != 1 {
                        anyhow::bail!(
                            "creating draft AgentMessage returned unexpected _docIDs={returned:?} data={:?}",
                            response.data
                        );
                    }
                    let persisted = require_one_candidate(
                        load_message_draft_candidates(
                            node,
                            desired.session_id,
                            desired.sequence,
                            desired.message_key,
                        )
                        .await?,
                        desired,
                    )?
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "created AgentMessageDraft _docID={} was not observable by exact logical key/order",
                            returned[0]
                        )
                    })?;
                    if persisted.doc_id != returned[0]
                        || !draft_identity_matches(&persisted, desired)
                        || persisted.content != desired.content
                        || normalized(persisted.reasoning.as_deref()) != desired.reasoning
                    {
                        anyhow::bail!(
                            "created AgentMessageDraft did not round-trip exact facts: returned _docID={} observed _docID={}",
                            returned[0],
                            persisted.doc_id
                        );
                    }
                    return Ok(());
                }
            }
        }
        if attempt < MESSAGE_FACT_ATTEMPTS {
            tokio::task::yield_now().await;
        }
    }
    anyhow::bail!(
        "AgentMessage draft persistence did not converge after {MESSAGE_FACT_ATTEMPTS} attempts: session_id={} message_key={} sequence={}",
        desired.session_id,
        desired.message_key,
        desired.sequence
    )
}

fn create_message_mutation(desired: DesiredMessageFact<'_>, collection: &str) -> String {
    let requester_did_field = super::requester_did_create_field(desired.requester_did);
    let timestamp = chrono::Utc::now().to_rfc3339();
    format!(
        r#"mutation {{
            add_{collection}(input: {{
                message_key: "{message_key}",
                session_id: "{session_id}",
                agent_did: "{agent_did}",
                {requester_did_field}
                request_id: "{request_id}",
                request_doc_id: {request_doc_id},
                sequence: {sequence},
                role: "{role}",
                content: "{content}",
                reasoning: "{reasoning}",
                timestamp: "{timestamp}"
            }}) {{ _docID }}
        }}"#,
        message_key = escape_graphql_string(desired.message_key),
        session_id = escape_graphql_string(desired.session_id),
        agent_did = escape_graphql_string(desired.agent_did),
        request_id = escape_graphql_string(desired.request_id.unwrap_or("")),
        request_doc_id = desired
            .request_doc_id
            .map(|doc_id| format!("\"{}\"", escape_graphql_string(doc_id)))
            .unwrap_or_else(|| "null".to_string()),
        sequence = desired.sequence,
        role = escape_graphql_string(desired.role),
        content = escape_graphql_string(desired.content),
        reasoning = escape_graphql_string(desired.reasoning),
        timestamp = escape_graphql_string(&timestamp),
    )
}

fn exact_draft_filter(row: &MessageFactRow) -> String {
    format!(
        r#"_docID: {{ _eq: "{doc_id}" }},
            message_key: {{ _eq: "{message_key}" }},
            session_id: {{ _eq: "{session_id}" }},
            agent_did: {{ _eq: "{agent_did}" }},
            sequence: {{ _eq: {sequence} }},
            role: {{ _eq: "{role}" }},
            content: {{ _eq: "{content}" }},
            reasoning: {{ _eq: "{reasoning}" }},
            timestamp: {{ _eq: "{timestamp}" }}"#,
        doc_id = escape_graphql_string(&row.doc_id),
        message_key = escape_graphql_string(&row.message_key),
        session_id = escape_graphql_string(&row.session_id),
        agent_did = escape_graphql_string(&row.agent_did),
        sequence = row.sequence,
        role = escape_graphql_string(&row.role),
        content = escape_graphql_string(&row.content),
        reasoning = escape_graphql_string(row.reasoning.as_deref().unwrap_or("")),
        timestamp = escape_graphql_string(&row.timestamp),
    )
}

fn update_draft_mutation(row: &MessageFactRow, desired: DesiredMessageFact<'_>) -> String {
    let filter = exact_draft_filter(row);
    let timestamp = chrono::Utc::now().to_rfc3339();
    format!(
        r#"mutation {{
            update_AgentMessageDraft(
                filter: {{ {filter} }},
                input: {{
                    content: "{content}",
                    reasoning: "{reasoning}",
                    timestamp: "{timestamp}"
                }}
            ) {{ _docID }}
        }}"#,
        content = escape_graphql_string(desired.content),
        reasoning = escape_graphql_string(desired.reasoning),
        timestamp = escape_graphql_string(&timestamp),
    )
}

fn mutation_doc_ids(data: Option<&Value>, field: &str) -> Vec<String> {
    let Some(value) = data.and_then(|data| data.get(field)) else {
        return Vec::new();
    };
    if let Some(doc_id) = value.get("_docID").and_then(Value::as_str) {
        return vec![doc_id.to_owned()];
    }
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|row| row.get("_docID").and_then(Value::as_str).map(str::to_owned))
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn append_message_with_requester_did(
    node: &EmbeddedNode,
    session_id: &str,
    agent_did: &str,
    requester_did: Option<&str>,
    role: &str,
    content: &str,
    reasoning: Option<&str>,
    request_id: Option<&str>,
) -> Result<u32> {
    let mut attempts = 0;
    loop {
        attempts += 1;
        let sequence = next_append_sequence(node, session_id).await?;
        match create_message(
            node,
            session_id,
            agent_did,
            requester_did,
            sequence,
            role,
            content,
            reasoning,
            request_id,
            None,
        )
        .await
        {
            Ok(outcome) => {
                tracing::trace!(
                    doc_id = %outcome.fact_ref.doc_id,
                    composite_commit_cid = %outcome.fact_ref.composite_commit_cid,
                    signer_did = %outcome.fact_ref.signer_did,
                    sequence = outcome.sequence,
                    "appended exact finalized AgentMessage fact"
                );
                return Ok(outcome.sequence);
            }
            Err(error) if attempts < 5 => {
                tracing::debug!(
                    session_id = %session_id,
                    sequence,
                    error = %error,
                    "append_message create failed; retrying with refreshed sequence"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Append a message exactly once under a caller-owned stable key.
///
/// Concurrent writers can reserve the same next sequence or race on the same
/// key. A successful key winner is authoritative; losers re-read that durable
/// row and return its sequence without updating its content.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn append_message_once_with_key_and_requester_did(
    node: &EmbeddedNode,
    session_id: &str,
    agent_did: &str,
    requester_did: Option<&str>,
    role: &str,
    content: &str,
    reasoning: Option<&str>,
    request_id: Option<&str>,
    message_key: &str,
    preferred_sequence: Option<u32>,
) -> Result<(u32, bool)> {
    if let Some(existing) = message_fact_for_key(node, session_id, message_key).await? {
        let desired = DesiredMessageFact {
            message_key,
            session_id,
            agent_did,
            requester_did,
            request_id,
            request_doc_id: None,
            sequence: existing.sequence,
            role,
            content,
            reasoning: reasoning.unwrap_or(""),
        };
        if finalized_fact_matches(&existing, desired) {
            verify_finalized_message_fact(node, &existing).await?;
            return Ok((existing.sequence, false));
        }
        anyhow::bail!(
            "AgentMessage keyed replay conflicts with finalized fact: _docID={} message_key={} sequence={}",
            existing.doc_id,
            existing.message_key,
            existing.sequence
        );
    }

    let mut attempts = 0;
    loop {
        attempts += 1;
        let sequence = match preferred_sequence {
            Some(sequence) if !message_sequence_exists(node, session_id, sequence).await? => {
                sequence
            }
            Some(_) | None => next_append_sequence(node, session_id).await?,
        };
        match create_message(
            node,
            session_id,
            agent_did,
            requester_did,
            sequence,
            role,
            content,
            reasoning,
            request_id,
            Some(message_key),
        )
        .await
        {
            Ok(outcome) => return Ok((outcome.sequence, outcome.created)),
            Err(error) => {
                if let Some(existing) = message_fact_for_key(node, session_id, message_key).await? {
                    let desired = DesiredMessageFact {
                        message_key,
                        session_id,
                        agent_did,
                        requester_did,
                        request_id,
                        request_doc_id: None,
                        sequence: existing.sequence,
                        role,
                        content,
                        reasoning: reasoning.unwrap_or(""),
                    };
                    if finalized_fact_matches(&existing, desired) {
                        verify_finalized_message_fact(node, &existing).await?;
                        return Ok((existing.sequence, false));
                    }
                    return Err(error.context(format!(
                        "AgentMessage keyed replay conflicts with _docID={} sequence={}",
                        existing.doc_id, existing.sequence
                    )));
                }
                if attempts >= 5 {
                    return Err(error);
                }
                tracing::debug!(
                    session_id,
                    message_key,
                    sequence,
                    error = %error,
                    "keyed append lost a sequence race; retrying"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
}

async fn message_sequence_exists(
    node: &EmbeddedNode,
    session_id: &str,
    sequence: u32,
) -> Result<bool> {
    let escaped_session_id = escape_graphql_string(session_id);
    let query = format!(
        r#"{{
            AgentMessage(
                filter: {{
                    session_id: {{ _eq: "{escaped_session_id}" }},
                    sequence: {{ _eq: {sequence} }}
                }},
                limit: 1
            ) {{ sequence }}
        }}"#
    );
    let response = execute_query_timed(node, &query, "message_sequence_exists").await;
    if response.has_errors() {
        anyhow::bail!(
            "checking AgentMessage sequence for session_id={} sequence={}: {:?}",
            session_id,
            sequence,
            response.errors
        );
    }
    Ok(response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentMessage"))
        .and_then(Value::as_array)
        .is_some_and(|rows| !rows.is_empty()))
}

async fn message_fact_for_key(
    node: &EmbeddedNode,
    session_id: &str,
    message_key: &str,
) -> Result<Option<MessageFactRow>> {
    let escaped_session_id = escape_graphql_string(session_id);
    let escaped_message_key = escape_graphql_string(message_key);
    let query = format!(
        r#"{{
            AgentMessage(
                filter: {{
                    session_id: {{ _eq: "{escaped_session_id}" }},
                    message_key: {{ _eq: "{escaped_message_key}" }}
                }},
            ) {{
                _docID message_key session_id agent_did requester_did request_id request_doc_id
                sequence role content reasoning timestamp
            }}
        }}"#
    );
    let response = execute_query_timed(node, &query, "message_sequence_for_key").await;
    if response.has_errors() {
        anyhow::bail!(
            "keyed AgentMessage lookup failed for session_id={} message_key={}: {:?}",
            session_id,
            message_key,
            response.errors
        );
    }
    let rows: Vec<MessageFactRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentMessage"))
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()?
        .unwrap_or_default();
    match rows.as_slice() {
        [] => Ok(None),
        [_] => Ok(rows.into_iter().next()),
        _ => {
            let doc_ids = rows
                .iter()
                .map(|row| row.doc_id.as_str())
                .collect::<Vec<_>>();
            anyhow::bail!(
                "keyed AgentMessage lookup is ambiguous for session_id={session_id} message_key={message_key}: _docIDs={doc_ids:?}"
            )
        }
    }
}

/// #497: durable request-scoped dedup. Return the sequence of an already-persisted
/// message for `(session_id, request_id, content)`, if one exists. Used to keep
/// the turn-1 user prompt + `<context>` message exactly-once across daemon retry
/// attempts (each attempt builds a fresh hook, so in-memory turn counting cannot
/// prevent a duplicate row after a transient failure before the first token).
pub(crate) async fn message_sequence_for_request_content(
    node: &EmbeddedNode,
    session_id: &str,
    request_id: &str,
    content: &str,
) -> Result<Option<u32>> {
    if request_id.is_empty() {
        return Ok(None);
    }
    let escaped_session_id = escape_graphql_string(session_id);
    let escaped_request_id = escape_graphql_string(request_id);
    let escaped_content = escape_graphql_string(content);
    let query = format!(
        r#"{{
            AgentMessage(
                filter: {{
                    session_id: {{ _eq: "{escaped_session_id}" }},
                    request_id: {{ _eq: "{escaped_request_id}" }},
                    content: {{ _eq: "{escaped_content}" }}
                }},
                order: {{ sequence: ASC }},
                limit: 1
            ) {{ sequence }}
        }}"#
    );

    let resp = execute_query_timed(node, &query, "message_sequence_for_request_content").await;
    if resp.has_errors() {
        anyhow::bail!(
            "dedup lookup for session_id={} request_id={}: {:?}",
            session_id,
            request_id,
            resp.errors
        );
    }

    let rows: Vec<ToolCallSequenceRow2> = resp
        .data
        .as_ref()
        .and_then(|data| data.get("AgentMessage"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    Ok(rows.first().map(|row| row.sequence))
}

#[derive(Deserialize)]
struct ToolCallSequenceRow2 {
    sequence: u32,
}

async fn next_append_sequence(node: &EmbeddedNode, session_id: &str) -> Result<u32> {
    let message_max = super::sessions::max_sequence(node, session_id).await?;
    let draft_max = max_message_draft_sequence(node, session_id).await?;
    let tool_call_reserved_max = max_tool_call_reserved_sequence(node, session_id).await?;
    Ok(message_max.max(draft_max).max(tool_call_reserved_max) + 1)
}

async fn max_message_draft_sequence(node: &EmbeddedNode, session_id: &str) -> Result<u32> {
    let escaped_session_id = escape_graphql_string(session_id);
    let query = format!(
        r#"{{
            AgentMessageDraft(
                filter: {{ session_id: {{ _eq: "{escaped_session_id}" }} }},
                order: {{ sequence: DESC }},
                limit: 1
            ) {{ sequence }}
        }}"#
    );
    let response = execute_query_timed(node, &query, "max_message_draft_sequence").await;
    if response.has_errors() {
        anyhow::bail!(
            "loading max draft sequence for session_id={}: {:?}",
            session_id,
            response.errors
        );
    }
    Ok(response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentMessageDraft"))
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(|message| message.get("sequence"))
        .and_then(Value::as_u64)
        .unwrap_or(0) as u32)
}

#[derive(Deserialize)]
struct ToolCallSequenceRow {
    message_sequence: u32,
}

async fn max_tool_call_reserved_sequence(node: &EmbeddedNode, session_id: &str) -> Result<u32> {
    let escaped_session_id = escape_graphql_string(session_id);
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{
                    session_id: {{ _eq: "{escaped_session_id}" }}
                    await_mode: {{ _eq: "background" }}
                }}
            ) {{ message_sequence }}
        }}"#
    );

    let resp = execute_query_timed(node, &query, "max_tool_call_reserved_sequence").await;
    if resp.has_errors() {
        anyhow::bail!(
            "loading tool-call message sequences for session_id={}: {:?}",
            session_id,
            resp.errors
        );
    }

    let rows: Vec<ToolCallSequenceRow> = resp
        .data
        .as_ref()
        .and_then(|data| data.get("AgentToolCall"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    // Background spawns reserve one result position after their assistant
    // turn so an independently appended completion cannot overtake the
    // immediate receipt. Foreground results do not reserve a position: they
    // append when the owned loop observes completion.
    let mut counts = std::collections::BTreeMap::<u32, u32>::new();
    for row in rows {
        *counts.entry(row.message_sequence).or_default() += 1;
    }
    Ok(counts
        .into_iter()
        .map(|(sequence, count)| sequence + count)
        .max()
        .unwrap_or(0))
}

#[allow(clippy::too_many_arguments)]
async fn create_message(
    node: &EmbeddedNode,
    session_id: &str,
    agent_did: &str,
    requester_did: Option<&str>,
    sequence: u32,
    role: &str,
    content: &str,
    reasoning: Option<&str>,
    request_id: Option<&str>,
    message_key: Option<&str>,
) -> Result<MessagePersistOutcome> {
    let message_key = message_key
        .map(str::to_owned)
        .unwrap_or_else(|| format!("{session_id}:{sequence}"));
    persist_finalized_message(
        node,
        DesiredMessageFact {
            message_key: &message_key,
            session_id,
            agent_did,
            requester_did,
            request_id,
            request_doc_id: None,
            sequence,
            role,
            content,
            reasoning: reasoning.unwrap_or(""),
        },
    )
    .await
}

pub(crate) async fn mark_response_materialized(
    node: &EmbeddedNode,
    request_id: &str,
    sequence: u32,
) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    let escaped_request_id = escape_graphql_string(request_id);
    let mutation = format!(
        r#"mutation {{
            update_AgentResponse(
                filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                input: {{
                    materialized_message_sequence: {sequence},
                    materialized_at: "{now}"
                }}
            ) {{ _docID }}
        }}"#
    );

    super::retry::execute_mutation_with_retry(node, &mutation, "mark_response_materialized")
        .await?;
    Ok(())
}
