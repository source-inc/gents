use super::query::load_conversation_document;
use super::query::load_recent_conversation_titles;
use super::retry::execute_mutation_with_retry;
use super::rows::ConversationDocument;
use super::*;

pub(crate) const CONVERSATION_TITLE_SOURCE_FALLBACK: &str = "placeholder";
pub(crate) const CONVERSATION_TITLE_SOURCE_GENERATED: &str = "generated";
pub(crate) const CONVERSATION_TITLE_SOURCE_TASK: &str = "task";

struct ConversationFields<'a> {
    session_id: &'a str,
    agent_name: &'a str,
    agent_did: &'a str,
    requester_did: Option<&'a str>,
    behavior_id: &'a str,
    title: &'a str,
    title_source: &'a str,
    preview_text: &'a str,
    status: &'a str,
    created_at: &'a str,
    updated_at: &'a str,
    latest_request_id: &'a str,
}

impl ConversationFields<'_> {
    fn mutable_input(&self) -> String {
        format!(
            r#"agent_name: "{agent_name}",
                    behavior_id: "{behavior_id}",
                    title: "{title}",
                    title_source: "{title_source}",
                    preview_text: "{preview_text}",
                    status: "{status}",
                    created_at: "{created_at}",
                    updated_at: "{updated_at}",
                    latest_request_id: "{latest_request_id}""#,
            agent_name = escape_graphql_string(self.agent_name),
            behavior_id = escape_graphql_string(self.behavior_id),
            title = escape_graphql_string(self.title),
            title_source = escape_graphql_string(self.title_source),
            preview_text = escape_graphql_string(self.preview_text),
            status = escape_graphql_string(self.status),
            created_at = escape_graphql_string(self.created_at),
            updated_at = escape_graphql_string(self.updated_at),
            latest_request_id = escape_graphql_string(self.latest_request_id),
        )
    }

    fn create_input(&self) -> String {
        format!(
            r#"session_id: "{session_id}",
                    agent_did: "{agent_did}",
                    {requester_did_field}
                    {mutable}"#,
            session_id = escape_graphql_string(self.session_id),
            agent_did = escape_graphql_string(self.agent_did),
            requester_did_field = super::requester_did_create_field(self.requester_did),
            mutable = self.mutable_input(),
        )
    }
}

/// Write a conversation: update the doc addressed by `doc_id`, or create one
/// when the session has no doc yet.
///
/// **Never upsert by a `session_id` filter (#693).** `session_id` is unique in
/// the current schema, but DefraDB cannot add an index to an already-created
/// collection, so stores whose `AgentConversation` predates the unique index
/// carry duplicate rows permanently (replication can mint them too). A
/// `filter: { session_id }` upsert matches every duplicate and DefraDB refuses
/// it with `cannot upsert multiple matching documents` — which bricked both
/// startup recovery *and* ordinary request handling on the affected hosts.
/// Addressing one `_docID` is what makes the write total on those stores; the
/// Lean sweep contract pins the selector (`targetSelector = "_docID"`).
async fn write_conversation_doc(
    node: &EmbeddedNode,
    doc_id: Option<&str>,
    fields: &ConversationFields<'_>,
    operation: &str,
) -> Result<()> {
    let mutation = match doc_id {
        Some(doc_id) => format!(
            r#"mutation {{
            update_AgentConversation(
                filter: {{ _docID: {{ _eq: "{doc_id}" }} }},
                input: {{
                    {input}
                }}
            ) {{ _docID }}
        }}"#,
            doc_id = escape_graphql_string(doc_id),
            input = fields.mutable_input(),
        ),
        None => format!(
            r#"mutation {{
            create_AgentConversation(
                input: {{
                    {input}
                }}
            ) {{ _docID }}
        }}"#,
            input = fields.create_input(),
        ),
    };

    execute_mutation_with_retry(node, &mutation, operation).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(crate) async fn upsert_conversation_from_request_with_identity(
    node: &EmbeddedNode,
    session_id: &str,
    agent_name: &str,
    agent_did: &str,
    behavior_id: &str,
    request_id: &str,
    content: &str,
    status: &str,
) -> Result<()> {
    upsert_conversation_from_request_with_identity_and_requester_did(
        node,
        session_id,
        agent_name,
        agent_did,
        behavior_id,
        request_id,
        content,
        status,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn upsert_conversation_from_request_with_identity_and_requester_did(
    node: &EmbeddedNode,
    session_id: &str,
    agent_name: &str,
    agent_did: &str,
    behavior_id: &str,
    request_id: &str,
    content: &str,
    status: &str,
    requester_did: Option<&str>,
) -> Result<()> {
    upsert_conversation_from_request_with_identity_and_title(
        node,
        session_id,
        agent_name,
        agent_did,
        behavior_id,
        request_id,
        content,
        status,
        requester_did,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn upsert_conversation_from_request_with_identity_and_title(
    node: &EmbeddedNode,
    session_id: &str,
    agent_name: &str,
    agent_did: &str,
    behavior_id: &str,
    request_id: &str,
    content: &str,
    status: &str,
    requester_did: Option<&str>,
    title_override: Option<(&str, &str)>,
) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    let preview = derive_conversation_preview(content);
    let existing = load_conversation_document(node, session_id).await?;
    let resolved_behavior_id =
        resolve_behavior_id(existing.as_ref(), behavior_id, "AgentConversation")?;
    let (title, title_source) = existing_title_state(existing.as_ref(), title_override);
    let created_at = existing
        .as_ref()
        .map(|conversation| conversation.created_at.clone())
        .unwrap_or_else(|| now.clone());

    let fields = ConversationFields {
        session_id,
        agent_name,
        agent_did,
        requester_did,
        behavior_id: &resolved_behavior_id,
        title: &title,
        title_source: &title_source,
        preview_text: &preview,
        status,
        created_at: &created_at,
        updated_at: &now,
        latest_request_id: request_id,
    };

    write_conversation_doc(
        node,
        existing
            .as_ref()
            .map(|conversation| conversation.doc_id.as_str()),
        &fields,
        "upsert_conversation_from_request",
    )
    .await
}

pub(crate) async fn update_conversation_status_if_latest_with_identity(
    node: &EmbeddedNode,
    session_id: &str,
    agent_name: &str,
    agent_did: &str,
    behavior_id: &str,
    latest_request_id: &str,
    status: &str,
) -> Result<ConversationUpdateOutcome> {
    let now = chrono::Utc::now().to_rfc3339();
    let escaped_agent_name = escape_graphql_string(agent_name);
    // `agent_did` is intentionally not interpolated: it is the immutable scope
    // key and must never appear in an update mutation. The parameter is retained
    // for signature symmetry with the create-path identity helpers.
    let _ = agent_did;
    let escaped_latest_request_id = escape_graphql_string(latest_request_id);
    let escaped_status = escape_graphql_string(status);
    let existing = load_conversation_document(node, session_id).await?;

    let Some(existing) = existing else {
        return Ok(ConversationUpdateOutcome::SkippedStaleRequest);
    };
    let resolved_behavior_id =
        resolve_behavior_id(Some(&existing), behavior_id, "AgentConversation")?;

    if existing.latest_request_id != latest_request_id {
        return Ok(ConversationUpdateOutcome::SkippedStaleRequest);
    }

    if existing.status == status {
        return Ok(ConversationUpdateOutcome::AlreadyApplied);
    }

    let escaped_title = escape_graphql_string(&existing.title);
    let escaped_title_source = escape_graphql_string(
        existing
            .title_source
            .as_deref()
            .unwrap_or(CONVERSATION_TITLE_SOURCE_FALLBACK),
    );
    let escaped_preview_text = escape_graphql_string(&existing.preview_text);
    let escaped_created_at = escape_graphql_string(&existing.created_at);
    let escaped_behavior_id = escape_graphql_string(&resolved_behavior_id);
    let escaped_doc_id = escape_graphql_string(&existing.doc_id);
    let mutation = format!(
        r#"mutation {{
            update_AgentConversation(
                filter: {{ _docID: {{ _eq: "{escaped_doc_id}" }} }},
                input: {{
                    agent_name: "{escaped_agent_name}",
                    behavior_id: "{escaped_behavior_id}",
                    title: "{escaped_title}",
                    title_source: "{escaped_title_source}",
                    preview_text: "{escaped_preview_text}",
                    status: "{escaped_status}",
                    created_at: "{escaped_created_at}",
                    updated_at: "{now}",
                    latest_request_id: "{escaped_latest_request_id}"
                }}
            ) {{ _docID }}
        }}"#
    );

    let resp = node.execute(&mutation).await;
    if resp.has_errors() {
        anyhow::bail!(
            "update_conversation_status_if_latest mutation failed for session_id={session_id}: {:?}",
            resp.errors
        );
    }

    if resp
        .data
        .as_ref()
        .and_then(|data| data.get("update_AgentConversation"))
        .is_some_and(response_has_documents)
    {
        return Ok(ConversationUpdateOutcome::Updated);
    }

    match load_conversation_document(node, session_id).await? {
        Some(latest) if latest.latest_request_id != latest_request_id => {
            Ok(ConversationUpdateOutcome::SkippedStaleRequest)
        }
        Some(latest) if latest.status == status => Ok(ConversationUpdateOutcome::AlreadyApplied),
        Some(latest) => anyhow::bail!(
            "conversation session_id={session_id} stayed at status={} for latest_request_id={}",
            latest.status,
            latest.latest_request_id
        ),
        None => anyhow::bail!(
            "conversation disappeared while updating session_id={session_id} latest_request_id={latest_request_id}"
        ),
    }
}

pub(crate) async fn update_conversation_title_with_source(
    node: &EmbeddedNode,
    session_id: &str,
    title: &str,
    title_source: &str,
) -> Result<()> {
    let Some(existing) = load_conversation_document(node, session_id).await? else {
        return Ok(());
    };

    let now = chrono::Utc::now().to_rfc3339();
    let escaped_title = escape_graphql_string(title);
    let escaped_title_source = escape_graphql_string(title_source);
    let escaped_preview_text = escape_graphql_string(&existing.preview_text);
    let escaped_status = escape_graphql_string(&existing.status);
    let escaped_latest_request_id = escape_graphql_string(&existing.latest_request_id);
    let escaped_created_at = escape_graphql_string(&existing.created_at);
    let escaped_behavior_id =
        escape_graphql_string(existing.behavior_id.as_deref().unwrap_or_default());
    let escaped_doc_id = escape_graphql_string(&existing.doc_id);

    let mutation = format!(
        r#"mutation {{
            update_AgentConversation(
                filter: {{ _docID: {{ _eq: "{escaped_doc_id}" }} }},
                input: {{
                    behavior_id: "{escaped_behavior_id}",
                    title: "{escaped_title}",
                    title_source: "{escaped_title_source}",
                    preview_text: "{escaped_preview_text}",
                    status: "{escaped_status}",
                    created_at: "{escaped_created_at}",
                    updated_at: "{now}",
                    latest_request_id: "{escaped_latest_request_id}"
                }}
            ) {{ _docID }}
        }}"#
    );

    execute_mutation_with_retry(node, &mutation, "update_conversation_title_with_source").await?;
    Ok(())
}

pub(crate) async fn load_recent_titles_for_agent(
    node: &EmbeddedNode,
    agent_did: &str,
    exclude_session_id: &str,
    limit: usize,
) -> Result<Vec<String>> {
    load_recent_conversation_titles(node, agent_did, exclude_session_id, limit).await
}

pub(crate) async fn conversation_needs_generated_title(
    node: &EmbeddedNode,
    session_id: &str,
) -> Result<bool> {
    let Some(existing) = load_conversation_document(node, session_id).await? else {
        return Ok(false);
    };

    let title = existing.title.trim();
    let title_source = normalize_optional_string(existing.title_source.as_deref())
        .unwrap_or(CONVERSATION_TITLE_SOURCE_FALLBACK);

    Ok(title.is_empty() || title_source == CONVERSATION_TITLE_SOURCE_FALLBACK)
}

fn resolve_behavior_id(
    existing: Option<&ConversationDocument>,
    requested_behavior_id: &str,
    collection_name: &str,
) -> Result<String> {
    let existing_behavior_id = existing
        .and_then(|conversation| normalize_optional_string(conversation.behavior_id.as_deref()));
    let requested_behavior_id = normalize_optional_string(Some(requested_behavior_id));

    match (existing_behavior_id, requested_behavior_id) {
        (Some(existing), Some(requested)) if existing != requested => anyhow::bail!(
            "{collection_name} session behavior mismatch: existing={existing} requested={requested}"
        ),
        (Some(existing), _) => Ok::<String, anyhow::Error>(existing.to_string()),
        (None, Some(requested)) => Ok::<String, anyhow::Error>(requested.to_string()),
        (None, None) => Ok(String::new()),
    }
}

fn normalize_optional_string(value: Option<&str>) -> Option<&str> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then_some(trimmed)
    })
}

fn existing_title_state(
    existing: Option<&ConversationDocument>,
    title_override: Option<(&str, &str)>,
) -> (String, String) {
    let normalized_override = title_override.and_then(|(title, source)| {
        let title = title.trim();
        let source = source.trim();
        (!title.is_empty() && !source.is_empty()).then(|| (title.to_string(), source.to_string()))
    });

    match existing {
        Some(existing) => {
            let existing_title = existing.title.trim();
            let existing_source = normalize_optional_string(existing.title_source.as_deref())
                .unwrap_or(CONVERSATION_TITLE_SOURCE_FALLBACK);
            if existing_title.is_empty() || existing_source == CONVERSATION_TITLE_SOURCE_FALLBACK {
                if let Some((title, source)) = normalized_override.as_ref() {
                    return (title.clone(), source.clone());
                }
            }

            (existing.title.clone(), existing_source.to_string())
        }
        None => normalized_override.unwrap_or_else(|| {
            (
                String::new(),
                CONVERSATION_TITLE_SOURCE_FALLBACK.to_string(),
            )
        }),
    }
}

fn derive_conversation_preview(content: &str) -> String {
    truncate_chars(&normalize_conversation_text(content), 240)
}

fn normalize_conversation_text(content: &str) -> String {
    content.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}
