use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use defra_node::EmbeddedNode;

use super::BehaviorDaemon;
use crate::admission::{self, AdmissionCallContext, CallKind};
use crate::session;
use crate::watcher::AgentRequest;

const RECENT_TITLE_LIMIT: usize = 5;
const GENERATED_TITLE_MAX_WORDS: usize = 5;
const GENERATED_TITLE_MAX_LEN: usize = 48;
const TITLE_GENERATION_MAX_ATTEMPTS: i64 = 2;
const TITLE_GENERATION_TIMEOUT_SECS: u64 = 10;
const TITLE_GENERATION_PREAMBLE: &str =
    "Generate concise conversation titles. Return only a lowercase hyphenated 3-5 word title. Never call tools. Never explain.";

impl<M: rig::completion::CompletionModel + 'static> BehaviorDaemon<M> {
    pub(super) fn spawn_conversation_title_generation(
        &self,
        request: &AgentRequest,
        capture_context: crate::rendered_request::RenderedRequestContext,
        admission_context: AdmissionCallContext,
    ) {
        let node = Arc::clone(&self.node);
        let behavior_did = self.behavior.agent_did().to_string();
        let request = request.clone();
        let model = Arc::clone(&self.model);
        let mut title_config = crate::completion_factory::loop_config(
            self.behavior.as_ref(),
            title_generation_preamble(),
            0,
            crate::rendered_request::CaptureScopeKind::Title,
        );
        title_config.temperature = Some(0.0);
        title_config.max_tokens = Some(24);
        title_config.max_turns = 1;
        // Title generation already retries at its own layer
        // (generate_title_with_fallback); the inner completion must not also
        // inherit the parent's retry ladder (#648).
        title_config.retry_policy =
            crate::agent::completion_retry::CompletionRetryPolicy::no_retry();

        // Title generation runs on its own task, and task-locals are not
        // inherited by `tokio::spawn`. The capture scope therefore has to be
        // installed here, or `title_config`'s arming sink would find no ambient
        // scope and this provider call would be the one that stays uncaptured.
        let capture_factory = self.rendered_request_capture_factory.clone();

        tokio::spawn(async move {
            if let Err(error) = admission::scope_request(admission_context, async move {
                crate::rendered_request::scope::scope_request_if_configured(
                    capture_context,
                    capture_factory.as_ref(),
                    maybe_generate_conversation_title(
                        node,
                        &behavior_did,
                        request,
                        model,
                        title_config,
                    ),
                )
                .await
            })
            .await
            {
                tracing::warn!(
                    error = %error,
                    "failed to generate conversation title"
                );
            }
        });
    }
}

async fn maybe_generate_conversation_title<M: rig::completion::CompletionModel + 'static>(
    node: Arc<EmbeddedNode>,
    behavior_did: &str,
    request: AgentRequest,
    model: Arc<M>,
    config: crate::agent::loop_stream::LoopConfig,
) -> Result<()> {
    if !session::conversation_needs_generated_title(&node, &request.session_id).await? {
        return Ok(());
    }

    let recent_titles = session::load_recent_titles_for_agent(
        &node,
        behavior_did,
        &request.session_id,
        RECENT_TITLE_LIMIT,
    )
    .await
    .unwrap_or_default();

    let prompt = title_generation_prompt(&request.content, &recent_titles);
    let title = generate_title_with_fallback(&request, model, config, prompt).await;

    session::update_conversation_title_with_source(
        &node,
        &request.session_id,
        &title,
        session::CONVERSATION_TITLE_SOURCE_GENERATED,
    )
    .await?;

    tracing::info!(
        session_id = %request.session_id,
        request_id = %request.request_id,
        title = %title,
        "generated conversation title"
    );
    Ok(())
}

async fn generate_title_with_fallback<M: rig::completion::CompletionModel + 'static>(
    request: &AgentRequest,
    model: Arc<M>,
    config: crate::agent::loop_stream::LoopConfig,
    prompt: String,
) -> String {
    let mut last_error = None;

    for attempt in 1..=TITLE_GENERATION_MAX_ATTEMPTS {
        let prompt = prompt.clone();
        let model = (*model).clone();
        let loop_config = config.clone();
        match admission::scope_call(CallKind::OneOff, attempt, async move {
            tokio::time::timeout(
                Duration::from_secs(TITLE_GENERATION_TIMEOUT_SECS),
                crate::agent::loop_stream::run_loop_to_text(
                    model,
                    None,
                    crate::llm::message::Message::user(prompt),
                    Vec::new(),
                    std::sync::Arc::new(Vec::new()),
                    loop_config,
                ),
            )
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "conversation title inference timed out after {}s",
                    TITLE_GENERATION_TIMEOUT_SECS
                )
            })?
            .map_err(|error| anyhow::anyhow!("conversation title inference failed: {error}"))
        })
        .await
        {
            Ok(raw_title) => return sanitize_generated_title(&raw_title, &request.content),
            Err(error) => {
                tracing::warn!(
                    request_id = %request.request_id,
                    session_id = %request.session_id,
                    attempt,
                    error = %error,
                    "conversation title inference failed"
                );
                last_error = Some(error);
            }
        }
    }

    let fallback = sanitize_generated_title("", &request.content);
    tracing::info!(
        request_id = %request.request_id,
        session_id = %request.session_id,
        title = %fallback,
        error = ?last_error.as_ref().map(|error| error.to_string()),
        "using fallback conversation title after oneoff inference failure"
    );
    fallback
}

fn title_generation_preamble() -> String {
    TITLE_GENERATION_PREAMBLE.to_string()
}

fn title_generation_prompt(request_content: &str, recent_titles: &[String]) -> String {
    let request_excerpt = truncate_for_title_prompt(request_content, 600);
    let recent = if recent_titles.is_empty() {
        "none".to_string()
    } else {
        recent_titles
            .iter()
            .take(RECENT_TITLE_LIMIT)
            .map(|title| format!("- {title}"))
            .collect::<Vec<_>>()
            .join("\n")
    };

    format!(
        "Generate a concise session title for this conversation.\n\
Return only the title.\n\
Constraints:\n\
- 3-5 words\n\
- lowercase\n\
- hyphenated\n\
- no punctuation except hyphens\n\
- no quotes\n\
- avoid repeating a recent title exactly\n\n\
Recent session titles:\n{recent}\n\n\
First user request:\n{request_excerpt}"
    )
}

fn truncate_for_title_prompt(value: &str, max_chars: usize) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    normalized.chars().take(max_chars).collect()
}

fn sanitize_generated_title(raw_title: &str, fallback_source: &str) -> String {
    let first_line = raw_title
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or_default()
        .trim()
        .trim_matches('`')
        .trim_matches('"')
        .trim_matches('\'')
        .to_ascii_lowercase();

    let mut words = first_line
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '-' || c == '_'))
        .flat_map(|part| part.split(['-', '_']))
        .map(str::trim)
        .filter(|word| !word.is_empty())
        .take(GENERATED_TITLE_MAX_WORDS)
        .map(str::to_string)
        .collect::<Vec<_>>();

    if words.is_empty() {
        words = fallback_words(fallback_source);
    }

    let mut title = words.join("-");
    if title.len() > GENERATED_TITLE_MAX_LEN {
        title.truncate(GENERATED_TITLE_MAX_LEN);
        title = title.trim_matches('-').to_string();
    }

    if title.is_empty() {
        "conversation".to_string()
    } else {
        title
    }
}

fn fallback_words(source: &str) -> Vec<String> {
    const STOPWORDS: &[&str] = &[
        "a", "about", "agent", "amy", "an", "and", "are", "by", "can", "desktop", "do", "for",
        "give", "hello", "help", "hey", "how", "i", "in", "is", "it", "its", "me", "model", "of",
        "on", "please", "s", "tell", "that", "thats", "the", "think", "this", "to", "used", "via",
        "what", "with", "works", "you",
    ];

    let mut words = source
        .to_ascii_lowercase()
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
        .filter(|word| !STOPWORDS.contains(word))
        .take(GENERATED_TITLE_MAX_WORDS)
        .map(str::to_string)
        .collect::<Vec<_>>();

    if words.is_empty() {
        words.push("conversation".to_string());
    }

    words
}

#[cfg(test)]
mod tests {
    use super::sanitize_generated_title;

    #[test]
    fn sanitize_generated_title_normalizes_output() {
        assert_eq!(
            sanitize_generated_title("\"Agent Desktop Debugging Redux\"", "fallback text"),
            "agent-desktop-debugging-redux"
        );
    }

    #[test]
    fn sanitize_generated_title_falls_back_when_empty() {
        assert_eq!(
            sanitize_generated_title("", "please inspect p2p request model"),
            "inspect-p2p-request"
        );
    }

    #[test]
    fn sanitize_generated_title_filters_contraction_noise() {
        assert_eq!(
            sanitize_generated_title("", "document-based request model that's used by this agent"),
            "document-based-request"
        );
    }
}
