use crate::llm::message::{
    AssistantContent as AssistantMessageContent, Message as CompletionMessage,
    Reasoning as AssistantReasoning, Text as CompletionText, ToolCall as AssistantToolCall,
};
use anyhow::Result;
use rig::agent::MultiTurnStreamItem;
use rig::streaming::{StreamedAssistantContent, StreamedUserContent};

use crate::agent::loop_stream::LoopStreamItem;
use crate::hook::DefraSessionHook;
use crate::lifecycle::RequestLifecycle;
use crate::streaming::{DefraStreamWriter, StreamWriter};

pub(super) enum StreamAction {
    Continue,
    Done,
    Error(rig::agent::StreamingError),
}

pub(super) struct StreamProcessor<'a> {
    persistence_hook: &'a DefraSessionHook,
    stream_writer: &'a DefraStreamWriter,
    lifecycle: &'a mut RequestLifecycle,
    assistant_turn: AssistantTurnAccumulator,
    pub(super) streamed_text: String,
    committed_text_len: usize,
    pub(super) final_text: Option<String>,
    doc_id: &'a str,
}

#[cfg(test)]
mod tests;

impl<'a> StreamProcessor<'a> {
    pub(super) fn new(
        persistence_hook: &'a DefraSessionHook,
        stream_writer: &'a DefraStreamWriter,
        lifecycle: &'a mut RequestLifecycle,
        doc_id: &'a str,
    ) -> Self {
        Self {
            persistence_hook,
            stream_writer,
            lifecycle,
            assistant_turn: AssistantTurnAccumulator::default(),
            streamed_text: String::new(),
            committed_text_len: 0,
            final_text: None,
            doc_id,
        }
    }

    pub(super) async fn process_item<R>(
        &mut self,
        item: Result<LoopStreamItem<R>, rig::agent::StreamingError>,
    ) -> Result<StreamAction> {
        match item {
            Ok(LoopStreamItem::Item(MultiTurnStreamItem::StreamAssistantItem(
                StreamedAssistantContent::Text(text),
            ))) => {
                let had_visible_text = !self.streamed_text.trim().is_empty();
                self.assistant_turn.push_text(&text.text);
                self.streamed_text.push_str(&text.text);
                let _ = self
                    .stream_writer
                    .write_tokens(self.doc_id, &text.text)
                    .await?;
                let has_visible_text = !self.streamed_text.trim().is_empty();
                if !had_visible_text && has_visible_text {
                    let _ = self.stream_writer.flush_pending(self.doc_id).await?;
                    self.lifecycle.advance().await?;
                }
                Ok(StreamAction::Continue)
            }
            Ok(LoopStreamItem::Item(MultiTurnStreamItem::StreamAssistantItem(
                StreamedAssistantContent::Reasoning(reasoning),
            ))) => {
                let reasoning = crate::llm::rig_compat::from_rig_reasoning(&reasoning);
                let rendered = render_reasoning_text(&reasoning);
                self.assistant_turn.push_reasoning(reasoning);
                if !rendered.is_empty() {
                    let _ = self
                        .stream_writer
                        .write_reasoning(self.doc_id, &rendered)
                        .await?;
                }
                Ok(StreamAction::Continue)
            }
            Ok(LoopStreamItem::Item(MultiTurnStreamItem::StreamAssistantItem(
                StreamedAssistantContent::ReasoningDelta { reasoning, id },
            ))) => {
                self.assistant_turn.push_reasoning_delta(id, &reasoning);
                if !reasoning.is_empty() {
                    let _ = self
                        .stream_writer
                        .write_reasoning(self.doc_id, &reasoning)
                        .await?;
                }
                Ok(StreamAction::Continue)
            }
            Ok(LoopStreamItem::Item(MultiTurnStreamItem::StreamAssistantItem(
                StreamedAssistantContent::ToolCall {
                    tool_call,
                    internal_call_id,
                },
            ))) => {
                let _ = self.stream_writer.flush_pending(self.doc_id).await?;
                self.lifecycle.advance().await?;
                self.persistence_hook
                    .register_stream_tool_call_identity(
                        &internal_call_id,
                        &tool_call.id,
                        tool_call.call_id.as_deref(),
                    )
                    .await;
                self.assistant_turn
                    .push_tool_call(crate::llm::rig_compat::from_rig_tool_call(&tool_call));
                if let Some(message) = self.assistant_turn.message_snapshot() {
                    self.persistence_hook.apply_persistence_policy(
                        self.persistence_hook
                            .persist_inflight_assistant_turn(&message)
                            .await
                            .map(|_| ()),
                        "persist in-flight assistant tool-call turn",
                    )?;
                }
                Ok(StreamAction::Continue)
            }
            Ok(LoopStreamItem::Item(MultiTurnStreamItem::StreamUserItem(
                StreamedUserContent::ToolResult {
                    tool_result,
                    internal_call_id,
                },
            ))) => {
                let _ = self.stream_writer.flush_pending(self.doc_id).await?;
                self.lifecycle.advance().await?;
                if let Some(message) = self.assistant_turn.take_message() {
                    self.persistence_hook.apply_persistence_policy(
                        self.persistence_hook
                            .persist_message(&message)
                            .await
                            .map(|_| ()),
                        "persist streamed assistant turn",
                    )?;
                }
                self.committed_text_len = self.streamed_text.len();
                self.persistence_hook.apply_persistence_policy(
                    self.persistence_hook
                        .persist_stream_tool_result_message(
                            &crate::llm::rig_compat::from_rig_tool_result(&tool_result),
                            &internal_call_id,
                        )
                        .await,
                    "persist streamed tool result",
                )?;
                self.stream_writer.reset_tail(self.doc_id).await?;
                Ok(StreamAction::Continue)
            }
            Ok(LoopStreamItem::Item(MultiTurnStreamItem::FinalResponse(response))) => {
                self.assistant_turn.reconcile_text(response.response());
                let _ = self.stream_writer.flush_pending(self.doc_id).await?;
                self.lifecycle.advance().await?;
                if let Some(message) = self.assistant_turn.take_message() {
                    let fact_ref = self
                        .persistence_hook
                        .persist_message_with_fact_ref(&message)
                        .await?;
                    self.stream_writer
                        .bind_final_message_fact(self.doc_id, &fact_ref)
                        .await?;
                    self.committed_text_len = self.streamed_text.len();
                    self.stream_writer.reset_tail(self.doc_id).await?;
                }
                self.final_text = Some(response.response().to_string());
                Ok(StreamAction::Done)
            }
            Ok(LoopStreamItem::TurnRetracted { .. }) => {
                self.assistant_turn = AssistantTurnAccumulator::default();
                self.streamed_text.truncate(self.committed_text_len);
                self.stream_writer.reset_tail(self.doc_id).await?;
                Ok(StreamAction::Continue)
            }
            Ok(LoopStreamItem::Item(_)) | Ok(LoopStreamItem::AttemptFailed { .. }) => {
                Ok(StreamAction::Continue)
            }
            Err(error) => Ok(StreamAction::Error(error)),
        }
    }

    #[cfg(test)]
    pub(super) fn has_observable_activity(&self) -> bool {
        self.assistant_turn.has_content()
            || !self.streamed_text.trim().is_empty()
            || self
                .final_text
                .as_deref()
                .is_some_and(|text| !text.trim().is_empty())
    }

    pub(super) async fn persist_partial_turn(&mut self, context: &str) -> Result<bool> {
        let Some(message) = self.assistant_turn.take_message() else {
            return Ok(false);
        };

        let fact_ref = match self
            .persistence_hook
            .persist_message_with_fact_ref(&message)
            .await
        {
            Ok(fact_ref) => Some(fact_ref),
            Err(error) => {
                self.persistence_hook
                    .apply_persistence_policy(Err(error), context)?;
                None
            }
        };
        if let Some(fact_ref) = fact_ref {
            self.stream_writer
                .bind_final_message_fact(self.doc_id, &fact_ref)
                .await?;
        }
        self.stream_writer.reset_tail(self.doc_id).await?;

        Ok(true)
    }
}

#[derive(Clone, Default)]
pub(crate) struct AssistantTurnAccumulator {
    text: String,
    reasoning: Vec<AssistantReasoning>,
    pending_reasoning_delta_text: String,
    pending_reasoning_delta_id: Option<String>,
    tool_calls: Vec<AssistantToolCall>,
}

impl AssistantTurnAccumulator {
    pub(crate) fn push_text(&mut self, text: &str) {
        self.text.push_str(text);
    }

    pub(crate) fn push_reasoning(&mut self, reasoning: AssistantReasoning) {
        merge_reasoning_blocks(&mut self.reasoning, &reasoning);
    }

    pub(crate) fn push_reasoning_delta(&mut self, id: Option<String>, reasoning: &str) {
        self.pending_reasoning_delta_text.push_str(reasoning);
        if self.pending_reasoning_delta_id.is_none() {
            self.pending_reasoning_delta_id = id;
        }
    }

    pub(crate) fn push_tool_call(&mut self, tool_call: AssistantToolCall) {
        self.tool_calls.push(tool_call);
    }

    pub(crate) fn take_message(&mut self) -> Option<CompletionMessage> {
        self.build_message()
    }

    fn message_snapshot(&self) -> Option<CompletionMessage> {
        self.clone().build_message()
    }

    pub(crate) fn reconcile_text(&mut self, final_text: &str) {
        if final_text.is_empty() {
            return;
        }
        if self.text.is_empty() {
            self.text.push_str(final_text);
        } else if let Some(remainder) = final_text.strip_prefix(&self.text) {
            self.text.push_str(remainder);
        }
    }

    fn build_message(&mut self) -> Option<CompletionMessage> {
        if self.reasoning.is_empty() && !self.pending_reasoning_delta_text.is_empty() {
            let mut assembled =
                AssistantReasoning::new(&std::mem::take(&mut self.pending_reasoning_delta_text));
            if let Some(id) = self.pending_reasoning_delta_id.take() {
                assembled = assembled.with_id(id);
            }
            self.push_reasoning(assembled);
        }

        let mut content = Vec::new();
        if !self.text.is_empty() {
            content.push(AssistantMessageContent::Text(CompletionText {
                text: std::mem::take(&mut self.text),
            }));
        }
        content.extend(
            self.reasoning
                .drain(..)
                .map(AssistantMessageContent::Reasoning),
        );
        content.extend(
            self.tool_calls
                .drain(..)
                .map(AssistantMessageContent::ToolCall),
        );

        self.pending_reasoning_delta_text.clear();
        self.pending_reasoning_delta_id = None;

        (!content.is_empty()).then_some(CompletionMessage::Assistant { id: None, content })
    }

    #[cfg(test)]
    fn has_content(&self) -> bool {
        !self.text.is_empty()
            || !self.reasoning.is_empty()
            || !self.pending_reasoning_delta_text.is_empty()
            || !self.tool_calls.is_empty()
    }
}

fn merge_reasoning_blocks(
    accumulated_reasoning: &mut Vec<AssistantReasoning>,
    incoming: &AssistantReasoning,
) {
    let ids_match = |existing: &AssistantReasoning| {
        matches!(
            (&existing.id, &incoming.id),
            (Some(existing_id), Some(incoming_id)) if existing_id == incoming_id
        )
    };

    if let Some(existing) = accumulated_reasoning
        .iter_mut()
        .rev()
        .find(|existing| ids_match(existing))
    {
        existing.content.extend(incoming.content.clone());
    } else {
        accumulated_reasoning.push(incoming.clone());
    }
}

fn render_reasoning_text(reasoning: &AssistantReasoning) -> String {
    use crate::llm::message::ReasoningContent;

    let mut rendered = String::new();
    for part in &reasoning.content {
        let piece = match part {
            ReasoningContent::Text { text, .. } | ReasoningContent::Summary(text) => text.as_str(),
            ReasoningContent::Encrypted(_) => "[encrypted reasoning]",
            ReasoningContent::Redacted { .. } => "[redacted reasoning]",
        };

        if piece.is_empty() {
            continue;
        }
        if !rendered.is_empty() {
            rendered.push('\n');
        }
        rendered.push_str(piece);
    }

    rendered
}
