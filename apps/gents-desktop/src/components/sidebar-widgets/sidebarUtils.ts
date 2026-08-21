import type { ConversationSummary } from "@source-inc/gents-desktop-client";

export type ConversationLifecycleGroup = "attention" | "active" | "recent";

export function conversationLifecycleGroup(
  conversation: ConversationSummary,
): ConversationLifecycleGroup {
  const state = (conversation.turnState ?? conversation.status ?? "").toLowerCase();
  if (
    [
      "failed",
      "error",
      "cancelled",
      "dead",
      "inputrequired",
      "input_required",
      "awaitingapproval",
      "awaiting_approval",
    ].includes(state)
  ) {
    return "attention";
  }
  if (state && !["completed", "superseded", "interrupted", "idle"].includes(state)) {
    return "active";
  }
  return "recent";
}

export function conversationStatusClass(conversation: ConversationSummary) {
  const group = conversationLifecycleGroup(conversation);
  if (group === "attention") {
    return "conversation-status-dot conversation-status-dot-error";
  }
  return group === "active"
    ? "conversation-status-dot conversation-status-dot-running"
    : null;
}
