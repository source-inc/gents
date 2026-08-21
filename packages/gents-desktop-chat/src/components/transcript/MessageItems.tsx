import type {
  DerivedCancelCauseView,
  RenderedTimelineItem,
} from "@source-inc/gents-desktop-client";
import { CopyButton } from "@source-inc/gents-desktop-ui";
import { requestProgressPresentation } from "../../chat-shell.js";

import { CancelCauseBadge, CancelCauseDetails } from "../cancelUx/index.js";
import {
  MarkdownContent,
  MessageTime,
  ReasoningDisclosure,
  normalizeTranscriptText,
} from "./MarkdownContent.js";

type UserMessage = Extract<RenderedTimelineItem, { kind: "userMessage" }>;
type AssistantMessage = Extract<
  RenderedTimelineItem,
  { kind: "assistantMessage" }
>;
type PendingUserTurn = Extract<
  RenderedTimelineItem,
  { kind: "pendingUserTurn" }
>;
type LiveAssistant = Extract<RenderedTimelineItem, { kind: "liveAssistant" }>;

export function hasVisibleResponseCancelBadgeTarget(
  item: RenderedTimelineItem,
  responseMaterializedSequence?: number | null,
) {
  switch (item.kind) {
    case "assistantMessage":
      return (
        Boolean(
          normalizeTranscriptText(item.content) ||
          normalizeTranscriptText(item.reasoning),
        ) &&
        item.sequence != null &&
        item.sequence === responseMaterializedSequence
      );
    case "liveAssistant":
      return Boolean(
        normalizeTranscriptText(item.content) ||
        normalizeTranscriptText(item.reasoning),
      );
    default:
      return false;
  }
}

export function UserMessageItem({ item }: { item: UserMessage }) {
  const content = normalizeTranscriptText(item.content);
  return (
    <div className="turn-block">
      <article className="message-card user-card">
        <div className="message-role">
          user
          <MessageTime value={item.timestamp} />
          <CopyButton className="message-copy" getText={() => content} />
        </div>
        <div className="message-content">
          <MarkdownContent value={content} />
        </div>
      </article>
    </div>
  );
}

export function AssistantMessageItem({
  item,
  responseCancelCause,
  responseMaterializedSequence,
}: {
  item: AssistantMessage;
  responseCancelCause?: DerivedCancelCauseView | null;
  responseMaterializedSequence?: number | null;
}) {
  const content = normalizeTranscriptText(item.content);
  const reasoning = normalizeTranscriptText(item.reasoning);
  if (!content && !reasoning) {
    return null;
  }
  const showBadge =
    responseCancelCause != null &&
    item.sequence != null &&
    item.sequence === responseMaterializedSequence;
  return (
    <div className="turn-block">
      <article className="message-card" data-testid="assistant-message">
        <div className="message-role">
          assistant
          {showBadge ? (
            <CancelCauseBadge
              cause={responseCancelCause}
              className="assistant-turn-cause-badge"
            />
          ) : null}
          <MessageTime value={item.timestamp} />
          {content ? (
            <CopyButton className="message-copy" getText={() => content} />
          ) : null}
        </div>
        <ReasoningDisclosure value={reasoning} />
        {content ? (
          <div className="message-content">
            <MarkdownContent value={content} />
          </div>
        ) : null}
      </article>
    </div>
  );
}

export function PendingUserTurnItem({ item }: { item: PendingUserTurn }) {
  const progress = requestProgressPresentation(item.lifecycleState);
  return (
    <div className="turn-block">
      <article className="message-card pending-card">
        <div className="message-role">
          user
          {progress ? (
            <span
              className={`request-progress${progress.animated ? " is-active" : ""}`}
              data-testid="request-progress"
            >
              {progress.label}
            </span>
          ) : null}
        </div>
        <div className="message-content">
          <MarkdownContent value={normalizeTranscriptText(item.content)} />
        </div>
      </article>
    </div>
  );
}

export function LiveAssistantItem({
  item,
  responseCancelCause,
}: {
  item: LiveAssistant;
  responseCancelCause?: DerivedCancelCauseView | null;
}) {
  const content = normalizeTranscriptText(item.content);
  const reasoning = normalizeTranscriptText(item.reasoning);
  if (!content && !reasoning) {
    return null;
  }
  return (
    <article className="message-card" data-testid="assistant-message">
      <div className="message-role">
        assistant
        {responseCancelCause != null ? (
          <CancelCauseBadge
            cause={responseCancelCause}
            className="assistant-turn-cause-badge"
          />
        ) : null}
      </div>
      <ReasoningDisclosure value={reasoning} />
      {content ? (
        <div className="message-content">
          <MarkdownContent value={content} />
        </div>
      ) : null}
    </article>
  );
}

export function AssistantCancelCauseTurn({
  cause,
}: {
  cause: DerivedCancelCauseView;
}) {
  return (
    <div className="turn-block">
      <article className="message-card">
        <div className="message-role">
          assistant
          <CancelCauseBadge
            cause={cause}
            className="assistant-turn-cause-badge"
          />
        </div>
        <CancelCauseDetails cause={cause} />
      </article>
    </div>
  );
}
