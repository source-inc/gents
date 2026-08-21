import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import { MessageList } from "@source-inc/gents-desktop-chat";
import type { RenderedTimelineItem } from "@source-inc/gents-desktop-client";
import type { DerivedCancelCauseView } from "@source-inc/gents-desktop-client";

describe("Transcript cancel cause surfacing", () => {
  it("renders CancelCauseBadge on cancelled tool calls", () => {
    const items: RenderedTimelineItem[] = [
      {
        kind: "toolGroup",
        itemKey: "tools-1",
        messageSequence: 1,
        tools: [
          {
            itemKey: "tool-1",
            toolName: "background_tool",
            statusKind: "error",
            status: "cancelled",
            presentation: { kind: "generic", summary: null, input: null, output: null },
            cancelCause: {
              cause: "userCancelled",
              source: "requestInterrupt",
              confidence: "direct",
              at: "2026-05-20T10:32:14Z",
              evidence: ["AgentRequest.interrupt_requested_at = 2026-05-20T10:32:14Z"],
            },
          },
        ],
      },
    ];
    render(<MessageList timelineItems={items} />);
    expect(screen.getByText(/user cancelled/i)).toBeInTheDocument();
    // Open the disclosure to verify CancelCauseDetails is mounted inside the body.
    // Note: the badge is in summary which is always visible; details are in the body
    // and only rendered when <details> is open. Native <details> defaults to closed,
    // so details content is still in the DOM in jsdom — assert against it directly.
    expect(screen.getByText(/AgentRequest.interrupt_requested_at/)).toBeInTheDocument();
  });

  it("does not render a badge when cancelCause is missing", () => {
    const items: RenderedTimelineItem[] = [
      {
        kind: "toolGroup",
        itemKey: "tools-2",
        messageSequence: 2,
        tools: [
          {
            itemKey: "tool-2",
            toolName: "read_file",
            statusKind: "success",
            status: "completed",
            presentation: {
              kind: "fileRead",
              operation: "read_file",
              target: null,
              returnedCount: null,
              totalCount: null,
              truncated: false,
              body: "",
              fallbackOutput: null,
            },
          },
        ],
      },
    ];
    render(<MessageList timelineItems={items} />);
    expect(screen.queryByText(/user cancelled/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/cause unknown/i)).not.toBeInTheDocument();
  });

  it("renders different cause variants with their own class", () => {
    const items: RenderedTimelineItem[] = [
      {
        kind: "toolGroup",
        itemKey: "tools-3",
        messageSequence: 3,
        tools: [
          {
            itemKey: "tool-3",
            toolName: "index_repo",
            statusKind: "error",
            status: "cancelled",
            presentation: { kind: "generic", summary: null, input: null, output: null },
            cancelCause: {
              cause: "deadline",
              source: "toolLifecycle",
              confidence: "derived",
              at: "2026-05-20T10:35:02Z",
              evidence: ['AgentToolCall.lifecycle_state = "timedOut"'],
            },
          },
        ],
      },
    ];
    render(<MessageList timelineItems={items} />);
    const badge = screen.getByText(/deadline expired/i);
    expect(badge).toHaveClass("cause-deadline");
  });
});

describe("Transcript assistant-turn cancel cause", () => {
  const interruptedCause: DerivedCancelCauseView = {
    cause: "interrupted",
    source: "responseInterruptedAt",
    confidence: "direct",
    at: "2026-05-20T10:36:11Z",
    evidence: ["AgentResponse.interrupted_at = 2026-05-20T10:36:11Z"],
  };

  it("renders badge on the assistantMessage whose sequence matches latestResponse.materializedMessageSequence", () => {
    const items: RenderedTimelineItem[] = [
      {
        kind: "userMessage",
        itemKey: "u-1",
        sequence: 1,
        content: "tell me about X",
      },
      {
        kind: "assistantMessage",
        itemKey: "a-1",
        sequence: 2,
        content: "partial response",
        reasoning: null,
      },
    ];
    render(
      <MessageList
        timelineItems={items}
        responseCancelCause={interruptedCause}
        responseMaterializedSequence={2}
      />,
    );
    expect(
      screen.getByText(/interrupted/i, { selector: ".cause-badge" }),
    ).toBeInTheDocument();
  });

  it("does NOT render the badge on older assistant messages whose sequence does not match", () => {
    const items: RenderedTimelineItem[] = [
      {
        kind: "assistantMessage",
        itemKey: "a-old",
        sequence: 2,
        content: "older completed turn",
        reasoning: null,
      },
      {
        kind: "assistantMessage",
        itemKey: "a-new",
        sequence: 4,
        content: "most recent turn",
        reasoning: null,
      },
    ];
    render(
      <MessageList
        timelineItems={items}
        responseCancelCause={interruptedCause}
        responseMaterializedSequence={4}
      />,
    );
    expect(screen.getAllByText(/interrupted/i)).toHaveLength(1);
  });

  it("renders badge on the liveAssistant overlay when responseCancelCause is present", () => {
    const items: RenderedTimelineItem[] = [
      {
        kind: "liveAssistant",
        itemKey: "live",
        content: "streaming…",
        reasoning: null,
      },
    ];
    render(
      <MessageList timelineItems={items} responseCancelCause={interruptedCause} />,
    );
    expect(
      screen.getByText(/interrupted/i, { selector: ".cause-badge" }),
    ).toBeInTheDocument();
  });

  it("does NOT render badge when responseCancelCause is null", () => {
    const items: RenderedTimelineItem[] = [
      {
        kind: "assistantMessage",
        itemKey: "a-1",
        sequence: 2,
        content: "normal turn",
        reasoning: null,
      },
    ];
    render(
      <MessageList
        timelineItems={items}
        responseCancelCause={null}
        responseMaterializedSequence={2}
      />,
    );
    expect(screen.queryByText(/interrupted/i)).not.toBeInTheDocument();
  });

  it("renders a standalone badge when an interrupted response has no assistant message", () => {
    const items: RenderedTimelineItem[] = [
      {
        kind: "userMessage",
        itemKey: "u-1",
        sequence: 2,
        content: "user prompt",
      },
    ];
    render(
      <MessageList
        timelineItems={items}
        responseCancelCause={interruptedCause}
        responseMaterializedSequence={2}
      />,
    );
    expect(
      screen.getByText(/interrupted/i, { selector: ".cause-badge" }),
    ).toBeInTheDocument();
    expect(screen.getByText(/responseInterruptedAt/i)).toBeInTheDocument();
  });
});
