import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ChatTranscriptPanel } from "@source-inc/gents-desktop-chat";
import { MessageList } from "@source-inc/gents-desktop-chat";
import type {
  DesktopSessionSnapshot,
  RenderedTimelineItem,
} from "@source-inc/gents-desktop-client";

function makeSession(
  overrides: Partial<DesktopSessionSnapshot> = {},
): DesktopSessionSnapshot {
  return {
    sessionId: "s1",
    agentDid: "did:test:operator",
    turnState: "streaming",
    timelineItems: [],
    ...overrides,
  };
}

const pendingTurn: RenderedTimelineItem = {
  kind: "pendingUserTurn",
  itemKey: "p1",
  requestId: "req_1",
  content: "do the thing",
  selectedSkillIds: [],
  lifecycleState: "processing",
};

function expectReasoningBeforeAnswer() {
  const assistant = screen.getByTestId("assistant-message");
  const reasoning = assistant.querySelector(".reasoning-disclosure");
  const answer = assistant.querySelector(".message-content");
  expect(reasoning).not.toBeNull();
  expect(reasoning?.nextElementSibling).toBe(answer);
}

describe("assistant reasoning order", () => {
  it("renders completed reasoning before the assistant answer", () => {
    render(
      <MessageList
        timelineItems={[
          {
            kind: "assistantMessage",
            itemKey: "assistant-1",
            content: "Final answer",
            reasoning: "First I thought about it",
          },
        ]}
      />,
    );

    expectReasoningBeforeAnswer();
  });

  it("renders streaming reasoning before the live assistant answer", () => {
    render(
      <MessageList
        timelineItems={[
          {
            kind: "liveAssistant",
            itemKey: "live-1",
            content: "Answer in progress",
            reasoning: "Thinking in progress",
          },
        ]}
      />,
    );

    expectReasoningBeforeAnswer();
  });
});

describe("ChatTranscriptPanel states", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("shows a loading skeleton (not the first-message empty state) while a selected conversation loads", () => {
    render(<ChatTranscriptPanel selectedSessionId="s1" session={null} />);
    expect(screen.getByTestId("transcript-loading")).toBeInTheDocument();
    expect(screen.queryByText("Send the first message")).not.toBeInTheDocument();
  });

  it("reserves the first-message empty state for no selected session", () => {
    render(<ChatTranscriptPanel selectedSessionId={null} session={null} />);
    expect(screen.getByText("Send the first message")).toBeInTheDocument();
    expect(screen.queryByTestId("transcript-loading")).not.toBeInTheDocument();
  });

  it("shows the acknowledged send immediately while the session snapshot catches up", () => {
    render(
      <ChatTranscriptPanel
        selectedSessionId="s1"
        session={null}
        optimisticPendingTurn={{
          sessionId: "s1",
          requestId: "req_2",
          content: "check the upgrade",
          selectedSkillIds: [],
          lifecycleState: "pending",
          createdAt: "2026-08-20T19:40:12Z",
        }}
      />,
    );

    expect(screen.getByText("check the upgrade")).toBeInTheDocument();
    expect(screen.getByTestId("request-progress")).toHaveTextContent("Queued");
    expect(screen.queryByTestId("transcript-loading")).not.toBeInTheDocument();
  });

  it("hands the optimistic turn to the matching durable request without duplicating it", () => {
    render(
      <ChatTranscriptPanel
        selectedSessionId="s1"
        session={makeSession({ timelineItems: [pendingTurn] })}
        optimisticPendingTurn={{
          sessionId: "s1",
          requestId: "req_1",
          content: "do the thing",
          selectedSkillIds: [],
          lifecycleState: "pending",
          createdAt: null,
        }}
      />,
    );

    expect(screen.getAllByText("do the thing")).toHaveLength(1);
    expect(screen.getByTestId("request-progress")).toHaveTextContent("Working");
  });

  it("hands the optimistic turn to the matching durable user message", () => {
    render(
      <ChatTranscriptPanel
        selectedSessionId="s1"
        session={makeSession({
          timelineItems: [
            {
              kind: "userMessage",
              itemKey: "user_2",
              requestId: "req_2",
              content: "check the upgrade",
            },
          ],
        })}
        optimisticPendingTurn={{
          sessionId: "s1",
          requestId: "req_2",
          content: "check the upgrade",
          selectedSkillIds: [],
          lifecycleState: "pending",
          createdAt: null,
        }}
      />,
    );

    expect(screen.getAllByText("check the upgrade")).toHaveLength(1);
    expect(screen.queryByTestId("request-progress")).not.toBeInTheDocument();
  });

  it("shows the thinking indicator while the turn runs and the assistant is silent", () => {
    render(
      <ChatTranscriptPanel
        selectedSessionId="s1"
        session={makeSession({ timelineItems: [pendingTurn] })}
      />,
    );
    expect(screen.getByTestId("assistant-thinking")).toBeInTheDocument();
  });

  it("hides the thinking indicator once live assistant content streams", () => {
    render(
      <ChatTranscriptPanel
        selectedSessionId="s1"
        session={makeSession({
          timelineItems: [
            pendingTurn,
            { kind: "liveAssistant", itemKey: "l1", content: "first tokens" },
          ],
        })}
      />,
    );
    expect(screen.queryByTestId("assistant-thinking")).not.toBeInTheDocument();
  });

  it("hides the thinking indicator on terminal turn states", () => {
    render(
      <ChatTranscriptPanel
        selectedSessionId="s1"
        session={makeSession({
          turnState: "completed",
          timelineItems: [pendingTurn],
        })}
      />,
    );
    expect(screen.queryByTestId("assistant-thinking")).not.toBeInTheDocument();
  });

  it("follows the transcript with instant (not smooth) scrolling on a fresh send", async () => {
    const scrollSpy = vi
      .spyOn(HTMLElement.prototype, "scrollIntoView")
      .mockImplementation(() => {});
    vi.stubGlobal("requestAnimationFrame", (cb: FrameRequestCallback) => {
      cb(0);
      return 0;
    });
    vi.stubGlobal("cancelAnimationFrame", () => {});

    render(
      <ChatTranscriptPanel
        selectedSessionId="s1"
        session={makeSession({ timelineItems: [pendingTurn] })}
      />,
    );

    expect(scrollSpy).toHaveBeenCalledWith({
      block: "end",
      behavior: "instant",
    });
  });

  it("re-engages follow when an identical retry is first observed as a materialized user message", async () => {
    const scrollSpy = vi
      .spyOn(HTMLElement.prototype, "scrollIntoView")
      .mockImplementation(() => {});
    vi.stubGlobal("requestAnimationFrame", (cb: FrameRequestCallback) => {
      cb(0);
      return 0;
    });
    vi.stubGlobal("cancelAnimationFrame", () => {});

    const { rerender } = render(
      <ChatTranscriptPanel
        selectedSessionId="s1"
        session={makeSession({
          turnState: "failed",
          latestRequestId: "req_1",
          timelineItems: [
            {
              kind: "userMessage",
              itemKey: "user_1",
              content: "do the thing",
            },
          ],
        })}
      />,
    );

    const panel = screen.getByTestId("transcript-panel");
    Object.defineProperties(panel, {
      scrollHeight: { configurable: true, value: 1000 },
      scrollTop: { configurable: true, value: 100 },
      clientHeight: { configurable: true, value: 400 },
    });
    fireEvent.scroll(panel);
    scrollSpy.mockClear();

    rerender(
      <ChatTranscriptPanel
        selectedSessionId="s1"
        session={makeSession({
          latestRequestId: "req_2",
          timelineItems: [
            {
              kind: "userMessage",
              itemKey: "user_1",
              content: "do the thing",
            },
            {
              kind: "userMessage",
              itemKey: "user_2",
              content: "do the thing",
            },
          ],
        })}
      />,
    );

    await waitFor(() =>
      expect(scrollSpy).toHaveBeenCalledWith({
        block: "end",
        behavior: "instant",
      }),
    );
  });

  it("re-engages follow from the optimistic request before latestRequestId catches up", async () => {
    const scrollSpy = vi
      .spyOn(HTMLElement.prototype, "scrollIntoView")
      .mockImplementation(() => {});
    vi.stubGlobal("requestAnimationFrame", (cb: FrameRequestCallback) => {
      cb(0);
      return 0;
    });
    vi.stubGlobal("cancelAnimationFrame", () => {});

    const staleSession = makeSession({
      latestRequestId: "req_old",
      timelineItems: [
        {
          kind: "userMessage",
          itemKey: "user_old",
          requestId: "req_old",
          content: "earlier",
        },
      ],
    });
    const { rerender } = render(
      <ChatTranscriptPanel selectedSessionId="s1" session={staleSession} />,
    );

    const panel = screen.getByTestId("transcript-panel");
    Object.defineProperties(panel, {
      scrollHeight: { configurable: true, value: 1000 },
      scrollTop: { configurable: true, value: 100 },
      clientHeight: { configurable: true, value: 400 },
    });
    fireEvent.scroll(panel);
    scrollSpy.mockClear();

    rerender(
      <ChatTranscriptPanel
        selectedSessionId="s1"
        session={staleSession}
        optimisticPendingTurn={{
          sessionId: "s1",
          requestId: "req_new",
          content: "follow up",
          selectedSkillIds: [],
          lifecycleState: "pending",
          createdAt: "2026-08-20T20:00:00Z",
        }}
      />,
    );

    await waitFor(() =>
      expect(scrollSpy).toHaveBeenCalledWith({
        block: "end",
        behavior: "instant",
      }),
    );
  });
});
