import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type {
  BehaviorEnvironmentView,
  ConversationSummary,
} from "@source-inc/gents-desktop-client";
import { ConversationListSection } from "../src/components/sidebar-widgets/ConversationListSection";

const AGENT = "did:key:z6MkAgent";

function conv(overrides: Partial<ConversationSummary>): ConversationSummary {
  return {
    sessionId: "s-1",
    title: "release planning",
    previewText: "let's cut v2",
    messageCount: 3,
    toolCallCount: 0,
    updatedAt: "2026-07-17T10:00:00Z",
    ...overrides,
  } as ConversationSummary;
}

function environment(
  overrides: Partial<BehaviorEnvironmentView> = {},
): BehaviorEnvironmentView {
  return {
    behaviorId: "default",
    displayName: "Amy",
    enabled: true,
    isDefault: true,
    modelName: "gpt-5",
    inferenceProfileName: "Default",
    workspaceRoot: "/work/amygdala",
    fileAccess: "read-write",
    bashAccess: "unrestricted",
    networkAccess: "enabled",
    skillNames: [],
    sessionCount: 2,
    activeSessionCount: 0,
    ...overrides,
  };
}

function renderList(
  conversations: ConversationSummary[],
  overrides: {
    environments?: BehaviorEnvironmentView[];
    onCreateSession?: () => void;
    onOpenSession?: (sessionId: string) => void;
  } = {},
) {
  const onCreateSession = overrides.onCreateSession ?? vi.fn();
  const onOpenSession = overrides.onOpenSession ?? vi.fn();
  render(
    <ConversationListSection
      conversations={conversations}
      environments={
        overrides.environments ?? [
          environment(),
          environment({
            behaviorId: "review",
            displayName: "Review",
            isDefault: false,
            workspaceRoot: "/work/reviews",
          }),
        ]
      }
      selectedAgentDid={AGENT}
      selectedSessionId={null}
      onSelectSession={vi.fn()}
      onOpenSession={onOpenSession}
      onCreateSession={onCreateSession}
    />,
  );
  return { onCreateSession, onOpenSession };
}

describe("conversation list", () => {
  it("searches titles, previews, and behavior environments", () => {
    renderList([
      conv({ sessionId: "s-1", title: "release planning" }),
      conv({
        sessionId: "s-2",
        behaviorId: "review",
        title: "standup",
        previewText: "deploy notes",
      }),
    ]);

    fireEvent.change(screen.getByTestId("conversation-search"), {
      target: { value: "review" },
    });
    expect(screen.queryByTestId("conversation-s-1")).not.toBeInTheDocument();
    expect(screen.getByTestId("conversation-s-2")).toBeInTheDocument();

    fireEvent.change(screen.getByTestId("conversation-search"), {
      target: { value: "zzz" },
    });
    expect(screen.getByText("No sessions match the search.")).toBeInTheDocument();
  });

  it("shows every environment in one session list", () => {
    renderList([
      conv({ sessionId: "s-default", behaviorId: "default" }),
      conv({ sessionId: "s-unassigned", behaviorId: null, title: "unassigned chat" }),
      conv({ sessionId: "s-review", behaviorId: "review", title: "review chat" }),
    ]);

    expect(screen.getByTestId("conversation-s-default")).toHaveTextContent("Amy");
    expect(screen.getByTestId("conversation-s-unassigned")).toHaveTextContent(
      "Unassigned behavior",
    );
    expect(screen.getByTestId("conversation-s-review")).toHaveTextContent(
      "Review · reviews",
    );
  });

  it("prioritizes lifecycle states without inventing client state", () => {
    renderList([
      conv({ sessionId: "s-failed", title: "failed", turnState: "failed" }),
      conv({ sessionId: "s-running", title: "running", turnState: "processing" }),
      conv({ sessionId: "s-done", title: "done", turnState: "completed" }),
    ]);

    const headings = screen.getAllByRole("heading", { level: 3 });
    expect(headings.map((heading) => heading.textContent)).toEqual([
      "Needs attention",
      "Active",
      "Recent",
    ]);
  });

  it("shows relative time, preview, and task context", () => {
    renderList([
      conv({
        updatedAt: new Date(Date.now() - 7_200_000).toISOString(),
        taskId: "task-a",
        taskName: "Daily review",
      }),
    ]);

    expect(screen.getByText("2h ago")).toBeInTheDocument();
    expect(screen.getByText("let's cut v2")).toBeInTheDocument();
    expect(screen.getByText("Daily review")).toBeInTheDocument();
  });

  it("opens sessions and sends new-session intent to the behavior catalog", () => {
    const { onCreateSession, onOpenSession } = renderList([conv({ sessionId: "s-1" })]);

    fireEvent.click(screen.getByTestId("conversation-s-1"));
    expect(onOpenSession).toHaveBeenCalledWith("s-1");

    fireEvent.click(screen.getByTestId("session-new"));
    expect(onCreateSession).toHaveBeenCalledOnce();
  });
});
