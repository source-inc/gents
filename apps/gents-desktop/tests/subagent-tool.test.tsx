import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { MessageList } from "@source-inc/gents-desktop-chat";
import type { RenderedTimelineItem } from "@source-inc/gents-desktop-client";

describe("subagent transcript tool", () => {
  it("renders a running child as an open lifecycle card", () => {
    const items: RenderedTimelineItem[] = [
      {
        kind: "toolGroup",
        itemKey: "spawn-group",
        messageSequence: 2,
        tools: [
          {
            itemKey: "spawn-1",
            toolName: "spawn_subagent",
            status: "running",
            statusKind: "running",
            childRequestId: "child-request-123456789",
            awaitMode: "background",
            presentation: {
              kind: "subagent",
              action: "spawn",
              name: "researcher",
              childRequestId: "child-request-123456789",
              description: "Trace the completion control flow",
              output: null,
            },
            partialOutputTail: "Reading watcher.rs",
            partialOutputSeq: 18,
          },
        ],
      },
    ];

    const { container, getAllByText, getByText } = render(
      <MessageList timelineItems={items} />,
    );
    const card = container.querySelector('[data-testid="tool-spawn-1"]');

    expect(card).not.toBeNull();
    expect(card?.hasAttribute("open")).toBe(true);
    expect(card?.getAttribute("data-child-request-id")).toBe("child-request-123456789");
    expect(getByText("researcher")).toBeTruthy();
    expect(getByText("background")).toBeTruthy();
    expect(getByText("running")).toBeTruthy();
    expect(getAllByText("Trace the completion control flow")).toHaveLength(2);
    expect(getByText("Reading watcher.rs")).toBeTruthy();
  });

  it("renders terminal child output and status", () => {
    const items: RenderedTimelineItem[] = [
      {
        kind: "toolGroup",
        itemKey: "spawn-group-complete",
        messageSequence: 2,
        tools: [
          {
            itemKey: "spawn-complete",
            toolName: "spawn_subagent",
            status: "completed",
            statusKind: "success",
            childRequestId: "child-complete",
            awaitMode: "foreground",
            presentation: {
              kind: "subagent",
              action: "spawn",
              name: "reviewer",
              childRequestId: "child-complete",
              description: "Review the patch",
              output: "No blocking issues found.",
            },
          },
        ],
      },
    ];

    const { getByText } = render(<MessageList timelineItems={items} />);

    expect(getByText("completed")).toBeTruthy();
    expect(getByText("foreground")).toBeTruthy();
    expect(getByText("No blocking issues found.")).toBeTruthy();
  });
});
