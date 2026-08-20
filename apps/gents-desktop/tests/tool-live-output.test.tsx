import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { MessageList } from "@source-inc/gents-desktop-chat";
import type { RenderedTimelineItem } from "@source-inc/gents-desktop-client";

function toolGroup(statusKind: string, tail: string | null): RenderedTimelineItem {
  return {
    kind: "toolGroup",
    itemKey: "g1",
    messageSequence: 1,
    tools: [
      {
        itemKey: "t1",
        toolName: "gents_exec",
        statusKind,
        status: statusKind,
        presentation: { kind: "generic", summary: null, input: null, output: null },
        partialOutputTail: tail,
      },
    ],
  } as RenderedTimelineItem;
}

describe("live tool output", () => {
  it("shows the rolling tail, open by default, while a tool runs", () => {
    render(
      <MessageList timelineItems={[toolGroup("running", "line one\nline two")]} />,
    );
    const tail = screen.getByTestId("tool-live-t1");
    expect(tail).toHaveTextContent("line two");
    expect(tail.closest("details")).toHaveAttribute("open");
  });

  it("never shows a stale tail on finished tools", () => {
    render(<MessageList timelineItems={[toolGroup("success", "old tail")]} />);
    expect(screen.queryByTestId("tool-live-t1")).not.toBeInTheDocument();
  });

  it("stays quiet for running tools with no output yet", () => {
    render(<MessageList timelineItems={[toolGroup("running", null)]} />);
    expect(screen.queryByTestId("tool-live-t1")).not.toBeInTheDocument();
  });
});
