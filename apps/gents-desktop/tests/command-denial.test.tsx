import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { CommandDenialToolItem, MessageList } from "@source-inc/gents-desktop-chat";
import type {
  CommandDenialView,
  RenderedTimelineItem,
  RenderedToolCallView,
} from "@source-inc/gents-desktop-client";

const DENIAL: CommandDenialView = {
  category: "read-only-guard",
  categoryLabel: "Read-only guard",
  ruleId: "readOnlyArgumentNotAllowed",
  reasonLine: "sed in-place edits aren't allowed under the read-only tool.",
  deniedCommand: "sed",
  deniedArgument: "--in-place",
  diagnostic: "sed in-place edits are not allowed",
};

function deniedToolView(denial?: CommandDenialView): RenderedToolCallView {
  return {
    itemKey: "tool-1",
    toolName: "bash_read_only · sed",
    status: "failed",
    statusKind: "error",
    presentation: {
      kind: "generic",
      summary: null,
      input: null,
      output: "sed in-place edits are not allowed",
    },
    denial,
  };
}

function timeline(tool: RenderedToolCallView): RenderedTimelineItem[] {
  return [
    {
      kind: "toolGroup",
      itemKey: "group-1",
      tools: [tool],
    },
  ];
}

describe("CommandDenialToolItem", () => {
  it("renders the structured database projection", () => {
    const { container, getByText } = render(
      <CommandDenialToolItem tool={deniedToolView(DENIAL)} denial={DENIAL} />,
    );

    expect(container.querySelector(".tool-item-dot-denied")).not.toBeNull();
    expect(
      container.querySelector("details.tool-item-denied")?.getAttribute("data-rule-id"),
    ).toBe("readOnlyArgumentNotAllowed");
    expect(getByText("Read-only guard")).toBeTruthy();
    expect(container.querySelector(".denied-token")).not.toBeNull();
  });

  it("routes only a structured denial through the denial renderer", () => {
    const { container } = render(
      <MessageList timelineItems={timeline(deniedToolView(DENIAL))} />,
    );

    expect(container.querySelector(".tool-item-denied")).not.toBeNull();
    expect(
      container.querySelector("[data-rule-id]")?.getAttribute("data-rule-id"),
    ).toBe("readOnlyArgumentNotAllowed");
  });

  it("does not reinterpret free-form tool output as lifecycle state", () => {
    const { container } = render(
      <MessageList timelineItems={timeline(deniedToolView())} />,
    );

    expect(container.querySelector(".tool-item-denied")).toBeNull();
    expect(container.querySelector(".tool-item-dot-error")).not.toBeNull();
  });
});
