import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { MessageList } from "@source-inc/gents-desktop-chat";
import type { RenderedTimelineItem } from "@source-inc/gents-desktop-client";

function renderGeneric(summary: string | null) {
  const items: RenderedTimelineItem[] = [
    {
      kind: "toolGroup",
      itemKey: "tools-1",
      messageSequence: 1,
      tools: [
        {
          itemKey: "tool-1",
          toolName: "web_request",
          status: "completed",
          statusKind: "success",
          presentation: {
            kind: "generic",
            summary,
            input: '{"large":"payload remains collapsed"}',
            output: null,
          },
        },
      ],
    },
  ];
  return render(<MessageList timelineItems={items} />).container;
}

describe("generic tool summaries", () => {
  it("shows only the bridge-projected safe summary", () => {
    const container = renderGeneric("src/main.rs");
    expect(container.querySelector(".tool-summary-preview")).toHaveTextContent(
      "src/main.rs",
    );
    expect(container.querySelector("details")).not.toHaveAttribute("open");
  });

  it("does not invent a preview when the bridge suppresses one", () => {
    const container = renderGeneric(null);
    expect(container.querySelector(".tool-summary-preview")).toBeNull();
  });
});
