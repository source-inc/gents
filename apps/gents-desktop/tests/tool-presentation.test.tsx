import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { MessageList } from "@source-inc/gents-desktop-chat";
import type {
  RenderedTimelineItem,
  RenderedToolCallView,
} from "@source-inc/gents-desktop-client";

function renderTool(tool: RenderedToolCallView) {
  const timeline: RenderedTimelineItem[] = [
    {
      kind: "toolGroup",
      itemKey: "tools-1",
      messageSequence: 2,
      tools: [tool],
    },
  ];
  return render(<MessageList timelineItems={timeline} />);
}

function baseTool(
  presentation: RenderedToolCallView["presentation"],
  overrides: Partial<RenderedToolCallView> = {},
): RenderedToolCallView {
  return {
    itemKey: "tool-1",
    toolName: "tool",
    status: "completed",
    statusKind: "success",
    presentation,
    ...overrides,
  };
}

describe("unified tool presentation", () => {
  afterEach(() => vi.restoreAllMocks());

  it("renders a command consistently with bounded, copyable streams", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.assign(navigator, { clipboard: { writeText } });
    renderTool(
      baseTool({
        kind: "command",
        command: "cargo test --workspace",
        exitCode: 1,
        timedOut: false,
        failed: true,
        durationMs: 1230,
        cwd: "/work/repo",
        executionMode: "read_only",
        networkMode: "disabled",
        stdout: "tests ran",
        stderr: "one failure",
        fallbackOutput: null,
      }),
    );

    expect(screen.getByText("cargo test --workspace")).toBeInTheDocument();
    expect(screen.getByText("exit 1")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("tool-tool-1").querySelector("summary")!);
    expect(screen.getByText("1.2s")).toBeInTheDocument();
    expect(screen.getByText("one failure")).toHaveClass("tool-payload");
    fireEvent.click(screen.getAllByRole("button", { name: "Copy" })[0]);
    await waitFor(() => expect(writeText).toHaveBeenCalledWith("tests ran"));
  });

  it("renders file edits as a bounded diff", () => {
    renderTool(
      baseTool({
        kind: "fileEdit",
        operation: "edit_file",
        path: "src/main.rs",
        created: false,
        replacementsApplied: 2,
        diff: [
          { kind: "del", text: "old" },
          { kind: "add", text: "new" },
        ],
        fallbackOutput: null,
      }),
    );
    expect(screen.getByText("src/main.rs")).toBeInTheDocument();
    expect(screen.getByText("×2")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("tool-tool-1").querySelector("summary")!);
    expect(screen.getByText("new").closest("pre")).toHaveClass("tool-diff");
  });

  it("makes MCP identity useful without opening raw JSON", () => {
    renderTool(
      baseTool({
        kind: "mcp",
        serviceId: "github",
        selectedToolName: "search_issues",
        arguments: '{"query":"mobile sync"}',
        output: '{"count":2}',
      }),
    );
    expect(screen.getByText("search_issues")).toBeInTheDocument();
    expect(screen.getByText("github")).toBeInTheDocument();
    expect(screen.getByTestId("tool-tool-1")).not.toHaveAttribute("open");
  });

  it("shows background process lifecycle inline", () => {
    renderTool(
      baseTool(
        {
          kind: "process",
          action: "spawn",
          target: "bash_unrestricted",
          description: '{"command":"cargo test"}',
          output: null,
        },
        {
          toolName: "spawn_process",
          status: "running",
          statusKind: "running",
          awaitMode: "background",
        },
      ),
    );
    expect(screen.getByText("process · spawn")).toBeInTheDocument();
    expect(screen.getByText("background")).toBeInTheDocument();
    expect(screen.getByText("running")).toBeInTheDocument();
  });
});
