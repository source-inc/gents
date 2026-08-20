import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ChatHeader } from "@source-inc/gents-desktop-chat";
import { BackgroundedToolsPanel } from "@source-inc/gents-desktop-operations";
import { useOperationsSnapshot } from "@source-inc/gents-desktop-operations";

const mockedSnapshot = vi.fn<typeof useOperationsSnapshot>();

describe("session ops", () => {
  it("reports pairing progress instead of calling an idle transport healthy", () => {
    render(
      <ChatHeader
        selectedSessionId="session-1"
        selectedConversationTitle="planning"
        behaviorLabel={null}
        runtimeHealth={{
          status: "healthy",
          connectedPeerCount: 0,
          replicatorCount: 1,
          consecutiveFailures: 0,
        }}
        configuredPeerCount={2}
        dialedPeerCount={1}
        onRenameConversationTitle={vi.fn()}
      />,
    );

    expect(screen.getByText("Reconnecting 1/2")).toHaveAttribute(
      "title",
      "Transport healthy; 1/2 saved peers dialed; 0 active connections; 1 replicators",
    );
  });

  it("offers a phone navigation control without changing desktop chat semantics", () => {
    const onOpenMobileNavigation = vi.fn();
    render(
      <ChatHeader
        selectedSessionId="session-1"
        selectedConversationTitle="planning"
        behaviorLabel="Amy"
        runtimeHealth={null}
        onRenameConversationTitle={vi.fn()}
        onOpenMobileNavigation={onOpenMobileNavigation}
      />,
    );

    fireEvent.click(screen.getByTestId("mobile-chat-navigation"));
    expect(onOpenMobileNavigation).toHaveBeenCalledOnce();
  });

  it("keeps a failed title rename open with the operator's draft", async () => {
    const onRenameConversationTitle = vi
      .fn()
      .mockRejectedValue(new Error("replica unavailable"));
    render(
      <ChatHeader
        selectedSessionId="session-1"
        selectedConversationTitle="planning"
        behaviorLabel={null}
        runtimeHealth={null}
        onRenameConversationTitle={onRenameConversationTitle}
      />,
    );

    fireEvent.click(screen.getByTestId("conversation-title-edit"));
    const input = screen.getByTestId("conversation-title-input");
    expect(input).toHaveAccessibleName("Rename planning");
    fireEvent.change(input, { target: { value: "revised planning" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() =>
      expect(screen.getByTestId("conversation-title-input")).toHaveValue(
        "revised planning",
      ),
    );
    expect(onRenameConversationTitle).toHaveBeenCalledWith(
      "session-1",
      "revised planning",
    );
  });

  it("offers Resend on stuck diagnostics rows", async () => {
    mockedSnapshot.mockReturnValue({
      snapshot: {
        fetchedAt: new Date().toISOString(),
        backgroundedTools: [],
        stuckDiagnostics: [
          {
            requestId: "req-stale-1",
            severity: "critical",
            reason: "expiredProcessing",
          },
        ],
      },
      error: null,
      isLoading: false,
      refresh: vi.fn(),
    } as never);
    const onResendRequest = vi.fn();
    render(
      <BackgroundedToolsPanel
        agentDid="did:a"
        onResendRequest={onResendRequest}
        useSnapshot={mockedSnapshot}
      />,
    );

    await waitFor(() =>
      expect(screen.getByTestId("stuck-resend-req-stale-1")).toBeInTheDocument(),
    );
    fireEvent.click(screen.getByTestId("stuck-resend-req-stale-1"));
    expect(onResendRequest).toHaveBeenCalledWith("req-stale-1");
  });
});
