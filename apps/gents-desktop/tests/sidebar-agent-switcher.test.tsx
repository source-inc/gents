import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ConnectedPeerSection } from "../src/components/sidebar-widgets/ConnectedPeerSection";
import type { DeploymentView } from "@source-inc/gents-desktop-client";

function dep(agentDid: string, label: string): DeploymentView {
  return {
    peerId: `peer-${label}`,
    label,
    agentDid,
    agentPrincipal: { agentDid },
    behaviors: [],
    tasks: [],
    conversations: [],
  } as unknown as DeploymentView;
}

describe("sidebar agent switcher", () => {
  it("switches between deployments", () => {
    const onSelectAgent = vi.fn();
    render(
      <ConnectedPeerSection
        deployments={[dep("did:a", "Alpha"), dep("did:b", "Beta")]}
        selectedAgentDid="did:a"
        onSelectAgent={onSelectAgent}
      />,
    );

    fireEvent.change(screen.getByTestId("sidebar-agent-switcher"), {
      target: { value: "did:b" },
    });
    expect(onSelectAgent).toHaveBeenCalledWith("did:b");
  });

  it("keeps the static title for a single deployment", () => {
    render(
      <ConnectedPeerSection
        deployments={[dep("did:a", "Alpha")]}
        selectedAgentDid="did:a"
        onSelectAgent={vi.fn()}
      />,
    );
    expect(screen.queryByTestId("sidebar-agent-switcher")).not.toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Alpha" })).toBeInTheDocument();
  });

  it("keeps document counts at the top of agent details", () => {
    const deployment = {
      ...dep("did:a", "Alpha"),
      behaviors: [{ behaviorId: "default" }, { behaviorId: "review" }],
      conversations: [{ sessionId: "session-a" }],
      tasks: [{ taskId: "task-a" }],
    } as unknown as DeploymentView;

    render(
      <ConnectedPeerSection deployments={[deployment]} selectedAgentDid="did:a" />,
    );

    expect(
      screen.getByLabelText("2 behaviors, 1 conversations, 1 tasks"),
    ).toBeInTheDocument();
  });
});
