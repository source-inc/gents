import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { DeploymentView } from "@source-inc/gents-desktop-client";
import { ConnectedPeerSection } from "../src/components/sidebar-widgets/ConnectedPeerSection";

function dep(agentDid: string, label: string): DeploymentView {
  return {
    peerId: `peer-${label}`,
    label,
    agentDid,
    agentPrincipal: { agentDid, displayName: label },
    behaviorEnvironments: [],
    behaviors: [],
    tasks: [],
    conversations: [],
    dialSucceeded: true,
  } as unknown as DeploymentView;
}

describe("sidebar agent header", () => {
  it("switches between deployments", () => {
    const onSelectAgent = vi.fn();
    render(
      <ConnectedPeerSection
        deployments={[dep("did:a", "Alpha"), dep("did:b", "Beta")]}
        selectedAgentDid="did:a"
        onOpenFleet={vi.fn()}
        onConfigureDeployment={vi.fn()}
        onSelectAgent={onSelectAgent}
      />,
    );

    fireEvent.change(screen.getByTestId("sidebar-agent-switcher"), {
      target: { value: "did:b" },
    });
    expect(onSelectAgent).toHaveBeenCalledWith("did:b");
  });

  it("shows one canonical agent identity and connection summary", () => {
    const deployment = {
      ...dep("did:a", "Workstation 1"),
      agentPrincipal: { agentDid: "did:a", displayName: "Amy" },
      behaviorEnvironments: [
        {
          behaviorId: "default",
          activeSessionCount: 2,
        },
      ],
    } as unknown as DeploymentView;

    render(
      <ConnectedPeerSection
        deployments={[deployment]}
        selectedAgentDid="did:a"
        onOpenFleet={vi.fn()}
        onConfigureDeployment={vi.fn()}
      />,
    );

    expect(screen.getByRole("heading", { name: "Amy" })).toBeInTheDocument();
    expect(document.querySelector(".connected-peer-meta")).toHaveTextContent(
      "Connected · Workstation 1 · 2 active",
    );
    expect(screen.queryByText(/behaviors/)).not.toBeInTheDocument();
  });

  it("keeps back navigation and configuration in an overflow menu", () => {
    const onOpenFleet = vi.fn();
    const onConfigureDeployment = vi.fn();
    render(
      <ConnectedPeerSection
        deployments={[dep("did:a", "Alpha")]}
        selectedAgentDid="did:a"
        onOpenFleet={onOpenFleet}
        onConfigureDeployment={onConfigureDeployment}
      />,
    );

    fireEvent.click(screen.getByTestId("sidebar-back-to-fleet"));
    expect(onOpenFleet).toHaveBeenCalledOnce();

    fireEvent.click(screen.getByRole("button", { name: "Configure" }));
    expect(onConfigureDeployment).toHaveBeenCalledWith("did:a");
  });
});
