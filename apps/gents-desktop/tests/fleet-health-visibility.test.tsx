import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { FleetRow, type FleetRowProps } from "@source-inc/gents-desktop-fleet";
import { deploymentStatus } from "@source-inc/gents-desktop-fleet";
import type { DeploymentView } from "@source-inc/gents-desktop-client";
import { deployment } from "./config-panel-wiring/fixtures";

function renderRow(dep: DeploymentView) {
  const props: FleetRowProps = {
    bootstrap: null,
    deployment: dep,
    onOpenChat: vi.fn(),
    onOpenConfig: vi.fn(),
  };
  render(
    <table>
      <tbody>
        <FleetRow {...props} />
      </tbody>
    </table>,
  );
}

describe("fleet health visibility", () => {
  it("labels statuses instead of relying on a hover dot", () => {
    expect(
      deploymentStatus({ ...deployment, dialSucceeded: true, lastError: null }).label,
    ).toBe("Online");
    expect(deploymentStatus({ ...deployment, dialSucceeded: false }).label).toBe(
      "Not connected",
    );
    expect(deploymentStatus({ ...deployment, lastError: "dial timeout" }).label).toBe(
      "Error",
    );
  });

  it("shows a visible error line with remediation-aware copy on failing rows", () => {
    renderRow({ ...deployment, dialSucceeded: true, lastError: "dial timeout" });
    expect(screen.getByTestId("fleet-status-peer-1")).toHaveTextContent("Error");
    expect(screen.getByTestId("fleet-error-peer-1")).toHaveTextContent("dial timeout");
  });

  it("shows useful document counts instead of transport identifiers", () => {
    renderRow({ ...deployment, dialSucceeded: true, lastError: null });
    expect(screen.getByTestId("fleet-summary-peer-1")).toHaveTextContent(
      "2 behaviors · 0 conversations · 2 tasks",
    );
    expect(screen.queryByRole("button", { name: "Copy DID" })).not.toBeInTheDocument();
    expect(screen.queryByText(/GraphQL/)).not.toBeInTheDocument();
    expect(screen.queryByTestId("fleet-error-peer-1")).not.toBeInTheDocument();
  });

  it("keeps the fleet actions focused on sessions and configuration", () => {
    renderRow({ ...deployment, dialSucceeded: true, lastError: null });
    expect(screen.getByTestId("fleet-chat-peer-1")).toBeInTheDocument();
    expect(screen.getByTestId("fleet-config-peer-1")).toBeInTheDocument();
    expect(screen.queryByTestId("fleet-code-peer-1")).not.toBeInTheDocument();
  });
});
