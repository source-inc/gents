import { screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { DesktopApiAdapter } from "@source-inc/gents-desktop-client";
import type { DesktopClientUpdatedListenerFactory } from "@source-inc/gents-desktop-client";
import type {
  DesktopClientSnapshot,
  TaskRunResult,
} from "@source-inc/gents-desktop-client";

import { bootstrap, deployment } from "./config-panel-wiring/fixtures";
import { renderTauriAppDriverWithBridge, type TauriDriverBridge } from "./tauri-driver";

function desktopSnapshot(): DesktopClientSnapshot {
  return {
    bootstrap,
    client: {
      localPeerId: "local-peer",
      listenAddresses: ["127.0.0.1:9191"],
      p2pHealth: {
        status: "healthy",
        connectedPeerCount: 1,
        replicatorCount: 1,
        consecutiveFailures: 0,
      },
      bootstrapErrors: [],
      configuredPeerCount: 1,
      dialedPeerCount: 1,
      peerIssueCount: 0,
      rowCount: 42,
      approxSerializedBytes: 2048,
      deployments: [deployment],
    },
  };
}

function makeBridge(overrides: Partial<DesktopApiAdapter>): TauriDriverBridge {
  const snapshot = desktopSnapshot();
  const listenerFactory: DesktopClientUpdatedListenerFactory = async () => () => {};
  const defaults: Partial<DesktopApiAdapter> = {
    fetchDesktopSnapshot: vi.fn(async () => snapshot),
    fetchSessionSnapshot: vi.fn(async () => null),
    setSelectedAgent: vi.fn(async () => undefined),
    startDesktopClient: vi.fn(async () => snapshot),
    shutdownDesktopClient: vi.fn(async () => ({
      bootstrap,
      client: null,
    })),
  };

  const adapter = new Proxy(
    {
      ...defaults,
      ...overrides,
    },
    {
      get(target, prop: string | symbol) {
        if (prop in target) {
          return target[prop as keyof DesktopApiAdapter];
        }
        return async () => {
          throw new Error(`DesktopApiAdapter.${String(prop)} not stubbed in this test`);
        };
      },
    },
  ) as DesktopApiAdapter;

  return {
    adapter,
    listenerFactory,
    sentRequests: [],
  };
}

describe("App shell command sad paths", () => {
  it("opens the agent navigation pane when Fleet selects a deployment", async () => {
    const driver = renderTauriAppDriverWithBridge(makeBridge({}), deployment.peerId);

    try {
      await driver.ready();
      await driver.user.click(driver.chatButton());

      await waitFor(() => {
        expect(document.querySelector(".workspace")).toHaveAttribute(
          "data-mobile-chat-pane",
          "navigation",
        );
      });
      expect(screen.getByRole("button", { name: "Sessions" })).toHaveAttribute(
        "aria-pressed",
        "true",
      );
      expect(screen.getByRole("button", { name: "New session" })).toBeInTheDocument();
    } finally {
      await driver.dispose();
    }
  });

  it("surfaces rejected backend saves and keeps the editor usable", async () => {
    const saveBackendConfig = vi.fn(async () => {
      throw new Error("backend save rejected");
    });
    const driver = renderTauriAppDriverWithBridge(
      makeBridge({ saveBackendConfig }),
      deployment.peerId,
    );

    try {
      await driver.ready();
      await driver.openConfig();
      await driver.openConfigSection("backends");
      await driver.replaceInput("backend-name", "Backend A Edited");

      await driver.user.click(screen.getByTestId("backend-save"));

      await waitFor(() => {
        expect(screen.getByTestId("error-banner")).toHaveTextContent(
          "backend save rejected",
        );
      });
      expect(saveBackendConfig).toHaveBeenCalledTimes(1);
      expect(screen.getByTestId("backend-save")).toBeEnabled();
      expect(screen.getByTestId("backend-name")).toHaveValue("Backend A Edited");
    } finally {
      await driver.dispose();
    }
  });

  it("surfaces rejected task runs and returns the run button to ready", async () => {
    const runTask = vi.fn<
      [(request: { taskId: string; args?: unknown }) => Promise<TaskRunResult>]
    >(async () => {
      throw new Error("task run rejected");
    });
    const driver = renderTauriAppDriverWithBridge(
      makeBridge({ runTask }),
      deployment.peerId,
    );

    try {
      await driver.ready();
      await driver.openConfig();
      await driver.openConfigSection("tasks");
      await waitFor(() => {
        expect(screen.getByTestId("task-run")).toBeEnabled();
      });

      await driver.user.click(screen.getByTestId("task-run"));

      await waitFor(() => {
        expect(screen.getByTestId("error-banner")).toHaveTextContent(
          "task run rejected",
        );
      });
      expect(runTask).toHaveBeenCalledWith({ taskId: "task-a", args: {} });
      expect(screen.getByTestId("task-run")).toBeEnabled();
      expect(screen.getByTestId("task-run-args")).toHaveValue("{}");
    } finally {
      await driver.dispose();
    }
  });
});
