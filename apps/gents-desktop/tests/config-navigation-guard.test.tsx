import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ConfigWorkspace } from "../src/components/ConfigWorkspace";
import { ConfigNavigationGuardBoundary } from "../src/components/config/ConfigNavigationGuard";
import { useConfigNavigationGuard } from "../src/components/config/ConfigNavigationGuard";
import {
  bootstrap,
  deployment,
  workspaceHandlers,
} from "./config-panel-wiring/fixtures";

function ExternalNavigationProbe({ onNavigate }: { onNavigate: () => void }) {
  const { requestNavigation } = useConfigNavigationGuard();
  return (
    <button onClick={() => requestNavigation(onNavigate)} type="button">
      Fleet destination
    </button>
  );
}

function renderWorkspace(onExternalNavigate?: () => void) {
  const handlers = {
    ...workspaceHandlers(),
    onDeleteSkillConfig: vi.fn(),
    onDeleteTaskConfig: vi.fn(),
    onDeleteScheduleConfig: vi.fn(),
    onDeleteEventTriggerConfig: vi.fn(),
    onDeleteBackendConfig: vi.fn(),
    onDeleteInferenceProfileConfig: vi.fn(),
    onDeleteToolSelectionConfig: vi.fn(),
    onDeleteToolServiceConfig: vi.fn(),
    onDeleteBehaviorConfig: vi.fn(),
  };

  render(
    <ConfigNavigationGuardBoundary>
      <ConfigWorkspace
        bootstrap={bootstrap}
        selectedBehaviorId="default"
        selectedDeployment={deployment}
        saving={false}
        runningTask={false}
        {...handlers}
      />
      {onExternalNavigate ? (
        <ExternalNavigationProbe onNavigate={onExternalNavigate} />
      ) : null}
    </ConfigNavigationGuardBoundary>,
  );
  return handlers;
}

function editBehaviorPrompt(value: string) {
  fireEvent.change(screen.getByTestId("behavior-system-prompt"), {
    target: { value },
  });
  expect(screen.getByTestId("unsaved-chip")).toBeInTheDocument();
}

describe("config navigation guard", () => {
  it("keeps an edited form mounted until tab navigation is confirmed", () => {
    renderWorkspace();
    editBehaviorPrompt("keep this tab edit");

    fireEvent.click(screen.getByTestId("config-tab-backends"));

    expect(screen.getByTestId("confirm-dialog")).toBeInTheDocument();
    expect(screen.getByTestId("config-tab-behavior")).toHaveAttribute(
      "aria-selected",
      "true",
    );
    expect(screen.getByTestId("behavior-system-prompt")).toHaveValue(
      "keep this tab edit",
    );

    fireEvent.click(screen.getByTestId("confirm-dialog-cancel"));
    expect(screen.queryByTestId("confirm-dialog")).not.toBeInTheDocument();
    expect(screen.getByTestId("behavior-system-prompt")).toHaveValue(
      "keep this tab edit",
    );

    fireEvent.click(screen.getByTestId("config-tab-backends"));
    fireEvent.click(screen.getByTestId("confirm-dialog-confirm"));

    expect(screen.getByTestId("config-tab-backends")).toHaveAttribute(
      "aria-selected",
      "true",
    );
    expect(screen.getByTestId("backend-endpoint")).toBeInTheDocument();
  });

  it("guards document selection and hydrates the next document after discard", async () => {
    renderWorkspace();
    editBehaviorPrompt("keep this document edit");

    fireEvent.click(screen.getByTestId("config-behavior-ops"));

    expect(screen.getByTestId("confirm-dialog")).toBeInTheDocument();
    expect(screen.getByTestId("behavior-system-prompt")).toHaveValue(
      "keep this document edit",
    );

    fireEvent.click(screen.getByTestId("confirm-dialog-confirm"));

    await waitFor(() =>
      expect(screen.getByTestId("behavior-system-prompt")).toHaveValue(
        "You are the ops behavior.",
      ),
    );
  });

  it("guards returning to chat and protects browser-level navigation", () => {
    const handlers = renderWorkspace();
    editBehaviorPrompt("keep this back-navigation edit");

    const beforeUnload = new Event("beforeunload", {
      cancelable: true,
    }) as BeforeUnloadEvent;
    expect(window.dispatchEvent(beforeUnload)).toBe(false);
    expect(beforeUnload.defaultPrevented).toBe(true);

    fireEvent.click(screen.getByTestId("config-back-tab"));
    expect(handlers.onBack).not.toHaveBeenCalled();

    fireEvent.click(screen.getByTestId("confirm-dialog-confirm"));
    expect(handlers.onBack).toHaveBeenCalledOnce();
  });

  it("guards destinations outside the configuration workspace", () => {
    const onNavigate = vi.fn();
    renderWorkspace(onNavigate);
    editBehaviorPrompt("keep this edit during global navigation");

    fireEvent.click(screen.getByRole("button", { name: "Fleet destination" }));
    expect(onNavigate).not.toHaveBeenCalled();

    fireEvent.click(screen.getByTestId("confirm-dialog-cancel"));
    expect(onNavigate).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: "Fleet destination" }));
    fireEvent.click(screen.getByTestId("confirm-dialog-confirm"));
    expect(onNavigate).toHaveBeenCalledOnce();
  });
});
