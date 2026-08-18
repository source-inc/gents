import { fireEvent, render, screen } from "@testing-library/react";
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";

import { AppNavigation } from "../src/components/AppNavigation";
import {
  loadNavigationExpanded,
  saveNavigationExpanded,
} from "../src/lib/navigationPreference";

const handlers = () => ({
  onCloseDrawer: vi.fn(),
  onNavigate: vi.fn(),
  onOpenDrawer: vi.fn(),
  onShowShortcuts: vi.fn(),
  onToggleExpanded: vi.fn(),
});

describe("app navigation", () => {
  it("marks the current destination and disables agent views without a deployment", () => {
    const props = handlers();
    render(
      <AppNavigation
        {...props}
        currentView="fleet"
        deploymentAvailable={false}
        drawerOpen={false}
        expanded
      />,
    );

    expect(screen.getByTestId("app-nav-fleet")).toHaveAttribute("aria-current", "page");
    expect(screen.getByTestId("app-nav-chat")).toBeDisabled();
    expect(screen.getByTestId("app-nav-code")).toHaveAccessibleName(
      "Code — select an agent first",
    );
    expect(screen.getByTestId("app-nav-config")).toBeDisabled();
  });

  it("wires destinations, utilities, and the expansion control", () => {
    const props = handlers();
    render(
      <AppNavigation
        {...props}
        currentView="chat"
        deploymentAvailable
        drawerOpen={false}
        expanded
      />,
    );

    fireEvent.click(screen.getByTestId("app-nav-code"));
    expect(props.onNavigate).toHaveBeenCalledWith("code");
    fireEvent.click(screen.getByRole("button", { name: "Shortcuts" }));
    expect(props.onShowShortcuts).toHaveBeenCalledOnce();
    fireEvent.click(screen.getByTestId("app-navigation-collapse"));
    expect(props.onToggleExpanded).toHaveBeenCalledOnce();
  });

  it("closes the mobile drawer with Escape and restores trigger focus", () => {
    function Harness() {
      const [open, setOpen] = useState(false);
      return (
        <AppNavigation
          currentView="fleet"
          deploymentAvailable
          drawerOpen={open}
          expanded
          onCloseDrawer={() => setOpen(false)}
          onNavigate={vi.fn()}
          onOpenDrawer={() => setOpen(true)}
          onShowShortcuts={vi.fn()}
          onToggleExpanded={vi.fn()}
        />
      );
    }

    render(<Harness />);
    const trigger = screen.getByTestId("app-menu-trigger");
    fireEvent.click(trigger);
    expect(trigger).toHaveAttribute("aria-expanded", "true");
    fireEvent.keyDown(window, { key: "Escape" });
    expect(trigger).toHaveAttribute("aria-expanded", "false");
    expect(trigger).toHaveFocus();
  });
});

describe("navigation expansion preference", () => {
  it("defaults to expanded and remembers an explicit collapse", () => {
    window.localStorage.removeItem("gents-desktop-navigation-expanded");
    expect(loadNavigationExpanded()).toBe(true);
    saveNavigationExpanded(false);
    expect(loadNavigationExpanded()).toBe(false);
  });
});
