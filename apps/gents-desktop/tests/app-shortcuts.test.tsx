import { fireEvent, render, screen } from "@testing-library/react";
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";

import { ShortcutsHelp } from "../src/components/ShortcutsHelp";
import { useAppShortcuts } from "../src/hooks/useAppShortcuts";
import type { AppShortcutHandlers } from "../src/hooks/useAppShortcuts";

function Probe(handlers: AppShortcutHandlers) {
  useAppShortcuts(handlers);
  return null;
}

function makeHandlers(): AppShortcutHandlers {
  return {
    setView: vi.fn(),
    newConversation: vi.fn(),
    focusComposer: vi.fn(),
    toggleHelp: vi.fn(),
  };
}

function ShortcutsHarness() {
  const [open, setOpen] = useState(false);
  return (
    <>
      <button onClick={() => setOpen(true)} type="button">
        Open shortcuts
      </button>
      <ShortcutsHelp open={open} onClose={() => setOpen(false)} />
    </>
  );
}

describe("app shortcuts", () => {
  it("maps modifier chords to actions and ignores bare keys", () => {
    const handlers = makeHandlers();
    render(<Probe {...handlers} />);

    fireEvent.keyDown(window, { key: "2", metaKey: true });
    expect(handlers.setView).toHaveBeenCalledWith("chat");
    fireEvent.keyDown(window, { key: "4", ctrlKey: true });
    expect(handlers.setView).toHaveBeenCalledWith("config");
    fireEvent.keyDown(window, { key: "n", metaKey: true });
    expect(handlers.newConversation).toHaveBeenCalledTimes(1);
    fireEvent.keyDown(window, { key: "k", metaKey: true });
    expect(handlers.focusComposer).toHaveBeenCalledTimes(1);
    fireEvent.keyDown(window, { key: "/", metaKey: true });
    expect(handlers.toggleHelp).toHaveBeenCalledTimes(1);

    fireEvent.keyDown(window, { key: "2" });
    fireEvent.keyDown(window, { key: "n", metaKey: true, shiftKey: true });
    expect(handlers.setView).toHaveBeenCalledTimes(2);
    expect(handlers.newConversation).toHaveBeenCalledTimes(1);
  });

  it("renders the reference dialog and closes on Escape and backdrop", () => {
    const onClose = vi.fn();
    render(<ShortcutsHelp open onClose={onClose} />);

    expect(screen.getByTestId("shortcuts-help")).toHaveAttribute("aria-modal", "true");
    expect(screen.getByText("New conversation")).toBeInTheDocument();

    fireEvent.keyDown(window, { key: "Escape" });
    expect(onClose).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByRole("presentation"));
    expect(onClose).toHaveBeenCalledTimes(2);
  });

  it("contains focus and restores it to the opener when closed", () => {
    render(<ShortcutsHarness />);
    const opener = screen.getByRole("button", { name: "Open shortcuts" });
    opener.focus();
    fireEvent.click(opener);

    const close = screen.getByRole("button", {
      name: "Close keyboard shortcuts",
    });
    expect(close).toHaveFocus();

    fireEvent.keyDown(close, { key: "Tab" });
    expect(close).toHaveFocus();
    fireEvent.keyDown(close, { key: "Tab", shiftKey: true });
    expect(close).toHaveFocus();

    fireEvent.click(close);
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(opener).toHaveFocus();
  });
});
