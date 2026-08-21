import { useEffect } from "react";

const IS_MAC = navigator.platform.toUpperCase().includes("MAC");
const MOD = IS_MAC ? "⌘" : "Ctrl+";

const SHORTCUTS: Array<[string, string]> = [
  [`${MOD}1`, "Fleet dashboard"],
  [`${MOD}2`, "Chat"],
  [`${MOD}3`, "Configuration"],
  [`${MOD}N`, "New conversation"],
  [`${MOD}K`, "Focus the composer"],
  [`${MOD}/`, "Show this reference"],
];

export function ShortcutsHelp({
  open,
  onClose,
}: {
  open: boolean;
  onClose: () => void;
}) {
  useEffect(() => {
    if (!open) {
      return;
    }
    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        onClose();
      }
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [open, onClose]);

  if (!open) {
    return null;
  }
  return (
    <div className="dialog-backdrop open" role="presentation" onClick={onClose}>
      <div
        aria-label="Keyboard shortcuts"
        className="dialog shortcuts-help"
        data-testid="shortcuts-help"
        role="dialog"
        onClick={(event) => event.stopPropagation()}
      >
        <p className="eyebrow">Keyboard shortcuts</p>
        <dl className="shortcuts-list">
          {SHORTCUTS.map(([keys, action]) => (
            <div className="shortcuts-row" key={keys}>
              <dt>
                <kbd>{keys}</kbd>
              </dt>
              <dd>{action}</dd>
            </div>
          ))}
        </dl>
      </div>
    </div>
  );
}
