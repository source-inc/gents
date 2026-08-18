import { useEffect, useRef, type KeyboardEvent as ReactKeyboardEvent } from "react";

const IS_MAC = navigator.platform.toUpperCase().includes("MAC");
const MOD = IS_MAC ? "⌘" : "Ctrl+";

const SHORTCUTS: Array<[string, string]> = [
  [`${MOD}1`, "Fleet dashboard"],
  [`${MOD}2`, "Chat"],
  [`${MOD}3`, "Code mode"],
  [`${MOD}4`, "Configuration"],
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
  const closeButton = useRef<HTMLButtonElement>(null);
  const dialog = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const previousFocus =
      document.activeElement instanceof HTMLElement ? document.activeElement : null;
    closeButton.current?.focus();
    return () => previousFocus?.focus();
  }, [open]);

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

  function onDialogKeyDown(event: ReactKeyboardEvent<HTMLDivElement>) {
    if (event.key !== "Tab" || !dialog.current) return;
    const focusable = Array.from(
      dialog.current.querySelectorAll<HTMLElement>(
        'button:not([disabled]), a[href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])',
      ),
    );
    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    if (!first || !last) {
      event.preventDefault();
      dialog.current.focus();
    } else if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  }

  return (
    <div className="dialog-backdrop open" role="presentation" onClick={onClose}>
      <div
        aria-labelledby="shortcuts-help-title"
        aria-modal="true"
        className="dialog shortcuts-help"
        data-testid="shortcuts-help"
        onKeyDown={onDialogKeyDown}
        ref={dialog}
        role="dialog"
        tabIndex={-1}
        onClick={(event) => event.stopPropagation()}
      >
        <header className="shortcuts-help-header">
          <p className="eyebrow" id="shortcuts-help-title">
            Keyboard shortcuts
          </p>
          <button
            aria-label="Close keyboard shortcuts"
            className="ghost-button shortcuts-help-close"
            onClick={onClose}
            ref={closeButton}
            type="button"
          >
            <span aria-hidden="true">×</span>
          </button>
        </header>
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
