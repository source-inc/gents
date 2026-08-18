import { useState } from "react";

import { applyTheme, loadTheme } from "../lib/theme";

export function ThemeToggle({ showLabel = false }: { showLabel?: boolean } = {}) {
  const [theme, setTheme] = useState(loadTheme);
  const next = theme === "dark" ? "light" : "dark";

  return (
    <button
      aria-label={`Switch to ${next} theme`}
      className="ghost-button theme-toggle"
      data-testid="theme-toggle"
      onClick={() => {
        applyTheme(next);
        setTheme(next);
      }}
      title={`Switch to ${next} theme`}
      type="button"
    >
      {theme === "dark" ? (
        <svg aria-hidden="true" viewBox="0 0 24 24">
          <circle cx="12" cy="12" r="4" />
          <path d="M12 3v2" />
          <path d="M12 19v2" />
          <path d="m5.6 5.6 1.4 1.4" />
          <path d="m17 17 1.4 1.4" />
          <path d="M3 12h2" />
          <path d="M19 12h2" />
          <path d="m5.6 18.4 1.4-1.4" />
          <path d="m17 7 1.4-1.4" />
        </svg>
      ) : (
        <svg aria-hidden="true" viewBox="0 0 24 24">
          <path d="M20 13.2A7.8 7.8 0 1 1 10.8 4 6.2 6.2 0 0 0 20 13.2Z" />
        </svg>
      )}
      {showLabel ? <span className="app-navigation-label">Theme</span> : null}
    </button>
  );
}
