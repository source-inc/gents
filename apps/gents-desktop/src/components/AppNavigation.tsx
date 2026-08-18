import { useEffect, useRef, type ReactNode } from "react";
import sourceMarkUrl from "../assets/source-mark-light.png";
import type { WorkspaceView } from "../hooks/useAppNavigation";
import { ThemeToggle } from "./ThemeToggle";

const SHORTCUT_MODIFIER = navigator.platform.toUpperCase().includes("MAC")
  ? "⌘"
  : "Ctrl+";

type NavigationItem = {
  view: WorkspaceView;
  label: string;
  shortcut: string;
  requiresDeployment: boolean;
  icon: ReactNode;
};

const NAVIGATION_ITEMS: NavigationItem[] = [
  {
    view: "fleet",
    label: "Fleet",
    shortcut: "1",
    requiresDeployment: false,
    icon: <FleetIcon />,
  },
  {
    view: "chat",
    label: "Chat",
    shortcut: "2",
    requiresDeployment: true,
    icon: <ChatIcon />,
  },
  {
    view: "code",
    label: "Code",
    shortcut: "3",
    requiresDeployment: true,
    icon: <CodeIcon />,
  },
  {
    view: "config",
    label: "Configure",
    shortcut: "4",
    requiresDeployment: true,
    icon: <SettingsIcon />,
  },
];

export type AppNavigationProps = {
  currentView: WorkspaceView;
  deploymentAvailable: boolean;
  drawerOpen: boolean;
  expanded: boolean;
  onCloseDrawer: () => void;
  onNavigate: (view: WorkspaceView) => void;
  onOpenDrawer: () => void;
  onShowShortcuts: () => void;
  onToggleExpanded: () => void;
};

export function AppNavigation({
  currentView,
  deploymentAvailable,
  drawerOpen,
  expanded,
  onCloseDrawer,
  onNavigate,
  onOpenDrawer,
  onShowShortcuts,
  onToggleExpanded,
}: AppNavigationProps) {
  const drawer = useRef<HTMLElement>(null);
  const trigger = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (!drawerOpen) return;
    const navigation = drawer.current;
    const focusable = navigation?.querySelectorAll<HTMLElement>(
      'button:not([disabled]), [href], [tabindex]:not([tabindex="-1"])',
    );
    const first = focusable?.[0];
    const last = focusable?.[focusable.length - 1];
    const frame = requestAnimationFrame(() => {
      navigation?.querySelector<HTMLElement>('[aria-current="page"]')?.focus();
    });

    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        event.preventDefault();
        onCloseDrawer();
        return;
      }
      if (event.key !== "Tab" || !first || !last) return;
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    }

    window.addEventListener("keydown", onKeyDown);
    return () => {
      cancelAnimationFrame(frame);
      window.removeEventListener("keydown", onKeyDown);
      trigger.current?.focus();
    };
  }, [drawerOpen, onCloseDrawer]);

  return (
    <>
      <button
        aria-controls="app-navigation"
        aria-expanded={drawerOpen}
        aria-label="Open navigation"
        className="ghost-button app-menu-trigger"
        data-testid="app-menu-trigger"
        onClick={onOpenDrawer}
        ref={trigger}
        type="button"
      >
        <MenuIcon />
      </button>
      <button
        aria-label="Close navigation"
        className={`app-navigation-backdrop${drawerOpen ? " open" : ""}`}
        onClick={onCloseDrawer}
        tabIndex={drawerOpen ? 0 : -1}
        type="button"
      />
      <aside
        aria-label="Application"
        className={`app-navigation${expanded ? " expanded" : " collapsed"}${
          drawerOpen ? " drawer-open" : ""
        }`}
        data-testid="app-navigation"
        id="app-navigation"
        ref={drawer}
      >
        <div className="app-navigation-brand">
          <img alt="" src={sourceMarkUrl} />
          <div className="app-navigation-label app-navigation-brand-copy">
            <span>Source Network</span>
            <strong>Gents</strong>
          </div>
        </div>

        <nav aria-label="Workspace" className="app-navigation-items">
          {NAVIGATION_ITEMS.map((item) => {
            const disabled = item.requiresDeployment && !deploymentAvailable;
            const title = disabled
              ? `${item.label} — select an agent first`
              : `${item.label} (⌘/Ctrl+${item.shortcut})`;
            return (
              <button
                aria-current={currentView === item.view ? "page" : undefined}
                aria-label={
                  disabled ? `${item.label} — select an agent first` : item.label
                }
                className="app-navigation-item"
                data-testid={`app-nav-${item.view}`}
                disabled={disabled}
                key={item.view}
                onClick={() => onNavigate(item.view)}
                title={title}
                type="button"
              >
                <span className="app-navigation-icon">{item.icon}</span>
                <span className="app-navigation-label">{item.label}</span>
                <kbd className="app-navigation-label">
                  {SHORTCUT_MODIFIER}
                  {item.shortcut}
                </kbd>
              </button>
            );
          })}
        </nav>

        <div className="app-navigation-utilities">
          <ThemeToggle showLabel />
          <button
            className="ghost-button app-navigation-utility"
            onClick={onShowShortcuts}
            title="Keyboard shortcuts (⌘/Ctrl+/)"
            type="button"
          >
            <HelpIcon />
            <span className="app-navigation-label">Shortcuts</span>
          </button>
          <button
            aria-expanded={expanded}
            className="ghost-button app-navigation-utility app-navigation-collapse"
            data-testid="app-navigation-collapse"
            onClick={onToggleExpanded}
            title={expanded ? "Collapse navigation" : "Expand navigation"}
            type="button"
          >
            <CollapseIcon />
            <span className="app-navigation-label">
              {expanded ? "Collapse" : "Expand"}
            </span>
          </button>
        </div>
      </aside>
    </>
  );
}

function SvgIcon({ children }: { children: ReactNode }) {
  return (
    <svg aria-hidden="true" viewBox="0 0 24 24">
      {children}
    </svg>
  );
}

function FleetIcon() {
  return (
    <SvgIcon>
      <rect height="7" rx="1" width="7" x="3" y="3" />
      <rect height="7" rx="1" width="7" x="14" y="3" />
      <rect height="7" rx="1" width="7" x="3" y="14" />
      <rect height="7" rx="1" width="7" x="14" y="14" />
    </SvgIcon>
  );
}

function ChatIcon() {
  return (
    <SvgIcon>
      <path d="M20 15a3 3 0 0 1-3 3H8l-5 3V7a3 3 0 0 1 3-3h11a3 3 0 0 1 3 3Z" />
    </SvgIcon>
  );
}

function CodeIcon() {
  return (
    <SvgIcon>
      <path d="m8 9-4 3 4 3" />
      <path d="m16 9 4 3-4 3" />
      <path d="m14 5-4 14" />
    </SvgIcon>
  );
}

function SettingsIcon() {
  return (
    <SvgIcon>
      <circle cx="12" cy="12" r="3" />
      <path d="M19.4 15a1.7 1.7 0 0 0 .3 1.9l.1.1-2.8 2.8-.1-.1a1.7 1.7 0 0 0-1.9-.3 1.7 1.7 0 0 0-1 1.6v.2h-4V21a1.7 1.7 0 0 0-1-1.6 1.7 1.7 0 0 0-1.9.3l-.1.1L4.2 17l.1-.1a1.7 1.7 0 0 0 .3-1.9A1.7 1.7 0 0 0 3 14H2.8v-4H3a1.7 1.7 0 0 0 1.6-1 1.7 1.7 0 0 0-.3-1.9L4.2 7 7 4.2l.1.1A1.7 1.7 0 0 0 9 4.6a1.7 1.7 0 0 0 1-1.6v-.2h4V3a1.7 1.7 0 0 0 1 1.6 1.7 1.7 0 0 0 1.9-.3l.1-.1L19.8 7l-.1.1a1.7 1.7 0 0 0-.3 1.9 1.7 1.7 0 0 0 1.6 1h.2v4H21a1.7 1.7 0 0 0-1.6 1Z" />
    </SvgIcon>
  );
}

function HelpIcon() {
  return (
    <SvgIcon>
      <circle cx="12" cy="12" r="9" />
      <path d="M9.8 9a2.3 2.3 0 1 1 3.4 2c-.8.5-1.2 1-1.2 2" />
      <path d="M12 17h.01" />
    </SvgIcon>
  );
}

function CollapseIcon() {
  return (
    <SvgIcon>
      <path d="m14 6-6 6 6 6" />
      <path d="M20 4v16" />
    </SvgIcon>
  );
}

function MenuIcon() {
  return (
    <SvgIcon>
      <path d="M4 7h16M4 12h16M4 17h16" />
    </SvgIcon>
  );
}
