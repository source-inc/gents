import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { ConfirmDialog } from "@source-inc/gents-desktop-ui";

type ConfigNavigationGuardValue = {
  reportDirty: (dirty: boolean) => void;
  requestNavigation: (navigate: () => void) => void;
};

const unguardedNavigation: ConfigNavigationGuardValue = {
  reportDirty: () => undefined,
  requestNavigation: (navigate) => navigate(),
};

const ConfigNavigationGuardContext =
  createContext<ConfigNavigationGuardValue>(unguardedNavigation);

export function ConfigNavigationGuardProvider({
  children,
  value,
}: {
  children: ReactNode;
  value: ConfigNavigationGuardValue;
}) {
  return (
    <ConfigNavigationGuardContext.Provider value={value}>
      {children}
    </ConfigNavigationGuardContext.Provider>
  );
}

export function ConfigNavigationGuardBoundary({ children }: { children: ReactNode }) {
  const [dirty, setDirty] = useState(false);
  const [confirmingDiscard, setConfirmingDiscard] = useState(false);
  const pendingNavigation = useRef<(() => void) | null>(null);

  const requestNavigation = useCallback(
    (navigate: () => void) => {
      if (!dirty) {
        navigate();
        return;
      }
      pendingNavigation.current = navigate;
      setConfirmingDiscard(true);
    },
    [dirty],
  );

  const value = useMemo(
    () => ({ reportDirty: setDirty, requestNavigation }),
    [requestNavigation],
  );

  useEffect(() => {
    if (!dirty) return;
    const preventAccidentalClose = (event: BeforeUnloadEvent) => {
      event.preventDefault();
      event.returnValue = "";
    };
    window.addEventListener("beforeunload", preventAccidentalClose);
    return () => window.removeEventListener("beforeunload", preventAccidentalClose);
  }, [dirty]);

  const cancelDiscard = useCallback(() => {
    pendingNavigation.current = null;
    setConfirmingDiscard(false);
  }, []);

  const confirmDiscard = useCallback(() => {
    const navigate = pendingNavigation.current;
    pendingNavigation.current = null;
    setConfirmingDiscard(false);
    setDirty(false);
    navigate?.();
  }, []);

  return (
    <ConfigNavigationGuardProvider value={value}>
      {children}
      <ConfirmDialog
        cancelLabel="Keep editing"
        confirmLabel="Discard changes"
        danger
        message="This configuration has unsaved changes. Discard them and continue?"
        onCancel={cancelDiscard}
        onConfirm={confirmDiscard}
        open={confirmingDiscard}
        title="Discard unsaved changes?"
      />
    </ConfigNavigationGuardProvider>
  );
}

export function useConfigNavigationGuard() {
  return useContext(ConfigNavigationGuardContext);
}

export function useReportConfigDirty(dirty: boolean) {
  const { reportDirty } = useConfigNavigationGuard();

  useLayoutEffect(() => {
    reportDirty(dirty);
    return () => reportDirty(false);
  }, [dirty, reportDirty]);
}
