import { useEffect, type MutableRefObject } from "react";

import type { ChatWorkflowState } from "@source-inc/gents-desktop-chat";
import { conversationBelongsToBehavior } from "@source-inc/gents-desktop-chat";
import type {
  DeploymentView,
  DesktopApiAdapter,
  DesktopClientUpdatedListenerFactory,
  DesktopClientSnapshot,
  DesktopSessionSnapshot,
  P2PHealth,
} from "@source-inc/gents-desktop-client";
import {
  createTrailingRefreshQueue,
  logShellEvent,
  shouldAutoRestartP2P,
  timingConfig,
} from "./desktopShellRuntime";
import { listenToDesktopClientUpdates } from "@source-inc/gents-desktop-client";

type DesktopShellEffectsArgs = {
  api: DesktopApiAdapter;
  autoRestartInFlight: MutableRefObject<boolean>;
  autostartAttempted: MutableRefObject<boolean>;
  deployments: DeploymentView[];
  lastObservedP2PHealth: MutableRefObject<P2PHealth | null>;
  lastP2PAutoRestartAt: MutableRefObject<number | null>;
  localWorkflow: ChatWorkflowState;
  localServerAvailable: MutableRefObject<boolean | null>;
  listenToUpdates: DesktopClientUpdatedListenerFactory;
  newConversationAgentRef: MutableRefObject<string | null>;
  refreshSession: (sessionId: string | null) => Promise<DesktopSessionSnapshot | null>;
  refreshSnapshot: () => Promise<void>;
  restartDesktopClient: (reason: string) => Promise<void>;
  runtimeHealth: P2PHealth | null;
  selectedAgentDid: string | null;
  selectedBehaviorId: string | null;
  selectedDeployment: DeploymentView | null;
  selectedSessionId: string | null;
  selectedSessionIdRef: MutableRefObject<string | null>;
  selectedTrackedRequestId: string | null;
  sending: boolean;
  setLocalWorkflow: (workflow: ChatWorkflowState) => void;
  setError: (error: string | null) => void;
  setSelectedAgentDid: (agentDid: string | null) => void;
  setSelectedBehaviorId: (behaviorId: string | null) => void;
  setSelectedSessionId: (sessionId: string | null) => void;
  snapshot: DesktopClientSnapshot | null;
  starting: boolean;
  stopping: boolean;
  onStartClient: () => Promise<void>;
};

export function useDesktopShellEffects({
  api,
  autoRestartInFlight,
  autostartAttempted,
  deployments,
  lastObservedP2PHealth,
  lastP2PAutoRestartAt,
  localWorkflow,
  localServerAvailable,
  listenToUpdates,
  newConversationAgentRef,
  refreshSession,
  refreshSnapshot,
  restartDesktopClient,
  runtimeHealth,
  selectedAgentDid,
  selectedBehaviorId,
  selectedDeployment,
  selectedSessionId,
  selectedSessionIdRef,
  selectedTrackedRequestId,
  sending,
  setLocalWorkflow,
  setError,
  setSelectedAgentDid,
  setSelectedBehaviorId,
  setSelectedSessionId,
  snapshot,
  starting,
  stopping,
  onStartClient,
}: DesktopShellEffectsArgs) {
  const clientAvailable = Boolean(snapshot?.client);

  useEffect(() => {
    selectedSessionIdRef.current = selectedSessionId;
  }, [selectedSessionId, selectedSessionIdRef]);

  useEffect(() => {
    let cancelled = false;
    void restoreManagedServer(api)
      .then((available) => {
        localServerAvailable.current = available;
      })
      .catch((error) => {
        if (!cancelled) setError(String(error));
      })
      .finally(() => {
        if (!cancelled) void refreshSnapshot();
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!snapshot || snapshot.client || starting || sending) {
      return;
    }

    if (!shouldAutoStartDesktopClient(snapshot, localServerAvailable.current)) {
      return;
    }

    if (autostartAttempted.current) {
      return;
    }

    autostartAttempted.current = true;
    void onStartClient();
  }, [
    autostartAttempted,
    localServerAvailable,
    onStartClient,
    sending,
    snapshot,
    starting,
  ]);

  useEffect(() => {
    const previousHealth = lastObservedP2PHealth.current;
    lastObservedP2PHealth.current = runtimeHealth;

    if (!runtimeHealth) {
      return;
    }

    if (runtimeHealth.status === "healthy") {
      lastP2PAutoRestartAt.current = null;
      return;
    }

    if (
      autoRestartInFlight.current ||
      starting ||
      stopping ||
      sending ||
      !shouldAutoRestartP2P(
        previousHealth,
        runtimeHealth,
        lastP2PAutoRestartAt.current,
        Date.now(),
        timingConfig().p2pAutoRestartCooldownMs,
      )
    ) {
      return;
    }

    lastP2PAutoRestartAt.current = Date.now();
    logShellEvent(
      `auto restart requested reason="P2P transport wedged" status=${runtimeHealth.status} failures=${runtimeHealth.consecutiveFailures}`,
    );
    void restartDesktopClient("P2P transport wedged");
  }, [
    autoRestartInFlight,
    lastObservedP2PHealth,
    lastP2PAutoRestartAt,
    restartDesktopClient,
    runtimeHealth,
    sending,
    starting,
    stopping,
  ]);

  useEffect(() => {
    let disposed = false;
    let unlisten: (() => void) | undefined;
    const refreshQueue = createTrailingRefreshQueue(async () => {
      await refreshSnapshot();
      const sessionId = selectedSessionIdRef.current;
      if (sessionId) {
        await refreshSession(sessionId);
      }
    });

    const reportListenerError = (listenerError: unknown) => {
      if (disposed) {
        return;
      }
      const message =
        listenerError instanceof Error ? listenerError.message : String(listenerError);
      logShellEvent(`desktop update listener failed: ${message}`);
      setError(message);
    };

    void listenToDesktopClientUpdates(
      async () => {
        if (disposed) {
          return;
        }
        await refreshQueue.request();
      },
      reportListenerError,
      listenToUpdates,
    )
      .then((cleanup) => {
        if (disposed) {
          cleanup();
          return;
        }
        unlisten = cleanup;
      })
      .catch(reportListenerError);

    return () => {
      disposed = true;
      refreshQueue.dispose();
      unlisten?.();
    };
  }, [
    listenToUpdates,
    selectedAgentDid,
    selectedSessionId,
    selectedTrackedRequestId,
    setError,
  ]);

  useEffect(() => {
    if (!deployments.length) {
      setSelectedAgentDid(null);
      return;
    }

    if (
      selectedAgentDid &&
      deployments.some((deployment) => deployment.agentDid === selectedAgentDid)
    ) {
      return;
    }

    setSelectedAgentDid(deployments[0].agentDid);
  }, [deployments, selectedAgentDid, setSelectedAgentDid]);

  useEffect(() => {
    if (!clientAvailable) {
      return;
    }

    let disposed = false;
    void api.setSelectedAgent(selectedAgentDid).catch((err) => {
      if (disposed) {
        return;
      }
      setError(String(err));
    });

    return () => {
      disposed = true;
    };
  }, [api, clientAvailable, selectedAgentDid, setError]);

  useEffect(() => {
    if (!selectedDeployment) {
      setSelectedBehaviorId(null);
      setSelectedSessionId(null);
      return;
    }

    const defaultBehaviorId =
      selectedDeployment.defaultBehaviorId ??
      selectedDeployment.behaviors.find((behavior) => behavior.isDefault)?.behaviorId ??
      selectedDeployment.behaviors[0]?.behaviorId ??
      null;
    const effectiveBehaviorId =
      selectedBehaviorId &&
      selectedDeployment.behaviors.some(
        (behavior) => behavior.behaviorId === selectedBehaviorId,
      )
        ? selectedBehaviorId
        : defaultBehaviorId;

    if (selectedBehaviorId !== effectiveBehaviorId) {
      setSelectedBehaviorId(defaultBehaviorId);
    }

    if (
      selectedSessionId &&
      (selectedDeployment.conversations.some(
        (conversation) =>
          conversation.sessionId === selectedSessionId &&
          conversationBelongsToBehavior(
            conversation,
            effectiveBehaviorId,
            defaultBehaviorId,
          ),
      ) ||
        ((localWorkflow.kind === "awaitingObservation" ||
          localWorkflow.kind === "turnInProgress") &&
          localWorkflow.agentDid === selectedDeployment.agentDid &&
          localWorkflow.sessionId === selectedSessionId))
    ) {
      newConversationAgentRef.current = null;
      return;
    }

    if (
      !selectedSessionId &&
      newConversationAgentRef.current === selectedDeployment.agentDid
    ) {
      return;
    }

    setSelectedSessionId(
      selectedDeployment.conversations.find((conversation) =>
        conversationBelongsToBehavior(
          conversation,
          effectiveBehaviorId,
          defaultBehaviorId,
        ),
      )?.sessionId ?? null,
    );
  }, [
    localWorkflow,
    newConversationAgentRef,
    selectedBehaviorId,
    selectedDeployment,
    selectedSessionId,
    setSelectedBehaviorId,
    setSelectedSessionId,
  ]);

  useEffect(() => {
    void refreshSession(selectedSessionId);
  }, [selectedAgentDid, selectedSessionId, selectedTrackedRequestId]);

  useEffect(() => {
    if (!clientAvailable || !selectedSessionId || !selectedTrackedRequestId) {
      return;
    }

    let disposed = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const poll = async () => {
      await refreshSession(selectedSessionId);
      if (!disposed) {
        timer = setTimeout(poll, timingConfig().activeSessionPollMs);
      }
    };
    timer = setTimeout(poll, timingConfig().activeSessionPollMs);

    return () => {
      disposed = true;
      if (timer) clearTimeout(timer);
    };
  }, [clientAvailable, selectedAgentDid, selectedSessionId, selectedTrackedRequestId]);

  useEffect(() => {
    if (!clientAvailable) {
      return;
    }

    const refreshForegroundState = () => {
      if (document.visibilityState === "hidden") {
        return;
      }
      void refreshSnapshot();
      if (selectedSessionId) {
        void refreshSession(selectedSessionId);
      }
    };
    const onVisibilityChange = () => {
      if (document.visibilityState === "visible") {
        refreshForegroundState();
      }
    };

    document.addEventListener("visibilitychange", onVisibilityChange);
    window.addEventListener("focus", refreshForegroundState);
    return () => {
      document.removeEventListener("visibilitychange", onVisibilityChange);
      window.removeEventListener("focus", refreshForegroundState);
    };
  }, [clientAvailable, selectedSessionId, selectedTrackedRequestId]);

  useEffect(() => {
    if (localWorkflow.kind === "submittingRequest" && !sending) {
      setLocalWorkflow({ kind: "ready" });
    }
  }, [localWorkflow, sending, setLocalWorkflow]);
}

const managedServerRestoreInFlight = new WeakMap<
  DesktopApiAdapter,
  Promise<boolean | null>
>();

export function restoreManagedServer(api: DesktopApiAdapter): Promise<boolean | null> {
  const existing = managedServerRestoreInFlight.get(api);
  if (existing) return existing;

  const pending = restoreManagedServerOnce(api).finally(() => {
    if (managedServerRestoreInFlight.get(api) === pending) {
      managedServerRestoreInFlight.delete(api);
    }
  });
  managedServerRestoreInFlight.set(api, pending);
  return pending;
}

async function restoreManagedServerOnce(
  api: DesktopApiAdapter,
): Promise<boolean | null> {
  if (!api.managedServerStatus || !api.startManagedServer) return null;

  const status = await api.managedServerStatus();
  if (status.state === "running" || status.state === "external") {
    return true;
  }
  if (!status.autoStart) {
    return false;
  }

  await api.startManagedServer(status.agentName?.trim() || "Local Agent");
  return true;
}

export function shouldAutoStartDesktopClient(
  snapshot: DesktopClientSnapshot,
  localServerAvailable: boolean | null,
): boolean {
  return snapshot.bootstrap.savedPeers.some(
    (peer) => peer.source !== "local-standard" || localServerAvailable !== false,
  );
}
