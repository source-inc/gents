import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type SetStateAction,
} from "react";

import {
  isTerminalTurnState,
  projectChatShell,
  reconcileProjectedWorkflow,
  type ChatWorkflowState,
  type OptimisticPendingTurn,
} from "@source-inc/gents-desktop-chat";
import {
  delay,
  logShellEvent,
  setDesktopShellTimingConfigForTests,
  timingConfig,
  trackedRequestIdForSession,
} from "./desktopShellRuntime";
import { createDesktopShellChatActions } from "./desktopShellChatActions";
import { createDesktopShellConfigActions } from "./desktopShellConfigActions";
import { useDesktopShellEffects } from "./desktopShellEffects";
import { createDesktopShellPeerActions } from "./desktopShellPeerActions";
import { createDesktopShellTaskActions } from "./desktopShellTaskActions";
import type {
  DesktopApiAdapter,
  DesktopClientUpdatedListenerFactory,
  DesktopClientSnapshot,
  DesktopSessionSnapshot,
  P2PHealth,
} from "@source-inc/gents-desktop-client";

export { setDesktopShellTimingConfigForTests };

export type DesktopShellBridge = {
  api: DesktopApiAdapter;
  listenToUpdates: DesktopClientUpdatedListenerFactory;
};

export type DesktopStartupPhase =
  | "loading-configuration"
  | "starting-client"
  | "configuration-error"
  | "client-error"
  | "ready";

export function useDesktopShell({ api, listenToUpdates }: DesktopShellBridge) {
  const autostartAttempted = useRef(false);
  const localServerAvailable = useRef<boolean | null>(null);
  const autoRestartInFlight = useRef(false);
  const lastP2PAutoRestartAt = useRef<number | null>(null);
  const lastObservedP2PHealth = useRef<P2PHealth | null>(null);
  const selectedSessionIdRef = useRef<string | null>(null);
  const selectedAgentDidRef = useRef<string | null>(null);
  const selectedTrackedRequestIdRef = useRef<string | null>(null);
  const snapshotRefreshSeq = useRef(0);
  const sessionRefreshSeq = useRef(0);
  const newConversationAgentRef = useRef<string | null>(null);
  const startupPhaseRef = useRef<DesktopStartupPhase>("loading-configuration");
  /** Coalesce concurrent shell start paths (autostart + pair + add-peer). */
  const startClientInFlight = useRef<Promise<DesktopClientSnapshot | null> | null>(
    null,
  );
  const [snapshot, setSnapshot] = useState<DesktopClientSnapshot | null>(null);
  const [session, setSession] = useState<DesktopSessionSnapshot | null>(null);
  const [startupPhase, setStartupPhaseState] = useState<DesktopStartupPhase>(
    "loading-configuration",
  );
  const [loading, setLoading] = useState(true);
  const [starting, setStarting] = useState(false);
  const [stopping, setStopping] = useState(false);
  const [sending, setSending] = useState(false);
  const [savingBehaviorConfig, setSavingBehaviorConfig] = useState(false);
  const [savingConfig, setSavingConfig] = useState(false);
  const [addingPeer, setAddingPeer] = useState(false);
  const [repairingP2P, setRepairingP2P] = useState(false);
  const [runningTask, setRunningTask] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedAgentDid, setSelectedAgentDid] = useState<string | null>(null);
  const [selectedSessionId, setSelectedSessionId] = useState<string | null>(null);
  const [selectedBehaviorId, setSelectedBehaviorId] = useState<string | null>(null);
  const [localWorkflow, setLocalWorkflow] = useState<ChatWorkflowState>({
    kind: "ready",
  });
  const [optimisticPendingTurn, setOptimisticPendingTurn] =
    useState<OptimisticPendingTurn | null>(null);
  const [draftsByContext, setDraftsByContext] = useState<Record<string, string>>({});

  const deployments = snapshot?.client?.deployments ?? [];
  const selectedDeployment =
    deployments.find((deployment) => deployment.agentDid === selectedAgentDid) ?? null;
  const draftContextKey = JSON.stringify(
    selectedSessionId
      ? ["session", selectedAgentDid, selectedSessionId]
      : [
          "new",
          selectedAgentDid,
          selectedBehaviorId ?? selectedDeployment?.defaultBehaviorId ?? null,
        ],
  );
  const draft = draftsByContext[draftContextKey] ?? "";
  const setDraft = useCallback(
    (next: SetStateAction<string>) => {
      setDraftsByContext((current) => {
        const currentDraft = current[draftContextKey] ?? "";
        const nextDraft = typeof next === "function" ? next(currentDraft) : next;
        if (nextDraft === currentDraft) {
          return current;
        }
        if (!nextDraft) {
          const remaining = { ...current };
          delete remaining[draftContextKey];
          return remaining;
        }
        return { ...current, [draftContextKey]: nextDraft };
      });
    },
    [draftContextKey],
  );
  const selectedConversation =
    selectedDeployment?.conversations.find(
      (conversation) => conversation.sessionId === selectedSessionId,
    ) ?? null;
  const behaviorOptions = selectedDeployment?.behaviors ?? [];
  const runtimeHealth = snapshot?.client?.p2pHealth ?? null;
  const shellProjection = useMemo(
    () =>
      projectChatShell({
        clientAvailable: Boolean(snapshot?.client),
        selectedAgentDid,
        selectedSessionId,
        draft,
        sending,
        session,
        selectedConversation,
        localWorkflow,
      }),
    [
      draft,
      localWorkflow,
      selectedAgentDid,
      selectedConversation,
      selectedSessionId,
      sending,
      session,
      snapshot?.client,
    ],
  );
  const canSendMessage = shellProjection.sendStatus.kind === "ready";

  function setStartupPhase(next: DesktopStartupPhase) {
    startupPhaseRef.current = next;
    setStartupPhaseState(next);
  }

  useEffect(() => {
    setLocalWorkflow((current) =>
      reconcileProjectedWorkflow(current, shellProjection.workflow),
    );
  }, [shellProjection.workflow]);

  useEffect(() => {
    setOptimisticPendingTurn((current) => {
      if (!current || current.sessionId !== session?.sessionId) {
        return current;
      }
      const durableOwner = session.timelineItems.some(
        (item) =>
          (item.kind === "pendingUserTurn" && item.requestId === current.requestId) ||
          (item.kind === "userMessage" && item.requestId === current.requestId),
      );
      return durableOwner ? null : current;
    });
  }, [session]);

  const selectedTrackedRequestId =
    trackedRequestIdForSession(selectedSessionId, shellProjection.workflow) ??
    (!isTerminalTurnState(shellProjection.turnState)
      ? shellProjection.activeRequestId
      : null);
  selectedAgentDidRef.current = selectedAgentDid;
  selectedSessionIdRef.current = selectedSessionId;
  selectedTrackedRequestIdRef.current = selectedTrackedRequestId;

  async function refreshSnapshot() {
    const refreshSeq = snapshotRefreshSeq.current + 1;
    snapshotRefreshSeq.current = refreshSeq;
    const resolvingConfiguration = startupPhaseRef.current === "loading-configuration";
    setLoading(true);
    try {
      const next = await api.fetchDesktopSnapshot();
      if (snapshotRefreshSeq.current === refreshSeq) {
        setSnapshot(next);
        setError(null);
        if (resolvingConfiguration) {
          setStartupPhase(
            next.client || next.bootstrap.savedPeers.length === 0
              ? "ready"
              : "starting-client",
          );
        }
      }
    } catch (err) {
      if (snapshotRefreshSeq.current === refreshSeq) {
        setError(String(err));
        if (resolvingConfiguration) {
          setStartupPhase("configuration-error");
        }
      }
    } finally {
      if (snapshotRefreshSeq.current === refreshSeq) {
        setLoading(false);
      }
    }
  }

  async function refreshSession(
    nextSessionId: string | null,
  ): Promise<DesktopSessionSnapshot | null> {
    const refreshSeq = sessionRefreshSeq.current + 1;
    sessionRefreshSeq.current = refreshSeq;

    if (!nextSessionId) {
      if (sessionRefreshSeq.current === refreshSeq) {
        setSession(null);
      }
      return null;
    }

    try {
      const next = await api.fetchSessionSnapshot(
        nextSessionId,
        selectedAgentDidRef.current,
        selectedTrackedRequestIdRef.current,
      );
      if (sessionRefreshSeq.current === refreshSeq) {
        setSession(next);
      }
      return next;
    } catch (err) {
      if (sessionRefreshSeq.current === refreshSeq) {
        setError(String(err));
      }
      return null;
    }
  }

  async function ensureDesktopClientStarted(): Promise<DesktopClientSnapshot | null> {
    if (startClientInFlight.current) {
      return startClientInFlight.current;
    }

    setStarting(true);
    setError(null);
    const pending = (async () => {
      let started = false;
      try {
        const next = await api.startDesktopClient();
        setSnapshot(next);
        started = true;
        return next;
      } catch (err) {
        setError(String(err));
        return null;
      } finally {
        if (startupPhaseRef.current === "starting-client") {
          setStartupPhase(started ? "ready" : "client-error");
        }
        startClientInFlight.current = null;
        setStarting(false);
      }
    })();
    startClientInFlight.current = pending;
    return pending;
  }

  async function onStartClient() {
    await ensureDesktopClientStarted();
  }

  async function onRetryStartup() {
    autostartAttempted.current = false;
    setStartupPhase("loading-configuration");
    await refreshSnapshot();
  }

  async function restartDesktopClient(reason: string) {
    if (autoRestartInFlight.current) {
      return;
    }

    autoRestartInFlight.current = true;
    const sessionId = selectedSessionIdRef.current;
    logShellEvent(`restart begin reason="${reason}" sessionId=${sessionId ?? "none"}`);
    setStopping(true);
    setStarting(true);
    setError(null);

    try {
      let next: DesktopClientSnapshot | null = null;
      for (
        let attempt = 1;
        attempt <= timingConfig().clientRestartMaxAttempts;
        attempt += 1
      ) {
        try {
          logShellEvent(`restart attempt=${attempt} phase=shutdown`);
          await api.shutdownDesktopClient();
          logShellEvent(`restart attempt=${attempt} phase=start`);
          next = await api.startDesktopClient();
          logShellEvent(`restart attempt=${attempt} phase=started`);
          break;
        } catch (err) {
          logShellEvent(`restart attempt=${attempt} failed error=${String(err)}`);
          if (attempt === timingConfig().clientRestartMaxAttempts) {
            throw err;
          }
          await delay(timingConfig().clientRestartBackoffMs);
        }
      }

      if (!next) {
        throw new Error("desktop restart returned no snapshot");
      }

      setSnapshot(next);
      if (sessionId) {
        await refreshSession(sessionId);
      } else {
        setSession(null);
      }
      logShellEvent(`restart complete reason="${reason}"`);
    } catch (err) {
      logShellEvent(`restart failed reason="${reason}" error=${String(err)}`);
      setError(`desktop client restart failed after ${reason}: ${String(err)}`);
    } finally {
      setStopping(false);
      setStarting(false);
      autoRestartInFlight.current = false;
    }
  }

  useDesktopShellEffects({
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
    onStartClient,
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
  });

  const {
    onAddPeer,
    onFetchPeerStatus,
    onProbePeerAddress,
    onInitLocalRuntime,
    onPairBearer,
    onRemovePeer,
    onRenamePeer,
    onRepairP2P,
  } = createDesktopShellPeerActions({
    api,
    snapshot,
    ensureDesktopClientStarted,
    setAddingPeer,
    setError,
    setRepairingP2P,
    setSelectedAgentDid,
    setSnapshot,
    setStarting,
  });
  const foregroundRepairRef = useRef(onRepairP2P);
  const foregroundRepairEnabledRef = useRef(Boolean(snapshot?.client));
  foregroundRepairRef.current = onRepairP2P;
  foregroundRepairEnabledRef.current = Boolean(snapshot?.client);

  useEffect(() => {
    function repairAfterForeground() {
      if (
        document.visibilityState === "visible" &&
        foregroundRepairEnabledRef.current
      ) {
        void foregroundRepairRef.current().catch(() => {});
      }
    }

    document.addEventListener("visibilitychange", repairAfterForeground);
    return () =>
      document.removeEventListener("visibilitychange", repairAfterForeground);
  }, []);

  const {
    onSaveAgentConfig,
    onSaveBackendConfig,
    onSaveBehaviorConfig,
    onDeleteSkillConfig,
    onDeleteTaskConfig,
    onDeleteScheduleConfig,
    onDeleteEventTriggerConfig,
    onDeleteBackendConfig,
    onDeleteInferenceProfileConfig,
    onDeleteToolSelectionConfig,
    onDeleteToolServiceConfig,
    onDeleteBehaviorConfig,
    onProbeInferenceEndpoint,
    onCodexLogin,
    onCancelCodexLogin,
    onGrokLogin,
    onCancelGrokLogin,
    onSaveInferenceProfileConfig,
    onSaveSkillConfig,
    onSaveToolSelectionConfig,
    onSaveToolServiceConfig,
    onTestToolService,
  } = createDesktopShellConfigActions({
    api,
    setError,
    setSavingBehaviorConfig,
    setSavingConfig,
    setSelectedAgentDid,
    setSelectedBehaviorId,
    setSnapshot,
  });

  const {
    onRenameConversationTitle,
    onRetryMessage,
    onSelectSession,
    onSendMessage,
    onStartNewConversation,
  } = createDesktopShellChatActions({
    api,
    draft,
    newConversationAgentRef,
    refreshSession,
    refreshSnapshot,
    selectedBehaviorId,
    selectedDeployment,
    selectedSessionId,
    session,
    setDraft,
    setError,
    setLocalWorkflow,
    setOptimisticPendingTurn,
    setSelectedBehaviorId,
    setSelectedSessionId,
    setSending,
    setSession,
    shellProjection,
  });

  const {
    onRunSchedule,
    onRunTask,
    onSaveEventTriggerConfig,
    onSaveScheduleConfig,
    onSaveTaskConfig,
  } = createDesktopShellTaskActions({
    api,
    refreshSession,
    refreshSnapshot,
    setError,
    setRunningTask,
    setSavingConfig,
    setSelectedSessionId,
    setSnapshot,
  });

  function onDismissError() {
    setError(null);
  }

  return {
    snapshot,
    session,
    optimisticPendingTurn,
    startupPhase,
    loading,
    starting,
    stopping,
    sending,
    savingBehaviorConfig,
    savingConfig,
    addingPeer,
    repairingP2P,
    runningTask,
    error,
    onDismissError,
    onRetryStartup,
    selectedAgentDid,
    selectedSessionId,
    selectedBehaviorId,
    draft,
    deployments,
    selectedDeployment,
    selectedConversation,
    behaviorOptions,
    runtimeHealth,
    canSendMessage,
    chatWorkflow: shellProjection.workflow,
    activeRequestId: shellProjection.activeRequestId,
    turnState: shellProjection.turnState,
    interruptVisible:
      shellProjection.workflow.kind === "awaitingObservation" ||
      shellProjection.workflow.kind === "turnInProgress",
    sendStatus: shellProjection.sendStatus,
    setSelectedAgentDid,
    setSelectedSessionId,
    setSelectedBehaviorId,
    setDraft,
    onSelectSession,
    onStartNewConversation,
    refreshSession,
    refreshSnapshot,
    onAddPeer,
    onPairBearer,
    onRemovePeer,
    onRenamePeer,
    onFetchPeerStatus,
    onProbePeerAddress,
    onInitLocalRuntime,
    onRepairP2P,
    onSendMessage,
    onRetryMessage,
    onRenameConversationTitle,
    onSaveAgentConfig,
    onSaveBehaviorConfig,
    onDeleteSkillConfig,
    onDeleteTaskConfig,
    onDeleteScheduleConfig,
    onDeleteEventTriggerConfig,
    onDeleteBackendConfig,
    onDeleteInferenceProfileConfig,
    onDeleteToolSelectionConfig,
    onDeleteToolServiceConfig,
    onDeleteBehaviorConfig,
    onSaveSkillConfig,
    onSaveBackendConfig,
    onProbeInferenceEndpoint,
    onCodexLogin,
    onCancelCodexLogin,
    onGrokLogin,
    onCancelGrokLogin,
    onSaveInferenceProfileConfig,
    onSaveToolSelectionConfig,
    onSaveToolServiceConfig,
    onTestToolService,
    onSaveTaskConfig,
    onSaveScheduleConfig,
    onRunSchedule,
    onSaveEventTriggerConfig,
    onRunTask,
  };
}
