import { useCallback, useEffect, useMemo, useState } from "react";

import { createDesktopClient } from "@source-inc/gents-desktop-client";
import { listen } from "@tauri-apps/api/event";
import { ChatWorkspace } from "./components/ChatWorkspace";
import { AppNavigation } from "./components/AppNavigation";
import { CodeContextHeader } from "./components/code/CodeContextHeader";
import { ConfigWorkspace } from "./components/ConfigWorkspace";
import { ErrorBoundary } from "./components/ErrorBoundary";
import { FleetHostDashboard } from "./components/fleet/FleetHostDashboard";
import { ErrorBanner } from "./components/ErrorBanner";
import { applyTheme, loadTheme } from "./lib/theme";
import { applyShellPlatform } from "./lib/shellPlatform";
import { ShortcutsHelp } from "./components/ShortcutsHelp";
import { useAppShortcuts } from "./hooks/useAppShortcuts";
import { useMobileBackSwipe } from "./hooks/useMobileBackSwipe";
import { Sidebar } from "./components/Sidebar";
import { StartupScreen } from "./components/StartupScreen";
import {
  ConfigNavigationGuardBoundary,
  useConfigNavigationGuard,
} from "./components/config/ConfigNavigationGuard";
import { useDesktopShell, type DesktopShellBridge } from "./hooks/useDesktopShell";
import {
  useAppNavigation,
  type MobileChatPane,
  type WorkspaceView,
} from "./hooks/useAppNavigation";
import { installExternalLinkGuard } from "./lib/externalLinks";
import {
  loadNavigationExpanded,
  saveNavigationExpanded,
} from "./lib/navigationPreference";
import { startNativeSimulatorE2e } from "./lib/nativeSimulatorE2e";
import "./App.css";

function App({ bridge }: { bridge?: DesktopShellBridge } = {}) {
  return (
    <ErrorBoundary>
      <ConfigNavigationGuardBoundary>
        <AppShell bridge={bridge} />
      </ConfigNavigationGuardBoundary>
    </ErrorBoundary>
  );
}

function AppShell({ bridge: explicitBridge }: { bridge?: DesktopShellBridge }) {
  const defaultBridge = useMemo<DesktopShellBridge>(() => {
    const client = createDesktopClient();
    return {
      api: client.api,
      listenToUpdates: (handler) => client.transport.listenClientUpdated(handler),
    };
  }, []);
  const bridge = explicitBridge ?? defaultBridge;
  const shell = useDesktopShell(bridge);
  const navigation = useAppNavigation();
  const { requestNavigation } = useConfigNavigationGuard();

  useEffect(() => {
    applyTheme(loadTheme());
    applyShellPlatform();
  }, []);

  // External links (e.g. markdown links in the transcript) must open in the
  // OS browser — an unguarded anchor click navigates the whole webview away.
  useEffect(() => installExternalLinkGuard(document), []);
  useEffect(() => {
    void startNativeSimulatorE2e();
  }, []);
  useEffect(() => {
    if (!bridge.api.stopManagedServer || !("__TAURI_INTERNALS__" in window)) return;
    let unlisten: (() => void) | undefined;
    void listen("desktop://managed-server-tray-stop", () => {
      void bridge.api.stopManagedServer?.(true);
    }).then((cleanup) => {
      unlisten = cleanup;
    });
    return () => unlisten?.();
  }, [bridge.api]);
  const [navigationExpanded, setNavigationExpanded] = useState(loadNavigationExpanded);
  const [navigationDrawerOpen, setNavigationDrawerOpen] = useState(false);
  const [shortcutsOpen, setShortcutsOpen] = useState(false);
  const closeNavigationDrawer = useCallback(() => setNavigationDrawerOpen(false), []);
  const openNavigationDrawer = useCallback(() => setNavigationDrawerOpen(true), []);
  const showShortcuts = useCallback(() => setShortcutsOpen(true), []);
  const toggleNavigationExpanded = useCallback(() => {
    setNavigationExpanded((expanded) => {
      saveNavigationExpanded(!expanded);
      return !expanded;
    });
  }, []);

  const navigateTo = useCallback(
    (view: WorkspaceView, mobileChatPane?: MobileChatPane) => {
      if (view !== "fleet" && !shell.selectedDeployment) return;
      requestNavigation(() => {
        navigation.navigate(view, { mobileChatPane });
        setNavigationDrawerOpen(false);
      });
    },
    [navigation.navigate, requestNavigation, shell.selectedDeployment],
  );

  const navigateBack = useCallback(() => {
    requestNavigation(navigation.back);
  }, [navigation.back, requestNavigation]);

  useMobileBackSwipe(navigation.view !== "fleet", navigateBack);

  useAppShortcuts({
    setView: (view) => {
      navigateTo(
        view,
        view === "chat" || view === "code" ? "conversation" : "navigation",
      );
    },
    newConversation: () => {
      const behaviorId =
        shell.selectedBehaviorId ?? shell.behaviorOptions[0]?.behaviorId ?? null;
      if (behaviorId) {
        requestNavigation(() => {
          navigation.navigate("chat", { mobileChatPane: "conversation" });
          shell.onStartNewConversation(behaviorId);
        });
      }
    },
    focusComposer: () => {
      if (!shell.selectedDeployment) return;
      requestNavigation(() => {
        navigation.navigate("chat", { mobileChatPane: "conversation" });
        requestAnimationFrame(() => {
          document
            .querySelector<HTMLTextAreaElement>('[data-testid="composer-input"]')
            ?.focus();
        });
      });
    },
    toggleHelp: () => setShortcutsOpen((open) => !open),
  });

  function openChat(agentDid?: string) {
    if (agentDid) {
      shell.setSelectedAgentDid(agentDid);
    }
    // Fleet selects an agent instance first. On narrow screens the sidebar is
    // that instance view (behaviors + conversations); opening the conversation
    // pane here made it impossible to reach that navigation from Fleet.
    requestNavigation(() =>
      navigation.navigate("chat", { mobileChatPane: "navigation" }),
    );
  }

  function openCode(agentDid?: string) {
    if (agentDid) {
      shell.setSelectedAgentDid(agentDid);
    }
    requestNavigation(() =>
      navigation.navigate("code", { mobileChatPane: "conversation" }),
    );
  }

  function openConfig(agentDid?: string) {
    if (agentDid) {
      shell.setSelectedAgentDid(agentDid);
    }
    requestNavigation(() => navigation.navigate("config"));
  }

  return (
    <main className={`app-shell app-view-${navigation.view}`}>
      <div aria-hidden="true" className="titlebar-drag-region" data-tauri-drag-region />
      {shell.error && shell.startupPhase === "ready" ? (
        <ErrorBanner message={shell.error} onDismiss={shell.onDismissError} />
      ) : null}
      <ShortcutsHelp open={shortcutsOpen} onClose={() => setShortcutsOpen(false)} />

      {shell.startupPhase !== "ready" ? (
        <StartupScreen
          error={shell.error}
          onRetry={shell.onRetryStartup}
          phase={shell.startupPhase}
        />
      ) : (
        <section className="app-ready-shell">
          <AppNavigation
            currentView={navigation.view}
            deploymentAvailable={Boolean(shell.selectedDeployment)}
            drawerOpen={navigationDrawerOpen}
            expanded={navigationExpanded}
            onCloseDrawer={closeNavigationDrawer}
            onNavigate={(view) =>
              navigateTo(view, view === "code" ? "conversation" : "navigation")
            }
            onOpenDrawer={openNavigationDrawer}
            onShowShortcuts={showShortcuts}
            onToggleExpanded={toggleNavigationExpanded}
          />
          <div className="app-ready-content">
            {navigation.view === "fleet" ? (
              <FleetHostDashboard
                api={bridge.api}
                addingPeer={shell.addingPeer}
                bootstrap={shell.snapshot?.bootstrap ?? null}
                deployments={shell.deployments}
                loading={shell.loading}
                p2pHealth={shell.runtimeHealth}
                repairingP2P={shell.repairingP2P}
                starting={shell.starting}
                onAddPeer={shell.onAddPeer}
                onPairBearer={shell.onPairBearer}
                onProbePeerAddress={shell.onProbePeerAddress}
                onInitLocalRuntime={shell.onInitLocalRuntime}
                onStartManagedServer={
                  bridge.api.startManagedServer
                    ? (agentName) => bridge.api.startManagedServer!(agentName)
                    : undefined
                }
                onCommitManagedServerAutoStart={
                  bridge.api.commitManagedServerAutoStart
                    ? (agentName) => bridge.api.commitManagedServerAutoStart!(agentName)
                    : undefined
                }
                onOpenChat={openChat}
                onOpenCode={openCode}
                onOpenConfig={openConfig}
                onRemovePeer={shell.onRemovePeer}
                onRenamePeer={shell.onRenamePeer}
                onRepairP2P={shell.onRepairP2P}
                onSaveBackendConfig={shell.onSaveBackendConfig}
                onSaveBehaviorConfig={shell.onSaveBehaviorConfig}
                onProbeInferenceEndpoint={shell.onProbeInferenceEndpoint}
                onCodexLogin={shell.onCodexLogin}
                onCancelCodexLogin={shell.onCancelCodexLogin}
                onGrokLogin={shell.onGrokLogin}
                onCancelGrokLogin={shell.onCancelGrokLogin}
              />
            ) : navigation.view === "chat" || navigation.view === "code" ? (
              <section
                className={`workspace mobile-chat-pane-${navigation.mobileChatPane}`}
                data-mobile-chat-pane={navigation.mobileChatPane}
              >
                <Sidebar
                  behaviorOptions={shell.behaviorOptions}
                  conversations={shell.selectedDeployment?.conversations ?? []}
                  deployments={shell.deployments}
                  onSelectBehavior={shell.setSelectedBehaviorId}
                  onSelectAgent={(agentDid) => {
                    shell.setSelectedAgentDid(agentDid);
                    shell.setSelectedSessionId(null);
                  }}
                  onSelectSession={shell.onSelectSession}
                  onOpenSession={(sessionId) => {
                    shell.onSelectSession(sessionId);
                    navigation.showConversation();
                  }}
                  onRenameConversationTitle={shell.onRenameConversationTitle}
                  onSyncConversations={shell.onRepairP2P}
                  syncingConversations={shell.repairingP2P}
                  onStartNewConversation={(behaviorId) => {
                    shell.onStartNewConversation(behaviorId);
                    navigation.showConversation();
                  }}
                  selectedAgentDid={shell.selectedAgentDid}
                  selectedBehaviorId={shell.selectedBehaviorId}
                  selectedSessionId={shell.selectedSessionId}
                />

                <section className="chat-column">
                  {navigation.view === "code" ? (
                    <CodeContextHeader
                      deployment={shell.selectedDeployment ?? null}
                      selectedBehaviorId={shell.selectedBehaviorId}
                      onBackToChat={() =>
                        navigation.navigate("chat", {
                          mobileChatPane: "conversation",
                          replace: true,
                        })
                      }
                    />
                  ) : null}
                  <ChatWorkspace
                    api={bridge.api}
                    activeRequestId={
                      shell.activeRequestId ?? shell.session?.latestRequestId ?? null
                    }
                    approxSerializedBytes={
                      shell.snapshot?.client?.approxSerializedBytes ?? 0
                    }
                    canSend={shell.canSendMessage}
                    configuredPeerCount={
                      shell.snapshot?.client?.configuredPeerCount ?? 0
                    }
                    dialedPeerCount={shell.snapshot?.client?.dialedPeerCount ?? 0}
                    draft={shell.draft}
                    interruptVisible={shell.interruptVisible}
                    onDraftChange={shell.setDraft}
                    onRenameConversationTitle={shell.onRenameConversationTitle}
                    onSend={shell.onSendMessage}
                    onRetryMessage={shell.onRetryMessage}
                    rowCount={shell.snapshot?.client?.rowCount ?? 0}
                    runtimeHealth={shell.runtimeHealth}
                    sendHint={
                      shell.sendStatus.kind === "disabled"
                        ? shell.sendStatus.hint
                        : null
                    }
                    selectedBehaviorId={shell.selectedBehaviorId}
                    selectedConversationTitle={
                      shell.session
                        ? (shell.session.title ?? null)
                        : (shell.selectedConversation?.title ?? null)
                    }
                    selectedDeployment={shell.selectedDeployment}
                    selectedSessionId={shell.selectedSessionId}
                    sending={shell.sending}
                    session={shell.session}
                    turnState={shell.turnState ?? shell.session?.turnState ?? null}
                    onOpenMobileNavigation={navigation.showChatNavigation}
                    onForkedConversation={shell.onSelectSession}
                    onInterruptAccepted={async () => {
                      await shell.refreshSession(shell.selectedSessionId);
                    }}
                  />
                </section>
              </section>
            ) : (
              <section className="config-page">
                <ConfigWorkspace
                  api={bridge.api}
                  bootstrap={shell.snapshot?.bootstrap ?? null}
                  onBack={navigation.back}
                  onDeleteSkillConfig={shell.onDeleteSkillConfig}
                  onDeleteTaskConfig={shell.onDeleteTaskConfig}
                  onDeleteScheduleConfig={shell.onDeleteScheduleConfig}
                  onDeleteEventTriggerConfig={shell.onDeleteEventTriggerConfig}
                  onDeleteBackendConfig={shell.onDeleteBackendConfig}
                  onDeleteInferenceProfileConfig={shell.onDeleteInferenceProfileConfig}
                  onDeleteToolSelectionConfig={shell.onDeleteToolSelectionConfig}
                  onDeleteToolServiceConfig={shell.onDeleteToolServiceConfig}
                  onDeleteBehaviorConfig={shell.onDeleteBehaviorConfig}
                  onSaveAgentConfig={shell.onSaveAgentConfig}
                  onRunTask={shell.onRunTask}
                  onSaveBackendConfig={shell.onSaveBackendConfig}
                  onSaveBehaviorConfig={shell.onSaveBehaviorConfig}
                  onSaveEventTriggerConfig={shell.onSaveEventTriggerConfig}
                  onSaveInferenceProfileConfig={shell.onSaveInferenceProfileConfig}
                  onSaveScheduleConfig={shell.onSaveScheduleConfig}
                  onSaveSkillConfig={shell.onSaveSkillConfig}
                  onSaveTaskConfig={shell.onSaveTaskConfig}
                  onSaveToolSelectionConfig={shell.onSaveToolSelectionConfig}
                  onSaveToolServiceConfig={shell.onSaveToolServiceConfig}
                  onTestToolService={shell.onTestToolService}
                  onRunSchedule={shell.onRunSchedule}
                  runningTask={shell.runningTask}
                  saving={shell.savingConfig}
                  selectedBehaviorId={shell.selectedBehaviorId}
                  selectedDeployment={shell.selectedDeployment}
                />
              </section>
            )}
          </div>
        </section>
      )}
    </main>
  );
}

export default App;
