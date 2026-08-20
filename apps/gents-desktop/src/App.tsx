import { useCallback, useEffect, useMemo, useState } from "react";

import { createDesktopClient } from "@source-inc/gents-desktop-client";
import { ConfirmDialog } from "@source-inc/gents-desktop-ui";
import { listen } from "@tauri-apps/api/event";
import { ChatWorkspace } from "./components/ChatWorkspace";
import { CodeContextHeader } from "./components/code/CodeContextHeader";
import { ConfigWorkspace } from "./components/ConfigWorkspace";
import { useConfigNavigationController } from "./components/config/ConfigNavigationGuard";
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
import { useDesktopShell, type DesktopShellBridge } from "./hooks/useDesktopShell";
import { installExternalLinkGuard } from "./lib/externalLinks";
import { startNativeSimulatorE2e } from "./lib/nativeSimulatorE2e";
import "./App.css";

function App({ bridge }: { bridge?: DesktopShellBridge } = {}) {
  return (
    <ErrorBoundary>
      <AppShell bridge={bridge} />
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
  const [workspaceView, setWorkspaceView] = useState<
    "fleet" | "chat" | "config" | "code"
  >("fleet");
  const [mobileChatPane, setMobileChatPane] = useState<"navigation" | "conversation">(
    "navigation",
  );
  const [configReturnView, setConfigReturnView] = useState<"fleet" | "chat">("fleet");
  const [shortcutsOpen, setShortcutsOpen] = useState(false);
  const configNavigation = useConfigNavigationController();
  const requestConfigNavigation = configNavigation.requestNavigation;

  const requestWorkspaceNavigation = useCallback(
    (navigate: () => void) => {
      if (workspaceView === "config") {
        requestConfigNavigation(navigate);
      } else {
        navigate();
      }
    },
    [requestConfigNavigation, workspaceView],
  );

  const navigateBack = useCallback(() => {
    if (workspaceView === "config") {
      requestConfigNavigation(() => {
        setWorkspaceView(configReturnView);
        if (configReturnView === "chat") {
          setMobileChatPane("navigation");
        }
      });
      return;
    }
    if (workspaceView === "code") {
      setWorkspaceView("chat");
      setMobileChatPane("navigation");
      return;
    }
    if (workspaceView === "chat" && mobileChatPane === "conversation") {
      setMobileChatPane("navigation");
      return;
    }
    if (workspaceView === "chat") {
      setWorkspaceView("fleet");
    }
  }, [configReturnView, mobileChatPane, requestConfigNavigation, workspaceView]);

  useMobileBackSwipe(workspaceView !== "fleet", navigateBack);

  useAppShortcuts({
    setView: (view) => {
      if (view === "config") {
        if (workspaceView !== "config") {
          openConfig();
        }
        return;
      }
      requestWorkspaceNavigation(() => {
        if (view === "chat" || view === "code") {
          setMobileChatPane("conversation");
        }
        setWorkspaceView(view);
      });
    },
    newConversation: () => {
      const behaviorId =
        shell.selectedBehaviorId ?? shell.behaviorOptions[0]?.behaviorId ?? null;
      if (behaviorId) {
        requestWorkspaceNavigation(() => {
          setWorkspaceView("chat");
          setMobileChatPane("conversation");
          shell.onStartNewConversation(behaviorId);
        });
      }
    },
    focusComposer: () => {
      requestWorkspaceNavigation(() => {
        setWorkspaceView("chat");
        setMobileChatPane("conversation");
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
    requestWorkspaceNavigation(() => {
      if (agentDid) {
        shell.setSelectedAgentDid(agentDid);
      }
      // Fleet selects an agent instance first. On narrow screens the sidebar is
      // that instance view (behaviors + conversations); opening the conversation
      // pane here made it impossible to reach that navigation from Fleet.
      setMobileChatPane("navigation");
      setWorkspaceView("chat");
    });
  }

  function openCode(agentDid?: string) {
    requestWorkspaceNavigation(() => {
      if (agentDid) {
        shell.setSelectedAgentDid(agentDid);
      }
      setMobileChatPane("conversation");
      setWorkspaceView("code");
    });
  }

  function openConfig(agentDid?: string) {
    if (agentDid) {
      shell.setSelectedAgentDid(agentDid);
    }
    setConfigReturnView(workspaceView === "fleet" ? "fleet" : "chat");
    setWorkspaceView("config");
  }

  return (
    <main className={`app-shell app-view-${workspaceView}`}>
      <div aria-hidden="true" className="titlebar-drag-region" data-tauri-drag-region />
      {shell.error && shell.startupPhase === "ready" ? (
        <ErrorBanner message={shell.error} onDismiss={shell.onDismissError} />
      ) : null}
      <ShortcutsHelp open={shortcutsOpen} onClose={() => setShortcutsOpen(false)} />
      <ConfirmDialog
        cancelLabel="Keep editing"
        confirmLabel="Discard changes"
        danger
        message="This configuration has unsaved changes. Discard them and continue?"
        onCancel={configNavigation.cancelDiscard}
        onConfirm={configNavigation.confirmDiscard}
        open={configNavigation.confirmingDiscard}
        title="Discard unsaved changes?"
      />

      {shell.startupPhase !== "ready" ? (
        <StartupScreen
          error={shell.error}
          onRetry={shell.onRetryStartup}
          phase={shell.startupPhase}
        />
      ) : workspaceView === "fleet" ? (
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
      ) : workspaceView === "chat" || workspaceView === "code" ? (
        <section
          className={`workspace mobile-chat-pane-${mobileChatPane}`}
          data-mobile-chat-pane={mobileChatPane}
        >
          <Sidebar
            behaviorOptions={shell.behaviorOptions}
            conversations={shell.selectedDeployment?.conversations ?? []}
            deployments={shell.deployments}
            onConfigureDeployment={(agentDid) => openConfig(agentDid)}
            onOpenCode={(agentDid) => openCode(agentDid)}
            onOpenFleet={() => setWorkspaceView("fleet")}
            onSelectBehavior={shell.setSelectedBehaviorId}
            onSelectAgent={(agentDid) => {
              shell.setSelectedAgentDid(agentDid);
              shell.setSelectedSessionId(null);
            }}
            onSelectSession={shell.onSelectSession}
            onOpenSession={(sessionId) => {
              shell.onSelectSession(sessionId);
              setMobileChatPane("conversation");
            }}
            onRenameConversationTitle={shell.onRenameConversationTitle}
            onSyncConversations={shell.onRepairP2P}
            syncingConversations={shell.repairingP2P}
            onStartNewConversation={(behaviorId) => {
              shell.onStartNewConversation(behaviorId);
              setMobileChatPane("conversation");
            }}
            selectedAgentDid={shell.selectedAgentDid}
            selectedBehaviorId={shell.selectedBehaviorId}
            selectedSessionId={shell.selectedSessionId}
          />

          <section className="chat-column">
            {workspaceView === "code" ? (
              <CodeContextHeader
                deployment={shell.selectedDeployment ?? null}
                selectedBehaviorId={shell.selectedBehaviorId}
                onBackToChat={() => setWorkspaceView("chat")}
              />
            ) : null}
            <ChatWorkspace
              api={bridge.api}
              activeRequestId={
                shell.activeRequestId ?? shell.session?.latestRequestId ?? null
              }
              approxSerializedBytes={shell.snapshot?.client?.approxSerializedBytes ?? 0}
              canSend={shell.canSendMessage}
              configuredPeerCount={shell.snapshot?.client?.configuredPeerCount ?? 0}
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
                shell.sendStatus.kind === "disabled" ? shell.sendStatus.hint : null
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
              onOpenMobileNavigation={() => setMobileChatPane("navigation")}
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
            backLabel={configReturnView === "fleet" ? "Back to Fleet" : "Back to Chat"}
            bootstrap={shell.snapshot?.bootstrap ?? null}
            onBack={navigateBack}
            onDirtyChange={configNavigation.reportDirty}
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
            requestNavigation={requestConfigNavigation}
            onRunSchedule={shell.onRunSchedule}
            runningTask={shell.runningTask}
            saving={shell.savingConfig}
            selectedBehaviorId={shell.selectedBehaviorId}
            selectedDeployment={shell.selectedDeployment}
          />
        </section>
      )}
    </main>
  );
}

export default App;
