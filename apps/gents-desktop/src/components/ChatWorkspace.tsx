import { useCallback, useEffect, useMemo, useState, type FormEvent } from "react";

import type {
  DeploymentView,
  DesktopApiAdapter,
  DesktopSessionSnapshot,
  P2PHealth,
} from "@source-inc/gents-desktop-client";
import {
  displayBehaviorLabel,
  getDesktopApiAdapter,
  resendRequest,
} from "@source-inc/gents-desktop-client";
import { BackendHealthPanel } from "@source-inc/gents-desktop-operations";
import {
  CascadeCancelDialog,
  interruptChatRequest,
  previewChatInterruptCascade,
} from "@source-inc/gents-desktop-chat";
import {
  ChatComposer,
  ChatHeader,
  ChatTranscriptPanel,
} from "@source-inc/gents-desktop-chat";
import { effectiveBehaviorSkills } from "@source-inc/gents-desktop-chat";
import { McpHealthPanel } from "@source-inc/gents-desktop-operations";
import {
  OperationsRail,
  OperationsRailProvider,
} from "@source-inc/gents-desktop-operations";
import type { OperationsRailTabDescriptor } from "@source-inc/gents-desktop-operations";
import { BackgroundedToolsPanel } from "@source-inc/gents-desktop-operations";
import { HoldsPanel } from "@source-inc/gents-desktop-operations";
import { useToolCallHolds } from "@source-inc/gents-desktop-operations";
import { WorkspaceTreePanel } from "@source-inc/gents-desktop-operations";
import { RequestTracePanel } from "@source-inc/gents-desktop-operations";
import { useOperationsSnapshot } from "@source-inc/gents-desktop-operations";
import { SubagentLineageView } from "@source-inc/gents-desktop-operations";

export type ChatWorkspaceProps = {
  api?: DesktopApiAdapter;
  activeRequestId: string | null;
  selectedDeployment: DeploymentView | null;
  selectedConversationTitle: string | null;
  selectedBehaviorId: string | null;
  selectedSessionId: string | null;
  session: DesktopSessionSnapshot | null;
  runtimeHealth: P2PHealth | null;
  rowCount: number;
  approxSerializedBytes: number;
  dialedPeerCount: number;
  configuredPeerCount: number;
  canSend: boolean;
  sendHint: string | null;
  draft: string;
  interruptVisible: boolean;
  sending: boolean;
  turnState: string | null;
  onRenameConversationTitle: (sessionId: string, title: string) => void | Promise<void>;
  onDraftChange: (value: string) => void;
  onSend: (event: FormEvent) => void;
  onRetryMessage?: (requestId: string) => void | Promise<void>;
  onOpenMobileNavigation?: () => void;
  onInterruptAccepted?: () => void | Promise<void>;
};

export type ActiveChatWorkspaceProps = Omit<
  ChatWorkspaceProps,
  "selectedDeployment"
> & {
  selectedDeployment: DeploymentView;
};

export function ChatWorkspace(props: ChatWorkspaceProps) {
  const { selectedDeployment } = props;

  if (!selectedDeployment) {
    return (
      <article className="panel centered-panel">
        <p className="eyebrow">Chat</p>
        <h2>Select an agent</h2>
        <p className="muted">Open the fleet dashboard to choose an agent connection.</p>
      </article>
    );
  }

  return <ActiveChatWorkspace {...props} selectedDeployment={selectedDeployment} />;
}

export function ActiveChatWorkspace({
  api: explicitApi,
  activeRequestId,
  selectedDeployment,
  selectedConversationTitle,
  selectedBehaviorId,
  selectedSessionId,
  session,
  runtimeHealth,
  rowCount,
  approxSerializedBytes,
  dialedPeerCount,
  configuredPeerCount,
  canSend,
  sendHint,
  draft,
  interruptVisible,
  sending,
  turnState,
  onRenameConversationTitle,
  onDraftChange,
  onSend,
  onRetryMessage,
  onOpenMobileNavigation,
  onInterruptAccepted,
}: ActiveChatWorkspaceProps) {
  const api = getDesktopApiAdapter(explicitApi);
  const previewInterrupt =
    explicitApi?.previewInterruptCascade ?? previewChatInterruptCascade;
  const interrupt = explicitApi?.interruptRequest ?? interruptChatRequest;
  const activeBehaviorId =
    selectedBehaviorId ?? selectedDeployment.defaultBehaviorId ?? null;
  const activeBehavior =
    selectedDeployment.behaviors.find(
      (behavior) => behavior.behaviorId === activeBehaviorId,
    ) ?? null;
  const behaviorLabel =
    activeBehavior?.displayName ?? displayBehaviorLabel(activeBehaviorId);
  const activeBehaviorSkills = useMemo(
    () => effectiveBehaviorSkills(selectedDeployment.skills ?? [], activeBehavior),
    [activeBehavior, selectedDeployment.skills],
  );

  const [cascade, setCascade] = useState<null | { rootRequestId: string }>(null);
  const [interruptResultBanner, setInterruptResultBanner] = useState<{
    text: string;
    tone: "info" | "error";
  } | null>(null);
  const [operationsOpen, setOperationsOpen] = useState(false);
  async function resendStaleRequest(requestId: string) {
    try {
      const submitted = await (explicitApi?.resendRequest ?? resendRequest)(requestId);
      setInterruptResultBanner({
        text: `Request resent as ${submitted.requestId.slice(0, 14)}…`,
        tone: "info",
      });
    } catch (error) {
      setInterruptResultBanner({
        text: `Resend failed: ${error instanceof Error ? error.message : String(error)}`,
        tone: "error",
      });
    }
  }
  const [lineageRootOverride, setLineageRootOverride] = useState<string | null>(null);

  useEffect(() => {
    setLineageRootOverride(null);
  }, [selectedSessionId, selectedDeployment.agentDid]);

  useEffect(() => {
    if (!interruptResultBanner) return;
    const t = setTimeout(() => setInterruptResultBanner(null), 5000);
    return () => clearTimeout(t);
  }, [interruptResultBanner]);

  const beginInterrupt = useCallback(
    async (requestId: string) => {
      try {
        const preview = await previewInterrupt({
          requestId,
          agentDid: selectedDeployment.agentDid,
          includeTerminal: false,
        });
        const childCount =
          preview.willInterrupt.length +
          preview.willDetach.length +
          preview.unknownPolicy.length;
        if (childCount === 0) {
          const result = await interrupt({
            requestId,
            agentDid: selectedDeployment.agentDid,
            cause: "userCancelled",
            cascade: false,
          });
          if (result.accepted) {
            setInterruptResultBanner({ text: "Interrupt requested", tone: "info" });
            void onInterruptAccepted?.();
          } else if (result.alreadyInterrupted)
            setInterruptResultBanner({ text: "Already interrupted", tone: "info" });
          return;
        }
        setCascade({ rootRequestId: requestId });
      } catch (e) {
        setInterruptResultBanner({
          text: `Couldn't interrupt: ${String(e)}`,
          tone: "error",
        });
      }
    },
    [interrupt, onInterruptAccepted, previewInterrupt, selectedDeployment.agentDid],
  );

  const opsSnapshotRequest = useMemo(
    () => ({ agentDid: selectedDeployment.agentDid, rootRequestId: null }),
    [selectedDeployment.agentDid],
  );
  const { snapshot: opsSnapshot } = useOperationsSnapshot(opsSnapshotRequest, {
    api,
    enabled: !operationsOpen,
  });
  const stuckCount =
    opsSnapshot?.agentDid === selectedDeployment.agentDid
      ? opsSnapshot.stuckDiagnostics.length
      : 0;
  const { holds: heldToolCalls } = useToolCallHolds(selectedDeployment.agentDid, api);
  const heldCount = heldToolCalls?.length ?? 0;

  const operationsRailTabs = useMemo<OperationsRailTabDescriptor[]>(() => {
    const rootRequestId = lineageRootOverride ?? session?.latestRequestId ?? null;
    const lineageAgentDid = selectedDeployment.agentDid;
    return [
      {
        id: "background-tools",
        label: "Background",
        badge: stuckCount > 0 ? String(stuckCount) : null,
        render: () => (
          <BackgroundedToolsPanel
            agentDid={selectedDeployment.agentDid}
            onResendRequest={(requestId) => {
              void resendStaleRequest(requestId);
            }}
            rootRequestId={rootRequestId}
            runtime={selectedDeployment?.runtime ?? null}
            onOpenLineage={setLineageRootOverride}
            onInterruptParent={(requestId) => {
              void beginInterrupt(requestId);
            }}
          />
        ),
      },
      {
        id: "holds",
        label: "Holds",
        badge: heldCount > 0 ? String(heldCount) : null,
        render: () => <HoldsPanel agentDid={selectedDeployment.agentDid} />,
      },
      {
        id: "lineage",
        label: "Lineage",
        render: () => (
          <SubagentLineageView
            rootRequestId={rootRequestId}
            agentDid={lineageAgentDid}
          />
        ),
      },
      {
        id: "trace",
        label: "Trace",
        render: () => (
          <RequestTracePanel
            agentDid={selectedDeployment.agentDid}
            rootRequestId={rootRequestId}
          />
        ),
      },
      {
        id: "workspace",
        label: "Files",
        render: () => <WorkspaceTreePanel />,
      },
      {
        id: "backend-health",
        label: "Backends",
        render: () => <BackendHealthPanel />,
      },
      {
        id: "mcp-health",
        label: "MCP health",
        render: () => <McpHealthPanel />,
      },
    ];
  }, [
    session?.latestRequestId,
    selectedDeployment.agentDid,
    lineageRootOverride,
    beginInterrupt,
    stuckCount,
    heldCount,
  ]);

  function onInterruptClick() {
    const requestId = activeRequestId;
    if (!requestId) return;
    void beginInterrupt(requestId);
  }

  return (
    <OperationsRailProvider api={api} tabs={operationsRailTabs}>
      <ChatHeader
        behaviorLabel={behaviorLabel}
        configuredPeerCount={configuredPeerCount}
        dialedPeerCount={dialedPeerCount}
        onOpenMobileNavigation={onOpenMobileNavigation}
        runtimeHealth={runtimeHealth}
        selectedConversationTitle={selectedConversationTitle}
        selectedSessionId={selectedSessionId}
        onRenameConversationTitle={onRenameConversationTitle}
      />

      <section className="chat-workspace">
        <div className="chat-main">
          <ChatTranscriptPanel
            selectedSessionId={selectedSessionId}
            session={session}
            onRetryMessage={onRetryMessage}
          />

          <ChatComposer
            activeRequestId={activeRequestId}
            approxSerializedBytes={approxSerializedBytes}
            behaviorLabel={behaviorLabel}
            canSend={canSend}
            configuredPeerCount={configuredPeerCount}
            dialedPeerCount={dialedPeerCount}
            draft={draft}
            interruptVisible={interruptVisible}
            rowCount={rowCount}
            sendHint={sendHint}
            sending={sending}
            turnState={turnState}
            onDraftChange={onDraftChange}
            onInterruptClick={onInterruptClick}
            onSend={onSend}
            skills={activeBehaviorSkills}
          />
        </div>
        <OperationsRail
          open={operationsOpen}
          onOpenChange={setOperationsOpen}
          attentionCount={stuckCount}
        />
        {interruptResultBanner ? (
          <div
            className={`chat-toast${
              interruptResultBanner.tone === "error" ? " is-error" : ""
            }`}
            data-testid="chat-toast"
            role="status"
            aria-live="polite"
          >
            {interruptResultBanner.text}
          </div>
        ) : null}
      </section>

      {cascade ? (
        <CascadeCancelDialog
          open
          rootRequestId={cascade.rootRequestId}
          agentDid={selectedDeployment.agentDid}
          previewInterrupt={previewInterrupt}
          interrupt={interrupt}
          onClose={() => setCascade(null)}
          onAccepted={(at) => {
            setCascade(null);
            void at;
            setInterruptResultBanner({ text: "Interrupt requested", tone: "info" });
            void onInterruptAccepted?.();
          }}
          onAlreadyInterrupted={() => {
            setCascade(null);
            setInterruptResultBanner({ text: "Already interrupted", tone: "info" });
          }}
          onError={(msg) => {
            setCascade(null);
            setInterruptResultBanner({
              text: `Couldn't interrupt: ${msg}`,
              tone: "error",
            });
          }}
        />
      ) : null}
    </OperationsRailProvider>
  );
}
