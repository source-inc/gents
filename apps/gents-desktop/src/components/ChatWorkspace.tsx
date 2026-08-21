import { useCallback, useEffect, useMemo, useState, type FormEvent } from "react";

import type {
  DeploymentView,
  DesktopApiAdapter,
  DesktopSessionSnapshot,
  P2PHealth,
} from "@source-inc/gents-desktop-client";
import { displayBehaviorLabel } from "@source-inc/gents-desktop-client";
import {
  CascadeCancelDialog,
  interruptChatRequest,
  previewChatInterruptCascade,
  type OptimisticPendingTurn,
} from "@source-inc/gents-desktop-chat";
import {
  ChatComposer,
  ChatHeader,
  ChatTranscriptPanel,
} from "@source-inc/gents-desktop-chat";
import { effectiveBehaviorSkills } from "@source-inc/gents-desktop-chat";

export type ChatWorkspaceProps = {
  api?: DesktopApiAdapter;
  activeRequestId: string | null;
  selectedDeployment: DeploymentView | null;
  selectedConversationTitle: string | null;
  selectedBehaviorId: string | null;
  selectedSessionId: string | null;
  session: DesktopSessionSnapshot | null;
  optimisticPendingTurn?: OptimisticPendingTurn | null;
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
  optimisticPendingTurn,
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

  function onInterruptClick() {
    const requestId = activeRequestId;
    if (!requestId) return;
    void beginInterrupt(requestId);
  }

  return (
    <>
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
            optimisticPendingTurn={optimisticPendingTurn}
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
    </>
  );
}
