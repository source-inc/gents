import type { Dispatch, FormEvent, MutableRefObject, SetStateAction } from "react";

import type {
  ChatShellProjection,
  ChatWorkflowState,
  OptimisticPendingTurn,
} from "@source-inc/gents-desktop-chat";
import type {
  DeploymentView,
  DesktopApiAdapter,
  DesktopSessionSnapshot,
} from "@source-inc/gents-desktop-client";

type ChatActionParams = {
  api: DesktopApiAdapter;
  draft: string;
  newConversationAgentRef: MutableRefObject<string | null>;
  refreshSession: (
    nextSessionId: string | null,
  ) => Promise<DesktopSessionSnapshot | null>;
  refreshSnapshot: () => Promise<void>;
  selectedBehaviorId: string | null;
  selectedDeployment: DeploymentView | null;
  selectedSessionId: string | null;
  session: DesktopSessionSnapshot | null;
  setDraft: Dispatch<SetStateAction<string>>;
  setError: Dispatch<SetStateAction<string | null>>;
  setLocalWorkflow: Dispatch<SetStateAction<ChatWorkflowState>>;
  setOptimisticPendingTurn: Dispatch<SetStateAction<OptimisticPendingTurn | null>>;
  setSelectedBehaviorId: Dispatch<SetStateAction<string | null>>;
  setSelectedSessionId: Dispatch<SetStateAction<string | null>>;
  setSending: Dispatch<SetStateAction<boolean>>;
  setSession: Dispatch<SetStateAction<DesktopSessionSnapshot | null>>;
  shellProjection: ChatShellProjection;
};

export function createDesktopShellChatActions({
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
}: ChatActionParams) {
  async function submitContent(content: string): Promise<boolean> {
    if (!selectedDeployment || !content.trim()) {
      return false;
    }

    if (shellProjection.nonEmptyContentSendStatus.kind !== "ready") {
      setError(shellProjection.nonEmptyContentSendStatus.hint);
      return false;
    }

    setLocalWorkflow({
      kind: "submittingRequest",
      agentDid: selectedDeployment.agentDid,
      sessionId: selectedSessionId,
    });
    setSending(true);
    setError(null);
    try {
      const result = await api.sendChatMessage({
        agentDid: selectedDeployment.agentDid,
        behaviorId: selectedBehaviorId,
        sessionId: selectedSessionId,
        content,
      });
      newConversationAgentRef.current = null;
      setSelectedSessionId(result.sessionId);
      setOptimisticPendingTurn({
        sessionId: result.sessionId,
        requestId: result.requestId,
        content,
        selectedSkillIds: [],
        lifecycleState: "pending",
        createdAt: new Date().toISOString(),
      });
      setLocalWorkflow({
        kind: "awaitingObservation",
        agentDid: selectedDeployment.agentDid,
        sessionId: result.sessionId,
        requestId: result.requestId,
      });
      return true;
    } catch (err) {
      setLocalWorkflow({ kind: "ready" });
      setError(String(err));
      return false;
    } finally {
      setSending(false);
    }
  }

  async function onSendMessage(event: FormEvent) {
    event.preventDefault();
    if (await submitContent(draft)) {
      setDraft("");
    }
  }

  /** Retry the persisted interactive predecessor through the fenced retry API. */
  async function retryRequest(requestId: string) {
    if (!selectedDeployment) {
      return;
    }
    setLocalWorkflow({
      kind: "submittingRequest",
      agentDid: selectedDeployment.agentDid,
      sessionId: selectedSessionId,
    });
    setSending(true);
    setError(null);
    try {
      const result = await api.retryRequest(requestId);
      setSelectedSessionId(result.sessionId);
      setLocalWorkflow({
        kind: "awaitingObservation",
        agentDid: selectedDeployment.agentDid,
        sessionId: result.sessionId,
        requestId: result.requestId,
      });
    } catch (err) {
      setLocalWorkflow({ kind: "ready" });
      setError(String(err));
    } finally {
      setSending(false);
    }
  }

  function onRetryMessage(requestId: string) {
    return retryRequest(requestId);
  }

  async function onRenameConversationTitle(sessionId: string, title: string) {
    if (!selectedDeployment) {
      return;
    }
    setError(null);
    try {
      await api.renameConversation({
        agentDid: selectedDeployment.agentDid,
        sessionId,
        title,
      });
      await refreshSnapshot();
      await refreshSession(sessionId);
    } catch (err) {
      setError(String(err));
      throw err;
    }
  }

  function onSelectSession(sessionId: string) {
    const conversation = selectedDeployment?.conversations.find(
      (conversation) => conversation.sessionId === sessionId,
    );
    if (conversation?.behaviorId) {
      setSelectedBehaviorId(conversation.behaviorId);
    }
    newConversationAgentRef.current = null;
    if (session?.sessionId !== sessionId) {
      setSession(null);
    }
    setSelectedSessionId(sessionId);
  }

  function onStartNewConversation(behaviorId?: string | null) {
    if (!selectedDeployment) {
      return;
    }
    if (
      behaviorId &&
      selectedDeployment.behaviors.some(
        (behavior) => behavior.behaviorId === behaviorId,
      )
    ) {
      setSelectedBehaviorId(behaviorId);
    }
    newConversationAgentRef.current = selectedDeployment.agentDid;
    setSelectedSessionId(null);
    setSession(null);
    setLocalWorkflow({ kind: "ready" });
    setError(null);
  }

  return {
    onRenameConversationTitle,
    onRetryMessage,
    onSelectSession,
    onSendMessage,
    onStartNewConversation,
  };
}
