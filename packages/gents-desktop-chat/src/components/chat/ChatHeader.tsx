import { useEffect, useState } from "react";
import type { FormEvent } from "react";

import type { P2PHealth } from "@source-inc/gents-desktop-client";
import { displayConversationTitle } from "@source-inc/gents-desktop-client";

export type ChatHeaderProps = {
  behaviorLabel: string | null;
  runtimeHealth: P2PHealth | null;
  configuredPeerCount?: number;
  dialedPeerCount?: number;
  selectedConversationTitle: string | null;
  selectedSessionId: string | null;
  onRenameConversationTitle: (
    sessionId: string,
    title: string,
  ) => void | Promise<void>;
  onOpenMobileNavigation?: () => void;
};

export function p2pConnectionDisplay(
  runtimeHealth: P2PHealth | null,
  configuredPeerCount: number,
  dialedPeerCount: number,
) {
  const status = runtimeHealth?.status ?? "unknown";
  const title = runtimeHealth
    ? `Transport ${status}; ${dialedPeerCount}/${configuredPeerCount} saved peers dialed; ${runtimeHealth.connectedPeerCount} active connections; ${runtimeHealth.replicatorCount} replicators`
    : `Checking P2P transport; ${dialedPeerCount}/${configuredPeerCount} saved peers dialed`;

  if (!runtimeHealth) {
    return { label: "Checking sync", healthy: false, title };
  }
  if (runtimeHealth.status === "wedged") {
    return { label: "P2P stalled", healthy: false, title };
  }
  if (runtimeHealth.status !== "healthy") {
    return { label: "P2P retrying", healthy: false, title };
  }
  if (configuredPeerCount === 0) {
    return { label: "Local", healthy: true, title };
  }
  if (dialedPeerCount < configuredPeerCount) {
    return {
      label: `Reconnecting ${dialedPeerCount}/${configuredPeerCount}`,
      healthy: false,
      title,
    };
  }
  return { label: "Paired", healthy: true, title };
}

export function ChatHeader({
  behaviorLabel,
  runtimeHealth,
  configuredPeerCount = 0,
  dialedPeerCount = 0,
  selectedConversationTitle,
  selectedSessionId,
  onRenameConversationTitle,
  onOpenMobileNavigation,
}: ChatHeaderProps) {
  const p2pDisplay = p2pConnectionDisplay(
    runtimeHealth,
    configuredPeerCount,
    dialedPeerCount,
  );
  const visibleConversationTitle = selectedSessionId
    ? displayConversationTitle(selectedConversationTitle)
    : "Start a conversation";
  const [isRenamingTitle, setIsRenamingTitle] = useState(false);
  const [titleDraft, setTitleDraft] = useState(selectedConversationTitle ?? "");
  const [renamingTitle, setRenamingTitle] = useState(false);

  useEffect(() => {
    setIsRenamingTitle(false);
    setTitleDraft(selectedConversationTitle ?? "");
  }, [selectedConversationTitle, selectedSessionId]);

  async function submitTitleRename(event?: FormEvent) {
    event?.preventDefault();
    if (!selectedSessionId) {
      return;
    }

    const trimmed = titleDraft.trim();
    if (!trimmed) {
      setIsRenamingTitle(false);
      setTitleDraft(selectedConversationTitle ?? "");
      return;
    }

    if (trimmed === (selectedConversationTitle ?? "").trim()) {
      setIsRenamingTitle(false);
      return;
    }

    setRenamingTitle(true);
    try {
      await onRenameConversationTitle(selectedSessionId, trimmed);
      setIsRenamingTitle(false);
    } catch {
    } finally {
      setRenamingTitle(false);
    }
  }

  return (
    <header className="chat-header">
      <div className="chat-title-block">
        {onOpenMobileNavigation ? (
          <button
            className="ghost-button mobile-chat-navigation-button"
            data-testid="mobile-chat-navigation"
            onClick={onOpenMobileNavigation}
            type="button"
          >
            <span aria-hidden="true">←</span>
            Chats
          </button>
        ) : null}
        {selectedSessionId ? (
          isRenamingTitle ? (
            <form className="title-rename-form" onSubmit={submitTitleRename}>
              <input
                aria-label={`Rename ${visibleConversationTitle}`}
                autoFocus
                className="title-rename-input"
                data-testid="conversation-title-input"
                onBlur={() => void submitTitleRename()}
                onChange={(event) => setTitleDraft(event.currentTarget.value)}
                onKeyDown={(event) => {
                  if (event.key === "Escape") {
                    setIsRenamingTitle(false);
                    setTitleDraft(selectedConversationTitle ?? "");
                  }
                }}
                value={titleDraft}
              />
            </form>
          ) : (
            <div className="chat-title-row">
              <h2>{visibleConversationTitle}</h2>
              <button
                aria-label={`Rename ${visibleConversationTitle}`}
                className="icon-button"
                data-testid="conversation-title-edit"
                disabled={renamingTitle}
                onClick={() => setIsRenamingTitle(true)}
                type="button"
              >
                Edit
              </button>
            </div>
          )
        ) : (
          <h2>{visibleConversationTitle}</h2>
        )}
      </div>
      <div className="chat-status">
        {behaviorLabel ? <span className="chip">{behaviorLabel}</span> : null}
        <span
          className={p2pDisplay.healthy ? "chip chip-green" : "chip"}
          title={p2pDisplay.title}
        >
          {p2pDisplay.label}
        </span>
      </div>
    </header>
  );
}
