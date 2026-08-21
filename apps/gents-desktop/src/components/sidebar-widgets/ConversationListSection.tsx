import { useEffect, useMemo, useState } from "react";

import type {
  BehaviorEnvironmentView,
  ConversationSummary,
} from "@source-inc/gents-desktop-client";
import { displayConversationTitle } from "@source-inc/gents-desktop-client";
import { formatRelativeTime } from "@source-inc/gents-desktop-fleet";
import {
  conversationLifecycleGroup,
  conversationStatusClass,
  type ConversationLifecycleGroup,
} from "./sidebarUtils";

export type ConversationListSectionProps = {
  conversations: ConversationSummary[];
  environments: BehaviorEnvironmentView[];
  selectedAgentDid: string | null;
  selectedSessionId: string | null;
  onSelectSession: (sessionId: string) => void;
  onOpenSession?: (sessionId: string) => void;
  onCreateSession: () => void;
};

export function ConversationListSection({
  conversations,
  environments,
  selectedAgentDid,
  selectedSessionId,
  onSelectSession,
  onOpenSession,
  onCreateSession,
}: ConversationListSectionProps) {
  const [query, setQuery] = useState("");

  useEffect(() => setQuery(""), [selectedAgentDid]);

  const environmentById = useMemo(
    () =>
      new Map(environments.map((environment) => [environment.behaviorId, environment])),
    [environments],
  );
  const filteredConversations = useMemo(() => {
    const needle = query.trim().toLowerCase();
    if (!needle) return conversations;
    return conversations.filter((conversation) => {
      const environment = conversation.behaviorId
        ? environmentById.get(conversation.behaviorId)
        : undefined;
      return `${displayConversationTitle(conversation.title)} ${conversation.previewText ?? ""} ${environment?.displayName ?? conversation.behaviorId ?? ""}`
        .toLowerCase()
        .includes(needle);
    });
  }, [conversations, environmentById, query]);
  const grouped = useMemo(() => {
    const groups: Record<ConversationLifecycleGroup, ConversationSummary[]> = {
      attention: [],
      active: [],
      recent: [],
    };
    for (const conversation of filteredConversations) {
      groups[conversationLifecycleGroup(conversation)].push(conversation);
    }
    return groups;
  }, [filteredConversations]);

  return (
    <section className="sidebar-section conversation-section">
      <div className="session-section-header">
        <h2>Sessions</h2>
        <button
          className="primary-button session-new-button"
          data-testid="session-new"
          disabled={!selectedAgentDid || !environments.some((item) => item.enabled)}
          onClick={onCreateSession}
          type="button"
        >
          New session
        </button>
      </div>
      {selectedAgentDid && conversations.length > 0 ? (
        <input
          aria-label="Search sessions"
          className="conversation-search"
          data-testid="conversation-search"
          onChange={(event) => setQuery(event.currentTarget.value)}
          placeholder="Search sessions"
          type="search"
          value={query}
        />
      ) : null}
      {!selectedAgentDid ? (
        <p className="muted">Select an agent to see sessions.</p>
      ) : !conversations.length ? (
        <p className="muted">No sessions yet. Choose an environment to start one.</p>
      ) : !filteredConversations.length ? (
        <p className="muted">No sessions match the search.</p>
      ) : (
        <div className="conversation-list" data-testid="session-list">
          <SessionGroupList
            conversations={grouped.attention}
            environmentById={environmentById}
            label="Needs attention"
            onOpenSession={onOpenSession ?? onSelectSession}
            selectedSessionId={selectedSessionId}
          />
          <SessionGroupList
            conversations={grouped.active}
            environmentById={environmentById}
            label="Active"
            onOpenSession={onOpenSession ?? onSelectSession}
            selectedSessionId={selectedSessionId}
          />
          <SessionGroupList
            conversations={grouped.recent}
            environmentById={environmentById}
            label="Recent"
            onOpenSession={onOpenSession ?? onSelectSession}
            selectedSessionId={selectedSessionId}
          />
        </div>
      )}
    </section>
  );
}

function SessionGroupList({
  conversations,
  environmentById,
  label,
  onOpenSession,
  selectedSessionId,
}: {
  conversations: ConversationSummary[];
  environmentById: Map<string, BehaviorEnvironmentView>;
  label: string;
  onOpenSession: (sessionId: string) => void;
  selectedSessionId: string | null;
}) {
  if (!conversations.length) return null;
  return (
    <section className="session-group">
      <h3>{label}</h3>
      <div className="list session-group-list">
        {conversations.map((conversation) => {
          const when = conversation.updatedAt ?? conversation.createdAt;
          const environment = conversation.behaviorId
            ? environmentById.get(conversation.behaviorId)
            : undefined;
          const statusClass = conversationStatusClass(conversation);
          const lifecycle = conversationLifecycleGroup(conversation);
          const title = displayConversationTitle(conversation.title);
          return (
            <button
              aria-label={`${title}, ${sessionGroupLabel(lifecycle)}, ${environment?.displayName ?? "unassigned behavior"}`}
              className={
                conversation.sessionId === selectedSessionId
                  ? "list-item session-list-item selected"
                  : "list-item session-list-item"
              }
              data-testid={`conversation-${conversation.sessionId}`}
              key={conversation.sessionId}
              onClick={() => onOpenSession(conversation.sessionId)}
              type="button"
            >
              <span className="conversation-list-row">
                {statusClass ? (
                  <span aria-hidden="true" className={statusClass} />
                ) : null}
                <span
                  className={
                    conversation.title
                      ? "list-item-title conversation-list-title"
                      : "list-item-title conversation-list-title untitled-title"
                  }
                >
                  {title}
                </span>
                {when ? (
                  <span className="conversation-time" title={when}>
                    {formatRelativeTime(when)}
                  </span>
                ) : null}
              </span>
              <span className="session-environment-line">
                {environment?.displayName ??
                  conversation.behaviorId ??
                  "Unassigned behavior"}
                {environment?.workspaceRoot ? (
                  <>
                    <span aria-hidden="true"> · </span>
                    <span className="mono">
                      {workspaceName(environment.workspaceRoot)}
                    </span>
                  </>
                ) : null}
              </span>
              {conversation.previewText ? (
                <span className="session-preview">{conversation.previewText}</span>
              ) : null}
              {conversation.taskId ? (
                <span
                  className="conversation-task-tag"
                  title={displayConversationTaskLabel(conversation)}
                >
                  {displayConversationTaskLabel(conversation)}
                </span>
              ) : null}
            </button>
          );
        })}
      </div>
    </section>
  );
}

function workspaceName(root: string) {
  const parts = root.split(/[\\/]/).filter(Boolean);
  return parts[parts.length - 1] ?? root;
}

function sessionGroupLabel(group: ConversationLifecycleGroup) {
  return group === "attention" ? "needs attention" : group;
}

function displayConversationTaskLabel(conversation: ConversationSummary) {
  const name = conversation.taskName?.trim();
  return name && name.length > 0 ? name : (conversation.taskId ?? "Task");
}
