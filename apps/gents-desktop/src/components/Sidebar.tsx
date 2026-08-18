import type {
  BehaviorView,
  ConversationSummary,
  DeploymentView,
} from "@source-inc/gents-desktop-client";
import {
  BehaviorSelectorSection,
  ConnectedPeerSection,
  ConversationListSection,
} from "./sidebar-widgets";

export type SidebarProps = {
  deployments: DeploymentView[];
  conversations: ConversationSummary[];
  behaviorOptions: BehaviorView[];
  selectedAgentDid: string | null;
  selectedBehaviorId: string | null;
  selectedSessionId: string | null;
  onSelectBehavior: (behaviorId: string) => void;
  onSelectSession: (sessionId: string) => void;
  onOpenSession?: (sessionId: string) => void;
  onSelectAgent?: (agentDid: string) => void;
  onRenameConversationTitle?: (
    sessionId: string,
    title: string,
  ) => void | Promise<void>;
  onStartNewConversation: (behaviorId: string) => void;
  onSyncConversations?: () => Promise<unknown> | void;
  syncingConversations?: boolean;
};

export function Sidebar({
  deployments,
  conversations,
  behaviorOptions,
  selectedAgentDid,
  selectedBehaviorId,
  selectedSessionId,
  onSelectBehavior,
  onSelectSession,
  onOpenSession,
  onSelectAgent,
  onRenameConversationTitle,
  onStartNewConversation,
  onSyncConversations,
  syncingConversations,
}: SidebarProps) {
  return (
    <aside className="sidebar">
      <ConnectedPeerSection
        deployments={deployments}
        onSelectAgent={onSelectAgent}
        selectedAgentDid={selectedAgentDid}
      />

      <BehaviorSelectorSection
        behaviorOptions={behaviorOptions}
        selectedAgentDid={selectedAgentDid}
        selectedBehaviorId={selectedBehaviorId}
        onSelectBehavior={onSelectBehavior}
        onStartNewConversation={onStartNewConversation}
      />

      <ConversationListSection
        conversations={conversations}
        deployments={deployments}
        selectedAgentDid={selectedAgentDid}
        selectedBehaviorId={selectedBehaviorId}
        selectedSessionId={selectedSessionId}
        onSelectSession={onSelectSession}
        onOpenSession={onOpenSession}
        onRenameConversationTitle={onRenameConversationTitle}
        onSyncConversations={onSyncConversations}
        syncingConversations={syncingConversations}
      />
    </aside>
  );
}
