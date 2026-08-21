import { useEffect, useState } from "react";

import type {
  ConversationSummary,
  DeploymentView,
} from "@source-inc/gents-desktop-client";
import {
  BehaviorEnvironmentSection,
  ConnectedPeerSection,
  ConversationListSection,
} from "./sidebar-widgets";

export type SidebarProps = {
  deployments: DeploymentView[];
  conversations: ConversationSummary[];
  selectedAgentDid: string | null;
  selectedBehaviorId: string | null;
  selectedSessionId: string | null;
  onOpenFleet: () => void;
  onConfigureDeployment: (agentDid: string) => void;
  onSelectBehavior: (behaviorId: string) => void;
  onSelectSession: (sessionId: string) => void;
  onOpenSession?: (sessionId: string) => void;
  onSelectAgent?: (agentDid: string) => void;
  onStartNewConversation: (behaviorId: string) => void;
  onRepairP2P?: () => Promise<unknown> | void;
  repairingP2P?: boolean;
};

export function Sidebar({
  deployments,
  conversations,
  selectedAgentDid,
  selectedBehaviorId,
  selectedSessionId,
  onOpenFleet,
  onConfigureDeployment,
  onSelectBehavior,
  onSelectSession,
  onOpenSession,
  onSelectAgent,
  onStartNewConversation,
  onRepairP2P,
  repairingP2P,
}: SidebarProps) {
  const [section, setSection] = useState<"sessions" | "behaviors">("sessions");
  const selectedDeployment = deployments.find(
    (deployment) => deployment.agentDid === selectedAgentDid,
  );
  const environments = selectedDeployment?.behaviorEnvironments ?? [];

  useEffect(() => setSection("sessions"), [selectedAgentDid]);

  return (
    <aside className="sidebar">
      <ConnectedPeerSection
        deployments={deployments}
        onSelectAgent={onSelectAgent}
        selectedAgentDid={selectedAgentDid}
        onConfigureDeployment={onConfigureDeployment}
        onOpenFleet={onOpenFleet}
        onRepairP2P={onRepairP2P}
        repairingP2P={repairingP2P}
      />

      <div aria-label="Agent workspace" className="agent-section-tabs" role="group">
        <button
          aria-pressed={section === "sessions"}
          className={section === "sessions" ? "selected" : ""}
          data-testid="agent-tab-sessions"
          onClick={() => setSection("sessions")}
          type="button"
        >
          Sessions
        </button>
        <button
          aria-pressed={section === "behaviors"}
          className={section === "behaviors" ? "selected" : ""}
          data-testid="agent-tab-behaviors"
          onClick={() => setSection("behaviors")}
          type="button"
        >
          Behaviors
        </button>
      </div>

      {section === "sessions" ? (
        <ConversationListSection
          conversations={conversations}
          environments={environments}
          selectedAgentDid={selectedAgentDid}
          selectedSessionId={selectedSessionId}
          onSelectSession={onSelectSession}
          onOpenSession={onOpenSession}
          onCreateSession={() => setSection("behaviors")}
        />
      ) : (
        <BehaviorEnvironmentSection
          environments={environments}
          selectedAgentDid={selectedAgentDid}
          selectedBehaviorId={selectedBehaviorId}
          onSelectBehavior={onSelectBehavior}
          onStartNewConversation={(behaviorId) => {
            onStartNewConversation(behaviorId);
            setSection("sessions");
          }}
        />
      )}
    </aside>
  );
}
