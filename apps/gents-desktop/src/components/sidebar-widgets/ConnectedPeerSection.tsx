import type { DeploymentView } from "@source-inc/gents-desktop-client";

export type ConnectedPeerSectionProps = {
  deployments: DeploymentView[];
  selectedAgentDid: string | null;
  onSelectAgent?: (agentDid: string) => void;
};

export function ConnectedPeerSection({
  deployments,
  selectedAgentDid,
  onSelectAgent,
}: ConnectedPeerSectionProps) {
  const selectedDeployment =
    deployments.find((deployment) => deployment.agentDid === selectedAgentDid) ?? null;

  return (
    <section className="sidebar-section connected-peer-section">
      <div className="connected-peer-card">
        <div className="connected-peer-header">
          <div>
            <p className="eyebrow">Connected Peer</p>
            {deployments.length > 1 && onSelectAgent ? (
              <select
                aria-label="Switch agent"
                className="connected-peer-switcher"
                data-testid="sidebar-agent-switcher"
                onChange={(event) => onSelectAgent(event.currentTarget.value)}
                value={selectedAgentDid ?? ""}
              >
                {!selectedAgentDid ? <option value="">Select an agent</option> : null}
                {deployments.map((deployment) => (
                  <option key={deployment.agentDid} value={deployment.agentDid}>
                    {deployment.agentPrincipal.displayName ?? deployment.label}
                  </option>
                ))}
              </select>
            ) : (
              <h2>{selectedDeployment?.label ?? "No peer selected"}</h2>
            )}
          </div>
          {selectedDeployment ? (
            <span className="connected-peer-status">
              {selectedDeployment.dialSucceeded ? "connected" : "saved"}
            </span>
          ) : null}
        </div>

        {selectedDeployment ? (
          <div
            aria-label={`${selectedDeployment.behaviors.length} behaviors, ${selectedDeployment.conversations.length} conversations, ${selectedDeployment.tasks.length} tasks`}
            className="connected-peer-stats"
          >
            <PeerStat label="Behaviors" value={selectedDeployment.behaviors.length} />
            <PeerStat
              label="Conversations"
              value={selectedDeployment.conversations.length}
            />
            <PeerStat label="Tasks" value={selectedDeployment.tasks.length} />
          </div>
        ) : null}
      </div>
    </section>
  );
}

function PeerStat({ label, value }: { label: string; value: number }) {
  return (
    <span>
      <strong>{value}</strong>
      {label}
    </span>
  );
}
