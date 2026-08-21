import type { DeploymentView } from "@source-inc/gents-desktop-client";

export type ConnectedPeerSectionProps = {
  deployments: DeploymentView[];
  selectedAgentDid: string | null;
  onOpenFleet: () => void;
  onConfigureDeployment: (agentDid: string) => void;
  onRepairP2P?: () => Promise<unknown> | void;
  repairingP2P?: boolean;
  onSelectAgent?: (agentDid: string) => void;
};

export function ConnectedPeerSection({
  deployments,
  selectedAgentDid,
  onOpenFleet,
  onConfigureDeployment,
  onRepairP2P,
  repairingP2P = false,
  onSelectAgent,
}: ConnectedPeerSectionProps) {
  const selectedDeployment =
    deployments.find((deployment) => deployment.agentDid === selectedAgentDid) ?? null;
  const agentName =
    selectedDeployment?.agentPrincipal.displayName ??
    selectedDeployment?.label ??
    "No agent selected";
  const deploymentLabel = selectedDeployment?.label.trim();
  const showDeploymentLabel =
    deploymentLabel && deploymentLabel.toLowerCase() !== agentName.trim().toLowerCase();
  const activeSessionCount =
    selectedDeployment?.behaviorEnvironments.reduce(
      (count, environment) => count + environment.activeSessionCount,
      0,
    ) ?? 0;
  const needsRepair = Boolean(
    selectedDeployment &&
    (!selectedDeployment.dialSucceeded || selectedDeployment.lastError),
  );

  return (
    <section className="sidebar-section connected-peer-section">
      <div className="connected-peer-card">
        <button
          aria-label="Back to Fleet"
          className="ghost-button connected-peer-back"
          data-testid="sidebar-back-to-fleet"
          onClick={onOpenFleet}
          type="button"
        >
          <span aria-hidden="true">←</span>
          Fleet
        </button>

        <div className="connected-peer-header">
          <div className="connected-peer-identity">
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
              <h1>{agentName}</h1>
            )}
            {selectedDeployment ? (
              <p className="connected-peer-meta">
                <span
                  aria-hidden="true"
                  className={
                    selectedDeployment.dialSucceeded
                      ? "agent-health-dot connected"
                      : "agent-health-dot disconnected"
                  }
                />
                <span>{selectedDeployment.dialSucceeded ? "Connected" : "Saved"}</span>
                {showDeploymentLabel ? (
                  <>
                    <span aria-hidden="true"> · </span>
                    <span>{deploymentLabel}</span>
                  </>
                ) : null}
                {activeSessionCount > 0 ? (
                  <>
                    <span aria-hidden="true"> · </span>
                    <strong>{activeSessionCount} active</strong>
                  </>
                ) : null}
              </p>
            ) : null}
          </div>

          {selectedDeployment ? (
            <details className="agent-action-menu">
              <summary
                aria-label={`Actions for ${agentName}`}
                data-testid="agent-actions"
                title="Agent actions"
              >
                <span aria-hidden="true">•••</span>
              </summary>
              <div className="agent-action-menu-popover">
                <button
                  onClick={() => onConfigureDeployment(selectedDeployment.agentDid)}
                  type="button"
                >
                  Configure
                </button>
                {needsRepair && onRepairP2P ? (
                  <button
                    data-testid="agent-repair-p2p"
                    disabled={repairingP2P}
                    onClick={() => void Promise.resolve(onRepairP2P()).catch(() => {})}
                    type="button"
                  >
                    {repairingP2P ? "Reconnecting…" : "Retry connection"}
                  </button>
                ) : null}
              </div>
            </details>
          ) : null}
        </div>
      </div>
    </section>
  );
}
