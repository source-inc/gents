import { useEffect, useRef, useState, type ReactNode } from "react";

import type {
  BearerPairingRequest,
  BearerPairingResponse,
  BootstrapSummary,
  DesktopApiAdapter,
  DeploymentView,
  P2PHealth,
  PeerAddRequest,
} from "@source-inc/gents-desktop-client";
import type { FleetCopy } from "../copy.js";
import { formatPeerConnectionError } from "../peerConnectionErrors.js";
import { validateAgentDid } from "../peerConnectionImport.js";
import { needsInferenceSetup } from "../fleetMetrics.js";
import { AddPeerForm } from "./AddPeerForm.js";
import { FleetRow } from "./FleetRow.js";
import { NetworkPanel } from "./NetworkPanel.js";

export type FleetDashboardProps = {
  addingPeer: boolean;
  bootstrap: BootstrapSummary | null;
  deployments: DeploymentView[];
  loading: boolean;
  p2pHealth: P2PHealth | null;
  repairingP2P: boolean;
  starting: boolean;
  onAddPeer: (request: PeerAddRequest) => Promise<unknown>;
  onPairBearer: (
    request: BearerPairingRequest,
  ) => Promise<BearerPairingResponse>;
  onProbePeerAddress: (serverAddress: string) => Promise<unknown>;
  onOpenChat: (agentDid: string) => void;
  onOpenConfig: (agentDid: string) => void;
  onRemovePeer?: (peerId: string) => Promise<unknown> | void;
  onRenamePeer?: (peerId: string, label: string) => Promise<unknown> | void;
  onRepairP2P: () => Promise<unknown>;
  brand?: ReactNode;
  api?: DesktopApiAdapter;
  copy?: FleetCopy;
  headerLeadingActions?: ReactNode;
  localRuntimeSetup?: ReactNode;
  renderInferenceSetup?: (
    deployment: DeploymentView,
    close: () => void,
  ) => ReactNode;
};

const DEFAULT_PEER_FORM: PeerAddRequest = {
  label: "",
  agentDid: "",
  addr: "",
  graphql: null,
};

export function FleetDashboard({
  addingPeer,
  bootstrap,
  deployments,
  loading,
  p2pHealth,
  repairingP2P,
  starting,
  onAddPeer,
  onPairBearer,
  onProbePeerAddress,
  onOpenChat,
  onOpenConfig,
  onRemovePeer,
  onRenamePeer,
  onRepairP2P,
  brand,
  api,
  copy,
  headerLeadingActions,
  localRuntimeSetup,
  renderInferenceSetup,
}: FleetDashboardProps) {
  const [showAddPeer, setShowAddPeer] = useState(false);
  const [peerForm, setPeerForm] = useState(DEFAULT_PEER_FORM);
  const [peerFormError, setPeerFormError] = useState<string | null>(null);
  const [wizardDeployment, setWizardDeployment] =
    useState<DeploymentView | null>(null);
  const [pairingNotice, setPairingNotice] = useState<string | null>(null);
  const autoPromptedInference = useRef(new Set<string>());
  const hasDeployments = deployments.length > 0;
  const deploymentNeedingInference =
    deployments.find(needsInferenceSetup) ?? null;
  const activeWizardDeployment = wizardDeployment
    ? (deployments.find((entry) => entry.peerId === wizardDeployment.peerId) ??
      wizardDeployment)
    : null;
  const needsP2PRepair =
    deployments.some(
      (deployment) =>
        !deployment.dialSucceeded || Boolean(deployment.lastError),
    ) ||
    (p2pHealth
      ? p2pHealth.consecutiveFailures > 0 || Boolean(p2pHealth.lastError)
      : false);

  useEffect(() => {
    if (
      !renderInferenceSetup ||
      !deploymentNeedingInference ||
      deploymentNeedingInference.source !== "local-standard" ||
      autoPromptedInference.current.has(deploymentNeedingInference.peerId)
    ) {
      return;
    }
    autoPromptedInference.current.add(deploymentNeedingInference.peerId);
    setWizardDeployment(deploymentNeedingInference);
  }, [deploymentNeedingInference, renderInferenceSetup]);

  async function submitPeer(request: PeerAddRequest) {
    setPeerFormError(null);
    try {
      await onAddPeer({
        ...request,
        agentDid: validateAgentDid(request.agentDid),
      });
      setPeerForm(DEFAULT_PEER_FORM);
      setShowAddPeer(false);
    } catch (error) {
      setPeerFormError(formatPeerConnectionError(error, "add-peer"));
    }
  }

  async function pairWithBearer(request: BearerPairingRequest) {
    const response = await onPairBearer(request);
    setPeerFormError(null);
    setPairingNotice(
      `${response.pairing.label} is ready. Signed membership and bidirectional replication were observed.`,
    );
    setShowAddPeer(false);
    return response;
  }

  if (!hasDeployments) {
    return (
      <section className="fleet-empty" data-testid="fleet-empty">
        <div className="fleet-empty-card panel">
          {brand}
          <div className="fleet-empty-copy">
            <h2>{localRuntimeSetup ? "Set up Gents" : "Connect your agent"}</h2>
            <p className="muted">
              {localRuntimeSetup
                ? "Optionally create an agent on this machine, or skip local setup and connect a remote agent."
                : "Pair with a remote agent using its signed connection details."}
            </p>
          </div>
          {localRuntimeSetup}
          <details
            className="fleet-remote-disclosure"
            data-testid="fleet-remote-disclosure"
          >
            <summary aria-label="Connect a remote agent">
              {localRuntimeSetup
                ? "Skip local setup and connect a remote agent…"
                : "Connect agent"}
            </summary>
            <AddPeerForm
              addingPeer={addingPeer}
              disabled={starting || loading}
              localError={peerFormError}
              peerForm={peerForm}
              onPeerFormChange={setPeerForm}
              onProbePeerAddress={onProbePeerAddress}
              onPairBearer={pairWithBearer}
              pairingQrHint={copy?.pairingQrHint}
              onSubmit={submitPeer}
            />
          </details>
        </div>
      </section>
    );
  }

  return (
    <section className="fleet-dashboard" data-testid="fleet-dashboard">
      <header className="fleet-header">
        {brand}
        <div className="fleet-header-actions">
          {headerLeadingActions}
          {needsP2PRepair ? (
            <button
              className="ghost-button"
              data-testid="fleet-repair-p2p"
              disabled={repairingP2P}
              onClick={() => void onRepairP2P()}
              title="Re-dial saved peers and refresh the desktop client's P2P connections"
              type="button"
            >
              {repairingP2P ? "Reconnecting…" : "Reconnect P2P"}
            </button>
          ) : null}
          <button
            className="primary-button"
            onClick={() => {
              setPairingNotice(null);
              setShowAddPeer((value) => !value);
            }}
            type="button"
          >
            Add Agent
          </button>
        </div>
      </header>

      {pairingNotice ? (
        <p
          aria-live="polite"
          className="fleet-pairing-success"
          data-testid="fleet-pair-status"
        >
          {pairingNotice}
        </p>
      ) : null}

      {showAddPeer ? (
        <section className="panel fleet-add-panel">
          {localRuntimeSetup}
          <AddPeerForm
            addingPeer={addingPeer}
            disabled={starting || loading}
            localError={peerFormError}
            peerForm={peerForm}
            onPeerFormChange={setPeerForm}
            onProbePeerAddress={onProbePeerAddress}
            onPairBearer={pairWithBearer}
            pairingQrHint={copy?.pairingQrHint}
            onSubmit={submitPeer}
          />
        </section>
      ) : null}

      <div className="fleet-table-wrap">
        <table className="fleet-table">
          <thead>
            <tr>
              <th>Agent</th>
              <th>Behaviors</th>
              <th>Tasks</th>
              <th>Inference</th>
              <th>Tool ceiling</th>
              <th>Open work</th>
              <th>Last update</th>
              <th className="fleet-actions-header" aria-label="Actions" />
            </tr>
          </thead>
          <tbody>
            {deployments.map((deployment) => (
              <FleetRow
                bootstrap={bootstrap}
                deployment={deployment}
                key={deployment.peerId}
                onOpenChat={onOpenChat}
                onOpenConfig={onOpenConfig}
                onRemovePeer={onRemovePeer}
                onRenamePeer={onRenamePeer}
                onSetupInference={
                  renderInferenceSetup ? setWizardDeployment : undefined
                }
              />
            ))}
          </tbody>
        </table>
      </div>

      <NetworkPanel api={api} />

      {activeWizardDeployment && renderInferenceSetup
        ? renderInferenceSetup(activeWizardDeployment, () =>
            setWizardDeployment(null),
          )
        : null}
    </section>
  );
}
