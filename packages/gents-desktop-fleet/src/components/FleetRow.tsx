import { useState, type KeyboardEvent, type MouseEvent } from "react";

import { formatPeerConnectionError } from "../peerConnectionErrors.js";
import type {
  BootstrapSummary,
  DeploymentView,
} from "@source-inc/gents-desktop-client";
import { ConfirmDialog } from "@source-inc/gents-desktop-ui";
import {
  ChatIcon,
  ConfigIcon,
  PencilIcon,
  ToolIconGlyph,
  TrashIcon,
} from "./FleetIcons.js";
import {
  deploymentStatus,
  formatRelativeTime,
  inferenceBackendTitle,
  isLocalRuntimeSource,
  needsInferenceSetup,
  toolCeilingIcons,
  type ToolIcon,
} from "../fleetMetrics.js";

function isTerminalTurnState(turnState?: string | null) {
  return (
    turnState === "completed" ||
    turnState === "failed" ||
    turnState === "superseded" ||
    turnState === "interrupted"
  );
}

export type FleetRowProps = {
  bootstrap: BootstrapSummary | null;
  deployment: DeploymentView;
  onOpenChat: (agentDid: string) => void;
  onOpenConfig: (agentDid: string) => void;
  onRemovePeer?: (peerId: string) => Promise<unknown> | void;
  onRenamePeer?: (peerId: string, label: string) => Promise<unknown> | void;
  onSetupInference?: (deployment: DeploymentView) => void;
};

export function FleetRow({
  bootstrap,
  deployment,
  onOpenChat,
  onOpenConfig,
  onRemovePeer,
  onRenamePeer,
  onSetupInference,
}: FleetRowProps) {
  const [editingLabel, setEditingLabel] = useState<string | null>(null);
  const [confirmingRemove, setConfirmingRemove] = useState(false);
  const status = deploymentStatus(deployment);
  const localRuntime = isLocalRuntimeSource(deployment.source);
  const chatReady = deployment.pairingReady;

  function commitRename() {
    const label = (editingLabel ?? "").trim();
    setEditingLabel(null);
    if (label && label !== deployment.label && onRenamePeer) {
      void Promise.resolve(onRenamePeer(deployment.peerId, label)).catch(
        () => {},
      );
    }
  }
  const enabledTaskCount = deployment.tasks.filter(
    (task) => task.enabled !== false,
  ).length;
  const backendCount = deployment.inferenceBackends.filter(
    (backend) => backend.enabled !== false,
  ).length;
  const inferenceSetupNeeded = needsInferenceSetup(deployment);
  const openWorkCount = deployment.conversations.filter(
    (conversation) =>
      conversation.turnState && !isTerminalTurnState(conversation.turnState),
  ).length;
  const defaultBehavior = deployment.behaviors.find(
    (behavior) =>
      behavior.behaviorId ===
      (deployment.defaultBehaviorId ??
        deployment.agentPrincipal.defaultBehaviorId),
  );
  const toolIcons = toolCeilingIcons(
    deployment.toolSelections,
    defaultBehavior?.toolSelectionId,
    isLocalRuntimeSource(deployment.source) ? bootstrap?.initToolCeiling : null,
  );
  const runtimeLastUpdate = deployment.runtime?.updatedAt ?? null;

  function openDetailsFromCard(
    event: MouseEvent<HTMLTableRowElement> | KeyboardEvent<HTMLTableRowElement>,
  ) {
    const target = event.target;
    if (
      target instanceof Element &&
      target.closest("button, input, select, textarea, a")
    ) {
      return;
    }
    onOpenChat(deployment.agentDid);
  }

  return (
    <tr
      aria-label={`Open ${deployment.label} details`}
      className="fleet-deployment-row"
      data-testid={`fleet-row-${deployment.peerId}`}
      onClick={openDetailsFromCard}
      onKeyDown={(event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          openDetailsFromCard(event);
        }
      }}
      role="button"
      tabIndex={0}
    >
      <td>
        <div className="fleet-agent-cell">
          <span
            aria-label={status.label}
            className={`fleet-status ${status.tone}`}
            title={status.title}
          >
            <span
              aria-hidden="true"
              className={`fleet-status-dot ${status.tone}`}
            />
            <span
              className="fleet-status-label"
              data-testid={`fleet-status-${deployment.peerId}`}
            >
              {status.label}
            </span>
          </span>
          <div className="fleet-agent-copy">
            {editingLabel != null ? (
              <input
                autoFocus
                className="fleet-rename-input"
                data-testid={`fleet-rename-input-${deployment.peerId}`}
                onBlur={commitRename}
                onChange={(event) => setEditingLabel(event.currentTarget.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter") {
                    commitRename();
                  } else if (event.key === "Escape") {
                    setEditingLabel(null);
                  }
                }}
                value={editingLabel}
              />
            ) : (
              <button
                className="fleet-agent-name"
                data-testid={`fleet-detail-name-${deployment.peerId}`}
                onClick={() => onOpenChat(deployment.agentDid)}
                title={`Open ${deployment.label} details`}
                type="button"
              >
                {deployment.agentPrincipal.displayName ?? deployment.label}
              </button>
            )}
            {onRenamePeer && editingLabel == null ? (
              <button
                aria-label={`Rename ${deployment.label}`}
                className="ghost-button fleet-row-icon fleet-rename"
                data-testid={`fleet-rename-${deployment.peerId}`}
                onClick={() => setEditingLabel(deployment.label)}
                title="Rename deployment (saved label)"
                type="button"
              >
                <PencilIcon />
              </button>
            ) : null}
            <span
              className="muted fleet-agent-summary"
              data-testid={`fleet-summary-${deployment.peerId}`}
            >
              {deployment.behaviors.length} behaviors ·{" "}
              {deployment.conversations.length} conversations ·{" "}
              {deployment.tasks.length} tasks
            </span>
            {status.lastError ? (
              <span
                className="fleet-agent-error"
                data-testid={`fleet-error-${deployment.peerId}`}
              >
                {formatPeerConnectionError(status.lastError, "repair-p2p")}
              </span>
            ) : null}
          </div>
        </div>
      </td>
      <td>
        <Metric value={deployment.behaviors.length} label="total" />
      </td>
      <td>
        <Metric value={enabledTaskCount} label="enabled" />
      </td>
      <td>
        <div className="fleet-inference-cell">
          <Metric
            label={backendCount === 1 ? "backend" : "backends"}
            title={inferenceBackendTitle(deployment)}
            value={backendCount}
          />
          {inferenceSetupNeeded ? (
            onSetupInference ? (
              <button
                className="ghost-button fleet-inference-needed"
                data-testid={`fleet-inference-setup-${deployment.peerId}`}
                onClick={() => onSetupInference(deployment)}
                title={`Configure inference for ${deployment.label}`}
                type="button"
              >
                Setup needed
              </button>
            ) : (
              <span className="fleet-inference-needed">Setup needed</span>
            )
          ) : null}
        </div>
      </td>
      <td>
        <ToolIconStrip icons={toolIcons} />
      </td>
      <td>
        <Metric title="Processing conversations" value={openWorkCount} />
      </td>
      <td title="Last runtime state change reported by this agent (agents write this on change, not on a timer — an idle agent ages here without being dead)">
        {formatRelativeTime(runtimeLastUpdate)}
      </td>
      <td className="fleet-actions-cell">
        <div className="fleet-row-actions">
          <button
            aria-label={`Open ${deployment.label} chat`}
            className="primary-button fleet-table-action fleet-open-chat-action"
            data-testid={`fleet-chat-${deployment.peerId}`}
            disabled={!chatReady}
            onClick={() => onOpenChat(deployment.agentDid)}
            title={
              chatReady
                ? "Open chat"
                : "Chat unlocks after signed reciprocal pairing completes"
            }
            type="button"
          >
            <ChatIcon />
          </button>
          <button
            aria-label={`Configure ${deployment.label}`}
            className="ghost-button fleet-table-action"
            data-testid={`fleet-config-${deployment.peerId}`}
            onClick={() => onOpenConfig(deployment.agentDid)}
            title="Configure agent"
            type="button"
          >
            <ConfigIcon />
          </button>
          {onRemovePeer && !localRuntime ? (
            <button
              aria-label={`Remove ${deployment.label}`}
              className="ghost-button fleet-table-action danger-button"
              data-testid={`fleet-remove-${deployment.peerId}`}
              onClick={() => setConfirmingRemove(true)}
              title="Remove saved deployment"
              type="button"
            >
              <TrashIcon />
            </button>
          ) : null}
          <ConfirmDialog
            open={confirmingRemove}
            title="Remove deployment"
            message={`Remove "${deployment.label}" from this desktop's saved deployments? The remote agent itself is not touched.`}
            confirmLabel="Remove"
            danger
            onConfirm={() => {
              setConfirmingRemove(false);
              void Promise.resolve(onRemovePeer?.(deployment.peerId)).catch(
                () => {},
              );
            }}
            onCancel={() => setConfirmingRemove(false)}
          />
        </div>
      </td>
    </tr>
  );
}

function Metric({
  label,
  title,
  value,
}: {
  label?: string;
  title?: string;
  value: number;
}) {
  return (
    <span className="fleet-metric" title={title}>
      {value}
      {label ? <span>{label}</span> : null}
    </span>
  );
}

function ToolIconStrip({ icons }: { icons: ToolIcon[] }) {
  if (!icons.length) {
    return <span className="muted">none</span>;
  }

  return (
    <div className="fleet-tool-icons">
      {icons.map((icon) => (
        <span
          className={`fleet-tool-icon ${icon.tone}`}
          key={`${icon.kind}-${icon.title}`}
          title={icon.title}
        >
          <ToolIconGlyph kind={icon.kind} />
        </span>
      ))}
    </div>
  );
}
