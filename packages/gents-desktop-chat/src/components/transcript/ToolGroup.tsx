import type {
  RenderedToolCallView,
  ToolPresentationView,
} from "@source-inc/gents-desktop-client";
import { CopyButton } from "@source-inc/gents-desktop-ui";

import { CancelCauseBadge, CancelCauseDetails } from "../cancelUx/index.js";
import { CommandDenialToolItem } from "../commandDenial/index.js";

function statusClass(statusKind: string) {
  switch (statusKind.toLowerCase()) {
    case "success":
      return "tool-item-dot tool-item-dot-success";
    case "error":
      return "tool-item-dot tool-item-dot-error";
    case "awaitingapproval":
      return "tool-item-dot tool-item-dot-held";
    default:
      return "tool-item-dot tool-item-dot-running";
  }
}

function statusLabel(tool: RenderedToolCallView) {
  switch (tool.statusKind.toLowerCase()) {
    case "success":
      return "completed";
    case "error":
      return tool.status?.trim() || "failed";
    case "awaitingapproval":
      return "awaiting approval";
    default:
      return tool.status?.trim() || "working";
  }
}

function compact(value: string | null | undefined, maxLength = 80) {
  const flat = value?.replace(/\s+/g, " ").trim();
  if (!flat) return null;
  return flat.length > maxLength ? `${flat.slice(0, maxLength)}…` : flat;
}

function formatPayload(value: string) {
  try {
    return JSON.stringify(JSON.parse(value), null, 2);
  } catch {
    return value;
  }
}

function Payload({ label, value }: { label: string; value?: string | null }) {
  if (!value?.trim()) return null;
  const formatted = formatPayload(value);
  return (
    <div className="tool-payload-section">
      <div className="tool-detail-label">{label}</div>
      <div className="tool-payload-wrap">
        <CopyButton className="tool-payload-copy" getText={() => formatted} />
        <pre className="tool-payload">{formatted}</pre>
      </div>
    </div>
  );
}

function LiveOutput({ tool }: { tool: RenderedToolCallView }) {
  const tail =
    tool.statusKind.toLowerCase() === "running"
      ? (tool.partialOutputTail ?? null)
      : null;
  if (!tail) return null;
  return (
    <div className="tool-live-tail" data-testid={`tool-live-${tool.itemKey}`}>
      <span className="tool-live-tail-label">
        live output
        <span aria-hidden="true" className="tool-live-dot" />
      </span>
      <pre>{tail}</pre>
    </div>
  );
}

function commonBadges(tool: RenderedToolCallView) {
  return (
    <>
      {tool.awaitMode ? (
        <span className="tool-item-mode">{tool.awaitMode}</span>
      ) : null}
      <span
        className={`tool-item-status tool-item-status-${tool.statusKind.toLowerCase()}`}
      >
        {statusLabel(tool)}
      </span>
      {tool.cancelCause ? (
        <CancelCauseBadge
          cause={tool.cancelCause}
          className="tool-item-cause-badge"
        />
      ) : null}
    </>
  );
}

function commandExit(
  presentation: Extract<ToolPresentationView, { kind: "command" }>,
) {
  if (presentation.timedOut) return "timed out";
  if (presentation.exitCode != null) return `exit ${presentation.exitCode}`;
  if (presentation.failed) return "failed";
  return null;
}

function readCount(
  presentation: Extract<ToolPresentationView, { kind: "fileRead" }>,
) {
  if (presentation.returnedCount == null) return null;
  const total =
    presentation.totalCount != null &&
    presentation.totalCount !== presentation.returnedCount
      ? ` of ${presentation.totalCount}`
      : "";
  return `${presentation.returnedCount}${total}${presentation.truncated ? " · truncated" : ""}`;
}

function ToolSummary({ tool }: { tool: RenderedToolCallView }) {
  const view = tool.presentation;
  if (view.kind === "command") {
    const exit = commandExit(view);
    return (
      <>
        <span aria-hidden="true" className="tool-command-prompt mono">
          $
        </span>
        <span className="tool-primary mono">{view.command}</span>
        {commonBadges(tool)}
        {exit ? (
          <span className={view.failed ? "tool-exit is-error" : "tool-exit"}>
            {exit}
          </span>
        ) : null}
      </>
    );
  }
  if (view.kind === "fileRead") {
    return (
      <>
        <span className="tool-kind">{view.operation.replace("_file", "")}</span>
        <span className="tool-primary mono">
          {view.target ?? tool.toolName}
        </span>
        {readCount(view) ? (
          <span className="tool-secondary">{readCount(view)}</span>
        ) : null}
        {commonBadges(tool)}
      </>
    );
  }
  if (view.kind === "fileEdit") {
    const verb =
      view.created === true
        ? "created"
        : view.created === false && view.operation === "write_file"
          ? "overwrote"
          : tool.statusKind === "running"
            ? view.operation === "write_file"
              ? "writing"
              : "editing"
            : "edited";
    return (
      <>
        <span className="tool-kind">{verb}</span>
        <span className="tool-primary mono">{view.path ?? tool.toolName}</span>
        {view.replacementsApplied != null && view.replacementsApplied > 1 ? (
          <span className="tool-secondary">×{view.replacementsApplied}</span>
        ) : null}
        {commonBadges(tool)}
      </>
    );
  }
  if (view.kind === "subagent") {
    return (
      <>
        <span className="tool-kind">subagent · {view.action}</span>
        <span className="tool-primary">
          {view.name ?? view.childRequestId ?? "subagent"}
        </span>
        {commonBadges(tool)}
        {compact(view.description) ? (
          <span className="tool-secondary tool-summary-preview">
            {compact(view.description)}
          </span>
        ) : null}
      </>
    );
  }
  if (view.kind === "process") {
    return (
      <>
        <span className="tool-kind">process · {view.action}</span>
        <span className="tool-primary mono">
          {view.target ?? "background work"}
        </span>
        {commonBadges(tool)}
      </>
    );
  }
  if (view.kind === "mcp") {
    return (
      <>
        <span className="tool-kind">MCP</span>
        <span className="tool-primary">
          {view.selectedToolName ?? tool.toolName}
        </span>
        {view.serviceId ? (
          <span className="tool-secondary">{view.serviceId}</span>
        ) : null}
        {commonBadges(tool)}
      </>
    );
  }
  return (
    <>
      <span className="tool-primary">{tool.toolName}</span>
      {compact(view.summary) ? (
        <span className="tool-secondary tool-summary-preview">
          {compact(view.summary)}
        </span>
      ) : null}
      {commonBadges(tool)}
    </>
  );
}

function ToolBody({ tool }: { tool: RenderedToolCallView }) {
  const view = tool.presentation;
  return (
    <div className="tool-item-body">
      {tool.cancelCause ? (
        <CancelCauseDetails cause={tool.cancelCause} />
      ) : null}
      {tool.awaitMode || tool.cancelPolicy || tool.deadlineAt ? (
        <div
          className="tool-meta muted small"
          data-testid={`tool-lifecycle-${tool.itemKey}`}
        >
          {tool.awaitMode ? <span>await: {tool.awaitMode}</span> : null}
          {tool.cancelPolicy ? <span>cancel: {tool.cancelPolicy}</span> : null}
          {tool.deadlineAt ? <span>deadline: {tool.deadlineAt}</span> : null}
        </div>
      ) : null}
      {view.kind === "command" ? (
        <>
          {view.durationMs != null ||
          view.cwd ||
          view.executionMode ||
          view.networkMode ? (
            <div className="tool-meta muted small">
              {view.durationMs != null ? (
                <span>{formatDuration(view.durationMs)}</span>
              ) : null}
              {view.cwd ? <span className="mono">{view.cwd}</span> : null}
              {view.executionMode ? (
                <span>sandbox: {view.executionMode}</span>
              ) : null}
              {view.networkMode ? (
                <span>network: {view.networkMode}</span>
              ) : null}
            </div>
          ) : null}
          <Payload label="stdout" value={view.stdout} />
          <Payload label="stderr" value={view.stderr} />
          <Payload label="output" value={view.fallbackOutput} />
        </>
      ) : null}
      {view.kind === "fileRead" ? (
        <>
          <Payload label="contents" value={view.body} />
          <Payload label="output" value={view.fallbackOutput} />
        </>
      ) : null}
      {view.kind === "fileEdit" ? (
        <>
          {view.diff.length > 0 ? (
            <div className="tool-payload-wrap">
              <CopyButton
                className="tool-payload-copy"
                getText={() =>
                  view.diff
                    .map(
                      (line) =>
                        `${line.kind === "add" ? "+" : "-"}${line.text}`,
                    )
                    .join("\n")
                }
              />
              <pre className="tool-diff">
                {view.diff.map((line, index) => (
                  <span
                    className={`tool-diff-line is-${line.kind}`}
                    key={`${line.kind}-${index}`}
                  >
                    <span aria-hidden="true">
                      {line.kind === "add" ? "+" : "-"}
                    </span>
                    <span>{line.text}</span>
                  </span>
                ))}
              </pre>
            </div>
          ) : null}
          <Payload label="output" value={view.fallbackOutput} />
        </>
      ) : null}
      {view.kind === "subagent" ? (
        <>
          <Payload
            label={view.action === "spawn" ? "assignment" : "instruction"}
            value={view.description}
          />
          {view.childRequestId ? (
            <div className="tool-identity">
              <span className="tool-detail-label">child request</span>
              <code>{view.childRequestId}</code>
            </div>
          ) : null}
          <Payload label="result" value={view.output} />
        </>
      ) : null}
      {view.kind === "process" ? (
        <>
          <Payload label="arguments" value={view.description} />
          <Payload label="result" value={view.output} />
        </>
      ) : null}
      {view.kind === "mcp" ? (
        <>
          <Payload label="arguments" value={view.arguments} />
          <Payload label="result" value={view.output} />
        </>
      ) : null}
      {view.kind === "generic" ? (
        <>
          <Payload label="input" value={view.input} />
          <Payload label="result" value={view.output} />
        </>
      ) : null}
      <LiveOutput tool={tool} />
    </div>
  );
}

function UnifiedToolItem({ tool }: { tool: RenderedToolCallView }) {
  const live =
    tool.statusKind.toLowerCase() === "running" &&
    Boolean(tool.partialOutputTail);
  return (
    <details
      className={`tool-item tool-item-${tool.presentation.kind}`}
      data-child-request-id={tool.childRequestId ?? undefined}
      data-testid={`tool-${tool.itemKey}`}
      open={live}
    >
      <summary className="tool-item-summary">
        <span className={statusClass(tool.statusKind)} aria-hidden="true" />
        <span className="tool-item-summary-content">
          <ToolSummary tool={tool} />
        </span>
        <span aria-hidden="true" className="tool-item-action">
          ▸
        </span>
      </summary>
      <ToolBody tool={tool} />
    </details>
  );
}

export function ToolGroup({ tools }: { tools: RenderedToolCallView[] }) {
  return (
    <section className="tool-group">
      {tools.map((tool) => {
        const denial =
          tool.statusKind.toLowerCase() === "error" ? tool.denial : null;
        return denial ? (
          <CommandDenialToolItem
            denial={denial}
            key={tool.itemKey}
            tool={tool}
          />
        ) : (
          <UnifiedToolItem key={tool.itemKey} tool={tool} />
        );
      })}
    </section>
  );
}

function formatDuration(ms: number) {
  if (ms < 1000) return `${Math.round(ms)}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1).replace(/\.0$/, "")}s`;
  const minutes = Math.floor(ms / 60_000);
  const seconds = Math.round((ms % 60_000) / 1000);
  return seconds > 0 ? `${minutes}m ${seconds}s` : `${minutes}m`;
}
