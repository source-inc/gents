import { useEffect, useState } from "react";

import { loadSession, type SessionPayload } from "../live/pollRuntime.ts";
import type { DefenseNode, DefenseSnapshot } from "./types.ts";

type DefenseDrawerProps = {
  node: DefenseNode | null;
  snapshot: DefenseSnapshot;
};

export function DefenseDrawer({ node, snapshot }: DefenseDrawerProps) {
  const [payload, setPayload] = useState<SessionPayload | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [openTool, setOpenTool] = useState<number | null>(null);

  useEffect(() => {
    setOpenTool(null);
    if (!node?.requestId) {
      setPayload(null);
      setError(null);
      return;
    }
    let cancelled = false;
    const tick = async () => {
      try {
        const next = await loadSession(node.requestId!, node.sessionId);
        if (!cancelled) {
          setPayload(next);
          setError(null);
        }
      } catch (cause) {
        if (!cancelled) {
          setError(cause instanceof Error ? cause.message : String(cause));
        }
      }
    };
    void tick();
    const timer = window.setInterval(() => void tick(), 1000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [node?.requestId, node?.sessionId]);

  useEffect(() => {
    if (openTool === null) {
      return;
    }
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setOpenTool(null);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [openTool]);

  if (!node) {
    return (
      <section className="session-drawer">
        <p className="eyebrow">Evidence</p>
        <h2>Select a stage</h2>
        <p className="muted">
          Click any document or worker to inspect its typed graph row,
          interpolated prompt, token use, and tool calls.
        </p>
      </section>
    );
  }

  const document = documentForNode(node, snapshot);
  const promptTokens = payload?.promptTokens ?? 0;
  const completionTokens = payload?.completionTokens ?? 0;
  const totalTokens = promptTokens + completionTokens;
  const tools = payload?.tools ?? [];
  const selectedTool = openTool === null ? null : tools[openTool];

  return (
    <section className="session-drawer">
      <p className="eyebrow">Evidence</p>
      <h2>{node.label}</h2>
      {node.detail ? <p className="session-detail">{node.detail}</p> : null}
      <dl className="session-meta">
        <div>
          <dt>state</dt>
          <dd>{node.state}</dd>
        </div>
        <div>
          <dt>request</dt>
          <dd className="mono">{node.requestId ?? "—"}</dd>
        </div>
        <div>
          <dt>tokens</dt>
          <dd>
            {totalTokens > 0
              ? `${totalTokens.toLocaleString()} (${promptTokens.toLocaleString()} in · ${completionTokens.toLocaleString()} out)`
              : "—"}
          </dd>
        </div>
      </dl>
      {error ? <p className="error-line">{error}</p> : null}
      <h3>{document?.collection ?? "Document"}</h3>
      {document ? (
        <pre className="doc-json">
          {JSON.stringify(document.fields, null, 2)}
        </pre>
      ) : (
        <p className="muted">This stage has not written its document yet.</p>
      )}
      <h3>Task prompt</h3>
      <pre className="prompt">
        {payload?.prompt || "Waiting for interpolated prompt…"}
      </pre>
      <h3>Tools</h3>
      {tools.length === 0 ? (
        <p className="muted">No tool calls yet.</p>
      ) : (
        <ul className="tool-list">
          {tools.map((tool, index) => (
            <li key={`${tool.tool_name ?? "tool"}-${index}`}>
              <button
                type="button"
                className="tool-open"
                onClick={() => setOpenTool(index)}
              >
                <code>{tool.tool_name}</code>
                <span>{tool.lifecycle_state || tool.status || ""}</span>
              </button>
            </li>
          ))}
        </ul>
      )}
      {selectedTool ? (
        <div
          className="modal-scrim"
          onClick={() => setOpenTool(null)}
          role="presentation"
        >
          <div
            className="modal"
            role="dialog"
            aria-modal="true"
            aria-label={selectedTool.tool_name ?? "tool call"}
            onClick={(event) => event.stopPropagation()}
          >
            <header className="modal-head">
              <div>
                <p className="eyebrow">Tool call</p>
                <h2>
                  <code>{selectedTool.tool_name}</code>
                </h2>
              </div>
              <button
                type="button"
                className="ghost-button"
                onClick={() => setOpenTool(null)}
              >
                Close
              </button>
            </header>
            <p className="muted">
              {selectedTool.lifecycle_state ||
                selectedTool.status ||
                "unknown status"}
            </p>
            <h3>Args</h3>
            <pre className="doc-json">{pretty(selectedTool.args)}</pre>
            <h3>Result</h3>
            <pre className="doc-json">{pretty(selectedTool.result)}</pre>
          </div>
        </div>
      ) : null}
    </section>
  );
}

function documentForNode(
  node: DefenseNode,
  snapshot: DefenseSnapshot,
): { collection: string; fields: Record<string, unknown> } | null {
  const run = (row: { run_id: string }) => row.run_id === node.runId;
  const suffix = node.id.slice(node.id.indexOf(":") + 1);
  const result = (() => {
    switch (node.kind) {
      case "job":
        return pair("DefendingCodeJob", snapshot.jobs.find(run));
      case "threat":
        return pair("DefenseThreatModel", snapshot.threats.find(run));
      case "plan":
        return {
          collection: "DefenseReviewArea plan",
          fields: {
            run_id: node.runId,
            area_count: snapshot.areas.filter(run).length,
            areas: snapshot.areas.filter(run).map((area) => area.area_id),
          },
        };
      case "area":
        return pair(
          "DefenseReviewArea",
          snapshot.areas.find((row) => run(row) && row.area_id === suffix),
        );
      case "scan":
        return pair(
          "DefenseScanResult",
          snapshot.scans.find((row) => run(row) && row.area_id === suffix),
        );
      case "triage":
        return pair("DefenseTriageSummary", snapshot.triage.find(run));
      case "candidate":
        return pair(
          "DefenseCandidateFinding",
          snapshot.candidates.find(
            (row) => run(row) && row.finding_id === suffix,
          ),
        );
      case "verification-plan":
        return {
          collection: "DefenseVerificationAssignment work set",
          fields: {
            run_id: node.runId,
            assignment_count: snapshot.verificationAssignments.filter(run)
              .length,
            completion_count: snapshot.verificationCompletions.filter(run)
              .length,
            assignments: snapshot.verificationAssignments
              .filter(run)
              .map((assignment) => assignment.assignment_id),
          },
        };
      case "verification-assignment":
        return pair(
          "DefenseVerificationAssignment",
          snapshot.verificationAssignments.find(
            (row) => run(row) && row.finding_id === suffix,
          ),
        );
      case "verifier":
        return null;
      case "verdict":
        return pair(
          "DefenseFindingVerdict",
          snapshot.verdicts.find(
            (row) => run(row) && row.finding_id === suffix,
          ),
        );
      case "cluster-plan":
        return {
          collection: "DefenseRootCauseCluster work set",
          fields: {
            run_id: node.runId,
            cluster_count: snapshot.clusters.filter(run).length,
            clusters: snapshot.clusters.filter(run).map((row) => row.cluster_id),
          },
        };
      case "cluster":
        return pair(
          "DefenseRootCauseCluster",
          snapshot.clusters.find(
            (row) => run(row) && row.cluster_id === suffix,
          ),
        );
      case "contract-review":
        return pair(
          "DefenseContractReview",
          snapshot.contractReviews.find(
            (row) => run(row) && row.cluster_id === suffix,
          ),
        );
      case "remediation-plan":
        return {
          collection: "DefensePatchAssignment work set",
          fields: {
            run_id: node.runId,
            contract_count: snapshot.contractReviews.filter(run).length,
            assignment_count: snapshot.assignments.filter(run).length,
            assignments: snapshot.assignments
              .filter(run)
              .map((row) => row.assignment_id),
          },
        };
      case "assignment":
        return pair(
          "DefensePatchAssignment",
          snapshot.assignments.find(
            (row) => run(row) && row.assignment_id === suffix,
          ),
        );
      case "patch":
        return pair(
          "DefensePatchCandidate",
          snapshot.patches.find((row) => run(row) && row.patch_id === suffix),
        );
      case "validation":
        return pair(
          "DefensePatchValidation",
          snapshot.validations.find(
            (row) => run(row) && row.patch_id === suffix,
          ),
        );
      case "review":
        return pair(
          "DefensePatchReview",
          snapshot.reviews.find((row) => run(row) && row.patch_id === suffix),
        );
      case "security-review":
        return pair(
          "DefensePatchSecurityReview",
          snapshot.securityReviews.find(
            (row) => run(row) && row.patch_id === suffix,
          ),
        );
      case "report":
        return pair("DefenseReport", snapshot.reports.find(run));
    }
  })();
  return result ?? null;
}

function pair(collection: string, row: object | undefined) {
  if (!row) {
    return null;
  }
  const fields = Object.fromEntries(
    Object.entries(row).filter(
      ([key, value]) =>
        key !== "_docID" &&
        value !== undefined &&
        value !== null &&
        value !== "",
    ),
  );
  return { collection, fields };
}

function pretty(raw?: string): string {
  if (!raw) {
    return "—";
  }
  try {
    return JSON.stringify(JSON.parse(raw), null, 2);
  } catch {
    return raw;
  }
}
