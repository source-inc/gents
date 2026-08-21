import { useEffect, useMemo, useState } from "react";

import { formatTokenTotals, tokenTotalsForRun } from "../graph/tokenTotals.ts";
import { probeHealth, type RuntimeHealth } from "../live/pollRuntime.ts";
import { DefenseDag } from "./DefenseDag.tsx";
import { DefenseDrawer } from "./DefenseDrawer.tsx";
import { emptyDefenseSnapshot, loadDefenseSnapshot } from "./pollDefending.ts";
import { projectDefenseGraph } from "./projectDefenseGraph.ts";
import type { DefenseNode } from "./types.ts";

export function DefendingApp() {
  const [health, setHealth] = useState<RuntimeHealth>("offline");
  const [snapshot, setSnapshot] = useState(emptyDefenseSnapshot);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [followedRunId, setFollowedRunId] = useState<string | null>(() =>
    new URLSearchParams(window.location.search).get("run"),
  );

  const followRun = (runId: string | null) => {
    setFollowedRunId(runId);
    const url = new URL(window.location.href);
    if (runId) {
      url.searchParams.set("run", runId);
    } else {
      url.searchParams.delete("run");
    }
    window.history.replaceState(null, "", url);
  };

  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      const up = await probeHealth();
      if (!cancelled) {
        setHealth(up ? "ready" : "offline");
      }
    };
    void tick();
    const timer = window.setInterval(() => void tick(), 1000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, []);

  useEffect(() => {
    if (health === "offline") {
      return;
    }
    let cancelled = false;
    const tick = async () => {
      try {
        const next = await loadDefenseSnapshot();
        if (!cancelled) {
          setSnapshot(next);
          setHealth("ready");
        }
      } catch {
        if (!cancelled) {
          setHealth("query-failed");
        }
      }
    };
    void tick();
    const timer = window.setInterval(() => void tick(), 1500);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [health === "offline"]);

  const graph = useMemo(
    () => projectDefenseGraph(snapshot, { pinnedRunId: followedRunId }),
    [followedRunId, snapshot],
  );
  const selected = graph.nodes.find((node) => node.id === selectedId) ?? null;
  const tokens = useMemo(
    () => tokenTotalsForRun(snapshot.calls, snapshot.requests, graph.runId),
    [graph.runId, snapshot.calls, snapshot.requests],
  );
  const counts = useMemo(
    () => countsForRun(snapshot, graph.runId),
    [graph.runId, snapshot],
  );

  return (
    <div className="stage defending-stage">
      <header className="stage-bar">
        <strong>Gents defending code</strong>
        <span className="status">
          <span className={`dot ${health === "ready" ? "on" : ""}`} />
          {statusLine(health, graph.runId)}
        </span>
      </header>
      <div className="stage-body">
        <aside className="pane rail defense-rail">
          <p className="eyebrow">Campaign graph</p>
          <p className="talk-lead">
            Typed documents are both durable evidence and event edges. Select
            any node to inspect its row, request, token use, and tools.
          </p>
          <ol className="edge-list">
            {[
              ["Threat model", "Map assets, entry points, trust boundaries"],
              ["Plan", "Write one closed set of review areas"],
              ["Discover × N", "Parallel recall-first vulnerability scans"],
              [
                "Assign × K",
                "Documents trigger isolated verifier requests",
              ],
              ["Triage", "Completion barrier reduces the closed ledger"],
              ["Cluster × M", "Collapse consequences into root causes"],
              ["Contract × M", "Check specs, tests, and intended behavior"],
              ["Patch × M", "Draft, validate, maintain, then re-attack"],
              ["Report", "Reconcile the complete typed ledger"],
            ].map(([title, detail]) => (
              <li key={title} className="edge-step">
                <div className="edge-write">{title}</div>
                <div className="edge-arrow">{detail}</div>
              </li>
            ))}
          </ol>
          <p className="eyebrow defense-count-title">Live ledger</p>
          <dl className="ledger-counts">
            {Object.entries(counts).map(([label, value]) => (
              <div key={label}>
                <dt>{label}</dt>
                <dd>{value}</dd>
              </div>
            ))}
          </dl>
          <article className="feature-card boundary-card">
            <h3>Trust boundary</h3>
            <p>
              Discovery and review use read-only files, shell, and LSP. Patch
              authors write only graph documents. Validators apply drafts in
              disposable local clones until managed workspaces can bind each
              request to a runtime-enforced isolated root.
            </p>
          </article>
        </aside>
        <main className="pane dag-pane">
          <div className="live-head">
            <p className="eyebrow">Live run</p>
            <p className="token-totals">{formatTokenTotals(tokens)}</p>
          </div>
          <div className="state-legend" aria-label="Stage state colors">
            {[
              ["expected", "expected"],
              ["live", "running"],
              ["done", "complete"],
              ["waiting-group", "waiting"],
              ["failed", "failed"],
            ].map(([state, label]) => (
              <span key={state}>
                <i className={`legend-swatch state-${state}`} />
                {label}
              </span>
            ))}
          </div>
          {snapshot.jobs.length > 1 ? (
            <div className="run-chips">
              {snapshot.jobs.map((job) => (
                <button
                  key={job.run_id}
                  type="button"
                  className={`run-chip${graph.runId === job.run_id ? " on" : ""}`}
                  onClick={() => followRun(job.run_id)}
                >
                  {job.run_id}
                </button>
              ))}
            </div>
          ) : null}
          <DefenseDag
            graph={graph}
            selectedId={selectedId}
            onSelect={(node: DefenseNode) => setSelectedId(node.id)}
          />
        </main>
        <aside className="pane session-pane">
          <DefenseDrawer node={selected} snapshot={snapshot} />
        </aside>
      </div>
    </div>
  );
}

function statusLine(health: RuntimeHealth, runId: string | null): string {
  if (health === "offline") {
    return "waiting for runtime";
  }
  if (health === "query-failed") {
    return "graph query failed";
  }
  return runId
    ? `runtime ready · ${runId}`
    : "runtime ready · waiting for DefendingCodeJob";
}

function countsForRun(
  snapshot: ReturnType<typeof emptyDefenseSnapshot>,
  runId: string | null,
) {
  const count = (rows: { run_id: string }[]) =>
    rows.filter((row) => row.run_id === runId).length;
  return {
    areas: count(snapshot.areas),
    scans: count(snapshot.scans),
    candidates: count(snapshot.candidates),
    assignments: count(snapshot.verificationAssignments),
    completions: count(snapshot.verificationCompletions),
    verdicts: count(snapshot.verdicts),
    confirmed: count(snapshot.findings),
    clusters: count(snapshot.clusters),
    contracts: count(snapshot.contractReviews),
    patches: count(snapshot.patches),
    validations: count(snapshot.validations),
    reviews: count(snapshot.reviews),
    reattacks: count(snapshot.securityReviews),
  };
}
