import type { DefenseGraph, DefenseNode } from "./types.ts";

type DefenseDagProps = {
  graph: DefenseGraph;
  selectedId: string | null;
  onSelect: (node: DefenseNode) => void;
};

export function DefenseDag({ graph, selectedId, onSelect }: DefenseDagProps) {
  const one = (kind: DefenseNode["kind"]) =>
    graph.nodes.find((node) => node.kind === kind);
  const job = one("job");
  const threat = one("threat");
  const plan = one("plan");
  const triage = one("triage");
  const verificationPlan = one("verification-plan");
  const clusterPlan = one("cluster-plan");
  const remediationPlan = one("remediation-plan");
  const report = one("report");
  const areas = graph.nodes.filter((node) => node.kind === "area");
  const scans = bySuffix(
    graph.nodes.filter((node) => node.kind === "scan"),
    "scan:",
  );
  const assignments = graph.nodes.filter((node) => node.kind === "assignment");
  const clusters = graph.nodes.filter((node) => node.kind === "cluster");
  const contractReviews = bySuffix(
    graph.nodes.filter((node) => node.kind === "contract-review"),
    "contract-review:",
  );
  const candidates = graph.nodes.filter((node) => node.kind === "candidate");
  const verificationAssignments = bySuffix(
    graph.nodes.filter((node) => node.kind === "verification-assignment"),
    "verification-assignment:",
  );
  const verifiers = bySuffix(
    graph.nodes.filter((node) => node.kind === "verifier"),
    "verifier:",
  );
  const verdicts = bySuffix(
    graph.nodes.filter((node) => node.kind === "verdict"),
    "verdict:",
  );
  const legacySerialTriage = triage?.badges.includes("serial triage") ?? false;
  const patches = bySuffix(
    graph.nodes.filter((node) => node.kind === "patch"),
    "patch:",
  );
  const reviews = bySuffix(
    graph.nodes.filter((node) => node.kind === "review"),
    "review:",
  );
  const validations = bySuffix(
    graph.nodes.filter((node) => node.kind === "validation"),
    "validation:",
  );
  const securityReviews = bySuffix(
    graph.nodes.filter((node) => node.kind === "security-review"),
    "security-review:",
  );

  return (
    <div className="dag defense-dag" data-testid="defense-dag">
      {job ? (
        <DagNode
          node={job}
          selected={selectedId === job.id}
          onSelect={onSelect}
        />
      ) : null}
      <Edge />
      {threat ? (
        <DagNode
          node={threat}
          selected={selectedId === threat.id}
          onSelect={onSelect}
        />
      ) : null}
      <Edge />
      {plan ? (
        <DagNode
          node={plan}
          selected={selectedId === plan.id}
          onSelect={onSelect}
        />
      ) : null}
      <FanLabel>parallel discovery</FanLabel>
      <div className="dag-join down" />
      <div className="dag-fan defense-fan area-fan">
        {areas.map((area) => {
          const suffix = area.id.replace(/^area:/, "");
          const scan = scans.get(suffix);
          return (
            <div key={area.id} className="dag-col">
              <DagNode
                node={area}
                selected={selectedId === area.id}
                onSelect={onSelect}
              />
              {scan ? (
                <>
                  <Edge short />
                  <DagNode
                    node={scan}
                    selected={selectedId === scan.id}
                    onSelect={onSelect}
                  />
                </>
              ) : null}
            </div>
          );
        })}
      </div>
      <div className="dag-join up" />
      {triage ? (
        <section className="nested-dag triage-dag" aria-label="Triage subgraph">
          <p className="nested-dag-title">
            {verificationPlan
              ? "verification DAG · documents + triggered workers"
              : "triage DAG · coordinator + workers"}
          </p>
          {verificationPlan ? (
            <DagNode
              node={verificationPlan}
              selected={selectedId === verificationPlan.id}
              onSelect={onSelect}
            />
          ) : (
            <DagNode
              node={triage}
              selected={selectedId === triage.id}
              onSelect={onSelect}
            />
          )}
          {candidates.length > 0 ? (
            <>
              <FanLabel>
                {legacySerialTriage
                  ? "serial processing · active candidate is not persisted"
                  : verifiers.size > 0
                    ? "one isolated verifier per candidate"
                    : "candidate queue · awaiting verifier requests"}
              </FanLabel>
              <div className="dag-join down nested-join" />
              <div className="dag-fan defense-fan triage-fan">
                {candidates.map((candidate) => {
                  const suffix = candidate.id.replace(/^candidate:/, "");
                  const assignment = verificationAssignments.get(suffix);
                  const verifier = verifiers.get(suffix);
                  const verdict = verdicts.get(suffix);
                  return (
                    <div key={candidate.id} className="dag-col">
                      <DagNode
                        node={candidate}
                        selected={selectedId === candidate.id}
                        onSelect={onSelect}
                      />
                      {assignment ? (
                        <>
                          <Edge short />
                          <DagNode
                            node={assignment}
                            selected={selectedId === assignment.id}
                            onSelect={onSelect}
                          />
                        </>
                      ) : null}
                      {verifier ? (
                        <>
                          <Edge short />
                          <DagNode
                            node={verifier}
                            selected={selectedId === verifier.id}
                            onSelect={onSelect}
                          />
                        </>
                      ) : null}
                      {verdict ? (
                        <>
                          <Edge short />
                          <DagNode
                            node={verdict}
                            selected={selectedId === verdict.id}
                            onSelect={onSelect}
                          />
                        </>
                      ) : null}
                    </div>
                  );
                })}
              </div>
              <div className="dag-join up nested-join" />
              <p className="nested-dag-exit">closed verification ledger</p>
              {verificationPlan ? (
                <>
                  <Edge short />
                  <DagNode
                    node={triage}
                    selected={selectedId === triage.id}
                    onSelect={onSelect}
                  />
                </>
              ) : null}
            </>
          ) : null}
        </section>
      ) : null}
      <Edge />
      {clusterPlan ? (
        <section
          className="nested-dag cluster-dag"
          aria-label="Root-cause subgraph"
        >
          <p className="nested-dag-title">
            remediation DAG · root causes + contract workers
          </p>
          <DagNode
            node={clusterPlan}
            selected={selectedId === clusterPlan.id}
            onSelect={onSelect}
          />
          <FanLabel>one independent contract reviewer per root cause</FanLabel>
          <div className="dag-join down nested-join" />
          <div className="dag-fan defense-fan cluster-fan">
            {clusters.map((cluster) => {
              const suffix = cluster.id.replace(/^cluster:/, "");
              const contractReview = contractReviews.get(suffix);
              return (
                <div key={cluster.id} className="dag-col">
                  <DagNode
                    node={cluster}
                    selected={selectedId === cluster.id}
                    onSelect={onSelect}
                  />
                  <Edge short />
                  {contractReview ? (
                    <DagNode
                      node={contractReview}
                      selected={selectedId === contractReview.id}
                      onSelect={onSelect}
                    />
                  ) : null}
                </div>
              );
            })}
          </div>
          <div className="dag-join up nested-join" />
          {remediationPlan ? (
            <DagNode
              node={remediationPlan}
              selected={selectedId === remediationPlan.id}
              onSelect={onSelect}
            />
          ) : null}
        </section>
      ) : null}
      <FanLabel>
        {clusterPlan
          ? "one contract-aware patch pipeline per root cause"
          : "confirmed-finding patches"}
      </FanLabel>
      <div className="dag-join down" />
      <div className="dag-fan defense-fan patch-fan">
        {assignments.map((assignment) => {
          const suffix = assignment.id.replace(/^assignment:/, "");
          const patch = patches.get(suffix);
          const validation = validations.get(suffix);
          const review = reviews.get(suffix);
          const securityReview = securityReviews.get(suffix);
          return (
            <div key={assignment.id} className="dag-col">
              <DagNode
                node={assignment}
                selected={selectedId === assignment.id}
                onSelect={onSelect}
              />
              <Edge short />
              {patch ? (
                <DagNode
                  node={patch}
                  selected={selectedId === patch.id}
                  onSelect={onSelect}
                />
              ) : null}
              {clusterPlan ? (
                <>
                  <Edge short />
                  {validation ? (
                    <DagNode
                      node={validation}
                      selected={selectedId === validation.id}
                      onSelect={onSelect}
                    />
                  ) : null}
                </>
              ) : null}
              <Edge short />
              {review ? (
                <DagNode
                  node={review}
                  selected={selectedId === review.id}
                  onSelect={onSelect}
                />
              ) : null}
              {clusterPlan ? (
                <>
                  <Edge short />
                  {securityReview ? (
                    <DagNode
                      node={securityReview}
                      selected={selectedId === securityReview.id}
                      onSelect={onSelect}
                    />
                  ) : null}
                </>
              ) : null}
            </div>
          );
        })}
      </div>
      <div className="dag-join up" />
      {report ? (
        <DagNode
          node={report}
          selected={selectedId === report.id}
          onSelect={onSelect}
        />
      ) : null}
    </div>
  );
}

function bySuffix(
  nodes: DefenseNode[],
  prefix: string,
): Map<string, DefenseNode> {
  return new Map(
    nodes.map((node) => [node.id.replace(new RegExp(`^${prefix}`), ""), node]),
  );
}

function Edge({ short = false }: { short?: boolean }) {
  return <div className={`dag-edge${short ? " short" : ""}`} />;
}

function FanLabel({ children }: { children: string }) {
  return <p className="fan-label">{children}</p>;
}

function DagNode({
  node,
  selected,
  onSelect,
}: {
  node: DefenseNode;
  selected: boolean;
  onSelect: (node: DefenseNode) => void;
}) {
  return (
    <button
      type="button"
      title={node.detail || node.label}
      className={`dag-node kind-${node.kind} state-${node.state}${selected ? " selected" : ""}`}
      onClick={() => onSelect(node)}
    >
      <span className="dag-label">{node.label}</span>
      {node.badges.length > 0 ? (
        <span className="dag-badges">
          {node.badges.map((badge) => (
            <span key={badge} className="chip">
              {badge}
            </span>
          ))}
        </span>
      ) : null}
    </button>
  );
}
