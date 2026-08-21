import type { BehaviorEnvironmentView } from "@source-inc/gents-desktop-client";

export type BehaviorEnvironmentSectionProps = {
  environments: BehaviorEnvironmentView[];
  selectedAgentDid: string | null;
  selectedBehaviorId: string | null;
  onSelectBehavior: (behaviorId: string) => void;
  onStartNewConversation: (behaviorId: string) => void;
};

export function BehaviorEnvironmentSection({
  environments,
  selectedAgentDid,
  selectedBehaviorId,
  onSelectBehavior,
  onStartNewConversation,
}: BehaviorEnvironmentSectionProps) {
  if (!selectedAgentDid) {
    return <p className="muted">Select an agent to see its environments.</p>;
  }
  if (!environments.length) {
    return <p className="muted">No behaviors are configured.</p>;
  }

  return (
    <section
      aria-label="Behavior environments"
      className="behavior-environment-section"
    >
      <p className="behavior-environment-intro muted">
        Each behavior is an environment with its own model, workspace, and tool
        boundary.
      </p>
      <div className="behavior-environment-list">
        {environments.map((environment) => (
          <article
            className={
              environment.behaviorId === selectedBehaviorId
                ? "behavior-environment-card selected"
                : "behavior-environment-card"
            }
            data-testid={`sidebar-behavior-${environment.behaviorId}`}
            key={environment.behaviorId}
          >
            <button
              className="behavior-environment-select"
              onClick={() => onSelectBehavior(environment.behaviorId)}
              type="button"
            >
              <span className="behavior-environment-heading">
                <strong>{environment.displayName}</strong>
                {environment.isDefault ? (
                  <span className="environment-tag">default</span>
                ) : null}
                {!environment.enabled ? (
                  <span className="environment-tag environment-tag-muted">
                    disabled
                  </span>
                ) : null}
              </span>
              {environment.workspaceRoot ? (
                <span className="behavior-environment-workspace mono">
                  {environment.workspaceRoot}
                </span>
              ) : null}
              <span className="behavior-environment-summary">
                {environment.modelName ?? "Default model"}
                {environment.inferenceProfileName ? (
                  <>
                    <span aria-hidden="true"> · </span>
                    {environment.inferenceProfileName}
                  </>
                ) : null}
                <span aria-hidden="true"> · </span>
                files {environment.fileAccess}
                <span aria-hidden="true"> · </span>
                bash {environment.bashAccess}
                {environment.networkAccess ? (
                  <>
                    <span aria-hidden="true"> · </span>
                    network {environment.networkAccess.toLowerCase()}
                  </>
                ) : null}
              </span>
              {environment.skillNames.length ? (
                <span className="behavior-environment-skills">
                  {environment.skillNames.join(" · ")}
                </span>
              ) : null}
            </button>
            <footer className="behavior-environment-footer">
              <span className="muted">
                {environment.activeSessionCount > 0
                  ? `${environment.activeSessionCount} active · `
                  : ""}
                {environment.sessionCount} session
                {environment.sessionCount === 1 ? "" : "s"}
              </span>
              <button
                className="primary-button behavior-start-session"
                data-testid={`sidebar-new-chat-${environment.behaviorId}`}
                disabled={!environment.enabled}
                onClick={() => onStartNewConversation(environment.behaviorId)}
                type="button"
              >
                New session
              </button>
            </footer>
          </article>
        ))}
      </div>
    </section>
  );
}
