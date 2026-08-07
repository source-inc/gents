import { useCallback, useEffect, useRef, useState } from "react";

import { formatMessageTime } from "@source-inc/gents-desktop-ui";
import type {
  DesktopApiAdapter,
  RequestTimelineView,
  RunTimelineEventView,
} from "@source-inc/gents-desktop-client";
import { CopyButton } from "@source-inc/gents-desktop-ui";
import { useOperationsApi } from "../../apiContext.js";

export type RequestTracePanelProps = {
  agentDid: string;
  rootRequestId?: string | null;
  api?: DesktopApiAdapter;
};

export function RequestTracePanel({
  agentDid,
  rootRequestId,
  api: explicitApi,
}: RequestTracePanelProps) {
  const api = useOperationsApi(explicitApi);
  const [timeline, setTimeline] = useState<RequestTimelineView | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const generationRef = useRef(0);

  const load = useCallback(async () => {
    if (!rootRequestId) {
      return;
    }
    const generation = ++generationRef.current;
    setLoading(true);
    setError(null);
    try {
      const next = await api.fetchRequestTimeline(agentDid, rootRequestId);
      if (generationRef.current === generation) {
        setTimeline(next);
      }
    } catch (err) {
      if (generationRef.current === generation) {
        setError(err instanceof Error ? err.message : String(err));
      }
    } finally {
      if (generationRef.current === generation) {
        setLoading(false);
      }
    }
  }, [agentDid, api, rootRequestId]);

  useEffect(() => {
    setTimeline(null);
    void load();
  }, [load]);

  if (!rootRequestId) {
    return (
      <section className="trace-panel" data-testid="trace-panel">
        <p className="muted">No request selected — send a message first.</p>
      </section>
    );
  }

  return (
    <section className="trace-panel" data-testid="trace-panel">
      <div className="panel-summary trace-toolbar">
        <div className="live-count">
          <em>{timeline?.events.length ?? 0}</em> events
        </div>
        <div className="trace-toolbar-actions">
          {timeline ? (
            <CopyButton
              label="Copy JSON"
              getText={() => JSON.stringify(timeline, null, 2)}
            />
          ) : null}
          <button
            className="ghost-button"
            data-testid="trace-refresh"
            disabled={loading}
            onClick={() => void load()}
            type="button"
          >
            {loading ? "Loading..." : "Refresh"}
          </button>
        </div>
      </div>

      {error ? (
        <p className="trace-error" data-testid="trace-error" role="alert">
          Timeline failed: {error}
        </p>
      ) : null}
      {!error && !loading && timeline && timeline.events.length === 0 ? (
        <p className="muted">No persisted events for this request yet.</p>
      ) : null}

      <ol className="trace-events">
        {(timeline?.events ?? []).map((event, index) => (
          <TraceEventRow event={event} key={`${event.kind}-${index}`} />
        ))}
      </ol>
    </section>
  );
}

function TraceEventRow({ event }: { event: RunTimelineEventView }) {
  const time = eventTimestamp(event);
  const label = formatMessageTime(time);
  return (
    <li className={`trace-event trace-event-${event.kind}`}>
      <div className="trace-event-head">
        <span className={`chip trace-kind trace-kind-${event.kind}`}>
          {event.kind.replace("_", " ")}
        </span>
        <span className="trace-event-summary">{eventSummary(event)}</span>
        {label ? (
          <time
            className="trace-event-time"
            dateTime={time ?? undefined}
            title={time ?? undefined}
          >
            {label}
          </time>
        ) : null}
      </div>
      <details className="trace-event-details">
        <summary>details</summary>
        <pre>{JSON.stringify(event, null, 2)}</pre>
      </details>
    </li>
  );
}

function str(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value : null;
}

export function eventTimestamp(event: RunTimelineEventView): string | null {
  return (
    str(event.timestamp) ??
    str(event.started_at) ??
    str(event.queued_at) ??
    str(event.created_at) ??
    str(event.completed_at) ??
    null
  );
}

export function eventSummary(event: RunTimelineEventView): string {
  switch (event.kind) {
    case "request":
      return (
        [
          str(event.lifecycle_state) ?? str(event.status),
          str(event.failure_reason),
        ]
          .filter(Boolean)
          .join(" — ") || "request"
      );
    case "message": {
      const preview = str(event.content)?.slice(0, 96);
      return [str(event.role) ?? "message", preview].filter(Boolean).join(": ");
    }
    case "tool_call":
      return [
        str(event.tool_name) ?? "tool",
        str(event.lifecycle_state) ?? str(event.status),
        str(event.denial_reason),
      ]
        .filter(Boolean)
        .join(" — ");
    case "inference_call":
      return [
        `call #${event.call_seq ?? "?"}`,
        str(event.call_state),
        str(event.backend_id),
        str(event.failure_reason),
      ]
        .filter(Boolean)
        .join(" — ");
    case "rendered_request": {
      const turn =
        typeof event.turn_index === "number" ? `turn ${event.turn_index}` : null;
      const attempt =
        typeof event.attempt === "number" ? `attempt ${event.attempt}` : null;
      return [
        `captured ${str(event.capture_scope) ?? "request"}`,
        [turn, attempt].filter(Boolean).join(" ") || null,
        str(event.model_name),
        str(event.provenance_status),
      ]
        .filter(Boolean)
        .join(" — ");
    }
    case "response":
      return [str(event.status) ?? "response", str(event.error_message)]
        .filter(Boolean)
        .join(" — ");
    default:
      return event.kind;
  }
}
