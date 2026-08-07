import { render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { RequestTracePanel } from "@source-inc/gents-desktop-operations";
import { eventSummary, eventTimestamp } from "@source-inc/gents-desktop-operations";
import { setDesktopApiAdapterForTests } from "@source-inc/gents-desktop-client";
import type { DesktopApiAdapter } from "@source-inc/gents-desktop-client";

function adapterWith(timeline: unknown, fail = false) {
  return {
    fetchRequestTimeline: fail
      ? vi.fn().mockRejectedValue(new Error("peer unreachable"))
      : vi.fn().mockResolvedValue(timeline),
  } as unknown as DesktopApiAdapter;
}

describe("request trace panel", () => {
  afterEach(() => setDesktopApiAdapterForTests(null));

  it("renders the reconstructed event stream", async () => {
    setDesktopApiAdapterForTests(
      adapterWith({
        request_id: "req-1",
        events: [
          {
            kind: "message",
            role: "user",
            content: "hi",
            timestamp: "2026-06-03T14:05:00Z",
          },
          {
            kind: "rendered_request",
            capture_key: "rendered:v1:abc",
            capture_scope: "inference.1",
            turn_index: 0,
            attempt: 1,
            model_name: "gpt-5",
            provenance_status: "captured_only",
            created_at: "2026-06-03T14:05:01Z",
          },
          { kind: "tool_call", tool_name: "gents_exec", lifecycle_state: "Completed" },
          { kind: "response", status: "materialized" },
        ],
      }),
    );
    render(<RequestTracePanel agentDid="did:a" rootRequestId="req-1" />);

    await waitFor(() => expect(screen.getByText("user: hi")).toBeInTheDocument());
    expect(
      screen.getByText("captured inference.1 — turn 0 attempt 1 — gpt-5 — captured_only"),
    ).toBeInTheDocument();
    expect(screen.getByText("gents_exec — Completed")).toBeInTheDocument();
    expect(screen.getByText("materialized")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Copy JSON" })).toBeInTheDocument();
  });

  it("surfaces fetch failures with a retry affordance", async () => {
    setDesktopApiAdapterForTests(adapterWith(null, true));
    render(<RequestTracePanel agentDid="did:a" rootRequestId="req-1" />);

    await waitFor(() =>
      expect(screen.getByTestId("trace-error")).toHaveTextContent("peer unreachable"),
    );
    expect(screen.getByTestId("trace-refresh")).toBeEnabled();
  });

  it("asks for a request when none is selected", () => {
    render(<RequestTracePanel agentDid="did:a" rootRequestId={null} />);
    expect(screen.getByText(/No request selected/)).toBeInTheDocument();
  });

  it("summarizes and timestamps each event kind honestly", () => {
    expect(
      eventSummary({
        kind: "inference_call",
        call_seq: 2,
        call_state: "completed",
        backend_id: "b1",
      }),
    ).toBe("call #2 — completed — b1");
    expect(
      eventSummary({
        kind: "request",
        lifecycle_state: "Failed",
        failure_reason: "boom",
      }),
    ).toBe("Failed — boom");
    expect(
      eventTimestamp({ kind: "tool_call", started_at: "2026-01-01T00:00:00Z" }),
    ).toBe("2026-01-01T00:00:00Z");
    expect(eventTimestamp({ kind: "response" })).toBeNull();
    expect(
      eventSummary({
        kind: "rendered_request",
        capture_scope: "compaction.2",
        turn_index: 1,
        attempt: 0,
        provenance_status: "unsupported_manifest",
      }),
    ).toBe("captured compaction.2 — turn 1 attempt 0 — unsupported_manifest");
    expect(
      eventTimestamp({
        kind: "rendered_request",
        created_at: "2026-08-07T12:00:02Z",
      }),
    ).toBe("2026-08-07T12:00:02Z");
  });
});
