#!/usr/bin/env python3
"""Compare live ACP context metadata with persisted inference accounting."""
import argparse
import json
import urllib.request
from datetime import datetime

from grok_edge_probe import LeaderClient, initialize


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", required=True)
    parser.add_argument("--graphql", required=True)
    parser.add_argument("--cwd", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--context-window", type=int, default=524288)
    args = parser.parse_args()

    def query(source):
        request = urllib.request.Request(args.graphql,
            data=json.dumps({"query": source}).encode(), headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=30) as response:
            result = json.load(response)
        assert not result.get("errors"), result
        return result["data"]

    client = LeaderClient(args.socket, 180, args.model)
    evidence = []
    try:
        session, _ = initialize(client, args.cwd, args.context_window)
        for index in range(2):
            events = []
            response, _ = client.request("session/prompt", {
                "sessionId": session, "_meta": {"promptId": f"context-probe-{index}"},
                "prompt": [{"type": "text", "text":
                    f"Reply with only CONTEXT_OK_{index}. Do not use tools."}],
            }, events.append)
            assert response.get("result", {}).get("stopReason") == "end_turn", response
            requests = query("{ AgentRequest(filter: {session_id: {_eq: " + json.dumps(session)
                + "}}, order: {created_at: DESC}, limit: 1) {_docID request_id} }")["AgentRequest"]
            assert len(requests) == 1, requests
            calls = query("{ InferenceCall(filter: {request_doc_id: {_eq: "
                + json.dumps(requests[0]["_docID"])
                + '}, call_kind: {_eq: "inference"}}, order: [{queued_at: DESC}, {call_seq: DESC}, {call_id: DESC}], limit: 1)'
                + " {call_id context_accounting_json completion_tokens} }")["InferenceCall"]
            assert len(calls) == 1, calls
            accounting = json.loads(calls[0]["context_accounting_json"])
            expected = accounting["estimated_input_tokens"] + (calls[0]["completion_tokens"] or 0)
            observed = [event["params"]["_meta"]["totalTokens"] for event in events
                if event.get("params", {}).get("sessionId") == session
                and "totalTokens" in event.get("params", {}).get("_meta", {})]
            assert observed and observed[-1] == expected, (observed, expected, calls)
            evidence.append({"call": calls[0]["call_id"], "context": expected,
                             "generated": calls[0]["completion_tokens"]})
            info, _ = client.request("x.ai/session/info", {"sessionId": session})
            assert "error" not in info, info
            details = info["result"]["result"]
            assert details["context"]["used"] == expected, details
            assert details["context"]["total"] == accounting["context_window"], details
            captures = query("{ RenderedRequest(filter: {request_doc_id: {_eq: "
                + json.dumps(requests[0]["_docID"])
                + '}, capture_scope: {_like: "inference.%"}, turn_index: {_eq: '
                + str(accounting["turn_index"]) + '}, attempt: {_eq: '
                + str(accounting["attempt"]) + '}}) {request_json} }')["RenderedRequest"]
            assert len(captures) == 1, "missing or ambiguous provider capture"
            body = json.loads(captures[0]["request_json"])
            assert details["_meta"]["gents/partialContext"] is False, details
            assert details["context"]["toolDefinitionsCount"] == len(body.get("tools") or []), details
            assert details["context"]["messageCount"] == sum(
                message["role"] not in ("system", "developer") for message in body["messages"]), details
            assert details["context"]["systemPromptTokens"] + details["context"]["messageTokens"] == accounting["components"]["messages"], details
            usage, _ = client.request("x.ai/session/usage", {"sessionId": session})
            assert "error" not in usage, usage
            totals = usage["result"]["usage"]
            owned = query("{ AgentRequest(filter: {session_id: {_eq: " + json.dumps(session)
                + "}}) {_docID} }")["AgentRequest"]
            physical_ids = ",".join(json.dumps(row["_docID"]) for row in owned)
            billed = query("{ InferenceCall(filter: {request_doc_id: {_in: [" + physical_ids
                + "]}}) {prompt_tokens completion_tokens cached_input_tokens started_at ended_at} }")["InferenceCall"]
            for native, persisted in [("inputTokens", "prompt_tokens"),
                                      ("outputTokens", "completion_tokens"),
                                      ("cachedReadTokens", "cached_input_tokens")]:
                assert totals[native] == sum(row[persisted] or 0 for row in billed), (totals, billed)
            assert totals["totalTokens"] == totals["inputTokens"] + totals["outputTokens"], totals
            assert totals["modelCalls"] == len(billed), (totals, billed)
            assert totals["numTurns"] == details["turns"], (totals, details)
            durations = [(datetime.fromisoformat(row["ended_at"].replace("Z", "+00:00"))
                - datetime.fromisoformat(row["started_at"].replace("Z", "+00:00")))
                for row in billed]
            expected_ms = sum((span.days * 86400 + span.seconds) * 1000 + span.microseconds // 1000
                for span in durations)
            assert totals["apiDurationMs"] == expected_ms, (totals, expected_ms)
            assert usage["result"]["_meta"]["gents/durationIsIncomplete"] is False, usage
            assert "costUsdTicks" not in totals and totals["costIsPartial"], totals
            evidence[-1]["usage"] = totals
        billing, _ = client.request("x.ai/billing", {})
        assert billing.get("result", {}).get("result") == {"config": None, "on_demand_enabled": False}, billing
        print(json.dumps({"result": "PASS", "session": session, "turns": evidence}))
    finally:
        client.close()


if __name__ == "__main__":
    main()
