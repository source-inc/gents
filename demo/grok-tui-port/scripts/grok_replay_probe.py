#!/usr/bin/env python3
"""Read-only replay fidelity check for a completed request on a test server.

Compares assistant text with persisted native message blocks and checks that
historical chunks do not acquire timestamps after request completion.
"""
import argparse
import json
from datetime import datetime

from grok_edge_probe import LeaderClient
from grok_probe_common import graphql_escape, graphql_query


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("socket", "graphql", "cwd", "session", "request"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    request = graphql_escape(args.request)
    data = graphql_query(args.graphql, '{AgentRequest(filter:{request_id:{_eq:"' + request + '"}}){session_id metadata} AgentMessage(filter:{request_id:{_eq:"' + request + '"}},order:{sequence:ASC}){role content} AgentResponse(filter:{request_id:{_eq:"' + request + '"}}){status completed_at}}')
    owners = data["AgentRequest"]
    assert len(owners) == 1 and owners[0]["session_id"] == args.session, owners
    responses = data["AgentResponse"]
    assert len(responses) == 1 and responses[0]["status"] == "complete", responses
    end_ms = int(datetime.fromisoformat(responses[0]["completed_at"].replace("Z", "+00:00")).timestamp() * 1000)
    metadata = json.loads(owners[0].get("metadata") or "{}")
    prompt_id = metadata.get("promptId") or "notifications-" + args.request
    expected = "".join(
        block["text"]
        for row in data["AgentMessage"] if row["role"] == "assistant"
        for block in json.loads(row["content"])["content"]
        if isinstance(block.get("text"), str)
    )
    assert expected, "This probe requires persisted native assistant text"
    client = LeaderClient(args.socket, 30, "GLM-5.3-Flash-NVFP4")
    try:
        client.register()
        response, _ = client.request("initialize", {"protocolVersion": 1, "clientCapabilities": {},
            "clientInfo": {"name": "replay-fidelity-probe", "version": "1"}})
        assert "error" not in response, response
        response, events = client.request("session/load", {"sessionId": args.session,
            "cwd": args.cwd, "mcpServers": []})
        assert "error" not in response, response
        actual = []
        for event in events:
            params = event.get("params", {})
            meta = params.get("_meta", {})
            update = params.get("update", {})
            if meta.get("promptId") != prompt_id:
                continue
            kind = update.get("sessionUpdate")
            if kind in ("agent_message_chunk", "agent_thought_chunk"):
                assert meta.get("isReplay") is True, event
                assert meta["agentTimestampMs"] <= end_ms, event
            if kind == "agent_message_chunk":
                actual.append(update["content"]["text"])
        actual = "".join(actual)
        assert actual == expected, {"expected": expected, "actual": actual}
        print(json.dumps({"result": "PASS", "request": args.request,
            "assistant_characters": len(actual), "historical_timestamp_ceiling_ms": end_ms}))
    finally:
        client.close()


if __name__ == "__main__":
    main()
