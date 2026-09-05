#!/usr/bin/env python3
"""Check read-only replay, then live continuation in the same persisted session."""
import argparse
import json
import uuid
from grok_edge_probe import LeaderClient, initialize
from grok_probe_common import graphql_query, graphql_escape


def assistant_text(events):
    return "".join(event.get("params", {}).get("update", {}).get("content", {}).get("text", "")
        for event in events if event.get("params", {}).get("update", {}).get("sessionUpdate") == "agent_message_chunk")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", required=True)
    parser.add_argument("--graphql", required=True)
    parser.add_argument("--cwd", required=True)
    parser.add_argument("--model", default="GLM-5.3-Flash-NVFP4")
    parser.add_argument("--session", help="Existing pre-restart session to load")
    parser.add_argument("--expected", help="Exact previous answer for an existing session")
    args = parser.parse_args()
    assert bool(args.session) == bool(args.expected), "--session and --expected must be supplied together"
    session, marker = args.session, args.expected
    if session is None:
        marker = "RESUME_" + uuid.uuid4().hex[:12]
        client = LeaderClient(args.socket, 180, args.model)
        try:
            session, _ = initialize(client, args.cwd, 524288)
            events = []
            response, _ = client.request("session/prompt", {"sessionId":session,
                "prompt":[{"type":"text", "text":f"Reply only {marker}. Do not use tools."}]}, events.append)
            assert response.get("result", {}).get("stopReason") == "end_turn", response
            assert assistant_text(events).strip() == marker, assistant_text(events)
        finally:
            client.close()

    def requests():
        query = '{ AgentRequest(filter: {session_id: {_eq: "' + graphql_escape(session) + '"}}) {_docID request_id} }'
        return graphql_query(args.graphql, query)["AgentRequest"]

    before = requests()
    client = LeaderClient(args.socket, 180, args.model)
    try:
        client.register()
        initialized, _ = client.request("initialize", {"protocolVersion":1, "clientCapabilities":{}, "clientInfo":{"name":"resume-probe", "version":"1"}})
        assert initialized["result"]["agentCapabilities"]["loadSession"] is True, initialized
        history = []
        loaded, _ = client.request("session/load", {"sessionId":session, "cwd":args.cwd, "mcpServers":[]}, history.append)
        assert "error" not in loaded, loaded
        assert sorted(requests(), key=lambda row: row["_docID"]) == sorted(before, key=lambda row: row["_docID"]), "load created/replaced requests"
        assert assistant_text(history).count(marker) == 1, assistant_text(history)
        updates = [event for event in history if event.get("params", {}).get("update")]
        assert updates and all(event["params"]["_meta"]["isReplay"] for event in updates), updates
        live = []
        response, _ = client.request("session/prompt", {"sessionId":session,
            "prompt":[{"type":"text", "text":"Repeat the exact marker from your previous answer. Reply with only that marker; do not use tools."}]}, live.append)
        assert response.get("result", {}).get("stopReason") == "end_turn", response
        assert assistant_text(live).strip() == marker, assistant_text(live)
        assert all(not event.get("params", {}).get("_meta", {}).get("isReplay", False) for event in live)
        assert len(requests()) == len(before) + 1
        print(json.dumps({"result":"PASS", "session":session, "marker":marker,
            "replay_updates":len(updates), "requests_before":len(before), "requests_after":len(before) + 1}))
    finally:
        client.close()


if __name__ == "__main__":
    main()
