#!/usr/bin/env python3
"""Exercise /goal creation through a real leader socket and live inference.

Requires a behavior with get_goal/update_goal enabled. Creates a fresh session;
does not touch existing user goals or ask the model to run host tools.
"""
import argparse
import json
import uuid

from grok_edge_probe import LeaderClient, initialize
from grok_probe_common import graphql_escape, graphql_query


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", required=True)
    parser.add_argument("--graphql", required=True)
    parser.add_argument("--cwd", required=True)
    args = parser.parse_args()
    client = LeaderClient(args.socket, 180, "GLM-5.3-Flash-NVFP4")
    try:
        session, _ = initialize(client, args.cwd, 524288)
        prompt_id = str(uuid.uuid4())
        objective = (
            "Reply exactly GOAL_CREATE_OK and mark this goal complete with update_goal. "
            "Use only get_goal and update_goal if needed. Do not read or change files, "
            "run shell commands, or spawn tasks."
        )
        response, events = client.request("session/prompt", {
            "sessionId": session,
            "prompt": [{"type": "text", "text": "/goal " + objective + " --budget 100000"}],
            "_meta": {"promptId": prompt_id},
        })
        assert response.get("result", {}).get("stopReason") == "end_turn", response
        escaped = graphql_escape(session)
        data = graphql_query(args.graphql, '{Goal(filter:{session_id:{_eq:"' + escaped +
            '"}}){objective status token_budget} AgentRequest(filter:{session_id:{_eq:"' + escaped +
            '"}}){request_id content lifecycle_state metadata admission_signer_did}}')
        assert len(data["Goal"]) == 1, data
        goal = data["Goal"][0]
        assert goal == {"objective": objective, "status": "complete", "token_budget": 100000}, goal
        initial = [row for row in data["AgentRequest"] if row["content"] == objective]
        assert len(initial) == 1, data
        request = initial[0]
        assert request["lifecycle_state"] == "completed", request
        assert json.loads(request["metadata"])["promptId"] == prompt_id, request
        assert request["admission_signer_did"], request
        text = "".join(event.get("params", {}).get("update", {}).get("content", {}).get("text", "")
            for event in events if event.get("params", {}).get("update", {}).get("sessionUpdate") == "agent_message_chunk")
        assert "GOAL_CREATE_OK" in text, text
        assert any(event.get("params", {}).get("update", {}).get("sessionUpdate") == "goal_updated"
            for event in events), "missing native goal panel update"
        print(json.dumps({"result": "PASS", "session": session, "request": request["request_id"],
            "goal_status": goal["status"], "token_budget": goal["token_budget"],
            "checks": ["slash creation", "signed initial request", "native goal update", "live inference", "runtime completion"]}))
    finally:
        client.close()


if __name__ == "__main__":
    main()
