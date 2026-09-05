#!/usr/bin/env python3
"""Live-inference task-button/output probe. Cancels only its own test process.

Requires a served Grok shim with spawn_process and bash_unrestricted enabled.
Wire assertions complement (not replace) testing the actual stock task pane.
"""
import argparse
import json
import time
from grok_edge_probe import LeaderClient, initialize


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", required=True)
    parser.add_argument("--cwd", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--context-window", type=int, default=524288)
    parser.add_argument("--child", action="store_true",
                        help="Run the process in a configured control-worker subagent")
    args = parser.parse_args()
    client = LeaderClient(args.socket, 180, args.model)
    events = []
    def record(message):
        events.append(message)
    try:
        session, _ = initialize(client, args.cwd, args.context_window)
        command = """python3 -u -c 'import time; print("BG_BEGIN_" + "x" * 100000 + "_BG_END", flush=True); time.sleep(120)'"""
        task_prompt = (
            "Call spawn_process exactly once with tool_name bash_unrestricted and args containing command "
            + json.dumps(command) + ". Then finish your turn immediately. Do not poll, wait, edit files, "
            "or run other tools. Briefly acknowledge any later completion notification without tools.")
        prompt = task_prompt
        if args.child:
            prompt = (
                "Spawn exactly one control-worker subagent with await_mode background and this task: "
                + json.dumps(task_prompt)
                + ". Then finish immediately without polling, waiting, or using other tools. "
                "Briefly acknowledge later completion notifications without tools.")
        response, _ = client.request("session/prompt", {
            "sessionId": session, "_meta": {"promptId": "background-output-probe"},
            "prompt": [{"type": "text", "text": prompt}],
        }, record)
        assert response.get("result", {}).get("stopReason") == "end_turn", response
        deadline = time.monotonic() + 150
        task = None
        while time.monotonic() < deadline:
            starts = [(i, e["params"]["update"]) for i, e in enumerate(events)
                      if e.get("params", {}).get("update", {}).get("sessionUpdate") == "task_backgrounded"]
            if starts:
                start, task = starts[0]
                task_session = events[start]["params"]["sessionId"]
                assert (task_session != session) == args.child, (session, task_session)
                snapshots = [(i, e["params"]["update"]["rawOutput"]) for i, e in enumerate(events)
                             if e.get("params", {}).get("update", {}).get("sessionUpdate") == "tool_call_update"
                             and e["params"]["update"].get("toolCallId") == task["tool_call_id"]
                             and "rawOutput" in e["params"]["update"]]
                if args.child:
                    chunks = [(i, e["params"]["update"]["event_text"]) for i, e in enumerate(events)
                              if e.get("params", {}).get("update", {}).get("sessionUpdate") == "monitor_event"
                              and e["params"]["update"].get("task_id") == task["task_id"]]
                    output = "".join(chunk for _, chunk in chunks)
                    if "BG_BEGIN_" in output and "_BG_END" in output and len(output) > 100000:
                        assert all(i > start for i, _ in chunks), "output preceded task registration"
                        assert output.count("BG_BEGIN_") == output.count("_BG_END") == 1, "duplicated output"
                        break
                if any(i > start and "BG_BEGIN_" in s.get("output_for_prompt", "")
                       and "_BG_END" in s.get("output_for_prompt", "")
                       and len(s["output_for_prompt"]) > 100000 for i, s in snapshots):
                    break
            record(client.recv_acp())
        else:
            raise AssertionError("no complete >100 KB snapshot after task registration")
        response, _ = client.request("x.ai/task/kill", {
            "sessionId": session, "taskId": task["task_id"], "source": "clientUi"}, record)
        assert response.get("result", {}).get("result", {}).get("outcome") == "killed", response
        response, _ = client.request("x.ai/task/kill", {
            "sessionId": task_session, "taskId": task["task_id"]}, record)
        assert response.get("result", {}).get("result", {}).get("outcome") == "already_exited", response
        while time.monotonic() < deadline:
            completed = [e["params"]["update"]["task_snapshot"] for e in events
                         if e.get("params", {}).get("update", {}).get("sessionUpdate") == "task_completed"
                         and e["params"]["update"]["task_snapshot"]["task_id"] == task["task_id"]]
            if completed:
                assert completed[-1]["completed"] is True
                assert completed[-1].get("signal") == "cancelled", completed[-1]
                print(json.dumps({"result": "PASS", "session": session,
                                  "task_session": task_session, "task": task["task_id"],
                                  "checks": ["large cumulative output", "start-before-output",
                                             "native kill", "idempotent kill", "cancelled terminal card"]}))
                return
            record(client.recv_acp())
        raise AssertionError("no cancelled native task completion")
    finally:
        client.close()


if __name__ == "__main__":
    main()
