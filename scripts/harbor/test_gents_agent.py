"""Tests for the Harbor adapter's post-run metadata projection.

Runs with the standard library only::

    python3 scripts/harbor/test_gents_agent.py

Harbor and certifi are stubbed before import so the adapter's metadata
contract can be exercised without a Harbor installation.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _stub_module(name: str, **attrs: object) -> None:
    module = types.ModuleType(name)
    for attr, value in attrs.items():
        setattr(module, attr, value)
    sys.modules.setdefault(name, module)


class _AgentContext:
    def __init__(self) -> None:
        self.metadata = None


_stub_module("certifi", where=lambda: "/nonexistent/ca-bundle.pem")
_stub_module("harbor")
_stub_module("harbor.agents")
_stub_module("harbor.agents.base", BaseAgent=object)
_stub_module("harbor.environments")
_stub_module("harbor.environments.base", BaseEnvironment=object)
_stub_module("harbor.models")
_stub_module("harbor.models.agent")
_stub_module("harbor.models.agent.context", AgentContext=_AgentContext)

sys.path.insert(0, str(_REPO_ROOT))

from scripts.harbor.gents_agent import GentsAgent  # noqa: E402

_TRAJECTORY = {
    "session_id": "session-1",
    "trajectory_id": "trajectory-1",
    "final_metrics": {"total_steps": 7},
}
_MAX_TURN_ERROR = (
    "agent stream failed: PromptError: MaxTurnError: (reached max turn limit: 250)"
)


class PopulateContextPostRunTest(unittest.TestCase):
    def _run(self, files: dict[str, object]) -> dict[str, object]:
        agent = GentsAgent.__new__(GentsAgent)
        agent.logger = logging.getLogger("test_gents_agent")
        with tempfile.TemporaryDirectory() as temp_dir:
            agent.logs_dir = Path(temp_dir)
            for name, payload in files.items():
                text = payload if isinstance(payload, str) else json.dumps(payload)
                (agent.logs_dir / name).write_text(text)
            context = _AgentContext()
            agent.populate_context_post_run(context)
            return ((context.metadata or {}).get("gents")) or {}

    def test_max_turn_exhaustion_is_identified(self) -> None:
        gents = self._run(
            {
                "trajectory.json": _TRAJECTORY,
                "request.json": {"request_id": "req-1"},
                "gents-outcome.json": {
                    "outcome": "max_turns_exhausted",
                    "response_status": "error",
                    "max_turns": 250,
                    "request_id": "req-1",
                },
                "response.json": {"status": "error", "error_message": _MAX_TURN_ERROR},
            }
        )
        self.assertEqual(gents.get("outcome"), "max_turns_exhausted")
        self.assertIs(gents.get("budget_exhausted"), True)
        self.assertEqual(gents.get("terminal_error"), _MAX_TURN_ERROR)
        self.assertEqual(gents.get("request_id"), "req-1")
        self.assertEqual(gents.get("total_steps"), 7)

    def test_completed_run_is_not_budget_exhausted(self) -> None:
        gents = self._run(
            {
                "trajectory.json": _TRAJECTORY,
                "request.json": {"request_id": "req-2"},
                "gents-outcome.json": {
                    "outcome": "completed",
                    "response_status": "complete",
                },
                "response.json": {"status": "complete", "error_message": None},
            }
        )
        self.assertEqual(gents.get("outcome"), "completed")
        self.assertIs(gents.get("budget_exhausted"), False)
        self.assertIsNone(gents.get("terminal_error"))

    def test_missing_outcome_artifacts_degrade_to_null(self) -> None:
        gents = self._run(
            {
                "trajectory.json": _TRAJECTORY,
                "request.json": {"request_id": "req-3"},
            }
        )
        self.assertIsNone(gents.get("outcome"))
        self.assertIs(gents.get("budget_exhausted"), False)
        self.assertIsNone(gents.get("terminal_error"))

    def test_corrupt_outcome_file_degrades_to_null(self) -> None:
        gents = self._run(
            {
                "trajectory.json": _TRAJECTORY,
                "request.json": {"request_id": "req-4"},
                "gents-outcome.json": '{"outcome": "max_turns_exhausted", oops',
                "response.json": {"status": "error", "error_message": _MAX_TURN_ERROR},
            }
        )
        self.assertIsNone(gents.get("outcome"))
        self.assertIs(gents.get("budget_exhausted"), False)
        self.assertEqual(gents.get("terminal_error"), _MAX_TURN_ERROR)

    def test_server_loss_metadata_survives_without_atif(self) -> None:
        gents = self._run(
            {
                "request.json": {"request_id": "req-lost"},
                "gents-outcome.json": {"outcome": "runtime_server_lost"},
                "gents-diagnostic.json": {
                    "reason": "server_lost_during_request",
                    "graphql_available": False,
                },
                "gents-server-exit.json": {
                    "status": "signal",
                    "signal": 9,
                    "wait_status": 137,
                },
            }
        )
        self.assertEqual(gents.get("request_id"), "req-lost")
        self.assertEqual(gents.get("failure_origin"), "gents_server")
        self.assertIs(gents.get("diagnostic_graphql_available"), False)
        self.assertEqual((gents.get("server_exit") or {}).get("signal"), 9)

    def test_compaction_provider_failure_has_distinct_origin(self) -> None:
        gents = self._run(
            {
                "request.json": {"request_id": "req-compaction"},
                "gents-outcome.json": {"outcome": "compaction_provider_error"},
                "response.json": {
                    "status": "error",
                    "error_message": "compaction_provider_failure: guided and fallback output failed",
                },
            }
        )
        self.assertEqual(gents.get("outcome"), "compaction_provider_error")
        self.assertEqual(gents.get("failure_origin"), "compaction_provider")


class RunnerSupervisionTest(unittest.TestCase):
    def test_transient_graphql_waiter_failure_reconnects(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            logs = root / "logs"
            home = root / "home"
            instruction = root / "instruction.md"
            fake_gents = root / "gents"
            instruction.write_text("finish the task")
            fake_gents.write_text(_FAKE_GENTS)
            fake_gents.chmod(0o755)

            env = {
                **os.environ,
                "FAKE_WAITER_TRANSIENT_ONCE": "1",
                "GENTS_BINARY": str(fake_gents),
                "GENTS_HOME": str(home),
                "GENTS_INSTRUCTION_FILE": str(instruction),
                "GENTS_INFERENCE_URL": "http://127.0.0.1:8000/v1",
                "GENTS_MODEL": "fake-model",
                "GENTS_SEED": "1234",
                "GENTS_TOOL_ROOT": str(root),
                "GENTS_LOGS_DIR": str(logs),
                "GENTS_SERVER_STARTUP_TIMEOUT_SECS": "5",
                "GENTS_RESPONSE_WAITER_MAX_RESTARTS": "2",
                "GENTS_SUPERVISION_POLL_SECS": "0.05",
            }
            result = subprocess.run(
                ["sh", str(_REPO_ROOT / "scripts/harbor/run_gents.sh")],
                env=env,
                text=True,
                capture_output=True,
                timeout=15,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("restarting (1/2)", result.stderr)
            response = json.loads((logs / "response.json").read_text())
            self.assertEqual(response["status"], "complete")
            self.assertEqual(
                (home / "invocations.jsonl").read_text().count('["response", "wait"'),
                2,
            )
            self.assertTrue((logs / "trajectory.json").is_file())
            persisted = json.loads((logs / "request-persisted.json").read_text())
            self.assertEqual(persisted["request"]["seed"], 1234)

    def test_server_signal_cancels_waiter_and_preserves_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            logs = root / "logs"
            home = root / "home"
            instruction = root / "instruction.md"
            fake_gents = root / "gents"
            instruction.write_text("finish the task")
            fake_gents.write_text(_FAKE_GENTS)
            fake_gents.chmod(0o755)

            env = {
                **os.environ,
                "GENTS_BINARY": str(fake_gents),
                "GENTS_HOME": str(home),
                "GENTS_INSTRUCTION_FILE": str(instruction),
                "GENTS_INFERENCE_URL": "http://127.0.0.1:8000/v1",
                "GENTS_MODEL": "fake-model",
                "GENTS_SEED": "1234",
                "GENTS_TOOL_ROOT": str(root),
                "GENTS_LOGS_DIR": str(logs),
                "GENTS_SERVER_STARTUP_TIMEOUT_SECS": "5",
                "GENTS_DIAGNOSTIC_TIMEOUT_SECS": "2",
                "GENTS_SUPERVISION_POLL_SECS": "0.05",
            }
            result = subprocess.run(
                ["sh", str(_REPO_ROOT / "scripts/harbor/run_gents.sh")],
                env=env,
                text=True,
                capture_output=True,
                timeout=15,
                check=False,
            )

            self.assertEqual(result.returncode, 70, result.stderr)
            self.assertIn(
                "Gents server exited during active request (signal 15)", result.stderr
            )
            self.assertTrue((home / "waiter-cancelled").is_file())
            self.assertEqual(
                json.loads((logs / "gents-server-exit.json").read_text())["signal"],
                15,
            )
            diagnostic = json.loads((logs / "gents-diagnostic.json").read_text())
            self.assertEqual(diagnostic["reason"], "server_lost_during_request")
            self.assertIs(diagnostic["graphql_available"], False)
            self.assertTrue((logs / "graphql-unavailable.txt").is_file())
            self.assertGreater((logs / "gents-server-tail.txt").stat().st_size, 0)
            self.assertGreater((logs / "process-tree.txt").stat().st_size, 0)
            self.assertGreater((logs / "gents-home-inventory.txt").stat().st_size, 0)
            self.assertGreater((logs / "gents-home.tar.gz").stat().st_size, 0)
            self.assertGreater((logs / "partial-timeline.json").stat().st_size, 0)
            trajectory = json.loads((logs / "trajectory.json").read_text())
            self.assertEqual(trajectory["trajectory_id"], "partial-trajectory")

            invocations = [
                json.loads(line)
                for line in (home / "invocations.jsonl").read_text().splitlines()
            ]
            init = next(args for args in invocations if args[:1] == ["init"])
            self.assertIn("--tool-package", init)
            self.assertEqual(init[init.index("--tool-package") + 1], "write")
            self.assertNotIn("--yolo", init)

            profile = next(
                args
                for args in invocations
                if args[:3] == ["config", "profile", "set"]
            )
            for option_name, expected in {
                "--max-output-tokens": "393216",
                "--max-turns": "1000",
                "--temperature": "1.0",
                "--top-p": "0.95",
                "--reasoning-effort": "max",
            }.items():
                self.assertEqual(profile[profile.index(option_name) + 1], expected)

            tools = next(
                args
                for args in invocations
                if args[:3] == ["config", "tools", "set"]
            )
            for option_name, expected in {
                "--enable-file-tools": "true",
                "--file-tools-mode": "ReadWrite",
                "--enable-bash": "true",
                "--bash-mode": "Unrestricted",
                "--command-execution-policy": "unrestricted",
                "--enable-meta-tools": "false",
                "--backgroundable-tool-name": "bash_unrestricted",
                "--enable-memory": "false",
                "--enable-session-history-tool": "false",
                "--enable-context-budget": "false",
                "--enable-defra-query": "false",
                "--subagent-spawn-enabled": "false",
                "--orchestration-enabled": "false",
                "--subagent-steering-enabled": "false",
                "--subagent-background-enabled": "false",
                "--subagent-allow-cross-deployment": "false",
            }.items():
                self.assertEqual(tools[tools.index(option_name) + 1], expected)
            self.assertTrue(
                any(args[:2] == ["tools", "explain"] for args in invocations)
            )
            submitted = next(
                args for args in invocations if args[:2] == ["request", "submit"]
            )
            self.assertEqual(submitted[submitted.index("--seed") + 1], "1234")


_FAKE_GENTS = r'''#!/usr/bin/env python3
import json
import os
import signal
import sys
import time
from pathlib import Path

args = sys.argv[1:]

def option(name, default=None):
    if name not in args:
        return default
    return args[args.index(name) + 1]

home = Path(option("--home", os.environ.get("GENTS_HOME", "/tmp/fake-gents")))
home.mkdir(parents=True, exist_ok=True)
(home / "store-evidence.db").touch()
with (home / "invocations.jsonl").open("a") as invocations:
    invocations.write(json.dumps(args) + "\n")

if args[:1] == ["init"]:
    print(json.dumps({
        "agent_did": "did:key:fake",
        "default_behavior_id": "did:key:fake:default",
        "inference_profile_id": "profile-1",
        "tool_selection_id": "did:key:fake:tools",
    }, indent=2))
elif args[:1] == ["server"]:
    count_file = home / "server-count"
    count = int(count_file.read_text()) + 1 if count_file.exists() else 1
    count_file.write_text(str(count))
    print(f"fake server start {count}", flush=True)
    if count == 1:
        print("gents server is running with fake transport", flush=True)
        while True:
            time.sleep(1)
    # Model the production restart race: status can report the prior server's
    # persisted ready state before the replacement has acquired its store.
    time.sleep(0.2)
    (home / "second-server-ready").touch()
    print("gents server is running with fake transport", flush=True)
    if os.environ.get("FAKE_WAITER_TRANSIENT_ONCE"):
        while not (home / "waiter-finished").exists():
            time.sleep(0.01)
        while True:
            time.sleep(1)
    while not (home / "waiter-started").exists():
        time.sleep(0.01)
    time.sleep(0.05)
    (home / "server-lost").write_text("signal 15")
    os.kill(os.getpid(), signal.SIGTERM)
elif args[:3] == ["config", "profile", "set"]:
    print("{}")
elif args[:3] == ["config", "tools", "set"]:
    print("{}")
elif args[:2] == ["tools", "explain"]:
    if not (home / "second-server-ready").exists():
        print("database is locked by the restarting server", file=sys.stderr)
        sys.exit(73)
    print(json.dumps({"behaviors": []}))
elif args[:1] == ["status"]:
    if (home / "server-lost").exists():
        print("GraphQL connection refused", file=sys.stderr)
        sys.exit(1)
    print(json.dumps({"process_state": "ready", "behavior_readiness": "ready"}, indent=2))
elif args[:2] == ["request", "submit"]:
    request = {
        "request_id": "request-1",
        "seed": int(option("--seed")) if option("--seed") is not None else None,
    }
    Path(option("--output-file")).write_text(json.dumps(request, indent=2))
    print(json.dumps(request, indent=2))
elif args[:2] == ["request", "show"]:
    submitted = next(
        json.loads(line)
        for line in (home / "invocations.jsonl").read_text().splitlines()
        if json.loads(line)[:2] == ["request", "submit"]
    )
    print(json.dumps({
        "request": {
            "request_id": option("--request-id"),
            "seed": int(submitted[submitted.index("--seed") + 1])
            if "--seed" in submitted else None,
        }
    }, indent=2))
elif args[:2] == ["response", "wait"]:
    if os.environ.get("FAKE_WAITER_TRANSIENT_ONCE"):
        count_file = home / "waiter-count"
        count = int(count_file.read_text()) + 1 if count_file.exists() else 1
        count_file.write_text(str(count))
        if count == 1:
            print("Error: posting GraphQL to http://127.0.0.1:9191/api/v0/graphql", file=sys.stderr)
            sys.exit(1)
        print(json.dumps({
            "request_id": "request-1",
            "status": "complete",
            "content": "done",
            "error_message": None,
        }, indent=2))
        (home / "waiter-finished").touch()
        sys.exit(0)
    (home / "waiter-started").touch()
    def cancelled(_signum, _frame):
        (home / "waiter-cancelled").touch()
        sys.exit(143)
    signal.signal(signal.SIGTERM, cancelled)
    while True:
        time.sleep(1)
elif args[:2] == ["response", "show"]:
    print("GraphQL connection refused", file=sys.stderr)
    sys.exit(1)
elif args[:2] == ["trace", "timeline"]:
    print(json.dumps({"request_id": "request-1", "events": [{"kind": "partial"}]}))
elif args[:2] == ["trace", "project"]:
    trajectory = {
        "trajectory_id": "partial-trajectory",
        "session_id": "partial-session",
        "steps": [{"step_id": "partial-step"}],
    }
    Path(option("--output-file")).write_text(json.dumps(trajectory))
else:
    print(f"unsupported fake gents invocation: {args}", file=sys.stderr)
    sys.exit(2)
'''


if __name__ == "__main__":
    unittest.main()
