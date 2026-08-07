"""Tests for the Harbor adapter's post-run metadata projection.

Runs with the standard library only::

    python3 scripts/harbor/test_gents_agent.py

Harbor and certifi are stubbed before import so the adapter's metadata
contract can be exercised without a Harbor installation.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _stub_module(name: str, **attrs: object) -> None:
    module = types.ModuleType(name)
    for attr, value in attrs.items():
        setattr(module, attr, value)
    sys.modules.setdefault(name, module)


class _AgentContext:
    def __init__(self) -> None:
        self.metadata = None
        self.n_input_tokens = None
        self.n_cache_tokens = None
        self.n_output_tokens = None


class _CommandResult:
    def __init__(
        self, return_code: int = 0, stdout: str = "", stderr: str = ""
    ) -> None:
        self.return_code = return_code
        self.stdout = stdout
        self.stderr = stderr


class _RunEnvironment:
    def __init__(self, persisted_snapshot: dict[str, object]) -> None:
        self.persisted_snapshot = persisted_snapshot
        self.commands: list[str] = []

    async def upload_file(self, _source: Path, _destination: str) -> None:
        return None

    async def exec(self, command: str, **_kwargs: object) -> _CommandResult:
        self.commands.append(command)
        if command == f"cat {GentsAgent._REMOTE_PERSISTED_REQUEST}":
            return _CommandResult(stdout=json.dumps(self.persisted_snapshot))
        return _CommandResult()


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
from scripts.harbor import jack_bench_attestation  # noqa: E402

_TRAJECTORY = {
    "session_id": "session-1",
    "trajectory_id": "trajectory-1",
    "final_metrics": {
        "total_steps": 7,
        "total_prompt_tokens": 300,
        "total_cached_tokens": 60,
        "total_completion_tokens": 100,
    },
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
            self.last_context = context
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
        self.assertEqual(self.last_context.n_input_tokens, 300)
        self.assertEqual(self.last_context.n_cache_tokens, 60)
        self.assertEqual(self.last_context.n_output_tokens, 100)

    def test_token_exhaustion_is_identified(self) -> None:
        gents = self._run(
            {
                "trajectory.json": _TRAJECTORY,
                "request.json": {"request_id": "req-token"},
                "gents-outcome.json": {"outcome": "token_budget_exhausted"},
                "response.json": {
                    "status": "error",
                    "error_message": "aggregate_token_budget_exhausted: limit=100000",
                },
            }
        )
        self.assertEqual(gents.get("outcome"), "token_budget_exhausted")
        self.assertIs(gents.get("budget_exhausted"), True)

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


class PersistedRequestContractTest(unittest.TestCase):
    def test_null_value_is_distinct_from_an_omitted_contract_field(self) -> None:
        self.assertIsNone(
            GentsAgent._persisted_request_value(
                {"max_total_tokens": None}, "max_total_tokens"
            )
        )
        with self.assertRaisesRegex(
            RuntimeError, "omits required persisted field: max_total_tokens"
        ):
            GentsAgent._persisted_request_value({}, "max_total_tokens")

    def test_run_stays_empty_until_harbor_syncs_logs_for_post_run(self) -> None:
        agent = GentsAgent.__new__(GentsAgent)
        agent.logger = logging.getLogger("test_gents_agent")
        agent.model_name = "d4f"
        agent.session_id = "trial-1"
        agent.extra_env = {
            "GENTS_INFERENCE_URL": "http://127.0.0.1:8000/v1",
            "GENTS_MAX_TOTAL": "1000000",
            "GENTS_SEED": "1",
        }
        persisted_snapshot = {
            "request": {"seed": 1, "max_total_tokens": 1000000}
        }
        environment = _RunEnvironment(persisted_snapshot)
        context = _AgentContext()
        artifacts = {
            "trajectory.json": _TRAJECTORY,
            "request.json": {
                "request_id": "req-run",
                "temperature": 1.0,
                "top_p": 0.95,
                "max_tokens": 393216,
            },
            "request-persisted.json": persisted_snapshot,
            "response.json": {
                "status": "error",
                "error_message": "aggregate_token_budget_exhausted",
            },
            "gents-outcome.json": {"outcome": "token_budget_exhausted"},
            "gents-profile.json": {
                "reasoning_effort": "max",
                "context_window": 458752,
                "max_turns": 1000,
                "deadline_duration_secs": 28800,
                "retry_max_transport": 3,
            },
            "gents-init.json": {
                "init": {
                    "model_name": "d4f",
                    "endpoint": "http://127.0.0.1:8000/v1",
                }
            },
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            agent.logs_dir = Path(temp_dir)
            asyncio.run(agent.run("finish the task", environment, context))
            self.assertIsNone(context.metadata)
            self.assertIsNone(context.n_input_tokens)
            for name, payload in artifacts.items():
                (agent.logs_dir / name).write_text(json.dumps(payload))
            agent.populate_context_post_run(context)

        self.assertEqual(context.n_input_tokens, 300)
        self.assertEqual(context.n_cache_tokens, 60)
        self.assertEqual(context.n_output_tokens, 100)
        gents = ((context.metadata or {}).get("gents") or {})
        self.assertEqual(gents.get("outcome"), "token_budget_exhausted")
        self.assertEqual(gents.get("model"), "d4f")
        self.assertEqual(gents.get("seed"), 1)
        self.assertEqual(gents.get("max_total_tokens"), 1000000)
        self.assertIn(
            f"cat {GentsAgent._REMOTE_PERSISTED_REQUEST}", environment.commands
        )

    def test_incomplete_usage_does_not_partially_fill_context(self) -> None:
        incomplete_trajectory = json.loads(json.dumps(_TRAJECTORY))
        del incomplete_trajectory["final_metrics"]["total_completion_tokens"]
        context = _AgentContext()

        self.assertIs(
            GentsAgent._populate_token_usage(context, incomplete_trajectory), False
        )
        self.assertIsNone(context.metadata)
        self.assertIsNone(context.n_input_tokens)
        self.assertIsNone(context.n_cache_tokens)
        self.assertIsNone(context.n_output_tokens)


class JackBenchRuntimeAttestationTest(unittest.TestCase):
    def _fixture(self, root: Path) -> tuple[GentsAgent, Path, Path]:
        package = root / "package"
        adapter = package / "adapter"
        source = adapter / "scripts" / "harbor" / "gents_agent.py"
        source.parent.mkdir(parents=True)
        source.write_text("# pinned adapter\n")
        binary = adapter / "gents"
        binary.write_text("pinned gents binary\n")
        binary.chmod(0o755)
        (package / "environment").mkdir()
        (package / "environment" / "Dockerfile").write_text("FROM scratch\n")
        (package / "tests").mkdir()
        (package / "tests" / "test.sh").write_text("#!/bin/sh\n")
        (package / "tests" / "test.sh").chmod(0o755)
        (package / "task.toml").write_text('version = "1.0"\n')
        (package / "instruction.md").write_text("finish the task\n")

        controller = root / "harbor"
        controller.write_text("#!/bin/sh\n")
        controller.chmod(0o755)
        adapter_files = {
            "adapter/scripts/harbor/gents_agent.py": jack_bench_attestation.sha256_file(
                source
            )
        }
        payload = {
            "schema_version": "jack-bench-harbor-export/v1",
            "package_payload_sha256": jack_bench_attestation.package_sha256(package),
            "harbor": {
                "version": "0.20.0",
                "commit": "459ff6ec99417589b7f679d14ddf3b3f0ae4f1dc",
                "agent_adapter": "scripts.harbor.gents_agent:GentsAgent",
            },
            "gents": {
                "commit": "a" * 40,
                "sha256": jack_bench_attestation.sha256_file(binary),
                "package_path": "adapter/gents",
            },
            "gents_adapter_files_sha256": adapter_files,
            "environment_image": "example/environment@sha256:" + "b" * 64,
            "verifier_image": "example/verifier@sha256:" + "c" * 64,
            "platform": "linux/amd64",
        }
        export = {
            "artifact_schema": jack_bench_attestation.EXPORT_SCHEMA,
            "payload_sha256": jack_bench_attestation.serde_json_sha256(payload),
            "payload": payload,
        }
        (package / "jack-bench-export.json").write_text(json.dumps(export))

        trial = root / "trial"
        logs = trial / "agent"
        logs.mkdir(parents=True)
        agent = GentsAgent.__new__(GentsAgent)
        agent.logs_dir = logs
        agent.extra_env = {
            "GENTS_BINARY_PATH": str(binary),
            "GENTS_JACK_BENCH_ATTESTATION": "1",
            "GENTS_HARBOR_CONTROLLER_BINARY_SHA256": (
                jack_bench_attestation.sha256_file(controller)
            ),
        }
        return agent, package, controller

    def test_emits_observed_runtime_receipt_at_trial_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            agent, package, controller = self._fixture(Path(temp_dir))
            with mock.patch.object(sys, "argv", [str(controller)]), mock.patch(
                "scripts.harbor.gents_agent.importlib.metadata.version",
                return_value="0.20.0",
            ):
                agent._write_jack_bench_runtime_attestation(
                    f"gents 0.10.1 ({'a' * 40})"
                )

            receipt_path = (
                agent.logs_dir.parent
                / GentsAgent._JACK_BENCH_RUNTIME_ATTESTATION_FILE
            )
            receipt = json.loads(receipt_path.read_text())
            export = json.loads((package / "jack-bench-export.json").read_text())[
                "payload"
            ]
            self.assertEqual(
                receipt["schema_version"],
                GentsAgent._JACK_BENCH_RUNTIME_ATTESTATION_SCHEMA,
            )
            self.assertEqual(
                receipt["package_payload_sha256"],
                export["package_payload_sha256"],
            )
            self.assertEqual(
                receipt["task_content_sha256"],
                jack_bench_attestation.task_content_sha256(package),
            )
            self.assertEqual(
                receipt["controller_binary_sha256"],
                jack_bench_attestation.sha256_file(controller),
            )

    def test_rejects_package_drift_before_emitting_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            agent, package, controller = self._fixture(Path(temp_dir))
            (package / "instruction.md").write_text("tampered\n")
            with mock.patch.object(sys, "argv", [str(controller)]), mock.patch(
                "scripts.harbor.gents_agent.importlib.metadata.version",
                return_value="0.20.0",
            ), self.assertRaisesRegex(RuntimeError, "package payload hash mismatch"):
                agent._write_jack_bench_runtime_attestation(
                    f"gents 0.10.1 ({'a' * 40})"
                )

    def test_rejects_a_different_executing_controller(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            agent, _package, controller = self._fixture(Path(temp_dir))
            agent.extra_env["GENTS_HARBOR_CONTROLLER_BINARY_SHA256"] = "d" * 64
            with mock.patch.object(sys, "argv", [str(controller)]), mock.patch(
                "scripts.harbor.gents_agent.importlib.metadata.version",
                return_value="0.20.0",
            ), self.assertRaisesRegex(RuntimeError, "controller binary hash mismatch"):
                agent._write_jack_bench_runtime_attestation(
                    f"gents 0.10.1 ({'a' * 40})"
                )


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
                "GENTS_MAX_TOTAL": "100000",
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
                "GENTS_MAX_TOTAL": "100000",
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
            self.assertEqual(
                submitted[submitted.index("--max-total-tokens") + 1], "100000"
            )


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
        "max_total_tokens": int(option("--max-total-tokens")),
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
            "max_total_tokens": int(
                submitted[submitted.index("--max-total-tokens") + 1]
            ),
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
        "final_metrics": {
            "total_prompt_tokens": 300,
            "total_completion_tokens": 100,
            "total_cached_tokens": 60,
            "total_steps": 1,
            "extra": {
                "inference_call_count": 1,
                "inference_call_pending_count": 0,
                "inference_call_usage_count": 1,
            },
        },
    }
    Path(option("--output-file")).write_text(json.dumps(trajectory, indent=2))
else:
    print(f"unsupported fake gents invocation: {args}", file=sys.stderr)
    sys.exit(2)
'''


if __name__ == "__main__":
    unittest.main()
