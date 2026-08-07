"""Harbor custom agent that runs the native Gents runtime inside a task container.

Use this agent by import path from the repository root::

    harbor run ... --agent scripts.harbor.gents_agent:GentsAgent

The adapter deliberately installs Gents *inside* the task environment. Native
filesystem and shell tools therefore operate on the same ``/app`` tree that the
Harbor verifier inspects.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import tempfile
import uuid
from pathlib import Path
from typing import Any

import certifi

from harbor.agents.base import BaseAgent
from harbor.environments.base import BaseEnvironment
from harbor.models.agent.context import AgentContext


class GentsAgent(BaseAgent):
    """Run one Harbor instruction through a durable Gents request."""

    SUPPORTS_ATIF = True
    _REMOTE_BINARY = "/usr/local/bin/gents"
    # Keep the conventional basename for diagnostics and direct execution. The
    # explicit-loader case uses the filesystem-runner shim installed in setup.
    _REMOTE_REAL_BINARY = "/usr/local/libexec/gents"
    _REMOTE_BINARY_UPLOAD = "/tmp/gents-harbor-upload"
    _REMOTE_FS_RUNNER = "/usr/local/bin/gents-fs-runner"
    _REMOTE_RUNNER = "/usr/local/bin/run-gents-harbor"
    _REMOTE_RUNNER_UPLOAD = "/tmp/run-gents-harbor-upload"
    _REMOTE_CA_BUNDLE = "/tmp/gents-harbor-ca-bundle.pem"
    _REMOTE_GLIBC_BUNDLE = "/tmp/gents-harbor-glibc.tar.gz"
    _REMOTE_GLIBC_DIR = "/usr/local/lib/gents-harbor-glibc"
    _RUNNER_SOURCE = Path(__file__).with_name("run_gents.sh")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        docker_platform = self._env("GENTS_DOCKER_PLATFORM")
        if docker_platform:
            # Compose honors this for prebuilt images. Harbor 0.20.0 does not
            # honor it for buildx, whose resolver runs after agent creation.
            os.environ["DOCKER_DEFAULT_PLATFORM"] = docker_platform
            from harbor.environments.docker import docker as harbor_docker
            from harbor.environments.docker import utils as harbor_docker_utils

            async def configured_docker_platform() -> str:
                return docker_platform

            harbor_docker.default_docker_platform = configured_docker_platform
            harbor_docker_utils.default_docker_platform = configured_docker_platform

    @staticmethod
    def name() -> str:
        return "gents"

    def version(self) -> str | None:
        return self._env("GENTS_VERSION") or "source"

    def _env(self, name: str, default: str | None = None) -> str | None:
        value = self.extra_env.get(name)
        if value is None:
            value = os.environ.get(name)
        if value is None:
            return default
        value = value.strip()
        return value or default

    @staticmethod
    def _require_success(command: str, result: Any) -> None:
        if result.return_code == 0:
            return
        stdout = (result.stdout or "")[-4_000:]
        stderr = (result.stderr or "")[-4_000:]
        raise RuntimeError(
            f"Gents Harbor command failed with exit {result.return_code}: {command}\n"
            f"stdout:\n{stdout}\nstderr:\n{stderr}"
        )

    @staticmethod
    def _persisted_request_value(request: dict[str, Any], field: str) -> Any:
        if field not in request:
            raise RuntimeError(
                "Gents request-show contract omits required persisted field: "
                f"{field}"
            )
        return request[field]

    async def _install_ca_bundle(self, environment: BaseEnvironment) -> None:
        """Provide TLS roots without invoking a package manager in every task."""
        ca_bundle = Path(certifi.where())
        if not ca_bundle.is_file():
            raise FileNotFoundError(f"Harbor CA bundle is missing: {ca_bundle}")
        await environment.upload_file(ca_bundle, self._REMOTE_CA_BUNDLE)
        command = f"test -s {shlex.quote(self._REMOTE_CA_BUNDLE)}"
        result = await environment.exec(command=command, user="root")
        self._require_success("install Harbor CA bundle", result)

    async def _install_uploaded_binary(
        self, environment: BaseEnvironment, binary_path: Path
    ) -> None:
        if not binary_path.is_file():
            raise ValueError(f"GENTS_BINARY_PATH is not a file: {binary_path}")
        upload_path = self._REMOTE_BINARY_UPLOAD
        await environment.upload_file(binary_path, upload_path)
        bundle_path = self._env("GENTS_GLIBC_BUNDLE_PATH")
        if bundle_path:
            local_bundle = Path(bundle_path)
            if not local_bundle.is_file():
                raise ValueError(
                    f"GENTS_GLIBC_BUNDLE_PATH is not a file: {local_bundle}"
                )
            await environment.upload_file(local_bundle, self._REMOTE_GLIBC_BUNDLE)
            command = f"""
set -eu
install -d -m 0755 {shlex.quote(self._REMOTE_GLIBC_DIR)} /usr/local/libexec
tar -xzf {shlex.quote(self._REMOTE_GLIBC_BUNDLE)} -C {shlex.quote(self._REMOTE_GLIBC_DIR)}
install -m 0755 {shlex.quote(upload_path)} {shlex.quote(self._REMOTE_REAL_BINARY)}
loader=$(find {shlex.quote(self._REMOTE_GLIBC_DIR)} -maxdepth 1 -name 'ld-linux-*.so.*' -print -quit)
test -n "$loader"
printf '%s\\n' '#!/bin/sh' 'loader=$(find {self._REMOTE_GLIBC_DIR} -maxdepth 1 -name "ld-linux-*.so.*" -print -quit)' 'exec "$loader" --library-path {self._REMOTE_GLIBC_DIR} {self._REMOTE_REAL_BINARY} "$@"' > {shlex.quote(self._REMOTE_BINARY)}
chmod 0755 {shlex.quote(self._REMOTE_BINARY)}
rm -f {shlex.quote(upload_path)} {shlex.quote(self._REMOTE_GLIBC_BUNDLE)}
""".strip()
            result = await environment.exec(command=command, user="root")
            self._require_success("install Gents with glibc compatibility bundle", result)
            return

        loader_check = await environment.exec(
            command=(
                "test -x /lib64/ld-linux-x86-64.so.2 || "
                "test -x /lib/x86_64-linux-gnu/ld-linux-x86-64.so.2 || "
                "test -x /lib/ld-linux-aarch64.so.1 || "
                "test -x /lib/aarch64-linux-gnu/ld-linux-aarch64.so.1"
            )
        )
        if loader_check.return_code == 0:
            command = (
                f"install -m 0755 {shlex.quote(upload_path)} "
                f"{shlex.quote(self._REMOTE_BINARY)} && "
                f"rm -f {shlex.quote(upload_path)}"
            )
            result = await environment.exec(command=command, user="root")
            self._require_success(command, result)
            return

        raise RuntimeError(
            "The task image has no glibc loader. Set GENTS_GLIBC_BUNDLE_PATH "
            "to the matching Bullseye compatibility bundle."
        )

    async def _install_release_binary(
        self, environment: BaseEnvironment, release_url: str
    ) -> None:
        quoted_url = shlex.quote(release_url)
        command = f"""
set -eu
for command_name in curl tar; do
  command -v "$command_name" >/dev/null 2>&1 || {{
    echo "release install requires $command_name" >&2
    exit 1
  }}
done
install_dir=$(mktemp -d /tmp/gents-harbor-release.XXXXXX)
trap 'rm -rf "$install_dir"' EXIT
curl -fsSL {quoted_url} -o "$install_dir/gents.tar.gz"
tar -xzf "$install_dir/gents.tar.gz" -C "$install_dir"
binary=$(find "$install_dir" -type f -name gents -perm -u+x -print -quit)
test -n "$binary"
install -m 0755 "$binary" {shlex.quote(self._REMOTE_BINARY)}
""".strip()
        result = await environment.exec(
            command=command,
            user="root",
            timeout_sec=300,
        )
        self._require_success("install Gents release", result)

    async def setup(self, environment: BaseEnvironment) -> None:
        await self._install_ca_bundle(environment)

        binary_path = self._env("GENTS_BINARY_PATH")
        release_url = self._env("GENTS_RELEASE_URL")
        if binary_path:
            await self._install_uploaded_binary(environment, Path(binary_path))
        elif release_url:
            await self._install_release_binary(environment, release_url)
        else:
            raise ValueError(
                "Set GENTS_BINARY_PATH to a host Linux gents binary or "
                "GENTS_RELEASE_URL to a gents Linux release tarball"
            )

        # An explicit glibc loader becomes /proc/self/exe, so Gents cannot use
        # its executable basename to discover the embedded filesystem runner.
        # Install a stable external command and point the runtime at it.
        install_fs_runner = (
            f"printf '%s\\n' '#!/bin/sh' "
            f"'exec {self._REMOTE_BINARY} __native-fs-runner \"$@\"' "
            f"> {self._REMOTE_FS_RUNNER} && chmod 0755 {self._REMOTE_FS_RUNNER}"
        )
        result = await environment.exec(command=install_fs_runner, user="root")
        self._require_success("install Gents native filesystem runner", result)

        if not self._RUNNER_SOURCE.is_file():
            raise FileNotFoundError(f"Harbor runner is missing: {self._RUNNER_SOURCE}")
        runner_upload = self._REMOTE_RUNNER_UPLOAD
        await environment.upload_file(self._RUNNER_SOURCE, runner_upload)
        install_runner = (
            f"install -m 0755 {shlex.quote(runner_upload)} "
            f"{shlex.quote(self._REMOTE_RUNNER)} && "
            f"rm -f {shlex.quote(runner_upload)}"
        )
        result = await environment.exec(command=install_runner, user="root")
        self._require_success(install_runner, result)

        version_result = await environment.exec(command=f"{self._REMOTE_BINARY} version")
        self._require_success("gents version", version_result)
        detected_version = (version_result.stdout or "").strip()
        if detected_version:
            self.logger.debug("Installed %s", detected_version)

        help_result = await environment.exec(
            command=f"{self._REMOTE_BINARY} trace project --help"
        )
        self._require_success("gents trace project --help", help_result)
        help_text = f"{help_result.stdout or ''}\n{help_result.stderr or ''}"
        if "atif" not in help_text or "native-json" not in help_text:
            raise RuntimeError(
                "The installed Gents binary does not include Harbor ATIF support; "
                "build this branch or use a release containing PR #988"
            )

        request_help_result = await environment.exec(
            command=f"{self._REMOTE_BINARY} request submit --help"
        )
        self._require_success("gents request submit --help", request_help_result)
        request_help_text = (
            f"{request_help_result.stdout or ''}\n{request_help_result.stderr or ''}"
        )
        if "--max-total-tokens" not in request_help_text:
            raise RuntimeError(
                "The installed Gents binary does not enforce request-wide token "
                "budgets; build this branch or use a newer release"
            )

        fs_runner_result = await environment.exec(
            command=f"{self._REMOTE_FS_RUNNER} --self-test"
        )
        self._require_success("gents native filesystem runner self-test", fs_runner_result)

    async def run(
        self,
        instruction: str,
        environment: BaseEnvironment,
        context: AgentContext,
    ) -> None:
        if not self.model_name:
            raise ValueError("Harbor --model is required for the Gents agent")

        model_name = self._env("GENTS_MODEL", self.model_name) or self.model_name
        inference_url = self._env("GENTS_INFERENCE_URL") or self._env(
            "OPENAI_BASE_URL"
        )
        if not inference_url:
            raise ValueError(
                "Set GENTS_INFERENCE_URL or OPENAI_BASE_URL to the OpenAI-compatible "
                "inference endpoint, including /v1"
            )
        max_total = self._env("GENTS_MAX_TOTAL")
        if not max_total:
            raise ValueError(
                "Set GENTS_MAX_TOTAL to the positive request-wide token budget"
            )
        if not max_total.isdecimal() or int(max_total) <= 0 or (
            len(max_total) > 1 and max_total.startswith("0")
        ):
            raise ValueError(
                "GENTS_MAX_TOTAL must be a positive integer without leading zeros"
            )

        session_slug = re.sub(r"[^A-Za-z0-9_.-]+", "-", self.session_id or "trial")
        # Harbor retries retain the trial/session identity. A per-run suffix
        # guarantees that a cancelled attempt can never leave the next attempt
        # contending on the same RocksDB LOCK file.
        run_slug = f"{session_slug}-{uuid.uuid4().hex[:12]}"
        instruction_path = f"/tmp/gents-harbor-{run_slug}.instruction.md"
        with tempfile.TemporaryDirectory(prefix="gents-harbor-instruction-") as temp_dir:
            local_instruction = Path(temp_dir) / "instruction.md"
            local_instruction.write_text(instruction)
            await environment.upload_file(local_instruction, instruction_path)

        chmod_result = await environment.exec(
            command=f"chmod 0644 {shlex.quote(instruction_path)}", user="root"
        )
        self._require_success("prepare Gents instruction", chmod_result)

        request_timeout = int(self._env("GENTS_REQUEST_TIMEOUT_SECS", "86400") or 86400)
        tool_root = self._env("GENTS_TOOL_ROOT", "/app") or "/app"
        prepare_tool_root = f"install -d -m 0755 {shlex.quote(tool_root)}"
        prepare_result = await environment.exec(
            command=prepare_tool_root,
            cwd="/",
            user="root",
        )
        self._require_success("prepare Gents tool root", prepare_result)
        run_env = {
            "GENTS_BINARY": self._REMOTE_BINARY,
            "GENTS_FS_RUNNER": self._REMOTE_FS_RUNNER,
            "GENTS_HOME": f"/tmp/gents-harbor-{run_slug}",
            "GENTS_INSTRUCTION_FILE": instruction_path,
            "GENTS_INFERENCE_URL": inference_url.rstrip("/"),
            "GENTS_MODEL": model_name,
            "GENTS_TEMPERATURE": self._env("GENTS_TEMPERATURE", "1.0") or "1.0",
            "GENTS_TOP_P": self._env("GENTS_TOP_P", "0.95") or "0.95",
            "GENTS_TOP_K": self._env("GENTS_TOP_K", "") or "",
            "GENTS_SEED": self._env("GENTS_SEED", "") or "",
            "GENTS_REASONING_EFFORT": self._env(
                "GENTS_REASONING_EFFORT", "max"
            )
            or "max",
            # Avoid `TOKEN` in this environment key. Harbor treats matching
            # agent-env names as secrets and blindly replaces their values in
            # downloaded text artifacts, which can corrupt numeric JSON fields.
            "GENTS_MAX_OUTPUT": self._env("GENTS_MAX_OUTPUT", "393216") or "393216",
            "GENTS_MAX_TOTAL": max_total,
            # Keep 53,248 tokens of provider-tokenization headroom below D4F's
            # 512K server limit. Gents dynamically clamps each turn's 384K
            # output ceiling to the context remaining after the assembled input,
            # so compaction follows the 75% input threshold instead of a fixed
            # 65,536-token reservation.
            "GENTS_CONTEXT_WINDOW": self._env("GENTS_CONTEXT_WINDOW", "458752")
            or "458752",
            "GENTS_MAX_TURNS": self._env("GENTS_MAX_TURNS", "1000") or "1000",
            "GENTS_RETRY_MAX_TRANSPORT": self._env(
                "GENTS_RETRY_MAX_TRANSPORT", "3"
            )
            or "3",
            "GENTS_REQUEST_TIMEOUT_SECS": str(request_timeout),
            "GENTS_COMMAND_TIMEOUT_SECS": self._env(
                "GENTS_COMMAND_TIMEOUT_SECS", "600"
            )
            or "600",
            "GENTS_COMMAND_TIMEOUT_MAX_SECS": self._env(
                "GENTS_COMMAND_TIMEOUT_MAX_SECS", "3600"
            )
            or "3600",
            "GENTS_SERVER_STARTUP_TIMEOUT_SECS": self._env(
                "GENTS_SERVER_STARTUP_TIMEOUT_SECS", "300"
            )
            or "300",
            "GENTS_TOOL_ROOT": tool_root,
            "GENTS_API_KEY": self._env("GENTS_API_KEY", "no-key") or "no-key",
            "SSL_CERT_FILE": self._REMOTE_CA_BUNDLE,
        }
        result = await environment.exec(
            command=self._REMOTE_RUNNER,
            cwd=run_env["GENTS_TOOL_ROOT"],
            env=run_env,
            timeout_sec=request_timeout + 180,
        )
        runner_error: BaseException | None = None
        try:
            # Classify the runner while GENTS_HOME still exists. The runner's
            # failure trap projects partial traces and captures its bounded
            # diagnostic bundle before returning a nonzero status.
            self._require_success(self._REMOTE_RUNNER, result)
        except BaseException as error:
            runner_error = error
        cleanup_files = [
            self._REMOTE_BINARY,
            self._REMOTE_REAL_BINARY,
            self._REMOTE_BINARY_UPLOAD,
            self._REMOTE_FS_RUNNER,
            self._REMOTE_RUNNER,
            self._REMOTE_RUNNER_UPLOAD,
            self._REMOTE_CA_BUNDLE,
            self._REMOTE_GLIBC_BUNDLE,
            instruction_path,
        ]
        cleanup_dirs = [self._REMOTE_GLIBC_DIR, run_env["GENTS_HOME"]]
        cleanup_command = (
            "rm -f "
            + " ".join(shlex.quote(path) for path in cleanup_files)
            + "\nrm -rf "
            + " ".join(shlex.quote(path) for path in cleanup_dirs)
        )
        cleanup_result = await environment.exec(
            command=cleanup_command,
            cwd="/",
            user="root",
        )
        if cleanup_result.return_code != 0:
            self.logger.warning(
                "Failed to remove temporary Gents runtime artifacts: %s",
                (cleanup_result.stderr or cleanup_result.stdout or "").strip(),
            )
        if runner_error is not None:
            raise runner_error

        requested_seed = int(run_env["GENTS_SEED"]) if run_env["GENTS_SEED"] else None
        persisted_snapshot = self._read_json_object(
            self.logs_dir / "request-persisted.json"
        )
        persisted_request = persisted_snapshot.get("request") or {}
        persisted_seed = persisted_request.get("seed")
        if persisted_seed != requested_seed:
            raise RuntimeError(
                "Gents request seed persistence mismatch: "
                f"requested={requested_seed!r} persisted={persisted_seed!r}"
            )
        requested_max_total = int(run_env["GENTS_MAX_TOTAL"])
        persisted_max_total = self._persisted_request_value(
            persisted_request, "max_total_tokens"
        )
        if persisted_max_total != requested_max_total:
            raise RuntimeError(
                "Gents aggregate token-budget persistence mismatch: "
                f"requested={requested_max_total!r} persisted={persisted_max_total!r}"
            )

        context.metadata = {
            **(context.metadata or {}),
            "gents": {
                "model": model_name,
                "inference_url": inference_url,
                "temperature": float(run_env["GENTS_TEMPERATURE"]),
                "top_p": float(run_env["GENTS_TOP_P"]),
                "seed": persisted_seed,
                "reasoning_effort": run_env["GENTS_REASONING_EFFORT"],
                "context_window": int(run_env["GENTS_CONTEXT_WINDOW"]),
                "max_output_tokens": int(run_env["GENTS_MAX_OUTPUT"]),
                "max_total_tokens": persisted_max_total,
                "max_turns": int(run_env["GENTS_MAX_TURNS"]),
                "request_timeout_secs": request_timeout,
                "retry_max_transport": int(run_env["GENTS_RETRY_MAX_TRANSPORT"]),
            },
        }

    def populate_context_post_run(self, context: AgentContext) -> None:
        trajectory_path = self.logs_dir / "trajectory.json"
        trajectory: dict[str, Any] = {}
        if not trajectory_path.is_file():
            self.logger.warning("Gents did not emit %s", trajectory_path)
        else:
            try:
                parsed_trajectory = json.loads(trajectory_path.read_text())
                if isinstance(parsed_trajectory, dict):
                    trajectory = parsed_trajectory
            except (OSError, json.JSONDecodeError):
                self.logger.exception("Failed to read Gents ATIF trajectory")

        request = self._read_json_object(self.logs_dir / "request.json")
        outcome = self._read_json_object(self.logs_dir / "gents-outcome.json")
        response = self._read_json_object(self.logs_dir / "response.json")
        diagnostic = self._read_json_object(self.logs_dir / "gents-diagnostic.json")
        server_exit = self._read_json_object(self.logs_dir / "gents-server-exit.json")
        final_metrics = trajectory.get("final_metrics") or {}
        if isinstance(final_metrics, dict):
            for attribute, key in (
                ("n_input_tokens", "total_prompt_tokens"),
                ("n_cache_tokens", "total_cached_tokens"),
                ("n_output_tokens", "total_completion_tokens"),
            ):
                value = final_metrics.get(key)
                if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                    setattr(context, attribute, value)
        failure_origin = None
        if diagnostic.get("reason") == "server_lost_during_request":
            failure_origin = "gents_server"
        elif outcome.get("outcome") == "compaction_provider_error":
            failure_origin = "compaction_provider"

        context.metadata = {
            **(context.metadata or {}),
            "gents": {
                **((context.metadata or {}).get("gents") or {}),
                "request_id": request.get("request_id"),
                "session_id": trajectory.get("session_id"),
                "trajectory_id": trajectory.get("trajectory_id"),
                "total_steps": final_metrics.get("total_steps")
                if isinstance(final_metrics, dict)
                else None,
                # The runner returns control to Harbor for exhausted turn
                # budgets so the verifier can score the workspace. Surface the
                # distinction so budget-limited trials are identifiable.
                "outcome": outcome.get("outcome"),
                "budget_exhausted": outcome.get("outcome")
                in {"max_turns_exhausted", "token_budget_exhausted"},
                "terminal_error": response.get("error_message"),
                "failure_origin": failure_origin,
                "diagnostic_reason": diagnostic.get("reason"),
                "diagnostic_graphql_available": diagnostic.get("graphql_available"),
                "server_exit": server_exit or None,
            },
        }

    def _read_json_object(self, path: Path) -> dict[str, Any]:
        if not path.is_file():
            return {}
        try:
            parsed = json.loads(path.read_text())
        except (OSError, ValueError):
            # ValueError covers JSONDecodeError and the UnicodeDecodeError a
            # Harbor artifact rewrite can leave behind.
            self.logger.warning("Failed to read %s", path, exc_info=True)
            return {}
        return parsed if isinstance(parsed, dict) else {}
