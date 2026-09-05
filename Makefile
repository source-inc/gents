SHELL := /bin/sh

CARGO ?= cargo
LAKE ?= lake
NPM ?= npm

DESKTOP_DIR := apps/gents-desktop
PROOFS_DIR := crates/gents/proofs
FUZZ_TIME ?= 30s
MAINTENANCE_ROOT ?= $(CURDIR)
MAINTENANCE_BRANCH ?=
MAINTENANCE_HEAD ?= HEAD
MAINTENANCE_PR_BASE ?= main
MAINTENANCE_PROMPT ?= Find the next small, behavior-preserving repository cleanup wave and package the strongest work into focused 1-3 finding commits on one shared branch and worktree.
MAINTENANCE_AREAS ?= auto
MAINTENANCE_MIN_AREAS ?= 5
MAINTENANCE_MAX_AREAS ?= 10
MAINTENANCE_HISTORY_DEPTH ?= 250
MAINTENANCE_PORT ?= 19192
MAINTENANCE_JOB_ID ?=
MAINTENANCE_KEEP_HOME ?=
MAINTENANCE_CONTEXT_WINDOW ?= 262144
MAINTENANCE_MAX_OUTPUT_TOKENS ?= 65536
MAINTENANCE_MAX_TURNS ?= 1000000
MAINTENANCE_TEMPERATURE ?= 1.0
MAINTENANCE_TOP_P ?= 0.95
MAINTENANCE_COMPACTION_THRESHOLD ?= 0.85
MAINTENANCE_DEADLINE_SECS ?= 86400
MAINTENANCE_AWAIT_TIMEOUT_SECS ?= 86400
MAINTENANCE_STREAM_LIVENESS_SECS ?= 1800
MAINTENANCE_STREAM_BATCH_MS ?= 5000
MAINTENANCE_RETRY_MAX_TRANSPORT ?= 720
MAINTENANCE_RETRY_MAX_RESAMPLE ?= 32
DEFENDING_ROOT ?= $(CURDIR)
DEFENDING_PROMPT ?= Map the repository's trust boundaries, find plausible exploitable vulnerabilities, adversarially verify them, and draft minimal reviewable fixes for confirmed findings.
DEFENDING_ENDPOINT ?= http://100.73.235.38:8000/v1
DEFENDING_MODEL ?= GLM-5.2
DEFENDING_MIN_AREAS ?= 4
DEFENDING_MAX_AREAS ?= 10
DEFENDING_MAX_CONCURRENT ?= 8
DEFENDING_PORT ?= 19193
DEFENDING_PAGE_PORT ?= 19194
DEFENDING_JOB_ID ?=
DEFENDING_KEEP_HOME ?=
DEFENDING_CONTEXT_WINDOW ?= 262144
DEFENDING_MAX_OUTPUT_TOKENS ?= 65536
DEFENDING_MAX_TURNS ?= 1000000
DEFENDING_TEMPERATURE ?= 1.0
DEFENDING_TOP_P ?= 0.95
DEFENDING_COMPACTION_THRESHOLD ?= 0.762939453125
DEFENDING_DEADLINE_SECS ?= 86400
DEFENDING_AWAIT_TIMEOUT_SECS ?= 86400
DEFENDING_STREAM_LIVENESS_SECS ?= 1800
DEFENDING_STREAM_BATCH_MS ?= 5000
DEFENDING_RETRY_MAX_TRANSPORT ?= 720
DEFENDING_RETRY_MAX_RESAMPLE ?= 32
GROK_PORT_CEILING ?= $(CURDIR)
GROK_PORT_GENTS_ROOT ?= $(CURDIR)
GROK_PORT_GROK_ROOT ?= $(CURDIR)/demo/grok-tui-port/recon-input
GROK_PORT_PROMPT ?= Map the Grok TUI wire from grok-build and implement a Gents-only thin client. Do not add DefraDB access control or Grok permission UI. Prove model name, context window, tool-call semantics, subprocesses, subagents, and interrupts with live GLM turns.
GROK_PORT_BASE_SHA ?= HEAD
GROK_PORT_PR_BASE ?= main
GROK_PORT_BRANCH ?= agent/grok-tui-port-pack9
GROK_PORT_ENDPOINT_1 ?= http://127.0.0.1:8000/v1
GROK_PORT_MODEL ?= GLM-5.3-Flash-NVFP4
GROK_PORT_MIN_SURFACES ?= 13
GROK_PORT_MAX_SURFACES ?= 13
GROK_PORT_MAX_CONCURRENT_1 ?= 16
GROK_PORT_PORT ?= 19195
GROK_PORT_LIVE_PORT ?= 19196
GROK_PORT_JOB_ID ?=
GROK_PORT_KEEP_HOME ?=
GROK_PORT_CONTEXT_WINDOW ?= 524288
GROK_PORT_MAX_OUTPUT_TOKENS ?= 65536
GROK_PORT_MAX_TURNS ?= 1000000
GROK_PORT_TEMPERATURE ?= 1.0
GROK_PORT_RECON_TEMPERATURE ?= $(GROK_PORT_TEMPERATURE)
GROK_PORT_IMPLEMENT_TEMPERATURE ?= $(GROK_PORT_TEMPERATURE)
GROK_PORT_REVIEW_TEMPERATURE ?= $(GROK_PORT_TEMPERATURE)
GROK_PORT_CODE_REVIEW_TEMPERATURE ?= $(GROK_PORT_TEMPERATURE)
GROK_PORT_TOP_P ?= 0.95
GROK_PORT_REASONING_EFFORT ?= high
GROK_PORT_CODE_REVIEW_REASONING_EFFORT ?= $(GROK_PORT_REASONING_EFFORT)
GROK_PORT_COMPACTION_THRESHOLD ?= 0.762939453125
GROK_PORT_DEADLINE_SECS ?= 86400
GROK_PORT_AWAIT_TIMEOUT_SECS ?= 86400
GROK_PORT_STREAM_LIVENESS_SECS ?= 1800
GROK_PORT_STREAM_BATCH_MS ?= 5000
GROK_PORT_RETRY_MAX_TRANSPORT ?= 720
GROK_PORT_RETRY_MAX_RESAMPLE ?= 32

.DEFAULT_GOAL := help

.PHONY: help
help:
	@echo "Build:"
	@echo "  make build                 Build default Rust workspace members"
	@echo "  make build-cli             Build the Gents CLI"
	@echo "  make build-cli-headless    Build CLI without embedded Codex TUI"
	@echo "  make fast-dev-cli          Build CLI with lean dev debug artifacts"
	@echo "  make build-desktop         Build the Tauri Rust shell"
	@echo "  make build-desktop-ui      Build the desktop frontend"
	@echo
	@echo "Release (CI calls these; TARGET=<triple> optional, defaults to host):"
	@echo "  make release-cli           Build the release CLI (full features)"
	@echo "  make release-cli-headless  Build the release CLI without embedded Codex TUI"
	@echo "  make dist-cli              Build + package $(DIST_DIR)/gents-<triple>.tar.gz(+sha256)"
	@echo
	@echo "Measurements:"
	@echo "  make measure-build-graph   Report the normal CLI dependency graph"
	@echo "  make measure-release-cli   Build and report release binary metrics"
	@echo "  make measure-build-attribution  Cold build timing and linked-size attribution"
	@echo
	@echo "Checks:"
	@echo "  make fmt                   Format Rust and desktop UI code"
	@echo "  make fmt-check             Check Rust and desktop UI formatting"
	@echo "  make check-cli-headless    Check CLI without embedded Codex TUI"
	@echo "  make proofs                Build Lean proofs"
	@echo
	@echo "Tests:"
	@echo "  make test                  Run core Rust and CLI tests"
	@echo "  make test-agent            Run Gents runtime tests"
	@echo "  make test-agent-conformance  Run runtime conformance tests"
	@echo "  make test-agent-e2e        Run deterministic agent E2E tests"
	@echo "  make test-cli              Run CLI tests"
	@echo
	@echo "Desktop UI:"
	@echo "  make desktop-ui            Run full desktop UI suite"
	@echo "  make desktop-ui-qa-sweep   Run desktop QA sweep (format/build/unit/e2e/screenshots/fuzz)"
	@echo "  make desktop-ui-unit       Run desktop unit tests"
	@echo "  make desktop-ui-e2e        Run desktop Playwright journeys"
	@echo "  make desktop-ui-invariants Run desktop Playwright invariant checks"
	@echo "  make desktop-ui-screenshots  Capture stable desktop screenshot artifacts"
	@echo "  make desktop-ui-fuzz       Run desktop Bombadil smoke (FUZZ_TIME=$(FUZZ_TIME))"
	@echo "  make desktop-ui-fuzz-long  Run longer desktop Bombadil sweep"
	@echo "  make desktop-ui-agent      Start the JSONL browser driver for LLM agents"
	@echo "  make desktop-ui-visual     Run desktop visual baseline checks"
	@echo "  make desktop-ui-live-e2e   Run live browser-to-runtime desktop smoke"
	@echo "  make desktop-ui-live-e2e-real  Run live browser smoke against a configured real provider"
	@echo "  make desktop-native-preflight  Build frontend/Rust shell and print Tauri CLI version"
	@echo "  make desktop-native-dev    Launch the native Tauri dev app for manual QA"
	@echo "  make desktop-native-build  Build the native Tauri app bundle"
	@echo
	@echo "Live:"
	@echo "  make live-cli              Run live CLI smoke test"
	@echo "  make live-agent            Run ignored live runtime tests"
	@echo "  make live-desktop-smoke    Run live desktop smoke suites"
	@echo
	@echo "Bundled graphs:"
	@echo "  gents graph catalog        Inspect graphs shipped in the binary"
	@echo "  gents graph install code-review"
	@echo "  gents graph run code-review --repo DIR --base origin/main --head HEAD"
	@echo
	@echo "Maintenance:"
	@echo "  make maintain              Audit MAINTENANCE_ROOT and emit small cleanup work packages"
	@echo "    MAINTENANCE_PROMPT='...'  Override the maintenance focus"
	@echo "    MAINTENANCE_AREAS=auto    Let recon choose the area count"
	@echo "    MAINTENANCE_MIN_AREAS=5   Set the automatic lower bound"
	@echo "    MAINTENANCE_MAX_AREAS=10  Set the automatic upper bound"
	@echo "    MAINTENANCE_HISTORY_DEPTH=250  Set first-parent history depth"
	@echo "    MAINTENANCE_PR_BASE=main  Set the base branch for the final PR"
	@echo "    MAINTENANCE_BRANCH=BRANCH  Override the exact new maintenance branch"
	@echo "    MAINTENANCE_KEEP_HOME=1   Keep the generated runtime home"
	@echo
	@echo "Defending code:"
	@echo "  make defend-page           Open the live campaign visualizer on :$(DEFENDING_PAGE_PORT)"
	@echo "  make defend                Run the threat-model-driven defending-code pack"
	@echo "    DEFENDING_ROOT=DIR        Set the authorized repository root"
	@echo "    DEFENDING_PROMPT='...'    Override the defensive-review focus"
	@echo "    DEFENDING_ENDPOINT=URL    Set the OpenAI-compatible backend"
	@echo "    DEFENDING_MODEL=MODEL     Set the model (default GLM-5.2)"
	@echo "    DEFENDING_MIN_AREAS=4     Set the review-area lower bound"
	@echo "    DEFENDING_MAX_AREAS=10    Set the review-area upper bound"
	@echo "    DEFENDING_MAX_CONCURRENT=8  Cap concurrent backend requests"
	@echo "    DEFENDING_PAGE_PORT=19194  Set the visualizer port"
	@echo "    DEFENDING_KEEP_HOME=1     Keep the generated runtime home"
	@echo
	@echo "Grok TUI port:"
	@echo "  make grok-port             Map grok-build and implement a Gents-only Grok TUI shim"
	@echo "    GROK_PORT_PROMPT='...'   Override the port focus"
	@echo "    GROK_PORT_ENDPOINT_1=URL Set the OpenAI-compatible inference endpoint"
	@echo "    GROK_PORT_MODEL=MODEL    Set the model (default GLM-5.3-Flash-NVFP4)"
	@echo "    GROK_PORT_KEEP_HOME=1    Keep the generated runtime home"
	@echo
	@echo "Worktrees:"
	@echo "  make worktree BRANCH=<branch> [DIR=<dest>] [BASE=<ref>]"
	@echo "                             Create a worktree with target/ and proofs/.lake"
	@echo "                             cloned from this checkout (APFS clonefile)"

.PHONY: worktree
worktree:
	@test -n "$(BRANCH)" || { echo "usage: make worktree BRANCH=<branch> [DIR=<dest>] [BASE=<ref>]" >&2; exit 2; }
	@WORKTREE_DIR="$(DIR)" WORKTREE_BASE="$(BASE)" scripts/worktree-bootstrap.sh "$(BRANCH)"

.PHONY: build build-cli build-cli-headless build-desktop build-desktop-ui
build:
	$(CARGO) build

build-cli:
	$(CARGO) build -p gents-cli

.PHONY: maintain
maintain:
	@test -d "$(MAINTENANCE_ROOT)" || { echo "MAINTENANCE_ROOT is not a directory: $(MAINTENANCE_ROOT)" >&2; exit 2; }
	@case "$(MAINTENANCE_AREAS)" in auto) ;; ''|*[!0-9]*) echo "MAINTENANCE_AREAS must be auto or a positive integer: $(MAINTENANCE_AREAS)" >&2; exit 2;; *) test "$(MAINTENANCE_AREAS)" -gt 0 || { echo "MAINTENANCE_AREAS must be greater than zero" >&2; exit 2; };; esac
	@case "$(MAINTENANCE_MIN_AREAS)" in ''|*[!0-9]*) echo "MAINTENANCE_MIN_AREAS must be a positive integer: $(MAINTENANCE_MIN_AREAS)" >&2; exit 2;; esac
	@case "$(MAINTENANCE_MAX_AREAS)" in ''|*[!0-9]*) echo "MAINTENANCE_MAX_AREAS must be a positive integer: $(MAINTENANCE_MAX_AREAS)" >&2; exit 2;; esac
	@case "$(MAINTENANCE_HISTORY_DEPTH)" in ''|*[!0-9]*) echo "MAINTENANCE_HISTORY_DEPTH must be a positive integer: $(MAINTENANCE_HISTORY_DEPTH)" >&2; exit 2;; esac
	@test "$(MAINTENANCE_MIN_AREAS)" -ge 5 && test "$(MAINTENANCE_MAX_AREAS)" -ge "$(MAINTENANCE_MIN_AREAS)" || { echo "maintenance area bounds must satisfy 5 <= MAINTENANCE_MIN_AREAS <= MAINTENANCE_MAX_AREAS" >&2; exit 2; }
	@if test "$(MAINTENANCE_AREAS)" != auto; then test "$(MAINTENANCE_AREAS)" -ge "$(MAINTENANCE_MIN_AREAS)" && test "$(MAINTENANCE_AREAS)" -le "$(MAINTENANCE_MAX_AREAS)" || { echo "MAINTENANCE_AREAS must satisfy MAINTENANCE_MIN_AREAS <= MAINTENANCE_AREAS <= MAINTENANCE_MAX_AREAS" >&2; exit 2; }; fi
	@test "$(MAINTENANCE_HISTORY_DEPTH)" -gt 0 || { echo "MAINTENANCE_HISTORY_DEPTH must be greater than zero" >&2; exit 2; }
	@cd "$(MAINTENANCE_ROOT)" && git rev-parse --verify "$(MAINTENANCE_HEAD)^{commit}" >/dev/null || { echo "MAINTENANCE_HEAD is not a commit: $(MAINTENANCE_HEAD)" >&2; exit 2; }
	@command -v rust-analyzer >/dev/null 2>&1 || echo "warning: rust-analyzer not found on PATH; maintenance will fall back to file/search tools" >&2
	@maintenance_job_id="$(MAINTENANCE_JOB_ID)"; \
	if test -z "$$maintenance_job_id"; then maintenance_job_id="maintenance-$$(date -u +%Y%m%dT%H%M%SZ)-$$$$"; fi; \
	maintenance_branch="$(MAINTENANCE_BRANCH)"; \
	if test -z "$$maintenance_branch"; then maintenance_branch="agent/$$maintenance_job_id"; fi; \
	GENTS_MAINTENANCE_ROOT="$(abspath $(MAINTENANCE_ROOT))" \
	GENTS_MAINTENANCE_BRANCH="$$maintenance_branch" \
	GENTS_MAINTENANCE_HEAD_REF="$(MAINTENANCE_HEAD)" \
	GENTS_MAINTENANCE_PR_BASE="$(MAINTENANCE_PR_BASE)" \
	GENTS_MAINTENANCE_PROMPT="$(MAINTENANCE_PROMPT)" \
	GENTS_MAINTENANCE_AREA_COUNT="$(MAINTENANCE_AREAS)" \
	GENTS_MAINTENANCE_MIN_AREAS="$(MAINTENANCE_MIN_AREAS)" \
	GENTS_MAINTENANCE_MAX_AREAS="$(MAINTENANCE_MAX_AREAS)" \
	GENTS_MAINTENANCE_HISTORY_DEPTH="$(MAINTENANCE_HISTORY_DEPTH)" \
	GENTS_MAINTENANCE_CONTEXT_WINDOW="$(MAINTENANCE_CONTEXT_WINDOW)" \
	GENTS_MAINTENANCE_MAX_OUTPUT_TOKENS="$(MAINTENANCE_MAX_OUTPUT_TOKENS)" \
	GENTS_MAINTENANCE_MAX_TURNS="$(MAINTENANCE_MAX_TURNS)" \
	GENTS_MAINTENANCE_TEMPERATURE="$(MAINTENANCE_TEMPERATURE)" \
	GENTS_MAINTENANCE_TOP_P="$(MAINTENANCE_TOP_P)" \
	GENTS_MAINTENANCE_COMPACTION_THRESHOLD="$(MAINTENANCE_COMPACTION_THRESHOLD)" \
	GENTS_MAINTENANCE_DEADLINE_SECS="$(MAINTENANCE_DEADLINE_SECS)" \
	GENTS_MAINTENANCE_AWAIT_TIMEOUT_SECS="$(MAINTENANCE_AWAIT_TIMEOUT_SECS)" \
	GENTS_MAINTENANCE_STREAM_LIVENESS_SECS="$(MAINTENANCE_STREAM_LIVENESS_SECS)" \
	GENTS_MAINTENANCE_STREAM_BATCH_MS="$(MAINTENANCE_STREAM_BATCH_MS)" \
	GENTS_MAINTENANCE_RETRY_MAX_TRANSPORT="$(MAINTENANCE_RETRY_MAX_TRANSPORT)" \
	GENTS_MAINTENANCE_RETRY_MAX_RESAMPLE="$(MAINTENANCE_RETRY_MAX_RESAMPLE)" \
	$(CARGO) run -p gents-cli -- demo run "$(CURDIR)/demo/repo-maintenance" \
		--http-port "$(MAINTENANCE_PORT)" \
		--job-id "$$maintenance_job_id" \
		$(if $(MAINTENANCE_KEEP_HOME),--keep-home,)

.PHONY: defend
defend:
	@test -d "$(DEFENDING_ROOT)" || { echo "DEFENDING_ROOT is not a directory: $(DEFENDING_ROOT)" >&2; exit 2; }
	@case "$(DEFENDING_MIN_AREAS)" in ''|*[!0-9]*) echo "DEFENDING_MIN_AREAS must be a positive integer: $(DEFENDING_MIN_AREAS)" >&2; exit 2;; esac
	@case "$(DEFENDING_MAX_AREAS)" in ''|*[!0-9]*) echo "DEFENDING_MAX_AREAS must be a positive integer: $(DEFENDING_MAX_AREAS)" >&2; exit 2;; esac
	@case "$(DEFENDING_MAX_CONCURRENT)" in ''|*[!0-9]*) echo "DEFENDING_MAX_CONCURRENT must be a positive integer: $(DEFENDING_MAX_CONCURRENT)" >&2; exit 2;; esac
	@test "$(DEFENDING_MIN_AREAS)" -gt 0 && test "$(DEFENDING_MAX_AREAS)" -ge "$(DEFENDING_MIN_AREAS)" || { echo "defending area bounds must satisfy 0 < DEFENDING_MIN_AREAS <= DEFENDING_MAX_AREAS" >&2; exit 2; }
	@test "$(DEFENDING_MAX_CONCURRENT)" -gt 0 || { echo "DEFENDING_MAX_CONCURRENT must be greater than zero" >&2; exit 2; }
	@command -v rust-analyzer >/dev/null 2>&1 || echo "warning: rust-analyzer not found on PATH; defending-code will fall back to file/search tools" >&2
	@defending_job_id="$(DEFENDING_JOB_ID)"; \
	if test -z "$$defending_job_id"; then defending_job_id="defending-$$(date -u +%Y%m%dT%H%M%SZ)-$$$$"; fi; \
	GENTS_DEFENDING_ROOT="$(abspath $(DEFENDING_ROOT))" \
	GENTS_DEFENDING_PROMPT="$(DEFENDING_PROMPT)" \
	GENTS_DEFENDING_ENDPOINT="$(DEFENDING_ENDPOINT)" \
	GENTS_DEFENDING_MODEL="$(DEFENDING_MODEL)" \
	GENTS_DEFENDING_MIN_AREAS="$(DEFENDING_MIN_AREAS)" \
	GENTS_DEFENDING_MAX_AREAS="$(DEFENDING_MAX_AREAS)" \
	GENTS_DEFENDING_MAX_CONCURRENT="$(DEFENDING_MAX_CONCURRENT)" \
	GENTS_DEFENDING_CONTEXT_WINDOW="$(DEFENDING_CONTEXT_WINDOW)" \
	GENTS_DEFENDING_MAX_OUTPUT_TOKENS="$(DEFENDING_MAX_OUTPUT_TOKENS)" \
	GENTS_DEFENDING_MAX_TURNS="$(DEFENDING_MAX_TURNS)" \
	GENTS_DEFENDING_TEMPERATURE="$(DEFENDING_TEMPERATURE)" \
	GENTS_DEFENDING_TOP_P="$(DEFENDING_TOP_P)" \
	GENTS_DEFENDING_COMPACTION_THRESHOLD="$(DEFENDING_COMPACTION_THRESHOLD)" \
	GENTS_DEFENDING_DEADLINE_SECS="$(DEFENDING_DEADLINE_SECS)" \
	GENTS_DEFENDING_AWAIT_TIMEOUT_SECS="$(DEFENDING_AWAIT_TIMEOUT_SECS)" \
	GENTS_DEFENDING_STREAM_LIVENESS_SECS="$(DEFENDING_STREAM_LIVENESS_SECS)" \
	GENTS_DEFENDING_STREAM_BATCH_MS="$(DEFENDING_STREAM_BATCH_MS)" \
	GENTS_DEFENDING_RETRY_MAX_TRANSPORT="$(DEFENDING_RETRY_MAX_TRANSPORT)" \
	GENTS_DEFENDING_RETRY_MAX_RESAMPLE="$(DEFENDING_RETRY_MAX_RESAMPLE)" \
	$(CARGO) run -p gents-cli -- demo run "$(CURDIR)/demo/defending-code" \
		--http-port "$(DEFENDING_PORT)" \
		--job-id "$$defending_job_id" \
		$(if $(DEFENDING_KEEP_HOME),--keep-home,)

.PHONY: grok-port
grok-port:
	@test -d "$(GROK_PORT_GENTS_ROOT)" || { echo "GROK_PORT_GENTS_ROOT is not a directory: $(GROK_PORT_GENTS_ROOT)" >&2; exit 2; }
	@test -d "$(GROK_PORT_CEILING)" || { echo "GROK_PORT_CEILING is not a directory: $(GROK_PORT_CEILING)" >&2; exit 2; }
	@case "$(GROK_PORT_MIN_SURFACES)" in ''|*[!0-9]*) echo "GROK_PORT_MIN_SURFACES must be a positive integer: $(GROK_PORT_MIN_SURFACES)" >&2; exit 2;; esac
	@case "$(GROK_PORT_MAX_SURFACES)" in ''|*[!0-9]*) echo "GROK_PORT_MAX_SURFACES must be a positive integer: $(GROK_PORT_MAX_SURFACES)" >&2; exit 2;; esac
	@case "$(GROK_PORT_MAX_CONCURRENT_1)" in ''|*[!0-9]*) echo "GROK_PORT_MAX_CONCURRENT_1 must be a positive integer: $(GROK_PORT_MAX_CONCURRENT_1)" >&2; exit 2;; esac
	@test "$(GROK_PORT_MIN_SURFACES)" -gt 0 && test "$(GROK_PORT_MAX_SURFACES)" -ge "$(GROK_PORT_MIN_SURFACES)" || { echo "grok-port surface bounds must satisfy 0 < GROK_PORT_MIN_SURFACES <= GROK_PORT_MAX_SURFACES" >&2; exit 2; }
	@test "$(GROK_PORT_MAX_CONCURRENT_1)" -gt 0 || { echo "GROK_PORT_MAX_CONCURRENT_1 must be greater than zero" >&2; exit 2; }
	@command -v rust-analyzer >/dev/null 2>&1 || echo "warning: rust-analyzer not found on PATH; grok-tui-port will fall back to file/search tools" >&2
	@grok_port_job_id="$(GROK_PORT_JOB_ID)"; \
	if test -z "$$grok_port_job_id"; then grok_port_job_id="grok-port-$$(date -u +%Y%m%dT%H%M%SZ)-$$$$"; fi; \
	grok_port_base_sha="$$(git -C "$(abspath $(GROK_PORT_GENTS_ROOT))" rev-parse --verify "$(GROK_PORT_BASE_SHA)^{commit}")" || exit 2; \
	grok_port_models="$$(curl --fail --silent --show-error --max-time 10 "$(GROK_PORT_ENDPOINT_1)/models")" || { echo "GLM preflight failed: $(GROK_PORT_ENDPOINT_1)/models" >&2; exit 2; }; \
	case "$$grok_port_models" in *'"id":"$(GROK_PORT_MODEL)"'*) ;; *) echo "GLM preflight did not advertise $(GROK_PORT_MODEL): $(GROK_PORT_ENDPOINT_1)" >&2; exit 2;; esac; \
	grok_port_max_context="$$(printf '%s' "$$grok_port_models" | python3 -c 'import json,sys; model=sys.argv[1]; rows=json.load(sys.stdin).get("data", []); print(max((int(row.get("max_model_len", 0)) for row in rows if row.get("id") == model), default=0))' "$(GROK_PORT_MODEL)")" || exit 2; \
	test "$$grok_port_max_context" -ge "$(GROK_PORT_CONTEXT_WINDOW)" || { echo "GLM preflight context $$grok_port_max_context is smaller than required $(GROK_PORT_CONTEXT_WINDOW): $(GROK_PORT_ENDPOINT_1)" >&2; exit 2; }; \
	GENTS_GROK_PORT_CEILING="$(abspath $(GROK_PORT_CEILING))" \
	GENTS_GROK_PORT_GENTS_ROOT="$(abspath $(GROK_PORT_GENTS_ROOT))" \
	GENTS_GROK_PORT_GROK_ROOT="$(abspath $(GROK_PORT_GROK_ROOT))" \
	GENTS_GROK_PORT_PROMPT="$(GROK_PORT_PROMPT)" \
	GENTS_GROK_PORT_BASE_SHA="$$grok_port_base_sha" \
	GENTS_GROK_PORT_PR_BASE="$(GROK_PORT_PR_BASE)" \
	GENTS_GROK_PORT_BRANCH="$(GROK_PORT_BRANCH)" \
	GENTS_GROK_PORT_ENDPOINT_1="$(GROK_PORT_ENDPOINT_1)" \
	GENTS_GROK_PORT_MODEL="$(GROK_PORT_MODEL)" \
	GENTS_GROK_PORT_MIN_SURFACES="$(GROK_PORT_MIN_SURFACES)" \
	GENTS_GROK_PORT_MAX_SURFACES="$(GROK_PORT_MAX_SURFACES)" \
	GENTS_GROK_PORT_MAX_CONCURRENT_1="$(GROK_PORT_MAX_CONCURRENT_1)" \
	GENTS_GROK_PORT_ORCHESTRATOR_HOME="$(CURDIR)/demo/grok-tui-port/runs/$$grok_port_job_id/home" \
	GENTS_GROK_PORT_ORCHESTRATOR_GRAPHQL="http://127.0.0.1:$(GROK_PORT_PORT)/api/v0/graphql" \
	GENTS_GROK_PORT_LIVE_HOME="$(abspath $(GROK_PORT_GENTS_ROOT))/demo/grok-tui-port/runs/$$grok_port_job_id/live-home" \
	GENTS_GROK_PORT_LIVE_GRAPHQL="http://127.0.0.1:$(GROK_PORT_LIVE_PORT)/api/v0/graphql" \
	GENTS_GROK_PORT_LIVE_SOCKET="$(abspath $(GROK_PORT_GENTS_ROOT))/demo/grok-tui-port/runs/$$grok_port_job_id/grok-leader.sock" \
	GENTS_GROK_PORT_CONTEXT_WINDOW="$(GROK_PORT_CONTEXT_WINDOW)" \
	GENTS_GROK_PORT_MAX_OUTPUT_TOKENS="$(GROK_PORT_MAX_OUTPUT_TOKENS)" \
	GENTS_GROK_PORT_MAX_TURNS="$(GROK_PORT_MAX_TURNS)" \
	GENTS_GROK_PORT_TEMPERATURE="$(GROK_PORT_TEMPERATURE)" \
	GENTS_GROK_PORT_RECON_TEMPERATURE="$(GROK_PORT_RECON_TEMPERATURE)" \
	GENTS_GROK_PORT_IMPLEMENT_TEMPERATURE="$(GROK_PORT_IMPLEMENT_TEMPERATURE)" \
	GENTS_GROK_PORT_REVIEW_TEMPERATURE="$(GROK_PORT_REVIEW_TEMPERATURE)" \
	GENTS_GROK_PORT_CODE_REVIEW_TEMPERATURE="$(GROK_PORT_CODE_REVIEW_TEMPERATURE)" \
	GENTS_GROK_PORT_TOP_P="$(GROK_PORT_TOP_P)" \
	GENTS_GROK_PORT_REASONING_EFFORT="$(GROK_PORT_REASONING_EFFORT)" \
	GENTS_GROK_PORT_CODE_REVIEW_REASONING_EFFORT="$(GROK_PORT_CODE_REVIEW_REASONING_EFFORT)" \
	GENTS_GROK_PORT_COMPACTION_THRESHOLD="$(GROK_PORT_COMPACTION_THRESHOLD)" \
	GENTS_GROK_PORT_DEADLINE_SECS="$(GROK_PORT_DEADLINE_SECS)" \
	GENTS_GROK_PORT_AWAIT_TIMEOUT_SECS="$(GROK_PORT_AWAIT_TIMEOUT_SECS)" \
	GENTS_GROK_PORT_STREAM_LIVENESS_SECS="$(GROK_PORT_STREAM_LIVENESS_SECS)" \
	GENTS_GROK_PORT_STREAM_BATCH_MS="$(GROK_PORT_STREAM_BATCH_MS)" \
	GENTS_GROK_PORT_RETRY_MAX_TRANSPORT="$(GROK_PORT_RETRY_MAX_TRANSPORT)" \
	GENTS_GROK_PORT_RETRY_MAX_RESAMPLE="$(GROK_PORT_RETRY_MAX_RESAMPLE)" \
	$(CARGO) run -p gents-cli -- demo run "$(CURDIR)/demo/grok-tui-port" \
		--http-port "$(GROK_PORT_PORT)" \
		--job-id "$$grok_port_job_id" \
		$(if $(GROK_PORT_KEEP_HOME),--keep-home,)

.PHONY: defend-page
defend-page:
	@echo "page     http://127.0.0.1:$(DEFENDING_PAGE_PORT)/?pack=defending"
	@echo "runtime  http://127.0.0.1:$(DEFENDING_PORT)"
	@DEMO_RUNTIME_PORT="$(DEFENDING_PORT)" DEMO_PAGE_PORT="$(DEFENDING_PAGE_PORT)" VITE_DEMO_MODE=defending $(NPM) --prefix apps/review-demo run dev

build-cli-headless:
	$(CARGO) build -p gents-cli --no-default-features

build-desktop:
	$(CARGO) build -p gents-desktop-tauri

build-desktop-ui:
	$(NPM) --prefix $(DESKTOP_DIR) run build

# ---- Release / packaging ----
# Produces the Linux release artifacts; the release workflow calls these so CI
# and local builds run the same commands. LTO, codegen-units and build
# parallelism are controlled by the caller's environment
# (CARGO_PROFILE_RELEASE_LTO, CARGO_BUILD_JOBS) — per-arch memory tuning lives
# in .github/workflows/release-linux.yml, not here.
TARGET ?=
DIST_DIR ?= dist
CARGO_TARGET_FLAG := $(if $(TARGET),--target $(TARGET),)
TARGET_TRIPLE := $(if $(TARGET),$(TARGET),$(shell rustc -Vv | awk '/^host:/ { print $$2 }'))
RELEASE_BIN := target/$(if $(TARGET),$(TARGET)/,)release/gents
RELEASE_ARTIFACT := gents-$(TARGET_TRIPLE)

.PHONY: release-cli release-cli-headless fast-dev-cli dist-cli measure-build-graph measure-release-cli measure-build-attribution
release-cli:
	$(CARGO) build -p gents-cli --release --locked $(CARGO_TARGET_FLAG)

release-cli-headless:
	$(CARGO) build -p gents-cli --release --locked --no-default-features $(CARGO_TARGET_FLAG)

fast-dev-cli:
	$(CARGO) build -p gents-cli --profile fast-dev --locked $(CARGO_TARGET_FLAG)

measure-build-graph:
	MEASURE_MODE=graph scripts/measure-gents-binary.sh

measure-release-cli:
	scripts/measure-gents-binary.sh

measure-build-attribution:
	scripts/measure-gents-build-attribution.sh

dist-cli: release-cli
	@rm -rf "$(DIST_DIR)/$(RELEASE_ARTIFACT)"
	@mkdir -p "$(DIST_DIR)/$(RELEASE_ARTIFACT)"
	cp "$(RELEASE_BIN)" "$(DIST_DIR)/$(RELEASE_ARTIFACT)/gents"
	chmod 0755 "$(DIST_DIR)/$(RELEASE_ARTIFACT)/gents"
	cp LICENSE "$(DIST_DIR)/$(RELEASE_ARTIFACT)/LICENSE"
	chmod 0644 "$(DIST_DIR)/$(RELEASE_ARTIFACT)/LICENSE"
	tar -C "$(DIST_DIR)" -czf "$(DIST_DIR)/$(RELEASE_ARTIFACT).tar.gz" "$(RELEASE_ARTIFACT)"
	cd "$(DIST_DIR)" && sha256sum "$(RELEASE_ARTIFACT).tar.gz" > "$(RELEASE_ARTIFACT).tar.gz.sha256"
	@rm -rf "$(DIST_DIR)/$(RELEASE_ARTIFACT)"
	@ls -lh "$(DIST_DIR)/$(RELEASE_ARTIFACT).tar.gz"*

.PHONY: fmt fmt-check check-cli-headless proofs
fmt:
	$(CARGO) fmt --all
	$(NPM) --prefix $(DESKTOP_DIR) run format

fmt-check:
	$(CARGO) fmt --all --check
	$(NPM) --prefix $(DESKTOP_DIR) run format:check

check-cli-headless:
	$(CARGO) check -p gents-cli --no-default-features

proofs:
	cd $(PROOFS_DIR) && $(LAKE) build

.PHONY: test test-agent test-agent-conformance test-agent-e2e test-cli
test: test-agent test-cli

test-agent:
	$(CARGO) test -p gents

test-agent-conformance:
	$(CARGO) test -p gents --test conformance

test-agent-e2e:
	$(CARGO) test -p gents --test e2e_lifecycle
	$(CARGO) test -p gents --test e2e_runtime
	$(CARGO) test -p gents --test e2e_subagent
	$(CARGO) test -p gents --test e2e_triggers

test-cli:
	$(CARGO) test -p gents-cli -- --nocapture --test-threads=1

.PHONY: desktop-ui desktop-ui-qa-sweep desktop-ui-unit desktop-ui-e2e desktop-ui-invariants desktop-ui-screenshots desktop-ui-fuzz desktop-ui-fuzz-long desktop-ui-agent desktop-ui-visual desktop-ui-live-e2e desktop-ui-live-e2e-real desktop-native-preflight desktop-native-dev desktop-native-build
desktop-ui:
	$(NPM) --prefix $(DESKTOP_DIR) run test:ui

desktop-ui-qa-sweep:
	$(NPM) --prefix $(DESKTOP_DIR) run test:ui:qa-sweep

desktop-ui-unit:
	$(NPM) --prefix $(DESKTOP_DIR) run test:ui:unit

desktop-ui-e2e:
	$(NPM) --prefix $(DESKTOP_DIR) run test:ui:e2e

desktop-ui-invariants:
	$(NPM) --prefix $(DESKTOP_DIR) run test:ui:invariants

desktop-ui-screenshots:
	$(NPM) --prefix $(DESKTOP_DIR) run test:ui:screenshots

desktop-ui-fuzz:
	$(NPM) --prefix $(DESKTOP_DIR) run test:ui:fuzz -- --time-limit $(FUZZ_TIME)

desktop-ui-fuzz-long:
	$(NPM) --prefix $(DESKTOP_DIR) run test:ui:fuzz:long

desktop-ui-agent:
	$(NPM) --prefix $(DESKTOP_DIR) run test:ui:agent

desktop-ui-visual:
	$(NPM) --prefix $(DESKTOP_DIR) run test:ui:visual

desktop-ui-live-e2e:
	$(NPM) --prefix $(DESKTOP_DIR) run test:ui:live:e2e

desktop-ui-live-e2e-real:
	$(NPM) --prefix $(DESKTOP_DIR) run test:ui:live:e2e:real

desktop-native-preflight:
	$(NPM) --prefix $(DESKTOP_DIR) run test:ui:native:preflight

desktop-native-dev:
	$(NPM) --prefix $(DESKTOP_DIR) run tauri -- dev

desktop-native-build:
	$(NPM) --prefix $(DESKTOP_DIR) run tauri -- build

.PHONY: live-cli live-agent live-desktop-smoke
live-cli:
	$(CARGO) test -p gents-cli --features live-e2e --test cli_live_suite cli_live::standard_onboarding_live_demo_runs_real_conversation_with_filesystem_tools -- --ignored --nocapture --test-threads=1

live-agent:
	$(CARGO) test -p gents --features live-e2e --test e2e_live -- --ignored --nocapture --test-threads=1

live-desktop-smoke:
	$(NPM) --prefix $(DESKTOP_DIR) run test:live:chat
	$(NPM) --prefix $(DESKTOP_DIR) run test:live:config
	$(NPM) --prefix $(DESKTOP_DIR) run test:live:operations
	$(NPM) --prefix $(DESKTOP_DIR) run test:live:interrupt
