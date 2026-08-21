SHELL := /bin/sh

CARGO ?= cargo
LAKE ?= lake
NPM ?= npm

DESKTOP_DIR := apps/gents-desktop
PROOFS_DIR := crates/gents/proofs
FUZZ_TIME ?= 30s
REVIEW_ROOT ?= $(CURDIR)
REVIEW_BASE ?= origin/main
REVIEW_HEAD ?= HEAD
REVIEW_PROMPT ?= Review the PR diff for merge-blocking correctness, durability, authorization, concurrency, and provider-boundary defects.
REVIEW_LENSES ?= auto
REVIEW_MIN_LENSES ?= 4
REVIEW_MAX_LENSES ?= 12
REVIEW_PR ?= auto
REVIEW_PORT ?= 19191
REVIEW_PAGE_PORT ?= 19190
REVIEW_HOME ?= $(CURDIR)/demo/code-review/runs/demo-home
REVIEW_PACK ?= $(CURDIR)/demo/code-review
REVIEW_JOB_ID ?=
REVIEW_KEEP_HOME ?=
REVIEW_RESET ?=
REVIEW_CONTEXT_WINDOW ?= 262144
REVIEW_MAX_OUTPUT_TOKENS ?= 65536
REVIEW_MAX_TURNS ?= 1000000
REVIEW_TEMPERATURE ?= 1.0
REVIEW_TOP_P ?= 0.95
REVIEW_COMPACTION_THRESHOLD ?= 0.85
REVIEW_DEADLINE_SECS ?= 86400
REVIEW_AWAIT_TIMEOUT_SECS ?= 86400
REVIEW_STREAM_LIVENESS_SECS ?= 86400
REVIEW_STREAM_BATCH_MS ?= 5000
REVIEW_RETRY_MAX_TRANSPORT ?= 720
REVIEW_RETRY_MAX_RESAMPLE ?= 32
MAINTENANCE_ROOT ?= $(CURDIR)
MAINTENANCE_WORKTREE_PARENT ?= $(abspath $(MAINTENANCE_ROOT)/..)
MAINTENANCE_WORKTREE_PATH ?=
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
MAINTENANCE_STREAM_LIVENESS_SECS ?= 86400
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
DEFENDING_STREAM_LIVENESS_SECS ?= 86400
DEFENDING_STREAM_BATCH_MS ?= 5000
DEFENDING_RETRY_MAX_TRANSPORT ?= 720
DEFENDING_RETRY_MAX_RESAMPLE ?= 32

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
	@echo "Review:"
	@echo "  make review-page           Open the talk page on :$(REVIEW_PAGE_PORT)"
	@echo "  make review-serve          Start a durable pack node on :$(REVIEW_PORT)"
	@echo "    REVIEW_RESET=1           Wipe REVIEW_HOME before init"
	@echo "  make review                Seed a ReviewJob against the running pack node"
	@echo "    REVIEW_PROMPT='...'       Override the review focus"
	@echo "    REVIEW_LENSES=auto        Let recon choose the review-lens count"
	@echo "    REVIEW_MIN_LENSES=4       Set the automatic lower bound"
	@echo "    REVIEW_MAX_LENSES=12      Set the automatic upper bound"
	@echo "    REVIEW_PR=auto            Discover the current branch's GitHub PR"
	@echo "    REVIEW_CONTEXT_WINDOW=N   Match the serving endpoint's context window"
	@echo "    REVIEW_MAX_OUTPUT_TOKENS=N Reserve output tokens per model turn"
	@echo "    REVIEW_TEMPERATURE=N      Set the provider sampling temperature"
	@echo "    REVIEW_TOP_P=N            Set the provider nucleus-sampling threshold"
	@echo "    REVIEW_STREAM_BATCH_MS=N  Batch live token persistence writes"
	@echo
	@echo "Maintenance:"
	@echo "  make maintain              Audit MAINTENANCE_ROOT and emit small cleanup work packages"
	@echo "    MAINTENANCE_PROMPT='...'  Override the maintenance focus"
	@echo "    MAINTENANCE_AREAS=auto    Let recon choose the area count"
	@echo "    MAINTENANCE_MIN_AREAS=5   Set the automatic lower bound"
	@echo "    MAINTENANCE_MAX_AREAS=10  Set the automatic upper bound"
	@echo "    MAINTENANCE_HISTORY_DEPTH=250  Set first-parent history depth"
	@echo "    MAINTENANCE_PR_BASE=main  Set the base branch for the final PR"
	@echo "    MAINTENANCE_WORKTREE_PARENT=DIR  Root newly executed sibling worktrees here"
	@echo "    MAINTENANCE_WORKTREE_PATH=DIR  Override the exact new sibling worktree path"
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

export GENTS_REVIEW_ROOT := $(abspath $(REVIEW_ROOT))
export GENTS_REVIEW_BASE_REF := $(REVIEW_BASE)
export GENTS_REVIEW_HEAD_REF := $(REVIEW_HEAD)
export GENTS_REVIEW_PROMPT := $(REVIEW_PROMPT)
export GENTS_REVIEW_LENS_COUNT := $(REVIEW_LENSES)
export GENTS_REVIEW_MIN_LENSES := $(REVIEW_MIN_LENSES)
export GENTS_REVIEW_MAX_LENSES := $(REVIEW_MAX_LENSES)
export GENTS_REVIEW_CONTEXT_WINDOW := $(REVIEW_CONTEXT_WINDOW)
export GENTS_REVIEW_MAX_OUTPUT_TOKENS := $(REVIEW_MAX_OUTPUT_TOKENS)
export GENTS_REVIEW_MAX_TURNS := $(REVIEW_MAX_TURNS)
export GENTS_REVIEW_TEMPERATURE := $(REVIEW_TEMPERATURE)
export GENTS_REVIEW_TOP_P := $(REVIEW_TOP_P)
export GENTS_REVIEW_COMPACTION_THRESHOLD := $(REVIEW_COMPACTION_THRESHOLD)
export GENTS_REVIEW_DEADLINE_SECS := $(REVIEW_DEADLINE_SECS)
export GENTS_REVIEW_AWAIT_TIMEOUT_SECS := $(REVIEW_AWAIT_TIMEOUT_SECS)
export GENTS_REVIEW_STREAM_LIVENESS_SECS := $(REVIEW_STREAM_LIVENESS_SECS)
export GENTS_REVIEW_STREAM_BATCH_MS := $(REVIEW_STREAM_BATCH_MS)
export GENTS_REVIEW_RETRY_MAX_TRANSPORT := $(REVIEW_RETRY_MAX_TRANSPORT)
export GENTS_REVIEW_RETRY_MAX_RESAMPLE := $(REVIEW_RETRY_MAX_RESAMPLE)

.PHONY: review-page
review-page:
	@echo "page     http://127.0.0.1:$(REVIEW_PAGE_PORT)"
	@echo "runtime  http://127.0.0.1:$(REVIEW_PORT)  (start with make review-serve)"
	@REVIEW_PORT="$(REVIEW_PORT)" REVIEW_PAGE_PORT="$(REVIEW_PAGE_PORT)" $(NPM) --prefix apps/review-demo run dev

.PHONY: review-serve
review-serve:
	@test -d "$(REVIEW_ROOT)" || { echo "REVIEW_ROOT is not a directory: $(REVIEW_ROOT)" >&2; exit 2; }
	@if test -n "$(REVIEW_RESET)"; then rm -rf "$(REVIEW_HOME)"; fi
	@mkdir -p "$(REVIEW_HOME)"
	@if ! test -f "$(REVIEW_HOME)/review-root"; then printf '%s\n' "$(abspath $(REVIEW_ROOT))" > "$(REVIEW_HOME)/review-root"; fi
	@if ! test -f "$(REVIEW_HOME)/init.json"; then \
		echo "init     $(REVIEW_HOME)"; \
		$(CARGO) run -p gents-cli -- demo init "$(REVIEW_PACK)" --home "$(REVIEW_HOME)"; \
	fi
	@echo "page     http://127.0.0.1:$(REVIEW_PAGE_PORT)"
	@echo "graphql  http://127.0.0.1:$(REVIEW_PORT)/api/v0/graphql"
	@echo "home     $(REVIEW_HOME)"
	@$(CARGO) run -p gents-cli -- server \
		--home "$(REVIEW_HOME)" \
		--http-port "$(REVIEW_PORT)" \
		--apply-root "$(REVIEW_PACK)" \
		--p2p-transport none \
		--no-codex-shim

.PHONY: review
review:
	@test -d "$(REVIEW_ROOT)" || { echo "REVIEW_ROOT is not a directory: $(REVIEW_ROOT)" >&2; exit 2; }
	@case "$(REVIEW_LENSES)" in auto) ;; ''|*[!0-9]*) echo "REVIEW_LENSES must be auto or a positive integer: $(REVIEW_LENSES)" >&2; exit 2;; *) test "$(REVIEW_LENSES)" -gt 0 || { echo "REVIEW_LENSES must be greater than zero" >&2; exit 2; };; esac
	@case "$(REVIEW_MIN_LENSES)" in ''|*[!0-9]*) echo "REVIEW_MIN_LENSES must be a positive integer: $(REVIEW_MIN_LENSES)" >&2; exit 2;; esac
	@case "$(REVIEW_MAX_LENSES)" in ''|*[!0-9]*) echo "REVIEW_MAX_LENSES must be a positive integer: $(REVIEW_MAX_LENSES)" >&2; exit 2;; esac
	@test "$(REVIEW_MIN_LENSES)" -gt 0 && test "$(REVIEW_MAX_LENSES)" -ge "$(REVIEW_MIN_LENSES)" || { echo "review lens bounds must satisfy 0 < REVIEW_MIN_LENSES <= REVIEW_MAX_LENSES" >&2; exit 2; }
	@cd "$(REVIEW_ROOT)" && git rev-parse --verify "$(REVIEW_BASE)^{commit}" >/dev/null || { echo "REVIEW_BASE is not a commit: $(REVIEW_BASE)" >&2; exit 2; }
	@cd "$(REVIEW_ROOT)" && git rev-parse --verify "$(REVIEW_HEAD)^{commit}" >/dev/null || { echo "REVIEW_HEAD is not a commit: $(REVIEW_HEAD)" >&2; exit 2; }
	@if test -n "$(REVIEW_PR)" && test "$(REVIEW_PR)" != auto; then command -v gh >/dev/null 2>&1 || { echo "REVIEW_PR requires gh on PATH" >&2; exit 2; }; cd "$(REVIEW_ROOT)" && gh pr view "$(REVIEW_PR)" --json number >/dev/null || exit 2; fi
	@command -v rust-analyzer >/dev/null 2>&1 || echo "warning: rust-analyzer not found on PATH; review will fall back to file/search tools" >&2
	@review_pr="$(REVIEW_PR)"; \
	if test "$$review_pr" = auto; then \
		if command -v gh >/dev/null 2>&1; then review_pr=$$(cd "$(REVIEW_ROOT)" && gh pr view --json number --jq .number 2>/dev/null || true); else review_pr=; fi; \
	fi; \
	if test -n "$$review_pr"; then echo "reviewing GitHub PR $$review_pr"; else echo "no GitHub PR detected; reviewing the local ref diff"; fi; \
	GENTS_REVIEW_PR_NUMBER="$$review_pr" \
	$(CARGO) run -p gents-cli -- demo seed "$(REVIEW_PACK)" \
		--http-port "$(REVIEW_PORT)" \
		--home "$(REVIEW_HOME)" \
		--page-port "$(REVIEW_PAGE_PORT)" \
		--prompt "$(REVIEW_PROMPT)" \
		$(if $(REVIEW_JOB_ID),--job-id "$(REVIEW_JOB_ID)",)

.PHONY: maintain
maintain:
	@test -d "$(MAINTENANCE_ROOT)" || { echo "MAINTENANCE_ROOT is not a directory: $(MAINTENANCE_ROOT)" >&2; exit 2; }
	@test -d "$(MAINTENANCE_WORKTREE_PARENT)" || { echo "MAINTENANCE_WORKTREE_PARENT is not a directory: $(MAINTENANCE_WORKTREE_PARENT)" >&2; exit 2; }
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
	maintenance_worktree_path="$(MAINTENANCE_WORKTREE_PATH)"; \
	if test -z "$$maintenance_worktree_path"; then maintenance_worktree_path="$(abspath $(MAINTENANCE_WORKTREE_PARENT))/gents-$$maintenance_job_id"; fi; \
	maintenance_branch="$(MAINTENANCE_BRANCH)"; \
	if test -z "$$maintenance_branch"; then maintenance_branch="agent/$$maintenance_job_id"; fi; \
	test "$$(dirname "$$maintenance_worktree_path")" = "$(abspath $(MAINTENANCE_WORKTREE_PARENT))" || { echo "MAINTENANCE_WORKTREE_PATH must be a direct child of MAINTENANCE_WORKTREE_PARENT: $$maintenance_worktree_path" >&2; exit 2; }; \
	GENTS_MAINTENANCE_ROOT="$(abspath $(MAINTENANCE_ROOT))" \
	GENTS_MAINTENANCE_WORKTREE_PARENT="$(abspath $(MAINTENANCE_WORKTREE_PARENT))" \
	GENTS_MAINTENANCE_WORKTREE_PATH="$$maintenance_worktree_path" \
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
