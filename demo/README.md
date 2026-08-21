# Experiment packs

Each subdirectory is a **self-contained pack**: pack-scoped `schemas/`, desired
state (including `DatastoreToolSurface`), and a README.

Use a **dedicated home** for experiments. The pack rebinds to whatever agent
the home owns, so applying it to a working home mixes experiment config into
that agent.

```bash
# Run a pack end to end: init, apply, seed, await, report. Exit code is the result.
gents demo run pipeline

gents demo list                      # what packs exist
gents demo run pipeline --prompt "…" # override the seed prompt
gents demo run pipeline --keep-home  # keep the node home for debugging

# Against a node that is already serving (make review-serve, or demo run --keep-home):
gents demo init pipeline --home /tmp/pack-home
gents demo seed pipeline --http-port 19191 --home /tmp/pack-home
```

Everything a run produces lands under `<pack>/runs/<job_id>/` (gitignored):
`meta.json` with stage request ids, lifecycle states, collection counts and
token totals, signed request-version provenance, plus timeline and adapter
projection artifacts under `projections/`. A pack may declare
`expect.result_documents`; the runner exports those correlated rows to
`results.json` and records that artifact's path in `meta.json`. The runner fails when a configured
projection or result query cannot be built or its contract is invalid. `--keep-home` also
leaves the node home there, so a failed run can be re-opened and queried:

```bash
gents server --home demo/pipeline/runs/<job_id>/home --http-port 19393
```

Packs that exercise asynchronous delegation may add
`expect.background_completion` minimums and maxima. The runner then waits for
completed depth-positive child requests and canonical wake requests, reads the
same durable diagnostics exposed by `gents status`, requires the configured
acknowledged/pending/stranded notification counts, and exports each completed
wake timeline and adapter projection alongside the seed stage.

Each run uses a **fresh home** by default. Triggers are created/first-seen, so
a reused home can silently skip a stage whose source rows already existed.

`gents demo` with no subcommand is still the interactive shell — same node
lifecycle and pairing, with a human at the wheel.

To drive a pack by hand instead (what the runner automates):

```bash
gents server --home <home> --apply-root demo/<pack>
# wait for: event source now observing source collection source_collection=…
# then POST one create_<SeedCollection> mutation, and poll AgentRequest by
# caused_by_trigger_id

gents config apply --root demo/<pack> --home <home> \
  --bind-agent-did home --force-rebind-concrete-did
```

> **`--apply-prune` deletes config the pack does not declare.** It makes the
> pack the *complete* desired state for the home's agent, so that agent's
> other behaviors, tool selections, skills, surfaces, and their reachable
> tasks/schedules/triggers are removed. Only pass it on a home dedicated to
> the pack; never on a home you use for anything else.

## Retargeting a pack

Document JSON supports `${VAR}` and `${VAR:-default}`. The checked-in packs
default to the shared DeepSeek box, so they run as authored, and any run can be
retargeted without editing tracked config — which is how one pack is compared
across models:

```bash
GENTS_EXP_MODEL=some-other-model gents config apply --root demo/<pack> ...
GENTS_EXP_ENDPOINT=http://127.0.0.1:8000/v1 gents server --apply-root demo/<pack> ...
```

A `${VAR}` with no default and no value set is an error naming the variable —
it never silently becomes an empty string. Use `$$` for a literal `$`.
Interpolation applies to document JSON only; `.md` sidecars keep their runtime
`{{ }}` templates untouched.

When `<pack>/schemas/` exists, **apply registers those SDL/patches first**,
then agent config (surfaces → selections → behaviors → tasks/triggers). Packs
do not touch product baseline schemas.

## Operating a node vs embedding one

Packs show how to **operate** a node: config documents applied to a running
runtime, driven through the CLI. If you instead want to **embed** the runtime
in your own binary — build an `EmbeddedNode`, create the identity and config
documents programmatically, and drive `Gents::run` yourself — see
[`crates/gents/examples/serve_default_behavior.rs`](../crates/gents/examples/serve_default_behavior.rs).
It is the library-level counterpart to this directory, and it doubles as a
construction fence: `cargo check --workspace --all-targets` compiles it, so a
new required field on a public config struct breaks there first.

## Packs

| Pack | What it shows |
| --- | --- |
| [`pipeline/`](pipeline/README.md) | **Canonical example** — job → finding create via surface → stage-2 |
| [`code-review/`](code-review/README.md) | **Advanced graph example** — live-inference fan-out/fan-in with correlation propagation, source-field fills, exact cardinality, and one durable triage request |
| [`repo-maintenance/`](repo-maintenance/README.md) | Whole-repository cleanup round that verifies findings, executes focused 1-3 item commits, reviews the assembled change, and opens one PR |
| [`background-continuation/`](background-continuation/README.md) | Two background subagents → durable notification → acknowledged parent wake |
| [`security-scan/`](security-scan/README.md) | **Whole-codebase scan** — free regex pre-scan at kickoff, planner batch fan-out, adversarial revalidation barrier, bound query tools instead of `defra_query` |
| [`defending-code/`](defending-code/README.md) | Threat model → scoped discovery → per-candidate verification → root-cause clustering → contract review → patch validation → maintainer review + adversarial re-attack → report |
| [`lsp-rust/`](lsp-rust/README.md) | Native `lsp` + rust-analyzer on this Gents tree (not a CI gate) |

## Model

| Concept | Mapping |
| --- | --- |
| Node | Task + behavior |
| Edge | EventTrigger `event_kind: created` only |
| Create tools | `DatastoreToolSurface` linked from `ToolSelection` |
| Kickoff | One GraphQL create of the pack’s seed collection |
