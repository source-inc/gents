# Experiment packs

Each subdirectory is a **self-contained pack**: pack-scoped `schemas/`, desired
state (including `DatastoreToolSurface`), and a README.

```bash
# Preferred: server applies the pack after ready (in-process schemas + config)
gents server --home <home> --apply-root experiments/<pack> --apply-prune

# Or apply against a running server / home
gents config apply --root experiments/<pack> --home <home> \
  --bind-agent-did home --force-rebind-concrete-did
```

When `<pack>/schemas/` exists, **apply registers those SDL/patches first**,
then agent config (surfaces → selections → behaviors → tasks/triggers). Packs
do not touch product baseline schemas.

## Packs

| Pack | What it shows |
| --- | --- |
| [`pipeline/`](pipeline/README.md) | **Canonical example** — job → finding create via surface → stage-2 |

## Model

| Concept | Mapping |
| --- | --- |
| Node | Task + behavior |
| Edge | EventTrigger `event_kind: created` only |
| Create tools | `DatastoreToolSurface` linked from `ToolSelection` |
| Kickoff | One GraphQL create of the pack’s seed collection |
