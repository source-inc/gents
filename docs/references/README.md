# References — process standards behind the agentic SDLC pack

Source material for
[`docs/superpowers/specs/2026-08-20-agentic-sdlc-pack-design.md`](../superpowers/specs/2026-08-20-agentic-sdlc-pack-design.md).

The documents themselves are **not vendored**. This repository is public and the
standards below are copyright-protected — ISO/IEC/IEEE 12207 carries an explicit
"no part of this publication may be reproduced" notice, and ECSS grants no
redistribution licence. Get them from the sources named here; this file records
exactly which parts the design depends on so you can navigate straight to them.

## ECSS-E-ST-40C Rev.1 — Space engineering: Software

**30 April 2025. © European Space Agency for the members of ECSS.**
Free download: <https://ecss.nl/> (Standards → Engineering → E-40).

The primary source. The pack compiles clause 5 into agent nodes.

| Location | Why it matters |
| --- | --- |
| §5.2.2 | System requirements allocated to software — origin of the **requirements baseline (RB)**, the customer's artefact |
| §5.3.3.1a | Joint review exit criteria, six clauses. Lifted near-verbatim as the review behavior system prompt; clause 5 ("outputs are in such a status that the next activity can start") is the trigger edge condition |
| §5.4.2.1 | Requirements analysis. Ten expected outputs, each becoming an individually-addressable `TechnicalSpecification` document |
| §5.4.3.1 | Architectural design — seven-part decomposition, output `[DDF, SDD; PDR]` |
| §5.5.2, §5.5.3 | Design of software items; coding and unit testing |
| §5.6.3 vs §5.6.4 | **The validation split.** Validation against the TS `[DJF, SVS; CDR]` versus against the RB `[DJF, SVS; QR, AR]`. Load-bearing: CI only ever does the first |
| §5.8.3.1, §5.8.3.2 | Verification of the RB (feeds SRR) and of the TS |
| **Figure 4-2** | Review/artefact map. Gives each review its input set — the context loadout per review behavior, read off the table rather than invented |
| **Annex R, Table R-1** | Normative tailoring by software criticality (A–D). Keyed by stable requirement id, one row per expected output, `Y`/`N`/`Ytba`. Becomes the `TailoringRule` collection; the issue label selects the subgraph |

### Reading the clause notation

Every expected output is tagged `[file, DRD; review]`:

```
EXPECTED OUTPUT: Software architectural design [DDF, SDD; PDR].
```

— produced into the **DDF** file, per the **SDD** document requirements definition,
due at **PDR**. Output collection and gating review are given by the standard, which
is why the schema mapping is mechanical rather than interpretive.

Criticality categories A–D are defined in **ECSS-Q-ST-80 Annex D.1** (a separate
document, also free from ecss.nl).

## ISO/IEC/IEEE 12207:2017 — Software life cycle processes

**© ISO/IEC 2017, © IEEE 2017. Copyright protected — do not vendor.**
Purchase: <https://www.iso.org/standard/63712.html>. A free preview containing the
table of contents is available from the same page.

Used as the cross-check that ECSS's structure is not space-specific. Its clause 6.4
technical processes line up one-to-one with the pack's node list:

| 12207 | Pack node |
| --- | --- |
| §6.4.2 Stakeholder Needs and Requirements Definition | `sdlc-needs` → RB |
| §6.4.3 System/Software Requirements Definition | `sdlc-requirements` → TS |
| §6.4.4 Architecture Definition | `sdlc-architecture` |
| §6.4.5 Design Definition | `sdlc-item-design` |
| §6.4.7 Implementation | `sdlc-code` |
| §6.4.9 Verification | `sdlc-verification` |
| §6.4.11 Validation | `sdlc-validation` |

§6.3.5 **Configuration Management** is the other load-bearing reference: it is the
basis for treating a human override as a superseding revision rather than an
in-place edit, which is what lets the pack run on `created`-only triggers.

Clauses 6.1 (Agreement) and 6.2 (Organizational Project-Enabling) are explicitly
out of scope.

## "Something old, something new"

Internal position paper — *can established software-engineering process serve as a
template for agentic development loops?* Not vendored here; ask Jack for a copy.

Supplies the design commitment the pack implements:

- Heavyweight process lost to agile on **overhead cost**, not correctness. Agents
  are cheap at exactly that overhead, so the trade-off flips.
- **Humans ideate and validate; agents design and implement.** The V re-read as a
  value gradient — human judgement worth most at the two top corners, least in the
  trough.
- Everything between should be agent-run but **reclaimable**: a human can reach into
  any node and override it, and the machinery re-derives whatever that change
  invalidated downstream.
- "We are not designing a methodology, we are compiling an existing one into agents."
