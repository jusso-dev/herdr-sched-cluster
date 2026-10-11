# Implementation checklist

Status key: **done** (built and tested), **partial** (built, with a named gap),
**not started**. "Untested on hardware" means it compiles and has unit tests,
but has not run on a real machine of that kind.

This file is updated in the same commit as the work it describes.

## Phase 1: audit and design

| Item | Status |
| --- | --- |
| Clone and read herdr-sched | done |
| Read Herdr source and versioned docs (CLI, socket API, plugins, machines) | done |
| Verify every Herdr API we depend on in source | done ([GAP-ANALYSIS.md](GAP-ANALYSIS.md) section 3) |
| Check other Herdr queue and automation plugins | done |
| Licence, maintenance and compatibility check | done (MIT, see gap analysis section 1) |
| `docs/ARCHITECTURE.md`, `docs/GAP-ANALYSIS.md`, this checklist | done |

## Phase 2: resource collection

| Item | Status |
| --- | --- |
| `Snapshot` type and JSON shape | not started |
| Linux collector (memory, PSI pressure, swap, CPU, load, disk, uptime, power, thermal) | not started |
| macOS collector behind build tags | not started |
| Windows static-capacity backend | not started |
| Per-PID sampling (RSS, CPU) | not started |
| Agent runtime discovery (which agent CLIs are on `PATH`) | not started |
| `hsched cluster probe --json` | not started |

## Phase 3: placement core

| Item | Status |
| --- | --- |
| Hard constraints with reasons | not started |
| Scoring factors and policy weight sets | not started |
| Explanation output | not started |
| Property tests: hard constraints never overridden by score | not started |

## Phase 4: ledger and queue

| Item | Status |
| --- | --- |
| SQLite ledger, `0600`, schema migrations | not started |
| Atomic reserve with guarded state transitions | not started |
| Queue ordering, priority ageing, backfill, starvation guard | not started |
| Cancellation | not started |
| Property tests: no reservation consumed twice, no double dispatch | not started |

## Phase 5: multi-machine dispatch

| Item | Status |
| --- | --- |
| Worker discovery from `herdr machine list --json` plus Local | not started |
| Health from `herdr machine status --json` | not started |
| SSH probe transport with fixed argv and quoting | not started |
| `herdr --machine` adapter (workspace, worktree, agent start, prompt, list, close) | not started |
| Dispatch pipeline with failure handling and retries | not started |
| Reconciliation, restart recovery, disconnect handling | not started |
| Draining | not started |
| Simulated cluster harness | not started |
| Section 14 scenarios 1 to 10 in the harness | not started |

## Phase 6: learning from observed usage

| Item | Status |
| --- | --- |
| Per-agent sampling through `pane process-info` and the probe | not started |
| Overrun handling (raise reservation, event, never kill) | not started |
| History-based default estimates per agent kind and repository | not started |

## Phase 7: polish and integration

| Item | Status |
| --- | --- |
| Cluster verbs on both doors (CLI and MCP) through the registry | not started |
| Config reference for the `cluster_*` keys | not started |
| Prometheus metrics | not started |
| Dashboard popup | not started |
| README for people: install, register machines, config, CLI, limitations | not started |
| Install and upgrade steps | not started |
| CI on Linux and macOS | not started |
