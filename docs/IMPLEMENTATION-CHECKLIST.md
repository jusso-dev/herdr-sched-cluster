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
| `Snapshot` type and JSON shape (`internal/cluster/resource`) | done |
| Linux collector: `MemAvailable`, PSI memory and IO pressure, swap and swap activity, CPU use from `/proc/stat`, load, physical cores, uptime, disk, power supply, thermal zones, `nvidia-smi` when present | done, tested on real Linux (Debian 13, x86-64) |
| macOS collector: `sysctl` memory, page counts, `kern.memorystatus_vm_pressure_level`, swap, load, cores, perf cores, uptime; `ps` for CPU and per-process use; `pmset` for battery and thermal | partial: compiles and the parsers are unit tested; the live collector runs on the macOS CI runner. **Untested on Justin's M3 Max and M4 hardware.** CPU use on macOS is the sum of `ps` %CPU, which is a decaying average, not an instantaneous reading. |
| Windows | Explicit static backend: capacity declared in config. No live probe. |
| Per-PID sampling, children folded in (RSS, CPU) | done (Linux from `/proc`, macOS from `ps`) |
| Agent runtime discovery (Herdr's executable names, `PATH` plus common install directories) | done. Advisory only: a non-interactive SSH `PATH` can differ from the shell Herdr starts agents in. |
| `hsched cluster probe` (no daemon) | done |
| SSH transport: fixed argv, allow-listed and quoted words, `BatchMode=yes`, bounded output, schema check | done, tested with a stand-in `ssh` |

## Phase 3: placement core

| Item | Status |
| --- | --- |
| Hard constraints with readable reasons (`internal/cluster/placement`): state, pinning, OS, arch, labels, declared agents, repository missing, memory after margin, critical pressure, CPU commit, sustained load, agent cap, disk, battery, thermal, starvation hold | done |
| Scoring factors and six policy presets (`balanced`, `memory`, `performance`, `pack`, `spread`, `local-first`) | done |
| Explanation: per-worker reasons, warnings, factors, score and a one-line summary | done |
| Property test: a hard constraint is never overridden by a score (5,000 random clusters) | done |
| Property test: placement is deterministic under reordering | done |
| Property test: scores within 0..100, factors within 0..1 | done |
| Starvation helper (`Nearest`) and submit-time feasibility (`FitsAnywhereEver`) | done |

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
