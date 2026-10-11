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
| SQLite ledger, `0600`, schema migrations | done (`internal/cluster/ledger`, pure Go `modernc.org/sqlite`, WAL; refuses a file other users can read; schema version 1) |
| Atomic reserve with guarded state transitions | done (capacity check runs inside an `IMMEDIATE` transaction; every later transition needs the reservation token) |
| Queue ordering, priority ageing, backfill, starvation guard | done (`internal/cluster/queue`) |
| Cancellation | done for queued and reserved jobs; running jobs are refused, since we never kill agents |
| Property tests: no reservation consumed twice | done (16 goroutines over two handles on one file, five seeded rounds, under `-race`) |
| Property test: a queue pass never overcommits or assigns a job twice | done |
| Property test: no double dispatch end to end | see phase 5 (needs the dispatcher) |

## Phase 5: multi-machine dispatch

| Item | Status |
| --- | --- |
| Worker discovery from `herdr machine list --json` plus Local | done (`internal/cluster/coord`; disabled machines shown as disabled) |
| Health from `herdr machine status --json` | partial: the adapter reads it, but the coordinator classifies health from its own SSH probe (unreachable, auth required, incompatible) with exponential backoff, which is one SSH round trip instead of two |
| SSH probe transport with fixed argv and quoting | done (phase 2) |
| `herdr --machine` adapter (workspace, worktree, agent start, prompt, list, close, process-info) | done (`internal/cluster/herdrx`, tested against a fake `herdr` that records argv) |
| Dispatch pipeline with failure handling and retries | done: failures before an agent exists requeue up to `max_attempts`; an agent start whose answer was lost is checked with `agent list` before anything is retried |
| At-most-once dispatch when the outcome is unknown | done: a dispatch interrupted after the agent start, with no agent found later, is failed (not retried) for a person to resubmit |
| Reconciliation, restart recovery, disconnect handling | done: running jobs whose agent is gone are finished; reserved or dispatching jobs are adopted if their agent exists, otherwise decided after `cluster_dispatch_grace`; an unreachable worker marks its jobs disconnected and keeps their reservations |
| Draining | done (persisted in the ledger; running agents are left alone) |
| Simulated cluster harness | done (`internal/cluster/sim`: in-memory Herdr per machine, simulated prober, failure injection, test clock) |
| Section 14 scenarios 1 to 10 in the harness | done, one test each (`TestScenario01` to `TestScenario10`) |
| Property test: no double dispatch | done (30 seeded runs of 120 random steps with injected failures, lost replies, unreachable workers, restarts and clock jumps; mutation-checked: removing either guard makes it fail) |
| Lost versus finished | gap: an agent missing from `agent list` is recorded as finished. A worker Herdr restart is not told apart from a normal exit |

## Phase 6: learning from observed usage

| Item | Status |
| --- | --- |
| Per-agent sampling through `pane process-info` and the probe | done: shell PID from `pane process-info`, process tree RSS and CPU from the worker's probe. Tree sampling is tested on real Linux; on macOS it uses `ps` and is untested on hardware |
| Overrun handling (raise reservation, event, never kill) | done (reservation raised to observed RSS plus `cluster_overrun_margin_pct`; scenario 9) |
| History-based default estimates per agent kind and repository | done (EWMA of peak RSS per kind and repository, falling back to per kind, then `cluster_default_memory_mb`) |

## Phase 7: polish and integration

| Item | Status |
| --- | --- |
| Cluster verbs on both doors (CLI and MCP) through the registry | done: `cluster status`, `plan`, `queue`, `workers`, `submit`, `jobs`, `explain`, `cancel`, `detach`, `drain`, `undrain`, `events`, `metrics`; mutating ones gated as `sched.cluster.*`; parity tests pin the tool list; a daemon test fails any cluster verb that is declared but not routed |
| Daemon integration | done: own tick (`cluster_tick_seconds`), a pump straight after each submit, ledger at `<state dir>/cluster.db`; a bad cluster config turns only the cluster off, with the reason in every cluster verb's refusal |
| Config reference for the `cluster_*` keys | done: 25 flat keys in `sched.toml` (the upstream parser has no tables) plus `cluster-workers.json` for per-worker settings; unknown fields refused; README test checks every key is documented |
| Prometheus metrics | done: `hsched cluster metrics` and an optional loopback-only `/metrics` (`cluster_metrics_addr`) |
| Dashboard popup | done: the existing popup gains cluster workers and jobs (read-only verbs, checked by the manifest test) |
| README for people: install, register machines, config, CLI, limitations | done |
| Install and upgrade steps | done, run against real Herdr 0.9.3 on Linux x86-64: `herdr plugin install jusso-dev/herdr-sched-cluster` from GitHub, both the source build and the prebuilt release download, the startup hook, the restart action, reinstall as upgrade |
| End-to-end check of the real binary | done on Linux against real Herdr 0.9.3 with one local worker: daemon from the startup hook, `cluster status`, `plan`, `workers`, `submit` of two real OpenCode agents (the second in a Herdr worktree), `jobs`, `queue`, `explain`, a finished agent reconciled, the CLI run from inside a Herdr pane, and the MCP server over stdio. Not run on a real second machine or on macOS |
| CI on Linux and macOS | not enabled: the publishing token lacks GitHub's `workflow` scope. The workflow is in `docs/ci/ci.yml` with the two commands to enable it |
| Upstream tests | pass. Two upstream file-watcher tests assumed a filesystem clock finer than two back-to-back writes; one now moves the mtime on explicitly and the other skips when the clock did not tick |

## Phase 8: installable and agent-usable

| Item | Status |
| --- | --- |
| Install from GitHub with real Herdr's loader | done (Herdr 0.9.3; see above). `herdr plugin root` and `herdr plugin update` do not exist in 0.9.3, so the README no longer uses them |
| Build without Go | done: `scripts/build.sh` compiles with Go when present, otherwise downloads the release asset for the manifest version and checks its SHA-256 |
| Stable `hsched` path across updates | done: `start.sh` copies the binary to the plugin state dir and links `~/.local/bin/hsched`; `cluster_probe_program` now defaults to that path |
| Read-only `cluster plan` (N jobs, no dispatch, per-job outcome and reason) | done, with a test that a plan matches what submit then does |
| `cluster status` and `cluster queue` | done |
| MCP instructions aimed at agents | done |
| Agent skill and `AGENTS.md` snippet | done (`skills/herdr-cluster`) |
| Registration with Claude Code, Codex, OpenCode | done: `setup-agents` action; each agent's `mcp list` checked against the real binary |
| MCP over stdio with a JSON-RPC client | done (`scripts/mcp-smoke.py`) |

## Brief gap check (sections 3.3 to 12)

This check is against the section topics of the brief. The full text of those
sections was not available to this build either (the copy was truncated a
second time), so anything below marked against a topic, not a requirement, is
for Justin to confirm.

| Topic | Status |
| --- | --- |
| Adaptive estimation | partial: memory learned per agent kind and repository (EWMA of observed peak RSS, with headroom), used when `memory_mb` is omitted, in both submit and plan. CPU is not learned; it uses `cluster_default_cpu` or the request |
| Example scenario (ten agents on the MacBook Pro and Mac mini) | done in simulation (scenario 1 and `TestPlanMatchesWhatSubmitThenDoes`); not run on the real Macs |
| Worker architecture | done as a design choice: no worker daemon. The coordinator probes each worker over SSH with `hsched cluster probe` and drives Herdr with `herdr --machine`. A worker only needs the plugin installed |
| Agent lifecycle | partial: queued, reserved, dispatching, running, finished, failed, cancelled, detached. A worker Herdr restart is recorded as finished, not lost |
| Workspace and Git isolation | partial: automatic Herdr worktree when a repository is already busy on that machine, or on request. Fails on a repository with no commits (the job requeues). Worktrees are not cleaned up after the agent finishes |
| Queue: head-of-line blocking | done: backfill places smaller jobs past a blocked head; the starvation guard holds a worker clear for a job that has waited too long |
| Queue: reason output | done: `cluster queue` (new) and `cluster explain` |
| Load balancing | done: six policies over the same hard limits |
| Memory pressure protection | done: critical pressure is a hard limit, elevated pressure and swapping are warnings, overruns raise the reservation. Nothing is killed |
| Herdr integration CLI | partial: Herdr has no way for a plugin to add `herdr` subcommands, so the CLI is `hsched cluster ...`, plus Herdr actions (dashboard, restart, stop, setup-agents) |
| Observability | done: `explain`, `queue`, `events`, `metrics` (Prometheus text, optional loopback endpoint), dashboard popup |
| Security | done: fixed SSH argv with an allow-list, `0600` ledger, prompts never logged, loopback-only metrics, mutating verbs behind the policy gate |
| Configuration | done: `cluster_*` keys in `sched.toml` and `cluster-workers.json`, all documented and validated |

## Known gaps

- macOS collectors and the macOS process sampler have not run on Apple hardware.
- No live Windows collector; Windows workers are static only.
- A worker Herdr restart is recorded as `finished`, not `lost`.
- `herdr machine status` is read by the adapter but not used for placement.
- No per-worker repository path mapping.
- No end-to-end run against real Herdr servers on two machines, or on macOS.
- CPU use is not learned from history.
- Worktrees are not cleaned up, and cannot be made in a repository with no commits.
- Sections 3.3 to 12 of the brief were truncated in every copy this work has seen; the check above is against their topics. See GAP-ANALYSIS section 7.
