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

## Brief audit (sections 3.3 to 12), v0.5.0

Every bullet of [the brief](BRIEF-sections-3.3-12.md) is listed with its status and the file or test that shows it. "Blocked by Herdr" means Herdr 0.9.3 has no supported way to do it; the alternative is named.

Totals: 166 bullets, 154 met, 10 partial, 2 missing.

### 3.3 Adaptive agent resource estimation

| Bullet | Status | Evidence |
| --- | --- | --- |
| Do not assume every agent needs the same memory | met | Per kind and repository profiles: `ledger/runs.go`, `coord/submit.go` (prepare); `TestCPUIsLearnedPerKindAndRepo` |
| Profile for Claude Code | met | Profiles are keyed by Herdr agent kind, `claude` included; `ledger/runs.go` |
| Profile for OpenAI Codex CLI | met | As above, kind `codex` |
| Profile for OpenCode | met | As above, kind `opencode` |
| Profile for other Herdr-supported agents | met | Any kind Herdr starts gets its own profile; `resource/collect.go` finds installed CLIs |
| Profile for custom CLI agents | missing (blocked by Herdr) | `herdr agent start` only accepts Herdr's own `--kind` values, so an arbitrary command cannot be started as an agent. Alternative: run it under a supported kind with `agent_args` |
| Typical idle memory | partial | Each run records `typical_rss_mb` (median of samples) beside the peak, `ledger/runs.go`; idle and working phases are not told apart because Herdr's agent state is not sampled with memory |
| Peak working memory | met | `runs.peak_rss_mb`, used for estimates; `TestPercentileNearestRank` |
| CPU usage | met | Process-tree CPU per sample, learned per kind and repository and enforced against `max_cpu_utilisation_percent`; `TestV05CPULearnedAndEnforced` |
| Resource use during builds and tests | partial | Builds and tests run in the agent's process tree, so their memory and CPU are in the peak; they are not labelled as a separate phase |
| Resource use for large repositories | met | Profiles are per repository first, then per kind; `TestCPUIsLearnedPerKindAndRepo` |
| Concurrent process-tree consumption | met | Whole tree under the pane shell, `resource/collect_linux.go` and `collect_darwin.go` (`sampleTrees`) |
| Configurable default for unknown workloads | met | `cluster_default_memory_mb`, `cluster_default_cpu`; `TestV05Keys` |
| Rolling percentiles | met | Nearest-rank percentile over the last `cluster_history_window` runs at `cluster_history_percentile`; `TestPercentileNearestRank` |
| No LLM in basic scheduling | met | Placement is deterministic code, `placement/` |
| Any AI estimation advisory, bounded, replaceable | met | There is none; every estimate is deterministic and explained (`cluster explain`, `cluster estimates`) |

### 3.4 Example scheduling scenario

| Bullet | Status | Evidence |
| --- | --- | --- |
| Evaluate both machines | met | `TestScenario01TenAgentsCompeteForMemory` |
| Reserve configurable memory for the OS | met | `min_free_memory_mb` and `memory_reserve_percent`; `TestMemoryReservePercent` |
| Account for running and queued reservations | met | `placement.Worker.PendingMemoryMB`, `TestScenario01TenAgentsCompeteForMemory` |
| Allocate by available capacity | met | `TestScenario01TenAgentsCompeteForMemory`, `TestPlanMatchesWhatSubmitThenDoes` |
| Prevent overcommit beyond limits | met | Reservation re-checked in one transaction; `TestScenario10ConcurrentSubmissions`, `TestPropertyNoDoubleDispatch` |
| Queue what cannot be admitted | met | `TestScenario01TenAgentsCompeteForMemory` |
| Dispatch queued agents as capacity frees | met | `TestScenario01TenAgentsCompeteForMemory`, `TestScenario05LargeJobIsNotStarved` |
| Figures illustrative, separate memory pools | met | Nothing is hard-coded; each worker is its own pool. Not yet run on the real MacBook Pro and Mac mini |

### 4. Distributed worker architecture

| Bullet | Status | Evidence |
| --- | --- | --- |
| Worker component only where Herdr is insufficient | met | No worker daemon; `hsched cluster probe` and `hsched cluster gitop` run over SSH on demand |
| Report system metrics | met | `resource/`, `cluster probe` |
| Advertise supported agent runtimes | met | `Snapshot.Agents`, `resource/collect.go` (`findAgents`), static `agents` |
| Accept authenticated dispatches | met | Herdr's saved machines over SSH, `herdrx/`, `transport/` |
| Start sessions through Herdr | met | `herdr workspace create`, `worktree create`, `agent start`, `agent prompt`; `coord/dispatch.go` |
| Track process trees and resource use | met | `coord/upkeep.go` (Observe), `TestScenario09OverrunRaisesReservation` |
| Report session lifecycle events | met | `cluster.reconcile.*` events, `coord/reconcile.go` |
| Graceful termination | partial (blocked by Herdr) | Herdr 0.9.3 has no `agent stop`; terminating closes the workspace (`coord/upkeep.go` Pressure). Alternative: `detach` and stop the agent in Herdr yourself |
| Reconnect after network interruptions | met | `TestScenario03DisconnectKeepsAgentsAndCapacity`, `TestV05UnreachableThenLostThenReadopted` |
| Reuse Herdr's machine connections | met | `herdr --machine`, `herdr machine status`; `TestV05MachineStatusHealth` |
| No separate privileged daemon | met | Design, `docs/ARCHITECTURE.md` |
| No root or administrator | met | Everything runs as the user |
| Coordinator: maintain worker state | met | `coord/coord.go` (targets, machine status, identity) |
| Coordinator: schedule and dispatch | met | `coord/dispatch.go` |
| Coordinator: manage reservations | met | `ledger/` |
| Coordinator: queue workloads | met | `queue/` |
| Coordinator: track active sessions | met | `coord/reconcile.go` |
| Coordinator: detect failed workers | met | Unreachable, `herdr_unreachable`, lost after `cluster_lost_after_seconds`; `TestScenario02WorkerUnavailableDuringDispatch`, `TestV05UnreachableThenConfirmedLost` |
| Coordinator: recover after restart | met | `TestScenario04CoordinatorRestart`, `TestV05CrashMidDispatchIsRetriedSafely` |
| Coordinator: prevent duplicate dispatches | met | Start intents and tokens; `TestPropertyNoDoubleDispatch`, `TestV05CrashAfterStartIntentIsNotRetried` |
| SQLite persistence | met | `ledger/ledger.go`, schema v2; `TestMigrateFromSchema1` |
| Single coordinator, safe restart, explicit ownership | met | Daemon lock, reservation tokens; `TestIntentsNeedTheToken` |
| No consensus or cluster complexity | met | Design |

### 5. Agent lifecycle management

| Bullet | Status | Evidence |
| --- | --- | --- |
| Lifecycle states | met | queued, reserved, dispatching, running, finished, failed, cancelled, detached, lost; worker evaluation is the pass itself and is recorded by `explain`; `ledger/jobs.go` |
| Unique job ids | met | `ledger` (`j<n>`) |
| Idempotent dispatch | met | Workspace label lookup and start intent; `TestV05CrashMidDispatchIsRetriedSafely` |
| Reservation tracking | met | `ledger/` |
| Dispatch timeouts | met | `cluster_dispatch_timeout_seconds`, `cluster_dispatch_grace_seconds` |
| Retry policies | met | `cluster_max_attempts`, `--restartable`, `cluster_retry_lost`; `TestV05RestartableLostJobIsRetried` |
| Cancellation | met | `cluster cancel` |
| Graceful shutdown | met | Stopping the daemon leaves agents running and the ledger consistent; `TestScenario04CoordinatorRestart` |
| Job prioritisation | met | `queue/` |
| Queue visibility | met | `cluster queue`, `cluster status` |
| Recovery after coordinator restart | met | `TestScenario04CoordinatorRestart`, `TestV05CrashAfterStartIntentIsNotRetried` |
| Never launched twice | met | `TestPropertyNoDoubleDispatch` (now with Herdr restarts and closed sessions) |
| Reconcile with real Herdr sessions | met | `coord/reconcile.go`; `TestV05LostWhenHerdrRestarts`, `TestV05FinishedIsNotLost` |
| Reconnect to sessions through Herdr | met | Agents live in Herdr's own server; `cluster attach` |
| Do not kill agents when the UI closes | met | The scheduler never kills an agent it did not mark disposable |
| Reattach to remote sessions | partial (blocked by Herdr) | Herdr refuses attach across `--machine`; `cluster attach` prints `herdr --remote <target>` and `herdr agent focus`. `TestClusterOwnerChecksAndNewVerbs` |
| Preserve agent-native session ids | missing (blocked by Herdr) | Herdr 0.9.3 does not expose an agent's own session id. Alternative: the agent's pane and workspace persist in Herdr |
| No claim of live migration | met | README, Limitations |

### 6. Workspace and Git isolation

| Bullet | Status | Evidence |
| --- | --- | --- |
| Isolated workspace per agent | met | One Herdr workspace per job; `TestScenario06SameRepoUsesWorktrees` |
| Git worktrees | met | `herdr worktree create` |
| Separate branches | met | `hsc/j<n>` (and `-<attempt>`), `coord/dispatch.go` (branchFor) |
| Clone when required | met | `--clone_url`, `gitop/`; `TestV05CloneWhenMissing`, `TestCloneNeverWritesOverAnExistingPath` |
| Reuse existing repositories | met | Default path |
| Workspace cleanup | met | `coord/upkeep.go` (Cleanup); `TestV05WorktreeRetention` |
| Persistent workspaces | met | `always-retain`; `TestV05WorktreeRetention` |
| Shared repository metadata | met | Worktrees share the repository's `.git` |
| Repository-to-worker affinity | met | `repo_affinity` and `repo_locality` scores, `placement/score.go` |
| Worktrees by default for one repo, one machine | met | `TestScenario06SameRepoUsesWorktrees` |
| Explicit clone or sync across machines | met | `--clone_url` and per-worker `path_map`; `TestV05PathMapping` |
| No shared worktree across filesystems | met | Worktrees are made on the worker that runs the job |
| No two agents in one directory | met | ExclusiveRepo rule; `TestRepoRules` |
| Configurable retention | met | `cluster_worktree_retention` and friends; `TestV05WorktreeRetention` |

### 7. Intelligent queue management

| Bullet | Status | Evidence |
| --- | --- | --- |
| Persistent queue | met | `ledger/` |
| Priorities | met | `queue/` |
| FIFO within priority | met | `queue/` tests |
| Resource requirements | met | memory, CPU, labels, OS, arch |
| Dependencies | met | `--depends_on`; `TestV05Dependencies` |
| Per-project concurrency limits | met | `cluster_max_agents_per_repo`; `TestRepoRules` |
| Per-machine concurrency limits | met | `max_agents` |
| Ageing | met | `cluster_ageing_minutes`, starvation guard; `TestScenario05LargeJobIsNotStarved` |
| Cancellation | met | `cluster cancel` |
| Retry limits | met | `cluster_max_attempts` |
| Scheduling history | met | Event trail and `runs`; `cluster events`, `cluster estimates` |
| Re-evaluate waiting jobs as capacity frees | met | Every tick; `TestScenario01TenAgentsCompeteForMemory` |
| No head-of-line blocking | met | Backfill; `TestQueueViewExplainsAndDoesNotBlockOnItsHead` |
| Clear reasons, with next action | met | Per-worker reasons and `next_action` in `cluster queue` and `explain` |

### 8. Automatic load balancing

| Bullet | Status | Evidence |
| --- | --- | --- |
| Continuous health evaluation | met | Tick: reconcile, observe, pressure, cleanup, pump; `coord/upkeep.go` |
| Admission control | met | Hard limits in `placement/` |
| Dynamic placement of queued agents | met | `pump` |
| Detect sustained pressure | met | `TestPressureDuration` |
| Worker draining | met | `TestScenario08Draining` |
| Optional cancel and restart of eligible jobs | met | `--disposable` with `--restartable`; `TestV05PressureTerminatesDisposable` |
| Policy adjustments | met | `adaptive`; `TestAdaptivePolicy` |
| Capacity recommendations | met | `recommendations` in `cluster status`, `coord/plan.go` |
| No live migration | met | Design |
| Relocation only by explicit checkpoint and restart | partial (blocked by Herdr) | No agent checkpoint or resumable session is exposed. Alternative: `detach` and resubmit, or `--restartable` to start afresh elsewhere |
| Never interrupt an agent to balance load | met | Only disposable jobs under sustained critical pressure, and only with `terminate-disposable` |
| Pressure: stop admitting | met | `TestScenario07SuddenMemoryPressure` |
| Pressure: notify the coordinator | met | `cluster.pressure.*` events |
| Pressure: re-evaluate the queue | met | Every tick |
| Pressure: identify top consumers | met | Jobs ranked by observed RSS, `coord/upkeep.go` |
| Pressure: recommend remediation | met | `recommend`, `cluster status` |
| Pressure: pause or terminate disposable jobs | partial | Terminate is done (`TestV05PressureTerminatesDisposable`); pause is blocked, Herdr has no way to suspend an agent |
| Never terminate interactive sessions by default | met | `cluster_pressure_action` defaults to `recommend` |

### 9. Herdr integration

| Bullet | Status | Evidence |
| --- | --- | --- |
| Native extension on verified APIs | met | Herdr plugin manifest; Herdr 0.9.3 verbs checked by hand (see below) |
| Launch with automatic machine selection | met | `cluster submit` |
| Manual placement override | met | `--machine` |
| List running agents across workers | met | `cluster jobs`, `cluster workers` |
| Show the machine per agent | met | `cluster jobs` |
| Show resource utilisation | met | `cluster workers`, `cluster status` (memory, CPU, pressure) |
| Inspect queued jobs | met | `cluster queue` |
| Cancel queued jobs | met | `cluster cancel` |
| Attach to running sessions | partial (blocked by Herdr) | As in section 5: `cluster attach` gives the commands |
| Inspect scheduling decisions | met | `cluster explain` |
| Configure policies | met | `cluster_policy`, `--policy` |
| Plugin UI, else CLI and dashboard | met | Dashboard action and `hsched cluster` |
| `herdr scheduler ...` CLI | partial (blocked by Herdr) | Plugins cannot add `herdr` subcommands; the same verbs are `hsched cluster` with `status`, `workers`, `submit`, `queue`, `explain` and `drain` |

### 10. Cluster observability

| Bullet | Status | Evidence |
| --- | --- | --- |
| Cluster overview | met | `cluster status` |
| Worker health | met | states incl. `herdr_unreachable`, `incompatible` |
| Active agent count | met | `cluster status` |
| Reservations | met | `cluster workers` |
| Actual usage | met | Observed RSS and CPU |
| Queue depth | met | `cluster status`, metrics |
| Job execution history | met | `cluster jobs`, end reasons |
| Decision explanations | met | `cluster explain`, `cluster_explanations` |
| Agent runtime statistics | met | `cluster estimates` |
| Failure and retry history | met | attempts, end reasons, `cluster events` |
| Prometheus metrics | met | `cluster metrics`, `cluster_metrics_addr` |
| Structured logs with correlation ids | partial | Coordinator events carry a tick correlation id; the daemon log itself is still plain text |

### 11. Security

| Bullet | Status | Evidence |
| --- | --- | --- |
| SSH or mutual auth | met | Herdr machines and OpenSSH, `BatchMode=yes` |
| Worker identity verification | met | Trust on first use; `TestIdentityTrustOnFirstUse`, `TestV05IdentityChange` |
| Least privilege | met | User account only |
| Allowlisting or typed requests | met | `gitop` typed operations; `TestValidationRefusesAnythingButTheTypedOperations`, `TestGitOpWordsAreQuotedAndValidated` |
| Secure credential handling | met | No credentials stored; clone URLs with credentials refused |
| No secrets in logs or DB | met | Prompts never logged (hash only); kept in the ledger only until dispatch |
| Socket permissions | met | `0600`, state dir `0700` |
| Job ownership checks | partial | `cluster_owner_checks`, `TestClusterOwnerChecksAndNewVerbs`; advisory, as everyone on the socket is the same user |
| Command injection protection | met | Fixed argv, checked words; gitop host and user must start alphanumeric |
| Safe repository paths | met | `gitop`, `herdrx` validation |
| Worker registration and revocation | met | `herdr machine add/remove`, drain, `probe: off`, `cluster reregister` |
| Configurable execution permissions | met | Policy gate, per-worker `agents` |
| No unauthenticated remote execution | met | Nothing listens remotely; metrics loopback only |
| Prefer Herdr security primitives | met | Herdr machines |
| No unnecessary credentials to remote agents | met | The scheduler passes none |

### 12. Configuration

| Bullet | Status | Evidence |
| --- | --- | --- |
| Human-readable config | met | `sched.toml`, `cluster-workers.json` |
| policy (adaptive) | met | `cluster_policy = "adaptive"` |
| max_queue_size | met | `cluster_max_queue_size`; `TestV05QueueCap` |
| admission_interval_seconds | met | `cluster_tick_seconds` |
| default_agent_memory_mb | met | `cluster_default_memory_mb` |
| memory_reserve_percent | met | `cluster_memory_reserve_percent` |
| max_cpu_utilisation_percent | met | `cluster_max_cpu_utilisation_percent` |
| max_swap_pressure | met | `cluster_max_swap_pressure`; `TestSwapPressureLimit` |
| workers: name, enabled, max_agents, weight | met | Herdr machine enable/disable, `cluster-workers.json` |
| workspaces: isolation | met | `cluster_isolation` |
| workspaces: cleanup_policy | met | `cluster_worktree_retention` |
| queue: enabled, priority_aging | met | `cluster_queue_enabled`, `cluster_ageing_minutes` (0 is off) |
| observability: metrics_enabled, scheduling_explanations | met | `cluster_metrics_addr`, `cluster_explanations` |
| Validate config at startup | met | `TestConfigValidation`, `TestV05Keys` |
| All resource limits configurable | met | README, Configuration |
### Herdr 0.9.3 behaviour checked by hand

- `herdr pane process-info` needs `--pane <id>`; v0.4.0 passed the pane as a bare argument, which Herdr refuses. Fixed in v0.5.0.
- An idle pane lists its own shell as the foreground process.
- `worktree create` in a repository with no commits fails with `worktree_create_failed` and `invalid reference: HEAD`.
- `worktree remove` without `--force` refuses a dirty worktree with `dirty_worktree_requires_force`, closes the workspace when it succeeds, and leaves the branch.
- An unknown pane is `pane_not_found`.

## Known gaps

- Custom CLI agents, agent-native session ids, pausing, checkpoint relocation, a graceful agent stop, attaching across `--machine` and `herdr` subcommands are all blocked by Herdr 0.9.3, as above.
- Idle memory and build or test phases are not measured separately.
- The daemon log is plain text; correlation ids are on coordinator events only.
- Owner checks are advisory.
- A lost job that is retried gets a fresh workspace; the old worktree is no longer tracked for cleanup and is left in place.
- macOS collectors have not run on Apple hardware, and there has been no run on two real machines.
- No live Windows collector; Windows workers are static only.
