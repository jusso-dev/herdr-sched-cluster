# Architecture

herdr-sched-cluster adds a resource-aware, multi-machine agent scheduler to
[herdr-sched](https://github.com/husniadil/herdr-sched). You ask for a coding
agent; it picks the machine, reserves the memory and CPU, starts the agent in
Herdr on that machine, and keeps track of it. If nothing has room, the job
waits in a queue.

Read [GAP-ANALYSIS.md](GAP-ANALYSIS.md) first for what Herdr does and does not
offer. This document describes what we build on top.

## The picture

```text
                 you, or an agent in a pane, or a cron job
                                   |
                     hsched cluster submit ... (CLI or MCP)
                                   |
                         unix socket (0600)
                                   |
  +---------------------- hsched daemon (one per user) ----------------------+
  |  upstream: jobs, triggers, policy gate, parked actions, JSON store       |
  |                                                                          |
  |  cluster coordinator (runs on the daemon tick and on each submit)        |
  |    workers   <- herdr machine list --json, plus Local                    |
  |    probes    <- in-process on Local; `ssh <target> hsched cluster probe` |
  |    placement    pure: hard constraints, then scores, then explanation   |
  |    queue        pure: priority + ageing, backfill, starvation guard      |
  |    ledger       SQLite cluster.db (0600): jobs, reservations, decisions  |
  |    dispatch  -> herdr [--machine <id>] workspace|worktree create         |
  |                 herdr [--machine <id>] agent start hsc-<job> ...         |
  |                 herdr [--machine <id>] agent prompt hsc-<job> <text>     |
  |    reconcile -> herdr [--machine <id>] agent list (each tick)            |
  +--------------------------------------------------------------------------+
                 |                                   |
          Local Herdr server                 Remote Herdr servers
          (MacBook Pro M3 Max)          (Mac mini M4, future workstation)
```

There is one coordinator: the machine you run `hsched` on. Workers are the
Herdr servers it can reach: Local, plus every enabled saved machine from
`herdr machine list`. A worker needs Herdr, and for live resource numbers it
also needs `hsched` installed so the probe can run. Nothing else runs on a
worker. There is no agent daemon, no open port and no extra service.

## Packages

All new code lives under `internal/cluster/`. Upstream packages are only
touched where the daemon, the verb registry and the config need to know the
cluster exists.

| Package | Pure? | Responsibility |
| --- | --- | --- |
| `cluster/resource` | collectors are not | `Snapshot` type, Linux and macOS collectors behind build tags, the static backend for Windows, per-process sampling for given PIDs |
| `cluster/placement` | yes | Hard constraints, scoring, policies, the human-readable explanation |
| `cluster/queue` | yes | Ordering with priority ageing, backfill, the starvation guard |
| `cluster/ledger` | no (SQLite) | Jobs, reservations, decisions, history and events. Owns every state transition, each in one transaction. |
| `cluster/herdrx` | no (exec) | Typed adapter over the `herdr` CLI with the `--machine` prefix |
| `cluster/transport` | no (exec) | Runs the remote probe over OpenSSH with a fixed argv |
| `cluster/coord` | no | Ties it together: refresh workers, submit, pump, dispatch, reconcile, drain |
| `cluster/sim` | test only | Simulated cluster: fake machines with set resources, a fake Herdr and a fake probe, a controllable clock |

The decision code (`placement`, `queue`) takes plain values and returns plain
values. It never reads the clock, the disk or the network, so every rule is
tested directly and with property-based tests.

## Lifecycle of a job

```text
 submit --> queued --> reserved --> dispatching --> running --> finished
               ^           |              |             |
               |           +--(release)---+             +--> lost (worker's Herdr restarted, agent gone)
               |                          |
               +------ requeued <---------+   (dispatch failed, attempts left)
                                          +--> failed (attempts used up)
 queued/reserved --> cancelled
```

1. **Submit.** The request is validated and stored as `queued`. The
   coordinator then pumps the queue straight away rather than waiting for the
   next tick.
2. **Pump.** Worker snapshots older than `probe_ttl_seconds` are refreshed in
   parallel, each with a timeout. The queue is ordered. For each job the
   placement core filters out machines that break a hard constraint, scores
   the rest and picks the best.
3. **Reserve.** One SQLite `BEGIN IMMEDIATE` transaction re-reads the
   worker's live reservations, checks the job still fits, inserts the
   reservation and moves the job from `queued` to `reserved` with a guarded
   `UPDATE ... WHERE state = 'queued'`. If another process got there first the
   update changes no rows and nothing happens. This is what makes "no job may
   consume the same logical reservation twice" true, even when two
   submissions race.
4. **Dispatch.** Outside the transaction, the coordinator creates a workspace
   (or a worktree when another job is already using that repository on that
   machine), starts the agent with the name `hsc-<job id>`, and sends the
   prompt. Herdr refuses a second live agent with the same name, which is a
   second, independent guard against double dispatch.
5. **Running.** Each tick, the coordinator lists agents on every worker with
   running jobs. A job whose agent has gone is `finished` and its reservation
   is released. A worker that cannot be reached leaves its jobs `running`
   with a `disconnected` flag: the agents are probably still working, so the
   memory stays reserved.
6. **Failure.** A failed dispatch releases the reservation, closes the
   half-made workspace if there is one, marks the worker as backing off, and
   requeues the job until `max_attempts` is reached.

## Placement

### Hard constraints (never overridden by a score)

A worker is not eligible when any of these is true. The explanation records
which ones failed.

- not reachable, auth required, or Herdr too old for `--machine`
- disabled, or draining
- labels or OS/arch the job requires are missing
- the agent kind is not installed there (the probe reports which agent CLIs
  are on `PATH`)
- memory: `available - reserved_not_yet_used - job_memory < min_free_memory`
- memory pressure is `critical`
- CPU: `committed_cpu + job_cpu > cores * max_cpu_commit_ratio`
- active agents would exceed `max_agents`
- free disk below `min_free_disk_gb`
- on battery below `min_battery_percent`, when the worker is set to care
- thermal state `critical`
- the job is pinned to a different machine

### Score (only among eligible workers)

Each factor is normalised to 0..1 and multiplied by a weight set chosen by the
policy. The sum is scaled to 0..100.

| Factor | Meaning |
| --- | --- |
| memory headroom | free memory left after this job, as a share of total |
| memory pressure | `normal` 1, `warn` 0.4 |
| CPU headroom | uncommitted cores after this job, as a share of cores |
| agent count | fewer active agents is better |
| repository locality | the repository already exists on that machine |
| workspace affinity | another job on the same repository runs there (higher with `pack`, lower with `spread`) |
| latency | lower probe round trip is better |
| preference | the operator's `weight` for that worker |
| power and thermal | on mains power and nominal thermal state is better |

Policies are weight presets: `balanced` (default), `memory`, `performance`,
`pack`, `spread` and `local-first`. A job can ask for its own policy. Ties are
broken by worker id so the same inputs always give the same answer.

### Explanation

Every decision stores, for each worker, either the constraints that excluded
it or its score broken down by factor. `hsched cluster explain <job>` prints
it. A queued job explains why it is still waiting.

## Queue

- Effective priority is `priority + floor(wait_seconds / ageing_seconds)`, so
  low-priority work cannot wait forever.
- Ties break on submission time, then on job id.
- **Backfill:** if the job at the front does not fit anywhere, later jobs that
  do fit are still placed.
- **Starvation guard:** once the front job has waited longer than
  `starvation_seconds`, the coordinator picks the worker where it would fit
  soonest and stops backfilling onto that worker until the job is placed.
- Cancelling a queued or reserved job releases anything it holds. Cancelling
  a running job is refused unless `--force-detach` is given, which only stops
  tracking it. It never kills the agent.

## Resource accounting

- `available` comes from the operating system: `MemAvailable` on Linux, and
  free plus inactive plus purgeable pages on macOS. Swap and the kernel's
  pressure signal are used as well, because free RAM alone is misleading on
  macOS.
- A reservation counts in full until the agent has been running for
  `settle_seconds`. After that, the coordinator assumes the operating system's
  numbers include the agent and only counts the part of the reservation the
  agent has not used yet. This avoids counting a running agent twice.
- When a probe can see an agent's processes (`pane process-info` gives the
  PIDs), it samples their resident memory and CPU. If an agent uses more than
  it reserved, its reservation is raised to what it uses plus 10%, and a
  `cluster.job.overrun` event is written. Nothing is killed.
- Peak usage is remembered per agent kind and repository and used as the
  default estimate for the next job of the same shape (exponentially weighted).

## Draining

`hsched cluster drain <worker>` stops new placements on that worker straight
away. Running agents are left alone. `hsched cluster workers` shows how many
are still running there. `hsched cluster undrain` reverses it.

## Restarts

- **Coordinator restart:** all state is in SQLite. On start, jobs in
  `reserved` or `dispatching` are checked against Herdr: if the agent
  `hsc-<id>` exists, the job becomes `running`; if not, its reservation is
  released and it is requeued. Running jobs are reconciled on the first tick.
- **Worker Herdr restart:** agents on that worker are gone (see the gap
  analysis). Reconciliation marks those jobs `lost` and frees the capacity.
- **Worker disconnect:** jobs stay `running` and keep their reservations, and
  the worker is not eligible for new work until it answers again.

## Security

- **Typed execution.** Every external command is an argv slice passed to
  `exec.Command`. No `sh -c`, no string building. Prompts, paths and agent
  arguments travel as single argv elements to `herdr`, and Herdr sends them to
  remote machines as JSON over SSH.
- **The one SSH command.** The remote probe is the only thing we run over SSH
  ourselves. Its remote argv is fixed (`hsched cluster probe --json`, plus
  validated `--path` and `--pid` arguments). Each argument is checked against
  an allow-list of characters and then single-quoted, because OpenSSH joins
  the remote argv into a string. `BatchMode=yes` means we never prompt for or
  handle a password.
- **Authentication** is OpenSSH's, with the targets Herdr already saved. We
  store no keys, tokens or passwords.
- **Files.** The state directory is `0700`. The socket, the database and its
  WAL and SHM files are `0600`. The coordinator refuses to open a database
  that is readable by other users.
- **No secrets in logs or the database.** Prompts are stored (the queue needs
  them) but are never logged; events carry the prompt's length and SHA-256
  instead. Environment variables are not accepted on a job.
- **Policy gate.** `cluster.submit`, `cluster.cancel`, `cluster.drain` and
  `cluster.undrain` go through upstream's `gate_command`, so the operator can
  require approval.
- **No telemetry.** Nothing leaves the operator's machines except the SSH and
  Herdr traffic they already use. The optional Prometheus endpoint binds to
  loopback by default.

## Why no Kubernetes, Docker, Redis or Postgres

The cluster is two to five personal computers. The coordinator is a goroutine
in a daemon that already exists, the state is one SQLite file, and the
transport is SSH that is already set up for Herdr. Anything heavier would cost
more to run than the agents it schedules.
