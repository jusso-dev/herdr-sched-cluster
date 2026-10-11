# Gap analysis

This is the audit the build started with. Every claim about Herdr or
herdr-sched below was checked against source code, not only against
documentation. Where the docs and the code disagree, the code wins and the
difference is noted.

Audited revisions:

| Repository | Revision | Date |
| --- | --- | --- |
| [husniadil/herdr-sched](https://github.com/husniadil/herdr-sched) | `c68ad65` (v0.2.4) | 30 Aug 2026 |
| [herdrdev/herdr](https://github.com/herdrdev/herdr) | `2563803` (after v0.9.3) | 8 Oct 2026 |
| [husniadil/herdr-dispatch](https://github.com/husniadil/herdr-dispatch) | `main` | 31 Aug 2026 |

## 1. Licence, maintenance and compatibility

| Item | Finding |
| --- | --- |
| herdr-sched licence | MIT, copyright 2026 Husni Adil Makmur. Redistribution and modification are allowed as long as the copyright and permission notice travel with the code. `LICENSE` is kept unchanged and `NOTICE` credits the original author. |
| herdr-sched maintenance | Created 24 Aug 2026, last push 30 Aug 2026, 5 stars, no forks, no issues. Small and young, but the code is careful and well tested. Treat it as a stable base we now maintain ourselves rather than one that will track our changes. |
| herdr-sched upstream CI | Green on ubuntu-latest and macos-latest for every recent commit. |
| herdr-sched on this audit box | `make test` passes. `make test-full` has two failures in `internal/daemon` (`TestTheStampMovesEvenWhenThereIsNoRunnerToFireWith`, `TestASameSizeRewriteInsideOneMillisecondIsStillAChange`). Both depend on sub-millisecond file mtimes, and the audit box runs on overlayfs, which only moves mtimes every few milliseconds. They pass on GitHub's runners. Not a code defect; left unchanged. |
| Herdr licence | Apache 2.0. Nothing from Herdr is copied into this repository. We only call its CLI. |
| Herdr release line | v0.9.3 (29 Sep 2026) is the latest stable release. Very active: several commits a day. |
| Compatibility | herdr-sched declares `min_herdr_version = "0.8.0"`. The cluster features need `herdr --machine` (added in 0.9.1) and `herdr machine status` (0.9.2). The manifest minimum stays at 0.8.0 so the original scheduler keeps working on older Herdr; the cluster code checks for `--machine` support at run time and reports it in `hsched doctor` and `hsched cluster workers`. |

## 2. What herdr-sched actually is

herdr-sched is **not** a resource scheduler. It is a cron and trigger plugin:

- A **job** is a five-field UTC cron expression plus one action.
- A **trigger** is an HMAC-signed webhook or a polled file watch plus one action.
- An **action** is one of `task` (htask board), `mail` (hmail), `dispatch`
  (hdis) or `shell` (runs `sh -c` on the host).
- One Go binary, `hsched`, is the daemon, the CLI door and the MCP door. Both
  doors are generated from one verb registry (`internal/verbs`) and a parity
  test fails if they drift.
- One daemon per user, elected by `flock` on `<state dir>/sched.lock`,
  answering on a `0600` Unix socket inside a `0700` state directory.
- State is a single JSON document rewritten through a temp file and rename.
  The upstream README explains why it chose JSON over SQLite.
- Every mutating verb passes an optional external policy gate
  (`gate_command`) that can allow, deny or park the call.
- It does not know about machines, memory, CPU, queues or placement at all.
  `dispatch` shells out to `hdis`, which starts a worker on the local Herdr
  server only.

### What we reuse

| herdr-sched part | How the cluster scheduler uses it |
| --- | --- |
| `internal/verbs` registry and both doors | The cluster verbs are added to the same registry, so every cluster verb is on the CLI and on MCP, and the parity tests cover them. |
| `internal/daemon` (lock, socket, tick) | The coordinator lives inside the existing daemon and runs on its tick. No second daemon, no second socket. |
| `internal/config` (paths, overrides) | Cluster settings sit in the same `sched.toml`, with the same `SCHED_*` overrides and the same state directory. |
| `internal/gate` | Cluster verbs that change the world (`cluster.submit`, `cluster.cancel`, `cluster.drain`, `cluster.undrain`) go through the same policy gate. |
| `internal/client` (autostart) | `hsched cluster submit` starts the daemon if none is running, exactly like every other verb. |
| `internal/testenv` | The fake-`PATH` isolation idea is reused for the dispatch tests, which drive a fake `herdr` and a fake `ssh`. |

### What herdr-sched is missing for this brief

| Gap | Size |
| --- | --- |
| No resource measurement of any kind | New package |
| No idea of a worker or a machine | New package |
| No queue, reservation, priority or placement logic | New packages |
| JSON store has no transactions across processes and rewrites everything on each change | The cluster ledger uses SQLite instead (see section 5) |
| `dispatch` only reaches the local Herdr server, through hdis and the htask board | Cluster dispatch drives Herdr directly with `herdr --machine` |
| `shell` action runs `sh -c` with operator text | Left alone for backward compatibility. Nothing in the cluster path builds a shell string. |

## 3. Herdr extension points, verified in source

Herdr's own docs say: "There is no separate plugin SDK or restricted command
set. The entire Herdr CLI is the plugin API." That matches the code. There is no
Go, Rust or JS SDK to link against.

Stability key:

- **stable**: documented in the versioned docs for the current release
  (`docs/versions/0.9.3`), with tests in Herdr's source.
- **experimental**: shipped and documented, but marked experimental, preview
  or beta, or added in the last two releases with open follow-up work.
- **undocumented**: present in source but not in the docs. We do not rely on
  anything in this class.

| Extension point | Where verified | Stability | Used for |
| --- | --- | --- | --- |
| Plugin manifest: `[[build]]`, `[[startup]]`, `[[actions]]`, `[[events]]`, `[[panes]]` | `docs/.../plugins.mdx`, herdr-sched's own manifest and tests | stable (plugin v1) | Install, daemon start, dashboard popup |
| `HERDR_BIN_PATH`, `HERDR_PANE_ID` and the other injected env | `plugins.mdx` | stable | Finding the Herdr binary and the caller's pane |
| No plugin shutdown hook | `plugins.mdx` ("startup hooks are one-shot") | stable (documented absence) | Stop and restart are manifest actions, as upstream already does |
| No plugin storage API | `plugins.mdx` ("There is no Herdr-managed plugin storage API in v1") | stable (documented absence) | We own our SQLite file |
| `herdr machine list --json` | `src/cli/machine.rs` `list()`, `MachineListRow {id,label,target,session,enabled,selected}` | stable (0.9.x) | Worker discovery |
| `herdr machine status [<id>] --json` | `src/cli/machine.rs` `status()`, rows `{id,label,status,error}`, status is `reachable`, `auth required`, `error` or `disabled` | stable, added 0.9.2 | Worker health |
| `herdr machine add/rename/enable/disable/remove` | `src/cli/machine.rs` | stable | Machine registration stays in Herdr. We never write Herdr's catalog. |
| `herdr --machine <label-or-id> <command>` | `src/cli/target.rs` `parse_machine_prefix`, `validate_machine_command` | stable, added 0.9.1 | Every remote dispatch call |
| Commands allowed after `--machine` | `validate_machine_command`: `workspace`, `worktree`, `tab`, `pane`, `notification`, `agent` (not `attach`), `api snapshot`, `status server`, some `plugin` and `server` verbs | stable | We stay inside this list |
| `--machine` payloads are sent as JSON over non-interactive SSH, not interpolated into a shell | `cli-reference.mdx`, `src/cli/target.rs` | stable | Prompts and paths reach the remote Herdr without shell quoting risk |
| `herdr workspace create --cwd --label --no-focus` returning `.result.root_pane.pane_id` | `src/api/schema/workspaces.rs`, `agent-automation.mdx` | stable | Each job gets its own workspace |
| `herdr worktree create --cwd --branch --label --no-focus` | `cli-reference.mdx`, `src/cli/worktree.rs` | stable | Isolation when several agents share one repository on one machine |
| `herdr agent start <name> --kind K --pane P [--timeout ms] -- args` | `src/api/schema/agents.rs` `AgentStartParams`, herdr-dispatch's adapter and tests | stable | Launching the agent |
| Agent names unique among live agents, `[a-z][a-z0-9_-]{0,31}` | `cli-reference.mdx`, server validation | stable | `hsc-<job id>` names double as a duplicate-dispatch guard |
| `herdr agent prompt <target> <text>` | `AgentPromptParams` | stable | Delivering the first task prompt |
| `herdr agent list` / `agent get`, `AgentInfo {name, agent, agent_status, pane_id, workspace_id, cwd, ...}` | `src/api/schema/agents.rs` | stable | Reconciliation after restart and disconnect |
| `herdr pane process-info --pane P` returning `shell_pid` and `foreground_processes[].pid` | `src/api/schema/panes.rs` `PaneProcessInfo` | stable | Per-agent CPU and memory measurement |
| `herdr workspace close <id>` | `cli-reference.mdx` | stable | Cleaning up a failed dispatch |
| Socket API `events.subscribe` (`pane.exited`, `pane.agent_status_changed`) | `socket-api.mdx` | stable, but "event history is not durable" | Not used in v1. Polling `agent list` on the tick is simpler and survives restarts. Noted as a later optimisation. |
| Live handoff (`--handoff`) | `session-state.mdx` | experimental | Not used |
| Windows servers over saved SSH machines | `connecting-machines.mdx`, `windows-beta.mdx` | stable for attach; plugins on Windows are "preview" | Dispatch to Windows works through `--machine`; our probe does not run there (see section 6) |

### What does not exist in Herdr, and the alternative we chose

| Wanted | Reality | Alternative |
| --- | --- | --- |
| A Herdr API that reports a machine's memory, CPU, swap or thermal state | Does not exist. Herdr reports panes and agents, not host resources. | Our own probe, `hsched cluster probe --json`, run locally in-process or on a remote worker over SSH. |
| Running an arbitrary command on a saved machine through Herdr | `--machine` refuses anything outside the API-backed list. There is no `exec`. | Plain OpenSSH to the same target Herdr saved (`machine list` gives it), with `BatchMode=yes`, a fixed argv and per-argument quoting. Same keys, same `~/.ssh/config`, no new credentials. |
| A combined agent list across machines | "The prefix targets one machine at a time; combined listings ... are not included." | The coordinator asks each worker in turn and merges the answers. |
| Agents surviving a Herdr server restart | Processes do not survive a server restart. Only layout and, for some agents, the native conversation come back. | Our ledger survives a coordinator restart. A Herdr server restart on a worker ends that worker's agents, and reconciliation ends those jobs and frees their reservations rather than pretending they are still running. v1 records them as `finished`, because it cannot yet tell a restart from a normal exit (see the checklist). |
| A durable event stream | `events.subscribe` is not durable across restarts. | Polling plus reconciliation on every tick. |
| Plugin-provided UI beyond a terminal pane | "Native non-terminal plugin UI is not part of plugin v1." | Dashboard is a terminal popup script, as upstream already does. |

## 4. Other Herdr task and automation plugins checked

| Project | What it does | Why it is not the base |
| --- | --- | --- |
| husniadil/herdr-dispatch (`hdis`) | One worker pane per ready htask task, local Herdr only | Single machine, no resource model, needs the htask board. Its `herdrclient` adapter shows the right way to drive the CLI, and our adapter follows it. |
| nelsonPires5/herdr-board | Kanban board, cards dispatched as prompts | Single machine, no resource model |
| nikok6/herdr-mirror | Mirrors remote Herdr servers into the local sidebar over SSH | Viewing, not scheduling. Herdr 0.9 now does this natively with saved machines. |
| eliasstravik/herdr-projects | Coordinator conversation and worker threads | Agent orchestration, not machine placement |

None of them measures host resources or places work across machines.

## 5. Decisions taken in the audit

1. **Extend, do not replace.** The cluster scheduler is new packages under
   `internal/cluster/` plus new verbs in the existing registry, served by the
   existing daemon. Every upstream verb, file path and behaviour is kept.
2. **Go, not Rust.** herdr-sched is one statically linked Go binary that Herdr
   builds on install. A Rust sidecar would add a second toolchain and a second
   binary for no gain, so the brief's "Rust if the stack permits" does not
   apply.
3. **SQLite for the cluster ledger** (`<state dir>/cluster.db`, mode `0600`),
   through `modernc.org/sqlite`, which is pure Go. That keeps `go build`
   working without a C compiler, which matters because Herdr builds the plugin
   on install. Upstream's JSON store is untouched. The ledger needs real
   transactions so two submissions can never take the same capacity.
4. **Herdr owns machine registration.** We read `herdr machine list` and never
   write Herdr's catalog. Per-worker scheduling settings live in our config.
5. **Polling, not event subscriptions**, for v1 (see section 3).
6. **Never kill an agent.** Draining, memory pressure and overruns stop new
   placements and raise warnings. Ending an agent is always the operator's
   call.

## 6. Platform support at the end of the audit

| Platform | Probe | Dispatch | Status |
| --- | --- | --- | --- |
| Linux x86-64 / arm64 | `/proc`, `/sys`, `statfs` | `herdr` local or `--machine` | Built and tested on real Linux |
| macOS Apple Silicon | `sysctl` (`hw.memsize`, `vm.page_*`, `kern.memorystatus_vm_pressure_level`, `vm.swapusage`, `vm.loadavg`), `statfs` | `herdr` local or `--machine` | Compiles behind build tags, unit tested on GitHub's macOS runners. **Untested on Justin's hardware.** |
| Windows x86-64 | No probe. A Windows worker is declared with **static capacity** in config (`probe = "static"`). | `herdr --machine` (Herdr supports Windows servers) | Explicit, documented backend. Placement uses declared numbers, not live ones. |

## 7. Note on the brief itself

The copy of the build brief this work started from had its middle sections
(roughly 3.3 to 13) cut off in transit. Sections 1 to 3.2 and 13 to 17 were
read in full. The design covers the topics those missing sections almost
certainly ask for (scheduling policies, queueing, isolation, security,
configuration, observability, CLI), and the checklist marks anything inferred
so it can be checked against the full text.
