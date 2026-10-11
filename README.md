# herdr-sched-cluster

herdr-sched-cluster lets you launch coding agents in Herdr without choosing
which computer they run on. You queue an agent (Claude Code, Codex, Cursor,
Gemini and the other kinds Herdr knows) with a repository and a prompt, and
the scheduler places it on whichever of your machines has the memory and CPU
to spare, opens a Herdr workspace there and starts the agent. It reads real
memory pressure, swap, CPU, load, disk, battery and thermal state on each
machine, keeps a reservation for every agent it starts, never starts the same
job twice, and can tell you why a job went where it did or why it is waiting.

It is an extension of [herdr-sched](https://github.com/husniadil/herdr-sched)
by Husni Adil Makmur, used under the MIT licence, and it keeps everything
herdr-sched does: cron jobs and triggers that fire actions into the sibling
plugins. Machines are the ones you have already saved with
`herdr machine add`; this plugin never manages SSH keys or Herdr sessions
itself. There is no Kubernetes, Docker, Redis or database server: one Go
binary, `hsched`, plus a SQLite file.

The design and its trade-offs are in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md),
what was reused and what was missing in Herdr is in
[docs/GAP-ANALYSIS.md](docs/GAP-ANALYSIS.md), and what is built and tested is
in [docs/IMPLEMENTATION-CHECKLIST.md](docs/IMPLEMENTATION-CHECKLIST.md).

## Install

### Quick start with your coding agent

The fastest way to get going is to let your coding agent do the setup. Copy
the prompt in [docs/AGENT-SETUP-PROMPT.md](docs/AGENT-SETUP-PROMPT.md) into
Claude Code, Codex or OpenCode on the machine you will submit work from. It
checks the prerequisites, installs and starts the plugin, connects your
agents to the scheduler, helps you add and check your other machines, maps
repository paths where they differ, and finishes with a dry-run plan. It
asks before touching SSH keys, other machines or running work, and it never
submits a real job.

To do it by hand instead, follow the rest of this section.

You need Herdr 0.9.2 or later on every machine and key-based SSH from the
coordinator (the machine you submit from) to each worker. Go is optional: with
Go installed the plugin is compiled from source, and without it the matching
prebuilt binary is downloaded from this repository's releases and checked
against `checksums.txt`. Prebuilt binaries exist for macOS on Apple Silicon and
Intel, and Linux on x86-64 and ARM64.

These steps were run end to end against Herdr 0.9.3 on Linux x86-64. On every
machine, coordinator and workers alike:

```sh
herdr plugin install jusso-dev/herdr-sched-cluster
```

Herdr shows a preview and asks before installing; add `--yes` to skip the
question. The preview should say `id: herdr-sched`, `version: 0.5.0` and
`build: ./scripts/build.sh`. Then start the daemon. A running Herdr server only
runs startup hooks when it next starts, so either restart Herdr or run:

```sh
herdr plugin action invoke herdr-sched.restart
```

Check it:

```sh
hsched version                  # hsched 0.5.0 (herdr-sched)
hsched doctor                   # socket, state and config paths, gate
hsched cluster status           # every worker and how much room it has
```

If your shell cannot find `hsched`, add `~/.local/bin` to your `PATH`. Each
time the daemon starts, `scripts/start.sh` copies the binary to
`~/.local/state/herdr/plugins/herdr-sched/bin/hsched` and points
`~/.local/bin/hsched` at it. Herdr installs every update into a new directory,
so that link is the stable path for your shell, your agents' MCP config and
the coordinator's SSH probe. A real file already at `~/.local/bin/hsched` is
left alone; set `HSCHED_NO_LINK=1` to skip the link entirely.

To force one way of getting the binary, set `HSCHED_BUILD=source` or
`HSCHED_BUILD=prebuilt` in the environment you run `herdr plugin install` from.

### Plugin name

The plugin id is `herdr-sched` and the binary is `hsched`, the same as
upstream herdr-sched. That is deliberate: this is a drop-in replacement, so
your existing cron jobs, triggers and config in `~/.config/sched` and
`~/.local/state/sched` carry straight over. The two cannot run side by side,
because they would share one socket and one store. Installing this over
upstream replaces it in place (the preview says `replaces: herdr-sched`); to
be tidy, remove upstream first:

```sh
herdr plugin uninstall herdr-sched
herdr plugin install jusso-dev/herdr-sched-cluster
herdr plugin action invoke herdr-sched.restart
```

To remove this plugin, stop the daemon first, because Herdr has no shutdown
hook:

```sh
herdr plugin action invoke herdr-sched.stop
herdr plugin uninstall herdr-sched
rm -f ~/.local/bin/hsched
```

Your store and config stay where they are. To go back to upstream, install
`husniadil/herdr-sched` after that.

### Upgrading

Herdr 0.9.3 has no `plugin update` command, so run the install again, then
restart the daemon:

```sh
herdr plugin install jusso-dev/herdr-sched-cluster --yes
herdr plugin action invoke herdr-sched.restart
```

Newer Herdr releases also have `herdr plugin update herdr-sched`. The cluster
ledger carries a schema version and is refused, not overwritten, by a binary
that does not know it.

### Developing

```sh
git clone https://github.com/jusso-dev/herdr-sched-cluster
cd herdr-sched-cluster
make build
herdr plugin link .
```

`plugin link` does not run the build step, so build first. Herdr has no
shutdown hook, so disabling or unlinking the plugin leaves the daemon running.
The `[[actions]]` entries are how it is actually turned off: **Stop the sched
daemon** and **Restart the sched daemon**. Configuration is read once at daemon
start, so restart after editing it.

## Registering machines

Workers are Herdr's saved machines. Install the plugin on each worker as
above, so `~/.local/bin/hsched` exists there: the coordinator probes a worker
by running `~/.local/bin/hsched cluster probe --json` over SSH. If you keep
`hsched` somewhere else on a worker, set `cluster_probe_program` in the
coordinator's `~/.config/sched/sched.toml`, or `program` for that worker in
`cluster-workers.json`.

On the coordinator:

```sh
herdr machine add mini            # follow Herdr's prompts for the SSH target
herdr machine status              # every saved machine should be reachable
herdr --machine mini agent list   # Herdr itself can drive the remote server
hsched cluster probe              # this machine's own reading
hsched cluster workers --refresh  # every worker as the scheduler sees it
```

`hsched cluster workers` lists this computer as `local` (unless
`cluster_include_local = false`) and every enabled saved machine. A worker
shows `unreachable` or `auth_required` with the SSH error when the probe
fails, then `backoff` while it waits to retry. A machine you disable in Herdr
shows `disabled`.

Herdr's own health check is part of placement too. Every
`cluster_machine_status_seconds` (default 60) the coordinator reads
`herdr machine status`, and a saved machine whose Herdr server cannot be
reached shows `herdr_unreachable` and takes nothing, even when SSH to it still
works. One whose Herdr asks for authentication shows `auth_required`.

The first successful probe records each worker's identity (hostname, OS and
architecture). If a later probe reports a different machine under
the same name, the worker shows `incompatible` and takes nothing until you
accept the change with `hsched cluster reregister <worker>` (gated as
`sched.cluster.reregister`). `cluster_verify_identity = false` turns the check
off.

A machine that cannot run the probe, such as a Windows machine, can still
take agents if Herdr can drive it: declare its capacity in
`~/.config/sched/cluster-workers.json` with `"probe": "static"`. It is then
placed against that declared memory and core count and what the scheduler has
reserved there, not against live readings.

```json
{
  "local": { "max_agents": 6, "weight": 1.2 },
  "mini": {
    "max_agents": 3, "min_free_memory_mb": 3072, "labels": ["arm"],
    "max_cpu_utilisation_percent": 75, "memory_reserve_percent": 10,
    "path_map": { "~/src": "/Volumes/work/src" }
  },
  "winbox": {
    "probe": "static", "os": "windows", "arch": "amd64",
    "memory_mb": 32768, "cores": 8, "agents": ["codex", "claude"]
  }
}
```

Keys are a Herdr machine id or label, or `local`. Per-worker fields are
`probe` (`auto`, `static` or `off`), `program` (the remote `hsched` for this
worker), `max_agents`, `min_free_memory_mb`, `max_cpu_commit`,
`max_load_per_core`, `min_free_disk_gb`, `min_battery_pct`,
`max_cpu_utilisation_percent`, `memory_reserve_percent`, `max_swap_pressure`,
`weight` (a preference, higher is preferred), `labels`, `agents` (when set,
the only agent kinds this worker may run), `path_map`, and for static workers
`os`, `arch`, `memory_mb` and `cores`. An unknown field is an error, so a typo
cannot be silently ignored.

`path_map` rewrites repository paths for one worker. A job names the path as
you know it (`~/src/app`), and on `mini` above it runs in
`/Volumes/work/src/app`. The longest matching prefix wins, and a prefix only
matches whole path segments, so `~/src` does not rewrite `~/srcx`. The job,
its explanation and its events keep the path you submitted; the worker sees
the mapped one.

## Running agents on the cluster

```sh
# How much room is there, and where would ten agents go? Neither starts anything.
hsched cluster status
hsched cluster plan --kind claude --repo ~/src/app --count 10
hsched cluster plan --jobs '[{"kind":"claude","repo":"~/src/app","priority":20},{"kind":"codex","repo":"~/src/api","memory_mb":4096}]'

# Queue a Claude Code agent; the scheduler picks the machine.
hsched cluster submit claude ~/src/app --prompt "Fix the failing date tests"

# Reserve more memory, prefer memory headroom, and pin to the MacBook.
hsched cluster submit codex ~/src/api --memory_mb 8192 --policy memory --machine mbp

# Ask for a fresh Git worktree even when the repository is idle.
hsched cluster submit claude ~/src/app --isolation worktree --prompt "Try the refactor"

hsched cluster jobs               # what is queued, running and done, and where
hsched cluster queue              # waiting jobs in the order they will be tried, and why
hsched cluster explain j12        # why j12 went where it did, or why it waits
hsched cluster workers            # memory, pressure, CPU and reservations per machine
hsched cluster drain mini         # no new agents on mini; running ones carry on
hsched cluster undrain mini
hsched cluster cancel j14         # a job that has not started yet
hsched cluster detach j12         # stop counting a running agent; it keeps running
hsched cluster attach j12         # the Herdr command that opens j12's agent
hsched cluster estimates          # what earlier agents used, per kind and repository
hsched cluster reregister mini    # accept mini's new identity after a rebuild
hsched cluster events             # reservations, dispatches, failures, overruns
hsched cluster metrics            # Prometheus text

# Run after j3 and j4 finish; clone the repository where it is missing.
hsched cluster submit codex ~/src/api --depends_on j3,j4 \
  --clone_url git@github.com:you/api.git --prompt "Integrate both fixes"

# Safe to start again if lost; may be closed under memory pressure.
hsched cluster submit claude ~/src/app --restartable --disposable
```

The repository path is a path on the worker, absolute or starting with `~/`
(resolved against that worker's home directory). Machines with the same
layout under your home directory work without any mapping. A path that does
not exist on a worker rules that worker out, and `explain` says so.

`kind` is the agent kind as Herdr names it: `amp`, `claude`, `codex`,
`copilot`, `cursor`, `droid`, `gemini`, `grok`, `kiro`, `opencode`, `pi`,
`qwen` and the others Herdr detects.

With `--clone_url`, a worker that does not have the repository is no longer
ruled out: the scheduler clones the URL to the job's path there first (over
SSH, through `hsched cluster gitop` on the worker), and the job fails with
the Git error if the clone does. URLs carrying credentials are refused.

`--depends_on` holds a job until every job it names has finished. If one of
them fails, is cancelled or is lost, the dependent job fails too, with that
job named in the reason.

What happens to a job:

1. `submit` checks the request and refuses a job no worker could ever run,
   with the reason for each worker, and refuses any job once
   `cluster_max_queue_size` jobs already wait. Without `--memory_mb`, the
   estimate is learned from the last `cluster_history_window` runs of the same
   kind in the same repository, then the same kind anywhere, then
   `cluster_default_memory_mb`. The learned figure is the
   `cluster_history_percentile` (90th by default) of each run's peak, plus
   `cluster_history_headroom_pct`. Without `--cpu`, the CPU estimate is
   learned the same way from each run's process-tree CPU use.
2. Each pass orders the queue by priority plus ageing, and places each job on
   the best eligible worker. Hard constraints always win over the score: a
   worker is ruled out for memory below `min_free_memory_mb` (or
   `memory_reserve_percent` of its memory, whichever is larger) after the job,
   critical memory pressure, swapping above `max_swap_pressure`, CPU
   utilisation above `max_cpu_utilisation_percent` once the job's CPU is
   added, CPU over-commitment, load per core, low disk, low battery, critical
   thermal state, `max_agents`, a pin, labels, OS or architecture, a missing
   repository (unless it can be cloned), a declared agent list, Herdr being
   unreachable, or `cluster_max_agents_per_repo` agents already in that
   repository. The score then ranks what is left according to the policy
   (`balanced`, `memory`, `performance`, `pack`, `spread`, `local-first` or
   `adaptive`). `adaptive` picks one of the others each pass: `memory` when a
   worker reports memory pressure or less than 30% of the cluster's memory is
   unreserved, `performance` when every ready worker is idle, otherwise
   `balanced`, and `explain` says which and why.
3. Capacity is reserved in the SQLite ledger inside one transaction that
   re-checks the worker's load, so two submissions racing for the last
   gigabytes cannot both win.
4. On the chosen machine the scheduler runs `herdr --machine <id> workspace
   create` (or `worktree create` when another job already holds that
   repository there), then `agent start hsc-<job>` and `agent prompt`.
5. Every tick it reconciles with `herdr agent list` on each worker, and
   measures each agent's process tree. An agent using more than it reserved
   has its reservation raised to what it uses plus
   `cluster_overrun_margin_pct`. When an agent is no longer listed, the
   scheduler asks Herdr about the pane it ran in, and tells the cases apart:

   | What it finds | Job ends as | Reason |
   | --- | --- | --- |
   | The pane is there with the same shell | `finished` | `agent_exited` |
   | The pane is there with a new shell (Herdr restarted) | `lost` | `herdr_restarted` |
   | The pane is gone | `lost` | `session_vanished` |
   | The worker has been unreachable for `cluster_lost_after_seconds` | `lost` | `worker_unreachable` |

   A worker that is unreachable for less than that marks its jobs
   `disconnected` and keeps their reservations. A `worker_unreachable` job
   whose agent shows up again is taken back as running. A lost job is never
   started again unless it was submitted with `--restartable`,
   `cluster_retry_lost` is on (the default), and the pane it ran in has
   nothing but its shell in the foreground, since anything else may be the
   agent carrying on by itself.

6. When a job ends, its worktree is dealt with according to
   `cluster_worktree_retention`: `retain-on-failure` (the default) removes the
   worktree of a job that finished and keeps the others for you to inspect,
   `always-retain` keeps every one, `always-remove` removes every one, and
   `age` removes any once it is `cluster_worktree_max_age_hours` old. Removal
   goes through `herdr worktree remove`, which refuses a worktree with
   uncommitted changes unless `cluster_worktree_remove_dirty` is on, and then
   closes the workspace. The branch (`hsc/j<id>`) is kept unless
   `cluster_delete_branches` is on, and even then Git's `branch -d` keeps a
   branch that is not merged, so unmerged work is never lost. A worktree that could not be removed is
   kept, with the reason in the job, `explain` and the event trail.

Under sustained critical memory pressure (`cluster_pressure_grace_seconds`,
default 120), `cluster status` recommends what to do, such as draining the
worker or which jobs to move. With `cluster_pressure_action =
"terminate-disposable"` the scheduler also closes the workspace of the
largest job submitted with `--disposable` on that worker, at most one per
grace period, and records it with reason `terminated_under_pressure`: as
`failed`, or back in the queue if it was also `--restartable` and has
attempts left. Nothing else is ever killed.

If the coordinator stops in the middle of a dispatch, the next run decides
what happened. If it had not yet asked Herdr to start the agent, any
half-made workspace is found by its label and closed, and the job goes back
to the queue. If it had asked, the agent may already be running somewhere
the scheduler cannot see, so the job fails with `dispatch_interrupted`
rather than risk starting it twice.

A repository with no commits cannot have a worktree. A job that needs one
there fails with `empty_repository` and a message saying to make a first
commit, instead of retrying forever.

A big job that keeps losing to smaller ones is protected: once it has waited
`cluster_starvation_minutes`, the worker nearest to fitting it is held clear
for it, and small jobs that would fit elsewhere still run.

## Letting your coding agent spread the work

Herdr does not pass plugin tools to agents; plugins have no MCP hook. So each
agent connects to the same `hsched mcp` server itself. The quickest way, once
per machine, is the plugin's own action, which registers the server with
whichever of Claude Code, Codex and OpenCode are installed and copies the
skill for Claude Code:

```sh
herdr plugin action invoke herdr-sched.setup-agents
```

It is safe to run again. To do it by hand instead (use your own home directory
in the paths; JSON and TOML do not expand `~`):

Claude Code:

```sh
claude mcp add --scope user herdr-sched -- ~/.local/bin/hsched mcp
mkdir -p ~/.claude/skills/herdr-cluster
curl -fsSL https://raw.githubusercontent.com/jusso-dev/herdr-sched-cluster/main/skills/herdr-cluster/SKILL.md \
  -o ~/.claude/skills/herdr-cluster/SKILL.md
```

The cron and trigger half has its own skill, `skills/sched`, which the action
copies to `~/.claude/skills/sched` as well.

Codex, either `codex mcp add herdr-sched -- ~/.local/bin/hsched mcp` or in
`~/.codex/config.toml`:

```toml
[mcp_servers.herdr-sched]
command = "/Users/you/.local/bin/hsched"
args = ["mcp"]
```

OpenCode, in `~/.config/opencode/opencode.json`:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "herdr-sched": {
      "type": "local",
      "command": ["/Users/you/.local/bin/hsched", "mcp"],
      "enabled": true
    }
  }
}
```

Check with `claude mcp list`, `codex mcp list` or `opencode mcp list`; each
should show `herdr-sched` connected or enabled. All three were checked this way.

Then tell the agent when to use it. Claude Code picks up the skill on its own.
For Codex, OpenCode and anything else that reads `AGENTS.md`, paste
[skills/herdr-cluster/AGENTS-snippet.md](skills/herdr-cluster/AGENTS-snippet.md)
into your global or project `AGENTS.md`. After that you can say "spread these
five fixes across my machines" and the agent will:

1. call `cluster_status` to see free memory and room on each machine,
2. call `cluster_plan` with the jobs it intends to run, which answers per job
   `would_place` (and where), `would_queue` (and what it waits for) or
   `would_refuse` (and why), without starting anything,
3. submit each agent with `cluster_submit`, most important first or with a
   higher `priority`,
4. answer "why is that one waiting?" with `cluster_queue` or
   `cluster_explain`.

The read-only tools are `cluster_status`, `cluster_plan`, `cluster_workers`,
`cluster_queue`, `cluster_jobs`, `cluster_explain`, `cluster_attach`,
`cluster_estimates`, `cluster_events` and `cluster_metrics`. The ones that
change something are `cluster_submit`, `cluster_cancel`, `cluster_detach`,
`cluster_drain`, `cluster_undrain` and `cluster_reregister`, and
each can be held for your approval by the policy gate. The skill tells the
agent never to get round a hard limit by pinning work to a full machine or
understating memory, and to ask before draining or cancelling.

To try the MCP server by hand, `scripts/mcp-smoke.py` speaks JSON-RPC over
stdio the way an agent's client does: it lists the tools and calls
`cluster_status` and `cluster_plan`.

## Cron jobs, triggers and the two doors

Everything below this point is upstream herdr-sched's behaviour, kept as it
was, with the cluster verbs added to the same registry.

### The two doors

Every verb is on both doors, generated from one registry
(`internal/verbs`), and a parity test enumerates both surfaces against it: a
verb on one door and not the other is a test failure rather than something an
operator discovers. §7.3 leaves nothing on the CLI alone, so a harness with no
terminal loses no verb.

On the CLI:

```sh
hsched job add nightly-sweep "0 3 * * *" task --args '{"title":"sweep the board"}'
hsched job list                   # every schedule here, and when each fires next
hsched job disable nightly-sweep  # stop it firing, without losing it
hsched doctor                     # can this plugin work at all
hsched events --follow            # the append-only trail, as it is written
hsched dump --json                # the whole store, one document
hsched parked list                # what the policy gate deferred to you
hsched parked resolve pk-… --reject
hsched stop                       # end the one daemon serving every project
hsched version                    # this binary and the contract it satisfies
```

Over MCP, wire in the same binary:

```sh
hsched mcp
```

It registers as `herdr-sched` and serves the verbs under their bare names -
`job_add`, `job_list`, `job_remove`, `job_enable`, `job_disable`,
`trigger_add`, `trigger_list`, `trigger_remove`, `trigger_enable`,
`trigger_disable`, `doctor`, `dump`, `events`, `parked_list`,
`parked_resolve`, `stop`, and the cluster tools `cluster_status`,
`cluster_plan`, `cluster_queue`, `cluster_workers`,
`cluster_submit`, `cluster_jobs`, `cluster_explain`, `cluster_cancel`,
`cluster_detach`, `cluster_drain`, `cluster_undrain`, `cluster_events` and
`cluster_metrics`, so a
caller reads them as herdr-sched's, not as names that repeat the binary. A
dotted verb becomes an underscored tool name, and that name is a field on the
verb rather than a transformation applied at the door.

### The operator declaration

A door registered in a desktop MCP client stands in no Herdr pane, so it has no
pane to derive a principal from, and `human` is not what a plugin falls back to
when it knows nothing (§3.7). Declare the door instead, once, where the
operator writes the server configuration:

```sh
hsched mcp --operator
```

Every call that door makes is then the operator's: the §9 gate's subject, the
actor on the event trail, and who resolved a parked action. Without it a
paneless door's calls are `none`, written into the trail verbatim, so a row a
door nobody declared created says so rather than being filed under the
operator.

`hsched doctor` prints the calling principal on its `principal` line, so a
doctor call through a declared door answers `human` and one through an
undeclared door answers `none`. That is how an operator checks which of their
registrations speak for them, on either door and without reading the trail.

The CLI needs no declaration for the same reason it cannot take one. A CLI
invocation is one process per call, so the argv that ran IS the deliberate
human act §3.7 asks a paneless `human` to point at, and `hsched` outside a
Herdr pane is the operator on that alone (§3.6). Inside a pane it is that
pane's agent, on both doors: the pane is read first, always.

Four things hold, and each is pinned by a test (§7.5):

- It is read once, from the server command. `operator` is refused as a tool
  argument with `USAGE`, because a door that could be TOLD at call time that it
  speaks for the operator is a door with no identity at all (§3.2).
- It is on `hsched mcp` and nowhere else. It is not a persistent flag, and
  there is no environment variable and no `--as human`.
- **The pane wins.** A door started inside a pane is that pane's agent whatever
  it was declared, so an agent gains nothing by declaring one.
- **An ambiguous door does not start.** `hsched mcp --operator` inside a Herdr
  pane refuses with `FORBIDDEN` rather than running with two answers about who
  it is.

### The door's project

A door registered in a desktop MCP client is spawned in the client's working
directory, which is nobody's project. Name one on the server command and every
call that door makes acts in it:

```sh
hsched mcp --project ~/src/herdr-sched
```

It is a **default and not an override**: a tool call that passes `project`
acts in the one it named, and `all_projects` is untouched, the door's project
is not a second project to rank against it, so a call across every project is
still that. Without it a call names its own project or falls back to the
directory the door was spawned in, which is what it did before.

### The four globals

`--json`, `--project`, `--all-projects` and `--as` are on every verb (§3.2,
§4.2).

- `--json` prints one JSON document on stdout. A failure with it is one
  envelope carrying the contract code (§6.2); without it, a sentence goes to
  stderr and stdout stays empty.
- `--project` names one project and `--all-projects` names every one. Passing
  both is refused rather than ranked. A project is resolved in the door, not
  the daemon, because a relative path is the caller's (§4.1). On `hsched mcp`
  it is read once, from the server command, as the default described above.
  `parked list` is the one list verb that takes no `--all-projects` (§4.4) and
  refuses it: a parked action is resolved where it was parked.
- `--as` declares a `cron:`, `trigger:` or `plugin:` principal. `agent` and
  `human` are refused: both are derived from the calling process, and a caller
  who could declare one would be declaring the fact the rule exists to keep
  underived.

`--json` and `--as` are absent from the MCP door on purpose, and `--follow`
with them: a tool call already answers with a structured document, has no pane
to borrow a principal from, and answers once, because a stream is not a tool
call. `--operator` above is the door's counterpart to that `--as` exclusion and
is not a second spelling of it: it travels with the process rather than with a
call. `internal/mcpdoor`'s `Globals` table records each one, and a test walks
the whole subcommand tree against it, a flag that is neither mapped nor
excluded with a reason is a failing test.

## Jobs

A job is a five-field cron expression, one action from the vocabulary below,
and a flag saying what to do about a schedule the daemon was down for:

```sh
hsched job add nightly-sweep "0 3 * * *" task --args '{"title":"sweep the board"}'
hsched job add heartbeat "*/5 * * * *" shell --args '{"command":"./bin/health"}' --catch_up
```

The expression is the standard five fields, minute, hour, day of month,
month, day of week, with `*`, `*/n`, `a-b`, `a-b/n`, comma lists, and
three-letter month and day names. Both day fields restricted is an **or**, the
way every cron reads it. The parser is `internal/cron`, a hundred lines of
bit-setting over five bounded fields, and not a dependency: the syntax has not
moved in forty years, and herdr-dispatch's README is where the rule about what
earns a dependency is written down.

**Every expression is read in UTC**, and the arithmetic is UTC alone. A local
zone would put a schedule inside the hour a DST transition either repeats or
never has, where "3am daily" means twice one night and never another. An
operator who wants a local hour writes the UTC hour it falls on.

Everything that could not fire is refused when the job is **written**: an
expression that does not parse, an action nothing can run, an argument its
kind never takes. `--args` is one JSON object, which is what makes the same
call spell the same way on both doors.

### What a missed schedule does

The daemon holds a cursor per job: the last **scheduled instant** it was
decided for, not the clock it fired at. Everything below follows from that
one field.

- A tick that finds an instant has passed fires the action **once**, for that
  instant, and moves the cursor. Ticks run far more often than any schedule,
  so a schedule fires once however many ticks pass over it.
- The cursor moves **before** the action fires, and it moves whether the
  action fired, failed or was skipped. A daemon that dies mid-action leaves a
  schedule that did not fire, which the trail says, rather than one that
  fires again on the next start.
- A schedule missed while the daemon was **down** is **skipped**, which is
  cron's own semantics and the default here. The skip is recorded on the job's
  trail and named by `hsched doctor`, so a schedule that stopped working never
  looks like one with nothing to do.
- `--catch_up` fires such a job **once** at the next start, for the latest
  missed instant. Not once per miss: firing a backlog turns one missed nightly
  sweep into a week of them at once. The instants it stood in for are recorded
  as skipped anyway.
- A job **disabled** is passed over silently and records no skip. Re-enabled,
  it fires from the next instant rather than from the backlog.

The decision is a pure function, `internal/job`.`Due` is handed the clock,
the rows and the cursors, and answers what should happen, so every case above
is pinned by a test with no `time.Sleep` and no process in it.

## Triggers

A trigger is an inbound signal carrying one action from the vocabulary below.
There are two kinds:

```sh
hsched trigger add deploy webhook shell \
  --args '{"command":"./bin/deploy"}' --cooldown 60 --max_per_hour 10
hsched trigger add inbox watch task \
  --path /var/spool/herdr/inbox --args '{"title":"something arrived"}'
```

Everything that could not fire is refused when the trigger is **written**, the
same way a job is: an id that would not survive a URL or a `trigger:<id>`
principal, a webhook that names a path, a watch that names none, an action
nothing can run.

### The webhook secret is shown once

`trigger add` answers a webhook with its URL and its HMAC secret. That answer
is the **one** time the secret exists anywhere a caller can read it. It is not
in `trigger list`, not in `dump`, and not in the event trail, the key is kept
in a file of its own, `<state dir>/sched.secrets.json`, which no door renders.
Verifying an HMAC needs the key itself, so it cannot be hashed away; where it
is kept is the only thing left that can carry the rule, and a file `dump` does
not read cannot be forgotten the way a redaction inside `dump` could.

Copy it when you see it. A secret that is lost is a trigger removed and
written again.

```sh
body='{"ref":"refs/heads/main"}'
sig=$(printf '%s' "$body" | openssl dgst -sha256 -hmac "$SECRET" -hex | awk '{print $2}')
curl -X POST http://127.0.0.1:8797/trigger/deploy \
  -H "X-Sched-Signature: sha256=$sig" -d "$body"
```

The signature is verified over the **raw body, before anything parses a byte
of it**. What the body says decides nothing: the trigger id is in the path and
the proof is in the signature, so this door never unmarshals a payload a
stranger sent. A request that does not verify is **dropped**, it fires
nothing, and it lands on the run trail naming the trigger it was aimed at,
because a URL being probed is something an operator wants to see, and a
trigger that stopped working because a caller's secret drifted otherwise looks
exactly like one nobody is calling.

The door **defaults to loopback** (`127.0.0.1:8797`, `webhook_addr` in the
config, `off` for no door at all). The trust boundary is the local user
account (§3.5), and a scheduler that fires shell commands is not a thing to
put on a network interface, so reaching it from elsewhere is meant to be a
tunnel the operator sets up deliberately.

The port is **fixed rather than ephemeral**. A door on a port the kernel picks
would move on every daemon restart, and every caller already holding the URL
would break at the caller, where nothing here is watching. It is 8797 because
8787, which this plugin shipped first, is served by proxenos on the reference
fleet: a default nothing on the machine can bind is a default that only ever
starts degraded.

`webhook_addr` is **not** held to loopback: an operator who writes a routable
address has said so on purpose, and refusing it would be this plugin deciding
their network for them. It is not silent either, a door that binds something
other than a loopback address says so in the log, once, naming what is now
reachable off the host. `hsched doctor` prints the address the daemon
actually got, and why it got none when a port was taken, a door that cannot
bind never stops the daemon starting, because that would take every schedule
down with one port.

### The watcher polls

The path is **absolute**, and a relative one is refused when the row is
written: it would be relative to the caller's working directory, and the
daemon that stats it is somewhere else entirely.

A `watch` trigger stats its path on the daemon's own tick and fires when the
mtime, the size or the file's existence differs from the last look. The
**first** look records and does not fire: a trigger written against a file
that already exists must not fire for a change that predates it. The same
holds for a watcher re-enabled after a spell off.

Polling rather than `fsnotify` is the dependency budget doing its job: the
daemon already has this rhythm, and the cost is that a change is noticed
within one tick rather than at once, the latency a cron job already accepts.

### Both limits refuse loudly

Every trigger carries a `--cooldown` in seconds and a `--max_per_hour`, and
both are enforced in the pure core. A cooldown is what a **replayed** webhook
request meets: the same signed body sent twice fires once and is refused the
second time. The hourly limit counts firings inside the last hour and forgets
what falls out of it.

A refusal is never silent. It lands on the run trail as `sched.run.limited`
with the rule that refused and how long is left, and the caller is answered
`429` with the same words. A firing that vanished is indistinguishable from
one that never arrived.

Both decisions are made **under the store's lock**, and the cursor moves
before the action fires. The webhook door is one HTTP server answering any
number of connections, so two requests in the same millisecond would otherwise
both read a spent cooldown as unspent.

## The action vocabulary

An action is data on a job or a trigger row, a kind and its arguments, and
there are four kinds: `task` files one on the htask board, `mail` sends a
notify or an ask through hmail, `dispatch` brings a worker up through hdis,
and `shell` runs a command on the host. An unknown kind, a missing argument or
an argument of the wrong type is refused when the row is **written**, not when
it fires: a schedule that fails at 3am fails in a log nobody reads. What is
not bounded here is the length of a free-text argument, a `title`, a
`description`, a `body`, a `command`, because this plugin only carries it to
the sibling that stores it, and that sibling applies its own §5.9 bound at the
write that keeps it. A second bound written here is a number that drifts from
the one that actually decides.

Every sibling call shells out to that sibling's CLI with `--json` and never
opens its socket, so each daemon stays the only writer of its own store, and
every call declares `--as cron:<job>` or `--as trigger:<id>` (§3.2) so the
actor on the sibling's own trail is the schedule rather than "some plugin".
The pane is scrubbed out of the environment for the same reason: a call that
carried one would be attributed to this daemon instead.

A shell action runs **detached** from the tick, with its output captured onto
the run, so a slow command holds up no other schedule. Nothing is queued for
later and nothing is retried. A sibling that is unreachable is a loud failed
run on the trail carrying that sibling's own words and its §6.3 code, never a
silent skip: a schedule that quietly stopped working looks exactly like one
with nothing to do.

## The event trail

Every entity in the store keeps its own trail beside it, `<entity>_events`,
saved in the same write: a change and the event recording it can never land
one without the other. Today that is the actions the policy gate parked, in
`parked_events`, the cron jobs in `job_events`, the triggers in
`trigger_events`, and the runs a signal's actions fired, in `run_events`.

A run is a trail with no list of rows beside it, and deliberately so: the run
history IS the §8 stream, and a second table holding the same facts in another
shape is one more thing to keep in step.

An event is §8.1's shape: `{id, at, actor, project, entity, kind, detail}`
plus the name the parts spell out, `sched.<entity>.<kind>`. `hsched events`
reads the merged trail oldest first; `--since` takes an event id or a Unix
millisecond and resumes strictly after it. Each entity's trail keeps its
newest 1000 events, because the whole document is rewritten on every change,
and an id that has rotated past is **refused** rather than answered with the
whole window, a consumer resuming from it would otherwise take the window for
its own tail.

`hsched events --follow` is the subscription (§8.2). It is handed the backlog
and then every event as it is written, and the daemon says when the stream is
over, so a follower can tell "there is no more" from "I stopped being told".

### The event hook

`on_event` in the config is handed every event on stdin, run detached with its
output going nowhere. It is the operator's own program: this daemon does not
wait on it and never reads what it says. `hsched doctor` reports whether one
is configured, which is the only way to tell a hook that never fires from no
hook at all.

## The policy gate

Every verb that changes the world passes one gate before doing anything
(§9.1). This build gates nine:

```
sched.job.add
sched.job.remove
sched.job.enable
sched.job.disable
sched.trigger.add
sched.trigger.remove
sched.trigger.enable
sched.trigger.disable
sched.stop
```

`parked resolve` writes and is deliberately **not** gated: resolving a
deferral is the answer to a gate that already spoke, and gating it would let a
gate park its own resolution and strand every deferred action. The registry
requires that reason to be written down beside the verb, so an ungated write
is never reached by omission.

The gate is a command, configured in `gate_command`. It reads
`{"subject","verb","target"}` on stdin and prints `{"decision","reason"}`,
where the decision is `allow`, `deny` or `defer`. An unconfigured gate allows
(§9.2). Anything else fails **closed**: a non-zero exit, no answer, an
unreadable answer, an oversized answer or a timeout is a deny with the reason
saying which.

A gate that answers `defer` **parks** the call (§9.3), recorded, not
performed, and the caller is refused with `DENIED` carrying the `parked_id`.
`hsched parked list` is where those rows are read, and it answers with the
project you are standing in and no other (§4.4). Resolving one re-runs the
verb under the subject the gate stopped, never the resolver's, and does not
ask the gate again, because the resolution is the decision the gate deferred.
The row records who decided it, and when that principal is not the operator the
event is marked `on_behalf_of_operator` (§3.7): the trail is the whole
accountability for a verb whose authority is the operator's. A resolved action
whose verb then failed stays decided and stays visible: an action that errored
is not proof it had no effect, so the operator reads why and decides again. The
`failed` event says why and carries the same mark, because the failure is the
same operator verb the resolution was.

## Configuration

One config path per plugin, and no other: `~/.config/sched/sched.toml`, or
`$XDG_CONFIG_HOME/sched/sched.toml`, or `$SCHED_CONFIG_DIR/sched.toml`. A file
under a different directory or a different extension is a leftover, not an
alternative. `hsched doctor` prints the path it resolved and whether a file was
there.

It is read **once**, at daemon start. A change takes effect on the next start.

```toml
# How often the daemon's bounded timer runs (§11.5). Positive seconds.
tick_seconds = 30

# The §9.2 policy gate. An array, always: a bare string would assign nothing
# and read as unconfigured, which is every verb allowed by a typo, so it is
# refused at load.
gate_command = ["/usr/local/bin/herdr-policy"]

# The §8.3 hook, handed every event on stdin, run detached.
on_event = ["/usr/local/bin/notify-me"]

# Where the inbound webhook door listens. Loopback by default: the trust
# boundary is the local user account. A routable address is accepted, an
# operator who writes one has said so on purpose, and the daemon says so in
# the log, once, naming what is now reachable off this host. `off` is no
# inbound door at all, which is what a fleet with only file watchers wants.
webhook_addr = "127.0.0.1:8797"
```

Every key takes an environment override, spelled `SCHED_<KEY>`:
`SCHED_TICK_SECONDS`, `SCHED_GATE_COMMAND`, `SCHED_ON_EVENT`,
`SCHED_WEBHOOK_ADDR`, `SCHED_CLUSTER_POLICY` and so on. An override beats the
file.

The cluster keys, all optional, with their defaults:

```toml
cluster_enabled = true                 # false turns the cluster scheduler off
cluster_tick_seconds = 15              # how often to reconcile, measure and dispatch
cluster_policy = "balanced"            # balanced, memory, performance, pack, spread, local-first
cluster_include_local = true           # schedule onto this computer as the worker "local"
cluster_isolation = "auto"             # auto, worktree or none
cluster_probe_program = "~/.local/bin/hsched"  # the hsched a worker runs for the probe
cluster_probe_ttl_seconds = 20         # reuse a reading this long before probing again
cluster_probe_timeout_seconds = 20     # give up on one SSH probe after this
cluster_dispatch_timeout_seconds = 60  # how long agent start waits for the agent to be ready
cluster_dispatch_grace_seconds = 300   # after a restart, wait this long before deciding a half-done dispatch
cluster_max_attempts = 3               # dispatch attempts before a job fails
cluster_default_memory_mb = 3072       # estimate when there is no history
cluster_default_cpu = 1                # cores reserved when the job does not say
cluster_overrun_margin_pct = 20        # headroom added when an agent outgrows its reservation
cluster_history_headroom_pct = 25      # headroom added to learned estimates
cluster_ageing_minutes = 10            # a waiting job gains one priority point per this many minutes
cluster_starvation_minutes = 30        # after this, a worker is held clear for a waiting job
cluster_min_free_memory_mb = 2048      # memory every worker keeps free after placing a job
cluster_max_cpu_commit = 1.0           # cores that may be reserved, as a share of logical cores
cluster_max_load_per_core = 2.0        # five-minute load per core above which a worker takes nothing
cluster_min_free_disk_gb = 5           # free disk below which a worker takes nothing
cluster_min_battery_pct = 0            # on battery below this, take nothing; 0 is off
cluster_max_agents = 0                 # per worker; 0 is no limit
cluster_workers_file = "~/.config/sched/cluster-workers.json"
cluster_metrics_addr = "off"           # e.g. "127.0.0.1:9464" to serve /metrics; loopback only
cluster_history_window = 20            # learn estimates from this many recent runs
cluster_history_percentile = 90        # which percentile of those runs' peaks to use
cluster_max_cpu_utilisation_percent = 85  # per worker; take nothing above this CPU use; negative is off
cluster_memory_reserve_percent = 0     # per worker; keep this share of memory free as well
cluster_max_swap_pressure = "moderate" # per worker; none, low, moderate or high
cluster_max_agents_per_repo = 0        # active agents in one repository across the cluster; 0 is no limit
cluster_max_queue_size = 100           # refuse submits once this many jobs wait
cluster_queue_enabled = true           # false refuses every submit; running jobs carry on
cluster_worktree_retention = "retain-on-failure"  # retain-on-failure, always-retain, always-remove or age
cluster_worktree_max_age_hours = 72    # with "age", remove worktrees this old
cluster_worktree_remove_dirty = false  # remove worktrees with uncommitted changes too
cluster_delete_branches = false        # also delete the scheduler branch (only if merged) after removing its worktree
cluster_lost_after_seconds = 1800      # unreachable this long, a worker's jobs are lost
cluster_retry_lost = true              # retry lost jobs submitted with --restartable
cluster_machine_status_seconds = 60    # how often to read herdr machine status; 0 is off
cluster_pressure_action = "recommend"  # recommend, or terminate-disposable
cluster_pressure_grace_seconds = 120   # critical pressure lasting this long counts as sustained
cluster_verify_identity = true         # stop placing on a worker whose identity changed
cluster_owner_checks = true            # only a job's submitter (or you) may cancel or detach it
cluster_explanations = true            # keep a placement explanation per job
```

The worker defaults (`cluster_min_free_memory_mb` through `cluster_max_agents`,
and `cluster_max_cpu_utilisation_percent`, `cluster_memory_reserve_percent`
and `cluster_max_swap_pressure`) apply to every worker unless `cluster-workers.json` says otherwise for that
worker. A value that does not parse stops the cluster scheduler, with the
reason in the log and in every cluster verb's refusal, while cron jobs and
triggers carry on.


### Where everything lives

The short name is `sched`, and everything nameable is derived from it:

| What | Where |
| --- | --- |
| Config | `${XDG_CONFIG_HOME:-~/.config}/sched/sched.toml` |
| State dir | `${XDG_STATE_HOME:-~/.local/state}/sched`, mode `0700` |
| Socket | `<state dir>/sched.sock` |
| Lock | `<state dir>/sched.lock` |
| Log | `<state dir>/sched.log` |
| Store | `<state dir>/sched.json` |
| Webhook secrets | `<state dir>/sched.secrets.json`, mode `0600` |
| Cluster ledger | `<state dir>/cluster.db` (SQLite, WAL), mode `0600` |
| Per-worker settings | `<config dir>/cluster-workers.json` (optional) |

`SCHED_STATE_DIR` and `SCHED_CONFIG_DIR` override the two directories and take
precedence over the XDG variables. They are how the tests isolate, and how an
operator asks for a second store on purpose rather than by accident.

Herdr injects `HERDR_PLUGIN_STATE_DIR` and `HERDR_PLUGIN_CONFIG_DIR` into what
*it* spawns and into no managed pane, so this plugin reads neither: honouring
them would give one plugin two stores that never see each other's rows. What
`doctor` does instead is **name** the directories a store could have been left
in by a build that did read them. It never names the store in use, and it
never deletes anything.

## Reading the store

The cluster ledger (`cluster.db`) is SQLite, because it is where concurrent
reservations are decided and that needs real transactions. The cron and
trigger store below is unchanged from upstream.

The contract's §5.1 store is SQLite. The cron and trigger store is a JSON
document instead: the
whole set is a handful of rows held in memory anyway, rewritten whole on every
change, read by one process under the daemon's own lock. A SQLite driver is a
large dependency for a file that never needs a query, a schema or a second
reader, and the budget below is what makes that a decision rather than a
default. The document is written through a temp file and renamed, so a crash
mid-write leaves the previous one intact, and a document from a version this
binary does not know is refused rather than overwritten.

`hsched dump --json` is §5.8: the whole store in one document, with the file it
is written to named, so a reader who wants it without this binary knows where
to look.

## The dashboard, on a key

One popup: the cluster's workers and its newest jobs, every schedule with
when it fires next, every trigger with what is
holding it down, and the tail of the run trail. It is on Herdr's plugin menu,
and it is a plugin action, so a key can reach it. It is read-only: a popup
carries no `HERDR_PANE_ID`, so its principal is the human, and the CRUD stays
on the gated verbs. The script needs `jq` and says so rather than drawing an
empty dashboard.

Herdr does not load a plugin's keybindings, a plugin does not get to claim
keys in your config. Copy the block from `keybindings.toml` into
`~/.config/herdr/config.toml` and change the key to whatever is free:

```sh
cat keybindings.toml >> ~/.config/herdr/config.toml
```

```toml
[[keys.command]]
key = "prefix+shift+c"
type = "plugin_action"
command = "herdr-sched.dashboard"
description = "Open the sched dashboard"
```

`type = "plugin_action"` is what makes `command` an action rather than a shell
command; the value is `<plugin id>.<action id>`, both from `herdr-plugin.toml`.
Pressing the key when the dashboard is already open is a no-op rather than an
error, so it can be leaned on.

## Security

- Every remote action is a fixed argument vector handed to `herdr` or `ssh`,
  never a shell string built from your input. The prompt travels as one
  argument to `herdr agent prompt`, and over Herdr's own SSH bridge as a JSON
  field. Repository paths and agent arguments are checked against a
  conservative character set before anything runs; agent names, kinds, pane
  and machine ids are checked against Herdr's own formats.
- Remote access is Herdr's saved machines and your OpenSSH configuration,
  with `BatchMode=yes` so a probe never prompts. No keys or passwords are
  stored by this plugin.
- Prompts are kept in the ledger only so they can be sent when the job is
  dispatched. They are never written to the log, the event trail or a CLI
  listing; only their length and SHA-256 are.
- The state directory is `0700`, the socket and `cluster.db` are `0600`, and
  a ledger file other users can read is refused at start.
- Mutating cluster verbs go through the same policy gate as everything else:
  `sched.cluster.submit`, `sched.cluster.cancel`, `sched.cluster.detach`,
  `sched.cluster.drain`, `sched.cluster.undrain` and
  `sched.cluster.reregister`.
- Each job records who submitted it (the human, or a cron, trigger or agent
  principal), and with `cluster_owner_checks` on only that principal or the
  human can cancel or detach it. This stops one agent cancelling another's
  work by mistake; it is not a security boundary, because anyone who can
  reach the `0600` socket is already you.
- Clone URLs are refused if they carry credentials, start with `-`, or use a
  transport other than https, ssh or scp-style `user@host:path`. Git
  operations on a worker run `hsched cluster gitop` with quoted, checked
  arguments over SSH, never a shell string.
- No telemetry. The metrics endpoint is off by default and only binds a
  loopback address.

## Limitations

Be clear about what this does not do yet:

- **macOS collectors are untested on real hardware.** They compile and their
  parsers have unit tests against sample `sysctl`, `pmset` and `ps` output, but this build was developed and tested on Linux. Memory
  pressure on macOS comes from `kern.memorystatus_vm_pressure_level`, and
  per-agent memory from `ps`.
- **Windows has no live collector.** Declare a Windows worker as static.
  Placement then uses declared capacity and the scheduler's own reservations,
  not live readings.
- **At most once, not exactly once.** A job whose dispatch was interrupted
  after the agent start was requested fails with `dispatch_interrupted`
  rather than being retried, since it may already be running. Resubmit it if
  needed. One interrupted before that point is cleaned up and requeued.
- **No graceful stop.** Herdr 0.9.3 has no `agent stop`, so terminating a
  disposable job under pressure closes its workspace. There is no pause.
- **No live migration or checkpoints.** A running agent stays where it
  started. Herdr does not expose agent sessions in a form another machine can
  resume, so to move work, `detach` it and submit it again, or submit it as
  `--restartable` so a lost run starts afresh elsewhere.
- **Only the agent kinds Herdr knows.** `herdr agent start` takes a fixed
  `--kind`, so an arbitrary command line cannot be scheduled as an agent.
- **Attach is a command, not a connection.** Herdr will not attach to an
  agent across `--machine`, so `cluster attach` prints the commands to run:
  `herdr --remote <ssh target>` to open the worker's Herdr, then
  `herdr agent focus hsc-j<id>` inside it.
- **Polling, not events.** The scheduler reconciles on a timer
  (`cluster_tick_seconds`). Herdr's event subscription is not durable across
  restarts, so it is not relied on.
- **A plan is a snapshot.** `cluster plan` uses the same readings and rules
  as a real pass, but memory moves, so a job planned to start now can still
  queue a few seconds later, and the other way round.
- **Prometheus metrics are a text endpoint and a CLI verb only.** There is no
  bundled Grafana dashboard.
- **GitHub Actions CI** is described in [docs/ci/ci.yml](docs/ci/ci.yml) but
  not enabled, see that file for why and how to turn it on.

## Building and testing

```sh
make build          # bin/hsched
make test           # the loop, in seconds: -short, nothing spawned
make test-full      # the gate: the above plus go vet, a cross-compile vet, -race
make e2e            # layer 3: the shipped binary through its own scripts
make release-check  # test-full, build, and e2e with the skip turned into a failure
make install        # go install ./cmd/hsched
```

`make test-full` is the gate: nothing is committed on a green `make test`
alone. `make e2e` is out of the gate on purpose, it builds and runs the
shipped binary and its scripts, and it skips loudly naming what was missing
rather than passing quietly. `SCHED_E2E_REQUIRED=1` turns that skip into a
failure, which is what `make release-check` does before a release tag.

Tests never reach the operator's live Herdr, boards, mailbox, config or state
(§12.3). `internal/testenv` replaces `PATH` with a directory of stand-in
sibling binaries, replacing it rather than prepending to it, so a call whose
fake was never written fails as "not found" instead of quietly reaching the
real thing, and points the state and config directories at temporary ones.

## Dependencies

The standard library, plus:

- `github.com/spf13/cobra` for the CLI door (from upstream)
- `github.com/modelcontextprotocol/go-sdk` for the MCP door (from upstream)
- `modernc.org/sqlite` for the cluster ledger, a pure Go SQLite with no cgo,
  so the binary still cross-compiles for macOS and Linux
- `golang.org/x/sys` for `sysctl` on macOS

There is no TOML library: `internal/config/toml.go` is upstream's
hand-written parser for the subset this document needs, which is why the
cluster settings are flat `cluster_*` keys and per-worker settings live in a
separate JSON file.

## Credits and licence

MIT. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

This is a derivative of [herdr-sched](https://github.com/husniadil/herdr-sched)
by Husni Adil Makmur (MIT), whose commit history is preserved here. Everything
under `internal/cluster`, the cluster verbs and the cluster documentation are
additions. [Herdr](https://github.com/herdrdev/herdr) (Apache-2.0) is
called as an external program and is not included.
