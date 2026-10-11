---
name: herdr-cluster
description: Spread coding agents across the computers saved in Herdr without picking machines by hand. Use before starting more than one coding agent, when asked to run work in parallel, fan out, use the other machine, or check which computer has room, and when an agent you started is waiting. Trigger words - cluster, spread, fan out, parallel agents, which machine, capacity, queue, hsched cluster.
---

# Herdr cluster

The `herdr-sched` plugin places coding agents on whichever machine saved in
Herdr has safe room for them: memory after reservations, memory pressure, CPU,
load, disk, battery and thermal state. You say what to run. It decides where,
queues what does not fit, and starts queued work on its own as room frees up.

Use the MCP tools when the `herdr-sched` server is connected. Otherwise use the
same verbs on the CLI as `hsched cluster <verb>`; add `--json` for a document
you can parse.

## The loop

1. **Check capacity.** `cluster_status` (`hsched cluster status`). It lists each
   worker's state, free memory, the memory it always keeps free, agents running
   and roughly how many more default-size agents fit.
2. **Plan before you fan out.** `cluster_plan` (`hsched cluster plan`) with the
   jobs you intend to start. It answers per job: `would_place` on which worker
   and why, `would_queue` and what it is waiting for, or `would_refuse` and why.
   It launches nothing and records nothing, so call it as often as you like.
   - Same agent several times: `{"kind": "claude", "repo": "/abs/repo", "count": 4}`
   - Mixed batch: `{"jobs": [{"kind": "claude", "repo": "/abs/a", "priority": 20}, {"kind": "codex", "repo": "/abs/b", "memory_mb": 4096}]}`
3. **Tell the user the plan** when it matters: how many start now, on which
   machines, and how many will queue. Ask before submitting a large batch they
   did not ask for.
4. **Submit.** `cluster_submit` once per agent with `kind`, `repo`, `prompt` and
   a `priority` (-100 to 100, higher first; waiting jobs gain priority with
   age). Submit the most important work first.
5. **Follow up.** `cluster_jobs` shows where each agent runs (worker and Herdr
   pane). `cluster_queue` lists waiting jobs in order with why each waits.
   `cluster_explain <job>` gives the full reasoning for one job.

## Rules

- **Never bypass a hard limit.** Do not pin a job to a full machine with
  `machine`, and do not lower `memory_mb` below what the agent really uses to
  squeeze it in. A queued job is the scheduler working as intended; it starts
  by itself when capacity frees.
- Leave `memory_mb` out unless you know better. The scheduler learns what each
  agent kind uses in each repository and falls back to a safe default.
- Use absolute repository paths, or paths starting with `~/`. The repository
  must exist at that path on the machine that runs it.
- Only pin with `machine`, `os`, `arch` or `labels` when the work truly needs
  that machine (a macOS build, a GPU, a big repo only cloned there).
- Two agents in the same repository on one machine get separate git worktrees
  automatically (`isolation` = `auto`). Do not set `isolation` to `none` for
  parallel edits.
- Ask the user before `cluster_drain`, `cluster_undrain`, `cluster_cancel`,
  `cluster_detach` or `stop`. The scheduler never kills a running agent.
- If a tool answers `DENIED` with a `parked_id`, a policy gate wants the user to
  decide. Tell them; do not resolve it yourself.

## Reading the answers

- `would_refuse` with "no worker could ever run this job" means the job is
  bigger than every machine, names a repository missing everywhere, or asks
  for an agent, OS, label or machine nobody has. Fix the request.
- A warning such as "claude was not found on the PATH the probe saw" does not
  stop placement, but check that the agent is installed on that machine.
- A worker that is `unreachable`, `auth_required` or in `backoff` takes no work.
  `cluster_workers` with `refresh` shows the last error. Tell the user rather
  than retrying in a loop.
