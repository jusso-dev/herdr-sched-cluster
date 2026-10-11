<!-- Paste into AGENTS.md, CLAUDE.md or your agent's global instructions. -->
## Spreading agents across machines

This Herdr setup has the `herdr-sched` cluster scheduler (MCP server
`herdr-sched`, CLI `hsched cluster`). Before starting more than one coding
agent:

1. Call `cluster_status` to see free capacity on each machine.
2. Call `cluster_plan` with the jobs you intend to run (kind, repo, optional
   memory_mb and priority, or kind, repo and count). It is a dry run and
   launches nothing.
3. Submit each agent with `cluster_submit` (kind, repo, prompt, priority). Do
   not choose the machine yourself unless the work needs a specific one.
4. Use `cluster_queue` and `cluster_explain <job>` to say why something waits.

Never bypass a hard limit by pinning to a full machine or understating
`memory_mb`. Queued jobs start on their own. Ask before draining a machine,
cancelling or detaching a job, or stopping the scheduler.
