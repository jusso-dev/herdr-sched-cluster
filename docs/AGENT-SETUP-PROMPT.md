# Agent setup prompt

Copy everything inside the box below and paste it into Claude Code, Codex or
OpenCode, running on the machine you will submit work from (the
coordinator). The agent installs the plugin, connects itself to the
scheduler, helps you add your other machines, and finishes with a dry run. It
starts no real agents and asks before anything that changes another machine
or stops work.

Every command in the prompt was checked against herdr-sched-cluster v0.5.0
and Herdr 0.9.3.

```text
Please set up the herdr-sched-cluster plugin on this machine so I can spread
coding agents across my computers. Work through the steps below in order.
Show me each command before you run it and tell me plainly what happened.
If a step fails, stop, explain the error in plain words and what I can do
about it, and wait for me. Do not guess at fixes that change my system.

Ground rules for the whole job:
- Never generate, copy or install SSH keys, and never edit ~/.ssh, unless I
  say yes to that specific change first.
- Never drain a worker, cancel or detach a job, stop the daemon, or uninstall
  a plugin without asking me first.
- Never submit a real job. This setup ends with a dry run only.
- Never get round a scheduler limit (memory, CPU, pressure, max agents) by
  pinning work to a full machine or understating memory. If the plan says a
  job would queue or be refused, tell me why instead.

1. Check prerequisites.
   - Run `herdr --version`. It must be 0.9.2 or later. If it is older or
     missing, stop and tell me to install or update Herdr from
     https://herdr.dev first.
   - Check whether ~/.local/bin is on PATH (`echo "$PATH"`). If it is not,
     tell me the line to add to my shell profile and ask before adding it.
   - Go is optional. Prebuilt binaries are used for macOS (Apple Silicon and
     Intel) and Linux when Go is not installed, so do not install Go.

2. Check what is already installed.
   - Run `herdr plugin list`.
   - If it shows herdr-sched from github:husniadil/herdr-sched (the upstream
     plugin), explain that this plugin replaces it in place with the same id,
     binary, config and store, so existing cron jobs and triggers carry over.
     Ask me whether to remove upstream first. If I agree, run
     `herdr plugin action invoke herdr-sched.stop` and then
     `herdr plugin uninstall herdr-sched`.
   - If it already shows herdr-sched from jusso-dev/herdr-sched-cluster,
     treat the next step as an upgrade.

3. Install and start the plugin.
   - Run `herdr plugin install jusso-dev/herdr-sched-cluster --yes`.
   - Run `herdr plugin action invoke herdr-sched.restart` to start the
     daemon.
   - Run `hsched version` and `hsched doctor` and tell me the version and
     anything doctor warns about.

4. Connect coding agents to the scheduler.
   - Run `herdr plugin action invoke herdr-sched.setup-agents`. It registers
     the herdr-sched MCP server with whichever of Claude Code, Codex and
     OpenCode are installed, and installs the skill for Claude Code.
   - Check with `claude mcp list`, `codex mcp list` or `opencode mcp list`
     for each one that is installed, and tell me which ones show herdr-sched.
   - Tell me that the agent I am talking to may need restarting to see the
     new tools.

5. Add my other machines.
   - Run `herdr machine list` and show me what is already saved.
   - Ask me which other machines I want to add, and for each one its SSH
     target (for example me@mac-mini.local or a Host alias from
     ~/.ssh/config) and a short name.
   - For each, check key-based SSH without prompting:
     `ssh -o BatchMode=yes -o ConnectTimeout=10 <target> true`.
     If it fails, explain the error (unknown host, permission denied, host key
     not yet trusted, timeout) and what I need to do, and wait. Do not create
     or copy keys unless I ask you to.
   - Once SSH works, run `herdr machine add <target> --label <name>`.
   - The plugin must be installed on every worker too, because the scheduler
     probes each worker by running ~/.local/bin/hsched there. Ask me whether
     to install it over SSH for me. If I agree, run
     `ssh <target> 'herdr plugin install jusso-dev/herdr-sched-cluster --yes && herdr plugin action invoke herdr-sched.restart'`
     and then check with `ssh <target> '~/.local/bin/hsched cluster probe --json'`.
     If herdr is not on the PATH for a non-interactive SSH shell, tell me and
     let me run those two commands on that machine myself.
   - Run `herdr machine status` and tell me which machines are reachable.

6. Map repository paths if they differ.
   - Ask me where my repositories live on this machine and on each worker.
     If they are under the same path relative to my home directory (for
     example ~/src on all of them), nothing is needed.
   - If a worker keeps them somewhere else, add a path_map for that worker in
     ~/.config/sched/cluster-workers.json, for example
     {"mini": {"path_map": {"~/src": "/Volumes/work/src"}}}. Keep any
     settings already in that file, show me the whole file before saving,
     and then run `herdr plugin action invoke herdr-sched.restart` so the
     daemon reads it.

7. Look at the cluster.
   - Run `hsched cluster workers --refresh` and `hsched cluster status`.
   - Summarise each machine for me: state, free memory, CPU, memory pressure
     and roughly how many more default-size agents fit. If a worker shows
     unreachable, auth_required, herdr_unreachable, incompatible or backoff,
     explain what that means and what I can do.

8. Dry run.
   - Ask me for a repository path to try, or use the current directory if it
     is a Git repository.
   - Run `hsched cluster plan --kind claude --repo <repo> --count 5`, using
     codex or opencode instead of claude if that is the agent I use.
   - Explain the result in plain words: which jobs would start and where,
     which would queue and what they wait for, and which would be refused and
     why. Make clear that nothing was started.

9. Stop and summarise.
   - Tell me what was installed, which agents are connected, which machines
     are in the cluster and their state, any path mappings you added, and
     anything that still needs my attention.
   - Show me how I would submit a real job, for example
     `hsched cluster submit claude <repo> --prompt "..."`, or that I can just
     ask you to spread work across my machines. Do not submit anything until
     I ask.
```
