// Package verbs is the one registry both doors are built from. The CLI
// subcommands and the MCP tools are generated from this list rather than
// written twice, and a parity test enumerates both surfaces against it: a
// verb that exists on one door and not the other is a test failure, not
// something an operator discovers.
package verbs

// String, Bool, Int and Object are the kinds of argument a verb takes. The
// type is in the registry rather than at a door, so a switch is a flag on the
// CLI and a boolean on MCP without either door deciding that for itself.
const (
	String = "string"
	Bool   = "bool"
	Int    = "int"
	// Object is a set of named values — an action's own arguments, whose
	// names depend on which action it is. On the MCP door it is an object in
	// the schema, and on the CLI it is a flag carrying one JSON document,
	// because a repeatable --arg key=value is a shape the other door has no
	// way to spell.
	Object = "object"
	// List is an ordered list of objects, each shaped by the arg's Items. On
	// the MCP door it is an array in the schema, and on the CLI it is a flag
	// carrying one JSON array, for the same reason Object is.
	List = "list"
)

// Arg is one parameter of a verb. A positional arg is a CLI positional and an
// ordinary named field over MCP and the socket.
type Arg struct {
	Name       string
	Type       string
	Desc       string
	Required   bool
	Positional bool
	// Items describes the fields of each element of a List argument.
	Items []Arg
}

// Verb is one operation, in every surface it appears in.
type Verb struct {
	// Name is the verb on the socket, dotted for a namespaced verb:
	// parked.list, the way all three siblings spell theirs.
	Name string
	// MCP is the tool name: the verb alone, with dots as underscores. The
	// door serves bare verbs, so a caller reads doctor as herdr-sched's
	// doctor rather than as a name that repeats the binary. It is a field
	// and not a transformation applied at the door, so an absence from the
	// agent surface is a decision written beside the verb.
	MCP string
	// CLI is the subcommand path, e.g. {"parked", "list"}.
	CLI []string
	// Short is the one-line help both doors show.
	Short string
	// Long is what the MCP tool description adds for a caller that cannot
	// ask a follow-up question.
	Long string
	// Args is the parameter list, in CLI positional order.
	Args []Arg
	// NoAutostart sends the verb to whatever daemon is already listening
	// and refuses when none is, instead of starting one.
	NoAutostart bool
	// Mutates says the verb changes the world, which is what §9.1 puts
	// behind the policy gate. A verb that only reads is neither gated nor
	// asked to explain itself.
	Mutates bool
	// Gated is the §9.4 verb name handed to the policy gate, `<short
	// name>.<verb>` with the short name `sched` (§13.2). Empty means this
	// verb passes no name, which a Mutates verb must justify in Ungated.
	Gated string
	// Ungated is why a verb that writes passes no name to the policy gate.
	// Required exactly when Mutates is true and Gated is empty, so the
	// decision is written down beside the verb rather than inferred from
	// its absence.
	Ungated string
}

// All is the registry. Order is the order the CLI lists them in.
//
// Beside the five job verbs and the five trigger verbs are the seven §13 makes
// every sibling spell the same way, minus `status` and `sweep`, which are held
// back rather than stubbed. `sweep` is a reconciliation pass and neither half
// has one to run: a cursor on a job row and a stamp on a trigger row ARE the
// reconciliation, re-read from the store at every start. `status` waits on the
// same thing it always waited on, which is a decision about what it should say
// rather than a missing half; `doctor` carries the counts in the meantime.
// docs/contract-notes.md holds both reasons. A verb that exists and answers
// nothing teaches a caller a shape it will have to unlearn.
var All = []Verb{
	{
		Name: "doctor", MCP: "doctor", CLI: []string{"doctor"},
		Short: "Report whether the scheduler can work at all",
		Long: "Answers with this binary's version and the contract revision it " +
			"satisfies, the socket, the state and config directories it resolved, " +
			"whether a config file was found and where, the §9 policy gate and what " +
			"it has parked, and the §8 trail's depth. Run it first when something " +
			"else refuses.",
	},
	{
		Name: "dump", MCP: "dump", CLI: []string{"dump"},
		Short: "Print the whole store as JSON",
		Long: "Everything this daemon remembers across restarts, in one document " +
			"(§5.8): the cron jobs, the inbound triggers, the actions the policy " +
			"gate parked, and each of those entities' own §8 trail beside it, with " +
			"the run trail that has no rows beside it. A webhook's HMAC secret is " +
			"NOT here — it is kept in a file this document does not hold, so no door " +
			"that renders the document can print one. It is the daemon's own live " +
			"set rather than a re-read of the file, so it is what the next save will " +
			"write.",
	},
	{
		Name: "events", MCP: "events", CLI: []string{"events"},
		Short: "Read the append-only trail of what the scheduler did",
		Long: "The §8.1 events this plugin owns and nothing else records. Without " +
			"since this reads from the BEGINNING of what the daemon still holds, " +
			"oldest first, so a consumer that resumes passes since with the last " +
			"event id it saw, or a Unix-millisecond timestamp, or it reads " +
			"everything again. The trail is bounded: an id that has rotated out of " +
			"it is refused rather than answered with the whole window. On the CLI " +
			"--follow turns this into a subscription that keeps printing; a tool " +
			"call answers once and has no follow, because a stream is not a tool " +
			"call.",
		Args: []Arg{
			{Name: "since", Type: String, Desc: "An event id, or a Unix-millisecond timestamp, to resume after"},
			{Name: "limit", Type: Int, Desc: "Stop after this many events"},
		},
	},
	{
		Name: "job.add", MCP: "job_add", CLI: []string{"job", "add"},
		Short: "Write down a schedule and what it fires",
		Long: "A job is a five-field cron expression, one action from the vocabulary, " +
			"and whether a schedule missed while the daemon was down should fire once " +
			"at the next start. The expression is read in UTC, always: a local zone " +
			"would put a schedule inside the hour a DST transition either repeats or " +
			"never has. Everything that could not fire is refused HERE rather than at " +
			"3am — an expression that does not parse, an action nothing can run, an " +
			"argument its kind does not take. Every call the job makes declares itself " +
			"as `cron:<id>` (§3.2), so the actor on the sibling's own trail is the " +
			"schedule rather than this plugin.",
		Args: []Arg{
			{Name: "id", Type: String, Desc: "The name for this schedule; it is the id half of the `cron:<job id>` principal its calls declare", Required: true, Positional: true},
			{Name: "schedule", Type: String, Desc: "The five-field cron expression, read in UTC: minute hour day-of-month month day-of-week", Required: true, Positional: true},
			{Name: "action", Type: String, Desc: "What it fires: task, mail, dispatch or shell", Required: true, Positional: true},
			{Name: "args", Type: Object, Desc: "The action's own arguments: a task takes title, an hmail takes to and body, a shell takes command"},
			{Name: "catch_up", Type: Bool, Desc: "Fire ONCE at the next start when the daemon was down for a scheduled instant; without it the miss is skipped, which is cron's own semantics"},
		},
		Mutates: true,
		Gated:   "sched.job.add",
	},
	{
		Name: "job.list", MCP: "job_list", CLI: []string{"job", "list"},
		Short: "List the schedules and when each one next fires",
		Long: "Every job in this project, with its expression, its action, whether it " +
			"is enabled, when it last fired and when it fires next. A job that is " +
			"disabled is listed and says so: it is kept rather than removed, because " +
			"disabling is not deleting. Pass --all-projects to read every project's.",
	},
	{
		Name: "job.remove", MCP: "job_remove", CLI: []string{"job", "remove"},
		Short: "Take a schedule off for good",
		Long: "The row and its cursor go; the job's trail stays, because what a " +
			"schedule DID is not undone by removing it. To stop a job without losing " +
			"it, disable it instead.",
		Args: []Arg{
			{Name: "id", Type: String, Desc: "The job to remove", Required: true, Positional: true},
		},
		Mutates: true,
		Gated:   "sched.job.remove",
	},
	{
		Name: "job.enable", MCP: "job_enable", CLI: []string{"job", "enable"},
		Short: "Let a schedule fire again",
		Long: "A job enabled after a spell disabled does NOT fire what it missed: the " +
			"cursor moves past the instant that has just passed, and the first instant " +
			"after this one is what fires. Enabling a job that is already enabled changes " +
			"nothing and records nothing.",
		Args: []Arg{
			{Name: "id", Type: String, Desc: "The job to enable", Required: true, Positional: true},
		},
		Mutates: true,
		Gated:   "sched.job.enable",
	},
	{
		Name: "job.disable", MCP: "job_disable", CLI: []string{"job", "disable"},
		Short: "Stop a schedule from firing, without losing it",
		Long: "The row stays and the tick passes over it. Nothing is recorded as " +
			"skipped while it is off — an operator who disabled a job is not owed a " +
			"skip record every night — so a job re-enabled later fires from the next " +
			"instant and not from the backlog.",
		Args: []Arg{
			{Name: "id", Type: String, Desc: "The job to disable", Required: true, Positional: true},
		},
		Mutates: true,
		Gated:   "sched.job.disable",
	},
	{
		Name: "trigger.add", MCP: "trigger_add", CLI: []string{"trigger", "add"},
		Short: "Write down an inbound trigger and what it fires",
		Long: "A trigger is a webhook on a server-issued URL, or a watcher on a host " +
			"path, carrying one action from the vocabulary. A webhook answers with its " +
			"URL and its HMAC secret, and the secret is shown HERE and nowhere else " +
			"ever again: copy it now, because no list, get or dump will print it. Every " +
			"request is verified against that secret over the raw body before anything " +
			"parses a byte of it, and one that does not verify is dropped onto the trail " +
			"and fires nothing. A watcher polls its path on the daemon's own tick and " +
			"fires when the file changes; the first look records what is there and does " +
			"not fire. Both carry a cooldown and a max-fires-per-hour, and both refuse " +
			"loudly onto the trail rather than silently. Every call the trigger makes " +
			"declares itself as `trigger:<id>` (§3.2).",
		Args: []Arg{
			{Name: "id", Type: String, Desc: "The name for this trigger; it is the id half of the `trigger:<id>` principal its calls declare, and the last segment of a webhook's URL", Required: true, Positional: true},
			{Name: "kind", Type: String, Desc: "webhook for an inbound request, or watch for a file watcher", Required: true, Positional: true},
			{Name: "action", Type: String, Desc: "What it fires: task, mail, dispatch or shell", Required: true, Positional: true},
			{Name: "path", Type: String, Desc: "The host path a watch polls; a webhook takes none"},
			{Name: "args", Type: Object, Desc: "The action's own arguments: a task takes title, an hmail takes to and body, a shell takes command"},
			{Name: "cooldown", Type: Int, Desc: "Seconds after a firing in which the next one is refused; this is what a replayed webhook request meets"},
			{Name: "max_per_hour", Type: Int, Desc: "How many firings this trigger allows in any hour; zero is no limit"},
		},
		Mutates: true,
		Gated:   "sched.trigger.add",
	},
	{
		Name: "trigger.list", MCP: "trigger_list", CLI: []string{"trigger", "list"},
		Short: "List the inbound triggers and what each one is waiting for",
		Long: "Every trigger in this project, with its kind, its action, its limits, how " +
			"many times it has fired in the last hour and when it last did. A webhook " +
			"shows the URL it is reached on and NEVER its secret, which was shown once " +
			"at creation. Pass --all-projects to read every project's.",
	},
	{
		Name: "trigger.remove", MCP: "trigger_remove", CLI: []string{"trigger", "remove"},
		Short: "Take an inbound trigger off for good",
		Long: "The row goes, and a webhook's secret goes with it: a key with no trigger " +
			"left to use it is a secret kept for nothing. The trigger's trail stays, " +
			"because what it DID is not undone by removing it. To stop a trigger " +
			"without losing it, disable it instead.",
		Args: []Arg{
			{Name: "id", Type: String, Desc: "The trigger to remove", Required: true, Positional: true},
		},
		Mutates: true,
		Gated:   "sched.trigger.remove",
	},
	{
		Name: "trigger.enable", MCP: "trigger_enable", CLI: []string{"trigger", "enable"},
		Short: "Let an inbound trigger fire again",
		Long: "The webhook URL answers again and the watcher looks again. A watcher " +
			"re-enabled does NOT fire for what changed while it was off: it records " +
			"what is there now, the same way its first look did.",
		Args: []Arg{
			{Name: "id", Type: String, Desc: "The trigger to enable", Required: true, Positional: true},
		},
		Mutates: true,
		Gated:   "sched.trigger.enable",
	},
	{
		Name: "trigger.disable", MCP: "trigger_disable", CLI: []string{"trigger", "disable"},
		Short: "Stop an inbound trigger from firing, without losing it",
		Long: "The row stays, the watcher passes over it, and the webhook URL still " +
			"verifies what arrives and then refuses it onto the trail. A signal held " +
			"down by a disabled trigger is recorded rather than dropped: an operator " +
			"who forgot they disabled it has somewhere to read that.",
		Args: []Arg{
			{Name: "id", Type: String, Desc: "The trigger to disable", Required: true, Positional: true},
		},
		Mutates: true,
		Gated:   "sched.trigger.disable",
	},
	{
		Name: "parked.list", MCP: "parked_list", CLI: []string{"parked", "list"},
		Short: "List the actions the policy gate deferred to the operator",
		Long: "A gate that answers defer parks the call instead of performing it " +
			"(§9.3) and refuses with DENIED carrying the parked_id. This is where " +
			"those rows are read: who asked, which gated verb, what target, the " +
			"reason the gate gave, and whether the action is still waiting or was " +
			"resolved and then failed. A failed row is not finished business — the " +
			"operator decided and the verb did not run. This is the one list verb " +
			"that takes no all_projects (§4.4): a parked action is resolved where " +
			"it was parked, so the rows are this project's and asking for every " +
			"project is refused with USAGE.",
	},
	{
		Name: "parked.resolve", MCP: "parked_resolve", CLI: []string{"parked", "resolve"},
		Short: "Let a parked action through, or reject it",
		Long: "Re-runs the parked verb under the subject the gate stopped, never " +
			"the resolver's (§9.3), and skips the gate, because the resolution IS " +
			"the decision the gate deferred. The row records who resolved it. With " +
			"--reject the verb never runs and the row is closed. This is the " +
			"operator's authority and therefore advice rather than a refusal this " +
			"door makes (§3.7): confirm with the user before resolving one on their " +
			"behalf.",
		Args: []Arg{
			{Name: "id", Type: String, Desc: "The parked action id, as DENIED reported it", Required: true, Positional: true},
			{Name: "reject", Type: Bool, Desc: "Close the action without running the verb"},
		},
		Mutates: true,
		Ungated: "resolving a deferral is the answer to a gate that already spoke; gating it would let a gate park its own resolution and strand every deferred action",
	},
	{
		Name: "stop", MCP: "stop", CLI: []string{"stop"},
		Short: "Ask the running daemon to shut down",
		Long: "The daemon stops ticking, closes its socket, drops its lock and " +
			"exits. Answers CONFLICT as NOT_RUNNING when no daemon is listening: " +
			"nothing is started just to be stopped. It is a brake on the WHOLE " +
			"scheduler and not on one schedule: nothing this plugin drives fires " +
			"again until a daemon is started. Confirm with the operator before " +
			"calling it, the same way you would before any act whose blast radius " +
			"is everyone else's work.",
		NoAutostart: true,
		Mutates:     true,
		Gated:       "sched.stop",
	},
	{
		Name: "cluster.workers", MCP: "cluster_workers", CLI: []string{"cluster", "workers"},
		Short: "List the machines the cluster can place agents on, with their resources",
		Long: "Every worker: this computer (unless cluster_include_local is false) and " +
			"every enabled machine saved with `herdr machine add`. For each it gives the " +
			"state (ready, draining, disabled, unreachable, auth_required, incompatible, " +
			"backoff), the latest resource reading (memory available and pressure, swap, " +
			"CPU use and load, disk, power, thermal state, agents found on PATH), what " +
			"the scheduler has reserved there, and the last probe error. Readings are " +
			"cached for cluster_probe_ttl_seconds unless refresh is set.",
		Args: []Arg{
			{Name: "refresh", Type: Bool, Desc: "Probe every worker now instead of using a cached reading"},
		},
	},
	{
		Name: "cluster.status", MCP: "cluster_status", CLI: []string{"cluster", "status"},
		Short: "Summarise cluster capacity and work in one answer",
		Long: "Read-only. Start here before spreading work. For each worker: state, OS and " +
			"architecture, memory total and free after reservations, the memory it always " +
			"keeps free, pressure, agents running, and roughly how many more agents of the " +
			"default size fit under every hard limit. Also job counts by state and the " +
			"configured defaults. The room figure is a guide; cluster_plan gives the real " +
			"answer for a specific batch.",
	},
	{
		Name: "cluster.plan", MCP: "cluster_plan", CLI: []string{"cluster", "plan"},
		Short: "Work out where a batch of agents would run, without starting anything",
		Long: "Read-only dry run of submitting several jobs at once. Give either jobs (a list, " +
			"one entry per agent) or kind, repo and count for N identical agents. It runs the " +
			"same estimates, hard limits, scoring and queue pass a real submit would, behind " +
			"whatever is already queued, and answers per job: would_place on which worker and " +
			"why, would_queue and what it is waiting for, or would_refuse and why. It queues, " +
			"reserves and launches nothing and records no decision, so it is safe to call as " +
			"often as you like. Use it before fanning out, then submit the jobs you still want.",
		Args: []Arg{
			{Name: "jobs", Type: List, Desc: "The proposed jobs, in submission order", Items: PlanItems},
			{Name: "kind", Type: String, Desc: "With repo and count: the agent kind for N identical jobs"},
			{Name: "repo", Type: String, Desc: "With kind and count: the repository for N identical jobs"},
			{Name: "count", Type: Int, Desc: "With kind and repo: how many identical jobs, default 1"},
			{Name: "memory_mb", Type: Int, Desc: "With kind and repo: memory per job in MB; omit for the learned estimate"},
			{Name: "priority", Type: Int, Desc: "With kind and repo: priority per job, -100 to 100"},
			{Name: "policy", Type: String, Desc: "With kind and repo: balanced, memory, performance, pack, spread or local-first"},
		},
	},
	{
		Name: "cluster.queue", MCP: "cluster_queue", CLI: []string{"cluster", "queue"},
		Short: "Show waiting jobs in the order they will be tried, and why each waits",
		Long: "Read-only. Every queued job in the order the next pass tries it, with its " +
			"priority after ageing, how long it has waited, any worker held clear for it by " +
			"the starvation guard, whether it fits right now, and the constraint holding it " +
			"back on each worker. Smaller jobs behind a big one are still placed when they " +
			"fit; the queue never blocks on its head.",
	},
	{
		Name: "cluster.submit", MCP: "cluster_submit", CLI: []string{"cluster", "submit"},
		Short: "Queue a coding agent to run on whichever machine suits it",
		Long: "Queues one agent of the given kind in the given repository, with an " +
			"optional first prompt. The scheduler places it on the best worker that " +
			"passes every hard constraint (memory kept free, pressure not critical, " +
			"CPU commitment, load, disk, battery, thermal, max agents, pin, labels, " +
			"OS, architecture, repository present, declared agents), opens a Herdr " +
			"workspace there (a worktree when the repository is already busy on that " +
			"worker), and starts the agent with `herdr agent start`. Without memory_mb " +
			"the estimate comes from what earlier agents of this kind used in this " +
			"repository, else cluster_default_memory_mb. A job no worker could ever run " +
			"is refused here with the reason for each worker.",
		Args: []Arg{
			{Name: "kind", Type: String, Desc: "Agent kind as Herdr names it: claude, codex, cursor, gemini, opencode, amp, droid, kiro, qwen, pi, and so on", Required: true, Positional: true},
			{Name: "repo", Type: String, Desc: "Repository directory on the worker, absolute or starting with ~/", Required: true, Positional: true},
			{Name: "prompt", Type: String, Desc: "First prompt sent to the agent once it is up"},
			{Name: "memory_mb", Type: Int, Desc: "Memory to reserve in MB; omit to use the learned estimate"},
			{Name: "cpu", Type: String, Desc: "CPU cores to reserve, for example 1 or 1.5; default cluster_default_cpu"},
			{Name: "priority", Type: Int, Desc: "-100 to 100; higher goes first, and waiting jobs gain priority with age"},
			{Name: "policy", Type: String, Desc: "balanced, memory, performance, pack, spread or local-first"},
			{Name: "machine", Type: String, Desc: "Pin to one worker by id or label"},
			{Name: "labels", Type: String, Desc: "Comma-separated labels the worker must carry"},
			{Name: "os", Type: String, Desc: "Required worker OS: darwin, linux or windows"},
			{Name: "arch", Type: String, Desc: "Required worker architecture: arm64 or amd64"},
			{Name: "isolation", Type: String, Desc: "auto (worktree when the repo is busy there), worktree, or none"},
			{Name: "agent_args", Type: String, Desc: "Extra arguments for the agent, space-separated; each limited to letters, digits and ._/:=,@+%-"},
		},
		Mutates: true,
		Gated:   "sched.cluster.submit",
	},
	{
		Name: "cluster.jobs", MCP: "cluster_jobs", CLI: []string{"cluster", "jobs"},
		Short: "List cluster jobs and where they run",
		Long: "Cluster jobs, newest first: state (queued, reserved, dispatching, running, " +
			"finished, failed, cancelled, lost, detached), worker, agent name, workspace " +
			"and pane, memory reserved and observed, and the last error. The prompt " +
			"itself is never shown or stored in the event trail; only its length and " +
			"SHA-256 are.",
		Args: []Arg{
			{Name: "state", Type: String, Desc: "Comma-separated states to show; default all"},
			{Name: "limit", Type: Int, Desc: "Show at most this many"},
		},
	},
	{
		Name: "cluster.explain", MCP: "cluster_explain", CLI: []string{"cluster", "explain"},
		Short: "Explain why a job was placed where it was, or why it is waiting",
		Long: "Every placement decision recorded for the job, with each worker's " +
			"eligibility, the reasons it was ruled out, its score and the factors " +
			"behind the score. For a queued job it also places the job again against " +
			"the current readings, without dispatching, so the answer is up to date.",
		Args: []Arg{
			{Name: "id", Type: String, Desc: "The job id, for example j12", Required: true, Positional: true},
		},
	},
	{
		Name: "cluster.cancel", MCP: "cluster_cancel", CLI: []string{"cluster", "cancel"},
		Short: "Cancel a job that has not started",
		Long: "Cancels a queued or reserved job and releases its capacity. A running " +
			"job is refused: the scheduler never stops an agent. Close it in Herdr, " +
			"or use cluster detach to stop tracking it.",
		Args: []Arg{
			{Name: "id", Type: String, Desc: "The job id", Required: true, Positional: true},
		},
		Mutates: true,
		Gated:   "sched.cluster.cancel",
	},
	{
		Name: "cluster.detach", MCP: "cluster_detach", CLI: []string{"cluster", "detach"},
		Short: "Stop tracking a running job and release its reservation",
		Long: "Marks a running job detached and releases what it reserved. The agent " +
			"itself is left running in Herdr; only the scheduler forgets it. Use it " +
			"when an agent is being kept for interactive work and should no longer " +
			"count against the worker's capacity.",
		Args: []Arg{
			{Name: "id", Type: String, Desc: "The job id", Required: true, Positional: true},
		},
		Mutates: true,
		Gated:   "sched.cluster.detach",
	},
	{
		Name: "cluster.drain", MCP: "cluster_drain", CLI: []string{"cluster", "drain"},
		Short: "Stop placing new agents on a worker",
		Long: "A draining worker takes no new jobs. Agents already running there " +
			"carry on and are still tracked. Jobs pinned to it wait. Undo with " +
			"cluster undrain.",
		Args: []Arg{
			{Name: "worker", Type: String, Desc: "Worker id or label", Required: true, Positional: true},
		},
		Mutates: true,
		Gated:   "sched.cluster.drain",
	},
	{
		Name: "cluster.undrain", MCP: "cluster_undrain", CLI: []string{"cluster", "undrain"},
		Short: "Let a drained worker take new agents again",
		Long:  "Reverses cluster drain.",
		Args: []Arg{
			{Name: "worker", Type: String, Desc: "Worker id or label", Required: true, Positional: true},
		},
		Mutates: true,
		Gated:   "sched.cluster.undrain",
	},
	{
		Name: "cluster.events", MCP: "cluster_events", CLI: []string{"cluster", "events"},
		Short: "Read the cluster's own event trail",
		Long: "Reservations, dispatches, failures, adoptions after a restart, " +
			"disconnects, overruns, drains and probe failures, oldest first. Pass " +
			"since with the last id seen to resume.",
		Args: []Arg{
			{Name: "since", Type: Int, Desc: "Return events after this id"},
			{Name: "limit", Type: Int, Desc: "Return at most this many"},
		},
	},
	{
		Name: "cluster.metrics", MCP: "cluster_metrics", CLI: []string{"cluster", "metrics"},
		Short: "Print cluster metrics in Prometheus text format",
		Long: "Per-worker memory, pressure, CPU, load, reserved memory and agent counts, " +
			"and job counts by state, in the Prometheus text exposition format. The " +
			"same text is served on cluster_metrics_addr when that is set to a " +
			"loopback address. Nothing is sent anywhere.",
	},
}

// PlanItems is the shape of one proposed job in cluster.plan.
var PlanItems = []Arg{
	{Name: "kind", Type: String, Desc: "Agent kind as Herdr names it: claude, codex, opencode, and so on", Required: true},
	{Name: "repo", Type: String, Desc: "Repository directory on the worker, absolute or starting with ~/", Required: true},
	{Name: "memory_mb", Type: Int, Desc: "Memory to reserve in MB; omit to use the learned estimate"},
	{Name: "cpu", Type: String, Desc: "CPU cores to reserve, for example 1 or 1.5"},
	{Name: "priority", Type: Int, Desc: "-100 to 100; higher goes first"},
	{Name: "policy", Type: String, Desc: "balanced, memory, performance, pack, spread or local-first"},
	{Name: "machine", Type: String, Desc: "Pin to one worker by id or label"},
	{Name: "labels", Type: String, Desc: "Comma-separated labels the worker must carry"},
	{Name: "os", Type: String, Desc: "Required worker OS: darwin, linux or windows"},
	{Name: "arch", Type: String, Desc: "Required worker architecture: arm64 or amd64"},
}

// ByName finds the verb a socket request names.
func ByName(name string) (Verb, bool) {
	for _, v := range All {
		if v.Name == name {
			return v, true
		}
	}
	return Verb{}, false
}

// ByCLI finds the verb a subcommand path names.
func ByCLI(path []string) (Verb, bool) {
	for _, v := range All {
		if len(v.CLI) != len(path) {
			continue
		}
		same := true
		for i := range v.CLI {
			if v.CLI[i] != path[i] {
				same = false
				break
			}
		}
		if same {
			return v, true
		}
	}
	return Verb{}, false
}

// ByGated maps a §9.4 gate name back to the verb it belongs to, which is what
// resolving a parked action needs.
func ByGated(gated string) (Verb, bool) {
	for _, v := range All {
		if v.Gated != "" && v.Gated == gated {
			return v, true
		}
	}
	return Verb{}, false
}

// Accepts reports whether this verb declares an argument by that name.
func (v Verb) Accepts(name string) bool {
	for _, a := range v.Args {
		if a.Name == name {
			return true
		}
	}
	return false
}

// MCPTools is every verb the MCP door serves, in registry order.
func MCPTools() []Verb {
	out := make([]Verb, 0, len(All))
	for _, v := range All {
		if v.MCP != "" {
			out = append(out, v)
		}
	}
	return out
}

// GatedVerbs is the §9.4 list a policy plugin names, in registry order. The
// README carries the same list, and a test reads one against the other.
func GatedVerbs() []string {
	out := []string{}
	for _, v := range All {
		if v.Gated != "" {
			out = append(out, v.Gated)
		}
	}
	return out
}

// Help is the description a caller reads on either door: the one-line summary
// and the paragraph a caller who cannot ask a follow-up question needs. The
// MCP tool description and `hsched <verb> --help` are the same words, because
// a verb explained two ways is a verb that drifts.
func (v Verb) Help() string {
	if v.Long == "" {
		return v.Short
	}
	return v.Short + ". " + v.Long
}
