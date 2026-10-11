# Herdr Intelligent Distributed Agent Scheduler: build brief, sections 3.3 to 12 (from Justin, verbatim content)

## 3.3 Adaptive agent resource estimation
Do not assume every coding agent requires the same amount of memory.
Implement resource profiles for: Claude Code; OpenAI Codex CLI; OpenCode; Other Herdr-supported agents; Custom CLI agents.
Collect historical observations to estimate: typical idle memory; peak working memory; CPU usage; resource consumption during builds and tests; resource usage for large repositories; concurrent process-tree consumption.
Use a configurable default reservation for unknown workloads.
As historical data becomes available, update estimates using robust statistics such as rolling percentiles.
Do not depend on an LLM for basic scheduling decisions.
Any optional AI-assisted estimation must remain advisory, bounded, explainable, and replaceable with deterministic logic.

## 3.4 Example scheduling scenario
Assume: M3 Max 64GB RAM, 38GB available; M4 mini 16GB RAM, 9GB available; ten agents need to be launched; each has an estimated 3GB memory reservation.
The scheduler should:
1. Evaluate both machines.
2. Reserve a configurable amount of memory for the OS and other applications.
3. Account for memory already reserved by running or queued jobs.
4. Allocate agents across machines according to available capacity.
5. Prevent memory overcommit beyond configured limits.
6. Queue agents that cannot be safely admitted.
7. Dispatch queued agents when capacity becomes available.
These are illustrative figures, not hardcoded allocation rules. 64GB and 16GB are separate memory pools, not shared physical memory.

## 4. Distributed worker architecture
Implement a lightweight worker component only where existing Herdr capabilities are insufficient.
Each worker must: report system metrics; advertise supported agent runtimes; accept authenticated workload dispatches; start agent sessions through Herdr-compatible mechanisms; track process trees and resource consumption; report session lifecycle events; support graceful termination; reconnect automatically after temporary network interruptions.
Prefer reusing Herdr's existing machine connections and remote execution facilities. Avoid installing a separate privileged daemon unless absolutely necessary. Workers must not require root or administrator permissions for normal operation.
### Coordinator
Lightweight coordinator responsible for: maintaining worker state; scheduling and dispatching jobs; managing resource reservations; queueing workloads; tracking active sessions; detecting failed workers; recovering scheduler state after restart; preventing duplicate dispatches.
Persist state using SQLite unless existing plugin storage is more suitable. Single active coordinator initially, with safe restart recovery and explicit ownership of scheduling decisions. No distributed consensus or unnecessary cluster complexity.

## 5. Agent lifecycle management
Lifecycle: Submitted -> Queued -> Evaluating Workers -> Resource Reserved -> Dispatching -> Running -> Completed / Failed / Cancelled.
Include: unique job identifiers; idempotent dispatch; resource reservation tracking; dispatch timeouts; retry policies; cancellation; graceful shutdown; job prioritisation; queue visibility; recovery after coordinator restart.
An agent must never be launched twice because of a lost acknowledgement or network interruption.
Implement reconciliation between scheduler state and actual Herdr sessions.
### Session persistence
Preserve the ability to reconnect to existing agent sessions through Herdr. Do not automatically kill agents when a user closes the Herdr interface. Support reattachment to remote sessions. Preserve agent-native session identifiers and recovery mechanisms where supported. Do not claim to provide transparent migration of a live process between computers.

## 6. Workspace and Git isolation
Every scheduled coding agent should have an isolated workspace.
Support: Git worktrees; separate branches; repository cloning when required; existing repository reuse; workspace cleanup; persistent workspaces; shared repository metadata; repository-to-worker affinity.
Default to Git worktrees for multiple agents on the same repo on one machine.
For different machines, ensure repositories and workspaces are available through an explicit cloning or synchronisation mechanism. Do not assume a Git worktree can be transparently shared across separate filesystems.
Prevent concurrent agents from accidentally modifying the same working directory.
Support user-configurable workspace retention policies.

## 7. Intelligent queue management
Persistent queue with: job priorities; FIFO within priority classes; resource requirements; dependencies; per-project concurrency limits; per-machine concurrency limits; queue ageing to prevent starvation; cancellation; retry limits; scheduling history.
When a machine becomes available, automatically evaluate waiting jobs.
Avoid head-of-line blocking when a large job cannot fit but smaller eligible jobs can run.
Provide clear reasons why a job remains queued. Example:
  Job: Refactor authentication service
  Status: Queued
  M3 Max: Insufficient unreserved memory
  M4 Mini: CPU utilisation exceeds policy threshold
  Next action: Waiting for available capacity

## 8. Automatic load balancing
Continuously evaluate cluster health and workload distribution.
Implement: admission control for new agents; dynamic placement of queued agents; detection of sustained resource pressure; worker draining; optional cancellation and restart of eligible jobs; scheduling policy adjustments; capacity recommendations.
Do not attempt live process migration. For an already running agent, relocation must be an explicit checkpoint/restart workflow, only when the agent runtime can safely resume. Never silently interrupt an active coding agent simply to improve load balance.
### Memory pressure protection
If a worker approaches unsafe memory pressure:
1. Stop admitting new jobs.
2. Notify the coordinator.
3. Re-evaluate queued workloads.
4. Identify the workloads consuming the most resources.
5. Recommend remediation.
6. Optionally pause or terminate explicitly designated disposable jobs according to policy.
Never automatically terminate interactive sessions by default.

## 9. Herdr integration
Must behave like a native extension of Herdr, using verified Herdr APIs.
Required capabilities: launch an agent with automatic machine selection; override placement manually; list running agents across all workers; show the selected machine for each agent; show resource utilisation; inspect queued jobs; cancel queued jobs; attach to running sessions; inspect scheduling decisions; configure scheduling policies.
If Herdr exposes a supported plugin UI extension point, integrate there. If not, provide CLI commands and a lightweight optional dashboard rather than relying on undocumented UI internals.
Target CLI (subject to actual plugin API): herdr scheduler status | workers | launch --agent codex --repo ~/projects/myapp --policy balanced | queue | explain <job-id> | drain mac-mini. These are target interfaces, not assumptions about existing commands.

## 10. Cluster observability
Cluster overview example:
  HERDR COMPUTE CLUSTER
  Workers: 2 online / Active agents: 9 / Queued agents: 3
  M3 MAX  Memory: 42GB / 64GB  Agents: 7  CPU: 61%  Status: Healthy
  M4 MINI Memory: 12GB / 16GB  Agents: 2  CPU: 74%  Status: Memory pressure elevated
  QUEUE 3 agents awaiting capacity
Provide: worker health; active agent count; resource reservations; actual resource usage; queue depth; job execution history; scheduling decision explanations; agent runtime statistics; failure and retry history.
Optional Prometheus-compatible metrics. Structured logs with correlation IDs.

## 11. Security requirements
Implement: SSH or mutually authenticated secure transport; worker identity verification; least-privilege execution; explicit command allowlisting or typed execution requests; secure credential handling; no secrets in logs or scheduler DB; secure local socket permissions; job ownership checks; protection against command injection; safe repository path handling; worker registration and revocation; configurable execution permissions.
Do not create an unauthenticated remote command execution service. Prefer existing Herdr security primitives. Ensure remote agent processes do not receive unnecessary credentials.

## 12. Configuration
Human-readable config file. Illustrative:
scheduler: {policy: adaptive, max_queue_size: 100, admission_interval_seconds: 5}
resources: {default_agent_memory_mb: 3072, memory_reserve_percent: 20, max_cpu_utilisation_percent: 85, max_swap_pressure: moderate}
workers: [{name: macbook-m3-max, enabled: true, max_agents: 12, weight: 1.0}, {name: mac-mini-m4, enabled: true, max_agents: 4, weight: 0.8}]
workspaces: {isolation: git-worktree, cleanup_policy: retain-on-failure}
queue: {enabled: true, priority_aging: true}
observability: {metrics_enabled: true, scheduling_explanations: true}
Validate config at startup with meaningful error messages. All resource limits must be configurable.

## Definition of done (section 16, for reference)
1 Install into Herdr. 2 Register M3 Max MacBook Pro and M4 Mac mini. 3 See resources across both. 4 Launch ten agents without picking machines. 5 Auto-assigned by safe capacity. 6 Jobs queue when insufficient. 7 Reconnect to every running agent through Herdr. 8 Restart Herdr or the scheduler without losing track of active sessions. 9 See why a machine was selected. 10 Add another machine without redesign. 11 Drain a machine without silently killing interactive agents. 12 No Kubernetes or heavyweight infra.
