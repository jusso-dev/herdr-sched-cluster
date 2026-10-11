#!/usr/bin/env python3
"""Talk to `hsched mcp` over stdio the way an agent's MCP client does.

Lists the tools, then calls cluster_status and cluster_plan. Usage:
    python3 scripts/mcp-smoke.py [path/to/hsched] [repo]
Nothing is submitted: both calls are read-only.
"""
import json
import os
import subprocess
import sys

hsched = sys.argv[1] if len(sys.argv) > 1 else "hsched"
repo = sys.argv[2] if len(sys.argv) > 2 else os.getcwd()
p = subprocess.Popen([hsched, "mcp"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
seq = 0


def rpc(method, params=None, notify=False):
    global seq
    msg = {"jsonrpc": "2.0", "method": method}
    if params is not None:
        msg["params"] = params
    if not notify:
        seq += 1
        msg["id"] = seq
    p.stdin.write(json.dumps(msg) + "\n")
    p.stdin.flush()
    if notify:
        return None
    while True:
        line = p.stdout.readline()
        if not line:
            sys.exit("server closed the connection")
        reply = json.loads(line)
        if reply.get("id") == seq:
            if "error" in reply:
                sys.exit(f"{method}: {reply['error']}")
            return reply["result"]


init = rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                          "clientInfo": {"name": "mcp-smoke", "version": "1"}})
print("server:", init["serverInfo"]["name"], init["serverInfo"]["version"])
rpc("notifications/initialized", notify=True)
tools = rpc("tools/list")["tools"]
cluster = [t for t in tools if t["name"].startswith("cluster_")]
print(f"{len(tools)} tools, {len(cluster)} cluster tools:")
for t in cluster:
    print(f"  {t['name']}: {t['description'].split('. ')[0]}")


def call(name, args):
    res = rpc("tools/call", {"name": name, "arguments": args})
    body = json.loads(res["content"][0]["text"])
    if res.get("isError"):
        sys.exit(f"{name} failed: {body}")
    return body


st = call("cluster_status", {})
print("cluster_status:", st["summary"])
plan = call("cluster_plan", {"jobs": [
    {"kind": "claude", "repo": repo, "memory_mb": 1024, "priority": 10},
    {"kind": "codex", "repo": repo, "memory_mb": 1024},
    {"kind": "opencode", "repo": repo, "memory_mb": 1024},
]})
print("cluster_plan:", plan["summary"])
for j in plan["jobs"]:
    print(f"  {j['ref']} {j['kind']:<8} {j['outcome']:<12} {j.get('worker') or '-':<8} {j['reason'][:110]}")
same = call("cluster_plan", {"kind": "opencode", "repo": repo, "count": 4, "memory_mb": 800})
print("cluster_plan x4:", same["summary"])
p.stdin.close()
p.wait(timeout=10)
