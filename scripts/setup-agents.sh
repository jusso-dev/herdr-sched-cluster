#!/bin/sh
# Register the herdr-sched MCP server with the coding agents on this machine
# and install the herdr-cluster skill, so an agent can be told "spread this
# work across my machines" and know how.
#
#   Claude Code  `claude mcp add --scope user`, and the skills copied to
#                ~/.claude/skills/herdr-cluster and ~/.claude/skills/sched
#   Codex        `codex mcp add` (writes ~/.codex/config.toml)
#   OpenCode     ~/.config/opencode/opencode.json, written only when there is
#                no such file yet; otherwise the snippet to add is printed
#
# Run it once per machine after `herdr plugin install`, or through Herdr:
#   herdr plugin action invoke herdr-sched.setup-agents
# It is safe to run again: an agent that already has the server is skipped.
set -eu
cd "$(dirname "$0")/.."

hsched="${HSCHED_BIN:-$HOME/.local/bin/hsched}"
if [ ! -x "$hsched" ]; then
  if [ -x bin/hsched ]; then
    hsched="$(pwd)/bin/hsched"
    echo "note: $HOME/.local/bin/hsched is missing, so this uses $hsched; it moves on every update." >&2
    echo "      Restart the plugin (herdr plugin action invoke herdr-sched.restart) to create the stable link." >&2
  else
    echo "error: no hsched binary found; install the plugin first" >&2
    exit 1
  fi
fi

if command -v claude >/dev/null 2>&1; then
  if claude mcp get herdr-sched >/dev/null 2>&1; then
    echo "Claude Code: herdr-sched is already registered"
  else
    claude mcp add --scope user herdr-sched -- "$hsched" mcp
  fi
  for skill in herdr-cluster sched; do
    mkdir -p "$HOME/.claude/skills/$skill"
    cp -f "skills/$skill/SKILL.md" "$HOME/.claude/skills/$skill/SKILL.md"
  done
  echo "Claude Code: skills installed at ~/.claude/skills/herdr-cluster and ~/.claude/skills/sched"
else
  echo "Claude Code: not installed here, skipped"
fi

if command -v codex >/dev/null 2>&1; then
  if grep -q '^\[mcp_servers\.herdr-sched\]' "$HOME/.codex/config.toml" 2>/dev/null; then
    echo "Codex: herdr-sched is already in ~/.codex/config.toml"
  else
    codex mcp add herdr-sched -- "$hsched" mcp
  fi
else
  echo "Codex: not installed here, skipped"
fi

oc="$HOME/.config/opencode/opencode.json"
if command -v opencode >/dev/null 2>&1; then
  if [ ! -e "$oc" ]; then
    mkdir -p "$(dirname "$oc")"
    cat >"$oc" <<JSON
{
  "\$schema": "https://opencode.ai/config.json",
  "mcp": {
    "herdr-sched": {
      "type": "local",
      "command": ["$hsched", "mcp"],
      "enabled": true
    }
  }
}
JSON
    echo "OpenCode: wrote $oc"
  elif grep -q '"herdr-sched"' "$oc"; then
    echo "OpenCode: herdr-sched is already in $oc"
  else
    echo "OpenCode: $oc exists, so add this under its \"mcp\" key yourself:"
    echo "  \"herdr-sched\": { \"type\": \"local\", \"command\": [\"$hsched\", \"mcp\"], \"enabled\": true }"
  fi
else
  echo "OpenCode: not installed here, skipped"
fi

echo
echo "For Codex, OpenCode and anything else that reads AGENTS.md, paste"
echo "  $(pwd)/skills/herdr-cluster/AGENTS-snippet.md"
echo "into your global or project AGENTS.md so the agent knows when to use it."
