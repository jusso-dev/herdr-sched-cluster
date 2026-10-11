#!/bin/sh
# Start the sched daemon and exit, which is what Herdr expects of a startup
# command. `hsched daemon` refuses when one is already listening, and that
# refusal is success here: the daemon we wanted running is running.
#
# Herdr installs every update into a new directory and later deletes the old
# ones, so this checkout's bin/hsched is not a path anything outside Herdr can
# rely on. Each start copies it to a stable place under the plugin's state
# directory and runs it from there, and points ~/.local/bin/hsched at that copy
# so agents, MCP clients and remote probes always find the current binary.
# An existing ~/.local/bin/hsched that is a real file, not a link, is left
# alone. Set HSCHED_NO_LINK=1 to skip the link.
set -eu
cd "$(dirname "$0")/.."
if [ ! -x bin/hsched ]; then
  echo "sched: bin/hsched is missing; the [[build]] step did not run" >&2
  exit 1
fi
bin="$(pwd)/bin/hsched"
state="${HERDR_PLUGIN_STATE_DIR:-}"
if [ -n "$state" ]; then
  mkdir -p "$state/bin"
  cp -f bin/hsched "$state/bin/.hsched.$$"
  mv -f "$state/bin/.hsched.$$" "$state/bin/hsched"
  bin="$state/bin/hsched"
fi
if [ "${HSCHED_NO_LINK:-}" != "1" ] && [ -n "${HOME:-}" ]; then
  link="$HOME/.local/bin/hsched"
  if [ ! -e "$link" ] || [ -L "$link" ]; then
    mkdir -p "$HOME/.local/bin"
    ln -sfn "$bin" "$link"
  fi
fi
# Absolute path deliberately, so the daemon's argv says which binary it is
# when an operator looks.
nohup "$bin" daemon >/dev/null 2>&1 &
exit 0
