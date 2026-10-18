#!/usr/bin/env bash
# suites/perf/ctl.sh — the performance track's engine helper: pause or resume a broker container so
# that only the broker being measured runs, and run a load tool in a client container.
#   ctl.sh pause <container> | ctl.sh unpause <container>
#   ctl.sh client [engine run options...] -- <image> [command...]
# Only the engine abstraction (runner/lib/engine.sh) is called.
# shellcheck source-path=SCRIPTDIR
set -euo pipefail
# shellcheck source=../../runner/lib/engine.sh
. "$(dirname "${BASH_SOURCE[0]}")/../../runner/lib/engine.sh"
: "${CERT_ENGINE:?run from runner/certify.sh}"
case "${1:-}" in
  pause|unpause) engine_run "$1" "${2:?container}" >/dev/null ;;
  client) shift; engine_client_run "$@" ;;
  *) echo "usage: ctl.sh pause|unpause <container> | client [opts] -- <image> [cmd]" >&2; exit 2 ;;
esac
