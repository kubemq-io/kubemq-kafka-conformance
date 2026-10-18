#!/usr/bin/env bash
# suites/tools/ctr.sh — the tools track's container helper. The Go track calls this script, never
# an engine binary, so engine specifics stay in runner/lib/engine.sh.
#
#   ctr.sh start <name> [engine run options...] -- <image> [command...]
#                                 start a detached client container (removed when it stops) that
#                                 reaches both brokers through the host-gateway alias; prints its id
#   ctr.sh exec <name> <command...>        run a command in it (stdin is passed through)
#   ctr.sh kill <name> [signal]            send a signal (default KILL)
#   ctr.sh rm <name>                       stop and remove it (no error when it is gone)
#   ctr.sh running <name>                  exit 0 when it is running, 1 when not
#   ctr.sh exists <name>                   exit 0 when a container of that name exists, 1 when not
#                                 (both exit 2 when the engine does not answer: infrastructure)
#   ctr.sh logs <name>                     its log so far, on stdout
#   ctr.sh follow <name>                   stream its log until it stops (for a log file)
#   ctr.sh net-create <name> / net-rm <name>   a private network for helper containers
#   ctr.sh sweep <run-label>               remove every container and network of one run
#
# Every container and network carries the labels kcert-tools=1 and kcert-tools.run=<run label>
# (TOOLS_RUN_LABEL), so a run removes only what it created.
# Needs the CERT_ENGINE* and HOST_GATEWAY_ALIAS variables runner/certify.sh exports.
# shellcheck source-path=SCRIPTDIR
set -euo pipefail
# shellcheck source=../../runner/lib/engine.sh
. "$(dirname "${BASH_SOURCE[0]}")/../../runner/lib/engine.sh"
: "${CERT_ENGINE:?run from runner/certify.sh}" "${CERT_ENGINE_RUNTIME:?}" "${HOST_GATEWAY_ALIAS:?}"
RUN_LABEL="${TOOLS_RUN_LABEL:-manual}"

cmd="${1:-}"; shift || true
case "${cmd}" in
  start)
    name="$1"; shift
    engine_client_run -d --name "${name}" --label kcert-tools=1 --label "kcert-tools.run=${RUN_LABEL}" "$@"
    ;;
  exec)
    name="$1"; shift
    engine_run exec -i "${name}" "$@"
    ;;
  kill)
    engine_run kill --signal "${2:-KILL}" "$1" >/dev/null
    ;;
  rm)
    engine_run rm -f "$1" >/dev/null 2>&1 || true
    ;;
  running)
    engine_container_running "$1"
    ;;
  exists)
    engine_container_exists "$1"
    ;;
  logs)
    engine_run logs "$1" 2>&1
    ;;
  follow)
    engine_run logs -f "$1" 2>&1 || true
    ;;
  net-create)
    engine_run network create --label kcert-tools=1 --label "kcert-tools.run=${RUN_LABEL}" "$1" >/dev/null
    ;;
  net-rm)
    engine_run network rm "$1" >/dev/null 2>&1 || true
    ;;
  sweep)
    ids="$(engine_run ps -a -q --filter "label=kcert-tools.run=$1" 2>/dev/null || true)"
    # shellcheck disable=SC2086 # one id per word
    [[ -z "${ids}" ]] || engine_run rm -f ${ids} >/dev/null 2>&1 || true
    nets="$(engine_run network ls -q --filter "label=kcert-tools.run=$1" 2>/dev/null || true)"
    # shellcheck disable=SC2086
    [[ -z "${nets}" ]] || engine_run network rm ${nets} >/dev/null 2>&1 || true
    ;;
  *)
    sed -n '2,20p' "${BASH_SOURCE[0]}" >&2
    exit 2
    ;;
esac
