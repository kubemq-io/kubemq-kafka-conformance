#!/usr/bin/env bash
# Build one release report from run directories made on several machines.
#   runner/report/merge.sh --out reports/<version> [--allow-mixed-commits] [--superseded <run-dir>:<track>]... <run-dir>...
# Every run must use the same KubeMQ and Kafka images. Two runs of one track are combined only
# when their verdicts are on disjoint rows (NOT-RUN counts as none) and their commit is the same. Runs
# from different kit commits are refused unless --allow-mixed-commits (the header then says so).
# --superseded leaves out one track's results from one run because a later run replaces them; the
# per-run table says so. --not-run-reason <track>=<reason> says why a track no run covered was
# not run, on its rows. --public builds the public repository's report (--out results). --final <commit> builds the release report: every run from that one clean
# commit, a released KubeMQ, and none of the three options above. Writes reports/<version>/REPORT.md and summary.json, and COMPATIBILITY.md.
# Exit codes: 0 report passed, 1 report failed, 2 refused, malformed inputs or build failure.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
OUT=""; MIXED=(); PUB=(); SUP=(); NRR=(); FINAL=(); DIRS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) [[ $# -ge 2 ]] || { echo "[merge] ERROR: --out needs a path" >&2; exit 2; }; OUT="$2"; shift ;;
    --allow-mixed-commits) MIXED=(-allow-mixed-commits) ;;
    --public) PUB=(-public) ;;
    --final) [[ $# -ge 2 ]] || { echo "[merge] ERROR: --final needs the kit commit" >&2; exit 2; }; FINAL=(-final "$2"); shift ;;
    --not-run-reason) [[ $# -ge 2 ]] || { echo "[merge] ERROR: --not-run-reason needs <track>=<reason>" >&2; exit 2; }; NRR+=(-not-run-reason "$2"); shift ;;
    --superseded) [[ $# -ge 2 ]] || { echo "[merge] ERROR: --superseded needs <run-dir>:<track>" >&2; exit 2; }; SUP+=(-superseded "$2"); shift ;;
    -*) echo "[merge] ERROR: unknown option $1" >&2; exit 2 ;;
    *) DIRS+=("$1") ;;
  esac
  shift
done
[[ -n "${OUT}" && ${#DIRS[@]} -gt 0 ]] || { sed -n '2,14p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2; exit 2; }
command -v go >/dev/null 2>&1 || { echo "[merge] ERROR: Go is required to build the report" >&2; exit 2; }
BIN="$(mktemp -d)/merge"
(cd "${HERE}" && go build -o "${BIN}" ./cmd/merge) || exit 2
exec "${BIN}" -root "${ROOT}" -out "${OUT}" ${MIXED[@]+"${MIXED[@]}"} ${PUB[@]+"${PUB[@]}"} ${SUP[@]+"${SUP[@]}"} ${NRR[@]+"${NRR[@]}"} ${FINAL[@]+"${FINAL[@]}"} "${DIRS[@]}"
