#!/usr/bin/env bash
# Build and run the report renderer. Called by runner/certify.sh.
#   runner/report/render.sh --run-dir <dir> [--publish] [--public]
# Exit codes: 0 report passed, 1 report failed, 2 malformed inputs or build failure.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
RUN_DIR=""; PUBLISH=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-dir) RUN_DIR="$2"; shift ;;
    --publish) PUBLISH+=(-publish) ;;
    --public) PUBLISH+=(-public) ;;
    *) echo "[report] ERROR: unknown argument $1" >&2; exit 2 ;;
  esac
  shift
done
[[ -n "${RUN_DIR}" ]] || { echo "[report] ERROR: --run-dir is required" >&2; exit 2; }
command -v go >/dev/null 2>&1 || { echo "[report] ERROR: Go is required to render the report" >&2; exit 2; }
BIN="${RUN_DIR}/bin/render"
mkdir -p "$(dirname "${BIN}")"
(cd "${HERE}" && go build -o "${BIN}" ./cmd/render) || exit 2
exec "${BIN}" -run-dir "${RUN_DIR}" -root "${ROOT}" ${PUBLISH[@]+"${PUBLISH[@]}"}
