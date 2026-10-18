#!/usr/bin/env bash
# Regression check for an operator run: the pipeline produced a report, and every row that is
# not PASS is a known deviation listed in the docs/gaps.md register (signed or still awaiting
# sign-off). Anything else, a new FAIL, a NOT-RUN or a VOID on a row nobody has registered, is a
# regression, even while the report itself is FAILED for known deviations awaiting sign-off.
#
#   runner/report/check-known-deviations.sh <run-dir>
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUN_DIR="${1:?usage: check-known-deviations.sh <run-dir>}"
SUMMARY="${RUN_DIR}/report/summary.json"
for f in "${RUN_DIR}/report/REPORT.md" "${SUMMARY}"; do
  [[ -s "$f" ]] || { echo "[known-deviations] FAIL: $f is missing: the pipeline did not produce a report" >&2; exit 1; }
done
command -v jq >/dev/null 2>&1 || { echo "[known-deviations] ERROR: jq is required" >&2; exit 2; }

# shellcheck disable=SC2016 # backticks are literal Markdown code marks
listed="$(sed -n 's/^| *`\{0,1\}\([a-z0-9][a-z0-9._-]*\)`\{0,1\} *|.*/\1/p' "${ROOT}/docs/gaps.md" | sort -u)"
unexpected=""
while IFS=$'\t' read -r id verdict; do
  [[ -z "${id}" ]] && continue
  if ! grep -qxF "${id}" <<<"${listed}"; then
    unexpected="${unexpected}  ${id} ${verdict}"$'\n'
  fi
done < <(jq -r '.rows | to_entries[] | .value[] | select(.verdict != "PASS" and .verdict != "GAP") | [.id, .verdict] | @tsv' "${SUMMARY}")

status="$(jq -r 'if .passed then "PASSED" else "FAILED" end' "${SUMMARY}")"
if [[ -n "${unexpected}" ]]; then
  echo "[known-deviations] FAIL: report ${status}; rows not PASS and not listed in docs/gaps.md:" >&2
  printf '%s' "${unexpected}" >&2
  exit 1
fi
echo "[known-deviations] OK: report ${status}; every row that is not PASS is a listed deviation"
