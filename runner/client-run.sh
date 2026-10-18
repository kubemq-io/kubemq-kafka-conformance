#!/usr/bin/env bash
# Run a client container for a track: runner/client-run.sh [engine run options...] -- <image> [command...]
# Tracks call this (never an engine binary) so engine specifics stay in runner/lib/engine.sh.
# Needs the CERT_ENGINE* and HOST_GATEWAY_ALIAS variables runner/certify.sh exports.
# shellcheck source-path=SCRIPTDIR
set -euo pipefail
# shellcheck source=lib/engine.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib/engine.sh"
: "${CERT_ENGINE:?run from runner/certify.sh}" "${CERT_ENGINE_RUNTIME:?}" "${HOST_GATEWAY_ALIAS:?}"
engine_client_run "$@"
