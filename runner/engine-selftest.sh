#!/usr/bin/env bash
# Offline self-test for the engine abstraction's handling of an engine that does not answer
# (runner/lib/engine.sh engine_inspect). A fake engine binary plays the CLI, so this needs no
# container engine; it runs in pr.yml.
#
# Cases: a transient "cannot reach the socket" error is retried and the real state returned; a
# missing container is "not running" at once, without retries; an engine that never answers ends
# the caller with exit 2 (infrastructure), never "not running"; digests survive a retry.
# shellcheck disable=SC2015,SC2016 # each check ends in fail; snippets expand inside the probe shell
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
T="$(mktemp -d)"
trap 'rm -rf "${T}"' EXIT
fail() { echo "[engine-selftest] FAIL: $*" >&2; exit 1; }
ok() { echo "[engine-selftest] ok: $*"; }

# Fake engine: fails FAKE_FAILS times with an unreachable-socket error, then answers from
# FAKE_MODE: running | stopped | absent.
cat >"${T}/fake-engine" <<'FAKE'
#!/usr/bin/env bash
n="$(cat "${FAKE_COUNTER}" 2>/dev/null || echo 0)"
echo $((n + 1)) >"${FAKE_COUNTER}"
if (( n < FAKE_FAILS )); then
  echo 'Cannot connect: dial tcp 127.0.0.1:50123: connect: can'"'"'t assign requested address' >&2
  exit 125
fi
case "${FAKE_MODE}:$*" in
  absent:*) echo "Error: No such object: $4" >&2; exit 1 ;;
  running:inspect\ --format\ \{\{.State.Running\}\}*) echo true ;;
  stopped:inspect\ --format\ \{\{.State.Running\}\}*) echo false ;;
  *:inspect\ --format\ \{\{.Image\}\}*) echo sha256:img ;;
  *:image\ inspect*) printf 'repo@sha256:aaa\nrepo@sha256:bbb\n' ;;
  *) echo "fake engine: unexpected $*" >&2; exit 3 ;;
esac
FAKE
chmod +x "${T}/fake-engine"

# probe <fails> <mode> <snippet>: run snippet in a fresh shell with the fake engine; prints
# "rc=<exit code>" last.
probe() {
  rm -f "${T}/counter"
  FAKE_FAILS="$1" FAKE_MODE="$2" FAKE_COUNTER="${T}/counter" ENGINE_UNREACHABLE_TIMEOUT=3 \
    CERT_ENGINE="${T}/fake-engine" ROOT="${ROOT}" bash -c '
      set -euo pipefail
      . "${ROOT}/runner/lib/engine.sh"
      '"$3"'
    ' >"${T}/out" 2>&1 && echo "rc=0" >>"${T}/out" || echo "rc=$?" >>"${T}/out"
  tail -1 "${T}/out"
}

[[ "$(probe 2 running 'engine_container_running c && echo RUNNING')" == rc=0 ]] && grep -q RUNNING "${T}/out" \
  || { cat "${T}/out"; fail "two unreachable answers then running must read running"; }
ok "transient engine error retried, running state returned"

[[ "$(probe 2 stopped 'engine_container_running c || echo NOT-RUNNING')" == rc=0 ]] && grep -q NOT-RUNNING "${T}/out" \
  || { cat "${T}/out"; fail "stopped container must read not running"; }
ok "stopped container reads not running after a retry"

[[ "$(probe 0 absent 'engine_container_running c || echo NOT-RUNNING')" == rc=0 ]] && grep -q NOT-RUNNING "${T}/out" \
  && [[ "$(cat "${T}/counter")" == 1 ]] || { cat "${T}/out"; fail "missing container must read not running with one call"; }
ok "missing container reads not running, no retries"

[[ "$(probe 1000 running 'engine_container_running c || echo NOT-RUNNING')" == rc=2 ]] && ! grep -q NOT-RUNNING "${T}/out" \
  && grep -q "did not answer" "${T}/out" || { cat "${T}/out"; fail "an engine that never answers must exit 2, never read not running"; }
ok "engine that never answers ends the run with exit 2"

[[ "$(probe 1 running 'engine_image_digests c; echo "D=${ENGINE_IMAGE_DIGESTS//$'"'"'\n'"'"'/,}"')" == rc=0 ]] \
  && grep -q "D=sha256:aaa,sha256:bbb" "${T}/out" || { cat "${T}/out"; fail "digests after a retry"; }
ok "image digests read after a retry"
# engine_client_run adds the host-gateway mapping only on Linux Docker, and never beside a
# client that joins another container's network (Docker refuses the two together).
if [[ "$(uname -s)" == Linux ]]; then
  cat >"${T}/echo-engine" <<'FAKE'
#!/usr/bin/env bash
echo "ARGS $*"
FAKE
  chmod +x "${T}/echo-engine"
  client_args() {
    CERT_ENGINE="${T}/echo-engine" CERT_ENGINE_RUNTIME=docker HOST_GATEWAY_ALIAS=host.alias ROOT="${ROOT}" \
      bash -c '. "${ROOT}/runner/lib/engine.sh"; engine_client_run "$@"' _ "$@"
  }
  client_args -e A=1 -- img | grep -q -- "--add-host host.alias:host-gateway" || fail "plain client must get the host mapping on Linux Docker"
  for net in "--network container:peer" "--network=container:peer" "--net=container:peer"; do
    # shellcheck disable=SC2086 # split the option on purpose
    if client_args ${net} -- img | grep -q -- "--add-host"; then fail "client with ${net} must not get the host mapping"; fi
  done
  ok "host mapping added on Linux Docker, never beside a joined container network"
fi
echo "[engine-selftest] all engine self-tests passed"
