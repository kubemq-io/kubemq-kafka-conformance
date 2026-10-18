#!/usr/bin/env bash
# Offline self-test for runner/bump-kubemq.sh. A fake curl on PATH plays the registry, so this
# needs no network, no engine and no key; it runs in pr.yml.
#
# Cases: rewrite of exactly the four KubeMQ lines; re-pin is idempotent; --verify passes on a
# fresh pin and fails (exit 1) on a moved tag; rc tag refused; missing tag; single-architecture
# image; index bytes that do not match the advertised digest; architecture manifest missing.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUMP="${ROOT}/runner/bump-kubemq.sh"
T="$(mktemp -d)"
trap 'rm -rf "${T}"' EXIT

fail() { echo "[bump-selftest] FAIL: $*" >&2; exit 1; }
ok() { echo "[bump-selftest] ok: $*"; }

sha() {
  if command -v shasum >/dev/null 2>&1; then shasum -a 256 "$1" | awk '{print "sha256:" $1}'
  else sha256sum "$1" | awk '{print "sha256:" $1}'; fi
}

# --- fake registry ---------------------------------------------------------------------------
REG="${T}/registry"
mkdir -p "${REG}/manifests" "${T}/bin"
printf '{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","layers":["amd64"]}' >"${T}/amd64.json"
printf '{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","layers":["arm64"]}' >"${T}/arm64.json"
AMD="$(sha "${T}/amd64.json")"; ARM="$(sha "${T}/arm64.json")"
cp "${T}/amd64.json" "${REG}/manifests/${AMD}"
cp "${T}/arm64.json" "${REG}/manifests/${ARM}"

# publish <tag> <index-body-file>: serve the index under the tag with its true digest.
publish() { cp "$2" "${REG}/manifests/$1"; sha "$2" >"${REG}/manifests/$1.digest"; }

cat >"${T}/index.json" <<EOF
{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[
 {"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"${AMD}","platform":{"architecture":"amd64","os":"linux"}},
 {"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"${ARM}","platform":{"architecture":"arm64","os":"linux","variant":"v8"}},
 {"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"sha256:attestation","platform":{"architecture":"unknown","os":"unknown"}}]}
EOF
INDEX="$(sha "${T}/index.json")"
publish v9.9.9-pre.1 "${T}/index.json"

cat >"${T}/bin/curl" <<'EOF'
#!/usr/bin/env bash
# Fake curl for the self-test: understands only what bump-kubemq.sh sends.
out=/dev/null; hdr=""; url=""; writeout=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -o) out="$2"; shift ;;
    -D) hdr="$2"; shift ;;
    -w) writeout="$2"; shift ;;
    -H|--data-urlencode) shift ;;
    -*) ;;
    *) url="$1" ;;
  esac
  shift
done
emit_headers() { if [[ "${hdr}" == - ]]; then printf '%s' "$1"; elif [[ -n "${hdr}" ]]; then printf '%s' "$1" >"${hdr}"; fi; }
case "${url}" in
  */v2/)
    emit_headers $'HTTP/2 401\r\nwww-authenticate: Bearer realm="https://fake.test/v2/token"\r\n\r\n' ;;
  */v2/token)
    echo '{"token":"t"}' ;;
  */manifests/*)
    ref="${url##*/manifests/}"; f="${FAKE_REGISTRY}/manifests/${ref}"
    if [[ -f "${f}" ]]; then
      d="$(cat "${f}.digest" 2>/dev/null || true)"
      emit_headers "HTTP/2 200"$'\r\n'"${d:+docker-content-digest: ${d}}"$'\r\n\r\n'
      cat "${f}" >"${out}"; [[ -n "${writeout}" ]] && printf 200
    else
      emit_headers $'HTTP/2 404\r\n\r\n'; : >"${out}"; [[ -n "${writeout}" ]] && printf 404
    fi ;;
  *) echo "fake curl: unexpected URL ${url}" >&2; exit 7 ;;
esac
exit 0
EOF
chmod +x "${T}/bin/curl"
export PATH="${T}/bin:${PATH}" FAKE_REGISTRY="${REG}"

V="${T}/versions.env"
cp "${ROOT}/compose/versions.env" "${V}"
cp "${V}" "${T}/before.env"
A="${T}/apikeys.yaml"
echo "kubemq: before" >"${A}"

# Fake matrix regeneration (the real one boots both brokers): writes the version it was given,
# or fails when REGEN_FAIL is set.
cat >"${T}/fake-regen" <<'REGEN'
#!/usr/bin/env bash
set -euo pipefail
vf="" out=""
while [[ $# -gt 0 ]]; do case "$1" in --versions-file) vf="$2"; shift ;; --out) out="$2"; shift ;; esac; shift; done
[[ -z "${REGEN_FAIL:-}" ]] || { echo "fake regen: refusing" >&2; exit 2; }
echo "kubemq: $(sed -n 's/^KUBEMQ_VERSION=//p' "${vf}")" >"${out}"
REGEN
chmod +x "${T}/fake-regen"
export BUMP_APIKEYS_REGEN="${T}/fake-regen"

# bump <expected-exit> <args...>
bump() {
  local want="$1" rc=0; shift
  "${BUMP}" --versions-file "${V}" --apikeys-file "${A}" "$@" >"${T}/out.log" 2>&1 || rc=$?
  [[ "${rc}" == "${want}" ]] || { cat "${T}/out.log" >&2; fail "bump-kubemq.sh $* exited ${rc}, expected ${want}"; }
}

# --- cases -----------------------------------------------------------------------------------
bump 0 v9.9.9-pre.1
grep -qx "KUBEMQ_VERSION=v9.9.9-pre.1" "${V}" || fail "KUBEMQ_VERSION not rewritten"
grep -qx "KUBEMQ_IMAGE_DIGEST=${INDEX}" "${V}" || fail "index digest not rewritten"
grep -qx "KUBEMQ_IMAGE_DIGEST_AMD64=${AMD}" "${V}" || fail "amd64 digest not rewritten"
grep -qx "KUBEMQ_IMAGE_DIGEST_ARM64=${ARM}" "${V}" || fail "arm64 digest not rewritten"
changed="$(diff "${T}/before.env" "${V}" | grep -c '^>' || true)"
[[ "${changed}" == 5 ]] || fail "expected 4 rewritten lines and 1 resolved comment, diff shows ${changed} new lines"
grep -v '^KUBEMQ_\(VERSION\|IMAGE_DIGEST\|IMAGE_DIGEST_AMD64\|IMAGE_DIGEST_ARM64\)=\|^# KubeMQ resolved ' "${V}" >"${T}/rest-after"
grep -v '^KUBEMQ_\(VERSION\|IMAGE_DIGEST\|IMAGE_DIGEST_AMD64\|IMAGE_DIGEST_ARM64\)=\|^# KubeMQ resolved ' "${T}/before.env" >"${T}/rest-before"
cmp -s "${T}/rest-before" "${T}/rest-after" || fail "lines other than the KubeMQ pins changed"
ok "rewrites exactly the KubeMQ pins"
grep -qx "kubemq: v9.9.9-pre.1" "${A}" || fail "ApiKey matrix not regenerated for the new version"
ok "regenerates the ApiKey matrix from the rewritten pins"

bump 0 v9.9.9-pre.1
[[ "$(grep -c '^# KubeMQ resolved ' "${V}")" == 1 ]] || fail "re-pin added a second resolved comment"
ok "re-pin is idempotent"

bump 0 --verify
ok "verify passes on a fresh pin"

# The tag is moved to a different index (same images, different index bytes).
sed 's/"variant":"v8"/"variant":"v8","x":1/' "${T}/index.json" >"${T}/index2.json"
publish v9.9.9-pre.1 "${T}/index2.json"
bump 1 --verify
grep -q "MISMATCH KUBEMQ_IMAGE_DIGEST:" "${T}/out.log" || fail "verify did not name the moved index"
ok "verify fails with exit 1 on a moved tag"
publish v9.9.9-pre.1 "${T}/index.json"

cp "${V}" "${T}/pinned.env"
bump 2 v9.9.9-rc.1
cmp -s "${T}/pinned.env" "${V}" || fail "refused tag changed the file"
bump 2 latest
ok "rc and latest tags refused, file untouched"

bump 2 v9.9.8
grep -q "does not exist" "${T}/out.log" || fail "missing tag not reported"
ok "missing tag reported"

printf '{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json"}' >"${T}/single.json"
publish v9.9.7 "${T}/single.json"
bump 2 v9.9.7
grep -q "not a multi-architecture index" "${T}/out.log" || fail "single-architecture image not refused"
ok "single-architecture image refused"

cp "${T}/index.json" "${REG}/manifests/v9.9.6"; echo "sha256:not-the-bytes" >"${REG}/manifests/v9.9.6.digest"
bump 2 v9.9.6
grep -q "bytes served hash to" "${T}/out.log" || fail "digest/bytes mismatch not refused"
ok "index whose bytes do not match its digest refused"

sed "s/${ARM}/sha256:missing/" "${T}/index.json" >"${T}/arm-missing.json"
publish v9.9.5 "${T}/arm-missing.json"
bump 2 v9.9.5
grep -q "registry answered HTTP 404" "${T}/out.log" || fail "missing architecture manifest not refused"
ok "architecture manifest missing from the registry refused"

cmp -s "${T}/pinned.env" "${V}" || fail "a failed bump changed the file"

cp "${A}" "${T}/pinned-apikeys.yaml"
bump 0 v9.9.9-pre.1 --dry-run
cmp -s "${T}/pinned-apikeys.yaml" "${A}" || fail "--dry-run changed the ApiKey matrix"
ok "--dry-run leaves the ApiKey matrix alone"

publish v9.9.4 "${T}/index.json"
REGEN_FAIL=1 bump 2 v9.9.4
grep -q "were not changed" "${T}/out.log" || fail "a failed regeneration did not say the files are unchanged"
cmp -s "${T}/pinned.env" "${V}" || fail "a failed regeneration changed the versions file"
cmp -s "${T}/pinned-apikeys.yaml" "${A}" || fail "a failed regeneration changed the ApiKey matrix"
ok "failed regeneration changes neither file"

bump 0 v9.9.4 --no-apikeys
grep -qx "KUBEMQ_VERSION=v9.9.4" "${V}" || fail "--no-apikeys did not rewrite the pins"
cmp -s "${T}/pinned-apikeys.yaml" "${A}" || fail "--no-apikeys changed the ApiKey matrix"
grep -q "run runner/regen-apikeys.sh before committing" "${T}/out.log" || fail "--no-apikeys did not say to regenerate"
ok "--no-apikeys rewrites the pins only and says to regenerate"
echo "[bump-selftest] all bump-kubemq self-tests passed"
