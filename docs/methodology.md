# Methodology

How every row in this repository gets its verdict. This document is normative: the runner and
the report renderer implement exactly what is written here.

## The oracle model

Real Apache Kafka is the oracle. Every row runs against Apache Kafka first, then against KubeMQ
with only the bootstrap address changed. Nothing else in the client, tool or scenario changes
between the two legs.

- The Kafka broker is the official `apache/kafka` image, KRaft mode, pinned by OCI index digest in
  `compose/versions.env`.
- The KubeMQ broker is the latest released `kubemq-next` image (`KUBEMQ_VERSION=latest` in
  `compose/versions.env`). The runner resolves that tag to an image digest when the run starts
  and runs that digest for the whole run, so every row of a run is measured against the same
  image. A development build is never certified evidence.
- The Kafka image digest, every client version and the KubeMQ image digest the run resolved are
  printed in the header of every report and of `COMPATIBILITY.md`.

## Verdicts

Each row gets exactly one verdict.

| Verdict | Meaning |
|---|---|
| PASS | Identical observable behaviour to the oracle within the row's assertions. |
| FAIL | Differs from the oracle. |
| NOT-APPLICABLE | One of six broker-to-broker requests listed in `docs/gaps.md` before the run, counted separately from application PASS and GAP. Its exact observed range must be signed off before the run. Kafka broker interoperability is not claimed. |
| GAP | Intentional, **pre-registered** deviation, shown as **OPTIONAL** in the reports: a Kafka feature KubeMQ does not offer. The row must already be listed in `docs/gaps.md`, with its sign-off, before the run. A FAIL is never relabelled GAP after the fact. |
| VOID | The oracle leg failed, so the row proves nothing. Requires the oracle log, linked by digest, and a root-cause note. Each track has a VOID budget of 2% of its behaviour rows; exceeding it fails the report. |
| NOT-RUN | A matrix-owned row that produced no verdict, including a Go `t.Skip`. Rendered with the same prominence as FAIL. Any NOT-RUN fails a release report. |
| FLAKY | Quarantine. A row needs 2 of 3 consistent failures to become FAIL. A quarantined row carries a badge and an expiry; after two releases in quarantine it becomes FAIL. |

Badges show PASS / GAP / FAIL / VOID counts. There is never a binary "certified" badge.

`GAP` is the verdict's name in the machine-readable results (the track results, `summary.json`);
`REPORT.md` and `COMPATIBILITY.md` display the same verdict as **OPTIONAL**. Both names mean the
same thing everywhere in this repository.

### How the rules are enforced

- **GAP.** `docs/gaps.md` holds a register table: row id, the exact deviation signed off, and a
  sign-off reading `<who> <YYYY-MM-DDTHH:MMZ>` (UTC; seconds optional). A row is GAP only when the
  deviation observed in the run is exactly the one signed off and the sign-off was made before the
  run started (the run's start time, `started_utc` in `run.json`). A sign-off with only a date
  counts from 23:59:59Z of that day, so it binds only to runs that start on a later day. If KubeMQ
  or Kafka changes so the deviation changes (for example a narrower version range), the row is
  FAIL until it is signed off again. Every track that reports GAP binds the observed deviation
  through the shared register reader (`suites/protocol/gaps`); the report renderer checks again,
  for every track, that the sign-off exists and was made before the run started,
  and that the deviation the row reports (`deviation` in the track's results) is exactly the
  register's Deviation cell. A GAP row without it is FAIL, and is also listed in `summary.json`
  (`gap_rows_without_deviation`).
- **VOID.** Counted against the 2% budget over *behaviour* rows only (rows where a scenario runs
  on both brokers). Advertisement rows, which compare what each broker lists in ApiVersions,
  cannot dilute the budget. Every VOID row needs an entry in `matrix/void-notes.yaml` for that
  KubeMQ version, with the link to the published oracle log pinned by digest
  (`<link>@sha256:<digest>`) and the root cause; until then the report fails.
- **Oracle preconditions (features track).** Every probe declares the documented Kafka outcome
  it must see on the oracle (`suites/features/probe/expect.go`). If the oracle does not show it,
  the probe is not measuring anything and the row is VOID, not PASS: two brokers that both fail
  to show a feature must never compare equal. The probes run the same code against both brokers;
  only this check, applied to the oracle leg, depends on which broker is which.
- **Rig self-check.** Full-transcript rows first compare the oracle with franz-go's in-memory
  `kfake` broker. If those two known-good brokers disagree, the rig cannot measure the row: it
  is NOT-RUN, not VOID, because the oracle itself did not fail.
- **FLAKY.** A row enters `matrix/quarantine.yaml` only after 2 of 3 consistent failures and a
  reviewer sign-off. An entry without a reason and a `<who> <YYYY-MM-DD>` sign-off quarantines
  nothing: the row stays FAIL. The number of releases it has spent FLAKY is counted from the
  summaries of earlier published runs, not maintained by hand; in its third release it is FAIL.
- **Published reports** cover every registered track; a track that did not run is NOT-RUN. A
  matrix whose track is missing from `runner/tracks.txt` is reported NOT-RUN too, never left out.
- **Amendments.** A human never edits a verdict. The only change after a run is an amendment,
  recorded in that run's `run.json` (`amendments`: row, from, to, reason, decided_by, time in
  UTC): it may reclassify a FAIL to NOT-RUN, and only for an infrastructure cause named in the
  entry. The results files stay exactly as the run wrote them; the renderer applies the amendment. It refuses any other
  amendment, or one whose "from" is not the recorded verdict, and lists every amendment in the
  report header and on its row.
- **Measurements.** A row of kind `measurement` (the performance baseline) records a number,
  not a verdict. Its value is listed in the detail; the summary says how many measurement rows
  each track has and how many did not complete. It is left out of the PASS/FAIL/GAP counts, the
  totals, the VOID budget and the report's status. The track sets the kind on the rows it runs; a
  matrix row marked `measurement: true` that never ran is listed as "measurement not run", also
  outside the counts. (A matrix row's own `kind` field names the track's scenario type.)
- **Opt-in tracks.** A track of the `opt-in` tier in `runner/tracks.txt` tests software under a
  separate license (for example Schema Registry under the Confluent Community License). It runs
  only with `--full --include-opt-in` or `--track <name>`, and only after the operator accepts
  that license (the variable its `track.env` names, set to `yes`). If it did not run, the report
  lists it as not run, outside the counts, and it does not fail the report. If it ran, it has its
  own counts in `COMPATIBILITY.md` and its verdicts count toward the run's status like any other.
- **Provenance.** A row that ran without a provenance (or `upstream` without a source naming the
  upstream test) fails the report.
- **Checked on every pull request** with no broker (`runner/report/cmd/check-gaps` in `pr.yml`):
  the gap register names only matrix rows and every sign-off has the required form; every matrix
  row records its provenance; every matrix track is registered; quarantine entries and VOID notes
  have the required fields.

## Known differences between the two legs

Nothing about the client changes between the oracle and KubeMQ legs. The broker-side and
driver-side differences below are listed so nobody has to find them; each can only make the
oracle faster or stricter, never hide a KubeMQ difference:

- The oracle sets `group.initial.rebalance.delay.ms=0` (Kafka's default is 3 seconds) so group
  rows do not wait. KubeMQ runs its defaults. This can only make the oracle faster.
- The oracle checks retention every second and backs its log cleaner off for one second
  (Kafka's defaults are 5 minutes and 15 seconds). Log-semantics rows wait up to 120 seconds for
  retention and compaction on both brokers; KubeMQ runs its defaults. This only makes the oracle
  faster.
- The oracle runs as a single KRaft node, so its internal topics (consumer offsets, transaction
  state, share-group state) are created with one copy and a minimum in-sync count of one.
- The segmentio driver allows real Kafka (and only real Kafka) a bounded wait for topic
  metadata to propagate after CreateTopics. KubeMQ gets no allowance. This can produce a false
  FAIL on KubeMQ, never a false PASS.

## Symmetric broker configuration

A few rows need a broker setting that differs from both brokers' defaults. Such a setting is
applied identically to both brokers, through each broker's own configuration, and listed here:

| Setting | Kafka oracle | KubeMQ | Why |
|---|---|---|---|
| Committed-offset retention, 1 minute | `offsets.retention.minutes=1` | `CONNECTORS_KAFKA_OFFSETS_RETENTION_MINUTES=1` | The offset-retention row cannot wait the default 7 days. It only affects a group that is empty and idle for over a minute; no other row has one. |

The oracle also checks offset retention every 5 seconds (Kafka's default is 10 minutes), a
cadence-only change like the retention and cleaner cadences above.

## Security track: per-posture brokers

Security settings (TLS certificates, SASL users, authorization) are read by both brokers only at
boot, so the security track does not use the plain broker pair. The runner boots a separate pair
for each posture (`TRACK_POSTURES` in `suites/security/track.env`), with the same posture on both
sides: a plain client port and a TLS client port. Each product is configured the way its own
documentation says, and the files are generated per run by `suites/security/posture`:

| Posture | Kafka oracle | KubeMQ |
|---|---|---|
| tls | listener `CLIENT` PLAINTEXT, `CLIENTTLS` SSL with a PEM keystore and truststore, no client certificate requested | global `security.cert` and `security.key` (KubeMQ has no Kafka-specific TLS settings), Kafka connector `port` and `tlsport` |
| mtls | as tls, plus `listener.name.clienttls.ssl.client.auth=required`, `StandardAuthorizer`, super users `ANONYMOUS` (admin listener only) and the admin certificate's distinguished name, ACLs created over the admin listener after boot | as tls, plus `security.ca` (which makes KubeMQ require client certificates), authentication on (required for authorization), authorization with a policy granting the admin certificate's distinguished name (the principal Kafka's super user names) everything |
| sasl | listener `CLIENT` SASL_PLAINTEXT, `CLIENTTLS` SASL_SSL; `sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256,SCRAM-SHA-512`; PLAIN users in each listener's JAAS config; SCRAM credentials (4096 iterations) written at storage format | as tls, plus the Kafka connector's `credentials` list (one store for PLAIN and both SCRAM mechanisms) and `scramiterations: 4096`; KubeMQ makes the plain port SASL_PLAINTEXT and the TLS port SASL_SSL whenever SASL is configured |
| oauth | as sasl, plus OAUTHBEARER on both listeners (`OAuthBearerValidatorCallbackHandler`), key set read from a file, expected issuer and audience | as sasl, plus the Kafka connector's `oauthbearer.issuer` and `clientid` (the audience); KubeMQ fetches the issuer's discovery document at boot and offers OAUTHBEARER only on its TLS port |
| reauth | as sasl, plus `connections.max.reauth.ms=10000` (KIP-368) | as sasl; KubeMQ has no re-authentication setting |
| acl | as sasl, plus `StandardAuthorizer`, super user `User:admin`, alice's ACLs created over the admin listener after boot (topics `kcert-acl-ok-*` Read/Write/Describe/Create, groups `kcert-acl-g-*` Read, transactional ids `kcert-acl-tx-*` Write/Describe, DENY Write on `kcert-acl-ok-blocked`) | as sasl, plus authentication and authorization with the equivalent policy: Kafka Read and Describe map to KubeMQ Read, Write and Create to Write; the DENY rule is created through CreateAcls after boot, as on Kafka |
| quota | as sasl, plus a default user quota of 262144 bytes per second for produce and fetch, set with AlterClientQuotas over the admin listener after boot | as sasl, plus the Kafka connector's `producebyterate` and `fetchbyterate` (per principal) |

Quota enforcement is measured with clients, not by inspecting the broker: franz-go (produce and
fetch) and sarama (produce) each move 6 MiB of incompressible records at a 256 KiB/s quota, and a
row checks that the client was held near the quota and that a throttle time was signalled. KubeMQ
stamps throttle times and leaves the waiting to the client (Kafka also delays its responses); both
clients wait out throttle times, so they are held on both brokers. A client that ignores throttle
times is not tested.

The GSSAPI (Kerberos) row has no oracle leg: running Kafka's GSSAPI listener needs a Kerberos
KDC and keytabs, and KubeMQ cannot be configured for GSSAPI at all. The row is of kind
"advertisement": the Kafka side is the documented capability, and the KubeMQ side is the answer of
KubeMQ's configuration validation when the released image is started with GSSAPI in its mechanism
list (validation runs before the license check, so no key is used). The row is listed in
`docs/gaps.md`. Delegation tokens, by
contrast, run on both brokers: the oracle's sasl posture sets `delegation.token.secret.key`.

Container client rows (the Java and librdkafka re-authentication rows) join the broker
container's network namespace (`--network container:<broker>` through `runner/client-run.sh`), so
the broker's advertised 127.0.0.1 address reaches the broker for both legs.

OAUTHBEARER tokens come from a per-run issuer (`suites/security/oidc`): an RSA signing key, a
discovery document and a key set written as static files. KubeMQ reads the discovery document over
HTTP from a small file server in the posture's compose project; Kafka reads the key set from the
same file. Tokens are RS256, signed by the probes. A client that follows RFC 7628 (as Kafka's Java
client does) answers the server's error challenge with 0x01; franz-go's own mechanism does not, so
the invalid-token rows use an RFC 7628 mechanism.

The mtls posture grants the same permission twice, written the way each product spells it: once to
alice's full distinguished name (Kafka's default principal for a certificate) on topic prefix
`kcert-sec-dn-`, and once to her common name alone on `kcert-sec-cn-`. KubeMQ's policy loader
refuses a client name containing a comma or a space (the broker does not start), so on KubeMQ the
distinguished name is written as a regular expression with `.` in their place; it still matches the
name.

KubeMQ names a client certificate's principal by its full distinguished name, as Kafka's default
principal mapping does, so a policy entry for a certificate names the distinguished name (as a
regular expression, commas and spaces written as `.`).

The certificate authority, the broker certificate and the client certificates are created for
each run (`suites/security/tlsca`), valid for two days, and stay in the run directory; nothing
secret is committed. The oracle also has a plaintext `ADMIN` listener that only the runner's
readiness check and track setup use; no row runs on it.

## The license key

The runner refuses to start without `KUBEMQ_LICENSE_KEY`, never prints it, redacts it from every
saved log, and does not pass it to track scripts or the report renderer. That boundary is
advisory, not a sandbox: the key is in the KubeMQ container's environment, so any process on
the host that can inspect containers can read it. Use an evaluation key.

KubeMQ data starts empty on every run (an in-memory store). Only its license folder (installation
id, the record of the accepted key, the signed lease) is kept, in one container volume per
machine (`kcert-kubemq-license`, override with `KUBEMQ_LICENSE_VOLUME`). The first boot on a
machine activates against the license server; every later boot, including concurrent runs,
reuses that lease and only refreshes it in the background, so a busy rig does not hit the
license server's rate limit. The run log says which happened ("license activated" or "license
reused"). The volume holds a credential: never copy it off the machine. Removing it only costs
one activation on the next boot.

## Allowed client-config deviations

Clients and tools run unmodified with upstream defaults, except for the deviations enumerated
here. Any other deviation makes the row FAIL, not PASS-with-note.

| Setting | Why | Applies to |
|---|---|---|
| `group.protocol=classic` | Every client's scenario row runs the same group protocol on both brokers. KubeMQ also serves the KIP-848 consumer group protocol (ConsumerGroupHeartbeat and ConsumerGroupDescribe are advertised with the same version ranges as Kafka, `matrix/apikeys.yaml`). | The scenario rows of the clients track (`matrix/clients.yaml`). |

Every client additionally has one mandatory row, "stock defaults, zero overrides, quickstart as
documented upstream", with its own verdict. The effect of the pinned client's default group
protocol is therefore measured, not assumed.

## Provenance

Every row records where its scenario comes from:

- `upstream` — an Apache Kafka or client-project test: ducktape system tests,
  `kafka-verifiable-producer` / `kafka-verifiable-consumer`, Connect and Streams integration
  tests, librdkafka's own tests. The row names the upstream test.
- `kubemq-authored` — written for this program or ported from the KubeMQ server repository.

The ratio is published in `COMPATIBILITY.md`. Upstream scenarios are preferred wherever one
exists.

## Verifier mutation tests

Every verifier ships a mutation test. The test feeds the verifier a known-good result with one
injected defect at a time — a lost record, a duplicated record, a reordered record, a wrong
offset, a wrong error code — and proves the verifier reports FAIL for each. These tests need no
broker and run on every pull request (`.github/workflows/pr.yml`).

## Version pins

`compose/versions.env` is the single source of truth for the Kafka image digest, the KubeMQ
image tag and client versions.
Clients carry their own lockfiles (`pom.xml`, `go.sum`, `package-lock.json`, hashed
`requirements.txt`, `packages.lock.json`). The supported client matrix is the latest stable
release plus the previous minor. Kafka release candidates are tracked only by a separate preview
report and never change a certified verdict.

## What gets published

This repository publishes the result of one full run, exactly as the renderer wrote it:

- `results/REPORT.md`, every row with what was observed on each broker, including the
  performance baseline's measurements;
- `results/summary.json`, the same verdicts in machine-readable form;
- `COMPATIBILITY.md`, the one-page summary: counts per track, the Kafka and KubeMQ images
  and every client version.

A run of your own writes the same three files into its run directory (`docs/rerun.md`), so you
can compare them with the published ones. A run against a Kafka release candidate
(`--kafka-version`) is a preview and never changes a published verdict.

### Broker-internal application scope

Only WriteTxnMarkers (27) and share-state persistence requests (83–87) may be classified NOT-APPLICABLE. The curated impact entry must mark the scope `broker-internal`; the request must be absent from KubeMQ's advertisement; and `docs/gaps.md` must list the exact observed range, with its sign-off, before the run starts. The protocol track and renderer both validate the sign-off. Unsigned, changed, late, or client-facing exclusions fail. Every excluded request remains visible in the generated matrix, result JSON, and reports, with its own count outside PASS/GAP. This certifies application replacement, not Kafka broker interoperability.
