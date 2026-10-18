# Rerunning the suite

Everything needed to reproduce the published result is in this repository. A run starts the
Apache Kafka oracle and the latest KubeMQ image on your machine, runs every row against both, and
writes the same report that is published in `results/`.

## Requirements

- Docker (Docker Desktop on macOS, or Docker Engine on Linux) or podman with `podman compose`.
- Host tools: `curl`, `lsof`, `jq`, `tar`, `git`, `awk`, `sed`. `lsof` proves a port is free
  before boot and owned by the broker after it; minimal Linux images often lack it
  (`sudo apt-get install -y lsof`).
- arm64 or amd64. Every image used is multi-arch.
- Go, at least the version in `suites/protocol/go.mod`. The protocol track and the report
  renderer are built at run time; the runner checks Go before starting anything.
- Client drivers (clients track): each driver is built locally from its pinned sources when its
  prebuilt image (`suites/clients/clients.env`) cannot be pulled, automatically.
- A KubeMQ evaluation key. Request one from KubeMQ and export it:

  ```bash
  export KUBEMQ_LICENSE_KEY=<your evaluation key>
  ```

  The runner refuses to start without it, never prints it, and redacts it from every saved log
  (`docs/methodology.md`, "The license key").

## The images

The Kafka oracle is the official `apache/kafka` 4.3.1 image, pinned by digest in
`compose/versions.env`. KubeMQ is the latest released `kubemq-next` image
(`KUBEMQ_VERSION=latest`): the runner resolves it to a digest when the run starts, runs that
digest for the whole run, and records it in the report header and in `run.json`.

## Quick run (about 15 minutes)

```bash
runner/certify.sh --quick
```

Starts the Kafka oracle and a single KubeMQ node, runs the protocol track against both, and
renders the report.

## Full run (1 to 2 hours)

```bash
runner/certify.sh --full
```

Runs every track in `runner/tracks.txt`. The opt-in Schema Registry track runs only with
`--full --include-opt-in` and after you accept its license (`docs/tools.md`).

## One track

```bash
runner/certify.sh --track protocol
```

`runner/certify.sh --list` lists the tracks.

## Check your machine first

```bash
runner/certify.sh --preflight-only
```

Checks the license key is set (never prints it), the host tools (`curl`, `lsof`, `jq`, `tar`,
`git`), Go, the container engine and compose, refuses emulated architectures, compares the
engine's clock with the host's, warns when the machine already has over 10,000 TCP
connections, refuses ports inside the machine's range for outgoing connections, and proves
every port is free. Starts nothing.

## Engine selection

The runner detects the engine: docker first, then podman. Force one with
`CONTAINER_ENGINE=docker` or `CONTAINER_ENGINE=podman`. When the docker command talks to a
podman machine, the runner notices and uses podman's host alias.

Containers reach the host through `host.docker.internal` (Docker) or
`host.containers.internal` (podman); the runner picks the right one. On macOS published ports
bind to the loopback address only. On Linux they bind to all interfaces, because containers
reach the host through the bridge address; set `CERT_BIND_ADDR` to override.

## Where output goes

Each run writes to `.work/run-<UTC timestamp>/` (override with `--run-dir`): broker logs,
one log per track, results JSON and `run.json` with every version and digest. Nothing in
`.work/` is committed. The license key is redacted from every saved log and is not passed to
track scripts.

## The report

Every run ends with the report renderer. It writes `REPORT.md`, `summary.json` and
`COMPATIBILITY.md` into `<run directory>/report/` and leaves the repository untouched. Compare
them with the published `results/REPORT.md`, `results/summary.json` and `COMPATIBILITY.md`.

The renderer applies the rules of `docs/methodology.md`: a matrix row without a verdict is
NOT-RUN, a difference not listed in `docs/gaps.md` before the run is FAIL, a quarantined FAIL is
FLAKY for at most two releases, and more than 2% VOID rows in a track fails it. A listed
difference is shown as OPTIONAL.

To list every row of a run that is not PASS and is not listed in `docs/gaps.md`:

```bash
runner/report/check-known-deviations.sh <run directory>
```

It fails when the report is missing or any such row exists.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Report passed. |
| 1 | Report failed: a FAIL, a NOT-RUN, or VOID rows over budget. |
| 2 | Preflight or infrastructure failure; nothing was certified. |

## Ports

Each run picks a free block of ports between 24000 and 25999, chosen from its run id, and logs
them ("ports: block N chosen from run id ..."). Several runs can therefore share one machine.
To pin ports, export all four of `KAFKA_HOST_PORT`, `KAFKA_GATEWAY_PORT`, `KUBEMQ_KAFKA_PORT` and
`KUBEMQ_API_PORT`. None of the ports is a KubeMQ standard port, so a KubeMQ you already run on the
same machine is never dialed by mistake, and all are below 32768 (see Known issues). The range is
in `compose/versions.env` (`CERT_PORT_BLOCK_BASE`).

## Continuous integration

`.github/workflows/pr.yml` is the only workflow: lint, the verifier self-tests and the renderer
tests, on every pull request including forks. It never starts a broker and holds no secret: no
license key is stored in GitHub and no workflow starts KubeMQ. Runs against the brokers are made
on real machines with `runner/certify.sh`.

## Known issues

- The protocol track and the report renderer are built with Go at run time, so a rerun needs
  Go.
- Ports must be below the operating system's range for outgoing connections (49152-65535 on
  macOS, usually 32768-60999 on Linux); the defaults are all below 32768. Every program's
  outgoing connection takes a local port from that range, so a port inside it can be held at any
  moment, and publishing it fails with "address already in use" although nothing listens there.
  `lsof` does not show such ports, and restarting the container engine does not free them; only
  the connection ending does. The runner refuses any configured port inside the range before
  starting anything.
- Brokers are reached at 127.0.0.1, not "localhost": ports are published on IPv4 loopback only,
  and "localhost" also resolves to the IPv6 address ::1, which is never published.
- Between runs the runner waits up to 30 seconds (`PORT_FREE_WAIT`) for its ports to be released.
