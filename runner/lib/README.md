# runner/lib

Vendored from `kubemq-server` (commit recorded in each file header). Re-vendor on change; do
not edit here.

| File | Used by |
|---|---|
| `port-preflight.sh` | `certify.sh`: refuses KubeMQ standard ports and proves a port is free before boot. |

The cluster track was removed, and with it the vendored cluster fault-injection helpers.

Files written for this repository (not vendored): `engine.sh`, `preflight.sh`, `brokers.sh`.
Tracks start client containers through `runner/client-run.sh`, which wraps `engine_client_run`.
