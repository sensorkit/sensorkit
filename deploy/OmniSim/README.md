# OmniSim deployment — `go` and Docker

Drives the Alpaca **OmniSimulator** (running on the host) with a **synthetic
`sdasim` camera** in place of the simulator's camera. Runs bare-metal
(`sensorkit go`) or as a Docker stack. Program: Otto; plus a SENPAI
plate-solve/streak analyzer.

> **The camera is `sdasim`, not the OmniSimulator's.** The OmniSimulator provides
> the weather, dome, mount, rotator, focuser, and filter wheel. The camera device
> is `sdasimCamera`, which renders each frame from the live `OmniSimTelescope`
> pointing (star field + Space-Track satellites). `OmniSimCamera` is not used.

## Files

| file | purpose |
|---|---|
| `omnisim.yaml` | unified SensorKit config |
| `docker-compose.yaml` | the Docker stack |
| `sdasim_OmniSim.yaml` | sdasim scene — sensor, star catalog, observer site |
| `senpai_OmniSim.yaml` | SENPAI engine config |

## Prerequisites

- **OmniSimulator running on the host, bound to `0.0.0.0:11111`** (not
  `127.0.0.1`). Be sure to update your site coordinates for the simulator.
- Host paths, used verbatim (Docker bind-mounts them 1:1 to the same locations):
  - `/path/to/.env` — Slack token + Space-Track credentials
  - `/path/to/data` — FITS output
  - `/path/to/catalogs` — `sstrc7` star catalog + astrometry `indices/4100`

## Config

- Example path setup, i.e. replace all `/path/to` references with `C:/sk` on Windows or `/opt/sk` on Linux/macOS, for:
  - `omnisim.yaml`
  - `docker-compose.yaml`
  - `sdasim_OmniSim.yaml`
  - `senpai_OmniSim.yaml`
- Update the site coordinates in the OmniSimulator, in `omnisim.yaml` (`sensors[].site_position`, `config.OmniSim.SitePosition`), and in `sdasim_OmniSim.yaml`
- Update your Slack channel names in `omnisim.yaml` (or remove the references here and in `docker-compose.yaml` if not configured)

- Toggle the Alpaca endpoint host at `omnisim.yaml` → `alpaca[].endpoints[].host`:
  - Bare-metal (`go`) → `localhost`
  - Docker → `host.docker.internal`

- Toggle SENPAI's astrometry backend at `senpai_OmniSim.yaml` → `app.astrometry.docker_image`:
  - Bare-metal (`go`) → `astrometry-cli`
  - Docker → `null`

**Editing config needs a reload** (Docker): `docker compose … up config`, or
  restart the stack. Bare-metal `go` re-reads on launch.

## Run

**Bare-metal** — needs a running NATS (`nats://localhost:4222`):

```bash
sensorkit go -c /path/to/deploy/OmniSim/omnisim.yaml
```

**Docker**:

```bash
docker compose -f /path/to/deploy/OmniSim/docker-compose.yaml up -d --build
docker compose -f /path/to/deploy/OmniSim/docker-compose.yaml down
```