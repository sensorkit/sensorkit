# Mock UDL

A local UDL-compliant endpoint for developing and testing the `udl` module
without real tasking infrastructure. It serves just-in-time generated,
schema-compliant CollectRequests that target a real satellite currently above
20° from your site, and validates-then-discards everything the module pushes
back (CollectResponses, EOObservations, SkyImagery).

The tasking loop is reactive: one CollectRequest is live at a time, re-served
on every poll until its 5-minute window ends **or** a terminal CollectResponse
(COLLECTED / COMPLETED / REJECTED / FAILED) arrives — then the next poll gets a
fresh request. Targets are derived from the chosen satellite's TLE — served as
the elset itself (`tle`), a propagated J2000 state vector (`sv`), or its
topocentric RA/Dec at window midpoint (`radec`). Exposure (1–5 s), frame count
(3–6), and binning (1–4, carried in `notes` — UDL has no binning field) are
randomized per request. TLEs come from Spacebook (public, no credentials).

## Setup

The TLS cert/key always live at `certs/mock_udl.pem` / `certs/mock_udl.key`
beside `service.py` (gitignored). One self-signed cert serves TLS, acts as the
trust anchor for client-cert auth, and doubles as the client cert the udl
module can present. Generate it once:

```sh
openssl req -x509 -newkey rsa:4096 -sha256 -days 3650 -nodes \
  -keyout deploy/simulated/udl/certs/mock_udl.key \
  -out deploy/simulated/udl/certs/mock_udl.pem \
  -subj "/C=US/O=SensorKit/OU=MockUDL/CN=mock-udl" \
  -addext "subjectAltName=DNS:localhost,DNS:mock-udl,IP:127.0.0.1"
```

## Run

An independent app — no SensorKit service registration. It auto-loads the
nearest `.env` (the same one your other SensorKit services use):

```sh
python deploy/simulated/udl/service.py
```

Environment (`MOCK_UDL_*`):

| Variable | Default | Meaning |
|---|---|---|
| `MOCK_UDL_PORT` | `9000` | HTTPS port |
| `MOCK_UDL_UPLOAD_PORT` | unset | When set, also listen on this port — a second origin for exercising the `api.upload` SkyImagery split |
| `MOCK_UDL_ID_SENSOR` | `MockSensor` | Served as `idSensor` **and** `origSensorId`; must match the udl module's `api.id_sensor` |
| `MOCK_UDL_IDLE_S` | `0` | Cooldown between requests (seconds) — pace tasking below SENPAI's throughput for long soaks |
| `MOCK_UDL_TARGET_TYPE` | `tle` | `tle` \| `sv` \| `radec`, or a comma list to randomize |
| `MOCK_UDL_USERNAME` / `MOCK_UDL_PASSWORD` | `udl` / `udl` | Accepted Basic credentials (mirror as `UDL_USERNAME`/`UDL_PASSWORD` in the module's env_file) |
| `MOCK_UDL_CONTROLLER` | unset | Controller entity to read `SitePosition` from (over the backend; honors `NATS_URL`) |
| `MOCK_UDL_LATITUDE` / `MOCK_UDL_LONGITUDE` / `MOCK_UDL_ALTITUDE_KM` | unset | Site coordinates when no controller is given |
| `MOCK_UDL_TLES` | Spacebook | TLE catalog override: URL or local 2-line/3-line file |

## Pointing the udl module at it

```yaml
udl:
  - id: MACHINA            # whatever your deployment calls it
    controller: <controller>
    api:
      base_url: https://localhost:9000
      id_sensor: MockSensor          # = MOCK_UDL_ID_SENSOR
      source: <your-source>
      # basic auth: put UDL_USERNAME=udl / UDL_PASSWORD=udl in env_file
      env_file: /path/to/.env
      client_verify: false           # self-signed
      # — or cert auth instead: —
      #use_certs: true
      #client_cert: <repo>/deploy/simulated/udl/certs/mock_udl.pem
      #client_key: <repo>/deploy/simulated/udl/certs/mock_udl.key
      #client_verify: false
```

To exercise the separate-upload-endpoint feature (SkyImagery posted to a
different base_url than polling/responses), set `MOCK_UDL_UPLOAD_PORT=9010`
and add:

```yaml
      upload:
        base_url: https://localhost:9010
        env_file: /path/to/.env      # same mock credentials
        client_verify: false
```

Basic auth is checked strictly. Cert auth is enforced at the TLS layer: a
presented client cert must verify against the mock's own cert. A request with
neither (no Authorization header, no client cert) is allowed through — uvicorn
doesn't expose the peer certificate to the app, so the two cases are
indistinguishable there; don't treat the mock as an auth reference.

## Docker

```sh
docker compose --env-file /path/to/.env up
```

See `docker-compose.yml` — MOCK_UDL_* values pass through from your env file,
the cert/key ride along inside the source mount, and the container listens on
9000/9010.

## Endpoints

| Endpoint | Behavior |
|---|---|
| `GET /udl/collectrequest` | JIT tasking; honors `startTime`/`endTime` operators (`<`, `>`), `idSensor`, `origSensorId`, `firstResult`, `maxResults` |
| `POST /udl/collectresponse` | Validate (UDL schema via SDK models) → track lifecycle → discard; 201 |
| `POST /filedrop/udl-eo` (also `/udl/eoobservation/createBulk`) | Validate each record → discard; 202 |
| `POST /filedrop/udl-skyimagery` | Raw `application/zip` **or** the SDK's multipart form; validates the two-file zip layout and metadata JSON; 202 |

Validation failures return 400 with the pydantic error text and log a warning
server-side.
