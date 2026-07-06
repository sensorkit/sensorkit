# UDL Module

The `udl` module supports a tasking loop with the Unified Data Library: it
polls UDL for CollectRequests assigned to a sensor, executes them as
`StandardCollectTask`s on a controller, reports incremental progress back as
CollectResponses, and delivers the collected FITS imagery to UDL as SkyImagery.
Some supported features:

- **Target types** — each CollectRequest is converted to the first available of:
  Elset → `TLETarget`; state vector (J2000 / TEME / EFG-TDR / ICRF) →
  `StateVectorTarget`; RA/Dec → `ICRSTarget`. Requests carrying none of these
  are rejected.
- **Reporting** — CollectResponses at every
  stage: `ACCEPTED` on receipt, `COLLECTED` when the collect finishes (with the
  actual execution window), `COMPLETED` once the imagery set has landed in UDL,
  and `REJECTED` / `CANCELLED` / `FAILED` on the respective failure paths.
- **Dual endpoints** — polling/responses and imagery upload can target two
  different UDL-compliant endpoints with independent auth (`api.upload`).

## How It Works

1. The program polls UDL every `poll_frequency` seconds for CollectRequests
   whose `poll_filter` field (`idSensor` by default, or `origSensorId`) equals
   `api.id_sensor` and that are still executable (`endTime` in the future,
   `startTime` within the next day)
2. New requests are queued (sorted by start/end time), published to the agent
   scheduler as offers, and acknowledged with an `ACCEPTED` CollectResponse
3. The scheduler dispatches each request as a `StandardCollectTask` — target
   from the Elset / state vector / RA-Dec, exposure from `integrationTime`
   (ms), frame count from `numFrames` — with the request's `endTime` (plus
   `end_time_deadband_s`) as the execution deadline; requests that outlive
   their `endTime` are dropped from the queue
4. On success a `COLLECTED` response reports the actual execution window (`actualStartTime`, `actualEndTime`);
   failures send `FAILED` or `CANCELLED` instead
5. Collected FITS files land in the configured data directory, where the
   DataGraph (`watch_directory -> read_file -> context_from_fits -> app_sink`)
   hands each frame to the imagery publisher
6. Each frame is zipped with its SkyImagery metadata and uploaded; once every
   frame of the set has been attempted (and at least one landed), a `COMPLETED`
   response closes out the request with the same execution window `COLLECTED`
   reported

Pending requests are persisted to the KV store and restored — and re-offered —
on restart.

## Example Config

```yaml
entity: udl
key: UDLConfig
value:
  controller: controller1              # named controller for the agent to task
  poll_frequency: 10.0                 # seconds between CollectRequest polls
  end_time_deadband_s: 300             # extra seconds added to each task's deadline
  # skyimagery_save_path: /data/udl    # optional; archive each upload ZIP locally
  image_type: FITS                     # default SkyImagery imageType
  api:
    # base_url: https://test.unifieddatalibrary.com   # omit for production UDL
    id_sensor: MY_UDL_SENSOR           # UDL-registered sensor ID
    poll_filter: id_sensor             # CollectRequest field matched against id_sensor:
                                       # id_sensor (default) | orig_sensor_id
    source: MY_ORG                     # UDL provenance: org/system originating records
    env_file: .env                     # UDL_USERNAME / UDL_PASSWORD
    timeout: 60.0                      # JSON API request timeout (seconds)
    upload_timeout: 300.0              # SkyImagery upload timeout (imagery can be large)

---

entity: udl
key: DataGraph
value:
  nodes:
    source:
      op: watch_directory
      directory: /path/to/data
      match: "*.fits"
      output:
        - read_fits_file
    read_fits_file:
      op: read_file
      output:
        - read_context
    read_context:
      op: context_from_fits
      keyword_map:
        task_id: UDLREQID         # the module writes this header automatically;
                                  # this line maps it back out for correlation
        frame_num: FRAMENUM
        image_width: NAXIS1
        image_height: NAXIS2
        date_obs: DATE-OBS        # SkyImagery exposure window
        exptime: EXPTIME          # SkyImagery exposure window
      output:
        - sink
    sink:
      op: app_sink
```

Both blocks live on the same entity — the program reads its `DataGraph` from
its own entity name, so keep them identical (here `udl` for example, matching the `-n udl`
service name below).

As with Otto, the DataGraph is the delivery pipeline (bytes ride it from
`watch_directory` to the publisher) and the `keyword_map` feeds SkyImagery
metadata; the right side must mirror whatever the camera graph's
`array_to_fits` `header:` map wrote. The exception is `UDLREQID`: the module
stamps it into every frame itself — the task submission carries a pre-populated
`FITSHeader` card through the execution context, which `array_to_fits` writes
automatically, so no camera-graph header changes are needed. Mapping it back as
`task_id` is what correlates a frame to its CollectRequest (required for
publishing and the `COMPLETED` response); `frame_num` drives `sequenceId` and
set-completion counting. Unlike Otto, most SkyImagery metadata (`satNo`,
`classificationMarking`, `dataMode`, `origin`, set length) comes from the polled
CollectRequest itself rather than FITS headers.

## Authentication

Credentials live in `api.env_file` (default `.env`); any
key not present in the file falls back to the process environment variables:

```bash
# .env
UDL_USERNAME=<your_udl_username>
UDL_PASSWORD=<your_udl_password>
```

Set both or neither — with neither, requests are issued unauthenticated (for
local UDL-compliant endpoints that don't enforce auth). For endpoints that
require client certificates instead, set `use_certs: true` with `client_cert`,
`client_key`, and optionally `client_verify: false`:

```yaml
  api:
    base_url: https://udl-compliant.example
    use_certs: true
    client_cert: /path/to/client.cert
    client_key: /path/to/client.key
```

## Dual Endpoints

Polling and CollectResponses always use the primary `api` endpoint. To route
SkyImagery uploads elsewhere (e.g. poll UDL with basic auth, upload to a
cert-authenticated UDL-compliant endpoint), add an `api.upload` block with its
own connection and auth settings:

```yaml
  api:
    id_sensor: MY_UDL_SENSOR
    source: MY_ORG
    env_file: .env
    upload:
      base_url: https://udl-compliant.example
      use_certs: true
      client_cert: /path/to/client.pem
      client_key: /path/to/client.key
```

When `upload` is omitted, imagery goes to the primary endpoint.

## Usage

```sh
sensorkit service run udl sensorkit.udl.service
```

## TODO

- **Data delivery** — support other data types, e.g. EOObservation.