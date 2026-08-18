# UDL Module

The `udl` module supports a tasking loop with the Unified Data Library: it
polls UDL for CollectRequests assigned to a sensor, executes them as
`StandardCollectTask`s on a controller, reports incremental progress back as
CollectResponses, and delivers data products back to UDL via configurable
publishers. Some supported features:

- **Target types** — each CollectRequest is converted to the first available of:
  Elset → `TLETarget`; state vector (J2000 / TEME / EFG-TDR / ICRF) →
  `StateVectorTarget`; RA/Dec → `ICRSTarget`. Requests carrying none of these
  are rejected.
- **Reporting** — CollectResponses at every
  stage: `ACCEPTED` on receipt, `COLLECTED` when the collect finishes (with the
  actual execution window), `COMPLETED` once the frame set has been delivered,
  and `REJECTED` / `CANCELLED` / `FAILED` on the respective failure paths.
- **Data publishers** — `SkyImagery` frame uploads and/or `EOObservation`
  records built from the senpai module's satellite detections, each enabled by
  its block under `publish` (see [Data Publishers](#data-publishers)).
- **Dual endpoints** — polling/responses and data upload can target two
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
   hands each frame to the enabled publishers
6. With `publish.sky_imagery` configured, each frame is zipped with its
   SkyImagery metadata and uploaded; once every frame of the set has been seen
   (and, with imagery enabled, at least one landed), a `COMPLETED` response
   closes out the request with the same execution window `COLLECTED` reported
7. With `publish.eo_observation` configured, the module also consumes the
   senpai module's published `SenpaiResult`s, correlates each solved result
   back to its CollectRequest by `task_id`, and POSTs the satellite
   detections' RA/Decs as EOObservations (`createBulk`, one batch per frame)

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
  publish:
    sky_imagery:                       # block present ⇒ imagery upload enabled
      image_type: FITS                 # default SkyImagery imageType
      # save_path: /data/udl           # optional; archive each upload ZIP locally
    # eo_observation:                  # block present ⇒ EOObservation posting enabled
    #   sequence_only: true            # only post from sequence-derived SenpaiResults
    #   mag_bands: [G]                 # calibrated-magnitude band priority
    #   # save_path: /data/udl/eo      # optional; archive posted records locally
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
`watch_directory` to the publishers) and the `keyword_map` feeds SkyImagery
metadata; the right side must mirror whatever the camera graph's
`array_to_fits` `header:` map wrote. The collect context provides `task_id`
(and `frame_num`/`frame_count`) to that map during standard collects, so the
camera graph writes them as ordinary header entries (`UDLREQID: "{task_id}"`
here). Mapping `task_id` back out is what correlates a frame to its
CollectRequest (required for publishing and the `COMPLETED` response);
`frame_num` drives `sequenceId` and set-completion counting. Unlike Otto, most
SkyImagery metadata (`satNo`, `classificationMarking`, `dataMode`, `origin`,
set length) comes from the polled CollectRequest itself rather than FITS
headers.

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

## Data Publishers

**SkyImagery** (`publish.sky_imagery`) — each collected frame is zipped with
its metadata JSON and POSTed to the UDL imagery filedrop, as described above.

**EOObservation** (`publish.eo_observation`) — metric observations built from
the senpai module's detections:

- The publisher subscribes to `SenpaiResult` keywords on the backend stream
  (a cross-entity wildcard, so the senpai entity needs no naming here) and
  warns loudly at startup if no `senpai` config section exists.
- Only solved results that correlate to a CollectRequest (`task_id`) produce
  records; frames `senpai` processed that weren't UDL-tasked are dropped.
- With `sequence_only: true` (default), only sequence-derived results — the
  `senpai` module's multi-frame batches, where rate frames inherit a
  sidereal-anchored WCS — are posted; per-frame results are skipped. Sequence
  processing is the `senpai` module's default; disabling it there
  (`process_sequence: false`) posts nothing under `sequence_only`.
- One record per satellite detection: `"point"` detections from rate-tracked
  frames, confirmed `"streak"` detections from sidereal frames.
- Observations are raw against the WCS — no catalog correlation — so records
  carry `uct: true` and no `satNo`. `trackId` groups a collect's frames via the
  CollectRequest id; `taskId`, classification marking, dataMode, and origin come
  from the CollectRequest.
- Per UDL's observation-correction requirements, `obTime` is the photon arrival
  time (mid-exposure) with light time left uncorrected, and annual aberration
  is added back to each position: a plate solve fits the frame onto catalog star
  positions and so subtracts the ~20.5" displacement Earth's orbital velocity
  imparts to starlight, but the satellite shares that velocity and was never
  displaced by it. Diurnal aberration needs no such fix — the solve removes it
  from the target correctly.
- Records POST via `createBulk` (one call per frame; a single-detection frame
  is a one-element batch) to the same endpoint SkyImagery uses (`api.upload`
  when configured, the primary otherwise).

To make `senpai`'s results correlatable (and to enable its sequence batching),
the camera graph must write `task_id`/`frame_num`/`frame_count` headers and
`senpai`'s `context_from_fits` must map them back — see the `senpai` module README.

## Dual Endpoints

Polling and CollectResponses always use the primary `api` endpoint. To route
data uploads (SkyImagery and EOObservations) elsewhere (e.g. poll UDL with
basic auth, upload to a cert-authenticated UDL-compliant endpoint), add an
`api.upload` block with its own connection and auth settings:

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

When `upload` is omitted, data goes to the primary endpoint.

## Usage

```sh
sensorkit service run udl sensorkit.udl.service
```