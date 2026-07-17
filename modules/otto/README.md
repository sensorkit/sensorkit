# Otto Module

Otto is an autonomous satellite observation program, meant for collecting large
training data sets. Given a list of NORAD IDs and/or orbit regimes, it fetches
TLEs, tracks object visibility, and continuously generates `StandardCollectTask`
offers over the configured camera parameter grid (filters × exposures × binnings).
Collected FITS imagery can optionally be published to Google Drive, Dropbox, and/or
the UDL SkyImagery filedrop. Some supported features:

- **Target selection** — random whitelist selection by default or via orbit class
- (i.e. LEO/MEO/GEO/HEO), both supporting `scan_mode`, which
  orders all visible objects by hour angle to walk the sky in one direction and
  cross the meridian only once (one pier flip per pass).
- **Visibility** — explicit `task.objects` move between a whitelist (visible now), a
  graylist (below the altitude floor but rising; re-promoted periodically), and
  a blacklist (below the floor and setting, or no TLE). Lists are persisted
  across restarts. A future `TODO` item will populate these lists from object
  SNR as measured by SENPAI.
- **Collection** — `track_mode` selects per-frame tracking: `rate` (follow the
  TLE throughout), `rate_sidereal` (append a sidereally tracked frame to an
  otherwise rate tracked sequence), or `sidereal` (slew to the TLE and begin tracking
  sidereally); optional dithering about the TLE center to randomize where the object
  lands on the focal plane.
- **Publishing** — use a DataGraph to publish data and metadata to one or more apps.

## How It Works

1. Otto fetches TLEs from Spacebook for the configured NORAD IDs and orbit
   regimes (and re-fetches every `tle_update_interval_hours`)
2. It evaluates satellite visibility (altitude, rising/setting) from the
   controller's site position and maintains the whitelist / graylist / blacklist;
   orbit-regime members are instead selected live — whichever are above the
   altitude floor at task-generation time
3. Visible objects are queued as `StandardCollectTask` entries — one task per
   filter × exposure × binning combination, `num_frames` frames each — and
   offered to the agent scheduler
4. Tasks are created in order of *filter* -> *exposure* -> *binning*, i.e. for a given
   exposure time, each binning setting is collected.
5. The agent scheduler picks up task offers and drives the controller through
   slew, track, and collect; queued tasks that outlive their `end_time` are
   dropped, and the deadline is refreshed at dispatch
6. Collected FITS files land in the configured data directory
7. If publishing is enabled, a DataGraph (`watch_directory -> read_file ->
   context_from_fits -> app_sink`) picks up each new FITS file and dispatches it
   to all configured publishers

Object lists, the TLE cache, and program state are persisted to the KV store.
The config is also watched live — changing `task.objects` resets the lists and
starts fresh.

## Example Config

```yaml
entity: otto
key: OttoConfig
value:
  controller: controller1                # named controller for the agent to task
  task:
    objects: ["40105", "38833", "42741"] # NORAD IDs to observe
    orbits: []                           # Orbit regimes to observe (LEO/MEO/GEO/HEO), e.g. ["GEO"]
    tle_update_interval_hours: 4         # How often to poll Spacebook for new TLEs
    graylist_interval_minutes: 15        # How often to promote graylist -> whitelist
    end_time_deadband_seconds: 300       # Extra time added to each task's deadline
    inter_task_delay_seconds: 0          # Fixed pause after each completed task (0 = back-to-back)
  collect:
    altitude_min: 20                     # Minimum observing altitude (degrees)
    track_mode: rate_sidereal            # rate | sidereal | rate_sidereal (see above)
    scan_mode: false                     # walk all visible objects by hour angle
    scan_direction: eastward             # eastward | westward (scan_mode only)
    dither: false                        # randomly perturb the TLE pointing per task
    dither_amount_arcsec: 500            # max on-sky offset, uniform in [0, this]
    filters: ["Filter 1"]                # filters to cycle ([] = no filter selection)
    exposure_min: 1                      # exposure set lower bound (seconds)
    exposure_max: 5                      # exposure set upper bound (seconds, inclusive)
    exposure_delta: 1                    # spacing of the exposure set
    binning: [1, 2]                      # binning set
    num_frames: 10                       # frames per task
  publish:
    upload: true                         # master switch for publishing
    env_file: .env                       # publisher credentials (see Publishing below)
    gdrive:
      folder_id: <your_folder_id>        # destination Drive folder
    dropbox:
      upload_path: /otto/imagery         # destination Dropbox folder
    udl:
      # base_url: https://test.unifieddatalibrary.com   # omit for production UDL
      id_sensor: MY_UDL_SENSOR           # UDL-registered sensor ID (SkyImagery idSensor)
      source: MY_ORG                     # UDL provenance: org/system originating the record
      data_mode: TEST                    # SkyImagery dataMode

---

entity: otto
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
        task_id: TASK_ID
        sat_no: NORADID
      output:
        - sink
    sink:
      op: app_sink
```

Both blocks live on the same entity — the program reads its `DataGraph` from its
own entity name, so keep them identical (here `otto` for example, matching the `-n otto`
service name below).

The `otto` DataGraph is structured to monitor the directory defined in the camera DataGraph
for new FITS files. Currently, only the UDL publishing component makes use of the `keyword_map`, 
which is used to generate SkyImagery metadata. Note, the right side of this
`keyword_map` must mirror the camera DataGraph's `header:` map — the two configs
are the two halves of a context → header → context round trip.

## Publishing

Otto can publish collected FITS imagery to one or more destinations
simultaneously. Each destination is configured as a subsection under `publish:`,
and only destinations with a config block present are active. Set
`upload: true` to enable publishing.  Credentials live in `env_file` (default `.env`); any
key not present in the file falls back to the process environment variables:

| Destination | `env_file` keys |
| ----------- | --------------- |
| `gdrive`    | `GDRIVE_TOKEN_PATH` (path to the saved OAuth token) |
| `dropbox`   | `DROPBOX_APP_KEY`, `DROPBOX_APP_SECRET`, `DROPBOX_REFRESH_TOKEN` |
| `udl`       | `UDL_USERNAME`, `UDL_PASSWORD` |

### Google Drive Setup

Otto uploads FITS files to a Google Drive folder using OAuth2. This requires a one-time setup:

#### 1. Create a Google Cloud project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project (or select an existing one)
3. Enable the **Google Drive API** (APIs & Services > Library > search "Google Drive API" > Enable)

#### 2. Configure the OAuth consent screen

1. Go to APIs & Services > OAuth consent screen
2. Select **External** as the user type
3. Fill in the required app name and contact fields
4. Under **Test users**, add the Google account email that owns the target Drive folder

#### 3. Create OAuth2 credentials

1. Go to APIs & Services > Credentials > Create Credentials > OAuth client ID
2. Select **TVs and Limited Input devices** as the application type (required —
   the device flow below only serves the Drive scope for this client type)
3. Copy the resulting client ID and client secret (no file download needed)

#### 4. Generate the token

Paste your client ID and secret into this one-time script (stdlib only — no
extra packages) and run it:

```python
import json, time, urllib.parse, urllib.request

CLIENT_ID = "<your_client_id>"
CLIENT_SECRET = "<your_client_secret>"

def post(url, **data):
    req = urllib.request.Request(url, urllib.parse.urlencode(data).encode())
    try:
        return json.load(urllib.request.urlopen(req))
    except urllib.error.HTTPError as e:
        return json.load(e)  # OAuth errors ride 4xx responses

dev = post(
    "https://oauth2.googleapis.com/device/code",
    client_id=CLIENT_ID,
    scope="https://www.googleapis.com/auth/drive.file",
)
print(f"Visit {dev['verification_url']} and enter code: {dev['user_code']}")

while True:
    time.sleep(dev.get("interval", 5))
    tok = post(
        "https://oauth2.googleapis.com/token",
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        device_code=dev["device_code"],
        grant_type="urn:ietf:params:oauth:grant-type:device_code",
    )
    if tok.get("error") == "authorization_pending":
        continue
    if "error" in tok:
        raise SystemExit(tok.get("error_description", tok["error"]))
    break

with open("token.json", "w") as f:
    json.dump(
        {
            "type": "authorized_user",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": tok["refresh_token"],
        },
        f,
        indent=2,
    )
print("Saved token.json")
```

It prints a URL and a short code — open the URL in a browser, enter the code, and approve.

#### 5. Add the token path to your `.env`

```bash
# .env
GDRIVE_TOKEN_PATH=/path/to/token.json
```

#### 6. Get the folder ID

Open the target folder in Google Drive. The folder ID is the last part of the
URL — it goes in the `gdrive.folder_id` config field:

```
https://drive.google.com/drive/folders/<FOLDER_ID>
```

#### 7. Configure otto

Add the `gdrive` block under `publish` in your otto config YAML (see example above), then load it:

```sh
sensorkit kv load /path/to/<my_otto_config>.yaml
```

### Dropbox Setup

Otto uploads FITS files to Dropbox using an app with a long-lived refresh token.

1. Create an app at [Dropbox App Console](https://www.dropbox.com/developers/apps)
2. Set Permissions: `files.content.write`
3. In Settings, copy `App key` and `App secret`
4. Generate and copy the refresh token using the OAuth2 flow
5. Add `DROPBOX_APP_KEY`, `DROPBOX_APP_SECRET`, and `DROPBOX_REFRESH_TOKEN` to
   your `.env` (or export them), and configure the `dropbox` block in your otto
   config

### UDL

Otto uploads each collected frame to the UDL SkyImagery filedrop: the FITS file
is zipped together with a SkyImagery metadata JSON and POSTed as raw
`application/zip` (with Basic auth, via the `udl-sdk` client's HTTP transport)
to the imagery filedrop host. `base_url` selects the environment — omit it for
production UDL, set `https://test.unifieddatalibrary.com` for the test
environment, or point it at any UDL-compliant endpoint, which serves the
filedrop on the same host.

`id_sensor` is the UDL-registered sensor ID (SkyImagery `idSensor` — which
sensor collected the image). `source` is UDL's mandatory provenance field,
carried on every UDL record: the organization or system originating the data,
as distinct from the sensor that collected it. Frame metadata
(exposure, dimensions, optional `satNo`) is filled from the
DataGraph context when the `keyword_map` provides it — e.g. map
`date_obs: DATE-OBS`, `exptime: EXPTIME`, and `sat_no: <your NORAD header>` to
enrich the record.

```bash
# .env (or export as environment variables)
UDL_USERNAME=<your_udl_username>
UDL_PASSWORD=<your_udl_password>
```

Note: otto only *delivers imagery* to UDL. For the full UDL tasking round-trip —
polling CollectRequests and posting CollectResponses — use the standalone `udl`
program module instead.

## Usage

```sh
sensorkit service run otto sensorkit.otto.program
```

## TODO

- **Configurable TLE sourcing** — allow fixed-file (e.g. no internet connection)
  or unique sourcing (e.g. Space-Track, Celestrak, etc).
- **SNR-driven list transitions** — promote/demote objects across the
  whitelist / graylist / blacklist using measured object SNR fed back from
  SENPAI, rather than visibility geometry alone.
