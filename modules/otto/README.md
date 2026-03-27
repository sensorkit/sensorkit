# sensorkit-otto

Otto is an autonomous satellite observation program for SensorKit. It automatically generates tasks for specified NORAD-cataloged objects, schedules them based on visibility windows, and optionally publishes collected FITS imagery to external storage (Google Drive, Dropbox, or UDL).

## How It Works

1. Otto fetches TLEs from Spacebook for the configured NORAD IDs
2. It evaluates satellite visibility (altitude, rising/setting) from the sensor's location and maintains a whitelist (good-to-go), graylist (check back later), and blacklist (not visible for the operating period)
3. Whitelist objects are queued as `StandardCollectTask` entries with the configured collection parameters
4. The agent scheduler picks up task offers and drives the controller through slew, track, and collect
5. Collected FITS files land in the configured data directory
6. If publishing is enabled, a DataGraph (`WatchDirectory -> ReadFile -> ContextFromFITS -> AppSink`) picks up each new FITS file and dispatches it to all configured publishers

## Example Config

```yaml
entity: otto
key: OttoConfig
value:
  entity: otto_program
  controller: controller1
  task:
    objects: ["40105", "38833", "42741"] # NORAD IDs to observe
    tle_update_interval_hours: 4         # How often to poll Spacebook for new TLEs
    graylist_interval_minutes: 15        # How often to promote from graylist
    end_time_deadband_seconds: 3600      # Extra time added to task end time
  collect:
    altitude_min: 20                     # Minimum observing altitude (degrees)
    track_mode: rate_sidereal            # Collection mode [rate, sidereal, rate_sidereal]
    dither: false                        # Whether to add a random dither to the pointing (not yet implemented)
    dither_amount_arcsec: 500            # Random offset, taken from uniform distribution between 0 and dither_amount_arcsec
    filters: ["Filter 1"]                # Filters to observe (empty [] = no filter selection)
    exposure_min: 1                      # Min per-frame exposure (seconds)
    exposure_max: 5                      # Max per-frame exposure (seconds)
    exposure_delta: 1                    # Spacing in the exposure time set
    binning: [1, 2]                      # Binning set
    num_frames: 10                       # Frames per task
  publish:
    upload: true
    gdrive:
      credentials_file: ${GDRIVE_CREDENTIALS_PATH}
      token_file: ${GDRIVE_TOKEN_PATH}
      folder_id: ${GDRIVE_FOLDER_ID}
    dropbox:
      app_key: ${DROPBOX_APP_KEY}
      app_secret: ${DROPBOX_APP_SECRET}
      refresh_token: ${DROPBOX_REFRESH_TOKEN}
      upload_path: /otto/imagery
    udl:
      username: ${UDL_USERNAME}
      password: ${UDL_PASSWORD}
      sensor_name: MY_SENSOR
  server:
    host: 0.0.0.0
    port: 8001
    log_level: INFO

---

entity: otto_program
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
        frame_num: FRAMENUM
        image_width: NAXIS2
        image_height: NAXIS1
      output:
        - sink
    sink:
      op: app_sink
```

## Publishing

Otto can publish collected FITS imagery to one or more destinations simultaneously. Each destination is configured as a subsection under `publish:`. Only destinations with a config block present are active. Set `upload: true` to enable publishing.

Config values that reference environment variables use the `${VAR_NAME}` syntax. These are resolved at runtime from the process environment.

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
2. Select **Desktop app** as the application type
3. Download the resulting `credentials.json` file
4. Place it somewhere accessible (e.g., `/opt/sk/credentials.json`)

#### 4. Generate a token

Run the following one-time script to authorize and save a refresh token:

```python
from google_auth_oauthlib.flow import InstalledAppFlow

flow = InstalledAppFlow.from_client_secrets_file(
    "/path/to/credentials.json",
    scopes=["https://www.googleapis.com/auth/drive.file"],
)
creds = flow.run_local_server(port=0)
with open("/path/to/token.json", "w") as f:
    f.write(creds.to_json())
print("Saved token.json")
```

This opens a browser window for Google account authorization. After consent, a `token.json` file is saved containing the refresh token.

> Requires `google-auth-oauthlib`: `pip install google-auth-oauthlib`

#### 5. Get the folder ID

Open the target folder in Google Drive. The folder ID is the last part of the URL:

```
https://drive.google.com/drive/folders/<FOLDER_ID>
```

#### 6. Set environment variables

```bash
export GDRIVE_CREDENTIALS_PATH=/path/to/credentials.json
export GDRIVE_TOKEN_PATH=/path/to/token.json
export GDRIVE_FOLDER_ID=<your_folder_id>
```

#### 7. Configure otto

Add the `gdrive` block under `publish` in your otto config YAML (see example above), then load it:

```sh
sensorkit kv load otto.yaml
```

### Dropbox Setup

Otto uploads FITS files to Dropbox using an app with a long-lived refresh token.

1. Create an app at [Dropbox App Console](https://www.dropbox.com/developers/apps)
2. Set Permissions: `files.content.write`
3. In Settings, copy `App key` and `App secret`
3. Generate a copy the refresh token using the OAuth2 flow
4. Set environment variables and configure the `dropbox` block in your otto config

### UDL

UDL publishing is not yet implemented in otto.

## Usage

```sh
sensorkit service run -s sensorkit.otto.service -n otto
```
