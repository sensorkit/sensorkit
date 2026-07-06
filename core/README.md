# sensorkit-core

The core package provides the main framework, APIs, device models, and backend logic for Sensorkit.

## Key Components
- `api/` — Command models, entrypoints, and utilities
- `astro/` — Astrological calculations and utilities
- `auto/` — Automated device management and scheduling
- `backend/` — Backend communication (e.g., NATS)
- `cli/` — Command-line interface for managing services and data
- `common/` — Shared models and helpers
- `core/` — Device base classes, controllers, and program logic
- `data/` — Data handling, file systems, and streaming
- `std/` — Standard sensors and collection routines

## Example: Running with NATS Backend

To run a service using NATS as the backend:

1. Start a NATS server (see https://docs.nats.io/running-a-nats-service):
   ```sh
   nats-server -js
   ```
2. Configure your service (see module README for config example).
3. Activate your virtual environment if necessary:
   - Linux/macOS:
     ```sh
     source .venv/bin/activate
     ```
   - Windows:
     ```powershell
     .venv\Scripts\Activate.ps1
     ```
4. Run the service using the CLI:
   ```sh
   sensorkit service run pwi_service sensorkit.pwi4.service
   ```

## CLI Commands

- `sensorkit service run` — Start one or more services
- `sensorkit service ls` — List services registered in the backend
- `sensorkit kv ls` — List key-value pairs in the backend
- `sensorkit kv get` — Get a value by key
- `sensorkit kv set` — Set a value by key
