# SensorKit core

The `core` directory provides SensorKit's framework, public API, entity models, backend, and standard observatory controller.
It is packaged with the optional modules as `sensorkit`.

## Key Components

- `api/` - Entity declarations, service entry points, and configuration bootstrap
- `astro/` - Astronomical coordinates, targets, and orbit propagation
- `auto/` - Controller lifecycle supervision, safety constraints, and scheduling
- `backend/` - Messaging, event streams, key-value storage, and entity leases
- `cli/` - Command-line access to services, entities, and configuration
- `common/` - Shared models and helpers
- `config/` - Configuration parsing and JSON Schema generation
- `core/` - Entity clients, implementations, tasks, and event-sourced state
- `data/` - Data processing graphs, FITS encoding, and file storage
- `sensor/` - Device topology, lifecycle sequencing, and observation execution
- `std/` - Standard device commands, telemetry keywords, and observation tasks
- `webapi/` - HTTP access to entities, event streams, and data products

## Running with the NATS backend

To run a service using NATS as the backend:

1. Start a NATS server with JetStream enabled.
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

- `sensorkit service run` - Start one service
- `sensorkit service ls` - List services registered in the backend
- `sensorkit kv ls` - List key-value entries in the backend
- `sensorkit kv get` - Read a value by key
- `sensorkit kv put` - Write a value by key

See the [CLI reference](../docs/cli.md) for arguments and options.
