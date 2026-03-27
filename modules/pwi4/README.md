# PWI4 Module

The PWI4 module provides integration with PlaneWave Instruments PWI4 mounts and accessories.

## Features
- Mount control (slew, park, home, offsets)
- Focuser, rotator, and mirror cover support
- Custom path tracking and model management

## Example Config
```yaml
entity: pwi_service
key: Config
value:
  endpoints:
    - host: pwihost-1
      port: 8220
      devices:
        - entity: pwi4mount-1
          device_type: mount
    - host: pwihost-2
      port: 8220
      devices:
        - entity: pwi4mount-2
          device_type: mount
```

## Usage
- Add the config above to your main configuration YAML.
- Use the CLI to start the service:
  ```sh
  sensorkit service run -s sensorkit.pwi4.service -n pwi_service
  ```