# TheSky Module

The TheSky module provides scripting and device integration for TheSky platform.

## Features
- Device control via TheSky scripting
- Simulator and service support

## Example Config
```yaml
entity: thesky
key: TheSkyConfig
value:
  endpoints:
    - host: theskyhost
      port: 3040
      devices:
        - entity: mount1
          device_type: mount
```

## Usage
- Add the config above to your main configuration YAML.
- Use the CLI to start the service:
  ```sh
  sensorkit service run -s sensorkit.thesky.service -n thesky
  ```
