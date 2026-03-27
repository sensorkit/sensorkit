# Node Platform Module

The Node Platform module provides scripting and device integration for the OurSky Node Platform.

## Features
- Device control via Node Platform API calls
- Service support

## Example Config
```yaml
entity: node_platform
key: NodePlatformConfig
value:
  endpoints:
    - host: nodeplatformhost
      port: 8000
      devices:
        - entity: mount1
          device_type: mount
```

## Usage
- Add the config above to your main configuration YAML.
- Use the CLI to start the service:
  ```sh
  sensorkit service run -s sensorkit.node_platform.service -n node_platform
  ```

