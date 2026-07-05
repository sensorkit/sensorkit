# TheSky Module

The TheSky module provides integration for the Software Bisque TheSky application.

## Supported Devices
- Camera
- Dome
- FilterWheel
- Focuser
- Mount
- OTA (mirror cover)
- Rotator
- Weather

## Example Config
```yaml
entity: thesky_service
key: TheSkyConfig
value:
  endpoints:
    - host: localhost
      port: 3040
      devices:
        weather1:
          device_type: weather
          status_frequency: 5
          timeout: 60
        dome1:
          device_type: dome
          status_frequency: 5
          timeout: 60
        mirror_cover1:
          device_type: ota
          status_frequency: 5
          timeout: 60
        mount1:
          device_type: mount
          status_frequency: 5
          timeout: 60
        filter_wheel1:
          device_type: filter_wheel
          filters:
            Filter 1: 0
            Filter 2: 1
            Filter 3: 2
            Filter 4: 3
            Filter 5: 4
            Filter 6: 5
            Filter 7: 6
          status_frequency: 5
          timeout: 30
        camera1:
          device_type: camera
          temperature: -10
          status_frequency: 1
          timeout: 60                 # really, the filter wheel move timeout
```

## Usage
- Add the config above to your main configuration YAML.
- Use the CLI to start the service:
  ```sh
  sensorkit service run -s sensorkit.thesky.service -n thesky
  ```
