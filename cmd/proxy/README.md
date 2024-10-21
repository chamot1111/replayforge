# ReplayForge Proxy

ReplayForge Proxy is a versatile HTTP proxy server written in Go, designed to capture, process, and relay HTTP requests. It supports custom JavaScript processing, database storage, and flexible configuration options.

## Features

- Capture and store HTTP requests in SQLite database
- Custom lua processing of captured requests
- Periodic relay of captured data to a remote server
- Configurable via JSON configuration file
- Support for multiple buckets
- JWT-based authentication for relay communication
- Automatic database vacuuming to manage size
- Optional Tailscale network integration for secure communication

## Usage

To run the proxy, use the following command:

```bash
./replayforge-proxy -c config.json
```

Where `config.json` is your configuration file. The configuration file should be in JSON format and include the following structure:

```json
{
  "sources": [
    {
      "id": "http_source_1",
      "type": "http",
      "params": {
        "listenPort": "8080"
      },
      "transformScript": "scripts/http_transform.lua",
      "targetSink": "sink1",
      "hookInterval": 5
    },
    {
      "id": "logfile_source_1",
      "type": "logfile",
      "params": {
        "filePath": "/var/log/app.log",
        "removeAfterSecs": 3600,
        "rotateWaitSecs": 300,
        "fingerprintLines": 10
      },
      "transformScript": "scripts/logfile_transform.lua",
      "hookInterval": 10
    }
  ],
  "sinks": [
    {
      "id": "sink1",
      "type": "http",
      "url": "https://api.example.com/",
      "authBearer": "your-auth-token",
      "buckets": ["bucket1", "bucket2"],
      "useTsnet": false
    }
  ],
  "tsnetHostname": "your-tailscale-hostname"
}
```

Adjust the configuration file according to your specific sources, sinks, and transformation requirements.
```

This example provides a basic structure of the config file and explains how to run the proxy with the configuration file. You can expand on this by adding more details about each configuration option, such as explaining what each field in the sources and sinks represents.
