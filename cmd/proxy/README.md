# ReplayForge Proxy

ReplayForge Proxy is a versatile HTTP proxy server written in Go, designed to capture, process, and relay HTTP requests. It supports custom JavaScript processing, database storage, and flexible configuration options.

## Features

- Capture and store HTTP requests in SQLite database
- Custom JavaScript processing of captured requests
- Periodic relay of captured data to a remote server
- Configurable via command-line flags or JSON configuration file
- Support for multiple buckets
- JWT-based authentication for relay communication
- Automatic database vacuuming to manage size

## Prerequisites

- Go 1.15 or higher
- SQLite3

## Configuration

The service can be configured using command-line flags or a JSON configuration file.

### Command-line Flags

- `-c`: Path to config file
- `-r`: Relay URL
- `-s`: Server secret
- `-a`: Relay authentication bearer token
- `-b`: Bucket name (can be specified multiple times)
- `-p`: Listen port
- `-script`: Path to JavaScript script
- `-hook-interval`: Interval in seconds for timer_handler (default: 60)

### Configuration File

Alternatively, you can use a JSON configuration file. Example:

```json
{
  "relayUrl": "https://relay.example.com/",
  "serverSecret": "your-secret-here",
  "relayAuthenticationBearer": "your-bearer-token",
  "buckets": ["bucket1", "bucket2"],
  "scriptPath": "/path/to/your/script.js",
  "hookIntervalSeconds": 30,
  "port": 8080
}
```

## JavaScript Processing

If a script path is provided, the proxy will execute the specified JavaScript file. The script can define two main functions:

1. `process(event, emit)`: Called for each captured request
2. `timer_handler(emit)`: Called periodically based on the hook interval

Use the `emit` function to send processed data to the relay server.
