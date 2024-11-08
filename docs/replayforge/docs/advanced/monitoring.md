---
sidebar_position: 1
---
# Monitoring

## Status Monitoring (statusz) Documentation for relay

The relay server provides a status monitoring endpoint that exposes various metrics and information about the server's operation.

## Configuration

The status server is configured using the `portStatusZ` field in the configuration file:

```json
{
  "portStatusZ": 8082,  // Port where status endpoint will be available
  // ... other configuration
}
```

## Status Endpoint

### Endpoint Details
- **URL**: `/statusz`
- **Method**: GET
- **Port**: Configured via `portStatusZ` in config file

### Response Format

The endpoint returns a JSON object with the following structure:

```json
{
  "uptime": "1h2m3s",
  "buckets": {
    "bucket-name": {
      "kind": "relay",
      "id": "bucket-name",
      "rxMessagesByMinute": 100,
      "lastMinuteRxMessages": 95,
      "rxMessagesSinceStart": 1000,
      "rxLastMessageDate": "2023-01-01T12:00:00Z",
      "rxQueryByMinute": 10,
      "txMessageByMinute": 50,
      "txQueryByMinute": 5,
      "txLastAccess": "2023-01-01T12:00:00Z"
    }
  },
  "envs": {
    "env-name": {
      // Same structure as buckets
    }
  },
  "hostnames": {
    "host-name": {
      // Same structure as buckets
    }
  },
  "nodeInfo": {
    "hostname": {
      "memoryProcess": 100.5,
      "memoryHostTotal": 8192.0,
      "memoryHostFree": 4096.0,
      "memoryHostUsedPct": 50.0,
      "cpuPercentHost": 25.0,
      "lastUpdated": "2023-01-01T12:00:00Z",
      "warnCount": 0,
      "errorCount": 0
    }
  }
}
```

### Metrics Description

#### General Metrics
- `uptime`: Server uptime since start

#### Per-Bucket/Env/Hostname Statistics
- `rxMessagesByMinute`: Number of messages received in the current minute
- `lastMinuteRxMessages`: Number of messages received in the previous minute
- `rxMessagesSinceStart`: Total messages received since server start
- `rxLastMessageDate`: Timestamp of last received message
- `rxQueryByMinute`: Number of receive queries in the current minute
- `txMessageByMinute`: Number of messages transmitted in the current minute
- `txQueryByMinute`: Number of transmit queries in the current minute
- `txLastAccess`: Last transmit access timestamp

#### Node Information
- `memoryProcess`: Process memory usage (Bytes)
- `memoryHostTotal`: Total host memory (Bytes)
- `memoryHostFree`: Free host memory (Bytes)
- `memoryHostUsedPct`: Host memory usage percentage
- `cpuPercentHost`: Host CPU usage percentage
- `lastUpdated`: Last time node info was updated
- `warnCount`: Number of warnings
- `errorCount`: Number of errors

### Stats Reset
The per-minute statistics (metrics ending with "ByMinute") are automatically reset every minute, with the previous minute's values stored in corresponding "last minute" fields.

## Usage Example

To fetch the status information:

```bash
curl http://your-server:status-port/statusz
```

This endpoint is useful for:
- Monitoring server health
- Tracking message throughput
- Debugging performance issues
- System resource monitoring
- Node status tracking across a distributed system
