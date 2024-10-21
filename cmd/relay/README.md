# ReplayForge Relay

ReplayForge Relay is a Go-based service that acts as a temporary storage and relay system for data. It allows clients to record data to specific buckets, retrieve the first record from a bucket, and acknowledge (delete) records once processed.

## Features

- Record data to multiple buckets
- Retrieve the first record from a specific bucket
- Acknowledge and delete processed records
- Bucket-based authentication
- Configurable database size limit
- Time-to-live (TTL) for records
- SQLite database backend with Write-Ahead Logging (WAL) for improved performance

## Configuration

The relay server uses a JSON configuration file to define its behavior. You can specify the path to this file using the `-c` flag when starting the server.

### Example Configuration

```json
{
  "port": 8081,
  "useTailnet": false,
  "buckets": {
    "default": {
      "auth": "your-secret-token",
      "dbMaxSizeKb": 10240,
      "tls": 86400
    },
    "another-bucket": {
      "auth": "another-secret-token",
      "dbMaxSizeKb": 5120,
      "tls": 3600
    }
  }
}
```

### Configuration Options

- `port`: The port number the server will listen on. If not specified, it defaults to 8081.
- `useTailnet`: Set to `true` to use Tailscale networking, `false` for standard networking.
- `buckets`: A map of bucket configurations. Each bucket has the following properties:
  - `auth`: The authentication token for this bucket.
  - `dbMaxSizeKb`: The maximum size of the SQLite database for this bucket in kilobytes.
  - `tls`: Time-to-live in seconds for records in this bucket. Set to 0 for no expiration.


## Usage

1. Start the server:
   ```
   go run main.go -c config.json
   ```

2. Use the following endpoints:

   - POST `/record`: Record data to one or more buckets
   - GET `/first`: Retrieve the first record from a specific bucket
   - DELETE `/acknowledge`: Acknowledge and delete a processed record

## API

### Record Data

```
POST /record
Headers:
  - Authorization: Bearer <auth_token>
  - RF-BUCKETS: bucket1,bucket2
Body: Raw data to be recorded
```

### Retrieve First Record

```
GET /first
Headers:
  - Authorization: Bearer <auth_token>
  - RF-BUCKET: <bucket_name>
```

### Acknowledge Record

```
DELETE /acknowledge?id=<record_id>
Headers:
  - Authorization: Bearer <auth_token>
  - RF-BUCKET: <bucket_name>
```
