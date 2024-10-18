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

The service can be configured using command-line flags or a JSON configuration file.

### Command-line Flags

- `-c`: Path to the config file
- `-m`: Maximum database size in KB (default: 100000)
- `-p`: Port to listen on (default: 8081)
- `-t`: Time to live in seconds for records (0 for no delay)
- `-b`: Bucket authentication in the format 'bucket1:auth1,bucket2:auth2'

### Configuration File

Create a JSON file with the following structure:

```json
{
  "bucketAuth": {
    "bucket1": "auth1",
    "bucket2": "auth2"
  },
  "dbMaxSizeKb": 100000,
  "port": 8081,
  "tlS": 0
}
```

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
