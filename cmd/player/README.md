# ReplayForge Player

ReplayForge Player is a versatile Go application that acts as a relay player and database manager for the ReplayForge system. It can operate in two modes: as a relay to forward requests to a target host, and as a database manager to store and retrieve data.

## Features

- Relay incoming requests to a target host
- Store and manage data in a SQLite database
- HTTP server for database queries
- Basic authentication for HTTP endpoints
- Support for static asset serving
- Configurable via command-line flags or JSON configuration file

## Configuration

The application uses a JSON configuration file to set up its behavior. By default, it looks for a file named `config.json` in the same directory as the executable. You can specify a different configuration file path using the `-c` flag when running the application.
### Sample Configuration

```json
{
  "relayUrl": "https://your-relay-server.com/",
  "buckets": [
    {
      "name": "bucket1",
      "relayAuthenticationBearer": "your-auth-token",
      "sinkType": "http",
      "id": "unique-id-1",
      "params": {
        "targetHost": "http://localhost:8080"
      }
    },
    {
      "name": "bucket2",
      "relayAuthenticationBearer": "another-auth-token",
      "sinkType": "db",
      "id": "unique-id-2",
      "params": {
        "database": "path/to/your/database.sqlite",
        "listen_port": "8081",
        "listen_host": "localhost",
        "static_dir": "path/to/static/files"
      }
    }
  ],
  "tsnet": {
    "hostname": "your-tailscale-hostname"
  }
}
```

### Configuration Options

- `relayUrl`: The URL of the relay server.
- `buckets`: An array of bucket configurations.
  - `name`: The name of the bucket.
  - `relayAuthenticationBearer`: The authentication token for the relay server.
  - `sinkType`: The type of sink to use (`"http"`, `"db"`, or `"olapdb"`).
  - `id`: A unique identifier for the sink.
  - `params`: Sink-specific parameters.
    - For HTTP sink:
      - `targetHost`: The target host to forward requests to.
    - For DB sink:
      - `database`: Path to the SQLite database file.
      - `listen_port`: Port to listen on for database queries.
      - `listen_host`: Host to listen on (default: "localhost").
      - `static_dir`: Directory for static files.
- `tsnet` (optional): Tailscale network configuration.
  - `hostname`: The Tailscale hostname for this node.

Ensure that your configuration file is properly set up before running the application.

## Usage

Run the application with the desired configuration:

```
./replayforge-player -c config.json
```

Or with command-line flags:

```
./replayforge-player -r https://relay.example.com/ -a your-bearer-token -b your-bucket -t https://target.example.com/ -s your-server-secret
```

## Database Mode

When running in database mode, the application serves an HTTP endpoint for querying the SQLite database. You can make GET requests to retrieve data from specific tables.

Example:
```
GET http://localhost:8083/your_table?column1=value1&order=column2&limit=10
```

## Security

The HTTP server uses basic authentication. Make sure to set `httpUsername` and `httpPassword` in your configuration file.
