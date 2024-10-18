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

You can configure the application using command-line flags or a JSON configuration file. To use a configuration file, use the `-c` flag followed by the path to your JSON file.

### Command-line Flags

- `-c`: Path to config file
- `-r`: Relay URL
- `-a`: Relay authentication bearer
- `-b`: Bucket
- `-t`: Target host
- `-s`: Server secret
- `-db`: Enable DB mode (boolean)
- `-port`: HTTP server port (default: 8083)

### Configuration File Example

```json
{
  "relayUrl": "https://relay.example.com/",
  "relayAuthenticationBearer": "your-bearer-token",
  "bucket": "your-bucket-name",
  "targetHost": "https://target.example.com/",
  "serverSecret": "your-server-secret",
  "dbMode": true,
  "httpPort": 8083,
  "httpUsername": "admin",
  "httpPassword": "password",
  "staticAssetsFolder": "./assets"
}
```

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
