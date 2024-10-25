# ReplayForge

ReplayForge is an innovative fault-tolerant queuing system that acts as a distributed database for data flows. Important note: it should only be used for data that can tolerate loss, such as logs. Its unique architecture allows it to gracefully handle data loss and continue operating even when any of its three core components (proxy, relay, or player) experience interruptions or failures. The system ingests data from multiple sources including HTTP requests and log files, while providing output to various destinations such as HTTP clients, SQLite databases, and log files.

## Components

### Proxy Server

The proxy server intercepts HTTP requests, stores them in a local SQLite database, and forwards them to a relay server. It supports custom JavaScript processing of requests before forwarding.

Key features:
- Intercepts and stores HTTP requests
- Supports custom lua processing
- Forwards processed requests to a relay server
- Periodic database cleanup and maintenance
- Support Tailscale network

### Relay Server

The relay server acts as an intermediary, receiving requests from the proxy server and storing them for later retrieval by the player server.

Key features:
- Receives and stores requests from multiple proxy servers
- Supports multiple buckets for request segregation
- Provides authentication for secure communication

### Player Server

The player server retrieves requests from the relay server and can either store them in a local database or forward them to a target host.

Key features:
- Retrieves requests from the relay server
- Can operate in database mode or forward requests to a target host
- Provides a simple HTTP API for querying stored requests
- Supports custom lua processing
- Supports basic authentication for secure access

## Setup and Configuration

Configure each component using configuration files. Refer to the usage section of each component for details.

## Usage

### Proxy Server

```
./proxy -c config.json
```

### Relay Server

```
./relay -c config.json
```

### Player Server

```
./player -c config.json
```

Refer to the comments in each component's source code for detailed configuration options.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the [Apache License 2.0](LICENSE).
