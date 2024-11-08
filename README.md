# ReplayForge

[doc](https://chamot1111.github.io/replayforge/)

Sometimes you want to extract data from a server in a simple way, even if this simplicity means accepting some data loss. ReplayForge's core concept is to have a proxy program on the server that captures HTTP requests. These requests are then asynchronously relayed to another machine running a "player" program that can replay these same requests.

To achieve this, a relay is installed either on an internet-exposed server via HTTP, or on any machine that doesn't need internet exposure but is accessible through a private Tailscale network.

To ensure maximum system resilience, the proxy has a buffer that allows it to wait if the relay server is unavailable. To avoid unpleasant surprises, both the proxy buffers and relay databases have size limits to prevent accidental disk filling, transfers between proxy and relay are rate-limited, and ReplayForge logs are filtered to emit only small quantities of data.

Additionally, since proxies can be deployed across numerous systems, the relay server provides a locally accessible endpoint that displays statistics for all connected systems. This makes it easy to understand what's happening across all machines and the interactions between them.

While this is the basic idea, ReplayForge goes further. The proxy has been expanded to support additional data sources like files, commands, and PostgreSQL functions. The player has also been enhanced with additional output options like files, Discord notifications, and SQLite databases.

Furthermore, Lua scripts provide complete flexibility for filtering and transforming information as needed.

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

## License

This project is licensed under the [Apache License 2.0](LICENSE).
