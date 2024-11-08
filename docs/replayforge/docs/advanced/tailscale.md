---
sidebar_position: 1
---
# Use Tailscale

Tailscale offers a secure and convenient way to establish private networks between your applications and services. It provides encrypted point-to-point connections without requiring complex network configurations. When working with Kuma Monitor sources and sinks in distributed environments, Tailscale simplifies networking by creating a mesh VPN that just works, handling NAT traversal, encryption, and access controls automatically. This makes it easier to monitor applications across different networks while maintaining security.
:::note
When using Replayforge, Tailscale's tsnet is embedded directly in the application, eliminating the need to install the Tailscale client separately. This makes deployment simpler while maintaining all the benefits of secure Tailscale networking.
:::

## Proxy Configuration

proxy.json:

``` json
{
  "sources": [
    {
      "id": "appli1",
      "type": "pgcall",
      "params": {
        "intervalSeconds": 60,
        "host": "$PGM_DB_HOST_APPLI1_API",
        "port": "$PGM_DB_PORT_APPLI1_API",
        "database": "$PGM_DB_DATABASE_APPLI1_API",
        "user": "$PGM_DB_USER_APPLI1_API",
        "password": "$PGM_DB_PASSWORD_APPLI1_API",
        "calls": [
          {
            "name": "appli1_ping",
            "sql": "SELECT monitoring.ping()"
          },
          {
            "name": "appli1_connections",
            "sql": "SELECT monitoring.connections_since_5_minutes()"
          }
        ]
      },
      "targetSink": "monitoring_sink",
      "hookInterval": 60000
    }
  ],
  "sinks": [
    {
      "id": "monitoring_sink",
      "type": "http",
      "url": "http://relay-forwarder:8100/",
      "buckets": ["monitoring"],
      "authBearer": "secret",
      "useTsnet": true
    }
  ],
  "portStatusZ": 8000,
  "tsnetHostname": "monitoring-proxy",
  "envName": "monitoring"
}
```

## Relay Configuration



## Player Configuration
