---
sidebar_position: 1
---
# Use Var envs in configuration files

## JSON Configuration with Environment Variables

**Purpose**: Allows you to write JSON configuration files with environment variable support.

### Basic Usage:
```json
{
  "database": {
    "host": "$DB_HOST",
    "port": "$DB_PORT",
    "password": "$DB_PASSWORD"
  },
  "api": {
    "url": "https://$API_DOMAIN",
    "key": "$API_KEY"
  }
}
```

### Special Features:

**1. Environment Variables**
- Use `$VARIABLE_NAME` to reference environment variables
- Example: `$DB_HOST` will be replaced with actual database host

**2. Escaping Dollar Signs**
- Use `\$` to write literal dollar signs
- Example: `"price": "\$9.99"` stays as `"price": "$9.99"`

### Examples:

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


### Notes:
- Environment variables must be set before parsing
- Missing environment variables will be replaced with empty strings
- Invalid JSON will result in error
- `__ESCAPED_DOLLAR__` is a reserved string, don't use it in your config
