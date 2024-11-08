---
sidebar_position: 1
---
# Uptime Kuma Monitor Postgres Values

Kuma Monitor is a nice health monitoring and status page generator, the following examples show how to use postgres source to call status into sqlite sink let kuma display in dashboards.

## Basic Kuma Monitor Source -> Sink

### pg call source example

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
      "authBearer": "$RELAY_SECRET"
    }
  ],
  "portStatusZ": 8000,
  "envName": "monitoring"
}
```

### sqlite sink example

player.json:

``` json
{
  "relayUrl": "http://relay-forwarder:8100/",
  "sources": [
    {
      "name": "monitoring",
      "relayAuthenticationBearer": "$RELAY_SECRET",
      "transformScript": "./convert-monitoring.lua"
    }
  ],
  "sinks": [
    {
      "name": "status",
      "type": "db",
      "id": "status",
      "params": {
        "database": "./status.sqlite3",
        "listen_port": "8300",
        "static_dir": "./static"
      }
    }
  ]
}
```

convert-monitoring.lua

``` lua
function Process(content, emit)
    local data = json_decode(content)
    local body = data['body']

    -- Parse the body string into a table
    local body_data = json_decode(body)

    -- Iterate through each key in the body
    for key, value in pairs(body_data) do
        -- Create new body format for each key
        local new_body = {
            id = key,
            success = value.success,
            data = json_encode(value.data),  -- Convert data array back to string
            update_time = value.update_time
        }

        -- Create the wrapped object
        local wrapped = json_encode({
            body = json_encode(new_body),
            path = "/status",
            method = "POST"
        })

        -- Emit an event for this key
        emit('status', wrapped)
    end
end
```
