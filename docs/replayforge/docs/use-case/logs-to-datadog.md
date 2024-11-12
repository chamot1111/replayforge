---
sidebar_position: 4
---
# Send Logs to Datadog

The player can be configured to read logs from Pebble using the loki protocol, filter specific data, and forward it to Datadog. Here's how you can accomplish this using a custom transform script with the sink handler.

```yaml title="pebble.yaml"
log-targets:
  production-logs:
    override: merge
    type: loki
    location: http://localhost:9011/loki/api/v1/push
    services: [srv1, srv2]

```

```json title="player.json"
{
  "tsnetHostname": "api-rpf-${ENV_NAME}",
  "sources": [
    {
      "id": "pebble_loki",
      "type": "http",
      "params": {
        "listenPort": 9011
      },
      "transformScript": "./loki.lua",
      "hookInterval": 1000
    }
  ],
  "sinks": [
    {
      "id": "pebble_loki_sink",
      "type": "http",
      "url": "https://http-intake.logs.datadoghq.com/",
      "buckets": ["pebble_loki_${ENV_TYPE}"],
      "transformScript": "./log_to_data_dog.lua",
      "config": {
        "maxPayloadBytes": 5000000,
	      "batchMaxEvents": 1000,
				"batchTimeoutSecs": 5,
				"batchGoalBytes": 4250000
      }
    }
  ]
}
```

```lua title="loki.lua"
env_name = os.getenv("ENV_NAME")

function parse_loki_event(content)
    local parsed_content = json_decode(content)
    local body_str = parsed_content["body"]
    local streams = json_decode(body_str)["streams"]

    local parsed_streams = {}
    for _, stream in ipairs(streams) do
        local stream_info = stream["stream"]
        local values = stream["values"]

        local parsed_values = {}
        for _, value_tuple in pairs(values) do
            table.insert(parsed_values, {
                timestamp = value_tuple[1],
                content = value_tuple[2]
            })
        end

        table.insert(parsed_streams, {
            stream = stream_info,
            values = parsed_values
        })
    end

    return parsed_streams, parsed_content
end

function Process(content, emit)
    local parsed_streams, parsed_content = parse_loki_event(content)

    for _, stream in ipairs(parsed_streams) do
        service = stream.stream["pebble_service"]

        for _, value in ipairs(stream.values) do
            event = {
                body = json_encode({
                    timestamp = value.timestamp,
                    content = value.content,
                    service = service,
                    env_name = env_name
                })
            }
            for key, value in pairs(parsed_content) do
                if key ~= "body" then
                    event[key] = value
                end
            end

            emit("pebble_loki_sink", json_encode(event))
        end
    end
end

```lua title="log_to_data_dog.lua"
service_name = os.getenv("DD_SERVICE")
dd_api_key = os.getenv("DD_API_KEY")

function TransformBatch(messages, request)
    local dd_messages = {}
    for i, message in ipairs(messages) do
        local decoded = json_decode(message)
        local body = json_decode(decoded.body)

        local dd_message = {
            ddsource = body.stream,
            ddtags = "",
            hostname = "replayforge",
            message = body.content,
            service = service_name
        }
        table.insert(dd_messages, json_encode(dd_message))
    end

    request.path = "/api/v2/logs"
    request.headers = {
        ["Content-Type"] = "application/json",
        ["DD-API-KEY"] = dd_api_key
    }

    return dd_messages, request
end
```
