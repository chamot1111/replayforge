---
sidebar_position: 1
---
# Transform Scripts

## Script structure

A transform script is a Lua script that is executed on the player and proxy. It can be used to transform the content of the content before they are sent to the sink.
The lua script path is defined in the source configuration by the property `transformScript`.

``` lua
function Process(content, emit)
    emit('the-sink', content)
end
```

On player and proxy TimerHandler is called every `hookInterval` duration (define on the source).

The `hookInterval` in source configuration supports following duration format: `300ms`, `-1.5h` or `2h45m`
Valid time units are `ns`, `us` (or `µs`), `ms`, `s`, `m`, `h`.

``` lua
function TimerHandler(emit)
    emit('the-sink', json_encode({
        body = 'Hello, world!',
        method = 'POST',
    }))
end
```

## unwrap and wrap content

``` lua

function Process(content, emit)
    local data = json_decode(content)
    local body = data['body']

    -- body can be plain text or json
    -- if it's json, we can access its fields using json_decode(body)

    local ip = data['ip']
    local path = data['path']
    local params = data['params']
    local headers = data['headers']

    local wrapped = json_encode({
        body = body,
        ip = ip,
        path = path,
        params = params,
        headers = headers,
        method = r.Method,
    })

    emit('the-sink', wrapped)
end
```

## alert on no message

``` lua
local last_message_time = os.time()
local alert_sent = false

function TimerHandler(emit)
    local current_time = os.time()
    if current_time - last_message_time > 300 and not alert_sent then -- 5 minutes = 300 seconds
        emit('notifications', json_encode({
            method = 'POST',
            body = json_encode({
                message = 'No logs received in the last 5 minutes',
            })
        }))
        alert_sent = true
    end
end

function Process(content, emit)
    last_message_time = os.time()
    if alert_sent then
        emit('notifications', json_encode({
            body = json_encode({
                message = 'Service restored - logs are being received again',
            })
        }))
        alert_sent = false
    end
    emit('the-sink', content)
end
```

## grok parser

``` lua
env_name = os.getenv("ENV_NAME")
count = 0

function Process(content, emit)
    contentMap = json_decode(content)
    bodyStr = contentMap["body"]

    body = json_decode(bodyStr)
    content = body["content"]

    gr = "%{IPORHOST:client_ip} %{USER:ident} %{USER:auth} \\[%{HTTPDATE:timestamp}\\] \"%{WORD:http_method} %{NOTSPACE:request_path} HTTP/%{NUMBER:http_version}\" %{NUMBER:response_code:int} %{NUMBER:bytes:int} \"%{DATA:referer}\" \"%{DATA:user_agent}\""
    values = grok_parse(gr, content)

    count = count + 1

    if values and (count % 100 == 0 or (values.response_code and tonumber(values.response_code) > 399)) then
        if count % 100 == 0 then
            count = 0
        end
        values.env_name = env_name
        contentMap["body"] = json_encode(values)
        emit("nginx_log_sink", json_encode(contentMap))
    end
end
```
