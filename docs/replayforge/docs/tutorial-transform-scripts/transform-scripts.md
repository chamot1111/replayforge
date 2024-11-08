---
sidebar_position: 1
---
# Transform Scripts

## simple example

``` lua
function Process(content, emit)
    emit('the-sink', content)
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
