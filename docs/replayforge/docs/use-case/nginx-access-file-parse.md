---
sidebar_position: 3
---
# Nginx Access File Parse

proxy.json

``` json
{
  "sources": [
    {
      "id": "nginx_access_log",
      "type": "logfile",
      "params": {
        "filePath": "./nginx/access.log",
        "removeAfterSecs": 3600,
        "fingerprintLines": 1,
        "httpPath": "/nginx_access_log"
      },
      "transformScript": "./nginx.lua"
    }
  ],
  "sinks": [
    {
      "id": "nginx_log_sink",
      "type": "http",
      "url": "http://relay-forwarder:8100/",
      "authBearer": "$RELAY_SECRET",
      "buckets": ["nginx_log_${ENV_TYPE}"]
    }
  ]
}

```

nginx.lua

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
