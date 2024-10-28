function Process(content, emit)
    gr = "%{IPORHOST:client_ip} %{USER:ident} %{USER:auth} \\[%{HTTPDATE:timestamp}\\] \"%{WORD:http_method} %{NOTSPACE:request_path} HTTP/%{NUMBER:http_version}\" %{NUMBER:response_code:int} %{NUMBER:bytes:int} \"%{DATA:referer}\" \"%{DATA:user_agent}\""
    values = grok_parse(gr, content)
    for key, value in pairs(values) do
        print(string.format("%s = %s", key, value))
    end
    if tonumber(values.response_code) > 399 then
        emit("dbg_sink", json_encode({
            body = json_encode(values)
        }))
    end
end
