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
        print("Stream:")
        for key, value in pairs(stream.stream) do
            print(key, value)
        end

        for _, value in ipairs(stream.values) do
            print("Timestamp:", value.timestamp)
            print("Content:", value.content)
            event = {
                body ={
                    timestamp = value.timestamp,
                    content = value.content
                }
            }
            for key, value in pairs(parsed_content) do
                if key ~= "body" then
                    event[key] = value
                end
            end

            emit("dbg_sink", json_encode(event))
        end
    end
end
