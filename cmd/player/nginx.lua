function Process(content, emit)
    emit('api-logs-prod', content)
    emit('notifications-prod', json_encode(
            {
                method = 'POST',
                body = json_encode(
                    {
                        message = 'New API log'
                    }
                )
            }
        )
    )
end
