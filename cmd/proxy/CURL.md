curl -X POST http://localhost:8080 \
  -H "Content-Type: application/json" \
  -d '{
    "key1": "value1",
    "key2": "value2",
    "key3": {
      "nestedKey": "nestedValue"
    }
  }'
