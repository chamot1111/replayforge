curl -X POST http://localhost:8080 \
  -H "Content-Type: application/json" \
  -d '{
    "key1": "value1",
    "key2": "value2",
    "key3": {
      "nestedKey": "nestedValue"
    }
  }'

# db mode

curl -X POST http://localhost:8080/items \
  -H "Content-Type: application/json" \
  -d '{
    "id": 1,
    "key1": "value1",
    "key2": 1
  }'

curl -X POST http://localhost:8080/items \
  -H "Content-Type: application/json" \
  -d '{
    "id": 2,
    "key1": "value1",
    "key2": 1,
    "key4": "new"
  }'

curl -X PUT http://localhost:8080/items \
  -H "Content-Type: application/json" \
  -d '{
    "id": 2,
    "key1": "value1",
    "key2": 1,
    "key4": "new 2"
  }'

curl -X DELETE http://localhost:8080/items \
  -H "Content-Type: application/json" \
  -d '{
    "id": 2
  }'
