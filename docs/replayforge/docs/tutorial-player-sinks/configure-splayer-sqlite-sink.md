---
sidebar_position: 1
---
# SQLite Sink Plugin

The SQLite Sink plugin provides a way to store and retrieve data in a SQLite database through HTTP endpoints. It supports basic CRUD operations and querying capabilities.

## Configuration

To use the SQLite Sink plugin, you need to configure it with the following parameters:

```json
{
  "database": "/path/to/your/database.db",
  "listen_port": "8080",
  "listen_host": "localhost",
  "static_dir": "/path/to/static/files"
}
```

Complete example

```json
{
  "relayUrl": "http://localhost:8100/",
  "envName": "test",
  "listenUsingTailscale": false,
  "listenAddress": "",
  "exposedPort": 8200,
  "sources": [
    {
      "name": "the_source",
      "relayAuthenticationBearer": "my-secret",
      "sinks": ["the-sink"]
    }
  ],
  "sinks": [
    {
      "name": "the-sink",
      "type": "db",
      "id": "the-sink",
      "params": {
        "database": "./the-sink.sqlite3",
        "listen_port": "8300",
        "listen_host": "localhost",
        "static_dir": "./the-sink-assets"
      }
    }
  ]
}
```

- `database`: Path to the SQLite database file
- `listen_port`: Port number for the HTTP server
- `listen_host`: Host address for the HTTP server (defaults to "localhost")
- `static_dir`: Directory for serving static files

## Input event format

- Tables and columns are created automatically based on the data being inserted
- All operations require an `id` field (can be int or string)

### Create/Update Record (POST)
```http
POST /rpf-db/{table-name}
Content-Type: application/json

{
  "id": "unique-id",
  "field1": "value1",
  "field2": 123
}
```
- The `id` field is mandatory
- Table and columns are created automatically based on the data

### Update Record (PUT)
```http
PUT /rpf-db/{table-name}
Content-Type: application/json

{
  "id": "existing-id",
  "field1": "new-value"
}
```
- Updates existing record
- `id` is required to identify the record

### Delete Record (DELETE)
```http
DELETE /rpf-db/{table-name}
Content-Type: application/json

{
  "id": "record-id-to-delete"
}
```

## API Endpoints

### Query Records (GET)
```http
GET /{plugin-id}/rpf-db/{table-name}?field1=value1&order=field1&limit=10
```

Query Parameters:
- Any field can be used as a filter: `field1=value1`
- `order`: Sort results by specified field
- `limit`: Limit number of returned records

## Data Types

The plugin automatically determines and creates columns with appropriate SQLite data types:
- `INTEGER`: For integer values
- `REAL`: For floating-point numbers
- `BOOLEAN`: For boolean values
- `TEXT`: For strings and other types (default)

## Example Usage

4. Delete a record:
```bash
curl -X DELETE "http://localhost:8080/my-plugin/rpf-db/users" \
  -H "Content-Type: application/json" \
  -d '{"id": "user1"}'
```

## Notes

- Static files can be served from the configured static directory at `/{plugin-id}/static/`
