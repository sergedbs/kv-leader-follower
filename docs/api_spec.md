# API Specification

## Common Endpoints (Leader & Followers)

- `GET /get?key=<key>` → `{ "status": "ok", "value": "<value>" }` or `{ "status": "error", "error": "Key not found" }` (404)
- `GET /dump` → `{ "status": "ok", "store": { "key1": "value1", ... } }` (test helper)
- `GET /health` → `{ "status": "healthy", "role": "leader|follower" }` (200)

## Leader-Only Endpoints

- `POST /set`
  - Request: `{ "key": "mykey", "value": "myvalue" }`
  - Success (200): `{ "status": "ok", "acks": 3, "latency_ms": 12.5, "replication": [{"follower": "follower1", "status": "ok", "latency_ms": 2.1}, ...] }`
  - Failure (500): `{ "status": "error", "error": "Quorum not reached", "acks": 2, "required": 3, "replication": [...] }`

## Follower-Only Endpoints

- `POST /replicate`
  - Request: `{ "key": "mykey", "value": "myvalue" }`
  - Success: `{ "status": "ok" }` (200)
  - Error: `{ "status": "error", "error": "Invalid request" }` (400)

## Error Codes

- 200: Success
- 400: Bad request (invalid JSON, missing fields)
- 404: Key not found (GET only)
- 500: Internal error or quorum failure
