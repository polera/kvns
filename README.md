# kvns

A Redis-compatible in-memory key-value store written in Rust. Speaks the [RESP protocol](https://redis.io/docs/latest/develop/reference/protocol-spec/) so any Redis client works out of the box.

## Features

- RESP protocol — compatible with `redis-cli` and Redis client libraries
- Key namespacing via `namespace/key` syntax
- Configurable memory limit with OOM rejection
- Key expiry via `SET … EX` / `SET … PX`
- Prometheus metrics endpoint with per-namespace labels
- Structured logging via `tracing`

## Key namespacing

Keys may optionally include a namespace prefix separated by `/`:

```
namespace/localkey
```

- `SET db1/x 42` — stores key `x` in namespace `db1`
- `GET db1/x` — retrieves `x` from namespace `db1`
- Keys with no `/` are placed in the `default` namespace

Only the **first** `/` is treated as the separator, so local keys may themselves contain slashes (e.g. `SET ns/a/b value` → namespace `ns`, key `a/b`).

Namespaces are fully isolated: `db1/x` and `db2/x` are independent keys and their memory usage is tracked separately in metrics.

## Supported commands

| Command | Syntax | Notes |
|---------|--------|-------|
| `PING` | `PING` | Returns `PONG` |
| `SET` | `SET key value [EX seconds \| PX milliseconds]` | Overwrites existing key; respects memory limit |
| `GET` | `GET key` | Returns bulk string or nil |
| `DEL` | `DEL key` | Returns count of removed keys |
| `TTL` | `TTL key` | Returns remaining TTL in seconds, `0` if no expiry, nil if missing |
| `TOUCH` | `TOUCH key` | Resets hit counter to 0; returns new count |
| `INCR` | `INCR key` | Atomically increments an integer value; initialises to 0 if missing |
| `LPUSH` | `LPUSH key value [value …]` | Prepends values to a list; creates list if missing |
| `QUIT` | `QUIT` | Closes the connection |

All commands accept namespaced keys: `SET ns/counter 0`, `INCR ns/counter`, `LPUSH ns/queue item`, etc.

## Building and running

```sh
cargo build --release
./target/release/kvns
```

Or for development:

```sh
cargo run
```

## Configuration

All settings are read from environment variables at startup.

| Variable | Default | Description |
|----------|---------|-------------|
| `KVNS_HOST` | `0.0.0.0` | Interface to listen on |
| `KVNS_PORT` | `6480` | RESP listener port |
| `KVNS_MEMORY_LIMIT` | `1073741824` | Max memory in bytes (1 GiB) |
| `KVNS_METRICS_HOST` | `0.0.0.0` | Interface for the metrics endpoint |
| `KVNS_METRICS_PORT` | `9090` | Prometheus metrics port |

Example:

```sh
KVNS_PORT=6379 KVNS_MEMORY_LIMIT=536870912 cargo run
```

## Metrics

kvns exposes a Prometheus scrape endpoint at `http://<KVNS_METRICS_HOST>:<KVNS_METRICS_PORT>/metrics`.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `kvns_keys_total` | Gauge | `namespace` | Current number of live keys per namespace |
| `kvns_memory_used_bytes` | Gauge | `namespace` | Memory currently used per namespace |
| `kvns_memory_used_bytes_total` | Gauge | — | Total memory used across all namespaces |
| `kvns_memory_limit_bytes` | Gauge | — | Configured memory limit |
| `kvns_command_duration_seconds` | Histogram | `command`, `namespace` | Command latency (`set` / `get`) |

Per-namespace gauges are created on first write and set to `0` when the last key in a namespace is removed.

## Quick smoke test

```sh
# Start the server
cargo run &

# Write keys in different namespaces
redis-cli -p 6480 SET db1/x 42
redis-cli -p 6480 SET db2/x 99
redis-cli -p 6480 SET counter 0
redis-cli -p 6480 INCR counter

# Read them back
redis-cli -p 6480 GET db1/x   # → 42
redis-cli -p 6480 GET db2/x   # → 99
redis-cli -p 6480 GET counter # → 1

# Check per-namespace metrics
curl -s http://localhost:9090/metrics | grep kvns_memory
# kvns_memory_used_bytes{namespace="db1"} 6
# kvns_memory_used_bytes{namespace="db2"} 6
# kvns_memory_used_bytes{namespace="default"} 16
# kvns_memory_used_bytes_total 28
# kvns_memory_limit_bytes 1073741824
```

## Running tests

```sh
cargo test
```
