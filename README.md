# kvns

A Redis-compatible in-memory key-value store written in Rust. Speaks the [RESP protocol](https://redis.io/docs/latest/develop/reference/protocol-spec/) so any Redis client works out of the box.

## Features

- RESP protocol — compatible with `redis-cli` and Redis client libraries
- Configurable memory limit with OOM rejection
- Key expiry via `SET … EX` / `SET … PX`
- Prometheus metrics endpoint
- Structured logging via `tracing`

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

| Metric | Type | Description |
|--------|------|-------------|
| `kvns_keys_total` | Gauge | Current number of live keys |
| `kvns_memory_used_bytes` | Gauge | Memory currently used by the store |
| `kvns_memory_limit_bytes` | Gauge | Configured memory limit |
| `kvns_command_duration_seconds{command="set"}` | Histogram | SET command latency |
| `kvns_command_duration_seconds{command="get"}` | Histogram | GET command latency |

## Quick smoke test

```sh
# Start the server
cargo run &

# Write and read a key
redis-cli -p 6480 SET foo bar
redis-cli -p 6480 GET foo

# Check metrics
curl -s http://localhost:9090/metrics | grep kvns
```

## Running tests

```sh
cargo test
```
