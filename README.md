# kvns

A Redis-compatible in-memory key-value store written in Rust. Speaks the [RESP protocol](https://redis.io/docs/latest/develop/reference/protocol-spec/) so any Redis client works out of the box.


## LLM Disclosure
This project is an experiment and in part, uses code generated with models from Anthropic.

## Features

- RESP protocol — compatible with `redis-cli` and Redis client libraries
- Key namespacing via `namespace/key` syntax
- Configurable memory limit with OOM rejection
- Key expiry via `SET … EX` / `SET … PX`
- Optional disk persistence with configurable flush interval
- Configurable eviction (LRU / MRU) with per-namespace policy overrides
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
| `KEYS` | `KEYS pattern` | Returns all keys matching a glob pattern |
| `QUIT` | `QUIT` | Closes the connection |

All commands accept namespaced keys: `SET ns/counter 0`, `INCR ns/counter`, `LPUSH ns/queue item`, `KEYS ns/*`, etc.

### KEYS pattern syntax

| Pattern | Matches |
|---------|---------|
| `*` | Any sequence of characters (including none) |
| `?` | Exactly one character |
| `[ae]` | One of the listed characters (`a` or `e`) |
| `[^e]` / `[!e]` | Any character except those listed |
| `[a-z]` | Any character in the range |

Examples:
```sh
KEYS *          # all keys in all namespaces
KEYS ns/*       # all keys in namespace "ns"
KEYS h?llo      # hello, hallo, hxllo, …
KEYS h[ae]llo   # hello or hallo
```

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
| `KVNS_PERSIST_PATH` | *(unset)* | Path to the persistence file; persistence is disabled if unset |
| `KVNS_PERSIST_INTERVAL` | `300` | Seconds between automatic flushes to disk |
| `KVNS_EVICTION_POLICY` | `none` | Global eviction policy: `lru`, `mru`, or `none` |
| `KVNS_EVICTION_THRESHOLD` | `1.0` | Fraction of memory limit (0.0–1.0) at which eviction triggers |
| `KVNS_NS_EVICTION` | *(unset)* | Per-namespace policy overrides: `ns1:lru,ns2:mru` |

Example:

```sh
KVNS_PORT=6379 KVNS_MEMORY_LIMIT=536870912 cargo run

# With persistence enabled
KVNS_PERSIST_PATH=/var/lib/kvns/db.bin KVNS_PERSIST_INTERVAL=60 cargo run

# With LRU eviction, triggering at 80% capacity
KVNS_EVICTION_POLICY=lru KVNS_EVICTION_THRESHOLD=0.8 cargo run

# Per-namespace overrides: cache namespace uses MRU, sessions namespace uses LRU
KVNS_EVICTION_POLICY=lru KVNS_NS_EVICTION=cache:mru cargo run
```

## Eviction

When the store reaches capacity, writes that would exceed `KVNS_MEMORY_LIMIT` are normally rejected with an OOM error. Configuring an eviction policy tells kvns to automatically remove existing keys instead of rejecting the write.

### Policies

| Policy | Description |
|--------|-------------|
| `none` | No eviction (default). Writes beyond the limit return an error. |
| `lru` | Evict the least-recently-used keys first (lowest hit count). |
| `mru` | Evict the most-recently-used keys first (highest hit count). |

Hit counts are updated by `GET` and can be reset to zero with `TOUCH`.

### Threshold

`KVNS_EVICTION_THRESHOLD` (default `1.0`) sets the fraction of `KVNS_MEMORY_LIMIT` at which eviction begins. At the default of `1.0` eviction only triggers once the limit is fully reached. Setting it lower (e.g. `0.8`) starts evicting at 80% capacity, keeping headroom for bursty writes.

### Per-namespace overrides

`KVNS_NS_EVICTION` accepts a comma-separated list of `namespace:policy` pairs. A namespace listed here uses that policy regardless of the global setting:

```sh
# Global policy is LRU, but the "cache" namespace uses MRU
KVNS_EVICTION_POLICY=lru KVNS_NS_EVICTION=cache:mru cargo run
```

Eviction is always scoped to the namespace of the incoming write — keys from other namespaces are never evicted to make room.

## Persistence

When `KVNS_PERSIST_PATH` is set, kvns periodically serializes the entire store to disk using [bincode](https://github.com/bincode-org/bincode) and writes it atomically via a temp-file rename. On startup, if the file exists it is loaded back into memory; expired entries are silently dropped.

- Persistence is **opt-in** — omitting `KVNS_PERSIST_PATH` keeps the store fully in-memory
- Writes are atomic: a crash mid-flush will never corrupt the existing file
- The flush interval (default 5 minutes) is configurable via `KVNS_PERSIST_INTERVAL`
- On clean shutdown (SIGINT / SIGTERM) the store is flushed to disk immediately
- Parent directories of `KVNS_PERSIST_PATH` are created automatically if they do not exist

## Metrics

kvns exposes a Prometheus scrape endpoint at `http://<KVNS_METRICS_HOST>:<KVNS_METRICS_PORT>/metrics`.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `kvns_keys_total` | Gauge | `namespace` | Current number of live keys per namespace |
| `kvns_memory_used_bytes` | Gauge | `namespace` | Memory currently used per namespace |
| `kvns_memory_used_bytes_total` | Gauge | — | Total memory used across all namespaces |
| `kvns_memory_limit_bytes` | Gauge | — | Configured memory limit |
| `kvns_command_duration_seconds` | Histogram | `command`, `namespace` | Command latency (`set` / `get`) |
| `kvns_evictions_total` | Counter | `namespace` | Cumulative number of keys evicted per namespace |

Per-namespace gauges are created on first write and set to `0` when the last key in a namespace is removed.

## Quick smoke test

```sh
# Start the server with persistence enabled
KVNS_PERSIST_PATH=kvns.db cargo run &

# Write keys in different namespaces
redis-cli -p 6480 SET db1/x 42
redis-cli -p 6480 SET db2/x 99
redis-cli -p 6480 SET counter 0
redis-cli -p 6480 INCR counter

# List keys
redis-cli -p 6480 KEYS "*"      # → counter, db1/x, db2/x
redis-cli -p 6480 KEYS "db1/*"  # → db1/x

# Read values
redis-cli -p 6480 GET db1/x   # → 42
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
