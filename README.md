# kvns

A Redis-compatible in-memory key-value store written in Rust.
kvns speaks [RESP](https://redis.io/docs/latest/develop/reference/protocol-spec/) and works with `redis-cli` and Redis client libraries.

## LLM Disclosure

This project is an experiment and in part uses code generated with models from Anthropic.

## Features

- RESP2 server with RESP3 handshake support via `HELLO 3`
- Redis-like command surface across strings, lists, hashes, sets, and sorted sets
- Key namespacing via `namespace/key` syntax
- TTL/expiry management (`EXPIRE*`, `PEXPIRE*`, `PERSIST`, `EXPIRETIME`, `PEXPIRETIME`)
- Configurable memory limit with OOM rejection or namespace-scoped eviction (`lru`, `mru`)
- Memory-limit guardrails when configured (`KVNS_MEMORY_LIMIT` is capped at 70% of host RAM)
- Optional on-disk persistence with periodic flush and shutdown flush
- Prometheus metrics endpoint with per-namespace labels
- Structured logs via `tracing`

## Key namespacing

Keys may include a namespace prefix separated by `/`:

```text
namespace/localkey
```

- `SET db1/x 42` stores key `x` in namespace `db1`
- `GET db1/x` reads key `x` from namespace `db1`
- Keys with no `/` are stored in the `default` namespace

Only the first `/` is treated as the separator, so local keys may also contain `/` (for example `SET ns/a/b value` uses namespace `ns` and key `a/b`).

Namespaces are isolated: `db1/x` and `db2/x` are different keys with independent memory/accounting metrics.

## Supported commands

Command names are case-insensitive.

| Family | Commands |
| --- | --- |
| Connection | `PING`, `QUIT`, `HELLO`, `RESET`, `SELECT` |
| String | `SET`, `GET`, `MGET`, `MSET`, `MSETNX`, `SETNX`, `GETSET`, `GETDEL`, `GETEX`, `APPEND`, `STRLEN`, `INCR`, `INCRBY`, `DECR`, `DECRBY`, `INCRBYFLOAT`, `SETRANGE`, `GETRANGE`, `SUBSTR` |
| List | `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LREM`, `LTRIM`, `LINSERT`, `LPOS`, `LMOVE` |
| Hash | `HSET`, `HMSET`, `HGET`, `HDEL`, `HEXISTS`, `HGETALL`, `HKEYS`, `HVALS`, `HLEN`, `HMGET`, `HINCRBY`, `HINCRBYFLOAT`, `HRANDFIELD` |
| Set | `SADD`, `SREM`, `SMEMBERS`, `SCARD`, `SISMEMBER`, `SMISMEMBER`, `SUNION`, `SINTER`, `SDIFF`, `SUNIONSTORE`, `SINTERSTORE`, `SDIFFSTORE`, `SMOVE`, `SPOP`, `SRANDMEMBER` |
| Sorted set | `ZADD`, `ZRANGE`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `ZREVRANGE`, `ZRANK`, `ZREVRANK`, `ZSCORE`, `ZMSCORE`, `ZREM`, `ZCARD`, `ZCOUNT`, `ZINCRBY`, `ZRANGEBYLEX`, `ZLEXCOUNT`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, `ZREMRANGEBYLEX`, `ZPOPMIN`, `ZPOPMAX`, `ZRANDMEMBER` |
| Generic/keyspace | `DEL`, `UNLINK`, `EXISTS`, `TYPE`, `TTL`, `PTTL`, `EXPIRE`, `EXPIREAT`, `PEXPIRE`, `PEXPIREAT`, `PERSIST`, `EXPIRETIME`, `PEXPIRETIME`, `RENAME`, `RENAMENX`, `SCAN`, `KEYS`, `TOUCH`, `COPY`, `OBJECT` |
| Server/introspection | `DBSIZE`, `FLUSHDB`, `FLUSHALL`, `INFO`, `CONFIG`, `COMMAND`, `CLIENT`, `LATENCY`, `SLOWLOG`, `DEBUG`, `WAIT`, `XADD` |

Compatibility notes:

- `HMSET` is accepted as an alias for `HSET`.
- `SUBSTR` is accepted as an alias for `GETRANGE`.
- `XADD` currently returns `ERR stream type not supported`.
- Some server/introspection subcommands are compatibility shims and return static or empty responses.
- `SELECT` only supports database index `0`.

TTL and expiry return semantics match Redis-style integer responses:

- `TTL`/`PTTL` return `-2` for missing keys
- `TTL`/`PTTL` return `-1` for keys without expiry
- `EXPIRE`, `EXPIREAT`, `PEXPIRE`, and `PEXPIREAT` support `NX`, `XX`, `GT`, `LT`

### Pattern syntax (`KEYS`, `SCAN MATCH`)

| Pattern | Matches |
| --- | --- |
| `*` | Any sequence of characters (including none) |
| `?` | Exactly one character |
| `[ae]` | One of the listed characters (`a` or `e`) |
| `[^e]` / `[!e]` | Any character except those listed |
| `[a-z]` | Any character in the range |

Examples:

```sh
KEYS *           # all keys in all namespaces
KEYS ns/*        # all keys in namespace "ns"
SCAN 0 MATCH ns/* COUNT 50
KEYS h[ae]llo    # hello or hallo
```

## Building and running

Development run:

```sh
cargo run
```

Release build and run:

```sh
cargo build --release
./target/release/kvns
```

Or use the `Makefile`:

```sh
make run
make build
make release
```

## Container usage (Podman)

Build a local image:

```sh
make podman-build IMAGE=kvns:local
```

Run directly:

```sh
make podman-run IMAGE=kvns:local
```

Use `podman compose` with `docker-compose.yaml`:

```sh
make podman-compose-up
make podman-compose-logs
make podman-compose-down
```

Push a multi-arch image to GHCR:

```sh
make podman-login-ghcr GHCR_USER="<github-user>" GHCR_TOKEN="<github-token>"
make podman-push-ghcr GHCR_IMAGE="ghcr.io/<owner>/<repo>" TAG="v0.3.0"
```

Notes:

- `podman-push-ghcr` builds and pushes `linux/amd64,linux/arm64` by default
- Set `PLATFORMS` to override target platforms
- Set `PUSH_LATEST=false` to skip publishing `latest`

## Configuration

All settings are read from environment variables at startup.

| Variable | Default | Description |
| --- | --- | --- |
| `KVNS_HOST` | `0.0.0.0` | Interface to listen on |
| `KVNS_PORT` | `6480` | RESP listener port |
| `KVNS_MEMORY_LIMIT` | `1073741824` | Max memory in bytes (1 GiB). When set, kvns caps it at 70% of detected host RAM |
| `KVNS_METRICS_HOST` | `0.0.0.0` | Metrics listener host |
| `KVNS_METRICS_PORT` | `9090` | Metrics listener port |
| `KVNS_PERSIST_PATH` | *(unset)* | Persistence file path; persistence is disabled if unset |
| `KVNS_PERSIST_INTERVAL` | `300` | Seconds between automatic flushes |
| `KVNS_EVICTION_POLICY` | `none` | Global eviction policy: `lru`, `mru`, or `none` |
| `KVNS_EVICTION_THRESHOLD` | `1.0` | Fraction of memory limit (0.0-1.0) at which eviction starts |
| `KVNS_NS_EVICTION` | *(unset)* | Per-namespace policy overrides, e.g. `ns1:lru,ns2:mru` |
| `KVNS_SHARDED_MODE` | `false` | Enable experimental sharded lock backend (currently supports `PING`, `QUIT`, `SET`, `GET`, `MGET`, `MSET`, `MSETNX`, `SETNX`, `INCR`, `INCRBY`, `DECR`, `DECRBY`) |
| `KVNS_SHARD_COUNT` | `4 * CPU cores` | Number of lock shards when `KVNS_SHARDED_MODE=true` |

Examples:

```sh
# Custom port + memory limit
KVNS_PORT=6379 KVNS_MEMORY_LIMIT=536870912 cargo run

# Enable persistence
KVNS_PERSIST_PATH=/var/lib/kvns/db.rkyv KVNS_PERSIST_INTERVAL=60 cargo run

# Enable LRU eviction at 80% memory usage
KVNS_EVICTION_POLICY=lru KVNS_EVICTION_THRESHOLD=0.8 cargo run

# Override one namespace to MRU while global policy is LRU
KVNS_EVICTION_POLICY=lru KVNS_NS_EVICTION=cache:mru cargo run

# Run the experimental sharded backend
KVNS_SHARDED_MODE=true KVNS_SHARD_COUNT=64 cargo run
```

Sharded mode notes:

- `KVNS_SHARDED_MODE` is experimental and currently optimized for throughput-oriented string workloads.
- Under concurrent writers, multi-key command atomicity may differ from classic mode.

Memory limit behavior:

- If `KVNS_MEMORY_LIMIT` is unset, kvns uses the default `1073741824` bytes (1 GiB)
- If `KVNS_MEMORY_LIMIT` is set above 70% of detected host RAM, kvns clamps it to that 70% cap
- If `KVNS_MEMORY_LIMIT=0`, kvns uses the same 70% cap directly
- If host memory cannot be detected, kvns keeps the configured value; `0` falls back to the 1 GiB default

## Eviction

When a write would exceed the effective `KVNS_MEMORY_LIMIT`, kvns either rejects it with OOM or evicts keys depending on policy.

| Policy | Description |
| --- | --- |
| `none` | No eviction (default). Writes beyond limit return an error. |
| `lru` | Evict lowest-hit keys first. |
| `mru` | Evict highest-hit keys first. |

`KVNS_EVICTION_THRESHOLD` controls when eviction begins. With `1.0` (default), eviction starts only at full configured capacity. Lower values (for example `0.8`) start eviction earlier.

`KVNS_NS_EVICTION` supports comma-separated `namespace:policy` overrides. Eviction is namespace-scoped: a write in one namespace never evicts keys from another namespace.

## Persistence

When `KVNS_PERSIST_PATH` is set, kvns periodically snapshots the full store to disk using [`rkyv`](https://github.com/rkyv/rkyv). Writes are atomic: data is written to a temporary file (`*.tmp`) and then renamed into place.

- Persistence is opt-in (unset `KVNS_PERSIST_PATH` for in-memory only)
- Flush interval is controlled by `KVNS_PERSIST_INTERVAL`
- On startup, persisted data is loaded if present
- Expired entries are dropped during load
- On clean shutdown (`SIGINT`/`SIGTERM`), kvns flushes immediately
- Parent directories for `KVNS_PERSIST_PATH` are created automatically

## Metrics

kvns exposes Prometheus metrics at `http://<KVNS_METRICS_HOST>:<KVNS_METRICS_PORT>/metrics`.

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `kvns_keys_total` | Gauge | `namespace` | Current live key count per namespace |
| `kvns_memory_used_bytes` | Gauge | `namespace` | Current memory used per namespace |
| `kvns_memory_used_bytes_total` | Gauge | - | Total memory used across all namespaces |
| `kvns_memory_limit_bytes` | Gauge | - | Configured memory limit |
| `kvns_command_duration_seconds` | Histogram | `command`, `namespace` | Command latency histogram (currently instrumented for `SET`) |
| `kvns_evictions_total` | Counter | `namespace` | Total keys evicted per namespace |

Per-namespace gauges are created on first write and are set to `0` when the last key in a namespace is removed.

## Quick smoke test

```sh
# Start kvns with persistence enabled
KVNS_PERSIST_PATH=kvns.db cargo run &

# Write namespaced and default keys
redis-cli -p 6480 SET db1/x 42
redis-cli -p 6480 SET db2/x 99
redis-cli -p 6480 SET counter 0
redis-cli -p 6480 INCR counter

# Query data
redis-cli -p 6480 GET db1/x
redis-cli -p 6480 KEYS "db*/*"
redis-cli -p 6480 SCAN 0 MATCH "db*/*" COUNT 100

# Inspect metrics
curl -s http://localhost:9090/metrics | grep -E '^kvns_(memory|keys|evictions)'
```

## Running tests and checks

```sh
cargo test
make lint
make fmt-check
```

## Benchmarking

Run benchmark profiles aligned with Dragonfly's published benchmark patterns and print a comparison report:

```sh
make benchmark
```

Run the same benchmark suite against kvns experimental sharded backend:

```sh
make benchmark-sharded
```

Run classic and sharded back-to-back and print a direct speedup table:

```sh
make benchmark-compare
```

Notes:

- Benchmark script path: `scripts/benchmark_kvns_vs_dragonfly.sh`
- Output artifacts are written under `/tmp/kvns-bench-*` (or `BENCH_DIR` if set)
- Dragonfly baseline numbers are sourced from `https://github.com/dragonflydb/dragonfly#benchmarks` (as of February 23, 2026)
