#!/usr/bin/env bash
set -euo pipefail

# Compares local kvns results against published Dragonfly README benchmark values.
# Baselines captured from:
# https://github.com/dragonflydb/dragonfly#benchmarks
# (reference date: 2026-02-23)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require_cmd memtier_benchmark
require_cmd redis-cli
require_cmd jq
require_cmd awk

BENCH_DIR="${BENCH_DIR:-/tmp/kvns-bench-$(date +%s)}"
BENCH_TEST_TIME="${BENCH_TEST_TIME:-100}"
BENCH_PIPE_REQUESTS="${BENCH_PIPE_REQUESTS:-200000}"
BENCH_SHARDED_MODE="${BENCH_SHARDED_MODE:-0}"
BENCH_SHARD_COUNT="${BENCH_SHARD_COUNT:-64}"
BENCH_COMPARE_BOTH="${BENCH_COMPARE_BOTH:-0}"
KVNS_PORT="${KVNS_PORT:-6480}"

mkdir -p "$BENCH_DIR"

echo "Building release binary..."
cargo build --release >/dev/null

# Dragonfly baselines from README benchmark section.
DF_DIRECT_SET_OPS="183704.03"
DF_DIRECT_GET_OPS="195476.19"
DF_DIRECT_SET_LAT="0.340"
DF_DIRECT_GET_LAT="0.320"
DF_PIPE_SET_OPS="3800807.81"
DF_PIPE_GET_OPS="4383297.17"
DF_PIPE_SET_LAT="0.223"
DF_PIPE_GET_LAT="0.193"

pct_of() {
  awk -v a="$1" -v b="$2" 'BEGIN { if (b == 0) { print "n/a"; } else { printf "%.2f%%", (a / b) * 100 } }'
}

ratio_to() {
  awk -v a="$1" -v b="$2" 'BEGIN { if (b == 0) { print "n/a"; } else { printf "%.2fx", a / b } }'
}

collect_metric() {
  local dir="$1"
  local out="$2"
  local direct_set_ops direct_get_ops direct_set_lat direct_get_lat
  local pipe_set_ops pipe_get_ops pipe_set_lat pipe_get_lat

  direct_set_ops="$(jq -r '.["ALL STATS"].Totals["Ops/sec"]' "$dir/direct_set.json")"
  direct_get_ops="$(jq -r '.["ALL STATS"].Totals["Ops/sec"]' "$dir/direct_get.json")"
  direct_set_lat="$(awk '/^Totals/ { print $5; exit }' "$dir/direct_set.txt")"
  direct_get_lat="$(awk '/^Totals/ { print $5; exit }' "$dir/direct_get.txt")"

  pipe_set_ops="$(jq -r '.["ALL STATS"].Totals["Ops/sec"]' "$dir/pipe_set.json")"
  pipe_get_ops="$(jq -r '.["ALL STATS"].Totals["Ops/sec"]' "$dir/pipe_get.json")"
  pipe_set_lat="$(awk '/^Totals/ { print $5; exit }' "$dir/pipe_set.txt")"
  pipe_get_lat="$(awk '/^Totals/ { print $5; exit }' "$dir/pipe_get.txt")"

  cat > "$out" <<METRICS
DIRECT_SET_OPS=$direct_set_ops
DIRECT_GET_OPS=$direct_get_ops
DIRECT_SET_LAT=$direct_set_lat
DIRECT_GET_LAT=$direct_get_lat
PIPE_SET_OPS=$pipe_set_ops
PIPE_GET_OPS=$pipe_get_ops
PIPE_SET_LAT=$pipe_set_lat
PIPE_GET_LAT=$pipe_get_lat
METRICS
}

run_suite() {
  local mode="$1"
  local out_dir="$2"
  local name="$3"
  local kvns_pid=""

  mkdir -p "$out_dir"

  cleanup_local() {
    if [[ -n "$kvns_pid" ]]; then
      kill "$kvns_pid" >/dev/null 2>&1 || true
      wait "$kvns_pid" >/dev/null 2>&1 || true
    fi
  }
  trap cleanup_local RETURN

  echo "Starting kvns (${name}, BENCH_SHARDED_MODE=${mode}, BENCH_SHARD_COUNT=${BENCH_SHARD_COUNT})..."
  KVNS_MEMORY_LIMIT=0 \
  KVNS_SHARDED_MODE="$mode" \
  KVNS_SHARD_COUNT="$BENCH_SHARD_COUNT" \
  target/release/kvns >"$out_dir/server.log" 2>&1 &
  kvns_pid=$!

  for _ in $(seq 1 200); do
    if redis-cli -p "$KVNS_PORT" ping >/dev/null 2>&1; then
      break
    fi
    sleep 0.1
  done
  redis-cli -p "$KVNS_PORT" ping >/dev/null

  echo "[${name}] Running direct SET benchmark..."
  memtier_benchmark \
    -s 127.0.0.1 -p "$KVNS_PORT" \
    -c 20 --test-time "$BENCH_TEST_TIME" -t 8 -d 256 \
    --distinct-client-seed --ratio 1:0 --hide-histogram \
    --out-file "$out_dir/direct_set.txt" \
    --json-out-file "$out_dir/direct_set.json" \
    >"$out_dir/direct_set.cmd.log" 2>&1

  echo "[${name}] Running direct GET benchmark..."
  memtier_benchmark \
    -s 127.0.0.1 -p "$KVNS_PORT" \
    -c 20 --test-time "$BENCH_TEST_TIME" -t 8 -d 256 \
    --distinct-client-seed --ratio 0:1 --hide-histogram \
    --out-file "$out_dir/direct_get.txt" \
    --json-out-file "$out_dir/direct_get.json" \
    >"$out_dir/direct_get.cmd.log" 2>&1

  echo "[${name}] Running pipelined SET benchmark..."
  memtier_benchmark \
    -s 127.0.0.1 -p "$KVNS_PORT" \
    --protocol redis --clients 30 --threads 8 --pipeline 30 \
    --requests "$BENCH_PIPE_REQUESTS" --ratio 1:0 --hide-histogram \
    --out-file "$out_dir/pipe_set.txt" \
    --json-out-file "$out_dir/pipe_set.json" \
    >"$out_dir/pipe_set.cmd.log" 2>&1

  echo "[${name}] Running pipelined GET benchmark..."
  memtier_benchmark \
    -s 127.0.0.1 -p "$KVNS_PORT" \
    --protocol redis --clients 30 --threads 8 --pipeline 30 \
    --requests "$BENCH_PIPE_REQUESTS" --ratio 0:1 --hide-histogram \
    --out-file "$out_dir/pipe_get.txt" \
    --json-out-file "$out_dir/pipe_get.json" \
    >"$out_dir/pipe_get.cmd.log" 2>&1

  collect_metric "$out_dir" "$out_dir/metrics.env"

  kill "$kvns_pid" >/dev/null 2>&1 || true
  wait "$kvns_pid" >/dev/null 2>&1 || true
  kvns_pid=""
  trap - RETURN
}

print_mode_summary() {
  local mode_name="$1"
  local metrics_file="$2"

  # shellcheck disable=SC1090
  source "$metrics_file"

  echo
  echo "${mode_name} summary"
  printf "%-26s %14s %14s %14s\n" "Metric" "kvns" "dragonfly" "kvns vs df"
  printf "%-26s %14s %14s %14s\n" "direct SET ops/sec" "$DIRECT_SET_OPS" "$DF_DIRECT_SET_OPS" "$(pct_of "$DIRECT_SET_OPS" "$DF_DIRECT_SET_OPS")"
  printf "%-26s %14s %14s %14s\n" "direct GET ops/sec" "$DIRECT_GET_OPS" "$DF_DIRECT_GET_OPS" "$(pct_of "$DIRECT_GET_OPS" "$DF_DIRECT_GET_OPS")"
  printf "%-26s %14s %14s %14s\n" "direct SET avg ms" "$DIRECT_SET_LAT" "$DF_DIRECT_SET_LAT" "$(ratio_to "$DIRECT_SET_LAT" "$DF_DIRECT_SET_LAT")"
  printf "%-26s %14s %14s %14s\n" "direct GET avg ms" "$DIRECT_GET_LAT" "$DF_DIRECT_GET_LAT" "$(ratio_to "$DIRECT_GET_LAT" "$DF_DIRECT_GET_LAT")"
  printf "%-26s %14s %14s %14s\n" "pipeline SET ops/sec" "$PIPE_SET_OPS" "$DF_PIPE_SET_OPS" "$(pct_of "$PIPE_SET_OPS" "$DF_PIPE_SET_OPS")"
  printf "%-26s %14s %14s %14s\n" "pipeline GET ops/sec" "$PIPE_GET_OPS" "$DF_PIPE_GET_OPS" "$(pct_of "$PIPE_GET_OPS" "$DF_PIPE_GET_OPS")"
  printf "%-26s %14s %14s %14s\n" "pipeline SET avg ms" "$PIPE_SET_LAT" "$DF_PIPE_SET_LAT" "$(ratio_to "$PIPE_SET_LAT" "$DF_PIPE_SET_LAT")"
  printf "%-26s %14s %14s %14s\n" "pipeline GET avg ms" "$PIPE_GET_LAT" "$DF_PIPE_GET_LAT" "$(ratio_to "$PIPE_GET_LAT" "$DF_PIPE_GET_LAT")"
}

print_compare_summary() {
  local classic_metrics="$1"
  local sharded_metrics="$2"

  # shellcheck disable=SC1090
  source "$classic_metrics"
  local c_direct_set_ops="$DIRECT_SET_OPS"
  local c_direct_get_ops="$DIRECT_GET_OPS"
  local c_pipe_set_ops="$PIPE_SET_OPS"
  local c_pipe_get_ops="$PIPE_GET_OPS"
  local c_direct_set_lat="$DIRECT_SET_LAT"
  local c_direct_get_lat="$DIRECT_GET_LAT"
  local c_pipe_set_lat="$PIPE_SET_LAT"
  local c_pipe_get_lat="$PIPE_GET_LAT"

  # shellcheck disable=SC1090
  source "$sharded_metrics"
  local s_direct_set_ops="$DIRECT_SET_OPS"
  local s_direct_get_ops="$DIRECT_GET_OPS"
  local s_pipe_set_ops="$PIPE_SET_OPS"
  local s_pipe_get_ops="$PIPE_GET_OPS"
  local s_direct_set_lat="$DIRECT_SET_LAT"
  local s_direct_get_lat="$DIRECT_GET_LAT"
  local s_pipe_set_lat="$PIPE_SET_LAT"
  local s_pipe_get_lat="$PIPE_GET_LAT"

  echo
  echo "classic vs sharded"
  printf "%-26s %14s %14s %14s\n" "Metric" "classic" "sharded" "sharded/classic"
  printf "%-26s %14s %14s %14s\n" "direct SET ops/sec" "$c_direct_set_ops" "$s_direct_set_ops" "$(ratio_to "$s_direct_set_ops" "$c_direct_set_ops")"
  printf "%-26s %14s %14s %14s\n" "direct GET ops/sec" "$c_direct_get_ops" "$s_direct_get_ops" "$(ratio_to "$s_direct_get_ops" "$c_direct_get_ops")"
  printf "%-26s %14s %14s %14s\n" "pipeline SET ops/sec" "$c_pipe_set_ops" "$s_pipe_set_ops" "$(ratio_to "$s_pipe_set_ops" "$c_pipe_set_ops")"
  printf "%-26s %14s %14s %14s\n" "pipeline GET ops/sec" "$c_pipe_get_ops" "$s_pipe_get_ops" "$(ratio_to "$s_pipe_get_ops" "$c_pipe_get_ops")"
  printf "%-26s %14s %14s %14s\n" "direct SET avg ms" "$c_direct_set_lat" "$s_direct_set_lat" "$(ratio_to "$c_direct_set_lat" "$s_direct_set_lat")"
  printf "%-26s %14s %14s %14s\n" "direct GET avg ms" "$c_direct_get_lat" "$s_direct_get_lat" "$(ratio_to "$c_direct_get_lat" "$s_direct_get_lat")"
  printf "%-26s %14s %14s %14s\n" "pipeline SET avg ms" "$c_pipe_set_lat" "$s_pipe_set_lat" "$(ratio_to "$c_pipe_set_lat" "$s_pipe_set_lat")"
  printf "%-26s %14s %14s %14s\n" "pipeline GET avg ms" "$c_pipe_get_lat" "$s_pipe_get_lat" "$(ratio_to "$c_pipe_get_lat" "$s_pipe_get_lat")"
}

echo
echo "kvns benchmark run"
echo "  artifacts: $BENCH_DIR"
echo "  direct profile: -c 20 -t 4 -d 256 --test-time $BENCH_TEST_TIME --ratio 1:0 / 0:1"
echo "  pipeline profile: --clients 30 --threads 8 --pipeline 30 --requests $BENCH_PIPE_REQUESTS"

if [[ "$BENCH_COMPARE_BOTH" == "1" ]]; then
  run_suite 0 "$BENCH_DIR/classic" "classic"
  run_suite 1 "$BENCH_DIR/sharded" "sharded"
  print_mode_summary "classic" "$BENCH_DIR/classic/metrics.env"
  print_mode_summary "sharded" "$BENCH_DIR/sharded/metrics.env"
  print_compare_summary "$BENCH_DIR/classic/metrics.env" "$BENCH_DIR/sharded/metrics.env"
else
  run_suite "$BENCH_SHARDED_MODE" "$BENCH_DIR" "single"
  if [[ "$BENCH_SHARDED_MODE" == "1" ]]; then
    print_mode_summary "sharded" "$BENCH_DIR/metrics.env"
  else
    print_mode_summary "classic" "$BENCH_DIR/metrics.env"
  fi
fi

echo
echo "Done."
