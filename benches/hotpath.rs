/// Non-pipeline hot-path micro-benchmarks.
///
/// Each group targets a specific layer in the GET/SET critical path:
///   1. parse_ns_key   — namespace + key String allocation per command
///   2. resp_parse     — RESP2 array parsing into Vec<Vec<u8>>
///   3. resp_build     — response builder allocations
///   4. store_ops      — Db::put / Db::get under an RwLock (sync stand-in)
///
/// Run with:
///   cargo bench --bench hotpath
///
/// Compare across branches / after changes with:
///   cargo bench --bench hotpath -- --save-baseline before
///   # make changes
///   cargo bench --bench hotpath -- --baseline before

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::io::Cursor;
use tokio::runtime::Runtime;

// ── helpers to reach crate-internal items ──────────────────────────────────

// We use the binary as a library via `use kvns::*` — but kvns has no lib
// target, so we shell-out to the relevant logic by copy for now.
// Each benchmark only exercises the pure logic slice it targets.

// ── 1. parse_ns_key ────────────────────────────────────────────────────────

/// Old implementation: always allocates two Strings.
fn parse_ns_key_old(raw: &[u8]) -> (String, String) {
    let s = std::str::from_utf8(raw).unwrap_or("");
    match s.find('/') {
        Some(pos) => (s[..pos].to_owned(), s[pos + 1..].to_owned()),
        None => ("default".to_owned(), s.to_owned()),
    }
}

/// New implementation: Cow — zero allocation for valid UTF-8 (common path).
fn parse_ns_key_new(raw: &[u8]) -> (std::borrow::Cow<'_, str>, std::borrow::Cow<'_, str>) {
    use std::borrow::Cow;
    match std::str::from_utf8(raw) {
        Ok(s) => match s.find('/') {
            Some(pos) => (Cow::Borrowed(&s[..pos]), Cow::Borrowed(&s[pos + 1..])),
            None => (Cow::Borrowed("default"), Cow::Borrowed(s)),
        },
        Err(_) => {
            let s = String::from_utf8_lossy(raw).into_owned();
            match s.find('/') {
                Some(pos) => {
                    let key = s[pos + 1..].to_owned();
                    let ns = s[..pos].to_owned();
                    (Cow::Owned(ns), Cow::Owned(key))
                }
                None => (Cow::Borrowed("default"), Cow::Owned(s)),
            }
        }
    }
}

fn bench_parse_ns_key(c: &mut Criterion) {
    let mut g = c.benchmark_group("parse_ns_key");

    // Before: allocates two Strings every call.
    g.bench_function("bare_key_alloc", |b| {
        b.iter(|| parse_ns_key_old(black_box(b"mykey")))
    });
    g.bench_function("namespaced_key_alloc", |b| {
        b.iter(|| parse_ns_key_old(black_box(b"myns/mykey")))
    });
    g.bench_function("short_key_alloc", |b| {
        b.iter(|| parse_ns_key_old(black_box(b"k")))
    });

    // After: Cow::Borrowed — zero allocation for valid UTF-8.
    g.bench_function("bare_key_cow", |b| {
        b.iter(|| parse_ns_key_new(black_box(b"mykey")))
    });
    g.bench_function("namespaced_key_cow", |b| {
        b.iter(|| parse_ns_key_new(black_box(b"myns/mykey")))
    });
    g.bench_function("short_key_cow", |b| {
        b.iter(|| parse_ns_key_new(black_box(b"k")))
    });

    g.finish();
}

// ── 2. RESP parse ──────────────────────────────────────────────────────────

/// Raw bytes for a `SET mykey myvalue` in RESP2 array encoding.
const SET_CMD: &[u8] = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n";

/// Raw bytes for a `GET mykey` in RESP2 array encoding.
const GET_CMD: &[u8] = b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n";

/// Raw bytes for a bare `PING` (inline).
const PING_CMD: &[u8] = b"PING\r\n";

async fn parse_one(data: &[u8]) -> Option<Vec<Vec<u8>>> {
    use tokio::io::BufReader;
    let cursor = Cursor::new(data);
    let mut reader = BufReader::new(cursor);
    kvns_bench_parse_resp(&mut reader).await.unwrap()
}

/// Stand-in for resp::parse_resp (reimplemented inline so we don't need pub).
async fn kvns_bench_parse_resp<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
) -> std::io::Result<Option<Vec<Vec<u8>>>> {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt};

    let mut line = Vec::new();
    // read_until '\n'
    let n = reader.read_until(b'\n', &mut line).await?;
    if n == 0 {
        return Ok(None);
    }
    // strip \r\n
    while line.last() == Some(&b'\n') { line.pop(); }
    while line.last() == Some(&b'\r') { line.pop(); }

    if line.is_empty() {
        return Ok(Some(vec![]));
    }

    match line[0] {
        b'*' => {
            let count: usize = std::str::from_utf8(&line[1..]).unwrap().parse().unwrap();
            let mut args = Vec::with_capacity(count);
            for _ in 0..count {
                let mut hdr = Vec::new();
                reader.read_until(b'\n', &mut hdr).await?;
                while hdr.last() == Some(&b'\n') { hdr.pop(); }
                while hdr.last() == Some(&b'\r') { hdr.pop(); }
                let len: usize = std::str::from_utf8(&hdr[1..]).unwrap().parse().unwrap();
                let mut buf = vec![0u8; len];
                reader.read_exact(&mut buf).await?;
                let mut crlf = [0u8; 2];
                reader.read_exact(&mut crlf).await?;
                args.push(buf);
            }
            Ok(Some(args))
        }
        _ => {
            // inline
            let parts: Vec<Vec<u8>> = line.split(|&b| b == b' ')
                .filter(|s| !s.is_empty())
                .map(|s| s.to_vec())
                .collect();
            Ok(Some(parts))
        }
    }
}

fn bench_resp_parse(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut g = c.benchmark_group("resp_parse");

    g.bench_function("SET_3args", |b| {
        b.iter(|| rt.block_on(parse_one(black_box(SET_CMD))))
    });

    g.bench_function("GET_2args", |b| {
        b.iter(|| rt.block_on(parse_one(black_box(GET_CMD))))
    });

    g.bench_function("PING_inline", |b| {
        b.iter(|| rt.block_on(parse_one(black_box(PING_CMD))))
    });

    g.finish();
}

// ── 3. Response builder allocations ────────────────────────────────────────
//
// Compares the old (Vec<u8> allocation) pattern against the new
// (Cow::Borrowed, zero allocation) pattern for constant responses.

use std::borrow::Cow;

// Old pattern: always allocates.
fn resp_ok_old() -> Vec<u8> { b"+OK\r\n".to_vec() }
fn resp_null_old() -> Vec<u8> { b"$-1\r\n".to_vec() }

// New pattern: Borrowed for constants — zero allocation.
fn resp_ok_new() -> Cow<'static, [u8]> { Cow::Borrowed(b"+OK\r\n") }
fn resp_null_new() -> Cow<'static, [u8]> { Cow::Borrowed(b"$-1\r\n") }

// Dynamic response (unchanged in both approaches — allocation is inherent).
fn resp_bulk(data: &[u8]) -> Cow<'static, [u8]> {
    let mut out = Vec::with_capacity(data.len() + 32);
    out.push(b'$');
    out.extend_from_slice(data.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
    Cow::Owned(out)
}

fn bench_resp_build(c: &mut Criterion) {
    let mut g = c.benchmark_group("resp_build");

    // Before: Vec allocation per call.
    g.bench_function("resp_ok_alloc", |b| b.iter(|| black_box(resp_ok_old())));
    g.bench_function("resp_null_alloc", |b| b.iter(|| black_box(resp_null_old())));

    // After: Cow::Borrowed — zero allocation.
    g.bench_function("resp_ok_static", |b| b.iter(|| black_box(resp_ok_new())));
    g.bench_function("resp_null_static", |b| b.iter(|| black_box(resp_null_new())));

    let value = b"hello world this is a typical value";
    g.bench_function("resp_bulk_35b", |b| {
        b.iter(|| black_box(resp_bulk(value)))
    });

    let large_value = vec![b'x'; 256];
    g.bench_function("resp_bulk_256b", |b| {
        b.iter(|| black_box(resp_bulk(&large_value)))
    });

    g.finish();
}

// ── 4. Metrics overhead ────────────────────────────────────────────────────
//
// Measures the cost of metrics::gauge! and metrics::histogram! calls.
// These run on every write command and on delete().

fn bench_metrics_overhead(c: &mut Criterion) {
    // Install a no-op recorder so metric calls don't panic.
    // metrics::set_global_recorder requires a 'static recorder.
    // The simplest approach: use the noop recorder from the metrics crate.
    let _ = metrics::set_global_recorder(metrics::NoopRecorder);

    let mut g = c.benchmark_group("metrics");

    g.bench_function("gauge_set", |b| {
        b.iter(|| {
            metrics::gauge!("kvns_keys_total", "namespace" => "default")
                .set(black_box(42.0f64))
        })
    });

    g.bench_function("histogram_record", |b| {
        b.iter(|| {
            metrics::histogram!("kvns_command_duration_seconds", "command" => "set")
                .record(black_box(0.000123f64))
        })
    });

    g.bench_function("three_gauges_emit", |b| {
        b.iter(|| {
            metrics::gauge!("kvns_keys_total", "namespace" => "default").set(black_box(1.0f64));
            metrics::gauge!("kvns_memory_used_bytes", "namespace" => "default")
                .set(black_box(256.0f64));
            metrics::gauge!("kvns_memory_used_bytes_total").set(black_box(256.0f64));
        })
    });

    g.finish();
}

// ── 5. HashMap double-lookup in SET path ───────────────────────────────────
//
// Profiles the cost of the (namespace, key) pair lookups that happen
// 3 separate times in the current SET path: would_exceed → net_delta → put_deferred.

use std::collections::HashMap;

fn bench_hashmap_lookups(c: &mut Criterion) {
    let mut outer: HashMap<String, HashMap<String, Vec<u8>>> = HashMap::new();
    let ns = "default".to_owned();
    let key = "mykey".to_owned();
    outer
        .entry(ns.clone())
        .or_default()
        .insert(key.clone(), b"existing_value".to_vec());

    let mut g = c.benchmark_group("hashmap_lookup");

    // Single double-lookup (one pass through the nested map)
    g.bench_function("single_ns_key_lookup", |b| {
        b.iter(|| {
            black_box(
                outer
                    .get(black_box(&ns))
                    .and_then(|m| m.get(black_box(&key))),
            )
        })
    });

    // Three sequential lookups — mirrors would_exceed + net_delta + put_deferred
    g.bench_function("triple_ns_key_lookup", |b| {
        b.iter(|| {
            let _a = outer.get(&ns).and_then(|m| m.get(&key));
            let _b = outer.get(&ns).and_then(|m| m.get(&key));
            let _c = outer.get(&ns).and_then(|m| m.get(&key));
            black_box((_a, _b, _c))
        })
    });

    g.finish();
}

// ── registry ───────────────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_parse_ns_key,
    bench_resp_parse,
    bench_resp_build,
    bench_metrics_overhead,
    bench_hashmap_lookups,
);
criterion_main!(benches);
