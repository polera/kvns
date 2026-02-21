mod config;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tracing::{debug, error, info};

// ─── Store ────────────────────────────────────────────────────────────────────

struct Entry {
    value: Vec<u8>,
    hits: u64,
    expiry: Option<Instant>,
}

impl Entry {
    fn new(value: Vec<u8>, ttl: Option<Duration>) -> Self {
        Self {
            value,
            hits: 0,
            expiry: ttl.map(|d| Instant::now() + d),
        }
    }

    fn is_expired(&self) -> bool {
        self.expiry.is_some_and(|e| Instant::now() >= e)
    }

    fn time_to_expiry_secs(&self) -> i64 {
        match self.expiry {
            None => 0,
            Some(e) => {
                let now = Instant::now();
                if e <= now { 0 } else { (e - now).as_secs() as i64 }
            }
        }
    }
}

struct Db {
    entries: HashMap<String, Entry>,
    used_bytes: usize,
    memory_limit: usize,
}

impl Db {
    fn new(memory_limit: usize) -> Self {
        metrics::gauge!("kvns_memory_limit_bytes").set(memory_limit as f64);
        metrics::gauge!("kvns_memory_used_bytes").set(0.0);
        metrics::gauge!("kvns_keys_total").set(0.0);
        Self {
            entries: HashMap::new(),
            used_bytes: 0,
            memory_limit,
        }
    }

    fn entry_size(key: &str, value: &[u8]) -> usize {
        key.len() + value.len()
    }

    fn would_exceed(&self, key: &str, value: &[u8]) -> bool {
        let new_size = Self::entry_size(key, value);
        let old_size = self
            .entries
            .get(key)
            .map(|e| Self::entry_size(key, &e.value))
            .unwrap_or(0);
        let net_delta = new_size.saturating_sub(old_size);
        self.used_bytes.saturating_add(net_delta) > self.memory_limit
    }

    fn put(&mut self, key: String, entry: Entry) {
        let new_size = Self::entry_size(&key, &entry.value);
        let old_size = self
            .entries
            .get(&key)
            .map(|e| Self::entry_size(&key, &e.value))
            .unwrap_or(0);
        self.used_bytes = self
            .used_bytes
            .saturating_sub(old_size)
            .saturating_add(new_size);
        self.entries.insert(key, entry);
        metrics::gauge!("kvns_keys_total").set(self.entries.len() as f64);
        metrics::gauge!("kvns_memory_used_bytes").set(self.used_bytes as f64);
    }

    fn delete(&mut self, key: &str) -> Option<Entry> {
        let removed = self.entries.remove(key);
        if let Some(ref e) = removed {
            let size = Self::entry_size(key, &e.value);
            self.used_bytes = self.used_bytes.saturating_sub(size);
        }
        metrics::gauge!("kvns_keys_total").set(self.entries.len() as f64);
        metrics::gauge!("kvns_memory_used_bytes").set(self.used_bytes as f64);
        removed
    }
}

type Store = Arc<RwLock<Db>>;

// ─── RESP protocol ────────────────────────────────────────────────────────────

async fn parse_resp<R: AsyncBufReadExt + Unpin>(
    reader: &mut R,
) -> std::io::Result<Option<Vec<Vec<u8>>>> {
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Ok(None);
    }
    let trimmed = line.trim_end_matches(['\r', '\n']);
    if trimmed.is_empty() {
        return Ok(Some(vec![]));
    }
    if let Some(rest) = trimmed.strip_prefix('*') {
        let count: usize = rest
            .parse()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "bad count"))?;
        let mut args = Vec::with_capacity(count);
        for _ in 0..count {
            let mut hdr = String::new();
            reader.read_line(&mut hdr).await?;
            let hdr = hdr.trim_end_matches(['\r', '\n']);
            let len: i64 = hdr
                .strip_prefix('$')
                .ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "expected $")
                })?
                .parse()
                .map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "bad len")
                })?;
            if len < 0 {
                args.push(vec![]);
            } else {
                let mut buf = vec![0u8; len as usize + 2]; // +2 for \r\n
                reader.read_exact(&mut buf).await?;
                buf.truncate(len as usize);
                args.push(buf);
            }
        }
        Ok(Some(args))
    } else {
        // inline command (e.g. PING without framing)
        Ok(Some(
            trimmed
                .split_whitespace()
                .map(|s| s.as_bytes().to_vec())
                .collect(),
        ))
    }
}

fn resp_ok() -> Vec<u8>           { b"+OK\r\n".to_vec() }
fn resp_pong() -> Vec<u8>         { b"+PONG\r\n".to_vec() }
fn resp_null() -> Vec<u8>         { b"$-1\r\n".to_vec() }
fn resp_int(n: i64) -> Vec<u8>    { format!(":{n}\r\n").into_bytes() }
fn resp_err(msg: &str) -> Vec<u8> { format!("-ERR {msg}\r\n").into_bytes() }

fn resp_bulk(data: &[u8]) -> Vec<u8> {
    let mut out = format!("${}\r\n", data.len()).into_bytes();
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
    out
}

fn wrong_args(cmd: &[u8]) -> Vec<u8> {
    resp_err(&format!(
        "wrong number of arguments for {}",
        String::from_utf8_lossy(cmd)
    ))
}

// ─── Commands ─────────────────────────────────────────────────────────────────

async fn cmd_set(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 3 && args.len() != 5 {
        return wrong_args(&args[0]);
    }
    let key = String::from_utf8_lossy(&args[1]).into_owned();
    let value = args[2].clone();

    let ttl = if args.len() == 5 {
        let qualifier = String::from_utf8_lossy(&args[3]).to_ascii_lowercase();
        let amount: u64 = match String::from_utf8_lossy(&args[4]).parse() {
            Ok(v) => v,
            Err(_) => return resp_err("invalid expire time"),
        };
        Some(if qualifier == "px" {
            Duration::from_millis(amount)
        } else {
            Duration::from_secs(amount)
        })
    } else {
        None
    };

    let mut db = store.write().await;
    if db.would_exceed(&key, &value) {
        return resp_err("OOM command not allowed when used memory > 'maxmemory'");
    }
    let entry = Entry::new(value, ttl);
    let expiry = entry.expiry;
    db.put(key.clone(), entry);
    drop(db);

    // Spawn a background task to expire the key at the deadline.
    // We record the expiry Instant so we can skip deletion if the key
    // was overwritten before the timer fires.
    if let Some(deadline) = expiry {
        let store = Arc::clone(store);
        tokio::spawn(async move {
            let now = Instant::now();
            if deadline > now {
                tokio::time::sleep(deadline - now).await;
            }
            let mut db = store.write().await;
            if db.entries.get(&key).is_some_and(|e| e.expiry == Some(deadline)) {
                debug!(key = %key, "expiring key");
                db.delete(&key);
            }
        });
    }

    resp_ok()
}

async fn cmd_get(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let key = String::from_utf8_lossy(&args[1]).into_owned();
    let mut db = store.write().await;

    // Lazy expiry: remove on access if the entry has already expired.
    if db.entries.get(&key).is_some_and(|e| e.is_expired()) {
        db.delete(&key);
        return resp_null();
    }

    match db.entries.get_mut(&key) {
        None => resp_null(),
        Some(entry) => {
            entry.hits += 1;
            let value = entry.value.clone();
            debug!(key = %key, hits = entry.hits, "GET");
            resp_bulk(&value)
        }
    }
}

async fn cmd_del(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let key = String::from_utf8_lossy(&args[1]);
    let removed = store.write().await.delete(key.as_ref()).is_some();
    resp_int(if removed { 1 } else { 0 })
}

async fn cmd_ttl(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let key = String::from_utf8_lossy(&args[1]);
    match store.read().await.entries.get(key.as_ref()) {
        None => resp_null(),
        Some(entry) => resp_int(entry.time_to_expiry_secs()),
    }
}

async fn cmd_touch(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let key = String::from_utf8_lossy(&args[1]);
    let mut db = store.write().await;
    match db.entries.get_mut(key.as_ref()) {
        None => resp_null(),
        Some(entry) => {
            entry.hits = 0;
            debug!(key = %key, "TOUCH");
            resp_int(entry.hits as i64)
        }
    }
}

async fn cmd_incr(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let key = String::from_utf8_lossy(&args[1]).into_owned();
    let mut db = store.write().await;

    let current: i64 = match db.entries.get(&key) {
        None => 0,
        Some(entry) => match std::str::from_utf8(&entry.value).ok().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return resp_err("value is not an integer or out of range"),
        },
    };

    let next = match current.checked_add(1) {
        Some(n) => n,
        None => return resp_err("increment would overflow"),
    };

    let new_value = next.to_string().into_bytes();
    if db.would_exceed(&key, &new_value) {
        return resp_err("OOM command not allowed when used memory > 'maxmemory'");
    }
    db.put(key, Entry::new(new_value, None));
    resp_int(next)
}

async fn dispatch(args: &[Vec<u8>], store: &Store) -> (Vec<u8>, bool) {
    let cmd = String::from_utf8_lossy(&args[0]).to_ascii_lowercase();
    let resp = match cmd.as_str() {
        "ping"  => resp_pong(),
        "quit"  => return (resp_ok(), true),
        "set"   => cmd_set(args, store).await,
        "get"   => cmd_get(args, store).await,
        "del"   => cmd_del(args, store).await,
        "ttl"   => cmd_ttl(args, store).await,
        "touch" => cmd_touch(args, store).await,
        "incr"  => cmd_incr(args, store).await,
        _ => format!(
            "-ERR unknown command {}\r\n",
            String::from_utf8_lossy(&args[0])
        )
        .into_bytes(),
    };
    (resp, false)
}

// ─── Server ───────────────────────────────────────────────────────────────────

async fn handle_connection(stream: TcpStream, store: Store) {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    loop {
        match parse_resp(&mut reader).await {
            Ok(None) => break,
            Ok(Some(args)) if args.is_empty() => continue,
            Ok(Some(args)) => {
                let (response, quit) = dispatch(&args, &store).await;
                if write_half.write_all(&response).await.is_err() || quit {
                    break;
                }
            }
            Err(_) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_store() -> Store {
        Arc::new(RwLock::new(Db::new(config::DEFAULT_MEMORY_LIMIT)))
    }

    fn args(parts: &[&str]) -> Vec<Vec<u8>> {
        parts.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    fn parse_int_resp(resp: &[u8]) -> i64 {
        assert!(resp.starts_with(b":"), "expected integer, got {:?}", resp);
        std::str::from_utf8(&resp[1..resp.len() - 2])
            .unwrap()
            .parse()
            .unwrap()
    }

    // ── Entry ─────────────────────────────────────────────────────────────────

    #[test]
    fn entry_no_ttl_never_expires() {
        let e = Entry::new(b"v".to_vec(), None);
        assert!(!e.is_expired());
        assert_eq!(e.time_to_expiry_secs(), 0);
    }

    #[test]
    fn entry_future_ttl_not_expired() {
        let e = Entry::new(b"v".to_vec(), Some(Duration::from_secs(3600)));
        assert!(!e.is_expired());
        assert!(e.time_to_expiry_secs() > 0);
    }

    #[test]
    fn entry_elapsed_ttl_is_expired() {
        let e = Entry {
            value: b"v".to_vec(),
            hits: 0,
            expiry: Some(Instant::now() - Duration::from_secs(1)),
        };
        assert!(e.is_expired());
        assert_eq!(e.time_to_expiry_secs(), 0);
    }

    // ── RESP parser ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn parse_array_set_command() {
        let data = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let mut r = BufReader::new(&data[..]);
        let result = parse_resp(&mut r).await.unwrap().unwrap();
        assert_eq!(result, vec![b"SET", b"foo", b"bar"]);
    }

    #[tokio::test]
    async fn parse_inline_ping() {
        let data = b"PING\r\n";
        let mut r = BufReader::new(&data[..]);
        let result = parse_resp(&mut r).await.unwrap().unwrap();
        assert_eq!(result, vec![b"PING"]);
    }

    #[tokio::test]
    async fn parse_inline_with_args() {
        let data = b"GET mykey\r\n";
        let mut r = BufReader::new(&data[..]);
        let result = parse_resp(&mut r).await.unwrap().unwrap();
        assert_eq!(result[0], b"GET");
        assert_eq!(result[1], b"mykey");
    }

    #[tokio::test]
    async fn parse_eof_returns_none() {
        let data: &[u8] = b"";
        let mut r = BufReader::new(data);
        assert!(parse_resp(&mut r).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn parse_empty_line_returns_empty_vec() {
        let data = b"\r\n";
        let mut r = BufReader::new(&data[..]);
        let result = parse_resp(&mut r).await.unwrap().unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn parse_null_bulk_string() {
        let data = b"*2\r\n$3\r\nGET\r\n$-1\r\n";
        let mut r = BufReader::new(&data[..]);
        let result = parse_resp(&mut r).await.unwrap().unwrap();
        assert_eq!(result[0], b"GET");
        assert_eq!(result[1], b"");
    }

    // ── PING / QUIT ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn ping_returns_pong() {
        let store = make_store();
        let (resp, quit) = dispatch(&args(&["PING"]), &store).await;
        assert_eq!(resp, b"+PONG\r\n");
        assert!(!quit);
    }

    #[tokio::test]
    async fn ping_case_insensitive() {
        let store = make_store();
        let (resp, _) = dispatch(&args(&["ping"]), &store).await;
        assert_eq!(resp, b"+PONG\r\n");
    }

    #[tokio::test]
    async fn quit_returns_ok_and_sets_quit_flag() {
        let store = make_store();
        let (resp, quit) = dispatch(&args(&["QUIT"]), &store).await;
        assert_eq!(resp, b"+OK\r\n");
        assert!(quit);
    }

    #[tokio::test]
    async fn unknown_command_returns_error() {
        let store = make_store();
        let (resp, quit) = dispatch(&args(&["BLORP"]), &store).await;
        assert!(resp.starts_with(b"-ERR unknown command BLORP"));
        assert!(!quit);
    }

    // ── SET ───────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn set_and_get_roundtrip() {
        let store = make_store();
        assert_eq!(cmd_set(&args(&["SET", "k", "hello"]), &store).await, b"+OK\r\n");
        assert_eq!(cmd_get(&args(&["GET", "k"]), &store).await, b"$5\r\nhello\r\n");
    }

    #[tokio::test]
    async fn set_overwrites_existing_key() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "first"]), &store).await;
        cmd_set(&args(&["SET", "k", "second"]), &store).await;
        assert_eq!(cmd_get(&args(&["GET", "k"]), &store).await, b"$6\r\nsecond\r\n");
    }

    #[tokio::test]
    async fn set_too_few_args_returns_error() {
        let store = make_store();
        let resp = cmd_set(&args(&["SET", "k"]), &store).await;
        assert!(resp.starts_with(b"-ERR"));
    }

    #[tokio::test]
    async fn set_four_args_returns_error() {
        let store = make_store();
        let resp = cmd_set(&args(&["SET", "k", "v", "EX"]), &store).await;
        assert!(resp.starts_with(b"-ERR"));
    }

    #[tokio::test]
    async fn set_ex_stores_ttl_in_seconds() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v", "EX", "100"]), &store).await;
        let secs = parse_int_resp(&cmd_ttl(&args(&["TTL", "k"]), &store).await);
        assert!(secs > 90 && secs <= 100, "unexpected TTL: {secs}");
    }

    #[tokio::test]
    async fn set_px_stores_ttl_in_milliseconds() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v", "PX", "5000"]), &store).await;
        let secs = parse_int_resp(&cmd_ttl(&args(&["TTL", "k"]), &store).await);
        assert!(secs >= 4 && secs <= 5, "unexpected TTL: {secs}");
    }

    #[tokio::test]
    async fn set_ex_case_insensitive() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v", "ex", "60"]), &store).await;
        let secs = parse_int_resp(&cmd_ttl(&args(&["TTL", "k"]), &store).await);
        assert!(secs > 50 && secs <= 60);
    }

    // ── GET ───────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn get_missing_key_returns_null() {
        let store = make_store();
        assert_eq!(cmd_get(&args(&["GET", "missing"]), &store).await, b"$-1\r\n");
    }

    #[tokio::test]
    async fn get_wrong_args_returns_error() {
        let store = make_store();
        let resp = cmd_get(&args(&["GET"]), &store).await;
        assert!(resp.starts_with(b"-ERR"));
    }

    #[tokio::test]
    async fn get_increments_hit_counter() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v"]), &store).await;
        cmd_get(&args(&["GET", "k"]), &store).await;
        cmd_get(&args(&["GET", "k"]), &store).await;
        cmd_get(&args(&["GET", "k"]), &store).await;
        assert_eq!(store.read().await.entries.get("k").unwrap().hits, 3);
    }

    // ── DEL ───────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn del_existing_key_returns_1() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v"]), &store).await;
        assert_eq!(cmd_del(&args(&["DEL", "k"]), &store).await, b":1\r\n");
    }

    #[tokio::test]
    async fn del_removes_key() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v"]), &store).await;
        cmd_del(&args(&["DEL", "k"]), &store).await;
        assert_eq!(cmd_get(&args(&["GET", "k"]), &store).await, b"$-1\r\n");
    }

    #[tokio::test]
    async fn del_missing_key_returns_0() {
        let store = make_store();
        assert_eq!(cmd_del(&args(&["DEL", "nope"]), &store).await, b":0\r\n");
    }

    #[tokio::test]
    async fn del_wrong_args_returns_error() {
        let store = make_store();
        let resp = cmd_del(&args(&["DEL"]), &store).await;
        assert!(resp.starts_with(b"-ERR"));
    }

    // ── TTL ───────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn ttl_missing_key_returns_null() {
        let store = make_store();
        assert_eq!(cmd_ttl(&args(&["TTL", "nope"]), &store).await, b"$-1\r\n");
    }

    #[tokio::test]
    async fn ttl_key_without_expiry_returns_0() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v"]), &store).await;
        assert_eq!(cmd_ttl(&args(&["TTL", "k"]), &store).await, b":0\r\n");
    }

    #[tokio::test]
    async fn ttl_wrong_args_returns_error() {
        let store = make_store();
        let resp = cmd_ttl(&args(&["TTL"]), &store).await;
        assert!(resp.starts_with(b"-ERR"));
    }

    // ── TOUCH ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn touch_missing_key_returns_null() {
        let store = make_store();
        assert_eq!(cmd_touch(&args(&["TOUCH", "nope"]), &store).await, b"$-1\r\n");
    }

    #[tokio::test]
    async fn touch_resets_hits_to_zero_and_returns_0() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v"]), &store).await;
        cmd_get(&args(&["GET", "k"]), &store).await;
        cmd_get(&args(&["GET", "k"]), &store).await;
        assert_eq!(store.read().await.entries.get("k").unwrap().hits, 2);

        assert_eq!(cmd_touch(&args(&["TOUCH", "k"]), &store).await, b":0\r\n");
        assert_eq!(store.read().await.entries.get("k").unwrap().hits, 0);
    }

    #[tokio::test]
    async fn touch_wrong_args_returns_error() {
        let store = make_store();
        let resp = cmd_touch(&args(&["TOUCH"]), &store).await;
        assert!(resp.starts_with(b"-ERR"));
    }

    // ── Expiry ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn get_returns_null_after_ttl_elapses() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v", "PX", "50"]), &store).await;
        assert_eq!(cmd_get(&args(&["GET", "k"]), &store).await, b"$1\r\nv\r\n");

        tokio::time::sleep(Duration::from_millis(100)).await;
        // Lazy expiry via GET
        assert_eq!(cmd_get(&args(&["GET", "k"]), &store).await, b"$-1\r\n");
    }

    #[tokio::test]
    async fn background_task_removes_key_after_ttl() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v", "PX", "50"]), &store).await;

        tokio::time::sleep(Duration::from_millis(150)).await;
        // Background expiry task should have already removed it
        assert!(store.read().await.entries.get("k").is_none());
    }

    #[tokio::test]
    async fn ttl_returns_null_for_expired_but_not_yet_collected_key() {
        // TTL does not do lazy expiry, but the key should still be found
        // until the background task or GET removes it.
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v", "PX", "50"]), &store).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        // After background task fires the key is gone → null
        assert_eq!(cmd_ttl(&args(&["TTL", "k"]), &store).await, b"$-1\r\n");
    }

    #[tokio::test]
    async fn overwrite_with_no_ttl_blocks_old_expiry_task() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v1", "PX", "50"]), &store).await;
        // Immediately overwrite with a key that has no TTL
        cmd_set(&args(&["SET", "k", "v2"]), &store).await;

        // Wait longer than the original TTL
        tokio::time::sleep(Duration::from_millis(150)).await;
        tokio::task::yield_now().await;

        // Key should still exist because the expiry task saw a different expiry
        assert_eq!(cmd_get(&args(&["GET", "k"]), &store).await, b"$2\r\nv2\r\n");
    }

    #[tokio::test]
    async fn overwrite_with_new_ttl_blocks_old_expiry_task() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v1", "PX", "50"]), &store).await;
        // Overwrite with a longer TTL
        cmd_set(&args(&["SET", "k", "v2", "EX", "3600"]), &store).await;

        tokio::time::sleep(Duration::from_millis(150)).await;
        tokio::task::yield_now().await;

        // Old expiry task should not have deleted the re-set key
        assert_eq!(cmd_get(&args(&["GET", "k"]), &store).await, b"$2\r\nv2\r\n");
    }

    // ── Memory limit ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn set_within_limit_succeeds() {
        let store = make_store();
        let resp = cmd_set(&args(&["SET", "k", "v"]), &store).await;
        assert_eq!(resp, b"+OK\r\n");
        assert!(store.read().await.used_bytes > 0);
    }

    #[tokio::test]
    async fn set_refused_when_limit_exceeded() {
        let store = Arc::new(RwLock::new(Db::new(1)));
        let resp = cmd_set(&args(&["SET", "k", "hello"]), &store).await;
        assert!(resp.starts_with(b"-ERR"));
        assert!(resp.windows(3).any(|w| w == b"OOM"));
    }

    #[tokio::test]
    async fn set_overwrite_same_size_does_not_exceed_limit() {
        // limit = key("k").len() + value("v").len() = 1 + 1 = 2
        let store = Arc::new(RwLock::new(Db::new(2)));
        assert_eq!(cmd_set(&args(&["SET", "k", "v"]), &store).await, b"+OK\r\n");
        assert_eq!(cmd_set(&args(&["SET", "k", "w"]), &store).await, b"+OK\r\n");
    }

    #[tokio::test]
    async fn used_bytes_decremented_on_del() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v"]), &store).await;
        cmd_del(&args(&["DEL", "k"]), &store).await;
        assert_eq!(store.read().await.used_bytes, 0);
    }

    #[tokio::test]
    async fn used_bytes_decremented_on_expiry() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v", "PX", "50"]), &store).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(store.read().await.used_bytes, 0);
    }

    #[tokio::test]
    async fn used_bytes_accounting_for_overwrite_larger_value() {
        let store = make_store();
        // "k" + "v" = 1 + 1 = 2
        cmd_set(&args(&["SET", "k", "v"]), &store).await;
        // "k" + "longer" = 1 + 6 = 7
        cmd_set(&args(&["SET", "k", "longer"]), &store).await;
        assert_eq!(store.read().await.used_bytes, 7);
    }

    // ── INCR ──────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn incr_missing_key_initialises_to_1() {
        let store = make_store();
        assert_eq!(cmd_incr(&args(&["INCR", "n"]), &store).await, b":1\r\n");
    }

    #[tokio::test]
    async fn incr_existing_integer_key() {
        let store = make_store();
        cmd_set(&args(&["SET", "n", "41"]), &store).await;
        assert_eq!(cmd_incr(&args(&["INCR", "n"]), &store).await, b":42\r\n");
    }

    #[tokio::test]
    async fn incr_multiple_times() {
        let store = make_store();
        cmd_incr(&args(&["INCR", "n"]), &store).await;
        cmd_incr(&args(&["INCR", "n"]), &store).await;
        assert_eq!(cmd_incr(&args(&["INCR", "n"]), &store).await, b":3\r\n");
    }

    #[tokio::test]
    async fn incr_non_integer_value_returns_error() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "notanumber"]), &store).await;
        let resp = cmd_incr(&args(&["INCR", "k"]), &store).await;
        assert!(resp.starts_with(b"-ERR"));
    }

    #[tokio::test]
    async fn incr_wrong_args_returns_error() {
        let store = make_store();
        let resp = cmd_incr(&args(&["INCR"]), &store).await;
        assert!(resp.starts_with(b"-ERR"));
    }

    #[tokio::test]
    async fn incr_updates_used_bytes() {
        let store = make_store();
        cmd_incr(&args(&["INCR", "n"]), &store).await; // value "1", key "n" → 2 bytes
        assert_eq!(store.read().await.used_bytes, 2);
    }

    #[tokio::test]
    async fn incr_result_readable_via_get() {
        let store = make_store();
        cmd_incr(&args(&["INCR", "n"]), &store).await;
        assert_eq!(cmd_get(&args(&["GET", "n"]), &store).await, b"$1\r\n1\r\n");
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config = config::Config::from_env();

    let metrics_addr: SocketAddr = config
        .metrics_listen_addr()
        .parse()
        .expect("invalid metrics listen address");
    PrometheusBuilder::new()
        .with_http_listener(metrics_addr)
        .install()
        .expect("failed to install Prometheus exporter");

    metrics::describe_gauge!("kvns_keys_total", "Number of keys in the store");
    metrics::describe_gauge!("kvns_memory_used_bytes", "Memory currently used by the store in bytes");
    metrics::describe_gauge!("kvns_memory_limit_bytes", "Configured memory limit in bytes");

    let store: Store = Arc::new(RwLock::new(Db::new(config.memory_limit)));
    let addr = config.listen_addr();
    let listener = TcpListener::bind(&addr).await.expect("failed to bind");
    info!(addr = %addr, "kvns listening");

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                debug!(%peer, "accepted connection");
                tokio::spawn(handle_connection(stream, Arc::clone(&store)));
            }
            Err(e) => error!(?e, "accept error"),
        }
    }
}
