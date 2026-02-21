use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, warn};

use crate::resp::{
    resp_array, resp_bulk, resp_err, resp_int, resp_null, resp_ok, resp_pong, resp_wrongtype,
    wrong_args,
};
use crate::store::{Db, Entry, Store, Value};

/// Split a raw key into `(namespace, local_key)`.
///
/// Keys of the form `"namespace/local"` route to that namespace; keys with no
/// `/` use the empty-string default namespace so that unqualified keys continue
/// to work exactly as before.  Only the **first** `/` is used as the separator,
/// so local keys may themselves contain slashes.
fn parse_ns_key(raw: &[u8]) -> (String, String) {
    let s = String::from_utf8_lossy(raw);
    match s.find('/') {
        Some(pos) => (s[..pos].to_owned(), s[pos + 1..].to_owned()),
        None => ("default".to_owned(), s.into_owned()),
    }
}

pub(crate) async fn cmd_set(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    let start = Instant::now();
    let ns = if args.len() >= 2 { parse_ns_key(&args[1]).0 } else { "default".to_owned() };
    let resp = cmd_set_inner(args, store).await;
    metrics::histogram!("kvns_command_duration_seconds", "command" => "set", "namespace" => ns)
        .record(start.elapsed().as_secs_f64());
    resp
}

async fn cmd_set_inner(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 3 && args.len() != 5 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
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
    if db.would_exceed(&ns, &key, value.len()) {
        warn!(namespace = %ns, key = %key, "SET rejected: memory limit exceeded");
        return resp_err("OOM command not allowed when used memory > 'maxmemory'");
    }
    let entry = Entry::new(value, ttl);
    let expiry = entry.expiry;
    debug!(namespace = %ns, key = %key, ttl = ?ttl, "SET");
    db.put(ns.clone(), key.clone(), entry);
    drop(db);

    if let Some(deadline) = expiry {
        let store = Arc::clone(store);
        tokio::spawn(async move {
            let now = Instant::now();
            if deadline > now {
                tokio::time::sleep(deadline - now).await;
            }
            let mut db = store.write().await;
            if db
                .entries
                .get(&ns)
                .and_then(|nsm| nsm.get(&key))
                .is_some_and(|e| e.expiry == Some(deadline))
            {
                debug!(namespace = %ns, key = %key, "expiring key");
                db.delete(&ns, &key);
            }
        });
    }

    resp_ok()
}

pub(crate) async fn cmd_get(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    let start = Instant::now();
    let ns = if args.len() >= 2 { parse_ns_key(&args[1]).0 } else { "default".to_owned() };
    let resp = cmd_get_inner(args, store).await;
    metrics::histogram!("kvns_command_duration_seconds", "command" => "get", "namespace" => ns)
        .record(start.elapsed().as_secs_f64());
    resp
}

async fn cmd_get_inner(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;

    // Lazy expiry: remove on access if the entry has already expired.
    if db
        .entries
        .get(&ns)
        .and_then(|nsm| nsm.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_null();
    }

    match db
        .entries
        .get_mut(&ns)
        .and_then(|nsm| nsm.get_mut(&key))
    {
        None => resp_null(),
        Some(entry) => match entry.value.as_string() {
            None => resp_wrongtype(),
            Some(bytes) => {
                entry.hits += 1;
                let value = bytes.to_vec();
                debug!(namespace = %ns, key = %key, hits = entry.hits, "GET");
                resp_bulk(&value)
            }
        },
    }
}

pub(crate) async fn cmd_del(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let removed = store.write().await.delete(&ns, &key).is_some();
    debug!(namespace = %ns, key = %key, removed, "DEL");
    resp_int(if removed { 1 } else { 0 })
}

pub(crate) async fn cmd_ttl(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    match store
        .read()
        .await
        .entries
        .get(&ns)
        .and_then(|nsm| nsm.get(&key))
    {
        None => resp_null(),
        Some(entry) => resp_int(entry.time_to_expiry_secs()),
    }
}

pub(crate) async fn cmd_touch(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    match db
        .entries
        .get_mut(&ns)
        .and_then(|nsm| nsm.get_mut(&key))
    {
        None => resp_null(),
        Some(entry) => {
            entry.hits = 0;
            debug!(namespace = %ns, key = %key, "TOUCH");
            resp_int(entry.hits as i64)
        }
    }
}

pub(crate) async fn cmd_incr(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;

    let current: i64 = match db.entries.get(&ns).and_then(|nsm| nsm.get(&key)) {
        None => 0,
        Some(entry) => match entry.value.as_string() {
            None => return resp_wrongtype(),
            Some(bytes) => match std::str::from_utf8(bytes).ok().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return resp_err("value is not an integer or out of range"),
            },
        },
    };

    let next = match current.checked_add(1) {
        Some(n) => n,
        None => return resp_err("increment would overflow"),
    };

    let new_value = next.to_string().into_bytes();
    if db.would_exceed(&ns, &key, new_value.len()) {
        return resp_err("OOM command not allowed when used memory > 'maxmemory'");
    }
    debug!(namespace = %ns, key = %key, value = next, "INCR");
    db.put(ns, key, Entry::new(new_value, None));
    resp_int(next)
}

pub(crate) async fn cmd_lpush(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let new_items = &args[2..];
    let mut db = store.write().await;

    // One lookup: type-check and capture existing byte length together to
    // avoid re-scanning the list in the OOM check.
    let (existing_byte_len, is_new_key) =
        match db.entries.get(&ns).and_then(|nsm| nsm.get(&key)) {
            None => (0usize, true),
            Some(e) => match &e.value {
                Value::List(l) => (l.iter().map(|v| v.len()).sum(), false),
                Value::String(_) => return resp_wrongtype(),
            },
        };

    let added_byte_len: usize = new_items.iter().map(|v| v.len()).sum();

    // Inline OOM check using pre-computed lengths — avoids a second
    // O(N) list scan that would_exceed() would perform.
    let old_size = if is_new_key { 0 } else { Db::entry_size(&ns, &key, existing_byte_len) };
    let new_size = Db::entry_size(&ns, &key, existing_byte_len + added_byte_len);
    let net_delta = new_size.saturating_sub(old_size);
    if db.used_bytes.saturating_add(net_delta) > db.memory_limit {
        return resp_err("OOM command not allowed when used memory > 'maxmemory'");
    }

    // Mutate the list in place — no clone of the existing contents.
    let entry = db
        .entries
        .entry(ns.clone())
        .or_default()
        .entry(key.clone())
        .or_insert_with(|| Entry {
            value: Value::List(VecDeque::new()),
            hits: 0,
            expiry: None,
        });
    let list = match &mut entry.value {
        Value::List(l) => l,
        Value::String(_) => unreachable!(),
    };
    for item in new_items.iter() {
        list.push_front(item.clone());
    }
    let len = list.len();

    db.used_bytes = db.used_bytes.saturating_add(net_delta);
    let nb = db.namespace_bytes.entry(ns.clone()).or_insert(0);
    *nb = nb.saturating_add(net_delta);
    let ns_bytes = *nb;
    let ns_keys = db.entries.get(&ns).map(|nsm| nsm.len()).unwrap_or(0);
    metrics::gauge!("kvns_keys_total", "namespace" => ns.clone()).set(ns_keys as f64);
    metrics::gauge!("kvns_memory_used_bytes", "namespace" => ns.clone()).set(ns_bytes as f64);
    metrics::gauge!("kvns_memory_used_bytes_total").set(db.used_bytes as f64);
    debug!(namespace = %ns, key = %key, len, "LPUSH");
    resp_int(len as i64)
}

// ── Glob matching ─────────────────────────────────────────────────────────────

/// Returns true if `ch` is within the bracket-expression `class` (the content
/// between `[` and `]`, without the enclosing brackets).
/// Supports negation (`^` or `!`), literal characters, and ranges (`a-z`).
fn class_match(class: &[u8], ch: u8) -> bool {
    let (negate, class) = match class.first() {
        Some(b'^') | Some(b'!') => (true, &class[1..]),
        _ => (false, class),
    };
    let mut i = 0;
    let mut found = false;
    while i < class.len() {
        if i + 2 < class.len() && class[i + 1] == b'-' {
            if ch >= class[i] && ch <= class[i + 2] {
                found = true;
            }
            i += 3;
        } else {
            if ch == class[i] {
                found = true;
            }
            i += 1;
        }
    }
    if negate { !found } else { found }
}

/// Redis-compatible glob match supporting `*`, `?`, and `[...]` / `[^...]`.
fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    match (pattern, text) {
        ([], []) => true,
        ([], _) => false,
        ([b'*', rest @ ..], _) => {
            // `*` matches zero or more characters.
            glob_match(rest, text)
                || (!text.is_empty() && glob_match(pattern, &text[1..]))
        }
        (_, []) => false,
        ([b'?', p_rest @ ..], [_, t_rest @ ..]) => glob_match(p_rest, t_rest),
        ([b'[', p_rest @ ..], [ch, t_rest @ ..]) => {
            match p_rest.iter().position(|&b| b == b']') {
                // No closing `]` — treat `[` as a literal character.
                None => *ch == b'[' && glob_match(p_rest, t_rest),
                Some(end) => {
                    class_match(&p_rest[..end], *ch)
                        && glob_match(&p_rest[end + 1..], t_rest)
                }
            }
        }
        ([p, p_rest @ ..], [t, t_rest @ ..]) => *p == *t && glob_match(p_rest, t_rest),
    }
}

pub(crate) async fn cmd_keys(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let pattern = &args[1];
    let db = store.read().await;

    let mut matched: Vec<Vec<u8>> = Vec::new();
    for (ns, ns_map) in &db.entries {
        for (key, entry) in ns_map {
            if entry.is_expired() {
                continue;
            }
            // Reconstruct the client-visible key: bare for "default", prefixed for others.
            let display: Vec<u8> = if ns == "default" {
                key.as_bytes().to_vec()
            } else {
                format!("{ns}/{key}").into_bytes()
            };
            if glob_match(pattern, &display) {
                matched.push(display);
            }
        }
    }
    matched.sort();
    resp_array(&matched)
}

pub(crate) async fn dispatch(args: &[Vec<u8>], store: &Store) -> (Vec<u8>, bool) {
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
        "lpush" => cmd_lpush(args, store).await,
        "keys"  => cmd_keys(args, store).await,
        _ => format!(
            "-ERR unknown command {}\r\n",
            String::from_utf8_lossy(&args[0])
        )
        .into_bytes(),
    };
    (resp, false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config;
    use crate::store::{Db, Value};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::RwLock;

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

    // ── Namespace parsing ─────────────────────────────────────────────────────

    #[test]
    fn parse_ns_key_with_slash() {
        let (ns, key) = parse_ns_key(b"db1/foo");
        assert_eq!(ns, "db1");
        assert_eq!(key, "foo");
    }

    #[test]
    fn parse_ns_key_without_slash_uses_default() {
        let (ns, key) = parse_ns_key(b"foo");
        assert_eq!(ns, "default");
        assert_eq!(key, "foo");
    }

    #[test]
    fn parse_ns_key_splits_on_first_slash_only() {
        let (ns, key) = parse_ns_key(b"db1/sub/x");
        assert_eq!(ns, "db1");
        assert_eq!(key, "sub/x");
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
        assert_eq!(
            store.read().await.entries.get("default").and_then(|ns| ns.get("k")).unwrap().hits,
            3
        );
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
        assert_eq!(
            store.read().await.entries.get("default").and_then(|ns| ns.get("k")).unwrap().hits,
            2
        );

        assert_eq!(cmd_touch(&args(&["TOUCH", "k"]), &store).await, b":0\r\n");
        assert_eq!(
            store.read().await.entries.get("default").and_then(|ns| ns.get("k")).unwrap().hits,
            0
        );
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
        assert!(store.read().await.entries.get("default").and_then(|ns| ns.get("k")).is_none());
    }

    #[tokio::test]
    async fn ttl_returns_null_for_expired_but_not_yet_collected_key() {
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
        // "default"(7) + "k"(1) + "v"(1) = 9
        let store = Arc::new(RwLock::new(Db::new(9)));
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
        // "default"(7) + "k"(1) + "v"(1) = 9
        cmd_set(&args(&["SET", "k", "v"]), &store).await;
        assert_eq!(store.read().await.used_bytes, 9);
        // "default"(7) + "k"(1) + "longer"(6) = 14
        cmd_set(&args(&["SET", "k", "longer"]), &store).await;
        assert_eq!(store.read().await.used_bytes, 14);
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
        // "default"(7) + "n"(1) + "1"(1) = 9
        cmd_incr(&args(&["INCR", "n"]), &store).await;
        assert_eq!(store.read().await.used_bytes, 9);
    }

    #[tokio::test]
    async fn incr_result_readable_via_get() {
        let store = make_store();
        cmd_incr(&args(&["INCR", "n"]), &store).await;
        assert_eq!(cmd_get(&args(&["GET", "n"]), &store).await, b"$1\r\n1\r\n");
    }

    #[tokio::test]
    async fn incr_on_list_key_returns_wrongtype() {
        let store = make_store();
        cmd_lpush(&args(&["LPUSH", "l", "a"]), &store).await;
        let resp = cmd_incr(&args(&["INCR", "l"]), &store).await;
        assert!(resp.starts_with(b"-WRONGTYPE"));
    }

    // ── LPUSH ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn lpush_missing_key_creates_list_and_returns_1() {
        let store = make_store();
        assert_eq!(cmd_lpush(&args(&["LPUSH", "l", "a"]), &store).await, b":1\r\n");
    }

    #[tokio::test]
    async fn lpush_prepends_to_existing_list() {
        let store = make_store();
        cmd_lpush(&args(&["LPUSH", "l", "a"]), &store).await;
        assert_eq!(cmd_lpush(&args(&["LPUSH", "l", "b"]), &store).await, b":2\r\n");
    }

    #[tokio::test]
    async fn lpush_multiple_values_prepended_in_order() {
        // LPUSH l a b c → list becomes [c, b, a]
        let store = make_store();
        cmd_lpush(&args(&["LPUSH", "l", "a", "b", "c"]), &store).await;
        let db = store.read().await;
        let list = match &db.entries.get("default").unwrap().get("l").unwrap().value {
            Value::List(l) => l.iter().map(|v| v.as_slice()).collect::<Vec<_>>(),
            _ => panic!("expected list"),
        };
        assert_eq!(list, vec![b"c" as &[u8], b"b", b"a"]);
    }

    #[tokio::test]
    async fn lpush_returns_length_after_each_push() {
        let store = make_store();
        assert_eq!(cmd_lpush(&args(&["LPUSH", "l", "x"]), &store).await, b":1\r\n");
        assert_eq!(cmd_lpush(&args(&["LPUSH", "l", "y"]), &store).await, b":2\r\n");
        assert_eq!(cmd_lpush(&args(&["LPUSH", "l", "z"]), &store).await, b":3\r\n");
    }

    #[tokio::test]
    async fn lpush_wrong_args_returns_error() {
        let store = make_store();
        let resp = cmd_lpush(&args(&["LPUSH", "l"]), &store).await;
        assert!(resp.starts_with(b"-ERR"));
    }

    #[tokio::test]
    async fn lpush_on_string_key_returns_wrongtype() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v"]), &store).await;
        let resp = cmd_lpush(&args(&["LPUSH", "k", "a"]), &store).await;
        assert!(resp.starts_with(b"-WRONGTYPE"));
    }

    #[tokio::test]
    async fn get_on_list_key_returns_wrongtype() {
        let store = make_store();
        cmd_lpush(&args(&["LPUSH", "l", "a"]), &store).await;
        let resp = cmd_get(&args(&["GET", "l"]), &store).await;
        assert!(resp.starts_with(b"-WRONGTYPE"));
    }

    #[tokio::test]
    async fn lpush_updates_used_bytes() {
        let store = make_store();
        // "default"(7) + "l"(1) + "ab"(2) = 10
        cmd_lpush(&args(&["LPUSH", "l", "ab"]), &store).await;
        assert_eq!(store.read().await.used_bytes, 10);
        // add "cd"(2) → "default"(7) + "l"(1) + "ab"(2) + "cd"(2) = 12
        cmd_lpush(&args(&["LPUSH", "l", "cd"]), &store).await;
        assert_eq!(store.read().await.used_bytes, 12);
    }

    #[tokio::test]
    async fn del_removes_list_key() {
        let store = make_store();
        cmd_lpush(&args(&["LPUSH", "l", "a"]), &store).await;
        assert_eq!(cmd_del(&args(&["DEL", "l"]), &store).await, b":1\r\n");
        assert_eq!(store.read().await.used_bytes, 0);
    }

    // ── Namespace isolation ───────────────────────────────────────────────────

    #[tokio::test]
    async fn namespaced_set_and_get_roundtrip() {
        let store = make_store();
        cmd_set(&args(&["SET", "ns1/k", "v1"]), &store).await;
        assert_eq!(cmd_get(&args(&["GET", "ns1/k"]), &store).await, b"$2\r\nv1\r\n");
    }

    #[tokio::test]
    async fn same_local_key_in_different_namespaces_are_independent() {
        let store = make_store();
        cmd_set(&args(&["SET", "ns1/k", "v1"]), &store).await;
        cmd_set(&args(&["SET", "ns2/k", "v2"]), &store).await;
        assert_eq!(cmd_get(&args(&["GET", "ns1/k"]), &store).await, b"$2\r\nv1\r\n");
        assert_eq!(cmd_get(&args(&["GET", "ns2/k"]), &store).await, b"$2\r\nv2\r\n");
    }

    #[tokio::test]
    async fn namespaced_key_does_not_shadow_unnamespaced_key() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "plain"]), &store).await;
        cmd_set(&args(&["SET", "ns/k", "namespaced"]), &store).await;
        assert_eq!(cmd_get(&args(&["GET", "k"]), &store).await, b"$5\r\nplain\r\n");
        assert_eq!(cmd_get(&args(&["GET", "ns/k"]), &store).await, b"$10\r\nnamespaced\r\n");
    }

    #[tokio::test]
    async fn del_only_removes_key_in_specified_namespace() {
        let store = make_store();
        cmd_set(&args(&["SET", "ns1/k", "v1"]), &store).await;
        cmd_set(&args(&["SET", "ns2/k", "v2"]), &store).await;
        cmd_del(&args(&["DEL", "ns1/k"]), &store).await;
        assert_eq!(cmd_get(&args(&["GET", "ns1/k"]), &store).await, b"$-1\r\n");
        assert_eq!(cmd_get(&args(&["GET", "ns2/k"]), &store).await, b"$2\r\nv2\r\n");
    }

    #[tokio::test]
    async fn namespaced_incr_counters_are_isolated() {
        let store = make_store();
        cmd_incr(&args(&["INCR", "ns1/counter"]), &store).await;
        cmd_incr(&args(&["INCR", "ns1/counter"]), &store).await;
        cmd_incr(&args(&["INCR", "ns2/counter"]), &store).await;
        assert_eq!(cmd_incr(&args(&["INCR", "ns1/counter"]), &store).await, b":3\r\n");
        assert_eq!(cmd_incr(&args(&["INCR", "ns2/counter"]), &store).await, b":2\r\n");
    }

    #[tokio::test]
    async fn namespaced_lpush_lists_are_isolated() {
        let store = make_store();
        cmd_lpush(&args(&["LPUSH", "ns1/list", "a"]), &store).await;
        cmd_lpush(&args(&["LPUSH", "ns2/list", "b"]), &store).await;
        assert_eq!(cmd_lpush(&args(&["LPUSH", "ns1/list", "c"]), &store).await, b":2\r\n");
        assert_eq!(cmd_lpush(&args(&["LPUSH", "ns2/list", "d"]), &store).await, b":2\r\n");
    }

    #[tokio::test]
    async fn namespaced_ttl_is_per_namespace() {
        let store = make_store();
        cmd_set(&args(&["SET", "ns1/k", "v", "EX", "100"]), &store).await;
        cmd_set(&args(&["SET", "ns2/k", "v"]), &store).await;
        let secs = parse_int_resp(&cmd_ttl(&args(&["TTL", "ns1/k"]), &store).await);
        assert!(secs > 0, "ns1/k should have TTL");
        assert_eq!(cmd_ttl(&args(&["TTL", "ns2/k"]), &store).await, b":0\r\n");
    }

    #[tokio::test]
    async fn del_last_key_in_namespace_cleans_up_namespace_map() {
        let store = make_store();
        cmd_set(&args(&["SET", "ns/k", "v"]), &store).await;
        cmd_del(&args(&["DEL", "ns/k"]), &store).await;
        // The namespace entry itself should be gone
        assert!(store.read().await.entries.get("ns").is_none());
    }

    #[tokio::test]
    async fn namespaced_memory_accounting() {
        let store = make_store();
        // "ns" (2) + "k" (1) + "v" (1) = 4
        cmd_set(&args(&["SET", "ns/k", "v"]), &store).await;
        assert_eq!(store.read().await.used_bytes, 4);
    }

    // ── glob_match unit tests ─────────────────────────────────────────────────

    #[test]
    fn glob_star_matches_anything() {
        assert!(glob_match(b"*", b"hello"));
        assert!(glob_match(b"*", b""));
        assert!(glob_match(b"h*", b"hello"));
        assert!(glob_match(b"*o", b"hello"));
        assert!(glob_match(b"h*o", b"hello"));
        assert!(!glob_match(b"h*x", b"hello"));
    }

    #[test]
    fn glob_question_mark_matches_one_char() {
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(glob_match(b"h?llo", b"hallo"));
        assert!(!glob_match(b"h?llo", b"hllo"));
        assert!(!glob_match(b"h?llo", b"heello"));
    }

    #[test]
    fn glob_bracket_class() {
        assert!(glob_match(b"h[ae]llo", b"hello"));
        assert!(glob_match(b"h[ae]llo", b"hallo"));
        assert!(!glob_match(b"h[ae]llo", b"hillo"));
    }

    #[test]
    fn glob_bracket_negate() {
        assert!(glob_match(b"h[^e]llo", b"hallo"));
        assert!(!glob_match(b"h[^e]llo", b"hello"));
        assert!(glob_match(b"h[!e]llo", b"hbllo"));
    }

    #[test]
    fn glob_bracket_range() {
        assert!(glob_match(b"h[a-b]llo", b"hallo"));
        assert!(glob_match(b"h[a-b]llo", b"hbllo"));
        assert!(!glob_match(b"h[a-b]llo", b"hcllo"));
    }

    // ── KEYS command tests ────────────────────────────────────────────────────

    fn parse_keys_resp(resp: &[u8]) -> Vec<String> {
        // Decode a RESP array of bulk strings into a sorted Vec<String>.
        let s = std::str::from_utf8(resp).unwrap();
        let mut lines = s.split("\r\n");
        let header = lines.next().unwrap();
        assert!(header.starts_with('*'));
        let count: usize = header[1..].parse().unwrap();
        let mut keys = Vec::with_capacity(count);
        for _ in 0..count {
            let len_line = lines.next().unwrap();
            assert!(len_line.starts_with('$'));
            keys.push(lines.next().unwrap().to_string());
        }
        keys.sort();
        keys
    }

    #[tokio::test]
    async fn keys_wildcard_returns_all_keys() {
        let store = make_store();
        cmd_set(&args(&["SET", "foo", "1"]), &store).await;
        cmd_set(&args(&["SET", "bar", "2"]), &store).await;
        let resp = cmd_keys(&args(&["KEYS", "*"]), &store).await;
        let keys = parse_keys_resp(&resp);
        assert_eq!(keys, vec!["bar", "foo"]);
    }

    #[tokio::test]
    async fn keys_empty_store_returns_empty_array() {
        let store = make_store();
        assert_eq!(cmd_keys(&args(&["KEYS", "*"]), &store).await, b"*0\r\n");
    }

    #[tokio::test]
    async fn keys_prefix_pattern() {
        let store = make_store();
        cmd_set(&args(&["SET", "foo", "1"]), &store).await;
        cmd_set(&args(&["SET", "foobar", "2"]), &store).await;
        cmd_set(&args(&["SET", "baz", "3"]), &store).await;
        let keys = parse_keys_resp(&cmd_keys(&args(&["KEYS", "foo*"]), &store).await);
        assert_eq!(keys, vec!["foo", "foobar"]);
    }

    #[tokio::test]
    async fn keys_question_mark_pattern() {
        let store = make_store();
        cmd_set(&args(&["SET", "hello", "1"]), &store).await;
        cmd_set(&args(&["SET", "hallo", "2"]), &store).await;
        cmd_set(&args(&["SET", "hillo", "3"]), &store).await;
        cmd_set(&args(&["SET", "world", "4"]), &store).await;
        let keys = parse_keys_resp(&cmd_keys(&args(&["KEYS", "h?llo"]), &store).await);
        assert_eq!(keys, vec!["hallo", "hello", "hillo"]);
    }

    #[tokio::test]
    async fn keys_bracket_pattern() {
        let store = make_store();
        cmd_set(&args(&["SET", "hello", "1"]), &store).await;
        cmd_set(&args(&["SET", "hallo", "2"]), &store).await;
        cmd_set(&args(&["SET", "hillo", "3"]), &store).await;
        let keys = parse_keys_resp(&cmd_keys(&args(&["KEYS", "h[ae]llo"]), &store).await);
        assert_eq!(keys, vec!["hallo", "hello"]);
    }

    #[tokio::test]
    async fn keys_no_match_returns_empty_array() {
        let store = make_store();
        cmd_set(&args(&["SET", "foo", "1"]), &store).await;
        assert_eq!(cmd_keys(&args(&["KEYS", "z*"]), &store).await, b"*0\r\n");
    }

    #[tokio::test]
    async fn keys_skips_expired_entries() {
        let store = make_store();
        cmd_set(&args(&["SET", "live", "1"]), &store).await;
        cmd_set(&args(&["SET", "dying", "2", "PX", "1"]), &store).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        let keys = parse_keys_resp(&cmd_keys(&args(&["KEYS", "*"]), &store).await);
        assert_eq!(keys, vec!["live"]);
    }

    #[tokio::test]
    async fn keys_namespaced_wildcard() {
        let store = make_store();
        cmd_set(&args(&["SET", "ns1/a", "1"]), &store).await;
        cmd_set(&args(&["SET", "ns1/b", "2"]), &store).await;
        cmd_set(&args(&["SET", "ns2/c", "3"]), &store).await;
        cmd_set(&args(&["SET", "plain", "4"]), &store).await;
        let all = parse_keys_resp(&cmd_keys(&args(&["KEYS", "*"]), &store).await);
        assert!(all.contains(&"ns1/a".to_string()));
        assert!(all.contains(&"ns1/b".to_string()));
        assert!(all.contains(&"ns2/c".to_string()));
        assert!(all.contains(&"plain".to_string()));
        let ns1 = parse_keys_resp(&cmd_keys(&args(&["KEYS", "ns1/*"]), &store).await);
        assert_eq!(ns1, vec!["ns1/a", "ns1/b"]);
    }

    #[tokio::test]
    async fn keys_wrong_args_returns_error() {
        let store = make_store();
        assert!(cmd_keys(&args(&["KEYS"]), &store).await.starts_with(b"-ERR"));
    }
}
