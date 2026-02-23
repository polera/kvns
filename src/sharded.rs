use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

use crate::resp::{resp_bulk, resp_err, resp_int, resp_null, resp_ok, resp_pong, wrong_args};

#[derive(Clone)]
struct ShardedEntry {
    value: Vec<u8>,
    expiry: Option<Instant>,
}

#[derive(Clone)]
struct ParsedKey {
    ns: String,
    key: String,
    canonical: String,
    shard_idx: usize,
}

pub(crate) struct ShardedDb {
    shards: Vec<RwLock<HashMap<String, ShardedEntry>>>,
    used_bytes: AtomicUsize,
    memory_limit: usize,
}

pub(crate) type ShardedStore = Arc<ShardedDb>;

impl ShardedDb {
    pub(crate) fn new(memory_limit: usize, shard_count: usize) -> ShardedStore {
        let count = shard_count.max(1);
        let mut shards = Vec::with_capacity(count);
        for _ in 0..count {
            shards.push(RwLock::new(HashMap::new()));
        }
        metrics::gauge!("kvns_memory_limit_bytes").set(memory_limit as f64);
        metrics::gauge!("kvns_memory_used_bytes_total").set(0.0);
        Arc::new(Self {
            shards,
            used_bytes: AtomicUsize::new(0),
            memory_limit,
        })
    }

    fn parse_ns_key(raw: &[u8]) -> (String, String) {
        let s = String::from_utf8_lossy(raw);
        match s.find('/') {
            Some(pos) => (s[..pos].to_owned(), s[pos + 1..].to_owned()),
            None => ("default".to_owned(), s.into_owned()),
        }
    }

    fn canonical_key(ns: &str, key: &str) -> String {
        let mut out = String::with_capacity(ns.len() + 1 + key.len());
        out.push_str(ns);
        out.push('/');
        out.push_str(key);
        out
    }

    fn entry_size(ns: &str, key: &str, value_len: usize) -> usize {
        ns.len() + key.len() + value_len
    }

    fn shard_idx(&self, ns: &str, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        ns.hash(&mut hasher);
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len()
    }

    fn parse_key(&self, raw_key: &[u8]) -> ParsedKey {
        let (ns, key) = Self::parse_ns_key(raw_key);
        ParsedKey {
            shard_idx: self.shard_idx(&ns, &key),
            canonical: Self::canonical_key(&ns, &key),
            ns,
            key,
        }
    }

    fn reserve_bytes(&self, extra_bytes: usize) -> bool {
        if extra_bytes == 0 {
            return true;
        }
        let mut current = self.used_bytes.load(Ordering::Relaxed);
        loop {
            if current.saturating_add(extra_bytes) > self.memory_limit {
                return false;
            }
            match self.used_bytes.compare_exchange_weak(
                current,
                current + extra_bytes,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }

    fn release_bytes(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let mut current = self.used_bytes.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_sub(bytes);
            match self.used_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    fn record_total_used(&self) {
        metrics::gauge!("kvns_memory_used_bytes_total")
            .set(self.used_bytes.load(Ordering::Relaxed) as f64);
    }

    fn purge_expired_locked(
        &self,
        shard: &mut HashMap<String, ShardedEntry>,
        parsed: &ParsedKey,
        now: Instant,
    ) {
        let expired = shard
            .get(&parsed.canonical)
            .is_some_and(|entry| entry.expiry.is_some_and(|deadline| now >= deadline));
        if expired {
            if let Some(entry) = shard.remove(&parsed.canonical) {
                let size = Self::entry_size(&parsed.ns, &parsed.key, entry.value.len());
                self.release_bytes(size);
            }
            self.record_total_used();
        }
    }

    async fn get_value(&self, raw_key: &[u8]) -> Option<Vec<u8>> {
        let parsed = self.parse_key(raw_key);
        let now = Instant::now();

        {
            let shard = self.shards[parsed.shard_idx].read().await;
            match shard.get(&parsed.canonical) {
                None => return None,
                Some(entry) if entry.expiry.is_some_and(|deadline| now >= deadline) => {}
                Some(entry) => return Some(entry.value.clone()),
            }
        }

        let mut shard = self.shards[parsed.shard_idx].write().await;
        self.purge_expired_locked(&mut shard, &parsed, now);
        shard
            .get(&parsed.canonical)
            .map(|entry| entry.value.clone())
    }

    async fn put_value(
        &self,
        raw_key: &[u8],
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), ()> {
        let parsed = self.parse_key(raw_key);
        let now = Instant::now();

        let mut shard = self.shards[parsed.shard_idx].write().await;
        self.purge_expired_locked(&mut shard, &parsed, now);

        let old_size = shard
            .get(&parsed.canonical)
            .map(|entry| Self::entry_size(&parsed.ns, &parsed.key, entry.value.len()))
            .unwrap_or(0);
        let new_size = Self::entry_size(&parsed.ns, &parsed.key, value.len());

        if new_size > old_size {
            if !self.reserve_bytes(new_size - old_size) {
                return Err(());
            }
        } else {
            self.release_bytes(old_size - new_size);
        }

        let expiry = ttl.map(|duration| now + duration);
        shard.insert(parsed.canonical, ShardedEntry { value, expiry });
        self.record_total_used();
        Ok(())
    }

    async fn apply_integer_delta(
        &self,
        raw_key: &[u8],
        delta: i64,
        overflow_err: &'static str,
    ) -> Result<i64, Vec<u8>> {
        let parsed = self.parse_key(raw_key);
        let now = Instant::now();

        let mut shard = self.shards[parsed.shard_idx].write().await;
        self.purge_expired_locked(&mut shard, &parsed, now);

        let current = match shard.get(&parsed.canonical) {
            None => 0,
            Some(entry) => match std::str::from_utf8(&entry.value)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
            {
                Some(n) => n,
                None => return Err(resp_err("value is not an integer or out of range")),
            },
        };

        let next = match current.checked_add(delta) {
            Some(n) => n,
            None => return Err(resp_err(overflow_err)),
        };

        let new_value = next.to_string().into_bytes();
        let old_size = shard
            .get(&parsed.canonical)
            .map(|entry| Self::entry_size(&parsed.ns, &parsed.key, entry.value.len()))
            .unwrap_or(0);
        let new_size = Self::entry_size(&parsed.ns, &parsed.key, new_value.len());

        if new_size > old_size {
            if !self.reserve_bytes(new_size - old_size) {
                return Err(resp_err(
                    "OOM command not allowed when used memory > 'maxmemory'",
                ));
            }
        } else {
            self.release_bytes(old_size - new_size);
        }

        shard.insert(
            parsed.canonical,
            ShardedEntry {
                value: new_value,
                expiry: None,
            },
        );
        self.record_total_used();
        Ok(next)
    }

    /// Insert `value` only if the key is absent (or expired). Returns `Ok(true)` on
    /// success, `Ok(false)` if the key already exists, `Err(())` on OOM.
    async fn put_value_if_absent(&self, raw_key: &[u8], value: Vec<u8>) -> Result<bool, ()> {
        let parsed = self.parse_key(raw_key);
        let now = Instant::now();
        let mut shard = self.shards[parsed.shard_idx].write().await;
        self.purge_expired_locked(&mut shard, &parsed, now);
        if shard.contains_key(&parsed.canonical) {
            return Ok(false);
        }
        let new_size = Self::entry_size(&parsed.ns, &parsed.key, value.len());
        if !self.reserve_bytes(new_size) {
            return Err(());
        }
        shard.insert(parsed.canonical, ShardedEntry { value, expiry: None });
        self.record_total_used();
        Ok(true)
    }

    /// Append `suffix` to the existing value (defaulting to empty). Returns the
    /// new length on success, or `Err(bytes)` on OOM where `bytes` is an error response.
    async fn append_value(&self, raw_key: &[u8], suffix: &[u8]) -> Result<i64, Vec<u8>> {
        let parsed = self.parse_key(raw_key);
        let now = Instant::now();
        let mut shard = self.shards[parsed.shard_idx].write().await;
        self.purge_expired_locked(&mut shard, &parsed, now);
        let old_value = shard
            .get(&parsed.canonical)
            .map(|e| e.value.clone())
            .unwrap_or_default();
        let old_size = Self::entry_size(&parsed.ns, &parsed.key, old_value.len());
        let mut new_value = old_value;
        new_value.extend_from_slice(suffix);
        let new_size = Self::entry_size(&parsed.ns, &parsed.key, new_value.len());
        if new_size > old_size {
            if !self.reserve_bytes(new_size - old_size) {
                return Err(resp_err("OOM command not allowed when used memory > 'maxmemory'"));
            }
        } else {
            self.release_bytes(old_size - new_size);
        }
        let len = new_value.len() as i64;
        shard.insert(parsed.canonical, ShardedEntry { value: new_value, expiry: None });
        self.record_total_used();
        Ok(len)
    }

    /// Atomically replace the value, returning the old value (if any). Returns
    /// `Err(())` on OOM.
    async fn get_and_set(&self, raw_key: &[u8], new_value: Vec<u8>) -> Result<Option<Vec<u8>>, ()> {
        let parsed = self.parse_key(raw_key);
        let now = Instant::now();
        let mut shard = self.shards[parsed.shard_idx].write().await;
        self.purge_expired_locked(&mut shard, &parsed, now);
        let old = shard.get(&parsed.canonical).map(|e| e.value.clone());
        let old_size = old.as_ref()
            .map(|v| Self::entry_size(&parsed.ns, &parsed.key, v.len()))
            .unwrap_or(0);
        let new_size = Self::entry_size(&parsed.ns, &parsed.key, new_value.len());
        if new_size > old_size {
            if !self.reserve_bytes(new_size - old_size) {
                return Err(());
            }
        } else {
            self.release_bytes(old_size - new_size);
        }
        shard.insert(parsed.canonical, ShardedEntry { value: new_value, expiry: None });
        self.record_total_used();
        Ok(old)
    }

    /// Set all key-value pairs only if none of the keys already exist. Acquires
    /// shard write locks in index order to avoid deadlock. Returns `Ok(true)` on
    /// success, `Ok(false)` if any key exists, `Err(())` on OOM.
    async fn msetnx_atomic(&self, pairs: Vec<(ParsedKey, Vec<u8>)>) -> Result<bool, ()> {
        use std::collections::BTreeMap;
        let now = Instant::now();
        // Group pairs by shard index (BTreeMap → sorted order prevents deadlock).
        let mut by_shard: BTreeMap<usize, Vec<(ParsedKey, Vec<u8>)>> = BTreeMap::new();
        for (parsed, value) in pairs {
            by_shard.entry(parsed.shard_idx).or_default().push((parsed, value));
        }
        // Acquire all write locks in shard-index order.
        let mut guards: Vec<(usize, tokio::sync::RwLockWriteGuard<'_, HashMap<String, ShardedEntry>>)> =
            Vec::with_capacity(by_shard.len());
        for &shard_idx in by_shard.keys() {
            let guard = self.shards[shard_idx].write().await;
            guards.push((shard_idx, guard));
        }
        // First pass: purge expired + check existence.
        for (shard_idx, guard) in &mut guards {
            if let Some(shard_pairs) = by_shard.get(&*shard_idx) {
                let shard: &mut HashMap<String, ShardedEntry> = guard;
                for (parsed, _) in shard_pairs {
                    self.purge_expired_locked(shard, parsed, now);
                    if shard.contains_key(&parsed.canonical) {
                        return Ok(false);
                    }
                }
            }
        }
        // Second pass: reserve bytes + insert.
        for (shard_idx, guard) in &mut guards {
            if let Some(shard_pairs) = by_shard.get(&*shard_idx) {
                let shard: &mut HashMap<String, ShardedEntry> = guard;
                for (parsed, value) in shard_pairs {
                    let size = Self::entry_size(&parsed.ns, &parsed.key, value.len());
                    if !self.reserve_bytes(size) {
                        return Err(());
                    }
                    shard.insert(parsed.canonical.clone(), ShardedEntry { value: value.clone(), expiry: None });
                }
            }
        }
        self.record_total_used();
        Ok(true)
    }
}

fn set_ttl_from_args(args: &[Vec<u8>]) -> Result<Option<Duration>, Vec<u8>> {
    if args.len() == 3 {
        return Ok(None);
    }
    if args.len() != 5 {
        return Err(wrong_args(&args[0]));
    }
    let amount = match std::str::from_utf8(&args[4])
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    {
        Some(v) => v,
        None => return Err(resp_err("invalid expire time")),
    };
    if args[3].eq_ignore_ascii_case(b"PX") {
        Ok(Some(Duration::from_millis(amount)))
    } else if args[3].eq_ignore_ascii_case(b"EX") {
        Ok(Some(Duration::from_secs(amount)))
    } else {
        Err(resp_err("syntax error"))
    }
}

fn parse_i64_arg(raw: &[u8]) -> Result<i64, Vec<u8>> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or_else(|| resp_err("value is not an integer or out of range"))
}

pub(crate) async fn dispatch(args: &[Vec<u8>], store: &ShardedStore) -> (Vec<u8>, bool) {
    if args.is_empty() {
        return (resp_err("empty command"), false);
    }
    let cmd = args[0].as_slice();

    if cmd.eq_ignore_ascii_case(b"PING") {
        return (resp_pong(), false);
    }
    if cmd.eq_ignore_ascii_case(b"QUIT") {
        return (resp_ok(), true);
    }

    if cmd.eq_ignore_ascii_case(b"SET") {
        let ttl = match set_ttl_from_args(args) {
            Ok(v) => v,
            Err(err) => return (err, false),
        };
        let value = args[2].clone();
        let response = if store.put_value(&args[1], value, ttl).await.is_ok() {
            resp_ok()
        } else {
            resp_err("OOM command not allowed when used memory > 'maxmemory'")
        };
        return (response, false);
    }

    if cmd.eq_ignore_ascii_case(b"GET") {
        if args.len() != 2 {
            return (wrong_args(&args[0]), false);
        }
        let response = match store.get_value(&args[1]).await {
            Some(value) => resp_bulk(&value),
            None => resp_null(),
        };
        return (response, false);
    }

    if cmd.eq_ignore_ascii_case(b"MGET") {
        if args.len() < 2 {
            return (wrong_args(&args[0]), false);
        }
        let mut out = format!("*{}\r\n", args.len() - 1).into_bytes();
        for raw_key in &args[1..] {
            match store.get_value(raw_key).await {
                Some(value) => out.extend_from_slice(&resp_bulk(&value)),
                None => out.extend_from_slice(&resp_null()),
            }
        }
        return (out, false);
    }

    if cmd.eq_ignore_ascii_case(b"MSET") {
        if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
            return (wrong_args(&args[0]), false);
        }
        let mut i = 1usize;
        while i + 1 < args.len() {
            if store
                .put_value(&args[i], args[i + 1].clone(), None)
                .await
                .is_err()
            {
                return (
                    resp_err("OOM command not allowed when used memory > 'maxmemory'"),
                    false,
                );
            }
            i += 2;
        }
        return (resp_ok(), false);
    }

    if cmd.eq_ignore_ascii_case(b"MSETNX") {
        if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
            return (wrong_args(&args[0]), false);
        }
        let mut pairs: Vec<(ParsedKey, Vec<u8>)> = Vec::new();
        let mut i = 1usize;
        while i + 1 < args.len() {
            pairs.push((store.parse_key(&args[i]), args[i + 1].clone()));
            i += 2;
        }
        return match store.msetnx_atomic(pairs).await {
            Ok(true) => (resp_int(1), false),
            Ok(false) => (resp_int(0), false),
            Err(()) => (resp_err("OOM command not allowed when used memory > 'maxmemory'"), false),
        };
    }

    if cmd.eq_ignore_ascii_case(b"SETNX") {
        if args.len() != 3 {
            return (wrong_args(&args[0]), false);
        }
        return match store.put_value_if_absent(&args[1], args[2].clone()).await {
            Ok(true) => (resp_int(1), false),
            Ok(false) => (resp_int(0), false),
            Err(()) => (resp_err("OOM command not allowed when used memory > 'maxmemory'"), false),
        };
    }

    if cmd.eq_ignore_ascii_case(b"INCR") {
        if args.len() != 2 {
            return (wrong_args(&args[0]), false);
        }
        return match store
            .apply_integer_delta(&args[1], 1, "increment would overflow")
            .await
        {
            Ok(next) => (resp_int(next), false),
            Err(err) => (err, false),
        };
    }

    if cmd.eq_ignore_ascii_case(b"INCRBY") {
        if args.len() != 3 {
            return (wrong_args(&args[0]), false);
        }
        let by = match parse_i64_arg(&args[2]) {
            Ok(v) => v,
            Err(err) => return (err, false),
        };
        return match store
            .apply_integer_delta(&args[1], by, "increment would overflow")
            .await
        {
            Ok(next) => (resp_int(next), false),
            Err(err) => (err, false),
        };
    }

    if cmd.eq_ignore_ascii_case(b"DECR") {
        if args.len() != 2 {
            return (wrong_args(&args[0]), false);
        }
        return match store
            .apply_integer_delta(&args[1], -1, "decrement would overflow")
            .await
        {
            Ok(next) => (resp_int(next), false),
            Err(err) => (err, false),
        };
    }

    if cmd.eq_ignore_ascii_case(b"DECRBY") {
        if args.len() != 3 {
            return (wrong_args(&args[0]), false);
        }
        let by = match parse_i64_arg(&args[2]) {
            Ok(v) => v,
            Err(err) => return (err, false),
        };
        let delta = match by.checked_neg() {
            Some(v) => v,
            None => return (resp_err("decrement would overflow"), false),
        };
        return match store
            .apply_integer_delta(&args[1], delta, "decrement would overflow")
            .await
        {
            Ok(next) => (resp_int(next), false),
            Err(err) => (err, false),
        };
    }

    if cmd.eq_ignore_ascii_case(b"STRLEN") {
        if args.len() != 2 {
            return (wrong_args(&args[0]), false);
        }
        let response = match store.get_value(&args[1]).await {
            Some(value) => resp_int(value.len() as i64),
            None => resp_int(0),
        };
        return (response, false);
    }

    if cmd.eq_ignore_ascii_case(b"TYPE") {
        if args.len() != 2 {
            return (wrong_args(&args[0]), false);
        }
        let response = if store.get_value(&args[1]).await.is_some() {
            resp_bulk(b"string")
        } else {
            resp_bulk(b"none")
        };
        return (response, false);
    }

    if cmd.eq_ignore_ascii_case(b"APPEND") {
        if args.len() != 3 {
            return (wrong_args(&args[0]), false);
        }
        return match store.append_value(&args[1], &args[2]).await {
            Ok(len) => (resp_int(len), false),
            Err(err) => (err, false),
        };
    }

    if cmd.eq_ignore_ascii_case(b"GETSET") {
        if args.len() != 3 {
            return (wrong_args(&args[0]), false);
        }
        return match store.get_and_set(&args[1], args[2].clone()).await {
            Ok(Some(old)) => (resp_bulk(&old), false),
            Ok(None) => (resp_null(), false),
            Err(()) => (resp_err("OOM command not allowed when used memory > 'maxmemory'"), false),
        };
    }

    if cmd.eq_ignore_ascii_case(b"GETDEL") {
        if args.len() != 2 {
            return (wrong_args(&args[0]), false);
        }
        let parsed = store.parse_key(&args[1]);
        let now = Instant::now();
        let mut shard = store.shards[parsed.shard_idx].write().await;
        store.purge_expired_locked(&mut shard, &parsed, now);
        let removed = shard.remove(&parsed.canonical);
        if let Some(entry) = removed {
            let size = ShardedDb::entry_size(&parsed.ns, &parsed.key, entry.value.len());
            store.release_bytes(size);
            store.record_total_used();
            return (resp_bulk(&entry.value), false);
        }
        return (resp_null(), false);
    }

    if cmd.eq_ignore_ascii_case(b"GETEX") {
        if args.len() < 2 {
            return (wrong_args(&args[0]), false);
        }
        if args.len() > 2 {
            return (resp_err("command not supported in sharded mode"), false);
        }
        let response = match store.get_value(&args[1]).await {
            Some(value) => resp_bulk(&value),
            None => resp_null(),
        };
        return (response, false);
    }

    if cmd.eq_ignore_ascii_case(b"DBSIZE") {
        if args.len() != 1 {
            return (wrong_args(&args[0]), false);
        }
        let now = Instant::now();
        let mut total = 0usize;
        for shard_lock in &store.shards {
            let mut shard = shard_lock.write().await;
            let expired: Vec<String> = shard
                .iter()
                .filter_map(|(k, v)| {
                    if v.expiry.is_some_and(|deadline| now >= deadline) {
                        Some(k.clone())
                    } else {
                        None
                    }
                })
                .collect();
            for key in expired {
                if let Some(entry) = shard.remove(&key) {
                    let mut parts = key.splitn(2, '/');
                    let ns = parts.next().unwrap_or("default");
                    let local_key = parts.next().unwrap_or("");
                    let size = ShardedDb::entry_size(ns, local_key, entry.value.len());
                    store.release_bytes(size);
                }
            }
            total += shard.len();
        }
        store.record_total_used();
        return (resp_int(total as i64), false);
    }

    if cmd.eq_ignore_ascii_case(b"SELECT") {
        if args.len() != 2 {
            return (wrong_args(&args[0]), false);
        }
        let idx = match parse_i64_arg(&args[1]) {
            Ok(v) => v,
            Err(err) => return (err, false),
        };
        if idx == 0 {
            return (resp_ok(), false);
        }
        return (resp_err("ERR DB index is out of range"), false);
    }

    if cmd.eq_ignore_ascii_case(b"HELLO") {
        return (resp_err("command not supported in sharded mode"), false);
    }

    (resp_err("command not supported in sharded mode"), false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(items: &[&str]) -> Vec<Vec<u8>> {
        items.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    #[tokio::test]
    async fn set_get_roundtrip() {
        let store = ShardedDb::new(1024 * 1024, 8);
        assert_eq!(
            dispatch(&args(&["SET", "k", "v"]), &store).await.0,
            b"+OK\r\n"
        );
        assert_eq!(
            dispatch(&args(&["GET", "k"]), &store).await.0,
            b"$1\r\nv\r\n"
        );
    }

    #[tokio::test]
    async fn mset_mget_roundtrip() {
        let store = ShardedDb::new(1024 * 1024, 8);
        assert_eq!(
            dispatch(&args(&["MSET", "a", "1", "b", "2"]), &store)
                .await
                .0,
            b"+OK\r\n"
        );
        assert_eq!(
            dispatch(&args(&["MGET", "a", "b", "missing"]), &store)
                .await
                .0,
            b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$-1\r\n"
        );
    }

    #[tokio::test]
    async fn setnx_and_msetnx() {
        let store = ShardedDb::new(1024 * 1024, 8);
        assert_eq!(
            dispatch(&args(&["SETNX", "k", "v1"]), &store).await.0,
            b":1\r\n"
        );
        assert_eq!(
            dispatch(&args(&["SETNX", "k", "v2"]), &store).await.0,
            b":0\r\n"
        );
        assert_eq!(
            dispatch(&args(&["MSETNX", "a", "1", "b", "2"]), &store)
                .await
                .0,
            b":1\r\n"
        );
        assert_eq!(
            dispatch(&args(&["MSETNX", "b", "3", "c", "4"]), &store)
                .await
                .0,
            b":0\r\n"
        );
    }

    #[tokio::test]
    async fn incr_family_roundtrip() {
        let store = ShardedDb::new(1024 * 1024, 8);
        assert_eq!(dispatch(&args(&["INCR", "n"]), &store).await.0, b":1\r\n");
        assert_eq!(
            dispatch(&args(&["INCRBY", "n", "5"]), &store).await.0,
            b":6\r\n"
        );
        assert_eq!(dispatch(&args(&["DECR", "n"]), &store).await.0, b":5\r\n");
        assert_eq!(
            dispatch(&args(&["DECRBY", "n", "2"]), &store).await.0,
            b":3\r\n"
        );
    }

    #[tokio::test]
    async fn incr_on_non_integer_errors() {
        let store = ShardedDb::new(1024 * 1024, 8);
        let _ = dispatch(&args(&["SET", "k", "abc"]), &store).await;
        assert_eq!(
            dispatch(&args(&["INCR", "k"]), &store).await.0,
            b"-ERR value is not an integer or out of range\r\n"
        );
    }

    #[tokio::test]
    async fn set_with_px_expires() {
        let store = ShardedDb::new(1024 * 1024, 4);
        let _ = dispatch(&args(&["SET", "k", "v", "PX", "20"]), &store).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(dispatch(&args(&["GET", "k"]), &store).await.0, b"$-1\r\n");
    }

    #[tokio::test]
    async fn unknown_command_returns_error() {
        let store = ShardedDb::new(1024 * 1024, 4);
        assert_eq!(
            dispatch(&args(&["HSET", "h", "f", "v"]), &store).await.0,
            b"-ERR command not supported in sharded mode\r\n"
        );
    }

    #[tokio::test]
    async fn setnx_atomic_no_race() {
        // Concurrent SETNX: only one should win.
        let store = ShardedDb::new(1024 * 1024, 8);
        let s1 = Arc::clone(&store);
        let s2 = Arc::clone(&store);
        let r1 = tokio::spawn(async move {
            dispatch(&args(&["SETNX", "racekey", "v1"]), &s1).await.0
        });
        let r2 = tokio::spawn(async move {
            dispatch(&args(&["SETNX", "racekey", "v2"]), &s2).await.0
        });
        let (a, b) = tokio::join!(r1, r2);
        let wins: i32 = [a.unwrap(), b.unwrap()]
            .iter()
            .filter(|r| r.as_slice() == b":1\r\n")
            .count() as i32;
        assert_eq!(wins, 1, "exactly one SETNX should succeed");
    }

    #[tokio::test]
    async fn msetnx_atomic_all_or_nothing() {
        let store = ShardedDb::new(1024 * 1024, 8);
        // Pre-set key "b" so MSETNX a,b should fail atomically.
        let _ = dispatch(&args(&["SET", "b", "existing"]), &store).await;
        let r = dispatch(&args(&["MSETNX", "a", "1", "b", "2"]), &store).await.0;
        assert_eq!(r, b":0\r\n", "MSETNX must fail if any key exists");
        // "a" must not have been set.
        let ga = dispatch(&args(&["GET", "a"]), &store).await.0;
        assert_eq!(ga, b"$-1\r\n", "a must not be set after failed MSETNX");
    }

    #[tokio::test]
    async fn append_atomic_accumulates() {
        let store = ShardedDb::new(1024 * 1024, 8);
        let r1 = dispatch(&args(&["APPEND", "s", "hello"]), &store).await.0;
        assert_eq!(r1, b":5\r\n");
        let r2 = dispatch(&args(&["APPEND", "s", " world"]), &store).await.0;
        assert_eq!(r2, b":11\r\n");
        let r3 = dispatch(&args(&["GET", "s"]), &store).await.0;
        assert_eq!(r3, b"$11\r\nhello world\r\n");
    }

    #[tokio::test]
    async fn getset_atomic_returns_old_value() {
        let store = ShardedDb::new(1024 * 1024, 8);
        // GETSET on missing key returns null.
        let r1 = dispatch(&args(&["GETSET", "k", "new"]), &store).await.0;
        assert_eq!(r1, b"$-1\r\n");
        // GETSET on existing key returns old value.
        let r2 = dispatch(&args(&["GETSET", "k", "newer"]), &store).await.0;
        assert_eq!(r2, b"$3\r\nnew\r\n");
        // Value is updated.
        let r3 = dispatch(&args(&["GET", "k"]), &store).await.0;
        assert_eq!(r3, b"$5\r\nnewer\r\n");
    }
}
