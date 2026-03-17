use std::borrow::Cow;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use ahash::{AHashMap, RandomState};
use parking_lot::RwLock;
use smallvec::SmallVec;
use std::hash::BuildHasher;

use crate::commands::i64_to_bytes;
use crate::resp::{
    append_array_header, append_bulk, append_null, resp_bulk, resp_err, resp_int, resp_null,
    resp_ok, resp_pong, wrong_args,
};

#[derive(Clone)]
struct ShardedEntry {
    value: Vec<u8>,
    expiry: Option<Instant>,
}

/// Inline-allocated canonical key: `"namespace/key"`. The 64-byte inline
/// capacity covers the vast majority of keys without a heap allocation.
type CanonicalKey = SmallVec<[u8; 64]>;

#[derive(Clone)]
struct ParsedKey {
    canonical: CanonicalKey,
    key_len: usize,
    shard_idx: usize,
}

type ShardWriteGuard<'a> = (usize, parking_lot::RwLockWriteGuard<'a, AHashMap<CanonicalKey, ShardedEntry>>);

pub(crate) struct ShardedDb {
    shards: Vec<RwLock<AHashMap<CanonicalKey, ShardedEntry>>>,
    used_bytes: AtomicUsize,
    memory_limit: usize,
    /// Cached random state so shard_idx never re-seeds from thread-local storage.
    hasher: RandomState,
    /// Set to true on the first TTL insert; once true, never cleared.
    /// When false, get_value/put_value skip Instant::now() entirely.
    has_ttl: AtomicBool,
    /// Monotonically incrementing write counter used to throttle gauge emission.
    write_gen: AtomicUsize,
}

pub(crate) type ShardedStore = Arc<ShardedDb>;
const DEFAULT_NAMESPACE: &[u8] = b"default";

/// How often (in writes) to emit the kvns_memory_used_bytes_total gauge.
/// Keeps the gauge reasonably fresh without locking the metrics registry
/// on every write.
const GAUGE_EMIT_STRIDE: usize = 256;

impl ShardedDb {
    pub(crate) fn new(memory_limit: usize, shard_count: usize) -> ShardedStore {
        let count = shard_count.max(1);
        let mut shards = Vec::with_capacity(count);
        for _ in 0..count {
            shards.push(RwLock::new(AHashMap::new()));
        }
        metrics::gauge!("kvns_memory_limit_bytes").set(memory_limit as f64);
        metrics::gauge!("kvns_memory_used_bytes_total").set(0.0);
        Arc::new(Self {
            shards,
            used_bytes: AtomicUsize::new(0),
            memory_limit,
            hasher: RandomState::new(),
            has_ttl: AtomicBool::new(false),
            write_gen: AtomicUsize::new(0),
        })
    }

    fn entry_size(key_len: usize, value_len: usize) -> usize {
        key_len + value_len
    }

    fn shard_idx(&self, ns: &[u8], key: &[u8]) -> usize {
        let mut hasher = self.hasher.build_hasher();
        ns.hash(&mut hasher);
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len()
    }

    fn parse_key(&self, raw_key: &[u8]) -> ParsedKey {
        if let Some(pos) = raw_key.iter().position(|b| *b == b'/') {
            let ns = &raw_key[..pos];
            let key = &raw_key[pos + 1..];
            return ParsedKey {
                shard_idx: self.shard_idx(ns, key),
                canonical: SmallVec::from_slice(raw_key),
                key_len: raw_key.len().saturating_sub(1),
            };
        }
        let mut canonical: CanonicalKey =
            SmallVec::with_capacity(DEFAULT_NAMESPACE.len() + 1 + raw_key.len());
        canonical.extend_from_slice(DEFAULT_NAMESPACE);
        canonical.push(b'/');
        canonical.extend_from_slice(raw_key);
        ParsedKey {
            shard_idx: self.shard_idx(DEFAULT_NAMESPACE, raw_key),
            canonical,
            key_len: DEFAULT_NAMESPACE.len() + raw_key.len(),
        }
    }

    fn reserve_bytes(&self, extra_bytes: usize) -> bool {
        if extra_bytes == 0 {
            return true;
        }
        // Optimistic fetch_add — no spin loop. If we overshoot the limit, roll back.
        // There is a brief window where used_bytes exceeds the limit, but it is
        // immediately corrected. This trades perfect precision for removing CAS
        // contention across shards.
        let prev = self.used_bytes.fetch_add(extra_bytes, Ordering::Relaxed);
        if prev.saturating_add(extra_bytes) > self.memory_limit {
            self.used_bytes.fetch_sub(extra_bytes, Ordering::Relaxed);
            return false;
        }
        true
    }

    fn release_bytes(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.used_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Emit the memory gauge every GAUGE_EMIT_STRIDE writes to avoid locking the
    /// metrics registry on every command.
    fn maybe_record_total_used(&self) {
        let n = self.write_gen.fetch_add(1, Ordering::Relaxed);
        if n % GAUGE_EMIT_STRIDE == 0 {
            metrics::gauge!("kvns_memory_used_bytes_total")
                .set(self.used_bytes.load(Ordering::Relaxed) as f64);
        }
    }

    fn purge_expired_locked(
        &self,
        shard: &mut AHashMap<CanonicalKey, ShardedEntry>,
        parsed: &ParsedKey,
        now: Instant,
    ) {
        let expired = shard
            .get(&parsed.canonical)
            .is_some_and(|entry| entry.expiry.is_some_and(|deadline| now >= deadline));
        if expired
            && let Some(entry) = shard.remove(&parsed.canonical)
        {
            let size = Self::entry_size(parsed.key_len, entry.value.len());
            self.release_bytes(size);
        }
    }

    fn now_if_ttl(&self) -> Option<Instant> {
        self.has_ttl.load(Ordering::Relaxed).then(Instant::now)
    }

    fn get_value(&self, raw_key: &[u8]) -> Option<Vec<u8>> {
        let parsed = self.parse_key(raw_key);

        // Fast path: if no key with a TTL has ever been inserted, skip the
        // clock call and expiry bookkeeping entirely.
        if !self.has_ttl.load(Ordering::Relaxed) {
            let shard = self.shards[parsed.shard_idx].read();
            return shard.get(&parsed.canonical).map(|e| e.value.clone());
        }

        let now = Instant::now();
        {
            let shard = self.shards[parsed.shard_idx].read();
            match shard.get(&parsed.canonical) {
                None => return None,
                Some(entry) if entry.expiry.is_some_and(|deadline| now >= deadline) => {}
                Some(entry) => return Some(entry.value.clone()),
            }
        }

        // Key was expired in the read pass; upgrade to write lock to purge it.
        let mut shard = self.shards[parsed.shard_idx].write();
        self.purge_expired_locked(&mut shard, &parsed, now);
        None
    }

    fn put_value(
        &self,
        raw_key: &[u8],
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), ()> {
        let parsed = self.parse_key(raw_key);
        let key_len = parsed.key_len;

        // Only pay the clock cost when TTL keys are actually in use.
        let now_opt = if ttl.is_some() { Some(Instant::now()) } else { self.now_if_ttl() };

        let mut shard = self.shards[parsed.shard_idx].write();
        if let Some(now) = now_opt {
            self.purge_expired_locked(&mut shard, &parsed, now);
        }

        let old_size = shard
            .get(&parsed.canonical)
            .map(|entry| Self::entry_size(key_len, entry.value.len()))
            .unwrap_or(0);
        let new_size = Self::entry_size(key_len, value.len());

        if new_size > old_size {
            if !self.reserve_bytes(new_size - old_size) {
                return Err(());
            }
        } else {
            self.release_bytes(old_size - new_size);
        }

        let expiry = ttl.map(|duration| {
            self.has_ttl.store(true, Ordering::Relaxed);
            now_opt.unwrap() + duration
        });
        shard.insert(parsed.canonical, ShardedEntry { value, expiry });
        self.maybe_record_total_used();
        Ok(())
    }

    fn apply_integer_delta(
        &self,
        raw_key: &[u8],
        delta: i64,
        overflow_err: &'static str,
    ) -> Result<i64, Cow<'static, [u8]>> {
        let parsed = self.parse_key(raw_key);
        let key_len = parsed.key_len;
        let now_opt = self.now_if_ttl();

        let mut shard = self.shards[parsed.shard_idx].write();
        if let Some(now) = now_opt {
            self.purge_expired_locked(&mut shard, &parsed, now);
        }

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

        let new_value = i64_to_bytes(next);
        let old_size = shard
            .get(&parsed.canonical)
            .map(|entry| Self::entry_size(key_len, entry.value.len()))
            .unwrap_or(0);
        let new_size = Self::entry_size(key_len, new_value.len());

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
        self.maybe_record_total_used();
        Ok(next)
    }

    /// Insert `value` only if the key is absent (or expired). Returns `Ok(true)` on
    /// success, `Ok(false)` if the key already exists, `Err(())` on OOM.
    fn put_value_if_absent(&self, raw_key: &[u8], value: Vec<u8>) -> Result<bool, ()> {
        let parsed = self.parse_key(raw_key);
        let now_opt = self.now_if_ttl();
        let mut shard = self.shards[parsed.shard_idx].write();
        if let Some(now) = now_opt {
            self.purge_expired_locked(&mut shard, &parsed, now);
        }
        if shard.contains_key(&parsed.canonical) {
            return Ok(false);
        }
        let new_size = Self::entry_size(parsed.key_len, value.len());
        if !self.reserve_bytes(new_size) {
            return Err(());
        }
        shard.insert(
            parsed.canonical,
            ShardedEntry {
                value,
                expiry: None,
            },
        );
        self.maybe_record_total_used();
        Ok(true)
    }

    /// Append `suffix` to the existing value (defaulting to empty). Returns the
    /// new length on success, or `Err(bytes)` on OOM where `bytes` is an error response.
    fn append_value(&self, raw_key: &[u8], suffix: &[u8]) -> Result<i64, Cow<'static, [u8]>> {
        let parsed = self.parse_key(raw_key);
        let now_opt = self.now_if_ttl();
        let mut shard = self.shards[parsed.shard_idx].write();
        if let Some(now) = now_opt {
            self.purge_expired_locked(&mut shard, &parsed, now);
        }
        if let Some(entry) = shard.get_mut(&parsed.canonical) {
            if !self.reserve_bytes(suffix.len()) {
                return Err(resp_err(
                    "OOM command not allowed when used memory > 'maxmemory'",
                ));
            }
            entry.value.extend_from_slice(suffix);
            entry.expiry = None;
            let len = entry.value.len() as i64;
            self.maybe_record_total_used();
            return Ok(len);
        }
        let new_size = Self::entry_size(parsed.key_len, suffix.len());
        if !self.reserve_bytes(new_size) {
            return Err(resp_err(
                "OOM command not allowed when used memory > 'maxmemory'",
            ));
        }
        let len = suffix.len() as i64;
        shard.insert(
            parsed.canonical,
            ShardedEntry {
                value: suffix.to_vec(),
                expiry: None,
            },
        );
        self.maybe_record_total_used();
        Ok(len)
    }

    /// Atomically replace the value, returning the old value (if any). Returns
    /// `Err(())` on OOM.
    fn get_and_set(&self, raw_key: &[u8], new_value: Vec<u8>) -> Result<Option<Vec<u8>>, ()> {
        use std::collections::hash_map::Entry;

        let parsed = self.parse_key(raw_key);
        let key_len = parsed.key_len;
        let now_opt = self.now_if_ttl();
        let mut shard = self.shards[parsed.shard_idx].write();
        if let Some(now) = now_opt {
            self.purge_expired_locked(&mut shard, &parsed, now);
        }
        match shard.entry(parsed.canonical) {
            Entry::Occupied(mut occupied) => {
                let old_size = Self::entry_size(key_len, occupied.get().value.len());
                let new_size = Self::entry_size(key_len, new_value.len());
                if new_size > old_size {
                    if !self.reserve_bytes(new_size - old_size) {
                        return Err(());
                    }
                } else {
                    self.release_bytes(old_size - new_size);
                }
                let entry = occupied.get_mut();
                let old = std::mem::replace(&mut entry.value, new_value);
                entry.expiry = None;
                self.maybe_record_total_used();
                Ok(Some(old))
            }
            Entry::Vacant(vacant) => {
                let new_size = Self::entry_size(key_len, new_value.len());
                if !self.reserve_bytes(new_size) {
                    return Err(());
                }
                vacant.insert(ShardedEntry {
                    value: new_value,
                    expiry: None,
                });
                self.maybe_record_total_used();
                Ok(None)
            }
        }
    }

    /// Set all key-value pairs only if none of the keys already exist. Acquires
    /// shard write locks in index order to avoid deadlock. Returns `Ok(true)` on
    /// success, `Ok(false)` if any key exists, `Err(())` on OOM.
    fn msetnx_atomic(&self, pairs: Vec<(ParsedKey, Vec<u8>)>) -> Result<bool, ()> {
        let now_opt = self.now_if_ttl();
        // Group pairs by shard index (BTreeMap → sorted order prevents deadlock).
        let mut by_shard: BTreeMap<usize, Vec<(ParsedKey, Vec<u8>)>> = BTreeMap::new();
        for (parsed, value) in pairs {
            by_shard
                .entry(parsed.shard_idx)
                .or_default()
                .push((parsed, value));
        }
        // Acquire all write locks in shard-index order.
        let mut guards: Vec<ShardWriteGuard<'_>> = Vec::with_capacity(by_shard.len());
        for &shard_idx in by_shard.keys() {
            let guard = self.shards[shard_idx].write();
            guards.push((shard_idx, guard));
        }
        // First pass: purge expired + check existence.
        for (shard_idx, guard) in &mut guards {
            if let Some(shard_pairs) = by_shard.get(&*shard_idx) {
                let shard: &mut AHashMap<CanonicalKey, ShardedEntry> = guard;
                for (parsed, _) in shard_pairs {
                    if let Some(now) = now_opt {
                        self.purge_expired_locked(shard, parsed, now);
                    }
                    if shard.contains_key(&parsed.canonical) {
                        return Ok(false);
                    }
                }
            }
        }
        // Second pass: reserve bytes + insert.
        for (shard_idx, guard) in &mut guards {
            if let Some(shard_pairs) = by_shard.get_mut(&*shard_idx) {
                let shard: &mut AHashMap<CanonicalKey, ShardedEntry> = guard;
                for (parsed, value) in shard_pairs.drain(..) {
                    let size = Self::entry_size(parsed.key_len, value.len());
                    if !self.reserve_bytes(size) {
                        return Err(());
                    }
                    shard.insert(
                        parsed.canonical,
                        ShardedEntry {
                            value,
                            expiry: None,
                        },
                    );
                }
            }
        }
        self.maybe_record_total_used();
        Ok(true)
    }
}

fn set_ttl_from_args(args: &[Vec<u8>]) -> Result<Option<Duration>, Cow<'static, [u8]>> {
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

fn parse_i64_arg(raw: &[u8]) -> Result<i64, Cow<'static, [u8]>> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or_else(|| resp_err("value is not an integer or out of range"))
}

/// Dispatch a RESP command against the sharded store.
///
/// This is a synchronous function — all shard operations use `parking_lot::RwLock`
/// which never needs to `.await`. The `async fn` wrapper was removed to eliminate
/// per-command future construction overhead.
pub(crate) fn dispatch(
    args: &mut [Vec<u8>],
    store: &ShardedStore,
) -> (Cow<'static, [u8]>, bool) {
    if args.is_empty() {
        return (resp_err("empty command"), false);
    }
    // Normalize to ASCII uppercase in a stack buffer — zero heap allocation.
    let mut cmd_upper = [0u8; 20];
    let cmd_len = args[0].len().min(cmd_upper.len());
    for (dst, &src) in cmd_upper[..cmd_len].iter_mut().zip(&args[0][..cmd_len]) {
        *dst = src.to_ascii_uppercase();
    }
    let cmd = &cmd_upper[..cmd_len];

    let resp = match cmd {
        b"PING" => return (resp_pong(), false),
        b"QUIT" => return (resp_ok(), true),

        b"SET" => {
            let ttl = match set_ttl_from_args(args) {
                Ok(v) => v,
                Err(err) => return (err, false),
            };
            let value = std::mem::take(&mut args[2]);
            if store.put_value(&args[1], value, ttl).is_ok() {
                resp_ok()
            } else {
                resp_err("OOM command not allowed when used memory > 'maxmemory'")
            }
        }
        b"GET" => {
            if args.len() != 2 { return (wrong_args(&args[0]), false); }
            match store.get_value(&args[1]) {
                Some(value) => resp_bulk(&value),
                None => resp_null(),
            }
        }
        b"MGET" => {
            if args.len() < 2 { return (wrong_args(&args[0]), false); }
            let key_count = args.len() - 1;
            let mut out = Vec::with_capacity(16 + key_count * 32);
            append_array_header(&mut out, key_count);
            for raw_key in &args[1..] {
                match store.get_value(raw_key) {
                    Some(value) => append_bulk(&mut out, &value),
                    None => append_null(&mut out),
                }
            }
            return (out.into(), false);
        }
        b"MSET" => {
            if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                return (wrong_args(&args[0]), false);
            }
            let mut i = 1usize;
            while i + 1 < args.len() {
                let value = std::mem::take(&mut args[i + 1]);
                if store.put_value(&args[i], value, None).is_err() {
                    return (resp_err("OOM command not allowed when used memory > 'maxmemory'"), false);
                }
                i += 2;
            }
            resp_ok()
        }
        b"MSETNX" => {
            if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                return (wrong_args(&args[0]), false);
            }
            let mut pairs: Vec<(ParsedKey, Vec<u8>)> = Vec::new();
            let mut i = 1usize;
            while i + 1 < args.len() {
                let key = store.parse_key(&args[i]);
                let value = std::mem::take(&mut args[i + 1]);
                pairs.push((key, value));
                i += 2;
            }
            return match store.msetnx_atomic(pairs) {
                Ok(true) => (resp_int(1), false),
                Ok(false) => (resp_int(0), false),
                Err(()) => (resp_err("OOM command not allowed when used memory > 'maxmemory'"), false),
            };
        }
        b"SETNX" => {
            if args.len() != 3 { return (wrong_args(&args[0]), false); }
            let value = std::mem::take(&mut args[2]);
            return match store.put_value_if_absent(&args[1], value) {
                Ok(true) => (resp_int(1), false),
                Ok(false) => (resp_int(0), false),
                Err(()) => (resp_err("OOM command not allowed when used memory > 'maxmemory'"), false),
            };
        }
        b"INCR" => {
            if args.len() != 2 { return (wrong_args(&args[0]), false); }
            return match store.apply_integer_delta(&args[1], 1, "increment would overflow") {
                Ok(next) => (resp_int(next), false),
                Err(err) => (err, false),
            };
        }
        b"INCRBY" => {
            if args.len() != 3 { return (wrong_args(&args[0]), false); }
            let by = match parse_i64_arg(&args[2]) {
                Ok(v) => v,
                Err(err) => return (err, false),
            };
            return match store.apply_integer_delta(&args[1], by, "increment would overflow") {
                Ok(next) => (resp_int(next), false),
                Err(err) => (err, false),
            };
        }
        b"DECR" => {
            if args.len() != 2 { return (wrong_args(&args[0]), false); }
            return match store.apply_integer_delta(&args[1], -1, "decrement would overflow") {
                Ok(next) => (resp_int(next), false),
                Err(err) => (err, false),
            };
        }
        b"DECRBY" => {
            if args.len() != 3 { return (wrong_args(&args[0]), false); }
            let by = match parse_i64_arg(&args[2]) {
                Ok(v) => v,
                Err(err) => return (err, false),
            };
            let delta = match by.checked_neg() {
                Some(v) => v,
                None => return (resp_err("decrement would overflow"), false),
            };
            return match store.apply_integer_delta(&args[1], delta, "decrement would overflow") {
                Ok(next) => (resp_int(next), false),
                Err(err) => (err, false),
            };
        }
        b"STRLEN" => {
            if args.len() != 2 { return (wrong_args(&args[0]), false); }
            match store.get_value(&args[1]) {
                Some(value) => resp_int(value.len() as i64),
                None => resp_int(0),
            }
        }
        b"TYPE" => {
            if args.len() != 2 { return (wrong_args(&args[0]), false); }
            if store.get_value(&args[1]).is_some() {
                resp_bulk(b"string")
            } else {
                resp_bulk(b"none")
            }
        }
        b"APPEND" => {
            if args.len() != 3 { return (wrong_args(&args[0]), false); }
            return match store.append_value(&args[1], &args[2]) {
                Ok(len) => (resp_int(len), false),
                Err(err) => (err, false),
            };
        }
        b"GETSET" => {
            if args.len() != 3 { return (wrong_args(&args[0]), false); }
            let value = std::mem::take(&mut args[2]);
            return match store.get_and_set(&args[1], value) {
                Ok(Some(old)) => (resp_bulk(&old), false),
                Ok(None) => (resp_null(), false),
                Err(()) => (resp_err("OOM command not allowed when used memory > 'maxmemory'"), false),
            };
        }
        b"GETDEL" => {
            if args.len() != 2 { return (wrong_args(&args[0]), false); }
            let parsed = store.parse_key(&args[1]);
            let now_opt = store.now_if_ttl();
            let mut shard = store.shards[parsed.shard_idx].write();
            if let Some(now) = now_opt {
                store.purge_expired_locked(&mut shard, &parsed, now);
            }
            let removed = shard.remove(&parsed.canonical);
            match removed {
                Some(entry) => {
                    let size = ShardedDb::entry_size(parsed.key_len, entry.value.len());
                    store.release_bytes(size);
                    store.maybe_record_total_used();
                    resp_bulk(&entry.value)
                }
                None => resp_null(),
            }
        }
        b"GETEX" => {
            if args.len() < 2 { return (wrong_args(&args[0]), false); }
            if args.len() > 2 {
                return (resp_err("command not supported in sharded mode"), false);
            }
            match store.get_value(&args[1]) {
                Some(value) => resp_bulk(&value),
                None => resp_null(),
            }
        }
        b"DBSIZE" => {
            if args.len() != 1 { return (wrong_args(&args[0]), false); }
            let now_opt = store.now_if_ttl();
            let mut total = 0usize;
            for shard_lock in &store.shards {
                let shard = shard_lock.read();
                total += match now_opt {
                    Some(now) => shard.values().filter(|e| !e.expiry.is_some_and(|d| now >= d)).count(),
                    None => shard.len(),
                };
            }
            resp_int(total as i64)
        }
        b"SELECT" => {
            if args.len() != 2 { return (wrong_args(&args[0]), false); }
            let idx = match parse_i64_arg(&args[1]) {
                Ok(v) => v,
                Err(err) => return (err, false),
            };
            if idx == 0 { resp_ok() } else { resp_err("ERR DB index is out of range") }
        }
        b"HELLO" | b"DEL" => resp_err("command not supported in sharded mode"),

        _ => resp_err("command not supported in sharded mode"),
    };
    (resp, false)
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
        assert_eq!(&*(dispatch(&mut args(&["SET", "k", "v"]), &store).0), b"+OK\r\n");
        assert_eq!(
            &*(dispatch(&mut args(&["GET", "k"]), &store).0),
            b"$1\r\nv\r\n"
        );
    }

    #[tokio::test]
    async fn mset_mget_roundtrip() {
        let store = ShardedDb::new(1024 * 1024, 8);
        assert_eq!(
            &*(dispatch(&mut args(&["MSET", "a", "1", "b", "2"]), &store).0),
            b"+OK\r\n"
        );
        assert_eq!(
            &*(dispatch(&mut args(&["MGET", "a", "b", "missing"]), &store).0),
            b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$-1\r\n"
        );
    }

    #[tokio::test]
    async fn setnx_and_msetnx() {
        let store = ShardedDb::new(1024 * 1024, 8);
        assert_eq!(
            &*(dispatch(&mut args(&["SETNX", "k", "v1"]), &store).0),
            b":1\r\n"
        );
        assert_eq!(
            &*(dispatch(&mut args(&["SETNX", "k", "v2"]), &store).0),
            b":0\r\n"
        );
        assert_eq!(
            &*(dispatch(&mut args(&["MSETNX", "a", "1", "b", "2"]), &store).0),
            b":1\r\n"
        );
        assert_eq!(
            &*(dispatch(&mut args(&["MSETNX", "b", "3", "c", "4"]), &store).0),
            b":0\r\n"
        );
    }

    #[tokio::test]
    async fn incr_family_roundtrip() {
        let store = ShardedDb::new(1024 * 1024, 8);
        assert_eq!(&*(dispatch(&mut args(&["INCR", "n"]), &store).0), b":1\r\n");
        assert_eq!(&*(dispatch(&mut args(&["INCRBY", "n", "5"]), &store).0), b":6\r\n");
        assert_eq!(&*(dispatch(&mut args(&["DECR", "n"]), &store).0), b":5\r\n");
        assert_eq!(&*(dispatch(&mut args(&["DECRBY", "n", "2"]), &store).0), b":3\r\n");
    }

    #[tokio::test]
    async fn incr_on_non_integer_errors() {
        let store = ShardedDb::new(1024 * 1024, 8);
        let _ = dispatch(&mut args(&["SET", "k", "abc"]), &store);
        assert_eq!(
            &*(dispatch(&mut args(&["INCR", "k"]), &store).0),
            b"-ERR value is not an integer or out of range\r\n"
        );
    }

    #[tokio::test]
    async fn set_with_px_expires() {
        let store = ShardedDb::new(1024 * 1024, 4);
        let _ = dispatch(&mut args(&["SET", "k", "v", "PX", "20"]), &store);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(&*(dispatch(&mut args(&["GET", "k"]), &store).0), b"$-1\r\n");
    }

    #[tokio::test]
    async fn unknown_command_returns_error() {
        let store = ShardedDb::new(1024 * 1024, 4);
        assert_eq!(
            &*(dispatch(&mut args(&["HSET", "h", "f", "v"]), &store).0),
            b"-ERR command not supported in sharded mode\r\n"
        );
    }

    #[tokio::test]
    async fn setnx_atomic_no_race() {
        // Concurrent SETNX: only one should win.
        let store = ShardedDb::new(1024 * 1024, 8);
        let s1 = Arc::clone(&store);
        let s2 = Arc::clone(&store);
        let r1 = tokio::spawn(async move { dispatch(&mut args(&["SETNX", "racekey", "v1"]), &s1).0 });
        let r2 = tokio::spawn(async move { dispatch(&mut args(&["SETNX", "racekey", "v2"]), &s2).0 });
        let (a, b) = tokio::join!(r1, r2);
        let wins: i32 = [a.unwrap(), b.unwrap()]
            .iter()
            .filter(|r| r.as_ref() == b":1\r\n" as &[u8])
            .count() as i32;
        assert_eq!(wins, 1, "exactly one SETNX should succeed");
    }

    #[tokio::test]
    async fn msetnx_atomic_all_or_nothing() {
        let store = ShardedDb::new(1024 * 1024, 8);
        // Pre-set key "b" so MSETNX a,b should fail atomically.
        let _ = dispatch(&mut args(&["SET", "b", "existing"]), &store);
        let r = dispatch(&mut args(&["MSETNX", "a", "1", "b", "2"]), &store).0;
        assert_eq!(&*r, b":0\r\n", "MSETNX must fail if any key exists");
        // "a" must not have been set.
        let ga = dispatch(&mut args(&["GET", "a"]), &store).0;
        assert_eq!(&*ga, b"$-1\r\n", "a must not be set after failed MSETNX");
    }

    #[tokio::test]
    async fn append_atomic_accumulates() {
        let store = ShardedDb::new(1024 * 1024, 8);
        let r1 = dispatch(&mut args(&["APPEND", "s", "hello"]), &store).0;
        assert_eq!(&*r1, b":5\r\n");
        let r2 = dispatch(&mut args(&["APPEND", "s", " world"]), &store).0;
        assert_eq!(&*r2, b":11\r\n");
        let r3 = dispatch(&mut args(&["GET", "s"]), &store).0;
        assert_eq!(&*r3, b"$11\r\nhello world\r\n");
    }

    #[tokio::test]
    async fn getset_atomic_returns_old_value() {
        let store = ShardedDb::new(1024 * 1024, 8);
        // GETSET on missing key returns null.
        let r1 = dispatch(&mut args(&["GETSET", "k", "new"]), &store).0;
        assert_eq!(&*r1, b"$-1\r\n");
        // GETSET on existing key returns old value.
        let r2 = dispatch(&mut args(&["GETSET", "k", "newer"]), &store).0;
        assert_eq!(&*r2, b"$3\r\nnew\r\n");
        // Value is updated.
        let r3 = dispatch(&mut args(&["GET", "k"]), &store).0;
        assert_eq!(&*r3, b"$5\r\nnewer\r\n");
    }
}
