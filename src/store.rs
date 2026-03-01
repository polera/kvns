use std::cmp::Reverse;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// A sorted set with O(1) member lookup and O(log n) rank queries.
#[derive(Clone, Default)]
pub(crate) struct ZSetData {
    /// Entries sorted by (score ASC, member ASC).
    pub sorted: Vec<ZEntry>,
    /// Member → score index for O(1) lookups.
    pub index: HashMap<Vec<u8>, f64>,
}

impl ZSetData {
    pub fn len(&self) -> usize {
        self.sorted.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sorted.is_empty()
    }
}

use tokio::sync::RwLock;

use crate::config::EvictionPolicy;

/// A single entry in a sorted set.
#[derive(Clone)]
pub(crate) struct ZEntry {
    pub score: f64,
    pub member: Vec<u8>,
}

#[derive(Clone)]
pub(crate) enum Value {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Hash(HashMap<Vec<u8>, Vec<u8>>),
    Set(HashSet<Vec<u8>>),
    /// Dual-structure: sorted vec + member→score index.
    ZSet(ZSetData),
}

impl Value {
    pub(crate) fn byte_len(&self) -> usize {
        match self {
            Value::String(b) => b.len(),
            Value::List(items) => items.iter().map(|b| b.len()).sum(),
            Value::Hash(m) => m.iter().map(|(k, v)| k.len() + v.len()).sum(),
            Value::Set(s) => s.iter().map(|v| v.len()).sum(),
            Value::ZSet(data) => data.sorted.iter().map(|e| 8 + e.member.len()).sum::<usize>()
                + data.index.keys().map(|k| k.len() + 8).sum::<usize>(),
        }
    }

    pub(crate) fn as_string(&self) -> Option<&[u8]> {
        if let Value::String(b) = self {
            Some(b)
        } else {
            None
        }
    }

    pub(crate) fn as_string_mut(&mut self) -> Option<&mut Vec<u8>> {
        if let Value::String(b) = self {
            Some(b)
        } else {
            None
        }
    }

    pub(crate) fn as_hash(&self) -> Option<&HashMap<Vec<u8>, Vec<u8>>> {
        if let Value::Hash(m) = self {
            Some(m)
        } else {
            None
        }
    }

    pub(crate) fn as_hash_mut(&mut self) -> Option<&mut HashMap<Vec<u8>, Vec<u8>>> {
        if let Value::Hash(m) = self {
            Some(m)
        } else {
            None
        }
    }

    pub(crate) fn as_set(&self) -> Option<&HashSet<Vec<u8>>> {
        if let Value::Set(s) = self {
            Some(s)
        } else {
            None
        }
    }

    pub(crate) fn as_set_mut(&mut self) -> Option<&mut HashSet<Vec<u8>>> {
        if let Value::Set(s) = self {
            Some(s)
        } else {
            None
        }
    }

    pub(crate) fn as_zset(&self) -> Option<&ZSetData> {
        if let Value::ZSet(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub(crate) fn as_zset_mut(&mut self) -> Option<&mut ZSetData> {
        if let Value::ZSet(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub(crate) fn type_name(&self) -> &'static str {
        match self {
            Value::String(_) => "string",
            Value::List(_) => "list",
            Value::Hash(_) => "hash",
            Value::Set(_) => "set",
            Value::ZSet(_) => "zset",
        }
    }
}

pub(crate) struct Entry {
    pub(crate) value: Value,
    /// Hit counter for LRU/MRU eviction.  Stored as an atomic so read commands
    /// can increment it while holding only a read lock on the store.
    pub(crate) hits: AtomicU64,
    pub(crate) expiry: Option<Instant>,
}

impl Clone for Entry {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            hits: AtomicU64::new(self.hits.load(Ordering::Relaxed)),
            expiry: self.expiry,
        }
    }
}

impl Entry {
    pub(crate) fn new(value: Vec<u8>, ttl: Option<Duration>) -> Self {
        Self {
            value: Value::String(value),
            hits: AtomicU64::new(0),
            expiry: ttl.map(|d| Instant::now() + d),
        }
    }

    pub(crate) fn is_expired(&self) -> bool {
        self.expiry.is_some_and(|e| Instant::now() >= e)
    }

    /// Returns:
    /// - `-1` if the key has no expiry
    /// - `0`  if the key has already expired (should be lazily removed)
    /// - `N`  seconds remaining until expiry
    pub(crate) fn time_to_expiry_secs(&self) -> i64 {
        match self.expiry {
            None => -1,
            Some(e) => {
                let now = Instant::now();
                if e <= now {
                    0
                } else {
                    (e - now).as_secs() as i64
                }
            }
        }
    }

    /// Same as `time_to_expiry_secs` but returns milliseconds.
    pub(crate) fn time_to_expiry_ms(&self) -> i64 {
        match self.expiry {
            None => -1,
            Some(e) => {
                let now = Instant::now();
                if e <= now {
                    0
                } else {
                    (e - now).as_millis() as i64
                }
            }
        }
    }
}

/// Gauge values captured by `put_deferred`; emit them after releasing the store lock
/// to avoid holding the write lock while Prometheus's internal DashMap is updated.
pub(crate) struct StoreMetrics {
    namespace: String,
    ns_keys: usize,
    ns_bytes: usize,
    total_bytes: usize,
}

impl StoreMetrics {
    pub(crate) fn emit(self) {
        metrics::gauge!("kvns_keys_total", "namespace" => self.namespace.clone())
            .set(self.ns_keys as f64);
        metrics::gauge!("kvns_memory_used_bytes", "namespace" => self.namespace)
            .set(self.ns_bytes as f64);
        metrics::gauge!("kvns_memory_used_bytes_total").set(self.total_bytes as f64);
    }
}

pub(crate) struct Db {
    /// Outer key: namespace ("default" = unqualified keys).
    /// Inner key: the local key within that namespace.
    pub(crate) entries: HashMap<String, HashMap<String, Entry>>,
    pub(crate) used_bytes: usize,
    pub(crate) memory_limit: usize,
    /// Per-namespace byte totals used to drive per-namespace gauge labels.
    pub(crate) namespace_bytes: HashMap<String, usize>,
    pub(crate) eviction_threshold: f64,
    pub(crate) eviction_policy: EvictionPolicy,
    pub(crate) namespace_eviction_policies: HashMap<String, EvictionPolicy>,
    /// Keys pending deletion at the next EAR sweep.
    pub(crate) ear_pending: HashSet<(String, String)>,
}

impl Db {
    pub(crate) fn new(memory_limit: usize) -> Self {
        metrics::gauge!("kvns_memory_limit_bytes").set(memory_limit as f64);
        metrics::gauge!("kvns_memory_used_bytes_total").set(0.0);
        Self {
            entries: HashMap::new(),
            used_bytes: 0,
            memory_limit,
            namespace_bytes: HashMap::new(),
            eviction_threshold: 1.0,
            eviction_policy: EvictionPolicy::None,
            namespace_eviction_policies: HashMap::new(),
            ear_pending: HashSet::new(),
        }
    }

    /// Builder-style setter for eviction configuration.
    pub(crate) fn with_eviction(
        mut self,
        threshold: f64,
        policy: EvictionPolicy,
        namespace_policies: HashMap<String, EvictionPolicy>,
    ) -> Self {
        self.eviction_threshold = threshold;
        self.eviction_policy = policy;
        self.namespace_eviction_policies = namespace_policies;
        self
    }

    /// Returns the effective eviction policy for `namespace`, preferring a
    /// per-namespace override over the global default.
    fn policy_for_namespace(&self, namespace: &str) -> EvictionPolicy {
        self.namespace_eviction_policies
            .get(namespace)
            .cloned()
            .unwrap_or_else(|| self.eviction_policy.clone())
    }

    /// Whether reads in `namespace` should update hit counters for eviction.
    pub(crate) fn tracks_hits(&self, namespace: &str) -> bool {
        matches!(
            self.policy_for_namespace(namespace),
            EvictionPolicy::Lru | EvictionPolicy::Mru
        )
    }

    /// Returns `true` if `namespace` uses the `ExpireAfterRead` eviction policy.
    pub(crate) fn is_ear_namespace(&self, namespace: &str) -> bool {
        matches!(
            self.policy_for_namespace(namespace),
            EvictionPolicy::ExpireAfterRead
        )
    }

    /// Mark `(namespace, key)` for deletion at the next EAR sweep.
    /// Only marks if the key currently exists and is not expired.
    pub(crate) fn mark_ear(&mut self, namespace: &str, key: &str) {
        if self
            .entries
            .get(namespace)
            .and_then(|m| m.get(key))
            .is_some_and(|e| !e.is_expired())
        {
            self.ear_pending
                .insert((namespace.to_owned(), key.to_owned()));
        }
    }

    /// Attempt to free enough memory in `namespace` so that a write of
    /// `net_delta` additional bytes can succeed.
    ///
    /// Returns `true` if there is now enough room for the write; `false` if
    /// eviction is not configured or could not free enough space.
    pub(crate) fn evict_for_write(&mut self, namespace: &str, net_delta: usize) -> bool {
        // 1. Zero-delta writes always fit.
        if net_delta == 0 {
            return true;
        }
        // 2. No eviction policy or EAR → cannot help.
        let policy = self.policy_for_namespace(namespace);
        if matches!(
            policy,
            EvictionPolicy::None | EvictionPolicy::ExpireAfterRead
        ) {
            return false;
        }
        // 3. Only evict when we are at or above the threshold fraction.
        let threshold_bytes = (self.memory_limit as f64 * self.eviction_threshold) as usize;
        if self.used_bytes < threshold_bytes {
            return false;
        }
        // 4. Evict in rounds using reservoir sampling to avoid O(n) clone.
        const EVICTION_SAMPLE_SIZE: usize = 32;
        const MAX_ROUNDS: usize = 20;
        let mut total_count = 0u64;
        let mut rounds = 0;
        while self.used_bytes.saturating_add(net_delta) > self.memory_limit && rounds < MAX_ROUNDS {
            let would_use = self.used_bytes.saturating_add(net_delta);
            if would_use <= self.memory_limit {
                break;
            }
            let overflow = would_use - self.memory_limit;

            // 5. Sample up to EVICTION_SAMPLE_SIZE keys from the namespace.
            let ns_map = match self.entries.get(namespace) {
                None => return false,
                Some(m) => m,
            };
            let mut candidates: Vec<(String, u64, usize)> = {
                let it = ns_map.iter().map(|(k, e)| {
                    (
                        k.clone(),
                        e.hits.load(Ordering::Relaxed),
                        Self::entry_size(namespace, k, e.value.byte_len()),
                    )
                });
                if ns_map.len() > EVICTION_SAMPLE_SIZE {
                    it.take(EVICTION_SAMPLE_SIZE).collect()
                } else {
                    it.collect()
                }
            };
            if candidates.is_empty() {
                break;
            }

            // 6. Sort sample by eviction order.
            match policy {
                EvictionPolicy::Lru => candidates.sort_by_key(|(_, hits, _)| *hits),
                EvictionPolicy::Mru => candidates.sort_by_key(|(_, hits, _)| Reverse(*hits)),
                EvictionPolicy::None | EvictionPolicy::ExpireAfterRead => unreachable!(),
            }

            // 7. Evict from sample until round's overflow is covered.
            let mut freed = 0usize;
            for (key, _, size) in candidates {
                if freed >= overflow {
                    break;
                }
                self.delete(namespace, &key);
                freed += size;
                total_count += 1;
            }
            rounds += 1;
        }

        // 8. Trace log.
        tracing::debug!(namespace, rounds, "evicted keys");
        // 9. Metric.
        metrics::counter!("kvns_evictions_total", "namespace" => namespace.to_owned())
            .increment(total_count);

        // 10. Did we free enough?
        self.used_bytes.saturating_add(net_delta) <= self.memory_limit
    }

    /// Net additional bytes required to store `new_value_byte_len` bytes under
    /// `(namespace, key)`, accounting for any existing entry that will be
    /// replaced.  Returns 0 when the new value is smaller than the old one.
    pub(crate) fn net_delta(&self, namespace: &str, key: &str, new_value_byte_len: usize) -> usize {
        let new_size = Self::entry_size(namespace, key, new_value_byte_len);
        let old_size = self
            .entries
            .get(namespace)
            .and_then(|ns| ns.get(key))
            .map(|e| Self::entry_size(namespace, key, e.value.byte_len()))
            .unwrap_or(0);
        new_size.saturating_sub(old_size)
    }

    /// Bytes attributed to one entry: namespace + key lengths + value payload.
    pub(crate) fn entry_size(namespace: &str, key: &str, value_byte_len: usize) -> usize {
        namespace.len() + key.len() + value_byte_len
    }

    pub(crate) fn would_exceed(
        &self,
        namespace: &str,
        key: &str,
        new_value_byte_len: usize,
    ) -> bool {
        let new_size = Self::entry_size(namespace, key, new_value_byte_len);
        let old_size = self
            .entries
            .get(namespace)
            .and_then(|ns| ns.get(key))
            .map(|e| Self::entry_size(namespace, key, e.value.byte_len()))
            .unwrap_or(0);
        let net_delta = new_size.saturating_sub(old_size);
        self.used_bytes.saturating_add(net_delta) > self.memory_limit
    }

    /// Insert an entry and return the metric snapshot; caller is responsible for
    /// calling `.emit()` at an appropriate point (ideally after releasing the lock).
    pub(crate) fn put_deferred(
        &mut self,
        namespace: &str,
        key: &str,
        entry: Entry,
    ) -> StoreMetrics {
        let namespace = namespace.to_owned();
        let key = key.to_owned();
        // A fresh write cancels any pending EAR eviction for this key.
        self.ear_pending.remove(&(namespace.clone(), key.clone()));
        let new_size = Self::entry_size(&namespace, &key, entry.value.byte_len());
        let old_size = self
            .entries
            .get(&namespace)
            .and_then(|ns| ns.get(&key))
            .map(|e| Self::entry_size(&namespace, &key, e.value.byte_len()))
            .unwrap_or(0);
        self.used_bytes = self
            .used_bytes
            .saturating_sub(old_size)
            .saturating_add(new_size);
        let nb = self.namespace_bytes.entry(namespace.clone()).or_insert(0);
        *nb = nb.saturating_sub(old_size).saturating_add(new_size);
        let ns_bytes = *nb;
        self.entries
            .entry(namespace.clone())
            .or_default()
            .insert(key, entry);
        let ns_keys = self.entries[&namespace].len();
        StoreMetrics {
            namespace,
            ns_keys,
            ns_bytes,
            total_bytes: self.used_bytes,
        }
    }

    /// Insert an entry and immediately emit Prometheus gauge updates.
    /// Most callers should prefer this; use `put_deferred` only when the write
    /// lock will already be dropped before metrics are needed.
    pub(crate) fn put(&mut self, namespace: &str, key: &str, entry: Entry) {
        self.put_deferred(namespace, key, entry).emit();
    }

    pub(crate) fn delete(&mut self, namespace: &str, key: &str) -> Option<Entry> {
        self.ear_pending
            .remove(&(namespace.to_owned(), key.to_owned()));
        let removed = self
            .entries
            .get_mut(namespace)
            .and_then(|ns| ns.remove(key));
        if let Some(ref e) = removed {
            let size = Self::entry_size(namespace, key, e.value.byte_len());
            self.used_bytes = self.used_bytes.saturating_sub(size);
            if let Some(nb) = self.namespace_bytes.get_mut(namespace) {
                *nb = nb.saturating_sub(size);
            }
            // Drop the namespace map itself when it becomes empty.
            if self.entries.get(namespace).is_some_and(|ns| ns.is_empty()) {
                self.entries.remove(namespace);
                self.namespace_bytes.remove(namespace);
            }
        }
        let ns_keys = self.entries.get(namespace).map(|ns| ns.len()).unwrap_or(0);
        let ns_bytes = self.namespace_bytes.get(namespace).copied().unwrap_or(0);
        metrics::gauge!("kvns_keys_total", "namespace" => namespace.to_owned()).set(ns_keys as f64);
        metrics::gauge!("kvns_memory_used_bytes", "namespace" => namespace.to_owned())
            .set(ns_bytes as f64);
        metrics::gauge!("kvns_memory_used_bytes_total").set(self.used_bytes as f64);
        removed
    }

    /// Total number of non-expired keys across all namespaces.
    pub(crate) fn total_keys(&self) -> usize {
        self.entries
            .values()
            .map(|ns| ns.values().filter(|e| !e.is_expired()).count())
            .sum()
    }

    /// Flush all entries across all namespaces.
    pub(crate) fn flush_all(&mut self) {
        self.entries.clear();
        self.used_bytes = 0;
        self.namespace_bytes.clear();
        self.ear_pending.clear();
        metrics::gauge!("kvns_memory_used_bytes_total").set(0.0);
    }
}

pub(crate) type Store = Arc<RwLock<Db>>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EvictionPolicy;

    // ── Entry tests ───────────────────────────────────────────────────────────

    #[test]
    fn entry_no_ttl_never_expires() {
        let e = Entry::new(b"v".to_vec(), None);
        assert!(!e.is_expired());
        assert_eq!(e.time_to_expiry_secs(), -1);
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
            value: Value::String(b"v".to_vec()),
            hits: AtomicU64::new(0),
            expiry: Some(Instant::now() - Duration::from_secs(1)),
        };
        assert!(e.is_expired());
        assert_eq!(e.time_to_expiry_secs(), 0);
    }

    // ── Eviction tests ────────────────────────────────────────────────────────

    fn make_db_with_eviction(memory_limit: usize, threshold: f64, policy: EvictionPolicy) -> Db {
        Db::new(memory_limit).with_eviction(threshold, policy, HashMap::new())
    }

    // Populate a namespace with three entries whose sizes are all equal.
    // Returns the per-entry size: ns.len() + key.len() + value.len().
    fn populate_ns(db: &mut Db, ns: &str, entries: &[(&str, u64)], value: &[u8]) -> usize {
        for (key, hits) in entries {
            db.put(
                ns,
                key,
                Entry::new(value.to_vec(), None),
            );
            db.entries
                .get_mut(ns)
                .unwrap()
                .get_mut(*key)
                .unwrap()
                .hits
                .store(*hits, Ordering::Relaxed);
        }
        Db::entry_size(ns, entries[0].0, value.len())
    }

    #[test]
    fn evict_for_write_returns_true_when_net_delta_zero() {
        let mut db = make_db_with_eviction(100, 1.0, EvictionPolicy::Lru);
        assert!(db.evict_for_write("ns", 0));
    }

    #[test]
    fn evict_for_write_returns_false_when_policy_none() {
        let mut db = make_db_with_eviction(100, 0.0, EvictionPolicy::None);
        assert!(!db.evict_for_write("ns", 5));
    }

    #[test]
    fn evict_for_write_returns_false_below_threshold() {
        // threshold=1.0 → threshold_bytes=100; used_bytes=6 < 100 → no eviction
        let mut db = make_db_with_eviction(100, 1.0, EvictionPolicy::Lru);
        // "ns"(2) + "a"(1) + "vvv"(3) = 6
        db.put(
            "ns",
            "a",
            Entry::new(b"vvv".to_vec(), None),
        );
        assert_eq!(db.used_bytes, 6);
        // net_delta=100: 6+100=106 > 100, but used_bytes=6 < threshold_bytes=100
        assert!(!db.evict_for_write("ns", 100));
    }

    #[test]
    fn evict_lru_evicts_lowest_hit_key_first() {
        // memory_limit=30, threshold=0.0 → always triggers
        let mut db = make_db_with_eviction(30, 0.0, EvictionPolicy::Lru);
        // Each entry: "ns"(2)+"x"(1)+"vvv"(3) = 6 bytes; 3 entries = 18 used
        let entry_size = populate_ns(&mut db, "ns", &[("a", 3), ("b", 1), ("c", 5)], b"vvv");
        assert_eq!(db.used_bytes, entry_size * 3);
        // net_delta=15: would use 33 > 30; overflow=3; b (hits=1) evicted first
        assert!(db.evict_for_write("ns", 15));
        assert!(
            db.entries.get("ns").unwrap().get("b").is_none(),
            "b should be evicted"
        );
        assert!(db.entries.get("ns").unwrap().get("a").is_some());
        assert!(db.entries.get("ns").unwrap().get("c").is_some());
    }

    #[test]
    fn evict_mru_evicts_highest_hit_key_first() {
        let mut db = make_db_with_eviction(30, 0.0, EvictionPolicy::Mru);
        populate_ns(&mut db, "ns", &[("a", 3), ("b", 1), ("c", 5)], b"vvv");
        // net_delta=15: overflow=3; c (hits=5) evicted first
        assert!(db.evict_for_write("ns", 15));
        assert!(
            db.entries.get("ns").unwrap().get("c").is_none(),
            "c should be evicted"
        );
        assert!(db.entries.get("ns").unwrap().get("a").is_some());
        assert!(db.entries.get("ns").unwrap().get("b").is_some());
    }

    #[test]
    fn evict_for_write_returns_false_when_not_enough_keys_to_evict() {
        let mut db = make_db_with_eviction(10, 0.0, EvictionPolicy::Lru);
        // "ns"(2)+"a"(1)+"vvv"(3) = 6 bytes; net_delta=10 → need 6+10=16, overflow=6
        // Only 6 bytes available to free (evict "a"), but 6 < overflow=6 → freed=6 >= overflow=6 → fits
        // Actually 6-6+10 = 10 <= 10 → true. Let's use a bigger delta.
        // net_delta=15: 6+15=21 > 10, overflow=11; only "a"(6) can be evicted → freed=6 < 11
        db.put(
            "ns",
            "a",
            Entry::new(b"vvv".to_vec(), None),
        );
        assert!(!db.evict_for_write("ns", 15));
    }

    #[test]
    fn evict_for_write_returns_true_when_exact_fit_after_eviction() {
        // memory_limit=12, threshold=0.0, LRU
        // "ns"(2)+"a"(1)+"vvv"(3)=6, "ns"(2)+"b"(1)+"vvv"(3)=6 → used=12
        // net_delta=6: 12+6=18, overflow=6; evict "a"(6) → used=6, 6+6=12 <= 12 → true
        let mut db = make_db_with_eviction(12, 0.0, EvictionPolicy::Lru);
        populate_ns(&mut db, "ns", &[("a", 0), ("b", 1)], b"vvv");
        assert_eq!(db.used_bytes, 12);
        assert!(db.evict_for_write("ns", 6));
        assert!(db.entries.get("ns").unwrap().get("a").is_none());
    }

    #[test]
    fn per_namespace_policy_overrides_global() {
        let mut ns_policies = HashMap::new();
        ns_policies.insert("special".to_owned(), EvictionPolicy::Mru);
        let db = Db::new(100).with_eviction(1.0, EvictionPolicy::Lru, ns_policies);
        assert_eq!(db.policy_for_namespace("special"), EvictionPolicy::Mru);
        assert_eq!(db.policy_for_namespace("other"), EvictionPolicy::Lru);
    }

    #[test]
    fn net_delta_new_key() {
        let db = Db::new(1000);
        // "ns"(2)+"k"(1)+"val"(3) = 6, old=0
        assert_eq!(db.net_delta("ns", "k", 3), 6);
    }

    #[test]
    fn net_delta_existing_key_grows() {
        let mut db = Db::new(1000);
        db.put(
            "ns",
            "k",
            Entry::new(b"hi".to_vec(), None),
        );
        // old: "ns"(2)+"k"(1)+"hi"(2)=5; new: "ns"(2)+"k"(1)+"hello"(5)=8; delta=3
        assert_eq!(db.net_delta("ns", "k", 5), 3);
    }

    #[test]
    fn net_delta_existing_key_shrinks_returns_zero() {
        let mut db = Db::new(1000);
        db.put(
            "ns",
            "k",
            Entry::new(b"hello".to_vec(), None),
        );
        // old=8, new=5: saturating_sub → 0
        assert_eq!(db.net_delta("ns", "k", 2), 0);
    }

    // ── EAR tests ─────────────────────────────────────────────────────────────

    #[test]
    fn tracks_hits_returns_false_for_ear() {
        let mut ns_policies = HashMap::new();
        ns_policies.insert("ns".to_owned(), EvictionPolicy::ExpireAfterRead);
        let db = Db::new(1000).with_eviction(1.0, EvictionPolicy::None, ns_policies);
        assert!(!db.tracks_hits("ns"));
    }

    #[test]
    fn tracks_hits_returns_true_for_lru_and_mru() {
        let db_lru = Db::new(1000).with_eviction(1.0, EvictionPolicy::Lru, HashMap::new());
        assert!(db_lru.tracks_hits("any"));
        let db_mru = Db::new(1000).with_eviction(1.0, EvictionPolicy::Mru, HashMap::new());
        assert!(db_mru.tracks_hits("any"));
    }

    #[test]
    fn evict_for_write_returns_false_for_ear() {
        let mut ns_policies = HashMap::new();
        ns_policies.insert("ns".to_owned(), EvictionPolicy::ExpireAfterRead);
        let mut db = Db::new(100).with_eviction(0.0, EvictionPolicy::None, ns_policies);
        assert!(!db.evict_for_write("ns", 5));
    }

    #[test]
    fn is_ear_namespace_returns_true_for_ear() {
        let mut ns_policies = HashMap::new();
        ns_policies.insert("session".to_owned(), EvictionPolicy::ExpireAfterRead);
        let db = Db::new(1000).with_eviction(1.0, EvictionPolicy::None, ns_policies);
        assert!(db.is_ear_namespace("session"));
        assert!(!db.is_ear_namespace("other"));
    }

    #[test]
    fn mark_ear_marks_existing_non_expired_key() {
        let mut db = Db::new(1000);
        db.put(
            "ns",
            "k",
            Entry::new(b"v".to_vec(), None),
        );
        db.mark_ear("ns", "k");
        assert!(
            db.ear_pending
                .contains(&("ns".to_owned(), "k".to_owned()))
        );
    }

    #[test]
    fn mark_ear_does_not_mark_missing_key() {
        let mut db = Db::new(1000);
        db.mark_ear("ns", "k");
        assert!(
            !db.ear_pending
                .contains(&("ns".to_owned(), "k".to_owned()))
        );
    }

    #[test]
    fn mark_ear_does_not_mark_expired_key() {
        let mut db = Db::new(1000);
        let expired = Entry {
            value: Value::String(b"v".to_vec()),
            hits: AtomicU64::new(0),
            expiry: Some(Instant::now() - Duration::from_secs(1)),
        };
        db.put("ns", "k", expired);
        db.mark_ear("ns", "k");
        assert!(
            !db.ear_pending
                .contains(&("ns".to_owned(), "k".to_owned()))
        );
    }

    #[test]
    fn put_clears_ear_mark() {
        let mut db = Db::new(1000);
        db.put(
            "ns",
            "k",
            Entry::new(b"v".to_vec(), None),
        );
        db.mark_ear("ns", "k");
        assert!(
            db.ear_pending
                .contains(&("ns".to_owned(), "k".to_owned()))
        );
        db.put(
            "ns",
            "k",
            Entry::new(b"v2".to_vec(), None),
        );
        assert!(
            !db.ear_pending
                .contains(&("ns".to_owned(), "k".to_owned()))
        );
    }

    #[test]
    fn delete_clears_ear_mark() {
        let mut db = Db::new(1000);
        db.put(
            "ns",
            "k",
            Entry::new(b"v".to_vec(), None),
        );
        db.mark_ear("ns", "k");
        db.delete("ns", "k");
        assert!(
            !db.ear_pending
                .contains(&("ns".to_owned(), "k".to_owned()))
        );
    }

    #[test]
    fn flush_all_clears_ear_marks() {
        let mut db = Db::new(1000);
        db.put(
            "ns",
            "k",
            Entry::new(b"v".to_vec(), None),
        );
        db.mark_ear("ns", "k");
        db.flush_all();
        assert!(db.ear_pending.is_empty());
    }
}
