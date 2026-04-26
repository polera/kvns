use std::cmp::Reverse;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
            // Count only the sorted vec (8 bytes for f64 score + member bytes).
            // The index HashMap is a derived structure; including it would
            // double-count every member and inflate OOM/eviction thresholds.
            Value::ZSet(data) => data.sorted.iter().map(|e| 8 + e.member.len()).sum::<usize>(),
        }
    }

    pub(crate) fn as_string(&self) -> Option<&[u8]> {
        match self {
            Value::String(b) => Some(b),
            _ => None,
        }
    }

    pub(crate) fn as_string_mut(&mut self) -> Option<&mut Vec<u8>> {
        match self {
            Value::String(b) => Some(b),
            _ => None,
        }
    }

    pub(crate) fn as_hash(&self) -> Option<&HashMap<Vec<u8>, Vec<u8>>> {
        match self {
            Value::Hash(m) => Some(m),
            _ => None,
        }
    }

    pub(crate) fn as_hash_mut(&mut self) -> Option<&mut HashMap<Vec<u8>, Vec<u8>>> {
        match self {
            Value::Hash(m) => Some(m),
            _ => None,
        }
    }

    pub(crate) fn as_set(&self) -> Option<&HashSet<Vec<u8>>> {
        match self {
            Value::Set(s) => Some(s),
            _ => None,
        }
    }

    pub(crate) fn as_set_mut(&mut self) -> Option<&mut HashSet<Vec<u8>>> {
        match self {
            Value::Set(s) => Some(s),
            _ => None,
        }
    }

    pub(crate) fn as_zset(&self) -> Option<&ZSetData> {
        match self {
            Value::ZSet(v) => Some(v),
            _ => None,
        }
    }

    pub(crate) fn as_zset_mut(&mut self) -> Option<&mut ZSetData> {
        match self {
            Value::ZSet(v) => Some(v),
            _ => None,
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

/// Container for [`Value`] inside an [`Entry`].  Chosen at startup via
/// [`set_shared_values`] based on the user's workload:
///
/// - `Inline`: the value is stored directly in the entry.  Cheapest possible
///   writes — no Arc allocation — but snapshotting (e.g. the periodic
///   persistence flush) must deep-copy every byte under the read lock.
/// - `Shared`: the value is wrapped in an `Arc`.  Each write allocates one
///   16-byte Arc header, but snapshots only bump refcounts, keeping the
///   persist-time tail latency bounded regardless of store size.
///
/// Both variants deref to `&Value`, so read-side code is variant-agnostic.
/// Mutation goes through [`ValueCell::as_mut`], which performs `Arc::make_mut`
/// in the `Shared` case to preserve copy-on-write semantics.
pub(crate) enum ValueCell {
    Inline(Value),
    Shared(Arc<Value>),
}

/// When true, new entries wrap their value in `Arc`; when false, values are
/// stored inline.  Set once at startup from config — read uncontended on
/// every write.
static SHARED_VALUES: AtomicBool = AtomicBool::new(true);

/// Toggle the variant picked by [`ValueCell::new`].  Call once at startup
/// after parsing config; later changes affect only subsequent constructions.
pub(crate) fn set_shared_values(enabled: bool) {
    SHARED_VALUES.store(enabled, Ordering::Relaxed);
}

pub(crate) fn shared_values_enabled() -> bool {
    SHARED_VALUES.load(Ordering::Relaxed)
}

impl ValueCell {
    /// Construct based on the process-wide sharing flag.
    pub(crate) fn new(value: Value) -> Self {
        if shared_values_enabled() {
            Self::Shared(Arc::new(value))
        } else {
            Self::Inline(value)
        }
    }

    /// Mutable access to the underlying value.  In the `Shared` variant this
    /// is `Arc::make_mut`, cloning the payload only when the Arc is held
    /// elsewhere (e.g. a persistence snapshot is in flight).
    pub(crate) fn as_mut(&mut self) -> &mut Value {
        match self {
            Self::Inline(v) => v,
            Self::Shared(a) => Arc::make_mut(a),
        }
    }
}

impl Deref for ValueCell {
    type Target = Value;
    fn deref(&self) -> &Value {
        match self {
            Self::Inline(v) => v,
            Self::Shared(a) => a,
        }
    }
}

impl Clone for ValueCell {
    fn clone(&self) -> Self {
        match self {
            // Deep clone of inline values matches the pre-Arc behaviour.
            Self::Inline(v) => Self::Inline(v.clone()),
            // Shared clone is a pointer bump — the persist-snapshot fast path.
            Self::Shared(a) => Self::Shared(Arc::clone(a)),
        }
    }
}

pub(crate) struct Entry {
    pub(crate) value: ValueCell,
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
            value: ValueCell::new(Value::String(value)),
            hits: AtomicU64::new(0),
            expiry: ttl.map(|d| Instant::now() + d),
        }
    }

    /// Mutable access to the underlying value; performs copy-on-write if the
    /// value is currently shared with a persistence snapshot.
    pub(crate) fn value_mut(&mut self) -> &mut Value {
        self.value.as_mut()
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
    /// Monotonically increasing write counter; bumped on every put/delete.
    pub(crate) write_version: u64,
    /// Last write version for each (namespace, key).  Used by WATCH to detect
    /// concurrent modifications between WATCH and EXEC.
    /// Nested to allow zero-allocation lookups via &str keys.
    pub(crate) key_versions: HashMap<String, HashMap<String, u64>>,
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
            write_version: 0,
            key_versions: HashMap::new(),
        }
    }

    /// Remove the entry if it exists and is expired. No-op otherwise.
    pub(crate) fn purge_if_expired(&mut self, ns: &str, key: &str) {
        let expired = self
            .entries
            .get(ns)
            .and_then(|m| m.get(key))
            .is_some_and(|e| e.is_expired());
        if expired {
            self.delete(ns, key);
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
        // 4. Evict in rounds.  Candidate buffer lives across rounds to amortise
        //    allocation; keys are still owned because we need `&mut self` to
        //    call `delete_deferred` after borrowing from `self.entries`.
        const EVICTION_SAMPLE_SIZE: usize = 32;
        const MAX_ROUNDS: usize = 20;
        let mut total_count = 0u64;
        let mut rounds = 0;
        let mut candidates: Vec<(String, u64, usize)> = Vec::with_capacity(EVICTION_SAMPLE_SIZE);
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
            candidates.clear();
            for (k, e) in ns_map.iter().take(EVICTION_SAMPLE_SIZE) {
                candidates.push((
                    k.clone(),
                    e.hits.load(Ordering::Relaxed),
                    Self::entry_size(namespace, k, e.value.byte_len()),
                ));
            }
            if candidates.is_empty() {
                break;
            }

            // 6. Sort sample by eviction order (sample is ≤ 32 items; plain sort
            //    is fine).
            match policy {
                EvictionPolicy::Lru => candidates.sort_unstable_by_key(|(_, hits, _)| *hits),
                EvictionPolicy::Mru => {
                    candidates.sort_unstable_by_key(|(_, hits, _)| Reverse(*hits))
                }
                EvictionPolicy::None | EvictionPolicy::ExpireAfterRead => unreachable!(),
            }

            // 7. Evict from sample until round's overflow is covered.
            // Use delete_deferred to avoid emitting Prometheus gauge updates on
            // every evicted key while the write lock is held; we emit once below.
            let mut freed = 0usize;
            for (key, _, size) in candidates.drain(..) {
                if freed >= overflow {
                    break;
                }
                self.delete_deferred(namespace, &key);
                freed += size;
                total_count += 1;
            }
            rounds += 1;
        }

        // 8. Trace log.
        tracing::debug!(namespace, rounds, "evicted keys");
        // 9. Emit one consolidated gauge snapshot after all evictions complete
        //    rather than one per deleted key.
        let ns_keys = self.entries.get(namespace).map(|ns| ns.len()).unwrap_or(0);
        let ns_bytes = self.namespace_bytes.get(namespace).copied().unwrap_or(0);
        StoreMetrics {
            namespace: namespace.to_owned(),
            ns_keys,
            ns_bytes,
            total_bytes: self.used_bytes,
        }
        .emit();
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
        self.write_version += 1;
        self.key_versions
            .entry(namespace.clone())
            .or_default()
            .insert(key.clone(), self.write_version);
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

    /// Stamp `(namespace, key)` with the next write version without going
    /// through `put_deferred`.  Call this in every command that mutates an
    /// existing entry in-place (HSET, SADD, LPUSH, etc.) so that WATCH
    /// correctly detects the modification.
    pub(crate) fn touch_key_version(&mut self, namespace: &str, key: &str) {
        self.write_version += 1;
        self.key_versions
            .entry(namespace.to_owned())
            .or_default()
            .insert(key.to_owned(), self.write_version);
    }

    /// Returns the write version recorded the last time `(namespace, key)` was
    /// written.  Returns `0` if the key has never been written.
    pub(crate) fn key_version(&self, namespace: &str, key: &str) -> u64 {
        self.key_versions
            .get(namespace)
            .and_then(|m| m.get(key))
            .copied()
            .unwrap_or(0)
    }

    /// Remove a key and return the removed entry along with a metrics snapshot.
    /// The caller is responsible for calling `.emit()` on the snapshot —
    /// ideally after the write lock has been released to avoid emitting
    /// Prometheus gauge updates while writers are blocked.
    ///
    /// Use `delete` when immediate emission is acceptable; use this variant
    /// inside hot loops (e.g. eviction) where deferring is critical.
    pub(crate) fn delete_deferred(
        &mut self,
        namespace: &str,
        key: &str,
    ) -> (Option<Entry>, StoreMetrics) {
        let ns_key = (namespace.to_owned(), key.to_owned());
        self.ear_pending.remove(&ns_key);
        self.write_version += 1;
        if let Some(m) = self.key_versions.get_mut(namespace) {
            m.remove(key);
        }
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
        let m = StoreMetrics {
            namespace: namespace.to_owned(),
            ns_keys,
            ns_bytes,
            total_bytes: self.used_bytes,
        };
        (removed, m)
    }

    pub(crate) fn delete(&mut self, namespace: &str, key: &str) -> Option<Entry> {
        let (entry, m) = self.delete_deferred(namespace, key);
        m.emit();
        entry
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
        self.write_version += 1;
        self.key_versions.clear();
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
            value: ValueCell::new(Value::String(b"v".to_vec())),
            hits: AtomicU64::new(0),
            expiry: Some(Instant::now() - Duration::from_secs(1)),
        };
        assert!(e.is_expired());
        assert_eq!(e.time_to_expiry_secs(), 0);
    }

    // ── ValueCell variant tests ──────────────────────────────────────────────
    //
    // Reads and mutations should behave identically regardless of the variant.
    // Construct each variant explicitly (bypassing the global flag) so these
    // tests don't race with other tests that flip `set_shared_values`.

    #[test]
    fn valuecell_inline_read_and_mutate() {
        let mut cell = ValueCell::Inline(Value::String(b"hello".to_vec()));
        assert_eq!(cell.as_string(), Some(b"hello".as_slice()));
        cell.as_mut().as_string_mut().unwrap().extend_from_slice(b" world");
        assert_eq!(cell.as_string(), Some(b"hello world".as_slice()));
    }

    #[test]
    fn valuecell_shared_read_and_mutate() {
        let mut cell = ValueCell::Shared(Arc::new(Value::String(b"hello".to_vec())));
        assert_eq!(cell.as_string(), Some(b"hello".as_slice()));
        cell.as_mut().as_string_mut().unwrap().extend_from_slice(b" world");
        assert_eq!(cell.as_string(), Some(b"hello world".as_slice()));
    }

    #[test]
    fn valuecell_shared_make_mut_is_cow() {
        // Two Entries sharing the same Arc — mutating one must not observably
        // affect the other.  This is the property that lets the persistence
        // snapshot safely hold a pointer while writers keep mutating.
        let arc = Arc::new(Value::String(b"orig".to_vec()));
        let snapshot_side = ValueCell::Shared(Arc::clone(&arc));
        let mut writer_side = ValueCell::Shared(arc);
        writer_side
            .as_mut()
            .as_string_mut()
            .unwrap()
            .extend_from_slice(b"-modified");
        assert_eq!(writer_side.as_string(), Some(b"orig-modified".as_slice()));
        assert_eq!(snapshot_side.as_string(), Some(b"orig".as_slice()));
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
            value: ValueCell::new(Value::String(b"v".to_vec())),
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
