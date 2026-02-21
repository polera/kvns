use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

pub(crate) enum Value {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>),
}

impl Value {
    pub(crate) fn byte_len(&self) -> usize {
        match self {
            Value::String(b) => b.len(),
            Value::List(items) => items.iter().map(|b| b.len()).sum(),
        }
    }

    pub(crate) fn as_string(&self) -> Option<&[u8]> {
        match self {
            Value::String(b) => Some(b),
            Value::List(_) => None,
        }
    }
}

pub(crate) struct Entry {
    pub(crate) value: Value,
    pub(crate) hits: u64,
    pub(crate) expiry: Option<Instant>,
}

impl Entry {
    pub(crate) fn new(value: Vec<u8>, ttl: Option<Duration>) -> Self {
        Self {
            value: Value::String(value),
            hits: 0,
            expiry: ttl.map(|d| Instant::now() + d),
        }
    }

    pub(crate) fn is_expired(&self) -> bool {
        self.expiry.is_some_and(|e| Instant::now() >= e)
    }

    pub(crate) fn time_to_expiry_secs(&self) -> i64 {
        match self.expiry {
            None => 0,
            Some(e) => {
                let now = Instant::now();
                if e <= now { 0 } else { (e - now).as_secs() as i64 }
            }
        }
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
        }
    }

    /// Bytes attributed to one entry: namespace + key lengths + value payload.
    pub(crate) fn entry_size(namespace: &str, key: &str, value_byte_len: usize) -> usize {
        namespace.len() + key.len() + value_byte_len
    }

    pub(crate) fn would_exceed(&self, namespace: &str, key: &str, new_value_byte_len: usize) -> bool {
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

    pub(crate) fn put(&mut self, namespace: String, key: String, entry: Entry) {
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
        self.entries.entry(namespace.clone()).or_default().insert(key, entry);
        let ns_keys = self.entries[&namespace].len();
        metrics::gauge!("kvns_keys_total", "namespace" => namespace.clone()).set(ns_keys as f64);
        metrics::gauge!("kvns_memory_used_bytes", "namespace" => namespace).set(ns_bytes as f64);
        metrics::gauge!("kvns_memory_used_bytes_total").set(self.used_bytes as f64);
    }

    pub(crate) fn delete(&mut self, namespace: &str, key: &str) -> Option<Entry> {
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
        metrics::gauge!("kvns_memory_used_bytes", "namespace" => namespace.to_owned()).set(ns_bytes as f64);
        metrics::gauge!("kvns_memory_used_bytes_total").set(self.used_bytes as f64);
        removed
    }
}

pub(crate) type Store = Arc<RwLock<Db>>;

#[cfg(test)]
mod tests {
    use super::*;

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
            value: Value::String(b"v".to_vec()),
            hits: 0,
            expiry: Some(Instant::now() - Duration::from_secs(1)),
        };
        assert!(e.is_expired());
        assert_eq!(e.time_to_expiry_secs(), 0);
    }
}
