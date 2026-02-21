use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use serde::{Deserialize, Serialize};
use tracing::{debug, error, info};

use crate::store::{Db, Entry, Store, Value};

// ── Serializable mirror types ─────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
struct PersistedEntry {
    value: Value,
    hits: u64,
    /// Milliseconds since the Unix epoch at which this entry expires.
    /// `None` means no expiry.
    expiry_unix_ms: Option<u64>,
}

#[derive(Serialize, Deserialize)]
struct PersistedDb {
    entries: HashMap<String, HashMap<String, PersistedEntry>>,
}

// ── Entry conversion helpers ──────────────────────────────────────────────────

fn entry_to_persisted(entry: &Entry) -> PersistedEntry {
    let expiry_unix_ms = entry.expiry.and_then(|instant| {
        let remaining = instant.saturating_duration_since(Instant::now());
        SystemTime::now()
            .checked_add(remaining)
            .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as u64)
    });
    PersistedEntry {
        value: entry.value.clone(),
        hits: entry.hits,
        expiry_unix_ms,
    }
}

/// Returns `None` for entries that have already expired.
fn persisted_to_entry(p: PersistedEntry) -> Option<Entry> {
    let expiry = match p.expiry_unix_ms {
        None => None,
        Some(ms) => {
            let target = SystemTime::UNIX_EPOCH + Duration::from_millis(ms);
            // duration_since returns Err if target is in the past (already expired).
            let remaining = target.duration_since(SystemTime::now()).ok()?;
            Some(Instant::now() + remaining)
        }
    };
    Some(Entry { value: p.value, hits: p.hits, expiry })
}

// ── Public I/O functions ──────────────────────────────────────────────────────

/// Atomically serialise `db` to `path` (write to `<path>.tmp`, then rename).
pub(crate) fn save(db: &Db, path: &Path) -> io::Result<()> {
    let persisted = PersistedDb {
        entries: db
            .entries
            .iter()
            .map(|(ns, ns_map)| {
                let persisted_ns = ns_map
                    .iter()
                    .map(|(key, entry)| (key.clone(), entry_to_persisted(entry)))
                    .collect();
                (ns.clone(), persisted_ns)
            })
            .collect(),
    };

    // Ensure the parent directory exists.
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let tmp = path.with_extension("tmp");
    let mut file = File::create(&tmp)?;
    bincode::serialize_into(&mut file, &persisted)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    file.flush()?;
    drop(file);
    fs::rename(&tmp, path)?;
    Ok(())
}

/// Deserialise a `Db` from `path`.  Expired entries are silently dropped.
///
/// Returns `Err` with `ErrorKind::NotFound` if the file does not exist, which
/// callers can use to distinguish "first run" from genuine I/O errors.
pub(crate) fn load(path: &Path, memory_limit: usize) -> io::Result<Db> {
    let file = File::open(path)?;
    let persisted: PersistedDb = bincode::deserialize_from(file)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let mut db = Db::new(memory_limit);
    for (ns, ns_map) in persisted.entries {
        for (key, p_entry) in ns_map {
            if let Some(entry) = persisted_to_entry(p_entry) {
                db.put(ns.clone(), key, entry);
            }
        }
    }
    Ok(db)
}

// ── Background flush task ─────────────────────────────────────────────────────

pub(crate) async fn run_periodic_flush(store: Store, path: PathBuf, interval_secs: u64) {
    info!(
        path = %path.display(),
        interval_secs,
        "persistence flush task started"
    );
    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
    ticker.tick().await; // skip the immediate first tick
    loop {
        ticker.tick().await;
        let db = store.read().await;
        match save(&db, &path) {
            Ok(()) => debug!(path = %path.display(), "flushed store to disk"),
            Err(e) => error!(error = %e, path = %path.display(), "failed to flush store to disk"),
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DEFAULT_MEMORY_LIMIT;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_path() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("kvns_test_{}_{}.bin", std::process::id(), n))
    }

    fn string_entry(val: &str) -> Entry {
        Entry::new(val.as_bytes().to_vec(), None)
    }

    // ── Conversion helpers ────────────────────────────────────────────────────

    #[test]
    fn entry_without_expiry_roundtrips() {
        let entry = string_entry("hello");
        let p = entry_to_persisted(&entry);
        assert!(p.expiry_unix_ms.is_none());
        let restored = persisted_to_entry(p).unwrap();
        assert_eq!(restored.value.as_string().unwrap(), b"hello");
        assert_eq!(restored.hits, 0);
        assert!(restored.expiry.is_none());
    }

    #[test]
    fn entry_with_future_ttl_roundtrips() {
        let mut entry = string_entry("v");
        entry.expiry = Some(Instant::now() + Duration::from_secs(3600));
        let p = entry_to_persisted(&entry);
        assert!(p.expiry_unix_ms.is_some());
        let restored = persisted_to_entry(p).unwrap();
        // Remaining TTL should be close to 3600 s
        let remaining = restored
            .expiry
            .unwrap()
            .saturating_duration_since(Instant::now());
        assert!(remaining.as_secs() > 3590, "TTL should be ~3600s, got {:?}", remaining);
    }

    #[test]
    fn expired_entry_is_dropped_on_load() {
        let mut entry = string_entry("v");
        // Already expired
        entry.expiry = Some(Instant::now() - Duration::from_secs(1));
        let p = entry_to_persisted(&entry);
        // expiry_unix_ms will be None because saturating_duration_since returns 0
        // for already-past instants, so SystemTime::now() + 0 is in the past.
        assert!(persisted_to_entry(p).is_none());
    }

    // ── Save / load roundtrip ─────────────────────────────────────────────────

    #[test]
    fn save_and_load_roundtrip() {
        let path = temp_path();
        let mut db = Db::new(DEFAULT_MEMORY_LIMIT);
        db.put("default".into(), "foo".into(), string_entry("bar"));
        db.put("ns1".into(), "x".into(), string_entry("42"));

        save(&db, &path).expect("save failed");
        let loaded = load(&path, DEFAULT_MEMORY_LIMIT).expect("load failed");

        assert_eq!(
            loaded.entries.get("default").and_then(|ns| ns.get("foo"))
                .and_then(|e| e.value.as_string()),
            Some(b"bar".as_slice())
        );
        assert_eq!(
            loaded.entries.get("ns1").and_then(|ns| ns.get("x"))
                .and_then(|e| e.value.as_string()),
            Some(b"42".as_slice())
        );
        assert_eq!(loaded.used_bytes, db.used_bytes);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn load_skips_expired_entries() {
        let path = temp_path();
        let mut db = Db::new(DEFAULT_MEMORY_LIMIT);
        db.put("default".into(), "live".into(), string_entry("v"));
        // Inject an already-expired entry directly (bypassing put's normal path)
        db.entries
            .entry("default".into())
            .or_default()
            .insert("dead".into(), Entry {
                value: Value::String(b"v".to_vec()),
                hits: 0,
                expiry: Some(Instant::now() - Duration::from_secs(1)),
            });

        save(&db, &path).expect("save failed");
        let loaded = load(&path, DEFAULT_MEMORY_LIMIT).expect("load failed");

        assert!(loaded.entries.get("default").and_then(|ns| ns.get("live")).is_some());
        assert!(loaded.entries.get("default").and_then(|ns| ns.get("dead")).is_none());

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn load_nonexistent_file_returns_not_found() {
        let path = temp_path(); // never created
        let result = load(&path, DEFAULT_MEMORY_LIMIT);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().kind(), io::ErrorKind::NotFound);
    }
}
