use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;
use tracing::debug;

use crate::resp::{
    resp_array, resp_bulk, resp_err, resp_int, resp_map, resp_null, resp_null_array, resp_ok,
    resp_pong, resp_verbatim, resp_wrongtype, wrong_args,
};
use crate::store::{Db, Entry, Store, Value, ZEntry};

// ── Connection state ──────────────────────────────────────────────────────────

pub(crate) struct ConnState {
    pub resp_version: u8,
    pub client_name: Option<Vec<u8>>,
    pub client_id: u64,
}

impl ConnState {
    pub(crate) fn new(id: u64) -> Self {
        Self {
            resp_version: 2,
            client_name: None,
            client_id: id,
        }
    }
}

// ── Namespace key parsing ─────────────────────────────────────────────────────

fn parse_ns_key(raw: &[u8]) -> (String, String) {
    let s = String::from_utf8_lossy(raw);
    match s.find('/') {
        Some(pos) => (s[..pos].to_owned(), s[pos + 1..].to_owned()),
        None => ("default".to_owned(), s.into_owned()),
    }
}

// ── Expiry task helper ────────────────────────────────────────────────────────

#[derive(Clone)]
struct ExpiryEvent {
    ns: String,
    key: String,
    deadline: Instant,
}

struct ScheduledExpiry {
    seq: u64,
    event: ExpiryEvent,
}

impl PartialEq for ScheduledExpiry {
    fn eq(&self, other: &Self) -> bool {
        self.seq == other.seq && self.event.deadline == other.event.deadline
    }
}

impl Eq for ScheduledExpiry {}

impl PartialOrd for ScheduledExpiry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScheduledExpiry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse order for min-heap behaviour on BinaryHeap.
        other
            .event
            .deadline
            .cmp(&self.event.deadline)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

static EXPIRY_QUEUE_TX_BY_STORE: LazyLock<
    Mutex<HashMap<usize, mpsc::UnboundedSender<ExpiryEvent>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn store_scheduler_key(store: &Store) -> usize {
    Arc::as_ptr(store) as usize
}

fn ensure_expiry_scheduler(store: &Store) -> mpsc::UnboundedSender<ExpiryEvent> {
    let key = store_scheduler_key(store);
    let mut guard = EXPIRY_QUEUE_TX_BY_STORE
        .lock()
        .expect("expiry scheduler mutex poisoned");
    let needs_init = match guard.get(&key) {
        Some(tx) => tx.is_closed(),
        None => true,
    };
    if needs_init {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(run_expiry_scheduler(Arc::clone(store), rx, key));
        guard.insert(key, tx);
    }
    guard
        .get(&key)
        .expect("expiry scheduler sender should be initialized")
        .clone()
}

fn schedule_expiry(store: &Store, ns: String, key: String, deadline: Instant) {
    let key_id = store_scheduler_key(store);
    for _ in 0..2 {
        let tx = ensure_expiry_scheduler(store);
        if tx
            .send(ExpiryEvent {
                ns: ns.clone(),
                key: key.clone(),
                deadline,
            })
            .is_ok()
        {
            return;
        }
        let mut guard = EXPIRY_QUEUE_TX_BY_STORE
            .lock()
            .expect("expiry scheduler mutex poisoned");
        guard.remove(&key_id);
    }
}

async fn run_expiry_scheduler(
    store: Store,
    mut rx: mpsc::UnboundedReceiver<ExpiryEvent>,
    key_id: usize,
) {
    let mut queue: BinaryHeap<ScheduledExpiry> = BinaryHeap::new();
    let mut seq = 0u64;

    loop {
        let now = Instant::now();
        while let Some(item) = queue.peek() {
            if item.event.deadline > now {
                break;
            }
            let item = queue.pop().expect("queue peeked but empty");
            expire_if_deadline_matches(&store, item.event).await;
        }

        let Some(next_deadline) = queue.peek().map(|item| item.event.deadline) else {
            match rx.recv().await {
                Some(event) => {
                    queue.push(ScheduledExpiry { seq, event });
                    seq = seq.wrapping_add(1);
                }
                None => break,
            }
            continue;
        };

        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(next_deadline)) => {}
            maybe_event = rx.recv() => {
                match maybe_event {
                    Some(event) => {
                        queue.push(ScheduledExpiry { seq, event });
                        seq = seq.wrapping_add(1);
                    }
                    None => {
                        while let Some(item) = queue.pop() {
                            expire_if_deadline_matches(&store, item.event).await;
                        }
                        break;
                    }
                }
            }
        }
    }

    let mut guard = EXPIRY_QUEUE_TX_BY_STORE
        .lock()
        .expect("expiry scheduler mutex poisoned");
    guard.remove(&key_id);
}

async fn expire_if_deadline_matches(store: &Store, event: ExpiryEvent) {
    let mut db = store.write().await;
    if db
        .entries
        .get(&event.ns)
        .and_then(|nsm| nsm.get(&event.key))
        .is_some_and(|e| e.expiry == Some(event.deadline))
    {
        debug!(namespace = %event.ns, key = %event.key, "expiring key");
        db.delete(&event.ns, &event.key);
    }
}

// ── EAR sweep ─────────────────────────────────────────────────────────────────

const EAR_SWEEP_INTERVAL_SECS: u64 = 1;

pub(crate) async fn run_ear_sweep(store: Store) {
    let mut interval =
        tokio::time::interval(tokio::time::Duration::from_secs(EAR_SWEEP_INTERVAL_SECS));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        // Snapshot pending set under a brief read lock.
        let pending: Vec<(String, String)> = {
            let db = store.read().await;
            if db.ear_pending.is_empty() {
                continue;
            }
            db.ear_pending.iter().cloned().collect()
        };
        // Delete in a single write lock pass.
        let mut db = store.write().await;
        let mut deleted = 0u64;
        for (ns, key) in &pending {
            // Re-check: a concurrent write may have cleared the mark via put().
            if db.ear_pending.contains(&(ns.clone(), key.clone()))
                && db
                    .entries
                    .get(ns)
                    .and_then(|m| m.get(key.as_str()))
                    .is_some_and(|e| !e.is_expired())
            {
                db.delete(ns, key);
                deleted += 1;
            }
        }
        if deleted > 0 {
            tracing::debug!(deleted, "EAR sweep deleted keys");
            metrics::counter!("kvns_ear_evictions_total").increment(deleted);
        }
    }
}

/// Call AFTER dropping any read lock. Acquires write lock only if needed.
async fn mark_ear_after_read(store: &Store, ns: &str, key: &str) {
    {
        let db = store.read().await;
        if !db.is_ear_namespace(ns) {
            return;
        }
    }
    let mut db = store.write().await;
    db.mark_ear(ns, key);
}

/// Mark multiple keys for EAR. Filters to only EAR namespaces.
async fn mark_ear_keys(store: &Store, keys: &[(String, String)]) {
    if keys.is_empty() {
        return;
    }
    let ear_ns: HashSet<String> = {
        let db = store.read().await;
        keys.iter()
            .filter(|(ns, _)| db.is_ear_namespace(ns))
            .map(|(ns, _)| ns.clone())
            .collect()
    };
    if ear_ns.is_empty() {
        return;
    }
    let mut db = store.write().await;
    for (ns, key) in keys {
        if ear_ns.contains(ns) {
            db.mark_ear(ns, key);
        }
    }
}

// ── Glob matching ─────────────────────────────────────────────────────────────

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

fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    match (pattern, text) {
        ([], []) => true,
        ([], _) => false,
        ([b'*', rest @ ..], _) => {
            glob_match(rest, text) || (!text.is_empty() && glob_match(pattern, &text[1..]))
        }
        (_, []) => false,
        ([b'?', p_rest @ ..], [_, t_rest @ ..]) => glob_match(p_rest, t_rest),
        ([b'[', p_rest @ ..], [ch, t_rest @ ..]) => match p_rest.iter().position(|&b| b == b']') {
            None => *ch == b'[' && glob_match(p_rest, t_rest),
            Some(end) => class_match(&p_rest[..end], *ch) && glob_match(&p_rest[end + 1..], t_rest),
        },
        ([p, p_rest @ ..], [t, t_rest @ ..]) => *p == *t && glob_match(p_rest, t_rest),
    }
}

// ── ZSet helpers ──────────────────────────────────────────────────────────────

fn zset_find_member_idx(entries: &[ZEntry], member: &[u8]) -> Option<usize> {
    entries.iter().position(|e| e.member == member)
}

/// Insert or update a member in the sorted set. Returns true if it was NEW.
fn zset_insert_or_update(entries: &mut Vec<ZEntry>, score: f64, member: Vec<u8>) -> bool {
    if let Some(idx) = zset_find_member_idx(entries, &member) {
        entries[idx].score = score;
        // Re-sort after update
        entries.sort_by(|a, b| {
            a.score
                .partial_cmp(&b.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.member.cmp(&b.member))
        });
        false
    } else {
        let pos = entries.partition_point(|e| {
            e.score < score || (e.score == score && e.member.as_slice() < member.as_slice())
        });
        entries.insert(pos, ZEntry { score, member });
        true
    }
}

fn parse_score_bound(s: &[u8]) -> Option<(f64, bool)> {
    if s.eq_ignore_ascii_case(b"+inf") || s == b"+\xE2\x88\x9E" {
        return Some((f64::INFINITY, false));
    }
    if s.eq_ignore_ascii_case(b"-inf") || s == b"-\xE2\x88\x9E" {
        return Some((f64::NEG_INFINITY, false));
    }
    if s.first() == Some(&b'(') {
        let n: f64 = std::str::from_utf8(&s[1..]).ok()?.parse().ok()?;
        Some((n, true))
    } else {
        let n: f64 = std::str::from_utf8(s).ok()?.parse().ok()?;
        Some((n, false))
    }
}

#[derive(Debug, Clone)]
enum LexBound {
    NegInf,
    PosInf,
    Included(Vec<u8>),
    Excluded(Vec<u8>),
}

fn parse_lex_bound(s: &[u8]) -> Option<LexBound> {
    if s == b"-" {
        return Some(LexBound::NegInf);
    }
    if s == b"+" {
        return Some(LexBound::PosInf);
    }
    match s.first() {
        Some(b'[') => Some(LexBound::Included(s[1..].to_vec())),
        Some(b'(') => Some(LexBound::Excluded(s[1..].to_vec())),
        _ => None,
    }
}

fn member_in_lex_range(member: &[u8], min: &LexBound, max: &LexBound) -> bool {
    let above_min = match min {
        LexBound::NegInf => true,
        LexBound::PosInf => false,
        LexBound::Included(v) => member >= v.as_slice(),
        LexBound::Excluded(v) => member > v.as_slice(),
    };
    let below_max = match max {
        LexBound::PosInf => true,
        LexBound::NegInf => false,
        LexBound::Included(v) => member <= v.as_slice(),
        LexBound::Excluded(v) => member < v.as_slice(),
    };
    above_min && below_max
}

fn format_score(s: f64) -> Vec<u8> {
    if s == f64::INFINITY {
        return b"inf".to_vec();
    }
    if s == f64::NEG_INFINITY {
        return b"-inf".to_vec();
    }
    format!("{}", s).into_bytes()
}

// ── OOM helper ────────────────────────────────────────────────────────────────

fn check_oom(db: &mut Db, ns: &str, key: &str, new_byte_len: usize) -> bool {
    if db.would_exceed(ns, key, new_byte_len) {
        let net_delta = db.net_delta(ns, key, new_byte_len);
        if !db.evict_for_write(ns, net_delta) {
            return false;
        }
    }
    true
}

fn resp_array_of_nulls(count: usize) -> Vec<u8> {
    let mut out = format!("*{}\r\n", count).into_bytes();
    for _ in 0..count {
        out.extend_from_slice(&resp_null());
    }
    out
}

async fn cleanup_expired_key(store: &Store, ns: &str, key: &str) {
    let mut db = store.write().await;
    if db
        .entries
        .get(ns)
        .and_then(|m| m.get(key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(ns, key);
    }
}

async fn cleanup_expired_keys(store: &Store, keys: &[(String, String)]) {
    if keys.is_empty() {
        return;
    }
    let mut db = store.write().await;
    for (ns, key) in keys {
        if db
            .entries
            .get(ns)
            .and_then(|m| m.get(key))
            .is_some_and(|e| e.is_expired())
        {
            db.delete(ns, key);
        }
    }
}

enum ZsetLookup {
    Missing,
    Expired,
    WrongType,
    Found(Vec<ZEntry>),
}

async fn read_zset_snapshot(store: &Store, ns: &str, key: &str) -> ZsetLookup {
    let db = store.read().await;
    match db.entries.get(ns).and_then(|m| m.get(key)) {
        None => ZsetLookup::Missing,
        Some(entry) if entry.is_expired() => ZsetLookup::Expired,
        Some(entry) => match entry.value.as_zset() {
            Some(z) => ZsetLookup::Found(z.clone()),
            None => ZsetLookup::WrongType,
        },
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// STRING COMMANDS
// ═══════════════════════════════════════════════════════════════════════════════

pub(crate) async fn cmd_set(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    let start = Instant::now();
    let ns = if args.len() >= 2 {
        parse_ns_key(&args[1]).0
    } else {
        "default".to_owned()
    };
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
        let amount: u64 = match String::from_utf8_lossy(&args[4]).parse() {
            Ok(v) => v,
            Err(_) => return resp_err("invalid expire time"),
        };
        Some(if args[3].eq_ignore_ascii_case(b"PX") {
            Duration::from_millis(amount)
        } else {
            Duration::from_secs(amount)
        })
    } else {
        None
    };

    let mut db = store.write().await;
    if db.would_exceed(&ns, &key, value.len()) {
        let net_delta = db.net_delta(&ns, &key, value.len());
        if !db.evict_for_write(&ns, net_delta) {
            return resp_err("OOM command not allowed when used memory > 'maxmemory'");
        }
    }
    let entry = Entry::new(value, ttl);
    let expiry = entry.expiry;
    debug!(namespace = %ns, key = %key, ttl = ?ttl, "SET");
    db.put(ns.clone(), key.clone(), entry);
    drop(db);

    if let Some(deadline) = expiry {
        schedule_expiry(store, ns, key, deadline);
    }

    resp_ok()
}

async fn cmd_get(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    enum ReadGet {
        Missing,
        Expired,
        WrongType,
        ValueNoHit(Vec<u8>),
        NeedsHitUpdate,
    }

    let read_state = {
        let db = store.read().await;
        match db.entries.get(&ns).and_then(|m| m.get(&key)) {
            None => ReadGet::Missing,
            Some(entry) if entry.is_expired() => ReadGet::Expired,
            Some(entry) => match entry.value.as_string() {
                None => ReadGet::WrongType,
                Some(bytes) => {
                    if db.tracks_hits(&ns) {
                        ReadGet::NeedsHitUpdate
                    } else {
                        ReadGet::ValueNoHit(bytes.to_vec())
                    }
                }
            },
        }
    };

    match read_state {
        ReadGet::Missing => return resp_null(),
        ReadGet::Expired => {
            cleanup_expired_key(store, &ns, &key).await;
            return resp_null();
        }
        ReadGet::WrongType => return resp_wrongtype(),
        ReadGet::ValueNoHit(value) => {
            mark_ear_after_read(store, &ns, &key).await;
            return resp_bulk(&value);
        }
        ReadGet::NeedsHitUpdate => {}
    }

    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_null();
    }
    match db.entries.get_mut(&ns).and_then(|m| m.get_mut(&key)) {
        None => resp_null(),
        Some(entry) => match entry.value.as_string() {
            None => resp_wrongtype(),
            Some(bytes) => {
                entry.hits += 1;
                resp_bulk(bytes)
            }
        },
    }
}

async fn cmd_mget(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let keys: Vec<(String, String)> = args[1..].iter().map(|a| parse_ns_key(a)).collect();
    let (out, expired_keys, found_keys) = {
        let db = store.read().await;
        let mut out = format!("*{}\r\n", keys.len()).into_bytes();
        let mut expired_keys: Vec<(String, String)> = Vec::new();
        let mut found_keys: Vec<(String, String)> = Vec::new();
        for (ns, key) in &keys {
            match db.entries.get(ns).and_then(|m| m.get(key)) {
                None => out.extend_from_slice(&resp_null()),
                Some(entry) if entry.is_expired() => {
                    expired_keys.push((ns.clone(), key.clone()));
                    out.extend_from_slice(&resp_null());
                }
                Some(entry) => match entry.value.as_string() {
                    None => out.extend_from_slice(&resp_null()),
                    Some(bytes) => {
                        out.extend_from_slice(&resp_bulk(bytes));
                        found_keys.push((ns.clone(), key.clone()));
                    }
                },
            }
        }
        (out, expired_keys, found_keys)
    };
    cleanup_expired_keys(store, &expired_keys).await;
    mark_ear_keys(store, &found_keys).await;
    out
}

async fn cmd_mset(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
        return wrong_args(&args[0]);
    }
    let mut db = store.write().await;
    let mut i = 1;
    while i + 1 < args.len() {
        let (ns, key) = parse_ns_key(&args[i]);
        let value = args[i + 1].clone();
        if !check_oom(&mut db, &ns, &key, value.len()) {
            return resp_err("OOM command not allowed when used memory > 'maxmemory'");
        }
        db.put(ns, key, Entry::new(value, None));
        i += 2;
    }
    resp_ok()
}

async fn cmd_msetnx(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
        return wrong_args(&args[0]);
    }
    let pairs: Vec<(String, String, Vec<u8>)> = args[1..]
        .chunks(2)
        .map(|c| {
            let (ns, key) = parse_ns_key(&c[0]);
            (ns, key, c[1].clone())
        })
        .collect();

    let mut db = store.write().await;
    // Check that none of the keys exist (non-expired)
    for (ns, key, _) in &pairs {
        if let Some(e) = db.entries.get(ns).and_then(|m| m.get(key.as_str()))
            && !e.is_expired()
        {
            return resp_int(0);
        }
    }
    for (ns, key, value) in pairs {
        if !check_oom(&mut db, &ns, &key, value.len()) {
            return resp_err("OOM command not allowed when used memory > 'maxmemory'");
        }
        db.put(ns, key, Entry::new(value, None));
    }
    resp_int(1)
}

async fn cmd_setnx(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let value = args[2].clone();
    let mut db = store.write().await;
    if let Some(e) = db.entries.get(&ns).and_then(|m| m.get(&key))
        && !e.is_expired()
    {
        return resp_int(0);
    }
    if !check_oom(&mut db, &ns, &key, value.len()) {
        return resp_err("OOM command not allowed when used memory > 'maxmemory'");
    }
    db.put(ns, key, Entry::new(value, None));
    resp_int(1)
}

async fn cmd_getset(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let new_value = args[2].clone();
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
    }
    let old = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => resp_null(),
        Some(entry) => match entry.value.as_string() {
            None => return resp_wrongtype(),
            Some(bytes) => resp_bulk(bytes),
        },
    };
    if !check_oom(&mut db, &ns, &key, new_value.len()) {
        return resp_err("OOM command not allowed when used memory > 'maxmemory'");
    }
    db.put(ns, key, Entry::new(new_value, None));
    old
}

async fn cmd_getdel(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_null();
    }
    // Clone value before releasing borrow, then delete.
    let val = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => return resp_null(),
        Some(entry) => match entry.value.as_string() {
            None => return resp_wrongtype(),
            Some(bytes) => resp_bulk(bytes),
        },
    };
    db.delete(&ns, &key);
    val
}

async fn cmd_getex(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_null();
    }
    // Get value first
    let (val_bytes, _old_expiry) = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => return resp_null(),
        Some(entry) => match entry.value.as_string() {
            None => return resp_wrongtype(),
            Some(bytes) => (bytes.to_vec(), entry.expiry),
        },
    };
    let response = resp_bulk(&val_bytes);

    // Parse options
    if args.len() == 2 {
        return response;
    }
    let opt = String::from_utf8_lossy(&args[2]).to_ascii_uppercase();
    let new_expiry: Option<Option<Instant>> = match opt.as_str() {
        "PERSIST" => Some(None),
        "EX" => {
            if args.len() < 4 {
                return resp_err("syntax error");
            }
            match std::str::from_utf8(&args[3])
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
            {
                None => return resp_err("invalid expire time"),
                Some(n) => Some(Some(Instant::now() + Duration::from_secs(n))),
            }
        }
        "PX" => {
            if args.len() < 4 {
                return resp_err("syntax error");
            }
            match std::str::from_utf8(&args[3])
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
            {
                None => return resp_err("invalid expire time"),
                Some(n) => Some(Some(Instant::now() + Duration::from_millis(n))),
            }
        }
        "EXAT" => {
            if args.len() < 4 {
                return resp_err("syntax error");
            }
            match std::str::from_utf8(&args[3])
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
            {
                None => return resp_err("invalid expire time"),
                Some(unix_secs) => {
                    let target = SystemTime::UNIX_EPOCH + Duration::from_secs(unix_secs);
                    let deadline = match target.duration_since(SystemTime::now()) {
                        Ok(d) => Some(Instant::now() + d),
                        Err(_) => Some(Instant::now()),
                    };
                    Some(deadline)
                }
            }
        }
        "PXAT" => {
            if args.len() < 4 {
                return resp_err("syntax error");
            }
            match std::str::from_utf8(&args[3])
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
            {
                None => return resp_err("invalid expire time"),
                Some(unix_ms) => {
                    let target = SystemTime::UNIX_EPOCH + Duration::from_millis(unix_ms);
                    let deadline = match target.duration_since(SystemTime::now()) {
                        Ok(d) => Some(Instant::now() + d),
                        Err(_) => Some(Instant::now()),
                    };
                    Some(deadline)
                }
            }
        }
        _ => return resp_err("syntax error"),
    };

    if let Some(new_exp) = new_expiry
        && let Some(entry) = db.entries.get_mut(&ns).and_then(|m| m.get_mut(&key))
    {
        entry.expiry = new_exp;
        let deadline = new_exp;
        drop(db);
        if let Some(d) = deadline {
            schedule_expiry(store, ns, key, d);
        }
    }
    response
}

async fn cmd_append(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let append_data = args[2].clone();
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
    }
    let existing_len: usize = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => 0,
        Some(e) => match e.value.as_string() {
            None => return resp_wrongtype(),
            Some(b) => b.len(),
        },
    };
    let new_len = existing_len + append_data.len();
    if !check_oom(&mut db, &ns, &key, new_len) {
        return resp_err("OOM command not allowed when used memory > 'maxmemory'");
    }
    let entry = db
        .entries
        .entry(ns.clone())
        .or_default()
        .entry(key.clone())
        .or_insert_with(|| Entry::new(vec![], None));
    match entry.value.as_string_mut() {
        None => return resp_wrongtype(),
        Some(b) => b.extend_from_slice(&append_data),
    }
    let result_len = entry.value.as_string().unwrap().len();
    let delta = Db::entry_size(&ns, &key, result_len).saturating_sub(Db::entry_size(
        &ns,
        &key,
        existing_len,
    ));
    db.used_bytes = db.used_bytes.saturating_add(delta);
    let nb = db.namespace_bytes.entry(ns.clone()).or_insert(0);
    *nb = nb.saturating_add(delta);
    let ns_bytes = *nb;
    metrics::gauge!("kvns_memory_used_bytes", "namespace" => ns).set(ns_bytes as f64);
    metrics::gauge!("kvns_memory_used_bytes_total").set(db.used_bytes as f64);
    resp_int(result_len as i64)
}

async fn cmd_strlen(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => resp_int(0),
        Some(entry) => match entry.value.as_string() {
            None => resp_wrongtype(),
            Some(bytes) => resp_int(bytes.len() as i64),
        },
    }
}

pub(crate) async fn cmd_incr(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
    }
    let current: i64 = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
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
    if !check_oom(&mut db, &ns, &key, new_value.len()) {
        return resp_err("OOM command not allowed when used memory > 'maxmemory'");
    }
    db.put(ns, key, Entry::new(new_value, None));
    resp_int(next)
}

async fn cmd_incrby(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let by: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err("value is not an integer or out of range"),
    };
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
    }
    let current: i64 = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => 0,
        Some(entry) => match entry.value.as_string() {
            None => return resp_wrongtype(),
            Some(bytes) => match std::str::from_utf8(bytes).ok().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return resp_err("value is not an integer or out of range"),
            },
        },
    };
    let next = match current.checked_add(by) {
        Some(n) => n,
        None => return resp_err("increment would overflow"),
    };
    let new_value = next.to_string().into_bytes();
    if !check_oom(&mut db, &ns, &key, new_value.len()) {
        return resp_err("OOM command not allowed when used memory > 'maxmemory'");
    }
    db.put(ns, key, Entry::new(new_value, None));
    resp_int(next)
}

async fn cmd_decr(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
    }
    let current: i64 = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => 0,
        Some(entry) => match entry.value.as_string() {
            None => return resp_wrongtype(),
            Some(bytes) => match std::str::from_utf8(bytes).ok().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return resp_err("value is not an integer or out of range"),
            },
        },
    };
    let next = match current.checked_sub(1) {
        Some(n) => n,
        None => return resp_err("decrement would overflow"),
    };
    let new_value = next.to_string().into_bytes();
    if !check_oom(&mut db, &ns, &key, new_value.len()) {
        return resp_err("OOM command not allowed when used memory > 'maxmemory'");
    }
    db.put(ns, key, Entry::new(new_value, None));
    resp_int(next)
}

async fn cmd_decrby(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let by: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err("value is not an integer or out of range"),
    };
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
    }
    let current: i64 = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => 0,
        Some(entry) => match entry.value.as_string() {
            None => return resp_wrongtype(),
            Some(bytes) => match std::str::from_utf8(bytes).ok().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return resp_err("value is not an integer or out of range"),
            },
        },
    };
    let next = match current.checked_sub(by) {
        Some(n) => n,
        None => return resp_err("decrement would overflow"),
    };
    let new_value = next.to_string().into_bytes();
    if !check_oom(&mut db, &ns, &key, new_value.len()) {
        return resp_err("OOM command not allowed when used memory > 'maxmemory'");
    }
    db.put(ns, key, Entry::new(new_value, None));
    resp_int(next)
}

async fn cmd_incrbyfloat(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let by: f64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err("value is not a valid float"),
    };
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
    }
    let current: f64 = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => 0.0,
        Some(entry) => match entry.value.as_string() {
            None => return resp_wrongtype(),
            Some(bytes) => match std::str::from_utf8(bytes).ok().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return resp_err("value is not a valid float"),
            },
        },
    };
    let next = current + by;
    if next.is_nan() || next.is_infinite() {
        return resp_err("increment would produce NaN or Infinity");
    }
    let new_value = format!("{}", next).into_bytes();
    if !check_oom(&mut db, &ns, &key, new_value.len()) {
        return resp_err("OOM command not allowed when used memory > 'maxmemory'");
    }
    db.put(ns, key, Entry::new(new_value.clone(), None));
    resp_bulk(&new_value)
}

async fn cmd_setrange(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let offset: usize = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err("value is not an integer or out of range"),
    };
    let replacement = args[3].clone();
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
    }
    let existing: Vec<u8> = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => vec![],
        Some(entry) => match entry.value.as_string() {
            None => return resp_wrongtype(),
            Some(bytes) => bytes.to_vec(),
        },
    };
    let needed_len = offset + replacement.len();
    let mut new_val = existing;
    if new_val.len() < needed_len {
        new_val.resize(needed_len, 0);
    }
    new_val[offset..offset + replacement.len()].copy_from_slice(&replacement);
    let new_len = new_val.len();
    if !check_oom(&mut db, &ns, &key, new_len) {
        return resp_err("OOM command not allowed when used memory > 'maxmemory'");
    }
    db.put(ns, key, Entry::new(new_val, None));
    resp_int(new_len as i64)
}

async fn cmd_getrange(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let start_i: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err("value is not an integer or out of range"),
    };
    let end_i: i64 = match std::str::from_utf8(&args[3])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err("value is not an integer or out of range"),
    };
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_bulk(b"");
    }
    let (resp, mark) = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => (resp_bulk(b""), false),
        Some(entry) => match entry.value.as_string() {
            None => return resp_wrongtype(),
            Some(bytes) => {
                let len = bytes.len() as i64;
                let start = if start_i < 0 {
                    (len + start_i).max(0) as usize
                } else {
                    start_i.min(len) as usize
                };
                let end = if end_i < 0 {
                    (len + end_i).max(0) as usize
                } else {
                    end_i.min(len - 1).max(0) as usize
                };
                if start > end || bytes.is_empty() {
                    (resp_bulk(b""), false)
                } else {
                    (resp_bulk(&bytes[start..=end.min(bytes.len() - 1)]), true)
                }
            }
        },
    };
    if mark {
        db.mark_ear(&ns, &key);
    }
    resp
}

// ═══════════════════════════════════════════════════════════════════════════════
// LIST COMMANDS
// ═══════════════════════════════════════════════════════════════════════════════

pub(crate) async fn cmd_lpush(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let new_items = &args[2..];
    let mut db = store.write().await;

    let (existing_byte_len, is_new_key) = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => (0usize, true),
        Some(e) => match &e.value {
            Value::List(l) => (l.iter().map(|v| v.len()).sum(), false),
            _ => return resp_wrongtype(),
        },
    };

    let added_byte_len: usize = new_items.iter().map(|v| v.len()).sum();
    let old_size = if is_new_key {
        0
    } else {
        Db::entry_size(&ns, &key, existing_byte_len)
    };
    let new_size = Db::entry_size(&ns, &key, existing_byte_len + added_byte_len);
    let net_delta = new_size.saturating_sub(old_size);
    if db.used_bytes.saturating_add(net_delta) > db.memory_limit
        && !db.evict_for_write(&ns, net_delta)
    {
        return resp_err("OOM command not allowed when used memory > 'maxmemory'");
    }

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
        _ => unreachable!(),
    };
    for item in new_items.iter() {
        list.push_front(item.clone());
    }
    let len = list.len();
    db.used_bytes = db.used_bytes.saturating_add(net_delta);
    let nb = db.namespace_bytes.entry(ns.clone()).or_insert(0);
    *nb = nb.saturating_add(net_delta);
    let ns_bytes = *nb;
    let ns_keys = db.entries.get(&ns).map(|m| m.len()).unwrap_or(0);
    metrics::gauge!("kvns_keys_total", "namespace" => ns.clone()).set(ns_keys as f64);
    metrics::gauge!("kvns_memory_used_bytes", "namespace" => ns.clone()).set(ns_bytes as f64);
    metrics::gauge!("kvns_memory_used_bytes_total").set(db.used_bytes as f64);
    resp_int(len as i64)
}

async fn cmd_rpush(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let new_items = &args[2..];
    let mut db = store.write().await;

    let (existing_byte_len, is_new_key) = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => (0usize, true),
        Some(e) => match &e.value {
            Value::List(l) => (l.iter().map(|v| v.len()).sum(), false),
            _ => return resp_wrongtype(),
        },
    };

    let added_byte_len: usize = new_items.iter().map(|v| v.len()).sum();
    let old_size = if is_new_key {
        0
    } else {
        Db::entry_size(&ns, &key, existing_byte_len)
    };
    let new_size = Db::entry_size(&ns, &key, existing_byte_len + added_byte_len);
    let net_delta = new_size.saturating_sub(old_size);
    if db.used_bytes.saturating_add(net_delta) > db.memory_limit
        && !db.evict_for_write(&ns, net_delta)
    {
        return resp_err("OOM command not allowed when used memory > 'maxmemory'");
    }

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
        _ => unreachable!(),
    };
    for item in new_items.iter() {
        list.push_back(item.clone());
    }
    let len = list.len();
    db.used_bytes = db.used_bytes.saturating_add(net_delta);
    let nb = db.namespace_bytes.entry(ns.clone()).or_insert(0);
    *nb = nb.saturating_add(net_delta);
    let ns_bytes = *nb;
    let ns_keys = db.entries.get(&ns).map(|m| m.len()).unwrap_or(0);
    metrics::gauge!("kvns_keys_total", "namespace" => ns.clone()).set(ns_keys as f64);
    metrics::gauge!("kvns_memory_used_bytes", "namespace" => ns.clone()).set(ns_bytes as f64);
    metrics::gauge!("kvns_memory_used_bytes_total").set(db.used_bytes as f64);
    resp_int(len as i64)
}

async fn cmd_lpushx(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
    }
    if db.entries.get(&ns).and_then(|m| m.get(&key)).is_none() {
        return resp_int(0);
    }
    drop(db);
    cmd_lpush(args, store).await
}

async fn cmd_rpushx(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
    }
    if db.entries.get(&ns).and_then(|m| m.get(&key)).is_none() {
        return resp_int(0);
    }
    drop(db);
    cmd_rpush(args, store).await
}

async fn cmd_lpop(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 2 || args.len() > 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let count: Option<usize> = if args.len() == 3 {
        match std::str::from_utf8(&args[2])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(n) => Some(n),
            None => return resp_err("value is not an integer or out of range"),
        }
    } else {
        None
    };

    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return if count.is_some() {
            resp_null_array()
        } else {
            resp_null()
        };
    }
    // Pop and record whether list became empty, releasing borrow before possible delete.
    enum LPopResult {
        Null,
        WrongType,
        Single(Vec<u8>, bool),
        Multi(Vec<Vec<u8>>, bool),
    }
    let outcome = match db.entries.get_mut(&ns).and_then(|m| m.get_mut(&key)) {
        None => LPopResult::Null,
        Some(entry) => match &mut entry.value {
            Value::List(list) => {
                if let Some(c) = count {
                    let popped: Vec<Vec<u8>> = (0..c).filter_map(|_| list.pop_front()).collect();
                    let empty = list.is_empty();
                    LPopResult::Multi(popped, empty)
                } else {
                    match list.pop_front() {
                        None => LPopResult::Null,
                        Some(val) => {
                            let empty = list.is_empty();
                            LPopResult::Single(val, empty)
                        }
                    }
                }
            }
            _ => LPopResult::WrongType,
        },
    };
    match outcome {
        LPopResult::Null => {
            if count.is_some() {
                resp_null_array()
            } else {
                resp_null()
            }
        }
        LPopResult::WrongType => resp_wrongtype(),
        LPopResult::Single(val, empty) => {
            if empty {
                db.delete(&ns, &key);
            }
            resp_bulk(&val)
        }
        LPopResult::Multi(popped, empty) => {
            if empty {
                db.delete(&ns, &key);
            }
            resp_array(&popped)
        }
    }
}

async fn cmd_rpop(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 2 || args.len() > 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let count: Option<usize> = if args.len() == 3 {
        match std::str::from_utf8(&args[2])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(n) => Some(n),
            None => return resp_err("value is not an integer or out of range"),
        }
    } else {
        None
    };

    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return if count.is_some() {
            resp_null_array()
        } else {
            resp_null()
        };
    }
    enum RPopResult {
        Null,
        WrongType,
        Single(Vec<u8>, bool),
        Multi(Vec<Vec<u8>>, bool),
    }
    let outcome = match db.entries.get_mut(&ns).and_then(|m| m.get_mut(&key)) {
        None => RPopResult::Null,
        Some(entry) => match &mut entry.value {
            Value::List(list) => {
                if let Some(c) = count {
                    let popped: Vec<Vec<u8>> = (0..c).filter_map(|_| list.pop_back()).collect();
                    let empty = list.is_empty();
                    RPopResult::Multi(popped, empty)
                } else {
                    match list.pop_back() {
                        None => RPopResult::Null,
                        Some(val) => {
                            let empty = list.is_empty();
                            RPopResult::Single(val, empty)
                        }
                    }
                }
            }
            _ => RPopResult::WrongType,
        },
    };
    match outcome {
        RPopResult::Null => {
            if count.is_some() {
                resp_null_array()
            } else {
                resp_null()
            }
        }
        RPopResult::WrongType => resp_wrongtype(),
        RPopResult::Single(val, empty) => {
            if empty {
                db.delete(&ns, &key);
            }
            resp_bulk(&val)
        }
        RPopResult::Multi(popped, empty) => {
            if empty {
                db.delete(&ns, &key);
            }
            resp_array(&popped)
        }
    }
}

async fn cmd_llen(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => resp_int(0),
        Some(entry) => match &entry.value {
            Value::List(l) => resp_int(l.len() as i64),
            _ => resp_wrongtype(),
        },
    }
}

async fn cmd_lrange(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let start_i: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err("value is not an integer or out of range"),
    };
    let stop_i: i64 = match std::str::from_utf8(&args[3])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err("value is not an integer or out of range"),
    };
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_array(&[]);
    }
    let (resp, mark) = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => (resp_array(&[]), false),
        Some(entry) => match &entry.value {
            Value::List(list) => {
                let len = list.len() as i64;
                let start = if start_i < 0 {
                    (len + start_i).max(0)
                } else {
                    start_i.min(len)
                } as usize;
                let stop = if stop_i < 0 {
                    (len + stop_i).max(0)
                } else {
                    stop_i.min(len - 1)
                } as usize;
                if start > stop || list.is_empty() {
                    (resp_array(&[]), false)
                } else {
                    let items: Vec<Vec<u8>> = list
                        .iter()
                        .skip(start)
                        .take(stop - start + 1)
                        .cloned()
                        .collect();
                    (resp_array(&items), true)
                }
            }
            _ => return resp_wrongtype(),
        },
    };
    if mark {
        db.mark_ear(&ns, &key);
    }
    resp
}

async fn cmd_lindex(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let idx_i: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err("value is not an integer or out of range"),
    };
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_null();
    }
    let (resp, mark) = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => (resp_null(), false),
        Some(entry) => match &entry.value {
            Value::List(list) => {
                let len = list.len() as i64;
                let idx = if idx_i < 0 { len + idx_i } else { idx_i };
                if idx < 0 || idx >= len {
                    (resp_null(), false)
                } else {
                    (resp_bulk(&list[idx as usize]), true)
                }
            }
            _ => return resp_wrongtype(),
        },
    };
    if mark {
        db.mark_ear(&ns, &key);
    }
    resp
}

async fn cmd_lset(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let idx_i: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err("value is not an integer or out of range"),
    };
    let new_val = args[3].clone();
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_err("ERR no such key");
    }
    match db.entries.get_mut(&ns).and_then(|m| m.get_mut(&key)) {
        None => resp_err("ERR no such key"),
        Some(entry) => match &mut entry.value {
            Value::List(list) => {
                let len = list.len() as i64;
                let idx = if idx_i < 0 { len + idx_i } else { idx_i };
                if idx < 0 || idx >= len {
                    resp_err("ERR index out of range")
                } else {
                    list[idx as usize] = new_val;
                    resp_ok()
                }
            }
            _ => resp_wrongtype(),
        },
    }
}

async fn cmd_lrem(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let count_i: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err("value is not an integer or out of range"),
    };
    let element = args[3].clone();
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    match db.entries.get_mut(&ns).and_then(|m| m.get_mut(&key)) {
        None => resp_int(0),
        Some(entry) => match &mut entry.value {
            Value::List(list) => {
                let mut removed = 0i64;
                if count_i == 0 {
                    let before = list.len();
                    list.retain(|e| e != &element);
                    removed = (before - list.len()) as i64;
                } else if count_i > 0 {
                    let mut to_remove = count_i;
                    list.retain(|e| {
                        if to_remove > 0 && e == &element {
                            to_remove -= 1;
                            removed += 1;
                            false
                        } else {
                            true
                        }
                    });
                } else {
                    // count < 0: remove from tail
                    let to_remove = (-count_i) as usize;
                    let len = list.len();
                    let mut indices: Vec<usize> =
                        (0..len).filter(|&i| list[i] == element).collect();
                    indices.reverse();
                    for idx in indices.iter().take(to_remove) {
                        list.remove(*idx);
                        removed += 1;
                    }
                }
                resp_int(removed)
            }
            _ => resp_wrongtype(),
        },
    }
}

async fn cmd_ltrim(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let start_i: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err("value is not an integer or out of range"),
    };
    let stop_i: i64 = match std::str::from_utf8(&args[3])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err("value is not an integer or out of range"),
    };
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_ok();
    }
    match db.entries.get_mut(&ns).and_then(|m| m.get_mut(&key)) {
        None => resp_ok(),
        Some(entry) => match &mut entry.value {
            Value::List(list) => {
                let len = list.len() as i64;
                let start = if start_i < 0 {
                    (len + start_i).max(0)
                } else {
                    start_i.min(len)
                } as usize;
                let stop = if stop_i < 0 {
                    (len + stop_i).max(0)
                } else {
                    stop_i.min(len - 1)
                } as usize;
                if start > stop as usize || list.is_empty() {
                    list.clear();
                } else {
                    let new_list: VecDeque<Vec<u8>> = list
                        .iter()
                        .skip(start)
                        .take(stop - start + 1)
                        .cloned()
                        .collect();
                    *list = new_list;
                }
                resp_ok()
            }
            _ => resp_wrongtype(),
        },
    }
}

async fn cmd_linsert(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 5 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let position = String::from_utf8_lossy(&args[2]).to_ascii_uppercase();
    let pivot = args[3].clone();
    let element = args[4].clone();
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    match db.entries.get_mut(&ns).and_then(|m| m.get_mut(&key)) {
        None => resp_int(0),
        Some(entry) => match &mut entry.value {
            Value::List(list) => match list.iter().position(|e| e == &pivot) {
                None => resp_int(-1),
                Some(idx) => {
                    let insert_at = if position == "AFTER" { idx + 1 } else { idx };
                    list.insert(insert_at, element);
                    resp_int(list.len() as i64)
                }
            },
            _ => resp_wrongtype(),
        },
    }
}

async fn cmd_lpos(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let element = args[2].clone();
    let mut rank: i64 = 1;
    let mut count_opt: Option<usize> = None;
    let mut i = 3;
    while i + 1 < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
        match opt.as_str() {
            "RANK" => {
                rank = match std::str::from_utf8(&args[i + 1])
                    .ok()
                    .and_then(|s| s.parse().ok())
                {
                    Some(n) => n,
                    None => return resp_err("value is not an integer or out of range"),
                };
            }
            "COUNT" => {
                count_opt = match std::str::from_utf8(&args[i + 1])
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                {
                    Some(n) => Some(n),
                    None => return resp_err("value is not an integer or out of range"),
                };
            }
            _ => {}
        }
        i += 2;
    }
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_null();
    }
    match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => resp_null(),
        Some(entry) => match &entry.value {
            Value::List(list) => {
                let mut results: Vec<Vec<u8>> = Vec::new();
                let mut match_count = 0i64;
                let forward = rank > 0;
                let skip = if rank > 0 { rank - 1 } else { (-rank) - 1 } as usize;

                if forward {
                    for (idx, item) in list.iter().enumerate() {
                        if item == &element {
                            match_count += 1;
                            if match_count > skip as i64 {
                                results.push(format!("{}", idx).into_bytes());
                                if let Some(c) = count_opt {
                                    if c > 0 && results.len() >= c {
                                        break;
                                    }
                                } else {
                                    break;
                                }
                            }
                        }
                    }
                } else {
                    for (idx, item) in list.iter().enumerate().rev() {
                        if item == &element {
                            match_count += 1;
                            if match_count > skip as i64 {
                                results.push(format!("{}", idx).into_bytes());
                                if let Some(c) = count_opt {
                                    if c > 0 && results.len() >= c {
                                        break;
                                    }
                                } else {
                                    break;
                                }
                            }
                        }
                    }
                }

                if count_opt.is_some() {
                    resp_array(&results)
                } else if results.is_empty() {
                    resp_null()
                } else {
                    resp_bulk(&results[0])
                }
            }
            _ => resp_wrongtype(),
        },
    }
}

async fn cmd_lmove(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 5 {
        return wrong_args(&args[0]);
    }
    let (src_ns, src_key) = parse_ns_key(&args[1]);
    let (dst_ns, dst_key) = parse_ns_key(&args[2]);
    let src_dir = String::from_utf8_lossy(&args[3]).to_ascii_uppercase();
    let dst_dir = String::from_utf8_lossy(&args[4]).to_ascii_uppercase();
    let mut db = store.write().await;

    if db
        .entries
        .get(&src_ns)
        .and_then(|m| m.get(&src_key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&src_ns, &src_key);
        return resp_null();
    }

    let element = match db
        .entries
        .get_mut(&src_ns)
        .and_then(|m| m.get_mut(&src_key))
    {
        None => return resp_null(),
        Some(entry) => match &mut entry.value {
            Value::List(list) => {
                let el = if src_dir == "LEFT" {
                    list.pop_front()
                } else {
                    list.pop_back()
                };
                match el {
                    None => return resp_null(),
                    Some(v) => v,
                }
            }
            _ => return resp_wrongtype(),
        },
    };

    let result = resp_bulk(&element);

    // Insert into dst
    match db
        .entries
        .get_mut(&dst_ns)
        .and_then(|m| m.get_mut(&dst_key))
    {
        Some(entry) => match &mut entry.value {
            Value::List(list) => {
                if dst_dir == "LEFT" {
                    list.push_front(element);
                } else {
                    list.push_back(element);
                }
            }
            _ => return resp_wrongtype(),
        },
        None => {
            let mut new_list = VecDeque::new();
            if dst_dir == "LEFT" {
                new_list.push_front(element);
            } else {
                new_list.push_back(element);
            }
            db.entries.entry(dst_ns.clone()).or_default().insert(
                dst_key.clone(),
                Entry {
                    value: Value::List(new_list),
                    hits: 0,
                    expiry: None,
                },
            );
        }
    }

    result
}

// ═══════════════════════════════════════════════════════════════════════════════
// HASH COMMANDS
// ═══════════════════════════════════════════════════════════════════════════════

async fn cmd_hset(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 4 || !(args.len() - 2).is_multiple_of(2) {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = args[2..]
        .chunks(2)
        .map(|c| (c[0].clone(), c[1].clone()))
        .collect();
    let mut db = store.write().await;

    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
    }

    let existing_byte_len: usize = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => 0,
        Some(e) => match e.value.as_hash() {
            None => return resp_wrongtype(),
            Some(h) => h.iter().map(|(k, v)| k.len() + v.len()).sum(),
        },
    };

    let added_len: usize = pairs.iter().map(|(k, v)| k.len() + v.len()).sum();
    let new_approx = existing_byte_len + added_len;
    if !check_oom(&mut db, &ns, &key, new_approx) {
        return resp_err("OOM command not allowed when used memory > 'maxmemory'");
    }

    let entry = db
        .entries
        .entry(ns.clone())
        .or_default()
        .entry(key.clone())
        .or_insert_with(|| Entry {
            value: Value::Hash(HashMap::new()),
            hits: 0,
            expiry: None,
        });

    let hash = match entry.value.as_hash_mut() {
        Some(h) => h,
        None => return resp_wrongtype(),
    };

    let mut new_fields = 0i64;
    for (field, value) in pairs {
        if hash.insert(field, value).is_none() {
            new_fields += 1;
        }
    }

    // Update memory accounting
    let new_byte_len: usize = hash.iter().map(|(k, v)| k.len() + v.len()).sum();
    let old_size = Db::entry_size(&ns, &key, existing_byte_len);
    let new_size = Db::entry_size(&ns, &key, new_byte_len);
    let delta = new_size.saturating_sub(old_size);
    db.used_bytes = db.used_bytes.saturating_add(delta);
    let nb = db.namespace_bytes.entry(ns.clone()).or_insert(0);
    *nb = nb.saturating_add(delta);
    let ns_bytes = *nb;
    let ns_keys = db.entries.get(&ns).map(|m| m.len()).unwrap_or(0);
    metrics::gauge!("kvns_keys_total", "namespace" => ns.clone()).set(ns_keys as f64);
    metrics::gauge!("kvns_memory_used_bytes", "namespace" => ns.clone()).set(ns_bytes as f64);
    metrics::gauge!("kvns_memory_used_bytes_total").set(db.used_bytes as f64);

    resp_int(new_fields)
}

async fn cmd_hget(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let field = &args[2];
    let (resp, expired, field_found) = {
        let db = store.read().await;
        match db.entries.get(&ns).and_then(|m| m.get(&key)) {
            None => (resp_null(), false, false),
            Some(entry) if entry.is_expired() => (resp_null(), true, false),
            Some(entry) => match entry.value.as_hash() {
                None => (resp_wrongtype(), false, false),
                Some(h) => match h.get(field.as_slice()) {
                    None => (resp_null(), false, false),
                    Some(v) => (resp_bulk(v), false, true),
                },
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    }
    if field_found {
        mark_ear_after_read(store, &ns, &key).await;
    }
    resp
}

async fn cmd_hdel(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let fields = &args[2..];
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    match db.entries.get_mut(&ns).and_then(|m| m.get_mut(&key)) {
        None => resp_int(0),
        Some(entry) => match entry.value.as_hash_mut() {
            None => resp_wrongtype(),
            Some(h) => {
                let mut removed = 0i64;
                for f in fields {
                    if h.remove(f.as_slice()).is_some() {
                        removed += 1;
                    }
                }
                resp_int(removed)
            }
        },
    }
}

async fn cmd_hexists(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let field = &args[2];
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => resp_int(0),
        Some(entry) => match entry.value.as_hash() {
            None => resp_wrongtype(),
            Some(h) => resp_int(if h.contains_key(field.as_slice()) {
                1
            } else {
                0
            }),
        },
    }
}

async fn cmd_hgetall(args: &[Vec<u8>], store: &Store, conn: &ConnState) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let empty = if conn.resp_version >= 3 {
        b"%0\r\n".to_vec()
    } else {
        resp_array(&[])
    };
    let (resp, expired, found) = {
        let db = store.read().await;
        match db.entries.get(&ns).and_then(|m| m.get(&key)) {
            None => (empty, false, false),
            Some(entry) if entry.is_expired() => (empty, true, false),
            Some(entry) => match entry.value.as_hash() {
                None => (resp_wrongtype(), false, false),
                Some(h) => {
                    if conn.resp_version >= 3 {
                        let pairs: Vec<(Vec<u8>, Vec<u8>)> =
                            h.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                        (resp_map(&pairs), false, true)
                    } else {
                        let mut flat: Vec<Vec<u8>> = Vec::with_capacity(h.len() * 2);
                        for (k, v) in h {
                            flat.push(k.clone());
                            flat.push(v.clone());
                        }
                        (resp_array(&flat), false, true)
                    }
                }
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    }
    if found {
        mark_ear_after_read(store, &ns, &key).await;
    }
    resp
}

async fn cmd_hkeys(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let (resp, expired, found) = {
        let db = store.read().await;
        match db.entries.get(&ns).and_then(|m| m.get(&key)) {
            None => (resp_array(&[]), false, false),
            Some(entry) if entry.is_expired() => (resp_array(&[]), true, false),
            Some(entry) => match entry.value.as_hash() {
                None => (resp_wrongtype(), false, false),
                Some(h) => {
                    let keys: Vec<Vec<u8>> = h.keys().cloned().collect();
                    (resp_array(&keys), false, true)
                }
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    }
    if found {
        mark_ear_after_read(store, &ns, &key).await;
    }
    resp
}

async fn cmd_hvals(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let (resp, expired, found) = {
        let db = store.read().await;
        match db.entries.get(&ns).and_then(|m| m.get(&key)) {
            None => (resp_array(&[]), false, false),
            Some(entry) if entry.is_expired() => (resp_array(&[]), true, false),
            Some(entry) => match entry.value.as_hash() {
                None => (resp_wrongtype(), false, false),
                Some(h) => {
                    let vals: Vec<Vec<u8>> = h.values().cloned().collect();
                    (resp_array(&vals), false, true)
                }
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    }
    if found {
        mark_ear_after_read(store, &ns, &key).await;
    }
    resp
}

async fn cmd_hlen(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let (resp, expired) = {
        let db = store.read().await;
        match db.entries.get(&ns).and_then(|m| m.get(&key)) {
            None => (resp_int(0), false),
            Some(entry) if entry.is_expired() => (resp_int(0), true),
            Some(entry) => match entry.value.as_hash() {
                None => (resp_wrongtype(), false),
                Some(h) => (resp_int(h.len() as i64), false),
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    }
    resp
}

async fn cmd_hmget(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let fields = &args[2..];
    let nulls = resp_array_of_nulls(fields.len());
    let (resp, expired, found) = {
        let db = store.read().await;
        match db.entries.get(&ns).and_then(|m| m.get(&key)) {
            None => (nulls, false, false),
            Some(entry) if entry.is_expired() => (nulls, true, false),
            Some(entry) => match entry.value.as_hash() {
                None => (resp_wrongtype(), false, false),
                Some(h) => {
                    let mut out = format!("*{}\r\n", fields.len()).into_bytes();
                    for f in fields {
                        match h.get(f.as_slice()) {
                            None => out.extend_from_slice(&resp_null()),
                            Some(v) => out.extend_from_slice(&resp_bulk(v)),
                        }
                    }
                    (out, false, true)
                }
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    }
    if found {
        mark_ear_after_read(store, &ns, &key).await;
    }
    resp
}

async fn cmd_hincrby(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let field = args[2].clone();
    let by: i64 = match std::str::from_utf8(&args[3])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err("value is not an integer or out of range"),
    };
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
    }

    let current: i64 = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => 0,
        Some(entry) => match entry.value.as_hash() {
            None => return resp_wrongtype(),
            Some(h) => match h.get(field.as_slice()) {
                None => 0,
                Some(v) => match std::str::from_utf8(v).ok().and_then(|s| s.parse().ok()) {
                    Some(n) => n,
                    None => return resp_err("hash value is not an integer"),
                },
            },
        },
    };

    let next = match current.checked_add(by) {
        Some(n) => n,
        None => return resp_err("increment would overflow"),
    };
    let new_val = next.to_string().into_bytes();

    let entry = db
        .entries
        .entry(ns.clone())
        .or_default()
        .entry(key.clone())
        .or_insert_with(|| Entry {
            value: Value::Hash(HashMap::new()),
            hits: 0,
            expiry: None,
        });
    match entry.value.as_hash_mut() {
        Some(h) => {
            h.insert(field, new_val);
        }
        None => return resp_wrongtype(),
    }
    resp_int(next)
}

async fn cmd_hincrbyfloat(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let field = args[2].clone();
    let by: f64 = match std::str::from_utf8(&args[3])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err("value is not a valid float"),
    };
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
    }

    let current: f64 = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => 0.0,
        Some(entry) => match entry.value.as_hash() {
            None => return resp_wrongtype(),
            Some(h) => match h.get(field.as_slice()) {
                None => 0.0,
                Some(v) => match std::str::from_utf8(v).ok().and_then(|s| s.parse().ok()) {
                    Some(n) => n,
                    None => return resp_err("hash value is not a float"),
                },
            },
        },
    };

    let next = current + by;
    if next.is_nan() || next.is_infinite() {
        return resp_err("increment would produce NaN or Infinity");
    }
    let new_val = format!("{}", next).into_bytes();

    let entry = db
        .entries
        .entry(ns.clone())
        .or_default()
        .entry(key.clone())
        .or_insert_with(|| Entry {
            value: Value::Hash(HashMap::new()),
            hits: 0,
            expiry: None,
        });
    match entry.value.as_hash_mut() {
        Some(h) => {
            h.insert(field, new_val.clone());
        }
        None => return resp_wrongtype(),
    }
    resp_bulk(&new_val)
}

async fn cmd_hrandfield(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 2 || args.len() > 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let count_opt: Option<i64> = if args.len() >= 3 {
        match std::str::from_utf8(&args[2])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(n) => Some(n),
            None => return resp_err("value is not an integer or out of range"),
        }
    } else {
        None
    };
    let withvalues =
        args.len() >= 4 && String::from_utf8_lossy(&args[3]).to_ascii_uppercase() == "WITHVALUES";

    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return if count_opt.is_some() {
            resp_array(&[])
        } else {
            resp_null()
        };
    }
    let (resp, mark) = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => {
            if count_opt.is_some() {
                (resp_array(&[]), false)
            } else {
                (resp_null(), false)
            }
        }
        Some(entry) => match entry.value.as_hash() {
            None => return resp_wrongtype(),
            Some(h) => {
                // Clone to release borrow on db before potential mark_ear call.
                let fields: Vec<(Vec<u8>, Vec<u8>)> =
                    h.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                if fields.is_empty() {
                    if count_opt.is_some() {
                        (resp_array(&[]), false)
                    } else {
                        (resp_null(), false)
                    }
                } else {
                    match count_opt {
                        None => (resp_bulk(&fields[0].0), true),
                        Some(count) => {
                            let abs_count = count.unsigned_abs() as usize;
                            let allow_repeat = count < 0;
                            let mut result: Vec<Vec<u8>> = Vec::new();
                            if allow_repeat {
                                for i in 0..abs_count {
                                    let idx = i % fields.len();
                                    result.push(fields[idx].0.clone());
                                    if withvalues {
                                        result.push(fields[idx].1.clone());
                                    }
                                }
                            } else {
                                let take = abs_count.min(fields.len());
                                for (field, value) in fields.iter().take(take) {
                                    result.push(field.clone());
                                    if withvalues {
                                        result.push(value.clone());
                                    }
                                }
                            }
                            (resp_array(&result), true)
                        }
                    }
                }
            }
        },
    };
    if mark {
        db.mark_ear(&ns, &key);
    }
    resp
}

// ═══════════════════════════════════════════════════════════════════════════════
// SET COMMANDS
// ═══════════════════════════════════════════════════════════════════════════════

async fn cmd_sadd(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let members = &args[2..];
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
    }

    let existing_byte_len: usize = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => 0,
        Some(e) => match e.value.as_set() {
            None => return resp_wrongtype(),
            Some(s) => s.iter().map(|v| v.len()).sum(),
        },
    };
    let added_len: usize = members.iter().map(|m| m.len()).sum();
    if !check_oom(&mut db, &ns, &key, existing_byte_len + added_len) {
        return resp_err("OOM command not allowed when used memory > 'maxmemory'");
    }

    let entry = db
        .entries
        .entry(ns.clone())
        .or_default()
        .entry(key.clone())
        .or_insert_with(|| Entry {
            value: Value::Set(HashSet::new()),
            hits: 0,
            expiry: None,
        });
    let set = match entry.value.as_set_mut() {
        Some(s) => s,
        None => return resp_wrongtype(),
    };

    let before = set.len();
    for m in members {
        set.insert(m.clone());
    }
    let added = (set.len() - before) as i64;

    // Update memory
    let new_byte_len: usize = set.iter().map(|v| v.len()).sum();
    let old_size = Db::entry_size(&ns, &key, existing_byte_len);
    let new_size = Db::entry_size(&ns, &key, new_byte_len);
    let delta = new_size.saturating_sub(old_size);
    db.used_bytes = db.used_bytes.saturating_add(delta);
    let nb = db.namespace_bytes.entry(ns.clone()).or_insert(0);
    *nb = nb.saturating_add(delta);
    let ns_bytes = *nb;
    metrics::gauge!("kvns_memory_used_bytes", "namespace" => ns.clone()).set(ns_bytes as f64);
    metrics::gauge!("kvns_memory_used_bytes_total").set(db.used_bytes as f64);

    resp_int(added)
}

async fn cmd_srem(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let members = &args[2..];
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    match db.entries.get_mut(&ns).and_then(|m| m.get_mut(&key)) {
        None => resp_int(0),
        Some(entry) => match entry.value.as_set_mut() {
            None => resp_wrongtype(),
            Some(s) => {
                let mut removed = 0i64;
                for m in members {
                    if s.remove(m.as_slice()) {
                        removed += 1;
                    }
                }
                resp_int(removed)
            }
        },
    }
}

async fn cmd_smembers(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_array(&[]);
    }
    let (resp, mark) = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => (resp_array(&[]), false),
        Some(entry) => match entry.value.as_set() {
            None => return resp_wrongtype(),
            Some(s) => {
                let mut members: Vec<Vec<u8>> = s.iter().cloned().collect();
                members.sort();
                (resp_array(&members), true)
            }
        },
    };
    if mark {
        db.mark_ear(&ns, &key);
    }
    resp
}

async fn cmd_scard(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => resp_int(0),
        Some(entry) => match entry.value.as_set() {
            None => resp_wrongtype(),
            Some(s) => resp_int(s.len() as i64),
        },
    }
}

async fn cmd_sismember(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let member = &args[2];
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    let (resp, mark) = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => (resp_int(0), false),
        Some(entry) => match entry.value.as_set() {
            None => return resp_wrongtype(),
            Some(s) => {
                let found = s.contains(member.as_slice());
                (resp_int(if found { 1 } else { 0 }), found)
            }
        },
    };
    if mark {
        db.mark_ear(&ns, &key);
    }
    resp
}

async fn cmd_smismember(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let members = &args[2..];
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        let mut out = format!("*{}\r\n", members.len()).into_bytes();
        for _ in members {
            out.extend_from_slice(&resp_int(0));
        }
        return out;
    }
    let (resp, mark) = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => {
            let mut out = format!("*{}\r\n", members.len()).into_bytes();
            for _ in members {
                out.extend_from_slice(&resp_int(0));
            }
            (out, false)
        }
        Some(entry) => match entry.value.as_set() {
            None => return resp_wrongtype(),
            Some(s) => {
                let mut out = format!("*{}\r\n", members.len()).into_bytes();
                for m in members {
                    out.extend_from_slice(&resp_int(if s.contains(m.as_slice()) { 1 } else { 0 }));
                }
                (out, true)
            }
        },
    };
    if mark {
        db.mark_ear(&ns, &key);
    }
    resp
}

async fn cmd_sunion(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let mut db = store.write().await;
    let mut result: HashSet<Vec<u8>> = HashSet::new();
    for raw_key in &args[1..] {
        let (ns, key) = parse_ns_key(raw_key);
        if db
            .entries
            .get(&ns)
            .and_then(|m| m.get(&key))
            .is_some_and(|e| e.is_expired())
        {
            db.delete(&ns, &key);
            continue;
        }
        match db.entries.get(&ns).and_then(|m| m.get(&key)) {
            None => {}
            Some(entry) => match entry.value.as_set() {
                None => return resp_wrongtype(),
                Some(s) => {
                    result.extend(s.iter().cloned());
                }
            },
        }
    }
    let mut members: Vec<Vec<u8>> = result.into_iter().collect();
    members.sort();
    resp_array(&members)
}

async fn cmd_sinter(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let mut db = store.write().await;
    let mut sets: Vec<HashSet<Vec<u8>>> = Vec::new();
    for raw_key in &args[1..] {
        let (ns, key) = parse_ns_key(raw_key);
        if db
            .entries
            .get(&ns)
            .and_then(|m| m.get(&key))
            .is_some_and(|e| e.is_expired())
        {
            db.delete(&ns, &key);
            return resp_array(&[]);
        }
        match db.entries.get(&ns).and_then(|m| m.get(&key)) {
            None => return resp_array(&[]),
            Some(entry) => match entry.value.as_set() {
                None => return resp_wrongtype(),
                Some(s) => sets.push(s.clone()),
            },
        }
    }
    if sets.is_empty() {
        return resp_array(&[]);
    }
    let first = sets[0].clone();
    let result: HashSet<Vec<u8>> = sets[1..]
        .iter()
        .fold(first, |acc, s| acc.intersection(s).cloned().collect());
    let mut members: Vec<Vec<u8>> = result.into_iter().collect();
    members.sort();
    resp_array(&members)
}

async fn cmd_sdiff(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let mut db = store.write().await;
    let mut sets: Vec<HashSet<Vec<u8>>> = Vec::new();
    for raw_key in &args[1..] {
        let (ns, key) = parse_ns_key(raw_key);
        if db
            .entries
            .get(&ns)
            .and_then(|m| m.get(&key))
            .is_some_and(|e| e.is_expired())
        {
            db.delete(&ns, &key);
            sets.push(HashSet::new());
            continue;
        }
        match db.entries.get(&ns).and_then(|m| m.get(&key)) {
            None => sets.push(HashSet::new()),
            Some(entry) => match entry.value.as_set() {
                None => return resp_wrongtype(),
                Some(s) => sets.push(s.clone()),
            },
        }
    }
    if sets.is_empty() {
        return resp_array(&[]);
    }
    let first = sets[0].clone();
    let result: HashSet<Vec<u8>> = sets[1..]
        .iter()
        .fold(first, |acc, s| acc.difference(s).cloned().collect());
    let mut members: Vec<Vec<u8>> = result.into_iter().collect();
    members.sort();
    resp_array(&members)
}

async fn cmd_sunionstore(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (dst_ns, dst_key) = parse_ns_key(&args[1]);
    let mut new_args = vec![args[0].clone()];
    new_args.extend_from_slice(&args[2..]);
    // get union result
    let mut db = store.write().await;
    let mut result: HashSet<Vec<u8>> = HashSet::new();
    for raw_key in &args[2..] {
        let (ns, key) = parse_ns_key(raw_key);
        if db
            .entries
            .get(&ns)
            .and_then(|m| m.get(&key))
            .is_some_and(|e| e.is_expired())
        {
            db.delete(&ns, &key);
            continue;
        }
        match db.entries.get(&ns).and_then(|m| m.get(&key)) {
            None => {}
            Some(entry) => match entry.value.as_set() {
                None => return resp_wrongtype(),
                Some(s) => {
                    result.extend(s.iter().cloned());
                }
            },
        }
    }
    let count = result.len() as i64;
    let entry = Entry {
        value: Value::Set(result),
        hits: 0,
        expiry: None,
    };
    db.put(dst_ns, dst_key, entry);
    resp_int(count)
}

async fn cmd_sinterstore(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (dst_ns, dst_key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    let mut sets: Vec<HashSet<Vec<u8>>> = Vec::new();
    for raw_key in &args[2..] {
        let (ns, key) = parse_ns_key(raw_key);
        if db
            .entries
            .get(&ns)
            .and_then(|m| m.get(&key))
            .is_some_and(|e| e.is_expired())
        {
            db.delete(&ns, &key);
            sets.push(HashSet::new());
            continue;
        }
        match db.entries.get(&ns).and_then(|m| m.get(&key)) {
            None => sets.push(HashSet::new()),
            Some(entry) => match entry.value.as_set() {
                None => return resp_wrongtype(),
                Some(s) => sets.push(s.clone()),
            },
        }
    }
    let result: HashSet<Vec<u8>> = if sets.is_empty() {
        HashSet::new()
    } else {
        let first = sets[0].clone();
        sets[1..]
            .iter()
            .fold(first, |acc, s| acc.intersection(s).cloned().collect())
    };
    let count = result.len() as i64;
    db.put(
        dst_ns,
        dst_key,
        Entry {
            value: Value::Set(result),
            hits: 0,
            expiry: None,
        },
    );
    resp_int(count)
}

async fn cmd_sdiffstore(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (dst_ns, dst_key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    let mut sets: Vec<HashSet<Vec<u8>>> = Vec::new();
    for raw_key in &args[2..] {
        let (ns, key) = parse_ns_key(raw_key);
        if db
            .entries
            .get(&ns)
            .and_then(|m| m.get(&key))
            .is_some_and(|e| e.is_expired())
        {
            db.delete(&ns, &key);
            sets.push(HashSet::new());
            continue;
        }
        match db.entries.get(&ns).and_then(|m| m.get(&key)) {
            None => sets.push(HashSet::new()),
            Some(entry) => match entry.value.as_set() {
                None => return resp_wrongtype(),
                Some(s) => sets.push(s.clone()),
            },
        }
    }
    let result: HashSet<Vec<u8>> = if sets.is_empty() {
        HashSet::new()
    } else {
        let first = sets[0].clone();
        sets[1..]
            .iter()
            .fold(first, |acc, s| acc.difference(s).cloned().collect())
    };
    let count = result.len() as i64;
    db.put(
        dst_ns,
        dst_key,
        Entry {
            value: Value::Set(result),
            hits: 0,
            expiry: None,
        },
    );
    resp_int(count)
}

async fn cmd_smove(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (src_ns, src_key) = parse_ns_key(&args[1]);
    let (dst_ns, dst_key) = parse_ns_key(&args[2]);
    let member = args[3].clone();
    let mut db = store.write().await;

    if db
        .entries
        .get(&src_ns)
        .and_then(|m| m.get(&src_key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&src_ns, &src_key);
        return resp_int(0);
    }

    let found = match db
        .entries
        .get_mut(&src_ns)
        .and_then(|m| m.get_mut(&src_key))
    {
        None => return resp_int(0),
        Some(entry) => match entry.value.as_set_mut() {
            None => return resp_wrongtype(),
            Some(s) => s.remove(member.as_slice()),
        },
    };

    if !found {
        return resp_int(0);
    }

    // Insert into dst
    match db
        .entries
        .get_mut(&dst_ns)
        .and_then(|m| m.get_mut(&dst_key))
    {
        Some(entry) => match entry.value.as_set_mut() {
            Some(s) => {
                s.insert(member);
            }
            None => return resp_wrongtype(),
        },
        None => {
            let mut s = HashSet::new();
            s.insert(member);
            db.entries.entry(dst_ns.clone()).or_default().insert(
                dst_key.clone(),
                Entry {
                    value: Value::Set(s),
                    hits: 0,
                    expiry: None,
                },
            );
        }
    }
    resp_int(1)
}

async fn cmd_spop(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 2 || args.len() > 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let count: Option<usize> = if args.len() == 3 {
        match std::str::from_utf8(&args[2])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(n) => Some(n),
            None => return resp_err("value is not an integer or out of range"),
        }
    } else {
        None
    };

    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return if count.is_some() {
            resp_array(&[])
        } else {
            resp_null()
        };
    }

    match db.entries.get_mut(&ns).and_then(|m| m.get_mut(&key)) {
        None => {
            if count.is_some() {
                resp_array(&[])
            } else {
                resp_null()
            }
        }
        Some(entry) => match entry.value.as_set_mut() {
            None => resp_wrongtype(),
            Some(s) => {
                if let Some(c) = count {
                    let popped: Vec<Vec<u8>> = s.iter().take(c).cloned().collect();
                    for m in &popped {
                        s.remove(m.as_slice());
                    }
                    resp_array(&popped)
                } else {
                    // pop one
                    let member = s.iter().next().cloned();
                    match member {
                        None => resp_null(),
                        Some(m) => {
                            s.remove(m.as_slice());
                            resp_bulk(&m)
                        }
                    }
                }
            }
        },
    }
}

async fn cmd_srandmember(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 2 || args.len() > 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let count_opt: Option<i64> = if args.len() == 3 {
        match std::str::from_utf8(&args[2])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(n) => Some(n),
            None => return resp_err("value is not an integer or out of range"),
        }
    } else {
        None
    };

    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return if count_opt.is_some() {
            resp_array(&[])
        } else {
            resp_null()
        };
    }

    let (resp, mark) = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => {
            if count_opt.is_some() {
                (resp_array(&[]), false)
            } else {
                (resp_null(), false)
            }
        }
        Some(entry) => match entry.value.as_set() {
            None => return resp_wrongtype(),
            Some(s) => {
                // Clone members to release borrow before potential mark_ear call.
                let mut members: Vec<Vec<u8>> = s.iter().cloned().collect();
                members.sort();
                match count_opt {
                    None => {
                        if members.is_empty() {
                            (resp_null(), false)
                        } else {
                            (resp_bulk(&members[0]), true)
                        }
                    }
                    Some(count) => {
                        let abs_count = count.unsigned_abs() as usize;
                        let allow_repeat = count < 0;
                        let mut result: Vec<Vec<u8>> = Vec::new();
                        if allow_repeat {
                            for i in 0..abs_count {
                                let idx = i % members.len().max(1);
                                result.push(members[idx].clone());
                            }
                        } else {
                            let take = abs_count.min(members.len());
                            for member in members.iter().take(take) {
                                result.push(member.clone());
                            }
                        }
                        (resp_array(&result), !result.is_empty())
                    }
                }
            }
        },
    };
    if mark {
        db.mark_ear(&ns, &key);
    }
    resp
}

// ═══════════════════════════════════════════════════════════════════════════════
// ZSET COMMANDS
// ═══════════════════════════════════════════════════════════════════════════════

async fn cmd_zadd(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let mut idx = 2usize;
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    let mut ch = false;
    let mut incr = false;

    // Parse flags
    loop {
        if idx >= args.len() {
            break;
        }
        let flag = String::from_utf8_lossy(&args[idx]).to_ascii_uppercase();
        match flag.as_str() {
            "NX" => {
                nx = true;
                idx += 1;
            }
            "XX" => {
                xx = true;
                idx += 1;
            }
            "GT" => {
                gt = true;
                idx += 1;
            }
            "LT" => {
                lt = true;
                idx += 1;
            }
            "CH" => {
                ch = true;
                idx += 1;
            }
            "INCR" => {
                incr = true;
                idx += 1;
            }
            _ => break,
        }
    }

    if idx + 1 >= args.len() || !(args.len() - idx).is_multiple_of(2) {
        return wrong_args(&args[0]);
    }

    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
    }
    // Validate type
    if let Some(e) = db.entries.get(&ns).and_then(|m| m.get(&key))
        && e.value.as_zset().is_none()
    {
        return resp_wrongtype();
    }

    let mut added = 0i64;
    let mut changed = 0i64;
    let mut last_score: Option<f64> = None;

    // Parse score/member pairs
    let pairs: Vec<(f64, Vec<u8>)> = {
        let mut v = Vec::new();
        let mut i = idx;
        while i + 1 < args.len() {
            let score_str = &args[i];
            let member = args[i + 1].clone();
            let score = if score_str.eq_ignore_ascii_case(b"+inf") {
                f64::INFINITY
            } else if score_str.eq_ignore_ascii_case(b"-inf") {
                f64::NEG_INFINITY
            } else {
                match std::str::from_utf8(score_str)
                    .ok()
                    .and_then(|s| s.parse::<f64>().ok())
                {
                    Some(s) => s,
                    None => return resp_err("not a float or out of range"),
                }
            };
            v.push((score, member));
            i += 2;
        }
        v
    };

    for (score, member) in pairs {
        let entry = db
            .entries
            .entry(ns.clone())
            .or_default()
            .entry(key.clone())
            .or_insert_with(|| Entry {
                value: Value::ZSet(Vec::new()),
                hits: 0,
                expiry: None,
            });
        let zset = match entry.value.as_zset_mut() {
            Some(z) => z,
            None => return resp_wrongtype(),
        };

        let existing_idx = zset_find_member_idx(zset, &member);
        let existing_score = existing_idx.map(|i| zset[i].score);

        let should_update = match existing_score {
            None => {
                // New member
                if xx {
                    continue;
                } // XX: only update existing
                true
            }
            Some(cur_score) => {
                if nx {
                    continue;
                } // NX: only add new
                if gt && score <= cur_score {
                    continue;
                }
                if lt && score >= cur_score {
                    continue;
                }
                true
            }
        };

        if should_update {
            let is_new = if incr {
                let new_score = existing_score.unwrap_or(0.0) + score;
                last_score = Some(new_score);
                zset_insert_or_update(zset, new_score, member)
            } else {
                last_score = Some(score);
                let is_new = zset_insert_or_update(zset, score, member);
                if !is_new && existing_score != Some(score) {
                    changed += 1;
                }
                is_new
            };
            if is_new {
                added += 1;
            }
        }
    }

    if incr {
        return match last_score {
            Some(s) => resp_bulk(&format_score(s)),
            None => resp_null(),
        };
    }

    if ch {
        resp_int(added + changed)
    } else {
        resp_int(added)
    }
}

async fn cmd_zrange(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let start_raw = &args[2];
    let stop_raw = &args[3];

    let mut byscore = false;
    let mut bylex = false;
    let mut rev = false;
    let mut withscores = false;
    let mut limit_offset: usize = 0;
    let mut limit_count: Option<usize> = None;

    let mut i = 4;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
        match opt.as_str() {
            "BYSCORE" => {
                byscore = true;
                i += 1;
            }
            "BYLEX" => {
                bylex = true;
                i += 1;
            }
            "REV" => {
                rev = true;
                i += 1;
            }
            "WITHSCORES" => {
                withscores = true;
                i += 1;
            }
            "LIMIT" => {
                if i + 2 >= args.len() {
                    return resp_err("syntax error");
                }
                limit_offset = std::str::from_utf8(&args[i + 1])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                limit_count = std::str::from_utf8(&args[i + 2])
                    .ok()
                    .and_then(|s| s.parse().ok());
                i += 3;
            }
            _ => {
                i += 1;
            }
        }
    }

    let zset = match read_zset_snapshot(store, &ns, &key).await {
        ZsetLookup::Missing => return resp_array(&[]),
        ZsetLookup::Expired => {
            cleanup_expired_key(store, &ns, &key).await;
            return resp_array(&[]);
        }
        ZsetLookup::WrongType => return resp_wrongtype(),
        ZsetLookup::Found(zset) => {
            mark_ear_after_read(store, &ns, &key).await;
            zset
        }
    };

    let results: Vec<(Vec<u8>, f64)> = if bylex {
        let (min_raw, max_raw) = if rev {
            (stop_raw, start_raw)
        } else {
            (start_raw, stop_raw)
        };
        let min = match parse_lex_bound(min_raw) {
            Some(b) => b,
            None => return resp_err("invalid lex range"),
        };
        let max = match parse_lex_bound(max_raw) {
            Some(b) => b,
            None => return resp_err("invalid lex range"),
        };
        let mut items: Vec<(Vec<u8>, f64)> = zset
            .iter()
            .filter(|e| member_in_lex_range(&e.member, &min, &max))
            .map(|e| (e.member.clone(), e.score))
            .collect();
        if rev {
            items.reverse();
        }
        items
    } else if byscore {
        let (min_raw, max_raw) = if rev {
            (stop_raw, start_raw)
        } else {
            (start_raw, stop_raw)
        };
        let (min_score, min_excl) = match parse_score_bound(min_raw) {
            Some(b) => b,
            None => return resp_err("min or max is not a float"),
        };
        let (max_score, max_excl) = match parse_score_bound(max_raw) {
            Some(b) => b,
            None => return resp_err("min or max is not a float"),
        };
        let mut items: Vec<(Vec<u8>, f64)> = zset
            .iter()
            .filter(|e| {
                let above = if min_excl {
                    e.score > min_score
                } else {
                    e.score >= min_score
                };
                let below = if max_excl {
                    e.score < max_score
                } else {
                    e.score <= max_score
                };
                above && below
            })
            .map(|e| (e.member.clone(), e.score))
            .collect();
        if rev {
            items.reverse();
        }
        items
    } else {
        // By rank
        let len = zset.len() as i64;
        let start_i: i64 = std::str::from_utf8(start_raw)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let stop_i: i64 = std::str::from_utf8(stop_raw)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(-1);
        let start = if start_i < 0 {
            (len + start_i).max(0)
        } else {
            start_i.min(len)
        } as usize;
        let stop = if stop_i < 0 {
            (len + stop_i).max(0)
        } else {
            stop_i.min(len - 1)
        } as usize;
        if start > stop {
            return resp_array(&[]);
        }
        let mut items: Vec<(Vec<u8>, f64)> = zset[start..=stop.min(zset.len().saturating_sub(1))]
            .iter()
            .map(|e| (e.member.clone(), e.score))
            .collect();
        if rev {
            items.reverse();
        }
        items
    };

    // Apply LIMIT
    let results = if limit_count.is_some() || limit_offset > 0 {
        let skipped: Vec<_> = results.into_iter().skip(limit_offset).collect();
        match limit_count {
            Some(c) => skipped.into_iter().take(c).collect(),
            None => skipped,
        }
    } else {
        results
    };

    let mut out_items: Vec<Vec<u8>> = Vec::new();
    for (member, score) in results {
        out_items.push(member);
        if withscores {
            out_items.push(format_score(score));
        }
    }
    resp_array(&out_items)
}

async fn cmd_zrangebyscore(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let min_raw = &args[2];
    let max_raw = &args[3];

    let (min_score, min_excl) = match parse_score_bound(min_raw) {
        Some(b) => b,
        None => return resp_err("min or max is not a float"),
    };
    let (max_score, max_excl) = match parse_score_bound(max_raw) {
        Some(b) => b,
        None => return resp_err("min or max is not a float"),
    };

    let mut withscores = false;
    let mut limit_offset: usize = 0;
    let mut limit_count: Option<usize> = None;
    let mut i = 4;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
        match opt.as_str() {
            "WITHSCORES" => {
                withscores = true;
                i += 1;
            }
            "LIMIT" => {
                if i + 2 >= args.len() {
                    return resp_err("syntax error");
                }
                limit_offset = std::str::from_utf8(&args[i + 1])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                limit_count = std::str::from_utf8(&args[i + 2])
                    .ok()
                    .and_then(|s| s.parse().ok());
                i += 3;
            }
            _ => {
                i += 1;
            }
        }
    }

    let zset = match read_zset_snapshot(store, &ns, &key).await {
        ZsetLookup::Missing => return resp_array(&[]),
        ZsetLookup::Expired => {
            cleanup_expired_key(store, &ns, &key).await;
            return resp_array(&[]);
        }
        ZsetLookup::WrongType => return resp_wrongtype(),
        ZsetLookup::Found(zset) => {
            mark_ear_after_read(store, &ns, &key).await;
            zset
        }
    };

    let filtered: Vec<(Vec<u8>, f64)> = zset
        .iter()
        .filter(|e| {
            let above = if min_excl {
                e.score > min_score
            } else {
                e.score >= min_score
            };
            let below = if max_excl {
                e.score < max_score
            } else {
                e.score <= max_score
            };
            above && below
        })
        .map(|e| (e.member.clone(), e.score))
        .collect();

    let filtered = filtered.into_iter().skip(limit_offset);
    let filtered: Vec<_> = match limit_count {
        Some(c) => filtered.take(c).collect(),
        None => filtered.collect(),
    };

    let mut out: Vec<Vec<u8>> = Vec::new();
    for (m, s) in filtered {
        out.push(m);
        if withscores {
            out.push(format_score(s));
        }
    }
    resp_array(&out)
}

async fn cmd_zrevrangebyscore(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let max_raw = &args[2];
    let min_raw = &args[3];

    let (min_score, min_excl) = match parse_score_bound(min_raw) {
        Some(b) => b,
        None => return resp_err("min or max is not a float"),
    };
    let (max_score, max_excl) = match parse_score_bound(max_raw) {
        Some(b) => b,
        None => return resp_err("min or max is not a float"),
    };

    let mut withscores = false;
    let mut limit_offset: usize = 0;
    let mut limit_count: Option<usize> = None;
    let mut i = 4;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
        match opt.as_str() {
            "WITHSCORES" => {
                withscores = true;
                i += 1;
            }
            "LIMIT" => {
                if i + 2 >= args.len() {
                    return resp_err("syntax error");
                }
                limit_offset = std::str::from_utf8(&args[i + 1])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                limit_count = std::str::from_utf8(&args[i + 2])
                    .ok()
                    .and_then(|s| s.parse().ok());
                i += 3;
            }
            _ => {
                i += 1;
            }
        }
    }

    let zset = match read_zset_snapshot(store, &ns, &key).await {
        ZsetLookup::Missing => return resp_array(&[]),
        ZsetLookup::Expired => {
            cleanup_expired_key(store, &ns, &key).await;
            return resp_array(&[]);
        }
        ZsetLookup::WrongType => return resp_wrongtype(),
        ZsetLookup::Found(zset) => {
            mark_ear_after_read(store, &ns, &key).await;
            zset
        }
    };

    let mut filtered: Vec<(Vec<u8>, f64)> = zset
        .iter()
        .filter(|e| {
            let above = if min_excl {
                e.score > min_score
            } else {
                e.score >= min_score
            };
            let below = if max_excl {
                e.score < max_score
            } else {
                e.score <= max_score
            };
            above && below
        })
        .map(|e| (e.member.clone(), e.score))
        .collect();
    filtered.reverse();

    let filtered = filtered.into_iter().skip(limit_offset);
    let filtered: Vec<_> = match limit_count {
        Some(c) => filtered.take(c).collect(),
        None => filtered.collect(),
    };

    let mut out: Vec<Vec<u8>> = Vec::new();
    for (m, s) in filtered {
        out.push(m);
        if withscores {
            out.push(format_score(s));
        }
    }
    resp_array(&out)
}

async fn cmd_zrevrange(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 4 {
        return wrong_args(&args[0]);
    }
    // Build zrange-compatible args: ZRANGE key start stop REV [WITHSCORES]
    let withscores =
        args.len() > 4 && String::from_utf8_lossy(&args[4]).to_ascii_uppercase() == "WITHSCORES";

    let (ns, key) = parse_ns_key(&args[1]);
    let start_i: i64 = std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let stop_i: i64 = std::str::from_utf8(&args[3])
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(-1);

    let zset = match read_zset_snapshot(store, &ns, &key).await {
        ZsetLookup::Missing => return resp_array(&[]),
        ZsetLookup::Expired => {
            cleanup_expired_key(store, &ns, &key).await;
            return resp_array(&[]);
        }
        ZsetLookup::WrongType => return resp_wrongtype(),
        ZsetLookup::Found(zset) => {
            mark_ear_after_read(store, &ns, &key).await;
            zset
        }
    };

    let len = zset.len() as i64;
    let start = if start_i < 0 {
        (len + start_i).max(0)
    } else {
        start_i.min(len)
    } as usize;
    let stop = if stop_i < 0 {
        (len + stop_i).max(0)
    } else {
        stop_i.min(len - 1)
    } as usize;
    if start > stop || zset.is_empty() {
        return resp_array(&[]);
    }
    let mut slice: Vec<(Vec<u8>, f64)> = zset[start..=stop.min(zset.len().saturating_sub(1))]
        .iter()
        .map(|e| (e.member.clone(), e.score))
        .collect();
    slice.reverse();

    let mut out: Vec<Vec<u8>> = Vec::new();
    for (m, s) in slice {
        out.push(m);
        if withscores {
            out.push(format_score(s));
        }
    }
    resp_array(&out)
}

async fn cmd_zrank(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 || args.len() > 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let member = &args[2];
    let withscore =
        args.len() == 4 && String::from_utf8_lossy(&args[3]).to_ascii_uppercase() == "WITHSCORE";

    let zset = match read_zset_snapshot(store, &ns, &key).await {
        ZsetLookup::Missing => return resp_null(),
        ZsetLookup::Expired => {
            cleanup_expired_key(store, &ns, &key).await;
            return resp_null();
        }
        ZsetLookup::WrongType => return resp_wrongtype(),
        ZsetLookup::Found(zset) => zset,
    };

    match zset.iter().position(|e| e.member == member.as_slice()) {
        None => resp_null(),
        Some(rank) => {
            mark_ear_after_read(store, &ns, &key).await;
            if withscore {
                let score = zset[rank].score;
                let mut out = b"*2\r\n".to_vec();
                out.extend_from_slice(&resp_int(rank as i64));
                out.extend_from_slice(&resp_bulk(&format_score(score)));
                out
            } else {
                resp_int(rank as i64)
            }
        }
    }
}

async fn cmd_zrevrank(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 || args.len() > 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let member = &args[2];
    let withscore =
        args.len() == 4 && String::from_utf8_lossy(&args[3]).to_ascii_uppercase() == "WITHSCORE";

    let zset = match read_zset_snapshot(store, &ns, &key).await {
        ZsetLookup::Missing => return resp_null(),
        ZsetLookup::Expired => {
            cleanup_expired_key(store, &ns, &key).await;
            return resp_null();
        }
        ZsetLookup::WrongType => return resp_wrongtype(),
        ZsetLookup::Found(zset) => zset,
    };

    match zset.iter().position(|e| e.member == member.as_slice()) {
        None => resp_null(),
        Some(rank) => {
            mark_ear_after_read(store, &ns, &key).await;
            let rev_rank = zset.len() - 1 - rank;
            if withscore {
                let score = zset[rank].score;
                let mut out = b"*2\r\n".to_vec();
                out.extend_from_slice(&resp_int(rev_rank as i64));
                out.extend_from_slice(&resp_bulk(&format_score(score)));
                out
            } else {
                resp_int(rev_rank as i64)
            }
        }
    }
}

async fn cmd_zscore(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let member = &args[2];
    let zset = match read_zset_snapshot(store, &ns, &key).await {
        ZsetLookup::Missing => return resp_null(),
        ZsetLookup::Expired => {
            cleanup_expired_key(store, &ns, &key).await;
            return resp_null();
        }
        ZsetLookup::WrongType => return resp_wrongtype(),
        ZsetLookup::Found(zset) => zset,
    };

    match zset.iter().find(|e| e.member == member.as_slice()) {
        None => resp_null(),
        Some(e) => {
            mark_ear_after_read(store, &ns, &key).await;
            resp_bulk(&format_score(e.score))
        }
    }
}

async fn cmd_zmscore(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let members = &args[2..];
    let zset = match read_zset_snapshot(store, &ns, &key).await {
        ZsetLookup::Missing => return resp_array_of_nulls(members.len()),
        ZsetLookup::Expired => {
            cleanup_expired_key(store, &ns, &key).await;
            return resp_array_of_nulls(members.len());
        }
        ZsetLookup::WrongType => return resp_wrongtype(),
        ZsetLookup::Found(zset) => {
            mark_ear_after_read(store, &ns, &key).await;
            zset
        }
    };

    let mut out = format!("*{}\r\n", members.len()).into_bytes();
    for m in members {
        match zset.iter().find(|e| e.member == m.as_slice()) {
            None => out.extend_from_slice(&resp_null()),
            Some(e) => out.extend_from_slice(&resp_bulk(&format_score(e.score))),
        }
    }
    out
}

async fn cmd_zrem(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let members = &args[2..];
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    match db.entries.get_mut(&ns).and_then(|m| m.get_mut(&key)) {
        None => resp_int(0),
        Some(entry) => match entry.value.as_zset_mut() {
            None => resp_wrongtype(),
            Some(z) => {
                let before = z.len();
                for m in members {
                    if let Some(idx) = z.iter().position(|e| e.member == m.as_slice()) {
                        z.remove(idx);
                    }
                }
                resp_int((before - z.len()) as i64)
            }
        },
    }
}

async fn cmd_zcard(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => resp_int(0),
        Some(entry) => match entry.value.as_zset() {
            None => resp_wrongtype(),
            Some(z) => resp_int(z.len() as i64),
        },
    }
}

async fn cmd_zcount(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let (min_score, min_excl) = match parse_score_bound(&args[2]) {
        Some(b) => b,
        None => return resp_err("min or max is not a float"),
    };
    let (max_score, max_excl) = match parse_score_bound(&args[3]) {
        Some(b) => b,
        None => return resp_err("min or max is not a float"),
    };
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => resp_int(0),
        Some(entry) => match entry.value.as_zset() {
            None => resp_wrongtype(),
            Some(z) => {
                let count = z
                    .iter()
                    .filter(|e| {
                        let above = if min_excl {
                            e.score > min_score
                        } else {
                            e.score >= min_score
                        };
                        let below = if max_excl {
                            e.score < max_score
                        } else {
                            e.score <= max_score
                        };
                        above && below
                    })
                    .count();
                resp_int(count as i64)
            }
        },
    }
}

async fn cmd_zincrby(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let by: f64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err("not a float or out of range"),
    };
    let member = args[3].clone();
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
    }

    let current_score: f64 = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => 0.0,
        Some(entry) => match entry.value.as_zset() {
            None => return resp_wrongtype(),
            Some(z) => z
                .iter()
                .find(|e| e.member == member.as_slice())
                .map(|e| e.score)
                .unwrap_or(0.0),
        },
    };

    let new_score = current_score + by;
    let entry = db
        .entries
        .entry(ns.clone())
        .or_default()
        .entry(key.clone())
        .or_insert_with(|| Entry {
            value: Value::ZSet(Vec::new()),
            hits: 0,
            expiry: None,
        });
    match entry.value.as_zset_mut() {
        Some(z) => {
            zset_insert_or_update(z, new_score, member);
        }
        None => return resp_wrongtype(),
    }
    resp_bulk(&format_score(new_score))
}

async fn cmd_zrangebylex(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let min = match parse_lex_bound(&args[2]) {
        Some(b) => b,
        None => return resp_err("invalid lex range"),
    };
    let max = match parse_lex_bound(&args[3]) {
        Some(b) => b,
        None => return resp_err("invalid lex range"),
    };

    let mut limit_offset: usize = 0;
    let mut limit_count: Option<usize> = None;
    let mut i = 4;
    while i + 2 < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
        if opt == "LIMIT" {
            limit_offset = std::str::from_utf8(&args[i + 1])
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            limit_count = std::str::from_utf8(&args[i + 2])
                .ok()
                .and_then(|s| s.parse().ok());
            i += 3;
        } else {
            i += 1;
        }
    }

    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_array(&[]);
    }
    let zset = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => return resp_array(&[]),
        Some(entry) => match entry.value.as_zset() {
            None => return resp_wrongtype(),
            Some(z) => z.clone(),
        },
    };

    let filtered: Vec<Vec<u8>> = zset
        .iter()
        .filter(|e| member_in_lex_range(&e.member, &min, &max))
        .map(|e| e.member.clone())
        .collect();

    let filtered = filtered.into_iter().skip(limit_offset);
    let result: Vec<Vec<u8>> = match limit_count {
        Some(c) => filtered.take(c).collect(),
        None => filtered.collect(),
    };
    resp_array(&result)
}

async fn cmd_zlexcount(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let min = match parse_lex_bound(&args[2]) {
        Some(b) => b,
        None => return resp_err("invalid lex range"),
    };
    let max = match parse_lex_bound(&args[3]) {
        Some(b) => b,
        None => return resp_err("invalid lex range"),
    };
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => resp_int(0),
        Some(entry) => match entry.value.as_zset() {
            None => resp_wrongtype(),
            Some(z) => {
                let count = z
                    .iter()
                    .filter(|e| member_in_lex_range(&e.member, &min, &max))
                    .count();
                resp_int(count as i64)
            }
        },
    }
}

async fn cmd_zremrangebyrank(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let start_i: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err("value is not an integer or out of range"),
    };
    let stop_i: i64 = match std::str::from_utf8(&args[3])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err("value is not an integer or out of range"),
    };
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    match db.entries.get_mut(&ns).and_then(|m| m.get_mut(&key)) {
        None => resp_int(0),
        Some(entry) => match entry.value.as_zset_mut() {
            None => resp_wrongtype(),
            Some(z) => {
                let len = z.len() as i64;
                let start = if start_i < 0 {
                    (len + start_i).max(0)
                } else {
                    start_i.min(len)
                } as usize;
                let stop = if stop_i < 0 {
                    (len + stop_i).max(0)
                } else {
                    stop_i.min(len - 1)
                } as usize;
                if start > stop {
                    return resp_int(0);
                }
                let count = stop - start + 1;
                z.drain(start..=stop.min(z.len().saturating_sub(1)));
                resp_int(count as i64)
            }
        },
    }
}

async fn cmd_zremrangebyscore(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let (min_score, min_excl) = match parse_score_bound(&args[2]) {
        Some(b) => b,
        None => return resp_err("min or max is not a float"),
    };
    let (max_score, max_excl) = match parse_score_bound(&args[3]) {
        Some(b) => b,
        None => return resp_err("min or max is not a float"),
    };
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    match db.entries.get_mut(&ns).and_then(|m| m.get_mut(&key)) {
        None => resp_int(0),
        Some(entry) => match entry.value.as_zset_mut() {
            None => resp_wrongtype(),
            Some(z) => {
                let before = z.len();
                z.retain(|e| {
                    let above = if min_excl {
                        e.score > min_score
                    } else {
                        e.score >= min_score
                    };
                    let below = if max_excl {
                        e.score < max_score
                    } else {
                        e.score <= max_score
                    };
                    !(above && below)
                });
                resp_int((before - z.len()) as i64)
            }
        },
    }
}

async fn cmd_zremrangebylex(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let min = match parse_lex_bound(&args[2]) {
        Some(b) => b,
        None => return resp_err("invalid lex range"),
    };
    let max = match parse_lex_bound(&args[3]) {
        Some(b) => b,
        None => return resp_err("invalid lex range"),
    };
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    match db.entries.get_mut(&ns).and_then(|m| m.get_mut(&key)) {
        None => resp_int(0),
        Some(entry) => match entry.value.as_zset_mut() {
            None => resp_wrongtype(),
            Some(z) => {
                let before = z.len();
                z.retain(|e| !member_in_lex_range(&e.member, &min, &max));
                resp_int((before - z.len()) as i64)
            }
        },
    }
}

async fn cmd_zpopmin(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 2 || args.len() > 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let count: usize = if args.len() == 3 {
        match std::str::from_utf8(&args[2])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(n) => n,
            None => return resp_err("value is not an integer or out of range"),
        }
    } else {
        1
    };

    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_array(&[]);
    }
    match db.entries.get_mut(&ns).and_then(|m| m.get_mut(&key)) {
        None => resp_array(&[]),
        Some(entry) => match entry.value.as_zset_mut() {
            None => resp_wrongtype(),
            Some(z) => {
                let mut result: Vec<Vec<u8>> = Vec::new();
                for _ in 0..count {
                    if z.is_empty() {
                        break;
                    }
                    let e = z.remove(0);
                    result.push(e.member);
                    result.push(format_score(e.score));
                }
                resp_array(&result)
            }
        },
    }
}

async fn cmd_zpopmax(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 2 || args.len() > 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let count: usize = if args.len() == 3 {
        match std::str::from_utf8(&args[2])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(n) => n,
            None => return resp_err("value is not an integer or out of range"),
        }
    } else {
        1
    };

    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_array(&[]);
    }
    match db.entries.get_mut(&ns).and_then(|m| m.get_mut(&key)) {
        None => resp_array(&[]),
        Some(entry) => match entry.value.as_zset_mut() {
            None => resp_wrongtype(),
            Some(z) => {
                let mut result: Vec<Vec<u8>> = Vec::new();
                for _ in 0..count {
                    if z.is_empty() {
                        break;
                    }
                    let e = z.pop().unwrap();
                    result.push(e.member);
                    result.push(format_score(e.score));
                }
                resp_array(&result)
            }
        },
    }
}

async fn cmd_zrandmember(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 2 || args.len() > 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let count_opt: Option<i64> = if args.len() >= 3 {
        match std::str::from_utf8(&args[2])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(n) => Some(n),
            None => return resp_err("value is not an integer or out of range"),
        }
    } else {
        None
    };
    let withscores =
        args.len() >= 4 && String::from_utf8_lossy(&args[3]).to_ascii_uppercase() == "WITHSCORES";

    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return if count_opt.is_some() {
            resp_array(&[])
        } else {
            resp_null()
        };
    }
    let (resp, mark) = match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => {
            if count_opt.is_some() {
                (resp_array(&[]), false)
            } else {
                (resp_null(), false)
            }
        }
        Some(entry) => match entry.value.as_zset() {
            None => return resp_wrongtype(),
            Some(z) => {
                if z.is_empty() {
                    if count_opt.is_some() {
                        (resp_array(&[]), false)
                    } else {
                        (resp_null(), false)
                    }
                } else {
                    // Clone to release borrow before potential mark_ear call.
                    let z: Vec<crate::store::ZEntry> = z.clone();
                    match count_opt {
                        None => (resp_bulk(&z[0].member), true),
                        Some(count) => {
                            let abs_count = count.unsigned_abs() as usize;
                            let allow_repeat = count < 0;
                            let mut result: Vec<Vec<u8>> = Vec::new();
                            if allow_repeat {
                                for i in 0..abs_count {
                                    let idx = i % z.len();
                                    result.push(z[idx].member.clone());
                                    if withscores {
                                        result.push(format_score(z[idx].score));
                                    }
                                }
                            } else {
                                let take = abs_count.min(z.len());
                                for item in z.iter().take(take) {
                                    result.push(item.member.clone());
                                    if withscores {
                                        result.push(format_score(item.score));
                                    }
                                }
                            }
                            (resp_array(&result), true)
                        }
                    }
                }
            }
        },
    };
    if mark {
        db.mark_ear(&ns, &key);
    }
    resp
}

// ═══════════════════════════════════════════════════════════════════════════════
// GENERIC KEY COMMANDS
// ═══════════════════════════════════════════════════════════════════════════════

async fn cmd_del(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let mut count = 0i64;
    let mut db = store.write().await;
    for raw_key in &args[1..] {
        let (ns, key) = parse_ns_key(raw_key);
        if db.delete(&ns, &key).is_some() {
            count += 1;
        }
    }
    resp_int(count)
}

async fn cmd_exists(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let (count, expired_keys) = {
        let db = store.read().await;
        let mut count = 0i64;
        let mut expired_keys: Vec<(String, String)> = Vec::new();
        for raw_key in &args[1..] {
            let (ns, key) = parse_ns_key(raw_key);
            match db.entries.get(&ns).and_then(|m| m.get(&key)) {
                None => {}
                Some(entry) if entry.is_expired() => expired_keys.push((ns, key)),
                Some(_) => count += 1,
            }
        }
        (count, expired_keys)
    };
    cleanup_expired_keys(store, &expired_keys).await;
    resp_int(count)
}

async fn cmd_type(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let (resp, expired) = {
        let db = store.read().await;
        match db.entries.get(&ns).and_then(|m| m.get(&key)) {
            None => (b"+none\r\n".to_vec(), false),
            Some(entry) if entry.is_expired() => (b"+none\r\n".to_vec(), true),
            Some(entry) => (
                format!("+{}\r\n", entry.value.type_name()).into_bytes(),
                false,
            ),
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    }
    resp
}

async fn cmd_ttl(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let (resp, expired) = {
        let db = store.read().await;
        match db.entries.get(&ns).and_then(|m| m.get(&key)) {
            None => (resp_int(-2), false),
            Some(entry) if entry.is_expired() => (resp_int(-2), true),
            Some(entry) => (resp_int(entry.time_to_expiry_secs()), false),
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    }
    resp
}

async fn cmd_pttl(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let (resp, expired) = {
        let db = store.read().await;
        match db.entries.get(&ns).and_then(|m| m.get(&key)) {
            None => (resp_int(-2), false),
            Some(entry) if entry.is_expired() => (resp_int(-2), true),
            Some(entry) => (resp_int(entry.time_to_expiry_ms()), false),
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    }
    resp
}

async fn cmd_expire(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let secs: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err("value is not an integer or out of range"),
    };
    let condition = args
        .get(3)
        .map(|b| String::from_utf8_lossy(b).to_ascii_uppercase());

    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    let entry = match db.entries.get_mut(&ns).and_then(|m| m.get_mut(&key)) {
        None => return resp_int(0),
        Some(e) => e,
    };

    let new_expiry = Instant::now() + Duration::from_secs(secs.max(0) as u64);
    let should_set = match condition.as_deref() {
        Some("NX") => entry.expiry.is_none(),
        Some("XX") => entry.expiry.is_some(),
        Some("GT") => match entry.expiry {
            None => true,
            Some(cur) => new_expiry > cur,
        },
        Some("LT") => match entry.expiry {
            None => true,
            Some(cur) => new_expiry < cur,
        },
        _ => true,
    };

    if !should_set {
        return resp_int(0);
    }
    entry.expiry = Some(new_expiry);
    let deadline = new_expiry;
    drop(db);
    schedule_expiry(store, ns, key, deadline);
    resp_int(1)
}

async fn cmd_expireat(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let unix_secs: u64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err("value is not an integer or out of range"),
    };
    let condition = args
        .get(3)
        .map(|b| String::from_utf8_lossy(b).to_ascii_uppercase());

    let target = SystemTime::UNIX_EPOCH + Duration::from_secs(unix_secs);
    let new_expiry = match target.duration_since(SystemTime::now()) {
        Ok(d) => Instant::now() + d,
        Err(_) => Instant::now(),
    };

    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    let entry = match db.entries.get_mut(&ns).and_then(|m| m.get_mut(&key)) {
        None => return resp_int(0),
        Some(e) => e,
    };

    let should_set = match condition.as_deref() {
        Some("NX") => entry.expiry.is_none(),
        Some("XX") => entry.expiry.is_some(),
        Some("GT") => match entry.expiry {
            None => true,
            Some(cur) => new_expiry > cur,
        },
        Some("LT") => match entry.expiry {
            None => true,
            Some(cur) => new_expiry < cur,
        },
        _ => true,
    };
    if !should_set {
        return resp_int(0);
    }
    entry.expiry = Some(new_expiry);
    let deadline = new_expiry;
    drop(db);
    schedule_expiry(store, ns, key, deadline);
    resp_int(1)
}

async fn cmd_pexpire(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let ms: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err("value is not an integer or out of range"),
    };
    let condition = args
        .get(3)
        .map(|b| String::from_utf8_lossy(b).to_ascii_uppercase());

    let new_expiry = Instant::now() + Duration::from_millis(ms.max(0) as u64);

    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    let entry = match db.entries.get_mut(&ns).and_then(|m| m.get_mut(&key)) {
        None => return resp_int(0),
        Some(e) => e,
    };

    let should_set = match condition.as_deref() {
        Some("NX") => entry.expiry.is_none(),
        Some("XX") => entry.expiry.is_some(),
        Some("GT") => match entry.expiry {
            None => true,
            Some(cur) => new_expiry > cur,
        },
        Some("LT") => match entry.expiry {
            None => true,
            Some(cur) => new_expiry < cur,
        },
        _ => true,
    };
    if !should_set {
        return resp_int(0);
    }
    entry.expiry = Some(new_expiry);
    let deadline = new_expiry;
    drop(db);
    schedule_expiry(store, ns, key, deadline);
    resp_int(1)
}

async fn cmd_pexpireat(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let unix_ms: u64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err("value is not an integer or out of range"),
    };
    let condition = args
        .get(3)
        .map(|b| String::from_utf8_lossy(b).to_ascii_uppercase());

    let target = SystemTime::UNIX_EPOCH + Duration::from_millis(unix_ms);
    let new_expiry = match target.duration_since(SystemTime::now()) {
        Ok(d) => Instant::now() + d,
        Err(_) => Instant::now(),
    };

    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    let entry = match db.entries.get_mut(&ns).and_then(|m| m.get_mut(&key)) {
        None => return resp_int(0),
        Some(e) => e,
    };

    let should_set = match condition.as_deref() {
        Some("NX") => entry.expiry.is_none(),
        Some("XX") => entry.expiry.is_some(),
        Some("GT") => match entry.expiry {
            None => true,
            Some(cur) => new_expiry > cur,
        },
        Some("LT") => match entry.expiry {
            None => true,
            Some(cur) => new_expiry < cur,
        },
        _ => true,
    };
    if !should_set {
        return resp_int(0);
    }
    entry.expiry = Some(new_expiry);
    let deadline = new_expiry;
    drop(db);
    schedule_expiry(store, ns, key, deadline);
    resp_int(1)
}

async fn cmd_persist(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    match db.entries.get_mut(&ns).and_then(|m| m.get_mut(&key)) {
        None => resp_int(0),
        Some(entry) => {
            if entry.expiry.is_none() {
                resp_int(0)
            } else {
                entry.expiry = None;
                resp_int(1)
            }
        }
    }
}

async fn cmd_expiretime(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(-2);
    }
    match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => resp_int(-2),
        Some(entry) => match entry.expiry {
            None => resp_int(-1),
            Some(deadline) => {
                let now_instant = Instant::now();
                let now_sys = SystemTime::now();
                let unix_secs = if deadline > now_instant {
                    let remaining = deadline - now_instant;
                    now_sys
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs()
                        + remaining.as_secs()
                } else {
                    now_sys
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs()
                };
                resp_int(unix_secs as i64)
            }
        },
    }
}

async fn cmd_pexpiretime(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    if db
        .entries
        .get(&ns)
        .and_then(|m| m.get(&key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(-2);
    }
    match db.entries.get(&ns).and_then(|m| m.get(&key)) {
        None => resp_int(-2),
        Some(entry) => match entry.expiry {
            None => resp_int(-1),
            Some(deadline) => {
                let now_instant = Instant::now();
                let now_sys = SystemTime::now();
                let unix_ms = if deadline > now_instant {
                    let remaining = deadline - now_instant;
                    now_sys
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64
                        + remaining.as_millis() as u64
                } else {
                    now_sys
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64
                };
                resp_int(unix_ms as i64)
            }
        },
    }
}

async fn cmd_rename(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (src_ns, src_key) = parse_ns_key(&args[1]);
    let (dst_ns, dst_key) = parse_ns_key(&args[2]);
    let mut db = store.write().await;

    if db
        .entries
        .get(&src_ns)
        .and_then(|m| m.get(&src_key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&src_ns, &src_key);
        return resp_err("ERR no such key");
    }

    let entry = match db.delete(&src_ns, &src_key) {
        None => return resp_err("ERR no such key"),
        Some(e) => e,
    };
    db.put(dst_ns, dst_key, entry);
    resp_ok()
}

async fn cmd_renamenx(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (src_ns, src_key) = parse_ns_key(&args[1]);
    let (dst_ns, dst_key) = parse_ns_key(&args[2]);
    let mut db = store.write().await;

    if db
        .entries
        .get(&src_ns)
        .and_then(|m| m.get(&src_key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&src_ns, &src_key);
        return resp_err("ERR no such key");
    }
    if db
        .entries
        .get(&src_ns)
        .and_then(|m| m.get(&src_key))
        .is_none()
    {
        return resp_err("ERR no such key");
    }

    if db
        .entries
        .get(&dst_ns)
        .and_then(|m| m.get(&dst_key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&dst_ns, &dst_key);
    }
    if db
        .entries
        .get(&dst_ns)
        .and_then(|m| m.get(&dst_key))
        .is_some()
    {
        return resp_int(0);
    }

    let entry = db.delete(&src_ns, &src_key).unwrap();
    db.put(dst_ns, dst_key, entry);
    resp_int(1)
}

async fn cmd_scan(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let cursor: usize = std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let mut pattern: Option<&[u8]> = None;
    let mut page_size: usize = 10;
    let mut type_filter: Option<String> = None;

    let mut i = 2;
    while i + 1 < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
        match opt.as_str() {
            "MATCH" => {
                pattern = Some(&args[i + 1]);
                i += 2;
            }
            "COUNT" => {
                page_size = std::str::from_utf8(&args[i + 1])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(10);
                i += 2;
            }
            "TYPE" => {
                type_filter = Some(String::from_utf8_lossy(&args[i + 1]).to_ascii_lowercase());
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    let db = store.read().await;
    let mut all_keys: Vec<Vec<u8>> = Vec::new();
    for (ns, ns_map) in &db.entries {
        for (key, entry) in ns_map {
            if entry.is_expired() {
                continue;
            }
            if let Some(ref tf) = type_filter
                && entry.value.type_name() != tf.as_str()
            {
                continue;
            }
            let display: Vec<u8> = if ns == "default" {
                key.as_bytes().to_vec()
            } else {
                format!("{ns}/{key}").into_bytes()
            };
            if let Some(pat) = pattern
                && !glob_match(pat, &display)
            {
                continue;
            }
            all_keys.push(display);
        }
    }
    all_keys.sort();

    let start = cursor;
    let end = (start + page_size).min(all_keys.len());
    let next_cursor = if end >= all_keys.len() { 0 } else { end };
    let page = &all_keys[start.min(all_keys.len())..end];

    let mut out = b"*2\r\n".to_vec();
    out.extend_from_slice(&resp_bulk(next_cursor.to_string().as_bytes()));
    out.extend_from_slice(&resp_array(page));
    out
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

pub(crate) async fn cmd_touch(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let mut count = 0i64;
    let mut db = store.write().await;
    for raw_key in &args[1..] {
        let (ns, key) = parse_ns_key(raw_key);
        if db
            .entries
            .get(&ns)
            .and_then(|m| m.get(&key))
            .is_some_and(|e| e.is_expired())
        {
            db.delete(&ns, &key);
            continue;
        }
        if let Some(entry) = db.entries.get_mut(&ns).and_then(|m| m.get_mut(&key)) {
            entry.hits = 0;
            count += 1;
        }
    }
    resp_int(count)
}

async fn cmd_copy(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (src_ns, src_key) = parse_ns_key(&args[1]);
    let (dst_ns, dst_key) = parse_ns_key(&args[2]);
    let replace =
        args.len() > 3 && String::from_utf8_lossy(&args[3]).to_ascii_uppercase() == "REPLACE";

    let mut db = store.write().await;

    if db
        .entries
        .get(&src_ns)
        .and_then(|m| m.get(&src_key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&src_ns, &src_key);
        return resp_int(0);
    }

    let src_value = match db.entries.get(&src_ns).and_then(|m| m.get(&src_key)) {
        None => return resp_int(0),
        Some(e) => e.value.clone(),
    };
    let src_expiry = db
        .entries
        .get(&src_ns)
        .and_then(|m| m.get(&src_key))
        .and_then(|e| e.expiry);

    if db
        .entries
        .get(&dst_ns)
        .and_then(|m| m.get(&dst_key))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&dst_ns, &dst_key);
    }

    if !replace
        && db
            .entries
            .get(&dst_ns)
            .and_then(|m| m.get(&dst_key))
            .is_some()
    {
        return resp_int(0);
    }

    let new_entry = Entry {
        value: src_value,
        hits: 0,
        expiry: src_expiry,
    };
    db.put(dst_ns.clone(), dst_key.clone(), new_entry);

    if let Some(deadline) = src_expiry {
        schedule_expiry(store, dst_ns, dst_key, deadline);
    }

    resp_int(1)
}

async fn cmd_object(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let subcommand = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
    match subcommand.as_str() {
        "ENCODING" => {
            if args.len() < 3 {
                return wrong_args(&args[0]);
            }
            let (ns, key) = parse_ns_key(&args[2]);
            let db = store.read().await;
            match db.entries.get(&ns).and_then(|m| m.get(&key)) {
                None => resp_null(),
                Some(entry) => {
                    let enc = match &entry.value {
                        Value::String(_) => "embstr",
                        Value::List(_) => "listpack",
                        Value::Hash(_) => "listpack",
                        Value::Set(_) => "listpack",
                        Value::ZSet(_) => "listpack",
                    };
                    resp_bulk(enc.as_bytes())
                }
            }
        }
        "IDLETIME" => resp_int(0),
        "FREQ" => {
            if args.len() < 3 {
                return wrong_args(&args[0]);
            }
            let (ns, key) = parse_ns_key(&args[2]);
            let db = store.read().await;
            match db.entries.get(&ns).and_then(|m| m.get(&key)) {
                None => resp_null(),
                Some(entry) => resp_int(entry.hits as i64),
            }
        }
        "HELP" => {
            let help = vec![
                b"ENCODING <key>".to_vec(),
                b"FREQ <key>".to_vec(),
                b"IDLETIME <key>".to_vec(),
                b"HELP".to_vec(),
            ];
            resp_array(&help)
        }
        _ => resp_err("unknown subcommand"),
    }
}

async fn cmd_unlink(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    // Same as DEL
    cmd_del(args, store).await
}

// ═══════════════════════════════════════════════════════════════════════════════
// SERVER COMMANDS
// ═══════════════════════════════════════════════════════════════════════════════

async fn cmd_hello(args: &[Vec<u8>], _store: &Store, conn: &mut ConnState) -> Vec<u8> {
    let version: u8 = if args.len() >= 2 {
        match std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(v @ 2..=3) => v,
            _ => return resp_err("NOPROTO unsupported protocol version"),
        }
    } else {
        conn.resp_version
    };
    conn.resp_version = version;

    let id_str = conn.client_id.to_string();
    let proto_str = version.to_string();

    if version >= 3 {
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (b"server".to_vec(), b"kvns".to_vec()),
            (b"version".to_vec(), b"7.0.0".to_vec()),
            (b"proto".to_vec(), proto_str.into_bytes()),
            (b"id".to_vec(), id_str.into_bytes()),
            (b"mode".to_vec(), b"standalone".to_vec()),
            (b"role".to_vec(), b"master".to_vec()),
            (b"modules".to_vec(), b"".to_vec()),
        ];
        resp_map(&pairs)
    } else {
        // RESP2: flat array
        let items = vec![
            b"server".to_vec(),
            b"kvns".to_vec(),
            b"version".to_vec(),
            b"7.0.0".to_vec(),
            b"proto".to_vec(),
            proto_str.into_bytes(),
            b"id".to_vec(),
            id_str.into_bytes(),
            b"mode".to_vec(),
            b"standalone".to_vec(),
            b"role".to_vec(),
            b"master".to_vec(),
            b"modules".to_vec(),
            b"".to_vec(),
        ];
        resp_array(&items)
    }
}

fn cmd_reset(conn: &mut ConnState) -> Vec<u8> {
    conn.resp_version = 2;
    conn.client_name = None;
    b"+RESET\r\n".to_vec()
}

async fn cmd_select(args: &[Vec<u8>], _store: &Store) -> Vec<u8> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let db_idx: i64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err("value is not an integer or out of range"),
    };
    if db_idx == 0 {
        resp_ok()
    } else {
        resp_err("ERR DB index is out of range")
    }
}

async fn cmd_dbsize(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    if args.len() != 1 {
        return wrong_args(&args[0]);
    }
    let db = store.read().await;
    resp_int(db.total_keys() as i64)
}

async fn cmd_flushdb(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    let _ = args;
    store.write().await.flush_all();
    resp_ok()
}

async fn cmd_flushall(args: &[Vec<u8>], store: &Store) -> Vec<u8> {
    let _ = args;
    store.write().await.flush_all();
    resp_ok()
}

async fn cmd_info(_args: &[Vec<u8>], store: &Store, conn: &ConnState) -> Vec<u8> {
    let db = store.read().await;
    let used = db.used_bytes;
    let total_keys = db.total_keys();
    let namespaces = db.entries.len();
    drop(db);

    let info = format!(
        "# Server\r\nkvns_version:7.0.0\r\nredis_version:7.0.0\r\nproto:{}\r\n\
         # Memory\r\nused_memory:{}\r\n\
         # Keyspace\r\ntotal_keys:{}\r\nnamespaces:{}\r\n",
        conn.resp_version, used, total_keys, namespaces
    );

    if conn.resp_version >= 3 {
        resp_verbatim(b"txt", info.as_bytes())
    } else {
        resp_bulk(info.as_bytes())
    }
}

async fn cmd_config(args: &[Vec<u8>], _store: &Store) -> Vec<u8> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let sub = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
    match sub.as_str() {
        "GET" => resp_array(&[]),
        "SET" => resp_ok(),
        "RESETSTAT" => resp_ok(),
        "REWRITE" => resp_ok(),
        _ => resp_err("unknown subcommand"),
    }
}

async fn cmd_command(args: &[Vec<u8>], _store: &Store) -> Vec<u8> {
    if args.len() == 1 {
        return resp_array(&[]);
    }
    let sub = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
    match sub.as_str() {
        "COUNT" => resp_int(80),
        "INFO" => resp_array(&[]),
        "DOCS" => resp_array(&[]),
        "GETKEYS" => resp_array(&[]),
        _ => resp_array(&[]),
    }
}

async fn cmd_client(args: &[Vec<u8>], _store: &Store, conn: &mut ConnState) -> Vec<u8> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let sub = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
    match sub.as_str() {
        "SETNAME" => {
            if args.len() < 3 {
                return wrong_args(&args[0]);
            }
            conn.client_name = Some(args[2].clone());
            resp_ok()
        }
        "GETNAME" => match &conn.client_name {
            None => resp_null(),
            Some(name) => resp_bulk(name),
        },
        "ID" => resp_int(conn.client_id as i64),
        "INFO" => {
            let name = conn
                .client_name
                .as_deref()
                .map(|b| String::from_utf8_lossy(b).to_string())
                .unwrap_or_default();
            let info = format!("id={} name={}\r\n", conn.client_id, name);
            resp_bulk(info.as_bytes())
        }
        "LIST" => {
            let name = conn
                .client_name
                .as_deref()
                .map(|b| String::from_utf8_lossy(b).to_string())
                .unwrap_or_default();
            let info = format!("id={} name={}\r\n", conn.client_id, name);
            resp_bulk(info.as_bytes())
        }
        "NO-EVICT" => resp_ok(),
        "NO-TOUCH" => resp_ok(),
        "CACHING" => resp_ok(),
        "REPLY" => resp_ok(),
        "UNPAUSE" => resp_ok(),
        "PAUSE" => resp_ok(),
        _ => resp_err("unknown subcommand"),
    }
}

async fn cmd_latency(args: &[Vec<u8>], _store: &Store) -> Vec<u8> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let sub = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
    match sub.as_str() {
        "RESET" => resp_int(0),
        "LATEST" => resp_array(&[]),
        "HISTORY" => resp_array(&[]),
        _ => resp_array(&[]),
    }
}

async fn cmd_slowlog(args: &[Vec<u8>], _store: &Store) -> Vec<u8> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let sub = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
    match sub.as_str() {
        "GET" => resp_array(&[]),
        "LEN" => resp_int(0),
        "RESET" => resp_ok(),
        _ => resp_array(&[]),
    }
}

async fn cmd_debug(args: &[Vec<u8>], _store: &Store) -> Vec<u8> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let sub = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
    match sub.as_str() {
        "SLEEP" => {
            if args.len() >= 3 {
                let secs: f64 = std::str::from_utf8(&args[2])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                tokio::time::sleep(Duration::from_secs_f64(secs)).await;
            }
            resp_ok()
        }
        "JMAP" | "RELOAD" | "LOADAOF" | "FLUSHALL" => resp_ok(),
        _ => resp_ok(),
    }
}

async fn cmd_wait(_args: &[Vec<u8>], _store: &Store) -> Vec<u8> {
    resp_int(0)
}

async fn cmd_xadd(_args: &[Vec<u8>], _store: &Store) -> Vec<u8> {
    resp_err("stream type not supported")
}

async fn dispatch_fast_path(
    args: &[Vec<u8>],
    store: &Store,
    _conn: &mut ConnState,
) -> Option<(Vec<u8>, bool)> {
    let cmd = args[0].as_slice();
    if cmd.eq_ignore_ascii_case(b"PING") {
        return Some((resp_pong(), false));
    }
    if cmd.eq_ignore_ascii_case(b"QUIT") {
        return Some((resp_ok(), true));
    }
    if cmd.eq_ignore_ascii_case(b"SET") {
        return Some((cmd_set(args, store).await, false));
    }
    if cmd.eq_ignore_ascii_case(b"GET") {
        return Some((cmd_get(args, store).await, false));
    }
    if cmd.eq_ignore_ascii_case(b"MGET") {
        return Some((cmd_mget(args, store).await, false));
    }
    if cmd.eq_ignore_ascii_case(b"INCR") {
        return Some((cmd_incr(args, store).await, false));
    }
    if cmd.eq_ignore_ascii_case(b"INCRBY") {
        return Some((cmd_incrby(args, store).await, false));
    }
    if cmd.eq_ignore_ascii_case(b"DECR") {
        return Some((cmd_decr(args, store).await, false));
    }
    if cmd.eq_ignore_ascii_case(b"DECRBY") {
        return Some((cmd_decrby(args, store).await, false));
    }
    if cmd.eq_ignore_ascii_case(b"LPUSH") {
        return Some((cmd_lpush(args, store).await, false));
    }
    if cmd.eq_ignore_ascii_case(b"RPUSH") {
        return Some((cmd_rpush(args, store).await, false));
    }
    None
}

// ═══════════════════════════════════════════════════════════════════════════════
// DISPATCH
// ═══════════════════════════════════════════════════════════════════════════════

pub(crate) async fn dispatch(
    args: &[Vec<u8>],
    store: &Store,
    conn: &mut ConnState,
) -> (Vec<u8>, bool) {
    if args.is_empty() {
        return (resp_err("empty command"), false);
    }
    if let Some(result) = dispatch_fast_path(args, store, conn).await {
        return result;
    }
    let cmd = String::from_utf8_lossy(&args[0]).to_ascii_lowercase();
    let resp = match cmd.as_str() {
        // Connection
        "ping" => resp_pong(),
        "quit" => return (resp_ok(), true),
        "hello" => cmd_hello(args, store, conn).await,
        "reset" => cmd_reset(conn),
        "select" => cmd_select(args, store).await,

        // String
        "set" => cmd_set(args, store).await,
        "get" => cmd_get(args, store).await,
        "mget" => cmd_mget(args, store).await,
        "mset" => cmd_mset(args, store).await,
        "msetnx" => cmd_msetnx(args, store).await,
        "setnx" => cmd_setnx(args, store).await,
        "getset" => cmd_getset(args, store).await,
        "getdel" => cmd_getdel(args, store).await,
        "getex" => cmd_getex(args, store).await,
        "append" => cmd_append(args, store).await,
        "strlen" => cmd_strlen(args, store).await,
        "incr" => cmd_incr(args, store).await,
        "incrby" => cmd_incrby(args, store).await,
        "decr" => cmd_decr(args, store).await,
        "decrby" => cmd_decrby(args, store).await,
        "incrbyfloat" => cmd_incrbyfloat(args, store).await,
        "setrange" => cmd_setrange(args, store).await,
        "getrange" => cmd_getrange(args, store).await,
        "substr" => cmd_getrange(args, store).await, // alias

        // List
        "lpush" => cmd_lpush(args, store).await,
        "rpush" => cmd_rpush(args, store).await,
        "lpushx" => cmd_lpushx(args, store).await,
        "rpushx" => cmd_rpushx(args, store).await,
        "lpop" => cmd_lpop(args, store).await,
        "rpop" => cmd_rpop(args, store).await,
        "llen" => cmd_llen(args, store).await,
        "lrange" => cmd_lrange(args, store).await,
        "lindex" => cmd_lindex(args, store).await,
        "lset" => cmd_lset(args, store).await,
        "lrem" => cmd_lrem(args, store).await,
        "ltrim" => cmd_ltrim(args, store).await,
        "linsert" => cmd_linsert(args, store).await,
        "lpos" => cmd_lpos(args, store).await,
        "lmove" => cmd_lmove(args, store).await,

        // Hash
        "hset" => cmd_hset(args, store).await,
        "hmset" => {
            let r = cmd_hset(args, store).await;
            if r.starts_with(b":") { resp_ok() } else { r }
        }
        "hget" => cmd_hget(args, store).await,
        "hdel" => cmd_hdel(args, store).await,
        "hexists" => cmd_hexists(args, store).await,
        "hgetall" => cmd_hgetall(args, store, conn).await,
        "hkeys" => cmd_hkeys(args, store).await,
        "hvals" => cmd_hvals(args, store).await,
        "hlen" => cmd_hlen(args, store).await,
        "hmget" => cmd_hmget(args, store).await,
        "hincrby" => cmd_hincrby(args, store).await,
        "hincrbyfloat" => cmd_hincrbyfloat(args, store).await,
        "hrandfield" => cmd_hrandfield(args, store).await,

        // Set
        "sadd" => cmd_sadd(args, store).await,
        "srem" => cmd_srem(args, store).await,
        "smembers" => cmd_smembers(args, store).await,
        "scard" => cmd_scard(args, store).await,
        "sismember" => cmd_sismember(args, store).await,
        "smismember" => cmd_smismember(args, store).await,
        "sunion" => cmd_sunion(args, store).await,
        "sinter" => cmd_sinter(args, store).await,
        "sdiff" => cmd_sdiff(args, store).await,
        "sunionstore" => cmd_sunionstore(args, store).await,
        "sinterstore" => cmd_sinterstore(args, store).await,
        "sdiffstore" => cmd_sdiffstore(args, store).await,
        "smove" => cmd_smove(args, store).await,
        "spop" => cmd_spop(args, store).await,
        "srandmember" => cmd_srandmember(args, store).await,

        // ZSet
        "zadd" => cmd_zadd(args, store).await,
        "zrange" => cmd_zrange(args, store).await,
        "zrangebyscore" => cmd_zrangebyscore(args, store).await,
        "zrevrangebyscore" => cmd_zrevrangebyscore(args, store).await,
        "zrevrange" => cmd_zrevrange(args, store).await,
        "zrank" => cmd_zrank(args, store).await,
        "zrevrank" => cmd_zrevrank(args, store).await,
        "zscore" => cmd_zscore(args, store).await,
        "zmscore" => cmd_zmscore(args, store).await,
        "zrem" => cmd_zrem(args, store).await,
        "zcard" => cmd_zcard(args, store).await,
        "zcount" => cmd_zcount(args, store).await,
        "zincrby" => cmd_zincrby(args, store).await,
        "zrangebylex" => cmd_zrangebylex(args, store).await,
        "zlexcount" => cmd_zlexcount(args, store).await,
        "zremrangebyrank" => cmd_zremrangebyrank(args, store).await,
        "zremrangebyscore" => cmd_zremrangebyscore(args, store).await,
        "zremrangebylex" => cmd_zremrangebylex(args, store).await,
        "zpopmin" => cmd_zpopmin(args, store).await,
        "zpopmax" => cmd_zpopmax(args, store).await,
        "zrandmember" => cmd_zrandmember(args, store).await,

        // Generic
        "del" => cmd_del(args, store).await,
        "unlink" => cmd_unlink(args, store).await,
        "exists" => cmd_exists(args, store).await,
        "type" => cmd_type(args, store).await,
        "ttl" => cmd_ttl(args, store).await,
        "pttl" => cmd_pttl(args, store).await,
        "expire" => cmd_expire(args, store).await,
        "expireat" => cmd_expireat(args, store).await,
        "pexpire" => cmd_pexpire(args, store).await,
        "pexpireat" => cmd_pexpireat(args, store).await,
        "persist" => cmd_persist(args, store).await,
        "expiretime" => cmd_expiretime(args, store).await,
        "pexpiretime" => cmd_pexpiretime(args, store).await,
        "rename" => cmd_rename(args, store).await,
        "renamenx" => cmd_renamenx(args, store).await,
        "scan" => cmd_scan(args, store).await,
        "keys" => cmd_keys(args, store).await,
        "touch" => cmd_touch(args, store).await,
        "copy" => cmd_copy(args, store).await,
        "object" => cmd_object(args, store).await,

        // Server
        "dbsize" => cmd_dbsize(args, store).await,
        "flushdb" => cmd_flushdb(args, store).await,
        "flushall" => cmd_flushall(args, store).await,
        "info" => cmd_info(args, store, conn).await,
        "config" => cmd_config(args, store).await,
        "command" => cmd_command(args, store).await,
        "client" => cmd_client(args, store, conn).await,
        "latency" => cmd_latency(args, store).await,
        "slowlog" => cmd_slowlog(args, store).await,
        "debug" => cmd_debug(args, store).await,
        "wait" => cmd_wait(args, store).await,
        "xadd" => cmd_xadd(args, store).await,

        _ => format!(
            "-ERR unknown command {}\r\n",
            String::from_utf8_lossy(&args[0])
        )
        .into_bytes(),
    };
    (resp, false)
}

// ═══════════════════════════════════════════════════════════════════════════════
// TESTS
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config;
    use crate::store::Db;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::RwLock;

    fn make_store() -> Store {
        Arc::new(RwLock::new(Db::new(config::DEFAULT_MEMORY_LIMIT)))
    }

    fn make_conn() -> ConnState {
        ConnState::new(0)
    }

    fn args(parts: &[&str]) -> Vec<Vec<u8>> {
        parts.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    fn parse_int_resp(resp: &[u8]) -> i64 {
        assert!(
            resp.starts_with(b":"),
            "expected integer, got {:?}",
            std::str::from_utf8(resp)
        );
        std::str::from_utf8(&resp[1..resp.len() - 2])
            .unwrap()
            .parse()
            .unwrap()
    }

    fn parse_keys_resp(resp: &[u8]) -> Vec<String> {
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
        let mut conn = make_conn();
        let (resp, quit) = dispatch(&args(&["PING"]), &store, &mut conn).await;
        assert_eq!(resp, b"+PONG\r\n");
        assert!(!quit);
    }

    #[tokio::test]
    async fn ping_case_insensitive() {
        let store = make_store();
        let mut conn = make_conn();
        let (resp, _) = dispatch(&args(&["ping"]), &store, &mut conn).await;
        assert_eq!(resp, b"+PONG\r\n");
    }

    #[tokio::test]
    async fn quit_returns_ok_and_sets_quit_flag() {
        let store = make_store();
        let mut conn = make_conn();
        let (resp, quit) = dispatch(&args(&["QUIT"]), &store, &mut conn).await;
        assert_eq!(resp, b"+OK\r\n");
        assert!(quit);
    }

    #[tokio::test]
    async fn unknown_command_returns_error() {
        let store = make_store();
        let mut conn = make_conn();
        let (resp, quit) = dispatch(&args(&["BLORP"]), &store, &mut conn).await;
        assert!(resp.starts_with(b"-ERR unknown command BLORP"));
        assert!(!quit);
    }

    // ── SET ───────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn set_and_get_roundtrip() {
        let store = make_store();
        assert_eq!(
            cmd_set(&args(&["SET", "k", "hello"]), &store).await,
            b"+OK\r\n"
        );
        assert_eq!(
            cmd_get(&args(&["GET", "k"]), &store).await,
            b"$5\r\nhello\r\n"
        );
    }

    #[tokio::test]
    async fn set_overwrites_existing_key() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "first"]), &store).await;
        cmd_set(&args(&["SET", "k", "second"]), &store).await;
        assert_eq!(
            cmd_get(&args(&["GET", "k"]), &store).await,
            b"$6\r\nsecond\r\n"
        );
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
        assert!((4..=5).contains(&secs), "unexpected TTL: {secs}");
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
        assert_eq!(
            cmd_get(&args(&["GET", "missing"]), &store).await,
            b"$-1\r\n"
        );
    }

    #[tokio::test]
    async fn get_wrong_args_returns_error() {
        let store = make_store();
        let resp = cmd_get(&args(&["GET"]), &store).await;
        assert!(resp.starts_with(b"-ERR"));
    }

    #[tokio::test]
    async fn get_increments_hit_counter() {
        let store = Arc::new(RwLock::new(
            Db::new(config::DEFAULT_MEMORY_LIMIT).with_eviction(
                1.0,
                config::EvictionPolicy::Lru,
                HashMap::new(),
            ),
        ));
        cmd_set(&args(&["SET", "k", "v"]), &store).await;
        cmd_get(&args(&["GET", "k"]), &store).await;
        cmd_get(&args(&["GET", "k"]), &store).await;
        cmd_get(&args(&["GET", "k"]), &store).await;
        assert_eq!(
            store
                .read()
                .await
                .entries
                .get("default")
                .and_then(|ns| ns.get("k"))
                .unwrap()
                .hits,
            3
        );
    }

    // ── DEL (single) ─────────────────────────────────────────────────────────

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

    // ── DEL multiple keys ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn del_multiple_keys_returns_count() {
        let store = make_store();
        cmd_set(&args(&["SET", "a", "1"]), &store).await;
        cmd_set(&args(&["SET", "b", "2"]), &store).await;
        cmd_set(&args(&["SET", "c", "3"]), &store).await;
        let resp = cmd_del(&args(&["DEL", "a", "b", "missing"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 2);
    }

    // ── TTL ───────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn ttl_missing_key_returns_minus2() {
        let store = make_store();
        assert_eq!(cmd_ttl(&args(&["TTL", "nope"]), &store).await, b":-2\r\n");
    }

    #[tokio::test]
    async fn ttl_key_without_expiry_returns_minus1() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v"]), &store).await;
        assert_eq!(cmd_ttl(&args(&["TTL", "k"]), &store).await, b":-1\r\n");
    }

    #[tokio::test]
    async fn ttl_wrong_args_returns_error() {
        let store = make_store();
        let resp = cmd_ttl(&args(&["TTL"]), &store).await;
        assert!(resp.starts_with(b"-ERR"));
    }

    // ── INCR ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn incr_creates_key_with_value_1() {
        let store = make_store();
        assert_eq!(
            cmd_incr(&args(&["INCR", "counter"]), &store).await,
            b":1\r\n"
        );
    }

    #[tokio::test]
    async fn incr_increments_existing_value() {
        let store = make_store();
        cmd_set(&args(&["SET", "counter", "5"]), &store).await;
        assert_eq!(
            cmd_incr(&args(&["INCR", "counter"]), &store).await,
            b":6\r\n"
        );
    }

    #[tokio::test]
    async fn incr_wrong_type_returns_error() {
        let store = make_store();
        cmd_lpush(&args(&["LPUSH", "mylist", "v"]), &store).await;
        let resp = cmd_incr(&args(&["INCR", "mylist"]), &store).await;
        assert!(resp.starts_with(b"-WRONGTYPE"));
    }

    // ── INCRBY ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn incrby_increments_by_amount() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "10"]), &store).await;
        let resp = cmd_incrby(&args(&["INCRBY", "k", "5"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 15);
    }

    // ── APPEND / STRLEN ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn append_creates_and_extends() {
        let store = make_store();
        let r1 = cmd_append(&args(&["APPEND", "k", "hello"]), &store).await;
        assert_eq!(parse_int_resp(&r1), 5);
        let r2 = cmd_append(&args(&["APPEND", "k", " world"]), &store).await;
        assert_eq!(parse_int_resp(&r2), 11);
        assert_eq!(
            cmd_get(&args(&["GET", "k"]), &store).await,
            b"$11\r\nhello world\r\n"
        );
    }

    #[tokio::test]
    async fn strlen_returns_length() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "hello"]), &store).await;
        let resp = cmd_strlen(&args(&["STRLEN", "k"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 5);
    }

    #[tokio::test]
    async fn strlen_missing_key_returns_0() {
        let store = make_store();
        let resp = cmd_strlen(&args(&["STRLEN", "missing"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 0);
    }

    // ── LPUSH ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn lpush_creates_list() {
        let store = make_store();
        assert_eq!(
            cmd_lpush(&args(&["LPUSH", "mylist", "a"]), &store).await,
            b":1\r\n"
        );
    }

    #[tokio::test]
    async fn lpush_multiple_values() {
        let store = make_store();
        let resp = cmd_lpush(&args(&["LPUSH", "mylist", "a", "b", "c"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 3);
    }

    // ── RPUSH / RPOP ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn rpush_and_rpop() {
        let store = make_store();
        cmd_rpush(&args(&["RPUSH", "list", "a", "b", "c"]), &store).await;
        let resp = cmd_rpop(&args(&["RPOP", "list"]), &store).await;
        assert_eq!(resp, b"$1\r\nc\r\n");
    }

    #[tokio::test]
    async fn rpush_returns_length() {
        let store = make_store();
        let resp = cmd_rpush(&args(&["RPUSH", "list", "x", "y"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 2);
    }

    // ── MGET / MSET ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn mset_and_mget_roundtrip() {
        let store = make_store();
        cmd_mset(&args(&["MSET", "a", "1", "b", "2", "c", "3"]), &store).await;
        let resp = cmd_mget(&args(&["MGET", "a", "b", "c", "missing"]), &store).await;
        assert!(resp.starts_with(b"*4\r\n"), "expected *4 array");
        // value "1" encoded as bulk string
        let needle: &[u8] = b"$1\r\n1\r\n";
        assert!(
            resp.windows(needle.len()).any(|w| w == needle),
            "mget missing value 1"
        );
        let null_suffix: &[u8] = b"$-1\r\n";
        assert!(
            resp.ends_with(null_suffix),
            "mget missing null for missing key"
        );
    }

    // ── HSET / HGET / HGETALL ────────────────────────────────────────────────

    #[tokio::test]
    async fn hset_and_hget() {
        let store = make_store();
        let resp = cmd_hset(&args(&["HSET", "myhash", "field1", "value1"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 1);
        let resp = cmd_hget(&args(&["HGET", "myhash", "field1"]), &store).await;
        assert_eq!(resp, b"$6\r\nvalue1\r\n");
    }

    #[tokio::test]
    async fn hset_multiple_fields() {
        let store = make_store();
        let resp = cmd_hset(&args(&["HSET", "h", "f1", "v1", "f2", "v2"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 2);
    }

    #[tokio::test]
    async fn hgetall_resp2_flat_array() {
        let store = make_store();
        cmd_hset(&args(&["HSET", "h", "k", "v"]), &store).await;
        let mut conn = make_conn();
        conn.resp_version = 2;
        let resp = cmd_hgetall(&args(&["HGETALL", "h"]), &store, &conn).await;
        assert!(resp.starts_with(b"*2\r\n"));
    }

    #[tokio::test]
    async fn hget_missing_field_returns_null() {
        let store = make_store();
        cmd_hset(&args(&["HSET", "h", "f", "v"]), &store).await;
        let resp = cmd_hget(&args(&["HGET", "h", "nofield"]), &store).await;
        assert_eq!(resp, b"$-1\r\n");
    }

    // ── SADD / SMEMBERS ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn sadd_and_smembers() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "myset", "a", "b", "c"]), &store).await;
        let resp = cmd_smembers(&args(&["SMEMBERS", "myset"]), &store).await;
        let keys = parse_keys_resp(&resp);
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn sadd_returns_new_count() {
        let store = make_store();
        let r1 = cmd_sadd(&args(&["SADD", "s", "a", "b"]), &store).await;
        assert_eq!(parse_int_resp(&r1), 2);
        let r2 = cmd_sadd(&args(&["SADD", "s", "b", "c"]), &store).await;
        assert_eq!(parse_int_resp(&r2), 1); // only c is new
    }

    // ── ZADD / ZRANGE ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn zadd_and_zrange() {
        let store = make_store();
        cmd_zadd(&args(&["ZADD", "z", "1", "a", "2", "b", "3", "c"]), &store).await;
        let resp = cmd_zrange(&args(&["ZRANGE", "z", "0", "-1"]), &store).await;
        let keys = parse_keys_resp(&resp);
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn zadd_returns_added_count() {
        let store = make_store();
        let resp = cmd_zadd(&args(&["ZADD", "z", "1", "a", "2", "b"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 2);
    }

    #[tokio::test]
    async fn zrange_withscores() {
        let store = make_store();
        cmd_zadd(&args(&["ZADD", "z", "1.5", "a"]), &store).await;
        let resp = cmd_zrange(&args(&["ZRANGE", "z", "0", "-1", "WITHSCORES"]), &store).await;
        assert!(resp.contains(&b'a'));
        assert!(resp.windows(3).any(|w| w == b"1.5"));
    }

    // ── TYPE ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn type_returns_correct_types() {
        let store = make_store();
        cmd_set(&args(&["SET", "s", "v"]), &store).await;
        cmd_lpush(&args(&["LPUSH", "l", "v"]), &store).await;
        cmd_hset(&args(&["HSET", "h", "f", "v"]), &store).await;
        cmd_sadd(&args(&["SADD", "st", "v"]), &store).await;
        cmd_zadd(&args(&["ZADD", "z", "1", "v"]), &store).await;

        assert_eq!(
            cmd_type(&args(&["TYPE", "s"]), &store).await,
            b"+string\r\n"
        );
        assert_eq!(cmd_type(&args(&["TYPE", "l"]), &store).await, b"+list\r\n");
        assert_eq!(cmd_type(&args(&["TYPE", "h"]), &store).await, b"+hash\r\n");
        assert_eq!(cmd_type(&args(&["TYPE", "st"]), &store).await, b"+set\r\n");
        assert_eq!(cmd_type(&args(&["TYPE", "z"]), &store).await, b"+zset\r\n");
        assert_eq!(
            cmd_type(&args(&["TYPE", "missing"]), &store).await,
            b"+none\r\n"
        );
    }

    // ── DBSIZE ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn dbsize_counts_all_keys() {
        let store = make_store();
        cmd_set(&args(&["SET", "a", "1"]), &store).await;
        cmd_set(&args(&["SET", "b", "2"]), &store).await;
        cmd_set(&args(&["SET", "ns/c", "3"]), &store).await;
        let resp = cmd_dbsize(&args(&["DBSIZE"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 3);
    }

    // ── EXISTS ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn exists_returns_count() {
        let store = make_store();
        cmd_set(&args(&["SET", "a", "1"]), &store).await;
        cmd_set(&args(&["SET", "b", "2"]), &store).await;
        let resp = cmd_exists(&args(&["EXISTS", "a", "b", "missing"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 2);
    }

    // ── EXPIRE ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn expire_sets_ttl() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v"]), &store).await;
        let resp = cmd_expire(&args(&["EXPIRE", "k", "100"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 1);
        let ttl = parse_int_resp(&cmd_ttl(&args(&["TTL", "k"]), &store).await);
        assert!(ttl > 90 && ttl <= 100);
    }

    #[tokio::test]
    async fn expire_missing_key_returns_0() {
        let store = make_store();
        let resp = cmd_expire(&args(&["EXPIRE", "missing", "100"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 0);
    }

    #[tokio::test]
    async fn scheduler_expires_key_without_followup_access() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v", "PX", "20"]), &store).await;

        // Poll for asynchronous scheduler-driven expiration.
        for _ in 0..30 {
            let exists = store
                .read()
                .await
                .entries
                .get("default")
                .and_then(|ns| ns.get("k"))
                .is_some();
            if !exists {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let exists = store
            .read()
            .await
            .entries
            .get("default")
            .and_then(|ns| ns.get("k"))
            .is_some();
        assert!(!exists, "key should be removed by expiry scheduler");
    }

    #[tokio::test]
    async fn stale_expiry_events_do_not_delete_after_persist() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v", "PX", "20"]), &store).await;
        assert_eq!(
            cmd_persist(&args(&["PERSIST", "k"]), &store).await,
            b":1\r\n"
        );

        tokio::time::sleep(Duration::from_millis(60)).await;

        assert_eq!(cmd_get(&args(&["GET", "k"]), &store).await, b"$1\r\nv\r\n");
    }

    #[tokio::test]
    async fn stale_shorter_expiry_does_not_override_longer_one() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v", "PX", "20"]), &store).await;
        assert_eq!(
            cmd_pexpire(&args(&["PEXPIRE", "k", "120"]), &store).await,
            b":1\r\n"
        );

        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(cmd_get(&args(&["GET", "k"]), &store).await, b"$1\r\nv\r\n");
    }

    // ── Namespace tests ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn namespaced_keys_are_isolated() {
        let store = make_store();
        cmd_set(&args(&["SET", "ns1/k", "v1"]), &store).await;
        cmd_set(&args(&["SET", "ns2/k", "v2"]), &store).await;
        assert_eq!(
            cmd_get(&args(&["GET", "ns1/k"]), &store).await,
            b"$2\r\nv1\r\n"
        );
        assert_eq!(
            cmd_get(&args(&["GET", "ns2/k"]), &store).await,
            b"$2\r\nv2\r\n"
        );
    }

    #[tokio::test]
    async fn namespaced_del_only_affects_its_namespace() {
        let store = make_store();
        cmd_set(&args(&["SET", "ns1/k", "v1"]), &store).await;
        cmd_set(&args(&["SET", "ns2/k", "v2"]), &store).await;
        cmd_del(&args(&["DEL", "ns1/k"]), &store).await;
        assert_eq!(cmd_get(&args(&["GET", "ns1/k"]), &store).await, b"$-1\r\n");
        assert_eq!(
            cmd_get(&args(&["GET", "ns2/k"]), &store).await,
            b"$2\r\nv2\r\n"
        );
    }

    #[tokio::test]
    async fn namespaced_incr_counters_are_isolated() {
        let store = make_store();
        cmd_incr(&args(&["INCR", "ns1/counter"]), &store).await;
        cmd_incr(&args(&["INCR", "ns1/counter"]), &store).await;
        cmd_incr(&args(&["INCR", "ns2/counter"]), &store).await;
        assert_eq!(
            cmd_incr(&args(&["INCR", "ns1/counter"]), &store).await,
            b":3\r\n"
        );
        assert_eq!(
            cmd_incr(&args(&["INCR", "ns2/counter"]), &store).await,
            b":2\r\n"
        );
    }

    #[tokio::test]
    async fn namespaced_lpush_lists_are_isolated() {
        let store = make_store();
        cmd_lpush(&args(&["LPUSH", "ns1/list", "a"]), &store).await;
        cmd_lpush(&args(&["LPUSH", "ns2/list", "b"]), &store).await;
        assert_eq!(
            cmd_lpush(&args(&["LPUSH", "ns1/list", "c"]), &store).await,
            b":2\r\n"
        );
        assert_eq!(
            cmd_lpush(&args(&["LPUSH", "ns2/list", "d"]), &store).await,
            b":2\r\n"
        );
    }

    #[tokio::test]
    async fn namespaced_ttl_is_per_namespace() {
        let store = make_store();
        cmd_set(&args(&["SET", "ns1/k", "v", "EX", "100"]), &store).await;
        cmd_set(&args(&["SET", "ns2/k", "v"]), &store).await;
        let secs = parse_int_resp(&cmd_ttl(&args(&["TTL", "ns1/k"]), &store).await);
        assert!(secs > 0, "ns1/k should have TTL");
        // ns2/k has no expiry -> -1
        assert_eq!(cmd_ttl(&args(&["TTL", "ns2/k"]), &store).await, b":-1\r\n");
    }

    #[tokio::test]
    async fn del_last_key_in_namespace_cleans_up_namespace_map() {
        let store = make_store();
        cmd_set(&args(&["SET", "ns/k", "v"]), &store).await;
        cmd_del(&args(&["DEL", "ns/k"]), &store).await;
        assert!(!store.read().await.entries.contains_key("ns"));
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
        assert!(
            cmd_keys(&args(&["KEYS"]), &store)
                .await
                .starts_with(b"-ERR")
        );
    }

    // ── Memory limit tests ────────────────────────────────────────────────────

    #[tokio::test]
    async fn set_rejected_when_memory_limit_exceeded() {
        let store = Arc::new(RwLock::new(Db::new(1)));
        let resp = cmd_set(&args(&["SET", "k", "toolarge"]), &store).await;
        assert!(resp.starts_with(b"-ERR OOM"));
    }

    // ── EAR tests ─────────────────────────────────────────────────────────────

    fn make_ear_store() -> Store {
        let mut ns_policies = HashMap::new();
        ns_policies.insert(
            "session".to_string(),
            config::EvictionPolicy::ExpireAfterRead,
        );
        let db = Db::new(config::DEFAULT_MEMORY_LIMIT).with_eviction(
            1.0,
            config::EvictionPolicy::None,
            ns_policies,
        );
        Arc::new(RwLock::new(db))
    }

    #[tokio::test]
    async fn get_on_ear_namespace_marks_key_pending() {
        let store = make_ear_store();
        cmd_set(&args(&["SET", "session/token", "abc"]), &store).await;
        cmd_get(&args(&["GET", "session/token"]), &store).await;
        assert!(
            store
                .read()
                .await
                .ear_pending
                .contains(&("session".to_string(), "token".to_string()))
        );
    }

    #[tokio::test]
    async fn get_on_non_ear_namespace_does_not_mark() {
        let store = make_ear_store();
        cmd_set(&args(&["SET", "default/foo", "bar"]), &store).await;
        cmd_get(&args(&["GET", "default/foo"]), &store).await;
        assert!(
            !store
                .read()
                .await
                .ear_pending
                .contains(&("default".to_string(), "foo".to_string()))
        );
    }

    #[tokio::test]
    async fn get_on_missing_key_does_not_mark() {
        let store = make_ear_store();
        cmd_get(&args(&["GET", "session/missing"]), &store).await;
        assert!(store.read().await.ear_pending.is_empty());
    }

    #[tokio::test]
    async fn ear_sweep_deletes_marked_keys() {
        let store = make_ear_store();
        cmd_set(&args(&["SET", "session/token", "abc"]), &store).await;
        // Mark the key for EAR eviction.
        store.write().await.mark_ear("session", "token");
        // Simulate a single sweep cycle.
        let pending: Vec<(String, String)> =
            store.read().await.ear_pending.iter().cloned().collect();
        {
            let mut db = store.write().await;
            for (ns, key) in &pending {
                if db.ear_pending.contains(&(ns.clone(), key.clone()))
                    && db
                        .entries
                        .get(ns)
                        .and_then(|m| m.get(key.as_str()))
                        .is_some_and(|e| !e.is_expired())
                {
                    db.delete(ns, key);
                }
            }
        }
        assert!(
            store
                .read()
                .await
                .entries
                .get("session")
                .and_then(|ns| ns.get("token"))
                .is_none()
        );
    }

    #[tokio::test]
    async fn ear_write_after_mark_cancels_eviction() {
        let store = make_ear_store();
        cmd_set(&args(&["SET", "session/token", "abc"]), &store).await;
        // Mark for eviction.
        store.write().await.mark_ear("session", "token");
        assert!(
            store
                .read()
                .await
                .ear_pending
                .contains(&("session".to_string(), "token".to_string()))
        );
        // A write clears the EAR mark.
        cmd_set(&args(&["SET", "session/token", "new"]), &store).await;
        assert!(
            !store
                .read()
                .await
                .ear_pending
                .contains(&("session".to_string(), "token".to_string()))
        );
    }

    #[tokio::test]
    async fn hget_found_field_marks_hash_key() {
        let store = make_ear_store();
        cmd_hset(&args(&["HSET", "session/h", "f1", "v1"]), &store).await;
        cmd_hget(&args(&["HGET", "session/h", "f1"]), &store).await;
        assert!(
            store
                .read()
                .await
                .ear_pending
                .contains(&("session".to_string(), "h".to_string()))
        );
    }

    #[tokio::test]
    async fn hget_missing_field_does_not_mark_hash_key() {
        let store = make_ear_store();
        cmd_hset(&args(&["HSET", "session/h", "f1", "v1"]), &store).await;
        cmd_hget(&args(&["HGET", "session/h", "missing"]), &store).await;
        assert!(
            !store
                .read()
                .await
                .ear_pending
                .contains(&("session".to_string(), "h".to_string()))
        );
    }

    #[tokio::test]
    async fn run_ear_sweep_task_deletes_keys() {
        let store = make_ear_store();
        cmd_set(&args(&["SET", "session/tok", "val"]), &store).await;
        // GET marks the key for EAR eviction.
        cmd_get(&args(&["GET", "session/tok"]), &store).await;
        assert!(
            store
                .read()
                .await
                .ear_pending
                .contains(&("session".to_string(), "tok".to_string()))
        );
        // Spawn the sweep task.
        tokio::spawn(run_ear_sweep(Arc::clone(&store)));
        // Wait for the sweep to run (interval is 1 s, but give it headroom).
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if store
                .read()
                .await
                .entries
                .get("session")
                .and_then(|ns| ns.get("tok"))
                .is_none()
            {
                return;
            }
        }
        panic!("EAR sweep did not delete the key within 2 seconds");
    }
}
