use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, LazyLock, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;
use tracing::debug;

use crate::pubsub::{PubSubHub, PubSubMessage};

use crate::resp::{
    append_array_header, append_bulk, append_int, append_null, build_array, resp_array, resp_bulk,
    resp_err, resp_err_invalid_expire_time, resp_err_invalid_lex_range,
    resp_err_min_or_max_not_float, resp_err_no_such_key, resp_err_not_float, resp_err_not_integer,
    resp_err_oom, resp_err_syntax, resp_err_unknown_subcommand, resp_int, resp_map, resp_null,
    resp_null_array, resp_ok, resp_pong, resp_usize, resp_verbatim, resp_wrongtype, wrong_args,
};
use crate::store::{Db, Entry, Store, StoreMetrics, Value, ValueCell, ZEntry, ZSetData};

// ── Connection state ──────────────────────────────────────────────────────────

/// Queued-command state active between MULTI and EXEC/DISCARD.
pub(crate) struct MultiState {
    pub queued: Vec<Vec<Vec<u8>>>,
    /// Set when a command is rejected during queuing (EXEC → EXECABORT).
    pub error: bool,
}

impl MultiState {
    fn new() -> Self {
        Self { queued: Vec::new(), error: false }
    }
}

pub(crate) struct ConnState {
    pub resp_version: u8,
    pub client_name: Option<Vec<u8>>,
    pub client_id: u64,
    /// Active transaction (MULTI issued but EXEC/DISCARD not yet received).
    pub multi_state: Option<MultiState>,
    /// (namespace, key) → write_version at WATCH time.
    pub watched: HashMap<(String, String), u64>,
    /// Sender half of the per-connection pub-sub message channel.
    /// Created lazily on the first SUBSCRIBE/PSUBSCRIBE command so that
    /// non-pub-sub connections pay no channel allocation cost.
    pub pubsub_tx: Option<mpsc::UnboundedSender<PubSubMessage>>,
    /// Receiver half, held here only until the server loop picks it up via
    /// `take()` after the first subscribe command returns.
    pub pubsub_rx_slot: Option<mpsc::UnboundedReceiver<PubSubMessage>>,
    /// Channels this connection has explicitly subscribed to.
    pub subscribed_channels: HashSet<String>,
    /// Patterns this connection has subscribed to via PSUBSCRIBE.
    pub subscribed_patterns: HashSet<String>,
}

impl ConnState {
    pub(crate) fn new(id: u64) -> Self {
        Self {
            resp_version: 2,
            client_name: None,
            client_id: id,
            multi_state: None,
            watched: HashMap::new(),
            pubsub_tx: None,
            pubsub_rx_slot: None,
            subscribed_channels: HashSet::new(),
            subscribed_patterns: HashSet::new(),
        }
    }

    /// Returns the sender for this connection's pub-sub channel, creating the
    /// channel on the first call.  The receiver is placed in `pubsub_rx_slot`
    /// for the server loop to `take()` after dispatch returns.
    pub(crate) fn pubsub_sender(&mut self) -> mpsc::UnboundedSender<PubSubMessage> {
        if let Some(tx) = &self.pubsub_tx {
            return tx.clone();
        }
        let (tx, rx) = mpsc::unbounded_channel();
        self.pubsub_tx = Some(tx.clone());
        self.pubsub_rx_slot = Some(rx);
        tx
    }

    /// True when the connection is in pub-sub mode (at least one active subscription).
    pub(crate) fn in_pubsub(&self) -> bool {
        !self.subscribed_channels.is_empty() || !self.subscribed_patterns.is_empty()
    }
}

// ── Namespace key parsing ─────────────────────────────────────────────────────

fn parse_ns_key(raw: &[u8]) -> (std::borrow::Cow<'_, str>, std::borrow::Cow<'_, str>) {
    use std::borrow::Cow;
    match std::str::from_utf8(raw) {
        Ok(s) => match s.find('/') {
            Some(pos) => (Cow::Borrowed(&s[..pos]), Cow::Borrowed(&s[pos + 1..])),
            None => (Cow::Borrowed("default"), Cow::Borrowed(s)),
        },
        Err(_) => {
            // Non-UTF-8 key: rare, fall back to lossy conversion.
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
    RwLock<HashMap<usize, mpsc::UnboundedSender<ExpiryEvent>>>,
> = LazyLock::new(|| RwLock::new(HashMap::new()));

fn store_scheduler_key(store: &Store) -> usize {
    Arc::as_ptr(store) as usize
}

fn ensure_expiry_scheduler(store: &Store) -> mpsc::UnboundedSender<ExpiryEvent> {
    let key = store_scheduler_key(store);
    // Fast path: shared read lock.
    {
        let guard = EXPIRY_QUEUE_TX_BY_STORE
            .read()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(tx) = guard.get(&key)
            && !tx.is_closed()
        {
            return tx.clone();
        }
    }
    // Slow path: exclusive write lock with double-check.
    let mut guard = EXPIRY_QUEUE_TX_BY_STORE
        .write()
        .unwrap_or_else(|e| e.into_inner());
    if let Some(tx) = guard.get(&key)
        && !tx.is_closed()
    {
        return tx.clone();
    }
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(run_expiry_scheduler(Arc::clone(store), rx, key));
    guard.insert(key, tx.clone());
    tx
}

fn schedule_expiry(store: &Store, ns: &str, key: &str, deadline: Instant) {
    let key_id = store_scheduler_key(store);
    for _ in 0..2 {
        let tx = ensure_expiry_scheduler(store);
        if tx
            .send(ExpiryEvent {
                ns: ns.to_owned(),
                key: key.to_owned(),
                deadline,
            })
            .is_ok()
        {
            return;
        }
        let mut guard = EXPIRY_QUEUE_TX_BY_STORE
            .write()
            .unwrap_or_else(|e| e.into_inner());
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
        .write()
        .unwrap_or_else(|e| e.into_inner());
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
        // Single write lock: drain ear_pending and delete live entries.
        let deleted = {
            let mut db = store.write().await;
            if db.ear_pending.is_empty() {
                continue;
            }
            let pending: Vec<(String, String)> = db.ear_pending.drain().collect();
            let mut count = 0u64;
            for (ns, key) in &pending {
                // put() clears EAR marks for re-written keys; re-check not-expired.
                if db
                    .entries
                    .get::<str>(ns.as_ref())
                    .and_then(|m| m.get::<str>(key.as_ref()))
                    .is_some_and(|e| !e.is_expired())
                {
                    db.delete(ns, key);
                    count += 1;
                }
            }
            count
        };
        if deleted > 0 {
            tracing::debug!(deleted, "EAR sweep deleted keys");
            metrics::counter!("kvns_ear_evictions_total").increment(deleted);
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

/// Iterative glob match — O(N·M) worst case, no stack growth.
///
/// Supports Redis-compatible glob syntax: `*` (any sequence), `?` (any single
/// byte), `[abc]` / `[a-z]` / `[^abc]` character classes.
pub(crate) fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let mut pi = 0usize; // current position in pattern
    let mut ti = 0usize; // current position in text
    // Saved positions used for backtracking when a `*` was seen.
    // `usize::MAX` means "no star seen yet".
    let mut star_pi = usize::MAX;
    let mut star_ti = 0usize;

    loop {
        if ti < text.len() {
            if pi < pattern.len() {
                match pattern[pi] {
                    b'*' => {
                        // Record where the star is; advance pattern but not text
                        // so we first try matching zero characters.
                        star_pi = pi;
                        star_ti = ti;
                        pi += 1;
                    }
                    b'?' => {
                        pi += 1;
                        ti += 1;
                    }
                    b'[' => {
                        match pattern[pi + 1..].iter().position(|&b| b == b']') {
                            Some(rel_end) => {
                                let class = &pattern[pi + 1..pi + 1 + rel_end];
                                let close = pi + 1 + rel_end; // index of ']'
                                if class_match(class, text[ti]) {
                                    pi = close + 1;
                                    ti += 1;
                                } else if star_pi != usize::MAX {
                                    pi = star_pi + 1;
                                    star_ti += 1;
                                    ti = star_ti;
                                } else {
                                    return false;
                                }
                            }
                            None => {
                                // Unclosed '[': treat as literal '['.
                                if text[ti] == b'[' {
                                    pi += 1;
                                    ti += 1;
                                } else if star_pi != usize::MAX {
                                    pi = star_pi + 1;
                                    star_ti += 1;
                                    ti = star_ti;
                                } else {
                                    return false;
                                }
                            }
                        }
                    }
                    p_ch => {
                        if p_ch == text[ti] {
                            pi += 1;
                            ti += 1;
                        } else if star_pi != usize::MAX {
                            pi = star_pi + 1;
                            star_ti += 1;
                            ti = star_ti;
                        } else {
                            return false;
                        }
                    }
                }
            } else if star_pi != usize::MAX {
                // Pattern exhausted but text has more: let the last `*` consume one more char.
                pi = star_pi + 1;
                star_ti += 1;
                ti = star_ti;
            } else {
                return false;
            }
        } else {
            // Text exhausted: skip any trailing `*`s in pattern.
            while pi < pattern.len() && pattern[pi] == b'*' {
                pi += 1;
            }
            return pi == pattern.len();
        }
    }
}

// ── ZSet helpers ──────────────────────────────────────────────────────────────

/// Insert or update a member in the dual-structure sorted set. Returns true if
/// the member was NEW (not an update of an existing member).
fn zset_insert_or_update(data: &mut ZSetData, score: f64, member: Vec<u8>) -> bool {
    if let Some(&old_score) = data.index.get(&member) {
        // O(log n): find old position by binary search, remove it.
        let old_pos = data.sorted.partition_point(|e| {
            e.score < old_score || (e.score == old_score && e.member.as_slice() < member.as_slice())
        });
        data.sorted.remove(old_pos);
        // O(log n): find new position and insert.
        let new_pos = data.sorted.partition_point(|e| {
            e.score < score || (e.score == score && e.member.as_slice() < member.as_slice())
        });
        data.sorted.insert(
            new_pos,
            ZEntry {
                score,
                member: member.clone(),
            },
        );
        data.index.insert(member, score);
        false // updated, not new
    } else {
        // O(log n): binary-search insertion point.
        let pos = data.sorted.partition_point(|e| {
            e.score < score || (e.score == score && e.member.as_slice() < member.as_slice())
        });
        data.sorted.insert(
            pos,
            ZEntry {
                score,
                member: member.clone(),
            },
        );
        data.index.insert(member, score);
        true // new member
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

// ── Integer formatting ─────────────────────────────────────────────────────────

/// Format `n` into a `Vec<u8>` using a stack buffer, avoiding the intermediate
/// `String` heap allocation that `n.to_string().into_bytes()` would incur.
/// i64::MIN is 20 chars including the leading '-'; 21 bytes is sufficient.
pub(crate) fn i64_to_bytes(n: i64) -> Vec<u8> {
    use std::io::Write;
    let mut buf = [0u8; 21];
    let mut cur = std::io::Cursor::new(&mut buf[..]);
    write!(cur, "{n}").expect("i64 always formats without error");
    let len = cur.position() as usize;
    buf[..len].to_vec()
}

// ── OOM helper ────────────────────────────────────────────────────────────────

fn check_oom_net(db: &mut Db, ns: &str, net: usize) -> bool {
    if db.used_bytes.saturating_add(net) > db.memory_limit
        && !db.evict_for_write(ns, net)
    {
        return false;
    }
    true
}

fn check_oom(db: &mut Db, ns: &str, key: &str, new_byte_len: usize) -> bool {
    check_oom_net(db, ns, db.net_delta(ns, key, new_byte_len))
}

fn resp_array_of_nulls(count: usize) -> std::borrow::Cow<'static, [u8]> {
    let mut out = Vec::new();
    append_array_header(&mut out, count);
    for _ in 0..count {
        append_null(&mut out);
    }
    std::borrow::Cow::Owned(out)
}

async fn cleanup_expired_key(store: &Store, ns: &str, key: &str) {
    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
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
            .get::<str>(ns.as_ref())
            .and_then(|m| m.get::<str>(key.as_ref()))
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
    Found(ZSetData, bool), // data, is_ear
}

async fn read_zset_snapshot(store: &Store, ns: &str, key: &str) -> ZsetLookup {
    let db = store.read().await;
    match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
        None => ZsetLookup::Missing,
        Some(entry) if entry.is_expired() => ZsetLookup::Expired,
        Some(entry) => match entry.value.as_zset() {
            Some(z) => ZsetLookup::Found(z.clone(), db.is_ear_namespace(ns)),
            None => ZsetLookup::WrongType,
        },
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// STRING COMMANDS
// ═══════════════════════════════════════════════════════════════════════════════

pub(crate) async fn cmd_set(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    let start = Instant::now();
    if args.len() != 3 && args.len() != 5 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let value = args[2].clone();

    let ttl = if args.len() == 5 {
        let amount: u64 = match String::from_utf8_lossy(&args[4]).parse() {
            Ok(v) => v,
            Err(_) => return resp_err_invalid_expire_time(),
        };
        Some(if args[3].eq_ignore_ascii_case(b"PX") {
            Duration::from_millis(amount)
        } else {
            Duration::from_secs(amount)
        })
    } else {
        None
    };

    // Build the entry before acquiring the lock; Entry::new calls Instant::now()
    // and we don't want that inside the critical section.
    let value_len = value.len();
    let entry = Entry::new(value, ttl);
    let expiry = entry.expiry;

    let mut db = store.write().await;
    let net_delta = db.net_delta(&ns, &key, value_len);
    if db.used_bytes.saturating_add(net_delta) > db.memory_limit
        && !db.evict_for_write(&ns, net_delta)
    {
        return resp_err_oom();
    }
    let m = db.put_deferred(ns.as_ref(), key.as_ref(), entry);
    drop(db);
    // Emit Prometheus gauges after releasing the write lock to reduce contention.
    m.emit();
    debug!(namespace = %ns, key = %key, ttl = ?ttl, "SET");

    if let Some(deadline) = expiry {
        schedule_expiry(store, ns.as_ref(), key.as_ref(), deadline);
    }

    metrics::histogram!("kvns_command_duration_seconds", "command" => "set")
        .record(start.elapsed().as_secs_f64());
    resp_ok()
}

async fn cmd_get(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    enum ReadGet {
        Missing,
        Expired,
        WrongType,
        Value(std::borrow::Cow<'static, [u8]>, bool), // resp-encoded value, is_ear
    }

    let read_state = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => ReadGet::Missing,
            Some(entry) if entry.is_expired() => ReadGet::Expired,
            Some(entry) => match entry.value.as_string() {
                None => ReadGet::WrongType,
                Some(bytes) => {
                    // Atomic increment: no write lock needed for hit tracking.
                    if db.tracks_hits(&ns) {
                        entry
                            .hits
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    // Build the RESP response while still holding the read lock so
                    // we copy the value bytes exactly once (into the response buffer)
                    // rather than first into an intermediate Vec and then again into
                    // the response.
                    ReadGet::Value(resp_bulk(bytes), db.is_ear_namespace(&ns))
                }
            },
        }
    };

    match read_state {
        ReadGet::Missing => resp_null(),
        ReadGet::Expired => {
            cleanup_expired_key(store, &ns, &key).await;
            resp_null()
        }
        ReadGet::WrongType => resp_wrongtype(),
        ReadGet::Value(resp, is_ear) => {
            if is_ear {
                let mut db = store.write().await;
                db.mark_ear(&ns, &key);
            }
            resp
        }
    }
}

async fn cmd_mget(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let keys: Vec<(std::borrow::Cow<'_, str>, std::borrow::Cow<'_, str>)> = args[1..].iter().map(|a| parse_ns_key(a)).collect();
    let (out, expired_keys, found_keys, ear_ns) = {
        let db = store.read().await;
        let mut out = Vec::new();
        append_array_header(&mut out, keys.len());
        let mut expired_keys: Vec<(String, String)> = Vec::new();
        let mut found_keys: Vec<(String, String)> = Vec::new();
        for (ns, key) in &keys {
            match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
                None => append_null(&mut out),
                Some(entry) if entry.is_expired() => {
                    expired_keys.push((ns.as_ref().to_owned(), key.as_ref().to_owned()));
                    append_null(&mut out);
                }
                Some(entry) => match entry.value.as_string() {
                    None => append_null(&mut out),
                    Some(bytes) => {
                        append_bulk(&mut out, bytes);
                        found_keys.push((ns.as_ref().to_owned(), key.as_ref().to_owned()));
                    }
                },
            }
        }
        // Capture EAR namespaces while holding the read lock.
        let ear_ns: HashSet<String> = found_keys
            .iter()
            .filter(|(ns, _)| db.is_ear_namespace(ns))
            .map(|(ns, _)| ns.clone())
            .collect();
        (out, expired_keys, found_keys, ear_ns)
    };
    cleanup_expired_keys(store, &expired_keys).await;
    if !ear_ns.is_empty() {
        let mut db = store.write().await;
        for (ns, key) in &found_keys {
            if ear_ns.contains::<str>(ns.as_ref()) {
                db.mark_ear(ns, key);
            }
        }
    }
    out.into()
}

async fn cmd_mset(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
        return wrong_args(&args[0]);
    }
    let mut db = store.write().await;
    let mut metric_updates: Vec<StoreMetrics> = Vec::new();
    let mut i = 1;
    while i + 1 < args.len() {
        let (ns, key) = parse_ns_key(&args[i]);
        let value = args[i + 1].clone();
        if !check_oom(&mut db, &ns, &key, value.len()) {
            return resp_err_oom();
        }
        metric_updates.push(db.put_deferred(ns.as_ref(), key.as_ref(), Entry::new(value, None)));
        i += 2;
    }
    drop(db);
    for m in metric_updates {
        m.emit();
    }
    resp_ok()
}

async fn cmd_msetnx(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
        return wrong_args(&args[0]);
    }
    let pairs: Vec<(std::borrow::Cow<'_, str>, std::borrow::Cow<'_, str>, Vec<u8>)> = args[1..]
        .chunks(2)
        .map(|c| {
            let (ns, key) = parse_ns_key(&c[0]);
            (ns, key, c[1].clone())
        })
        .collect();

    let mut db = store.write().await;
    let mut metric_updates: Vec<StoreMetrics> = Vec::with_capacity(pairs.len());
    // Check that none of the keys exist (non-expired)
    for (ns, key, _) in &pairs {
        if let Some(e) = db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref()))
            && !e.is_expired()
        {
            return resp_int(0);
        }
    }
    for (ns, key, value) in pairs {
        if !check_oom(&mut db, &ns, &key, value.len()) {
            return resp_err_oom();
        }
        metric_updates.push(db.put_deferred(ns.as_ref(), key.as_ref(), Entry::new(value, None)));
    }
    drop(db);
    for m in metric_updates {
        m.emit();
    }
    resp_int(1)
}

async fn cmd_setnx(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let value = args[2].clone();
    let mut db = store.write().await;
    if let Some(e) = db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref()))
        && !e.is_expired()
    {
        return resp_int(0);
    }
    if !check_oom(&mut db, &ns, &key, value.len()) {
        return resp_err_oom();
    }
    let m = db.put_deferred(ns.as_ref(), key.as_ref(), Entry::new(value, None));
    drop(db);
    m.emit();
    resp_int(1)
}

async fn cmd_getset(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let new_value = args[2].clone();
    let mut db = store.write().await;
    db.purge_if_expired(&ns, &key);
    let old = match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
        None => resp_null(),
        Some(entry) => match entry.value.as_string() {
            None => return resp_wrongtype(),
            Some(bytes) => resp_bulk(bytes),
        },
    };
    if !check_oom(&mut db, &ns, &key, new_value.len()) {
        return resp_err_oom();
    }
    let m = db.put_deferred(ns.as_ref(), key.as_ref(), Entry::new(new_value, None));
    drop(db);
    m.emit();
    old
}

async fn cmd_getdel(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_null();
    }
    // Clone value before releasing borrow, then delete.
    let val = match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
        None => return resp_null(),
        Some(entry) => match entry.value.as_string() {
            None => return resp_wrongtype(),
            Some(bytes) => resp_bulk(bytes),
        },
    };
    db.delete(&ns, &key);
    val
}

async fn cmd_getex(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_null();
    }
    // Get value first
    let (val_bytes, _old_expiry) = match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
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

    // Helper: convert a unix timestamp to an Instant.
    let unix_to_instant = |target: SystemTime| -> Instant {
        match target.duration_since(SystemTime::now()) {
            Ok(d) => Instant::now() + d,
            Err(_) => Instant::now(),
        }
    };

    let new_expiry: Option<Option<Instant>> = if opt == "PERSIST" {
        Some(None)
    } else {
        if args.len() < 4 {
            return resp_err_syntax();
        }
        let amount: u64 = match std::str::from_utf8(&args[3]).ok().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return resp_err_invalid_expire_time(),
        };
        match opt.as_str() {
            "EX" => Some(Some(Instant::now() + Duration::from_secs(amount))),
            "PX" => Some(Some(Instant::now() + Duration::from_millis(amount))),
            "EXAT" => Some(Some(unix_to_instant(SystemTime::UNIX_EPOCH + Duration::from_secs(amount)))),
            "PXAT" => Some(Some(unix_to_instant(SystemTime::UNIX_EPOCH + Duration::from_millis(amount)))),
            _ => return resp_err_syntax(),
        }
    };

    if let Some(new_exp) = new_expiry
        && let Some(entry) = db.entries.get_mut::<str>(ns.as_ref()).and_then(|m| m.get_mut::<str>(key.as_ref()))
    {
        entry.expiry = new_exp;
        let deadline = new_exp;
        drop(db);
        if let Some(d) = deadline {
            schedule_expiry(store, ns.as_ref(), key.as_ref(), d);
        }
    }
    response
}

async fn cmd_append(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let append_data = args[2].clone();
    let mut db = store.write().await;
    db.purge_if_expired(&ns, &key);
    let existing_len: usize = match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
        None => 0,
        Some(e) => match e.value.as_string() {
            None => return resp_wrongtype(),
            Some(b) => b.len(),
        },
    };
    let new_len = existing_len + append_data.len();
    if !check_oom_net(&mut db, &ns, append_data.len()) {
        return resp_err_oom();
    }
    let entry = db
        .entries
        .entry(ns.clone().into_owned())
        .or_default()
        .entry(key.clone().into_owned())
        .or_insert_with(|| Entry::new(vec![], None));
    match entry.value_mut().as_string_mut() {
        None => return resp_wrongtype(),
        Some(b) => b.extend_from_slice(&append_data),
    }
    let result_len = new_len;
    let delta = Db::entry_size(&ns, &key, result_len).saturating_sub(Db::entry_size(
        &ns,
        &key,
        existing_len,
    ));
    db.used_bytes = db.used_bytes.saturating_add(delta);
    let nb = db.namespace_bytes.entry(ns.clone().into_owned()).or_insert(0);
    *nb = nb.saturating_add(delta);
    let ns_bytes = *nb;
    let total_bytes = db.used_bytes;
    let ns_str = ns.as_ref().to_owned();
    drop(db);
    metrics::gauge!("kvns_memory_used_bytes", "namespace" => ns_str).set(ns_bytes as f64);
    metrics::gauge!("kvns_memory_used_bytes_total").set(total_bytes as f64);
    resp_usize(result_len)
}

async fn cmd_strlen(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let (resp, expired) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => (resp_int(0), false),
            Some(entry) if entry.is_expired() => (resp_int(0), true),
            Some(entry) => match entry.value.as_string() {
                None => (resp_wrongtype(), false),
                Some(bytes) => (resp_usize(bytes.len()), false),
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    }
    resp
}

/// Shared helper for INCR, INCRBY, DECR, DECRBY — reads the current integer
/// value, applies `delta`, stores the result, and returns a RESP integer.
async fn apply_integer_op(
    ns: &str,
    key: &str,
    store: &Store,
    delta: i64,
    overflow_msg: &str,
) -> std::borrow::Cow<'static, [u8]> {
    let mut db = store.write().await;
    db.purge_if_expired(ns, key);
    let current: i64 = match db.entries.get::<str>(ns).and_then(|m| m.get::<str>(key)) {
        None => 0,
        Some(entry) => match entry.value.as_string() {
            None => return resp_wrongtype(),
            Some(bytes) => match std::str::from_utf8(bytes).ok().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return resp_err_not_integer(),
            },
        },
    };
    let next = match current.checked_add(delta) {
        Some(n) => n,
        None => return resp_err(overflow_msg),
    };
    let new_value = i64_to_bytes(next);
    if !check_oom(&mut db, ns, key, new_value.len()) {
        return resp_err_oom();
    }
    let m = db.put_deferred(ns, key, Entry::new(new_value, None));
    drop(db);
    m.emit();
    resp_int(next)
}

pub(crate) async fn cmd_incr(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    apply_integer_op(&ns, &key, store, 1, "increment would overflow").await
}

async fn cmd_incrby(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let by: i64 = match std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return resp_err_not_integer(),
    };
    apply_integer_op(&ns, &key, store, by, "increment would overflow").await
}

async fn cmd_decr(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    apply_integer_op(&ns, &key, store, -1, "decrement would overflow").await
}

async fn cmd_decrby(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let by: i64 = match std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return resp_err_not_integer(),
    };
    let delta = match by.checked_neg() {
        Some(v) => v,
        None => return resp_err("decrement would overflow"),
    };
    apply_integer_op(&ns, &key, store, delta, "decrement would overflow").await
}

async fn cmd_incrbyfloat(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let by: f64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err_not_float(),
    };
    let mut db = store.write().await;
    db.purge_if_expired(&ns, &key);
    let current: f64 = match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
        None => 0.0,
        Some(entry) => match entry.value.as_string() {
            None => return resp_wrongtype(),
            Some(bytes) => match std::str::from_utf8(bytes).ok().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return resp_err_not_float(),
            },
        },
    };
    let next = current + by;
    if next.is_nan() || next.is_infinite() {
        return resp_err("increment would produce NaN or Infinity");
    }
    let mut new_value = Vec::with_capacity(24);
    { use std::io::Write; write!(new_value, "{}", next).unwrap(); }
    if !check_oom(&mut db, &ns, &key, new_value.len()) {
        return resp_err_oom();
    }
    let m = db.put_deferred(ns.as_ref(), key.as_ref(), Entry::new(new_value.clone(), None));
    drop(db);
    m.emit();
    resp_bulk(&new_value)
}

async fn cmd_setrange(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let offset: usize = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err_not_integer(),
    };
    let replacement = args[3].clone();
    let mut db = store.write().await;
    db.purge_if_expired(&ns, &key);
    let existing: Vec<u8> = match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
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
        return resp_err_oom();
    }
    let m = db.put_deferred(ns.as_ref(), key.as_ref(), Entry::new(new_val, None));
    drop(db);
    m.emit();
    resp_usize(new_len)
}

async fn cmd_getrange(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let start_i: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err_not_integer(),
    };
    let end_i: i64 = match std::str::from_utf8(&args[3])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err_not_integer(),
    };
    let (resp, expired, mark, is_ear) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => (resp_bulk(b""), false, false, false),
            Some(entry) if entry.is_expired() => (resp_bulk(b""), true, false, false),
            Some(entry) => match entry.value.as_string() {
                None => (resp_wrongtype(), false, false, false),
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
                        (resp_bulk(b""), false, false, false)
                    } else {
                        (
                            resp_bulk(&bytes[start..=end.min(bytes.len() - 1)]),
                            false,
                            true,
                            db.is_ear_namespace(&ns),
                        )
                    }
                }
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    } else if mark && is_ear {
        let mut db = store.write().await;
        db.mark_ear(&ns, &key);
    }
    resp
}

// ═══════════════════════════════════════════════════════════════════════════════
// LIST COMMANDS
// ═══════════════════════════════════════════════════════════════════════════════

pub(crate) async fn cmd_lpush(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let new_items = &args[2..];
    let mut db = store.write().await;

    let (existing_byte_len, is_new_key) = match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
        None => (0usize, true),
        Some(e) => match &*e.value {
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
        return resp_err_oom();
    }

    let entry = db
        .entries
        .entry(ns.clone().into_owned())
        .or_default()
        .entry(key.clone().into_owned())
        .or_insert_with(|| Entry {
            value: ValueCell::new(Value::List(VecDeque::new())),
            hits: AtomicU64::new(0),
            expiry: None,
        });
    let list = match entry.value_mut() {
        Value::List(l) => l,
        _ => unreachable!(),
    };
    for item in new_items.iter() {
        list.push_front(item.clone());
    }
    let len = list.len();
    db.used_bytes = db.used_bytes.saturating_add(net_delta);
    let nb = db.namespace_bytes.entry(ns.clone().into_owned()).or_insert(0);
    *nb = nb.saturating_add(net_delta);
    let ns_bytes = *nb;
    let ns_keys = db.entries.get::<str>(ns.as_ref()).map(|m| m.len()).unwrap_or(0);
    let total_bytes = db.used_bytes;
    let ns_str = ns.as_ref().to_owned();
    db.touch_key_version(&ns, &key);
    drop(db);
    metrics::gauge!("kvns_keys_total", "namespace" => ns_str.clone()).set(ns_keys as f64);
    metrics::gauge!("kvns_memory_used_bytes", "namespace" => ns_str).set(ns_bytes as f64);
    metrics::gauge!("kvns_memory_used_bytes_total").set(total_bytes as f64);
    resp_usize(len)
}

async fn cmd_rpush(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let new_items = &args[2..];
    let mut db = store.write().await;

    let (existing_byte_len, is_new_key) = match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
        None => (0usize, true),
        Some(e) => match &*e.value {
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
        return resp_err_oom();
    }

    let entry = db
        .entries
        .entry(ns.clone().into_owned())
        .or_default()
        .entry(key.clone().into_owned())
        .or_insert_with(|| Entry {
            value: ValueCell::new(Value::List(VecDeque::new())),
            hits: AtomicU64::new(0),
            expiry: None,
        });
    let list = match entry.value_mut() {
        Value::List(l) => l,
        _ => unreachable!(),
    };
    for item in new_items.iter() {
        list.push_back(item.clone());
    }
    let len = list.len();
    db.used_bytes = db.used_bytes.saturating_add(net_delta);
    let nb = db.namespace_bytes.entry(ns.clone().into_owned()).or_insert(0);
    *nb = nb.saturating_add(net_delta);
    let ns_bytes = *nb;
    let ns_keys = db.entries.get::<str>(ns.as_ref()).map(|m| m.len()).unwrap_or(0);
    let total_bytes = db.used_bytes;
    let ns_str = ns.as_ref().to_owned();
    db.touch_key_version(&ns, &key);
    drop(db);
    metrics::gauge!("kvns_keys_total", "namespace" => ns_str.clone()).set(ns_keys as f64);
    metrics::gauge!("kvns_memory_used_bytes", "namespace" => ns_str).set(ns_bytes as f64);
    metrics::gauge!("kvns_memory_used_bytes_total").set(total_bytes as f64);
    resp_usize(len)
}

async fn cmd_lpushx(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let new_items = &args[2..];
    let mut db = store.write().await;
    // Expire under the same lock so the existence check and push are atomic.
    db.purge_if_expired(&ns, &key);
    let existing_byte_len = match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
        None => return resp_int(0),
        Some(e) => match &*e.value {
            Value::List(l) => l.iter().map(|v| v.len()).sum::<usize>(),
            _ => return resp_wrongtype(),
        },
    };
    let added_byte_len: usize = new_items.iter().map(|v| v.len()).sum();
    let net_delta = Db::entry_size(&ns, &key, existing_byte_len + added_byte_len)
        .saturating_sub(Db::entry_size(&ns, &key, existing_byte_len));
    if db.used_bytes.saturating_add(net_delta) > db.memory_limit && !db.evict_for_write(&ns, net_delta) {
        return resp_err_oom();
    }
    let entry = db.entries.entry(ns.clone().into_owned()).or_default().entry(key.clone().into_owned())
        .or_insert_with(|| Entry { value: ValueCell::new(Value::List(VecDeque::new())), hits: AtomicU64::new(0), expiry: None });
    let list = match entry.value_mut() { Value::List(l) => l, _ => unreachable!() };
    for item in new_items.iter() { list.push_front(item.clone()); }
    let len = list.len();
    db.used_bytes = db.used_bytes.saturating_add(net_delta);
    let nb = db.namespace_bytes.entry(ns.clone().into_owned()).or_insert(0);
    *nb = nb.saturating_add(net_delta);
    let ns_bytes = *nb;
    let ns_keys = db.entries.get::<str>(ns.as_ref()).map(|m| m.len()).unwrap_or(0);
    let total_bytes = db.used_bytes;
    let ns_str = ns.as_ref().to_owned();
    db.touch_key_version(&ns, &key);
    drop(db);
    metrics::gauge!("kvns_keys_total", "namespace" => ns_str.clone()).set(ns_keys as f64);
    metrics::gauge!("kvns_memory_used_bytes", "namespace" => ns_str).set(ns_bytes as f64);
    metrics::gauge!("kvns_memory_used_bytes_total").set(total_bytes as f64);
    resp_usize(len)
}

async fn cmd_rpushx(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let new_items = &args[2..];
    let mut db = store.write().await;
    // Expire under the same lock so the existence check and push are atomic.
    db.purge_if_expired(&ns, &key);
    let existing_byte_len = match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
        None => return resp_int(0),
        Some(e) => match &*e.value {
            Value::List(l) => l.iter().map(|v| v.len()).sum::<usize>(),
            _ => return resp_wrongtype(),
        },
    };
    let added_byte_len: usize = new_items.iter().map(|v| v.len()).sum();
    let net_delta = Db::entry_size(&ns, &key, existing_byte_len + added_byte_len)
        .saturating_sub(Db::entry_size(&ns, &key, existing_byte_len));
    if db.used_bytes.saturating_add(net_delta) > db.memory_limit && !db.evict_for_write(&ns, net_delta) {
        return resp_err_oom();
    }
    let entry = db.entries.entry(ns.clone().into_owned()).or_default().entry(key.clone().into_owned())
        .or_insert_with(|| Entry { value: ValueCell::new(Value::List(VecDeque::new())), hits: AtomicU64::new(0), expiry: None });
    let list = match entry.value_mut() { Value::List(l) => l, _ => unreachable!() };
    for item in new_items.iter() { list.push_back(item.clone()); }
    let len = list.len();
    db.used_bytes = db.used_bytes.saturating_add(net_delta);
    let nb = db.namespace_bytes.entry(ns.clone().into_owned()).or_insert(0);
    *nb = nb.saturating_add(net_delta);
    let ns_bytes = *nb;
    let ns_keys = db.entries.get::<str>(ns.as_ref()).map(|m| m.len()).unwrap_or(0);
    let total_bytes = db.used_bytes;
    let ns_str = ns.as_ref().to_owned();
    db.touch_key_version(&ns, &key);
    drop(db);
    metrics::gauge!("kvns_keys_total", "namespace" => ns_str.clone()).set(ns_keys as f64);
    metrics::gauge!("kvns_memory_used_bytes", "namespace" => ns_str).set(ns_bytes as f64);
    metrics::gauge!("kvns_memory_used_bytes_total").set(total_bytes as f64);
    resp_usize(len)
}

async fn cmd_lpop(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
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
            None => return resp_err_not_integer(),
        }
    } else {
        None
    };

    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
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
    let outcome = match db.entries.get_mut::<str>(ns.as_ref()).and_then(|m| m.get_mut::<str>(key.as_ref())) {
        None => LPopResult::Null,
        Some(entry) => match entry.value_mut() {
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
            } else {
                db.touch_key_version(&ns, &key);
            }
            resp_bulk(&val)
        }
        LPopResult::Multi(popped, empty) => {
            if empty {
                db.delete(&ns, &key);
            } else if !popped.is_empty() {
                db.touch_key_version(&ns, &key);
            }
            resp_array(&popped)
        }
    }
}

async fn cmd_rpop(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
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
            None => return resp_err_not_integer(),
        }
    } else {
        None
    };

    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
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
    let outcome = match db.entries.get_mut::<str>(ns.as_ref()).and_then(|m| m.get_mut::<str>(key.as_ref())) {
        None => RPopResult::Null,
        Some(entry) => match entry.value_mut() {
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
            } else {
                db.touch_key_version(&ns, &key);
            }
            resp_bulk(&val)
        }
        RPopResult::Multi(popped, empty) => {
            if empty {
                db.delete(&ns, &key);
            } else if !popped.is_empty() {
                db.touch_key_version(&ns, &key);
            }
            resp_array(&popped)
        }
    }
}

async fn cmd_llen(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let (resp, expired) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => (resp_int(0), false),
            Some(entry) if entry.is_expired() => (resp_int(0), true),
            Some(entry) => match &*entry.value {
                Value::List(l) => (resp_usize(l.len()), false),
                _ => (resp_wrongtype(), false),
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    }
    resp
}

async fn cmd_lrange(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let start_i: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err_not_integer(),
    };
    let stop_i: i64 = match std::str::from_utf8(&args[3])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err_not_integer(),
    };
    let (resp, expired, mark, is_ear) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => (resp_array(&[]), false, false, false),
            Some(entry) if entry.is_expired() => (resp_array(&[]), true, false, false),
            Some(entry) => match &*entry.value {
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
                        (resp_array(&[]), false, false, false)
                    } else {
                        let count = stop - start + 1;
                        // Write directly into the response buffer — avoids the
                        // intermediate Vec<Vec<u8>> materialization.
                        let mut out = Vec::with_capacity(16 + count * 24);
                        append_array_header(&mut out, count);
                        for item in list.iter().skip(start).take(count) {
                            append_bulk(&mut out, item);
                        }
                        (
                            std::borrow::Cow::Owned(out),
                            false,
                            true,
                            db.is_ear_namespace(&ns),
                        )
                    }
                }
                _ => (resp_wrongtype(), false, false, false),
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    } else if mark && is_ear {
        let mut db = store.write().await;
        db.mark_ear(&ns, &key);
    }
    resp
}

async fn cmd_lindex(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let idx_i: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err_not_integer(),
    };
    let (resp, expired, mark, is_ear) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => (resp_null(), false, false, false),
            Some(entry) if entry.is_expired() => (resp_null(), true, false, false),
            Some(entry) => match &*entry.value {
                Value::List(list) => {
                    let len = list.len() as i64;
                    let idx = if idx_i < 0 { len + idx_i } else { idx_i };
                    if idx < 0 || idx >= len {
                        (resp_null(), false, false, false)
                    } else {
                        (
                            resp_bulk(&list[idx as usize]),
                            false,
                            true,
                            db.is_ear_namespace(&ns),
                        )
                    }
                }
                _ => (resp_wrongtype(), false, false, false),
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    } else if mark && is_ear {
        let mut db = store.write().await;
        db.mark_ear(&ns, &key);
    }
    resp
}

async fn cmd_lset(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let idx_i: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err_not_integer(),
    };
    let new_val = args[3].clone();
    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_err_no_such_key();
    }
    let (resp, modified) = match db.entries.get_mut::<str>(ns.as_ref()).and_then(|m| m.get_mut::<str>(key.as_ref())) {
        None => (resp_err_no_such_key(), false),
        Some(entry) => match entry.value_mut() {
            Value::List(list) => {
                let len = list.len() as i64;
                let idx = if idx_i < 0 { len + idx_i } else { idx_i };
                if idx < 0 || idx >= len {
                    (resp_err("ERR index out of range"), false)
                } else {
                    list[idx as usize] = new_val;
                    (resp_ok(), true)
                }
            }
            _ => return resp_wrongtype(),
        },
    };
    if modified {
        db.touch_key_version(&ns, &key);
    }
    resp
}

async fn cmd_lrem(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let count_i: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err_not_integer(),
    };
    let element = args[3].clone();
    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    let (resp, modified) = match db.entries.get_mut::<str>(ns.as_ref()).and_then(|m| m.get_mut::<str>(key.as_ref())) {
        None => (resp_int(0), false),
        Some(entry) => match entry.value_mut() {
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
                (resp_int(removed), removed > 0)
            }
            _ => return resp_wrongtype(),
        },
    };
    if modified {
        db.touch_key_version(&ns, &key);
    }
    resp
}

async fn cmd_ltrim(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let start_i: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err_not_integer(),
    };
    let stop_i: i64 = match std::str::from_utf8(&args[3])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err_not_integer(),
    };
    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_ok();
    }
    let (resp, modified) = match db.entries.get_mut::<str>(ns.as_ref()).and_then(|m| m.get_mut::<str>(key.as_ref())) {
        None => (resp_ok(), false),
        Some(entry) => match entry.value_mut() {
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
                (resp_ok(), true)
            }
            _ => return resp_wrongtype(),
        },
    };
    if modified {
        db.touch_key_version(&ns, &key);
    }
    resp
}

async fn cmd_linsert(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
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
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    let (resp, modified) = match db.entries.get_mut::<str>(ns.as_ref()).and_then(|m| m.get_mut::<str>(key.as_ref())) {
        None => (resp_int(0), false),
        Some(entry) => match entry.value_mut() {
            Value::List(list) => match list.iter().position(|e| e == &pivot) {
                None => (resp_int(-1), false),
                Some(idx) => {
                    let insert_at = if position == "AFTER" { idx + 1 } else { idx };
                    list.insert(insert_at, element);
                    (resp_usize(list.len()), true)
                }
            },
            _ => return resp_wrongtype(),
        },
    };
    if modified {
        db.touch_key_version(&ns, &key);
    }
    resp
}

async fn cmd_lpos(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
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
                    None => return resp_err_not_integer(),
                };
            }
            "COUNT" => {
                count_opt = match std::str::from_utf8(&args[i + 1])
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                {
                    Some(n) => Some(n),
                    None => return resp_err_not_integer(),
                };
            }
            _ => {}
        }
        i += 2;
    }
    let (resp, expired) = {
    let db = store.read().await;
    match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
        None => (resp_null(), false),
        Some(entry) if entry.is_expired() => (resp_null(), true),
        Some(entry) => match &*entry.value {
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
                                results.push(i64_to_bytes(idx as i64));
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
                                results.push(i64_to_bytes(idx as i64));
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

                let r = if count_opt.is_some() {
                    resp_array(&results)
                } else if results.is_empty() {
                    resp_null()
                } else {
                    resp_bulk(&results[0])
                };
                (r, false)
            }
            _ => (resp_wrongtype(), false),
        },
    }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    }
    resp
}

async fn cmd_lmove(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
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
        .get::<str>(src_ns.as_ref())
        .and_then(|m| m.get::<str>(src_key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&src_ns, &src_key);
        return resp_null();
    }

    // Capture old sizes before any mutation for byte accounting.
    let old_src_size = db.entries.get::<str>(src_ns.as_ref())
        .and_then(|m| m.get::<str>(src_key.as_ref()))
        .map(|e| Db::entry_size(&src_ns, &src_key, e.value.byte_len()))
        .unwrap_or(0);
    let old_dst_size = db.entries.get::<str>(dst_ns.as_ref())
        .and_then(|m| m.get::<str>(dst_key.as_ref()))
        .map(|e| Db::entry_size(&dst_ns, &dst_key, e.value.byte_len()))
        .unwrap_or(0);

    let element = match db
        .entries
        .get_mut::<str>(src_ns.as_ref())
        .and_then(|m| m.get_mut::<str>(src_key.as_ref()))
    {
        None => return resp_null(),
        Some(entry) => match entry.value_mut() {
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
        .get_mut::<str>(dst_ns.as_ref())
        .and_then(|m| m.get_mut::<str>(dst_key.as_ref()))
    {
        Some(entry) => match entry.value_mut() {
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
            db.entries.entry(dst_ns.clone().into_owned()).or_default().insert(
                dst_key.clone().into_owned(),
                Entry {
                    value: ValueCell::new(Value::List(new_list)),
                    hits: AtomicU64::new(0),
                    expiry: None,
                },
            );
        }
    }

    // Update byte accounting for both keys.
    let new_src_size = db.entries.get::<str>(src_ns.as_ref())
        .and_then(|m| m.get::<str>(src_key.as_ref()))
        .map(|e| Db::entry_size(&src_ns, &src_key, e.value.byte_len()))
        .unwrap_or(0);
    let new_dst_size = db.entries.get::<str>(dst_ns.as_ref())
        .and_then(|m| m.get::<str>(dst_key.as_ref()))
        .map(|e| Db::entry_size(&dst_ns, &dst_key, e.value.byte_len()))
        .unwrap_or(0);
    db.used_bytes = db.used_bytes
        .saturating_sub(old_src_size).saturating_add(new_src_size)
        .saturating_sub(old_dst_size).saturating_add(new_dst_size);
    let nb_src = db.namespace_bytes.entry(src_ns.clone().into_owned()).or_insert(0);
    *nb_src = nb_src.saturating_sub(old_src_size).saturating_add(new_src_size);
    let nb_dst = db.namespace_bytes.entry(dst_ns.clone().into_owned()).or_insert(0);
    *nb_dst = nb_dst.saturating_sub(old_dst_size).saturating_add(new_dst_size);
    db.write_version += 1;
    let v = db.write_version;
    db.key_versions.entry(src_ns.into_owned()).or_default().insert(src_key.into_owned(), v);
    db.key_versions.entry(dst_ns.into_owned()).or_default().insert(dst_key.into_owned(), v);

    result
}

// ═══════════════════════════════════════════════════════════════════════════════
// HASH COMMANDS
// ═══════════════════════════════════════════════════════════════════════════════

async fn cmd_hset(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 4 || !(args.len() - 2).is_multiple_of(2) {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = args[2..]
        .chunks(2)
        .map(|c| (c[0].clone(), c[1].clone()))
        .collect();
    let mut db = store.write().await;

    db.purge_if_expired(&ns, &key);

    let (existing_byte_len, oom_net, is_new_key): (usize, usize, bool) =
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => {
                let value_bytes: usize = pairs.iter().map(|(k, v)| k.len() + v.len()).sum();
                (0, Db::entry_size(&ns, &key, value_bytes), true)
            }
            Some(e) => match e.value.as_hash() {
                None => return resp_wrongtype(),
                Some(h) => {
                    let existing = h.iter().map(|(k, v)| k.len() + v.len()).sum();
                    let net: isize = pairs
                        .iter()
                        .map(|(field, value)| match h.get(field.as_slice()) {
                            None => (field.len() + value.len()) as isize,
                            Some(old_v) => value.len() as isize - old_v.len() as isize,
                        })
                        .sum();
                    (existing, net.max(0) as usize, false)
                }
            },
        };
    if !check_oom_net(&mut db, &ns, oom_net) {
        return resp_err_oom();
    }

    let entry = db
        .entries
        .entry(ns.clone().into_owned())
        .or_default()
        .entry(key.clone().into_owned())
        .or_insert_with(|| Entry {
            value: ValueCell::new(Value::Hash(HashMap::new())),
            hits: AtomicU64::new(0),
            expiry: None,
        });

    let hash = match entry.value_mut().as_hash_mut() {
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
    let old_size = if is_new_key { 0 } else { Db::entry_size(&ns, &key, existing_byte_len) };
    let new_size = Db::entry_size(&ns, &key, new_byte_len);
    let delta = new_size.saturating_sub(old_size);
    db.used_bytes = db.used_bytes.saturating_add(delta);
    let nb = db.namespace_bytes.entry(ns.clone().into_owned()).or_insert(0);
    *nb = nb.saturating_add(delta);
    let ns_bytes = *nb;
    let ns_keys = db.entries.get::<str>(ns.as_ref()).map(|m| m.len()).unwrap_or(0);
    let total_bytes = db.used_bytes;
    let ns_owned = ns.as_ref().to_owned();
    db.touch_key_version(&ns, &key);
    drop(db);
    metrics::gauge!("kvns_keys_total", "namespace" => ns_owned.clone()).set(ns_keys as f64);
    metrics::gauge!("kvns_memory_used_bytes", "namespace" => ns_owned).set(ns_bytes as f64);
    metrics::gauge!("kvns_memory_used_bytes_total").set(total_bytes as f64);

    resp_int(new_fields)
}

async fn cmd_hget(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let field = &args[2];
    let (resp, expired, field_found, is_ear) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => (resp_null(), false, false, false),
            Some(entry) if entry.is_expired() => (resp_null(), true, false, false),
            Some(entry) => match entry.value.as_hash() {
                None => (resp_wrongtype(), false, false, false),
                Some(h) => match h.get(field.as_slice()) {
                    None => (resp_null(), false, false, false),
                    Some(v) => (resp_bulk(v), false, true, db.is_ear_namespace(&ns)),
                },
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    }
    if field_found && is_ear {
        let mut db = store.write().await;
        db.mark_ear(&ns, &key);
    }
    resp
}

async fn cmd_hdel(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let fields = &args[2..];
    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    let (resp, modified) = match db.entries.get_mut::<str>(ns.as_ref()).and_then(|m| m.get_mut::<str>(key.as_ref())) {
        None => (resp_int(0), false),
        Some(entry) => match entry.value_mut().as_hash_mut() {
            None => return resp_wrongtype(),
            Some(h) => {
                let mut removed = 0i64;
                for f in fields {
                    if h.remove(f.as_slice()).is_some() {
                        removed += 1;
                    }
                }
                (resp_int(removed), removed > 0)
            }
        },
    };
    if modified {
        db.touch_key_version(&ns, &key);
    }
    resp
}

async fn cmd_hexists(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let field = &args[2];
    let (resp, expired) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => (resp_int(0), false),
            Some(entry) if entry.is_expired() => (resp_int(0), true),
            Some(entry) => match entry.value.as_hash() {
                None => (resp_wrongtype(), false),
                Some(h) => (resp_int(if h.contains_key(field.as_slice()) { 1 } else { 0 }), false),
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    }
    resp
}

async fn cmd_hgetall(args: &[Vec<u8>], store: &Store, conn: &ConnState) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let empty: std::borrow::Cow<'static, [u8]> = if conn.resp_version >= 3 {
        std::borrow::Cow::Borrowed(b"%0\r\n")
    } else {
        resp_array(&[])
    };
    let (resp, expired, found, is_ear) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => (empty, false, false, false),
            Some(entry) if entry.is_expired() => (empty, true, false, false),
            Some(entry) => match entry.value.as_hash() {
                None => (resp_wrongtype(), false, false, false),
                Some(h) => {
                    let is_ear = db.is_ear_namespace(&ns);
                    if conn.resp_version >= 3 {
                        let pairs: Vec<(Vec<u8>, Vec<u8>)> =
                            h.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                        (resp_map(&pairs), false, true, is_ear)
                    } else {
                        let resp = build_array(
                            h.len() * 2,
                            h.iter()
                                .flat_map(|(k, v)| [k.as_slice(), v.as_slice()]),
                        );
                        (resp, false, true, is_ear)
                    }
                }
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    }
    if found && is_ear {
        let mut db = store.write().await;
        db.mark_ear(&ns, &key);
    }
    resp
}

async fn cmd_hkeys(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let (resp, expired, found, is_ear) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => (resp_array(&[]), false, false, false),
            Some(entry) if entry.is_expired() => (resp_array(&[]), true, false, false),
            Some(entry) => match entry.value.as_hash() {
                None => (resp_wrongtype(), false, false, false),
                Some(h) => {
                    let resp = build_array(h.len(), h.keys().map(|k| k.as_slice()));
                    (resp, false, true, db.is_ear_namespace(&ns))
                }
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    }
    if found && is_ear {
        let mut db = store.write().await;
        db.mark_ear(&ns, &key);
    }
    resp
}

async fn cmd_hvals(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let (resp, expired, found, is_ear) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => (resp_array(&[]), false, false, false),
            Some(entry) if entry.is_expired() => (resp_array(&[]), true, false, false),
            Some(entry) => match entry.value.as_hash() {
                None => (resp_wrongtype(), false, false, false),
                Some(h) => {
                    let resp = build_array(h.len(), h.values().map(|v| v.as_slice()));
                    (resp, false, true, db.is_ear_namespace(&ns))
                }
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    }
    if found && is_ear {
        let mut db = store.write().await;
        db.mark_ear(&ns, &key);
    }
    resp
}

async fn cmd_hlen(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let (resp, expired) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => (resp_int(0), false),
            Some(entry) if entry.is_expired() => (resp_int(0), true),
            Some(entry) => match entry.value.as_hash() {
                None => (resp_wrongtype(), false),
                Some(h) => (resp_usize(h.len()), false),
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    }
    resp
}

async fn cmd_hmget(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let fields = &args[2..];
    let nulls = resp_array_of_nulls(fields.len());
    let (resp, expired, found, is_ear) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => (nulls, false, false, false),
            Some(entry) if entry.is_expired() => (nulls, true, false, false),
            Some(entry) => match entry.value.as_hash() {
                None => (resp_wrongtype(), false, false, false),
                Some(h) => {
                    let mut out = Vec::new();
                    append_array_header(&mut out, fields.len());
                    for f in fields {
                        match h.get(f.as_slice()) {
                            None => append_null(&mut out),
                            Some(v) => append_bulk(&mut out, v),
                        }
                    }
                    (out.into(), false, true, db.is_ear_namespace(&ns))
                }
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    }
    if found && is_ear {
        let mut db = store.write().await;
        db.mark_ear(&ns, &key);
    }
    resp
}

async fn cmd_hincrby(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
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
        None => return resp_err_not_integer(),
    };
    let mut db = store.write().await;
    db.purge_if_expired(&ns, &key);

    let current: i64 = match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
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
        .entry(ns.clone().into_owned())
        .or_default()
        .entry(key.clone().into_owned())
        .or_insert_with(|| Entry {
            value: ValueCell::new(Value::Hash(HashMap::new())),
            hits: AtomicU64::new(0),
            expiry: None,
        });
    match entry.value_mut().as_hash_mut() {
        Some(h) => {
            h.insert(field, new_val);
        }
        None => return resp_wrongtype(),
    }
    db.touch_key_version(&ns, &key);
    resp_int(next)
}

async fn cmd_hincrbyfloat(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
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
        None => return resp_err_not_float(),
    };
    let mut db = store.write().await;
    db.purge_if_expired(&ns, &key);

    let current: f64 = match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
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
        .entry(ns.clone().into_owned())
        .or_default()
        .entry(key.clone().into_owned())
        .or_insert_with(|| Entry {
            value: ValueCell::new(Value::Hash(HashMap::new())),
            hits: AtomicU64::new(0),
            expiry: None,
        });
    match entry.value_mut().as_hash_mut() {
        Some(h) => {
            h.insert(field, new_val.clone());
        }
        None => return resp_wrongtype(),
    }
    db.touch_key_version(&ns, &key);
    resp_bulk(&new_val)
}

async fn cmd_hrandfield(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
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
            None => return resp_err_not_integer(),
        }
    } else {
        None
    };
    let withvalues =
        args.len() >= 4 && String::from_utf8_lossy(&args[3]).to_ascii_uppercase() == "WITHVALUES";

    let (resp, expired, mark, is_ear) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            Some(entry) if entry.is_expired() => {
                let resp = if count_opt.is_some() {
                    resp_array(&[])
                } else {
                    resp_null()
                };
                (resp, true, false, false)
            }
            None => {
                let resp = if count_opt.is_some() {
                    resp_array(&[])
                } else {
                    resp_null()
                };
                (resp, false, false, false)
            }
            Some(entry) => match entry.value.as_hash() {
                None => (resp_wrongtype(), false, false, false),
                Some(h) => {
                    let fields: Vec<(Vec<u8>, Vec<u8>)> =
                        h.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                    if fields.is_empty() {
                        let resp = if count_opt.is_some() {
                            resp_array(&[])
                        } else {
                            resp_null()
                        };
                        (resp, false, false, false)
                    } else {
                        let is_ear = db.is_ear_namespace(&ns);
                        match count_opt {
                            None => (resp_bulk(&fields[0].0), false, true, is_ear),
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
                                (resp_array(&result), false, true, is_ear)
                            }
                        }
                    }
                }
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    } else if mark && is_ear {
        let mut db = store.write().await;
        db.mark_ear(&ns, &key);
    }
    resp
}

// ═══════════════════════════════════════════════════════════════════════════════
// SET COMMANDS
// ═══════════════════════════════════════════════════════════════════════════════

async fn cmd_sadd(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let members = &args[2..];
    let mut db = store.write().await;
    db.purge_if_expired(&ns, &key);

    let (existing_byte_len, oom_net, is_new_key): (usize, usize, bool) =
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => {
                let value_bytes: usize = members.iter().map(|m| m.len()).sum();
                (0, Db::entry_size(&ns, &key, value_bytes), true)
            }
            Some(e) => match e.value.as_set() {
                None => return resp_wrongtype(),
                Some(s) => {
                    let existing = s.iter().map(|v| v.len()).sum();
                    let net: usize = members
                        .iter()
                        .filter(|m| !s.contains(m.as_slice()))
                        .map(|m| m.len())
                        .sum();
                    (existing, net, false)
                }
            },
        };
    if !check_oom_net(&mut db, &ns, oom_net) {
        return resp_err_oom();
    }

    let entry = db
        .entries
        .entry(ns.clone().into_owned())
        .or_default()
        .entry(key.clone().into_owned())
        .or_insert_with(|| Entry {
            value: ValueCell::new(Value::Set(HashSet::new())),
            hits: AtomicU64::new(0),
            expiry: None,
        });
    let set = match entry.value_mut().as_set_mut() {
        Some(s) => s,
        None => return resp_wrongtype(),
    };

    let before = set.len();
    for m in members {
        set.insert(m.clone());
    }
    let added = set.len() - before;

    // Update memory
    let new_byte_len: usize = set.iter().map(|v| v.len()).sum();
    let old_size = if is_new_key { 0 } else { Db::entry_size(&ns, &key, existing_byte_len) };
    let new_size = Db::entry_size(&ns, &key, new_byte_len);
    let delta = new_size.saturating_sub(old_size);
    db.used_bytes = db.used_bytes.saturating_add(delta);
    let nb = db.namespace_bytes.entry(ns.clone().into_owned()).or_insert(0);
    *nb = nb.saturating_add(delta);
    let ns_bytes = *nb;
    let total_bytes = db.used_bytes;
    let ns_owned = ns.as_ref().to_owned();
    db.touch_key_version(&ns, &key);
    drop(db);
    metrics::gauge!("kvns_memory_used_bytes", "namespace" => ns_owned).set(ns_bytes as f64);
    metrics::gauge!("kvns_memory_used_bytes_total").set(total_bytes as f64);

    resp_usize(added)
}

async fn cmd_srem(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let members = &args[2..];
    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    let (resp, modified) = match db.entries.get_mut::<str>(ns.as_ref()).and_then(|m| m.get_mut::<str>(key.as_ref())) {
        None => (resp_int(0), false),
        Some(entry) => match entry.value_mut().as_set_mut() {
            None => return resp_wrongtype(),
            Some(s) => {
                let mut removed = 0i64;
                for m in members {
                    if s.remove(m.as_slice()) {
                        removed += 1;
                    }
                }
                (resp_int(removed), removed > 0)
            }
        },
    };
    if modified {
        db.touch_key_version(&ns, &key);
    }
    resp
}

async fn cmd_smembers(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let (resp, expired, mark, is_ear) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => (resp_array(&[]), false, false, false),
            Some(entry) if entry.is_expired() => (resp_array(&[]), true, false, false),
            Some(entry) => match entry.value.as_set() {
                None => (resp_wrongtype(), false, false, false),
                Some(s) => {
                    // Sort references, not owned bytes — avoids cloning payloads.
                    let mut members: Vec<&[u8]> = s.iter().map(Vec::as_slice).collect();
                    members.sort_unstable();
                    let resp = build_array(members.len(), members.iter().copied());
                    (resp, false, true, db.is_ear_namespace(&ns))
                }
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    } else if mark && is_ear {
        let mut db = store.write().await;
        db.mark_ear(&ns, &key);
    }
    resp
}

async fn cmd_scard(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let (resp, expired) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => (resp_int(0), false),
            Some(entry) if entry.is_expired() => (resp_int(0), true),
            Some(entry) => match entry.value.as_set() {
                None => (resp_wrongtype(), false),
                Some(s) => (resp_usize(s.len()), false),
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    }
    resp
}

async fn cmd_sismember(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let member = &args[2];
    let (resp, expired, mark, is_ear) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => (resp_int(0), false, false, false),
            Some(entry) if entry.is_expired() => (resp_int(0), true, false, false),
            Some(entry) => match entry.value.as_set() {
                None => (resp_wrongtype(), false, false, false),
                Some(s) => {
                    let found = s.contains(member.as_slice());
                    (
                        resp_int(if found { 1 } else { 0 }),
                        false,
                        found,
                        found && db.is_ear_namespace(&ns),
                    )
                }
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    } else if mark && is_ear {
        let mut db = store.write().await;
        db.mark_ear(&ns, &key);
    }
    resp
}

async fn cmd_smismember(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let members = &args[2..];
    let (resp, expired, mark, is_ear) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => {
                let mut out = Vec::new();
                append_array_header(&mut out, members.len());
                for _ in members {
                    append_int(&mut out, 0);
                }
                (out.into(), false, false, false)
            }
            Some(entry) if entry.is_expired() => {
                let mut out = Vec::new();
                append_array_header(&mut out, members.len());
                for _ in members {
                    append_int(&mut out, 0);
                }
                (out.into(), true, false, false)
            }
            Some(entry) => match entry.value.as_set() {
                None => (resp_wrongtype(), false, false, false),
                Some(s) => {
                    let mut out = Vec::new();
                    append_array_header(&mut out, members.len());
                    for m in members {
                        append_int(&mut out, if s.contains(m.as_slice()) { 1 } else { 0 });
                    }
                    (out.into(), false, true, db.is_ear_namespace(&ns))
                }
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    } else if mark && is_ear {
        let mut db = store.write().await;
        db.mark_ear(&ns, &key);
    }
    resp
}

async fn cmd_sunion(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let mut expired: Vec<(String, String)> = Vec::new();
    let resp = {
        let db = store.read().await;
        // Accumulate references into the store; no member bytes are cloned.
        let mut acc: HashSet<&[u8]> = HashSet::new();
        for raw_key in &args[1..] {
            let (ns, key) = parse_ns_key(raw_key);
            match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
                None => {}
                Some(entry) if entry.is_expired() => {
                    expired.push((ns.into_owned(), key.into_owned()));
                }
                Some(entry) => match entry.value.as_set() {
                    None => return resp_wrongtype(),
                    Some(s) => {
                        for member in s {
                            acc.insert(member.as_slice());
                        }
                    }
                },
            }
        }
        let mut members: Vec<&[u8]> = acc.into_iter().collect();
        members.sort_unstable();
        build_array(members.len(), members.iter().copied())
    };
    for (ns, key) in &expired {
        cleanup_expired_key(store, ns, key).await;
    }
    resp
}

async fn cmd_sinter(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    // Single read lock for the entire intersection computation — references
    // into the store remain valid for the duration of the function.
    let db = store.read().await;
    let mut sets: Vec<&HashSet<Vec<u8>>> = Vec::with_capacity(args.len() - 1);
    for raw_key in &args[1..] {
        let (ns, key) = parse_ns_key(raw_key);
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            // Missing or expired key means empty intersection.
            None => return resp_array(&[]),
            Some(entry) if entry.is_expired() => return resp_array(&[]),
            Some(entry) => match entry.value.as_set() {
                None => return resp_wrongtype(),
                Some(s) => sets.push(s),
            },
        }
    }
    if sets.is_empty() {
        return resp_array(&[]);
    }
    let (smallest_idx, smallest) = sets
        .iter()
        .enumerate()
        .min_by_key(|(_, s)| s.len())
        .unwrap_or_else(|| unreachable!("sets is non-empty, guarded above"));
    let mut members: Vec<&[u8]> = Vec::new();
    for member in smallest.iter() {
        if sets
            .iter()
            .enumerate()
            .all(|(idx, s)| idx == smallest_idx || s.contains(member))
        {
            members.push(member.as_slice());
        }
    }
    members.sort_unstable();
    build_array(members.len(), members.iter().copied())
}

async fn cmd_sdiff(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let db = store.read().await;
    // None means "key missing or expired" for non-first slots.
    let mut sets: Vec<Option<&HashSet<Vec<u8>>>> = Vec::with_capacity(args.len() - 1);
    for (idx, raw_key) in args[1..].iter().enumerate() {
        let (ns, key) = parse_ns_key(raw_key);
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => {
                if idx == 0 {
                    return resp_array(&[]);
                }
                sets.push(None);
            }
            Some(entry) if entry.is_expired() => {
                if idx == 0 {
                    return resp_array(&[]);
                }
                sets.push(None);
            }
            Some(entry) => match entry.value.as_set() {
                None => return resp_wrongtype(),
                Some(s) => sets.push(Some(s)),
            },
        }
    }
    if sets.is_empty() || sets[0].is_none() {
        return resp_array(&[]);
    }
    let first = sets[0].unwrap_or_else(|| unreachable!("guarded above"));
    let mut members: Vec<&[u8]> = Vec::new();
    for member in first {
        let present_elsewhere = sets[1..].iter().flatten().any(|s| s.contains(member));
        if !present_elsewhere {
            members.push(member.as_slice());
        }
    }
    members.sort_unstable();
    build_array(members.len(), members.iter().copied())
}

async fn cmd_sunionstore(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (dst_ns, dst_key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    let mut result: HashSet<Vec<u8>> = HashSet::new();
    for raw_key in &args[2..] {
        let (ns, key) = parse_ns_key(raw_key);
        if db
            .entries
            .get::<str>(ns.as_ref())
            .and_then(|m| m.get::<str>(key.as_ref()))
            .is_some_and(|e| e.is_expired())
        {
            db.delete(&ns, &key);
            continue;
        }
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => {}
            Some(entry) => match entry.value.as_set() {
                None => return resp_wrongtype(),
                Some(s) => {
                    for member in s {
                        result.insert(member.clone());
                    }
                }
            },
        }
    }
    let count = result.len();
    let entry = Entry {
        value: ValueCell::new(Value::Set(result)),
        hits: AtomicU64::new(0),
        expiry: None,
    };
    let m = db.put_deferred(dst_ns.as_ref(), dst_key.as_ref(), entry);
    drop(db);
    m.emit();
    resp_usize(count)
}

async fn cmd_sinterstore(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (dst_ns, dst_key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    let parsed: Vec<(std::borrow::Cow<'_, str>, std::borrow::Cow<'_, str>)> = args[2..].iter().map(|raw| parse_ns_key(raw)).collect();
    for (ns, key) in &parsed {
        if db
            .entries
            .get::<str>(ns.as_ref())
            .and_then(|m| m.get::<str>(key.as_ref()))
            .is_some_and(|e| e.is_expired())
        {
            db.delete(ns, key);
        }
    }

    let mut sets: Vec<&HashSet<Vec<u8>>> = Vec::new();
    let mut empty = false;
    for (ns, key) in &parsed {
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => {
                empty = true;
                break;
            }
            Some(entry) => match entry.value.as_set() {
                None => return resp_wrongtype(),
                Some(s) => sets.push(s),
            },
        }
    }
    let result: HashSet<Vec<u8>> = if empty || sets.is_empty() {
        HashSet::new()
    } else {
        let (smallest_idx, smallest) = sets
            .iter()
            .enumerate()
            .min_by_key(|(_, s)| s.len())
            .expect("non-empty sets");
        let mut out = HashSet::new();
        for member in smallest.iter() {
            if sets
                .iter()
                .enumerate()
                .all(|(idx, s)| idx == smallest_idx || s.contains(member))
            {
                out.insert(member.clone());
            }
        }
        out
    };
    let count = result.len();
    let m = db.put_deferred(
        dst_ns.as_ref(),
        dst_key.as_ref(),
        Entry {
            value: ValueCell::new(Value::Set(result)),
            hits: AtomicU64::new(0),
            expiry: None,
        },
    );
    drop(db);
    m.emit();
    resp_usize(count)
}

async fn cmd_sdiffstore(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (dst_ns, dst_key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    let parsed: Vec<(std::borrow::Cow<'_, str>, std::borrow::Cow<'_, str>)> = args[2..].iter().map(|raw| parse_ns_key(raw)).collect();
    for (ns, key) in &parsed {
        if db
            .entries
            .get::<str>(ns.as_ref())
            .and_then(|m| m.get::<str>(key.as_ref()))
            .is_some_and(|e| e.is_expired())
        {
            db.delete(ns, key);
        }
    }

    let mut sets: Vec<Option<&HashSet<Vec<u8>>>> = Vec::new();
    for (ns, key) in &parsed {
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => sets.push(None),
            Some(entry) => match entry.value.as_set() {
                None => return resp_wrongtype(),
                Some(s) => sets.push(Some(s)),
            },
        }
    }
    let result: HashSet<Vec<u8>> = if sets.is_empty() {
        HashSet::new()
    } else {
        match sets[0] {
            None => HashSet::new(),
            Some(first) => {
                let mut out = HashSet::new();
                for member in first {
                    let present_elsewhere = sets[1..].iter().flatten().any(|s| s.contains(member));
                    if !present_elsewhere {
                        out.insert(member.clone());
                    }
                }
                out
            }
        }
    };
    let count = result.len();
    let m = db.put_deferred(
        dst_ns.as_ref(),
        dst_key.as_ref(),
        Entry {
            value: ValueCell::new(Value::Set(result)),
            hits: AtomicU64::new(0),
            expiry: None,
        },
    );
    drop(db);
    m.emit();
    resp_usize(count)
}

async fn cmd_smove(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (src_ns, src_key) = parse_ns_key(&args[1]);
    let (dst_ns, dst_key) = parse_ns_key(&args[2]);
    let member = args[3].clone();
    let mut db = store.write().await;

    if db
        .entries
        .get::<str>(src_ns.as_ref())
        .and_then(|m| m.get::<str>(src_key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&src_ns, &src_key);
        return resp_int(0);
    }

    // Capture old sizes before any mutation for byte accounting.
    let old_src_size = db.entries.get::<str>(src_ns.as_ref())
        .and_then(|m| m.get::<str>(src_key.as_ref()))
        .map(|e| Db::entry_size(&src_ns, &src_key, e.value.byte_len()))
        .unwrap_or(0);
    let old_dst_size = db.entries.get::<str>(dst_ns.as_ref())
        .and_then(|m| m.get::<str>(dst_key.as_ref()))
        .map(|e| Db::entry_size(&dst_ns, &dst_key, e.value.byte_len()))
        .unwrap_or(0);

    let found = match db
        .entries
        .get_mut::<str>(src_ns.as_ref())
        .and_then(|m| m.get_mut::<str>(src_key.as_ref()))
    {
        None => return resp_int(0),
        Some(entry) => match entry.value_mut().as_set_mut() {
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
        .get_mut::<str>(dst_ns.as_ref())
        .and_then(|m| m.get_mut::<str>(dst_key.as_ref()))
    {
        Some(entry) => match entry.value_mut().as_set_mut() {
            Some(s) => {
                s.insert(member);
            }
            None => return resp_wrongtype(),
        },
        None => {
            let mut s = HashSet::new();
            s.insert(member);
            db.entries.entry(dst_ns.clone().into_owned()).or_default().insert(
                dst_key.clone().into_owned(),
                Entry {
                    value: ValueCell::new(Value::Set(s)),
                    hits: AtomicU64::new(0),
                    expiry: None,
                },
            );
        }
    }

    // Update byte accounting for both keys.
    let new_src_size = db.entries.get::<str>(src_ns.as_ref())
        .and_then(|m| m.get::<str>(src_key.as_ref()))
        .map(|e| Db::entry_size(&src_ns, &src_key, e.value.byte_len()))
        .unwrap_or(0);
    let new_dst_size = db.entries.get::<str>(dst_ns.as_ref())
        .and_then(|m| m.get::<str>(dst_key.as_ref()))
        .map(|e| Db::entry_size(&dst_ns, &dst_key, e.value.byte_len()))
        .unwrap_or(0);
    db.used_bytes = db.used_bytes
        .saturating_sub(old_src_size).saturating_add(new_src_size)
        .saturating_sub(old_dst_size).saturating_add(new_dst_size);
    let nb_src = db.namespace_bytes.entry(src_ns.clone().into_owned()).or_insert(0);
    *nb_src = nb_src.saturating_sub(old_src_size).saturating_add(new_src_size);
    let nb_dst = db.namespace_bytes.entry(dst_ns.clone().into_owned()).or_insert(0);
    *nb_dst = nb_dst.saturating_sub(old_dst_size).saturating_add(new_dst_size);
    db.write_version += 1;
    let v = db.write_version;
    db.key_versions.entry(src_ns.into_owned()).or_default().insert(src_key.into_owned(), v);
    db.key_versions.entry(dst_ns.into_owned()).or_default().insert(dst_key.into_owned(), v);

    resp_int(1)
}

async fn cmd_spop(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
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
            None => return resp_err_not_integer(),
        }
    } else {
        None
    };

    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return if count.is_some() {
            resp_array(&[])
        } else {
            resp_null()
        };
    }

    let (resp, modified) = match db.entries.get_mut::<str>(ns.as_ref()).and_then(|m| m.get_mut::<str>(key.as_ref())) {
        None => (if count.is_some() { resp_array(&[]) } else { resp_null() }, false),
        Some(entry) => match entry.value_mut().as_set_mut() {
            None => return resp_wrongtype(),
            Some(s) => {
                if let Some(c) = count {
                    let popped: Vec<Vec<u8>> = s.iter().take(c).cloned().collect();
                    for m in &popped {
                        s.remove(m.as_slice());
                    }
                    let was_popped = !popped.is_empty();
                    (resp_array(&popped), was_popped)
                } else {
                    // pop one
                    let member = s.iter().next().cloned();
                    match member {
                        None => (resp_null(), false),
                        Some(m) => {
                            s.remove(m.as_slice());
                            (resp_bulk(&m), true)
                        }
                    }
                }
            }
        },
    };
    if modified {
        db.touch_key_version(&ns, &key);
    }
    resp
}

async fn cmd_srandmember(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
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
            None => return resp_err_not_integer(),
        }
    } else {
        None
    };

    let (resp, expired, mark, is_ear) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => {
                let r = if count_opt.is_some() { resp_array(&[]) } else { resp_null() };
                (r, false, false, false)
            }
            Some(entry) if entry.is_expired() => {
                let r = if count_opt.is_some() { resp_array(&[]) } else { resp_null() };
                (r, true, false, false)
            }
            Some(entry) => match entry.value.as_set() {
                None => (resp_wrongtype(), false, false, false),
                Some(s) => {
                    let is_ear = db.is_ear_namespace(&ns);
                    let mut members: Vec<Vec<u8>> = s.iter().cloned().collect();
                    members.sort();
                    let (r, mark) = match count_opt {
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
                            let nonempty = !result.is_empty();
                            (resp_array(&result), nonempty)
                        }
                    };
                    (r, false, mark, is_ear)
                }
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    } else if mark && is_ear {
        let mut db = store.write().await;
        db.mark_ear(&ns, &key);
    }
    resp
}

// ═══════════════════════════════════════════════════════════════════════════════
// ZSET COMMANDS
// ═══════════════════════════════════════════════════════════════════════════════

async fn cmd_zadd(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
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

    if nx && xx {
        return resp_err("XX and NX options at the same time are not compatible");
    }
    if gt && lt || nx && (gt || lt) {
        return resp_err("GT, LT, and NX options at the same time are not compatible");
    }

    if idx + 1 >= args.len() || !(args.len() - idx).is_multiple_of(2) {
        return wrong_args(&args[0]);
    }

    let mut db = store.write().await;
    db.purge_if_expired(&ns, &key);
    // Validate type
    if let Some(e) = db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref()))
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
                    Some(s) => {
                        if s.is_nan() {
                            return resp_err("not a finite value");
                        }
                        s
                    }
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
            .entry(ns.clone().into_owned())
            .or_default()
            .entry(key.clone().into_owned())
            .or_insert_with(|| Entry {
                value: ValueCell::new(Value::ZSet(ZSetData::default())),
                hits: AtomicU64::new(0),
                expiry: None,
            });
        let zset = match entry.value_mut().as_zset_mut() {
            Some(z) => z,
            None => return resp_wrongtype(),
        };

        let existing_score = zset.index.get(&member).copied();

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

    if added > 0 || changed > 0 || (incr && last_score.is_some()) {
        db.touch_key_version(&ns, &key);
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

async fn cmd_zrange(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
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
        let opt = args[i].as_slice();
        if opt.eq_ignore_ascii_case(b"BYSCORE") {
            byscore = true;
            i += 1;
        } else if opt.eq_ignore_ascii_case(b"BYLEX") {
            bylex = true;
            i += 1;
        } else if opt.eq_ignore_ascii_case(b"REV") {
            rev = true;
            i += 1;
        } else if opt.eq_ignore_ascii_case(b"WITHSCORES") {
            withscores = true;
            i += 1;
        } else if opt.eq_ignore_ascii_case(b"LIMIT") {
            if i + 2 >= args.len() {
                return resp_err_syntax();
            }
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

    let (resp, expired, mark, is_ear) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => (resp_array(&[]), false, false, false),
            Some(entry) if entry.is_expired() => (resp_array(&[]), true, false, false),
            Some(entry) => match entry.value.as_zset() {
                None => (resp_wrongtype(), false, false, false),
                Some(data) => {
                    let mut out_items: Vec<Vec<u8>> = Vec::new();
                    let mut skipped = 0usize;
                    let mut selected = 0usize;
                    let take = limit_count.unwrap_or(usize::MAX);

                    if bylex {
                        let (min_raw, max_raw) = if rev {
                            (stop_raw, start_raw)
                        } else {
                            (start_raw, stop_raw)
                        };
                        let min = match parse_lex_bound(min_raw) {
                            Some(b) => b,
                            None => return resp_err_invalid_lex_range(),
                        };
                        let max = match parse_lex_bound(max_raw) {
                            Some(b) => b,
                            None => return resp_err_invalid_lex_range(),
                        };
                        if rev {
                            for e in data.sorted.iter().rev() {
                                if !member_in_lex_range(&e.member, &min, &max) {
                                    continue;
                                }
                                if skipped < limit_offset {
                                    skipped += 1;
                                    continue;
                                }
                                if selected >= take {
                                    break;
                                }
                                out_items.push(e.member.clone());
                                if withscores {
                                    out_items.push(format_score(e.score));
                                }
                                selected += 1;
                            }
                        } else {
                            for e in &data.sorted {
                                if !member_in_lex_range(&e.member, &min, &max) {
                                    continue;
                                }
                                if skipped < limit_offset {
                                    skipped += 1;
                                    continue;
                                }
                                if selected >= take {
                                    break;
                                }
                                out_items.push(e.member.clone());
                                if withscores {
                                    out_items.push(format_score(e.score));
                                }
                                selected += 1;
                            }
                        }
                    } else if byscore {
                        let (min_raw, max_raw) = if rev {
                            (stop_raw, start_raw)
                        } else {
                            (start_raw, stop_raw)
                        };
                        let (min_score, min_excl) = match parse_score_bound(min_raw) {
                            Some(b) => b,
                            None => return resp_err_min_or_max_not_float(),
                        };
                        let (max_score, max_excl) = match parse_score_bound(max_raw) {
                            Some(b) => b,
                            None => return resp_err_min_or_max_not_float(),
                        };
                        if rev {
                            for e in data.sorted.iter().rev() {
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
                                if !(above && below) {
                                    continue;
                                }
                                if skipped < limit_offset {
                                    skipped += 1;
                                    continue;
                                }
                                if selected >= take {
                                    break;
                                }
                                out_items.push(e.member.clone());
                                if withscores {
                                    out_items.push(format_score(e.score));
                                }
                                selected += 1;
                            }
                        } else {
                            for e in &data.sorted {
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
                                if !(above && below) {
                                    continue;
                                }
                                if skipped < limit_offset {
                                    skipped += 1;
                                    continue;
                                }
                                if selected >= take {
                                    break;
                                }
                                out_items.push(e.member.clone());
                                if withscores {
                                    out_items.push(format_score(e.score));
                                }
                                selected += 1;
                            }
                        }
                    } else {
                        let len = data.sorted.len() as i64;
                        if len > 0 {
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
                            if start <= stop {
                                let stop_capped = stop.min(data.sorted.len().saturating_sub(1));
                                if rev {
                                    for e in data.sorted[start..=stop_capped].iter().rev() {
                                        if skipped < limit_offset {
                                            skipped += 1;
                                            continue;
                                        }
                                        if selected >= take {
                                            break;
                                        }
                                        out_items.push(e.member.clone());
                                        if withscores {
                                            out_items.push(format_score(e.score));
                                        }
                                        selected += 1;
                                    }
                                } else {
                                    for e in &data.sorted[start..=stop_capped] {
                                        if skipped < limit_offset {
                                            skipped += 1;
                                            continue;
                                        }
                                        if selected >= take {
                                            break;
                                        }
                                        out_items.push(e.member.clone());
                                        if withscores {
                                            out_items.push(format_score(e.score));
                                        }
                                        selected += 1;
                                    }
                                }
                            }
                        }
                    }

                    (
                        resp_array(&out_items),
                        false,
                        true,
                        db.is_ear_namespace(&ns),
                    )
                }
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    } else if mark && is_ear {
        let mut db = store.write().await;
        db.mark_ear(&ns, &key);
    }
    resp
}

async fn cmd_zrangebyscore(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let min_raw = &args[2];
    let max_raw = &args[3];

    let (min_score, min_excl) = match parse_score_bound(min_raw) {
        Some(b) => b,
        None => return resp_err_min_or_max_not_float(),
    };
    let (max_score, max_excl) = match parse_score_bound(max_raw) {
        Some(b) => b,
        None => return resp_err_min_or_max_not_float(),
    };

    let mut withscores = false;
    let mut limit_offset: usize = 0;
    let mut limit_count: Option<usize> = None;
    let mut i = 4;
    while i < args.len() {
        let opt = args[i].as_slice();
        if opt.eq_ignore_ascii_case(b"WITHSCORES") {
            withscores = true;
            i += 1;
        } else if opt.eq_ignore_ascii_case(b"LIMIT") {
            if i + 2 >= args.len() {
                return resp_err_syntax();
            }
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

    let (resp, expired, mark, is_ear) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => (resp_array(&[]), false, false, false),
            Some(entry) if entry.is_expired() => (resp_array(&[]), true, false, false),
            Some(entry) => match entry.value.as_zset() {
                None => (resp_wrongtype(), false, false, false),
                Some(data) => {
                    let mut out: Vec<Vec<u8>> = Vec::new();
                    let mut skipped = 0usize;
                    let mut selected = 0usize;
                    let take = limit_count.unwrap_or(usize::MAX);
                    for e in &data.sorted {
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
                        if !(above && below) {
                            continue;
                        }
                        if skipped < limit_offset {
                            skipped += 1;
                            continue;
                        }
                        if selected >= take {
                            break;
                        }
                        out.push(e.member.clone());
                        if withscores {
                            out.push(format_score(e.score));
                        }
                        selected += 1;
                    }
                    (resp_array(&out), false, true, db.is_ear_namespace(&ns))
                }
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    } else if mark && is_ear {
        let mut db = store.write().await;
        db.mark_ear(&ns, &key);
    }
    resp
}

async fn cmd_zrevrangebyscore(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let max_raw = &args[2];
    let min_raw = &args[3];

    let (min_score, min_excl) = match parse_score_bound(min_raw) {
        Some(b) => b,
        None => return resp_err_min_or_max_not_float(),
    };
    let (max_score, max_excl) = match parse_score_bound(max_raw) {
        Some(b) => b,
        None => return resp_err_min_or_max_not_float(),
    };

    let mut withscores = false;
    let mut limit_offset: usize = 0;
    let mut limit_count: Option<usize> = None;
    let mut i = 4;
    while i < args.len() {
        let opt = args[i].as_slice();
        if opt.eq_ignore_ascii_case(b"WITHSCORES") {
            withscores = true;
            i += 1;
        } else if opt.eq_ignore_ascii_case(b"LIMIT") {
            if i + 2 >= args.len() {
                return resp_err_syntax();
            }
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

    let (resp, expired, mark, is_ear) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => (resp_array(&[]), false, false, false),
            Some(entry) if entry.is_expired() => (resp_array(&[]), true, false, false),
            Some(entry) => match entry.value.as_zset() {
                None => (resp_wrongtype(), false, false, false),
                Some(data) => {
                    let mut out: Vec<Vec<u8>> = Vec::new();
                    let mut skipped = 0usize;
                    let mut selected = 0usize;
                    let take = limit_count.unwrap_or(usize::MAX);
                    for e in data.sorted.iter().rev() {
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
                        if !(above && below) {
                            continue;
                        }
                        if skipped < limit_offset {
                            skipped += 1;
                            continue;
                        }
                        if selected >= take {
                            break;
                        }
                        out.push(e.member.clone());
                        if withscores {
                            out.push(format_score(e.score));
                        }
                        selected += 1;
                    }
                    (resp_array(&out), false, true, db.is_ear_namespace(&ns))
                }
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    } else if mark && is_ear {
        let mut db = store.write().await;
        db.mark_ear(&ns, &key);
    }
    resp
}

async fn cmd_zrevrange(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
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

    let (resp, expired, mark, is_ear) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => (resp_array(&[]), false, false, false),
            Some(entry) if entry.is_expired() => (resp_array(&[]), true, false, false),
            Some(entry) => match entry.value.as_zset() {
                None => (resp_wrongtype(), false, false, false),
                Some(data) => {
                    if data.is_empty() {
                        (resp_array(&[]), false, true, db.is_ear_namespace(&ns))
                    } else {
                        let len = data.sorted.len() as i64;
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
                            (resp_array(&[]), false, true, db.is_ear_namespace(&ns))
                        } else {
                            let stop_capped = stop.min(data.sorted.len().saturating_sub(1));
                            let mut out: Vec<Vec<u8>> = Vec::new();
                            for e in data.sorted[start..=stop_capped].iter().rev() {
                                out.push(e.member.clone());
                                if withscores {
                                    out.push(format_score(e.score));
                                }
                            }
                            (resp_array(&out), false, true, db.is_ear_namespace(&ns))
                        }
                    }
                }
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    } else if mark && is_ear {
        let mut db = store.write().await;
        db.mark_ear(&ns, &key);
    }
    resp
}

async fn cmd_zrank(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 3 || args.len() > 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let member = &args[2];
    let withscore =
        args.len() == 4 && String::from_utf8_lossy(&args[3]).to_ascii_uppercase() == "WITHSCORE";

    let (zset, is_ear) = match read_zset_snapshot(store, &ns, &key).await {
        ZsetLookup::Missing => return resp_null(),
        ZsetLookup::Expired => {
            cleanup_expired_key(store, &ns, &key).await;
            return resp_null();
        }
        ZsetLookup::WrongType => return resp_wrongtype(),
        ZsetLookup::Found(zset, is_ear) => (zset, is_ear),
    };

    match zset.index.get(member.as_slice()) {
        None => resp_null(),
        Some(&score) => {
            let rank = zset.sorted.partition_point(|e| {
                e.score < score || (e.score == score && e.member.as_slice() < member.as_slice())
            });
            let resp: std::borrow::Cow<'static, [u8]> = if withscore {
                let mut out = b"*2\r\n".to_vec();
                out.extend_from_slice(&resp_usize(rank));
                out.extend_from_slice(&resp_bulk(&format_score(score)));
                out.into()
            } else {
                resp_usize(rank)
            };
            if is_ear {
                let mut db = store.write().await;
                db.mark_ear(&ns, &key);
            }
            resp
        }
    }
}

async fn cmd_zrevrank(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 3 || args.len() > 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let member = &args[2];
    let withscore =
        args.len() == 4 && String::from_utf8_lossy(&args[3]).to_ascii_uppercase() == "WITHSCORE";

    let (zset, is_ear) = match read_zset_snapshot(store, &ns, &key).await {
        ZsetLookup::Missing => return resp_null(),
        ZsetLookup::Expired => {
            cleanup_expired_key(store, &ns, &key).await;
            return resp_null();
        }
        ZsetLookup::WrongType => return resp_wrongtype(),
        ZsetLookup::Found(zset, is_ear) => (zset, is_ear),
    };

    match zset.index.get(member.as_slice()) {
        None => resp_null(),
        Some(&score) => {
            let rank = zset.sorted.partition_point(|e| {
                e.score < score || (e.score == score && e.member.as_slice() < member.as_slice())
            });
            let rev_rank = zset.sorted.len() - 1 - rank;
            let resp: std::borrow::Cow<'static, [u8]> = if withscore {
                let mut out = b"*2\r\n".to_vec();
                out.extend_from_slice(&resp_usize(rev_rank));
                out.extend_from_slice(&resp_bulk(&format_score(score)));
                out.into()
            } else {
                resp_usize(rev_rank)
            };
            if is_ear {
                let mut db = store.write().await;
                db.mark_ear(&ns, &key);
            }
            resp
        }
    }
}

async fn cmd_zscore(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let member = &args[2];
    let (zset, is_ear) = match read_zset_snapshot(store, &ns, &key).await {
        ZsetLookup::Missing => return resp_null(),
        ZsetLookup::Expired => {
            cleanup_expired_key(store, &ns, &key).await;
            return resp_null();
        }
        ZsetLookup::WrongType => return resp_wrongtype(),
        ZsetLookup::Found(zset, is_ear) => (zset, is_ear),
    };

    match zset.index.get(member.as_slice()) {
        None => resp_null(),
        Some(&score) => {
            if is_ear {
                let mut db = store.write().await;
                db.mark_ear(&ns, &key);
            }
            resp_bulk(&format_score(score))
        }
    }
}

async fn cmd_zmscore(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
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
        ZsetLookup::Found(zset, is_ear) => {
            if is_ear {
                let mut db = store.write().await;
                db.mark_ear(&ns, &key);
            }
            zset
        }
    };

    let mut out = Vec::new();
    append_array_header(&mut out, members.len());
    for m in members {
        match zset.index.get(m.as_slice()) {
            None => append_null(&mut out),
            Some(&score) => append_bulk(&mut out, &format_score(score)),
        }
    }
    out.into()
}

async fn cmd_zrem(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let members = &args[2..];
    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    let (resp, modified) = match db.entries.get_mut::<str>(ns.as_ref()).and_then(|m| m.get_mut::<str>(key.as_ref())) {
        None => (resp_int(0), false),
        Some(entry) => match entry.value_mut().as_zset_mut() {
            None => return resp_wrongtype(),
            Some(data) => {
                let before = data.len();
                for m in members {
                    if let Some(score) = data.index.remove(m.as_slice()) {
                        let pos = data.sorted.partition_point(|e| {
                            e.score < score
                                || (e.score == score && e.member.as_slice() < m.as_slice())
                        });
                        if pos < data.sorted.len() && data.sorted[pos].member == m.as_slice() {
                            data.sorted.remove(pos);
                        }
                    }
                }
                let removed = before - data.len();
                (resp_usize(removed), removed > 0)
            }
        },
    };
    if modified {
        db.touch_key_version(&ns, &key);
    }
    resp
}

async fn cmd_zcard(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
        None => resp_int(0),
        Some(entry) => match entry.value.as_zset() {
            None => resp_wrongtype(),
            Some(data) => resp_usize(data.len()),
        },
    }
}

async fn cmd_zcount(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let (min_score, min_excl) = match parse_score_bound(&args[2]) {
        Some(b) => b,
        None => return resp_err_min_or_max_not_float(),
    };
    let (max_score, max_excl) = match parse_score_bound(&args[3]) {
        Some(b) => b,
        None => return resp_err_min_or_max_not_float(),
    };
    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
        None => resp_int(0),
        Some(entry) => match entry.value.as_zset() {
            None => resp_wrongtype(),
            Some(data) => {
                let count = data
                    .sorted
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
                resp_usize(count)
            }
        },
    }
}

async fn cmd_zincrby(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
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
    db.purge_if_expired(&ns, &key);

    let current_score: f64 = match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
        None => 0.0,
        Some(entry) => match entry.value.as_zset() {
            None => return resp_wrongtype(),
            Some(data) => data.index.get(member.as_slice()).copied().unwrap_or(0.0),
        },
    };

    let new_score = current_score + by;
    let entry = db
        .entries
        .entry(ns.clone().into_owned())
        .or_default()
        .entry(key.clone().into_owned())
        .or_insert_with(|| Entry {
            value: ValueCell::new(Value::ZSet(ZSetData::default())),
            hits: AtomicU64::new(0),
            expiry: None,
        });
    match entry.value_mut().as_zset_mut() {
        Some(data) => {
            zset_insert_or_update(data, new_score, member);
        }
        None => return resp_wrongtype(),
    }
    db.touch_key_version(&ns, &key);
    resp_bulk(&format_score(new_score))
}

async fn cmd_zrangebylex(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let min = match parse_lex_bound(&args[2]) {
        Some(b) => b,
        None => return resp_err_invalid_lex_range(),
    };
    let max = match parse_lex_bound(&args[3]) {
        Some(b) => b,
        None => return resp_err_invalid_lex_range(),
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

    let (resp, expired, mark, is_ear) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            Some(entry) if entry.is_expired() => (resp_array(&[]), true, false, false),
            None => (resp_array(&[]), false, false, false),
            Some(entry) => match entry.value.as_zset() {
                None => (resp_wrongtype(), false, false, false),
                Some(data) => {
                    let mut result: Vec<Vec<u8>> = Vec::new();
                    let mut seen = 0usize;
                    let take = limit_count.unwrap_or(usize::MAX);
                    for e in &data.sorted {
                        if !member_in_lex_range(&e.member, &min, &max) {
                            continue;
                        }
                        if seen < limit_offset {
                            seen += 1;
                            continue;
                        }
                        if result.len() >= take {
                            break;
                        }
                        result.push(e.member.clone());
                    }
                    (resp_array(&result), false, true, db.is_ear_namespace(&ns))
                }
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    } else if mark && is_ear {
        let mut db = store.write().await;
        db.mark_ear(&ns, &key);
    }
    resp
}

async fn cmd_zlexcount(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let min = match parse_lex_bound(&args[2]) {
        Some(b) => b,
        None => return resp_err_invalid_lex_range(),
    };
    let max = match parse_lex_bound(&args[3]) {
        Some(b) => b,
        None => return resp_err_invalid_lex_range(),
    };
    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
        None => resp_int(0),
        Some(entry) => match entry.value.as_zset() {
            None => resp_wrongtype(),
            Some(data) => {
                let count = data
                    .sorted
                    .iter()
                    .filter(|e| member_in_lex_range(&e.member, &min, &max))
                    .count();
                resp_usize(count)
            }
        },
    }
}

async fn cmd_zremrangebyrank(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let start_i: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err_not_integer(),
    };
    let stop_i: i64 = match std::str::from_utf8(&args[3])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err_not_integer(),
    };
    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    let (resp, modified) = match db.entries.get_mut::<str>(ns.as_ref()).and_then(|m| m.get_mut::<str>(key.as_ref())) {
        None => (resp_int(0), false),
        Some(entry) => match entry.value_mut().as_zset_mut() {
            None => return resp_wrongtype(),
            Some(data) => {
                let len = data.sorted.len() as i64;
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
                let stop_capped = stop.min(data.sorted.len().saturating_sub(1));
                if start <= stop_capped {
                    let drained: Vec<ZEntry> = data.sorted.drain(start..=stop_capped).collect();
                    for e in &drained {
                        data.index.remove(e.member.as_slice());
                    }
                }
                (resp_usize(count), count > 0)
            }
        },
    };
    if modified {
        db.touch_key_version(&ns, &key);
    }
    resp
}

async fn cmd_zremrangebyscore(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let (min_score, min_excl) = match parse_score_bound(&args[2]) {
        Some(b) => b,
        None => return resp_err_min_or_max_not_float(),
    };
    let (max_score, max_excl) = match parse_score_bound(&args[3]) {
        Some(b) => b,
        None => return resp_err_min_or_max_not_float(),
    };
    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    let (resp, modified) = match db.entries.get_mut::<str>(ns.as_ref()).and_then(|m| m.get_mut::<str>(key.as_ref())) {
        None => (resp_int(0), false),
        Some(entry) => match entry.value_mut().as_zset_mut() {
            None => return resp_wrongtype(),
            Some(data) => {
                let before = data.len();
                let mut to_remove: Vec<Vec<u8>> = Vec::new();
                data.sorted.retain(|e| {
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
                    if above && below {
                        to_remove.push(e.member.clone());
                    }
                    !(above && below)
                });
                for m in to_remove {
                    data.index.remove(m.as_slice());
                }
                let removed = before - data.len();
                (resp_usize(removed), removed > 0)
            }
        },
    };
    if modified {
        db.touch_key_version(&ns, &key);
    }
    resp
}

async fn cmd_zremrangebylex(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 4 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let min = match parse_lex_bound(&args[2]) {
        Some(b) => b,
        None => return resp_err_invalid_lex_range(),
    };
    let max = match parse_lex_bound(&args[3]) {
        Some(b) => b,
        None => return resp_err_invalid_lex_range(),
    };
    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    let (resp, modified) = match db.entries.get_mut::<str>(ns.as_ref()).and_then(|m| m.get_mut::<str>(key.as_ref())) {
        None => (resp_int(0), false),
        Some(entry) => match entry.value_mut().as_zset_mut() {
            None => return resp_wrongtype(),
            Some(data) => {
                let before = data.len();
                let mut to_remove: Vec<Vec<u8>> = Vec::new();
                data.sorted.retain(|e| {
                    if member_in_lex_range(&e.member, &min, &max) {
                        to_remove.push(e.member.clone());
                        false
                    } else {
                        true
                    }
                });
                for m in to_remove {
                    data.index.remove(m.as_slice());
                }
                let removed = before - data.len();
                (resp_usize(removed), removed > 0)
            }
        },
    };
    if modified {
        db.touch_key_version(&ns, &key);
    }
    resp
}

async fn cmd_zpopmin(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
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
            None => return resp_err_not_integer(),
        }
    } else {
        1
    };

    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_array(&[]);
    }
    let (resp, modified) = match db.entries.get_mut::<str>(ns.as_ref()).and_then(|m| m.get_mut::<str>(key.as_ref())) {
        None => (resp_array(&[]), false),
        Some(entry) => match entry.value_mut().as_zset_mut() {
            None => return resp_wrongtype(),
            Some(data) => {
                let mut result: Vec<Vec<u8>> = Vec::new();
                for _ in 0..count {
                    if data.is_empty() {
                        break;
                    }
                    let e = data.sorted.remove(0);
                    data.index.remove(e.member.as_slice());
                    result.push(e.member);
                    result.push(format_score(e.score));
                }
                let was_popped = !result.is_empty();
                (resp_array(&result), was_popped)
            }
        },
    };
    if modified {
        db.touch_key_version(&ns, &key);
    }
    resp
}

async fn cmd_zpopmax(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
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
            None => return resp_err_not_integer(),
        }
    } else {
        1
    };

    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_array(&[]);
    }
    let (resp, modified) = match db.entries.get_mut::<str>(ns.as_ref()).and_then(|m| m.get_mut::<str>(key.as_ref())) {
        None => (resp_array(&[]), false),
        Some(entry) => match entry.value_mut().as_zset_mut() {
            None => return resp_wrongtype(),
            Some(data) => {
                let mut result: Vec<Vec<u8>> = Vec::new();
                for _ in 0..count {
                    if data.is_empty() {
                        break;
                    }
                    let e = data.sorted.pop().unwrap();
                    data.index.remove(e.member.as_slice());
                    result.push(e.member);
                    result.push(format_score(e.score));
                }
                let was_popped = !result.is_empty();
                (resp_array(&result), was_popped)
            }
        },
    };
    if modified {
        db.touch_key_version(&ns, &key);
    }
    resp
}

async fn cmd_zrandmember(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
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
            None => return resp_err_not_integer(),
        }
    } else {
        None
    };
    let withscores =
        args.len() >= 4 && String::from_utf8_lossy(&args[3]).to_ascii_uppercase() == "WITHSCORES";

    let (resp, expired, mark, is_ear) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            Some(entry) if entry.is_expired() => {
                let resp = if count_opt.is_some() {
                    resp_array(&[])
                } else {
                    resp_null()
                };
                (resp, true, false, false)
            }
            None => {
                let resp = if count_opt.is_some() {
                    resp_array(&[])
                } else {
                    resp_null()
                };
                (resp, false, false, false)
            }
            Some(entry) => match entry.value.as_zset() {
                None => (resp_wrongtype(), false, false, false),
                Some(data) => {
                    if data.is_empty() {
                        let resp = if count_opt.is_some() {
                            resp_array(&[])
                        } else {
                            resp_null()
                        };
                        (resp, false, false, false)
                    } else {
                        let is_ear = db.is_ear_namespace(&ns);
                        match count_opt {
                            None => (resp_bulk(&data.sorted[0].member), false, true, is_ear),
                            Some(count) => {
                                let abs_count = count.unsigned_abs() as usize;
                                let allow_repeat = count < 0;
                                let mut result: Vec<Vec<u8>> = Vec::new();
                                if allow_repeat {
                                    for i in 0..abs_count {
                                        let idx = i % data.sorted.len();
                                        result.push(data.sorted[idx].member.clone());
                                        if withscores {
                                            result.push(format_score(data.sorted[idx].score));
                                        }
                                    }
                                } else {
                                    let take = abs_count.min(data.sorted.len());
                                    for item in data.sorted.iter().take(take) {
                                        result.push(item.member.clone());
                                        if withscores {
                                            result.push(format_score(item.score));
                                        }
                                    }
                                }
                                (resp_array(&result), false, true, is_ear)
                            }
                        }
                    }
                }
            },
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    } else if mark && is_ear {
        let mut db = store.write().await;
        db.mark_ear(&ns, &key);
    }
    resp
}

// ═══════════════════════════════════════════════════════════════════════════════
// GENERIC KEY COMMANDS
// ═══════════════════════════════════════════════════════════════════════════════

async fn cmd_del(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
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

async fn cmd_exists(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let (count, expired_keys) = {
        let db = store.read().await;
        let mut count = 0i64;
        let mut expired_keys: Vec<(String, String)> = Vec::new();
        for raw_key in &args[1..] {
            let (ns, key) = parse_ns_key(raw_key);
            match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
                None => {}
                Some(entry) if entry.is_expired() => expired_keys.push((ns.as_ref().to_owned(), key.as_ref().to_owned())),
                Some(_) => count += 1,
            }
        }
        (count, expired_keys)
    };
    cleanup_expired_keys(store, &expired_keys).await;
    resp_int(count)
}

async fn cmd_type(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let (resp, expired) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
            None => (std::borrow::Cow::Borrowed(b"+none\r\n" as &[u8]), false),
            Some(entry) if entry.is_expired() => {
                (std::borrow::Cow::Borrowed(b"+none\r\n" as &[u8]), true)
            }
            Some(entry) => (
                std::borrow::Cow::Owned(format!("+{}\r\n", entry.value.type_name()).into_bytes()),
                false,
            ),
        }
    };
    if expired {
        cleanup_expired_key(store, &ns, &key).await;
    }
    resp
}

async fn cmd_ttl(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let (resp, expired) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
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

async fn cmd_pttl(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let (resp, expired) = {
        let db = store.read().await;
        match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
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

async fn cmd_expire(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let secs: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err_not_integer(),
    };
    let condition = args
        .get(3)
        .map(|b| String::from_utf8_lossy(b).to_ascii_uppercase());

    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    let entry = match db.entries.get_mut::<str>(ns.as_ref()).and_then(|m| m.get_mut::<str>(key.as_ref())) {
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
    schedule_expiry(store, ns.as_ref(), key.as_ref(), deadline);
    resp_int(1)
}

async fn cmd_expireat(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let unix_secs: u64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err_not_integer(),
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
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    let entry = match db.entries.get_mut::<str>(ns.as_ref()).and_then(|m| m.get_mut::<str>(key.as_ref())) {
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
    schedule_expiry(store, ns.as_ref(), key.as_ref(), deadline);
    resp_int(1)
}

async fn cmd_pexpire(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let ms: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err_not_integer(),
    };
    let condition = args
        .get(3)
        .map(|b| String::from_utf8_lossy(b).to_ascii_uppercase());

    let new_expiry = Instant::now() + Duration::from_millis(ms.max(0) as u64);

    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    let entry = match db.entries.get_mut::<str>(ns.as_ref()).and_then(|m| m.get_mut::<str>(key.as_ref())) {
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
    schedule_expiry(store, ns.as_ref(), key.as_ref(), deadline);
    resp_int(1)
}

async fn cmd_pexpireat(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 3 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let unix_ms: u64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err_not_integer(),
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
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    let entry = match db.entries.get_mut::<str>(ns.as_ref()).and_then(|m| m.get_mut::<str>(key.as_ref())) {
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
    schedule_expiry(store, ns.as_ref(), key.as_ref(), deadline);
    resp_int(1)
}

async fn cmd_persist(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(0);
    }
    match db.entries.get_mut::<str>(ns.as_ref()).and_then(|m| m.get_mut::<str>(key.as_ref())) {
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

async fn cmd_expiretime(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(-2);
    }
    match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
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
                resp_int(i64::try_from(unix_secs).unwrap_or(i64::MAX))
            }
        },
    }
}

async fn cmd_pexpiretime(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let (ns, key) = parse_ns_key(&args[1]);
    let mut db = store.write().await;
    if db
        .entries
        .get::<str>(ns.as_ref())
        .and_then(|m| m.get::<str>(key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&ns, &key);
        return resp_int(-2);
    }
    match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
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
                resp_int(i64::try_from(unix_ms).unwrap_or(i64::MAX))
            }
        },
    }
}

async fn cmd_rename(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (src_ns, src_key) = parse_ns_key(&args[1]);
    let (dst_ns, dst_key) = parse_ns_key(&args[2]);
    let mut db = store.write().await;

    if db
        .entries
        .get::<str>(src_ns.as_ref())
        .and_then(|m| m.get::<str>(src_key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&src_ns, &src_key);
        return resp_err_no_such_key();
    }

    let entry = match db.delete(&src_ns, &src_key) {
        None => return resp_err_no_such_key(),
        Some(e) => e,
    };
    let m = db.put_deferred(dst_ns.as_ref(), dst_key.as_ref(), entry);
    drop(db);
    m.emit();
    resp_ok()
}

async fn cmd_renamenx(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let (src_ns, src_key) = parse_ns_key(&args[1]);
    let (dst_ns, dst_key) = parse_ns_key(&args[2]);
    let mut db = store.write().await;

    if db
        .entries
        .get::<str>(src_ns.as_ref())
        .and_then(|m| m.get::<str>(src_key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&src_ns, &src_key);
        return resp_err_no_such_key();
    }
    if db
        .entries
        .get::<str>(src_ns.as_ref())
        .and_then(|m| m.get::<str>(src_key.as_ref()))
        .is_none()
    {
        return resp_err_no_such_key();
    }

    if db
        .entries
        .get::<str>(dst_ns.as_ref())
        .and_then(|m| m.get::<str>(dst_key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&dst_ns, &dst_key);
    }
    if db
        .entries
        .get::<str>(dst_ns.as_ref())
        .and_then(|m| m.get::<str>(dst_key.as_ref()))
        .is_some()
    {
        return resp_int(0);
    }

    let entry = db.delete(&src_ns, &src_key).unwrap();
    let m = db.put_deferred(dst_ns.as_ref(), dst_key.as_ref(), entry);
    drop(db);
    m.emit();
    resp_int(1)
}

async fn cmd_scan(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let cursor: usize = std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let mut pattern: Option<&[u8]> = None;
    let mut page_size: usize = 10;
    let mut type_filter: Option<&[u8]> = None;

    let mut i = 2;
    while i + 1 < args.len() {
        let opt = args[i].as_slice();
        if opt.eq_ignore_ascii_case(b"MATCH") {
            pattern = Some(&args[i + 1]);
            i += 2;
        } else if opt.eq_ignore_ascii_case(b"COUNT") {
            page_size = std::str::from_utf8(&args[i + 1])
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10);
            i += 2;
        } else if opt.eq_ignore_ascii_case(b"TYPE") {
            type_filter = Some(&args[i + 1]);
            i += 2;
        } else {
            i += 1;
        }
    }

    // Prevent infinite loop when COUNT 0 is supplied.
    let page_size = page_size.max(1);

    let db = store.read().await;
    let mut all_keys: Vec<Vec<u8>> = Vec::new();
    let mut scratch: Vec<u8> = Vec::new();

    for (ns, ns_map) in &db.entries {
        for (key, entry) in ns_map {
            if entry.is_expired() {
                continue;
            }
            if let Some(tf) = type_filter
                && !entry.value.type_name().as_bytes().eq_ignore_ascii_case(tf)
            {
                continue;
            }
            let display: &[u8] = if ns == "default" {
                key.as_bytes()
            } else {
                scratch.clear();
                scratch.extend_from_slice(ns.as_bytes());
                scratch.push(b'/');
                scratch.extend_from_slice(key.as_bytes());
                &scratch
            };
            if let Some(pat) = pattern
                && !glob_match(pat, display)
            {
                continue;
            }
            all_keys.push(display.to_vec());
        }
    }
    drop(db);
    all_keys.sort_unstable();

    let total = all_keys.len();
    let start = cursor.min(total);
    let end = (start + page_size).min(total);
    let page = &all_keys[start..end];
    let next_cursor: usize = if end < total { end } else { 0 };

    let mut out = Vec::new();
    append_array_header(&mut out, 2);
    append_bulk(&mut out, next_cursor.to_string().as_bytes());
    append_array_header(&mut out, page.len());
    for item in page {
        append_bulk(&mut out, item);
    }
    out.into()
}

pub(crate) async fn cmd_keys(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let pattern = &args[1];
    let db = store.read().await;

    let mut matched: Vec<Vec<u8>> = Vec::new();
    let mut scratch: Vec<u8> = Vec::new();
    for (ns, ns_map) in &db.entries {
        for (key, entry) in ns_map {
            if entry.is_expired() {
                continue;
            }
            let display: &[u8] = if ns == "default" {
                key.as_bytes()
            } else {
                scratch.clear();
                scratch.extend_from_slice(ns.as_bytes());
                scratch.push(b'/');
                scratch.extend_from_slice(key.as_bytes());
                &scratch
            };
            if glob_match(pattern, display) {
                matched.push(display.to_vec());
            }
        }
    }
    matched.sort();
    resp_array(&matched)
}

pub(crate) async fn cmd_touch(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let mut expired: Vec<(String, String)> = Vec::new();
    let count = {
        let db = store.read().await;
        let mut n = 0i64;
        for raw_key in &args[1..] {
            let (ns, key) = parse_ns_key(raw_key);
            match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
                None => {}
                Some(entry) if entry.is_expired() => {
                    expired.push((ns.into_owned(), key.into_owned()));
                }
                Some(entry) => {
                    entry.hits.store(0, std::sync::atomic::Ordering::Relaxed);
                    n += 1;
                }
            }
        }
        n
    };
    for (ns, key) in &expired {
        cleanup_expired_key(store, ns, key).await;
    }
    resp_int(count)
}

async fn cmd_copy(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
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
        .get::<str>(src_ns.as_ref())
        .and_then(|m| m.get::<str>(src_key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&src_ns, &src_key);
        return resp_int(0);
    }

    let src_value = match db.entries.get::<str>(src_ns.as_ref()).and_then(|m| m.get::<str>(src_key.as_ref())) {
        None => return resp_int(0),
        Some(e) => e.value.clone(),
    };
    let src_expiry = db
        .entries
        .get::<str>(src_ns.as_ref())
        .and_then(|m| m.get::<str>(src_key.as_ref()))
        .and_then(|e| e.expiry);

    if db
        .entries
        .get::<str>(dst_ns.as_ref())
        .and_then(|m| m.get::<str>(dst_key.as_ref()))
        .is_some_and(|e| e.is_expired())
    {
        db.delete(&dst_ns, &dst_key);
    }

    if !replace
        && db
            .entries
            .get::<str>(dst_ns.as_ref())
            .and_then(|m| m.get::<str>(dst_key.as_ref()))
            .is_some()
    {
        return resp_int(0);
    }

    let new_entry = Entry {
        value: src_value,
        hits: AtomicU64::new(0),
        expiry: src_expiry,
    };
    let m = db.put_deferred(dst_ns.as_ref(), dst_key.as_ref(), new_entry);
    drop(db);
    m.emit();

    if let Some(deadline) = src_expiry {
        schedule_expiry(store, dst_ns.as_ref(), dst_key.as_ref(), deadline);
    }

    resp_int(1)
}

async fn cmd_object(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
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
            match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
                None => resp_null(),
                Some(entry) => {
                    let enc = match &*entry.value {
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
            match db.entries.get::<str>(ns.as_ref()).and_then(|m| m.get::<str>(key.as_ref())) {
                None => resp_null(),
                Some(entry) => {
                    resp_int(i64::try_from(entry.hits.load(std::sync::atomic::Ordering::Relaxed)).unwrap_or(i64::MAX))
                }
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
        _ => resp_err_unknown_subcommand(),
    }
}

async fn cmd_unlink(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    // Same as DEL
    cmd_del(args, store).await
}

// ═══════════════════════════════════════════════════════════════════════════════
// SERVER COMMANDS
// ═══════════════════════════════════════════════════════════════════════════════

async fn cmd_hello(args: &[Vec<u8>], _store: &Store, conn: &mut ConnState) -> std::borrow::Cow<'static, [u8]> {
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

fn cmd_reset(conn: &mut ConnState) -> std::borrow::Cow<'static, [u8]> {
    conn.resp_version = 2;
    conn.client_name = None;
    std::borrow::Cow::Borrowed(b"+RESET\r\n")
}

async fn cmd_select(args: &[Vec<u8>], _store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let db_idx: i64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_err_not_integer(),
    };
    if db_idx == 0 {
        resp_ok()
    } else {
        resp_err("ERR DB index is out of range")
    }
}

async fn cmd_dbsize(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 1 {
        return wrong_args(&args[0]);
    }
    let db = store.read().await;
    resp_usize(db.total_keys())
}

async fn cmd_flushdb(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    let _ = args;
    store.write().await.flush_all();
    resp_ok()
}

async fn cmd_flushall(args: &[Vec<u8>], store: &Store) -> std::borrow::Cow<'static, [u8]> {
    let _ = args;
    store.write().await.flush_all();
    resp_ok()
}

async fn cmd_info(_args: &[Vec<u8>], store: &Store, conn: &ConnState) -> std::borrow::Cow<'static, [u8]> {
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

async fn cmd_config(args: &[Vec<u8>], _store: &Store) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let sub = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
    match sub.as_str() {
        "GET" => resp_array(&[]),
        "SET" => resp_ok(),
        "RESETSTAT" => resp_ok(),
        "REWRITE" => resp_ok(),
        _ => resp_err_unknown_subcommand(),
    }
}

async fn cmd_command(args: &[Vec<u8>], _store: &Store) -> std::borrow::Cow<'static, [u8]> {
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

async fn cmd_client(args: &[Vec<u8>], _store: &Store, conn: &mut ConnState) -> std::borrow::Cow<'static, [u8]> {
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
        "ID" => resp_int(i64::try_from(conn.client_id).unwrap_or(i64::MAX)),
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
        _ => resp_err_unknown_subcommand(),
    }
}

async fn cmd_latency(args: &[Vec<u8>], _store: &Store) -> std::borrow::Cow<'static, [u8]> {
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

async fn cmd_slowlog(args: &[Vec<u8>], _store: &Store) -> std::borrow::Cow<'static, [u8]> {
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

async fn cmd_debug(args: &[Vec<u8>], _store: &Store) -> std::borrow::Cow<'static, [u8]> {
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

async fn cmd_wait(_args: &[Vec<u8>], _store: &Store) -> std::borrow::Cow<'static, [u8]> {
    resp_int(0)
}

async fn cmd_xadd(_args: &[Vec<u8>], _store: &Store) -> std::borrow::Cow<'static, [u8]> {
    resp_err("stream type not supported")
}

// ═══════════════════════════════════════════════════════════════════════════════
// TRANSACTION COMMANDS (MULTI / EXEC / DISCARD / WATCH / UNWATCH)
// ═══════════════════════════════════════════════════════════════════════════════

fn cmd_multi(conn: &mut ConnState) -> std::borrow::Cow<'static, [u8]> {
    if conn.multi_state.is_some() {
        return resp_err("MULTI calls can not be nested");
    }
    conn.multi_state = Some(MultiState::new());
    resp_ok()
}

fn cmd_discard(conn: &mut ConnState) -> std::borrow::Cow<'static, [u8]> {
    if conn.multi_state.is_none() {
        return resp_err("DISCARD without MULTI");
    }
    conn.multi_state = None;
    conn.watched.clear();
    resp_ok()
}

async fn cmd_exec(
    store: &Store,
    conn: &mut ConnState,
    hub: &PubSubHub,
) -> std::borrow::Cow<'static, [u8]> {
    let Some(ms) = conn.multi_state.take() else {
        return resp_err("EXEC without MULTI");
    };
    // Clear WATCH state regardless of outcome.
    let watched = std::mem::take(&mut conn.watched);

    if ms.error {
        return std::borrow::Cow::Borrowed(b"-EXECABORT Transaction discarded because of previous errors.\r\n");
    }

    // Check watched keys for dirty writes.
    if !watched.is_empty() {
        let db = store.read().await;
        let dirty = watched
            .iter()
            .any(|((ns, key), &ver)| db.key_version(ns, key) != ver);
        drop(db);
        if dirty {
            // Optimistic lock failed: return null array (nil multi-bulk).
            return resp_null_array();
        }
    }

    // Execute queued commands and collect responses.
    let count = ms.queued.len();
    let mut buf: Vec<u8> = Vec::new();
    append_array_header(&mut buf, count);
    for cmd_args in ms.queued {
        let (resp, _quit) = Box::pin(dispatch(&cmd_args, store, conn, hub)).await;
        buf.extend_from_slice(&resp);
    }
    std::borrow::Cow::Owned(buf)
}

async fn cmd_watch(
    args: &[Vec<u8>],
    store: &Store,
    conn: &mut ConnState,
) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    if conn.multi_state.is_some() {
        return resp_err("WATCH inside MULTI is not allowed");
    }
    let db = store.read().await;
    for raw_key in &args[1..] {
        let (ns, key) = parse_ns_key(raw_key);
        let ver = db.key_version(&ns, &key);
        conn.watched.insert((ns.into_owned(), key.into_owned()), ver);
    }
    drop(db);
    resp_ok()
}

fn cmd_unwatch(conn: &mut ConnState) -> std::borrow::Cow<'static, [u8]> {
    conn.watched.clear();
    resp_ok()
}

// ═══════════════════════════════════════════════════════════════════════════════
// PUB-SUB COMMANDS (SUBSCRIBE / UNSUBSCRIBE / PSUBSCRIBE / PUNSUBSCRIBE / PUBLISH)
// ═══════════════════════════════════════════════════════════════════════════════

/// Build the subscription confirmation reply sent for SUBSCRIBE/UNSUBSCRIBE.
fn pubsub_reply(kind: &[u8], channel: &[u8], count: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    append_array_header(&mut buf, 3);
    append_bulk(&mut buf, kind);
    append_bulk(&mut buf, channel);
    append_int(&mut buf, count as i64);
    buf
}

/// Build the subscription confirmation reply sent for PSUBSCRIBE/PUNSUBSCRIBE.
fn ppubsub_reply(kind: &[u8], pattern: &[u8], count: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    append_array_header(&mut buf, 3);
    append_bulk(&mut buf, kind);
    append_bulk(&mut buf, pattern);
    append_int(&mut buf, count as i64);
    buf
}

async fn cmd_subscribe(
    args: &[Vec<u8>],
    conn: &mut ConnState,
    hub: &PubSubHub,
) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let tx = conn.pubsub_sender();
    {
        let mut hub_guard = hub.write().await;
        for channel_bytes in &args[1..] {
            let channel = String::from_utf8_lossy(channel_bytes).into_owned();
            hub_guard.subscribe(&channel, conn.client_id, tx.clone());
        }
    }
    let mut buf: Vec<u8> = Vec::with_capacity((args.len() - 1) * 80);
    for channel_bytes in &args[1..] {
        let channel = String::from_utf8_lossy(channel_bytes).into_owned();
        conn.subscribed_channels.insert(channel);
        let count = conn.subscribed_channels.len() + conn.subscribed_patterns.len();
        buf.extend_from_slice(&pubsub_reply(b"subscribe", channel_bytes, count));
    }
    std::borrow::Cow::Owned(buf)
}

async fn cmd_unsubscribe(
    args: &[Vec<u8>],
    conn: &mut ConnState,
    hub: &PubSubHub,
) -> std::borrow::Cow<'static, [u8]> {
    let channels: Vec<String> = if args.len() < 2 {
        conn.subscribed_channels.iter().cloned().collect()
    } else {
        args[1..].iter().map(|b| String::from_utf8_lossy(b).into_owned()).collect()
    };
    {
        let mut hub_guard = hub.write().await;
        for channel in &channels {
            hub_guard.unsubscribe(channel, conn.client_id);
        }
    }
    let mut buf: Vec<u8> = Vec::with_capacity(channels.len().max(1) * 80);
    if channels.is_empty() {
        let total = conn.subscribed_channels.len() + conn.subscribed_patterns.len();
        buf.extend_from_slice(&pubsub_reply(b"unsubscribe", b"", total));
    } else {
        for channel in &channels {
            conn.subscribed_channels.remove(channel);
            let count = conn.subscribed_channels.len() + conn.subscribed_patterns.len();
            buf.extend_from_slice(&pubsub_reply(b"unsubscribe", channel.as_bytes(), count));
        }
    }
    std::borrow::Cow::Owned(buf)
}

async fn cmd_psubscribe(
    args: &[Vec<u8>],
    conn: &mut ConnState,
    hub: &PubSubHub,
) -> std::borrow::Cow<'static, [u8]> {
    if args.len() < 2 {
        return wrong_args(&args[0]);
    }
    let tx = conn.pubsub_sender();
    {
        let mut hub_guard = hub.write().await;
        for pattern_bytes in &args[1..] {
            let pattern = String::from_utf8_lossy(pattern_bytes).into_owned();
            hub_guard.psubscribe(&pattern, conn.client_id, tx.clone());
        }
    }
    let mut buf: Vec<u8> = Vec::with_capacity((args.len() - 1) * 80);
    for pattern_bytes in &args[1..] {
        let pattern = String::from_utf8_lossy(pattern_bytes).into_owned();
        conn.subscribed_patterns.insert(pattern);
        let count = conn.subscribed_channels.len() + conn.subscribed_patterns.len();
        buf.extend_from_slice(&ppubsub_reply(b"psubscribe", pattern_bytes, count));
    }
    std::borrow::Cow::Owned(buf)
}

async fn cmd_punsubscribe(
    args: &[Vec<u8>],
    conn: &mut ConnState,
    hub: &PubSubHub,
) -> std::borrow::Cow<'static, [u8]> {
    let patterns: Vec<String> = if args.len() < 2 {
        conn.subscribed_patterns.iter().cloned().collect()
    } else {
        args[1..].iter().map(|b| String::from_utf8_lossy(b).into_owned()).collect()
    };
    {
        let mut hub_guard = hub.write().await;
        for pattern in &patterns {
            hub_guard.punsubscribe(pattern, conn.client_id);
        }
    }
    let mut buf: Vec<u8> = Vec::with_capacity(patterns.len().max(1) * 80);
    if patterns.is_empty() {
        let total = conn.subscribed_channels.len() + conn.subscribed_patterns.len();
        buf.extend_from_slice(&ppubsub_reply(b"punsubscribe", b"", total));
    } else {
        for pattern in &patterns {
            conn.subscribed_patterns.remove(pattern);
            let count = conn.subscribed_channels.len() + conn.subscribed_patterns.len();
            buf.extend_from_slice(&ppubsub_reply(b"punsubscribe", pattern.as_bytes(), count));
        }
    }
    std::borrow::Cow::Owned(buf)
}

async fn cmd_publish(
    args: &[Vec<u8>],
    hub: &PubSubHub,
) -> std::borrow::Cow<'static, [u8]> {
    if args.len() != 3 {
        return wrong_args(&args[0]);
    }
    let channel = String::from_utf8_lossy(&args[1]);
    let count = hub.read().await.publish(&channel, &args[2]);
    resp_usize(count)
}

/// Encode a PubSubMessage into RESP bytes to be written to a subscriber's socket.
#[cfg(not(target_os = "linux"))]
pub(crate) fn encode_pubsub_message(msg: &PubSubMessage, _resp_version: u8) -> Vec<u8> {
    let cap = match msg {
        PubSubMessage::Message { channel, data } => 32 + channel.len() + data.len(),
        PubSubMessage::PMessage { pattern, channel, data } => {
            48 + pattern.len() + channel.len() + data.len()
        }
    };
    let mut buf = Vec::with_capacity(cap);
    match msg {
        PubSubMessage::Message { channel, data } => {
            append_array_header(&mut buf, 3);
            append_bulk(&mut buf, b"message");
            append_bulk(&mut buf, channel.as_bytes());
            append_bulk(&mut buf, data);
        }
        PubSubMessage::PMessage { pattern, channel, data } => {
            append_array_header(&mut buf, 4);
            append_bulk(&mut buf, b"pmessage");
            append_bulk(&mut buf, pattern.as_bytes());
            append_bulk(&mut buf, channel.as_bytes());
            append_bulk(&mut buf, data);
        }
    }
    buf
}

// ═══════════════════════════════════════════════════════════════════════════════
// DISPATCH
// ═══════════════════════════════════════════════════════════════════════════════

pub(crate) async fn dispatch(
    args: &[Vec<u8>],
    store: &Store,
    conn: &mut ConnState,
    hub: &PubSubHub,
) -> (std::borrow::Cow<'static, [u8]>, bool) {
    if args.is_empty() {
        return (resp_err("empty command"), false);
    }
    // Normalize to ASCII uppercase in a stack buffer — zero heap allocation.
    // The longest Redis command name is 16 bytes ("ZREMRANGEBYSCORE"); 20 is a safe ceiling.
    let mut cmd_upper = [0u8; 20];
    let cmd_len = args[0].len().min(cmd_upper.len());
    for (dst, &src) in cmd_upper[..cmd_len].iter_mut().zip(&args[0][..cmd_len]) {
        *dst = src.to_ascii_uppercase();
    }
    let cmd = &cmd_upper[..cmd_len];

    // ── Pub-sub mode: only a restricted command set is allowed ────────────────
    if conn.in_pubsub() {
        let allowed = matches!(
            cmd,
            b"SUBSCRIBE"
                | b"UNSUBSCRIBE"
                | b"PSUBSCRIBE"
                | b"PUNSUBSCRIBE"
                | b"PING"
                | b"RESET"
                | b"QUIT"
        );
        if !allowed {
            return (
                resp_err(
                    "Command not allowed in pub/sub mode. \
                     Use SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE/PING/RESET/QUIT.",
                ),
                false,
            );
        }
    }

    // ── MULTI queuing: queue all commands except EXEC/DISCARD/MULTI ──────────
    if let Some(multi) = conn.multi_state.as_mut()
        && !matches!(cmd, b"EXEC" | b"DISCARD" | b"MULTI" | b"WATCH" | b"UNWATCH")
    {
        multi.queued.push(args.to_vec());
        return (std::borrow::Cow::Borrowed(b"+QUEUED\r\n"), false);
    }

    let resp = match cmd {
        // Connection
        b"PING" => resp_pong(),
        b"QUIT" => return (resp_ok(), true),
        b"HELLO" => cmd_hello(args, store, conn).await,
        b"RESET" => {
            // RESET also clears pub-sub state and exits MULTI.
            if let Some(ms) = conn.multi_state.take() {
                drop(ms);
            }
            conn.watched.clear();
            // Unsubscribe from all channels/patterns.
            if conn.in_pubsub() {
                let mut hub_guard = hub.write().await;
                hub_guard.remove_client(conn.client_id);
                drop(hub_guard);
                conn.subscribed_channels.clear();
                conn.subscribed_patterns.clear();
            }
            cmd_reset(conn)
        }
        b"SELECT" => cmd_select(args, store).await,

        // Transactions
        b"MULTI" => cmd_multi(conn),
        b"EXEC" => return (cmd_exec(store, conn, hub).await, false),
        b"DISCARD" => cmd_discard(conn),
        b"WATCH" => cmd_watch(args, store, conn).await,
        b"UNWATCH" => cmd_unwatch(conn),

        // Pub-sub
        b"SUBSCRIBE" => cmd_subscribe(args, conn, hub).await,
        b"UNSUBSCRIBE" => cmd_unsubscribe(args, conn, hub).await,
        b"PSUBSCRIBE" => cmd_psubscribe(args, conn, hub).await,
        b"PUNSUBSCRIBE" => cmd_punsubscribe(args, conn, hub).await,
        b"PUBLISH" => cmd_publish(args, hub).await,

        // String
        b"SET" => cmd_set(args, store).await,
        b"GET" => cmd_get(args, store).await,
        b"MGET" => cmd_mget(args, store).await,
        b"MSET" => cmd_mset(args, store).await,
        b"MSETNX" => cmd_msetnx(args, store).await,
        b"SETNX" => cmd_setnx(args, store).await,
        b"GETSET" => cmd_getset(args, store).await,
        b"GETDEL" => cmd_getdel(args, store).await,
        b"GETEX" => cmd_getex(args, store).await,
        b"APPEND" => cmd_append(args, store).await,
        b"STRLEN" => cmd_strlen(args, store).await,
        b"INCR" => cmd_incr(args, store).await,
        b"INCRBY" => cmd_incrby(args, store).await,
        b"DECR" => cmd_decr(args, store).await,
        b"DECRBY" => cmd_decrby(args, store).await,
        b"INCRBYFLOAT" => cmd_incrbyfloat(args, store).await,
        b"SETRANGE" => cmd_setrange(args, store).await,
        b"GETRANGE" | b"SUBSTR" => cmd_getrange(args, store).await,

        // List
        b"LPUSH" => cmd_lpush(args, store).await,
        b"RPUSH" => cmd_rpush(args, store).await,
        b"LPUSHX" => cmd_lpushx(args, store).await,
        b"RPUSHX" => cmd_rpushx(args, store).await,
        b"LPOP" => cmd_lpop(args, store).await,
        b"RPOP" => cmd_rpop(args, store).await,
        b"LLEN" => cmd_llen(args, store).await,
        b"LRANGE" => cmd_lrange(args, store).await,
        b"LINDEX" => cmd_lindex(args, store).await,
        b"LSET" => cmd_lset(args, store).await,
        b"LREM" => cmd_lrem(args, store).await,
        b"LTRIM" => cmd_ltrim(args, store).await,
        b"LINSERT" => cmd_linsert(args, store).await,
        b"LPOS" => cmd_lpos(args, store).await,
        b"LMOVE" => cmd_lmove(args, store).await,

        // Hash
        b"HSET" => cmd_hset(args, store).await,
        b"HMSET" => {
            let r = cmd_hset(args, store).await;
            if r.starts_with(b":") { resp_ok() } else { r }
        }
        b"HGET" => cmd_hget(args, store).await,
        b"HDEL" => cmd_hdel(args, store).await,
        b"HEXISTS" => cmd_hexists(args, store).await,
        b"HGETALL" => cmd_hgetall(args, store, conn).await,
        b"HKEYS" => cmd_hkeys(args, store).await,
        b"HVALS" => cmd_hvals(args, store).await,
        b"HLEN" => cmd_hlen(args, store).await,
        b"HMGET" => cmd_hmget(args, store).await,
        b"HINCRBY" => cmd_hincrby(args, store).await,
        b"HINCRBYFLOAT" => cmd_hincrbyfloat(args, store).await,
        b"HRANDFIELD" => cmd_hrandfield(args, store).await,

        // Set
        b"SADD" => cmd_sadd(args, store).await,
        b"SREM" => cmd_srem(args, store).await,
        b"SMEMBERS" => cmd_smembers(args, store).await,
        b"SCARD" => cmd_scard(args, store).await,
        b"SISMEMBER" => cmd_sismember(args, store).await,
        b"SMISMEMBER" => cmd_smismember(args, store).await,
        b"SUNION" => cmd_sunion(args, store).await,
        b"SINTER" => cmd_sinter(args, store).await,
        b"SDIFF" => cmd_sdiff(args, store).await,
        b"SUNIONSTORE" => cmd_sunionstore(args, store).await,
        b"SINTERSTORE" => cmd_sinterstore(args, store).await,
        b"SDIFFSTORE" => cmd_sdiffstore(args, store).await,
        b"SMOVE" => cmd_smove(args, store).await,
        b"SPOP" => cmd_spop(args, store).await,
        b"SRANDMEMBER" => cmd_srandmember(args, store).await,

        // ZSet
        b"ZADD" => cmd_zadd(args, store).await,
        b"ZRANGE" => cmd_zrange(args, store).await,
        b"ZRANGEBYSCORE" => cmd_zrangebyscore(args, store).await,
        b"ZREVRANGEBYSCORE" => cmd_zrevrangebyscore(args, store).await,
        b"ZREVRANGE" => cmd_zrevrange(args, store).await,
        b"ZRANK" => cmd_zrank(args, store).await,
        b"ZREVRANK" => cmd_zrevrank(args, store).await,
        b"ZSCORE" => cmd_zscore(args, store).await,
        b"ZMSCORE" => cmd_zmscore(args, store).await,
        b"ZREM" => cmd_zrem(args, store).await,
        b"ZCARD" => cmd_zcard(args, store).await,
        b"ZCOUNT" => cmd_zcount(args, store).await,
        b"ZINCRBY" => cmd_zincrby(args, store).await,
        b"ZRANGEBYLEX" => cmd_zrangebylex(args, store).await,
        b"ZLEXCOUNT" => cmd_zlexcount(args, store).await,
        b"ZREMRANGEBYRANK" => cmd_zremrangebyrank(args, store).await,
        b"ZREMRANGEBYSCORE" => cmd_zremrangebyscore(args, store).await,
        b"ZREMRANGEBYLEX" => cmd_zremrangebylex(args, store).await,
        b"ZPOPMIN" => cmd_zpopmin(args, store).await,
        b"ZPOPMAX" => cmd_zpopmax(args, store).await,
        b"ZRANDMEMBER" => cmd_zrandmember(args, store).await,

        // Generic
        b"DEL" => cmd_del(args, store).await,
        b"UNLINK" => cmd_unlink(args, store).await,
        b"EXISTS" => cmd_exists(args, store).await,
        b"TYPE" => cmd_type(args, store).await,
        b"TTL" => cmd_ttl(args, store).await,
        b"PTTL" => cmd_pttl(args, store).await,
        b"EXPIRE" => cmd_expire(args, store).await,
        b"EXPIREAT" => cmd_expireat(args, store).await,
        b"PEXPIRE" => cmd_pexpire(args, store).await,
        b"PEXPIREAT" => cmd_pexpireat(args, store).await,
        b"PERSIST" => cmd_persist(args, store).await,
        b"EXPIRETIME" => cmd_expiretime(args, store).await,
        b"PEXPIRETIME" => cmd_pexpiretime(args, store).await,
        b"RENAME" => cmd_rename(args, store).await,
        b"RENAMENX" => cmd_renamenx(args, store).await,
        b"SCAN" => cmd_scan(args, store).await,
        b"KEYS" => cmd_keys(args, store).await,
        b"TOUCH" => cmd_touch(args, store).await,
        b"COPY" => cmd_copy(args, store).await,
        b"OBJECT" => cmd_object(args, store).await,

        // Server
        b"DBSIZE" => cmd_dbsize(args, store).await,
        b"FLUSHDB" => cmd_flushdb(args, store).await,
        b"FLUSHALL" => cmd_flushall(args, store).await,
        b"INFO" => cmd_info(args, store, conn).await,
        b"CONFIG" => cmd_config(args, store).await,
        b"COMMAND" => cmd_command(args, store).await,
        b"CLIENT" => cmd_client(args, store, conn).await,
        b"LATENCY" => cmd_latency(args, store).await,
        b"SLOWLOG" => cmd_slowlog(args, store).await,
        b"DEBUG" => cmd_debug(args, store).await,
        b"WAIT" => cmd_wait(args, store).await,
        b"XADD" => cmd_xadd(args, store).await,

        _ => std::borrow::Cow::Owned(
            format!("-ERR unknown command {}\r\n", String::from_utf8_lossy(&args[0])).into_bytes(),
        ),
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

    fn make_hub() -> crate::pubsub::PubSubHub {
        crate::pubsub::new_hub()
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
        let (resp, quit) = dispatch(&args(&["PING"]), &store, &mut conn, &make_hub()).await;
        assert_eq!(&*resp, b"+PONG\r\n");
        assert!(!quit);
    }

    #[tokio::test]
    async fn ping_case_insensitive() {
        let store = make_store();
        let mut conn = make_conn();
        let (resp, _) = dispatch(&args(&["ping"]), &store, &mut conn, &make_hub()).await;
        assert_eq!(&*resp, b"+PONG\r\n");
    }

    #[tokio::test]
    async fn quit_returns_ok_and_sets_quit_flag() {
        let store = make_store();
        let mut conn = make_conn();
        let (resp, quit) = dispatch(&args(&["QUIT"]), &store, &mut conn, &make_hub()).await;
        assert_eq!(&*resp, b"+OK\r\n");
        assert!(quit);
    }

    #[tokio::test]
    async fn unknown_command_returns_error() {
        let store = make_store();
        let mut conn = make_conn();
        let (resp, quit) = dispatch(&args(&["BLORP"]), &store, &mut conn, &make_hub()).await;
        assert!(resp.starts_with(b"-ERR unknown command BLORP"));
        assert!(!quit);
    }

    // ── SET ───────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn set_and_get_roundtrip() {
        let store = make_store();
        assert_eq!(
            &*(cmd_set(&args(&["SET", "k", "hello"]), &store).await),
            b"+OK\r\n"
        );
        assert_eq!(
            &*(cmd_get(&args(&["GET", "k"]), &store).await),
            b"$5\r\nhello\r\n"
        );
    }

    #[tokio::test]
    async fn set_overwrites_existing_key() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "first"]), &store).await;
        cmd_set(&args(&["SET", "k", "second"]), &store).await;
        assert_eq!(
            &*(cmd_get(&args(&["GET", "k"]), &store).await),
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
            &*(cmd_get(&args(&["GET", "missing"]), &store).await),
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
                .hits
                .load(std::sync::atomic::Ordering::Relaxed),
            3
        );
    }

    // ── DEL (single) ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn del_existing_key_returns_1() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v"]), &store).await;
        assert_eq!(&*(cmd_del(&args(&["DEL", "k"]), &store).await), b":1\r\n");
    }

    #[tokio::test]
    async fn del_removes_key() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v"]), &store).await;
        cmd_del(&args(&["DEL", "k"]), &store).await;
        assert_eq!(&*(cmd_get(&args(&["GET", "k"]), &store).await), b"$-1\r\n");
    }

    #[tokio::test]
    async fn del_missing_key_returns_0() {
        let store = make_store();
        assert_eq!(&*(cmd_del(&args(&["DEL", "nope"]), &store).await), b":0\r\n");
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
        assert_eq!(&*(cmd_ttl(&args(&["TTL", "nope"]), &store).await), b":-2\r\n");
    }

    #[tokio::test]
    async fn ttl_key_without_expiry_returns_minus1() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v"]), &store).await;
        assert_eq!(&*(cmd_ttl(&args(&["TTL", "k"]), &store).await), b":-1\r\n");
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
            &*(cmd_incr(&args(&["INCR", "counter"]), &store).await),
            b":1\r\n"
        );
    }

    #[tokio::test]
    async fn incr_increments_existing_value() {
        let store = make_store();
        cmd_set(&args(&["SET", "counter", "5"]), &store).await;
        assert_eq!(
            &*(cmd_incr(&args(&["INCR", "counter"]), &store).await),
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
            &*(cmd_get(&args(&["GET", "k"]), &store).await),
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
            &*(cmd_lpush(&args(&["LPUSH", "mylist", "a"]), &store).await),
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
        assert_eq!(&*resp, b"$1\r\nc\r\n");
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
        assert_eq!(&*resp, b"$6\r\nvalue1\r\n");
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
        assert_eq!(&*resp, b"$-1\r\n");
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
            &*(cmd_type(&args(&["TYPE", "s"]), &store).await),
            b"+string\r\n"
        );
        assert_eq!(&*(cmd_type(&args(&["TYPE", "l"]), &store).await), b"+list\r\n");
        assert_eq!(&*(cmd_type(&args(&["TYPE", "h"]), &store).await), b"+hash\r\n");
        assert_eq!(&*(cmd_type(&args(&["TYPE", "st"]), &store).await), b"+set\r\n");
        assert_eq!(&*(cmd_type(&args(&["TYPE", "z"]), &store).await), b"+zset\r\n");
        assert_eq!(
            &*(cmd_type(&args(&["TYPE", "missing"]), &store).await),
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
            &*(cmd_persist(&args(&["PERSIST", "k"]), &store).await),
            b":1\r\n"
        );

        tokio::time::sleep(Duration::from_millis(60)).await;

        assert_eq!(&*(cmd_get(&args(&["GET", "k"]), &store).await), b"$1\r\nv\r\n");
    }

    #[tokio::test]
    async fn stale_shorter_expiry_does_not_override_longer_one() {
        let store = make_store();
        cmd_set(&args(&["SET", "k", "v", "PX", "20"]), &store).await;
        assert_eq!(
            &*(cmd_pexpire(&args(&["PEXPIRE", "k", "120"]), &store).await),
            b":1\r\n"
        );

        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(&*(cmd_get(&args(&["GET", "k"]), &store).await), b"$1\r\nv\r\n");
    }

    // ── Namespace tests ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn namespaced_keys_are_isolated() {
        let store = make_store();
        cmd_set(&args(&["SET", "ns1/k", "v1"]), &store).await;
        cmd_set(&args(&["SET", "ns2/k", "v2"]), &store).await;
        assert_eq!(
            &*(cmd_get(&args(&["GET", "ns1/k"]), &store).await),
            b"$2\r\nv1\r\n"
        );
        assert_eq!(
            &*(cmd_get(&args(&["GET", "ns2/k"]), &store).await),
            b"$2\r\nv2\r\n"
        );
    }

    #[tokio::test]
    async fn namespaced_del_only_affects_its_namespace() {
        let store = make_store();
        cmd_set(&args(&["SET", "ns1/k", "v1"]), &store).await;
        cmd_set(&args(&["SET", "ns2/k", "v2"]), &store).await;
        cmd_del(&args(&["DEL", "ns1/k"]), &store).await;
        assert_eq!(&*(cmd_get(&args(&["GET", "ns1/k"]), &store).await), b"$-1\r\n");
        assert_eq!(
            &*(cmd_get(&args(&["GET", "ns2/k"]), &store).await),
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
            &*(cmd_incr(&args(&["INCR", "ns1/counter"]), &store).await),
            b":3\r\n"
        );
        assert_eq!(
            &*(cmd_incr(&args(&["INCR", "ns2/counter"]), &store).await),
            b":2\r\n"
        );
    }

    #[tokio::test]
    async fn namespaced_lpush_lists_are_isolated() {
        let store = make_store();
        cmd_lpush(&args(&["LPUSH", "ns1/list", "a"]), &store).await;
        cmd_lpush(&args(&["LPUSH", "ns2/list", "b"]), &store).await;
        assert_eq!(
            &*(cmd_lpush(&args(&["LPUSH", "ns1/list", "c"]), &store).await),
            b":2\r\n"
        );
        assert_eq!(
            &*(cmd_lpush(&args(&["LPUSH", "ns2/list", "d"]), &store).await),
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
        assert_eq!(&*(cmd_ttl(&args(&["TTL", "ns2/k"]), &store).await), b":-1\r\n");
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
        assert_eq!(&*(cmd_keys(&args(&["KEYS", "*"]), &store).await), b"*0\r\n");
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
        assert_eq!(&*(cmd_keys(&args(&["KEYS", "z*"]), &store).await), b"*0\r\n");
    }

    // Degenerate glob pattern that was exponential in the recursive implementation.
    // Verifies the iterative O(N·M) implementation completes promptly.
    #[test]
    fn glob_match_degenerate_star_pattern_is_linear() {
        // Pattern: *a*a*a*a*a against a string of 30 'b's — no match, worst case for backtracking.
        let pattern = b"*a*a*a*a*a";
        let text = b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        assert!(!glob_match(pattern, text));
    }

    #[test]
    fn glob_match_star_matches_empty() {
        assert!(glob_match(b"*", b""));
        assert!(glob_match(b"a*", b"a"));
        assert!(glob_match(b"*b", b"b"));
    }

    #[test]
    fn glob_match_multiple_stars() {
        assert!(glob_match(b"a*b*c", b"aXbYc"));
        assert!(glob_match(b"a*b*c", b"abc"));
        assert!(glob_match(b"a*b*c", b"abXc")); // * matches empty, b matches b, * matches X
        assert!(!glob_match(b"a*b*c", b"aXYZ")); // no 'c' at end
    }

    #[test]
    fn glob_match_class_negation() {
        assert!(glob_match(b"h[^e]llo", b"hallo"));
        assert!(!glob_match(b"h[^e]llo", b"hello"));
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
                        .get::<str>(ns.as_ref())
                        .and_then(|m| m.get::<str>(key.as_ref()))
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

    // ── MULTI / EXEC / DISCARD ────────────────────────────────────────────────

    fn make_conn_with_pubsub() -> ConnState {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let mut conn = ConnState::new(0);
        conn.pubsub_tx = Some(tx);
        conn
    }

    #[tokio::test]
    async fn multi_exec_empty_transaction() {
        let store = make_store();
        let hub = make_hub();
        let mut conn = make_conn();
        let (r, _) = dispatch(&args(&["MULTI"]), &store, &mut conn, &hub).await;
        assert_eq!(&*r, b"+OK\r\n");
        let (r, _) = dispatch(&args(&["EXEC"]), &store, &mut conn, &hub).await;
        assert_eq!(&*r, b"*0\r\n");
    }

    #[tokio::test]
    async fn multi_exec_queues_and_executes() {
        let store = make_store();
        let hub = make_hub();
        let mut conn = make_conn();
        dispatch(&args(&["MULTI"]), &store, &mut conn, &hub).await;
        let (r, _) = dispatch(&args(&["SET", "k", "v"]), &store, &mut conn, &hub).await;
        assert_eq!(&*r, b"+QUEUED\r\n");
        let (r, _) = dispatch(&args(&["GET", "k"]), &store, &mut conn, &hub).await;
        assert_eq!(&*r, b"+QUEUED\r\n");
        let (resp, _) = dispatch(&args(&["EXEC"]), &store, &mut conn, &hub).await;
        // Should be *2 array containing +OK and $1\r\nv
        assert!(resp.starts_with(b"*2\r\n"));
        assert!(resp.windows(4).any(|w| w == b"+OK\r"));
        assert!(resp.windows(2).any(|w| w == b"$1"));
    }

    #[tokio::test]
    async fn multi_nested_returns_error() {
        let store = make_store();
        let hub = make_hub();
        let mut conn = make_conn();
        dispatch(&args(&["MULTI"]), &store, &mut conn, &hub).await;
        let (r, _) = dispatch(&args(&["MULTI"]), &store, &mut conn, &hub).await;
        assert!(r.starts_with(b"-ERR MULTI calls can not be nested"));
    }

    #[tokio::test]
    async fn discard_clears_queue() {
        let store = make_store();
        let hub = make_hub();
        let mut conn = make_conn();
        dispatch(&args(&["MULTI"]), &store, &mut conn, &hub).await;
        dispatch(&args(&["SET", "k", "v"]), &store, &mut conn, &hub).await;
        let (r, _) = dispatch(&args(&["DISCARD"]), &store, &mut conn, &hub).await;
        assert_eq!(&*r, b"+OK\r\n");
        assert!(conn.multi_state.is_none());
        // Key should not exist since transaction was discarded.
        let (r, _) = dispatch(&args(&["GET", "k"]), &store, &mut conn, &hub).await;
        assert_eq!(&*r, b"$-1\r\n");
    }

    #[tokio::test]
    async fn exec_without_multi_returns_error() {
        let store = make_store();
        let hub = make_hub();
        let mut conn = make_conn();
        let (r, _) = dispatch(&args(&["EXEC"]), &store, &mut conn, &hub).await;
        assert!(r.starts_with(b"-ERR EXEC without MULTI"));
    }

    #[tokio::test]
    async fn discard_without_multi_returns_error() {
        let store = make_store();
        let hub = make_hub();
        let mut conn = make_conn();
        let (r, _) = dispatch(&args(&["DISCARD"]), &store, &mut conn, &hub).await;
        assert!(r.starts_with(b"-ERR DISCARD without MULTI"));
    }

    // ── WATCH / UNWATCH ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn watch_exec_succeeds_when_key_unchanged() {
        let store = make_store();
        let hub = make_hub();
        let mut conn = make_conn();
        // Set key first.
        cmd_set(&args(&["SET", "k", "1"]), &store).await;
        // WATCH then MULTI+EXEC: no one else changed k.
        dispatch(&args(&["WATCH", "k"]), &store, &mut conn, &hub).await;
        dispatch(&args(&["MULTI"]), &store, &mut conn, &hub).await;
        dispatch(&args(&["GET", "k"]), &store, &mut conn, &hub).await;
        let (resp, _) = dispatch(&args(&["EXEC"]), &store, &mut conn, &hub).await;
        // Should return array (not nil).
        assert!(resp.starts_with(b"*1\r\n"));
    }

    #[tokio::test]
    async fn watch_exec_returns_nil_when_key_modified() {
        let store = make_store();
        let hub = make_hub();
        let mut conn = make_conn();
        cmd_set(&args(&["SET", "k", "1"]), &store).await;
        dispatch(&args(&["WATCH", "k"]), &store, &mut conn, &hub).await;
        // Simulate another client modifying k.
        cmd_set(&args(&["SET", "k", "2"]), &store).await;
        dispatch(&args(&["MULTI"]), &store, &mut conn, &hub).await;
        dispatch(&args(&["GET", "k"]), &store, &mut conn, &hub).await;
        let (resp, _) = dispatch(&args(&["EXEC"]), &store, &mut conn, &hub).await;
        // Optimistic lock failure → null array.
        assert_eq!(&*resp, b"*-1\r\n");
    }

    #[tokio::test]
    async fn watch_inside_multi_returns_error() {
        let store = make_store();
        let hub = make_hub();
        let mut conn = make_conn();
        dispatch(&args(&["MULTI"]), &store, &mut conn, &hub).await;
        let (r, _) = dispatch(&args(&["WATCH", "k"]), &store, &mut conn, &hub).await;
        assert!(r.starts_with(b"-ERR WATCH inside MULTI is not allowed"));
    }

    #[tokio::test]
    async fn unwatch_clears_watched_keys() {
        let store = make_store();
        let hub = make_hub();
        let mut conn = make_conn();
        dispatch(&args(&["WATCH", "k"]), &store, &mut conn, &hub).await;
        assert!(!conn.watched.is_empty());
        dispatch(&args(&["UNWATCH"]), &store, &mut conn, &hub).await;
        assert!(conn.watched.is_empty());
    }

    // ── PUBLISH / SUBSCRIBE ───────────────────────────────────────────────────

    #[tokio::test]
    async fn publish_with_no_subscribers_returns_zero() {
        let store = make_store();
        let hub = make_hub();
        let mut conn = make_conn();
        let (r, _) = dispatch(&args(&["PUBLISH", "chan", "hello"]), &store, &mut conn, &hub).await;
        assert_eq!(&*r, b":0\r\n");
    }

    #[tokio::test]
    async fn subscribe_reply_format() {
        let store = make_store();
        let hub = make_hub();
        let mut conn = make_conn_with_pubsub();
        let (r, _) = dispatch(&args(&["SUBSCRIBE", "news"]), &store, &mut conn, &hub).await;
        // *3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n
        assert!(r.starts_with(b"*3\r\n"));
        assert!(r.windows(9).any(|w| w == b"subscribe"));
        assert!(conn.subscribed_channels.contains("news"));
    }

    #[tokio::test]
    async fn publish_delivers_to_subscriber() {
        let hub = make_hub();
        let store = make_store();

        // Subscriber connection.
        let (sub_tx, mut sub_rx) = tokio::sync::mpsc::unbounded_channel::<PubSubMessage>();
        let mut sub_conn = ConnState::new(1);
        sub_conn.pubsub_tx = Some(sub_tx);

        // Publisher connection (no pubsub channel needed).
        let mut pub_conn = make_conn();

        dispatch(&args(&["SUBSCRIBE", "events"]), &store, &mut sub_conn, &hub).await;

        let (r, _) =
            dispatch(&args(&["PUBLISH", "events", "ping"]), &store, &mut pub_conn, &hub).await;
        assert_eq!(&*r, b":1\r\n"); // 1 subscriber received it

        let msg = sub_rx.try_recv().expect("message should be in channel");
        match msg {
            PubSubMessage::Message { channel, data } => {
                assert_eq!(channel, "events");
                assert_eq!(&data[..], b"ping");
            }
            other => panic!("unexpected message type: {other:?}"),
        }
    }

    #[tokio::test]
    async fn psubscribe_matches_pattern() {
        let hub = make_hub();
        let store = make_store();

        let (sub_tx, mut sub_rx) = tokio::sync::mpsc::unbounded_channel::<PubSubMessage>();
        let mut sub_conn = ConnState::new(2);
        sub_conn.pubsub_tx = Some(sub_tx);

        let mut pub_conn = make_conn();

        dispatch(&args(&["PSUBSCRIBE", "news.*"]), &store, &mut sub_conn, &hub).await;
        assert!(sub_conn.subscribed_patterns.contains("news.*"));

        dispatch(
            &args(&["PUBLISH", "news.sports", "goal"]),
            &store,
            &mut pub_conn,
            &hub,
        )
        .await;

        let msg = sub_rx.try_recv().expect("pmessage should be in channel");
        match msg {
            PubSubMessage::PMessage { pattern, channel, data } => {
                assert_eq!(pattern, "news.*");
                assert_eq!(channel, "news.sports");
                assert_eq!(&data[..], b"goal");
            }
            other => panic!("unexpected message type: {other:?}"),
        }
    }

    #[tokio::test]
    async fn command_rejected_in_pubsub_mode() {
        let store = make_store();
        let hub = make_hub();
        let mut conn = make_conn_with_pubsub();
        dispatch(&args(&["SUBSCRIBE", "ch"]), &store, &mut conn, &hub).await;
        let (r, _) = dispatch(&args(&["SET", "k", "v"]), &store, &mut conn, &hub).await;
        assert!(r.starts_with(b"-ERR Command not allowed in pub/sub mode"));
    }

    // ── HDEL ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn hdel_returns_count_of_deleted_fields() {
        let store = make_store();
        cmd_hset(&args(&["HSET", "h", "f1", "v1", "f2", "v2", "f3", "v3"]), &store).await;
        // delete two existing fields and one missing — should return 2
        let resp = cmd_hdel(&args(&["HDEL", "h", "f1", "f3", "nope"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 2);
        // f2 should still be present
        let resp = cmd_hget(&args(&["HGET", "h", "f2"]), &store).await;
        assert_eq!(&*resp, b"$2\r\nv2\r\n");
    }

    #[tokio::test]
    async fn hdel_missing_key_returns_0() {
        let store = make_store();
        let resp = cmd_hdel(&args(&["HDEL", "nokey", "f"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 0);
    }

    // ── HEXISTS ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn hexists_returns_1_for_existing_field() {
        let store = make_store();
        cmd_hset(&args(&["HSET", "h", "field", "val"]), &store).await;
        let resp = cmd_hexists(&args(&["HEXISTS", "h", "field"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 1);
    }

    #[tokio::test]
    async fn hexists_returns_0_for_missing_field_and_missing_key() {
        let store = make_store();
        cmd_hset(&args(&["HSET", "h", "field", "val"]), &store).await;
        // existing hash, missing field
        let resp = cmd_hexists(&args(&["HEXISTS", "h", "nope"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 0);
        // missing key entirely
        let resp = cmd_hexists(&args(&["HEXISTS", "nokey", "f"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 0);
    }

    // ── HKEYS ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn hkeys_returns_all_field_names() {
        let store = make_store();
        cmd_hset(&args(&["HSET", "h", "alpha", "1", "beta", "2", "gamma", "3"]), &store).await;
        let resp = cmd_hkeys(&args(&["HKEYS", "h"]), &store).await;
        let keys = parse_keys_resp(&resp);
        assert_eq!(keys, vec!["alpha", "beta", "gamma"]);
    }

    #[tokio::test]
    async fn hkeys_missing_key_returns_empty_array() {
        let store = make_store();
        let resp = cmd_hkeys(&args(&["HKEYS", "nokey"]), &store).await;
        assert_eq!(&*resp, b"*0\r\n");
    }

    // ── HVALS ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn hvals_returns_all_values() {
        let store = make_store();
        cmd_hset(&args(&["HSET", "h", "f1", "apple", "f2", "banana"]), &store).await;
        let resp = cmd_hvals(&args(&["HVALS", "h"]), &store).await;
        let vals = parse_keys_resp(&resp);
        assert_eq!(vals, vec!["apple", "banana"]);
    }

    #[tokio::test]
    async fn hvals_missing_key_returns_empty_array() {
        let store = make_store();
        let resp = cmd_hvals(&args(&["HVALS", "nokey"]), &store).await;
        assert_eq!(&*resp, b"*0\r\n");
    }

    // ── HLEN ──────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn hlen_returns_number_of_fields() {
        let store = make_store();
        cmd_hset(&args(&["HSET", "h", "a", "1", "b", "2", "c", "3"]), &store).await;
        let resp = cmd_hlen(&args(&["HLEN", "h"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 3);
    }

    #[tokio::test]
    async fn hlen_missing_key_returns_0() {
        let store = make_store();
        let resp = cmd_hlen(&args(&["HLEN", "nokey"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 0);
    }

    // ── HMGET ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn hmget_returns_values_with_nulls_for_missing() {
        let store = make_store();
        cmd_hset(&args(&["HSET", "h", "f1", "val1", "f2", "val2"]), &store).await;
        let resp = cmd_hmget(&args(&["HMGET", "h", "f1", "missing", "f2"]), &store).await;
        // expect *3 array: val1, null bulk string, val2
        assert!(resp.starts_with(b"*3\r\n"));
        let s = std::str::from_utf8(&resp).unwrap();
        assert!(s.contains("val1"));
        assert!(s.contains("$-1\r\n"), "expected null bulk string for missing field");
        assert!(s.contains("val2"));
    }

    #[tokio::test]
    async fn hmget_missing_key_returns_all_nulls() {
        let store = make_store();
        let resp = cmd_hmget(&args(&["HMGET", "nokey", "f1", "f2"]), &store).await;
        // *2 array of two null bulk strings
        assert!(resp.starts_with(b"*2\r\n"));
        // both entries should be $-1\r\n
        let s = std::str::from_utf8(&resp).unwrap();
        assert_eq!(s.matches("$-1\r\n").count(), 2);
    }

    // ── HINCRBY ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn hincrby_creates_field_and_increments() {
        let store = make_store();
        // field does not exist — should be created with value = increment
        let resp = cmd_hincrby(&args(&["HINCRBY", "h", "counter", "5"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 5);
        // increment again
        let resp = cmd_hincrby(&args(&["HINCRBY", "h", "counter", "3"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 8);
    }

    #[tokio::test]
    async fn hincrby_negative_increment() {
        let store = make_store();
        cmd_hset(&args(&["HSET", "h", "n", "10"]), &store).await;
        let resp = cmd_hincrby(&args(&["HINCRBY", "h", "n", "-4"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 6);
    }

    // ── HINCRBYFLOAT ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn hincrbyfloat_creates_field_and_increments() {
        let store = make_store();
        // field absent — created with value = increment
        let resp = cmd_hincrbyfloat(&args(&["HINCRBYFLOAT", "h", "score", "1.5"]), &store).await;
        let val: f64 = std::str::from_utf8(&resp[1..resp.len() - 2])
            .unwrap()
            .split("\r\n")
            .nth(1)
            .unwrap_or_else(|| {
                // resp_bulk format: $N\r\nDATA\r\n — extract DATA portion
                std::str::from_utf8(&resp).unwrap().split("\r\n").nth(1).unwrap()
            })
            .parse()
            .unwrap_or_else(|_| {
                // simpler parse: strip RESP bulk framing
                let s = std::str::from_utf8(&resp).unwrap();
                let data = s.split("\r\n").nth(1).unwrap();
                data.parse().unwrap()
            });
        assert!((val - 1.5).abs() < 1e-9, "expected 1.5, got {val}");
    }

    #[tokio::test]
    async fn hincrbyfloat_accumulates_on_existing_field() {
        let store = make_store();
        cmd_hset(&args(&["HSET", "h", "rate", "2.5"]), &store).await;
        let resp = cmd_hincrbyfloat(&args(&["HINCRBYFLOAT", "h", "rate", "0.5"]), &store).await;
        // result is a bulk string containing the new float value
        let s = std::str::from_utf8(&resp).unwrap();
        let data = s.split("\r\n").nth(1).unwrap();
        let val: f64 = data.parse().unwrap();
        assert!((val - 3.0).abs() < 1e-9, "expected 3.0, got {val}");
    }

    // ── GETSET ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn getset_absent_key_returns_null_and_stores_value() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        let (resp, _) = dispatch(&args(&["GETSET", "k", "hello"]), &store, &mut conn, &hub).await;
        assert_eq!(&*resp, b"$-1\r\n");
        assert_eq!(
            &*(cmd_get(&args(&["GET", "k"]), &store).await),
            b"$5\r\nhello\r\n"
        );
    }

    #[tokio::test]
    async fn getset_existing_key_returns_old_value_and_stores_new() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "k", "old"]), &store).await;
        let (resp, _) = dispatch(&args(&["GETSET", "k", "new"]), &store, &mut conn, &hub).await;
        assert_eq!(&*resp, b"$3\r\nold\r\n");
        assert_eq!(
            &*(cmd_get(&args(&["GET", "k"]), &store).await),
            b"$3\r\nnew\r\n"
        );
    }

    // ── GETDEL ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn getdel_existing_key_returns_value_and_removes_key() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "k", "hello"]), &store).await;
        let (resp, _) = dispatch(&args(&["GETDEL", "k"]), &store, &mut conn, &hub).await;
        assert_eq!(&*resp, b"$5\r\nhello\r\n");
        assert_eq!(&*(cmd_get(&args(&["GET", "k"]), &store).await), b"$-1\r\n");
    }

    #[tokio::test]
    async fn getdel_absent_key_returns_null() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        let (resp, _) = dispatch(&args(&["GETDEL", "missing"]), &store, &mut conn, &hub).await;
        assert_eq!(&*resp, b"$-1\r\n");
    }

    // ── GETEX ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn getex_without_options_returns_value_and_leaves_ttl_unchanged() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "k", "v", "EX", "100"]), &store).await;
        let (resp, _) = dispatch(&args(&["GETEX", "k"]), &store, &mut conn, &hub).await;
        assert_eq!(&*resp, b"$1\r\nv\r\n");
        let ttl = parse_int_resp(&cmd_ttl(&args(&["TTL", "k"]), &store).await);
        assert!(ttl > 90 && ttl <= 100, "TTL should be unchanged: {ttl}");
    }

    #[tokio::test]
    async fn getex_with_ex_option_sets_ttl() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "k", "v"]), &store).await;
        // Confirm no TTL initially.
        assert_eq!(&*(cmd_ttl(&args(&["TTL", "k"]), &store).await), b":-1\r\n");
        let (resp, _) =
            dispatch(&args(&["GETEX", "k", "EX", "60"]), &store, &mut conn, &hub).await;
        assert_eq!(&*resp, b"$1\r\nv\r\n");
        let ttl = parse_int_resp(&cmd_ttl(&args(&["TTL", "k"]), &store).await);
        assert!(ttl > 50 && ttl <= 60, "expected TTL ~60, got {ttl}");
    }

    #[tokio::test]
    async fn getex_with_persist_removes_ttl() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "k", "v", "EX", "100"]), &store).await;
        let (resp, _) =
            dispatch(&args(&["GETEX", "k", "PERSIST"]), &store, &mut conn, &hub).await;
        assert_eq!(&*resp, b"$1\r\nv\r\n");
        assert_eq!(&*(cmd_ttl(&args(&["TTL", "k"]), &store).await), b":-1\r\n");
    }

    // ── DECR ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn decr_absent_key_creates_with_minus1() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        let (resp, _) = dispatch(&args(&["DECR", "counter"]), &store, &mut conn, &hub).await;
        assert_eq!(&*resp, b":-1\r\n");
    }

    #[tokio::test]
    async fn decr_existing_value_decrements_by_1() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "counter", "10"]), &store).await;
        let (resp, _) = dispatch(&args(&["DECR", "counter"]), &store, &mut conn, &hub).await;
        assert_eq!(&*resp, b":9\r\n");
    }

    // ── DECRBY ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn decrby_decrements_by_given_amount() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "k", "20"]), &store).await;
        let (resp, _) = dispatch(&args(&["DECRBY", "k", "7"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&resp), 13);
    }

    #[tokio::test]
    async fn decrby_absent_key_creates_with_negative_amount() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        let (resp, _) = dispatch(&args(&["DECRBY", "k", "5"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&resp), -5);
    }

    // ── INCRBYFLOAT ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn incrbyfloat_absent_key_creates_with_amount() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        let (resp, _) =
            dispatch(&args(&["INCRBYFLOAT", "f", "3.14"]), &store, &mut conn, &hub).await;
        // Response is a bulk string containing the new value.
        assert!(
            resp.starts_with(b"$"),
            "expected bulk string, got {:?}",
            std::str::from_utf8(&resp)
        );
        assert!(resp.windows(4).any(|w| w == b"3.14"), "expected 3.14 in response");
    }

    #[tokio::test]
    async fn incrbyfloat_existing_value_adds_amount() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "f", "10.5"]), &store).await;
        let (resp, _) =
            dispatch(&args(&["INCRBYFLOAT", "f", "0.1"]), &store, &mut conn, &hub).await;
        assert!(resp.starts_with(b"$"), "expected bulk string");
        let raw = std::str::from_utf8(&resp).unwrap();
        // Bulk string format: $len\r\n<data>\r\n — data is the second field.
        let value: f64 = raw
            .split("\r\n")
            .nth(1)
            .unwrap()
            .parse()
            .expect("response body should be a float");
        assert!((value - 10.6).abs() < 1e-9, "expected ~10.6, got {value}");
    }

    // ── SETRANGE ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn setrange_overwrites_bytes_at_offset() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "k", "Hello World"]), &store).await;
        let (resp, _) =
            dispatch(&args(&["SETRANGE", "k", "6", "Redis"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&resp), 11);
        assert_eq!(
            &*(cmd_get(&args(&["GET", "k"]), &store).await),
            b"$11\r\nHello Redis\r\n"
        );
    }

    #[tokio::test]
    async fn setrange_zero_pads_when_offset_exceeds_length() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        // Key does not exist; writing "hi" at offset 5 should produce 7 bytes total.
        let (resp, _) =
            dispatch(&args(&["SETRANGE", "k", "5", "hi"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&resp), 7);
        let got = cmd_get(&args(&["GET", "k"]), &store).await;
        // Encoded as $7\r\n<5 zero bytes>hi\r\n
        assert_eq!(&got[..4], b"$7\r\n");
        assert_eq!(&got[4..9], b"\x00\x00\x00\x00\x00");
        assert_eq!(&got[9..13], b"hi\r\n");
    }

    // ── GETRANGE ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn getrange_returns_substring() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "k", "Hello, World!"]), &store).await;
        let (resp, _) =
            dispatch(&args(&["GETRANGE", "k", "0", "4"]), &store, &mut conn, &hub).await;
        assert_eq!(&*resp, b"$5\r\nHello\r\n");
    }

    #[tokio::test]
    async fn getrange_negative_indices_count_from_end() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "k", "Hello"]), &store).await;
        // -3 to -1 = last three characters = "llo"
        let (resp, _) =
            dispatch(&args(&["GETRANGE", "k", "-3", "-1"]), &store, &mut conn, &hub).await;
        assert_eq!(&*resp, b"$3\r\nllo\r\n");
    }

    // ── MSETNX ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn msetnx_sets_all_keys_when_none_exist() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        let (resp, _) = dispatch(
            &args(&["MSETNX", "k1", "v1", "k2", "v2"]),
            &store,
            &mut conn,
            &hub,
        )
        .await;
        assert_eq!(&*resp, b":1\r\n");
        assert_eq!(
            &*(cmd_get(&args(&["GET", "k1"]), &store).await),
            b"$2\r\nv1\r\n"
        );
        assert_eq!(
            &*(cmd_get(&args(&["GET", "k2"]), &store).await),
            b"$2\r\nv2\r\n"
        );
    }

    #[tokio::test]
    async fn msetnx_does_nothing_when_any_key_exists() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "k1", "existing"]), &store).await;
        let (resp, _) = dispatch(
            &args(&["MSETNX", "k1", "new1", "k2", "new2"]),
            &store,
            &mut conn,
            &hub,
        )
        .await;
        assert_eq!(&*resp, b":0\r\n");
        // k1 must be untouched; k2 must not have been created.
        assert_eq!(
            &*(cmd_get(&args(&["GET", "k1"]), &store).await),
            b"$8\r\nexisting\r\n"
        );
        assert_eq!(&*(cmd_get(&args(&["GET", "k2"]), &store).await), b"$-1\r\n");
    }

    // ── ZREM ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn zrem_removes_members_and_returns_count() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        dispatch(&args(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"]), &store, &mut conn, &hub).await;
        let resp = dispatch(&args(&["ZREM", "zs", "a", "c", "missing"]), &store, &mut conn, &hub).await;
        // removes "a" and "c"; "missing" is a no-op => count 2
        assert_eq!(parse_int_resp(&resp.0), 2);
        // confirm "b" is still present and cardinality is 1
        let card = dispatch(&args(&["ZCARD", "zs"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&card.0), 1);
    }

    // ── ZCARD ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn zcard_returns_cardinality() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        dispatch(&args(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"]), &store, &mut conn, &hub).await;
        let resp = dispatch(&args(&["ZCARD", "zs"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&resp.0), 3);
    }

    // ── ZCOUNT ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn zcount_returns_members_in_score_range() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        dispatch(&args(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"]), &store, &mut conn, &hub).await;
        // inclusive range 1..=2 => "a" and "b"
        let resp = dispatch(&args(&["ZCOUNT", "zs", "1", "2"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&resp.0), 2);
        // exclusive lower bound (1,3] => only "b" and "c"
        let resp2 = dispatch(&args(&["ZCOUNT", "zs", "(1", "3"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&resp2.0), 2);
        // -inf to +inf => all 3
        let resp3 = dispatch(&args(&["ZCOUNT", "zs", "-inf", "+inf"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&resp3.0), 3);
    }

    // ── ZINCRBY ──────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn zincrby_increments_score_and_returns_new_value() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        dispatch(&args(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"]), &store, &mut conn, &hub).await;
        let resp = dispatch(&args(&["ZINCRBY", "zs", "2", "b"]), &store, &mut conn, &hub).await;
        // "b" had score 2, +2 => 4; returned as bulk string
        assert!(resp.0.starts_with(b"$"), "expected bulk string, got {:?}", std::str::from_utf8(&resp.0));
        let s = std::str::from_utf8(&resp.0).unwrap();
        let score_str = s.split("\r\n").nth(1).unwrap();
        let score: f64 = score_str.parse().unwrap();
        assert!((score - 4.0).abs() < 1e-9, "expected 4.0, got {score}");
    }

    // ── ZRANGEBYSCORE ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn zrangebyscore_returns_members_in_score_range() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        dispatch(&args(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"]), &store, &mut conn, &hub).await;
        let resp = dispatch(&args(&["ZRANGEBYSCORE", "zs", "1", "2"]), &store, &mut conn, &hub).await;
        // expect array ["a", "b"]
        assert!(resp.0.starts_with(b"*2\r\n"), "expected *2 array, got {:?}", std::str::from_utf8(&resp.0));
        assert!(resp.0.windows(b"$1\r\na\r\n".len()).any(|w| w == b"$1\r\na\r\n"), "missing 'a'");
        assert!(resp.0.windows(b"$1\r\nb\r\n".len()).any(|w| w == b"$1\r\nb\r\n"), "missing 'b'");
        // WITHSCORES variant: 3 members x 2 entries each = 6 elements
        let resp2 = dispatch(&args(&["ZRANGEBYSCORE", "zs", "-inf", "+inf", "WITHSCORES"]), &store, &mut conn, &hub).await;
        assert!(resp2.0.starts_with(b"*6\r\n"), "expected *6 with scores, got {:?}", std::str::from_utf8(&resp2.0));
    }

    // ── ZREVRANGEBYSCORE ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn zrevrangebyscore_returns_members_in_reverse_score_order() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        dispatch(&args(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"]), &store, &mut conn, &hub).await;
        // ZREVRANGEBYSCORE key max min
        let resp = dispatch(&args(&["ZREVRANGEBYSCORE", "zs", "3", "1"]), &store, &mut conn, &hub).await;
        // expect *3 array; verify "c" appears before "a" in raw bytes
        assert!(resp.0.starts_with(b"*3\r\n"), "expected *3 array, got {:?}", std::str::from_utf8(&resp.0));
        let raw = std::str::from_utf8(&resp.0).unwrap();
        let pos_c = raw.find("\r\nc\r\n").unwrap_or(usize::MAX);
        let pos_a = raw.find("\r\na\r\n").unwrap_or(usize::MAX);
        assert!(pos_c < pos_a, "expected c before a (high to low order)");
    }

    // ── ZREVRANGE ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn zrevrange_returns_range_in_reverse_order() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        dispatch(&args(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"]), &store, &mut conn, &hub).await;
        // ZREVRANGE key 0 -1 => all members, highest score first
        let resp = dispatch(&args(&["ZREVRANGE", "zs", "0", "-1"]), &store, &mut conn, &hub).await;
        assert!(resp.0.starts_with(b"*3\r\n"), "expected *3 array");
        let raw = std::str::from_utf8(&resp.0).unwrap();
        let pos_c = raw.find("\r\nc\r\n").unwrap_or(usize::MAX);
        let pos_a = raw.find("\r\na\r\n").unwrap_or(usize::MAX);
        assert!(pos_c < pos_a, "expected c before a in reverse range");
    }

    // ── ZRANK ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn zrank_returns_zero_based_rank() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        dispatch(&args(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"]), &store, &mut conn, &hub).await;
        // "a" has score 1 => rank 0 (lowest)
        let resp = dispatch(&args(&["ZRANK", "zs", "a"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&resp.0), 0);
        // "c" has score 3 => rank 2 (highest)
        let resp2 = dispatch(&args(&["ZRANK", "zs", "c"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&resp2.0), 2);
        // missing member => null
        let resp3 = dispatch(&args(&["ZRANK", "zs", "missing"]), &store, &mut conn, &hub).await;
        assert_eq!(&*resp3.0, b"$-1\r\n");
    }

    // ── ZREVRANK ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn zrevrank_returns_reverse_rank() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        dispatch(&args(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"]), &store, &mut conn, &hub).await;
        // "c" has score 3 => revrank 0 (highest score = rank 0 in reverse)
        let resp = dispatch(&args(&["ZREVRANK", "zs", "c"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&resp.0), 0);
        // "a" has score 1 => revrank 2
        let resp2 = dispatch(&args(&["ZREVRANK", "zs", "a"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&resp2.0), 2);
        // missing member => null
        let resp3 = dispatch(&args(&["ZREVRANK", "zs", "missing"]), &store, &mut conn, &hub).await;
        assert_eq!(&*resp3.0, b"$-1\r\n");
    }

    // ── ZSCORE ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn zscore_returns_score_as_bulk_string() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        dispatch(&args(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"]), &store, &mut conn, &hub).await;
        let resp = dispatch(&args(&["ZSCORE", "zs", "b"]), &store, &mut conn, &hub).await;
        // score 2 => bulk string "2"
        assert_eq!(&*resp.0, b"$1\r\n2\r\n");
        // missing member => null
        let resp2 = dispatch(&args(&["ZSCORE", "zs", "missing"]), &store, &mut conn, &hub).await;
        assert_eq!(&*resp2.0, b"$-1\r\n");
    }

    // ── ZMSCORE ──────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn zmscore_returns_array_of_scores_with_nulls_for_missing() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        dispatch(&args(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"]), &store, &mut conn, &hub).await;
        let resp = dispatch(&args(&["ZMSCORE", "zs", "a", "missing", "c"]), &store, &mut conn, &hub).await;
        // expect *3 array: "1", null, "3"
        assert!(resp.0.starts_with(b"*3\r\n"), "expected *3 array, got {:?}", std::str::from_utf8(&resp.0));
        assert!(resp.0.windows(b"$1\r\n1\r\n".len()).any(|w| w == b"$1\r\n1\r\n"), "missing score for 'a'");
        assert!(resp.0.windows(b"$-1\r\n".len()).any(|w| w == b"$-1\r\n"), "missing null for missing member");
        assert!(resp.0.windows(b"$1\r\n3\r\n".len()).any(|w| w == b"$1\r\n3\r\n"), "missing score for 'c'");
    }

    // ── ZRANGEBYLEX ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn zrangebylex_returns_members_in_lex_range() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        // All members must have the same score for lex ordering to be meaningful
        dispatch(&args(&["ZADD", "zs", "0", "a", "0", "b", "0", "c", "0", "d"]), &store, &mut conn, &hub).await;
        // [a, (c] => "a", "b" (inclusive a, exclusive c)
        let resp = dispatch(&args(&["ZRANGEBYLEX", "zs", "[a", "(c"]), &store, &mut conn, &hub).await;
        assert!(resp.0.starts_with(b"*2\r\n"), "expected *2 array, got {:?}", std::str::from_utf8(&resp.0));
        assert!(resp.0.windows(b"$1\r\na\r\n".len()).any(|w| w == b"$1\r\na\r\n"), "missing 'a'");
        assert!(resp.0.windows(b"$1\r\nb\r\n".len()).any(|w| w == b"$1\r\nb\r\n"), "missing 'b'");
        // - to + => all 4 members
        let resp2 = dispatch(&args(&["ZRANGEBYLEX", "zs", "-", "+"]), &store, &mut conn, &hub).await;
        assert!(resp2.0.starts_with(b"*4\r\n"), "expected *4 for unbounded range");
    }

    // ── ZLEXCOUNT ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn zlexcount_returns_count_in_lex_range() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        dispatch(&args(&["ZADD", "zs", "0", "a", "0", "b", "0", "c", "0", "d"]), &store, &mut conn, &hub).await;
        // [a, [c] => "a", "b", "c" => 3
        let resp = dispatch(&args(&["ZLEXCOUNT", "zs", "[a", "[c"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&resp.0), 3);
        // - to + => 4
        let resp2 = dispatch(&args(&["ZLEXCOUNT", "zs", "-", "+"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&resp2.0), 4);
    }

    // ── ZREMRANGEBYRANK ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn zremrangebyrank_removes_by_rank_range_and_returns_count() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        dispatch(&args(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"]), &store, &mut conn, &hub).await;
        // remove ranks 0..=1 => removes "a" and "b" (2 members)
        let resp = dispatch(&args(&["ZREMRANGEBYRANK", "zs", "0", "1"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&resp.0), 2);
        // only "c" should remain
        let card = dispatch(&args(&["ZCARD", "zs"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&card.0), 1);
    }

    // ── ZREMRANGEBYSCORE ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn zremrangebyscore_removes_by_score_range_and_returns_count() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        dispatch(&args(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"]), &store, &mut conn, &hub).await;
        // remove scores 1..=2 => removes "a" and "b"
        let resp = dispatch(&args(&["ZREMRANGEBYSCORE", "zs", "1", "2"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&resp.0), 2);
        let card = dispatch(&args(&["ZCARD", "zs"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&card.0), 1);
    }

    // ── ZREMRANGEBYLEX ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn zremrangebylex_removes_by_lex_range_and_returns_count() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        dispatch(&args(&["ZADD", "zs", "0", "a", "0", "b", "0", "c", "0", "d"]), &store, &mut conn, &hub).await;
        // remove [a, [b] => removes "a" and "b"
        let resp = dispatch(&args(&["ZREMRANGEBYLEX", "zs", "[a", "[b"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&resp.0), 2);
        let card = dispatch(&args(&["ZCARD", "zs"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&card.0), 2);
    }

    // ── ZPOPMIN ──────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn zpopmin_removes_and_returns_lowest_score_member() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        dispatch(&args(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"]), &store, &mut conn, &hub).await;
        // ZPOPMIN without count => returns [member, score] for the lowest
        let resp = dispatch(&args(&["ZPOPMIN", "zs"]), &store, &mut conn, &hub).await;
        // expect *2 array: "a", "1"
        assert!(resp.0.starts_with(b"*2\r\n"), "expected *2 array, got {:?}", std::str::from_utf8(&resp.0));
        assert!(resp.0.windows(b"$1\r\na\r\n".len()).any(|w| w == b"$1\r\na\r\n"), "expected member 'a'");
        // cardinality should now be 2
        let card = dispatch(&args(&["ZCARD", "zs"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&card.0), 2);
    }

    // ── ZPOPMAX ──────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn zpopmax_removes_and_returns_highest_score_member() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        dispatch(&args(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"]), &store, &mut conn, &hub).await;
        // ZPOPMAX without count => returns [member, score] for the highest
        let resp = dispatch(&args(&["ZPOPMAX", "zs"]), &store, &mut conn, &hub).await;
        // expect *2 array: "c", "3"
        assert!(resp.0.starts_with(b"*2\r\n"), "expected *2 array, got {:?}", std::str::from_utf8(&resp.0));
        assert!(resp.0.windows(b"$1\r\nc\r\n".len()).any(|w| w == b"$1\r\nc\r\n"), "expected member 'c'");
        // cardinality should now be 2
        let card = dispatch(&args(&["ZCARD", "zs"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&card.0), 2);
    }

    // ── ZRANDMEMBER ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn zrandmember_returns_member_or_array() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        dispatch(&args(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"]), &store, &mut conn, &hub).await;
        // Without count => single bulk string (one of a/b/c)
        let resp = dispatch(&args(&["ZRANDMEMBER", "zs"]), &store, &mut conn, &hub).await;
        assert!(resp.0.starts_with(b"$"), "expected bulk string, got {:?}", std::str::from_utf8(&resp.0));
        // With count 2 => array of 2 distinct members
        let resp2 = dispatch(&args(&["ZRANDMEMBER", "zs", "2"]), &store, &mut conn, &hub).await;
        assert!(resp2.0.starts_with(b"*2\r\n"), "expected *2 array, got {:?}", std::str::from_utf8(&resp2.0));
        // Negative count allows repeats; -5 on a 3-member set => array of 5
        let resp3 = dispatch(&args(&["ZRANDMEMBER", "zs", "-5"]), &store, &mut conn, &hub).await;
        assert!(resp3.0.starts_with(b"*5\r\n"), "expected *5 array with repeats, got {:?}", std::str::from_utf8(&resp3.0));
    }

    // ── SREM ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn srem_removes_members_and_returns_count() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "s", "a", "b", "c"]), &store).await;
        let resp = cmd_srem(&args(&["SREM", "s", "a", "c"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 2);
        let members = parse_keys_resp(&cmd_smembers(&args(&["SMEMBERS", "s"]), &store).await);
        assert_eq!(members, vec!["b"]);
    }

    #[tokio::test]
    async fn srem_missing_member_not_counted() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "s", "a"]), &store).await;
        // "x" is not in the set; only "a" is removed.
        let resp = cmd_srem(&args(&["SREM", "s", "a", "x"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 1);
        // Key missing entirely also returns 0.
        let resp2 = cmd_srem(&args(&["SREM", "ghost", "a"]), &store).await;
        assert_eq!(parse_int_resp(&resp2), 0);
    }

    // ── SCARD ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn scard_returns_cardinality() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "s", "a", "b", "c"]), &store).await;
        let resp = cmd_scard(&args(&["SCARD", "s"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 3);
    }

    #[tokio::test]
    async fn scard_missing_key_returns_0() {
        let store = make_store();
        let resp = cmd_scard(&args(&["SCARD", "nokey"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 0);
    }

    // ── SISMEMBER ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn sismember_present_returns_1() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "s", "hello"]), &store).await;
        let resp = cmd_sismember(&args(&["SISMEMBER", "s", "hello"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 1);
    }

    #[tokio::test]
    async fn sismember_absent_and_missing_key_return_0() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "s", "hello"]), &store).await;
        // Member not in set.
        let resp = cmd_sismember(&args(&["SISMEMBER", "s", "world"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 0);
        // Key does not exist at all.
        let resp2 = cmd_sismember(&args(&["SISMEMBER", "ghost", "hello"]), &store).await;
        assert_eq!(parse_int_resp(&resp2), 0);
    }

    // ── SMISMEMBER ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn smismember_returns_per_member_results() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "s", "a", "b"]), &store).await;
        let resp = cmd_smismember(&args(&["SMISMEMBER", "s", "a", "x", "b"]), &store).await;
        // Expected: *3\r\n:1\r\n:0\r\n:1\r\n
        assert_eq!(&*resp, b"*3\r\n:1\r\n:0\r\n:1\r\n");
    }

    #[tokio::test]
    async fn smismember_missing_key_returns_all_zeros() {
        let store = make_store();
        let resp = cmd_smismember(&args(&["SMISMEMBER", "ghost", "a", "b"]), &store).await;
        assert_eq!(&*resp, b"*2\r\n:0\r\n:0\r\n");
    }

    // ── SUNION ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn sunion_combines_multiple_sets() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "s1", "a", "b"]), &store).await;
        cmd_sadd(&args(&["SADD", "s2", "b", "c"]), &store).await;
        let members =
            parse_keys_resp(&cmd_sunion(&args(&["SUNION", "s1", "s2"]), &store).await);
        assert_eq!(members, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn sunion_missing_key_treated_as_empty() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "s1", "x"]), &store).await;
        let members =
            parse_keys_resp(&cmd_sunion(&args(&["SUNION", "s1", "ghost"]), &store).await);
        assert_eq!(members, vec!["x"]);
    }

    // ── SINTER ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn sinter_returns_common_members() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "s1", "a", "b", "c"]), &store).await;
        cmd_sadd(&args(&["SADD", "s2", "b", "c", "d"]), &store).await;
        let members =
            parse_keys_resp(&cmd_sinter(&args(&["SINTER", "s1", "s2"]), &store).await);
        assert_eq!(members, vec!["b", "c"]);
    }

    #[tokio::test]
    async fn sinter_missing_key_returns_empty() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "s1", "a"]), &store).await;
        let resp = cmd_sinter(&args(&["SINTER", "s1", "ghost"]), &store).await;
        assert_eq!(&*resp, b"*0\r\n");
    }

    // ── SDIFF ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn sdiff_returns_members_in_first_not_others() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "s1", "a", "b", "c"]), &store).await;
        cmd_sadd(&args(&["SADD", "s2", "b"]), &store).await;
        cmd_sadd(&args(&["SADD", "s3", "c"]), &store).await;
        let members =
            parse_keys_resp(&cmd_sdiff(&args(&["SDIFF", "s1", "s2", "s3"]), &store).await);
        assert_eq!(members, vec!["a"]);
    }

    #[tokio::test]
    async fn sdiff_missing_first_key_returns_empty() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "s2", "a"]), &store).await;
        let resp = cmd_sdiff(&args(&["SDIFF", "ghost", "s2"]), &store).await;
        assert_eq!(&*resp, b"*0\r\n");
    }

    // ── SUNIONSTORE ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn sunionstore_stores_union_and_returns_count() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "s1", "a", "b"]), &store).await;
        cmd_sadd(&args(&["SADD", "s2", "b", "c"]), &store).await;
        let resp =
            cmd_sunionstore(&args(&["SUNIONSTORE", "dst", "s1", "s2"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 3);
        let members =
            parse_keys_resp(&cmd_smembers(&args(&["SMEMBERS", "dst"]), &store).await);
        assert_eq!(members, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn sunionstore_overwrites_existing_destination() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "dst", "old"]), &store).await;
        cmd_sadd(&args(&["SADD", "src", "new"]), &store).await;
        cmd_sunionstore(&args(&["SUNIONSTORE", "dst", "src"]), &store).await;
        let members =
            parse_keys_resp(&cmd_smembers(&args(&["SMEMBERS", "dst"]), &store).await);
        assert_eq!(members, vec!["new"]);
    }

    // ── SINTERSTORE ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn sinterstore_stores_intersection_and_returns_count() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "s1", "a", "b", "c"]), &store).await;
        cmd_sadd(&args(&["SADD", "s2", "b", "c", "d"]), &store).await;
        let resp =
            cmd_sinterstore(&args(&["SINTERSTORE", "dst", "s1", "s2"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 2);
        let members =
            parse_keys_resp(&cmd_smembers(&args(&["SMEMBERS", "dst"]), &store).await);
        assert_eq!(members, vec!["b", "c"]);
    }

    #[tokio::test]
    async fn sinterstore_missing_source_stores_empty_set() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "s1", "a"]), &store).await;
        let resp =
            cmd_sinterstore(&args(&["SINTERSTORE", "dst", "s1", "ghost"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 0);
        // dst exists but is an empty set; SCARD should return 0.
        let card = cmd_scard(&args(&["SCARD", "dst"]), &store).await;
        assert_eq!(parse_int_resp(&card), 0);
    }

    // ── SDIFFSTORE ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn sdiffstore_stores_diff_and_returns_count() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "s1", "a", "b", "c"]), &store).await;
        cmd_sadd(&args(&["SADD", "s2", "b", "c"]), &store).await;
        let resp =
            cmd_sdiffstore(&args(&["SDIFFSTORE", "dst", "s1", "s2"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 1);
        let members =
            parse_keys_resp(&cmd_smembers(&args(&["SMEMBERS", "dst"]), &store).await);
        assert_eq!(members, vec!["a"]);
    }

    #[tokio::test]
    async fn sdiffstore_missing_first_source_stores_empty_set() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "s2", "a"]), &store).await;
        let resp =
            cmd_sdiffstore(&args(&["SDIFFSTORE", "dst", "ghost", "s2"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 0);
        let card = cmd_scard(&args(&["SCARD", "dst"]), &store).await;
        assert_eq!(parse_int_resp(&card), 0);
    }

    // ── SMOVE ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn smove_transfers_member_between_sets() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "src", "a", "b"]), &store).await;
        cmd_sadd(&args(&["SADD", "dst", "c"]), &store).await;
        let resp = cmd_smove(&args(&["SMOVE", "src", "dst", "a"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 1);
        // "a" should now be in dst, not in src.
        assert_eq!(
            parse_int_resp(
                &cmd_sismember(&args(&["SISMEMBER", "src", "a"]), &store).await
            ),
            0
        );
        assert_eq!(
            parse_int_resp(
                &cmd_sismember(&args(&["SISMEMBER", "dst", "a"]), &store).await
            ),
            1
        );
    }

    #[tokio::test]
    async fn smove_returns_0_when_member_not_in_src() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "src", "a"]), &store).await;
        let resp = cmd_smove(&args(&["SMOVE", "src", "dst", "x"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 0);
        // dst should not have been created.
        assert_eq!(
            &*(cmd_scard(&args(&["SCARD", "dst"]), &store).await),
            b":0\r\n"
        );
    }

    // ── SPOP ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn spop_removes_and_returns_a_member() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "s", "only"]), &store).await;
        let resp = cmd_spop(&args(&["SPOP", "s"]), &store).await;
        assert_eq!(&*resp, b"$4\r\nonly\r\n");
        // Set should now be empty.
        assert_eq!(
            parse_int_resp(&cmd_scard(&args(&["SCARD", "s"]), &store).await),
            0
        );
    }

    #[tokio::test]
    async fn spop_with_count_removes_multiple_members() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "s", "a", "b", "c"]), &store).await;
        let resp = cmd_spop(&args(&["SPOP", "s", "2"]), &store).await;
        // Should return an array of 2 members.
        assert!(resp.starts_with(b"*2\r\n"));
        // 1 member should remain.
        assert_eq!(
            parse_int_resp(&cmd_scard(&args(&["SCARD", "s"]), &store).await),
            1
        );
    }

    // ── SRANDMEMBER ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn srandmember_returns_member_without_removing() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "s", "only"]), &store).await;
        let resp = cmd_srandmember(&args(&["SRANDMEMBER", "s"]), &store).await;
        assert_eq!(&*resp, b"$4\r\nonly\r\n");
        // Member must still be present.
        assert_eq!(
            parse_int_resp(&cmd_scard(&args(&["SCARD", "s"]), &store).await),
            1
        );
    }

    #[tokio::test]
    async fn srandmember_with_positive_count_returns_distinct_subset() {
        let store = make_store();
        cmd_sadd(&args(&["SADD", "s", "a", "b", "c", "d"]), &store).await;
        let resp = cmd_srandmember(&args(&["SRANDMEMBER", "s", "2"]), &store).await;
        // Returns an array of 2 distinct members; set is unmodified.
        assert!(resp.starts_with(b"*2\r\n"));
        assert_eq!(
            parse_int_resp(&cmd_scard(&args(&["SCARD", "s"]), &store).await),
            4
        );
    }

    // ── LPUSHX ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn lpushx_pushes_when_key_exists() {
        let store = make_store();
        // Seed the list so LPUSHX is allowed.
        cmd_lpush(&args(&["LPUSH", "lst", "a"]), &store).await;
        let resp = cmd_lpushx(&args(&["LPUSHX", "lst", "b"]), &store).await;
        // List is now ["b", "a"] -- length 2.
        assert_eq!(parse_int_resp(&resp), 2);
    }

    #[tokio::test]
    async fn lpushx_returns_zero_when_key_absent() {
        let store = make_store();
        let resp = cmd_lpushx(&args(&["LPUSHX", "ghost", "v"]), &store).await;
        assert_eq!(&*resp, b":0\r\n");
    }

    // ── RPUSHX ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn rpushx_appends_when_key_exists() {
        let store = make_store();
        cmd_rpush(&args(&["RPUSH", "lst", "x"]), &store).await;
        let resp = cmd_rpushx(&args(&["RPUSHX", "lst", "y"]), &store).await;
        // List is now ["x", "y"] -- length 2.
        assert_eq!(parse_int_resp(&resp), 2);
    }

    #[tokio::test]
    async fn rpushx_returns_zero_when_key_absent() {
        let store = make_store();
        let resp = cmd_rpushx(&args(&["RPUSHX", "ghost", "v"]), &store).await;
        assert_eq!(&*resp, b":0\r\n");
    }

    // ── LPOP ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn lpop_returns_head_element() {
        let store = make_store();
        // RPUSH preserves insertion order: list is ["a", "b", "c"].
        cmd_rpush(&args(&["RPUSH", "l", "a", "b", "c"]), &store).await;
        let resp = cmd_lpop(&args(&["LPOP", "l"]), &store).await;
        assert_eq!(&*resp, b"$1\r\na\r\n");
    }

    #[tokio::test]
    async fn lpop_with_count_returns_array() {
        let store = make_store();
        // List: ["a", "b", "c"]
        cmd_rpush(&args(&["RPUSH", "l", "a", "b", "c"]), &store).await;
        let resp = cmd_lpop(&args(&["LPOP", "l", "2"]), &store).await;
        // Expect *2 array containing "a" then "b".
        assert!(resp.starts_with(b"*2\r\n"), "expected 2-element array");
        assert!(resp.windows(7).any(|w| w == b"$1\r\na\r\n"), "expected 'a'");
        assert!(resp.windows(7).any(|w| w == b"$1\r\nb\r\n"), "expected 'b'");
    }

    #[tokio::test]
    async fn lpop_missing_key_returns_null() {
        let store = make_store();
        let resp = cmd_lpop(&args(&["LPOP", "ghost"]), &store).await;
        assert_eq!(&*resp, b"$-1\r\n");
    }

    // ── LLEN ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn llen_returns_list_length() {
        let store = make_store();
        cmd_rpush(&args(&["RPUSH", "l", "a", "b", "c"]), &store).await;
        let resp = cmd_llen(&args(&["LLEN", "l"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 3);
    }

    #[tokio::test]
    async fn llen_missing_key_returns_zero() {
        let store = make_store();
        let resp = cmd_llen(&args(&["LLEN", "ghost"]), &store).await;
        assert_eq!(&*resp, b":0\r\n");
    }

    // ── LRANGE ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn lrange_returns_subrange() {
        let store = make_store();
        // List: ["a", "b", "c", "d"]
        cmd_rpush(&args(&["RPUSH", "l", "a", "b", "c", "d"]), &store).await;
        let resp = cmd_lrange(&args(&["LRANGE", "l", "1", "2"]), &store).await;
        // Expect ["b", "c"]
        assert!(resp.starts_with(b"*2\r\n"));
        assert!(resp.windows(7).any(|w| w == b"$1\r\nb\r\n"));
        assert!(resp.windows(7).any(|w| w == b"$1\r\nc\r\n"));
    }

    #[tokio::test]
    async fn lrange_negative_indices() {
        let store = make_store();
        // List: ["a", "b", "c"]
        cmd_rpush(&args(&["RPUSH", "l", "a", "b", "c"]), &store).await;
        // -2..-1 selects the last two elements ["b", "c"].
        let resp = cmd_lrange(&args(&["LRANGE", "l", "-2", "-1"]), &store).await;
        assert!(resp.starts_with(b"*2\r\n"));
        assert!(resp.windows(7).any(|w| w == b"$1\r\nb\r\n"));
        assert!(resp.windows(7).any(|w| w == b"$1\r\nc\r\n"));
    }

    // ── LINDEX ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn lindex_returns_element_at_index() {
        let store = make_store();
        cmd_rpush(&args(&["RPUSH", "l", "a", "b", "c"]), &store).await;
        let resp = cmd_lindex(&args(&["LINDEX", "l", "1"]), &store).await;
        assert_eq!(&*resp, b"$1\r\nb\r\n");
    }

    #[tokio::test]
    async fn lindex_negative_index_and_out_of_range() {
        let store = make_store();
        cmd_rpush(&args(&["RPUSH", "l", "a", "b", "c"]), &store).await;
        // -1 is the last element.
        let resp = cmd_lindex(&args(&["LINDEX", "l", "-1"]), &store).await;
        assert_eq!(&*resp, b"$1\r\nc\r\n");
        // Index 10 is beyond the list -- null.
        let resp_oor = cmd_lindex(&args(&["LINDEX", "l", "10"]), &store).await;
        assert_eq!(&*resp_oor, b"$-1\r\n");
    }

    // ── LSET ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn lset_updates_element_at_index() {
        let store = make_store();
        cmd_rpush(&args(&["RPUSH", "l", "a", "b", "c"]), &store).await;
        let resp = cmd_lset(&args(&["LSET", "l", "1", "B"]), &store).await;
        assert_eq!(&*resp, b"+OK\r\n");
        // Verify with LINDEX.
        let check = cmd_lindex(&args(&["LINDEX", "l", "1"]), &store).await;
        assert_eq!(&*check, b"$1\r\nB\r\n");
    }

    #[tokio::test]
    async fn lset_out_of_range_returns_error() {
        let store = make_store();
        cmd_rpush(&args(&["RPUSH", "l", "only"]), &store).await;
        let resp = cmd_lset(&args(&["LSET", "l", "5", "v"]), &store).await;
        assert!(resp.starts_with(b"-ERR"));
    }

    // ── LREM ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn lrem_removes_from_head_when_count_positive() {
        let store = make_store();
        // List: ["x", "a", "x", "b", "x"]
        cmd_rpush(&args(&["RPUSH", "l", "x", "a", "x", "b", "x"]), &store).await;
        // Remove 2 occurrences of "x" scanning from head.
        let resp = cmd_lrem(&args(&["LREM", "l", "2", "x"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 2);
        // Remaining list length should be 3.
        let len = cmd_llen(&args(&["LLEN", "l"]), &store).await;
        assert_eq!(parse_int_resp(&len), 3);
    }

    #[tokio::test]
    async fn lrem_count_zero_removes_all_occurrences() {
        let store = make_store();
        cmd_rpush(&args(&["RPUSH", "l", "y", "y", "y"]), &store).await;
        let resp = cmd_lrem(&args(&["LREM", "l", "0", "y"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 3);
        let len = cmd_llen(&args(&["LLEN", "l"]), &store).await;
        assert_eq!(parse_int_resp(&len), 0);
    }

    // ── LTRIM ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn ltrim_shrinks_list_to_range() {
        let store = make_store();
        // List: ["a", "b", "c", "d", "e"]
        cmd_rpush(&args(&["RPUSH", "l", "a", "b", "c", "d", "e"]), &store).await;
        let resp = cmd_ltrim(&args(&["LTRIM", "l", "1", "3"]), &store).await;
        assert_eq!(&*resp, b"+OK\r\n");
        // List should now be ["b", "c", "d"].
        let len = cmd_llen(&args(&["LLEN", "l"]), &store).await;
        assert_eq!(parse_int_resp(&len), 3);
        let first = cmd_lindex(&args(&["LINDEX", "l", "0"]), &store).await;
        assert_eq!(&*first, b"$1\r\nb\r\n");
    }

    #[tokio::test]
    async fn ltrim_out_of_bounds_clears_list() {
        let store = make_store();
        cmd_rpush(&args(&["RPUSH", "l", "a", "b"]), &store).await;
        // start > stop: list should be emptied.
        cmd_ltrim(&args(&["LTRIM", "l", "5", "1"]), &store).await;
        let len = cmd_llen(&args(&["LLEN", "l"]), &store).await;
        assert_eq!(parse_int_resp(&len), 0);
    }

    // ── LINSERT ──────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn linsert_before_pivot() {
        let store = make_store();
        // List: ["a", "c"]
        cmd_rpush(&args(&["RPUSH", "l", "a", "c"]), &store).await;
        // Insert "b" BEFORE "c" -> ["a", "b", "c"]
        let resp = cmd_linsert(&args(&["LINSERT", "l", "BEFORE", "c", "b"]), &store).await;
        assert_eq!(parse_int_resp(&resp), 3);
        let middle = cmd_lindex(&args(&["LINDEX", "l", "1"]), &store).await;
        assert_eq!(&*middle, b"$1\r\nb\r\n");
    }

    #[tokio::test]
    async fn linsert_pivot_not_found_returns_minus_one() {
        let store = make_store();
        cmd_rpush(&args(&["RPUSH", "l", "a", "b"]), &store).await;
        let resp =
            cmd_linsert(&args(&["LINSERT", "l", "BEFORE", "zzz", "x"]), &store).await;
        assert_eq!(parse_int_resp(&resp), -1);
    }

    // ── LPOS ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn lpos_returns_index_of_element() {
        let store = make_store();
        // List: ["a", "b", "c"]
        cmd_rpush(&args(&["RPUSH", "l", "a", "b", "c"]), &store).await;
        // "b" is at index 1 -- returned as a bulk string containing "1".
        let resp = cmd_lpos(&args(&["LPOS", "l", "b"]), &store).await;
        assert_eq!(&*resp, b"$1\r\n1\r\n");
    }

    #[tokio::test]
    async fn lpos_returns_null_when_element_absent() {
        let store = make_store();
        cmd_rpush(&args(&["RPUSH", "l", "a", "b"]), &store).await;
        let resp = cmd_lpos(&args(&["LPOS", "l", "z"]), &store).await;
        assert_eq!(&*resp, b"$-1\r\n");
    }

    // ── LMOVE ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn lmove_left_right_transfers_element() {
        let store = make_store();
        // src list: ["a", "b", "c"]
        cmd_rpush(&args(&["RPUSH", "src", "a", "b", "c"]), &store).await;
        // Pop from LEFT of src, push to RIGHT of dst.
        let resp =
            cmd_lmove(&args(&["LMOVE", "src", "dst", "LEFT", "RIGHT"]), &store).await;
        // Returned element should be "a".
        assert_eq!(&*resp, b"$1\r\na\r\n");
        // src should now have length 2.
        let src_len = cmd_llen(&args(&["LLEN", "src"]), &store).await;
        assert_eq!(parse_int_resp(&src_len), 2);
        // dst should contain "a" at index 0.
        let dst_el = cmd_lindex(&args(&["LINDEX", "dst", "0"]), &store).await;
        assert_eq!(&*dst_el, b"$1\r\na\r\n");
    }

    #[tokio::test]
    async fn lmove_returns_null_when_source_absent() {
        let store = make_store();
        let resp =
            cmd_lmove(&args(&["LMOVE", "ghost", "dst", "LEFT", "RIGHT"]), &store).await;
        assert_eq!(&*resp, b"$-1\r\n");
    }

    // ── GENERIC/SERVER commands ──────────────────────────────────────────────

    #[tokio::test]
    async fn persist_removes_ttl() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "k", "v", "EX", "100"]), &store).await;
        let (r, _) = dispatch(&args(&["PERSIST", "k"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&r), 1);
        let (r2, _) = dispatch(&args(&["TTL", "k"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&r2), -1);
    }

    #[tokio::test]
    async fn persist_missing_key_returns_0() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        let (r, _) = dispatch(&args(&["PERSIST", "missing"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&r), 0);
    }

    #[tokio::test]
    async fn pttl_returns_millis() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "k", "v", "PX", "10000"]), &store).await;
        let (r, _) = dispatch(&args(&["PTTL", "k"]), &store, &mut conn, &hub).await;
        let ms = parse_int_resp(&r);
        assert!(ms > 0 && ms <= 10000);
    }

    #[tokio::test]
    async fn pttl_missing_returns_minus2() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        let (r, _) = dispatch(&args(&["PTTL", "missing"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&r), -2);
    }

    #[tokio::test]
    async fn pttl_no_expiry_returns_minus1() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "k", "v"]), &store).await;
        let (r, _) = dispatch(&args(&["PTTL", "k"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&r), -1);
    }

    #[tokio::test]
    async fn pexpire_sets_ttl_in_ms() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "k", "v"]), &store).await;
        let (r, _) = dispatch(&args(&["PEXPIRE", "k", "5000"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&r), 1);
        let (r2, _) = dispatch(&args(&["PTTL", "k"]), &store, &mut conn, &hub).await;
        assert!(parse_int_resp(&r2) > 0);
    }

    #[tokio::test]
    async fn expireat_sets_absolute_ttl() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "k", "v"]), &store).await;
        let future_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() + 3600;
        let (r, _) = dispatch(&args(&["EXPIREAT", "k", &future_ts.to_string()]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&r), 1);
    }

    #[tokio::test]
    async fn pexpireat_sets_absolute_ttl_ms() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "k", "v"]), &store).await;
        let future_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64 + 60_000;
        let (r, _) = dispatch(&args(&["PEXPIREAT", "k", &future_ms.to_string()]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&r), 1);
    }

    #[tokio::test]
    async fn expiretime_returns_unix_secs() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "k", "v", "EX", "3600"]), &store).await;
        let (r, _) = dispatch(&args(&["EXPIRETIME", "k"]), &store, &mut conn, &hub).await;
        assert!(parse_int_resp(&r) > 0);
    }

    #[tokio::test]
    async fn expiretime_no_expiry_returns_minus1() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "k", "v"]), &store).await;
        let (r, _) = dispatch(&args(&["EXPIRETIME", "k"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&r), -1);
    }

    #[tokio::test]
    async fn pexpiretime_returns_unix_ms() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "k", "v", "PX", "60000"]), &store).await;
        let (r, _) = dispatch(&args(&["PEXPIRETIME", "k"]), &store, &mut conn, &hub).await;
        assert!(parse_int_resp(&r) > 0);
    }

    #[tokio::test]
    async fn rename_renames_key() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "old", "val"]), &store).await;
        let (r, _) = dispatch(&args(&["RENAME", "old", "new"]), &store, &mut conn, &hub).await;
        assert_eq!(&*r, b"+OK\r\n");
        let (r2, _) = dispatch(&args(&["GET", "new"]), &store, &mut conn, &hub).await;
        assert_eq!(&*r2, b"$3\r\nval\r\n");
    }

    #[tokio::test]
    async fn rename_missing_source_returns_error() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        let (r, _) = dispatch(&args(&["RENAME", "missing", "dst"]), &store, &mut conn, &hub).await;
        assert!(r.starts_with(b"-ERR"));
    }

    #[tokio::test]
    async fn renamenx_renames_when_dst_absent() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "a", "val"]), &store).await;
        let (r, _) = dispatch(&args(&["RENAMENX", "a", "b"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&r), 1);
    }

    #[tokio::test]
    async fn renamenx_returns_0_when_dst_exists() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "a", "v1"]), &store).await;
        cmd_set(&args(&["SET", "b", "v2"]), &store).await;
        let (r, _) = dispatch(&args(&["RENAMENX", "a", "b"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&r), 0);
    }

    #[tokio::test]
    async fn scan_returns_all_keys() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "a", "1"]), &store).await;
        cmd_set(&args(&["SET", "b", "2"]), &store).await;
        let (r, _) = dispatch(&args(&["SCAN", "0"]), &store, &mut conn, &hub).await;
        let s = std::str::from_utf8(&r).unwrap();
        assert!(s.contains("a"));
        assert!(s.contains("b"));
    }

    #[tokio::test]
    async fn copy_copies_value() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "src", "val"]), &store).await;
        let (r, _) = dispatch(&args(&["COPY", "src", "dst"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&r), 1);
        let (r2, _) = dispatch(&args(&["GET", "dst"]), &store, &mut conn, &hub).await;
        assert_eq!(&*r2, b"$3\r\nval\r\n");
    }

    #[tokio::test]
    async fn copy_missing_src_returns_0() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        let (r, _) = dispatch(&args(&["COPY", "missing", "dst"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&r), 0);
    }

    #[tokio::test]
    async fn object_encoding_string() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "k", "val"]), &store).await;
        let (r, _) = dispatch(&args(&["OBJECT", "ENCODING", "k"]), &store, &mut conn, &hub).await;
        assert_eq!(&*r, b"$6\r\nembstr\r\n");
    }

    #[tokio::test]
    async fn touch_returns_existing_count() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "a", "1"]), &store).await;
        cmd_set(&args(&["SET", "b", "2"]), &store).await;
        let (r, _) = dispatch(&args(&["TOUCH", "a", "b", "missing"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&r), 2);
    }

    #[tokio::test]
    async fn unlink_deletes_keys() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "a", "1"]), &store).await;
        cmd_set(&args(&["SET", "b", "2"]), &store).await;
        let (r, _) = dispatch(&args(&["UNLINK", "a", "b", "missing"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&r), 2);
    }

    #[tokio::test]
    async fn flushdb_clears_all_keys() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "a", "1"]), &store).await;
        let (r, _) = dispatch(&args(&["FLUSHDB"]), &store, &mut conn, &hub).await;
        assert_eq!(&*r, b"+OK\r\n");
        let (r2, _) = dispatch(&args(&["DBSIZE"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&r2), 0);
    }

    #[tokio::test]
    async fn flushall_clears_all_keys() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        cmd_set(&args(&["SET", "a", "1"]), &store).await;
        let (r, _) = dispatch(&args(&["FLUSHALL"]), &store, &mut conn, &hub).await;
        assert_eq!(&*r, b"+OK\r\n");
        let (r2, _) = dispatch(&args(&["DBSIZE"]), &store, &mut conn, &hub).await;
        assert_eq!(parse_int_resp(&r2), 0);
    }

    #[tokio::test]
    async fn select_0_ok_select_1_error() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        let (r, _) = dispatch(&args(&["SELECT", "0"]), &store, &mut conn, &hub).await;
        assert_eq!(&*r, b"+OK\r\n");
        let (r2, _) = dispatch(&args(&["SELECT", "1"]), &store, &mut conn, &hub).await;
        assert!(r2.starts_with(b"-ERR"));
    }

    #[tokio::test]
    async fn hello_returns_response() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        let (r, _) = dispatch(&args(&["HELLO"]), &store, &mut conn, &hub).await;
        assert!(!r.starts_with(b"-ERR unknown"));
    }

    #[tokio::test]
    async fn reset_returns_reset() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        let (r, _) = dispatch(&args(&["RESET"]), &store, &mut conn, &hub).await;
        assert_eq!(&*r, b"+RESET\r\n");
    }

    #[tokio::test]
    async fn config_get_returns_array() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        let (r, _) = dispatch(&args(&["CONFIG", "GET", "maxmemory"]), &store, &mut conn, &hub).await;
        assert!(r.starts_with(b"*"));
    }

    #[tokio::test]
    async fn config_set_returns_ok() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        let (r, _) = dispatch(&args(&["CONFIG", "SET", "x", "y"]), &store, &mut conn, &hub).await;
        assert_eq!(&*r, b"+OK\r\n");
    }

    #[tokio::test]
    async fn command_returns_ok() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        let (r, _) = dispatch(&args(&["COMMAND"]), &store, &mut conn, &hub).await;
        // COMMAND returns an array (possibly empty), not an error
        assert!(!r.starts_with(b"-"), "COMMAND returned error: {:?}", std::str::from_utf8(&r));
        assert!(r.starts_with(b"*"), "expected array for COMMAND");
    }

    #[tokio::test]
    async fn info_returns_bulk_string() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        let (r, _) = dispatch(&args(&["INFO"]), &store, &mut conn, &hub).await;
        assert!(r.starts_with(b"$"));
    }

    #[tokio::test]
    async fn wait_returns_0() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        let (r, _) = dispatch(&args(&["WAIT", "0", "0"]), &store, &mut conn, &hub).await;
        assert_eq!(&*r, b":0\r\n");
    }

    #[tokio::test]
    async fn client_setname_and_getname() {
        let store = make_store();
        let mut conn = make_conn();
        let hub = make_hub();
        let (r, _) = dispatch(&args(&["CLIENT", "SETNAME", "myconn"]), &store, &mut conn, &hub).await;
        assert_eq!(&*r, b"+OK\r\n");
        let (r2, _) = dispatch(&args(&["CLIENT", "GETNAME"]), &store, &mut conn, &hub).await;
        assert_eq!(&*r2, b"$6\r\nmyconn\r\n");
    }
}
