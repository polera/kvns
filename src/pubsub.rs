use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{RwLock, mpsc};

// ── Message types ─────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) enum PubSubMessage {
    /// Regular channel message: ["message", channel, data]
    Message { channel: String, data: Arc<[u8]> },
    /// Pattern-matched message: ["pmessage", pattern, channel, data]
    PMessage { pattern: String, channel: String, data: Arc<[u8]> },
}

// ── Hub ───────────────────────────────────────────────────────────────────────

pub(crate) type PubSubHub = Arc<RwLock<PubSubHubInner>>;

pub(crate) fn new_hub() -> PubSubHub {
    Arc::new(RwLock::new(PubSubHubInner::default()))
}

#[derive(Default)]
pub(crate) struct PubSubHubInner {
    /// channel → client_id → sender.  HashMap keyed by client_id gives O(1)
    /// idempotency checks on subscribe and O(1) removal on unsubscribe/disconnect.
    channels: HashMap<String, HashMap<u64, mpsc::UnboundedSender<PubSubMessage>>>,
    /// pattern → client_id → sender. Same O(1) idempotency and removal as channels.
    patterns: HashMap<String, HashMap<u64, mpsc::UnboundedSender<PubSubMessage>>>,
}

impl PubSubHubInner {
    /// Register `client_id` as a subscriber of `channel`.
    pub(crate) fn subscribe(
        &mut self,
        channel: &str,
        client_id: u64,
        tx: mpsc::UnboundedSender<PubSubMessage>,
    ) {
        // O(1) idempotency via HashMap key.
        self.channels
            .entry(channel.to_owned())
            .or_default()
            .entry(client_id)
            .or_insert(tx);
    }

    /// Unregister `client_id` from `channel`.
    pub(crate) fn unsubscribe(&mut self, channel: &str, client_id: u64) {
        if let Some(subs) = self.channels.get_mut(channel) {
            subs.remove(&client_id);
            if subs.is_empty() {
                self.channels.remove(channel);
            }
        }
    }

    /// Unregister `client_id` from a specific pattern subscription.
    pub(crate) fn punsubscribe(&mut self, pattern: &str, client_id: u64) {
        if let Some(subs) = self.patterns.get_mut(pattern) {
            subs.remove(&client_id);
            if subs.is_empty() {
                self.patterns.remove(pattern);
            }
        }
    }

    /// Register `client_id` as a pattern subscriber.
    pub(crate) fn psubscribe(
        &mut self,
        pattern: &str,
        client_id: u64,
        tx: mpsc::UnboundedSender<PubSubMessage>,
    ) {
        self.patterns
            .entry(pattern.to_owned())
            .or_default()
            .entry(client_id)
            .or_insert(tx);
    }

    /// Remove all subscriptions for `client_id` (called on disconnect).
    pub(crate) fn remove_client(&mut self, client_id: u64) {
        self.channels.retain(|_, subs| {
            subs.remove(&client_id);
            !subs.is_empty()
        });
        self.patterns.retain(|_, subs| {
            subs.remove(&client_id);
            !subs.is_empty()
        });
    }

    /// Publish `data` to `channel`. Returns number of clients that received it.
    ///
    /// Takes `&self` so that concurrent publishes can hold simultaneous read
    /// locks rather than serializing through a single write lock.  Dead senders
    /// are not removed here; they are cleaned up lazily in `remove_client` and
    /// `subscribe`/`unsubscribe`, which already hold write locks.
    pub(crate) fn publish(&self, channel: &str, data: &[u8]) -> usize {
        let mut count = 0usize;
        // Pre-allocate shared data once; all subscribers receive Arc clones (ref-count bump only).
        let shared_data: Arc<[u8]> = Arc::from(data);
        let channel_owned = channel.to_owned();

        // Exact channel matches.
        if let Some(subs) = self.channels.get(channel) {
            for tx in subs.values() {
                if tx
                    .send(PubSubMessage::Message {
                        channel: channel_owned.clone(),
                        data: Arc::clone(&shared_data),
                    })
                    .is_ok()
                {
                    count += 1;
                }
            }
        }

        // Pattern matches.
        let channel_bytes = channel.as_bytes();
        for (pattern, subs) in &self.patterns {
            if crate::commands::glob_match(pattern.as_bytes(), channel_bytes) {
                for tx in subs.values() {
                    if tx
                        .send(PubSubMessage::PMessage {
                            pattern: pattern.clone(),
                            channel: channel_owned.clone(),
                            data: Arc::clone(&shared_data),
                        })
                        .is_ok()
                    {
                        count += 1;
                    }
                }
            }
        }

        count
    }
}
