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
        // Allocate the shared payload and owned channel name lazily: a publish to
        // a channel with no subscribers and no matching patterns (the common
        // case) does no allocation at all. On the first recipient the payload is
        // allocated once and every subscriber thereafter receives an Arc clone
        // (ref-count bump only).
        let mut shared_data: Option<Arc<[u8]>> = None;
        let mut channel_owned: Option<String> = None;

        // Exact channel matches.
        if let Some(subs) = self.channels.get(channel) {
            for tx in subs.values() {
                let payload = Arc::clone(shared_data.get_or_insert_with(|| Arc::from(data)));
                let chan = channel_owned.get_or_insert_with(|| channel.to_owned()).clone();
                if tx
                    .send(PubSubMessage::Message {
                        channel: chan,
                        data: payload,
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
                    let payload = Arc::clone(shared_data.get_or_insert_with(|| Arc::from(data)));
                    let chan = channel_owned.get_or_insert_with(|| channel.to_owned()).clone();
                    if tx
                        .send(PubSubMessage::PMessage {
                            pattern: pattern.clone(),
                            channel: chan,
                            data: payload,
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    /// Helper: create a default hub inner for testing (no need for Arc<RwLock>).
    fn hub() -> PubSubHubInner {
        PubSubHubInner::default()
    }

    #[test]
    fn new_hub_creates_valid_hub() {
        let h = new_hub();
        // Should be constructable and lockable without panic.
        let inner = h.try_write().expect("should be able to lock new hub");
        assert!(inner.channels.is_empty());
        assert!(inner.patterns.is_empty());
    }

    #[tokio::test]
    async fn subscribe_and_publish_delivers_message() {
        let mut h = hub();
        let (tx, mut rx) = mpsc::unbounded_channel();

        h.subscribe("news", 1, tx);
        let count = h.publish("news", b"hello");

        assert_eq!(count, 1);
        let msg = rx.recv().await.expect("should receive a message");
        match msg {
            PubSubMessage::Message { channel, data } => {
                assert_eq!(channel, "news");
                assert_eq!(&*data, b"hello");
            }
            other => panic!("expected Message, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn subscribe_is_idempotent() {
        let mut h = hub();
        let (tx, mut rx) = mpsc::unbounded_channel();

        // Subscribe the same client twice.
        h.subscribe("ch", 1, tx.clone());
        h.subscribe("ch", 1, tx);

        let count = h.publish("ch", b"data");
        assert_eq!(count, 1, "idempotent subscribe should not double-deliver");

        // Only one message in the channel.
        let _msg = rx.recv().await.unwrap();
        // No second message should be pending.
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn unsubscribe_removes_subscriber() {
        let mut h = hub();
        let (tx, mut rx) = mpsc::unbounded_channel();

        h.subscribe("ch", 1, tx);
        h.unsubscribe("ch", 1);

        let count = h.publish("ch", b"gone");
        assert_eq!(count, 0, "publish should reach no one after unsubscribe");
        assert!(rx.try_recv().is_err(), "receiver should be empty");
    }

    #[test]
    fn unsubscribe_cleans_up_empty_channel_entry() {
        let mut h = hub();
        let (tx, _rx) = mpsc::unbounded_channel();

        h.subscribe("ch", 1, tx);
        assert!(h.channels.contains_key("ch"));

        h.unsubscribe("ch", 1);
        assert!(
            !h.channels.contains_key("ch"),
            "empty channel entry should be removed"
        );
    }

    #[tokio::test]
    async fn psubscribe_and_publish_delivers_pmessage() {
        let mut h = hub();
        let (tx, mut rx) = mpsc::unbounded_channel();

        h.psubscribe("news.*", 1, tx);
        let count = h.publish("news.sports", b"goal!");

        assert_eq!(count, 1);
        let msg = rx.recv().await.expect("should receive a pmessage");
        match msg {
            PubSubMessage::PMessage {
                pattern,
                channel,
                data,
            } => {
                assert_eq!(pattern, "news.*");
                assert_eq!(channel, "news.sports");
                assert_eq!(&*data, b"goal!");
            }
            other => panic!("expected PMessage, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn punsubscribe_removes_pattern_subscriber() {
        let mut h = hub();
        let (tx, mut rx) = mpsc::unbounded_channel();

        h.psubscribe("news.*", 1, tx);
        h.punsubscribe("news.*", 1);

        let count = h.publish("news.sports", b"nope");
        assert_eq!(count, 0);
        assert!(rx.try_recv().is_err());

        // Pattern entry should also be cleaned up.
        assert!(!h.patterns.contains_key("news.*"));
    }

    #[test]
    fn remove_client_removes_from_channels_and_patterns() {
        let mut h = hub();
        let (tx1, _rx1) = mpsc::unbounded_channel();
        let (tx2, _rx2) = mpsc::unbounded_channel();

        h.subscribe("ch1", 1, tx1.clone());
        h.subscribe("ch2", 1, tx1);
        h.psubscribe("pat*", 1, tx2);

        // Another client to keep entries alive.
        let (tx3, _rx3) = mpsc::unbounded_channel();
        h.subscribe("ch1", 2, tx3);

        h.remove_client(1);

        // ch1 still exists (client 2 is there), but client 1 is gone.
        assert!(h.channels.contains_key("ch1"));
        assert!(!h.channels["ch1"].contains_key(&1));

        // ch2 should be fully removed (was only client 1).
        assert!(!h.channels.contains_key("ch2"));

        // Pattern should be fully removed (was only client 1).
        assert!(!h.patterns.contains_key("pat*"));
    }

    #[test]
    fn publish_with_no_subscribers_returns_zero() {
        let h = hub();
        assert_eq!(h.publish("empty", b"data"), 0);
    }

    #[tokio::test]
    async fn publish_counts_both_channel_and_pattern_matches() {
        let mut h = hub();
        let (tx_chan, mut rx_chan) = mpsc::unbounded_channel();
        let (tx_pat, mut rx_pat) = mpsc::unbounded_channel();

        h.subscribe("news.sports", 1, tx_chan);
        h.psubscribe("news.*", 2, tx_pat);

        let count = h.publish("news.sports", b"both");
        assert_eq!(count, 2, "should count channel + pattern subscribers");

        // Both receivers should get a message.
        let _ = rx_chan.recv().await.unwrap();
        let _ = rx_pat.recv().await.unwrap();
    }

    #[test]
    fn dead_sender_not_counted_in_publish() {
        let mut h = hub();
        let (tx, rx) = mpsc::unbounded_channel();

        h.subscribe("ch", 1, tx);

        // Drop the receiver so the sender is "dead".
        drop(rx);

        let count = h.publish("ch", b"dropped");
        assert_eq!(count, 0, "dead sender should not be counted");
    }
}
