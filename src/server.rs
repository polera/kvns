use std::sync::atomic::{AtomicU64, Ordering};

use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::mpsc;
use tracing::debug;

use crate::commands::{ConnState, dispatch as dispatch_classic, encode_pubsub_message};
use crate::pubsub::{PubSubHub, PubSubMessage};
use crate::resp::{RespLimits, parse_resp_with_limits};
use crate::sharded::{ShardedStore, dispatch as dispatch_sharded_sync};
use crate::store::Store;

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub(crate) enum Backend {
    Classic(Store),
    Sharded(ShardedStore),
}

#[derive(Clone, Copy)]
pub(crate) struct ServerLimits {
    pub resp: RespLimits,
}

pub(crate) async fn handle_connection(
    stream: TcpStream,
    backend: Backend,
    limits: ServerLimits,
    _permit: OwnedSemaphorePermit,
    hub: PubSubHub,
) {
    let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
    let mut conn = ConnState::new(client_id);
    // The pub-sub channel is created lazily on the first SUBSCRIBE/PSUBSCRIBE.
    // ConnState::pubsub_sender() sets conn.pubsub_rx_slot; we pick it up below.
    let mut pubsub_rx: Option<mpsc::UnboundedReceiver<PubSubMessage>> = None;

    // Disable Nagle: send responses immediately rather than waiting to coalesce small writes.
    let _ = stream.set_nodelay(true);
    let (read_half, write_half) = stream.into_split();
    let mut reader = BufReader::with_capacity(64 * 1024, read_half);
    // BufWriter accumulates responses; we flush only when no more pipelined commands are
    // buffered, collapsing N pipelined writes into a single syscall.
    let mut writer = BufWriter::with_capacity(64 * 1024, write_half);

    loop {
        // When the connection is in pub-sub mode we must interleave incoming
        // commands with push messages arriving on the pub-sub channel.
        if conn.in_pubsub() {
            tokio::select! {
                parse_result = parse_resp_with_limits(&mut reader, limits.resp) => {
                    match parse_result {
                        Ok(None) => break,
                        Ok(Some(args)) if args.is_empty() => continue,
                        Ok(Some(args)) => {
                            let (response, quit) = match &backend {
                                Backend::Classic(store) => {
                                    dispatch_classic(&args, store, &mut conn, &hub).await
                                }
                                Backend::Sharded(store) => dispatch_sharded_sync(&args, store),
                            };
                            // Pick up the receiver if SUBSCRIBE just created the channel.
                            if pubsub_rx.is_none() {
                                pubsub_rx = conn.pubsub_rx_slot.take();
                            }
                            if writer.write_all(&response).await.is_err() {
                                break;
                            }
                            if quit {
                                let _ = writer.flush().await;
                                break;
                            }
                            if reader.buffer().is_empty() && writer.flush().await.is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            debug!(error = %e, "parse error, closing connection");
                            break;
                        }
                    }
                }
                msg = async {
                    match pubsub_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                let Some(msg) = msg else { break };
                    let encoded = encode_pubsub_message(&msg, conn.resp_version);
                    if writer.write_all(&encoded).await.is_err() {
                        break;
                    }
                    // Flush immediately so subscribers see messages without delay.
                    if writer.flush().await.is_err() {
                        break;
                    }
                }
            }
        } else {
            match parse_resp_with_limits(&mut reader, limits.resp).await {
                Ok(None) => break,
                Ok(Some(args)) if args.is_empty() => continue,
                Ok(Some(args)) => {
                    let (response, quit) = match &backend {
                        Backend::Classic(store) => {
                            dispatch_classic(&args, store, &mut conn, &hub).await
                        }
                        Backend::Sharded(store) => dispatch_sharded_sync(&args, store),
                    };
                    // Pick up the receiver if SUBSCRIBE just created the channel.
                    if pubsub_rx.is_none() {
                        pubsub_rx = conn.pubsub_rx_slot.take();
                    }
                    if writer.write_all(&response).await.is_err() {
                        break;
                    }
                    if quit {
                        let _ = writer.flush().await;
                        break;
                    }
                    // Only flush when the read buffer is drained: pipelined commands share a flush.
                    if reader.buffer().is_empty() && writer.flush().await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    debug!(error = %e, "parse error, closing connection");
                    break;
                }
            }
        }
    }

    // Clean up hub subscriptions on disconnect.
    hub.write().await.remove_client(client_id);
}
