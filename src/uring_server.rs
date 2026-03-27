//! io_uring connection handler — Linux only.
//!
//! Uses tokio-uring's completion-based I/O:
//!   - `TcpStream::read(owned_buf)` / `write_all(owned_buf)` avoid epoll round-trips.
//!   - A synchronous RESP parser works directly on buffered bytes (no async traits).
//!   - All pipelined responses are coalesced into a single `write_all` syscall.
//!
//! The accept loop lives in `main.rs` (Linux path). Each worker thread runs its own
//! `tokio_uring::start` with a dedicated io_uring ring. SO_REUSEPORT distributes
//! incoming connections across rings at the kernel level.

use std::sync::atomic::{AtomicU64, Ordering};

use socket2::{Domain, Protocol, Socket, Type};
use tokio::sync::{OwnedSemaphorePermit, mpsc};
use tokio_uring::net::TcpStream;
use tracing::debug;

use crate::commands::{ConnState, dispatch as dispatch_classic};
use crate::pubsub::{PubSubHub, PubSubMessage};
use crate::resp::parse_resp_sync;
use crate::server::{Backend, ServerLimits};
use crate::sharded::dispatch as dispatch_sharded_sync;

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

const READ_BUF_CAP: usize = 64 * 1024;
const WRITE_BUF_CAP: usize = 64 * 1024;

/// Build a non-blocking TCP listener socket with SO_REUSEPORT so that multiple
/// threads can each bind to the same address and the kernel load-balances accepts.
pub(crate) fn make_listener(addr: &str) -> std::io::Result<tokio_uring::net::TcpListener> {
    let addr: std::net::SocketAddr = addr
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let domain = if addr.is_ipv6() {
        Domain::IPV6
    } else {
        Domain::IPV4
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    let std_listener: std::net::TcpListener = socket.into();
    Ok(tokio_uring::net::TcpListener::from_std(std_listener))
}

/// Per-connection handler using io_uring owned-buffer I/O.
///
/// Lifecycle:
///   1. Accumulate incoming bytes into `accum`.
///   2. Synchronously parse all complete RESP commands from `accum`.
///   3. Dispatch each command; collect responses into `write_buf`.
///   4. Flush `write_buf` with a single `write_all` (one syscall for pipelined batches).
///   5. Read more data; goto 2.
pub(crate) async fn handle_connection(
    stream: TcpStream,
    backend: Backend,
    limits: ServerLimits,
    _permit: OwnedSemaphorePermit,
    hub: PubSubHub,
) {
    let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
    let mut conn = ConnState::new(client_id);
    let mut pubsub_rx: Option<mpsc::UnboundedReceiver<PubSubMessage>> = None;

    // Accumulation buffer. `head` is the index of the first unprocessed byte.
    // We compact (drain or clear) only when head crosses half the buffer length,
    // amortising the O(N) shift to at most one per 2× consumed bytes.
    let mut accum: Vec<u8> = Vec::with_capacity(READ_BUF_CAP);
    let mut head = 0usize;
    // Arg slots reused across commands (outer Vec capacity + inner Vec capacity both reused).
    let mut args: Vec<Vec<u8>> = Vec::with_capacity(8);
    // Response bytes batched before a single write_all.
    let mut write_buf: Vec<u8> = Vec::with_capacity(WRITE_BUF_CAP);

    'conn: loop {
        // ── Parse phase ──────────────────────────────────────────────────────
        write_buf.clear();

        loop {
            match parse_resp_sync(&accum[head..], limits.resp, &mut args) {
                Ok(Some(consumed)) => {
                    head += consumed;
                    if args.is_empty() {
                        continue;
                    }
                    // Pubsub mode: flush each response immediately so the client
                    // sees command confirmations without waiting for a batch flush.
                    // This intentional per-command write_all is correct — do not
                    // merge into the batched write_buf path.
                    if conn.in_pubsub() {
                        if !write_buf.is_empty() {
                            let buf = std::mem::take(&mut write_buf);
                            let (res, buf) = stream.write_all(buf).await;
                            write_buf = buf;
                            write_buf.clear();
                            if res.is_err() {
                                break 'conn;
                            }
                        }
                        let (response, quit) = match &backend {
                            Backend::Classic(store) => {
                                dispatch_classic(&args, store, &mut conn, &hub).await
                            }
                            Backend::Sharded(store) => dispatch_sharded_sync(&mut args, store),
                        };
                        if pubsub_rx.is_none() {
                            pubsub_rx = conn.pubsub_rx_slot.take();
                        }
                        let (res, _) = stream.write_all(response.into_owned()).await;
                        if res.is_err() || quit {
                            break 'conn;
                        }
                        continue;
                    }

                    let (response, quit) = match &backend {
                        Backend::Classic(store) => {
                            dispatch_classic(&args, store, &mut conn, &hub).await
                        }
                        Backend::Sharded(store) => dispatch_sharded_sync(&mut args, store),
                    };
                    if pubsub_rx.is_none() {
                        pubsub_rx = conn.pubsub_rx_slot.take();
                    }
                    write_buf.extend_from_slice(&response);
                    if quit {
                        let (_, _) = stream.write_all(std::mem::take(&mut write_buf)).await;
                        break 'conn;
                    }
                }
                Ok(None) => break, // incomplete frame — need more data
                Err(e) => {
                    debug!(error = %e, "parse error, closing connection");
                    break 'conn;
                }
            }
        }

        // Compact the accumulator. When fully consumed, clear is O(1).
        // Otherwise only shift when head has passed half the buffer to amortise cost.
        if head == accum.len() {
            accum.clear();
            head = 0;
        } else if head > accum.len() / 2 {
            accum.drain(..head);
            head = 0;
        }

        // ── Write phase ──────────────────────────────────────────────────────
        // One write_all covers all pipelined responses — one syscall per batch.
        if !write_buf.is_empty() {
            let buf = std::mem::take(&mut write_buf);
            let (res, buf) = stream.write_all(buf).await;
            write_buf = buf;
            write_buf.clear();
            if res.is_err() {
                break;
            }
        }

        // ── Read phase ───────────────────────────────────────────────────────
        // Hand `accum`'s spare capacity directly to io_uring. The kernel DMA-fills
        // the uninitialized region after `accum.len()` and tokio-uring calls
        // `set_init(n)` to extend the length — zero intermediate copies.
        accum.reserve(READ_BUF_CAP);
        let (res, buf) = stream.read(accum).await;
        accum = buf;
        match res {
            Ok(0) => break, // EOF
            Ok(_) => {}     // bytes appended directly into accum by io_uring
            Err(e) => {
                debug!(error = %e, "read error, closing connection");
                break;
            }
        }
    }

    hub.write().await.remove_client(client_id);
}
