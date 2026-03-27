mod commands;
mod config;
mod persist;
mod pubsub;
mod resp;
mod server;
mod sharded;
mod store;
#[cfg(target_os = "linux")]
mod uring_server;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::sync::{RwLock, Semaphore};
use tracing::{error, info, warn};

// ── Shared helpers ───────────────────────────────────────────────────────────

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("KVNS_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}

fn install_metrics(config: &config::Config) {
    let metrics_addr: SocketAddr = config
        .metrics_listen_addr()
        .parse()
        .expect("invalid metrics listen address");
    PrometheusBuilder::new()
        .with_http_listener(metrics_addr)
        .install()
        .expect("failed to install Prometheus exporter");

    metrics::describe_gauge!("kvns_keys_total", "Number of keys in the store");
    metrics::describe_gauge!("kvns_memory_used_bytes", "Memory currently used by the store in bytes, per namespace");
    metrics::describe_gauge!("kvns_memory_used_bytes_total", "Total memory currently used by the store in bytes across all namespaces");
    metrics::describe_gauge!("kvns_memory_limit_bytes", "Configured memory limit in bytes");
    metrics::describe_histogram!("kvns_command_duration_seconds", "Command processing latency in seconds");
    metrics::describe_counter!("kvns_evictions_total", "Number of keys evicted from the store");
    metrics::describe_counter!("kvns_ear_evictions_total", "Number of keys deleted by the ExpireAfterRead sweep");
}

/// Build the backend (sharded or classic) and optionally return the classic store
/// for persistence / EAR sweep.  Spawns background tasks onto the current tokio runtime.
fn build_backend(config: &config::Config) -> (server::Backend, Option<Arc<RwLock<store::Db>>>) {
    if config.sharded_mode {
        if config.persist_path.is_some() {
            warn!("KVNS_PERSIST_PATH is ignored in sharded mode");
        }
        info!(shard_count = config.shard_count, "experimental sharded mode enabled");
        (
            server::Backend::Sharded(sharded::ShardedDb::new(config.memory_limit, config.shard_count)),
            None,
        )
    } else {
        let initial_db = match &config.persist_path {
            None => store::Db::new(config.memory_limit),
            Some(path) => {
                let p = PathBuf::from(path);
                match persist::load(&p, config.memory_limit) {
                    Ok(db) => {
                        let key_count: usize = db.entries.values().map(|ns| ns.len()).sum();
                        info!(path = %path, keys = key_count, "loaded store from disk");
                        db
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        info!(path = %path, "no existing store file, starting fresh");
                        store::Db::new(config.memory_limit)
                    }
                    Err(e) => {
                        warn!(error = %e, path = %path, "failed to load store from disk, starting fresh");
                        store::Db::new(config.memory_limit)
                    }
                }
            }
        };
        let initial_db = initial_db.with_eviction(
            config.eviction_threshold,
            config.eviction_policy.clone(),
            config.namespace_eviction_policies.clone(),
        );
        let store = Arc::new(RwLock::new(initial_db));
        if let Some(ref path) = config.persist_path {
            tokio::spawn(persist::run_periodic_flush(
                Arc::clone(&store),
                PathBuf::from(path),
                config.persist_interval_secs,
            ));
        }
        if config.eviction_policy == config::EvictionPolicy::ExpireAfterRead
            || config.namespace_eviction_policies.values().any(|p| *p == config::EvictionPolicy::ExpireAfterRead)
        {
            tokio::spawn(commands::run_ear_sweep(Arc::clone(&store)));
        }
        (server::Backend::Classic(Arc::clone(&store)), Some(store))
    }
}

fn build_server_limits(config: &config::Config) -> server::ServerLimits {
    server::ServerLimits {
        resp: resp::RespLimits {
            max_array_len: config.max_resp_args,
            max_bulk_len: config.max_resp_bulk_len,
            max_inline_len: config.max_resp_inline_len,
        },
    }
}

async fn shutdown_flush(persist_path: Option<&String>, classic_store: Option<&Arc<RwLock<store::Db>>>) {
    if let (Some(path), Some(store)) = (persist_path, classic_store) {
        info!(path = %path, "flushing store to disk on shutdown");
        let snapshot = { let db = store.read().await; db.entries.clone() };
        let shutdown_path = PathBuf::from(path);
        match tokio::task::spawn_blocking(move || persist::save_entries(&snapshot, &shutdown_path)).await {
            Ok(Ok(())) => info!(path = %path, "store flushed to disk"),
            Ok(Err(e)) => error!(error = %e, path = %path, "failed to flush store to disk on shutdown"),
            Err(e) => error!(error = %e, path = %path, "shutdown flush task join error"),
        }
    }
}

// ── Linux entry point: thread-per-core io_uring ──────────────────────────────
//
// Each worker thread owns its own io_uring ring and a SO_REUSEPORT listener on
// the same address. The kernel distributes incoming connections across rings,
// so there is zero cross-thread coordination on the accept path.
#[cfg(target_os = "linux")]
fn main() {
    use tracing::debug;
    init_tracing();

    let config = config::Config::from_env();
    let addr = config.listen_addr();

    // Number of worker threads = available hardware parallelism.
    let num_workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    // Everything else is set up inside tokio_uring::start on the first thread,
    // then cloned into subsequent threads.  We use a channel to hand the
    // constructed shared state to the worker threads.
    let (tx, rx) = std::sync::mpsc::sync_channel::<(server::Backend, pubsub::PubSubHub, Arc<Semaphore>, server::ServerLimits, Option<Arc<RwLock<store::Db>>>, Option<String>)>(0);

    // Thread 0: build shared state, then run the accept loop.
    let addr0 = addr.clone();
    let config0 = config.clone();
    let tx_clone = tx.clone();
    let t0 = std::thread::spawn(move || {
        tokio_uring::start(async move {
            install_metrics(&config0);
            let (backend, classic_store) = build_backend(&config0);

            let hub = pubsub::new_hub();
            let max_clients = config0.max_clients.max(1);
            let client_limiter = Arc::new(Semaphore::new(max_clients));
            let limits = build_server_limits(&config0);
            let persist_path = config0.persist_path.clone();

            // Distribute shared state to worker threads (num_workers - 1 times).
            for _ in 1..num_workers {
                let _ = tx_clone.send((backend.clone(), hub.clone(), Arc::clone(&client_limiter), limits, classic_store.as_ref().map(Arc::clone), persist_path.clone()));
            }
            drop(tx_clone);

            info!(addr = %addr0, workers = num_workers, "kvns listening (io_uring)");
            let listener = uring_server::make_listener(&addr0).expect("failed to bind");

            #[cfg(unix)]
            let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("failed to install SIGTERM handler");

            loop {
                #[cfg(unix)]
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, peer)) => {
                                debug!(%peer, "accepted connection");
                                let permit = match Arc::clone(&client_limiter).try_acquire_owned() {
                                    Ok(p) => p,
                                    Err(_) => { debug!(%peer, "connection rejected: max clients reached"); continue; }
                                };
                                tokio_uring::spawn(uring_server::handle_connection(stream, backend.clone(), limits, permit, hub.clone()));
                            }
                            Err(e) => error!(?e, "accept error"),
                        }
                    }
                    _ = tokio::signal::ctrl_c() => { info!("received SIGINT, shutting down"); break; }
                    _ = sigterm.recv() => { info!("received SIGTERM, shutting down"); break; }
                }
                #[cfg(not(unix))]
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, peer)) => {
                                debug!(%peer, "accepted connection");
                                let permit = match Arc::clone(&client_limiter).try_acquire_owned() {
                                    Ok(p) => p,
                                    Err(_) => { debug!(%peer, "connection rejected: max clients reached"); continue; }
                                };
                                tokio_uring::spawn(uring_server::handle_connection(stream, backend.clone(), limits, permit, hub.clone()));
                            }
                            Err(e) => error!(?e, "accept error"),
                        }
                    }
                    _ = tokio::signal::ctrl_c() => { info!("received SIGINT, shutting down"); break; }
                }
            }

            shutdown_flush(persist_path.as_ref(), classic_store.as_ref()).await;
        });
    });

    // Worker threads 1..N: wait for shared state then run their own accept loops.
    let worker_handles: Vec<_> = (1..num_workers).map(|_| {
        let state = rx.recv().expect("state channel closed unexpectedly");
        let addr_w = addr.clone();
        std::thread::spawn(move || {
            let (backend, hub, client_limiter, limits, _classic_store, _persist_path) = state;
            tokio_uring::start(async move {
                let listener = uring_server::make_listener(&addr_w).expect("failed to bind");
                let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("failed to install SIGTERM handler");
                loop {
                    tokio::select! {
                        result = listener.accept() => {
                            match result {
                                Ok((stream, _peer)) => {
                                    let permit = match Arc::clone(&client_limiter).try_acquire_owned() {
                                        Ok(p) => p,
                                        Err(_) => continue,
                                    };
                                    tokio_uring::spawn(uring_server::handle_connection(stream, backend.clone(), limits, permit, hub.clone()));
                                }
                                Err(e) => { error!(?e, "worker accept error"); break; }
                            }
                        }
                        _ = tokio::signal::ctrl_c() => break,
                        _ = sigterm.recv() => break,
                    }
                }
            });
        })
    }).collect();

    t0.join().ok();
    for h in worker_handles { h.join().ok(); }
}

// ── Non-Linux entry point: existing tokio + epoll path ───────────────────────
#[cfg(not(target_os = "linux"))]
#[tokio::main]
async fn main() {
    use tokio::net::TcpListener;
    use tokio::signal;
    use tracing::debug;
    init_tracing();

    let config = config::Config::from_env();
    install_metrics(&config);
    let (backend, classic_store) = build_backend(&config);

    let hub = pubsub::new_hub();
    let addr = config.listen_addr();
    let listener = TcpListener::bind(&addr).await.expect("failed to bind");
    let max_clients = config.max_clients.max(1);
    let client_limiter = Arc::new(Semaphore::new(max_clients));
    let limits = build_server_limits(&config);
    info!(addr = %addr, "kvns listening");

    #[cfg(unix)]
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");

    loop {
        #[cfg(unix)]
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, peer)) => {
                        debug!(%peer, "accepted connection");
                        let permit = match Arc::clone(&client_limiter).try_acquire_owned() {
                            Ok(p) => p,
                            Err(_) => {
                                debug!(
                                    %peer,
                                    max_clients,
                                    "connection rejected: max clients reached"
                                );
                                continue;
                            }
                        };
                        tokio::spawn(server::handle_connection(stream, backend.clone(), limits, permit, hub.clone()));
                    }
                    Err(e) => error!(?e, "accept error"),
                }
            }
            _ = signal::ctrl_c() => {
                info!("received SIGINT, shutting down");
                break;
            }
            _ = sigterm.recv() => {
                info!("received SIGTERM, shutting down");
                break;
            }
        }

        #[cfg(not(unix))]
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, peer)) => {
                        debug!(%peer, "accepted connection");
                        let permit = match Arc::clone(&client_limiter).try_acquire_owned() {
                            Ok(p) => p,
                            Err(_) => {
                                debug!(
                                    %peer,
                                    max_clients,
                                    "connection rejected: max clients reached"
                                );
                                continue;
                            }
                        };
                        tokio::spawn(server::handle_connection(stream, backend.clone(), limits, permit, hub.clone()));
                    }
                    Err(e) => error!(?e, "accept error"),
                }
            }
            _ = signal::ctrl_c() => {
                info!("received SIGINT, shutting down");
                break;
            }
        }
    }

    shutdown_flush(config.persist_path.as_ref(), classic_store.as_ref()).await;
}
