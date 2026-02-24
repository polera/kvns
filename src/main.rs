mod commands;
mod config;
mod persist;
mod resp;
mod server;
mod sharded;
mod store;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::{RwLock, Semaphore};
use tracing::{debug, error, info, warn};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("KVNS_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = config::Config::from_env();

    let metrics_addr: SocketAddr = config
        .metrics_listen_addr()
        .parse()
        .expect("invalid metrics listen address");
    PrometheusBuilder::new()
        .with_http_listener(metrics_addr)
        .install()
        .expect("failed to install Prometheus exporter");

    metrics::describe_gauge!("kvns_keys_total", "Number of keys in the store");
    metrics::describe_gauge!(
        "kvns_memory_used_bytes",
        "Memory currently used by the store in bytes, per namespace"
    );
    metrics::describe_gauge!(
        "kvns_memory_used_bytes_total",
        "Total memory currently used by the store in bytes across all namespaces"
    );
    metrics::describe_gauge!(
        "kvns_memory_limit_bytes",
        "Configured memory limit in bytes"
    );
    metrics::describe_histogram!(
        "kvns_command_duration_seconds",
        "Command processing latency in seconds"
    );
    metrics::describe_counter!(
        "kvns_evictions_total",
        "Number of keys evicted from the store"
    );
    metrics::describe_counter!(
        "kvns_ear_evictions_total",
        "Number of keys deleted by the ExpireAfterRead sweep"
    );

    let (backend, classic_store) = if config.sharded_mode {
        if config.persist_path.is_some() {
            warn!("KVNS_PERSIST_PATH is ignored in sharded mode");
        }
        info!(
            shard_count = config.shard_count,
            "experimental sharded mode enabled (supports a subset of string commands; see README)"
        );
        (
            server::Backend::Sharded(sharded::ShardedDb::new(
                config.memory_limit,
                config.shard_count,
            )),
            None,
        )
    } else {
        // Load persisted state from disk, or start fresh.
        let initial_db = match &config.persist_path {
            None => store::Db::new(config.memory_limit),
            Some(path) => {
                let p = PathBuf::from(path);
                match persist::load(&p, config.memory_limit) {
                    Ok(db) => {
                        let key_count: usize = db.entries.values().map(|ns| ns.len()).sum();
                        info!(
                            path = %path,
                            keys = key_count,
                            "loaded store from disk"
                        );
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

        // Spawn background flush task if persistence is configured.
        if let Some(ref path) = config.persist_path {
            tokio::spawn(persist::run_periodic_flush(
                Arc::clone(&store),
                PathBuf::from(path),
                config.persist_interval_secs,
            ));
        }

        // Spawn EAR sweep task if any namespace uses ExpireAfterRead.
        if config.eviction_policy == config::EvictionPolicy::ExpireAfterRead
            || config
                .namespace_eviction_policies
                .values()
                .any(|p| *p == config::EvictionPolicy::ExpireAfterRead)
        {
            tokio::spawn(commands::run_ear_sweep(Arc::clone(&store)));
        }

        (server::Backend::Classic(Arc::clone(&store)), Some(store))
    };

    let addr = config.listen_addr();
    let listener = TcpListener::bind(&addr).await.expect("failed to bind");
    let max_clients = config.max_clients.max(1);
    let client_limiter = Arc::new(Semaphore::new(max_clients));
    let limits = server::ServerLimits {
        resp: resp::RespLimits {
            max_array_len: config.max_resp_args,
            max_bulk_len: config.max_resp_bulk_len,
            max_inline_len: config.max_resp_inline_len,
        },
    };
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
                        tokio::spawn(server::handle_connection(stream, backend.clone(), limits, permit));
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
                        tokio::spawn(server::handle_connection(stream, backend.clone(), limits, permit));
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

    if let (Some(path), Some(store)) = (config.persist_path.as_ref(), classic_store.as_ref()) {
        info!(path = %path, "flushing store to disk on shutdown");
        let snapshot = {
            let db = store.read().await;
            db.entries.clone()
        };
        let shutdown_path = PathBuf::from(path);
        match tokio::task::spawn_blocking(move || persist::save_entries(&snapshot, &shutdown_path))
            .await
        {
            Ok(Ok(())) => info!(path = %path, "store flushed to disk"),
            Ok(Err(e)) => {
                error!(error = %e, path = %path, "failed to flush store to disk on shutdown")
            }
            Err(e) => {
                error!(error = %e, path = %path, "shutdown flush task join error")
            }
        }
    }
}
