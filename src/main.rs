mod commands;
mod config;
mod persist;
mod resp;
mod server;
mod store;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

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
    metrics::describe_gauge!("kvns_memory_used_bytes", "Memory currently used by the store in bytes, per namespace");
    metrics::describe_gauge!("kvns_memory_used_bytes_total", "Total memory currently used by the store in bytes across all namespaces");
    metrics::describe_gauge!("kvns_memory_limit_bytes", "Configured memory limit in bytes");
    metrics::describe_histogram!("kvns_command_duration_seconds", "Command processing latency in seconds");
    metrics::describe_counter!("kvns_evictions_total", "Number of keys evicted from the store");

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

    let addr = config.listen_addr();
    let listener = TcpListener::bind(&addr).await.expect("failed to bind");
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
                        tokio::spawn(server::handle_connection(stream, Arc::clone(&store)));
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
                        tokio::spawn(server::handle_connection(stream, Arc::clone(&store)));
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

    if let Some(ref path) = config.persist_path {
        info!(path = %path, "flushing store to disk on shutdown");
        let db = store.read().await;
        match persist::save(&db, &PathBuf::from(path)) {
            Ok(()) => info!(path = %path, "store flushed to disk"),
            Err(e) => error!(error = %e, path = %path, "failed to flush store to disk on shutdown"),
        }
    }
}
