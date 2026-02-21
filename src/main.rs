mod commands;
mod config;
mod resp;
mod server;
mod store;

use std::net::SocketAddr;
use std::sync::Arc;

use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

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

    let store = Arc::new(RwLock::new(store::Db::new(config.memory_limit)));
    let addr = config.listen_addr();
    let listener = TcpListener::bind(&addr).await.expect("failed to bind");
    info!(addr = %addr, "kvns listening");

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                debug!(%peer, "accepted connection");
                tokio::spawn(server::handle_connection(stream, Arc::clone(&store)));
            }
            Err(e) => error!(?e, "accept error"),
        }
    }
}
