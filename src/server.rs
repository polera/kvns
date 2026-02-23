use std::sync::atomic::{AtomicU64, Ordering};

use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tracing::debug;

use crate::commands::{ConnState, dispatch as dispatch_classic};
use crate::resp::parse_resp;
use crate::sharded::{ShardedStore, dispatch as dispatch_sharded};
use crate::store::Store;

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub(crate) enum Backend {
    Classic(Store),
    Sharded(ShardedStore),
}

pub(crate) async fn handle_connection(stream: TcpStream, backend: Backend) {
    let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
    let mut conn = ConnState::new(client_id);
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    loop {
        match parse_resp(&mut reader).await {
            Ok(None) => break,
            Ok(Some(args)) if args.is_empty() => continue,
            Ok(Some(args)) => {
                let (response, quit) = match &backend {
                    Backend::Classic(store) => dispatch_classic(&args, store, &mut conn).await,
                    Backend::Sharded(store) => dispatch_sharded(&args, store).await,
                };
                if write_half.write_all(&response).await.is_err() || quit {
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
