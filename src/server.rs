use std::sync::atomic::{AtomicU64, Ordering};

use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tracing::debug;

use crate::commands::{ConnState, dispatch};
use crate::resp::parse_resp;
use crate::store::Store;

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) async fn handle_connection(stream: TcpStream, store: Store) {
    let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
    let mut conn = ConnState::new(client_id);
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    loop {
        match parse_resp(&mut reader).await {
            Ok(None) => break,
            Ok(Some(args)) if args.is_empty() => continue,
            Ok(Some(args)) => {
                let (response, quit) = dispatch(&args, &store, &mut conn).await;
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
