use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tracing::debug;

use crate::commands::dispatch;
use crate::resp::parse_resp;
use crate::store::Store;

pub(crate) async fn handle_connection(stream: TcpStream, store: Store) {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    loop {
        match parse_resp(&mut reader).await {
            Ok(None) => break,
            Ok(Some(args)) if args.is_empty() => continue,
            Ok(Some(args)) => {
                let (response, quit) = dispatch(&args, &store).await;
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
