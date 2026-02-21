use tokio::io::{AsyncBufReadExt, AsyncReadExt};

pub(crate) async fn parse_resp<R: AsyncBufReadExt + Unpin>(
    reader: &mut R,
) -> std::io::Result<Option<Vec<Vec<u8>>>> {
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Ok(None);
    }
    let trimmed = line.trim_end_matches(['\r', '\n']);
    if trimmed.is_empty() {
        return Ok(Some(vec![]));
    }
    if let Some(rest) = trimmed.strip_prefix('*') {
        let count: usize = rest
            .parse()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "bad count"))?;
        let mut args = Vec::with_capacity(count);
        for _ in 0..count {
            let mut hdr = String::new();
            reader.read_line(&mut hdr).await?;
            let hdr = hdr.trim_end_matches(['\r', '\n']);
            let len: i64 = hdr
                .strip_prefix('$')
                .ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "expected $")
                })?
                .parse()
                .map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "bad len")
                })?;
            if len < 0 {
                args.push(vec![]);
            } else {
                let mut buf = vec![0u8; len as usize + 2]; // +2 for \r\n
                reader.read_exact(&mut buf).await?;
                buf.truncate(len as usize);
                args.push(buf);
            }
        }
        Ok(Some(args))
    } else {
        // inline command (e.g. PING without framing)
        Ok(Some(
            trimmed
                .split_whitespace()
                .map(|s| s.as_bytes().to_vec())
                .collect(),
        ))
    }
}

pub(crate) fn resp_ok() -> Vec<u8>           { b"+OK\r\n".to_vec() }
pub(crate) fn resp_pong() -> Vec<u8>         { b"+PONG\r\n".to_vec() }
pub(crate) fn resp_null() -> Vec<u8>         { b"$-1\r\n".to_vec() }
pub(crate) fn resp_int(n: i64) -> Vec<u8>    { format!(":{n}\r\n").into_bytes() }
pub(crate) fn resp_err(msg: &str) -> Vec<u8> { format!("-ERR {msg}\r\n").into_bytes() }
pub(crate) fn resp_wrongtype() -> Vec<u8>    { b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec() }

pub(crate) fn resp_bulk(data: &[u8]) -> Vec<u8> {
    let mut out = format!("${}\r\n", data.len()).into_bytes();
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
    out
}

pub(crate) fn resp_array(items: &[Vec<u8>]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", items.len()).into_bytes();
    for item in items {
        out.extend_from_slice(&resp_bulk(item));
    }
    out
}

pub(crate) fn wrong_args(cmd: &[u8]) -> Vec<u8> {
    resp_err(&format!(
        "wrong number of arguments for {}",
        String::from_utf8_lossy(cmd)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn parse_array_set_command() {
        let data = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let mut r = BufReader::new(&data[..]);
        let result = parse_resp(&mut r).await.unwrap().unwrap();
        assert_eq!(result, vec![b"SET", b"foo", b"bar"]);
    }

    #[tokio::test]
    async fn parse_inline_ping() {
        let data = b"PING\r\n";
        let mut r = BufReader::new(&data[..]);
        let result = parse_resp(&mut r).await.unwrap().unwrap();
        assert_eq!(result, vec![b"PING"]);
    }

    #[tokio::test]
    async fn parse_inline_with_args() {
        let data = b"GET mykey\r\n";
        let mut r = BufReader::new(&data[..]);
        let result = parse_resp(&mut r).await.unwrap().unwrap();
        assert_eq!(result[0], b"GET");
        assert_eq!(result[1], b"mykey");
    }

    #[tokio::test]
    async fn parse_eof_returns_none() {
        let data: &[u8] = b"";
        let mut r = BufReader::new(data);
        assert!(parse_resp(&mut r).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn parse_empty_line_returns_empty_vec() {
        let data = b"\r\n";
        let mut r = BufReader::new(&data[..]);
        let result = parse_resp(&mut r).await.unwrap().unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn parse_null_bulk_string() {
        let data = b"*2\r\n$3\r\nGET\r\n$-1\r\n";
        let mut r = BufReader::new(&data[..]);
        let result = parse_resp(&mut r).await.unwrap().unwrap();
        assert_eq!(result[0], b"GET");
        assert_eq!(result[1], b"");
    }
}
