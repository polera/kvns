use std::borrow::Cow;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};

use crate::config::{
    DEFAULT_MAX_RESP_ARGS, DEFAULT_MAX_RESP_BULK_LEN, DEFAULT_MAX_RESP_INLINE_LEN,
};

#[derive(Clone, Copy, Debug)]
pub(crate) struct RespLimits {
    pub max_array_len: usize,
    pub max_bulk_len: usize,
    pub max_inline_len: usize,
}

impl Default for RespLimits {
    fn default() -> Self {
        Self {
            max_array_len: DEFAULT_MAX_RESP_ARGS,
            max_bulk_len: DEFAULT_MAX_RESP_BULK_LEN,
            max_inline_len: DEFAULT_MAX_RESP_INLINE_LEN,
        }
    }
}

fn invalid_data(msg: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg)
}

fn parse_i64(bytes: &[u8], err_msg: &'static str) -> std::io::Result<i64> {
    let s = std::str::from_utf8(bytes).map_err(|_| invalid_data(err_msg))?;
    s.parse::<i64>().map_err(|_| invalid_data(err_msg))
}

fn parse_bulk_len(hdr: &[u8]) -> std::io::Result<i64> {
    let body = hdr
        .strip_prefix(b"$")
        .ok_or_else(|| invalid_data("expected $"))?;
    parse_i64(body, "bad len")
}

async fn read_resp_line<'a, R: AsyncBufRead + Unpin>(
    reader: &mut R,
    buf: &'a mut Vec<u8>,
    max_line_len: usize,
) -> std::io::Result<Option<&'a [u8]>> {
    buf.clear();
    loop {
        let chunk = reader.fill_buf().await?;
        if chunk.is_empty() {
            if buf.is_empty() {
                return Ok(None);
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected EOF",
            ));
        }
        if let Some(pos) = chunk.iter().position(|&b| b == b'\n') {
            let take = pos + 1;
            if buf.len().saturating_add(take) > max_line_len.saturating_add(2) {
                return Err(invalid_data("line too long"));
            }
            buf.extend_from_slice(&chunk[..take]);
            reader.consume(take);
            break;
        }
        if buf.len().saturating_add(chunk.len()) > max_line_len.saturating_add(2) {
            return Err(invalid_data("line too long"));
        }
        let take = chunk.len();
        buf.extend_from_slice(chunk);
        reader.consume(take);
    }
    if buf.ends_with(b"\n") {
        buf.pop();
        if buf.ends_with(b"\r") {
            buf.pop();
        }
    }
    if buf.len() > max_line_len {
        return Err(invalid_data("line too long"));
    }
    Ok(Some(buf.as_slice()))
}

fn split_inline_command(line: &[u8]) -> Vec<Vec<u8>> {
    let mut out: Vec<Vec<u8>> = Vec::new();
    let mut i = 0usize;
    while i < line.len() {
        while i < line.len() && line[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= line.len() {
            break;
        }
        let start = i;
        while i < line.len() && !line[i].is_ascii_whitespace() {
            i += 1;
        }
        out.push(line[start..i].to_vec());
    }
    out
}

pub(crate) async fn parse_resp<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> std::io::Result<Option<Vec<Vec<u8>>>> {
    parse_resp_with_limits(reader, RespLimits::default()).await
}

pub(crate) async fn parse_resp_with_limits<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    limits: RespLimits,
) -> std::io::Result<Option<Vec<Vec<u8>>>> {
    let mut line = Vec::new();
    let Some(trimmed) = read_resp_line(reader, &mut line, limits.max_inline_len).await? else {
        return Ok(None);
    };
    if trimmed.is_empty() {
        return Ok(Some(vec![]));
    }

    let first = trimmed.first().copied().unwrap_or(0);
    let rest = &trimmed[1..];

    match first {
        // ── RESP2 / RESP3 array ─────────────────────────────────────────────
        b'*' => {
            let count = parse_i64(rest, "bad count")?;
            if count < 0 {
                return Ok(Some(vec![])); // null array
            }
            let count = usize::try_from(count).map_err(|_| invalid_data("bad count"))?;
            if count > limits.max_array_len {
                return Err(invalid_data("too many bulk strings"));
            }
            let mut args = Vec::with_capacity(count as usize);
            let mut hdr = Vec::new();
            for _ in 0..count {
                let Some(hdr_line) =
                    read_resp_line(reader, &mut hdr, limits.max_inline_len).await?
                else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "unexpected EOF",
                    ));
                };
                let len = parse_bulk_len(hdr_line)?;
                if len < 0 {
                    args.push(vec![]);
                } else {
                    let len = usize::try_from(len).map_err(|_| invalid_data("bad len"))?;
                    if len > limits.max_bulk_len {
                        return Err(invalid_data("bulk string too large"));
                    }
                    let mut buf = vec![0u8; len];
                    reader.read_exact(&mut buf).await?;
                    let mut crlf = [0u8; 2];
                    reader.read_exact(&mut crlf).await?;
                    args.push(buf);
                }
            }
            Ok(Some(args))
        }

        // ── RESP3 null ──────────────────────────────────────────────────────
        b'_' => Ok(Some(vec![])),

        // ── RESP3 boolean ───────────────────────────────────────────────────
        b'#' => {
            let val = if rest == b"t" {
                b"1".to_vec()
            } else {
                b"0".to_vec()
            };
            Ok(Some(vec![val]))
        }

        // ── RESP3 double ────────────────────────────────────────────────────
        b',' => Ok(Some(vec![rest.to_vec()])),

        // ── RESP3 big number ────────────────────────────────────────────────
        b'(' => Ok(Some(vec![rest.to_vec()])),

        // ── RESP3 set type (~N) — treat like array ──────────────────────────
        b'~' => {
            let count = parse_i64(rest, "bad count")?;
            if count < 0 {
                return Err(invalid_data("bad count"));
            }
            let count = usize::try_from(count).map_err(|_| invalid_data("bad count"))?;
            if count > limits.max_array_len {
                return Err(invalid_data("too many bulk strings"));
            }
            read_bulk_strings(reader, count, limits).await.map(Some)
        }

        // ── RESP3 map type (%N) — read 2N bulk strings ──────────────────────
        b'%' => {
            let count = parse_i64(rest, "bad count")?;
            if count < 0 {
                return Err(invalid_data("bad count"));
            }
            let count = usize::try_from(count).map_err(|_| invalid_data("bad count"))?;
            if count > limits.max_array_len / 2 {
                return Err(invalid_data("too many bulk strings"));
            }
            let count = count
                .checked_mul(2)
                .ok_or_else(|| invalid_data("bad count"))?;
            read_bulk_strings(reader, count, limits).await.map(Some)
        }

        // ── Inline command ──────────────────────────────────────────────────
        _ => {
            let out = split_inline_command(trimmed);
            if out.len() > limits.max_array_len {
                return Err(invalid_data("too many inline arguments"));
            }
            Ok(Some(out))
        }
    }
}

/// Read exactly `n` RESP bulk strings from `reader`.
async fn read_bulk_strings<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    n: usize,
    limits: RespLimits,
) -> std::io::Result<Vec<Vec<u8>>> {
    let mut args = Vec::with_capacity(n);
    let mut hdr = Vec::new();
    for _ in 0..n {
        let Some(hdr_line) = read_resp_line(reader, &mut hdr, limits.max_inline_len).await? else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected EOF",
            ));
        };
        let len = parse_bulk_len(hdr_line)?;
        if len < 0 {
            args.push(vec![]);
        } else {
            let len = usize::try_from(len).map_err(|_| invalid_data("bad len"))?;
            if len > limits.max_bulk_len {
                return Err(invalid_data("bulk string too large"));
            }
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf).await?;
            let mut crlf = [0u8; 2];
            reader.read_exact(&mut crlf).await?;
            args.push(buf);
        }
    }
    Ok(args)
}

// ── RESP2 response builders ───────────────────────────────────────────────────

pub(crate) fn resp_ok() -> Cow<'static, [u8]> {
    Cow::Borrowed(b"+OK\r\n")
}
pub(crate) fn resp_pong() -> Cow<'static, [u8]> {
    Cow::Borrowed(b"+PONG\r\n")
}
pub(crate) fn resp_null() -> Cow<'static, [u8]> {
    Cow::Borrowed(b"$-1\r\n")
}
pub(crate) fn resp_int(n: i64) -> Cow<'static, [u8]> {
    Cow::Owned(format!(":{n}\r\n").into_bytes())
}
pub(crate) fn resp_usize(n: usize) -> Cow<'static, [u8]> {
    resp_int(i64::try_from(n).unwrap_or(i64::MAX))
}
pub(crate) fn resp_err(msg: &str) -> Cow<'static, [u8]> {
    Cow::Owned(format!("-ERR {msg}\r\n").into_bytes())
}
pub(crate) fn resp_wrongtype() -> Cow<'static, [u8]> {
    Cow::Borrowed(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
}

pub(crate) fn append_array_header(out: &mut Vec<u8>, len: usize) {
    out.push(b'*');
    out.extend_from_slice(len.to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
}

pub(crate) fn append_int(out: &mut Vec<u8>, n: i64) {
    out.push(b':');
    out.extend_from_slice(n.to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
}

pub(crate) fn append_null(out: &mut Vec<u8>) {
    out.extend_from_slice(b"$-1\r\n");
}

pub(crate) fn append_bulk(out: &mut Vec<u8>, data: &[u8]) {
    out.push(b'$');
    out.extend_from_slice(data.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
}

pub(crate) fn resp_bulk(data: &[u8]) -> Cow<'static, [u8]> {
    let mut out = Vec::with_capacity(data.len() + 32);
    append_bulk(&mut out, data);
    Cow::Owned(out)
}

pub(crate) fn resp_array(items: &[Vec<u8>]) -> Cow<'static, [u8]> {
    let mut out = Vec::new();
    append_array_header(&mut out, items.len());
    for item in items {
        append_bulk(&mut out, item);
    }
    Cow::Owned(out)
}

pub(crate) fn resp_null_array() -> Cow<'static, [u8]> {
    Cow::Borrowed(b"*-1\r\n")
}

pub(crate) fn wrong_args(cmd: &[u8]) -> Cow<'static, [u8]> {
    resp_err(&format!(
        "wrong number of arguments for {}",
        String::from_utf8_lossy(cmd)
    ))
}

// ── RESP3 response builders ───────────────────────────────────────────────────

#[allow(dead_code)]
pub(crate) fn resp_null_resp3() -> Cow<'static, [u8]> {
    Cow::Borrowed(b"_\r\n")
}

#[allow(dead_code)]
pub(crate) fn resp_bool(b: bool) -> Cow<'static, [u8]> {
    if b {
        Cow::Borrowed(b"#t\r\n")
    } else {
        Cow::Borrowed(b"#f\r\n")
    }
}

#[allow(dead_code)]
pub(crate) fn resp_double(f: f64) -> Cow<'static, [u8]> {
    if f.is_nan() {
        Cow::Borrowed(b",nan\r\n")
    } else if f.is_infinite() {
        if f > 0.0 {
            Cow::Borrowed(b",inf\r\n")
        } else {
            Cow::Borrowed(b",-inf\r\n")
        }
    } else {
        Cow::Owned(format!(",{f}\r\n").into_bytes())
    }
}

#[allow(dead_code)]
pub(crate) fn resp_big_number(n: &str) -> Cow<'static, [u8]> {
    Cow::Owned(format!("({n}\r\n").into_bytes())
}

#[allow(dead_code)]
pub(crate) fn resp_blob_error(code: &str, msg: &str) -> Cow<'static, [u8]> {
    let payload = format!("{code} {msg}");
    let mut out = format!("!{}\r\n", payload.len()).into_bytes();
    out.extend_from_slice(payload.as_bytes());
    out.extend_from_slice(b"\r\n");
    Cow::Owned(out)
}

pub(crate) fn resp_verbatim(enc: &[u8; 3], data: &[u8]) -> Cow<'static, [u8]> {
    // =<len>\r\n<enc>:<data>\r\n
    let payload_len = 3 + 1 + data.len(); // enc(3) + ':' + data
    let mut out = format!("={payload_len}\r\n").into_bytes();
    out.extend_from_slice(enc);
    out.push(b':');
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
    Cow::Owned(out)
}

/// RESP3 map: %<N>\r\n followed by alternating bulk key/value pairs.
pub(crate) fn resp_map(pairs: &[(Vec<u8>, Vec<u8>)]) -> Cow<'static, [u8]> {
    let mut out = format!("%{}\r\n", pairs.len()).into_bytes();
    for (k, v) in pairs {
        append_bulk(&mut out, k);
        append_bulk(&mut out, v);
    }
    Cow::Owned(out)
}

/// RESP3 set type: ~<N>\r\n followed by bulk items.
#[allow(dead_code)]
pub(crate) fn resp_set_type(items: &[Vec<u8>]) -> Cow<'static, [u8]> {
    let mut out = format!("~{}\r\n", items.len()).into_bytes();
    for item in items {
        append_bulk(&mut out, item);
    }
    Cow::Owned(out)
}

/// RESP3 push type: ><N>\r\n followed by bulk items.
#[allow(dead_code)]
pub(crate) fn resp_push(items: &[Vec<u8>]) -> Cow<'static, [u8]> {
    let mut out = format!(">{}\r\n", items.len()).into_bytes();
    for item in items {
        append_bulk(&mut out, item);
    }
    Cow::Owned(out)
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
        assert_eq!(&*result[0], b"GET");
        assert_eq!(&*result[1], b"mykey");
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
        assert_eq!(&*result[0], b"GET");
        assert_eq!(&*result[1], b"");
    }

    #[tokio::test]
    async fn parse_resp3_null() {
        let data = b"_\r\n";
        let mut r = BufReader::new(&data[..]);
        let result = parse_resp(&mut r).await.unwrap().unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn parse_resp3_bool_true() {
        let data = b"#t\r\n";
        let mut r = BufReader::new(&data[..]);
        let result = parse_resp(&mut r).await.unwrap().unwrap();
        assert_eq!(result, vec![b"1"]);
    }

    #[tokio::test]
    async fn parse_resp3_bool_false() {
        let data = b"#f\r\n";
        let mut r = BufReader::new(&data[..]);
        let result = parse_resp(&mut r).await.unwrap().unwrap();
        assert_eq!(result, vec![b"0"]);
    }

    #[tokio::test]
    async fn parse_resp3_double() {
        let data = b",3.14\r\n";
        let mut r = BufReader::new(&data[..]);
        let result = parse_resp(&mut r).await.unwrap().unwrap();
        assert_eq!(result, vec![b"3.14"]);
    }

    #[tokio::test]
    async fn parse_rejects_too_many_bulk_strings() {
        let data = b"*2\r\n$4\r\nPING\r\n$4\r\nPONG\r\n";
        let mut r = BufReader::new(&data[..]);
        let limits = RespLimits {
            max_array_len: 1,
            ..RespLimits::default()
        };
        let err = parse_resp_with_limits(&mut r, limits)
            .await
            .expect_err("should reject oversized array");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn parse_rejects_oversized_bulk_string() {
        let data = b"*1\r\n$4\r\nPING\r\n";
        let mut r = BufReader::new(&data[..]);
        let limits = RespLimits {
            max_bulk_len: 3,
            ..RespLimits::default()
        };
        let err = parse_resp_with_limits(&mut r, limits)
            .await
            .expect_err("should reject oversized bulk");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn parse_rejects_oversized_inline_line() {
        let data = b"PING PONG\r\n";
        let mut r = BufReader::new(&data[..]);
        let limits = RespLimits {
            max_inline_len: 4,
            ..RespLimits::default()
        };
        let err = parse_resp_with_limits(&mut r, limits)
            .await
            .expect_err("should reject oversized inline command");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn resp3_null_serializes_correctly() {
        assert_eq!(&*resp_null_resp3(), b"_\r\n");
    }

    #[tokio::test]
    async fn resp3_bool_serializes_correctly() {
        assert_eq!(&*resp_bool(true), b"#t\r\n");
        assert_eq!(&*resp_bool(false), b"#f\r\n");
    }

    #[tokio::test]
    async fn resp3_map_serializes_correctly() {
        let pairs = vec![(b"key".to_vec(), b"val".to_vec())];
        let out = resp_map(&pairs);
        assert!(out.starts_with(b"%1\r\n"));
        assert!(out.windows(3).any(|w| w == b"key"));
    }

    #[tokio::test]
    async fn resp_null_array_is_star_minus_one() {
        assert_eq!(&*resp_null_array(), b"*-1\r\n");
    }

    #[tokio::test]
    async fn resp_double_infinity() {
        assert_eq!(&*resp_double(f64::INFINITY), b",inf\r\n");
        assert_eq!(&*resp_double(f64::NEG_INFINITY), b",-inf\r\n");
    }

    #[tokio::test]
    async fn resp_verbatim_format() {
        let out = resp_verbatim(b"txt", b"hello");
        // =9\r\ntxt:hello\r\n
        assert!(out.starts_with(b"=9\r\n"));
        assert!(out.ends_with(b"txt:hello\r\n"));
    }
}
