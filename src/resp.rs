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


/// Parse one RESP command into `out`, reusing existing `Vec<u8>` slot capacity.
///
/// Returns `true` if a command was parsed, `false` on clean EOF.
/// `out` is cleared and refilled on every call; inner `Vec<u8>` allocations are
/// reused across calls (the same `out` should be passed on every iteration of
/// the connection loop).
pub(crate) async fn parse_resp_with_limits<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    limits: RespLimits,
    out: &mut Vec<Vec<u8>>,
) -> std::io::Result<bool> {
    let mut line = Vec::new();
    let Some(trimmed) = read_resp_line(reader, &mut line, limits.max_inline_len).await? else {
        out.clear();
        return Ok(false);
    };
    out.clear();
    if trimmed.is_empty() {
        return Ok(true); // empty args — caller checks out.is_empty()
    }

    let first = trimmed.first().copied().unwrap_or(0);
    let rest = &trimmed[1..];

    match first {
        // ── RESP2 / RESP3 array ─────────────────────────────────────────────
        b'*' => {
            let count = parse_i64(rest, "bad count")?;
            if count < 0 {
                return Ok(true); // null array → empty out
            }
            let count = usize::try_from(count).map_err(|_| invalid_data("bad count"))?;
            if count > limits.max_array_len {
                return Err(invalid_data("too many bulk strings"));
            }
            let mut hdr = Vec::new();
            for i in 0..count {
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
                    reuse_slot(out, i, b"");
                } else {
                    let len = usize::try_from(len).map_err(|_| invalid_data("bad len"))?;
                    if len > limits.max_bulk_len {
                        return Err(invalid_data("bulk string too large"));
                    }
                    let slot = reuse_slot_uninit(out, i, len);
                    reader.read_exact(slot).await?;
                    let mut crlf = [0u8; 2];
                    reader.read_exact(&mut crlf).await?;
                }
            }
            Ok(true)
        }

        // ── RESP3 null ──────────────────────────────────────────────────────
        b'_' => Ok(true), // empty out

        // ── RESP3 boolean ───────────────────────────────────────────────────
        b'#' => {
            let val = if rest == b"t" { b"1" as &[u8] } else { b"0" };
            reuse_slot(out, 0, val);
            Ok(true)
        }

        // ── RESP3 double ────────────────────────────────────────────────────
        b',' => {
            reuse_slot(out, 0, rest);
            Ok(true)
        }

        // ── RESP3 big number ────────────────────────────────────────────────
        b'(' => {
            reuse_slot(out, 0, rest);
            Ok(true)
        }

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
            read_bulk_strings_into(reader, count, limits, out).await?;
            Ok(true)
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
            read_bulk_strings_into(reader, count, limits, out).await?;
            Ok(true)
        }

        // ── Inline command ──────────────────────────────────────────────────
        _ => {
            split_inline_command_into(trimmed, limits.max_array_len, out)?;
            Ok(true)
        }
    }
}

/// Write `data` into slot `idx` of `out`, reusing the existing allocation if present.
fn reuse_slot(out: &mut Vec<Vec<u8>>, idx: usize, data: &[u8]) {
    if idx < out.len() {
        out[idx].clear();
        out[idx].extend_from_slice(data);
    } else {
        out.push(data.to_vec());
    }
}

/// Resize slot `idx` of `out` to exactly `len` bytes and return a mutable reference.
/// Reuses the existing Vec allocation when capacity allows.
fn reuse_slot_uninit(out: &mut Vec<Vec<u8>>, idx: usize, len: usize) -> &mut [u8] {
    if idx < out.len() {
        let slot = &mut out[idx];
        slot.clear();
        slot.resize(len, 0);
        slot.as_mut_slice()
    } else {
        out.push(vec![0u8; len]);
        out[idx].as_mut_slice()
    }
}

/// Read exactly `n` RESP bulk strings into `out` (reusing existing slots).
async fn read_bulk_strings_into<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    n: usize,
    limits: RespLimits,
    out: &mut Vec<Vec<u8>>,
) -> std::io::Result<()> {
    let mut hdr = Vec::new();
    for i in 0..n {
        let Some(hdr_line) = read_resp_line(reader, &mut hdr, limits.max_inline_len).await? else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected EOF",
            ));
        };
        let len = parse_bulk_len(hdr_line)?;
        if len < 0 {
            reuse_slot(out, i, b"");
        } else {
            let len = usize::try_from(len).map_err(|_| invalid_data("bad len"))?;
            if len > limits.max_bulk_len {
                return Err(invalid_data("bulk string too large"));
            }
            let slot = reuse_slot_uninit(out, i, len);
            reader.read_exact(slot).await?;
            let mut crlf = [0u8; 2];
            reader.read_exact(&mut crlf).await?;
        }
    }
    Ok(())
}

fn split_inline_command_into(
    line: &[u8],
    max_args: usize,
    out: &mut Vec<Vec<u8>>,
) -> std::io::Result<()> {
    let mut i = 0usize;
    let mut idx = 0usize;
    while i < line.len() {
        while i < line.len() && line[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= line.len() {
            break;
        }
        if idx >= max_args {
            return Err(invalid_data("too many inline arguments"));
        }
        let start = i;
        while i < line.len() && !line[i].is_ascii_whitespace() {
            i += 1;
        }
        reuse_slot(out, idx, &line[start..i]);
        idx += 1;
    }
    out.truncate(idx);
    Ok(())
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
    let mut buf = itoa::Buffer::new();
    let s = buf.format(n);
    let mut out = Vec::with_capacity(s.len() + 3);
    out.push(b':');
    out.extend_from_slice(s.as_bytes());
    out.extend_from_slice(b"\r\n");
    Cow::Owned(out)
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
    let mut buf = itoa::Buffer::new();
    out.push(b'*');
    out.extend_from_slice(buf.format(len).as_bytes());
    out.extend_from_slice(b"\r\n");
}

pub(crate) fn append_int(out: &mut Vec<u8>, n: i64) {
    let mut buf = itoa::Buffer::new();
    out.push(b':');
    out.extend_from_slice(buf.format(n).as_bytes());
    out.extend_from_slice(b"\r\n");
}

pub(crate) fn append_null(out: &mut Vec<u8>) {
    out.extend_from_slice(b"$-1\r\n");
}

pub(crate) fn append_bulk(out: &mut Vec<u8>, data: &[u8]) {
    let mut buf = itoa::Buffer::new();
    out.push(b'$');
    out.extend_from_slice(buf.format(data.len()).as_bytes());
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
    // Pre-size: array header + per-item bulk header (≤ ~16 bytes) + data.
    let capacity = 16 + items.iter().map(|b| b.len() + 16).sum::<usize>();
    let mut out = Vec::with_capacity(capacity);
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

// ── Synchronous RESP parser (for io_uring / owned-buffer I/O) ────────────────
//
// Works on a `&[u8]` slice of already-buffered data. Returns the number of bytes
// consumed so the caller can advance its accumulation buffer. Returns `Ok(None)`
// when the buffer holds an incomplete frame — the caller should read more data
// and retry without clearing the existing buffer contents.
//
// The helpers below are used exclusively from `uring_server` (Linux). The
// `allow(dead_code)` on each prevents warnings on non-Linux builds.

/// Scan forward for a `\n` (bare or `\r\n`) and return (line_without_terminator, bytes_consumed).
#[allow(dead_code)]
fn sync_read_line(buf: &[u8]) -> Option<(&[u8], usize)> {
    let pos = buf.iter().position(|&b| b == b'\n')?;
    let consumed = pos + 1;
    let line = &buf[..pos];
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    Some((line, consumed))
}

#[allow(dead_code)]
fn sync_parse_i64(bytes: &[u8]) -> Option<i64> {
    // No trim — RESP does not allow trailing whitespace in length fields.
    // Matches the behaviour of the async parse_i64() path.
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

/// Read `n` bulk strings from `buf[pos..]`, filling `out` slots.
/// Returns the updated absolute position on success, or `Ok(None)` on incomplete data.
#[allow(dead_code)]
fn sync_read_bulks(
    buf: &[u8],
    mut pos: usize,
    n: usize,
    max_bulk_len: usize,
    out: &mut Vec<Vec<u8>>,
) -> std::io::Result<Option<usize>> {
    for i in 0..n {
        let Some((hdr, hdr_consumed)) = sync_read_line(&buf[pos..]) else {
            return Ok(None);
        };
        if hdr.first().copied() != Some(b'$') {
            return Err(invalid_data("expected $"));
        }
        let len = sync_parse_i64(&hdr[1..]).ok_or_else(|| invalid_data("bad len"))?;
        pos += hdr_consumed;
        if len < 0 {
            reuse_slot(out, i, b"");
        } else {
            let len = len as usize;
            if len > max_bulk_len {
                return Err(invalid_data("bulk string too large"));
            }
            if buf.len() < pos + len + 2 {
                return Ok(None); // data + \r\n not yet buffered
            }
            reuse_slot(out, i, &buf[pos..pos + len]);
            pos += len + 2;
        }
    }
    out.truncate(n);
    Ok(Some(pos))
}

/// Parse one RESP command from `buf` into `out`, reusing existing slot capacity.
///
/// Returns `Ok(Some(n))` — `n` bytes consumed, `out` holds the args.
/// Returns `Ok(None)` — incomplete frame; caller should buffer more data and retry.
/// Returns `Err(_)` — protocol error; caller should close the connection.
#[allow(dead_code)]
pub(crate) fn parse_resp_sync(
    buf: &[u8],
    limits: RespLimits,
    out: &mut Vec<Vec<u8>>,
) -> std::io::Result<Option<usize>> {
    out.clear();
    let Some((line, after_line)) = sync_read_line(buf) else {
        return Ok(None);
    };

    match line.first().copied().unwrap_or(0) {
        b'*' => {
            let count = sync_parse_i64(&line[1..]).ok_or_else(|| invalid_data("bad count"))?;
            if count < 0 {
                return Ok(Some(after_line)); // null array → empty out
            }
            let count = count as usize;
            if count > limits.max_array_len {
                return Err(invalid_data("too many bulk strings"));
            }
            sync_read_bulks(buf, after_line, count, limits.max_bulk_len, out)
        }
        0 => Ok(Some(after_line)), // empty line → empty out
        b'_' => Ok(Some(after_line)), // RESP3 null → empty out
        b'#' => {
            reuse_slot(out, 0, if line == b"#t" { b"1" } else { b"0" });
            Ok(Some(after_line))
        }
        b',' | b'(' => {
            reuse_slot(out, 0, &line[1..]);
            Ok(Some(after_line))
        }
        b'~' => {
            let count = sync_parse_i64(&line[1..]).ok_or_else(|| invalid_data("bad count"))?;
            if count < 0 {
                return Err(invalid_data("bad count"));
            }
            let count = count as usize;
            if count > limits.max_array_len {
                return Err(invalid_data("too many bulk strings"));
            }
            sync_read_bulks(buf, after_line, count, limits.max_bulk_len, out)
        }
        b'%' => {
            let count = sync_parse_i64(&line[1..]).ok_or_else(|| invalid_data("bad count"))?;
            if count < 0 {
                return Err(invalid_data("bad count"));
            }
            let count = count as usize;
            if count > limits.max_array_len / 2 {
                return Err(invalid_data("too many bulk strings"));
            }
            let count = count.checked_mul(2).ok_or_else(|| invalid_data("bad count"))?;
            sync_read_bulks(buf, after_line, count, limits.max_bulk_len, out)
        }
        _ => {
            // Inline command
            split_inline_command_into(line, limits.max_array_len, out)?;
            Ok(Some(after_line))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    async fn parse(data: &[u8]) -> Vec<Vec<u8>> {
        let mut r = BufReader::new(data);
        let mut out = Vec::new();
        let got = parse_resp_with_limits(&mut r, RespLimits::default(), &mut out)
            .await
            .unwrap();
        assert!(got, "expected a command, got EOF");
        out
    }

    async fn parse_eof(data: &[u8]) -> bool {
        let mut r = BufReader::new(data);
        let mut out = Vec::new();
        !parse_resp_with_limits(&mut r, RespLimits::default(), &mut out)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn parse_array_set_command() {
        let result = parse(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n").await;
        assert_eq!(result, vec![b"SET", b"foo", b"bar"]);
    }

    #[tokio::test]
    async fn parse_inline_ping() {
        let result = parse(b"PING\r\n").await;
        assert_eq!(result, vec![b"PING"]);
    }

    #[tokio::test]
    async fn parse_inline_with_args() {
        let result = parse(b"GET mykey\r\n").await;
        assert_eq!(&*result[0], b"GET");
        assert_eq!(&*result[1], b"mykey");
    }

    #[tokio::test]
    async fn parse_eof_returns_false() {
        assert!(parse_eof(b"").await);
    }

    #[tokio::test]
    async fn parse_empty_line_returns_empty_vec() {
        let result = parse(b"\r\n").await;
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn parse_null_bulk_string() {
        let result = parse(b"*2\r\n$3\r\nGET\r\n$-1\r\n").await;
        assert_eq!(&*result[0], b"GET");
        assert_eq!(&*result[1], b"");
    }

    #[tokio::test]
    async fn parse_resp3_null() {
        let result = parse(b"_\r\n").await;
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn parse_resp3_bool_true() {
        let result = parse(b"#t\r\n").await;
        assert_eq!(result, vec![b"1"]);
    }

    #[tokio::test]
    async fn parse_resp3_bool_false() {
        let result = parse(b"#f\r\n").await;
        assert_eq!(result, vec![b"0"]);
    }

    #[tokio::test]
    async fn parse_resp3_double() {
        let result = parse(b",3.14\r\n").await;
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
        let mut out = Vec::new();
        let err = parse_resp_with_limits(&mut r, limits, &mut out)
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
        let mut out = Vec::new();
        let err = parse_resp_with_limits(&mut r, limits, &mut out)
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
        let mut out = Vec::new();
        let err = parse_resp_with_limits(&mut r, limits, &mut out)
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
