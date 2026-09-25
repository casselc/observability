//! Value rendering, byte for byte as the contrib clickhouse exporter (and so
//! parquetgo's `valueString` / pcommon `Value.AsString`) renders values.
//!
//! - strings are passed through as raw bytes (invalid UTF-8 included);
//! - ints in decimal, bools as `true` / `false`;
//! - doubles as Go's `float64AsString`: shortest round-trip digits, fixed
//!   notation for 1e-6 <= |f| < 1e21 (and 0), else `e` notation with the
//!   exponent's leading zero removed (`1e-7`, `1e+21`), `NaN`,
//!   `Infinity`, `-Infinity`;
//! - bytes as standard base64;
//! - maps and slices as the JSON Go's `encoding/json` writes with HTML
//!   escaping off: map keys sorted by raw bytes, duplicate keys last-wins
//!   (pcommon `Map.AsRaw` builds a Go map), `\ufffd` for each invalid UTF-8
//!   byte, `\u2028`/`\u2029` escaped, and **the whole value is the empty
//!   string when any double inside is NaN or ±Inf** (json.Encoder fails and
//!   `AsString` ignores the error);
//! - Empty as the empty string (top level) or `null` (inside JSON).
//!
//! The functions take the backend-agnostic `AnyValueView`, so the same code
//! renders values read from OTLP protobuf bytes and from OTAP Arrow tables.

use base64::Engine as _;
use otel_arrow_dfe_pdata_views::views::common::{AnyValueView, AttributeView, ValueType};

/// Appends the rendering of an attribute's value (`None` = Empty).
pub fn value<'a, V: AnyValueView<'a>>(dst: &mut Vec<u8>, v: Option<&V>) {
    let Some(v) = v else { return };
    match v.value_type() {
        ValueType::Empty => {}
        ValueType::String => dst.extend_from_slice(v.as_string().unwrap_or_default()),
        ValueType::Int64 => int(dst, v.as_int64().unwrap_or_default()),
        ValueType::Bool => bool_(dst, v.as_bool().unwrap_or_default()),
        ValueType::Double => double(dst, v.as_double().unwrap_or_default()),
        ValueType::Bytes => base64(dst, v.as_bytes().unwrap_or_default()),
        ValueType::Array | ValueType::KeyValueList => {
            let mark = dst.len();
            if json(dst, v).is_err() {
                dst.truncate(mark); // AsString: "" when json.Encoder fails
            }
        }
    }
}

#[inline]
pub fn int(dst: &mut Vec<u8>, i: i64) {
    let mut b = itoa::Buffer::new();
    dst.extend_from_slice(b.format(i).as_bytes());
}

#[inline]
fn bool_(dst: &mut Vec<u8>, b: bool) {
    dst.extend_from_slice(if b { b"true" } else { b"false" });
}

pub fn base64(dst: &mut Vec<u8>, b: &[u8]) {
    let start = dst.len();
    dst.resize(start + base64::encoded_len(b.len(), true).unwrap_or(0), 0);
    let n = base64::engine::general_purpose::STANDARD
        .encode_slice(b, &mut dst[start..])
        .unwrap_or(0);
    dst.truncate(start + n);
}

/// Go's float64AsString (top level): NaN / ±Infinity spelled out.
pub fn double(dst: &mut Vec<u8>, f: f64) {
    if f.is_nan() {
        dst.extend_from_slice(b"NaN");
    } else if f.is_infinite() {
        dst.extend_from_slice(if f > 0.0 { b"Infinity" } else { b"-Infinity" });
    } else {
        finite(dst, f);
    }
}

/// ES6 / Go encoding/json number formatting of a finite double.
fn finite(dst: &mut Vec<u8>, f: f64) {
    use std::io::Write as _;
    let abs = f.abs();
    if abs == 0.0 || (1e-6..1e21).contains(&abs) {
        // Rust's Display is the shortest round-trip digits in fixed
        // notation, like strconv.FormatFloat(f, 'f', -1, 64); "-0" for -0.
        let _ = write!(dst, "{f}");
    } else {
        // LowerExp: "1e21", "1.5e-7", "5e-324"; Go writes "1e+21", "1.5e-07"
        // and float64AsString / encoding/json strip the exponent's leading 0.
        let mut s = [0u8; 40];
        let mut cur = std::io::Cursor::new(&mut s[..]);
        let _ = write!(cur, "{f:e}");
        let n = cur.position() as usize;
        let s = &s[..n];
        let e = s.iter().position(|&c| c == b'e').unwrap_or(n);
        dst.extend_from_slice(&s[..e]);
        dst.push(b'e');
        if s.get(e + 1) == Some(&b'-') {
            dst.push(b'-');
            dst.extend_from_slice(&s[e + 2..]);
        } else {
            dst.push(b'+');
            dst.extend_from_slice(&s[e + 1..]);
        }
    }
}

/// A NaN or ±Inf double inside a map or slice: Go's json.Encoder fails.
#[derive(Debug)]
pub struct Unsupported;

fn json<'a, V: AnyValueView<'a>>(dst: &mut Vec<u8>, v: &V) -> Result<(), Unsupported> {
    match v.value_type() {
        ValueType::Empty => dst.extend_from_slice(b"null"),
        ValueType::String => json_str(dst, v.as_string().unwrap_or_default()),
        ValueType::Int64 => int(dst, v.as_int64().unwrap_or_default()),
        ValueType::Bool => bool_(dst, v.as_bool().unwrap_or_default()),
        ValueType::Double => {
            let f = v.as_double().unwrap_or_default();
            if !f.is_finite() {
                return Err(Unsupported);
            }
            finite(dst, f);
        }
        ValueType::Bytes => {
            // []byte marshals as a base64 JSON string.
            dst.push(b'"');
            base64(dst, v.as_bytes().unwrap_or_default());
            dst.push(b'"');
        }
        ValueType::Array => {
            dst.push(b'[');
            if let Some(it) = v.as_array() {
                for (i, e) in it.enumerate() {
                    if i > 0 {
                        dst.push(b',');
                    }
                    json(dst, &e)?;
                }
            }
            dst.push(b']');
        }
        ValueType::KeyValueList => {
            // Go map semantics: last duplicate wins, keys sorted bytewise.
            let mut kvs: Vec<_> = match v.as_kvlist() {
                Some(it) => it.collect(),
                None => Vec::new(),
            };
            // Stable sort keeps the input order among equal keys, so the
            // last of each run is the one Go's map would hold.
            kvs.sort_by(|a, b| a.key().cmp(b.key()));
            dst.push(b'{');
            let mut first = true;
            for i in 0..kvs.len() {
                if i + 1 < kvs.len() && kvs[i + 1].key() == kvs[i].key() {
                    continue;
                }
                if !first {
                    dst.push(b',');
                }
                first = false;
                json_str(dst, kvs[i].key());
                dst.push(b':');
                match kvs[i].value() {
                    Some(val) => json(dst, &val)?,
                    None => dst.extend_from_slice(b"null"),
                }
            }
            dst.push(b'}');
        }
    }
    Ok(())
}

const HEX: &[u8; 16] = b"0123456789abcdef";

/// encoding/json string escaping with SetEscapeHTML(false) (Go >= 1.22).
pub fn json_str(dst: &mut Vec<u8>, s: &[u8]) {
    dst.push(b'"');
    let mut i = 0;
    let mut start = 0;
    while i < s.len() {
        let b = s[i];
        if b < 0x80 {
            if b >= 0x20 && b != b'"' && b != b'\\' {
                i += 1;
                continue;
            }
            dst.extend_from_slice(&s[start..i]);
            match b {
                b'\\' | b'"' => dst.extend_from_slice(&[b'\\', b]),
                0x08 => dst.extend_from_slice(b"\\b"),
                0x0c => dst.extend_from_slice(b"\\f"),
                b'\n' => dst.extend_from_slice(b"\\n"),
                b'\r' => dst.extend_from_slice(b"\\r"),
                b'\t' => dst.extend_from_slice(b"\\t"),
                _ => dst.extend_from_slice(&[
                    b'\\',
                    b'u',
                    b'0',
                    b'0',
                    HEX[(b >> 4) as usize],
                    HEX[(b & 0xf) as usize],
                ]),
            }
            i += 1;
            start = i;
            continue;
        }
        match decode_rune(&s[i..]) {
            None => {
                dst.extend_from_slice(&s[start..i]);
                dst.extend_from_slice(b"\\ufffd");
                i += 1;
                start = i;
            }
            Some((c, n)) => {
                if c == '\u{2028}' || c == '\u{2029}' {
                    dst.extend_from_slice(&s[start..i]);
                    dst.extend_from_slice(b"\\u202");
                    dst.push(HEX[(c as u32 & 0xf) as usize]);
                    i += n;
                    start = i;
                } else {
                    i += n;
                }
            }
        }
    }
    dst.extend_from_slice(&s[start..]);
    dst.push(b'"');
}

/// utf8.DecodeRune: the first rune and its width, or None for an invalid
/// byte (Go then consumes exactly one byte).
fn decode_rune(s: &[u8]) -> Option<(char, usize)> {
    let n = match s[0] {
        0xc2..=0xdf => 2,
        0xe0..=0xef => 3,
        0xf0..=0xf4 => 4,
        _ => return None,
    };
    let chunk = s.get(..n)?;
    std::str::from_utf8(chunk).ok().and_then(|t| t.chars().next()).map(|c| (c, n))
}

/// Lower-case hex of an id, or nothing for an all-zero (or absent) id.
#[inline]
pub fn hex_id(dst: &mut Vec<u8>, id: Option<&[u8]>) {
    if let Some(id) = id {
        if id.iter().any(|&b| b != 0) {
            for &b in id {
                dst.push(HEX[(b >> 4) as usize]);
                dst.push(HEX[(b & 0xf) as usize]);
            }
        }
    }
}

pub fn span_kind(k: i32) -> &'static [u8] {
    match k {
        0 => b"Unspecified",
        1 => b"Internal",
        2 => b"Server",
        3 => b"Client",
        4 => b"Producer",
        5 => b"Consumer",
        _ => b"",
    }
}

pub fn status_code(c: i32) -> &'static [u8] {
    match c {
        0 => b"Unset",
        1 => b"Ok",
        2 => b"Error",
        _ => b"",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(f: f64) -> String {
        let mut v = Vec::new();
        double(&mut v, f);
        String::from_utf8(v).unwrap()
    }

    #[test]
    fn doubles_match_go() {
        // Expected strings are strconv / pcommon.Value.AsString output.
        assert_eq!(d(0.0), "0");
        assert_eq!(d(-0.0), "-0");
        assert_eq!(d(1.0), "1");
        assert_eq!(d(5.0), "5");
        assert_eq!(d(0.1 + 0.2), "0.30000000000000004");
        assert_eq!(d(1e21), "1e+21");
        assert_eq!(d(1e20), "100000000000000000000");
        assert_eq!(d(1e-7), "1e-7");
        assert_eq!(d(1e-6), "0.000001");
        assert_eq!(d(5e-324), "5e-324");
        assert_eq!(d(f64::MAX), "1.7976931348623157e+308");
        assert_eq!(d(-f64::MAX), "-1.7976931348623157e+308");
        assert_eq!(d(123456789.123456789), "123456789.12345679");
        assert_eq!(d(1.25e-10), "1.25e-10");
        assert_eq!(d(f64::NAN), "NaN");
        assert_eq!(d(f64::INFINITY), "Infinity");
        assert_eq!(d(f64::NEG_INFINITY), "-Infinity");
    }

    #[test]
    fn json_strings_match_go() {
        let mut v = Vec::new();
        json_str(&mut v, b"a\x00b\x08\x0c\n\r\t\"\\<>&\x7f\xff\xfe\xe2\x80\xa8\xf0\x9f\x9a\x80");
        assert_eq!(
            String::from_utf8(v).unwrap(),
            "\"a\\u0000b\\b\\f\\n\\r\\t\\\"\\\\<>&\x7f\\ufffd\\ufffd\\u2028🚀\""
        );
        // A truncated multi-byte sequence: each byte is replaced on its own.
        let mut v = Vec::new();
        json_str(&mut v, b"\xe2\x80");
        assert_eq!(String::from_utf8(v).unwrap(), "\"\\ufffd\\ufffd\"");
    }

    #[test]
    fn ids() {
        let mut v = Vec::new();
        hex_id(&mut v, Some(&[0, 0]));
        assert!(v.is_empty());
        hex_id(&mut v, Some(&[0x4b, 0xf9]));
        assert_eq!(v, b"4bf9");
    }
}
