//! The payload offloader (`src/offload.rs`, DECISIONS.md D36, FORMAT.md §2.3)
//! at the Rust edge:
//!
//! - the shared vectors (`../../langfuse/testdata/offload_vectors.json`):
//!   this file WRITES them (`OFFLOAD_VECTORS_WRITE=1`) and otherwise checks
//!   the file on disk is what the code gives; the Go edge
//!   (`../../parquetgo/offload_test.go`) checks them;
//! - the hostile corpus through the whole walk and the Parquet object: every
//!   reference resolves in the object's payload part, a split value's
//!   elements are its payloads in order, a truncated value is its prefix
//!   with the original size, nothing is lost or invented;
//! - Hegel properties: the bounded JSON scan agrees with serde_json on
//!   valid documents and never panics or recurses on hostile bytes; the
//!   offload of arbitrary attributes round-trips; the payload part is
//!   decided per slot and marks nothing twice.
//!
//!   cargo test --release --test offload
//!   OFFLOAD_VECTORS_WRITE=1 cargo test --release --test offload -- vectors

use arrow::array::{Array, BinaryArray, ListArray, MapArray, RecordBatch, StringArray};
use hegel::TestCase;
use hegel::generators as gs;
use otap_s3pq::Signal;
use otap_s3pq::batch::{Encoder, Format, Input};
use otap_s3pq::encode::ParquetOptions;
use otap_s3pq::flatten::Envelope;
use otap_s3pq::offload::{
    self, OffloadOptions, Offloader, Outcome, PayloadCache, Policy, json_array_elements, payload_hash, tenant_key, truncate_at,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value::Value};
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{LogRecord, LogsData, ResourceLogs, ScopeLogs};
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
use otel_arrow_dfe_pdata::proto::opentelemetry::trace::v1::{ResourceSpans, ScopeSpans, Span, TracesData, span::Event};
use prost::Message;
use serde_json::{Value as J, json};
use std::collections::HashMap;
use std::sync::Arc;

const T: u64 = 1_790_000_000_000_000_000;
const VECTORS_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../langfuse/testdata/offload_vectors.json");

fn offloader(opts: OffloadOptions, cluster: &str, received: u64) -> Offloader {
    Offloader::new(Arc::new(Policy::new(opts)), cluster, received)
}

// ---- the vectors ---------------------------------------------------------------------------

/// A value in the vectors: `{"runs": [[hex, count], ...]}`, each run's bytes
/// repeated `count` times, so the large ones stay small in the file.
fn gen_value(v: &J) -> Vec<u8> {
    let mut out = Vec::new();
    for r in v["runs"].as_array().unwrap() {
        let b = hex::decode(r[0].as_str().unwrap()).unwrap();
        for _ in 0..r[1].as_u64().unwrap() {
            out.extend_from_slice(&b);
        }
    }
    out
}

fn hv(b: &[u8]) -> J {
    json!({ "runs": [[hex::encode(b), 1]] })
}

/// `prefix`, then `fill` `n` times, then `suffix` (a run of its own when it
/// repeats one byte, as a bomb's closing brackets do).
fn filled(prefix: &[u8], fill: &[u8], n: usize, suffix: &[u8]) -> J {
    let mut runs = vec![json!([hex::encode(prefix), 1]), json!([hex::encode(fill), n])];
    if !suffix.is_empty() && suffix.iter().all(|c| *c == suffix[0]) {
        runs.push(json!([hex::encode(&suffix[..1]), suffix.len()]));
    } else {
        runs.push(json!([hex::encode(suffix), 1]));
    }
    json!({ "runs": runs })
}

/// The hostile JSON documents for the split scan.
fn json_cases() -> Vec<(J, usize, usize)> {
    let mut v: Vec<(J, usize, usize)> = Vec::new();
    for s in [
        "[]", " [ 1 , \"a\" ,{\"k\":[1,{}]} , [] ,null,true,false,-0.5e+3 ] \n", "[\"\\u00e9\\n\",\"\u{1F680}\"]",
        "", "{}", "1", "[", "[1,]", "[,1]", "[1 2]", "[01]", "[1.]", "[.1]", "[1e]", "[tru]", "[\"a]", "[\"\\x\"]",
        "[\"\\u12g4\"]", "[\"a\u{1}\"]", "[1]]", "[1] x", "[{\"a\"}]", "[{\"a\":}]", "[{1:2}]", "[{\"a\":1,}]", "[+1]",
        "[-]", "[nul]", "[\"h:0123456789abcdef0123456789abcdef\"]", "[{\"role\":\"user\",\"content\":\"hi\"},{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"yo\"}]}]",
        "\t[\r\n{\"a\" : \"b\" }\r\n]\t", "[1e5,-0,0.0,1E-2,123456789012345678901234567890]", "[\"\\\"\",\"\\\\\",\"\\/\"]",
    ] {
        v.push((hv(s.as_bytes()), 64, 4096));
    }
    v.push((hv(b"[\"\xff\xfe\",\"\xc3\"]"), 64, 4096)); // invalid UTF-8 inside strings is data
    v.push((hv(b"[\"a\0b\"]"), 64, 4096)); // a raw NUL is a control character: not JSON
    v.push((filled(b"", b"[", 100_000, &b"]".repeat(100_000)), 64, 4096)); // a nesting bomb
    let ok64 = format!("{}{}", "[".repeat(64), "]".repeat(64));
    v.push((hv(ok64.as_bytes()), 64, 4096));
    v.push((hv(ok64.as_bytes()), 63, 4096));
    v.push((filled(b"[1", b",1", 9_999, b"]"), 64, 4096)); // 10,000 elements
    v.push((filled(b"[1", b",1", 9_999, b"]"), 64, 10_000));
    v
}

/// The hostile values for the offloader, as (key, value) lists, one request each.
fn value_cases() -> Vec<(String, J, Vec<(Vec<u8>, J)>)> {
    let small = OffloadOptions { max_value: 4096, redact_keys: vec!["secret".into(), "@body".into()], ..Default::default() };
    let dflt = OffloadOptions::default();
    let conv1 = br#"[{"role":"system","content":"You are terse."},{"role":"user","content":"q1"}]"#.to_vec();
    let conv2 = br#"[{"role":"system","content":"You are terse."},{"role":"user","content":"q1"},{"role":"assistant","content":"a1"},{"role":"user","content":"q2"}]"#.to_vec();
    let k = |s: &str| s.as_bytes().to_vec();
    vec![
        (
            "small policy: inline, listed, split, over the threshold, truncated, redacted".into(),
            serde_json::to_value(&small).unwrap(),
            vec![
                (k("k"), hv(b"small")),
                (k("gen_ai.input.messages"), hv(b"")),
                (k("input.value"), hv(b"hi")),
                (k("gen_ai.input.messages"), hv(&conv1)),
                (k("gen_ai.input.messages"), hv(&conv2)),
                (k("gen_ai.output.messages"), hv(b"not json at all")),
                (k("blob"), filled(b"", b"z", 5000, b"")),
                (k("blob"), filled(b"", b"z", 2048, b"")),
                (k("blob"), filled(b"", b"z", 2049, b"")),
                (k("secret"), hv(b"hunter2")),
                (k("secret"), hv(b"")),
                (k("@body"), hv(b"a log body")),
                (k("utf8"), filled(b"", "é".as_bytes(), 2100, b"")), // 4200 bytes: cut at 4096 on a boundary
                (k("rocket"), filled(b"x", "🚀".as_bytes(), 1100, b"")), // 4401 bytes: backs off to 4093
                (k("bad-utf8"), filled(b"", b"\xff\x80", 1500, b"")),
                (k("gen_ai.system_instructions"), hv(b"[\"\xff\xfe\",\"\xc3\"]")),
                (vec![0xff, 0x00, b'k'], filled(b"", b"n", 3000, b"")), // a hostile key
                (k("otel.payload.fake.bytes"), filled(b"", b"m", 3000, b"")), // a producer key in the marker space
                (k("spoof"), hv(b"[\"h:0123456789abcdef0123456789abcdef\"]")), // a reference-looking value stays inline
                (k("gen_ai.input.messages"), filled(b"", b"[", 100_000, &b"]".repeat(100_000))), // a bomb: truncated, not split
            ],
        ),
        (
            "default policy: 8 MiB cap, dedup inside the request".into(),
            serde_json::to_value(&dflt).unwrap(),
            vec![
                (k("gen_ai.output.messages"), filled(b"[\"", b"w", 9 << 20, b"\"]")),
                (k("gen_ai.input.messages"), hv(&conv1)),
                (k("langfuse.observation.input"), hv(&conv1)),
                (k("input.value"), hv(b"{\"a\":1}")),
                (k("big"), filled(b"", b"0123456789", 1000, b"")),
                (k("big"), filled(b"", b"0123456789", 1000, b"")),
            ],
        ),
    ]
}

/// The offloader over one value list: each value's outcome, stored value and
/// markers, the payloads, and the row's references.
fn run_values(opts: &OffloadOptions, cluster: &str, ns: &[u8], received: u64, vals: &[(Vec<u8>, J)]) -> J {
    let mut o = offloader(opts.clone(), cluster, received);
    o.tenant(ns);
    let mut out = Vec::new();
    let mut res = Vec::new();
    for (row, (key, v)) in vals.iter().enumerate() {
        let v = gen_value(v);
        let oc = o.value(key, &v, row as u32, &mut out);
        let markers: Vec<J> = o.markers.drain(..).map(|(k, v)| json!([hex::encode(k), String::from_utf8(v).unwrap()])).collect();
        let stored = match oc {
            Outcome::Inline => J::Null,
            Outcome::Replaced => J::String(String::from_utf8(out.clone()).expect("a reference document is ASCII")),
        };
        let mut refs = otap_s3pq::columns::Bin::default();
        refs.clear();
        let _ = o.end_row(&mut refs);
        let refs: Vec<String> = (0..refs.len()).map(|i| String::from_utf8(refs.data[refs.off[i] as usize..refs.off[i + 1] as usize].to_vec()).unwrap()).collect();
        res.push(json!({ "stored": stored, "markers": markers, "row_refs": refs }));
    }
    let payloads: Vec<J> = o
        .payloads
        .iter()
        .map(|p| json!({ "hash": hex::encode(p.hash), "len": p.content.len(), "blake3": blake3::hash(&p.content).to_hex().to_string(), "first_row": p.first_row }))
        .collect();
    json!({ "values": res, "payloads": payloads, "stats": {
        "offloaded": o.stats.offloaded, "offloaded_bytes": o.stats.offloaded_bytes, "split": o.stats.split,
        "truncated": o.stats.truncated, "redacted": o.stats.redacted } })
}

fn vectors() -> J {
    let hashes: Vec<J> = [
        ("c1", &b"ns-a"[..], T, &b"the same prompt"[..]),
        ("c1", b"ns-b", T, b"the same prompt"),
        ("c2", b"ns-a", T, b"the same prompt"),
        ("c1", b"ns-a", T + 86_400_000_000_000, b"the same prompt"),
        ("c1", b"ns-a", T + 1, b"the same prompt"),
        ("c1", b"", T, b""),
        ("c1", b"\xff\x00x", 0, b"\x00\x01\xff"),
        ("c1\0", b"x", T, b"p"),
        ("c1", b"\0x", T, b"p"),
    ]
    .iter()
    .map(|(c, ns, r, content)| {
        json!({ "cluster": c, "namespace_hex": hex::encode(ns), "received_ns": r, "content_hex": hex::encode(content),
                "key": hex::encode(tenant_key(c, ns, *r)), "hash": hex::encode(payload_hash(&tenant_key(c, ns, *r), content)) })
    })
    .collect();
    let splits: Vec<J> = json_cases()
        .into_iter()
        .map(|(v, d, m)| {
            let b = gen_value(&v);
            // ranges listed when few; always a digest: BLAKE3 of the ranges as LE u64 pairs
            let got = json_array_elements(&b, d, m).map(|e| {
                let mut h = blake3::Hasher::new();
                for (a, b) in &e {
                    let _ = h.update(&(*a as u64).to_le_bytes());
                    let _ = h.update(&(*b as u64).to_le_bytes());
                }
                let ranges = (e.len() <= 64).then(|| e.iter().map(|(a, b)| json!([a, b])).collect::<Vec<_>>());
                json!({ "n": e.len(), "ranges": ranges, "digest": h.finalize().to_hex().to_string() })
            });
            json!({ "value": v, "max_depth": d, "max_elements": m, "elements": got })
        })
        .collect();
    let truncs: Vec<J> = [
        ("aé🚀".as_bytes().to_vec(), vec![7usize, 3, 2, 5, 0, 1, 100]),
        (vec![0x80u8; 10], vec![5, 0, 3, 4]),
        (b"plain".to_vec(), vec![3, 5, 9]),
    ]
    .iter()
    .map(|(v, ns)| json!({ "value_hex": hex::encode(v), "cuts": ns.iter().map(|n| json!([n, truncate_at(v, *n)])).collect::<Vec<_>>() }))
    .collect();
    let requests: Vec<J> = value_cases()
        .into_iter()
        .map(|(name, pol, vals)| {
            let opts: OffloadOptions = serde_json::from_value(pol.clone()).unwrap();
            let want = run_values(&opts, "c1", b"team-a", T, &vals);
            json!({ "name": name, "policy": pol, "cluster": "c1", "namespace": "team-a", "received_ns": T,
                    "values": vals.iter().map(|(k, v)| json!({ "key_hex": hex::encode(k), "value": v })).collect::<Vec<_>>(), "want": want })
        })
        .collect();
    json!({
        "about": "D36 payload offloader vectors: written by otap-rs/tests/offload.rs (OFFLOAD_VECTORS_WRITE=1), checked by it and by parquetgo/offload_test.go. FORMAT.md §2.3.",
        "hash_context": offload::HASH_CONTEXT,
        "hashes": hashes, "splits": splits, "truncations": truncs, "requests": requests,
    })
}

#[test]
fn vectors_file_is_current() {
    let v = vectors();
    let text = serde_json::to_string_pretty(&v).unwrap() + "\n";
    if std::env::var("OFFLOAD_VECTORS_WRITE").is_ok() {
        std::fs::create_dir_all(std::path::Path::new(VECTORS_PATH).parent().unwrap()).unwrap();
        std::fs::write(VECTORS_PATH, &text).unwrap();
        return;
    }
    let disk = std::fs::read_to_string(VECTORS_PATH).expect("the vectors file (OFFLOAD_VECTORS_WRITE=1 writes it)");
    assert!(disk == text, "{VECTORS_PATH} is stale: OFFLOAD_VECTORS_WRITE=1 cargo test --release --test offload -- vectors");
}

/// The vectors' claims, checked against their meaning (not only against
/// this code's earlier output).
#[test]
fn vectors_mean_what_they_say() {
    let v = vectors();
    let hs = v["hashes"].as_array().unwrap();
    assert_ne!(hs[0]["hash"], hs[1]["hash"], "another namespace");
    assert_ne!(hs[0]["hash"], hs[2]["hash"], "another cluster");
    assert_ne!(hs[0]["hash"], hs[3]["hash"], "another day");
    assert_eq!(hs[0]["hash"], hs[4]["hash"], "the same day");
    assert_ne!(hs[7]["key"], hs[8]["key"], "length-prefixed");
    for s in v["splits"].as_array().unwrap() {
        let b = gen_value(&s["value"]);
        if let (Ok(J::Array(want)), J::Array(got)) = (serde_json::from_slice::<J>(&b), &s["elements"]["ranges"]) {
            assert_eq!(want.len(), got.len());
            for (w, g) in want.iter().zip(got) {
                let (a, e) = (g[0].as_u64().unwrap() as usize, g[1].as_u64().unwrap() as usize);
                assert_eq!(&serde_json::from_slice::<J>(&b[a..e]).unwrap(), w);
            }
        }
    }
    let r = &v["requests"][0]["want"]["values"];
    assert_eq!(r[0]["stored"], J::Null, "small stays inline");
    assert_eq!(r[1]["stored"], J::Null, "empty is never offloaded");
    assert_eq!(r[4]["row_refs"].as_array().unwrap().len(), 4, "a split conversation: four messages");
    assert_eq!(r[7]["stored"], J::Null, "exactly the threshold stays inline");
    assert!(r[8]["stored"].is_string(), "one byte over is offloaded");
    assert_eq!(r[18]["stored"], J::Null, "a reference-looking value is data");
    let bomb = &r[19]["markers"];
    assert!(bomb.as_array().unwrap().iter().any(|m| m[0] == hex::encode("otel.payload.gen_ai.input.messages.truncated_from")));
    assert!(!bomb.as_array().unwrap().iter().any(|m| m[0].as_str().unwrap().ends_with(&hex::encode(".elements"))), "a bomb is not split");
    let p = v["requests"][0]["want"]["payloads"].as_array().unwrap();
    // conv2 shares its first two messages with conv1
    let firsts: Vec<u64> = p.iter().map(|x| x["first_row"].as_u64().unwrap()).collect();
    assert_eq!(firsts.iter().filter(|r| **r == 4).count(), 2);
    let d = &v["requests"][1]["want"];
    assert_eq!(d["payloads"][0]["len"], json!(8 << 20), "capped at 8 MiB");
    assert_eq!(d["values"][4]["row_refs"], d["values"][5]["row_refs"], "equal content, one payload");
    assert_eq!(d["payloads"].as_array().unwrap().len(), 5, "9 MiB, two messages (shared), {{a:1}}, one big");
}

// ---- the hostile corpus through the walk and the object ----------------------------------

fn kv(k: &str, v: Value) -> KeyValue {
    KeyValue { key: k.into(), value: Some(AnyValue { value: Some(v) }) }
}

fn s(v: &str) -> Value {
    Value::StringValue(v.into())
}

fn resource(ns: &str) -> Option<Resource> {
    Some(Resource {
        attributes: vec![kv("service.name", s("agent")), kv("k8s.namespace.name", s(ns))],
        dropped_attributes_count: 0,
        entity_refs: Vec::new(),
    })
}

/// A decoded object: its rows.
fn read(body: bytes::Bytes) -> RecordBatch {
    let batches: Vec<RecordBatch> = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(body)
        .unwrap()
        .build()
        .unwrap()
        .map(|b| b.unwrap())
        .collect();
    arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap()
}

fn bytes_at(a: &dyn Array, i: usize) -> Vec<u8> {
    if let Some(b) = a.as_any().downcast_ref::<BinaryArray>() {
        return b.value(i).to_vec();
    }
    a.as_any().downcast_ref::<StringArray>().unwrap().value(i).as_bytes().to_vec()
}

fn map_at(m: &MapArray, i: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let e = m.value(i);
    (0..e.len()).map(|j| (bytes_at(e.column(0).as_ref(), j), bytes_at(e.column(1).as_ref(), j))).collect()
}

/// What one value became: its key, the content it stands for (resolved
/// through the payloads when offloaded) and its markers.
struct Resolved {
    key: Vec<u8>,
    content: Vec<u8>,
    offloaded: bool,
    markers: HashMap<String, Vec<u8>>,
}

/// Resolves a map whose first `n` entries are the producer's values and the
/// rest the edge's markers, in the order the values were met (`bytes`, then
/// `truncated_from`, then `elements`; or `redacted_from`), against `payloads`.
fn resolve(attrs: &[(Vec<u8>, Vec<u8>)], n: usize, payloads: &HashMap<String, Vec<u8>>) -> Vec<Resolved> {
    let (vals, markers) = attrs.split_at(n);
    let mut mi = markers.iter().peekable();
    let mut out = Vec::new();
    for (k, v) in vals {
        let mk = |w: &str| [offload::MARKER_PREFIX, k, b".", w.as_bytes()].concat();
        let mut m: HashMap<String, Vec<u8>> = HashMap::new();
        for w in ["redacted_from", "bytes", "truncated_from", "elements"] {
            if let Some((_, val)) = mi.next_if(|(mk_, _)| *mk_ == mk(w)) {
                let _ = m.insert(w.to_string(), val.clone());
            }
        }
        if !m.contains_key("bytes") {
            out.push(Resolved { key: k.clone(), content: v.clone(), offloaded: false, markers: m });
            continue;
        }
        let doc: Vec<String> = serde_json::from_slice(v).expect("a reference document");
        let parts: Vec<&Vec<u8>> = doc
            .iter()
            .map(|r| {
                assert!(r.len() == 34 && r.starts_with("h:") && r[2..].bytes().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()), "{r}");
                payloads.get(&r[2..]).unwrap_or_else(|| panic!("dangling reference {r}"))
            })
            .collect();
        let content = if m.contains_key("elements") {
            assert_eq!(m["elements"], parts.len().to_string().into_bytes());
            let mut c = b"[".to_vec();
            for (i, p) in parts.iter().enumerate() {
                if i > 0 {
                    c.push(b',');
                }
                c.extend_from_slice(p);
            }
            c.push(b']');
            c
        } else {
            assert_eq!(parts.len(), 1);
            parts[0].clone()
        };
        out.push(Resolved { key: k.clone(), content, offloaded: true, markers: m });
    }
    assert!(mi.next().is_none(), "a marker no value accounts for");
    out
}

/// Normalises a value the way a split does (whitespace between elements
/// dropped): the elements joined by ',' in '[' ']'.
fn normal(k: &[u8], v: &[u8], opts: &OffloadOptions) -> Vec<u8> {
    let v = &v[..truncate_at(v, opts.max_value)];
    if opts.split_keys.iter().any(|s| s.as_bytes() == k)
        && let Some(e) = json_array_elements(v, opts.split_max_depth, opts.split_max_elements)
    {
        let mut c = b"[".to_vec();
        for (i, (a, b)) in e.iter().enumerate() {
            if i > 0 {
                c.push(b',');
            }
            c.extend_from_slice(&v[*a..*b]);
        }
        c.push(b']');
        return c;
    }
    v.to_vec()
}

fn corpus_values() -> Vec<(&'static str, String)> {
    let conv = |n: usize| {
        let mut m = vec![json!({"role": "system", "content": "You are a careful agent. ".repeat(40)})];
        for i in 0..n {
            m.push(json!({"role": "user", "content": format!("question {i}")}));
            m.push(json!({"role": "assistant", "content": format!("answer {i} ").repeat(30)}));
        }
        serde_json::to_string_pretty(&m).unwrap()
    };
    vec![
        ("gen_ai.input.messages", conv(1)),
        ("gen_ai.input.messages", conv(3)),
        ("gen_ai.output.messages", "[{\"role\":\"assistant\",\"parts\":[{\"type\":\"text\",\"content\":\"ok\"}]}]".into()),
        ("gen_ai.system_instructions", "not an array".into()),
        ("input.value", "{\"q\": \"é🚀\"}".into()),
        ("huge", "x".repeat(9 << 20)),
        ("bomb", "[".repeat(50_000)),
        ("gen_ai.input.messages", format!("{}{}", "[".repeat(100), "]".repeat(100))),
        ("otel.payload.x.bytes", "5".into()),
        ("small", "fine".into()),
        ("gen_ai.tool.call.arguments", String::new()),
    ]
}

fn traces_request(ns: &[&str]) -> Vec<u8> {
    let vals = corpus_values();
    TracesData {
        resource_spans: ns
            .iter()
            .map(|n| ResourceSpans {
                resource: resource(n),
                schema_url: String::new(),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    schema_url: String::new(),
                    spans: (0..3)
                        .map(|i| Span {
                            trace_id: vec![1; 16],
                            span_id: vec![i as u8 + 1; 8],
                            name: format!("chat {i}"),
                            start_time_unix_nano: T + i,
                            end_time_unix_nano: T + i + 5,
                            attributes: vals.iter().skip(i as usize).map(|(k, v)| kv(k, s(v))).collect(),
                            events: vec![Event { time_unix_nano: T, name: "gen_ai.choice".into(), attributes: vec![kv("gen_ai.output.messages", s(&vals[0].1))], dropped_attributes_count: 0 }],
                            ..Default::default()
                        })
                        .collect(),
                }],
            })
            .collect(),
    }
    .encode_to_vec()
}

fn logs_request(ns: &str) -> Vec<u8> {
    let vals = corpus_values();
    LogsData {
        resource_logs: vec![ResourceLogs {
            resource: resource(ns),
            schema_url: String::new(),
            scope_logs: vec![ScopeLogs {
                scope: None,
                schema_url: String::new(),
                log_records: vals
                    .iter()
                    .map(|(k, v)| LogRecord {
                        time_unix_nano: T,
                        body: Some(AnyValue { value: Some(s(v)) }),
                        attributes: vec![kv(k, s(v))],
                        event_name: "gen_ai.client.inference.operation.details".into(),
                        ..Default::default()
                    })
                    .collect(),
            }],
        }],
    }
    .encode_to_vec()
}

fn encoder(opts: &OffloadOptions) -> Encoder {
    Encoder::new(ParquetOptions::default(), Format::Parquet).with_offload(opts.clone(), "c1")
}

fn env(seq: u64) -> Envelope {
    Envelope { producer: "p".into(), epoch: "E".into(), batch: seq, received_ns: T }
}

/// Every offloaded value of every row resolves in the object's own payload
/// part to what the producer sent (up to the cap and a split's whitespace);
/// the markers say what happened; nothing inline changed.
#[test]
fn the_hostile_corpus_round_trips_through_the_object() {
    // D36 phase 1 exit criterion (ci/trace/exit-criteria.txt): hostile sizes and shapes
    let _trace = otap_s3pq::oscope_trace::covers("PH", &["UCA-L2", "UCA-L3"]);
    let opts = OffloadOptions::default();
    for (sig, bytes) in [(Signal::Traces, traces_request(&["team-a", "team-b"])), (Signal::Logs, logs_request("team-a"))] {
        let mut enc = encoder(&opts);
        let f = enc.flatten_all_at(&Input::Otlp(sig, &bytes), T).unwrap().pop().unwrap();
        let (o, carried) = enc.encode_carrying(&f, &env(1), &|_| true, &|_| true).unwrap();
        assert_eq!(o.meta["oscope-payloads"], f.payloads.len().to_string());
        assert_eq!(o.meta["oscope-payload-refs"], f.payloads.len().to_string());
        assert_eq!(o.meta["oscope-schema"], "3");
        assert_eq!(carried.payloads.len(), f.payloads.len());
        let rb = read(o.body);
        let pm = rb.column_by_name("payloads").unwrap().as_any().downcast_ref::<MapArray>().unwrap();
        let mut payloads: HashMap<String, Vec<u8>> = HashMap::new();
        for r in 0..rb.num_rows() {
            for (k, v) in map_at(pm, r) {
                assert!(payloads.insert(String::from_utf8(k).unwrap(), v).is_none(), "a payload carried twice in one object");
            }
        }
        assert_eq!(payloads.len(), f.payloads.len());
        let vals = corpus_values();
        let refs = rb.column_by_name("payload_refs").unwrap().as_any().downcast_ref::<ListArray>().unwrap();
        let attr_col = if sig == Signal::Traces { "SpanAttributes" } else { "LogAttributes" };
        let am = rb.column_by_name(attr_col).unwrap().as_any().downcast_ref::<MapArray>().unwrap();
        for r in 0..rb.num_rows() {
            // the producer's values of row r, in order (a log body first)
            let mut sent: Vec<(&[u8], &[u8])> = Vec::new();
            if sig == Signal::Logs {
                sent.push((b"@body", vals[r].1.as_bytes()));
                sent.push((vals[r].0.as_bytes(), vals[r].1.as_bytes()));
            } else {
                sent.extend(vals.iter().skip(r % 3).map(|(k, v)| (k.as_bytes(), v.as_bytes())));
            }
            let mut attrs = map_at(am, r);
            if sig == Signal::Logs {
                attrs.insert(0, (b"@body".to_vec(), bytes_at(rb.column_by_name("Body").unwrap().as_ref(), r)));
            }
            let got = resolve(&attrs, sent.len(), &payloads);
            assert_eq!(got.len(), sent.len());
            // every reference of the row is listed in payload_refs, distinct
            let listed: Vec<Vec<u8>> = { let l = refs.value(r); (0..l.len()).map(|i| bytes_at(l.as_ref(), i)).collect() };
            let mut d = listed.clone();
            d.sort();
            d.dedup();
            assert_eq!(d.len(), listed.len(), "payload_refs are distinct per row");
            for (g, (k, orig)) in got.iter().zip(&sent) {
                assert_eq!(g.key, k.to_vec());
                let should = orig.len() > opts.threshold || (opts.keys.iter().any(|x| x.as_bytes() == *k) && !orig.is_empty());
                assert_eq!(g.offloaded, should, "{} row {r}", String::from_utf8_lossy(k));
                if !should {
                    assert_eq!(&g.content, orig);
                    continue;
                }
                assert_eq!(g.content, normal(k, orig, &opts), "{} row {r}", String::from_utf8_lossy(k));
                assert_eq!(g.markers["bytes"], truncate_at(orig, opts.max_value).to_string().into_bytes());
                assert_eq!(g.markers.get("truncated_from").cloned(), (orig.len() > opts.max_value).then(|| orig.len().to_string().into_bytes()));
            }
            // each offloaded value's references are in payload_refs
            for (i, (k, v)) in attrs.iter().enumerate().take(sent.len()) {
                if got[i].offloaded {
                    for h in serde_json::from_slice::<Vec<String>>(v).unwrap() {
                        assert!(listed.contains(&h[2..].as_bytes().to_vec()), "{}", String::from_utf8_lossy(k));
                    }
                }
            }
        }
        // the tenants: the same content in two namespaces is two payloads
        if sig == Signal::Traces {
            let h = |ns: &[u8]| hex::encode(payload_hash(&tenant_key("c1", ns, T), b"fine"));
            assert!(!payloads.contains_key(&h(b"team-a")), "under the threshold: inline");
            let big = |ns: &[u8]| hex::encode(payload_hash(&tenant_key("c1", ns, T), "x".repeat(8 << 20).as_bytes()));
            assert!(payloads.contains_key(&big(b"team-a")) && payloads.contains_key(&big(b"team-b")));
        }
    }
}

/// The payload part is decided per slot: a payload the lane's epoch already
/// carried in a committed object is referenced, not carried; a new epoch
/// carries it again; the part rides on the first row that references it.
#[test]
fn the_payload_part_follows_the_lane_cache() {
    let opts = OffloadOptions::default();
    let mut enc = encoder(&opts);
    let bytes = traces_request(&["team-a"]);
    let f = enc.flatten_all_at(&Input::Otlp(Signal::Traces, &bytes), T).unwrap().pop().unwrap();
    let mut cache = PayloadCache::default();
    let (o1, c1) = enc.encode_carrying(&f, &env(1), &|_| true, &|h| cache.wants("E", h)).unwrap();
    assert_eq!(c1.payloads.len(), f.payloads.len());
    cache.sent("E", &c1.payloads, 1000);
    let (o2, c2) = enc.encode_carrying(&f, &env(2), &|_| true, &|h| cache.wants("E", h)).unwrap();
    assert!(c2.payloads.is_empty());
    assert_eq!(o2.meta["oscope-payloads"], "0");
    assert_eq!(o2.meta["oscope-payload-refs"], f.payloads.len().to_string());
    let (_, c3) = enc.encode_carrying(&f, &env(1), &|_| true, &|h| cache.wants("E2", h)).unwrap();
    assert_eq!(c3.payloads.len(), f.payloads.len(), "a new epoch carries again");
    let rb2 = read(o2.body);
    let pm2 = rb2.column_by_name("payloads").unwrap().as_any().downcast_ref::<MapArray>().unwrap();
    assert!((0..rb2.num_rows()).all(|r| pm2.value_length(r) == 0), "the second object carries no content");
    // the part is on each payload's first row
    let rb = read(o1.body);
    let pm = rb.column_by_name("payloads").unwrap().as_any().downcast_ref::<MapArray>().unwrap();
    for p in &f.payloads {
        let row = map_at(pm, p.first_row as usize);
        assert!(row.iter().any(|(k, _)| k == hex::encode(p.hash).as_bytes()));
    }
}

/// Off: nothing changes but the empty columns (the stock row shape).
#[test]
fn offloading_off_keeps_every_value() {
    let bytes = traces_request(&["team-a"]);
    let mut on = encoder(&OffloadOptions::off());
    let mut plain = Encoder::new(ParquetOptions::default(), Format::Parquet);
    let a = on.flatten_all_at(&Input::Otlp(Signal::Traces, &bytes), T).unwrap().pop().unwrap();
    let b = plain.flatten(&Input::Otlp(Signal::Traces, &bytes)).unwrap();
    assert_eq!(a.cols.len(), b.cols.len());
    for (x, y) in a.cols.iter().zip(&b.cols) {
        assert_eq!(x.to_data(), y.to_data());
    }
    assert!(a.payloads.is_empty());
}

/// A split request (D31) carries each part's own payloads, on the part's
/// first row that references each.
#[test]
fn a_late_split_carries_each_parts_payloads() {
    let opts = OffloadOptions::default();
    let mut enc = encoder(&opts);
    let big = |c: char| c.to_string().repeat(5000);
    let d = TracesData {
        resource_spans: vec![ResourceSpans {
            resource: resource("team-a"),
            schema_url: String::new(),
            scope_spans: vec![ScopeSpans {
                scope: None,
                schema_url: String::new(),
                spans: [(T, 'a'), (T - 3_600_000_000_000, 'b'), (T - 1, 'a'), (T - 3_600_000_000_001, 'a')]
                    .iter()
                    .map(|(t, c)| Span { trace_id: vec![1; 16], span_id: vec![2; 8], start_time_unix_nano: *t, attributes: vec![kv("v", s(&big(*c)))], ..Default::default() })
                    .collect(),
            }],
        }],
    }
    .encode_to_vec();
    let f = enc.flatten_all_at(&Input::Otlp(Signal::Traces, &d), T).unwrap().pop().unwrap();
    assert_eq!(f.payloads.len(), 2);
    let parts = enc.split_late(f, 15 * 60_000_000_000, |n, _| n.to_string()).unwrap();
    assert_eq!(parts.len(), 2);
    let h = |c: char| payload_hash(&tenant_key("c1", b"team-a", T), big(c).as_bytes());
    assert_eq!(parts[0].payloads.iter().map(|p| (p.hash, p.first_row)).collect::<Vec<_>>(), vec![(h('a'), 0)]);
    assert_eq!(parts[1].payloads.iter().map(|p| (p.hash, p.first_row)).collect::<Vec<_>>(), vec![(h('b'), 0), (h('a'), 1)]);
    for p in &parts {
        let (o, c) = enc.encode_carrying(p, &env(1), &|_| true, &|_| true).unwrap();
        assert_eq!(c.payloads.len(), p.payloads.len());
        assert_eq!(o.meta["oscope-payloads"], p.payloads.len().to_string());
    }
}

/// The request cap and the policy are validated together (CAST 25).
#[test]
fn the_policy_is_validated_together() {
    assert!(OffloadOptions::default().validate().is_ok());
    let bad = OffloadOptions { max_value: 256 << 20, ..Default::default() };
    assert!(bad.validate().unwrap_err().contains("max_request_bytes"));
    let cfg: OffloadOptions = serde_json::from_value(json!({"threshold": 1024, "keys": ["a"], "split_keys": []})).unwrap();
    assert!(cfg.validate().is_ok() && cfg.enabled);
    assert!(serde_json::from_value::<OffloadOptions>(json!({"treshold": 1})).is_err(), "unknown fields are refused");
}

// ---- properties ------------------------------------------------------------------------------

fn json_doc(tc: &TestCase, depth: usize) -> J {
    let leaf = tc.draw(gs::integers::<u8>().max_value(if depth == 0 { 3 } else { 5 }));
    match leaf {
        0 => J::Null,
        1 => J::Bool(tc.draw(gs::booleans())),
        2 => json!(tc.draw(gs::integers::<i64>())),
        3 => J::String(tc.draw(gs::text().max_size(12))),
        4 => J::Array((0..tc.draw(gs::integers::<usize>().max_value(4))).map(|_| json_doc(tc, depth - 1)).collect()),
        _ => J::Object((0..tc.draw(gs::integers::<usize>().max_value(4))).map(|_| (tc.draw(gs::text().max_size(6)), json_doc(tc, depth - 1))).collect()),
    }
}

/// On valid JSON (serde_json's output, compact or pretty), the scan's
/// elements are the array's elements.
#[hegel::test]
fn prop_scan_agrees_with_serde_on_valid_documents(tc: TestCase) {
    let n = tc.draw(gs::integers::<usize>().max_value(8));
    let arr: Vec<J> = (0..n).map(|_| json_doc(&tc, 3)).collect();
    let text = if tc.draw(gs::booleans()) { serde_json::to_string_pretty(&arr).unwrap() } else { serde_json::to_string(&arr).unwrap() };
    let e = json_array_elements(text.as_bytes(), 64, 4096).expect("a valid array is split");
    assert_eq!(e.len(), arr.len());
    for ((a, b), want) in e.iter().zip(&arr) {
        assert_eq!(&serde_json::from_str::<J>(&text[*a..*b]).unwrap(), want);
    }
}

/// On arbitrary bytes the scan never panics, and whatever it accepts is an
/// array serde_json accepts too (when it is UTF-8 without lone surrogates),
/// with the same element count.
#[hegel::test]
fn prop_scan_is_total_and_never_more_lenient(tc: TestCase) {
    let alphabet: Vec<u8> = b"[]{},:\" \\/u0123456789aefnrtl-+.E\xff\x00".to_vec();
    let n = tc.draw(gs::integers::<usize>().max_value(64));
    let v: Vec<u8> = (0..n).map(|_| alphabet[tc.draw(gs::integers::<usize>().max_value(alphabet.len() - 1))]).collect();
    let d = tc.draw(gs::integers::<usize>().min_value(1).max_value(8));
    if let Some(e) = json_array_elements(&v, d, 64)
        && let Ok(text) = std::str::from_utf8(&v)
        && !text.contains("\\u")
    {
        match serde_json::from_str::<J>(text) {
            Ok(J::Array(a)) => assert_eq!(a.len(), e.len(), "{text:?}"),
            other => panic!("scan accepted {text:?}, serde: {other:?}"),
        }
    }
}

/// Arbitrary attributes through the offloader: every value is either stored
/// as it came or replaced by a reference document whose payloads give it
/// back (up to the cap and a split's whitespace), with exactly the markers
/// its fate calls for; payloads are distinct and the stats add up.
#[hegel::test]
fn prop_offload_round_trips(tc: TestCase) {
    let threshold = tc.draw(gs::integers::<usize>().min_value(1).max_value(64));
    let max_value = tc.draw(gs::integers::<usize>().min_value(threshold + 1).max_value(256));
    let opts = OffloadOptions {
        threshold,
        max_value,
        keys: vec!["k.listed".into(), "k.split".into()],
        split_keys: vec!["k.split".into()],
        redact_keys: vec!["k.secret".into()],
        split_max_depth: tc.draw(gs::integers::<usize>().min_value(1).max_value(6)),
        split_max_elements: tc.draw(gs::integers::<usize>().min_value(1).max_value(8)),
        ..Default::default()
    };
    let mut o = offloader(opts.clone(), "c", T);
    let ns = tc.draw(gs::binary().max_size(8));
    o.tenant(&ns);
    let keys = ["k.listed", "k.split", "k.secret", "k.other", "k.x"];
    let n = tc.draw(gs::integers::<usize>().max_value(10));
    let mut out = Vec::new();
    let mut stored: Vec<(String, Vec<u8>, Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)> = Vec::new();
    for row in 0..n {
        let k = keys[tc.draw(gs::integers::<usize>().max_value(keys.len() - 1))];
        let v: Vec<u8> = if tc.draw(gs::booleans()) {
            serde_json::to_vec(&(0..tc.draw(gs::integers::<usize>().max_value(10))).map(|_| json_doc(&tc, 2)).collect::<Vec<_>>()).unwrap()
        } else {
            tc.draw(gs::binary().max_size(300))
        };
        let oc = o.value(k.as_bytes(), &v, row as u32, &mut out);
        let st = if oc == Outcome::Inline { v.clone() } else { out.clone() };
        stored.push((k.to_string(), v, st, std::mem::take(&mut o.markers)));
        let mut refs = otap_s3pq::columns::Bin::default();
        refs.clear();
        let _ = o.end_row(&mut refs);
    }
    let payloads: HashMap<String, Vec<u8>> = o.payloads.iter().map(|p| (hex::encode(p.hash), p.content.clone())).collect();
    assert_eq!(payloads.len(), o.payloads.len(), "distinct");
    for p in &o.payloads {
        assert_eq!(p.hash, payload_hash(&tenant_key("c", &ns, T), &p.content), "keyed by the tenant and day");
    }
    let mut offloaded = 0;
    for (k, v, st, markers) in &stored {
        let m: HashMap<Vec<u8>, Vec<u8>> = markers.iter().cloned().collect();
        let mk = |w: &str| [offload::MARKER_PREFIX, k.as_bytes(), b".", w.as_bytes()].concat();
        if k == "k.secret" {
            assert!(st.is_empty());
            assert_eq!(m.contains_key(&mk("redacted_from")), !v.is_empty());
            continue;
        }
        let should = v.len() > threshold || ((k == "k.listed" || k == "k.split") && !v.is_empty());
        if !should {
            assert_eq!(st, v);
            assert!(m.is_empty());
            continue;
        }
        offloaded += 1;
        let mut attrs = vec![(k.as_bytes().to_vec(), st.clone())];
        attrs.extend(markers.iter().cloned());
        let got = resolve(&attrs, 1, &payloads);
        assert!(got[0].offloaded);
        assert_eq!(got[0].content, normal(k.as_bytes(), v, &opts));
        assert_eq!(m[&mk("bytes")], truncate_at(v, max_value).to_string().into_bytes());
        assert_eq!(m.contains_key(&mk("truncated_from")), v.len() > max_value);
    }
    assert_eq!(o.stats.offloaded, offloaded);
}
