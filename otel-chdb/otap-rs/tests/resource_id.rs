//! `resource_id` at the Rust edge against the shared vectors
//! (`../../entities/testdata/resource_id_vectors.json`, generated from the
//! entity controller's `internal/rid`, checked there and by parquetgo), and
//! Hegel properties: arbitrary attribute lists give the reference
//! implementation's id (a transcription of the controller's `rid.Split` +
//! `rid.ID`), whatever their order; the walk stamps that id on every row of
//! the resource, on the OTLP and the OTAP path alike, and announces each
//! resource once, on its first row.
//!
//!   cargo test --release --test resource_id

use hegel::TestCase;
use hegel::generators as gs;
use otap_s3pq::Signal;
use otap_s3pq::batch::{Encoder, Format, Input};
use otap_s3pq::encode::ParquetOptions;
use otap_s3pq::resource::{COVERED_KEYS, CoveredBuilder, LABEL_PREFIX, empty_id, is_covered_key};
use std::collections::BTreeMap;

const VECTORS: &str = include_str!("../../entities/testdata/resource_id_vectors.json");

/// One attribute: key, and the value's bytes if it is a string.
type Attr = (Vec<u8>, Option<Vec<u8>>);

fn unhex(s: &str) -> Vec<u8> {
    hex::decode(s).expect("hex")
}

fn attr_of(a: &serde_json::Value) -> Attr {
    let k = match a.get("k_hex") {
        Some(h) => unhex(h.as_str().unwrap()),
        None => a["k"].as_str().unwrap_or("").as_bytes().to_vec(),
    };
    let v = match a.get("v_hex") {
        Some(h) => unhex(h.as_str().unwrap()),
        None => a["v"].as_str().unwrap_or("").as_bytes().to_vec(),
    };
    let string = a.get("t").and_then(|t| t.as_str()).is_none_or(|t| t == "str");
    (k, string.then_some(v))
}

fn ours(attrs: &[Attr]) -> otap_s3pq::resource::Covered {
    let mut b = CoveredBuilder::default();
    for (k, v) in attrs {
        b.push(k, v.as_deref());
    }
    b.finish()
}

/// The reference: the controller's `rid.Split` then `rid.ID`, written
/// plainly (a map of first occurrences, then the hash over the sorted map).
fn reference(attrs: &[Attr]) -> (u64, Vec<(Vec<u8>, Vec<u8>)>) {
    let mut seen = std::collections::HashSet::new();
    let mut m: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for (k, v) in attrs {
        if !seen.insert(k.clone()) {
            continue;
        }
        let named = COVERED_KEYS.iter().any(|c| c.as_bytes() == k.as_slice())
            || (k.len() > LABEL_PREFIX.len() && k.starts_with(LABEL_PREFIX) && !k.contains(&0));
        if let Some(v) = v
            && named
            && !v.is_empty()
            && !v.contains(&0)
        {
            let _ = m.insert(k.clone(), v.clone());
        }
    }
    let mut b = b"res.v1\0".to_vec();
    for (k, v) in &m {
        b.extend_from_slice(k);
        b.push(0);
        b.extend_from_slice(v);
        b.push(0);
    }
    (xxhash_rust::xxh3::xxh3_64(&b), m.into_iter().collect())
}

#[test]
fn the_shared_vectors() {
    let f: serde_json::Value = serde_json::from_str(VECTORS).unwrap();
    let keys: Vec<&str> = f["covered_keys"].as_array().unwrap().iter().map(|k| k.as_str().unwrap()).collect();
    assert_eq!(keys, COVERED_KEYS, "the vectors' covered keys are the controller's: resource.rs must list the same");
    assert_eq!(f["label_prefix"].as_str().unwrap().as_bytes(), LABEL_PREFIX);
    let vs = f["vectors"].as_array().unwrap();
    assert!(vs.len() >= 15);
    for v in vs {
        let attrs: Vec<Attr> = v["attrs"].as_array().unwrap().iter().map(attr_of).collect();
        let got = ours(&attrs);
        let want_id: u64 = v["resource_id"].as_str().unwrap().parse().unwrap();
        let want_pairs: Vec<(Vec<u8>, Vec<u8>)> = v["covered_hex"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| (unhex(p[0].as_str().unwrap()), unhex(p[1].as_str().unwrap())))
            .collect();
        assert_eq!(got.pairs, want_pairs, "{}: covered set", v["name"]);
        assert_eq!(got.id, want_id, "{}: resource_id", v["name"]);
        assert_eq!(format!("{:016x}", got.id), v["resource_id_hex"].as_str().unwrap());
        assert_eq!(reference(&attrs).0, want_id, "{}: the test's reference", v["name"]);
    }
}

/// A key: covered by name, a label (any rest, NUL and invalid UTF-8
/// included), a near miss, or anything.
fn key(tc: &TestCase) -> Vec<u8> {
    match tc.draw(gs::integers::<u8>().max_value(9)) {
        0..=3 => tc.draw(gs::sampled_from(COVERED_KEYS.to_vec())).as_bytes().to_vec(),
        4..=5 => {
            let mut k = LABEL_PREFIX.to_vec();
            if tc.draw(gs::booleans()) {
                k.extend_from_slice(tc.draw(gs::text().max_size(12)).as_bytes());
            } else {
                k.extend(tc.draw(gs::binary().max_size(6)));
            }
            k
        }
        6 => tc
            .draw(gs::sampled_from(vec!["K8s.pod.name", "k8s.pod.name ", "k8s.pod.labels.x", "service.version", "k8s.pod.label", "", "telemetry.sdk.name"]))
            .as_bytes()
            .to_vec(),
        _ => tc.draw(gs::binary().max_size(20)),
    }
}

fn value(tc: &TestCase) -> Option<Vec<u8>> {
    match tc.draw(gs::integers::<u8>().max_value(9)) {
        0 => None, // a non-string value
        1 => Some(Vec::new()),
        2 => Some(tc.draw(gs::binary().max_size(16))), // NUL, invalid UTF-8
        _ => Some(tc.draw(gs::text().max_size(24)).into_bytes()),
    }
}

fn attrs(tc: &TestCase, max: usize) -> Vec<Attr> {
    let n = tc.draw(gs::integers::<usize>().max_value(max));
    (0..n).map(|_| (key(tc), value(tc))).collect()
}

/// Any attribute list: the edge's split and hash equal the reference.
#[hegel::test]
fn prop_resource_id_is_the_references(tc: TestCase) {
    let a = attrs(&tc, 40);
    let got = ours(&a);
    let (id, pairs) = reference(&a);
    assert_eq!(got.pairs, pairs);
    assert_eq!(got.id, id);
    assert!(got.pairs.iter().all(|(k, v)| is_covered_key(k) && !v.is_empty() && !v.contains(&0)));
    if got.pairs.is_empty() {
        assert_eq!(got.id, empty_id());
    }
}

/// Attribute order doesn't matter once each key appears once (with
/// duplicates the first occurrence decides, which is order by definition).
#[hegel::test]
fn prop_resource_id_ignores_attribute_order(tc: TestCase) {
    let mut seen = std::collections::HashSet::new();
    let a: Vec<Attr> = attrs(&tc, 40).into_iter().filter(|(k, _)| seen.insert(k.clone())).collect();
    let b = tc.draw(gs::permutations(a.clone()));
    assert_eq!(ours(&a), ours(&b));
    // Residual keys added anywhere change nothing.
    let mut c = b.clone();
    let at = tc.draw(gs::integers::<usize>().max_value(c.len()));
    c.insert(at, (b"telemetry.sdk.language".to_vec(), Some(b"rust".to_vec())));
    if !a.iter().any(|(k, _)| k == b"telemetry.sdk.language") {
        assert_eq!(ours(&a).id, ours(&c).id);
    }
}

// ---- through the walk: OTLP and OTAP ------------------------------------------------------

use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value::Value};
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{LogRecord, LogsData, ResourceLogs, ScopeLogs};
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
use otel_arrow_dfe_pdata::proto::opentelemetry::trace::v1::{ResourceSpans, ScopeSpans, Span, TracesData};
use prost::Message;

/// A proto attribute from a drawn one: string keys and values only (proto
/// strings must be UTF-8 on the OTAP path's conversion), a non-string as int.
fn proto_attrs(tc: &TestCase) -> (Vec<KeyValue>, Vec<Attr>) {
    let n = tc.draw(gs::integers::<usize>().max_value(12));
    let mut kvs = Vec::new();
    let mut raw = Vec::new();
    for _ in 0..n {
        let k = String::from_utf8_lossy(&key(tc)).into_owned();
        let v = value(tc).map(|v| String::from_utf8_lossy(&v).into_owned());
        let value = Some(AnyValue { value: Some(match &v { Some(s) => Value::StringValue(s.clone()), None => Value::IntValue(7) }) });
        raw.push((k.as_bytes().to_vec(), v.map(String::into_bytes)));
        kvs.push(KeyValue { key: k, value });
    }
    (kvs, raw)
}

/// Every row carries its resource's id; every distinct non-empty resource is
/// announced once, on its first row; the OTLP and OTAP paths agree.
#[hegel::test]
fn prop_rows_carry_resource_id_on_both_paths(tc: TestCase) {
    use arrow::array::{Array, MapArray, UInt64Array};
    use otel_arrow_dfe_pdata::{OtapArrowRecords, OtlpProtoBytes, TryIntoWithOptions};
    let logs = tc.draw(gs::booleans());
    let nres = tc.draw(gs::integers::<usize>().min_value(1).max_value(4));
    let mut ress: Vec<(Vec<KeyValue>, Vec<Attr>)> = Vec::new();
    let mut per: Vec<usize> = Vec::new();
    for _ in 0..nres {
        let r = if tc.draw(gs::booleans()) && !ress.is_empty() {
            ress[0].clone() // the same resource again, in another ResourceSpans
        } else {
            proto_attrs(&tc)
        };
        ress.push(r);
        per.push(tc.draw(gs::integers::<usize>().min_value(1).max_value(3)));
    }
    let expect: Vec<u64> = ress.iter().zip(&per).flat_map(|((_, raw), n)| std::iter::repeat_n(reference(raw).0, *n)).collect();
    let (sig, bytes) = if logs {
        let d = LogsData {
            resource_logs: ress
                .iter()
                .zip(&per)
                .map(|((kvs, _), n)| ResourceLogs {
                    resource: Some(Resource { attributes: kvs.clone(), dropped_attributes_count: 0, entity_refs: Vec::new() }),
                    schema_url: String::new(),
                    scope_logs: vec![ScopeLogs {
                        scope: None,
                        schema_url: String::new(),
                        log_records: (0..*n).map(|i| LogRecord { time_unix_nano: 1_790_000_000_000_000_000 + i as u64, ..Default::default() }).collect(),
                    }],
                })
                .collect(),
        };
        (Signal::Logs, d.encode_to_vec())
    } else {
        let d = TracesData {
            resource_spans: ress
                .iter()
                .zip(&per)
                .map(|((kvs, _), n)| ResourceSpans {
                    resource: Some(Resource { attributes: kvs.clone(), dropped_attributes_count: 0, entity_refs: Vec::new() }),
                    schema_url: String::new(),
                    scope_spans: vec![ScopeSpans {
                        scope: None,
                        schema_url: String::new(),
                        spans: (0..*n)
                            .map(|i| Span { trace_id: vec![1; 16], span_id: vec![2; 8], start_time_unix_nano: 1_790_000_000_000_000_000 + i as u64, ..Default::default() })
                            .collect(),
                    }],
                })
                .collect(),
        };
        (Signal::Traces, d.encode_to_vec())
    };
    let mut enc = Encoder::new(ParquetOptions::default(), Format::Parquet);
    let direct = enc.flatten(&Input::Otlp(sig, &bytes)).expect("flatten");
    let ids = direct.cols.last().unwrap().as_any().downcast_ref::<UInt64Array>().expect("resource_id is the last content column");
    assert_eq!(ids.values().to_vec(), expect);
    // one announcement per distinct non-empty resource, at its first row
    let mut distinct: Vec<u64> = Vec::new();
    for ((_, raw), _) in ress.iter().zip(&per) {
        let (id, pairs) = reference(raw);
        if !pairs.is_empty() && !distinct.contains(&id) {
            distinct.push(id);
        }
    }
    assert_eq!(direct.resources.iter().map(|(c, _)| c.id).collect::<Vec<_>>(), distinct);
    for (c, row) in &direct.resources {
        assert_eq!(expect.iter().position(|x| *x == c.id), Some(*row as usize));
    }
    // encoded: resource_announce holds each covered set on its first row
    let env = otap_s3pq::flatten::Envelope { producer: "p".into(), epoch: "E".into(), batch: 1, received_ns: 1_790_000_000_000_000_000 };
    let (o, announced) = enc.encode_announcing(&direct, &env, &|_| true).unwrap();
    assert_eq!(announced, distinct);
    assert_eq!(o.meta.get("oscope-announce").map(String::as_str), Some(distinct.len().to_string().as_str()));
    let rb = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(o.body)
        .unwrap()
        .build()
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let ann = rb.column_by_name("resource_announce").unwrap().as_any().downcast_ref::<MapArray>().unwrap();
    let rid = rb.column_by_name("resource_id").unwrap().as_any().downcast_ref::<UInt64Array>().unwrap();
    let mut seen_ann = Vec::new();
    for r in 0..rb.num_rows() {
        let n = ann.value_length(r);
        if n > 0 {
            let id = rid.value(r);
            let (_, pairs) = reference(&ress.iter().find(|(_, raw)| reference(raw).0 == id).unwrap().1);
            assert_eq!(n as usize, pairs.len());
            seen_ann.push(id);
        }
    }
    seen_ann.sort_unstable();
    let mut d = distinct.clone();
    d.sort_unstable();
    assert_eq!(seen_ann, d, "each announced resource once, on a row of its own");
    // none announced: every map empty
    let (o2, none) = enc.encode_announcing(&direct, &env, &|_| false).unwrap();
    assert!(none.is_empty() && o2.meta.get("oscope-announce").map(String::as_str) == Some("0"));
    // the OTAP path
    let p = if logs { OtlpProtoBytes::ExportLogsRequest(bytes.clone().into()) } else { OtlpProtoBytes::ExportTracesRequest(bytes.clone().into()) };
    let recs: OtapArrowRecords = match p.try_into_with_default() {
        Ok(r) => r,
        Err(e) => {
            tc.note(&format!("OTLP -> OTAP refused: {e}"));
            return;
        }
    };
    let via = enc.flatten(&Input::Otap(sig, &recs)).expect("flatten otap");
    assert_eq!(via.cols.last().unwrap().to_data(), direct.cols.last().unwrap().to_data(), "resource_id differs between the OTLP and OTAP paths");
    assert_eq!(via.resources, direct.resources);
}
