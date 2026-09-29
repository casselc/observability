#![no_main]
//! Object metadata is written by whoever wrote the object: `Slot::from_meta`
//! never panics, and a slot is a tombstone only when its kind says so.
use std::collections::HashMap;

use libfuzzer_sys::fuzz_target;
use otap_s3pq::proto::{self, Slot};

fuzz_target!(|data: &[u8]| {
    let s = String::from_utf8_lossy(data);
    let keys = [proto::META_KIND, proto::META_EPOCH, proto::META_SEQ, proto::META_CONTENT, proto::META_ROWS, proto::META_LOW, proto::META_RECEIVED];
    let mut meta = HashMap::new();
    for (i, v) in s.split('\u{0}').enumerate() {
        let k = keys.get(i % (keys.len() + 1)).map(|k| k.to_string()).unwrap_or_else(|| v.to_string());
        meta.insert(k, v.to_string());
    }
    let tomb = meta.get(proto::META_KIND).map(String::as_str) == Some(proto::KIND_TOMB);
    match Slot::from_meta(&meta) {
        Slot::Tomb => assert!(tomb),
        _ => assert!(!tomb),
    }
});
