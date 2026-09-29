#![no_main]
//! An S3 key the consumer lists is a slot only in its one spelling: when
//! `parse_slot_key` accepts a key, `slot_key` of what it parsed gives back
//! the same key (a second spelling would give a slot a second object, which
//! the create-only PUT on the real key never sees).
use libfuzzer_sys::fuzz_target;
use otap_s3pq::proto::{parse_slot_key, slot_key};

fuzz_target!(|data: &[u8]| {
    let s = String::from_utf8_lossy(data);
    // the prefix is a lane's (no trailing slash matters), the key anything
    let (prefix, key) = match s.split_once('\n') {
        Some((p, k)) => (p.to_string(), k.to_string()),
        None => ("r/c1/p1/traces".to_string(), s.to_string()),
    };
    if let Some((epoch, seq)) = parse_slot_key(&prefix, &key) {
        assert_eq!(slot_key(&prefix, &epoch, seq), key, "accepted a second spelling of a slot key");
        assert!(!epoch.is_empty() && !epoch.contains('/'), "epoch {epoch:?}");
    }
});
