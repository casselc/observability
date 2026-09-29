#![no_main]
//! The offloader reads values an SDK sent (GenAI messages as JSON arrays):
//! `json_array_elements` never panics and never recurses (deep nesting is
//! refused, not a stack overflow), and its ranges are inside the input, in
//! order, non-overlapping, at most `max_elements`; `truncate_at` cuts at or
//! below `n` and never inside a UTF-8 sequence of a valid string.
use libfuzzer_sys::fuzz_target;
use otap_s3pq::offload::{json_array_elements, truncate_at};

fuzz_target!(|data: &[u8]| {
    if data.len() < 2 {
        return;
    }
    let (depth, max_el, v) = (data[0] as usize % 64, 1 + data[1] as usize % 64, &data[2..]);
    if let Some(els) = json_array_elements(v, depth, max_el) {
        assert!(els.len() <= max_el);
        let mut end = 0;
        for &(a, b) in &els {
            assert!(a >= end && a < b && b <= v.len(), "range {a}..{b} after {end} in {}", v.len());
            end = b;
        }
    }
    let n = data[0] as usize * 4 + data[1] as usize;
    let cut = truncate_at(v, n);
    assert!(cut <= v.len() && (v.len() <= n || cut <= n));
    if let Ok(s) = std::str::from_utf8(v) {
        assert!(s.is_char_boundary(cut), "cut {cut} inside a character");
    }
});
