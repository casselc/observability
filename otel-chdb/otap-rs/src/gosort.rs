//! Go's `slices.SortFunc` (pdqsort, Go 1.21+), ported so that the order of
//! equal elements comes out exactly as Go leaves it.
//!
//! Why: the contrib clickhouse exporter builds every metrics map column with
//! clickhouse-go's `orderedmap.CollectN`, which sorts the entries by key with
//! `slices.SortFunc`. That sort is not stable: a map holding the same key twice
//! (possible on the wire, and kept by pcommon) comes out in pdqsort's order,
//! and a ClickHouse `Map` keeps both entries in that order. Up to 12 entries
//! pdqsort is an insertion sort, which is stable; above that only the same
//! algorithm reproduces it. `sort_like_go` takes the cheap paths when the
//! result can't depend on the algorithm (already strictly sorted, or no
//! duplicate keys), and runs this port otherwise.

use std::cmp::Ordering;

/// Sorts `data` exactly as Go's `slices.SortFunc(data, cmp)` would.
pub fn sort_func<T, F: FnMut(&T, &T) -> Ordering>(data: &mut [T], mut cmp: F) {
    let n = data.len();
    let limit = usize::BITS - n.leading_zeros();
    let mut less = |a: &T, b: &T| cmp(a, b) == Ordering::Less;
    pdqsort(data, 0, n, limit as usize, &mut less);
}

/// Sorts `idx` (indices into some entries) by `key(i)` as Go would sort the
/// entries themselves.
pub fn sort_like_go<'k, K: Fn(u32) -> &'k [u8]>(idx: &mut [u32], key: K) {
    if idx.windows(2).all(|w| key(w[0]) < key(w[1])) {
        return; // strictly increasing: every algorithm leaves it as is
    }
    if idx.len() <= 12 {
        // pdqsort's insertion sort, which is stable.
        idx.sort_by(|a, b| key(*a).cmp(key(*b)));
        return;
    }
    let mut sorted: Vec<&[u8]> = idx.iter().map(|&i| key(i)).collect();
    sorted.sort_unstable();
    if sorted.windows(2).all(|w| w[0] != w[1]) {
        idx.sort_unstable_by(|a, b| key(*a).cmp(key(*b)));
        return;
    }
    sort_func(idx, |a, b| key(*a).cmp(key(*b)));
}

type Less<'a, T> = dyn FnMut(&T, &T) -> bool + 'a;

fn insertion_sort<T>(data: &mut [T], a: usize, b: usize, less: &mut Less<'_, T>) {
    for i in a + 1..b {
        let mut j = i;
        while j > a && less(&data[j], &data[j - 1]) {
            data.swap(j, j - 1);
            j -= 1;
        }
    }
}

fn sift_down<T>(data: &mut [T], lo: usize, hi: usize, first: usize, less: &mut Less<'_, T>) {
    let mut root = lo;
    loop {
        let mut child = 2 * root + 1;
        if child >= hi {
            break;
        }
        if child + 1 < hi && less(&data[first + child], &data[first + child + 1]) {
            child += 1;
        }
        if !less(&data[first + root], &data[first + child]) {
            return;
        }
        data.swap(first + root, first + child);
        root = child;
    }
}

fn heap_sort<T>(data: &mut [T], a: usize, b: usize, less: &mut Less<'_, T>) {
    let first = a;
    let hi = b - a;
    let mut i = (hi as isize - 1) / 2;
    while i >= 0 {
        sift_down(data, i as usize, hi, first, less);
        i -= 1;
    }
    let mut i = hi as isize - 1;
    while i >= 0 {
        data.swap(first, first + i as usize);
        sift_down(data, 0, i as usize, first, less);
        i -= 1;
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Hint {
    Unknown,
    Increasing,
    Decreasing,
}

fn pdqsort<T>(data: &mut [T], mut a: usize, mut b: usize, mut limit: usize, less: &mut Less<'_, T>) {
    const MAX_INSERTION: usize = 12;
    let mut was_balanced = true;
    let mut was_partitioned = true;
    loop {
        let length = b - a;
        if length <= MAX_INSERTION {
            insertion_sort(data, a, b, less);
            return;
        }
        if limit == 0 {
            heap_sort(data, a, b, less);
            return;
        }
        if !was_balanced {
            break_patterns(data, a, b);
            limit -= 1;
        }
        let (mut pivot, mut hint) = choose_pivot(data, a, b, less);
        if hint == Hint::Decreasing {
            data[a..b].reverse();
            pivot = (b - 1) - (pivot - a);
            hint = Hint::Increasing;
        }
        if was_balanced && was_partitioned && hint == Hint::Increasing && partial_insertion_sort(data, a, b, less) {
            return;
        }
        if a > 0 && !less(&data[a - 1], &data[pivot]) {
            a = partition_equal(data, a, b, pivot, less);
            continue;
        }
        let (mid, already) = partition(data, a, b, pivot, less);
        was_partitioned = already;
        let (left, right) = (mid - a, b - mid);
        let threshold = length / 8;
        if left < right {
            was_balanced = left >= threshold;
            pdqsort(data, a, mid, limit, less);
            a = mid + 1;
        } else {
            was_balanced = right >= threshold;
            pdqsort(data, mid + 1, b, limit, less);
            b = mid;
        }
    }
}

fn partition<T>(data: &mut [T], a: usize, b: usize, pivot: usize, less: &mut Less<'_, T>) -> (usize, bool) {
    data.swap(a, pivot);
    let (mut i, mut j) = (a as isize + 1, b as isize - 1);
    while i <= j && less(&data[i as usize], &data[a]) {
        i += 1;
    }
    while i <= j && !less(&data[j as usize], &data[a]) {
        j -= 1;
    }
    if i > j {
        data.swap(j as usize, a);
        return (j as usize, true);
    }
    data.swap(i as usize, j as usize);
    i += 1;
    j -= 1;
    loop {
        while i <= j && less(&data[i as usize], &data[a]) {
            i += 1;
        }
        while i <= j && !less(&data[j as usize], &data[a]) {
            j -= 1;
        }
        if i > j {
            break;
        }
        data.swap(i as usize, j as usize);
        i += 1;
        j -= 1;
    }
    data.swap(j as usize, a);
    (j as usize, false)
}

fn partition_equal<T>(data: &mut [T], a: usize, b: usize, pivot: usize, less: &mut Less<'_, T>) -> usize {
    data.swap(a, pivot);
    let (mut i, mut j) = (a as isize + 1, b as isize - 1);
    loop {
        while i <= j && !less(&data[a], &data[i as usize]) {
            i += 1;
        }
        while i <= j && less(&data[a], &data[j as usize]) {
            j -= 1;
        }
        if i > j {
            break;
        }
        data.swap(i as usize, j as usize);
        i += 1;
        j -= 1;
    }
    i as usize
}

fn partial_insertion_sort<T>(data: &mut [T], a: usize, b: usize, less: &mut Less<'_, T>) -> bool {
    const MAX_STEPS: usize = 5;
    const SHORTEST_SHIFTING: usize = 50;
    let mut i = a + 1;
    for _ in 0..MAX_STEPS {
        while i < b && !less(&data[i], &data[i - 1]) {
            i += 1;
        }
        if i == b {
            return true;
        }
        if b - a < SHORTEST_SHIFTING {
            return false;
        }
        data.swap(i, i - 1);
        if i - a >= 2 {
            let mut j = i - 1;
            while j >= 1 {
                if !less(&data[j], &data[j - 1]) {
                    break;
                }
                data.swap(j, j - 1);
                j -= 1;
            }
        }
        if b - i >= 2 {
            let mut j = i + 1;
            while j < b {
                if !less(&data[j], &data[j - 1]) {
                    break;
                }
                data.swap(j, j - 1);
                j += 1;
            }
        }
    }
    false
}

fn break_patterns<T>(data: &mut [T], a: usize, b: usize) {
    let length = b - a;
    if length >= 8 {
        let mut random = length as u64;
        let modulus = 1usize << (usize::BITS - length.leading_zeros());
        let mut idx = a + (length / 4) * 2 - 1;
        while idx <= a + (length / 4) * 2 + 1 {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            let mut other = (random as usize) & (modulus - 1);
            if other >= length {
                other -= length;
            }
            data.swap(idx, a + other);
            idx += 1;
        }
    }
}

fn choose_pivot<T>(data: &mut [T], a: usize, b: usize, less: &mut Less<'_, T>) -> (usize, Hint) {
    const SHORTEST_NINTHER: usize = 50;
    const MAX_SWAPS: usize = 4 * 3;
    let l = b - a;
    let mut swaps = 0usize;
    let mut i = a + l / 4;
    let mut j = a + l / 4 * 2;
    let mut k = a + l / 4 * 3;
    if l >= 8 {
        if l >= SHORTEST_NINTHER {
            i = median(data, i - 1, i, i + 1, &mut swaps, less);
            j = median(data, j - 1, j, j + 1, &mut swaps, less);
            k = median(data, k - 1, k, k + 1, &mut swaps, less);
        }
        j = median(data, i, j, k, &mut swaps, less);
    }
    match swaps {
        0 => (j, Hint::Increasing),
        MAX_SWAPS => (j, Hint::Decreasing),
        _ => (j, Hint::Unknown),
    }
}

fn order2<T>(data: &[T], a: usize, b: usize, swaps: &mut usize, less: &mut Less<'_, T>) -> (usize, usize) {
    if less(&data[b], &data[a]) {
        *swaps += 1;
        return (b, a);
    }
    (a, b)
}

fn median<T>(data: &[T], a: usize, b: usize, c: usize, swaps: &mut usize, less: &mut Less<'_, T>) -> usize {
    let (a, b) = order2(data, a, b, swaps, less);
    let (b, c) = order2(data, b, c, swaps, less);
    let (_a, b) = order2(data, a, b, swaps, less);
    let _ = c;
    b
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Expected permutations from Go 1.26:
    /// `slices.SortFunc(es, func(a, b e) int { return cmp.Compare(a.k, b.k) })`
    /// over entries {key, original index}, for the three key patterns below.
    fn check(keys: Vec<String>, go: &[u32]) {
        let mut idx: Vec<u32> = (0..keys.len() as u32).collect();
        sort_like_go(&mut idx, |i| keys[i as usize].as_bytes());
        assert_eq!(idx, go);
    }

    #[test]
    fn matches_go_on_duplicates() {
        check((0..40u32).map(|i| ((b'a' + ((i * 7) % 5) as u8) as char).to_string()).collect(), &GO_40);
        check((0..200u64).map(|i| format!("k{:02}", (i * i * 31 + i) % 23)).collect(), &GO_200);
        check((0..60u32).map(|i| format!("k{:02}", 59 - i / 2)).collect(), &GO_60);
    }

    const GO_40: [u32; 40] = [20, 10, 35, 30, 0, 5, 25, 15, 28, 3, 8, 38, 33, 13, 18, 23, 26, 11, 1, 36, 6, 21, 16, 31, 34, 14, 24, 39, 19, 29, 9, 4, 7, 12, 22, 2, 32, 37, 17, 27];
    const GO_200: [u32; 200] = [115, 135, 112, 69, 66, 0, 89, 158, 46, 43, 92, 161, 184, 138, 23, 20, 181, 125, 148, 79, 194, 56, 10, 33, 102, 171, 86, 164, 17, 178, 141, 72, 118, 155, 63, 109, 187, 26, 3, 132, 40, 95, 49, 22, 44, 45, 113, 91, 90, 182, 160, 159, 68, 67, 114, 136, 21, 183, 137, 13, 59, 105, 145, 151, 191, 82, 197, 53, 7, 128, 36, 168, 174, 30, 122, 99, 76, 180, 162, 1, 185, 70, 65, 134, 19, 116, 139, 24, 88, 157, 111, 47, 42, 93, 156, 64, 94, 163, 179, 48, 25, 117, 87, 41, 133, 18, 71, 186, 2, 140, 110, 149, 9, 34, 170, 55, 103, 101, 57, 32, 195, 193, 172, 147, 124, 78, 126, 80, 11, 77, 146, 169, 196, 54, 104, 81, 127, 100, 35, 12, 192, 58, 150, 173, 31, 123, 8, 188, 108, 27, 142, 73, 16, 119, 154, 177, 96, 62, 85, 131, 50, 39, 4, 165, 83, 14, 198, 167, 106, 121, 52, 98, 152, 60, 6, 175, 190, 144, 29, 129, 75, 37, 74, 61, 143, 84, 130, 28, 97, 189, 176, 15, 153, 51, 5, 120, 38, 107, 166, 199];
    const GO_60: [u32; 60] = [59, 58, 57, 56, 55, 54, 53, 52, 51, 50, 48, 49, 46, 47, 45, 44, 42, 43, 41, 40, 39, 38, 37, 36, 35, 34, 32, 33, 31, 30, 29, 28, 26, 27, 25, 24, 23, 22, 21, 20, 18, 19, 17, 16, 15, 14, 13, 12, 10, 11, 9, 8, 7, 6, 5, 4, 3, 2, 0, 1];

    #[test]
    fn sorts() {
        let mut v: Vec<i32> = (0..500).map(|i| (i * 7919) % 257).collect();
        sort_func(&mut v, |a, b| a.cmp(b));
        assert!(v.windows(2).all(|w| w[0] <= w[1]));
        let mut v: Vec<i32> = (0..500).rev().collect();
        sort_func(&mut v, |a, b| a.cmp(b));
        assert!(v.windows(2).all(|w| w[0] <= w[1]));
    }
}
