//! Parquet (and Arrow IPC) encoding of one batch, tuned for central ingest.
//!
//! One row group per batch; zstd; dictionary on every column except the
//! near-unique ones (some of those DELTA_BINARY_PACKED or BYTE_STREAM_SPLIT,
//! per `Schemas`); chunk + page statistics and the page index (layout B:
//! none, `series.statistics`); bloom filters only where they pay (default:
//! TraceId, sized for the batch). No `ARROW:schema` in the footer. The batch
//! description goes into the footer's key-value metadata (sorted keys), so
//! the object is self-describing without its S3 metadata. Encoding is deterministic: the same rows and metadata give
//! the same bytes.

use crate::schema::{HIGH_CARDINALITY, Schemas};
use arrow::array::{Array, BinaryArray, RecordBatch, TimestampNanosecondArray};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::schema::types::ColumnPath;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::ops::Range;

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ParquetOptions {
    /// zstd level (0 = none, i.e. uncompressed).
    pub zstd_level: i32,
    pub dictionary: bool,
    /// "none", "chunk" or "page" (page = chunk + page stats + column index).
    /// A signal's schemas may override it (layout B: `series.statistics`).
    pub statistics: String,
    /// Leaf column paths (dotted, e.g. "TraceId") that get a bloom filter.
    pub bloom_columns: Vec<String>,
    pub bloom_fpp: f64,
    pub data_page_size: usize,
    /// "1.0" (V1 data pages) or "2.0".
    pub writer_version: String,
    /// Row order and row groups of trace and log objects. Off by default
    /// (arrival order, one row group); bench/sorting/README.md.
    pub sort: SortOptions,
}

/// Row order of trace and log objects.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SortBy {
    /// Arrival order (the request's resource, scope, record order).
    #[default]
    None,
    /// By (ServiceName, Timestamp), ties in arrival order.
    ServiceTime,
}

/// How the sorted rows are cut into row groups.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RowGroupSplit {
    /// Row group `xxh3_64(ServiceName) mod row_groups`: a service lands in
    /// the same group index in every object, so a reader that knows the
    /// service can pick its group from the footer's bucket list without
    /// statistics. Rows are ordered (bucket, ServiceName, Timestamp); empty
    /// buckets have no row group.
    #[default]
    Hash,
    /// Contiguous runs of the (ServiceName, Timestamp) order, cut at service
    /// boundaries into about equal row counts: each group's ServiceName
    /// min/max covers only its own services, and the object stays sorted as
    /// a whole.
    Range,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct SortOptions {
    pub by: SortBy,
    /// Row groups per object (at most; 1 = one row group). Only with `by`.
    pub row_groups: usize,
    pub split: RowGroupSplit,
}

impl Default for SortOptions {
    fn default() -> Self {
        Self { by: SortBy::None, row_groups: 1, split: RowGroupSplit::Hash }
    }
}

impl SortOptions {
    pub fn enabled(&self) -> bool {
        self.by != SortBy::None
    }
}

/// Footer key of a sorted object: "service_time;hash:16;0,3,5,…" (the
/// bucket of each row group, in order) or "service_time;range:4".
pub const META_SORT: &str = "oscope-sort";

/// A sorted object's row order and row groups.
pub struct SortPlan {
    /// Source row of each output row.
    pub perm: Vec<u32>,
    /// Output row ranges, one per row group.
    pub groups: Vec<Range<usize>>,
    /// The hash bucket of each group (`RowGroupSplit::Hash`).
    pub buckets: Vec<u32>,
    /// Most distinct services in one group.
    pub max_services: usize,
}

impl SortPlan {
    pub fn footer_value(&self, o: &SortOptions) -> String {
        match o.split {
            RowGroupSplit::Hash => format!(
                "service_time;hash:{};{}",
                o.row_groups.max(1),
                self.buckets.iter().map(|b| b.to_string()).collect::<Vec<_>>().join(",")
            ),
            RowGroupSplit::Range => format!("service_time;range:{}", o.row_groups.max(1)),
        }
    }
}

/// The hash bucket of a service among `n`.
pub fn service_bucket(service: &[u8], n: usize) -> u32 {
    (xxhash_rust::xxh3::xxh3_64(service) % n.max(1) as u64) as u32
}

/// Sorts rows by (ServiceName, Timestamp), ties by arrival (a total order,
/// so the same rows always give the same object), and cuts row groups.
/// A pure function of the two columns.
pub fn sort_plan(service: &BinaryArray, ts: &TimestampNanosecondArray, o: &SortOptions) -> SortPlan {
    let n = service.len();
    let k = o.row_groups.max(1);
    let hash = o.split == RowGroupSplit::Hash && k > 1;
    let bucket: Vec<u32> = if hash { (0..n).map(|i| service_bucket(service.value(i), k)).collect() } else { Vec::new() };
    let mut perm: Vec<u32> = (0..n as u32).collect();
    perm.sort_unstable_by(|&a, &b| {
        let (a, b) = (a as usize, b as usize);
        let kb = if hash { bucket[a].cmp(&bucket[b]) } else { std::cmp::Ordering::Equal };
        kb.then_with(|| service.value(a).cmp(service.value(b)))
            .then_with(|| ts.value(a).cmp(&ts.value(b)))
            .then_with(|| a.cmp(&b))
    });
    // Runs of one service, in output order.
    let mut runs: Vec<Range<usize>> = Vec::new();
    for i in 0..n {
        if i == 0 || service.value(perm[i] as usize) != service.value(perm[i - 1] as usize) {
            runs.push(i..i + 1);
        } else {
            runs.last_mut().expect("a run").end = i + 1;
        }
    }
    // Group id per run: its hash bucket, or its midpoint's share of the rows.
    let gid = |r: &Range<usize>| -> u32 {
        if k == 1 || n == 0 {
            0
        } else if hash {
            bucket[perm[r.start] as usize]
        } else {
            (((r.start + r.end) * k) / (2 * n)).min(k - 1) as u32
        }
    };
    let mut groups: Vec<Range<usize>> = Vec::new();
    let mut buckets = Vec::new();
    let mut services = Vec::new();
    for r in &runs {
        let g = gid(r);
        if buckets.last() == Some(&g) {
            groups.last_mut().expect("a group").end = r.end;
            *services.last_mut().expect("a group") += 1;
        } else {
            groups.push(r.clone());
            buckets.push(g);
            services.push(1usize);
        }
    }
    if groups.is_empty() {
        groups.push(0..0);
        buckets.push(0);
    }
    SortPlan { perm, groups, buckets, max_services: services.into_iter().max().unwrap_or(0) }
}

impl Default for ParquetOptions {
    fn default() -> Self {
        Self {
            zstd_level: 3,
            dictionary: true,
            statistics: "page".into(),
            bloom_columns: vec!["TraceId".into()],
            bloom_fpp: 0.005,
            data_page_size: 1 << 20,
            writer_version: "1.0".into(),
            sort: SortOptions::default(),
        }
    }
}

impl ParquetOptions {
    /// Every string/map/list leaf, as parquetgo's (and ClickHouse's) default.
    pub fn all_blooms(sc: &Schemas) -> Vec<String> {
        sc.parquet.columns().iter().map(|c| c.path().string()).collect()
    }

    /// `rows`: the largest row group's rows (bloom filters are per row
    /// group); `services`: the most distinct services in one row group, the
    /// ServiceName filter's NDV when the rows are sorted (0: `rows`).
    fn properties(&self, sc: &Schemas, rows: usize, services: usize, kv: Vec<KeyValue>) -> WriterProperties {
        let stats = match sc.statistics.as_deref().unwrap_or(&self.statistics) {
            "none" => EnabledStatistics::None,
            "chunk" => EnabledStatistics::Chunk,
            _ => EnabledStatistics::Page,
        };
        let mut b = WriterProperties::builder()
            .set_created_by(format!("otap-s3pq {} (parquet-rs 58)", env!("CARGO_PKG_VERSION")))
            .set_writer_version(if self.writer_version == "2.0" {
                WriterVersion::PARQUET_2_0
            } else {
                WriterVersion::PARQUET_1_0
            })
            .set_max_row_group_row_count(Some(rows.max(1)))
            .set_data_page_size_limit(self.data_page_size)
            .set_dictionary_page_size_limit(self.data_page_size)
            .set_dictionary_enabled(self.dictionary)
            .set_statistics_enabled(stats)
            // A signal that turns statistics off (layout B) drops the offset
            // index too: its objects are only ever read whole.
            .set_offset_index_disabled(sc.statistics.as_deref() == Some("none"))
            .set_compression(if self.zstd_level > 0 {
                Compression::ZSTD(ZstdLevel::try_new(self.zstd_level).unwrap_or_default())
            } else {
                Compression::UNCOMPRESSED
            })
            .set_key_value_metadata(Some(kv));
        for p in HIGH_CARDINALITY {
            b = b.set_column_dictionary_enabled(
                ColumnPath::new(p.iter().map(|s| s.to_string()).collect()),
                false,
            );
        }
        for p in &sc.plain {
            b = b.set_column_dictionary_enabled(ColumnPath::new(p.clone()), false);
        }
        for p in &sc.delta {
            b = b
                .set_column_dictionary_enabled(ColumnPath::new(p.clone()), false)
                .set_column_encoding(ColumnPath::new(p.clone()), parquet::basic::Encoding::DELTA_BINARY_PACKED);
        }
        for p in &sc.byte_stream_split {
            b = b
                .set_column_dictionary_enabled(ColumnPath::new(p.clone()), false)
                .set_column_encoding(ColumnPath::new(p.clone()), parquet::basic::Encoding::BYTE_STREAM_SPLIT);
        }
        for c in &self.bloom_columns {
            let path = ColumnPath::from(c.as_str());
            b = b
                .set_column_bloom_filter_enabled(path.clone(), true)
                .set_column_bloom_filter_fpp(path.clone(), self.bloom_fpp)
                // Sized for this batch: the default NDV (1M) would make a
                // ~1.8 MB filter per column.
                .set_column_bloom_filter_ndv(
                    path,
                    (if c == "ServiceName" && services > 0 { services } else { rows.max(1) }) as u64,
                );
        }
        b.build()
    }
}

/// Writes one batch as a Parquet file with the given footer metadata.
pub fn parquet(
    sc: &Schemas,
    opts: &ParquetOptions,
    batch: &RecordBatch,
    meta: &BTreeMap<String, String>,
    out: &mut Vec<u8>,
) -> parquet::errors::Result<()> {
    let kv = meta
        .iter()
        .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
        .collect();
    let options = ArrowWriterOptions::new()
        .with_properties(opts.properties(sc, batch.num_rows(), 0, kv))
        .with_skip_arrow_metadata(true)
        .with_parquet_schema(sc.parquet.clone());
    let mut w = ArrowWriter::try_new_with_options(out, sc.arrow.clone(), options)?;
    w.write(batch)?;
    w.close()?;
    Ok(())
}

/// Writes one batch as a Parquet file of one row group per range of `groups`
/// (in order, covering the batch), e.g. a `SortPlan`'s.
pub fn parquet_groups(
    sc: &Schemas,
    opts: &ParquetOptions,
    batch: &RecordBatch,
    groups: &[Range<usize>],
    max_services: usize,
    meta: &BTreeMap<String, String>,
    out: &mut Vec<u8>,
) -> parquet::errors::Result<()> {
    let kv = meta
        .iter()
        .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
        .collect();
    let largest = groups.iter().map(|g| g.len()).max().unwrap_or(0);
    let options = ArrowWriterOptions::new()
        .with_properties(opts.properties(sc, largest, max_services, kv))
        .with_skip_arrow_metadata(true)
        .with_parquet_schema(sc.parquet.clone());
    let mut w = ArrowWriter::try_new_with_options(out, sc.arrow.clone(), options)?;
    for g in groups {
        w.write(&batch.slice(g.start, g.len()))?;
        w.flush()?;
    }
    w.close()?;
    Ok(())
}

/// Arrow IPC file (zstd buffers), for comparison only.
pub fn arrow_ipc(batch: &RecordBatch, out: &mut Vec<u8>) -> arrow::error::Result<()> {
    use arrow::ipc::writer::{FileWriter, IpcWriteOptions};
    let o = IpcWriteOptions::default()
        .try_with_compression(Some(arrow::ipc::CompressionType::ZSTD))?;
    let mut w = FileWriter::try_new_with_options(out, &batch.schema(), o)?;
    w.write(batch)?;
    w.finish()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols(rows: &[(&str, i64)]) -> (BinaryArray, TimestampNanosecondArray) {
        (
            BinaryArray::from_iter_values(rows.iter().map(|r| r.0.as_bytes())),
            TimestampNanosecondArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>()),
        )
    }

    fn opts(k: usize, split: RowGroupSplit) -> SortOptions {
        SortOptions { by: SortBy::ServiceTime, row_groups: k, split }
    }

    const ROWS: &[(&str, i64)] = &[
        ("cart", 5), ("auth", 9), ("cart", 1), ("zeta", 3), ("auth", 9), ("mail", 2), ("cart", 1), ("beta", 7),
    ];

    #[test]
    fn one_group_is_service_then_time_then_arrival() {
        let (s, t) = cols(ROWS);
        let p = sort_plan(&s, &t, &opts(1, RowGroupSplit::Hash));
        // auth 9 (1), auth 9 (4), beta, cart 1 (2), cart 1 (6), cart 5, mail, zeta
        assert_eq!(p.perm, vec![1, 4, 7, 2, 6, 0, 5, 3]);
        assert_eq!(p.groups, vec![0..8]);
        assert_eq!(p.max_services, 5);
    }

    #[test]
    fn hash_groups_hold_one_bucket_each_in_bucket_order() {
        let (s, t) = cols(ROWS);
        let p = sort_plan(&s, &t, &opts(4, RowGroupSplit::Hash));
        assert_eq!(p.groups.len(), p.buckets.len());
        assert!(p.buckets.windows(2).all(|w| w[0] < w[1]), "{:?}", p.buckets);
        assert_eq!(p.groups.first().unwrap().start, 0);
        assert_eq!(p.groups.last().unwrap().end, ROWS.len());
        for (g, b) in p.groups.iter().zip(&p.buckets) {
            let mut prev: Option<(&[u8], i64)> = None;
            for i in g.clone() {
                let r = p.perm[i] as usize;
                assert_eq!(service_bucket(s.value(r), 4), *b);
                let cur = (s.value(r), t.value(r));
                assert!(prev.is_none_or(|p| p <= cur), "sorted within the group");
                prev = Some(cur);
            }
        }
        let mut all = p.perm.clone();
        all.sort();
        assert_eq!(all, (0..ROWS.len() as u32).collect::<Vec<_>>(), "a permutation");
    }

    #[test]
    fn range_groups_are_contiguous_services_and_the_whole_stays_sorted() {
        let (s, t) = cols(ROWS);
        let p = sort_plan(&s, &t, &opts(3, RowGroupSplit::Range));
        let one = sort_plan(&s, &t, &opts(1, RowGroupSplit::Range));
        assert_eq!(p.perm, one.perm, "same order as one group");
        assert!(p.groups.len() <= 3 && p.groups.len() > 1, "{:?}", p.groups);
        for w in p.groups.windows(2) {
            assert_eq!(w[0].end, w[1].start);
            // no service spans two groups
            assert_ne!(s.value(p.perm[w[0].end - 1] as usize), s.value(p.perm[w[1].start] as usize));
        }
    }

    #[test]
    fn empty_batch_has_one_empty_group() {
        let (s, t) = cols(&[]);
        let p = sort_plan(&s, &t, &opts(16, RowGroupSplit::Hash));
        assert_eq!(p.groups, vec![0..0]);
        assert!(p.perm.is_empty());
    }
}
