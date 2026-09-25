//! Layout B (`series.rs`) against the spike's Go prototype
//! (../metrics-layout/seriesenc), run by `tools/cmd/seriesref` over the same
//! requests with the same envelope: the same objects, the same Parquet
//! schema (names, physical and logical types, levels) and the same rows,
//! series ids included, request after request, through the series cache.
//!
//!   seriesref -out $GO/corr  metrics-testgen-3000.pb metrics-nasty-700.pb metrics-extra.pb
//!   seriesref -fleet $FLEET -services 2 -rounds 130 -pods-per-batch 20
//!   seriesref -out $GO/fleet $FLEET/*.pb
//!   OTAPRS_DATA=... OTAPRS_SERIES_GO=$GO OTAPRS_SERIES_FLEET=$FLEET cargo test --release --test series -- --nocapture
//!
//! The Rust side runs with `SeriesOptions::prototype()` (no gauge+sum merge,
//! no exemplar attributes), which is the prototype's layout. The one
//! intended difference: maps with duplicate keys above 12 entries are in the
//! contrib exporter's order here and in stable order in the prototype
//! (`metrics-extra.pb` has them); those rows must hold the same entries.

use arrow::array::{Array, ArrayRef, ListArray, RecordBatch};
use otap_s3pq::Signal;
use otap_s3pq::batch::{Encoder, Format, Input};
use otap_s3pq::encode::ParquetOptions;
use otap_s3pq::flatten::Envelope;
use otap_s3pq::series::{MetricsLayout, SeriesOptions};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::BTreeMap;

fn go_name(s: Signal) -> &'static str {
    match s {
        Signal::MetricsGaugePoints => "gauge",
        Signal::MetricsSumPoints => "sum",
        Signal::MetricsHistogramPoints => "histogram",
        Signal::MetricsExpHistogramPoints => "exponential_histogram",
        Signal::MetricsSummaryPoints => "summary",
        Signal::MetricsSeries => "series",
        s => panic!("{s:?} has no prototype object"),
    }
}

/// (root name, per leaf: path, physical, logical, def, rep)
fn describe(b: &bytes::Bytes) -> (String, Vec<String>) {
    let r = ParquetRecordBatchReaderBuilder::try_new(b.clone()).unwrap();
    let sd = r.metadata().file_metadata().schema_descr();
    let cols = sd
        .columns()
        .iter()
        .map(|c| {
            format!(
                "{} {:?} {:?} d{} r{}",
                c.path().string(),
                c.physical_type(),
                c.logical_type_ref(),
                c.max_def_level(),
                c.max_rep_level()
            )
        })
        .collect();
    (sd.root_schema().name().to_string(), cols)
}

fn describe_rows(b: &bytes::Bytes) -> usize {
    ParquetRecordBatchReaderBuilder::try_new(b.clone()).unwrap().metadata().file_metadata().num_rows() as usize
}

/// Every leaf column's definition and repetition levels and values, read
/// with the low-level column readers (strings stay bytes: the hostile data
/// has invalid UTF-8, which Arrow's Utf8 reader refuses). Doubles compare by
/// bits, so NaN payloads count.
fn leaves(b: &bytes::Bytes) -> Vec<(String, Vec<i16>, Vec<i16>, Vec<Vec<u8>>)> {
    use parquet::column::reader::ColumnReader;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let r = SerializedFileReader::new(b.clone()).unwrap();
    let md = r.metadata();
    let ncol = md.file_metadata().schema_descr().num_columns();
    let mut out: Vec<(String, Vec<i16>, Vec<i16>, Vec<Vec<u8>>)> = (0..ncol)
        .map(|c| (md.file_metadata().schema_descr().column(c).path().string(), vec![], vec![], vec![]))
        .collect();
    for g in 0..md.num_row_groups() {
        let rg = r.get_row_group(g).unwrap();
        let n = md.row_group(g).num_rows() as usize;
        for (c, o) in out.iter_mut().enumerate() {
            let (mut d, mut rp) = (Vec::new(), Vec::new());
            macro_rules! read {
                ($rd:expr, $conv:expr) => {{
                    let mut vals = Vec::new();
                    let mut rd = $rd;
                    loop {
                        let (recs, _, _) = rd.read_records(n, Some(&mut d), Some(&mut rp), &mut vals).unwrap();
                        if recs == 0 {
                            break;
                        }
                    }
                    o.3.extend(vals.iter().map($conv));
                }};
            }
            match rg.get_column_reader(c).unwrap() {
                ColumnReader::BoolColumnReader(x) => read!(x, |v: &bool| vec![*v as u8]),
                ColumnReader::Int32ColumnReader(x) => read!(x, |v: &i32| v.to_le_bytes().to_vec()),
                ColumnReader::Int64ColumnReader(x) => read!(x, |v: &i64| v.to_le_bytes().to_vec()),
                ColumnReader::DoubleColumnReader(x) => read!(x, |v: &f64| v.to_bits().to_le_bytes().to_vec()),
                ColumnReader::ByteArrayColumnReader(x) => read!(x, |v: &parquet::data_type::ByteArray| v.data().to_vec()),
                _ => panic!("unexpected physical type"),
            }
            o.1.extend(d);
            o.2.extend(rp);
        }
    }
    out
}

/// Rows of an object whose strings are valid UTF-8, as Arrow.
fn rows(b: &bytes::Bytes) -> RecordBatch {
    let r = ParquetRecordBatchReaderBuilder::try_new(b.clone()).unwrap().with_batch_size(1 << 22).build().unwrap();
    let bs: Vec<RecordBatch> = r.map(|x| x.unwrap()).collect();
    arrow::compute::concat_batches(&bs[0].schema(), &bs).unwrap()
}

/// The entries of row i of a keys list and a values list, sorted.
fn entries(k: &ArrayRef, v: &ArrayRef, i: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let (k, v) = (k.as_any().downcast_ref::<ListArray>().unwrap(), v.as_any().downcast_ref::<ListArray>().unwrap());
    let (k, v) = (k.value(i), v.value(i));
    let k = k.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    let v = v.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    let mut e: Vec<_> = (0..k.len()).map(|j| (k.value(j).as_bytes().to_vec(), v.value(j).as_bytes().to_vec())).collect();
    e.sort();
    e
}

struct Tally {
    objects: usize,
    rows: usize,
    series: usize,
    order_only: usize,
}

/// Encodes `files` in order with one Rust encoder (one epoch) and compares
/// every object with the prototype's in `go`.
fn compare(files: &[String], go: &str) -> Tally {
    let mut enc = Encoder::new(ParquetOptions::default(), Format::Parquet)
        .with_metrics_layout(MetricsLayout::SeriesTable, SeriesOptions::prototype());
    let mut t = Tally { objects: 0, rows: 0, series: 0, order_only: 0 };
    for (i, f) in files.iter().enumerate() {
        let req = std::fs::read(f).unwrap();
        let env = Envelope { producer: "p".into(), epoch: "e".into(), batch: i as u64, received_ns: 1_700_000_000_000_000_000 };
        let flats = enc.flatten_all(&Input::OtlpMetrics(&req)).unwrap();
        let mut mine: BTreeMap<&str, bytes::Bytes> = BTreeMap::new();
        for fl in &flats {
            let _ = mine.insert(go_name(fl.signal), enc.encode(fl, &env).unwrap().body);
        }
        let theirs: BTreeMap<&str, bytes::Bytes> = ["gauge", "sum", "histogram", "exponential_histogram", "summary", "series"]
            .into_iter()
            .filter_map(|n| std::fs::read(format!("{go}/{i:04}.{n}.parquet")).ok().map(|b| (n, bytes::Bytes::from(b))))
            .collect();
        assert_eq!(mine.keys().collect::<Vec<_>>(), theirs.keys().collect::<Vec<_>>(), "{f}: object set");
        for fl in &flats {
            let n = go_name(fl.signal);
            let (a, b) = (&mine[n], &theirs[n]);
            assert_eq!(describe(a), describe(b), "{f} {n}: Parquet schema");
            let (la, lb) = (leaves(a), leaves(b));
            let nrows = describe_rows(a);
            assert_eq!(nrows, describe_rows(b), "{f} {n}: rows");
            let differing: Vec<&str> = la.iter().zip(&lb).filter(|(x, y)| x != y).map(|(x, _)| x.0.as_str()).collect();
            if !differing.is_empty() {
                eprintln!("{f} {n}: differing leaves {differing:?}");
                // Only the series maps may differ, and only in the order of
                // duplicate keys (contrib's order here, stable there).
                assert!(
                    n == "series" && differing.iter().all(|p| p.contains("Attributes")),
                    "{f} {n}: columns differ: {differing:?}"
                );
                let (ra, rb) = (rows(a), rows(b));
                for (c, field) in ra.schema().fields().iter().enumerate() {
                    let name = field.name();
                    if name.ends_with("Values") {
                        continue; // compared with its keys
                    }
                    if !name.ends_with("Keys") {
                        assert_eq!(ra.column(c), rb.column(c), "{f} {n}: {name}");
                        continue;
                    }
                    let (x, y, xv, yv) = (ra.column(c), rb.column(c), ra.column(c + 1), rb.column(c + 1));
                    let l = |a: &ArrayRef, r: usize| a.as_any().downcast_ref::<ListArray>().unwrap().value(r);
                    for r in 0..ra.num_rows() {
                        if l(x, r) != l(y, r) || l(xv, r) != l(yv, r) {
                            assert_eq!(entries(x, xv, r), entries(y, yv, r), "{f} {n} {name} row {r}: entries differ");
                            t.order_only += 1;
                        }
                    }
                }
            }
            let _ = &la;
            t.objects += 1;
            t.rows += nrows;
            if n == "series" {
                t.series += nrows;
            }
        }
        // As if every series object committed, in epoch "e".
        for fl in &flats {
            if !fl.announce.is_empty() {
                enc.series_announced(&fl.announce, "e");
            }
        }
    }
    t
}

#[test]
fn same_as_go_prototype_corr() {
    let (Ok(data), Ok(go)) = (std::env::var("OTAPRS_DATA"), std::env::var("OTAPRS_SERIES_GO")) else { return };
    let files: Vec<String> =
        ["testgen-3000", "nasty-700", "extra"].iter().map(|d| format!("{data}/metrics-{d}.pb")).collect();
    let t = compare(&files, &format!("{go}/corr"));
    println!(
        "corr: {} objects, {} rows ({} series rows) compared; all identical except {} series-row maps that differ only in the order of duplicate keys",
        t.objects, t.rows, t.series, t.order_only
    );
    // metrics-extra.pb: 15 series rows, each with three maps that have duplicate keys.
    assert!(t.order_only > 0 && t.order_only <= 45, "only metrics-extra.pb has duplicate keys");
}

#[test]
fn same_as_go_prototype_fleet() {
    let (Ok(fleet), Ok(go)) = (std::env::var("OTAPRS_SERIES_FLEET"), std::env::var("OTAPRS_SERIES_GO")) else { return };
    let mut files: Vec<String> = std::fs::read_dir(&fleet)
        .unwrap()
        .map(|e| e.unwrap().path().to_string_lossy().into_owned())
        .filter(|p| p.ends_with(".pb"))
        .collect();
    files.sort();
    let t = compare(&files, &format!("{go}/fleet"));
    println!("fleet: {} requests, {} objects, {} rows ({} series rows) identical", files.len(), t.objects, t.rows, t.series);
    assert_eq!(t.order_only, 0);
}

/// The cache: a request's series come back until they are marked announced;
/// then only a new window or a new epoch announces them again. Same input,
/// same bytes.
#[test]
fn cache_and_determinism() {
    let Ok(data) = std::env::var("OTAPRS_DATA") else { return };
    let req = std::fs::read(format!("{data}/metrics-testgen-3000.pb")).unwrap();
    let env = Envelope { producer: "p".into(), epoch: "e".into(), batch: 1, received_ns: 1 };
    let new = || Encoder::new(ParquetOptions::default(), Format::Parquet).with_metrics_layout(MetricsLayout::SeriesTable, SeriesOptions::default());
    let (mut a, mut b) = (new(), new());
    let fa = a.flatten_all(&Input::OtlpMetrics(&req)).unwrap();
    let fb = b.flatten_all(&Input::OtlpMetrics(&req)).unwrap();
    // gauge+sum merged, histogram, exp. histogram, summary, series
    assert_eq!(fa.iter().map(|f| f.signal).collect::<Vec<_>>(), vec![
        Signal::MetricsNumberPoints,
        Signal::MetricsHistogramPoints,
        Signal::MetricsExpHistogramPoints,
        Signal::MetricsSummaryPoints,
        Signal::MetricsSeries
    ]);
    for (x, y) in fa.iter().zip(&fb) {
        assert_eq!(x.content, y.content);
        assert_eq!(a.encode(x, &env).unwrap().body, b.encode(y, &env).unwrap().body);
    }
    let series = fa.last().unwrap();
    let n = series.announce.len();
    assert_eq!(n, series.stats.rows);
    // Not committed: the retry announces the same series, so the same object.
    let again = a.flatten_all(&Input::OtlpMetrics(&req)).unwrap();
    assert_eq!(again.last().unwrap().content, series.content);
    // Committed: nothing new.
    a.series_announced(&again.last().unwrap().announce, "e1");
    let after = a.flatten_all(&Input::OtlpMetrics(&req)).unwrap();
    assert!(after.iter().all(|f| f.signal != Signal::MetricsSeries));
    assert_eq!(after.len(), 4);
    // A commit in a new epoch empties the cache: everything again.
    a.series_announced(&[], "e2");
    let fresh = a.flatten_all(&Input::OtlpMetrics(&req)).unwrap();
    assert_eq!(fresh.last().unwrap().announce.len(), n);
}

#[test]
#[ignore]
fn dump_rust_corr() {
    let data = std::env::var("OTAPRS_DATA").unwrap();
    let out = std::env::var("OUT").unwrap();
    let mut enc = Encoder::new(ParquetOptions::default(), Format::Parquet)
        .with_metrics_layout(MetricsLayout::SeriesTable, SeriesOptions::prototype());
    for (i, d) in ["testgen-3000", "nasty-700", "extra"].iter().enumerate() {
        let req = std::fs::read(format!("{data}/metrics-{d}.pb")).unwrap();
        let env = Envelope { producer: "p".into(), epoch: "e".into(), batch: i as u64, received_ns: 1_700_000_000_000_000_000 };
        for fl in enc.flatten_all(&Input::OtlpMetrics(&req)).unwrap() {
            std::fs::write(format!("{out}/{i:04}.{}.parquet", go_name(fl.signal)), enc.encode(&fl, &env).unwrap().body).unwrap();
            if !fl.announce.is_empty() {
                enc.series_announced(&fl.announce, "e");
            }
        }
    }
}
