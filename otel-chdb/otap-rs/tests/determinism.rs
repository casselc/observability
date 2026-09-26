//! Encoding is deterministic: the same request, flattened and encoded twice
//! for the same slot and received time, gives byte-identical objects and the
//! same content key; the OTLP and OTAP input paths give the same rows.
//! Needs the otlpgen files in $OTAPRS_DATA; skipped otherwise.

use otap_s3pq::Signal;
use otap_s3pq::batch::{Encoder, Format, Input};
use otap_s3pq::encode::ParquetOptions;
use otap_s3pq::flatten::Envelope;
use otel_arrow_dfe_pdata::{OtapArrowRecords, OtlpProtoBytes, TryIntoWithOptions};

#[test]
fn same_request_same_bytes() {
    let Ok(dir) = std::env::var("OTAPRS_DATA") else { return };
    for (sig, file) in [(Signal::Traces, "traces-nasty-700.pb"), (Signal::Logs, "logs-testgen-3000.pb"), (Signal::Traces, "traces-testgen-3000.pb")] {
        let b = std::fs::read(format!("{dir}/{file}")).unwrap();
        let env = Envelope { producer: "p".into(), epoch: "E".into(), batch: 7, received_ns: 1_790_000_000_000_000_000 };
        let mut objs = Vec::new();
        for _ in 0..2 {
            let mut enc = Encoder::new(ParquetOptions::default(), Format::Parquet);
            let f = enc.flatten(&Input::Otlp(sig, &b)).unwrap();
            objs.push((f.content.clone(), enc.encode(&f, &env).unwrap().body));
        }
        assert_eq!(objs[0].0, objs[1].0, "{file}: content key");
        assert!(objs[0].1 == objs[1].1, "{file}: object bytes differ");
        // Reusing one encoder (its buffers) changes nothing either.
        let mut enc = Encoder::new(ParquetOptions::default(), Format::Parquet);
        let _ = enc.flatten(&Input::Otlp(sig, &b)).unwrap();
        let f = enc.flatten(&Input::Otlp(sig, &b)).unwrap();
        assert!(enc.encode(&f, &env).unwrap().body == objs[0].1, "{file}: reused encoder");
        println!("{file}: {} bytes, content {}, identical on re-encode", objs[0].1.len(), objs[0].0);
    }
    // OTLP bytes vs OTAP records (testgen: no invalid UTF-8 in maps, which
    // upstream's OTLP->OTAP conversion rejects): the same rows.
    for (sig, file) in [(Signal::Traces, "traces-testgen-3000.pb"), (Signal::Logs, "logs-testgen-3000.pb")] {
        let b = std::fs::read(format!("{dir}/{file}")).unwrap();
        let mut enc = Encoder::new(ParquetOptions::default(), Format::Parquet);
        let direct = enc.flatten(&Input::Otlp(sig, &b)).unwrap();
        let p = match sig {
            Signal::Traces => OtlpProtoBytes::ExportTracesRequest(b.clone().into()),
            Signal::Logs => OtlpProtoBytes::ExportLogsRequest(b.clone().into()),
            _ => unreachable!(),
        };
        let recs: OtapArrowRecords = p.try_into_with_default().unwrap();
        let via = enc.flatten(&Input::Otap(sig, &recs)).unwrap();
        for (i, (a, c)) in direct.cols.iter().zip(via.cols.iter()).enumerate() {
            assert_eq!(a.to_data(), c.to_data(), "{file}: column {i} differs between OTLP and OTAP input");
        }
        println!("{file}: OTLP and OTAP input give identical columns");
    }
}

/// Sorted objects (`parquet.sort`, bench/sorting) are deterministic too, and
/// hold the same rows: each row keeps its row_ordinal, and reading the rows
/// back in row_ordinal order gives the unsorted object's rows.
#[test]
fn sorted_objects_same_bytes_same_rows() {
    use arrow::array::{Array, UInt32Array};
    use arrow::compute::{sort_to_indices, take_record_batch};
    use otap_s3pq::encode::{META_SORT, RowGroupSplit, SortBy, SortOptions};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let Ok(dir) = std::env::var("OTAPRS_DATA") else { return };
    let read = |b: &bytes::Bytes| {
        let r = ParquetRecordBatchReaderBuilder::try_new(b.clone()).unwrap();
        let md = r.metadata().clone();
        let batches: Vec<_> = r.with_batch_size(1 << 20).build().unwrap().map(|b| b.unwrap()).collect();
        (md, arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap())
    };
    // testgen (valid UTF-8 throughout, so the rows read back as strings).
    for (sig, file) in [(Signal::Traces, "traces-testgen-3000.pb"), (Signal::Logs, "logs-testgen-3000.pb")] {
        let b = std::fs::read(format!("{dir}/{file}")).unwrap();
        let env = Envelope { producer: "p".into(), epoch: "E".into(), batch: 7, received_ns: 1_790_000_000_000_000_000 };
        let mut plain = Encoder::new(ParquetOptions::default(), Format::Parquet);
        let f = plain.flatten(&Input::Otlp(sig, &b)).unwrap();
        let (_, base) = read(&plain.encode(&f, &env).unwrap().body);
        for (k, split) in [(1, RowGroupSplit::Hash), (4, RowGroupSplit::Hash), (16, RowGroupSplit::Hash), (4, RowGroupSplit::Range)] {
            let mut o = ParquetOptions::default();
            o.sort = SortOptions { by: SortBy::ServiceTime, row_groups: k, split };
            let objs: Vec<_> = (0..2)
                .map(|_| {
                    let mut enc = Encoder::new(o.clone(), Format::Parquet);
                    let f = enc.flatten(&Input::Otlp(sig, &b)).unwrap();
                    enc.encode(&f, &env).unwrap().body
                })
                .collect();
            assert!(objs[0] == objs[1], "{file} k={k}: sorted object bytes differ");
            let (md, rows) = read(&objs[0]);
            let kv = md.file_metadata().key_value_metadata().unwrap();
            let sort = kv.iter().find(|e| e.key == META_SORT).and_then(|e| e.value.clone()).unwrap();
            assert!(sort.starts_with("service_time;"), "{sort}");
            assert!(md.num_row_groups() >= 1 && md.num_row_groups() <= k, "{file} k={k}: {} groups", md.num_row_groups());
            // Sorted by (ServiceName, Timestamp) within each row group.
            let mut at = 0;
            for g in md.row_groups() {
                let n = g.num_rows() as usize;
                let part = rows.slice(at, n);
                let s = part.column_by_name("ServiceName").unwrap();
                let s = s.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
                assert!((1..n).all(|i| s.value(i - 1) <= s.value(i)), "{file} k={k}: group not sorted");
                at += n;
            }
            assert_eq!(at, base.num_rows());
            // Back in row_ordinal order: the unsorted object's rows exactly.
            let ord = rows.column_by_name("row_ordinal").unwrap();
            let back = take_record_batch(&rows, &sort_to_indices(ord, None, None).unwrap()).unwrap();
            let want: Vec<u32> = (0..base.num_rows() as u32).collect();
            let got = back.column_by_name("row_ordinal").unwrap();
            assert_eq!(got.as_any().downcast_ref::<UInt32Array>().unwrap().values().to_vec(), want);
            assert_eq!(back, base, "{file} k={k}: rows differ from the unsorted object");
            println!("{file} sort k={k} {split:?}: {} bytes, {} row groups, identical on re-encode", objs[0].len(), md.num_row_groups());
        }
    }
}
