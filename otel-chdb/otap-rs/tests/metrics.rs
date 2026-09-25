//! Metrics: one object per metric type; deterministic encoding; the OTLP and
//! OTAP input paths give the same rows; the `DateTime` rendering. Needs the
//! `otlpgen -metrics` files in $OTAPRS_DATA for the data-driven parts.

use otap_s3pq::Signal;
use otap_s3pq::batch::{Encoder, Flat, Format, Input};
use otap_s3pq::encode::ParquetOptions;
use otap_s3pq::flatten::Envelope;
use otap_s3pq::metrics::dt_ms;
use otel_arrow_dfe_pdata::{OtapArrowRecords, OtlpProtoBytes, TryIntoWithOptions};

#[test]
fn datetime_as_clickhouse_go_stores_it() {
    // uint32(floor(int64(ns)/1e9)) * 1000, as ../parquetgo's dtMillis. (Two of
    // METRICS_SCHEMA.md's worked examples have arithmetic slips; its formula
    // and parquetgo's code give these.) Checked against ClickHouse's own
    // DateTime64 -> DateTime conversion too, which wraps the same way.
    assert_eq!(dt_ms(0), 0);
    assert_eq!(dt_ms(1_700_000_000_999_999_999), 1_700_000_000_000);
    assert_eq!(dt_ms(i64::MAX as u64), (9_223_372_036u64 % (1 << 32)) as i64 * 1000); // 633_437_444_000
    assert_eq!(dt_ms(1 << 63), 3_661_529_851_000); // -9_223_372_037 s mod 2^32
    assert_eq!(dt_ms(u64::MAX), 4_294_967_295_000); // -1 ns: floor to -1 s
}

fn flats(enc: &mut Encoder, input: &Input<'_>) -> Vec<Flat> {
    enc.flatten_all(input).unwrap()
}

#[test]
fn same_request_same_objects() {
    let Ok(dir) = std::env::var("OTAPRS_DATA") else { return };
    let env = Envelope { producer: "p".into(), epoch: "E".into(), batch: 7, received_ns: 1_790_000_000_000_000_000 };
    for file in ["metrics-testgen-3000.pb", "metrics-nasty-700.pb", "metrics-extra.pb"] {
        let b = std::fs::read(format!("{dir}/{file}")).unwrap();
        let mut runs = Vec::new();
        for _ in 0..2 {
            let mut enc = Encoder::new(ParquetOptions::default(), Format::Parquet);
            let fs = flats(&mut enc, &Input::OtlpMetrics(&b));
            runs.push(fs.iter().map(|f| (f.signal, f.content.clone(), enc.encode(f, &env).unwrap().body)).collect::<Vec<_>>());
        }
        assert_eq!(runs[0].len(), 5, "{file}: one object per type");
        assert!(runs[0] == runs[1], "{file}: objects differ on re-encode");
        // One encoder, reused: its buffers are reset between requests.
        let mut enc = Encoder::new(ParquetOptions::default(), Format::Parquet);
        let _ = flats(&mut enc, &Input::OtlpMetrics(&b));
        let again: Vec<_> = flats(&mut enc, &Input::OtlpMetrics(&b))
            .iter()
            .map(|f| (f.signal, f.content.clone(), enc.encode(f, &env).unwrap().body))
            .collect();
        assert!(again == runs[0], "{file}: reused encoder");
        // Every type has its own content key (the signal is in the hash).
        let keys: std::collections::BTreeSet<_> = runs[0].iter().map(|r| r.1.clone()).collect();
        assert_eq!(keys.len(), 5);
        for (s, c, o) in &runs[0] {
            println!("{file}: {} {} bytes, content {c}", s.name(), o.len());
        }
    }
}

#[test]
fn otlp_and_otap_inputs_agree() {
    let Ok(dir) = std::env::var("OTAPRS_DATA") else { return };
    // Not nasty-700: upstream's OTLP->OTAP conversion rejects its invalid UTF-8.
    for file in ["metrics-testgen-3000.pb", "metrics-extra.pb", "metrics-mixed-10000.pb"] {
        let b = std::fs::read(format!("{dir}/{file}")).unwrap();
        let mut enc = Encoder::new(ParquetOptions::default(), Format::Parquet);
        let direct = flats(&mut enc, &Input::OtlpMetrics(&b));
        let recs: OtapArrowRecords = OtlpProtoBytes::ExportMetricsRequest(b.clone().into()).try_into_with_default().unwrap();
        let via = flats(&mut enc, &Input::OtapMetrics(&recs));
        assert_eq!(direct.len(), via.len(), "{file}: object count");
        for (d, v) in direct.iter().zip(via.iter()) {
            assert_eq!(d.signal, v.signal);
            assert_eq!(d.stats, v.stats, "{file} {}: stats", d.signal.name());
            let names = otap_s3pq::schema::metrics(d.signal, &arrow::datatypes::DataType::Binary);
            for (i, (a, c)) in d.cols.iter().zip(v.cols.iter()).enumerate() {
                assert_eq!(a.to_data(), c.to_data(), "{file} {}: column {} differs between OTLP and OTAP input", d.signal.name(), names.field(i).name());
            }
        }
        println!("{file}: OTLP and OTAP input give identical columns for {:?}", direct.iter().map(|f| (f.signal.name(), f.stats.rows)).collect::<Vec<_>>());
    }
}

#[test]
fn empty_metric_rejects_request() {
    // ExportMetricsServiceRequest { resource_metrics { scope_metrics { metrics { name: "x" } } } }
    let metric = [0x0a, 0x01, b'x'];
    let sm = [&[0x12, metric.len() as u8][..], &metric].concat();
    let rm = [&[0x12, sm.len() as u8][..], &sm].concat();
    let req = [&[0x0a, rm.len() as u8][..], &rm].concat();
    let mut enc = Encoder::new(ParquetOptions::default(), Format::Parquet);
    let e = enc.flatten_all(&Input::OtlpMetrics(&req)).err().expect("rejected");
    assert!(e.0.contains("metrics type is unset"), "{}", e.0);
    // A request with no metrics at all: no objects, nothing to reject.
    assert!(enc.flatten_all(&Input::OtlpMetrics(&[])).unwrap().is_empty());
    let _ = Signal::MetricsGauge;
}
