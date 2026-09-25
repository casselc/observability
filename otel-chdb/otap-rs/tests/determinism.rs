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
        };
        let recs: OtapArrowRecords = p.try_into_with_default().unwrap();
        let via = enc.flatten(&Input::Otap(sig, &recs)).unwrap();
        for (i, (a, c)) in direct.cols.iter().zip(via.cols.iter()).enumerate() {
            assert_eq!(a.to_data(), c.to_data(), "{file}: column {i} differs between OTLP and OTAP input");
        }
        println!("{file}: OTLP and OTAP input give identical columns");
    }
}
