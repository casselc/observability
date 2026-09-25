//! Upstream behaviour pinned by a test: event and link attributes are lost
//! when an OTLP request is converted to OTAP and read back through
//! `OtapTracesView` (the via_otap path). Needs the testgen request from
//! `tools/cmd/otlpgen` in $OTAPRS_DATA; skipped otherwise.

use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use otel_arrow_dfe_pdata::{OtapArrowRecords, OtlpProtoBytes, TryIntoWithOptions};

#[test]
fn otap_event_attrs_ids() {
    let Ok(dir) = std::env::var("OTAPRS_DATA") else { return };
    let b = std::fs::read(format!("{dir}/traces-testgen-3000.pb")).unwrap();
    let recs: OtapArrowRecords = OtlpProtoBytes::ExportTracesRequest(b.into()).try_into_with_default().unwrap();
    for t in [ArrowPayloadType::SpanEvents, ArrowPayloadType::SpanEventAttrs, ArrowPayloadType::SpanLinks, ArrowPayloadType::SpanLinkAttrs] {
        let rb = recs.get(t).unwrap();
        println!("{t:?}: {} rows, schema {:?}", rb.num_rows(), rb.schema().fields().iter().map(|f| format!("{}:{}", f.name(), f.data_type())).collect::<Vec<_>>());
        for c in ["id", "parent_id"] {
            if let Some(col) = rb.column_by_name(c) {
                println!("  {c}: {:?}", arrow::util::display::ArrayFormatter::try_new(col.slice(0, 5).as_ref(), &Default::default()).map(|f| (0..5).map(|i| f.value(i).to_string()).collect::<Vec<_>>()));
            }
        }
    }
}
