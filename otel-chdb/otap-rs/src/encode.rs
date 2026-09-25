//! Parquet (and Arrow IPC) encoding of one batch, tuned for central ingest.
//!
//! One row group per batch; zstd; dictionary on every column except the
//! near-unique ones; chunk + page statistics and the page index; bloom
//! filters only where they pay (default: TraceId, sized for the batch). No
//! `ARROW:schema` in the footer. The batch description goes into the footer's
//! key-value metadata (sorted keys), so the object is self-describing without
//! its S3 metadata. Encoding is deterministic: the same rows and metadata give
//! the same bytes.

use crate::schema::{HIGH_CARDINALITY, Schemas};
use arrow::array::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::schema::types::ColumnPath;
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ParquetOptions {
    /// zstd level (0 = none, i.e. uncompressed).
    pub zstd_level: i32,
    pub dictionary: bool,
    /// "none", "chunk" or "page" (page = chunk + page stats + column index).
    pub statistics: String,
    /// Leaf column paths (dotted, e.g. "TraceId") that get a bloom filter.
    pub bloom_columns: Vec<String>,
    pub bloom_fpp: f64,
    pub data_page_size: usize,
    /// "1.0" (V1 data pages) or "2.0".
    pub writer_version: String,
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
        }
    }
}

impl ParquetOptions {
    /// Every string/map/list leaf, as parquetgo's (and ClickHouse's) default.
    pub fn all_blooms(sc: &Schemas) -> Vec<String> {
        sc.parquet.columns().iter().map(|c| c.path().string()).collect()
    }

    fn properties(&self, rows: usize, kv: Vec<KeyValue>) -> WriterProperties {
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
            .set_statistics_enabled(match self.statistics.as_str() {
                "none" => EnabledStatistics::None,
                "chunk" => EnabledStatistics::Chunk,
                _ => EnabledStatistics::Page,
            })
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
        for c in &self.bloom_columns {
            let path = ColumnPath::from(c.as_str());
            b = b
                .set_column_bloom_filter_enabled(path.clone(), true)
                .set_column_bloom_filter_fpp(path.clone(), self.bloom_fpp)
                // Sized for this batch: the default NDV (1M) would make a
                // ~1.8 MB filter per column.
                .set_column_bloom_filter_ndv(path, rows.max(1) as u64);
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
        .with_properties(opts.properties(batch.num_rows(), kv))
        .with_skip_arrow_metadata(true)
        .with_parquet_schema(sc.parquet.clone());
    let mut w = ArrowWriter::try_new_with_options(out, sc.arrow.clone(), options)?;
    w.write(batch)?;
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
