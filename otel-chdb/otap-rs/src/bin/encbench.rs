//! In-process edge benchmark, with the same accounting as parquetgo's
//! `pubbench`: one process per configuration, W warm-up batches then N timed
//! ones, one at a time; CPU is getrusage user+sys for the whole process; RSS
//! is peak ru_maxrss. A batch is: content hash, flatten (from OTLP bytes, or
//! via upstream's OTLP→OTAP conversion), encode, and (with --s3) the
//! create-only commit through the real lane (PUT, plus HEAD if ambiguous).
//!
//! A metrics request (`--signal metrics`) becomes one object per non-empty
//! metric type, each committed in its own type's lane (here one after the
//! other; the exporter runs them concurrently). ObjectBytes is their sum.
//!
//! `--layout series` encodes metrics as layout B (`series.rs`): points
//! objects plus a series object whenever a series is new in its window; a
//! committed series object marks its series announced, as in the exporter.
//! `--files a.pb,b.pb,...` (or a directory) cycles through distinct requests
//! instead of repeating `--file`, e.g. the spike's fleet batches, so the
//! series cache sees real series reuse and the hourly re-announce.
//! ObjectsPerBatch / BytesPerBatch average the timed batches.
//!
//!   encbench --file traces-testgen-10000.pb --signal traces|logs|metrics [--s3 URL --key K --secret S]
//!            [--path direct|via_otap] [--format parquet|arrow] [--bloom traceid|none|all]
//!            [--zstd 3] [--batches 30] [--warmup 3] [--out FILE] [--label NAME]
//!            [--layout clickstack|series] [--files LIST|DIR]
//!            [--sort none|service_time --row-groups N --split hash|range] [--bloom-add COL]

use otap_s3pq::Signal;
use otap_s3pq::batch::{Encoder, Format, Input};
use otap_s3pq::encode::{ParquetOptions, RowGroupSplit, SortBy};
use otap_s3pq::flatten::Envelope;
use otap_s3pq::proto::{Lane, PutOutcome, new_epoch};
use otap_s3pq::runner::{self, EncodedCache, Stats, Timeouts};
use otap_s3pq::series::{MetricsLayout, SeriesOptions};
use otap_s3pq::store::{Meta, S3Config, SlotStore, StoreError};
use otel_arrow_dfe_pdata::{OtapArrowRecords, OtlpProtoBytes, TryIntoWithOptions};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn rusage() -> (f64, f64) {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let t = |tv: libc::timeval| tv.tv_sec as f64 * 1000.0 + tv.tv_usec as f64 / 1000.0;
    (t(ru.ru_utime) + t(ru.ru_stime), ru.ru_maxrss as f64 / 1024.0)
}

fn rss_now_mb() -> f64 {
    let s = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: f64 = s.split_whitespace().nth(1).and_then(|x| x.parse().ok()).unwrap_or(0.0);
    pages * 4096.0 / (1 << 20) as f64
}

/// Counts requests by kind.
struct Counting<S> {
    inner: S,
    puts: Cell<u64>,
    heads: Cell<u64>,
    lists: Cell<u64>,
}

#[async_trait::async_trait(?Send)]
impl<S: SlotStore> SlotStore for Counting<S> {
    async fn put_create(&self, key: &str, body: bytes::Bytes, ct: &str, meta: &BTreeMap<String, String>) -> PutOutcome {
        self.puts.set(self.puts.get() + 1);
        self.inner.put_create(key, body, ct, meta).await
    }
    async fn head(&self, key: &str) -> Result<Option<Meta>, StoreError> {
        self.heads.set(self.heads.get() + 1);
        self.inner.head(key).await
    }
    async fn list_dirs(&self, p: &str) -> Result<Vec<String>, StoreError> {
        self.lists.set(self.lists.get() + 1);
        self.inner.list_dirs(p).await
    }
    async fn list_after(&self, p: &str, a: Option<&str>) -> Result<Vec<String>, StoreError> {
        self.lists.set(self.lists.get() + 1);
        self.inner.list_after(p, a).await
    }
}

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1).cloned())
}

fn main() {
    let t_main = Instant::now();
    otel_arrow_dfe_otap::crypto::install_crypto_provider().expect("crypto provider");
    let args: Vec<String> = std::env::args().collect();
    let files: Vec<String> = match arg(&args, "--files") {
        Some(f) if std::path::Path::new(&f).is_dir() => {
            let mut v: Vec<String> = std::fs::read_dir(&f)
                .expect("--files dir")
                .map(|e| e.unwrap().path().to_string_lossy().into_owned())
                .filter(|p| p.ends_with(".pb"))
                .collect();
            v.sort();
            v
        }
        Some(f) => f.split(',').map(str::to_string).collect(),
        None => vec![arg(&args, "--file").expect("--file or --files")],
    };
    let series = arg(&args, "--layout").as_deref() == Some("series");
    let is_metrics = arg(&args, "--signal").as_deref() == Some("metrics");
    let signal = match arg(&args, "--signal").as_deref() {
        Some("logs") => Signal::Logs,
        Some("metrics") => Signal::MetricsGauge,
        _ => Signal::Traces,
    };
    let mut signal_label = if is_metrics { "metrics" } else { signal.name() };
    let batches: usize = arg(&args, "--batches").map_or(30, |s| s.parse().unwrap());
    let warmup: usize = arg(&args, "--warmup").map_or(3, |s| s.parse().unwrap());
    let via_otap = arg(&args, "--path").as_deref() == Some("via_otap");
    // --path otap: the input is already OTAP (as from an OTAP receiver): the
    // records are built once, outside the timed loop, and walked per batch.
    let otap_input = arg(&args, "--path").as_deref() == Some("otap");
    let format = match arg(&args, "--format").as_deref() {
        Some("arrow") => Format::Arrow,
        _ => Format::Parquet,
    };
    let mut opts = ParquetOptions::default();
    if let Some(z) = arg(&args, "--zstd") {
        opts.zstd_level = z.parse().unwrap();
    }
    if let Some(v) = arg(&args, "--writer-version") {
        opts.writer_version = v;
    }
    let mut enc = Encoder::new(opts.clone(), format);
    if series {
        enc = enc.with_metrics_layout(MetricsLayout::SeriesTable, SeriesOptions::default());
    }
    match arg(&args, "--bloom").as_deref() {
        Some("none") => enc.opts.bloom_columns.clear(),
        Some("all") => enc.opts.bloom_columns = ParquetOptions::all_blooms(enc.schemas(signal)),
        _ => {}
    }
    // Trace and log row order (bench/sorting): --sort service_time
    // [--row-groups N] [--split hash|range]; --bloom-add COL adds a filter.
    if let Some(s) = arg(&args, "--sort") {
        enc.opts.sort.by = match s.as_str() {
            "service_time" => SortBy::ServiceTime,
            "none" => SortBy::None,
            o => panic!("--sort {o}"),
        };
    }
    if let Some(k) = arg(&args, "--row-groups") {
        enc.opts.sort.row_groups = k.parse().expect("--row-groups N");
    }
    if let Some(s) = arg(&args, "--split") {
        enc.opts.sort.split = match s.as_str() {
            "hash" => RowGroupSplit::Hash,
            "range" => RowGroupSplit::Range,
            o => panic!("--split {o}"),
        };
    }
    if let Some(c) = arg(&args, "--bloom-add") {
        enc.opts.bloom_columns.push(c);
    }
    let bodies: Vec<bytes::Bytes> = files.iter().map(|f| bytes::Bytes::from(std::fs::read(f).expect("read --file"))).collect();
    let body = bodies[0].clone();
    let otlp = |b: &bytes::Bytes| match signal {
        Signal::Traces => OtlpProtoBytes::ExportTracesRequest(b.clone()),
        Signal::Logs => OtlpProtoBytes::ExportLogsRequest(b.clone()),
        _ => OtlpProtoBytes::ExportMetricsRequest(b.clone()),
    };
    let prebuilt: Option<OtapArrowRecords> =
        otap_input.then(|| otlp(&body).try_into_with_default().expect("otlp -> otap"));
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let store = arg(&args, "--s3").map(|url| {
        let cfg = S3Config {
            url,
            access_key_id: arg(&args, "--key"),
            secret_access_key: arg(&args, "--secret"),
            ..Default::default()
        };
        Counting { inner: cfg.build().expect("s3"), puts: Cell::new(0), heads: Cell::new(0), lists: Cell::new(0) }
    });
    let prefix_of = |sig: Signal| store.as_ref().map(|s| format!("{}/{}", s.inner.prefix, sig.name())).unwrap_or_default();
    let label = arg(&args, "--label").unwrap_or_else(|| {
        format!(
            "rust-{}{}{}{}",
            if series { "series-" } else { "" },
            if via_otap { "via-otap" } else if otap_input { "otap-input" } else { "direct" },
            if format == Format::Arrow { "-arrow" } else { "" },
            match arg(&args, "--bloom").as_deref() {
                Some("none") => "-nobloom",
                Some("all") => "-allbloom",
                _ => "",
            }
        )
    });
    let producer = format!("bench-{label}");
    let mut lanes: BTreeMap<Signal, (Lane, EncodedCache)> =
        Signal::ALL.iter().map(|s| (*s, (Lane::new(new_epoch()), EncodedCache::default()))).collect();
    let stats = Stats::default();
    let timeouts = Timeouts { put: Duration::from_secs(10), head: Duration::from_secs(2) };
    let ready_ms = t_main.elapsed().as_secs_f64() * 1000.0;
    let rss_start = rss_now_mb();

    let mut phase = [0f64; 3]; // flatten (incl. hash, and conversion), encode, commit
    let mut obj_bytes = 0usize;
    let mut rows = 0usize;
    let mut objects = 0usize;
    let mut tot = [0usize; 3]; // objects, bytes, series objects (since the last reset)
    let mut push = |phase: &mut [f64; 3], tot: &mut [usize; 3], i: usize| {
        let body = &bodies[i % bodies.len()];
        let t0 = Instant::now();
        let recs: Option<OtapArrowRecords> = via_otap.then(|| {
            let mut r: OtapArrowRecords = otlp(body).try_into_with_default().expect("otlp -> otap");
            if std::env::var_os("DECODE_IDS").is_some() {
                r.decode_transport_optimized_ids().expect("decode ids");
            }
            r
        });
        let input = match (&recs, &prebuilt) {
            (Some(r), _) | (None, Some(r)) if is_metrics => Input::OtapMetrics(r),
            (Some(r), _) | (None, Some(r)) => Input::Otap(signal, r),
            (None, None) if is_metrics => Input::OtlpMetrics(body),
            (None, None) => Input::Otlp(signal, body),
        };
        let mut flats = enc.flatten_all(&input).expect("flatten");
        for flat in &mut flats {
            if via_otap && flat.signal != Signal::MetricsSeries {
                flat.content = otap_s3pq::batch::content_hash_otlp(flat.signal, body);
            }
            // Every batch is a new request: make the content key unique so the
            // lane commits each one (a real stream has distinct requests).
            flat.content = format!("{}{:08x}", &flat.content[..24], i);
        }
        rows = flats.iter().map(|f| f.stats.rows).sum();
        let t1 = Instant::now();
        let received_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        obj_bytes = 0;
        for flat in &flats {
            let (lane, cache) = lanes.get_mut(&flat.signal).unwrap();
            let t1 = Instant::now();
            match &store {
                None => {
                    let env = Envelope { producer: producer.clone(), epoch: lane.epoch.clone(), batch: i as u64, received_ns };
                    let o = enc.encode(flat, &env).expect("encode");
                    obj_bytes += o.body.len();
                    if !flat.announce.is_empty() {
                        enc.series_announced(&flat.announce, &env.epoch);
                    }
                    if let (Some(out), 0) = (arg(&args, "--out"), i) {
                        let out = if is_metrics { format!("{out}.{}", flat.signal.name()) } else { out };
                        std::fs::write(out, &o.body).expect("write --out");
                    }
                    phase[1] += t1.elapsed().as_secs_f64() * 1000.0;
                }
                Some(st) => {
                    let mut enc_ms = 0.0;
                    let mut size = 0;
                    let mut encode = |r: &otap_s3pq::proto::Ref| {
                        let te = Instant::now();
                        let env = Envelope { producer: producer.clone(), epoch: r.epoch.clone(), batch: r.seq, received_ns };
                        let o = enc.encode(flat, &env).map_err(|e| e.0);
                        if let Ok(o) = &o {
                            size = o.body.len();
                        }
                        enc_ms += te.elapsed().as_secs_f64() * 1000.0;
                        o
                    };
                    let prefix = prefix_of(flat.signal);
                    let r = rt
                        .block_on(runner::append(
                            lane, cache, st, &prefix, &producer, &flat.content, &mut encode, &timeouts, &stats,
                        ))
                        .expect("append");
                    if !flat.announce.is_empty() {
                        enc.series_announced(&flat.announce, &r.epoch);
                    }
                    obj_bytes += size;
                    phase[1] += enc_ms;
                    phase[2] += t1.elapsed().as_secs_f64() * 1000.0 - enc_ms;
                }
            }
        }
        objects = flats.len();
        tot[0] += flats.len();
        tot[1] += obj_bytes;
        tot[2] += flats.iter().filter(|f| f.signal == Signal::MetricsSeries).count();
        if is_metrics && flats.len() == 1 {
            signal_label = flats[0].signal.name(); // one metric type: name it
        }
        phase[0] += (t1 - t0).as_secs_f64() * 1000.0;
    };

    let t_first = Instant::now();
    push(&mut phase, &mut tot, 0);
    let first_ms = t_first.elapsed().as_secs_f64() * 1000.0;
    for i in 1..warmup {
        push(&mut phase, &mut tot, i);
    }
    phase = [0.0; 3];
    tot = [0; 3];
    let c0 = store.as_ref().map(|s| (s.puts.get(), s.heads.get()));
    let (cpu0, _) = rusage();
    let t0 = Instant::now();
    let mut lat = Vec::with_capacity(batches);
    for i in 0..batches {
        let s = Instant::now();
        push(&mut phase, &mut tot, warmup + i);
        lat.push(s.elapsed().as_secs_f64() * 1000.0);
    }
    let el = t0.elapsed().as_secs_f64();
    let (cpu1, maxrss) = rusage();
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = batches as f64;
    let mut out = serde_json::json!({
        "Impl": label, "Signal": signal_label, "Objects": objects, "Dest": if store.is_some() { "s3" } else { "local" },
        "Rows": rows, "Batches": batches, "ReadyMS": ready_ms, "FirstBatchMS": first_ms,
        "MedianMS": lat[lat.len() / 2], "MinMS": lat[0], "MaxMS": lat[lat.len() - 1],
        "RowsPerSec": (rows * batches) as f64 / el, "CPUMSPerBatch": (cpu1 - cpu0) / n,
        "MaxRSSMB": maxrss, "RSSAfterStartMB": rss_start, "ObjectBytes": obj_bytes,
        "ObjectsPerBatch": tot[0] as f64 / n, "BytesPerBatch": tot[1] as f64 / n,
        "SeriesObjectsPerBatch": tot[2] as f64 / n, "Files": bodies.len(),
        "FlattenMSPerBatch": phase[0] / n, "EncodeMSPerBatch": phase[1] / n, "CommitMSPerBatch": phase[2] / n,
    });
    if let (Some(s), Some((p0, h0))) = (&store, c0) {
        out["S3PerBatch"] = serde_json::json!({
            "PUT": (s.puts.get() - p0) as f64 / n, "HEAD": (s.heads.get() - h0) as f64 / n,
        });
    }
    println!("{out}");
}
