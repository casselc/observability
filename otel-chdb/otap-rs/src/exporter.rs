//! `urn:otel:exporter:s3pq`: the otap-dataflow node.
//!
//! Per request: content hash and flatten (on the pipeline thread), then
//! append to a lane's log (encode for the slot, create-only PUT, HEAD on a
//! 412 or no answer). The request is ACKed only when the commit is resolved
//! as ours (or found committed); NACKed (retryable) when the outcome is still
//! unknown, and permanently when the data can't be encoded. With the OTLP
//! receiver's `wait_for_result`, the client's response follows the commit.
//!
//! Lanes: one log per (signal, lane). A request goes to lane
//! `hash(content) mod lanes`, so a retry meets the lane, and the unresolved
//! slot, of its first attempt. Lanes append concurrently; one lane appends
//! one batch at a time.

use crate::batch::{Encoder, Flat, Format, Input};
use crate::encode::ParquetOptions;
use crate::flatten::Envelope;
use crate::proto::{self, Lane, Ref};
use crate::runner::{self, AppendError, EncodedCache, Stats, Timeouts};
use crate::store::{S3Config, S3Store};
use crate::Signal;
use async_trait::async_trait;
use futures::StreamExt;
use futures::future::LocalBoxFuture;
use futures::stream::FuturesUnordered;
use linkme::distributed_slice;
use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::config::ExporterConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::control::{AckMsg, NackCause, NackMsg, NodeControlMsg};
use otel_arrow_dfe_engine::error::{Error, ExporterErrorKind};
use otel_arrow_dfe_engine::exporter::ExporterWrapper;
use otel_arrow_dfe_engine::local::exporter::{EffectHandler, Exporter};
use otel_arrow_dfe_engine::message::{ExporterInbox, Message};
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::terminal_state::TerminalState;
use otel_arrow_dfe_engine::{ConsumerEffectHandlerExtension, ExporterFactory};
use otel_arrow_dfe_otap::OTAP_EXPORTER_FACTORIES;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_pdata::{OtapArrowRecords, OtlpProtoBytes, PayloadData, TryIntoWithOptions};
use serde::Deserialize;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Instant, SystemTime};

pub const S3PQ_EXPORTER_URN: &str = "urn:otel:exporter:s3pq";

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OtlpPath {
    /// Walk the OTLP protobuf bytes directly (zero-copy views).
    #[default]
    Direct,
    /// Convert OTLP to OTAP records first (upstream's encoder), then walk those.
    ViaOtap,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub s3: S3Config,
    pub producer_id: String,
    #[serde(default = "one")]
    pub lanes: usize,
    #[serde(default)]
    pub format: Format,
    #[serde(default)]
    pub otlp_path: OtlpPath,
    #[serde(default)]
    pub parquet: ParquetOptions,
    /// Write each batch's stats line to stderr.
    #[serde(default)]
    pub verbose: bool,
}

fn one() -> usize {
    1
}

pub fn validate_config(v: &serde_json::Value) -> Result<(), otel_arrow_dfe_config::error::Error> {
    let c: Config = serde_json::from_value(v.clone()).map_err(|e| {
        otel_arrow_dfe_config::error::Error::InvalidUserConfig { error: e.to_string() }
    })?;
    if c.s3.url.is_empty() || c.producer_id.is_empty() {
        return Err(otel_arrow_dfe_config::error::Error::InvalidUserConfig {
            error: "s3.url and producer_id are required".into(),
        });
    }
    Ok(())
}

#[allow(unsafe_code)]
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
pub static S3PQ_EXPORTER: ExporterFactory<OtapPdata> = ExporterFactory {
    name: S3PQ_EXPORTER_URN,
    create:
        |_pipeline: PipelineContext,
         node: NodeId,
         node_config: Arc<NodeUserConfig>,
         exporter_config: &ExporterConfig,
         _capabilities: &otel_arrow_dfe_engine::capability::registry::Capabilities| {
            let config: Config = serde_json::from_value(node_config.config.clone()).map_err(|e| {
                otel_arrow_dfe_config::error::Error::InvalidUserConfig { error: e.to_string() }
            })?;
            Ok(ExporterWrapper::local(S3pqExporter { config }, node, node_config, exporter_config))
        },
    validate_config,
    context_declarations: None,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
};

pub struct S3pqExporter {
    config: Config,
}

struct LaneState {
    lane: Lane,
    cache: EncodedCache,
}

type Done = (OtapPdata, Result<Ref, AppendError>, Instant, usize);

struct Shared {
    store: S3Store,
    encoder: RefCell<Encoder>,
    stats: Stats,
    timeouts: Timeouts,
    producer: String,
    lanes: HashMap<(Signal, usize), Rc<tokio::sync::Mutex<LaneState>>>,
    prefixes: HashMap<Signal, String>,
    verbose: bool,
}

fn signal_of(t: SignalType) -> Option<Signal> {
    match t {
        SignalType::Traces => Some(Signal::Traces),
        SignalType::Logs => Some(Signal::Logs),
        _ => None,
    }
}

fn now_ns() -> u64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_nanos() as u64
}

/// Content hash + flattened columns for one request.
fn prepare(sh: &Shared, pdata: &OtapPdata, path: OtlpPath) -> Result<Flat, String> {
    let signal = signal_of(pdata.signal_type()).ok_or("only traces and logs are supported")?;
    let mut enc = sh.encoder.borrow_mut();
    match pdata.payload_ref().data() {
        PayloadData::OtlpBytes(b) => {
            let bytes = match b {
                OtlpProtoBytes::ExportTracesRequest(x) | OtlpProtoBytes::ExportLogsRequest(x) => x,
                OtlpProtoBytes::ExportMetricsRequest(_) => return Err("metrics are not supported".into()),
            };
            match path {
                OtlpPath::Direct => enc.flatten(&Input::Otlp(signal, bytes)).map_err(|e| e.0),
                OtlpPath::ViaOtap => {
                    let recs: OtapArrowRecords = b.clone().try_into_with_default().map_err(|e| format!("{e}"))?;
                    let mut f = enc.flatten(&Input::Otap(signal, &recs)).map_err(|e| e.0)?;
                    // Keep the request's content key, so both paths dedup alike.
                    f.content = crate::batch::content_hash_otlp(signal, bytes);
                    Ok(f)
                }
            }
        }
        PayloadData::OtapArrowRecords(r) => enc.flatten(&Input::Otap(signal, r)).map_err(|e| e.0),
    }
}

fn commit(sh: Rc<Shared>, pdata: OtapPdata, flat: Flat, received_ns: u64) -> LocalBoxFuture<'static, Done> {
    Box::pin(async move {
        let started = Instant::now();
        let n = sh.lanes.len() / 2;
        let idx = if n <= 1 {
            0
        } else {
            u64::from_str_radix(&flat.content[..16], 16).unwrap_or(0) as usize % n
        };
        let lane = sh.lanes[&(flat.signal, idx)].clone();
        let mut g = lane.lock().await;
        let st = &mut *g;
        let prefix = &sh.prefixes[&flat.signal];
        let producer = sh.producer.clone();
        let mut encode = |r: &Ref| {
            let env = Envelope {
                producer: producer.clone(),
                epoch: r.epoch.clone(),
                batch: r.seq,
                received_ns,
            };
            sh.encoder.borrow().encode(&flat, &env).map_err(|e| e.0)
        };
        let res = runner::append(
            &mut st.lane,
            &mut st.cache,
            &sh.store,
            prefix,
            &sh.producer,
            &flat.content,
            &mut encode,
            &sh.timeouts,
            &sh.stats,
        )
        .await;
        if sh.verbose {
            match &res {
                Ok(r) => crate::log(&format!(
                    "{} rows={} content={} -> {}/{} in {:?}",
                    flat.signal.name(),
                    flat.stats.rows,
                    flat.content,
                    r.epoch,
                    r.seq,
                    started.elapsed()
                )),
                Err(e) => crate::log(&format!("{} content={}: {e}", flat.signal.name(), flat.content)),
            }
        }
        (pdata, res, started, flat.stats.rows)
    })
}

#[async_trait(?Send)]
impl Exporter<OtapPdata> for S3pqExporter {
    async fn start(
        self: Box<Self>,
        mut inbox: ExporterInbox<OtapPdata>,
        effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let cfg = self.config;
        let store = cfg.s3.build().map_err(|e| Error::ExporterError {
            exporter: effect_handler.exporter_id(),
            kind: ExporterErrorKind::Configuration,
            error: e.to_string(),
            source_detail: String::new(),
        })?;
        let mut lanes = HashMap::new();
        let mut prefixes = HashMap::new();
        for s in [Signal::Traces, Signal::Logs] {
            let _ = prefixes.insert(s, format!("{}/{}", store.prefix, s.name()).trim_start_matches('/').to_string());
            for i in 0..cfg.lanes.max(1) {
                let _ = lanes.insert(
                    (s, i),
                    Rc::new(tokio::sync::Mutex::new(LaneState {
                        lane: Lane::new(proto::new_epoch()),
                        cache: EncodedCache::default(),
                    })),
                );
            }
        }
        crate::log(&format!(
            "exporter start: {} lanes, epochs {:?}",
            cfg.lanes,
            lanes.iter().map(|(k, v)| (k.0.name(), k.1, v.try_lock().map(|g| g.lane.epoch.clone()).unwrap_or_default())).collect::<Vec<_>>()
        ));
        let sh = Rc::new(Shared {
            timeouts: Timeouts { put: cfg.s3.put_timeout, head: cfg.s3.head_timeout },
            store,
            encoder: RefCell::new(Encoder::new(cfg.parquet.clone(), cfg.format)),
            stats: Stats::default(),
            producer: cfg.producer_id.clone(),
            lanes,
            prefixes,
            verbose: cfg.verbose,
        });
        let max_in_flight = cfg.lanes.max(1) * 2;
        let mut in_flight: FuturesUnordered<LocalBoxFuture<'static, Done>> = FuturesUnordered::new();

        let finish = |d: Done, eh: &EffectHandler<OtapPdata>| {
            let (pdata, res, _started, _rows) = d;
            let eh = eh.clone();
            async move {
                match res {
                    Ok(_) => eh.notify_ack(AckMsg::new(pdata)).await,
                    // The data can't be encoded: a client error (OTLP 400 / INVALID_ARGUMENT).
                    Err(AppendError::Encode(e)) => {
                        eh.notify_nack(NackMsg::new_permanent_with_cause(e, pdata, NackCause::Refused)).await
                    }
                    Err(e @ AppendError::Unresolved(_)) => eh.notify_nack(NackMsg::new(e.to_string(), pdata)).await,
                }
            }
        };

        loop {
            let accepting = in_flight.len() < max_in_flight;
            let msg = tokio::select! {
                biased;
                Some(d) = in_flight.next(), if !in_flight.is_empty() => {
                    finish(d, &effect_handler).await?;
                    continue;
                }
                m = inbox.recv_when(accepting) => m?,
            };
            match msg {
                Message::Control(NodeControlMsg::Shutdown { deadline, .. }) => {
                    let until = tokio::time::Instant::from_std(deadline);
                    while !in_flight.is_empty() {
                        match tokio::time::timeout_at(until, in_flight.next()).await {
                            Ok(Some(d)) => finish(d, &effect_handler).await?,
                            _ => break,
                        }
                    }
                    let s = &sh.stats;
                    crate::log(&format!(
                        "exporter stop: committed={} resolved_own={} resent={} learned_other={} halted={} known_skipped={} encodes={} puts={} heads={} abandoned={}",
                        s.committed.get(), s.resolved_own.get(), s.resent.get(), s.learned_other.get(),
                        s.halted.get(), s.known_skipped.get(), s.encodes.get(), s.puts.get(), s.heads.get(),
                        in_flight.len()
                    ));
                    return Ok(TerminalState::new(deadline, Vec::<otel_arrow_dfe_telemetry::metrics::MetricSetSnapshot>::new()));
                }
                Message::Control(_) => {}
                Message::PData(pdata) => {
                    if pdata.is_empty() {
                        effect_handler.notify_ack(AckMsg::new(pdata)).await?;
                        continue;
                    }
                    let received_ns = now_ns();
                    match prepare(&sh, &pdata, cfg.otlp_path) {
                        Ok(flat) => in_flight.push(commit(sh.clone(), pdata, flat, received_ns)),
                        Err(e) => {
                            crate::log(&format!("rejecting request: {e}"));
                            effect_handler
                                .notify_nack(NackMsg::new_permanent_with_cause(e, pdata, NackCause::Refused))
                                .await?;
                        }
                    }
                }
            }
        }
    }
}
