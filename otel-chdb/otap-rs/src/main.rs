//! A lean otap-dataflow engine: the OTLP receiver (from core-nodes, `otlp`
//! feature only) and this crate's `exporter:s3pq`.

use otel_arrow_dfe_config::config_provider::{ConfigFormat, resolve_config};
use otel_arrow_dfe_config::engine::OtelDataflowSpec;
use otel_arrow_dfe_controller::startup;
use otel_arrow_dfe_controller::{BuildInfo, Controller, ControllerRunOptions};
use otel_arrow_dfe_otap::OTAP_PIPELINE_FACTORY;
// Side-effect imports: link the crates so their linkme registrations are seen.
use otap_s3pq as _;
use otel_arrow_dfe_core_nodes as _;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    otel_arrow_dfe_otap::crypto::install_crypto_provider()
        .map_err(|e| format!("install rustls crypto provider: {e}"))?;
    let mut args = std::env::args().skip(1);
    let mut config = None;
    let mut validate = false;
    while let Some(a) = args.next() {
        match a.as_str() {
            "-c" | "--config" => config = args.next(),
            "--validate-and-exit" => validate = true,
            other => return Err(format!("unknown argument {other}").into()),
        }
    }
    let resolved = resolve_config(config.as_deref())?;
    let engine_cfg = match resolved.format {
        ConfigFormat::Json => OtelDataflowSpec::from_json(&resolved.content)?,
        ConfigFormat::Yaml => OtelDataflowSpec::from_yaml(&resolved.content)?,
    };
    let run_options = ControllerRunOptions {
        build_info: BuildInfo {
            service_name: Some("otap-s3pq".into()),
            service_version: Some(env!("CARGO_PKG_VERSION").into()),
        },
        handle_os_signals: true,
        ..Default::default()
    };
    startup::validate_engine_components(&engine_cfg, &OTAP_PIPELINE_FACTORY)?;
    startup::validate_controller_extensions(&engine_cfg, &run_options.extensions)?;
    if validate {
        println!("configuration '{}' is valid", resolved.source);
        return Ok(());
    }
    let controller = Controller::new(&OTAP_PIPELINE_FACTORY);
    match controller.run_forever_with_options(engine_cfg, run_options) {
        Ok(_) => Ok(()),
        Err(e) => {
            eprintln!("pipeline failed: {e}");
            std::process::exit(1);
        }
    }
}
