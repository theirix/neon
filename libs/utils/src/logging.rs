use std::{
    fs::{File, OpenOptions},
    path::Path,
};

use anyhow::{Context, Result};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

macro_rules! init_tracing {
    ($subscriber:expr) => {
        let otel_service_name = std::env::var("OTEL_SERVICE_NAME")
            .ok()
            .filter(|v| !v.trim().is_empty());
        let jaeger_endpoint = std::env::var("OTEL_EXPORTER_JAEGER_ENDPOINT")
            .ok()
            .filter(|v| !v.trim().is_empty());

        if otel_service_name.is_some() || jaeger_endpoint.is_some() {
            let pipeline_builder = opentelemetry_jaeger::new_pipeline().with_trace_config(
                opentelemetry::sdk::trace::Config::default().with_resource(
                    opentelemetry::sdk::Resource::new(vec![opentelemetry::KeyValue::new(
                        "hostname",
                        std::env::var("HOSTNAME")
                            .ok()
                            .unwrap_or_else(|| "unknown".to_string()),
                    )]),
                ),
            );
            let tracer = pipeline_builder.install_simple()?;

            let subscriber =
                $subscriber.with(tracing_opentelemetry::OpenTelemetryLayer::new(tracer));
            tracing::dispatcher::Dispatch::new(subscriber).try_init()?;
        } else {
            tracing::dispatcher::Dispatch::new($subscriber).try_init()?;
        }
    };
}

pub fn init(log_filename: impl AsRef<Path>, daemonize: bool) -> Result<File> {
    // Don't open the same file for output multiple times;
    // the different fds could overwrite each other's output.
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_filename)
        .with_context(|| format!("failed to open {:?}", log_filename.as_ref()))?;

    let default_filter_str = "info";

    // We fall back to printing all spans at info-level or above if
    // the RUST_LOG environment variable is not set.
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter_str));

    let base_logger = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false) // don't include event targets
        .with_ansi(false); // don't use colors in log file;

    // we are cloning and returning log file in order to allow redirecting daemonized stdout and stderr to it
    // if we do not use daemonization (e.g. in docker) it is better to log to stdout directly
    // for example to be in line with docker log command which expects logs comimg from stdout
    if daemonize {
        let x = log_file.try_clone().unwrap();
        let subscriber = base_logger
            .with_writer(move || x.try_clone().unwrap())
            .finish();
        init_tracing!(subscriber);
    } else {
        let subscriber = base_logger.finish();
        init_tracing!(subscriber);
    };

    Ok(log_file)
}
