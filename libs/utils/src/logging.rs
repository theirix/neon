use std::str::FromStr;

use anyhow::Context;
use strum_macros::{EnumString, EnumVariantNames};
use tracing_subscriber::{
    fmt::{format, Layer},
    layer::Layered,
    reload, EnvFilter, Registry,
};

#[derive(EnumString, EnumVariantNames, Eq, PartialEq, Debug, Clone, Copy)]
#[strum(serialize_all = "snake_case")]
pub enum LogFormat {
    Plain,
    Json,
}

impl LogFormat {
    pub fn from_config(s: &str) -> anyhow::Result<LogFormat> {
        use strum::VariantNames;
        LogFormat::from_str(s).with_context(|| {
            format!(
                "Unrecognized log format. Please specify one of: {:?}",
                LogFormat::VARIANTS
            )
        })
    }
}

pub type PlainSubscriber = Layered<Layer<Registry>, Registry>;

pub type StdoutWriter = fn() -> std::io::Stdout;
pub type JsonSubscriber = Layered<
    Layer<Registry, format::JsonFields, format::Format<format::Json>, StdoutWriter>,
    Registry,
>;

/// A helper enum to ease log reloads.
/// Tracing [0.4](https://github.com/tokio-rs/tracing/milestone/11)
/// includes https://github.com/tokio-rs/tracing/pull/1035
/// that makes this enum and related types obsolete.
pub enum LogReloadHandle {
    Plain(reload::Handle<EnvFilter, PlainSubscriber>),
    Json(reload::Handle<EnvFilter, JsonSubscriber>),
}

pub fn init(log_format: LogFormat) -> LogReloadHandle {
    let default_filter_str = "info";

    // We fall back to printing all spans at info-level or above if
    // the RUST_LOG environment variable is not set.
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter_str));

    let base_logger = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .with_ansi(atty::is(atty::Stream::Stdout))
        .with_writer(std::io::stdout as StdoutWriter);

    match log_format {
        LogFormat::Json => {
            let json = base_logger.json().with_filter_reloading();
            let handle = json.reload_handle();
            json.init();
            LogReloadHandle::Json(handle)
        }
        LogFormat::Plain => {
            let plain = base_logger.with_filter_reloading();
            let handle = plain.reload_handle();
            plain.init();
            LogReloadHandle::Plain(handle)
        }
    }
}
