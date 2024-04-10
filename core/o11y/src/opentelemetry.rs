use crate::reload::TracingLayer;
use near_crypto::PublicKey;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::AccountId;
use opentelemetry::sdk::trace::{self, IdGenerator, Sampler};
use opentelemetry::sdk::Resource;
use opentelemetry::trace::TraceId;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use tracing::level_filters::LevelFilter;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::filter::targets::Targets;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{reload, Layer};

// Doesn't define WARN and ERROR, because the highest verbosity of spans is INFO.
#[derive(Copy, Clone, Debug, Default, clap::ValueEnum)]
pub enum OpenTelemetryLevel {
    #[default]
    OFF,
    INFO,
    DEBUG,
    TRACE,
}

/// Constructs an OpenTelemetryConfig which sends span data to an external collector.
//
// NB: this function is `async` because `install_batch(Tokio)` requires a tokio context to
// register timers and channels and whatnot.
pub(crate) async fn add_opentelemetry_layer<S>(
    opentelemetry_level: OpenTelemetryLevel,
    chain_id: String,
    node_public_key: PublicKey,
    account_id: Option<AccountId>,
    subscriber: S,
) -> (TracingLayer<S>, reload::Handle<Targets, S>)
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync,
{
    let filter = get_opentelemetry_filter(opentelemetry_level);
    let (filter, handle) = reload::Layer::<Targets, S>::new(filter);

    let mut resource = vec![
        KeyValue::new("chain_id", chain_id),
        KeyValue::new("node_id", node_public_key.to_string()),
    ];
    // Prefer account name as the node name.
    // Fallback to a node public key if a validator key is unavailable.
    let service_name = if let Some(account_id) = account_id {
        resource.push(KeyValue::new("account_id", account_id.to_string()));
        format!("neard:{}", account_id)
    } else {
        format!("neard:{}", node_public_key)
    };
    resource.push(KeyValue::new(SERVICE_NAME, service_name));

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter().tonic().with_endpoint("http://34.32.208.68:5433"),
        )
        .with_trace_config(
            trace::config()
                .with_sampler(Sampler::AlwaysOn)
                .with_id_generator(IdGenerator::default())
                .with_resource(Resource::new(resource)),
        )
        .install_batch(opentelemetry::runtime::Tokio)
        .unwrap();
    let layer = tracing_opentelemetry::layer().with_tracer(tracer).with_filter(filter);
    (subscriber.with(layer), handle)
}

pub(crate) fn get_opentelemetry_filter(opentelemetry_level: OpenTelemetryLevel) -> Targets {
    Targets::new().with_default(match opentelemetry_level {
        OpenTelemetryLevel::OFF => LevelFilter::OFF,
        OpenTelemetryLevel::INFO => LevelFilter::INFO,
        OpenTelemetryLevel::DEBUG => LevelFilter::DEBUG,
        OpenTelemetryLevel::TRACE => LevelFilter::TRACE,
    })
}

pub fn root_span_for_chunk(chunk_hash: CryptoHash) -> Span {
    let mut chunk_hash_first_16_bytes = [0u8; 16];
    chunk_hash_first_16_bytes.copy_from_slice(&chunk_hash.0[..16]);
    let trace_id = TraceId::from_bytes(chunk_hash_first_16_bytes);

    let span =
        tracing::info_span!( target: "chunk_tracing", parent: None, "chunk_tracing", ?chunk_hash);
    span.set_trace_id(trace_id);
    span
}
