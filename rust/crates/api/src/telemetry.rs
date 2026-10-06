use std::{sync::OnceLock, time::Duration};

use anyhow::Context;
use opentelemetry::{
    global,
    metrics::{Counter, Gauge, Histogram},
    trace::TracerProvider as _,
    KeyValue,
};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::{
    logs::SdkLoggerProvider,
    metrics::{PeriodicReader, SdkMeterProvider},
    trace::{Sampler, SdkTracerProvider},
    Resource,
};
use tracing::warn;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const METRICS_METER_NAME: &str = "turbo-vector-api";
const DEFAULT_METRIC_EXPORT_INTERVAL_MS: u64 = 5_000;

#[derive(Debug, Clone)]
pub(crate) struct TelemetryConfig {
    pub(crate) enabled: bool,
    pub(crate) exporter_otlp_endpoint: String,
    pub(crate) service_name: String,
    pub(crate) metric_export_interval_ms: u64,
    pub(crate) sample_ratio: f64,
}

impl TelemetryConfig {
    pub(crate) fn with_defaults(
        enabled: bool,
        exporter_otlp_endpoint: String,
        service_name: String,
        metric_export_interval_ms: u64,
        sample_ratio: f64,
    ) -> Self {
        Self {
            enabled,
            exporter_otlp_endpoint,
            service_name,
            metric_export_interval_ms: metric_export_interval_ms
                .max(DEFAULT_METRIC_EXPORT_INTERVAL_MS),
            sample_ratio: sample_ratio.clamp(0.0, 1.0),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TelemetryRuntime {
    pub(crate) enabled: bool,
    pub(crate) service_name: String,
    pub(crate) exporter_otlp_endpoint: String,
    pub(crate) metric_export_interval_ms: u64,
    pub(crate) sample_ratio: f64,
}

static TELEMETRY_PROVIDERS: OnceLock<(SdkTracerProvider, SdkMeterProvider, SdkLoggerProvider)> =
    OnceLock::new();
static APP_METRICS: OnceLock<AppMetrics> = OnceLock::new();

pub(crate) fn init_telemetry(config: TelemetryConfig) -> anyhow::Result<TelemetryRuntime> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer();

    if !config.enabled {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .init();
        return Ok(TelemetryRuntime {
            enabled: false,
            service_name: config.service_name,
            exporter_otlp_endpoint: config.exporter_otlp_endpoint,
            metric_export_interval_ms: config.metric_export_interval_ms,
            sample_ratio: config.sample_ratio,
        });
    }

    let trace_endpoint = normalize_otlp_endpoint(&config.exporter_otlp_endpoint, "traces");
    let metric_endpoint = normalize_otlp_endpoint(&config.exporter_otlp_endpoint, "metrics");
    let log_endpoint = normalize_otlp_endpoint(&config.exporter_otlp_endpoint, "logs");

    let resource = Resource::builder()
        .with_attributes([KeyValue::new("service.name", config.service_name.clone())])
        .build();

    let trace_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(trace_endpoint)
        .build()
        .context("failed to build OTLP trace exporter")?;

    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            config.sample_ratio,
        ))))
        .with_batch_exporter(trace_exporter)
        .build();
    let tracer = tracer_provider.tracer(METRICS_METER_NAME);

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(metric_endpoint)
        .build()
        .context("failed to build OTLP metric exporter")?;
    let reader = PeriodicReader::builder(metric_exporter)
        .with_interval(Duration::from_millis(config.metric_export_interval_ms))
        .build();
    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .with_reader(reader)
        .build();

    let log_exporter = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(log_endpoint)
        .build()
        .context("failed to build OTLP log exporter")?;
    let logger_provider = SdkLoggerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(log_exporter)
        .build();

    global::set_tracer_provider(tracer_provider.clone());
    global::set_meter_provider(meter_provider.clone());

    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let otel_log_layer = OpenTelemetryTracingBridge::new(&logger_provider);
    let _ = TELEMETRY_PROVIDERS.set((
        tracer_provider.clone(),
        meter_provider.clone(),
        logger_provider,
    ));

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(otel_layer)
        .with(otel_log_layer)
        .init();

    Ok(TelemetryRuntime {
        enabled: true,
        service_name: config.service_name,
        exporter_otlp_endpoint: config.exporter_otlp_endpoint,
        metric_export_interval_ms: config.metric_export_interval_ms,
        sample_ratio: config.sample_ratio,
    })
}

fn normalize_otlp_endpoint(raw: &str, signal: &str) -> String {
    let trimmed = raw.trim().trim_end_matches('/');
    if trimmed.contains("/v1/traces")
        || trimmed.contains("/v1/metrics")
        || trimmed.contains("/v1/logs")
    {
        return trimmed.to_string();
    }
    format!("{trimmed}/v1/{signal}")
}

struct AppMetrics {
    query_duration_seconds: Histogram<f64>,
    query_total: Counter<u64>,
    upsert_request_duration_seconds: Histogram<f64>,
    upsert_accepted_total: Counter<u64>,
    upsert_applied_total: Counter<u64>,
    operation_apply_lag_seconds: Histogram<f64>,
    queue_depth: Gauge<f64>,
    queue_claim_total: Counter<u64>,
    queue_ack_total: Counter<u64>,
    queue_requeue_total: Counter<u64>,
    worker_flush_applied_jobs_total: Counter<u64>,
    cache_hits_total: Counter<u64>,
    cache_misses_total: Counter<u64>,
    cache_evictions_total: Counter<u64>,
    cache_entries: Gauge<f64>,
    cache_bytes: Gauge<f64>,
    cache_capacity_entries: Gauge<f64>,
    cache_capacity_bytes: Gauge<f64>,
    cache_utilization_ratio: Gauge<f64>,
    query_temperature_total: Counter<u64>,
    query_namespace_load_duration_seconds: Histogram<f64>,
    query_cache_fill_total: Counter<u64>,
    query_ann_fallback_total: Counter<u64>,
    query_ann_fetch_errors_total: Counter<u64>,
    query_distributed_total: Counter<u64>,
    query_distributed_duration_seconds: Histogram<f64>,
    query_distributed_degradation_total: Counter<u64>,
    query_distributed_shard_outcome_total: Counter<u64>,
    query_distributed_shard_latency_ms: Histogram<f64>,
    query_distributed_successful_shard_ratio: Histogram<f64>,
    query_distributed_required_shard_ratio: Histogram<f64>,
}

fn app_metrics() -> &'static AppMetrics {
    APP_METRICS.get_or_init(|| {
        let meter = global::meter(METRICS_METER_NAME);
        AppMetrics {
            query_duration_seconds: meter
                .f64_histogram("tv_query_duration_seconds")
                .with_description("Vector query request duration in seconds")
                .build(),
            query_total: meter
                .u64_counter("tv_query_total")
                .with_description("Total vector query requests")
                .build(),
            upsert_request_duration_seconds: meter
                .f64_histogram("tv_upsert_request_duration_seconds")
                .with_description("Upsert request acceptance latency in seconds")
                .build(),
            upsert_accepted_total: meter
                .u64_counter("tv_upsert_accepted_total")
                .with_description("Total accepted upsert requests")
                .build(),
            upsert_applied_total: meter
                .u64_counter("tv_upsert_applied_total")
                .with_description("Total applied upsert operations")
                .build(),
            operation_apply_lag_seconds: meter
                .f64_histogram("tv_operation_apply_lag_seconds")
                .with_description("Lag between accepted_at and applied_at in seconds")
                .build(),
            queue_depth: meter
                .f64_gauge("tv_queue_depth")
                .with_description("Current queue depth")
                .build(),
            queue_claim_total: meter
                .u64_counter("tv_queue_claim_total")
                .with_description("Total claimed queue jobs")
                .build(),
            queue_ack_total: meter
                .u64_counter("tv_queue_ack_total")
                .with_description("Total acknowledged queue jobs")
                .build(),
            queue_requeue_total: meter
                .u64_counter("tv_queue_requeue_total")
                .with_description("Total stale queue jobs requeued by broker scans")
                .build(),
            worker_flush_applied_jobs_total: meter
                .u64_counter("tv_worker_flush_applied_jobs_total")
                .with_description("Total jobs applied by worker queue flushes")
                .build(),
            cache_hits_total: meter
                .u64_counter("tv_cache_hits_total")
                .with_description("Total cache hits")
                .build(),
            cache_misses_total: meter
                .u64_counter("tv_cache_misses_total")
                .with_description("Total cache misses")
                .build(),
            cache_evictions_total: meter
                .u64_counter("tv_cache_evictions_total")
                .with_description("Total cache evictions")
                .build(),
            cache_entries: meter
                .f64_gauge("tv_cache_entries")
                .with_description("Current cache entries")
                .build(),
            cache_bytes: meter
                .f64_gauge("tv_cache_bytes")
                .with_description("Current cache size in bytes")
                .build(),
            cache_capacity_entries: meter
                .f64_gauge("tv_cache_capacity_entries")
                .with_description("Configured cache capacity by entry count")
                .build(),
            cache_capacity_bytes: meter
                .f64_gauge("tv_cache_capacity_bytes")
                .with_description("Configured cache capacity by bytes")
                .build(),
            cache_utilization_ratio: meter
                .f64_gauge("tv_cache_utilization_ratio")
                .with_description("Cache bytes / cache capacity bytes")
                .build(),
            query_temperature_total: meter
                .u64_counter("tv_query_temperature_total")
                .with_description("Total queries by warm/cold cache temperature")
                .build(),
            query_namespace_load_duration_seconds: meter
                .f64_histogram("tv_query_namespace_load_duration_seconds")
                .with_description("Time spent loading namespace vectors from object storage")
                .build(),
            query_cache_fill_total: meter
                .u64_counter("tv_query_cache_fill_total")
                .with_description("Total query-time cache fills")
                .build(),
            query_ann_fallback_total: meter
                .u64_counter("tv_query_ann_fallback_total")
                .with_description("Total ANN fallbacks by reason")
                .build(),
            query_ann_fetch_errors_total: meter
                .u64_counter("tv_query_ann_fetch_errors_total")
                .with_description("Total ANN bucket fetch/decode errors")
                .build(),
            query_distributed_total: meter
                .u64_counter("tv_query_distributed_total")
                .with_description("Total distributed query requests")
                .build(),
            query_distributed_duration_seconds: meter
                .f64_histogram("tv_query_distributed_duration_seconds")
                .with_description("Distributed query request duration in seconds")
                .build(),
            query_distributed_degradation_total: meter
                .u64_counter("tv_query_distributed_degradation_total")
                .with_description("Total distributed query degradations by reason")
                .build(),
            query_distributed_shard_outcome_total: meter
                .u64_counter("tv_query_distributed_shard_outcome_total")
                .with_description("Total distributed shard outcomes by status")
                .build(),
            query_distributed_shard_latency_ms: meter
                .f64_histogram("tv_query_distributed_shard_latency_ms")
                .with_description("Distributed per-shard query latency in milliseconds")
                .build(),
            query_distributed_successful_shard_ratio: meter
                .f64_histogram("tv_query_distributed_successful_shard_ratio")
                .with_description("Distributed successful shard ratio per query")
                .build(),
            query_distributed_required_shard_ratio: meter
                .f64_histogram("tv_query_distributed_required_shard_ratio")
                .with_description("Distributed required-successful shard ratio per query")
                .build(),
        }
    })
}

fn service_attrs(service: &str) -> [KeyValue; 1] {
    [KeyValue::new("service", service.to_string())]
}

fn seconds_to_millis(seconds: f64) -> f64 {
    seconds * 1_000.0
}

fn cache_attrs(service: &str, node: &str, cache: &str, shard_scope: &str) -> [KeyValue; 4] {
    [
        KeyValue::new("service", service.to_string()),
        KeyValue::new("node", node.to_string()),
        KeyValue::new("cache", cache.to_string()),
        KeyValue::new("shard_scope", shard_scope.to_string()),
    ]
}

pub(crate) fn status_class(code: u16) -> &'static str {
    match code {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        _ => "5xx",
    }
}

pub(crate) fn record_query(
    service: &str,
    route: &str,
    strategy: &str,
    status_class: &str,
    duration_seconds: f64,
) {
    let attrs = [
        KeyValue::new("service", service.to_string()),
        KeyValue::new("route", route.to_string()),
        KeyValue::new("strategy", strategy.to_string()),
        KeyValue::new("status_class", status_class.to_string()),
    ];
    let metrics = app_metrics();
    metrics.query_total.add(1, &attrs);
    metrics
        .query_duration_seconds
        .record(seconds_to_millis(duration_seconds), &attrs);
}

pub(crate) fn record_upsert_request(
    service: &str,
    route: &str,
    status_class: &str,
    duration_seconds: f64,
) {
    let attrs = [
        KeyValue::new("service", service.to_string()),
        KeyValue::new("route", route.to_string()),
        KeyValue::new("status_class", status_class.to_string()),
    ];
    app_metrics()
        .upsert_request_duration_seconds
        .record(seconds_to_millis(duration_seconds), &attrs);
}

pub(crate) fn increment_upsert_accepted(service: &str) {
    app_metrics()
        .upsert_accepted_total
        .add(1, &service_attrs(service));
}

pub(crate) fn increment_upsert_applied(service: &str) {
    app_metrics()
        .upsert_applied_total
        .add(1, &service_attrs(service));
}

pub(crate) fn record_operation_apply_lag(service: &str, lag_seconds: f64) {
    if lag_seconds.is_sign_negative() {
        warn!(
            lag_seconds,
            "operation apply lag was negative; dropping metric"
        );
        return;
    }
    app_metrics()
        .operation_apply_lag_seconds
        .record(seconds_to_millis(lag_seconds), &service_attrs(service));
}

pub(crate) fn record_queue_depth(service: &str, queue_depth: usize) {
    app_metrics()
        .queue_depth
        .record(queue_depth as f64, &service_attrs(service));
}

pub(crate) fn increment_queue_claim(service: &str, claimed_jobs: u64) {
    if claimed_jobs == 0 {
        return;
    }
    app_metrics()
        .queue_claim_total
        .add(claimed_jobs, &service_attrs(service));
}

pub(crate) fn increment_queue_ack(service: &str, acked_jobs: u64) {
    if acked_jobs == 0 {
        return;
    }
    app_metrics()
        .queue_ack_total
        .add(acked_jobs, &service_attrs(service));
}

pub(crate) fn increment_queue_requeue(service: &str, requeued_jobs: u64) {
    if requeued_jobs == 0 {
        return;
    }
    app_metrics()
        .queue_requeue_total
        .add(requeued_jobs, &service_attrs(service));
}

pub(crate) fn increment_worker_flush_applied_jobs(service: &str, applied_jobs: u64) {
    if applied_jobs == 0 {
        return;
    }
    app_metrics()
        .worker_flush_applied_jobs_total
        .add(applied_jobs, &service_attrs(service));
}

pub(crate) fn increment_cache_hits(service: &str, cache: &str, count: u64) {
    increment_cache_hits_scoped(service, "unknown", cache, "cluster", count);
}

pub(crate) fn increment_cache_hits_scoped(
    service: &str,
    node: &str,
    cache: &str,
    shard_scope: &str,
    count: u64,
) {
    if count == 0 {
        return;
    }
    let attrs = cache_attrs(service, node, cache, shard_scope);
    app_metrics().cache_hits_total.add(count, &attrs);
}

pub(crate) fn increment_cache_misses(service: &str, cache: &str, count: u64) {
    increment_cache_misses_scoped(service, "unknown", cache, "cluster", count);
}

pub(crate) fn increment_cache_misses_scoped(
    service: &str,
    node: &str,
    cache: &str,
    shard_scope: &str,
    count: u64,
) {
    if count == 0 {
        return;
    }
    let attrs = cache_attrs(service, node, cache, shard_scope);
    app_metrics().cache_misses_total.add(count, &attrs);
}

pub(crate) fn increment_cache_evictions_scoped(
    service: &str,
    node: &str,
    cache: &str,
    shard_scope: &str,
    count: u64,
) {
    if count == 0 {
        return;
    }
    let attrs = cache_attrs(service, node, cache, shard_scope);
    app_metrics().cache_evictions_total.add(count, &attrs);
}

pub(crate) fn record_cache_snapshot_scoped(
    service: &str,
    node: &str,
    cache: &str,
    shard_scope: &str,
    entries: usize,
    bytes: usize,
    capacity_entries: usize,
    capacity_bytes: usize,
) {
    let attrs = cache_attrs(service, node, cache, shard_scope);
    let metrics = app_metrics();
    metrics.cache_entries.record(entries as f64, &attrs);
    metrics.cache_bytes.record(bytes as f64, &attrs);
    metrics
        .cache_capacity_entries
        .record(capacity_entries as f64, &attrs);
    metrics
        .cache_capacity_bytes
        .record(capacity_bytes as f64, &attrs);
    let utilization_ratio = if capacity_bytes > 0 {
        (bytes as f64) / (capacity_bytes as f64)
    } else if capacity_entries > 0 {
        (entries as f64) / (capacity_entries as f64)
    } else {
        0.0
    };
    metrics
        .cache_utilization_ratio
        .record(utilization_ratio, &attrs);
}

pub(crate) fn increment_query_temperature(service: &str, temperature: &str) {
    app_metrics().query_temperature_total.add(
        1,
        &[
            KeyValue::new("service", service.to_string()),
            KeyValue::new("temperature", temperature.to_string()),
        ],
    );
}

pub(crate) fn record_query_namespace_load_duration(service: &str, duration_seconds: f64) {
    app_metrics()
        .query_namespace_load_duration_seconds
        .record(seconds_to_millis(duration_seconds), &service_attrs(service));
}

pub(crate) fn increment_query_cache_fill(service: &str, cache: &str) {
    app_metrics().query_cache_fill_total.add(
        1,
        &[
            KeyValue::new("service", service.to_string()),
            KeyValue::new("cache", cache.to_string()),
        ],
    );
}

pub(crate) fn increment_query_ann_fetch_errors(service: &str, count: u64) {
    if count == 0 {
        return;
    }
    app_metrics()
        .query_ann_fetch_errors_total
        .add(count, &service_attrs(service));
}

pub(crate) fn increment_query_ann_fallback_reason(service: &str, reason: &str) {
    let normalized_reason = if reason.trim().is_empty() {
        "unknown"
    } else {
        reason
    };
    app_metrics().query_ann_fallback_total.add(
        1,
        &[
            KeyValue::new("service", service.to_string()),
            KeyValue::new("reason", normalized_reason.to_string()),
        ],
    );
}

pub(crate) fn increment_query_distributed(
    service: &str,
    node: &str,
    strategy: &str,
    degraded: bool,
    status_class: &str,
) {
    let degraded_label = if degraded { "true" } else { "false" };
    app_metrics().query_distributed_total.add(
        1,
        &[
            KeyValue::new("service", service.to_string()),
            KeyValue::new("node", node.to_string()),
            KeyValue::new("strategy", strategy.to_string()),
            KeyValue::new("degraded", degraded_label.to_string()),
            KeyValue::new("status_class", status_class.to_string()),
        ],
    );
}

pub(crate) fn record_query_distributed_duration(
    service: &str,
    node: &str,
    strategy: &str,
    degraded: bool,
    status_class: &str,
    duration_seconds: f64,
) {
    let degraded_label = if degraded { "true" } else { "false" };
    app_metrics().query_distributed_duration_seconds.record(
        seconds_to_millis(duration_seconds),
        &[
            KeyValue::new("service", service.to_string()),
            KeyValue::new("node", node.to_string()),
            KeyValue::new("strategy", strategy.to_string()),
            KeyValue::new("degraded", degraded_label.to_string()),
            KeyValue::new("status_class", status_class.to_string()),
        ],
    );
}

pub(crate) fn increment_query_distributed_degradation_reason(
    service: &str,
    node: &str,
    reason: &str,
) {
    let normalized_reason = if reason.trim().is_empty() {
        "unknown"
    } else {
        reason
    };
    app_metrics().query_distributed_degradation_total.add(
        1,
        &[
            KeyValue::new("service", service.to_string()),
            KeyValue::new("node", node.to_string()),
            KeyValue::new("reason", normalized_reason.to_string()),
        ],
    );
}

pub(crate) fn increment_query_distributed_shard_outcome(
    service: &str,
    node: &str,
    strategy: &str,
    status: &str,
) {
    let normalized_status = if status.trim().is_empty() {
        "unknown"
    } else {
        status
    };
    app_metrics().query_distributed_shard_outcome_total.add(
        1,
        &[
            KeyValue::new("service", service.to_string()),
            KeyValue::new("node", node.to_string()),
            KeyValue::new("strategy", strategy.to_string()),
            KeyValue::new("status", normalized_status.to_string()),
        ],
    );
}

pub(crate) fn record_query_distributed_shard_latency_ms(
    service: &str,
    node: &str,
    strategy: &str,
    status: &str,
    latency_ms: f64,
) {
    if !latency_ms.is_finite() || latency_ms <= 0.0 {
        return;
    }
    let normalized_status = if status.trim().is_empty() {
        "unknown"
    } else {
        status
    };
    app_metrics().query_distributed_shard_latency_ms.record(
        latency_ms,
        &[
            KeyValue::new("service", service.to_string()),
            KeyValue::new("node", node.to_string()),
            KeyValue::new("strategy", strategy.to_string()),
            KeyValue::new("status", normalized_status.to_string()),
        ],
    );
}

pub(crate) fn record_query_distributed_shard_ratios(
    service: &str,
    node: &str,
    strategy: &str,
    degraded: bool,
    planned_shards: u32,
    successful_shards: u32,
    required_successful_shards: u32,
) {
    if planned_shards == 0 {
        return;
    }
    let degraded_label = if degraded { "true" } else { "false" };
    let planned = planned_shards as f64;
    let successful_ratio = (successful_shards as f64 / planned).clamp(0.0, 1.0);
    let required_ratio = (required_successful_shards as f64 / planned).clamp(0.0, 1.0);
    let attrs = [
        KeyValue::new("service", service.to_string()),
        KeyValue::new("node", node.to_string()),
        KeyValue::new("strategy", strategy.to_string()),
        KeyValue::new("degraded", degraded_label.to_string()),
    ];
    let metrics = app_metrics();
    metrics
        .query_distributed_successful_shard_ratio
        .record(successful_ratio, &attrs);
    metrics
        .query_distributed_required_shard_ratio
        .record(required_ratio, &attrs);
}
