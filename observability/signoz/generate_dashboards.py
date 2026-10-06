# /// script
# dependencies = [
#   "python-dotenv",
# ]
# ///

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

OUTPUT_DIR = Path(__file__).resolve().parent


@dataclass(frozen=True)
class MetricPanel:
    title: str
    query: str
    y_axis_unit: str
    panel_type: str = "graph"


@dataclass(frozen=True)
class DashboardConfig:
    slug: str
    title: str
    description: str
    kpis: list[MetricPanel]
    graphs: list[MetricPanel]
    trace_services: list[str]
    log_services: list[str]


def stable_id(seed: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"turbo-vector-dashboard::{seed}"))


def base_query_stub(data_source: str) -> dict:
    return {
        "aggregateAttribute": {
            "dataType": "",
            "id": "------false",
            "isColumn": False,
            "isJSON": False,
            "key": "",
            "type": "",
        },
        "aggregateOperator": "avg",
        "dataSource": data_source,
        "disabled": False,
        "expression": "A",
        "filters": {"items": [], "op": "AND"},
        "functions": [],
        "groupBy": [],
        "having": [],
        "legend": "",
        "limit": None,
        "orderBy": [],
        "queryName": "A",
        "reduceTo": "sum",
        "spaceAggregation": "avg",
        "stepInterval": 60,
        "timeAggregation": "avg",
    }


def thresholds_for_title(title: str) -> list[dict]:
    thresholds = []
    normalized = title.lower()
    if (
        "health kpi" in normalized
        or "success rate" in normalized
        or "healthy query rate" in normalized
        or "ack/claim ratio" in normalized
    ):
        thresholds = [
            {"color": "red", "threshold": 95},
            {"color": "yellow", "threshold": 98},
            {"color": "green", "threshold": 99},
        ]
    elif "exact distributed latency p99" in normalized:
        thresholds = [
            {"color": "green", "threshold": 150},
            {"color": "yellow", "threshold": 170},
            {"color": "red", "threshold": 220},
        ]
    elif "exact distributed latency p95" in normalized:
        thresholds = [
            {"color": "green", "threshold": 80},
            {"color": "yellow", "threshold": 100},
            {"color": "red", "threshold": 150},
        ]
    elif "ann/auto distributed latency p99" in normalized:
        thresholds = [
            {"color": "green", "threshold": 120},
            {"color": "yellow", "threshold": 160},
            {"color": "red", "threshold": 220},
        ]
    elif "ann/auto distributed latency p95" in normalized:
        thresholds = [
            {"color": "green", "threshold": 60},
            {"color": "yellow", "threshold": 90},
            {"color": "red", "threshold": 140},
        ]
    elif "degraded distributed latency p99" in normalized:
        thresholds = [
            {"color": "green", "threshold": 190},
            {"color": "yellow", "threshold": 220},
            {"color": "red", "threshold": 280},
        ]
    elif "latency p99" in normalized:
        thresholds = [
            {"color": "green", "threshold": 120},
            {"color": "yellow", "threshold": 160},
            {"color": "red", "threshold": 220},
        ]
    elif "latency p95" in normalized:
        thresholds = [
            {"color": "green", "threshold": 80},
            {"color": "yellow", "threshold": 120},
            {"color": "red", "threshold": 180},
        ]
    elif "distributed degraded query rate" in normalized:
        thresholds = [
            {"color": "green", "threshold": 0.5},
            {"color": "yellow", "threshold": 1.0},
            {"color": "red", "threshold": 2.0},
        ]
    elif "timeout reason rate" in normalized:
        thresholds = [
            {"color": "green", "threshold": 0.2},
            {"color": "yellow", "threshold": 0.5},
            {"color": "red", "threshold": 1.0},
        ]
    elif "dropped shard reason rate" in normalized:
        thresholds = [
            {"color": "green", "threshold": 0.0},
            {"color": "yellow", "threshold": 0.05},
            {"color": "red", "threshold": 0.2},
        ]
    elif "ann fallback rate" in normalized:
        thresholds = [
            {"color": "green", "threshold": 0.5},
            {"color": "yellow", "threshold": 1.0},
            {"color": "red", "threshold": 2.0},
        ]
    return thresholds


def promql_widget(seed: str, title: str, query: str, panel_type: str, y_axis_unit: str) -> dict:
    thresholds = thresholds_for_title(title)
    widget_id = stable_id(f"{seed}:widget:{title}")
    return {
        "bucketCount": 30,
        "bucketWidth": 0,
        "columnUnits": {},
        "description": "",
        "fillSpans": False,
        "id": widget_id,
        "isStacked": False,
        "mergeAllActiveQueries": False,
        "nullZeroValues": "zero",
        "opacity": "1",
        "panelTypes": panel_type,
        "query": {
            "builder": {"queryData": [base_query_stub("metrics")], "queryFormulas": []},
            "clickhouse_sql": [{"disabled": False, "legend": "", "name": "A", "query": ""}],
            "id": stable_id(f"{seed}:query:{title}"),
            "promql": [{"disabled": False, "legend": "", "name": "A", "query": query}],
            "queryType": "promql",
        },
        "selectedLogFields": [
            {"dataType": "string", "name": "body", "type": ""},
            {"dataType": "string", "name": "timestamp", "type": ""},
        ],
        "selectedTracesFields": [],
        "softMax": 0,
        "softMin": 0,
        "stackedBarChart": False,
        "thresholds": thresholds,
        "timePreferance": "GLOBAL_TIME",
        "title": title,
        "yAxisUnit": y_axis_unit,
    }


def service_label(service_name: str) -> str:
    compact = service_name.removeprefix("turbo-vector-")
    return compact.replace("-", " ").title()


def logs_widget(seed: str, title: str, service_name: str, group_by: list[dict], panel_type: str) -> dict:
    widget_id = stable_id(f"{seed}:widget:{title}")
    return {
        "bucketCount": 30,
        "bucketWidth": 0,
        "columnUnits": {"A": "short"} if panel_type == "table" else {},
        "description": "",
        "fillSpans": False,
        "id": widget_id,
        "isStacked": False,
        "mergeAllActiveQueries": False,
        "nullZeroValues": "zero",
        "opacity": "1",
        "panelTypes": panel_type,
        "query": {
            "builder": {
                "queryData": [
                    {
                        **base_query_stub("logs"),
                        "aggregateOperator": "count",
                        "spaceAggregation": "sum",
                        "timeAggregation": "rate",
                        "reduceTo": "avg",
                        "filters": {
                            "items": [
                                {
                                    "id": stable_id(f"{seed}:log-filter:service"),
                                    "key": {
                                        "dataType": "string",
                                        "id": "service.name--string--resource--false",
                                        "isColumn": False,
                                        "key": "service.name",
                                        "type": "resource",
                                    },
                                    "op": "=",
                                    "value": service_name,
                                }
                            ],
                            "op": "AND",
                        },
                        "groupBy": group_by,
                    }
                ],
                "queryFormulas": [],
            },
            "clickhouse_sql": [{"disabled": False, "legend": "Count", "name": "A", "query": ""}],
            "id": stable_id(f"{seed}:query:{title}"),
            "promql": [{"disabled": False, "legend": "", "name": "A", "query": ""}],
            "queryType": "builder",
        },
        "selectedLogFields": [
            {"dataType": "string", "name": "body", "type": ""},
            {"dataType": "string", "name": "timestamp", "type": ""},
        ],
        "selectedTracesFields": [],
        "softMax": 0,
        "softMin": 0,
        "stackedBarChart": False,
        "thresholds": [],
        "timePreferance": "GLOBAL_TIME",
        "title": title,
        "yAxisUnit": "short",
    }


def traces_widget(seed: str, title: str, service_name: str, panel_type: str, group_by: list[dict]) -> dict:
    widget_id = stable_id(f"{seed}:widget:{title}")
    return {
        "bucketCount": 30,
        "bucketWidth": 0,
        "columnUnits": {"A": "short"} if panel_type == "table" else {},
        "description": "",
        "fillSpans": False,
        "id": widget_id,
        "isStacked": False,
        "mergeAllActiveQueries": False,
        "nullZeroValues": "zero",
        "opacity": "1",
        "panelTypes": panel_type,
        "query": {
            "builder": {
                "queryData": [
                    {
                        **base_query_stub("traces"),
                        "aggregateOperator": "count",
                        "spaceAggregation": "sum",
                        "timeAggregation": "rate",
                        "reduceTo": "avg",
                        "filters": {
                            "items": [
                                {
                                    "id": stable_id(f"{seed}:trace-filter:service"),
                                    "key": {
                                        "dataType": "string",
                                        "id": "serviceName--string--tag--true",
                                        "isColumn": True,
                                        "isJSON": False,
                                        "key": "serviceName",
                                        "type": "tag",
                                    },
                                    "op": "=",
                                    "value": service_name,
                                }
                            ],
                            "op": "AND",
                        },
                        "groupBy": group_by,
                    }
                ],
                "queryFormulas": [],
            },
            "clickhouse_sql": [{"disabled": False, "legend": "Count", "name": "A", "query": ""}],
            "id": stable_id(f"{seed}:query:{title}"),
            "promql": [{"disabled": False, "legend": "", "name": "A", "query": ""}],
            "queryType": "builder",
        },
        "selectedLogFields": [
            {"dataType": "string", "name": "body", "type": ""},
            {"dataType": "string", "name": "timestamp", "type": ""},
        ],
        "selectedTracesFields": [
            {
                "dataType": "string",
                "id": "serviceName--string--tag--true",
                "isColumn": True,
                "isJSON": False,
                "key": "serviceName",
                "type": "tag",
            },
            {
                "dataType": "string",
                "id": "name--string--tag--true",
                "isColumn": True,
                "isJSON": False,
                "key": "name",
                "type": "tag",
            },
        ],
        "softMax": 0,
        "softMin": 0,
        "stackedBarChart": False,
        "thresholds": [],
        "timePreferance": "GLOBAL_TIME",
        "title": title,
        "yAxisUnit": "short",
    }


def build_layout(widgets: list[dict]) -> list[dict]:
    layout = []
    y = 0
    for index, widget in enumerate(widgets):
        wide = widget["panelTypes"] == "table"
        w = 12 if wide else 6
        h = 6
        x = 0 if wide or index % 2 == 0 else 6
        if wide and index % 2 == 1:
            y += 6
        layout.append({"h": h, "i": widget["id"], "moved": False, "static": False, "w": w, "x": x, "y": y})
        if wide or x == 6:
            y += 6
    return layout


def dashboard_payload(config: DashboardConfig) -> dict:
    seed = config.slug
    widgets = [
        promql_widget(
            seed,
            panel.title,
            panel.query,
            panel.panel_type,
            panel.y_axis_unit,
        )
        for panel in config.kpis
    ]
    widgets.extend(
        promql_widget(
            seed,
            panel.title,
            panel.query,
            panel.panel_type,
            panel.y_axis_unit,
        )
        for panel in config.graphs
    )
    for trace_service in config.trace_services:
        label = service_label(trace_service)
        widgets.extend(
            [
                traces_widget(
                    seed,
                    f"{label} Trace Throughput",
                    trace_service,
                    "value",
                    [],
                ),
                traces_widget(
                    seed,
                    f"{label} Top Trace Operations",
                    trace_service,
                    "table",
                    [{"dataType": "string", "id": "name--string--tag--true", "isColumn": True, "isJSON": False, "key": "name", "type": "tag"}],
                ),
            ]
        )
    for log_service in config.log_services:
        label = service_label(log_service)
        widgets.extend(
            [
                logs_widget(seed, f"{label} Log Throughput", log_service, [], "value"),
                logs_widget(
                    seed,
                    f"{label} Logs by Severity",
                    log_service,
                    [{"dataType": "string", "id": "severity_text--string----true", "isColumn": True, "isJSON": False, "key": "severity_text", "type": ""}],
                    "table",
                ),
            ]
        )
    return {
        "description": config.description,
        "layout": build_layout(widgets),
        "panelMap": {},
        "tags": ["turbo-vector", config.slug],
        "title": config.title,
        "uploadedGrafana": False,
        "variables": {},
        "version": "v4",
        "widgets": widgets,
    }


def write_dashboard(filename: str, config: DashboardConfig) -> None:
    path = OUTPUT_DIR / filename
    payload = dashboard_payload(config)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")


def main() -> None:
    global_retrieval = DashboardConfig(
        slug="global-retrieval",
        title="Turbo Vector Global Retrieval - Dashboard",
        description="Global retrieval health for throughput, latency, success, and distributed exposure.",
        kpis=[
            MetricPanel("Query Throughput (ops/s)", 'sum(rate(tv_query_total{service="turbo-vector-api"}[5m]))', "ops", "graph"),
            MetricPanel(
                "Query Latency p50/p90/p99 (ms)",
                'label_replace(histogram_quantile(0.50, sum by(le,service) (rate({__name__="tv_query_duration_seconds.bucket",service="turbo-vector-api"}[5m]))), "percentile", "p50", "service", ".*") or label_replace(histogram_quantile(0.90, sum by(le,service) (rate({__name__="tv_query_duration_seconds.bucket",service="turbo-vector-api"}[5m]))), "percentile", "p90", "service", ".*") or label_replace(histogram_quantile(0.99, sum by(le,service) (rate({__name__="tv_query_duration_seconds.bucket",service="turbo-vector-api"}[5m]))), "percentile", "p99", "service", ".*")',
                "ms",
                "graph",
            ),
            MetricPanel(
                "Query Success Rate (%)",
                '100 * (1 - (sum(rate(tv_query_total{service="turbo-vector-api",status_class=~"4xx|5xx"}[5m])) / clamp_min(sum(rate(tv_query_total{service="turbo-vector-api"}[5m])), 0.0001)))',
                "percent",
                "graph",
            ),
        ],
        graphs=[
            MetricPanel("Query RPS by Strategy", 'sum by(strategy) (rate(tv_query_total{service="turbo-vector-api"}[5m]))', "ops"),
            MetricPanel("Query RPS by Status Class", 'sum by(status_class) (rate(tv_query_total{service="turbo-vector-api"}[5m]))', "ops"),
            MetricPanel("Distributed Traffic Share (%)", '100 * sum(rate(tv_query_distributed_total{service="turbo-vector-api"}[5m])) / clamp_min(sum(rate(tv_query_total{service="turbo-vector-api"}[5m])), 0.0001)', "percent"),
            MetricPanel("Distributed Degraded Query Rate (%)", '100 * sum(rate(tv_query_distributed_total{service="turbo-vector-api",degraded="true"}[5m])) / clamp_min(sum(rate(tv_query_distributed_total{service="turbo-vector-api"}[5m])), 0.0001)', "percent"),
            MetricPanel("Distributed Query Latency p95 (ms)", 'histogram_quantile(0.95, sum by(le) (rate({__name__="tv_query_distributed_duration_seconds.bucket",service="turbo-vector-api"}[5m])))', "ms"),
            MetricPanel("ANN Fallback Rate (%)", '100 * sum(rate(tv_query_ann_fallback_total{service="turbo-vector-api"}[5m])) / clamp_min(sum(rate(tv_query_total{service="turbo-vector-api"}[5m])), 0.0001)', "percent"),
            MetricPanel("ANN Fallback by Reason", 'sum by(reason) (rate(tv_query_ann_fallback_total{service="turbo-vector-api"}[5m]))', "ops"),
            MetricPanel("Namespace Load Latency (ms)", 'sum(rate({__name__="tv_query_namespace_load_duration_seconds.sum",service="turbo-vector-api"}[5m])) / clamp_min(sum(rate({__name__="tv_query_namespace_load_duration_seconds.count",service="turbo-vector-api"}[5m])), 0.0001)', "ms"),
            MetricPanel("Namespace Cache Hit Ratio (%)", '100 * sum(rate(tv_cache_hits_total{service="turbo-vector-api",cache="namespace_vectors"}[5m])) / clamp_min(sum(rate(tv_cache_hits_total{service="turbo-vector-api",cache="namespace_vectors"}[5m])) + sum(rate(tv_cache_misses_total{service="turbo-vector-api",cache="namespace_vectors"}[5m])), 0.0001)', "percent"),
            MetricPanel("API Cache Utilization by Cache (%)", '100 * max by(cache) (tv_cache_utilization_ratio{service="turbo-vector-api"})', "percent"),
        ],
        trace_services=["turbo-vector-api"],
        log_services=["turbo-vector-api"],
    )

    distributed_retrieval = DashboardConfig(
        slug="distributed-retrieval",
        title="Turbo Vector Distributed Retrieval - Dashboard",
        description="Distributed retrieval operations: node/shard fanout behavior, degradation reasons, and tail latency.",
        kpis=[
            MetricPanel("Distributed Throughput (ops/s)", 'sum(rate(tv_query_distributed_total{service="turbo-vector-api"}[5m]))', "ops", "graph"),
            MetricPanel(
                "Distributed Latency p95 (ms)",
                'histogram_quantile(0.95, sum by(le) (rate({__name__="tv_query_distributed_duration_seconds.bucket",service="turbo-vector-api"}[5m])))',
                "ms",
                "graph",
            ),
            MetricPanel(
                "Distributed Latency p99 (ms)",
                'histogram_quantile(0.99, sum by(le) (rate({__name__="tv_query_distributed_duration_seconds.bucket",service="turbo-vector-api"}[5m])))',
                "ms",
                "graph",
            ),
            MetricPanel(
                "Distributed Healthy Query Rate (%)",
                '100 * (1 - (sum(rate(tv_query_distributed_total{service="turbo-vector-api",degraded="true"}[5m])) / clamp_min(sum(rate(tv_query_distributed_total{service="turbo-vector-api"}[5m])), 0.0001)))',
                "percent",
                "graph",
            ),
        ],
        graphs=[
            MetricPanel("Distributed Query RPS by Node", 'sum by(node) (rate(tv_query_distributed_total{service="turbo-vector-api"}[5m]))', "ops"),
            MetricPanel("Distributed Query RPS by Strategy", 'sum by(strategy) (rate(tv_query_distributed_total{service="turbo-vector-api"}[5m]))', "ops"),
            MetricPanel("Distributed Query RPS by Status Class", 'sum by(status_class) (rate(tv_query_distributed_total{service="turbo-vector-api"}[5m]))', "ops"),
            MetricPanel("Distributed Degraded Query Rate (%)", '100 * sum(rate(tv_query_distributed_total{service="turbo-vector-api",degraded="true"}[5m])) / clamp_min(sum(rate(tv_query_distributed_total{service="turbo-vector-api"}[5m])), 0.0001)', "percent"),
            MetricPanel(
                "Distributed Degraded Query Rate by Node (%)",
                '100 * sum by(node) (rate(tv_query_distributed_total{service="turbo-vector-api",degraded="true"}[5m])) / clamp_min(sum by(node) (rate(tv_query_distributed_total{service="turbo-vector-api"}[5m])), 0.0001)',
                "percent",
            ),
            MetricPanel("Degradation Reasons by Type", 'sum by(reason) (rate(tv_query_distributed_degradation_total{service="turbo-vector-api"}[5m]))', "ops"),
            MetricPanel("Timeout Reason Rate (%)", '100 * sum(rate(tv_query_distributed_degradation_total{service="turbo-vector-api",reason="slow_shard_timeout"}[5m])) / clamp_min(sum(rate(tv_query_distributed_total{service="turbo-vector-api"}[5m])), 0.0001)', "percent"),
            MetricPanel("Dropped Shard Reason Rate (%)", '100 * sum(rate(tv_query_distributed_degradation_total{service="turbo-vector-api",reason="dropped_shard"}[5m])) / clamp_min(sum(rate(tv_query_distributed_total{service="turbo-vector-api"}[5m])), 0.0001)', "percent"),
            MetricPanel("Shard Outcomes by Node and Status", 'sum by(node,status) (rate(tv_query_distributed_shard_outcome_total{service="turbo-vector-api"}[5m]))', "ops"),
            MetricPanel("Shard Latency p95 by Node and Status (ms)", 'histogram_quantile(0.95, sum by(le,node,status) (rate({__name__="tv_query_distributed_shard_latency_ms.bucket",service="turbo-vector-api"}[5m])))', "ms"),
            MetricPanel("Exact Distributed Latency p95 (ms)", 'histogram_quantile(0.95, sum by(le) (rate({__name__="tv_query_distributed_duration_seconds.bucket",service="turbo-vector-api",strategy="exact"}[5m])))', "ms"),
            MetricPanel("Exact Distributed Latency p99 (ms)", 'histogram_quantile(0.99, sum by(le) (rate({__name__="tv_query_distributed_duration_seconds.bucket",service="turbo-vector-api",strategy="exact"}[5m])))', "ms"),
            MetricPanel("ANN/Auto Distributed Latency p95 (ms)", 'histogram_quantile(0.95, sum by(le) (rate({__name__="tv_query_distributed_duration_seconds.bucket",service="turbo-vector-api",strategy=~"ann|auto"}[5m])))', "ms"),
            MetricPanel("ANN/Auto Distributed Latency p99 (ms)", 'histogram_quantile(0.99, sum by(le) (rate({__name__="tv_query_distributed_duration_seconds.bucket",service="turbo-vector-api",strategy=~"ann|auto"}[5m])))', "ms"),
            MetricPanel("Degraded Distributed Latency p99 (ms)", 'histogram_quantile(0.99, sum by(le) (rate({__name__="tv_query_distributed_duration_seconds.bucket",service="turbo-vector-api",degraded="true"}[5m])))', "ms"),
            MetricPanel("Successful Shard Ratio p95 (%)", '100 * histogram_quantile(0.95, sum by(le) (rate({__name__="tv_query_distributed_successful_shard_ratio.bucket",service="turbo-vector-api"}[5m])))', "percent"),
            MetricPanel("Required Shard Ratio p95 (%)", '100 * histogram_quantile(0.95, sum by(le) (rate({__name__="tv_query_distributed_required_shard_ratio.bucket",service="turbo-vector-api"}[5m])))', "percent"),
            MetricPanel(
                "Cache Hit Ratio by Node and Scope (%)",
                '100 * sum by(node, shard_scope, cache) (rate(tv_cache_hits_total{service="turbo-vector-api"}[5m])) / clamp_min(sum by(node, shard_scope, cache) (rate(tv_cache_hits_total{service="turbo-vector-api"}[5m])) + sum by(node, shard_scope, cache) (rate(tv_cache_misses_total{service="turbo-vector-api"}[5m])), 0.0001)',
                "percent",
            ),
            MetricPanel("Cache Utilization by Node Cache (%)", '100 * max by(node, cache) (tv_cache_utilization_ratio{service="turbo-vector-api"})', "percent"),
        ],
        trace_services=["turbo-vector-api"],
        log_services=["turbo-vector-api"],
    )

    ingest_runtime = DashboardConfig(
        slug="ingest-runtime",
        title="Turbo Vector Ingest Runtime - Dashboard",
        description="Ingest pipeline health for broker and worker queue pressure, apply lag, and cache pressure.",
        kpis=[
            MetricPanel("Upsert Accepted Throughput (ops/s)", 'sum(rate(tv_upsert_accepted_total{service="turbo-vector-api"}[5m]))', "ops", "graph"),
            MetricPanel(
                "Apply Lag p95 (ms)",
                'histogram_quantile(0.95, sum by(le) (rate({__name__="tv_operation_apply_lag_seconds.bucket",service="turbo-vector-worker"}[5m])))',
                "ms",
                "graph",
            ),
            MetricPanel(
                "Queue Ack/Claim Ratio (%)",
                '100 * sum(rate(tv_queue_ack_total{service="turbo-vector-broker"}[5m])) / clamp_min(sum(rate(tv_queue_claim_total{service="turbo-vector-broker"}[5m])), 0.0001)',
                "percent",
                "graph",
            ),
        ],
        graphs=[
            MetricPanel("Queue Claim Rate", 'sum(rate(tv_queue_claim_total{service="turbo-vector-broker"}[5m]))', "ops"),
            MetricPanel("Queue Ack Rate", 'sum(rate(tv_queue_ack_total{service="turbo-vector-broker"}[5m]))', "ops"),
            MetricPanel("Queue Requeue Rate", 'sum(rate(tv_queue_requeue_total{service="turbo-vector-broker"}[5m]))', "ops"),
            MetricPanel("Ack to Claim Ratio (%)", '100 * sum(rate(tv_queue_ack_total{service="turbo-vector-broker"}[5m])) / clamp_min(sum(rate(tv_queue_claim_total{service="turbo-vector-broker"}[5m])), 0.0001)', "percent"),
            MetricPanel("Requeue to Claim Ratio (%)", '100 * sum(rate(tv_queue_requeue_total{service="turbo-vector-broker"}[5m])) / clamp_min(sum(rate(tv_queue_claim_total{service="turbo-vector-broker"}[5m])), 0.0001)', "percent"),
            MetricPanel("Worker Flush Applied Jobs Rate", 'sum(rate(tv_worker_flush_applied_jobs_total{service="turbo-vector-worker"}[5m]))', "ops"),
            MetricPanel("Upsert Applied Rate", 'sum(rate(tv_upsert_applied_total{service="turbo-vector-worker"}[5m]))', "ops"),
            MetricPanel("Queue Depth", 'max(tv_queue_depth{service="turbo-vector-worker"})', "short"),
            MetricPanel("Apply Lag p95 (ms)", 'histogram_quantile(0.95, sum by(le) (rate({__name__="tv_operation_apply_lag_seconds.bucket",service="turbo-vector-worker"}[5m])))', "ms"),
            MetricPanel("Cache Evictions by Cache", 'sum by(cache) (rate(tv_cache_evictions_total{service="turbo-vector-worker"}[5m]))', "ops"),
            MetricPanel("Worker Cache Utilization by Cache (%)", '100 * max by(cache) (tv_cache_utilization_ratio{service="turbo-vector-worker"})', "percent"),
            MetricPanel("API Namespace Cache Hit Ratio (%)", '100 * sum(rate(tv_cache_hits_total{service="turbo-vector-api",cache="namespace_vectors"}[5m])) / clamp_min(sum(rate(tv_cache_hits_total{service="turbo-vector-api",cache="namespace_vectors"}[5m])) + sum(rate(tv_cache_misses_total{service="turbo-vector-api",cache="namespace_vectors"}[5m])), 0.0001)', "percent"),
        ],
        trace_services=["turbo-vector-broker", "turbo-vector-worker"],
        log_services=["turbo-vector-broker", "turbo-vector-worker"],
    )

    write_dashboard("dashboard-api.json", global_retrieval)
    write_dashboard("dashboard-broker.json", distributed_retrieval)
    write_dashboard("dashboard-worker.json", ingest_runtime)


if __name__ == "__main__":
    main()
