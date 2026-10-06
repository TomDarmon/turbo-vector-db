from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LIMITS_SNAPSHOT_START = "<!-- LIMITS_SNAPSHOT_START -->"
LIMITS_SNAPSHOT_END = "<!-- LIMITS_SNAPSHOT_END -->"
SNAPSHOT_SCHEMA_VERSION = "2.2"


def _nested(data: dict[str, Any], *path: str, default: Any = None) -> Any:
    cursor: Any = data
    for key in path:
        if not isinstance(cursor, dict):
            return default
        cursor = cursor.get(key)
        if cursor is None:
            return default
    return cursor


def _to_float(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return bool(value)


def build_limits_snapshot(
    report: dict[str, Any],
    source_report_path: str | None = None,
) -> dict[str, Any]:
    deployment_mode = _nested(report, "deployment", "mode", default="unknown")
    config = report.get("config", {}) if isinstance(report.get("config"), dict) else {}
    phases = report.get("phases", {}) if isinstance(report.get("phases"), dict) else {}
    indexing = phases.get("indexing", {}) if isinstance(phases.get("indexing"), dict) else {}
    query_warm = (
        phases.get("query_warm", {}) if isinstance(phases.get("query_warm"), dict) else {}
    )
    upsert = phases.get("upsert", {}) if isinstance(phases.get("upsert"), dict) else {}
    resources = (
        report.get("resource_monitoring", {}).get("services", {})
        if isinstance(report.get("resource_monitoring"), dict)
        else {}
    )
    consistency = (
        report.get("consistency", {}) if isinstance(report.get("consistency"), dict) else {}
    )
    warnings = report.get("warnings", [])
    if not isinstance(warnings, list):
        warnings = [str(warnings)]

    strict_checks_passed = _to_bool(
        consistency.get("vector_count_matches_expected"), default=False
    ) and not _to_bool(indexing.get("timed_out"), default=False)

    snapshot: dict[str, Any] = {
        "snapshot_schema_version": SNAPSHOT_SCHEMA_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_started_utc": report.get("run_started_utc"),
        "run_finished_utc": report.get("run_finished_utc"),
        "run_duration_seconds": _to_float(report.get("run_duration_seconds"), default=0.0),
        "source_report_path": source_report_path,
        "deployment_mode": deployment_mode,
        "strict_checks_passed": strict_checks_passed,
        "workload": {
            "profile": config.get("profile"),
            "total_docs": config.get("total_docs"),
            "dimension": config.get("dimension"),
            "upsert_batch_size": config.get("upsert_batch_size"),
            "upsert_concurrency": config.get("upsert_concurrency"),
            "query_mode": config.get("query_mode"),
            "query_count": config.get("query_count"),
            "query_duration_seconds": config.get("query_duration_seconds"),
            "query_concurrency": config.get("query_concurrency"),
            "query_top_k": config.get("query_top_k"),
        },
        "throughput": {
            "upsert_requests_per_second": _to_float(
                upsert.get("throughput_ops_per_second"), default=0.0
            ),
            "upsert_vectors_per_second": _to_float(
                upsert.get("throughput_vectors_per_second"), default=0.0
            ),
            "query_requests_per_second": _to_float(
                query_warm.get("throughput_ops_per_second"), default=0.0
            ),
        },
        "indexing": {
            "time_to_expected_vector_count_seconds": _to_float(
                indexing.get("time_to_expected_vector_count_seconds"), default=0.0
            ),
            "time_to_ann_ready_seconds": _to_float(
                indexing.get("time_to_ann_ready_seconds"), default=0.0
            ),
            "time_to_all_operations_applied_seconds": _to_float(
                indexing.get("time_to_all_operations_applied_seconds"), default=0.0
            ),
            "queue_pending_operations_peak": indexing.get("queue_pending_operations_peak"),
            "operation_apply_lag_p95_seconds": _to_float(
                _nested(indexing, "operation_apply_lag_seconds", "p95"), default=0.0
            ),
            "generation_velocity_mean_per_second": _to_float(
                _nested(indexing, "generation_velocity_per_second", "mean"), default=0.0
            ),
            "generation_velocity_p95_per_second": _to_float(
                _nested(indexing, "generation_velocity_per_second", "p95"), default=0.0
            ),
            "indexing_timed_out": _to_bool(indexing.get("timed_out"), default=False),
        },
        "resources": {
            "api_cpu_p95_percent": _to_float(
                _nested(resources, "api", "cpu_percent", "p95"), default=0.0
            ),
            "api_mem_p95_mib": _to_float(
                _nested(resources, "api", "memory_usage_mib", "p95"), default=0.0
            ),
            "worker_cpu_p95_percent": _to_float(
                _nested(resources, "upsert-worker", "cpu_percent", "p95"), default=0.0
            ),
            "worker_mem_p95_mib": _to_float(
                _nested(resources, "upsert-worker", "memory_usage_mib", "p95"), default=0.0
            ),
        },
        "consistency": {
            "expected_vector_count": consistency.get("expected_vector_count"),
            "actual_vector_count": consistency.get("actual_vector_count"),
            "vector_count_matches_expected": _to_bool(
                consistency.get("vector_count_matches_expected"), default=False
            ),
        },
        "warnings": warnings,
    }

    recall = phases.get("recall_validation")
    if isinstance(recall, dict):
        snapshot["recall"] = {
            "sample_count": recall.get("sample_count_succeeded"),
            "strategy": recall.get("strategy"),
            "baseline_strategy": recall.get("baseline_strategy"),
            "mean_recall_at_k": _to_float(_nested(recall, "recall_at_k", "mean")),
            "p95_recall_at_k": _to_float(_nested(recall, "recall_at_k", "p95")),
            "min_recall_at_k": _to_float(_nested(recall, "recall_at_k", "min")),
        }

    ann_observability = query_warm.get("ann_observability")
    if isinstance(ann_observability, dict):
        snapshot["ann"] = {
            "ann_used_rate": _to_float(ann_observability.get("ann_used_rate"), default=0.0),
            "ann_fallback_rate": _to_float(
                ann_observability.get("ann_fallback_rate"), default=0.0
            ),
            "ann_fallback_count_total": ann_observability.get("ann_fallback_count_total"),
            "ann_fetch_errors_total": ann_observability.get("ann_fetch_errors_total"),
            "mean_buckets_probed": _to_float(
                ann_observability.get("mean_buckets_probed"), default=0.0
            ),
            "mean_candidates_scored": _to_float(
                ann_observability.get("mean_candidates_scored"), default=0.0
            ),
            "fallback_reasons": ann_observability.get("fallback_reasons", {}),
        }

    return snapshot


def _render_snapshot_markdown(snapshot: dict[str, Any]) -> str:
    workload = snapshot.get("workload", {})
    throughput = snapshot.get("throughput", {})
    indexing = snapshot.get("indexing", {})
    resources = snapshot.get("resources", {})
    consistency = snapshot.get("consistency", {})

    rows = [
        ("profile", workload.get("profile")),
        ("total_docs", workload.get("total_docs")),
        ("dimension", workload.get("dimension")),
        ("query_count", workload.get("query_count")),
        ("query_top_k", workload.get("query_top_k")),
        ("query_req_per_s", throughput.get("query_requests_per_second")),
        ("upsert_req_per_s", throughput.get("upsert_requests_per_second")),
        ("indexing_time_to_ann_ready_s", indexing.get("time_to_ann_ready_seconds")),
        ("queue_pending_peak_ops", indexing.get("queue_pending_operations_peak")),
        ("generation_velocity_mean_per_s", indexing.get("generation_velocity_mean_per_second")),
        ("api_cpu_p95_percent", resources.get("api_cpu_p95_percent")),
        ("worker_mem_p95_mib", resources.get("worker_mem_p95_mib")),
        ("expected_vector_count", consistency.get("expected_vector_count")),
        ("actual_vector_count", consistency.get("actual_vector_count")),
        ("vector_count_matches_expected", consistency.get("vector_count_matches_expected")),
    ]

    warnings = snapshot.get("warnings", [])
    if not isinstance(warnings, list):
        warnings = [str(warnings)]

    lines = [
        "## Latest benchmark snapshot (auto-managed)",
        "",
        f"- generated_at_utc: `{snapshot.get('generated_at_utc')}`",
        f"- run_started_utc: `{snapshot.get('run_started_utc')}`",
        f"- run_finished_utc: `{snapshot.get('run_finished_utc')}`",
        f"- run_duration_seconds: `{snapshot.get('run_duration_seconds')}`",
        f"- source_report_path: `{snapshot.get('source_report_path')}`",
        f"- deployment_mode: `{snapshot.get('deployment_mode')}`",
        f"- strict_checks: `{'pass' if snapshot.get('strict_checks_passed') else 'fail'}`",
        "",
        "| metric | value |",
        "| --- | --- |",
    ]
    lines.extend(f"| {metric} | {value} |" for metric, value in rows)
    lines.extend(["", "### Warnings"])
    if warnings:
        lines.extend(f"- {warning}" for warning in warnings)
    else:
        lines.append("- none")
    return "\n".join(lines)


def update_limits_registry_markdown(path: str, snapshot: dict[str, Any]) -> None:
    limits_path = Path(path)
    current = limits_path.read_text(encoding="utf-8") if limits_path.exists() else ""
    block = (
        f"{LIMITS_SNAPSHOT_START}\n"
        f"{_render_snapshot_markdown(snapshot)}\n"
        f"{LIMITS_SNAPSHOT_END}"
    )

    start_index = current.find(LIMITS_SNAPSHOT_START)
    end_index = current.find(LIMITS_SNAPSHOT_END)
    if start_index != -1 and end_index != -1 and end_index > start_index:
        end_index = end_index + len(LIMITS_SNAPSHOT_END)
        updated = f"{current[:start_index].rstrip()}\n\n{block}\n{current[end_index:].lstrip()}"
    elif current.strip():
        updated = f"{current.rstrip()}\n\n{block}\n"
    else:
        updated = f"{block}\n"
    limits_path.write_text(updated, encoding="utf-8")
