from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from benchmarks.limits_snapshot import (
    LIMITS_SNAPSHOT_END,
    LIMITS_SNAPSHOT_START,
    build_limits_snapshot,
    update_limits_registry_markdown,
)


def sample_report() -> dict[str, object]:
    return {
        "run_started_utc": "2026-02-15T00:00:00Z",
        "run_finished_utc": "2026-02-15T00:10:00Z",
        "run_duration_seconds": 600.0,
        "backend": "turbo",
        "collection": "bench-123",
        "namespace": "benchmark",
        "deployment": {
            "mode": "single_node_split",
            "compose_services": {"api": "api", "worker": "upsert-worker"},
            "runtime": {"wal_queue_background_drain_enabled": False},
        },
        "config": {
            "profile": "soak_5m",
            "query_strategy": "ann",
            "total_docs": 1000,
            "dimension": 128,
            "upsert_batch_size": 100,
            "upsert_concurrency": 8,
            "query_mode": "duration",
            "query_duration_seconds": 300,
            "query_count": 500,
            "query_concurrency": 16,
            "query_top_k": 10,
        },
        "phases": {
            "upsert": {
                "failed": 0,
                "throughput_ops_per_second": 45.5,
                "throughput_vectors_per_second": 4550.0,
                "latency_ms": {"p95": 20.0, "p99": 25.0},
            },
            "query_warm": {
                "failed": 0,
                "throughput_ops_per_second": 300.0,
                "latency_ms": {"p95": 35.0, "p99": 44.0},
            },
            "indexing": {
                "timed_out": False,
                "time_to_expected_vector_count_seconds": 1.4,
                "time_to_ann_ready_seconds": 2.1,
                "time_to_all_operations_applied_seconds": 2.4,
                "queue_pending_operations_peak": 14,
                "operation_apply_lag_seconds": {"p95": 0.8},
                "generation_velocity_per_second": {"mean": 5.0, "p95": 7.5},
                "operation_progress": {"tracked_operations": 10, "applied_operations": 10},
            },
        },
        "resource_monitoring": {
            "services": {
                "api": {
                    "cpu_percent": {"p95": 42.0},
                    "memory_usage_mib": {"p95": 512.0},
                },
                "upsert-worker": {
                    "cpu_percent": {"p95": 33.0},
                    "memory_usage_mib": {"p95": 384.0},
                },
            }
        },
        "stats": {"generation": 12, "segments": 7},
        "consistency": {
            "expected_vector_count": 1000,
            "actual_vector_count": 1000,
            "vector_count_matches_expected": True,
        },
        "warnings": [],
    }


class LimitsReportingTests(unittest.TestCase):
    def test_build_limits_snapshot_marks_strict_success(self) -> None:
        snapshot = build_limits_snapshot(
            sample_report(),
            source_report_path="../local/benchmarks/latest.json",
        )
        self.assertTrue(snapshot["strict_checks_passed"])
        self.assertEqual(snapshot["snapshot_schema_version"], "2.2")
        self.assertEqual(snapshot["deployment_mode"], "single_node_split")
        self.assertEqual(snapshot["workload"]["total_docs"], 1000)
        self.assertEqual(snapshot["throughput"]["query_requests_per_second"], 300.0)
        self.assertEqual(snapshot["indexing"]["time_to_ann_ready_seconds"], 2.1)
        self.assertEqual(snapshot["indexing"]["queue_pending_operations_peak"], 14)
        self.assertEqual(snapshot["indexing"]["operation_apply_lag_p95_seconds"], 0.8)
        self.assertEqual(snapshot["indexing"]["generation_velocity_mean_per_second"], 5.0)
        self.assertEqual(snapshot["resources"]["api_cpu_p95_percent"], 42.0)
        self.assertEqual(snapshot["resources"]["worker_mem_p95_mib"], 384.0)
        self.assertEqual(
            snapshot["consistency"]["vector_count_matches_expected"],
            True,
        )

    def test_build_limits_snapshot_marks_strict_failure_on_mismatch(self) -> None:
        report = sample_report()
        report["consistency"] = {
            "expected_vector_count": 1000,
            "actual_vector_count": 998,
            "vector_count_matches_expected": False,
        }
        snapshot = build_limits_snapshot(report)
        self.assertFalse(snapshot["strict_checks_passed"])

    def test_build_limits_snapshot_includes_recall_metrics_when_present(self) -> None:
        report = sample_report()
        report["phases"]["recall_validation"] = {  # type: ignore[index]
            "sample_count_succeeded": 100,
            "strategy": "ann",
            "baseline_strategy": "exact",
            "recall_at_k": {
                "mean": 0.93,
                "p95": 1.0,
                "min": 0.7,
            },
        }
        snapshot = build_limits_snapshot(report)
        self.assertEqual(snapshot["recall"]["sample_count"], 100)
        self.assertEqual(snapshot["recall"]["strategy"], "ann")
        self.assertEqual(snapshot["recall"]["baseline_strategy"], "exact")
        self.assertEqual(snapshot["recall"]["mean_recall_at_k"], 0.93)
        self.assertEqual(snapshot["recall"]["p95_recall_at_k"], 1.0)
        self.assertEqual(snapshot["recall"]["min_recall_at_k"], 0.7)

    def test_build_limits_snapshot_includes_ann_observability_when_present(self) -> None:
        report = sample_report()
        report["phases"]["query_warm"]["ann_observability"] = {  # type: ignore[index]
            "ann_used_rate": 0.9,
            "ann_fallback_rate": 0.1,
            "ann_fallback_count_total": 10,
            "ann_fetch_errors_total": 2,
            "mean_buckets_probed": 6.5,
            "mean_candidates_scored": 320.0,
            "fallback_reasons": {"bucket_fetch_error": 2},
        }
        snapshot = build_limits_snapshot(report)
        self.assertEqual(snapshot["ann"]["ann_used_rate"], 0.9)
        self.assertEqual(snapshot["ann"]["ann_fallback_rate"], 0.1)
        self.assertEqual(snapshot["ann"]["ann_fallback_count_total"], 10)
        self.assertEqual(snapshot["ann"]["ann_fetch_errors_total"], 2)
        self.assertEqual(snapshot["ann"]["mean_buckets_probed"], 6.5)
        self.assertEqual(snapshot["ann"]["mean_candidates_scored"], 320.0)
        self.assertEqual(snapshot["ann"]["fallback_reasons"], {"bucket_fetch_error": 2})

    def test_update_limits_registry_replaces_auto_managed_block(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            limits_path = Path(temp_dir) / "limits-registry.md"
            limits_path.write_text("# Limits registry\n\nManual section.\n", encoding="utf-8")

            snapshot = build_limits_snapshot(sample_report(), source_report_path="a.json")
            update_limits_registry_markdown(str(limits_path), snapshot)
            first_write = limits_path.read_text(encoding="utf-8")

            self.assertEqual(first_write.count(LIMITS_SNAPSHOT_START), 1)
            self.assertEqual(first_write.count(LIMITS_SNAPSHOT_END), 1)
            self.assertIn("Latest benchmark snapshot (auto-managed)", first_write)
            self.assertIn("| total_docs | 1000 |", first_write)

            report = sample_report()
            report["config"]["total_docs"] = 2000  # type: ignore[index]
            updated_snapshot = build_limits_snapshot(report, source_report_path="b.json")
            update_limits_registry_markdown(str(limits_path), updated_snapshot)
            second_write = limits_path.read_text(encoding="utf-8")

            self.assertEqual(second_write.count(LIMITS_SNAPSHOT_START), 1)
            self.assertIn("| total_docs | 2000 |", second_write)
            self.assertNotIn("| total_docs | 1000 |", second_write)


if __name__ == "__main__":
    unittest.main()
