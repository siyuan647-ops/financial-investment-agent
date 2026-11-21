"""Deterministic Step15 V0 release-gate calculations.

The module contains no network or database side effects so the release verdict can
be tested independently from the environment used to collect the evidence.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import ceil

REQUIRED_DOMAINS = (
    "stock_price",
    "k_line",
    "pe_pb",
    "revenue",
    "net_profit_parent",
    "balance_sheet",
    "cash_flow",
    "dividend",
    "industry",
)


@dataclass(frozen=True)
class Sample:
    sample_id: str
    ts_code: str
    selection_group: str
    list_date: str | None = None


def validate_samples(payload: dict) -> tuple[Sample, ...]:
    """Validate the frozen Step01 sample manifest without silently shrinking it."""
    rows = payload.get("samples")
    if not isinstance(rows, list) or len(rows) != 30:
        raise ValueError("step15_requires_exactly_30_samples")
    samples = tuple(
        Sample(
            sample_id=str(row["sample_id"]),
            ts_code=str(row["ts_code"]),
            selection_group=str(row["selection_group"]),
            list_date=None if row.get("list_date") is None else str(row["list_date"]),
        )
        for row in rows
    )
    if len({item.sample_id for item in samples}) != len(samples):
        raise ValueError("duplicate_sample_id")
    if len({item.ts_code for item in samples}) != len(samples):
        raise ValueError("duplicate_sample_security")
    if len({item.selection_group for item in samples}) < 4:
        raise ValueError("cross_industry_sample_required")
    return samples


def coverage_matrix(samples: tuple[Sample, ...], counts: dict[str, dict[str, int]]) -> dict:
    """Build a complete matrix; an absent count is a visible gap, never success."""
    securities = []
    totals = {domain: 0 for domain in REQUIRED_DOMAINS}
    for sample in samples:
        observed = counts.get(sample.ts_code, {})
        domains = {}
        for domain in REQUIRED_DOMAINS:
            count = max(0, int(observed.get(domain, 0)))
            available = count > 0
            domains[domain] = {"count": count, "available": available}
            totals[domain] += int(available)
        securities.append(
            {
                "sample_id": sample.sample_id,
                "ts_code": sample.ts_code,
                "selection_group": sample.selection_group,
                "domains": domains,
            }
        )
    return {
        "required_domains": list(REQUIRED_DOMAINS),
        "sample_count": len(samples),
        "industry_group_count": len({item.selection_group for item in samples}),
        "available_security_counts": totals,
        "securities": securities,
    }


def percentile(values: list[float], quantile: float) -> float | None:
    """Nearest-rank percentile, suitable for a release threshold without interpolation."""
    if not values:
        return None
    if not 0 < quantile <= 1:
        raise ValueError("invalid_quantile")
    ordered = sorted(values)
    return ordered[max(0, ceil(len(ordered) * quantile) - 1)]


def expansion_batches(samples: tuple[Sample, ...], size: int = 3) -> list[dict]:
    if size < 1 or size > 3:
        raise ValueError("batch_size_must_match_domain_flow_budget")
    return [
        {
            "batch": index // size + 1,
            "codes": [item.ts_code for item in samples[index : index + size]],
            "groups": sorted({item.selection_group for item in samples[index : index + size]}),
        }
        for index in range(0, len(samples), size)
    ]


def release_verdict(
    matrix: dict,
    *,
    three_year_window_verified: bool,
    core_semantics_verified: bool,
    idempotency_verified: bool,
    recovery_verified: bool,
    known_gaps_visible: bool,
    performance_p95_seconds: float | None,
    representative_performance_data: bool,
    prometheus_signals_verified: bool,
    task_throughput_verified: bool,
    source_limit_verified: bool,
    storage_growth_measured: bool,
) -> dict:
    sample_count = int(matrix["sample_count"])
    coverage = {
        domain: int(matrix["available_security_counts"].get(domain, 0)) == sample_count
        for domain in REQUIRED_DOMAINS
    }
    checks = {
        "thirty_cross_industry_samples": sample_count == 30
        and int(matrix["industry_group_count"]) >= 4,
        "nine_domains_available_for_all_samples": all(coverage.values()),
        "three_year_window_verified": three_year_window_verified,
        "core_semantics_verified": core_semantics_verified,
        "idempotency_verified": idempotency_verified,
        "backup_restore_verified": recovery_verified,
        "known_gaps_visible": known_gaps_visible,
        "ten_user_local_query_p95_le_2s": representative_performance_data
        and performance_p95_seconds is not None
        and performance_p95_seconds <= 2,
        "prometheus_failure_gap_lag_signals": prometheus_signals_verified,
        "task_throughput_verified": task_throughput_verified,
        "source_limit_verified": source_limit_verified,
        "storage_growth_measured": storage_growth_measured,
    }
    return {
        "result": "passed" if all(checks.values()) else "failed",
        "checks": checks,
        "domain_coverage": coverage,
    }
