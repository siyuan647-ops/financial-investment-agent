import pytest

from app.dataops.release import (
    REQUIRED_DOMAINS,
    coverage_matrix,
    expansion_batches,
    percentile,
    release_verdict,
    validate_samples,
)


def manifest(count=30):
    return {
        "samples": [
            {
                "sample_id": f"S{index:02d}",
                "ts_code": f"{index:06d}.SZ",
                "selection_group": f"group-{index % 5}",
            }
            for index in range(1, count + 1)
        ]
    }


def test_sample_manifest_and_bounded_expansion_batches():
    samples = validate_samples(manifest())
    batches = expansion_batches(samples)
    assert len(batches) == 10
    assert all(1 <= len(item["codes"]) <= 3 for item in batches)
    assert {code for item in batches for code in item["codes"]} == {
        item.ts_code for item in samples
    }


def test_manifest_cannot_silently_shrink_or_lose_industries():
    with pytest.raises(ValueError, match="exactly_30"):
        validate_samples(manifest(29))
    payload = manifest()
    for row in payload["samples"]:
        row["selection_group"] = "one"
    with pytest.raises(ValueError, match="cross_industry"):
        validate_samples(payload)


def test_missing_counts_are_visible_and_fail_release_gate():
    samples = validate_samples(manifest())
    counts = {sample.ts_code: {domain: 1 for domain in REQUIRED_DOMAINS} for sample in samples}
    counts[samples[-1].ts_code].pop("industry")
    matrix = coverage_matrix(samples, counts)
    verdict = release_verdict(
        matrix,
        three_year_window_verified=True,
        core_semantics_verified=True,
        idempotency_verified=True,
        recovery_verified=True,
        known_gaps_visible=True,
        performance_p95_seconds=0.2,
        representative_performance_data=True,
        prometheus_signals_verified=True,
        task_throughput_verified=True,
        source_limit_verified=True,
        storage_growth_measured=True,
    )
    assert matrix["available_security_counts"]["industry"] == 29
    assert verdict["result"] == "failed"
    assert not verdict["checks"]["nine_domains_available_for_all_samples"]


def test_empty_or_nonrepresentative_performance_never_passes():
    samples = validate_samples(manifest())
    complete = {sample.ts_code: {domain: 1 for domain in REQUIRED_DOMAINS} for sample in samples}
    verdict = release_verdict(
        coverage_matrix(samples, complete),
        three_year_window_verified=True,
        core_semantics_verified=True,
        idempotency_verified=True,
        recovery_verified=True,
        known_gaps_visible=True,
        performance_p95_seconds=0.01,
        representative_performance_data=False,
        prometheus_signals_verified=True,
        task_throughput_verified=True,
        source_limit_verified=True,
        storage_growth_measured=True,
    )
    assert not verdict["checks"]["ten_user_local_query_p95_le_2s"]
    assert percentile([0.4, 0.1, 0.3, 0.2], 0.95) == 0.4
    assert percentile([], 0.95) is None
