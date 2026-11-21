from datetime import date

import anyio
import pytest

from app.dataops.orchestration import (
    classify_failure,
    corporate_batches,
    financial_batches,
    market_batches,
    reference_batch,
)
from app.workflows import orchestration as orchestration_workflow


def test_after_close_refreshes_indicators_only_after_market_publication(monkeypatch):
    calls = []

    async def execute(specs, **kwargs):
        calls.append(("market", tuple(item.partition_key for item in specs), kwargs))
        return {"completed": ["market"]}

    async def technical(code, as_of_date):
        calls.append(("technical", code, as_of_date))
        return {"ts_code": code}

    monkeypatch.setattr(orchestration_workflow, "execute_batches", execute)
    monkeypatch.setattr(orchestration_workflow, "technical_market_window", technical)

    result = anyio.run(
        orchestration_workflow.close_incremental.fn,
        ["600519.SH", "000001.SZ"],
        date(2026, 11, 17),
        7,
    )

    assert [item[0] for item in calls] == [
        "market",
        "technical",
        "technical",
    ]
    assert [item[1] for item in calls[1:3]] == ["000001.SZ", "600519.SH"]
    assert [item["ts_code"] for item in result["technical"]] == [
        "000001.SZ",
        "600519.SH",
    ]


def test_market_backfill_is_bounded_and_deterministic():
    batches = market_batches(
        ["600519.SH", "000001.SZ"], date(2024, 1, 1), date(2024, 3, 15), window_days=31
    )
    assert len(batches) == 6
    assert batches[0].partition_key == "000001.SZ"
    assert all(
        (
            date.fromisoformat(item.parameters["end"])
            - date.fromisoformat(item.parameters["start"])
        ).days
        <= 30
        for item in batches
    )
    assert batches == market_batches(
        ["600519.SH", "000001.SZ"], date(2024, 1, 1), date(2024, 3, 15), window_days=31
    )


def test_revision_scan_cycle_gets_a_distinct_idempotency_key():
    first = market_batches(
        ["000001.SZ"], date(2024, 1, 1), date(2024, 1, 7), scan_on=date(2024, 1, 7)
    )[0]
    second = market_batches(
        ["000001.SZ"], date(2024, 1, 1), date(2024, 1, 7), scan_on=date(2024, 1, 8)
    )[0]
    assert first.idempotency_key != second.idempotency_key


def test_reference_refresh_is_bounded_and_cycle_scoped():
    batch = reference_batch(
        ["600519.SH", "000001.SZ"],
        date(2024, 4, 1),
        date(2024, 5, 15),
        scan_on=date(2024, 5, 1),
    )
    assert batch.domain == "reference"
    assert batch.parameters["codes"] == ["000001.SZ", "600519.SH"]
    assert batch.cursor_value == "2024-05-15"


def test_financial_batches_limit_each_provider_request():
    batches = financial_batches(
        ["000001.SZ", "600519.SH", "000002.SZ", "600000.SH"],
        [date(2023, 12, 31), date(2024, 3, 31)],
        scan_on=date(2024, 5, 1),
    )
    assert len(batches) == 4
    assert all(len(item.parameters["codes"]) <= 3 for item in batches)
    assert all(len(item.parameters["periods"]) == 1 for item in batches)


def test_corporate_batches_are_bounded_and_cycle_scoped():
    batches = corporate_batches(
        "dividend",
        ["000001.SZ", "600519.SH", "000002.SZ", "600000.SH"],
        date(2024, 5, 1),
    )
    assert [len(item.parameters["codes"]) for item in batches] == [3, 1]
    assert all(item.cursor_value == "2024-05-01" for item in batches)


@pytest.mark.parametrize(
    ("reason", "kind", "retryable"),
    [
        ("market_pipeline_failed:rate_limited", "transient", True),
        ("financial_pipeline_failed:permission_denied", "permission", False),
        ("invalid_market_record", "permanent", False),
        ("unsafe text with spaces", "permanent", False),
    ],
)
def test_failure_policy_is_conservative(reason, kind, retryable):
    policy = classify_failure(reason)
    assert policy.kind == kind
    assert policy.retryable is retryable
    if reason == "unsafe text with spaces":
        assert policy.reason == "unclassified_failure"


def test_invalid_batch_scope_is_rejected():
    with pytest.raises(ValueError, match="require_1_to_30_unique_codes"):
        market_batches([], date(2024, 1, 1), date(2024, 1, 2))
    with pytest.raises(ValueError, match="invalid_market_batch_range"):
        market_batches(["000001.SZ"], date(2024, 1, 1), date(2024, 1, 2), window_days=47)
