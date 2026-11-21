"""Bounded Step13 batch plans and safe failure classification."""

import hashlib
import json
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Literal

from app.dataops.financial import validate_request
from app.dataops.market import validate_window
from app.dataops.reference import security_key

Domain = Literal["reference", "market", "financial", "dividend", "industry"]
TRANSIENT_REASONS = {
    "transport_timeout",
    "transport_error",
    "total_timeout",
    "rate_limited",
    "source_failure",
    "circuit_open",
    "provider_not_ready",
}
PERMISSION_REASONS = {"permission_denied"}


def validate_batch_codes(codes):
    if not 1 <= len(codes) <= 30 or len(set(codes)) != len(codes):
        raise ValueError("require_1_to_30_unique_codes")
    for code in codes:
        security_key(code)


@dataclass(frozen=True)
class BatchSpec:
    domain: Domain
    partition_key: str
    parameters: dict[str, Any]
    cursor_kind: Literal["trade_date", "report_period", "observed_at"]
    cursor_value: str

    @property
    def idempotency_key(self) -> str:
        canonical = json.dumps(
            {
                "domain": self.domain,
                "partition_key": self.partition_key,
                "parameters": self.parameters,
                "cursor_kind": self.cursor_kind,
                "cursor_value": self.cursor_value,
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        return "step13:" + hashlib.sha256(canonical.encode()).hexdigest()


@dataclass(frozen=True)
class FailurePolicy:
    reason: str
    kind: Literal["transient", "permission", "permanent"]
    retryable: bool


def classify_failure(reason: str) -> FailurePolicy:
    safe = reason.rsplit(":", 1)[-1]
    if safe in TRANSIENT_REASONS:
        return FailurePolicy(safe, "transient", True)
    if safe in PERMISSION_REASONS:
        return FailurePolicy(safe, "permission", False)
    if not safe.replace("_", "").isalnum() or len(safe) > 80:
        safe = "unclassified_failure"
    return FailurePolicy(safe, "permanent", False)


def market_batches(
    codes: list[str],
    start: date,
    end: date,
    *,
    window_days: int = 31,
    scan_on: date | None = None,
):
    validate_batch_codes(codes)
    if start > end or not 1 <= window_days <= 46:
        raise ValueError("invalid_market_batch_range")
    batches = []
    for code in sorted(codes):
        cursor = start
        while cursor <= end:
            window_end = min(end, cursor + timedelta(days=window_days - 1))
            validate_window(code, cursor, window_end)
            batches.append(
                BatchSpec(
                    "market",
                    code,
                    {
                        "code": code,
                        "start": cursor.isoformat(),
                        "end": window_end.isoformat(),
                        **({"scan_on": scan_on.isoformat()} if scan_on else {}),
                    },
                    "trade_date",
                    window_end.isoformat(),
                )
            )
            cursor = window_end + timedelta(days=1)
    return batches


def reference_batch(codes: list[str], start: date, end: date, *, scan_on: date):
    validate_batch_codes(codes)
    if start > end or (end - start).days > 1100:
        raise ValueError("invalid_reference_batch_range")
    return BatchSpec(
        "reference",
        "CN_A:SZSE,SSE",
        {
            "codes": sorted(codes),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "scan_on": scan_on.isoformat(),
        },
        "trade_date",
        end.isoformat(),
    )


def financial_batches(
    codes: list[str],
    periods: list[date],
    *,
    report_types: tuple[str, ...] = ("1",),
    scan_on: date | None = None,
):
    validate_batch_codes(codes)
    if not codes or not periods:
        raise ValueError("financial_batch_scope_required")
    if len(periods) != len(set(periods)):
        raise ValueError("unique_financial_periods_required")
    batches = []
    for offset in range(0, len(codes), 3):
        code_batch = codes[offset : offset + 3]
        for period in sorted(periods):
            validate_request(code_batch, [period], report_types)
            partition = ",".join(sorted(code_batch)) + ":" + ",".join(report_types)
            batches.append(
                BatchSpec(
                    "financial",
                    partition,
                    {
                        "codes": sorted(code_batch),
                        "periods": [period.isoformat()],
                        "report_types": list(report_types),
                        **({"scan_on": scan_on.isoformat()} if scan_on else {}),
                    },
                    "report_period",
                    period.isoformat(),
                )
            )
    return batches


def corporate_batches(domain: Literal["dividend", "industry"], codes: list[str], observed: date):
    validate_batch_codes(codes)
    batches = []
    for offset in range(0, len(codes), 3):
        code_batch = sorted(codes[offset : offset + 3])
        batches.append(
            BatchSpec(
                domain,
                ",".join(code_batch),
                {"codes": code_batch},
                "observed_at",
                observed.isoformat(),
            )
        )
    return batches
