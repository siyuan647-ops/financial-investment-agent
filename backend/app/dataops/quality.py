"""Deterministic Step12 quality rules, completeness accounting and summaries."""

from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal, localcontext
from enum import StrEnum
from typing import Any

from app.contracts.common import Measure
from app.contracts.domain import FinancialFact, PriceBar

PROCESSING_VERSION = "step12-v1"


class Severity(StrEnum):
    BLOCKING = "blocking"
    WARNING = "warning"
    NOT_APPLICABLE = "not_applicable"


class Category(StrEnum):
    COMPLETENESS = "completeness"
    UNIQUENESS = "uniqueness"
    FRESHNESS = "freshness"
    PERIOD = "period"
    UNIT = "unit"
    RELATIONSHIP = "relationship"
    SOURCE = "source"
    APPLICABILITY = "applicability"


@dataclass(frozen=True)
class Rule:
    rule_id: str
    category: Category
    severity: Severity
    description: str


def _rule(rule_id: str, category: Category, severity: Severity, description: str) -> Rule:
    return Rule(rule_id, category, severity, description)


# Existing flow-level gates are listed beside reusable record-level rules.  A flow may
# report a more specific reason_code, but it cannot silently invent a severity.
RULES = {
    item.rule_id: item
    for item in (
        _rule(
            "required_field_missing",
            Category.COMPLETENESS,
            Severity.BLOCKING,
            "Required field is absent or null.",
        ),
        _rule(
            "duplicate_record",
            Category.UNIQUENESS,
            Severity.BLOCKING,
            "Natural key occurs more than once.",
        ),
        _rule(
            "unexpected_record",
            Category.COMPLETENESS,
            Severity.WARNING,
            "Record is outside the expected set.",
        ),
        _rule(
            "stale_record",
            Category.FRESHNESS,
            Severity.WARNING,
            "Record arrived after its freshness deadline.",
        ),
        _rule(
            "invalid_period",
            Category.PERIOD,
            Severity.BLOCKING,
            "Reporting period is invalid or inconsistent.",
        ),
        _rule(
            "unit_mismatch",
            Category.UNIT,
            Severity.BLOCKING,
            "Measure unit differs from the declared unit.",
        ),
        _rule(
            "invalid_price_relationship",
            Category.RELATIONSHIP,
            Severity.BLOCKING,
            "OHLC prices violate their ordering relationship.",
        ),
        _rule(
            "financial_reconciliation_failed",
            Category.RELATIONSHIP,
            Severity.BLOCKING,
            "Applicable statement relation exceeds rounding tolerance.",
        ),
        _rule(
            "field_not_applicable",
            Category.APPLICABILITY,
            Severity.NOT_APPLICABLE,
            "Field is not applicable to this record.",
        ),
        _rule(
            "reference_response_invalid",
            Category.SOURCE,
            Severity.BLOCKING,
            "Reference response failed validation.",
        ),
        _rule(
            "market_response_invalid",
            Category.SOURCE,
            Severity.BLOCKING,
            "Market response failed validation.",
        ),
        _rule(
            "financial_response_invalid",
            Category.SOURCE,
            Severity.BLOCKING,
            "Financial response failed validation.",
        ),
        _rule(
            "dividend_response_invalid",
            Category.SOURCE,
            Severity.BLOCKING,
            "Dividend response failed validation.",
        ),
        _rule(
            "industry_catalog_invalid",
            Category.SOURCE,
            Severity.BLOCKING,
            "Industry catalog failed validation.",
        ),
        _rule(
            "basic_industry_invalid",
            Category.SOURCE,
            Severity.BLOCKING,
            "Basic industry response failed validation.",
        ),
        _rule(
            "industry_membership_invalid",
            Category.SOURCE,
            Severity.BLOCKING,
            "Industry membership response failed validation.",
        ),
        _rule(
            "injected_quality_failure",
            Category.SOURCE,
            Severity.WARNING,
            "Synthetic failure used to verify transaction rollback.",
        ),
        _rule(
            "sample_quality_warning",
            Category.SOURCE,
            Severity.WARNING,
            "Synthetic warning used to verify quality persistence.",
        ),
    )
}


@dataclass(frozen=True)
class QualityIssue:
    rule_id: str
    severity: Severity
    subject_kind: str
    subject_key: str
    reason_code: str
    original_value: Any = None
    detail: Mapping[str, Any] = field(default_factory=dict)


class CompletenessOutcome(StrEnum):
    RECEIVED = "received"
    EXCEPTION = "exception"
    MISSING = "missing"
    UNEXPECTED = "unexpected"


class ExceptionReason(StrEnum):
    NON_TRADING_DAY = "non_trading_day"
    BEFORE_LISTING = "before_listing"
    AFTER_DELISTING = "after_delisting"
    SUSPENDED = "suspended"
    NOT_DISCLOSED = "not_disclosed"
    NOT_APPLICABLE = "not_applicable"


@dataclass(frozen=True)
class CompletenessItem:
    subject_key: str
    outcome: CompletenessOutcome
    reason_code: str | None = None


@dataclass(frozen=True)
class CompletenessReport:
    domain: str
    items: tuple[CompletenessItem, ...]
    expected_count: int
    eligible_count: int
    received_count: int
    exception_count: int
    missing_count: int
    unexpected_count: int
    completeness: Decimal

    @property
    def complete(self) -> bool:
        return self.missing_count == 0


@dataclass(frozen=True)
class QualitySummary:
    blocking: int
    warnings: int
    not_applicable: int
    by_rule: Mapping[str, int]
    completeness: Decimal | None

    @property
    def publishable(self) -> bool:
        return self.blocking == 0


class QualityEngine:
    """Pure checks. Persistence and retry lifecycle live in QualityRepository."""

    def __init__(self, rules: Mapping[str, Rule] = RULES):
        self.rules = dict(rules)
        if len(self.rules) != len(set(self.rules)):
            raise ValueError("duplicate_quality_rule")

    def issue(
        self,
        rule_id: str,
        *,
        subject_kind: str,
        subject_key: str,
        reason_code: str,
        original_value: Any = None,
        detail: Mapping[str, Any] | None = None,
    ) -> QualityIssue:
        try:
            rule = self.rules[rule_id]
        except KeyError:
            raise ValueError("unknown_quality_rule") from None
        return QualityIssue(
            rule_id=rule_id,
            severity=rule.severity,
            subject_kind=subject_kind,
            subject_key=subject_key,
            reason_code=reason_code,
            original_value=original_value,
            detail=dict(detail or {}),
        )

    def rows(
        self,
        rows: Sequence[Mapping[str, Any]],
        *,
        subject_kind: str,
        key_fields: Sequence[str],
        required_fields: Sequence[str],
        optional_fields: Sequence[str] = (),
        not_applicable: Mapping[str, Iterable[str]] | None = None,
    ) -> tuple[QualityIssue, ...]:
        """Check required values and natural-key uniqueness.

        Optional nulls intentionally produce no issue. Explicitly inapplicable fields produce
        an informational not_applicable result, never a blocking failure.
        """
        issues: list[QualityIssue] = []
        seen: dict[tuple[Any, ...], int] = {}
        optional = set(optional_fields)
        applicability = {key: set(value) for key, value in (not_applicable or {}).items()}
        if set(required_fields) & optional:
            raise ValueError("field_cannot_be_required_and_optional")
        for index, row in enumerate(rows):
            key = tuple(row.get(name) for name in key_fields)
            subject_key = "|".join("<null>" if value is None else str(value) for value in key)
            if key in seen:
                issues.append(
                    self.issue(
                        "duplicate_record",
                        subject_kind=subject_kind,
                        subject_key=subject_key,
                        reason_code="duplicate_natural_key",
                        original_value=row,
                        detail={"first_index": seen[key], "duplicate_index": index},
                    )
                )
            else:
                seen[key] = index
            for field_name in required_fields:
                if field_name in applicability.get(subject_key, set()):
                    issues.append(
                        self.issue(
                            "field_not_applicable",
                            subject_kind=subject_kind,
                            subject_key=subject_key,
                            reason_code=f"{field_name}_not_applicable",
                            detail={"field": field_name},
                        )
                    )
                elif row.get(field_name) is None:
                    issues.append(
                        self.issue(
                            "required_field_missing",
                            subject_kind=subject_kind,
                            subject_key=subject_key,
                            reason_code=f"{field_name}_missing",
                            original_value=row,
                            detail={"field": field_name},
                        )
                    )
        return tuple(issues)

    def price_bar(self, bar: PriceBar) -> tuple[QualityIssue, ...]:
        values = {name: getattr(bar, name).value for name in ("open", "high", "low", "close")}
        key = f"{bar.security.ts_code}:{bar.trade_date}"
        if any(value is None for value in values.values()):
            return (
                self.issue(
                    "required_field_missing",
                    subject_kind="price_bar",
                    subject_key=key,
                    reason_code="ohlc_value_missing",
                    original_value=values,
                ),
            )
        if min(values.values()) < 0 or not (
            values["low"]
            <= min(values["open"], values["close"])
            <= max(values["open"], values["close"])
            <= values["high"]
        ):
            return (
                self.issue(
                    "invalid_price_relationship",
                    subject_kind="price_bar",
                    subject_key=key,
                    reason_code="invalid_ohlc_relationship",
                    original_value=values,
                ),
            )
        return ()

    def unit(
        self,
        measure: Measure,
        expected_unit: str,
        *,
        subject_kind: str,
        subject_key: str,
        field_name: str,
    ) -> tuple[QualityIssue, ...]:
        if measure.unit == expected_unit:
            return ()
        return (
            self.issue(
                "unit_mismatch",
                subject_kind=subject_kind,
                subject_key=subject_key,
                reason_code="declared_unit_mismatch",
                original_value=measure.model_dump(mode="json"),
                detail={"field": field_name, "expected_unit": expected_unit},
            ),
        )

    def period(self, fact: FinancialFact) -> tuple[QualityIssue, ...]:
        end = fact.period.end
        key = f"{fact.security.ts_code}:{fact.metric}:{end}:{fact.report_type}"
        valid_end = (end.month, end.day) in {(3, 31), (6, 30), (9, 30), (12, 31)}
        if valid_end and (fact.period.start is None) == (fact.period.basis == "instant"):
            return ()
        return (
            self.issue(
                "invalid_period",
                subject_kind="financial_fact",
                subject_key=key,
                reason_code="invalid_financial_period",
                original_value=fact.period.model_dump(mode="json"),
            ),
        )

    def freshness(
        self,
        observed_at: datetime,
        expected_by: datetime,
        *,
        subject_kind: str,
        subject_key: str,
        grace: timedelta = timedelta(0),
    ) -> tuple[QualityIssue, ...]:
        if observed_at <= expected_by + grace:
            return ()
        return (
            self.issue(
                "stale_record",
                subject_kind=subject_kind,
                subject_key=subject_key,
                reason_code="freshness_deadline_exceeded",
                detail={
                    "observed_at": observed_at.isoformat(),
                    "expected_by": expected_by.isoformat(),
                    "grace_seconds": int(grace.total_seconds()),
                },
            ),
        )

    def reconcile(
        self,
        *,
        subject_key: str,
        left: Decimal | None,
        right: Decimal | None,
        relation: str,
        applicable: bool = True,
        absolute_tolerance: Decimal = Decimal("0.01"),
        relative_tolerance: Decimal = Decimal("0.000001"),
    ) -> tuple[QualityIssue, ...]:
        if not applicable:
            return (
                self.issue(
                    "field_not_applicable",
                    subject_kind="financial_reconciliation",
                    subject_key=subject_key,
                    reason_code="reconciliation_not_applicable",
                    detail={"relation": relation},
                ),
            )
        if left is None or right is None:
            # A legal optional/industry-specific null is not converted into a blocking failure.
            return ()
        with localcontext() as context:
            context.prec = 50
            difference = abs(left - right)
            allowed = max(absolute_tolerance, max(abs(left), abs(right)) * relative_tolerance)
        if difference <= allowed:
            return ()
        return (
            self.issue(
                "financial_reconciliation_failed",
                subject_kind="financial_reconciliation",
                subject_key=subject_key,
                reason_code="outside_rounding_tolerance",
                original_value={"left": str(left), "right": str(right)},
                detail={
                    "relation": relation,
                    "difference": str(difference),
                    "allowed": str(allowed),
                },
            ),
        )

    def completeness(
        self,
        domain: str,
        expected: Iterable[str],
        observed: Iterable[str],
        exceptions: Mapping[str, ExceptionReason | str] | None = None,
    ) -> CompletenessReport:
        expected_values = tuple(expected)
        observed_values = tuple(observed)
        if len(expected_values) != len(set(expected_values)):
            raise ValueError("duplicate_expected_subject")
        if len(observed_values) != len(set(observed_values)):
            raise ValueError("duplicate_observed_subject")
        expected_set, observed_set = set(expected_values), set(observed_values)
        exception_map = {key: ExceptionReason(value) for key, value in (exceptions or {}).items()}
        if not set(exception_map) <= expected_set:
            raise ValueError("exception_outside_expected_set")
        items: list[CompletenessItem] = []
        received = missing = exception_count = 0
        for key in sorted(expected_set):
            if key in observed_set:
                outcome, reason = CompletenessOutcome.RECEIVED, None
                received += 1
            elif key in exception_map:
                outcome, reason = CompletenessOutcome.EXCEPTION, exception_map[key].value
                exception_count += 1
            else:
                outcome, reason = CompletenessOutcome.MISSING, "expected_record_missing"
                missing += 1
            items.append(CompletenessItem(key, outcome, reason))
        unexpected = observed_set - expected_set
        items.extend(
            CompletenessItem(key, CompletenessOutcome.UNEXPECTED, "outside_expected_set")
            for key in sorted(unexpected)
        )
        eligible = len(expected_set) - exception_count
        ratio = Decimal(1) if eligible == 0 else Decimal(received) / Decimal(eligible)
        return CompletenessReport(
            domain=domain,
            items=tuple(items),
            expected_count=len(expected_set),
            eligible_count=eligible,
            received_count=received,
            exception_count=exception_count,
            missing_count=missing,
            unexpected_count=len(unexpected),
            completeness=ratio,
        )

    def summarize(
        self,
        issues: Iterable[QualityIssue],
        completeness: CompletenessReport | None = None,
    ) -> QualitySummary:
        items = tuple(issues)
        severities = Counter(item.severity for item in items)
        return QualitySummary(
            blocking=severities[Severity.BLOCKING],
            warnings=severities[Severity.WARNING],
            not_applicable=severities[Severity.NOT_APPLICABLE],
            by_rule=dict(sorted(Counter(item.rule_id for item in items).items())),
            completeness=completeness.completeness if completeness else None,
        )


ENGINE = QualityEngine()
