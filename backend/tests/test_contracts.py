import copy
import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from app.contracts import domain
from app.contracts.common import AsOfQuery, Availability, DataError, Measure, Provenance
from app.contracts.domain import FinancialFact, FinancialPeriod, SecurityKey
from app.contracts.mapping import content_version, evidence_id, map_measure, upstream_date

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = json.loads((ROOT / "docs/step-05/examples.json").read_text(encoding="utf-8"))
REQUEST = "0" * 32


def example(name, *, synthetic=False):
    return copy.deepcopy(
        next(
            item["record"]
            for item in EXAMPLES["records"]
            if item["model"] == name
            and (item["origin"] == "synthetic_not_market_data") == synthetic
        )
    )


@pytest.mark.parametrize("item", EXAMPLES["records"])
def test_wire_examples_roundtrip_and_schema(item):
    model = getattr(domain, item["model"])
    record = model.model_validate(item["record"])
    assert model.model_validate_json(record.model_dump_json()) == record
    assert model.model_json_schema()["additionalProperties"] is False


@pytest.mark.parametrize("item", EXAMPLES["errors"])
def test_error_examples(item):
    assert DataError.model_validate(item).status != "ok"


@pytest.mark.parametrize(
    "api,field,raw,expected,unit",
    [
        ("daily", "vol", "1045357.45", "104535745.00", "share"),
        ("daily", "amount", "1.234", "1234", "CNY"),
        ("daily_basic", "total_mv", "22452647.3574", "224526473574", "CNY"),
        ("daily_basic", "dv_ratio", "2.51", "0.0251", "ratio"),
        ("income", "n_income_attr_p", "-1.01", "-1.01", "CNY"),
    ],
)
def test_unit_conversion(api, field, raw, expected, unit):
    value = map_measure(api, field, {field: Decimal(raw)})
    assert value.value == Decimal(expected)
    assert value.raw_value == Decimal(raw)
    assert value.unit == unit
    assert isinstance(value.model_dump(mode="json")["value"], str)


def test_zero_null_and_unrequested_are_distinct():
    zero = map_measure("income", "revenue", {"revenue": 0})
    null = map_measure("income", "revenue", {"revenue": None})
    missing = map_measure("income", "revenue", {})
    assert zero.value == 0 and zero.status == "ok"
    assert null.value is None and null.status == "null_value"
    assert missing.value is None and missing.status == "not_requested"


@pytest.mark.parametrize("value", [1.1, True, "NaN", "Infinity", "", "9" * 39])
def test_unsafe_numeric_inputs_rejected(value):
    with pytest.raises(ValidationError):
        map_measure("income", "revenue", {"revenue": value})


@pytest.mark.parametrize(
    "patch", [{"unit": "share"}, {"value": "2"}, {"status": "permission_denied"}, {"value": None}]
)
def test_measure_rejects_inconsistent_unit_value_status(patch):
    data = map_measure("income", "revenue", {"revenue": "1"}).model_dump()
    data.update(patch)
    with pytest.raises(ValidationError):
        Measure.model_validate(data)


@pytest.mark.parametrize(
    "code,exchange",
    [("000001.SH", "SZSE"), ("000001", "SZSE"), ("430001.BJ", "SSE"), ("AAPL", "SSE")],
)
def test_security_requires_supported_exchange_qualified_code(code, exchange):
    with pytest.raises(ValidationError):
        SecurityKey(ts_code=code, exchange=exchange)


def test_date_only_not_known_at_open_and_system_time_separate():
    data = example("FinancialFact", synthetic=True)["provenance"]
    p = Provenance.model_validate(data)

    def query(at, knowledge="public"):
        return AsOfQuery(request_id=REQUEST, as_of=at, knowledge=knowledge)

    assert not query("2026-08-15T09:30:00+08:00").includes(p)
    assert not query("2026-08-15T23:59:59+08:00").includes(p)
    assert query("2026-08-16T00:00:00+08:00").includes(p)
    assert not query("2026-08-16T00:00:00+08:00", "system").includes(p)
    assert query("2026-11-08T12:00:00+08:00", "system").includes(p)


@pytest.mark.parametrize(
    "patch",
    [
        {"available_at": "2026-08-15T09:30:00+08:00"},
        {"available_at": "2026-08-16T00:00:00"},
        {"disclosed_on": 1786723200},
        {"disclosed_on": "20260815"},
        {"disclosed_on": "2026-08-15T00:00:00+08:00"},
    ],
)
def test_unsafe_timing_rejected(patch):
    data = example("FinancialFact", synthetic=True)["provenance"]["timing"]
    data.update(patch)
    with pytest.raises(ValidationError):
        Availability.model_validate(data)


def test_exact_disclosure_and_observed_revision():
    timing = Availability(
        fetched_at="2026-08-15T05:00:00Z",
        disclosed_on="2026-08-15",
        disclosed_at="2026-08-15T04:00:00Z",
        basis="exact_disclosure",
        available_at="2026-08-15T12:00:00+08:00",
    )
    assert timing.available_at.hour == 12
    data = example("FinancialFact")["provenance"]
    query = AsOfQuery(request_id=REQUEST, as_of="2026-08-16T00:00:00+08:00")
    assert not query.includes(Provenance.model_validate(data))


@pytest.mark.parametrize(
    "basis,start,end",
    [
        ("quarter", "2026-04-01", "2026-06-30"),
        ("ytd", "2026-01-01", "2026-06-30"),
        ("annual", "2025-01-01", "2025-12-31"),
        ("ttm", "2025-07-01", "2026-06-30"),
        ("instant", None, "2026-06-30"),
        ("unknown", None, "2026-06-30"),
    ],
)
def test_period_basis(basis, start, end):
    assert FinancialPeriod(basis=basis, start=start, end=end).basis == basis


@pytest.mark.parametrize(
    "basis,start,end",
    [
        ("quarter", "2026-01-01", "2026-06-30"),
        ("annual", "2026-01-01", "2026-06-30"),
        ("ttm", "2026-01-01", "2026-06-30"),
        ("instant", "2026-01-01", "2026-06-30"),
        ("ytd", "2026-01-01", "2026-05-31"),
    ],
)
def test_period_mislabel_rejected(basis, start, end):
    with pytest.raises(ValidationError):
        FinancialPeriod(basis=basis, start=start, end=end)


@pytest.mark.parametrize(
    "patch",
    [
        {"scope": "parent"},
        {"report_type": "2"},
        {"metric": "total_assets"},
        {"revision": "adjusted"},
    ],
)
def test_report_type_and_metric_conflicts(patch):
    data = example("FinancialFact", synthetic=True)
    data.update(patch)
    with pytest.raises(ValidationError):
        FinancialFact.model_validate(data)


def test_incomplete_projection_not_promoted_to_valid():
    data = example("FinancialFact")
    assert FinancialFact.model_validate(data).period.basis == "unknown"
    data["provenance"]["quality"] = "valid"
    with pytest.raises(ValidationError, match="unknown_semantics"):
        FinancialFact.model_validate(data)


def test_derived_ttm_requires_lineage():
    data = example("FinancialFact", synthetic=True)
    data.update(
        report_type=None,
        period={"basis": "ttm", "start": "2025-07-01", "end": "2026-06-30"},
        calculation={"formula": "sum_four_quarters_v1", "input_versions": ["a" * 64]},
    )
    assert FinancialFact.model_validate(data).period.basis == "ttm"
    data["calculation"]["input_versions"] = []
    with pytest.raises(ValidationError):
        FinancialFact.model_validate(data)


def test_snapshot_is_not_historical_and_payment_not_inferred():
    data = example("IndustryMembership")
    data["effective_from"] = "1991-04-03"
    with pytest.raises(ValidationError):
        domain.IndustryMembership.model_validate(data)
    dividend = domain.CorporateAction.model_validate(example("CorporateAction"))
    assert dividend.payment_status == "unverified" and dividend.stage == "unknown"


def test_evidence_requires_derivation_and_no_self_reference():
    data = example("Evidence", synthetic=True)
    data["kind"] = "derived"
    with pytest.raises(ValidationError):
        domain.Evidence.model_validate(data)
    data.update(formula="sum_v1", input_evidence_ids=[data["evidence_id"]])
    with pytest.raises(ValidationError):
        domain.Evidence.model_validate(data)


def test_content_and_evidence_identity():
    a = {"metric": "net_profit_parent", "amount": Decimal("100.00"), "report_type": "1"}
    b = {"report_type": "1", "amount": Decimal("100"), "metric": "net_profit_parent"}
    assert content_version(a) == content_version(b)
    assert content_version(a) != content_version({**a, "report_type": "4"})
    assert content_version(a) != content_version({**a, "metric": "net_profit_total"})
    assert evidence_id(("a" * 64,), "net_profit_parent") != evidence_id(
        ("a" * 64,), "net_profit_total"
    )


def test_actual_probe_decimal_replay():
    raw = json.loads((ROOT / "docs/step-04/probe-results.json").read_bytes(), parse_float=Decimal)
    records = {item["api"]: item for item in raw["records"]}
    row = records["daily_basic"]["requests"][0]["sample"][0]
    assert map_measure("daily_basic", "total_mv", row).value == Decimal("224526473574")
    income = records["income"]["requests"][0]["sample"]
    assert len(income) == 2  # Projection duplicates remain untouched; not proven revisions.
    assert "report_type" not in income[0]


@pytest.mark.parametrize("value", ["20260230", "2026-01-01", "2026111", 20261101])
def test_invalid_upstream_dates(value):
    with pytest.raises(ValueError):
        upstream_date(value)


def test_valid_upstream_date():
    assert upstream_date("20260228") == date(2026, 2, 28)


def test_later_revision_not_visible_before_its_disclosure():
    records = [
        FinancialFact.model_validate(item["record"])
        for item in EXAMPLES["records"]
        if item["model"] == "FinancialFact" and item["origin"] == "synthetic_not_market_data"
    ]
    original, revised = records[0], records[-1]
    query = AsOfQuery(request_id=REQUEST, as_of="2026-09-01T12:00:00+08:00")
    assert query.includes(original.provenance)
    assert not query.includes(revised.provenance)
    assert revised.supersedes_version == original.provenance.data_version


def test_extra_fields_rejected():
    with pytest.raises(ValidationError):
        SecurityKey(ts_code="000001.SZ", exchange="SZSE", guessed_market="CN_A")
