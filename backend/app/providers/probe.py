"""Small explicit probe, never a market backfill. Run with --env-file .env."""

import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from app.core.config import Settings
from app.providers.tushare import ProviderAdapter, ProviderError

CATALOG = {
    "stock_basic": ("证券/基础行业", "ts_code,name,industry,list_date", None),
    "daily": ("日线", "ts_code,trade_date,open,high,low,close,vol", "trade_date"),
    "daily_basic": ("估值", "ts_code,trade_date,pe,pe_ttm,pb,total_mv", "trade_date"),
    "income": ("利润表", "ts_code,ann_date,end_date,revenue,n_income_attr_p", "ann_date"),
    "balancesheet": ("资产负债表", "ts_code,ann_date,end_date,total_assets,total_liab", "ann_date"),
    "cashflow": ("现金流量表", "ts_code,ann_date,end_date,n_cashflow_act", "ann_date"),
    "dividend": ("分红", "ts_code,ann_date,end_date,cash_div,ex_date", None),
    "index_member_all": ("申万行业", "ts_code,l1_code,l1_name,in_date,out_date", None),
    "adj_factor": ("复权", "ts_code,trade_date,adj_factor", "trade_date"),
    "trade_cal": ("交易日历", "exchange,cal_date,is_open", "cal_date"),
    "stk_mins": ("分钟", "ts_code,trade_time,close", None),
    "rt_k": ("实时", "ts_code,close", None),
}


def assess(result, params, requested, date_field):
    rows = result.rows
    checks = {
        "fields": "effective"
        if set(result.fields) == set(requested.split(","))
        else "ignored_or_mismatched",
        "security": "not_applicable",
        "date": "not_applicable",
        "limit": "effective" if len(rows) <= params["limit"] else "ignored_or_mismatched",
    }
    for parameter, field in (("ts_code", "ts_code"), ("exchange", "exchange")):
        if parameter in params:
            checks["security"] = (
                "pending"
                if not rows or field not in result.fields
                else "effective"
                if all(r[field] == params[parameter] for r in rows)
                else "ignored_or_mismatched"
            )
    if date_field and "start_date" in params:
        checks["date"] = (
            "pending"
            if not rows or date_field not in result.fields
            else "effective"
            if all(
                isinstance(r[date_field], str)
                and params["start_date"] <= r[date_field] <= params["end_date"]
                for r in rows
            )
            else "ignored_or_mismatched"
        )
    if not rows:
        checks["limit"] = "pending"
    if "trade_date" in params:
        checks["date"] = (
            "pending"
            if not rows or "trade_date" not in result.fields
            else "effective"
            if all(r["trade_date"] == params["trade_date"] for r in rows)
            else "ignored_or_mismatched"
        )
    return checks


async def probe(settings, output: Path, apis=None):
    records = []
    catalog = {api: spec for api, spec in CATALOG.items() if apis is None or api in apis}
    # At most 39 HTTP requests, no retries, 180 seconds overall.
    settings = settings.model_copy(update={"provider_max_retries": 0})
    async with ProviderAdapter(settings) as adapter:
        try:
            async with asyncio.timeout(180):
                for api, (domain, fields, date_field) in catalog.items():
                    base = {"ts_code": "000001.SZ", "limit": 2, "offset": 0}
                    if api == "trade_cal":
                        base = {"exchange": "SSE", "limit": 2, "offset": 0}
                    if date_field:
                        base.update(start_date="20260901", end_date="20260930")
                    if api in ("income", "balancesheet", "cashflow"):
                        base.update(start_date="20230101", end_date="20260930")
                    if api == "stk_mins":
                        base.update(
                            freq="1min",
                            start_date="2026-09-30 09:30:00",
                            end_date="2026-09-30 09:35:00",
                        )
                    record = {
                        "api": api,
                        "domain": domain,
                        "status": "pending",
                        "permission": "pending",
                        "requests": [],
                        "history": "仅探测指定窗口；不证明历史完整性",
                        "pagination": "pending",
                    }
                    records.append(record)
                    results = []
                    variants = [
                        (base, fields),
                        ({**base, "offset": 2}, fields),
                        ({**base, "limit": 1}, fields.split(",")[0]),
                    ]
                    if api == "daily":
                        variants.extend(
                            [
                                (
                                    {"ts_code": "000001.SZ", "trade_date": "20260930", "limit": 2},
                                    fields,
                                ),
                                (
                                    {"ts_code": "000001.SZ", "trade_date": "20260929", "limit": 2},
                                    fields,
                                ),
                                ({**base, "ts_code": "600000.SH"}, fields),
                            ]
                        )
                    for params, requested in variants:
                        entry = {"params": params, "requested_fields": requested}
                        record["requests"].append(entry)
                        try:
                            result = await adapter.query(api, params, requested)
                        except ProviderError as exc:
                            entry["error"] = exc.kind
                            entry["http_status"] = exc.http_status
                            entry["business_code"] = exc.business_code
                            if exc.kind == "permission_denied":
                                record.update(status="unavailable", permission="denied")
                            break
                        entry.update(
                            fields=list(result.fields),
                            row_count=len(result.items),
                            checks=assess(result, params, requested, date_field),
                            sample=result.rows[:2],
                        )
                        results.append(result)
                    if results:
                        if record["permission"] != "denied":
                            record["permission"] = "observed_success"
                        bad = any(
                            "ignored_or_mismatched" in e.get("checks", {}).values()
                            for e in record["requests"]
                        )
                        failed = any("error" in e for e in record["requests"])
                        if record["status"] != "unavailable":
                            record["status"] = (
                                "pending" if failed or bad or not results[0].items else "available"
                            )
                        record["observation"] = (
                            "filter_mismatch"
                            if bad
                            else "success_with_data"
                            if results[0].items
                            else "success_empty"
                        )
                    if len(results) >= 2 and results[0].items and results[1].items:
                        left = {json.dumps(r, sort_keys=True) for r in results[0].rows}
                        right = {json.dumps(r, sort_keys=True) for r in results[1].rows}
                        record["pagination"] = (
                            "ignored_or_overlapping" if left & right else "observed_disjoint_pages"
                        )
                        if left & right:
                            record["status"] = "pending"
        except TimeoutError:
            pass
    present = {r["api"] for r in records}
    records.extend(
        {"api": api, "domain": spec[0], "status": "pending", "reason": "overall_budget_exhausted"}
        for api, spec in catalog.items()
        if api not in present
    )
    report = {
        "generated_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(),
        "budget": {
            "max_http_requests": len(catalog) * 3 + (3 if "daily" in catalog else 0),
            "max_seconds": 180,
        },
        "scope": (
            "000001.SZ；日线另对照600000.SH；行情20260901–20260930；"
            "财务公告20230101–20260930；分红仅按证券"
        ),
        "records": records,
    }
    # Defense against a third-party echo of credentials anywhere in returned values.
    serialized = json.dumps(report, ensure_ascii=False, indent=2)
    for secret in (
        settings.data_source_token.get_secret_value(),
        settings.data_source_url.get_secret_value(),
    ):
        serialized = serialized.replace(secret, "[REDACTED]")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(serialized + "\n", encoding="utf-8")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--output", default="docs/step-04/probe-results.json")
    parser.add_argument("--apis", nargs="+", choices=list(CATALOG))
    args = parser.parse_args()
    # Do not print raw settings exceptions, which may contain untrusted field names.
    try:
        settings = Settings(_env_file=args.env_file, _env_file_encoding="utf-8-sig")
    except Exception:
        raise SystemExit("Invalid or missing local configuration") from None
    asyncio.run(probe(settings, Path(args.output), args.apis))


if __name__ == "__main__":
    main()
