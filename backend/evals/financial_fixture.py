"""Synthetic statements only; not announcement evidence."""

import json

from app.providers.tushare import ProviderResult


class FinancialFixture:
    def __init__(self):
        self.revenue = "100.123456789012345678"
        self.mutate = None

    async def query(self, api, params, fields):
        names = fields.split(",")
        row = dict(
            ts_code=params["ts_code"],
            end_date=params["period"],
            report_type=params["report_type"],
            comp_type="2",
            update_flag="1",
            ann_date="20250315",
            f_ann_date="20250315",
            revenue=self.revenue,
            total_revenue=None,
            n_income_attr_p="20",
            n_income="21",
            total_assets="1000",
            total_liab="900",
            n_cashflow_act="30",
            n_cashflow_inv_act="-10",
            n_cash_flows_fnc_act="-5",
        )
        rows = [{name: row[name] for name in names}]
        if self.mutate:
            self.mutate(api, rows)
        items = [[row[name] for name in names] for row in rows]
        content = json.dumps({"code": 0, "data": {"fields": names, "items": items}}).encode()
        return ProviderResult(tuple(names), tuple(tuple(r) for r in items), content)
