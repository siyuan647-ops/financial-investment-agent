"""Explicit synthetic daily source; never evidence of real market prices/calendar."""

import json
from datetime import timedelta

from app.contracts.mapping import upstream_date
from app.providers.tushare import ProviderResult


class MarketFixture:
    def __init__(self):
        self.close = "11"
        self.mutate = None

    async def query(self, api, params, fields):
        names = fields.split(",")
        rows = []
        day, end = upstream_date(params["start_date"]), upstream_date(params["end_date"])
        while day <= end:
            if day.weekday() < 5 and day.strftime("%m%d") != "1001":
                row = dict(
                    ts_code=params["ts_code"],
                    trade_date=day.strftime("%Y%m%d"),
                    open="10",
                    high="12",
                    low="9",
                    close=self.close,
                    vol="2.5",
                    amount="3.25",
                    adj_factor="1.5",
                    pe=None,
                    pe_ttm="-3",
                    pb="1.2",
                    total_mv="100",
                    circ_mv="90",
                    dv_ratio=None,
                    dv_ttm="2",
                )
                rows.append({key: row[key] for key in names})
            day += timedelta(days=1)
        if self.mutate:
            self.mutate(api, rows)
        items = [list(row.values()) for row in rows]
        content = json.dumps({"code": 0, "data": {"fields": names, "items": items}}).encode()
        return ProviderResult(tuple(names), tuple(tuple(row) for row in items), content)
