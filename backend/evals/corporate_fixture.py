"""Synthetic Step10 source; it is not announcement or classification evidence."""

import json

from app.providers.tushare import ProviderResult


class CorporateFixture:
    def __init__(self):
        self.cash = "0.25"
        self.basic_industry = "银行"
        self.mutate = None

    async def query(self, api, params, fields):
        if api == "dividend":
            rows = [
                {
                    "ts_code": params["ts_code"],
                    "end_date": "20241231",
                    "ann_date": "20250301",
                    "div_proc": "预案",
                    "cash_div": None,
                    "cash_div_tax": self.cash,
                    "stk_div": "0",
                    "record_date": None,
                    "ex_date": None,
                    "pay_date": None,
                    "imp_ann_date": None,
                },
                {
                    "ts_code": params["ts_code"],
                    "end_date": "20241231",
                    "ann_date": "20250301",
                    "div_proc": "股东大会通过",
                    "cash_div": None,
                    "cash_div_tax": self.cash,
                    "stk_div": "0",
                    "record_date": None,
                    "ex_date": None,
                    "pay_date": None,
                    "imp_ann_date": None,
                },
                {
                    "ts_code": params["ts_code"],
                    "end_date": "20241231",
                    "ann_date": "20250301",
                    "div_proc": "实施",
                    "cash_div": "0.24",
                    "cash_div_tax": self.cash,
                    "stk_div": "0",
                    "record_date": "20250507",
                    "ex_date": "20250508",
                    "pay_date": "20250508",
                    "imp_ann_date": "20250501",
                },
            ]
        elif api == "stock_basic":
            rows = [{"ts_code": params["ts_code"], "industry": self.basic_industry}]
        elif api == "index_classify":
            rows = {
                "L1": [
                    {
                        "index_code": "801780.SI",
                        "industry_name": "银行",
                        "parent_code": "0",
                        "level": "L1",
                        "industry_code": "480000",
                        "is_pub": "1",
                        "src": "SW2021",
                    }
                ],
                "L2": [
                    {
                        "index_code": "801192.SI",
                        "industry_name": "股份制银行Ⅱ",
                        "parent_code": "480000",
                        "level": "L2",
                        "industry_code": "480300",
                        "is_pub": "1",
                        "src": "SW2021",
                    }
                ],
                "L3": [
                    {
                        "index_code": "851911.SI",
                        "industry_name": "股份制银行Ⅲ",
                        "parent_code": "480300",
                        "level": "L3",
                        "industry_code": "480301",
                        "is_pub": "1",
                        "src": "SW2021",
                    }
                ],
            }[params["level"]]
        elif api == "index_member_all":
            current = params["is_new"] == "Y"
            rows = [
                {
                    "ts_code": params["ts_code"],
                    "l1_code": "801780.SI",
                    "l1_name": "银行",
                    "l2_code": "801192.SI",
                    "l2_name": "股份制银行Ⅱ",
                    "l3_code": "851911.SI",
                    "l3_name": "股份制银行Ⅲ",
                    "in_date": "20210730" if current else "20140101",
                    "out_date": None if current else "20210729",
                    "is_new": params["is_new"],
                }
            ]
        else:
            raise AssertionError(f"unexpected fixture api: {api}")
        names = fields.split(",")
        projected = [{name: row[name] for name in names} for row in rows]
        if self.mutate:
            self.mutate(api, params, projected)
        items = [[row[name] for name in names] for row in projected]
        content = json.dumps(
            {"code": 0, "data": {"fields": names, "items": items}}, ensure_ascii=False
        ).encode()
        return ProviderResult(tuple(names), tuple(tuple(row) for row in items), content)
