# 五域基本面数据能力验证（000001.SZ）

日期：2026-11-14（Asia/Shanghai）  
数据源：当前配置的 Tushare 兼容中转服务  
范围：单证券、2024-12-31 报告期、2026-09-30 估值日期；不做全目录探测。

## 已验证并接入

| 域 | 接口 | 实测行数 | 接入记录类型 |
|---|---|---:|---|
| 公司档案 | `stock_basic` | 1 | `security_profile` |
| 公司档案 | `stock_company` | 1 | `company_description` |
| 公司档案 | `index_member_all` | 1 | `sw_industry` |
| 公司档案 | `ci_index_member` | 1 | `citic_industry` |
| 公司档案 | `fina_mainbz` | 26 | `business_segment` |
| 公司档案 | `top10_holders` | 10 | `top10_holder` |
| 公司档案 | `top10_floatholders` | 10 | `top10_float_holder` |
| 盈利 | `income` | 1 | `income_statement` |
| 盈利 | `fina_indicator` | 1 | `financial_indicator` |
| 资产负债 | `balancesheet` | 1 | `balance_sheet` |
| 现金流 | `cashflow` | 2 | `cash_flow_statement`（保留两个源版本） |
| 辅助基本面 | `dividend` | 98 | `dividend` |
| 辅助基本面 | `daily_basic` | 1 | `dividend_yield` |

首次发布共 154 条：公司档案 50、盈利 2、资产负债 1、现金流 2、辅助基本面 99。
相同参数完整重跑 `records=154, inserted=0`，证明业务版本幂等；原始响应仍独立留存并建立血缘。

## 未作为首批可采集能力

| 接口/需求 | 实测状态 | 处理 |
|---|---|---|
| `stk_managers` | 业务码 400 | 不建“已获取”记录；公司负责人仅使用 `stock_company` 已返回字段 |
| `stk_holdertrade` | 业务码 400 | 不提供 `get_insider_transactions` 成功假象 |
| `forecast` | 业务码 400 | 不写空业绩预告 |
| `express` | 业务码 400 | 不写空业绩快报 |
| `report_rc` | 业务码 503 | 不生成券商一致预期 |
| NIM、成本收入比、不良率、拨备覆盖率、资本充足率 | 无已验证 A 股直接字段 | 不进入结构化域表 |
| 商业模式、上下游、集团协同、护城河、牌照、政策和风险结论 | 需要文档解析/分析 | 不进入直接事实表 |

接口空表、业务错误、权限不足和源失败必须分别表达，均不得转换成数值零或“没有事件”。

