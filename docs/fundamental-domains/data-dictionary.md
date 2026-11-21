# 五域基本面业务库

## 表与记录类型

| 业务域 | 表 | 记录类型 |
|---|---|---|
| 公司基础档案 | `storage.company_profile_record` | `security_profile`、`company_description`、`sw_industry`、`citic_industry`、`business_segment`、`top10_holder`、`top10_float_holder`、`company_key_metrics` |
| 利润表/盈利 | `storage.profitability_record` | `income_statement`、`financial_indicator` |
| 资产负债表 | `storage.balance_sheet_record` | `balance_sheet`、`balance_sheet_point_in_time` |
| 现金流量表 | `storage.cash_flow_record` | `cash_flow_statement`、`cash_flow_statement_cumulative_input`、`cash_flow_statement_single_quarter` |
| 辅助基本面 | `storage.fundamental_aux_record` | `dividend`、`dividend_yield` |

五张表都是追加式版本表，禁止 UPDATE/DELETE。原始返回保存在 `storage.raw_response`，版本元数据
保存在 `storage.record_version`，`storage.lineage` 连接原始响应和业务记录。

`payload.values` 只保存本次响应中真实非空字段：源返回空值不会补零，也不会作为已获取字段写入。
派生指标不覆盖直接字段，应在读取时使用确定性公式和输入 `data_version` 计算。

## 日期字段

| 字段 | 含义 |
|---|---|
| `data_date` | 该记录用于增量查询和排序的业务日期；公告优先，其次报告期/生效日/观察日 |
| `period_end` | 财报、主营构成、股东快照或分红所属报告期 |
| `announced_on` | 该源记录披露日期；不假定必然晚于所有事件日期 |
| `effective_on` | 行业进入、上市、交易、除息等生效日期 |
| `record_version.available_at` | 系统允许历史研究使用该版本的时间；当前采用保守的实际观察时间 |
| `record_version.fetched_at` | 系统实际抓取时间 |

不能使用 `fetched_at` 替代报告期，也不能使用 `period_end` 假装公告当日已经可知。

## 查询

API：

```text
GET /api/v1/fundamental/{domain}/{code}
```

支持 `record_type`、`start`、`end`、`as_of` 和 `limit`。`domain` 为：
`company_profile`、`profitability`、`balance_sheet`、`cash_flow`、`auxiliary`。

CLI 示例：

```powershell
python -m app.workflows.fundamental --query-domain profitability --codes 000001.SZ
```

采集示例：

```powershell
python -m app.workflows.fundamental --codes 000001.SZ --period 2024-12-31 --market-date 2026-09-30
```

每日 deployment `daily-five-domain-fundamentals` 使用最近已结束季度，并用运行日尝试更新日频股息率；
非交易日或上游未就绪时保留明确空表状态，不写零值。

## 盈利分析窗口

盈利域提供可显式运行的“两年年度＋最近四个单季”窗口：

```powershell
python -m app.workflows.fundamental --codes 000001.SZ --profitability-window --as-of-date 2026-11-15
```

- `income_statement_annual`、`financial_indicator_annual` 保存最近两个已结束年度的直接源记录。
- `income_statement_cumulative_input`、`financial_indicator_cumulative_input` 保存单季还原所需的累计输入。
- `income_statement_single_quarter` 保存最近四个已结束单季；Q1 使用累计恒等转换，Q2/Q3/Q4 使用当前累计减前一累计。
- `financial_indicator_single_quarter` 只保存可由单季金额确定性得到的金额或重算比率，不直接相减累计比率或同比百分比。
- 派生记录使用 `origin=calculated`，并保存 `formula` 与全部 `input_versions`；原始累计响应继续保留以便追溯。

## 公司指标、资产负债与现金流窗口

当前入口按确认范围仅允许 `000001.SZ`：

```powershell
python -m app.workflows.fundamental --codes 000001.SZ --fundamental-analysis-window --as-of-date 2026-11-15
```

- `company_key_metrics` 保存最新三个报告期的 EPS、扣非 EPS、稀释 EPS、每股净资产、ROE、加权 ROE、扣非 ROE和资产负债率；最新一期以 `is_primary=true` 标记。这些都是源直接披露的报告期累计指标。
- `balance_sheet_point_in_time` 保存最近四个季度末时点，不做期间相减。
- `cash_flow_statement_cumulative_input` 保存单季还原需要的累计源版本；`cash_flow_statement_single_quarter` 保存最近四个独立单季。只对期间流量字段相减，期初/期末现金余额不进入单季差分。

