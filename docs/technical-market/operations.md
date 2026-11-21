# Market Analyst、OHLCV 与技术指标

## 架构

深研究图在 Risk 前并行执行三个固定职责 Agent：

```text
Fundamental ─┐
News ────────┼─> Risk Synthesis ─> Final
Market ──────┘
```

Market Analyst 只能调用 `get_stock_data` 与 `get_indicators`。证券代码、`as_of` 和
`as_of_window` 由程序注入；模型不能直接调用 Tushare、SQL 或 Python。任一专业 Agent
失败不会取消另外两个，Risk 必须把缺失上游列为数据缺口。

## 数据窗口

- `storage.price_bar` 保存最近90根实际日K，其中前30根只用于预热。
- `get_stock_data` 固定返回分析日及之前最近60根 OHLCV。
- `storage.technical_indicator` 保存最近30个交易日的11项指标，每项30条。
- `get_indicators` 单次允许选择1～8项，固定返回最近30个完整交易日。
- 所有源记录、计算版本、输入行情版本和血缘均追加保留；空值不补零。

指标参数：SMA(50)、EMA(10)、MACD(12,26,9)、Wilder RSI(14)、Bollinger(20,2)、
Wilder ATR(14)、VWMA(20)。`macdh = macd - macds`，布林带使用总体标准差。

## 运行

真实回填入口接受单个沪深交易所限定代码；每日 deployment 从 `APP_SCHEDULE_CODES` 逐只调用：

```powershell
python -m app.workflows.technical --migrate up
python -m app.workflows.technical --code 000001.SZ --as-of-date 2026-11-15
python -m app.workflows.technical --code 000001.SZ --verify-tools
```

工作流先复用本地证券与交易日历，缺失时才请求静态引用源；随后在候选日历范围内读取实际
存在的日K并选择最近90根。个股日线日期必须唯一且位于请求区间，但不会因个股停牌导致其
无法逐日覆盖交易所开市日而拒绝整段数据。

工作日收盘任务先发布最近行情，再自动调用本工作流重算全部11项指标。计算只使用数据库中最新
90根日K，发布最近30个指标日期。`get_indicators`除检查30个完整日期外，还要求指标最后日期等于
数据库最新行情日期；不一致时返回`coverage_incomplete`，不能用旧指标冒充最新结果。
