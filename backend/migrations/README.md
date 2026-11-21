# 数据迁移目录

Step06 使用 `006_storage.up.sql` / `006_storage.down.sql` 与
`app.repositories.migrate`。迁移事务内加锁，记录文件 SHA-256，重复执行无操作，
已应用文件被改动则拒绝继续。TimescaleDB 扩展由 Step03 基础设施准备。

完整运行、回滚及备份说明见 [Step06](../../docs/step-06/storage.md)。
回滚删除 storage schema 中本步全部数据，必须显式使用 `--allow-data-loss`；
不删除 TimescaleDB 扩展、其他业务 schema 或 Prefect 数据库。

Step08 新增 `008_market.up/down.sql`，入口 `app.workflows.market --migrate up`。
依赖 006、007 已应用；回滚顺序为 008 → 007 → 006。008 down 仅删除窗口水位表，
保留行情、估值、复权、原始响应及任务历史；重新 up 后可重跑窗口恢复水位。
详见 [Step08](../../docs/step-08/market.md)。

Step12 新增 `012_quality.up/down.sql`，入口
`python -m app.workflows.quality migrate up`。迁移依赖 008，新增规则目录、追加式隔离事件、
完整率明细和扩展血缘图；down 仅删除 Step12 新表，不删除既有质量问题、原始响应、标准记录
或基础血缘。详见 [Step12](../../docs/step-12/quality.md)。

Step13 新增 `013_orchestration.up/down.sql`，入口
`python -m app.workflows.orchestration migrate up`。迁移依赖 012，新增可恢复批次、分区水位、
追加失败记录和权限告警；down 只删除 Step13 编排状态，不删除领域数据、原始响应、质量记录
或 `pipeline_run`。详见 [Step13](../../docs/step-13/operations.md)。

Step14 新增 `014_api.up/down.sql`，入口 `python -m app.repositories.api up`，保存 API
任务所有权、幂等键及 Prefect flow run 关联。

Step16 新增 `016_llm.up/down.sql`，入口 `python -m app.repositories.llm up`。迁移依赖
014，只保存模型调用元数据、关联 ID、状态、耗时、Token 与估算成本，不保存提示词、回答正文、
上游错误正文或密钥。down 会删除全部模型调用观测记录，执行前必须备份并获得明确授权。详见
[Step16](../../docs/step-16/development.md)。

Step21 新增 `021_indexing.up/down.sql`，入口 `python -m app.repositories.indexing up`。迁移依赖
020，保存版本化集合、可重试索引任务及 PG 可见状态。down 会删除这些对账记录和状态列，但不会删除
Milvus 集合；执行前必须备份并获得明确授权。详见
[Step21](../../docs/step-21/development.md)。

Step26 新增 `026_monitoring.up/down.sql`，入口为
`app.repositories.monitoring.migrate_monitoring`。迁移依赖 014，保存用户观察列表、规则、带输入
数据版本的事件及站内通知。down 会删除全部 Step26 运营记录，执行前必须备份并获得明确授权。
详见 [Step26](../../docs/step-26/development.md)。

Step27 新增 `027_text_monitoring.up/down.sql`，入口为
`python -m app.repositories.text_monitoring up`，依赖 020 文档表和 026 告警运营表。
保存来源能力、增量文本、同步结果及关注事件；down 会删除全部 Step27 采集状态和文本正文，执行前
必须备份并获得明确授权。详见 [Step27](../../docs/step-27/development.md)。

查询路由迭代新增 `031_query_routing.up/down.sql`，入口为
`python -m app.repositories.query_routing up`，依赖 016，仅扩展模型调用审计的 purpose 约束以记录
`route` 调用。down 会删除 route 审计记录，必须显式使用 `--allow-data-loss`。

五域基本面重构新增 `032_fundamental_domains.up/down.sql`，入口为
`python -m app.repositories.fundamental up` 或 `python -m app.workflows.fundamental --migrate up`。
迁移新增公司档案、盈利、资产负债、现金流和辅助基本面五张追加式业务表；每条记录显式保存
业务日期、报告期、公告日、事件生效日，并继续复用原始响应、版本和血缘表。down 会删除五域数据，
必须显式使用 `--allow-data-loss`。033、034 是同一入口管理的日期语义兼容迁移：不同源的公告日、
报告期和事件生效日分别保存，不施加跨数据类型不成立的通用先后关系。

节点级研究检查点使用 `036_research_checkpoints.up/down.sql`，入口为
`python -m app.repositories.research_checkpoints up`。依赖 019；部署新 Worker 前先执行 up。
每个任务的五个角色各保留一份已完成输出，写入时校验 PostgreSQL 租约所有者、有效期、取消标记和
执行代次。down 会删除所有节点检查点，需显式 `--allow-data-loss`。
