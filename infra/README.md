# 本地运行

从项目根目录执行 `./infra/manage.ps1 init` 创建忽略的 `infra/.env`，设置 DataOps 数据源后运行 `./infra/manage.ps1 start`。停止使用 `./infra/manage.ps1 stop`；保留卷恢复使用 `./infra/manage.ps1 recover`；配置校验使用 `./infra/manage.ps1 validate`。

Compose 保留 PostgreSQL/TimescaleDB、Redis、Prefect、API、研究 Worker、Prometheus 和 Grafana。研究 Worker 使用 `research` profile 启动；DataOps Worker 负责采集与技术指标部署。当前五角色研究需要配置 Qwen 和数据源凭据；未配置时应报告服务不可用。

旧数据库迁移和持久化卷保留，清理代码不会删除已有业务数据。操作背景见 [DataOps 运行说明](../docs/step-13/operations.md)。
