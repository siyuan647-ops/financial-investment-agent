# A 股 DataOps 与五角色投研

本项目面向**单只沪深 A 股**研究：DataOps 采集并发布结构化数据，Fundamental、News、Market 并行取证，Investment 汇总分析，Risk 终审风险。用户通过研究任务入口查看进度、报告和数据缺口。当前入口接收证券代码或准确名称，以及可选的 `as_of`。


```text
Tushare 兼容源 → DataOps 原始留存、标准化、质量检查、发布
                                      ├─ Fundamental：公司档案、盈利、资产负债、现金流
有界新闻/公告来源 ──────────────────────┼─ News：新闻、公告、业绩事件
已发布日线与指标 ────────────────────────└─ Market：行情、技术指标
                                      ↓
                               Investment：投资初评
                                      ↓
                               Risk：风险终审 → 研究报告
```

三个专业角色分别使用固定权限的九个业务工具；Investment 和 Risk 不调用工具。工具结果保留证券、期间、单位、来源、数据版本、Evidence、覆盖状态及缺口。News 使用已接入的近七日有界提供方接口；空窗口、来源失败和未核实内容需分别呈现。具体职责见[五角色工作流](docs/multi-agent/development.md)和[业务工具说明](docs/business-tools/development.md)。

## 当前能力与限制

- DataOps 覆盖证券与交易日历、日行情、估值、财务、公司行为、五域基本面和技术指标，保留原始响应、质量、版本、水位及补数能力。PE-TTM 和静态 PB 由 DataOps 发布到 `company_key_metrics`，供 `get_company_profile` 读取；实际证券与时间窗口的覆盖仍需逐项核验。
- 研究任务支持提交、查询、取消及节点检查点恢复。已完成角色的输出和调用元数据可随任务保存；中断后继续属于同一任务，不代表每个角色都已成功。


## 本地运行

项目使用 Python 3.13、FastAPI、LangGraph、Prefect、PostgreSQL/TimescaleDB、Redis、Qwen、React/TypeScript 和 Docker Compose。凭据只放在本地环境文件，参考 [.env.example](.env.example) 与 [infra/.env.example](infra/.env.example)，不要提交真实密钥。

在项目根目录运行：

```powershell
./infra/manage.ps1 init       # 仅首次：生成本地 infra/.env；已有文件不会覆盖
# 在 infra/.env 配置数据源和 Qwen
./infra/manage.ps1 validate
./infra/manage.ps1 start      # 启动基础服务与 API
docker compose --project-name a-share-step03 --env-file infra/.env -f infra/compose.yaml --profile research up -d --build research-worker
./infra/manage.ps1 status
```

DataOps 定时部署由 `python -m app.workflows.deployments --register` 注册，需先设置 `APP_SCHEDULE_CODES`；研究任务由 `app.workers.research` 消费。更多操作见 [DataOps 运行说明](docs/step-13/operations.md)和[基础设施说明](infra/README.md)。前端在 `frontend/` 执行 `npm ci`、`npm run dev`；Vite 将 `/api` 代理到本地 API。

## 主要接口

| 接口 | 用途 |
| --- | --- |
| `POST /api/v1/analysis/query` | 提交单股票五角色研究，输入 `target`、可选 `as_of` |
| `GET /api/v1/analysis/research/{task_id}` | 查询任务状态、事件、角色产物与报告 |
| `POST /api/v1/analysis/research/{task_id}/cancel` | 请求取消任务 |
| `/api/v1/data/*`、`/api/v1/fundamental/*` | 查询已发布 DataOps 数据及其状态 |
| `/health/live`、`/health/ready`、`/metrics` | 运行与依赖状态 |

## 评测与开发检查

[五角色 20 条单股票评测集](backend/evals/FIVE_AGENT_20.md)覆盖 20 只不同候选股票，已有用例与评分规则。其 `as_of`、工具快照、标准事实和 Evidence ID 仍待 DataOps 采集与复核；核心评分包括任务完成、关键事实与数值、证据支持、时点正确、五角色传递、风险与不确定性。

开发检查：`.venv/Scripts/ruff.exe check backend`、`.venv/Scripts/pytest.exe -q`，以及在 `frontend` 执行 `npm run build`。这些检查不等于用户验收。当前需求以 [prd.md](prd.md) 为准；
