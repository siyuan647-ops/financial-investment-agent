# Step 13 Prefect 回填、增量、补数与恢复

状态：待验收。本文只覆盖 Step 13，不提供 Step 14 HTTP 管理入口。

## 部署与凭据边界

- 工作池固定为 `a-share-local`，Worker 并发上限为 1；每个 deployment 另设并发上限 1。
- Worker 镜像由 `infra/Dockerfile.worker` 构建，代码与迁移随镜像固定，不从宿主机临时挂载源码。
- process Worker 的 deployment 使用可导入模块入口；批量注册时显式传入工作池，兼容当前 Prefect 的部署校验。
- 数据库地址、供应商 URL/Token 只由 Worker 环境变量引用，不进入 flow 参数、Prefect 结果或部署描述。
- `APP_SCHEDULE_CODES` 是逗号分隔的已确认证券范围；注册周期任务时不能为空，最多 30 只。

应用迁移并注册 deployments：

```powershell
docker compose --env-file infra/.env -f infra/compose.yaml build prefect-worker
docker compose --env-file infra/.env -f infra/compose.yaml up -d
docker compose --env-file infra/.env -f infra/compose.yaml exec -T prefect-worker python -m app.workflows.orchestration migrate up
docker compose --env-file infra/.env -f infra/compose.yaml exec -T prefect-worker python -m app.workflows.deployments --register
```

注册内容：

| Flow / deployment | 调度 | 作用 |
| --- | --- | --- |
| `historical-backfill/bounded-history` | 手动、默认暂停 | 按证券和最多 31 个自然日拆分历史回填 |
| `reference-refresh/daily-reference-refresh` | 每日 17:00 | 更新证券主数据及当前日前后交易日历 |
| `close-incremental/after-close` | 工作日 18:30，Asia/Shanghai | 回扫最近7日行情与估值；成功后为每只配置证券补齐90根日K并重算最近30日技术指标 |
| `financial-rescan/daily-revision-scan` | 每日 19:30 | 回扫最近8个报告期，捕获延迟披露及修订 |
| `fundamental-five-domains/daily-five-domain-fundamentals` | 每日 19:45 | 更新Fundamental四个工具要求的公司档案、盈利、资产负债和现金流固定窗口 |
| `corporate-rescan/daily-corporate-scan` | 每日 20:00 | 分红与行业增量检查 |
| `manual-batch-replay/failed-batch-replay` | 手动、默认暂停 | 按 `batch_id` 独立重放失败批次 |

全部周期调度均使用 `Asia/Shanghai`。调度时刻只是检查入口，不代表供应商必定已就绪。行情缺口在收盘后增量中记录为
`provider_not_ready`，不会推进水位；后续同一扫描周期或人工补跑可继续。

## 状态、锁与水位

- `storage.orchestration_batch` 保存确定性幂等键、参数、尝试次数和最终状态。
- PostgreSQL session advisory lock 覆盖整个子批次；进程退出后锁由数据库自动释放，下一 Worker 可接管仍为 `running` 的批次。
- `storage.partition_watermark` 只和 `succeeded` 状态在同一事务写入，并拒绝日期游标倒退。
- `storage.orchestration_failure` 追加保存每次失败；`storage.orchestration_alert` 保存权限错误告警。
- 自动重试只用于 transport timeout/error、429/源 5xx、熔断和供应商未就绪，最多追加一次；权限和未知错误不自动重试。

查看失败清单：

```powershell
docker compose --env-file infra/.env -f infra/compose.yaml exec -T prefect-worker python -m app.workflows.orchestration failures
docker compose --env-file infra/.env -f infra/compose.yaml exec -T prefect-worker python -m app.workflows.orchestration failures --retryable-only
```

权限错误必须先修复 Worker 凭据并重启 Worker，再通过 `batch_id` 手动运行
`manual-batch-replay/failed-batch-replay`。不得通过反复重启制造无限重试。

## 恢复与验证

中断恢复顺序：确认数据库与 Prefect 就绪 → 确认旧 Worker 已退出 → 查看失败清单和
Prefect flow run → 对可重试失败重新触发原 deployment，或按批次手动重放。成功批次再次
投递会命中缓存状态，不重复执行领域写入。

离线及隔离库开发验证：

```powershell
uv run --group dev pytest backend/tests/test_orchestration.py
./infra/verify-orchestration.ps1
```

隔离验证覆盖任务锁、重复投递、中断后接管、瞬时失败重试、权限错误停止、独立重放、
水位不倒退、失败历史及迁移 down/up。它不等同于真实供应商、进程强杀或生产灾备验收。
