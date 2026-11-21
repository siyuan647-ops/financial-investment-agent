"""Register Step13 deployments. Credentials stay in the Worker environment."""

import argparse
import os

from prefect import deploy
from prefect.client.schemas.schedules import CronSchedule
from prefect.types.entrypoint import EntrypointType

from app.dataops.orchestration import validate_batch_codes
from app.workflows.fundamental import fundamental_five_domains
from app.workflows.orchestration import (
    close_incremental,
    corporate_rescan,
    financial_rescan,
    historical_backfill,
    manual_batch_replay,
    reference_refresh,
)

POOL = "a-share-local"
TAGS = ["dataops", "step13", "cn-a"]


def scheduled_codes():
    codes = [item.strip().upper() for item in os.environ.get("APP_SCHEDULE_CODES", "").split(",")]
    codes = [item for item in codes if item]
    if not codes:
        raise ValueError("APP_SCHEDULE_CODES_required_for_scheduled_deployments")
    validate_batch_codes(codes)
    return codes


def deployment_specs(codes: list[str]):
    common = {
        "work_pool_name": POOL,
        "concurrency_limit": 1,
        "tags": TAGS,
        "version": "step13-v1",
        "entrypoint_type": EntrypointType.MODULE_PATH,
    }
    return manual_deployment_specs() + [
        reference_refresh.to_deployment(
            name="daily-reference-refresh",
            schedule=CronSchedule(cron="0 17 * * *", timezone="Asia/Shanghai"),
            parameters={"codes": codes},
            description="Daily bounded security master and trading-calendar refresh",
            **common,
        ),
        close_incremental.to_deployment(
            name="after-close",
            schedule=CronSchedule(cron="30 18 * * 1-5", timezone="Asia/Shanghai"),
            parameters={"codes": codes, "revision_lookback_days": 7},
            description="After-close OHLCV publication followed by technical-indicator refresh",
            **(common | {"version": "step13-v2"}),
        ),
        financial_rescan.to_deployment(
            name="daily-revision-scan",
            schedule=CronSchedule(cron="30 19 * * *", timezone="Asia/Shanghai"),
            parameters={"codes": codes, "lookback_quarters": 8},
            description="Daily bounded financial revision scan",
            **common,
        ),
        fundamental_five_domains.to_deployment(
            name="daily-five-domain-fundamentals",
            schedule=CronSchedule(cron="45 19 * * *", timezone="Asia/Shanghai"),
            parameters={"codes": codes},
            description="Daily refresh of every fixed Fundamental Agent tool window",
            work_pool_name=POOL,
            concurrency_limit=1,
            tags=["dataops", "fundamental", "cn-a"],
            version="fundamental-v2",
            entrypoint_type=EntrypointType.MODULE_PATH,
        ),
        corporate_rescan.to_deployment(
            name="daily-corporate-scan",
            schedule=CronSchedule(cron="0 20 * * *", timezone="Asia/Shanghai"),
            parameters={"codes": codes},
            description="Daily dividend and industry scan",
            **common,
        ),
    ]


def manual_deployment_specs():
    common = {
        "work_pool_name": POOL,
        "concurrency_limit": 1,
        "tags": TAGS,
        "version": "step13-v1",
        "entrypoint_type": EntrypointType.MODULE_PATH,
    }
    return [
        historical_backfill.to_deployment(
            name="bounded-history",
            paused=True,
            description="Operator-triggered bounded history",
            **common,
        ),
        manual_batch_replay.to_deployment(
            name="failed-batch-replay",
            paused=True,
            description="Explicit failed batch replay",
            **common,
        ),
    ]


def register():
    return deploy(
        *deployment_specs(scheduled_codes()),
        work_pool_name=POOL,
        print_next_steps_message=False,
    )


def register_manual():
    return deploy(
        *manual_deployment_specs(),
        work_pool_name=POOL,
        print_next_steps_message=False,
    )


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--register", action="store_true")
    group.add_argument("--register-manual", action="store_true")
    args = parser.parse_args()
    register() if args.register else register_manual()


if __name__ == "__main__":
    main()
