from prefect import flow, task

@task
def ping():
    return "step03-ok"

@flow(log_prints=False)
def infrastructure_smoke():
    return ping()

if __name__ == "__main__":
    infrastructure_smoke.from_source(
        source="/opt/smoke", entrypoint="flow.py:infrastructure_smoke"
    ).deploy(name="step03-smoke", work_pool_name="a-share-local")

