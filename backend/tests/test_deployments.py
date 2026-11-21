from prefect.types.entrypoint import EntrypointType

from app.workflows.deployments import deployment_specs


def test_process_worker_deployments_use_importable_module_entrypoints():
    deployments = deployment_specs(["600519.SH"])
    assert deployments
    assert all(item.entrypoint_type == EntrypointType.MODULE_PATH for item in deployments)
    assert any(item.name == "daily-five-domain-fundamentals" for item in deployments)
    by_name = {item.name: item for item in deployments}
    assert by_name["after-close"].version == "step13-v2"
    assert by_name["daily-five-domain-fundamentals"].version == "fundamental-v2"
