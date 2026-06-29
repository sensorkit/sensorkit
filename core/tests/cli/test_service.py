import pytest
from asyncclick.testing import CliRunner

from sensorkit.cli.service import service_group


@pytest.mark.asyncio
async def test_service_run_imports_modules_for_explicit_spec(monkeypatch):
    calls: list[str] = []

    async def fake_load_config():
        raise FileNotFoundError

    async def fake_import_modules(*, fail_policy="error"):
        calls.append(f"import:{fail_policy}")

    def fake_from_spec(spec, load_file=False):
        calls.append(f"from_spec:{spec}:{load_file}")
        return object()

    async def fake_run_services(entrypoints, max_restarts):
        calls.append(f"run_services:{sorted(entrypoints)}:{max_restarts}")

    monkeypatch.setattr("sensorkit.api.load_config", fake_load_config)
    monkeypatch.setattr("sensorkit.api.import_modules", fake_import_modules)
    monkeypatch.setattr(
        "sensorkit.api.entrypoint.ServiceEntrypoint.from_spec",
        fake_from_spec,
    )
    monkeypatch.setattr("sensorkit.api.entrypoint.run_services", fake_run_services)

    result = await CliRunner().invoke(
        service_group,
        ["run", "satsim_service", "sensorkit.satsim.service"],
        standalone_mode=False,
    )

    assert result.exit_code == 0
    assert calls == [
        "import:error",
        "from_spec:sensorkit.satsim.service:True",
        "run_services:['satsim_service']:0",
    ]
