"""Live infrastructure tests from the host. Skipped unless AS_INTEGRATION=1 (needs the Docker stack running).

PowerShell:  $env:AS_INTEGRATION="1"; .venv\\Scripts\\python -m pytest -q tests/integration -k "not hdfs"
HDFS from the host needs `127.0.0.1 datanode` in the hosts file; inside Docker use
`docker compose run --rm tools` (scripts/check_infra.py), which runs the same checks.
"""
import os

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("AS_INTEGRATION") != "1",
                                reason="set AS_INTEGRATION=1 with the Docker stack running")


@pytest.fixture(scope="module")
def checks():
    import importlib.util, pathlib
    path = pathlib.Path(__file__).resolve().parents[2] / "scripts" / "check_infra.py"
    spec = importlib.util.spec_from_file_location("check_infra", path)
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def s():
    from accesssentinel.settings import Settings
    return Settings()


@pytest.mark.parametrize("name", ["kafka", "hdfs", "postgres", "redis"])
def test_live(checks, s, name):
    detail = getattr(checks, f"check_{name}")(s, 30.0)
    assert "OK" in detail
