import os
import pytest


@pytest.fixture(scope="session", autouse=True)
def _set_dagster_home(tmp_path_factory):
    """Ensure DAGSTER_HOME points to a writable temp dir for integration tests.

    Matches project docs: Dagster components expect DAGSTER_HOME to be set.
    """
    home = tmp_path_factory.mktemp("dagster_home")
    os.environ["DAGSTER_HOME"] = str(home)
    return str(home)

