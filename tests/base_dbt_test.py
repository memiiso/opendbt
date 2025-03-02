from pathlib import Path
from unittest import TestCase

from dbt.version import get_installed_version as get_dbt_version

from opendbt import OpenDbtCli


class BaseDbtTest(TestCase):
    TESTS_ROOT = Path(__file__).parent
    PROJECT_ROOT = TESTS_ROOT.parent
    RESOURCES_DIR = TESTS_ROOT.joinpath("resources")
    DBTCORE_DIR = RESOURCES_DIR.joinpath("dbtcore")
    DBTFINANCE_DIR = RESOURCES_DIR.joinpath("dbtfinance")
    DBT_VERSION = get_dbt_version()

    @classmethod
    def setUpClass(cls):
        BaseDbtTest.PROJECT_ROOT.joinpath("dev.duckdb").unlink(missing_ok=True)
        BaseDbtTest.RESOURCES_DIR.joinpath("dev.duckdb").unlink(missing_ok=True)

        dpf = OpenDbtCli(project_dir=BaseDbtTest.DBTFINANCE_DIR, profiles_dir=BaseDbtTest.DBTFINANCE_DIR)
        dpc = OpenDbtCli(project_dir=BaseDbtTest.DBTCORE_DIR, profiles_dir=BaseDbtTest.DBTCORE_DIR)
        dpf.invoke(args=["clean"])
        dpc.invoke(args=["clean"])

    def setUp(self):
        # Setup actions to be performed before each test
        BaseDbtTest.PROJECT_ROOT.joinpath("dev.duckdb").unlink(missing_ok=True)
        BaseDbtTest.RESOURCES_DIR.joinpath("dev.duckdb").unlink(missing_ok=True)