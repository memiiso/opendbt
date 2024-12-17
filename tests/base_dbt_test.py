from pathlib import Path
from unittest import TestCase

from dbt.version import get_installed_version as get_dbt_version

from opendbt import OpenDbtCli


class BaseDbtTest(TestCase):
    RESOURCES_DIR = Path(__file__).parent.joinpath("resources")
    DBTCORE_DIR = RESOURCES_DIR.joinpath("dbtcore")
    DBTFINANCE_DIR = RESOURCES_DIR.joinpath("dbtfinance")
    DBT_VERSION = get_dbt_version()

    @classmethod
    def setUpClass(cls):
        dpf = OpenDbtCli(project_dir=BaseDbtTest.DBTFINANCE_DIR, profiles_dir=BaseDbtTest.DBTFINANCE_DIR)
        dpc = OpenDbtCli(project_dir=BaseDbtTest.DBTCORE_DIR, profiles_dir=BaseDbtTest.DBTCORE_DIR)
        dpf.invoke(args=["clean"])
        dpc.invoke(args=["clean"])
