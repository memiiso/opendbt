from pathlib import Path
from unittest import TestCase

from dbt.version import get_installed_version as get_dbt_version


class BaseDbtTest(TestCase):
    RESOURCES_DIR = Path(__file__).parent.joinpath("resources")
    DBTCORE_DIR = RESOURCES_DIR.joinpath("dbtcore")
    DBTFINANCE_DIR = RESOURCES_DIR.joinpath("dbtfinance")
    DBT_VERSION = get_dbt_version()
