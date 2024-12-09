from pathlib import Path
from unittest import TestCase

from opendbt import OpenDbtProject


class TestOpenDbtProject(TestCase):
    RESOURCES_DIR = Path(__file__).parent.joinpath("resources")
    DBTTEST_DIR = RESOURCES_DIR.joinpath("dbttest")

    def test_run_executedlt_materialization(self):
        dp = OpenDbtProject(project_dir=self.DBTTEST_DIR, profiles_dir=self.DBTTEST_DIR,
                            args=['--vars', 'dbt_custom_adapter: opendbt.examples.DuckDBAdapterV2Custom'])
        dp.run(command="run", args=['--select', 'my_executedlt_model'])
