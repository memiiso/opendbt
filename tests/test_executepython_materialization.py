from pathlib import Path
from unittest import TestCase

from opendbt import OpenDbtProject


class TestOpenDbtProject(TestCase):
    RESOURCES_DIR = Path(__file__).parent.joinpath("resources")
    DBTTEST_DIR = RESOURCES_DIR.joinpath("dbttest")

    def test_run_executepython_materialization(self):
        dp = OpenDbtProject(project_dir=self.DBTTEST_DIR, profiles_dir=self.DBTTEST_DIR,
                            args=['--vars', 'dbt_custom_adapter: opendbt.examples.DuckDBAdapterV2Custom'])
        dp.run(command="run", args=['--select', 'my_executepython_model'])

    def test_run_executepython_dlt_pipeline(self):
        dp = OpenDbtProject(project_dir=self.DBTTEST_DIR, profiles_dir=self.DBTTEST_DIR,
                            args=['--vars', 'dbt_custom_adapter: opendbt.examples.DuckDBAdapterV2Custom'])
        dp.run(command="run", args=['--select', 'my_executepython_dlt_model'])

    def test_run_executepython_materialization_subprocess(self):
        dp = OpenDbtProject(project_dir=self.DBTTEST_DIR, profiles_dir=self.DBTTEST_DIR,
                            args=['--vars', 'dbt_custom_adapter: opendbt.examples.DuckDBAdapterV2Custom'])
        dp.run(command="run", args=['--select', 'my_executepython_model'], use_subprocess=True)
