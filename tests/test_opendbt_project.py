from pathlib import Path
from unittest import TestCase

from opendbt import OpenDbtProject


class TestOpenDbtProject(TestCase):
    RESOURCES_DIR = Path(__file__).parent.joinpath("resources")
    DBTTEST_DIR = RESOURCES_DIR.joinpath("dbttest")

    def test_run_compile(self):
        dp = OpenDbtProject(project_dir=self.DBTTEST_DIR, profiles_dir=self.DBTTEST_DIR)
        dp.run(command="compile")

    def test_run_run(self):
        dp = OpenDbtProject(project_dir=self.DBTTEST_DIR, profiles_dir=self.DBTTEST_DIR)
        dp.run(command="run", args=['--select', 'my_first_dbt_model+'], use_subprocess=True)

    def test_project_attributes(self):
        dp = OpenDbtProject(project_dir=self.DBTTEST_DIR, profiles_dir=self.DBTTEST_DIR)
        self.assertEqual(dp.project.project_name, "dbttest")
        self.assertEqual(dp.project.profile_name, "dbttest")
        self.assertEqual(dp.project_vars, {'dbt_custom_adapter': 'opendbt.examples.DuckDBAdapterV2Custom'})
