from pathlib import Path
from unittest import TestCase

from opendbt import OpenDbtProject


class TestOpenDbtProject(TestCase):
    RESOURCES_DIR = Path(__file__).parent.joinpath("resources")
    DBTTEST_DIR = RESOURCES_DIR.joinpath("dbttest")

    def test_run_execute_materialization(self):
        dp = OpenDbtProject(project_dir=self.DBTTEST_DIR, profiles_dir=self.DBTTEST_DIR)
        dp.run(command="run", args=['--select', 'my_execute_dbt_model'])
