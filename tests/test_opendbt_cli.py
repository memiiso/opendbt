from pathlib import Path
from unittest import TestCase

from dbt.exceptions import DbtRuntimeError

from opendbt import OpenDbtProject


class TestOpenDbtCli(TestCase):
    RESOURCES_DIR = Path(__file__).parent.joinpath("resources")
    DBTTEST_DIR = RESOURCES_DIR.joinpath("dbttest")

    def test_run_failed(self):
        dp = OpenDbtProject(project_dir=self.DBTTEST_DIR, profiles_dir=self.DBTTEST_DIR)
        with self.assertRaises(DbtRuntimeError) as context:
            dp.run(command="run", args=['--select', '+my_failing_dbt_model'])

        self.assertIn('Referenced column "non_exists_column" not found in FROM clause', str(context.exception.msg))
