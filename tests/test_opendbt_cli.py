from pathlib import Path
from unittest import TestCase

from dbt.exceptions import DbtRuntimeError

from opendbt import OpenDbtProject, OpenDbtCli
from opendbt.airflow.callbacks import email_model_warnings_callback, email_test_errors_callback


class TestOpenDbtCli(TestCase):
    RESOURCES_DIR = Path(__file__).parent.joinpath("resources")
    DBTTEST_DIR = RESOURCES_DIR.joinpath("dbttest")

    def test_run_failed(self):
        dp = OpenDbtProject(project_dir=self.DBTTEST_DIR, profiles_dir=self.DBTTEST_DIR)
        with self.assertRaises(DbtRuntimeError) as context:
            dp.run(command="run", args=['--select', '+my_failing_dbt_model'])

        self.assertIn('Referenced column "non_exists_column" not found in FROM clause', str(context.exception.msg))

    def test_cli_attributes(self):
        dp = OpenDbtCli(project_dir=self.DBTTEST_DIR)
        self.assertEqual(dp.project.project_name, "dbttest")
        self.assertEqual(dp.project.profile_name, "dbttest")
        self.assertEqual(dp.project_vars['dbt_custom_adapter'], 'opendbt.examples.DuckDBAdapterV2Custom')

    def test_cli_callbacks(self):
        dp = OpenDbtCli(project_dir=self.DBTTEST_DIR)
        self.assertIn(email_model_warnings_callback, dp.project_callbacks)
        self.assertIn(email_test_errors_callback, dp.project_callbacks)
        # TODO RUN model with callback!
