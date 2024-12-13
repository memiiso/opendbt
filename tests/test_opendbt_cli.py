from pathlib import Path
from unittest import TestCase

from dbt.exceptions import DbtRuntimeError

from opendbt import OpenDbtProject, OpenDbtCli
from opendbt.examples import email_dbt_test_callback


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
        self.assertIn('dbt_custom_adapter', dp.project_vars)
        self.assertIn('dbt_callbacks', dp.project_vars)
        self.assertEqual(dp.project_vars['dbt_custom_adapter'], 'opendbt.examples.DuckDBAdapterV2Custom')

    def test_cli_callbacks(self):
        dp = OpenDbtCli(project_dir=self.DBTTEST_DIR)
        self.assertIn(email_dbt_test_callback, dp.project_callbacks)

        with self.assertLogs('dbtcallbacks', level='INFO') as cm:
            dp.invoke(args=["test", '--select', 'my_first_dbt_model'])

        self.assertIn('DBT callback `email_dbt_test_callback` called', str(cm.output))
        self.assertIn('Airflow send_email failed! this is expected for unit testing!', str(cm.output))
        # self.assertIn('dbt test', str(cm.output))
