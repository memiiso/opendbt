from dbt.exceptions import DbtRuntimeError

from base_dbt_test import BaseDbtTest
from opendbt import OpenDbtProject, OpenDbtCli
from opendbt.examples import email_dbt_test_callback


class TestOpenDbtCli(BaseDbtTest):

    def test_run_failed(self):
        dp = OpenDbtProject(project_dir=self.DBTCORE_DIR, profiles_dir=self.DBTCORE_DIR)
        with self.assertRaises(DbtRuntimeError) as context:
            dp.run(command="run", args=['--select', '+my_failing_dbt_model'])

        self.assertIn('Referenced column "non_exists_column" not found in FROM clause', str(context.exception.msg))

    def test_cli_attributes(self):
        dp = OpenDbtCli(project_dir=self.DBTCORE_DIR)
        self.assertEqual(dp.project.project_name, "dbtcore")
        self.assertEqual(dp.project.profile_name, "dbtcore")
        self.assertIn('dbt_custom_adapter', dp.project_vars)
        self.assertIn('dbt_callbacks', dp.project_vars)
        self.assertEqual(dp.project_vars['dbt_custom_adapter'], 'opendbt.examples.DuckDBAdapterV2Custom')

    def test_cli_callbacks(self):
        dp = OpenDbtCli(project_dir=self.DBTCORE_DIR)
        self.assertIn(email_dbt_test_callback, dp.project_callbacks)

        with self.assertLogs('dbtcallbacks', level='INFO') as cm:
            try:
                dp.invoke(args=["test", '--select', 'my_first_dbt_model', "--profiles-dir", dp.project_dir.as_posix()])
            except:
                pass

        self.assertIn('DBT callback `email_dbt_test_callback` called', str(cm.output))
        self.assertIn('Callback email sent', str(cm.output))
        # self.assertIn('dbt test', str(cm.output))

    def test_cli_run_models(self):
        dp = OpenDbtCli(project_dir=self.DBTCORE_DIR)
        dp.invoke(args=['run', '--select', 'my_first_dbt_model+', "--exclude", "my_failing_dbt_model", "--profiles-dir",
                        dp.project_dir.as_posix()])

    def test_cli_run_cross_project_ref_models(self):
        dpf = OpenDbtCli(project_dir=self.DBTFINANCE_DIR)
        dpc = OpenDbtCli(project_dir=self.DBTCORE_DIR)
        dpc.invoke(args=['run', '--select', 'my_core_table1', "--profiles-dir", dpc.project_dir.as_posix()])
        dpf.invoke(args=['run', '--select', 'my_cross_project_ref_model', "--profiles-dir", dpf.project_dir.as_posix()])
