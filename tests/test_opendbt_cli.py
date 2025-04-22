import json
import unittest

from dbt.exceptions import DbtRuntimeError
from dbt_common import semver

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
                dp.invoke(args=["test", '--select', 'my_core_table1 my_first_dbt_model', "--profiles-dir",
                                dp.project_dir.as_posix()])
            except:
                pass

        self.assertIn('DBT callback `email_dbt_test_callback` called', str(cm.output))
        self.assertIn('Callback email sent', str(cm.output))
        # self.assertIn('dbt test', str(cm.output))

    def test_cli_run_models(self):
        dp = OpenDbtCli(project_dir=self.DBTCORE_DIR)
        dp.invoke(args=['run', "--exclude", "my_failing_dbt_model", "--profiles-dir", dp.project_dir.as_posix()])

    def test_cli_run_cross_project_ref_models(self):
        dpf = OpenDbtCli(project_dir=self.DBTFINANCE_DIR)
        dpf.invoke(
            args=['run', '--select', '+my_cross_project_ref_model', "--profiles-dir", dpf.project_dir.as_posix()])

    @unittest.skipIf(BaseDbtTest.DBT_VERSION < semver.VersionSpecifier.from_version_string("1.8.0"), 'skip')
    def test_cli_run_result(self):
        run_info = self.DBTCORE_DIR.joinpath("target/run_info.json")
        if run_info.exists():
            run_info.write_text('')
        dp = OpenDbtProject(project_dir=self.DBTCORE_DIR, profiles_dir=self.DBTCORE_DIR)
        dp.run(command="build", args=['--select', 'my_core_table1'])
        data = json.loads(run_info.read_text())
        self.assertEqual(1, len(data['nodes']))
        self.assertIn("model.dbtcore.my_core_table1", data['nodes'])
        print(json.dumps(data, indent=4))

        dp.run(command="build", args=['--select', 'my_executesql_dbt_model'])
        data = json.loads(run_info.read_text())
        self.assertEqual(2, len(data['nodes']))
        self.assertIn("model.dbtcore.my_executesql_dbt_model", data['nodes'])
        print(json.dumps(data, indent=4))
