from base_dbt_test import BaseDbtTest
from opendbt import OpenDbtProject


class TestOpenDbtProject(BaseDbtTest):
    def test_run_compile(self):
        dp = OpenDbtProject(project_dir=self.DBTCORE_DIR, profiles_dir=self.DBTCORE_DIR)
        dp.run(command="compile")

    def test_run_run(self):
        dp = OpenDbtProject(project_dir=self.DBTCORE_DIR, profiles_dir=self.DBTCORE_DIR)
        dp.run(command="run", args=['--select', 'my_first_dbt_model+', "--exclude", "my_failing_dbt_model"],
               use_subprocess=True)

    def test_project_attributes(self):
        dp = OpenDbtProject(project_dir=self.DBTCORE_DIR, profiles_dir=self.DBTCORE_DIR)
        self.assertEqual(dp.project.project_name, "dbtcore")
        self.assertEqual(dp.project.profile_name, "dbtcore")
        self.assertEqual(dp.project_vars['dbt_custom_adapter'], 'opendbt.examples.DuckDBAdapterV2Custom')
