from base_dbt_test import BaseDbtTest
from opendbt import OpenDbtProject


class TestOpenDbtProject(BaseDbtTest):

    def test_run_executepython_materialization(self):
        dp = OpenDbtProject(project_dir=self.DBTCORE_DIR, profiles_dir=self.DBTCORE_DIR,
                            args=['--vars', 'dbt_custom_adapter: opendbt.examples.DuckDBAdapterV2Custom'])
        dp.run(command="run", args=['--select', 'my_executepython_model'])

    def test_run_executepython_dlt_pipeline(self):
        dp = OpenDbtProject(project_dir=self.DBTCORE_DIR, profiles_dir=self.DBTCORE_DIR,
                            args=['--vars', 'dbt_custom_adapter: opendbt.examples.DuckDBAdapterV2Custom'])
        dp.run(command="run", args=['--select', 'my_executepython_dlt_model'])

    def test_run_executepython_materialization_subprocess(self):
        dp = OpenDbtProject(project_dir=self.DBTCORE_DIR, profiles_dir=self.DBTCORE_DIR,
                            args=['--vars', 'dbt_custom_adapter: opendbt.examples.DuckDBAdapterV2Custom'])
        dp.run(command="run", args=['--select', 'my_executepython_model'], use_subprocess=True)
