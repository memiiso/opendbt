from base_dbt_test import BaseDbtTest
from opendbt import OpenDbtProject


class TestOpenDbtProject(BaseDbtTest):

    def test_run_executedlt_materialization(self):
        dp = OpenDbtProject(project_dir=self.DBTCORE_DIR, profiles_dir=self.DBTCORE_DIR,
                            args=['--vars', 'dbt_custom_adapter: opendbt.examples.DuckDBAdapterV2Custom'])
        dp.run(command="run", args=['--select', 'my_executedlt_model'])
