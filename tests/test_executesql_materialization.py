from base_dbt_test import BaseDbtTest
from opendbt import OpenDbtProject


class TestOpenDbtProject(BaseDbtTest):

    def test_run_executesql_materialization(self):
        dp = OpenDbtProject(project_dir=self.DBTCORE_DIR, profiles_dir=self.DBTCORE_DIR)
        dp.run(command="run", args=['--select', 'my_executesql_dbt_model'])
