from base_dbt_test import BaseDbtTest
from opendbt import OpenDbtProject


class TestDbtSqlFluff(BaseDbtTest):

    def test_run_sqlfluff_lint(self):
        dp = OpenDbtProject(project_dir=self.DBTCORE_DIR, profiles_dir=self.DBTCORE_DIR)
        dp.run(command="sqlfluff", args=['fix'])
        dp.run(command="sqlfluff", args=['lint'])

    def test_run_sqlfluff_fix(self):
        dp = OpenDbtProject(project_dir=self.DBTCORE_DIR, profiles_dir=self.DBTCORE_DIR)
        dp.run(command="sqlfluff", args=['fix'])
