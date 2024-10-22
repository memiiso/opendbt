from pathlib import Path
from unittest import TestCase

from opendbt import OpenDbtProject


class TestDbtSqlFluff(TestCase):
    RESOURCES_DIR = Path(__file__).parent.joinpath("resources")
    DBTTEST_DIR = RESOURCES_DIR.joinpath("dbttest")

    def test_run_sqlfluff_lint(self):
        dp = OpenDbtProject(project_dir=self.DBTTEST_DIR, profiles_dir=self.DBTTEST_DIR)
        dp.run(command="sqlfluff", args=['fix'])
        dp.run(command="sqlfluff", args=['lint'])

    def test_run_sqlfluff_fix(self):
        dp = OpenDbtProject(project_dir=self.DBTTEST_DIR, profiles_dir=self.DBTTEST_DIR)
        dp.run(command="sqlfluff", args=['fix'])
