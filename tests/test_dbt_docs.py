import unittest

from base_dbt_test import BaseDbtTest
from opendbt import OpenDbtProject


class TestDbtDocs(BaseDbtTest):

    def test_run_docs_generate(self):
        dp = OpenDbtProject(project_dir=self.DBTCORE_DIR, profiles_dir=self.DBTCORE_DIR)
        dp.run(command="docs", args=['generate'])

    @unittest.skip("reason for skipping")
    def test_run_docs_serve(self):
        dp = OpenDbtProject(project_dir=self.DBTCORE_DIR, profiles_dir=self.DBTCORE_DIR)
        dp.run(command="docs", args=['serve'])
