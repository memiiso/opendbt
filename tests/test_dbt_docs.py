import unittest

from base_dbt_test import BaseDbtTest
from opendbt import OpenDbtProject


class TestDbtDocs(BaseDbtTest):

    def test_run_docs_generate(self):
        dp = OpenDbtProject(project_dir=self.DBTCORE_DIR, profiles_dir=self.DBTCORE_DIR)
        # dp.run(command="run")
        dp.run(command="docs", args=['generate'])
        dp = OpenDbtProject(project_dir=self.DBTFINANCE_DIR, profiles_dir=self.DBTFINANCE_DIR)
        # dp.run(command="run")
        dp.run(command="docs", args=['generate'])
        index_html = self.DBTFINANCE_DIR.joinpath('target/index.html').read_text()
        # new html docs page
        self.assertTrue("tailwindcss" in str(index_html))
        self.assertTrue("vue.global.min.js" in str(index_html))

    @unittest.skip("reason for skipping")
    def test_run_docs_serve(self):
        dp = OpenDbtProject(project_dir=self.DBTFINANCE_DIR, profiles_dir=self.DBTFINANCE_DIR)
        dp.run(command="docs", args=['generate'])
        dp.run(command="docs", args=['serve'])
