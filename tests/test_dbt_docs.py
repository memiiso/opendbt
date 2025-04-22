import unittest

from dbt_common import semver

from base_dbt_test import BaseDbtTest
from opendbt import OpenDbtProject


class TestDbtDocs(BaseDbtTest):

    def test_run_docs_generate(self):
        dp = OpenDbtProject(project_dir=self.DBTCORE_DIR, profiles_dir=self.DBTCORE_DIR)
        # run to generate run_results.json and run_info.json file
        try:
            dp.run(command="build")
        except:
            pass
        dp.run(command="docs", args=['generate'])
        self.assertTrue(self.DBTCORE_DIR.joinpath('target/catalogl.json').exists())
        if self.DBT_VERSION > semver.VersionSpecifier.from_version_string("1.8.0"):
            self.assertTrue(self.DBTCORE_DIR.joinpath('target/run_info.json').exists())

        dp = OpenDbtProject(project_dir=self.DBTFINANCE_DIR, profiles_dir=self.DBTFINANCE_DIR)
        # dp.run(command="run")
        dp.run(command="docs", args=['generate'])
        index_html = self.DBTFINANCE_DIR.joinpath('target/index.html').read_text()
        # new html docs page
        self.assertTrue("tailwindcss" in str(index_html))
        self.assertTrue("vue.global.min.js" in str(index_html))
        self.assertTrue(self.DBTFINANCE_DIR.joinpath('target/catalogl.json').exists())

    @unittest.skip("reason for skipping")
    def test_run_docs_serve(self):
        dp = OpenDbtProject(project_dir=self.DBTFINANCE_DIR, profiles_dir=self.DBTFINANCE_DIR)
        dp.run(command="docs", args=['generate'])
        dp.run(command="docs", args=['serve'])
