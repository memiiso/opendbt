import unittest
from pathlib import Path
from unittest import TestCase

from opendbt.project import OpenDbtProject


class TestDbtDocs(TestCase):
    RESOURCES_DIR = Path(__file__).parent.joinpath("resources")
    DBTTEST_DIR = RESOURCES_DIR.joinpath("dbttest")

    def test_run_docs_generate(self):
        dp = OpenDbtProject(project_dir=self.DBTTEST_DIR, profiles_dir=self.DBTTEST_DIR)
        dp.run(command="docs", args=['generate'])

    @unittest.skip("reason for skipping")
    def test_run_docs_serve(self):
        dp = OpenDbtProject(project_dir=self.DBTTEST_DIR, profiles_dir=self.DBTTEST_DIR)
        dp.run(command="docs", args=['serve'])
