import json

from base_dbt_test import BaseDbtTest
from opendbt import OpenDbtProject


class TestOpenDbtMesh(BaseDbtTest):

    def test_run_cross_project(self):
        dp = OpenDbtProject(project_dir=self.DBTCORE_DIR, profiles_dir=self.DBTCORE_DIR)
        dp.run(command="compile")

        dp = OpenDbtProject(project_dir=self.DBTFINANCE_DIR, profiles_dir=self.DBTFINANCE_DIR)
        dp.run(command="compile")

        manifest = json.loads(self.DBTFINANCE_DIR.joinpath("target/manifest.json").read_text())
        model = manifest.get("nodes").get("model.dbtfinance.my_cross_project_ref_model", {})
        print(model)
        self.assertEqual(model["database"], 'dev')
        self.assertEqual(model['schema'], 'finance')
        self.assertEqual(model['name'], 'my_cross_project_ref_model')

        model = manifest.get("nodes").get("model.dbtcore.my_core_table1", {})
        self.assertEqual(model['database'], 'dev')
        self.assertEqual(model['schema'], 'core')
        self.assertEqual(model['name'], 'my_core_table1')
        print(model)
