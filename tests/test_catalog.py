import unittest

from base_dbt_test import BaseDbtTest
from opendbt import OpenDbtProject
from opendbt.catalog import OpenDbtCatalog


class TestOpenDbtCatalog(BaseDbtTest):

    def test_catalog_loading(self):
        dp = OpenDbtProject(project_dir=self.DBTFINANCE_DIR, profiles_dir=self.DBTFINANCE_DIR)
        dp.run(command="docs", args=['generate'])
        catalog = OpenDbtCatalog(
            manifest_path=self.DBTFINANCE_DIR.joinpath('target/manifest.json'),
            catalog_path=self.DBTFINANCE_DIR.joinpath('target/catalog.json'))
        self.assertIn("model.dbtfinance.my_cross_project_ref_model", catalog.nodes.keys())
        self.assertIn("model.dbtcore.my_core_table1", catalog.nodes.keys())
        # print(extractor.nodes.get("model.dbtcore.my_core_table1").columns)
        model1 = catalog.nodes.get("model.dbtfinance.my_cross_project_ref_model")
        model1_schema = model1.db_schema_dict(include_parents=True)
        self.assertIn("dev", model1_schema)
        self.assertIn("finance", model1_schema["dev"])
        self.assertIn("my_core_table1", model1_schema["dev"]["core"])
        self.assertIn("my_cross_project_ref_model", model1_schema["dev"]["finance"])
        # self.assertIn("row_data", model1_schema["dev"]["main"]['my_core_table1'])

        self.assertIn("num_rows", model1.populate_lineage(catalog.tables2nodes))
        self.assertIn("row_data", model1.populate_lineage(catalog.tables2nodes))

    @unittest.skip("reason for skipping")
    def test_catalog_export(self):
        dp = OpenDbtProject(project_dir=self.DBTFINANCE_DIR, profiles_dir=self.DBTFINANCE_DIR)
        dp.run(command="compile")
        dp.run(command="run", args=['--select', '+my_second_dbt_model'])
        dp.run(command="docs", args=['generate'])
        catalog = OpenDbtCatalog(
            manifest_path=self.DBTFINANCE_DIR.joinpath('target/manifest.json'),
            catalog_path=self.DBTFINANCE_DIR.joinpath('target/catalog.json'))
        catalog.export()

    def test_catalog_export_one_node(self):
        dp = OpenDbtProject(project_dir=self.DBTFINANCE_DIR, profiles_dir=self.DBTFINANCE_DIR)
        dp.run(command="compile")
        dp.run(command="run", args=['--select', '+my_second_dbt_model'])
        dp.run(command="docs", args=['generate'])
        catalog = OpenDbtCatalog(
            manifest_path=self.DBTFINANCE_DIR.joinpath('target/manifest.json'),
            catalog_path=self.DBTFINANCE_DIR.joinpath('target/catalog.json'))
        node = catalog.node(node_id="model.dbtcore.my_second_dbt_model")
        result = node.parent_db_schema_dict()
        self.assertIn("my_first_dbt_model", result["dev"]["core"])
        self.assertIn("column_3", result["dev"]["core"]["my_first_dbt_model"])
