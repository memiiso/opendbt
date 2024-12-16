import sys

from packaging.version import Version

from base_dbt_test import BaseDbtTest
from opendbt import OpenDbtProject


class TestOpenDbtProject(BaseDbtTest):

    def test_run_with_custom_adapter(self):
        if Version(self.DBT_VERSION.to_version_string(skip_matcher=True)) > Version("1.8.0"):
            dbt_custom_adapter = 'opendbt.examples.DuckDBAdapterTestingOnlyDbt18'
        else:
            dbt_custom_adapter = 'opendbt.examples.DuckDBAdapterTestingOnlyDbt17'

        dp = OpenDbtProject(project_dir=self.DBTCORE_DIR, profiles_dir=self.DBTCORE_DIR,
                            args=['--vars', f"{{'dbt_custom_adapter': '{dbt_custom_adapter}'}}"])
        with self.assertRaises(Exception) as context:
            sys.tracebacklimit = 0
            dp.run(command="compile")
        self.assertTrue("Custom user defined test adapter activated" in str(context.exception))
        with self.assertRaises(Exception) as context:
            sys.tracebacklimit = 0
            dp.run(command="compile")
        self.assertTrue("Custom user defined test adapter activated" in str(context.exception))

    def test_run_with_custom_adapter_mmodule_not_found(self):
        dp = OpenDbtProject(project_dir=self.DBTCORE_DIR, profiles_dir=self.DBTCORE_DIR,
                            args=['--vars', '{dbt_custom_adapter: not.exits.module.MyDbtTestAdapterV1}']
                            )
        with self.assertRaises(Exception) as context:
            sys.tracebacklimit = 0
            dp.run(command="compile")
        self.assertTrue("Module of provided adapter not found" in str(context.exception))

    def test_run_with_custom_adapter_class_not_found(self):
        dp = OpenDbtProject(project_dir=self.DBTCORE_DIR, profiles_dir=self.DBTCORE_DIR,
                            args=['--vars', '{dbt_custom_adapter: test_custom_adapter.NotExistsAdapterClass}']
                            )
        with self.assertRaises(Exception) as context:
            sys.tracebacklimit = 0
            dp.run(command="compile")
        self.assertTrue("as no attribute 'NotExistsAdapterClass'" in str(context.exception))

    def test_run_with_custom_adapter_wrong_name(self):
        dp = OpenDbtProject(project_dir=self.DBTCORE_DIR, profiles_dir=self.DBTCORE_DIR,
                            args=['--vars', 'dbt_custom_adapter: test_custom_adapterMyDbtTestAdapterV1']
                            )
        with self.assertRaises(Exception) as context:
            sys.tracebacklimit = 0
            dp.run(command="compile")
        self.assertTrue("Unexpected adapter class name" in str(context.exception))
