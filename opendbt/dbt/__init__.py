from dbt import version
from packaging.version import Version

import opendbt.dbt.shared.cli.main
from opendbt.runtime_patcher import RuntimePatcher

dbt_version = Version(version.get_installed_version().to_version_string(skip_matcher=True))
if Version("1.6.0") <= dbt_version < Version("1.8.0"):
    from opendbt.dbt.v17.adapters.factory import OpenDbtAdapterContainer
    from opendbt.dbt.v17.config.runtime import OpenDbtRuntimeConfig
    from opendbt.dbt.v17.task.docs.generate import OpenDbtGenerateTask
    from opendbt.dbt.v17.task.run import ModelRunner
elif Version("1.8.0") <= dbt_version < Version("1.10.0"):
    from opendbt.dbt.v18.adapters.factory import OpenDbtAdapterContainer
    from opendbt.dbt.v18.config.runtime import OpenDbtRuntimeConfig
    from opendbt.dbt.v18.task.docs.generate import OpenDbtGenerateTask
    from opendbt.dbt.v18.task.run import ModelRunner
else:
    raise Exception(
        f"Unsupported dbt version {dbt_version}, please make sure dbt version is supported/integrated by opendbt")

RuntimePatcher(module_name="dbt.adapters.factory").patch_attribute(attribute_name="FACTORY",
                                                                   new_value=OpenDbtAdapterContainer())
# shared code patches
from opendbt.dbt.shared.cli.main import sqlfluff
from opendbt.dbt.shared.cli.main import sqlfluff_lint
from opendbt.dbt.shared.cli.main import sqlfluff_fix
