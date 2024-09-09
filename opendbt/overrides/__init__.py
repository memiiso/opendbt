import dbt
from packaging.version import Version

from opendbt.overrides import common


def patch_dbt():
    # ================================================================================================================
    # Monkey Patching! Override dbt lib AdapterContainer.register_adapter method with new one above
    # ================================================================================================================
    if Version(dbt.version.get_installed_version().to_version_string(skip_matcher=True)) < Version("1.8.0"):
        dbt.task.generate.GenerateTask = common.OpenDbtGenerateTask
    else:
        dbt.task.docs.generate.GenerateTask = common.OpenDbtGenerateTask
    dbt.adapters.factory.FACTORY = common.OpenDbtAdapterContainer()
