import dbt
from packaging.version import Version

import opendbt.dbt.v17.cli.main


def patch_dbt():
    # ================================================================================================================
    # Monkey Patching! Override dbt lib AdapterContainer.register_adapter method with new one above
    # ================================================================================================================
    if Version(dbt.version.get_installed_version().to_version_string(skip_matcher=True)) < Version("1.8.0"):
        from opendbt.dbt.v17.task.docs.generate import OpenDbtGenerateTask
        from opendbt.dbt.v17.adapters.factory import OpenDbtAdapterContainer
        dbt.cli.main.sqlfluff = opendbt.dbt.v17.cli.main.sqlfluff
        dbt.cli.main.sqlfluff_lint = opendbt.dbt.v17.cli.main.sqlfluff_lint
        dbt.cli.main.sqlfluff_fix = opendbt.dbt.v17.cli.main.sqlfluff_fix
        dbt.task.generate.GenerateTask = OpenDbtGenerateTask
        dbt.adapters.factory.FACTORY = OpenDbtAdapterContainer()
    else:
        from opendbt.dbt.v18.task.docs.generate import OpenDbtGenerateTask
        from opendbt.dbt.v18.adapters.factory import OpenDbtAdapterContainer
        dbt.task.docs.generate.GenerateTask = OpenDbtGenerateTask
        dbt.adapters.factory.FACTORY = OpenDbtAdapterContainer()
