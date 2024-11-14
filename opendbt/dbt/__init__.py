import dbt
from packaging.version import Version


def patch_dbt():
    # ================================================================================================================
    # Monkey Patching! Override dbt lib code with new one
    # ================================================================================================================
    dbt_version = Version(dbt.version.get_installed_version().to_version_string(skip_matcher=True))
    if dbt_version >= Version("1.7.0") and dbt_version < Version("1.8.0"):
        from opendbt.dbt.v17.task.docs.generate import OpenDbtGenerateTask
        dbt.task.generate.GenerateTask = OpenDbtGenerateTask
        from opendbt.dbt.v17.adapters.factory import OpenDbtAdapterContainer
        dbt.adapters.factory.FACTORY = OpenDbtAdapterContainer()
    elif dbt_version >= Version("1.8.0") and dbt_version < Version("1.9.0"):
        from opendbt.dbt.v18.task.docs.generate import OpenDbtGenerateTask
        dbt.task.docs.generate.GenerateTask = OpenDbtGenerateTask
        from opendbt.dbt.v18.adapters.factory import OpenDbtAdapterContainer
        dbt.adapters.factory.FACTORY = OpenDbtAdapterContainer()
        from opendbt.dbt.v18.task.run import ModelRunner
        dbt.task.run.ModelRunner = ModelRunner
    else:
        raise Exception(
            f"Unsupported dbt version {dbt_version}, please make sure dbt version is supported/integrated by opendbt")

    # shared code patches
    import opendbt.dbt.shared.cli.main
    dbt.cli.main.sqlfluff = opendbt.dbt.shared.cli.main.sqlfluff
    dbt.cli.main.sqlfluff_lint = opendbt.dbt.shared.cli.main.sqlfluff_lint
    dbt.cli.main.sqlfluff_fix = opendbt.dbt.shared.cli.main.sqlfluff_fix
