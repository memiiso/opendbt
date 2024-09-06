import dbt
from dbt.cli.main import dbtRunner as DbtCliRunner
from dbt.cli.main import dbtRunnerResult
from dbt.contracts.results import RunResult
from dbt.exceptions import DbtRuntimeError
from dbt.version import get_installed_version as get_dbt_version
from packaging.version import Version

DBT_VERSION = get_dbt_version()
# ================================================================================================================
# Monkey Patching! Override dbt lib AdapterContainer.register_adapter method with new one above
# ================================================================================================================
from opendbt import dbtcommon
from dbt.adapters.factory import AdapterContainer

# STEP-1 add new methods
AdapterContainer.get_custom_adapter_config_value = dbtcommon.get_custom_adapter_config_value
AdapterContainer.get_custom_adapter_class_by_name = dbtcommon.get_custom_adapter_class_by_name
# # STEP-2 override existing method

if Version(DBT_VERSION.to_version_string(skip_matcher=True)) > Version("1.8.0"):
    from opendbt import dbt18
    # override imports
    from dbt.task.docs import DOCS_INDEX_FILE_PATH
    from dbt.task.docs.generate import GenerateTask
    from dbt.task.docs.serve import ServeTask

    ##
    GenerateTask.dbt_run = dbt.task.docs.generate.GenerateTask.run
    GenerateTask.run = dbtcommon.GenerateTask_run
    ServeTask.run = dbtcommon.ServeTask_run
    # adapter inheritance overrides
    AdapterContainer.register_adapter = dbt18.register_adapter
else:
    from opendbt import dbt17
    # override imports
    from dbt.include.global_project import DOCS_INDEX_FILE_PATH
    from dbt.task.generate import GenerateTask
    from dbt.task.serve import ServeTask
    # dbt docs overrides
    GenerateTask.dbt_run = dbt.task.generate.GenerateTask.run
    GenerateTask.run = dbtcommon.GenerateTask_run
    ServeTask.run = dbtcommon.ServeTask_run
    # adapter inheritance overrides
    AdapterContainer.register_adapter = dbt17.register_adapter

class OpenDbtCli:

    @staticmethod
    def run(args: list) -> dbtRunnerResult:
        """
        Run dbt with the given arguments.

        :param args: The arguments to pass to dbt.
        :return: The result of the dbt run.
        """
        # https://docs.getdbt.com/reference/programmatic-invocations
        dbt = DbtCliRunner()
        result: dbtRunnerResult = dbt.invoke(args)
        if result.success:
            return result

        # print query for user to run and see the failing rows
        rer: RunResult

        _exception = result.exception if result.exception else None
        if (_exception is None and result.result and result.result.results and
                len(result.result.results) > 0 and result.result.results[0].message
        ):
            _exception = DbtRuntimeError(result.result.results[0].message)

        if _exception is None:
            DbtRuntimeError(f"DBT execution failed!")
        raise _exception
