import argparse

from dbt.cli.main import dbtRunner as DbtCliRunner
from dbt.cli.main import dbtRunnerResult
from dbt.contracts.results import RunResult
from dbt.exceptions import DbtRuntimeError
from dbt.version import get_installed_version as get_dbt_version
from packaging.version import Version

from opendbt.logger import OpenDbtLogger

DBT_VERSION = get_dbt_version()
# ================================================================================================================
# Monkey Patching! Override dbt lib AdapterContainer.register_adapter method with new one above
# ================================================================================================================
from opendbt import dbtcommon as opendbt_dbtcommon
from dbt.adapters.factory import AdapterContainer

if Version(DBT_VERSION.to_version_string(skip_matcher=True)) > Version("1.8.0"):
    from opendbt import dbt18 as opendbt
    from dbt.task.docs.generate import GenerateTask
else:
    from opendbt import dbt17 as opendbt
    from dbt.task.generate import GenerateTask

# ================= add new methods =======================================================
AdapterContainer.get_custom_adapter_config_value = opendbt_dbtcommon.get_custom_adapter_config_value
AdapterContainer.get_custom_adapter_class_by_name = opendbt_dbtcommon.get_custom_adapter_class_by_name
# ================= override existing methods ==============================================
# dbt docs overrides
GenerateTask.dbt_run = GenerateTask.run
GenerateTask.run = opendbt_dbtcommon.GenerateTask_run
# Adapter inheritance override
AdapterContainer.register_adapter = opendbt.register_adapter


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
        if _exception:
            raise _exception
        else:
            return result


def main():
    p = argparse.ArgumentParser()
    _, args = p.parse_known_args()
    OpenDbtCli.run(args=args)


if __name__ == "__main__":
    main()
