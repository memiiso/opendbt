import importlib
from multiprocessing.context import SpawnContext
from typing import Optional

import dbt
from dbt.adapters.base.plugin import AdapterPlugin
from dbt.adapters.factory import FACTORY, Adapter
from dbt.cli.main import dbtRunner as DbtCliRunner
from dbt.cli.main import dbtRunnerResult
from dbt.contracts.results import RunResult
from dbt.exceptions import DbtRuntimeError
from dbt.version import get_installed_version as get_dbt_version
from packaging.version import Version

DBT_CUSTOM_ADAPTER_VAR = 'dbt_custom_adapter'
DBT_VERISON = get_dbt_version()

if Version(DBT_VERISON.to_version_string(skip_matcher=True)) > Version("1.8.0"):
    try:
        from dbt.adapters.contracts.connection import AdapterRequiredConfig
        from dbt.adapters.events.types import (
            AdapterRegistered,
        )
        from dbt_common.events.base_types import EventLevel
        from dbt_common.events.functions import fire_event
    except ImportError:
        pass
else:
    try:
        from dbt.events.base_types import EventLevel
        from dbt.events.functions import fire_event
        from dbt.events.types import AdapterRegistered
        from importlib import import_module
        from dbt.events.functions import fire_event
        from dbt.events.types import AdapterRegistered
        from dbt.semver import VersionSpecifier
    except ImportError:
        pass


def get_custom_adapter_config_value(self, config: 'AdapterRequiredConfig') -> str:
    # FIRST: it's set as cli value: dbt run --vars {'dbt_custom_adapter': 'custom_adapters.DuckDBAdapterV1Custom'}
    if hasattr(config, 'cli_vars') and DBT_CUSTOM_ADAPTER_VAR in config.cli_vars:
        custom_adapter_class_name: str = config.cli_vars[DBT_CUSTOM_ADAPTER_VAR]
        if custom_adapter_class_name and custom_adapter_class_name.strip():
            return custom_adapter_class_name
    # SECOND: it's set inside dbt_project.yml
    if hasattr(config, 'vars') and DBT_CUSTOM_ADAPTER_VAR in config.vars.to_dict():
        custom_adapter_class_name: str = config.vars.to_dict()[DBT_CUSTOM_ADAPTER_VAR]
        if custom_adapter_class_name and custom_adapter_class_name.strip():
            return custom_adapter_class_name

    return None


def get_custom_adapter_class_by_name(self, custom_adapter_class_name: str):
    if "." not in custom_adapter_class_name:
        raise ValueError(f"Unexpected adapter class name: `{custom_adapter_class_name}` ,"
                         f"Expecting something like:`my.sample.library.MyAdapterClass`")

    __module, __class = custom_adapter_class_name.rsplit('.', 1)
    try:
        user_adapter_module = importlib.import_module(__module)
        user_adapter_class = getattr(user_adapter_module, __class)
        return user_adapter_class
    except ModuleNotFoundError as mnfe:
        raise Exception(f"Module of provided adapter not found, provided: {custom_adapter_class_name}") from mnfe


# ================================================================================================================
# Add further extension below, extend dbt using Monkey Patching!
# ================================================================================================================
# dbt < 1.8
def register_adapter_v1(self, config: 'AdapterRequiredConfig') -> None:
    # ==== CUSTOM CODE ====
    # ==== END CUSTOM CODE ====
    adapter_name = config.credentials.type
    adapter_type = self.get_adapter_class_by_name(adapter_name)
    adapter_version = import_module(f".{adapter_name}.__version__", "dbt.adapters").version
    # ==== CUSTOM CODE ====
    custom_adapter_class_name: str = self.get_custom_adapter_config_value(config)
    if custom_adapter_class_name and custom_adapter_class_name.strip():
        # OVERRIDE DEFAULT ADAPTER BY USER GIVEN ADAPTER CLASS
        adapter_type = self.get_custom_adapter_class_by_name(custom_adapter_class_name)
    # ==== END CUSTOM CODE ====
    adapter_version_specifier = VersionSpecifier.from_version_string(
        adapter_version
    ).to_version_string()
    fire_event(
        AdapterRegistered(adapter_name=adapter_name, adapter_version=adapter_version_specifier)
    )
    with self.lock:
        if adapter_name in self.adapters:
            # this shouldn't really happen...
            return

        adapter: Adapter = adapter_type(config)  # type: ignore
        self.adapters[adapter_name] = adapter


# dbt >=1.8
def register_adapter_v2(
        self,
        config: 'AdapterRequiredConfig',
        mp_context: SpawnContext,
        adapter_registered_log_level: Optional[EventLevel] = EventLevel.INFO,
) -> None:
    adapter_name = config.credentials.type
    adapter_type = self.get_adapter_class_by_name(adapter_name)
    adapter_version = self._adapter_version(adapter_name)
    # ==== CUSTOM CODE ====
    custom_adapter_class_name: str = self.get_custom_adapter_config_value(config)
    if custom_adapter_class_name and custom_adapter_class_name.strip():
        # OVERRIDE DEFAULT ADAPTER BY USER GIVEN ADAPTER CLASS
        adapter_type = self.get_custom_adapter_class_by_name(custom_adapter_class_name)
    # ==== END CUSTOM CODE ====
    fire_event(
        AdapterRegistered(adapter_name=adapter_name, adapter_version=adapter_version),
        level=adapter_registered_log_level,
    )
    with self.lock:
        if adapter_name in self.adapters:
            # this shouldn't really happen...
            return

        adapter: Adapter = adapter_type(config, mp_context)  # type: ignore
        self.adapters[adapter_name] = adapter


# ================================================================================================================
# Monkey Patching! Override dbt lib AdapterContainer.register_adapter method with new one above
# ================================================================================================================
# add new methods
dbt.adapters.factory.AdapterContainer.get_custom_adapter_config_value = get_custom_adapter_config_value
dbt.adapters.factory.AdapterContainer.get_custom_adapter_class_by_name = get_custom_adapter_class_by_name
# override existing method
if Version(DBT_VERISON.to_version_string(skip_matcher=True)) > Version("1.8.0"):
    dbt.adapters.factory.AdapterContainer.register_adapter = register_adapter_v2
else:
    dbt.adapters.factory.AdapterContainer.register_adapter = register_adapter_v1


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
