import importlib
from importlib import import_module

from dbt.adapters import factory
from dbt.adapters.factory import Adapter
from dbt.events.functions import fire_event
from dbt.events.types import AdapterRegistered
from dbt.semver import VersionSpecifier

from opendbt.runtime_patcher import PatchClass


@PatchClass(module_name="dbt.adapters.factory", target_name="AdapterContainer")
class OpenDbtAdapterContainer(factory.AdapterContainer):
    DBT_CUSTOM_ADAPTER_VAR = 'dbt_custom_adapter'

    def register_adapter(self, config: 'AdapterRequiredConfig') -> None:
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

    def get_custom_adapter_config_value(self, config: 'AdapterRequiredConfig') -> str:
        # FIRST: it's set as cli value: dbt run --vars {'dbt_custom_adapter': 'custom_adapters.DuckDBAdapterV1Custom'}
        if hasattr(config, 'cli_vars') and self.DBT_CUSTOM_ADAPTER_VAR in config.cli_vars:
            custom_adapter_class_name: str = config.cli_vars[self.DBT_CUSTOM_ADAPTER_VAR]
            if custom_adapter_class_name and custom_adapter_class_name.strip():
                return custom_adapter_class_name
        # SECOND: it's set inside dbt_project.yml
        if hasattr(config, 'vars') and self.DBT_CUSTOM_ADAPTER_VAR in config.vars.to_dict():
            custom_adapter_class_name: str = config.vars.to_dict()[self.DBT_CUSTOM_ADAPTER_VAR]
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
