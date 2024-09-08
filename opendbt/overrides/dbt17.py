from importlib import import_module

from dbt.adapters.factory import Adapter
from dbt.events.functions import fire_event
from dbt.events.types import AdapterRegistered
from dbt.semver import VersionSpecifier


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
