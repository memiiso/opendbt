from multiprocessing.context import SpawnContext
from typing import Optional

from dbt.adapters import factory
from dbt.adapters.contracts.connection import AdapterRequiredConfig
from dbt.adapters.events.types import (
    AdapterRegistered,
)
from dbt.adapters.factory import Adapter
from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event


class AdapterContainerDbtOverride(factory.AdapterContainer):
    def register_adapter(
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
