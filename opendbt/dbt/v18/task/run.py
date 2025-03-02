from dbt.artifacts.schemas.results import NodeStatus
from dbt.events.types import (
    LogModelResult,
)
from dbt.task import run
from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event

from opendbt.runtime_patcher import PatchClass


@PatchClass(module_name="dbt.task.run", target_name="ModelRunner")
class ModelRunner(run.ModelRunner):

    def print_result_adapter_response(self, result):
        if hasattr(result, 'adapter_response') and result.adapter_response:
            if result.status == NodeStatus.Error:
                status = result.status
                level = EventLevel.ERROR
            else:
                status = result.message
                level = EventLevel.INFO
            fire_event(
                LogModelResult(
                    description=str(result.adapter_response),
                    status=status,
                    index=self.node_index,
                    total=self.num_nodes,
                    execution_time=result.execution_time,
                    node_info=self.node.node_info,
                ),
                level=level,
            )

    def print_result_line(self, result):
        super().print_result_line(result)
        self.print_result_adapter_response(result=result)
