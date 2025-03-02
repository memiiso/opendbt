import importlib
import sys
import tempfile
from typing import Dict

from dbt.adapters.base import available, BaseAdapter

from opendbt.runtime_patcher import PatchClass


@PatchClass(module_name="dbt.adapters.base", target_name="BaseAdapter")
class OpenDbtBaseAdapter(BaseAdapter):

    def _execute_python_model(self, model_name: str, compiled_code: str, **kwargs):
        with tempfile.NamedTemporaryFile(suffix=f'.py', delete=True) as model_file:
            model_file.write(compiled_code.lstrip().encode('utf-8'))
            model_file.flush()
            print(f"Created temp py file {model_file.name}")
            # Load the module spec
            spec = importlib.util.spec_from_file_location(model_name, model_file.name)
            # Create a module object
            module = importlib.util.module_from_spec(spec)
            # Load the module
            sys.modules[model_name] = module
            spec.loader.exec_module(module)
            dbt_obj = module.dbtObj(None)
            # Access and call `model` function of the model!
            # IMPORTANT: here we are passing down duckdb session from the adapter to the model
            module.model(dbt=dbt_obj, **kwargs)
            model_file.close()

    @available
    def submit_local_python_job(self, parsed_model: Dict, compiled_code: str):
        connection = self.connections.get_if_exists()
        if not connection:
            connection = self.connections.get_thread_connection()
        self._execute_python_model(model_name=parsed_model['name'], compiled_code=compiled_code,
                                   session=connection.handle)

    @available
    def submit_local_dlt_job(self, parsed_model: Dict, compiled_code: str):
        connection = self.connections.get_if_exists()
        if not connection:
            connection = self.connections.get_thread_connection()

        import dlt
        # IMPORTANT: here we are pre-configuring and preparing dlt.pipeline for the model!
        _pipeline = dlt.pipeline(
            pipeline_name=str(parsed_model['unique_id']).replace(".", "-"),
            destination=dlt.destinations.duckdb(connection.handle._env.conn),
            dataset_name=parsed_model['schema'],
            dev_mode=False,
        )
        self._execute_python_model(model_name=parsed_model['name'], compiled_code=compiled_code, pipeline=_pipeline)
