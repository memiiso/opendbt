import importlib
from multiprocessing.context import SpawnContext
from pathlib import Path
from typing import Dict

from dbt.adapters.base import available
from dbt.adapters.duckdb import DuckDBAdapter


class DuckDBAdapterV2Custom(DuckDBAdapter):

    def abs_compiled_path(self, parsed_model: Dict) -> Path:
        project_root = Path(self.config.project_root)
        if 'compiled_path' in parsed_model:
            return project_root.joinpath(parsed_model['compiled_path'])

        compiled_path = project_root.joinpath(self.config.target_path).joinpath("compiled")
        package_name = parsed_model['package_name']
        original_file_path = parsed_model['original_file_path']
        abs_compiled_path = compiled_path.joinpath(package_name).joinpath(original_file_path)
        return project_root.joinpath(abs_compiled_path)

    @available
    def submit_local_python_job(self, parsed_model: Dict, compiled_code: str):
        # full path to the compiled model file
        model_compiled_file_abs_path = self.abs_compiled_path(parsed_model)
        model_name = parsed_model['name']
        # Load the module spec
        spec = importlib.util.spec_from_file_location(model_name, model_compiled_file_abs_path)
        # Create a module object
        module = importlib.util.module_from_spec(spec)
        # Load the module
        spec.loader.exec_module(module)
        # Access and call `model` function of the model! NOTE: session argument is None here.
        dbt = module.dbtObj(None)
        module.model(dbt, None)


# NOTE! used for testing
class DuckDBAdapterTestingOnlyDbt17(DuckDBAdapter):
    def __init__(self, config) -> None:
        print(f"WARNING: Using User Provided DBT Adapter: {type(self).__module__}.{type(self).__name__}")
        # pylint: disable=no-value-for-parameter
        super().__init__(config=config)
        raise Exception("Custom user defined test adapter activated, test exception")


# NOTE! used for testing
class DuckDBAdapterTestingOnlyDbt18(DuckDBAdapter):
    def __init__(self, config, mp_context: SpawnContext) -> None:
        print(f"WARNING: Using User Provided DBT Adapter: {type(self).__module__}.{type(self).__name__}")
        super().__init__(config=config, mp_context=mp_context)
        raise Exception("Custom user defined test adapter activated, test exception")
