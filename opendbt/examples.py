import importlib
import tempfile
from multiprocessing.context import SpawnContext
from typing import Dict

from dbt.adapters.base import available
from dbt.adapters.duckdb import DuckDBAdapter


class DuckDBAdapterV2Custom(DuckDBAdapter):

    @available
    def submit_local_python_job(self, parsed_model: Dict, compiled_code: str):
        with tempfile.NamedTemporaryFile(suffix=f'.py', delete=True) as model_file:
            model_file.write(compiled_code.lstrip().encode('utf-8'))
            model_file.flush()
            print(f"Created temp py file {model_file.name}")
            # load and execute python code!
            model_name = parsed_model['name']
            # Load the module spec
            spec = importlib.util.spec_from_file_location(model_name, model_file.name)
            # Create a module object
            module = importlib.util.module_from_spec(spec)
            # Load the module
            spec.loader.exec_module(module)
            # Access and call `model` function of the model! NOTE: session argument is None here.
            dbt = module.dbtObj(None)
            module.model(dbt, None)
            model_file.close()


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
