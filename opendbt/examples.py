import tempfile
from multiprocessing.context import SpawnContext
from typing import Dict

from dbt.adapters.base import available
from dbt.adapters.duckdb import DuckDBAdapter

from opendbt.utils import Utils


class DuckDBAdapterV2Custom(DuckDBAdapter):
    @available
    def submit_local_python_job(self, parsed_model: Dict, compiled_code: str):
        model_unique_id = parsed_model.get('unique_id')
        __py_code = f"""
{compiled_code}

# NOTE this is local python execution so session is None
model(dbt=dbtObj(None), session=None)
        """
        with tempfile.NamedTemporaryFile(suffix=f'__{model_unique_id}.py', delete=True) as fp:
            fp.write(__py_code.encode('utf-8'))
            fp.flush()
            print(f"Created temp py file {fp.name}")
            Utils.runcommand(command=['python', fp.name])
            fp.close()


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
