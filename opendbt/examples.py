import importlib
import logging
import sys
import tempfile
from multiprocessing.context import SpawnContext
from typing import Dict

from dbt.adapters.base import available
from dbt.adapters.duckdb import DuckDBAdapter


class DuckDBAdapterV2Custom(DuckDBAdapter):

    @available
    def submit_local_python_job(self, parsed_model: Dict, compiled_code: str):
        connection = self.connections.get_if_exists()
        if not connection:
            connection = self.connections.get_thread_connection()

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
            sys.modules[model_name] = module
            spec.loader.exec_module(module)
            dbt = module.dbtObj(None)
            # Access and call `model` function of the model!
            # IMPORTANT: here we are passing down duckdb session from the adapter to the model
            module.model(dbt=dbt, session=connection.handle)
            model_file.close()

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
            sys.modules[model_name] = module
            spec.loader.exec_module(module)
            dbt = module.dbtObj(None)
            # IMPORTANT: here we are passing down duckdb session from the adapter to the model
            module.model(dbt=dbt, pipeline=_pipeline)
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


def email_dbt_test_callback(event: "EventMsg"):
    if event.info.name == "LogTestResult" and event.info.level in ["warn", "error"]:
        logging.getLogger('dbtcallbacks').warning("DBT callback `email_dbt_test_callback` called!")
        email_subject = f"[DBT] test {event.info.level} raised"
        email_html_content = f"""Following test raised {event.info.level}!
dbt msg: {event.info.msg}
dbt test: {event.data.name}
dbt node_relation: {event.data.node_info.node_relation}
--------------- full data ---------------
dbt data: {event.data}
"""
        # @TODO send email alert using airflow
        # from airflow.utils.email import send_email
        # send_email(
        #     subject=email_subject,
        #     to="my-slack-notification-channel@slack.com",
        #     html_content=email_html_content
        # )
        logging.getLogger('dbtcallbacks').error("Callback email sent!")
