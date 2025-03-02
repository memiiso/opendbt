import logging
from multiprocessing.context import SpawnContext

from dbt.adapters.duckdb import DuckDBAdapter


class DuckDBAdapterV2Custom(DuckDBAdapter):
    pass

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
