from pathlib import Path

from opendbt.airflow import dbtdocs

# create public page on airflow server to serve DBT docs
airflow_dbtdocs_page = dbtdocs.init_plugins_dbtdocs_page(Path("/opt/dbttest/target"))
