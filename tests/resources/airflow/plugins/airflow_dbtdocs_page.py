from pathlib import Path

from opendbt.airflow import plugin

# create public page on airflow server to serve DBT docs
airflow_dbtdocs_page = plugin.init_plugins_dbtdocs_page(Path("/opt/dbtcore/target"))
