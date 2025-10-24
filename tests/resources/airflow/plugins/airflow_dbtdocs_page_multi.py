from opendbt.airflow import plugin

# Multi-project mode
# This tests the new multi-project functionality with project switching
airflow_dbtdocs_page = plugin.init_plugins_dbtdocs_page(
    variable_name="dbt_docs_projects",
    default_project="dbtcore"
)
