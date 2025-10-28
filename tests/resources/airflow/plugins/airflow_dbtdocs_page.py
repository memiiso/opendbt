import os
from pathlib import Path
from opendbt.airflow import plugin

# Plugin mode is controlled by AIRFLOW_PLUGIN_MODE environment variable
# - "single" (default): Single-project mode with static path
# - "multi": Multi-project mode with Airflow Variable
plugin_mode = os.getenv('AIRFLOW_PLUGIN_MODE', 'single')

if plugin_mode == 'multi':
    airflow_dbtdocs_page = plugin.init_plugins_dbtdocs_page(
        variable_name="dbt_docs_projects"
    )
else:
    airflow_dbtdocs_page = plugin.init_plugins_dbtdocs_page(
        Path("/opt/dbtcore/target") # <-- Set this path to your project
    )