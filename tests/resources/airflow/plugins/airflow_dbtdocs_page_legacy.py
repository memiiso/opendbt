from pathlib import Path
from opendbt.airflow import plugin

# Single-project mode (backward compatibility test)
# This tests that the refactored code still works in legacy mode
airflow_dbtdocs_page = plugin.init_plugins_dbtdocs_page(
    Path("/opt/dbtcore/target")
)
