from pathlib import Path
from opendbt.airflow import plugin

# The plugin auto-detects mode:
# - If Airflow Variable 'opendbt_docs_projects' is set, it will use that (overrides path below)
# - Otherwise, it will use the path provided here
# - Path can be a single path (str/Path) or list of paths
airflow_dbtdocs_page = plugin.init_plugins_dbtdocs_page(
    Path("/opt/dbtcore/target")  # <-- Set this to your project path(s)
)