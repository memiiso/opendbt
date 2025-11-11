from pathlib import Path
from typing import Union, Dict, Optional, List
import json
import logging

from flask import request, jsonify, abort, Blueprint, Response
from flask_appbuilder import BaseView, expose
from airflow.www.auth import has_access
from airflow.security import permissions
from airflow.models import Variable
from airflow.plugins_manager import AirflowPlugin

log = logging.getLogger(__name__)

# Directory containing templates
TEMPLATE_DIR = Path(__file__).parent / "templates"


def _load_template(template_name: str) -> str:
    """Load a template file from the templates directory."""
    template_path = TEMPLATE_DIR / template_name
    return template_path.read_text()


def _create_error_response(title: str, message: str, status_code: int = 503) -> Response:
    """Create a custom HTML error response using external template."""
    template = _load_template("error.html")
    html = template.replace("{{ title }}", title).replace("{{ message }}", message)
    return Response(html, status=status_code, mimetype='text/html')


def _validate_project_info(name: str, path: Path) -> dict:
    """Validate a single project and return its info."""
    project_info = {
        "name": name,
        "path": str(path),
        "has_manifest": path.joinpath("manifest.json").exists(),
        "has_catalog": path.joinpath("catalog.json").exists(),
        "has_catalogl": path.joinpath("catalogl.json").exists(),
        "has_custom_index": path.joinpath("index.html").exists(),
    }
    # Valid if has manifest (index.html is optional - fallback to opendbt template)
    project_info["is_valid"] = project_info["has_manifest"]
    return project_info


def get_projects(
    project_paths: Optional[Union[Path, str, List[Union[Path, str]]]] = None
) -> Dict[str, Path]:
    """
    Get all configured projects.

    Args:
        project_paths: Single path (str/Path) or list of paths for project(s)

    Returns:
        Dictionary mapping project names to their paths
    """
    # Try to get from Variable first (this overrides .py file config)
    projects = {}
    try:
        projects_var = Variable.get("opendbt_docs_projects", deserialize_json=True)
        # Handle both single string and list of strings
        if projects_var and isinstance(projects_var, (str, Path)):
            projects.update({Path(p).parent.name: Path(p) for p in [projects_var]})
        if projects_var and isinstance(projects_var, list):
            projects.update({Path(p).parent.name: Path(p) for p in projects_var})
    except Exception as e:
        log.error(
            "Error loading projects from Variable 'opendbt_docs_projects': %s", e
        )

    # Combine to paths from .py file initialization
    # Normalize to list of Paths
    if project_paths and isinstance(project_paths, (list, tuple)):
        projects.update({path.parent.name: path for path in [Path(p) for p in project_paths]})
    if project_paths and isinstance(project_paths, (str, Path)):
        projects.update({path.parent.name: path for path in [Path(project_paths)]})

    return projects


class DBTDocsView(BaseView):
    """Flask view for serving DBT documentation with multi-project support."""

    route_base = "/dbt"
    default_view = "dbt_docs_index"

    def __init__(self, projects: Dict[str, Path]):
        super().__init__()
        self.projects = projects

    def _check_configuration(self) -> Optional[Response]:
        """Check if configuration is valid. Returns error response if invalid, None if OK."""
        if not self.projects:
            error_msg = _load_template("multi_project_config_error.txt")
            log.error("No DBT projects configured in multi-project mode")
            return _create_error_response(
                "DBT Docs - Configuration Required",
                error_msg
                )
        return None

    def _get_current_project(self) -> str:
        """Get current project from query param or default."""
        # Get from query param, fallback to first available
        project = request.args.get('project')
        if not project:
            project = list(self.projects.keys())[0]

        return project

    def _get_project_path(self, project_name: str) -> Path:
        """Get path for specific project."""
        if project_name not in self.projects:
            available = ", ".join(self.projects.keys())
            abort(404, f"Project '{project_name}' not found. Available: {available}")

        return self.projects[project_name]

    @expose("/projects")
    @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
    def list_projects(self):
        """Return list of available projects with validation."""
        error_response = self._check_configuration()
        if error_response:
            return error_response

        # Validate that target dirs exist and have required files
        valid_projects = [
            _validate_project_info(name, path)
            for name, path in self.projects.items()
        ]

        return jsonify({
            "projects": valid_projects,
            "current": self._get_current_project()
        })

    @expose("/dbt_docs_index.html")  # type: ignore[misc]
    @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
    def dbt_docs_index(self):
        """Serve the main DBT docs index page with fallback to opendbt template."""
        error_response = self._check_configuration()
        if error_response:
            return error_response

        project = self._get_current_project()
        project_path = self._get_project_path(project)

        # Try to load custom index.html from project directory (user override)
        custom_index = project_path.joinpath("index.html")

        if custom_index.is_file():
            # User provided custom index.html - use it
            return custom_index.read_text()

        # Fallback to opendbt's enhanced template
        opendbt_index = Path(__file__).parent.parent / "dbt" / "docs" / "index.html"

        if not opendbt_index.is_file():
            abort(500, "opendbt index.html template not found")

        return opendbt_index.read_text()

    def _return_json(self, json_file: str):
        """Generic handler for returning JSON files."""
        error_response = self._check_configuration()
        if error_response:
            return error_response

        project = self._get_current_project()
        project_path = self._get_project_path(project)

        file_path = project_path.joinpath(json_file)
        if not file_path.is_file():
            abort(404, f"{json_file} not found for project '{project}'")

        data = file_path.read_text()
        return data, 200, {"Content-Type": "application/json"}

    @expose("/catalog.json")  # type: ignore[misc]
    @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
    def catalog(self):
        """Serve catalog.json for current project."""
        return self._return_json("catalog.json")

    @expose("/manifest.json")  # type: ignore[misc]
    @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
    def manifest(self):
        """Serve manifest.json for current project."""
        return self._return_json("manifest.json")

    @expose("/run_info.json")  # type: ignore[misc]
    @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
    def run_info(self):
        """Serve run_info.json for current project."""
        return self._return_json("run_info.json")

    @expose("/catalogl.json")  # type: ignore[misc]
    @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
    def catalogl(self):
        """Serve catalogl.json for current project."""
        return self._return_json("catalogl.json")


def init_plugins_dbtdocs_page(
    dbt_docs_dir: Union[Path, str] = None
    ):
    """
    Initialize DBT Docs plugin with support for multiple projects.

    Args:
        dbt_docs_dir: Single project path (for backward compatibility)

    Returns:
        AirflowPlugin class
    """
    projects = get_projects(dbt_docs_dir)
    view = DBTDocsView(projects)

    static_folder = None
    
    bp = Blueprint(
        "DBT Plugin",
        __name__,
        template_folder=None,
        static_folder=static_folder,
    )

    class AirflowDbtDocsPlugin(AirflowPlugin):
        name = "DBT Docs Plugin"
        flask_blueprints = [bp]
        appbuilder_views = [{"name": "DBT Docs", "category": "", "view": view}]

    return AirflowDbtDocsPlugin