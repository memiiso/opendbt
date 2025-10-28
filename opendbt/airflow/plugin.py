from pathlib import Path
from typing import Union, Dict, Optional
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
        "has_index": path.joinpath("index.html").exists(),
    }
    project_info["is_valid"] = (
        project_info["has_manifest"] and
        project_info["has_index"]
    )
    return project_info


class ProjectConfig:
    """Configuration for DBT docs projects - handles both legacy and multi-project modes."""
    
    def __init__(
        self,
        legacy_path: Optional[Union[Path, str]] = None
    ):
        """
        Initialize project configuration.
        
        Args:
            legacy_path: Path for single-project legacy mode (if provided, enables legacy mode)
        """
        self.legacy_mode = legacy_path is not None
        self.legacy_path = Path(legacy_path) if legacy_path else None
    
    def get_projects(self) -> Dict[str, Path]:
        """
        Get all configured projects.
        
        Returns:
            Dictionary mapping project names to their paths
        """
        if self.legacy_mode:
            return {"default": self.legacy_path}
        
        try:
            projects_json = Variable.get("opendbt_docs_projects")
            
            if projects_json is None:
                return {}
            
            if isinstance(projects_json, str):
                projects_json = json.loads(projects_json)
            
            return {k: Path(v) for k, v in projects_json.items()}
        
        except Exception as e:
            log.error(
                "Error loading projects from Variable 'opendbt_docs_projects': %s", e
            )
            return {}


class DBTDocsView(BaseView):
    """Flask view for serving DBT documentation with multi-project support."""
    
    route_base = "/dbt"
    default_view = "dbt_docs_index"

    def __init__(self, config: ProjectConfig):
        super().__init__()
        self.config = config

    def _check_configuration(self) -> Optional[Response]:
        """Check if configuration is valid. Returns error response if invalid, None if OK."""
        if not self.config.legacy_mode:
            projects = self.config.get_projects()
            if not projects:
                error_msg = _load_template("multi_project_config_error.txt")
                log.error("No DBT projects configured in multi-project mode")
                return _create_error_response(
                    "DBT Docs - Configuration Required",
                    error_msg
                )
        return None

    def _get_current_project(self) -> str:
        """Get current project from query param or default."""
        if self.config.legacy_mode:
            return "default"

        projects = self.config.get_projects()

        # Get from query param, fallback to first available
        project = request.args.get('project')
        if not project:
            project = list(projects.keys())[0]

        return project

    def _get_project_path(self, project_name: str) -> Path:
        """Get path for specific project."""
        projects = self.config.get_projects()
        
        if project_name not in projects:
            available = ", ".join(projects.keys())
            abort(404, f"Project '{project_name}' not found. Available: {available}")
        
        return projects[project_name]

    @expose("/projects")
    @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
    def list_projects(self):
        """Return list of available projects with validation."""
        error_response = self._check_configuration()
        if error_response:
            return error_response

        projects = self.config.get_projects()

        # Validate that target dirs exist and have required files
        valid_projects = [
            _validate_project_info(name, path)
            for name, path in projects.items()
        ]

        return jsonify({
            "projects": valid_projects,
            "current": self._get_current_project(),
            "legacy_mode": self.config.legacy_mode
        })

    @expose("/dbt_docs_index.html")  # type: ignore[misc]
    @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
    def dbt_docs_index(self):
        """Serve the main DBT docs index page."""
        error_response = self._check_configuration()
        if error_response:
            return error_response

        project = self._get_current_project()
        project_path = self._get_project_path(project)

        index_file = project_path.joinpath("index.html")
        if not index_file.is_file():
            abort(404, f"index.html not found for project '{project}'")

        return index_file.read_text()

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
        dbt_docs_dir: Legacy single project path (for backward compatibility)

    Returns:
        AirflowPlugin class
    """
    # Create configuration
    config = ProjectConfig(
        legacy_path=dbt_docs_dir
    )
    
    # Create view instance
    view = DBTDocsView(config)
    
    # Create Flask blueprint
    # Note: In multi-project mode, static files are served from individual project dirs
    static_folder = config.legacy_path.as_posix() if config.legacy_mode else None
    
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