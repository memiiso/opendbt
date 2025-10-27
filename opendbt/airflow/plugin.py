import json
import time
from pathlib import Path
from typing import Union, Dict, Optional, Tuple, Type


def _create_dbt_docs_view_class(
    legacy_mode: bool,
    dbt_docs_dir: Optional[Path],
    variable_name: str,
    default_project: Optional[str]
) -> Type:
    """
    Factory function to create DBTDocsView class with configuration.

    Args:
        legacy_mode: Whether to use single-project legacy mode
        dbt_docs_dir: Path to single project (legacy mode only)
        variable_name: Name of Airflow Variable containing project dict
        default_project: Default project name to use if none specified

    Returns:
        DBTDocsView class configured with the given parameters
    """
    from flask import request, jsonify, abort
    from flask_appbuilder import BaseView, expose
    from airflow.www.auth import has_access
    from airflow.security import permissions
    from airflow.models import Variable

    class DBTDocsView(BaseView):
        route_base = "/dbt"
        default_view = "dbt_docs_index"

        # Simple cache: (projects_dict, timestamp)
        _projects_cache: Optional[Tuple[Dict[str, Path], float]] = None
        _cache_ttl: int = 60  # seconds

        def _get_projects_dict(self) -> Dict[str, Path]:
            """Read projects from Airflow Variable with caching (dynamic, no restart needed!)"""
            if legacy_mode:
                # Backward compatibility: single project mode
                return {"default": dbt_docs_dir}

            # Check cache first
            if self._projects_cache is not None:
                cached_projects, cached_time = self._projects_cache
                if time.time() - cached_time < self._cache_ttl:
                    return cached_projects

            # Cache miss or expired - fetch from Variable
            try:
                # Try lowercase first (for CLI-set variables)
                projects_json = Variable.get(variable_name, default_var=None)

                # If not found, try uppercase (for AIRFLOW_VAR_ environment variables)
                if projects_json is None:
                    projects_json = Variable.get(variable_name.upper(), default_var=None)

                if projects_json is None:
                    self.log.warning(f"Airflow Variable '{variable_name}' not found (tried both cases)")
                    return {}

                if isinstance(projects_json, str):
                    projects_json = json.loads(projects_json)

                projects_dict = {k: Path(v) for k, v in projects_json.items()}

                # Update cache
                self._projects_cache = (projects_dict, time.time())

                return projects_dict
            except Exception as e:
                self.log.error(f"Error loading projects from Variable '{variable_name}': {e}")
                return {}

        def _get_current_project(self) -> str:
            """Get current project from query param or default"""
            if legacy_mode:
                return "default"

            projects = self._get_projects_dict()
            if not projects:
                abort(503, "No DBT projects configured")

            # Get from query param
            project = request.args.get('project', default_project)

            # Fallback to first available project
            if not project:
                project = list(projects.keys())[0]

            return project

        def _get_project_path(self, project_name: str) -> Path:
            """Get path for specific project"""
            projects = self._get_projects_dict()

            if project_name not in projects:
                available = ", ".join(projects.keys())
                abort(404, f"Project '{project_name}' not found. Available: {available}")

            return projects[project_name]

        @expose("/projects")  # NEW ENDPOINT
        @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
        def list_projects(self):
            """Return list of available projects with validation"""
            projects = self._get_projects_dict()

            # Validate that target dirs exist and have required files
            valid_projects = []
            for name, path in projects.items():
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
                valid_projects.append(project_info)

            return jsonify({
                "projects": valid_projects,
                "current": self._get_current_project(),
                "legacy_mode": legacy_mode
            })

        @expose("/dbt_docs_index.html")  # type: ignore[misc]
        @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
        def dbt_docs_index(self):
            project = self._get_current_project()
            project_path = self._get_project_path(project)

            if not project_path.joinpath("index.html").is_file():
                abort(404, f"index.html not found for project '{project}'")

            return project_path.joinpath("index.html").read_text()

        def return_json(self, json_file: str):
            project = self._get_current_project()
            project_path = self._get_project_path(project)

            if not project_path.joinpath(json_file).is_file():
                abort(404, f"{json_file} not found for project '{project}'")

            data = project_path.joinpath(json_file).read_text()
            return data, 200, {"Content-Type": "application/json"}

        @expose("/catalog.json")  # type: ignore[misc]
        @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
        def catalog(self):
            return self.return_json("catalog.json")

        @expose("/manifest.json")  # type: ignore[misc]
        @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
        def manifest(self):
            return self.return_json("manifest.json")

        @expose("/run_info.json")  # type: ignore[misc]
        @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
        def run_info(self):
            return self.return_json("run_info.json")

        @expose("/catalogl.json")  # type: ignore[misc]
        @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
        def catalogl(self):
            return self.return_json("catalogl.json")

    return DBTDocsView


# pylint: disable=inconsistent-return-statements
def init_plugins_dbtdocs_page(
    dbt_docs_dir: Union[Path, str] = None,
    variable_name: str = "dbt_docs_projects",
    default_project: Optional[str] = None
):
    """
    Initialize DBT Docs plugin with support for multiple projects.

    Args:
        dbt_docs_dir: Legacy single project path (for backward compatibility)
        variable_name: Name of Airflow Variable containing project dict
        default_project: Default project name to use if none specified
    """
    from airflow.plugins_manager import AirflowPlugin
    from flask import Blueprint

    # Legacy mode: if dbt_docs_dir is provided, use single project
    legacy_mode = dbt_docs_dir is not None
    if legacy_mode:
        if isinstance(dbt_docs_dir, str):
            dbt_docs_dir = Path(dbt_docs_dir)

    # Create DBTDocsView class with configuration
    DBTDocsView = _create_dbt_docs_view_class(
        legacy_mode, dbt_docs_dir, variable_name, default_project
    )

    # Creating a flask blueprint to integrate the templates and static folder
    # Note: In multi-project mode, static files are served from individual project dirs
    static_folder = dbt_docs_dir.as_posix() if legacy_mode else None

    bp = Blueprint(
        "DBT Plugin",
        __name__,
        template_folder=None,
        static_folder=static_folder,
    )

    class AirflowDbtDocsPlugin(AirflowPlugin):
        name = "DBT Docs Plugin"
        flask_blueprints = [bp]
        appbuilder_views = [{"name": "DBT Docs", "category": "", "view": DBTDocsView()}]

    return AirflowDbtDocsPlugin
