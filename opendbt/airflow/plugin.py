from pathlib import Path


# pylint: disable=inconsistent-return-statements
def init_plugins_dbtdocs_page(dbt_docs_dir: Path):
    from airflow.plugins_manager import AirflowPlugin
    from flask import Blueprint
    from flask_appbuilder import BaseView, expose
    from flask import abort
    from airflow.www.auth import has_access
    from airflow.security import permissions

    class DBTDocsView(BaseView):
        route_base = "/dbt"
        default_view = "dbt_docs_index"

        @expose("/dbt_docs_index.html")  # type: ignore[misc]
        @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
        def dbt_docs_index(self):
            if not dbt_docs_dir.joinpath("index.html").is_file():
                abort(404)
            else:
                return dbt_docs_dir.joinpath("index.html").read_text()
            # return self.render_template("index.html", content="")

        @expose("/catalog.json")  # type: ignore[misc]
        @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
        def catalog(self):
            if not dbt_docs_dir.joinpath("catalog.json").is_file():
                abort(404)
            else:
                data = dbt_docs_dir.joinpath("catalog.json").read_text()
                return data, 200, {"Content-Type": "application/json"}
            # return self.render_template("index.html", content="")

        @expose("/manifest.json")  # type: ignore[misc]
        @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
        def manifest(self):
            if not dbt_docs_dir.joinpath("manifest.json").is_file():
                abort(404)
            else:
                data = dbt_docs_dir.joinpath("manifest.json").read_text()
                return data, 200, {"Content-Type": "application/json"}
            # return self.render_template("index.html", content="")

    # Creating a flask blueprint to integrate the templates and static folder
    bp = Blueprint(
        "DBT Plugin",
        __name__,
        template_folder=dbt_docs_dir.as_posix(),
        static_folder=dbt_docs_dir.as_posix(),
        # static_url_path='/dbtdocsview'
    )

    class AirflowDbtDocsPlugin(AirflowPlugin):
        name = "DBT Docs Plugin"
        flask_blueprints = [bp]
        appbuilder_views = [{"name": "DBT Docs", "category": "", "view": DBTDocsView()}]

    return AirflowDbtDocsPlugin
