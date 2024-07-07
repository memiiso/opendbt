from pathlib import Path


def init_plugins_dbtdocs_page(dbt_docs_dir: Path):
    from airflow.plugins_manager import AirflowPlugin
    from flask import Blueprint
    from flask_appbuilder import BaseView, expose

    class DBTDocsView(BaseView):
        # Use it like this if you want to restrict your view to readonly::
        base_permissions = ['can_list', 'can_show']
        default_view = "index"

        @expose("/")
        def index(self):
            return dbt_docs_dir.joinpath("index.html").read_text()
            # return self.render_template("index.html", content="")

    # Creating a flask blueprint to integrate the templates and static folder
    bp = Blueprint(
        "DBT Docs Plugin",
        __name__,
        template_folder=dbt_docs_dir.as_posix(),
        static_folder=dbt_docs_dir.as_posix(),
        static_url_path='/dbtdocsview'
    )

    v_header_menu = {"name": "DBT Docs", "category": "", "view": DBTDocsView()}

    class AirflowDbtDocsPlugin(AirflowPlugin):
        name = "DBT Docs Plugin"
        flask_blueprints = [bp]
        appbuilder_views = [v_header_menu]

    return AirflowDbtDocsPlugin
