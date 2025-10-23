import shutil
from pathlib import Path

import click
from dbt.task.docs.generate import GenerateTask, CATALOG_FILENAME, MANIFEST_FILE_NAME

from opendbt.catalog import OpenDbtCatalog
from opendbt.runtime_patcher import PatchClass


@PatchClass(module_name="dbt.task.docs.generate", target_name="GenerateTask")
class OpenDbtGenerateTask(GenerateTask):

    def deploy_user_index_html(self):
        # run custom code
        target = Path(self.config.project_target_path).joinpath("index.html")
        for dir in self.config.docs_paths:
            index_html = Path(self.config.project_root).joinpath(dir).joinpath("index.html")
            if index_html.is_file() and index_html.exists():
                # override default dbt provided index.html with user index.html file
                shutil.copyfile(index_html, target)
                click.echo(f"Using user provided documentation page: {index_html.as_posix()}")
                return

        # If no user-provided index.html found, deploy opendbt's enhanced catalog UI
        opendbt_index = Path(__file__).parent.parent.parent.joinpath("docs").joinpath("index.html")
        if opendbt_index.is_file() and opendbt_index.exists():
            shutil.copyfile(opendbt_index, target)
            click.echo(f"Using opendbt enhanced catalog UI: {opendbt_index.as_posix()}")

    def generate_opendbt_catalogl_json(self):
        catalog_path = Path(self.config.project_target_path).joinpath(CATALOG_FILENAME)
        manifest_path = Path(self.config.project_target_path).joinpath(MANIFEST_FILE_NAME)
        catalog = OpenDbtCatalog(manifest_path=manifest_path, catalog_path=catalog_path)
        catalog.export()

    def run(self):
        # Call the original dbt run method
        result = super().run()
        self.deploy_user_index_html()
        self.generate_opendbt_catalogl_json()
        return result
