import shutil
from pathlib import Path

import click
from dbt.task.generate import GenerateTask


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
                break

    def run(self):
        # Call the original dbt run method
        super().run()
        self.deploy_user_index_html()
