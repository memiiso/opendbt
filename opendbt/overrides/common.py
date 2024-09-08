import importlib

DBT_CUSTOM_ADAPTER_VAR = 'dbt_custom_adapter'
import shutil
from pathlib import Path

import click
from packaging.version import Version
import dbt

if Version(dbt.version.get_installed_version().to_version_string(skip_matcher=True)) < Version("1.8.0"):
    from dbt.task.generate import GenerateTask
    from opendbt.overrides.dbt17 import AdapterContainerDbtOverride
else:
    from dbt.task.docs.generate import GenerateTask
    from opendbt.overrides.dbt18 import AdapterContainerDbtOverride


class OpenDbtAdapterContainer(AdapterContainerDbtOverride):

    def get_custom_adapter_config_value(self, config: 'AdapterRequiredConfig') -> str:
        # FIRST: it's set as cli value: dbt run --vars {'dbt_custom_adapter': 'custom_adapters.DuckDBAdapterV1Custom'}
        if hasattr(config, 'cli_vars') and DBT_CUSTOM_ADAPTER_VAR in config.cli_vars:
            custom_adapter_class_name: str = config.cli_vars[DBT_CUSTOM_ADAPTER_VAR]
            if custom_adapter_class_name and custom_adapter_class_name.strip():
                return custom_adapter_class_name
        # SECOND: it's set inside dbt_project.yml
        if hasattr(config, 'vars') and DBT_CUSTOM_ADAPTER_VAR in config.vars.to_dict():
            custom_adapter_class_name: str = config.vars.to_dict()[DBT_CUSTOM_ADAPTER_VAR]
            if custom_adapter_class_name and custom_adapter_class_name.strip():
                return custom_adapter_class_name

        return None

    def get_custom_adapter_class_by_name(self, custom_adapter_class_name: str):
        if "." not in custom_adapter_class_name:
            raise ValueError(f"Unexpected adapter class name: `{custom_adapter_class_name}` ,"
                             f"Expecting something like:`my.sample.library.MyAdapterClass`")

        __module, __class = custom_adapter_class_name.rsplit('.', 1)
        try:
            user_adapter_module = importlib.import_module(__module)
            user_adapter_class = getattr(user_adapter_module, __class)
            return user_adapter_class
        except ModuleNotFoundError as mnfe:
            raise Exception(f"Module of provided adapter not found, provided: {custom_adapter_class_name}") from mnfe


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
