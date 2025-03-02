from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from dbt.config import RuntimeConfig
from dbt.config.project import load_yml_dict
from dbt.constants import DEPENDENCIES_FILE_NAME
from dbt.exceptions import DbtProjectError, NonUniquePackageNameError
from typing_extensions import override

from opendbt.runtime_patcher import PatchClass

# pylint: disable=too-many-ancestors
@dataclass
@PatchClass(module_name="dbt.config", target_name="RuntimeConfig")
@PatchClass(module_name="dbt.cli.requires", target_name="RuntimeConfig")
class OpenDbtRuntimeConfig(RuntimeConfig):
    def load_dependence_projects(self):
        dependencies_yml_dict = load_yml_dict(f"{self.project_root}/{DEPENDENCIES_FILE_NAME}")

        if "projects" not in dependencies_yml_dict:
            return

        projects = dependencies_yml_dict["projects"]
        project_root_parent = Path(self.project_root).parent
        for project in projects:
            path = project_root_parent.joinpath(project['name'])
            try:
                project = self.new_project(str(path.as_posix()))
            except DbtProjectError as e:
                raise DbtProjectError(
                    f"Failed to read depending project: {e} \n project path:{path.as_posix()}",
                    result_type="invalid_project",
                    path=path,
                ) from e

            yield project.project_name, project

    @override
    def load_dependencies(self, base_only=False) -> Mapping[str, "RuntimeConfig"]:
        # if self.dependencies is None:

        if self.dependencies is None:
            # this sets self.dependencies variable!
            self.dependencies = super().load_dependencies(base_only=base_only)

            # additionally load `projects` defined in `dependencies.yml`
            for project_name, project in self.load_dependence_projects():
                if project_name in self.dependencies:
                    raise NonUniquePackageNameError(project_name)
                self.dependencies[project_name] = project

        return self.dependencies
