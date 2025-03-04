import os
import sys
from pathlib import Path
from typing import List, Callable, Optional

# IMPORTANT! this will import the overrides, and activates the patches
from opendbt.dbt import *
from opendbt.logger import OpenDbtLogger
from opendbt.utils import Utils


class OpenDbtCli:
    def __init__(self, project_dir: Path, profiles_dir: Optional[Path] = None, callbacks: Optional[List[Callable]] = None):
        self.project_dir: Path = Path(get_nearest_project_dir(project_dir.as_posix()))
        self.profiles_dir: Optional[Path] = profiles_dir if profiles_dir else default_profiles_dir()
        self._project: Optional[PartialProject] = None
        self._user_callbacks: List[Callable] = callbacks if callbacks else []
        self._project_callbacks: List[Callable] = []

    @property
    def project(self) -> PartialProject:
        if not self._project:
            self._project = PartialProject.from_project_root(project_root=self.project_dir.as_posix(),
                                                             verify_version=True)

        return self._project

    @property
    def project_dict(self) -> dict:
        return self.project.project_dict

    @property
    def project_vars(self) -> dict:
        """
        :return: dict: Variables defined in the `dbt_project.yml` file, `vars`.
                Note:
                    This method only retrieves global project variables specified within the `dbt_project.yml` file.
                    Variables passed via command-line arguments are not included in the returned dictionary.
        """
        return self.project_dict.get('vars', {})


    @property
    def project_callbacks(self) -> List[Callable]:
        if not self._project_callbacks:
            self._project_callbacks = list(self._user_callbacks)
            dbt_callbacks_str = self.project_vars.get('dbt_callbacks', "")
            dbt_callbacks_list = [c for c in dbt_callbacks_str.split(',') if c.strip()]
            for callback_module_name in dbt_callbacks_list:
                callback_func = Utils.import_module_attribute_by_name(callback_module_name.strip())
                self._project_callbacks.append(callback_func)

        return self._project_callbacks

    def invoke(self, args: List[str], callbacks: Optional[List[Callable]] = None) -> dbtRunnerResult:
        """
        Run dbt with the given arguments.

        :param args: The arguments to pass to dbt.
        :param callbacks:
        :return: The result of the dbt run.
        """
        run_callbacks = self.project_callbacks + (callbacks if callbacks else self.project_callbacks)
        run_args = args or []
        if "--project-dir" not in run_args:
            run_args += ["--project-dir", self.project_dir.as_posix()]
        if "--profiles-dir" not in run_args and self.profiles_dir:
            run_args += ["--profiles-dir", self.profiles_dir.as_posix()]
        return self.run(args=run_args, callbacks=run_callbacks)

    @staticmethod
    def run(args: List[str], callbacks: Optional[List[Callable]] = None) -> dbtRunnerResult:
        """
        Run dbt with the given arguments.

        :param callbacks:
        :param args: The arguments to pass to dbt.
        :return: The result of the dbt run.
        """
        callbacks = callbacks if callbacks else []
        # https://docs.getdbt.com/reference/programmatic-invocations
        runner = DbtCliRunner(callbacks=callbacks)
        result: dbtRunnerResult = runner.invoke(args)

        if result.success:
            return result

        if result.exception:
            raise result.exception

        # take error message and raise it as exception
        err_messages = [res.message for res in result.result if isinstance(res, RunResult) and res.status == 'error']

        if err_messages:
            raise DbtRuntimeError(msg="\n".join(err_messages))

        raise DbtRuntimeError(msg=f"DBT execution failed!")

    def manifest(self, partial_parse: bool = True, no_write_manifest: bool = True) -> Manifest:
        args = ["parse"]
        if partial_parse:
            args.append("--partial-parse")
        if no_write_manifest:
            args.append("--no-write-json")

        result = self.invoke(args=args)
        if not result.success:
            raise Exception(f"DBT execution failed. result:{result}")
        if isinstance(result.result, Manifest):
            return result.result

        raise Exception(f"DBT execution did not return Manifest object. returned:{type(result.result)}")

    def generate_docs(self, args: Optional[List[str]] = None):
        _args = ["docs", "generate"] + (args if args else [])
        self.invoke(args=_args)


class OpenDbtProject(OpenDbtLogger):
    """
    This class is used to take action on a dbt project.
    """

    DEFAULT_TARGET = 'dev'  # development

    def __init__(self, project_dir: Path, target: Optional[str] = None, profiles_dir: Optional[Path] = None, args: Optional[List[str]] = None, callbacks: Optional[List[Callable]] = None):
        super().__init__()
        self.project_dir: Path = project_dir
        self.profiles_dir: Optional[Path] = profiles_dir
        self.target: str = target if target else self.DEFAULT_TARGET
        self.args: List[str] = args if args else []
        self.cli: OpenDbtCli = OpenDbtCli(project_dir=self.project_dir, profiles_dir=self.profiles_dir, callbacks=callbacks)

    @property
    def project(self) -> PartialProject:
        return self.cli.project

    @property
    def project_dict(self) -> dict:
        return self.cli.project_dict

    @property
    def project_vars(self) -> dict:
        return self.cli.project_vars

    def run(self, command: str = "build", target: Optional[str] = None, args: Optional[List[str]] = None, use_subprocess: bool = False,
            write_json: bool = False) -> Optional[dbtRunnerResult]:
        run_args = args if args else []
        run_args.extend(["--target", target if target else self.target])
        run_args.extend(["--project-dir", self.project_dir.as_posix()])
        if self.profiles_dir:
            run_args.extend(["--profiles-dir", self.profiles_dir.as_posix()])
        run_args = [command] + run_args + self.args
        if write_json:
            run_args.remove("--no-write-json")

        if use_subprocess:
            shell = False
            self.log.info(f"Working dir: {os.getcwd()}")
            py_executable = sys.executable if sys.executable else 'python'
            self.log.info(f"Python executable: {py_executable}")
            __command = [py_executable, '-m', 'opendbt'] + run_args
            self.log.info(f"Running command (shell={shell}) `{' '.join(__command)}`")
            Utils.runcommand(command=__command)
            return None

        self.log.info(f"Running `dbt {' '.join(run_args)}`")
        return self.cli.invoke(args=run_args)

    def manifest(self, partial_parse: bool = True, no_write_manifest: bool = True) -> Manifest:
        return self.cli.manifest(partial_parse=partial_parse, no_write_manifest=no_write_manifest)

    def generate_docs(self, args: Optional[List[str]] = None):
        return self.cli.generate_docs(args=args)
