import logging
import os
import sys
from pathlib import Path

from dbt.cli.main import dbtRunner as DbtCliRunner
from dbt.cli.main import dbtRunnerResult
from dbt.config import PartialProject
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.results import RunResult
from dbt.exceptions import DbtRuntimeError

from opendbt.dbt import patch_dbt
from opendbt.utils import Utils

######################
patch_dbt()


######################

class OpenDbtLogger:
    _log = None

    @property
    def log(self) -> logging.Logger:
        if self._log is None:
            self._log = logging.getLogger(name="opendbt")
            if not self._log.hasHandlers():
                handler = logging.StreamHandler(sys.stdout)
                formatter = logging.Formatter("[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s")
                handler.setFormatter(formatter)
                handler.setLevel(logging.INFO)
                self._log.addHandler(handler)
        return self._log


class OpenDbtCli:

    @staticmethod
    def run(args: list, callbacks: list = None) -> dbtRunnerResult:
        """
        Run dbt with the given arguments.

        :param callbacks:
        :param args: The arguments to pass to dbt.
        :return: The result of the dbt run.
        """
        callbacks = callbacks if callbacks else []
        # https://docs.getdbt.com/reference/programmatic-invocations
        dbtcr = DbtCliRunner(callbacks=callbacks)
        result: dbtRunnerResult = dbtcr.invoke(args)
        if result.success:
            return result

        if result.exception:
            raise result.exception

        # take error message and raise it as exception
        for _result in result.result:
            _result: RunResult
            _result_messages = ""
            if _result.status == 'error':
                _result_messages += f"{_result_messages}\n"
            if _result_messages:
                raise DbtRuntimeError(msg=_result.message)

        raise DbtRuntimeError(msg=f"DBT execution failed!")


class OpenDbtProject(OpenDbtLogger):
    """
    This class is used to take action on a dbt project.
    """

    DEFAULT_TARGET = 'dev'  # development

    def __init__(self, project_dir: Path, target: str = None, profiles_dir: Path = None, args: list = None):
        super().__init__()
        self.project_dir: Path = project_dir
        self.profiles_dir: Path = profiles_dir
        self.target: str = target if target else self.DEFAULT_TARGET
        self.args = args if args else []
        self._project: PartialProject = None

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
        if 'vars' in self.project_dict:
            return self.project_dict['vars']

        return {}

    def run(self, command: str = "build", target: str = None, args: list = None, use_subprocess: bool = False,
            write_json: bool = False) -> dbtRunnerResult:

        run_args = args if args else []
        run_args += ["--target", target if target else self.target]
        run_args += ["--project-dir", self.project_dir.as_posix()]
        if self.profiles_dir:
            run_args += ["--profiles-dir", self.profiles_dir.as_posix()]
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
        return OpenDbtCli.run(args=run_args)

    def manifest(self, partial_parse=True, no_write_manifest=True) -> Manifest:
        args = []
        if partial_parse:
            args += ["--partial-parse"]
        if no_write_manifest:
            args += ["--no-write-json"]

        result = self.run(command="parse", args=args)
        if isinstance(result.result, Manifest):
            return result.result

        raise Exception(f"DBT execution did not return Manifest object. returned:{type(result.result)}")

    def generate_docs(self, args: list = None):
        _args = ["generate"] + args if args else []
        self.run(command="docs", args=_args)
