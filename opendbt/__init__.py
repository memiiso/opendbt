import logging
import os
import sys
from pathlib import Path

from dbt.cli.main import dbtRunner as DbtCliRunner
from dbt.cli.main import dbtRunnerResult
from dbt.contracts.graph.manifest import Manifest
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
    def run(args: list) -> dbtRunnerResult:
        """
        Run dbt with the given arguments.

        :param args: The arguments to pass to dbt.
        :return: The result of the dbt run.
        """
        # https://docs.getdbt.com/reference/programmatic-invocations
        dbt = DbtCliRunner()
        result: dbtRunnerResult = dbt.invoke(args)
        if result.success:
            return result

        # print query for user to run and see the failing rows
        _exception = result.exception if result.exception else None
        if (_exception is None and hasattr(result.result, 'results') and result.result.results and
                len(result.result.results) > 0 and result.result.results[0].message
        ):
            _exception = DbtRuntimeError(result.result.results[0].message)

        if _exception is None:
            raise DbtRuntimeError(f"DBT execution failed!")
        if _exception:
            raise _exception
        else:
            return result


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
            self.log.info(f"Working dir is {os.getcwd()}")
            py_executable = sys.executable if sys.executable else 'python'
            self.log.info(f"Python executable: {py_executable}")
            __command = [py_executable, '-m', 'opendbt'] + run_args
            self.log.info(f"Running command (shell={shell}) `{' '.join(__command)}`")
            Utils.runcommand(command=__command)
            return None
        else:
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
