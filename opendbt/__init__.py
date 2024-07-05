import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

from dbt.cli.main import dbtRunnerResult
from dbt.contracts.graph.manifest import Manifest

import opendbt.client


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
            Utils.runcommand(command=['opendbt'] + run_args)
            return None
        else:
            self.log.info(f"Running `dbt {' '.join(run_args)}`")
            return client.OpenDbtCli.run(args=run_args)

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


class Utils(object):

    @staticmethod
    def runcommand(command: list, shell=False):
        logger = OpenDbtLogger()

        logger.log.info("Working dir is %s" % os.getcwd())
        logger.log.info("Running command (shell=%s) `%s`" % (shell, " ".join(command)))
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1,
                              universal_newlines=True, shell=shell) as p:
            for line in p.stdout:
                if line:
                    print(line.strip())

        if p.returncode != 0:
            raise subprocess.CalledProcessError(p.returncode, p.args)


def main():
    p = argparse.ArgumentParser()
    _, args = p.parse_known_args()
    client.OpenDbtCli.run(args=args)


if __name__ == "__main__":
    main()
