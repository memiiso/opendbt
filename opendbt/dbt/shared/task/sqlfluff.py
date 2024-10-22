import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from dbt.config import RuntimeConfig
from dbt.contracts.results import (
    CatalogResults,
    CatalogArtifact, RunExecutionResult,
)
from dbt.task.compile import CompileTask
from sqlfluff.cli import commands
from sqlfluff.core import Linter, FluffConfig
from sqlfluff.core.linter import LintingResult
from sqlfluff_templater_dbt import DbtTemplater


class SqlFluffTasks(CompileTask):

    def __init__(self, args, config, manifest):
        super().__init__(args, config, manifest)

        self.sqlfluff_config = FluffConfig.from_path(path=self.config.project_root)

        templater_obj = self.sqlfluff_config._configs["core"]["templater_obj"]
        if isinstance(templater_obj, DbtTemplater):
            templater_obj: DbtTemplater
            self.config: RuntimeConfig
            templater_obj.project_root = self.config.project_root
            templater_obj.working_dir = self.config.project_root
        self.linter = Linter(self.sqlfluff_config)

    def get_result(self, elapsed_time: float, violations: list, num_violations: int):
        run_result = RunExecutionResult(
            results=[],
            elapsed_time=elapsed_time,
            generated_at=datetime.now(),
            # args=dbt.utils.args_to_dict(self.args),
            args={},
        )
        result = CatalogArtifact.from_results(
            nodes={},
            sources={},
            generated_at=datetime.now(),
            errors=violations if violations else None,
            compile_results=run_result,
        )
        if num_violations > 0:
            setattr(result, 'exception', Exception(f"Linting {num_violations} errors found!"))
            result.exception = Exception(f"Linting {num_violations} errors found!")

        return result

    def lint(self) -> CatalogArtifact:
        os.chdir(self.config.project_root)
        lint_result: LintingResult = self.linter.lint_paths(paths=(self.config.project_root,))
        result = self.get_result(lint_result.total_time, lint_result.get_violations(), lint_result.num_violations())
        if lint_result.num_violations() > 0:
            print(f"Linting {lint_result.num_violations()} errors found!")
            for error in lint_result.as_records():
                filepath = Path(error['filepath'])
                violations: list = error['violations']
                if violations:
                    print(f"File: {filepath.relative_to(self.config.project_root)}")
                    for violation in violations:
                        print(f"    {violation}")
                        # print(f"Code:{violation['code']} Line:{violation['start_line_no']}, LinePos:{violation['start_line_pos']} {violation['description']}")
        return result

    def fix(self) -> CatalogArtifact:
        os.chdir(self.config.project_root)
        lnt, formatter = commands.get_linter_and_formatter(cfg=self.sqlfluff_config)
        lint_result: LintingResult = lnt.lint_paths(
            paths=(self.config.project_root,),
            fix=True,
            apply_fixes=True
        )
        result = self.get_result(lint_result.total_time, [], 0)
        return result

    @classmethod
    def interpret_results(self, results: Optional[CatalogResults]) -> bool:
        if results is None:
            return False
        if hasattr(results, "errors") and results.errors:
            return False
        return True
