import os
from datetime import datetime
from typing import Optional

from dbt.contracts.results import (
    CatalogResults,
    CatalogArtifact, RunExecutionResult,
)
from dbt.task.compile import CompileTask
from sqlfluff.cli import commands
from sqlfluff.core import Linter, FluffConfig


class SqlFluffTasks(CompileTask):

    def __init__(self, args, config, manifest):
        super().__init__(args, config, manifest)
        self.sqlfluff_config = FluffConfig.from_path(path=self.config.project_root)
        self.linter = Linter(self.sqlfluff_config)
        # dummy result
        run_result = RunExecutionResult(
            results=[],
            elapsed_time=0.0,
            generated_at=datetime.utcnow(),
            # args=dbt.utils.args_to_dict(self.args),
            args={},
        )
        self.results = CatalogArtifact.from_results(
            nodes={},
            sources={},
            generated_at=datetime.utcnow(),
            errors=None,
            compile_results=run_result,
        )

    def lint(self) -> CatalogArtifact:
        os.chdir(self.config.project_root)
        lint_result = self.linter.lint_path(path=self.config.project_root)
        return self.return_result(lint_result=lint_result)

    def fix(self) -> CatalogArtifact:
        os.chdir(self.config.project_root)
        # lint_result = commands.fix(paths=(self.config.project_root))
        # lint_result = self.linter.lint_paths(paths=(self.config.project_root,), fix=True, apply_fixes=True)
        # Instantiate the linter
        lnt, formatter = commands.get_linter_and_formatter(self.sqlfluff_config)
        # Dispatch the detailed config from the linter.
        lint_result = formatter.dispatch_config(lnt)
        lnt.fix(formatter=formatter)

        return self.return_result(lint_result=lint_result)

    def return_result(self, lint_result):
        violations: list = lint_result.get_violations()
        self.results.errors = violations
        if violations:
            print("SqlFluff Linting Errors")
            print("\n".join([str(item) for item in lint_result.as_records()]))
        return self.results

    @classmethod
    def interpret_results(self, results: Optional[CatalogResults]) -> bool:
        if results is None:
            return False
        if hasattr(results, "errors") and results.errors:
            return False
        return True
