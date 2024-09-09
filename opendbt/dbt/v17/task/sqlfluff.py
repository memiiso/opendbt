import os
from datetime import datetime
from typing import Optional

from dbt.contracts.results import (
    CatalogResults,
    CatalogArtifact, RunExecutionResult,
)
from dbt.task.compile import CompileTask
from sqlfluff.core import Linter, FluffConfig


class SqlFluffTasks(CompileTask):

    def __init__(self, args, config, manifest):
        super().__init__(args, config, manifest)
        self.sqlfluff_config = FluffConfig.from_path(path=self.config.project_root)

    def lint(self) -> CatalogArtifact:
        # dummy result
        run_result = RunExecutionResult(
            results=[],
            elapsed_time=0.0,
            generated_at=datetime.utcnow(),
            # args=dbt.utils.args_to_dict(self.args),
            args={},
        )
        results = CatalogArtifact.from_results(
            nodes={},
            sources={},
            generated_at=datetime.utcnow(),
            errors=None,
            compile_results=run_result,
        )

        os.chdir(self.config.project_root)
        linter = Linter(self.sqlfluff_config)
        result = linter.lint_paths(paths=(self.config.project_root,))
        violations: list = result.get_violations()
        results.errors = violations
        if violations:
            print("SqlFluff Linting Errors")
            print("\n".join([str(item) for item in violations]))
        return results

    @classmethod
    def interpret_results(self, results: Optional[CatalogResults]) -> bool:
        if results is None:
            return False
        if hasattr(results, "errors") and results.errors:
            return False
        return True
