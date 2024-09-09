import os
from datetime import datetime
from typing import Optional

from dbt.contracts.results import (
    CatalogResults,
    CatalogArtifact,
)
from dbt.task.compile import CompileTask
from sqlfluff.core import Linter, FluffConfig


class SqlFluffTasks(CompileTask):

    def __init__(self, args, config, manifest):
        super().__init__(args, config, manifest)
        self.sqlfluff_config = FluffConfig.from_path(path=self.config.project_root)

    def lint(self) -> CatalogArtifact:
        results = CatalogArtifact.from_results(
            nodes={},
            sources={},
            generated_at=datetime.utcnow(),
            errors=None,
            compile_results=None,
        )

        os.chdir(self.config.project_root)
        linter = Linter(self.sqlfluff_config)
        result = linter.lint_paths(paths=(self.config.project_root,))
        violations: list = result.get_violations()
        success = True if not violations else False
        if violations:
            print("SqlFluff Linting Errors")
            print("\n".join([str(item) for item in violations]))
        return results, success

    @classmethod
    def interpret_results(self, results: Optional[CatalogResults]) -> bool:
        if results is None:
            return False
        if hasattr(results, "errors") and results.errors:
            return False
        return True
