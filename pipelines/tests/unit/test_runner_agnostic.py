"""Guard: task code (defs, sources, checks, gold) never imports a runner.

CLAUDE.md: pipeline task functions stay runner-agnostic. If task code imported
the local runner, catalog or CLI, it could not run unchanged on an Atlas worker.
"""

import ast
from pathlib import Path

import atlas_pipelines

PKG = Path(atlas_pipelines.__file__).parent
TASK_PACKAGES = ("defs", "sources", "checks", "gold")
FORBIDDEN = ("atlas_pipelines.core.runner", "atlas_pipelines.core.catalog", "atlas_pipelines.cli")


def test_task_code_does_not_import_runners():
    offenders = []
    for sub in TASK_PACKAGES:
        for f in (PKG / sub).rglob("*.py") if (PKG / sub).exists() else []:
            for node in ast.walk(ast.parse(f.read_text(encoding="utf-8"))):
                names = []
                if isinstance(node, ast.Import):
                    names = [a.name for a in node.names]
                elif isinstance(node, ast.ImportFrom) and node.module:
                    names = [node.module]
                offenders += [f"{f.name}: {n}" for n in names if n.startswith(FORBIDDEN)]
    assert offenders == []
