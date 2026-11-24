"""Atlas Grid pipeline package: runner-agnostic tasks, market registry, lake helpers.

Pipeline task functions in this package never import Atlas server internals;
they run unchanged on the local runner (core/runner.py) or an Atlas worker.
"""

__version__ = "0.1.0"
