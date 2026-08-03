# SPDX-License-Identifier: Apache-2.0
"""Shared fixtures for the std tests."""
from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path

import pytest

GOLDEN = Path(__file__).resolve().parent / "golden"


@pytest.fixture(scope="session")
def assert_golden() -> Callable[[str, str], None]:
    """Compare rendered text against golden/<name>.txt.

    Regenerate deliberately, and read the diff::

        SK_REGEN=1 uv run pytest
    """
    def check(name: str, actual: str) -> None:
        path = GOLDEN / f"{name}.txt"
        actual = actual.rstrip("\n") + "\n"
        if os.environ.get("SK_REGEN"):
            path.parent.mkdir(exist_ok=True)
            path.write_text(actual)
            return
        assert path.exists(), f"missing golden file {path}; run with SK_REGEN=1"
        assert actual == path.read_text(), f"output changed vs {path}"

    return check
