# SPDX-License-Identifier: Apache-2.0
"""Root pytest configuration shared by the core and module test suites."""

import os

import pytest

localonly = os.getenv("ENV", "").lower() == "local"


def pytest_collection_modifyitems(items):
    """Skip tests marked `localonly` unless ENV=local.

    Tests carrying the marker need hardware, a running backend, or another
    site-local service that CI cannot provide.
    """
    if localonly:
        return

    skip = pytest.mark.skip(reason="local only test; enable by setting `ENV=local`")

    for item in items:
        if "localonly" in item.keywords:
            item.add_marker(skip)
