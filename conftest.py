# SPDX-License-Identifier: Apache-2.0
"""Root pytest configuration shared by the core and module test suites."""

import functools
import os

import pytest

localonly = os.getenv("ENV", "").lower() == "local"


@functools.cache
def docker_available() -> bool:
    """Whether a Docker daemon able to run Linux containers is reachable."""
    try:
        import testcontainers  # noqa: F401
        from docker.errors import DockerException

        import docker
    except ImportError:
        return False

    try:
        # We require Linux images for testcontainers.
        return docker.from_env().info().get("OSType") == "linux"
    except (DockerException, OSError):
        return False


def pytest_collection_modifyitems(items):
    """Skip tests whose environment this run cannot provide.

    `localonly` covers hardware, a running backend, or another site-local
    service. `docker` covers containers brought up by testcontainers.
    """
    skip_localonly = pytest.mark.skip(reason="local only test; enable by setting `ENV=local`")
    skip_docker = pytest.mark.skip(reason="requires a Docker daemon running Linux containers")

    for item in items:
        if not localonly and "localonly" in item.keywords:
            item.add_marker(skip_localonly)

        if "docker" in item.keywords and not docker_available():
            item.add_marker(skip_docker)
