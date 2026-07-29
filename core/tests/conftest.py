# SPDX-License-Identifier: Apache-2.0

import pytest
import pytest_asyncio


@pytest_asyncio.fixture(
    params=[
        "fake",
        pytest.param("nats", marks=pytest.mark.docker),
    ]
)
async def _backend(request, make_backend):
    """Backend fixture for internal backend tests."""
    async with make_backend(request.param) as backend:
        yield backend
