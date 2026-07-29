# SPDX-License-Identifier: Apache-2.0
"""NINA Advanced API client fake.

NINA is an external application reached over HTTP, so its client is stubbed rather than run
against a real stack.
"""

from __future__ import annotations


class FakeNinaClient:
    """NinaClient with its HTTP layer replaced — records requests and serves a configurable info response."""

    def __init__(
        self,
        info_response: dict | None = None,
        username: str | None = None,
        password: str | None = None,
    ):
        self._requests: list[tuple[str, dict]] = []
        # Connecting consults the credentials to decide whether to log in first.
        self._username = username
        self._password = password
        self.logins = 0
        self._info_response = info_response or {
            "Connected": True,
            "Slewing": False,
            "Tracking": False,
            "AtHome": True,
            "AtPark": False,
        }

    async def get(self, path: str, **params):
        self._requests.append((path, params))
        return self._info_response

    async def login(self):
        self.logins += 1

    async def close(self):
        pass

    def set_info(self, **overrides):
        self._info_response.update(overrides)

    def find_requests(self, path: str) -> list[tuple[str, dict]]:
        return [(p, params) for p, params in self._requests if p == path]

    def last_request(self) -> tuple[str, dict]:
        return self._requests[-1] if self._requests else ("", {})
