# SPDX-License-Identifier: Apache-2.0
"""Tests for Otto utility functions."""

import pytest

from sensorkit.otto.utils import ListType, ObjectListManager
from sensorkit.otto.program import OttoState


class TestObjectListManager:
    @pytest.fixture
    def state(self):
        return OttoState(
            whitelist=["25544", "42738", "39120"],
            graylist=[],
            blacklist=[],
        )

    @pytest.fixture
    def manager(self, state):
        save_callback = pytest.importorskip("unittest.mock").AsyncMock()
        return ObjectListManager(state, save_callback), save_callback

    @pytest.mark.asyncio
    async def test_move_whitelist_to_graylist(self, state, manager):
        mgr, save = manager
        result = await mgr.move_object("25544", ListType.WHITELIST, ListType.GRAYLIST)
        assert result is True
        assert "25544" not in state.whitelist
        assert "25544" in state.graylist
        save.assert_awaited()

    @pytest.mark.asyncio
    async def test_move_whitelist_to_blacklist(self, state, manager):
        mgr, save = manager
        result = await mgr.move_object("42738", ListType.WHITELIST, ListType.BLACKLIST)
        assert result is True
        assert "42738" not in state.whitelist
        assert "42738" in state.blacklist

    @pytest.mark.asyncio
    async def test_move_graylist_to_whitelist(self, state, manager):
        mgr, _ = manager
        # First move to graylist
        await mgr.move_object("25544", ListType.WHITELIST, ListType.GRAYLIST)
        assert "25544" in state.graylist

        # Then back to whitelist
        await mgr.move_object("25544", ListType.GRAYLIST, ListType.WHITELIST)
        assert "25544" in state.whitelist
        assert "25544" not in state.graylist

    @pytest.mark.asyncio
    async def test_move_nonexistent_object_raises(self, state, manager):
        mgr, _ = manager
        with pytest.raises(ValueError):
            await mgr.move_object("99999", ListType.WHITELIST, ListType.GRAYLIST)


class TestListType:
    def test_values(self):
        assert ListType.WHITELIST.value == "whitelist"
        assert ListType.GRAYLIST.value == "graylist"
        assert ListType.BLACKLIST.value == "blacklist"
