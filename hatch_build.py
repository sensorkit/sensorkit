"""
Hatchling build hook — performs build-time assembly of module source trees.
"""

from __future__ import annotations

import os

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    PLUGIN_NAME = "custom"

    def initialize(self, version: str, build_data: dict) -> None:
        if version != "standard":
            return

        for module in self.config.get("modules", []):
            subpath = self.config.get("subpackage-path", "{module}")
            src = os.path.join(self.root, subpath.format(module=module))
            dest = os.path.join("sensorkit", module)
            build_data["force_include"][src] = dest
