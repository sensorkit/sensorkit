"""
Hatchling build hook — performs build-time assembly of module source trees.

Wheel builds:    applies force_include for each module listed under
                 plugin-modules in [tool.hatch.build.hooks.custom].
Editable builds: returns immediately; dev-mode-dirs adds each module src
                 tree to the .pth file so all plugin subpackages resolve
                 live from their source directories.
"""

from __future__ import annotations

import os

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    PLUGIN_NAME = "custom"

    def initialize(self, version: str, build_data: dict) -> None:
        if version == "editable":
            return

        for module in self.config.get("modules", []):
            subpath = self.config.get("subpackage-path", "{module}")
            src = os.path.join(self.root, subpath.format(module=module))
            dest = os.path.join("sensorkit", module)
            build_data["force_include"][src] = dest
