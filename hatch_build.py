"""
Hatchling build hook — performs build-time assembly of module source trees.
"""

from __future__ import annotations

import os

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    PLUGIN_NAME = "custom"

    def initialize(self, version: str, build_data: dict) -> None:
        # Only assemble the flattened module layout for a standard wheel build.
        #
        # - sdist (target_name == "sdist"): must stay pure source; the module
        #   trees are shipped as-is via [tool.hatch.build.targets.sdist]. The
        #   hook re-runs when a wheel is later built from the sdist.
        # - editable install (version == "editable"): relies on `dev-mode-dirs`
        #   in pyproject.toml, not force_include, so this hook must not fire.
        if version != "standard" or self.target_name != "wheel":
            return

        for module in self.config.get("modules", []):
            subpath = self.config.get("subpackage-path", "{module}")
            src = os.path.join(self.root, subpath.format(module=module))
            dest = os.path.join("sensorkit", module)
            build_data["force_include"][src] = dest
