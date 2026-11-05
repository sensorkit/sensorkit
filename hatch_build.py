"""
Hatchling hooks for build-time assembly of module source trees and the PyPI readme.
"""

from __future__ import annotations

import os

from hatchling.builders.hooks.plugin.interface import BuildHookInterface
from hatchling.metadata.plugin.interface import MetadataHookInterface


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


class CustomMetadataHook(MetadataHookInterface):
    PLUGIN_NAME = "custom"

    def update(self, metadata: dict) -> None:
        # PyPI has no dark theme and ignores GitHub's theme fragments, so it would
        # show the dark-mode logo as a second, mostly invisible copy.
        with open(os.path.join(self.root, "README.md"), encoding="utf-8") as f:
            lines = [line for line in f if "#gh-dark-mode-only" not in line]

        metadata["readme"] = {"content-type": "text/markdown", "text": "".join(lines)}
