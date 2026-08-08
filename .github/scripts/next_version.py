# SPDX-License-Identifier: Apache-2.0
"""Resolve the version for the next release.

Derives the next version from the highest existing tag and the requested bump,
then writes `version`, `tag`, and `prerelease` to $GITHUB_OUTPUT. The emitted
tag always carries the PEP 440 normalized form, so the version hatch-vcs derives
at build time matches the tag character for character.

Releases only ever move forward, so a version that does not sort above every
tag and every release GitHub knows about is refused. Bumps that would be
ambiguous are refused rather than guessed: from a prerelease, only `prerelease`
and `final` are meaningful, and opening a new prerelease from a final version
needs `explicit`.
"""

import json
import os
import subprocess
import sys
from collections.abc import Iterable
from typing import NoReturn

from packaging.version import InvalidVersion, Version

TAG_PREFIX = "v"


def fail(message: str) -> NoReturn:
    print(f"::error::{message}")
    sys.exit(1)


def capture(*command: str) -> str:
    """Returns the stdout of a command, failing the run if it exits nonzero."""
    probe = subprocess.run(command, capture_output=True, text=True)
    if probe.returncode != 0:
        detail = probe.stderr.strip() or f"exit status {probe.returncode}"
        fail(f"{command[0]} failed: {detail}")

    return probe.stdout


def parsed(tags: Iterable[str]) -> list[Version]:
    """Returns the version behind every prefixed tag, skipping unparseable ones."""
    versions = []
    for tag in tags:
        if not tag.startswith(TAG_PREFIX):
            continue
        try:
            versions.append(Version(tag[len(TAG_PREFIX) :]))
        except InvalidVersion:
            continue

    return versions


def released() -> list[Version]:
    """Returns every parseable release tag, in PEP 440 order."""
    listing = capture("git", "tag", "--list", f"{TAG_PREFIX}*").split()

    return sorted(parsed(listing))


def claimed() -> list[Version]:
    """Returns the version of every release GitHub holds, drafts included."""
    listing = capture("gh", "release", "list", "--limit", "200", "--json", "tagName")

    return parsed(release["tagName"] for release in json.loads(listing))


def bumped(base: Version | None, bump: str, explicit: str) -> Version:
    match bump:
        case "explicit":
            if not explicit:
                fail("bump=explicit requires the version input")
            try:
                return Version(explicit)
            except InvalidVersion:
                fail(f"'{explicit}' is not a PEP 440 version")

        case _ if base is None:
            fail(f"no {TAG_PREFIX}* tags to bump from; use bump=explicit")

        case "prerelease":
            if base.pre is None:
                fail(f"{base} is not a prerelease; use bump=explicit to open one")
            phase, number = base.pre
            return Version(f"{base.major}.{base.minor}.{base.micro}{phase}{number + 1}")

        case "final":
            if not base.is_prerelease:
                fail(f"{base} is already a final release")
            return Version(f"{base.major}.{base.minor}.{base.micro}")

        case "major" | "minor" | "patch" if base.is_prerelease:
            fail(f"{base} is a prerelease; use bump=final or bump=explicit")

        case "major":
            return Version(f"{base.major + 1}.0.0")

        case "minor":
            return Version(f"{base.major}.{base.minor + 1}.0")

        case "patch":
            return Version(f"{base.major}.{base.minor}.{base.micro + 1}")

        case _:
            fail(f"unknown bump '{bump}'")


def main() -> None:
    bump = os.environ["BUMP"]
    explicit = os.environ.get("VERSION", "").strip()

    if explicit and bump != "explicit":
        fail(f"the version input only applies to bump=explicit, not bump={bump}")

    existing = released()
    base = existing[-1] if existing else None

    version = bumped(base, bump, explicit)
    if base is not None and version <= base:
        fail(f"{version} does not sort above the latest tag {TAG_PREFIX}{base}")

    # A draft carries no tag, so the git history above cannot see one. Anything
    # already drafted is left to be published or deleted rather than bumped
    # from, since a draft can still be thrown away.
    blocking = [held for held in claimed() if held >= version]
    if blocking:
        newest = max(blocking)
        fail(
            f"release {TAG_PREFIX}{newest} already claims {version} or a later "
            "version; publish or delete it first"
        )

    tag = f"{TAG_PREFIX}{version}"

    with open(os.environ["GITHUB_OUTPUT"], "a") as out:
        print(f"version={version}", file=out)
        print(f"tag={tag}", file=out)
        print(f"prerelease={str(version.is_prerelease).lower()}", file=out)

    print(f"{TAG_PREFIX}{base} -> {tag}" if base else f"first release: {tag}")


if __name__ == "__main__":
    main()
