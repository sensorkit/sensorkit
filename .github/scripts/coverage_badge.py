"""Render a shields.io endpoint badge from a Cobertura coverage report."""

import json
import sys
import xml.etree.ElementTree as ElementTree


def color_for(percent: int) -> str:
    """Pick the shields palette entry for a coverage percentage.

    Args:
        percent: Line coverage, rounded to a whole percent.

    Returns:
        A color name shields resolves against its own palette.
    """
    if percent >= 90:
        return "brightgreen"

    if percent >= 80:
        return "green"

    if percent >= 60:
        return "yellow"

    return "red"


def main() -> None:
    source, target = sys.argv[1], sys.argv[2]

    root = ElementTree.parse(source).getroot()
    percent = round(float(root.get("line-rate", "0")) * 100)

    badge = {
        "schemaVersion": 1,
        "label": "coverage",
        "message": f"{percent}%",
        "color": color_for(percent),
    }

    with open(target, "w", encoding="utf-8") as handle:
        json.dump(badge, handle)


if __name__ == "__main__":
    main()
