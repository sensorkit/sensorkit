# SPDX-License-Identifier: Apache-2.0
"""Just-in-time CollectRequest generation against a real satellite catalog."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
from loguru import logger
from skyfield.api import EarthSatellite, load, wgs84

from sensorkit.astro.common import SitePosition

CLASSIFICATION_MARKING = "U"
DATA_MODE = "TEST"
SOURCE = "SensorKit"
WINDOW = timedelta(minutes=5)
MIN_ALTITUDE_DEG = 20.0
EXPOSURE_RANGE_S = (1, 5)
FRAME_RANGE = (3, 6)
BINNING_RANGE = (1, 4)
PICK_ATTEMPTS = 300

# A CollectResponse with one of these statuses closes its request, prompting
# fresh tasking on the next poll (mirrors the udl module's resolution set).
RESOLVING_STATUSES = frozenset({"COLLECTED", "COMPLETED", "REJECTED", "FAILED"})


def _udl_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@dataclass(frozen=True)
class CatalogSat:
    """One catalog entry, keeping the raw lines for elset targets."""

    name: str | None
    line1: str
    line2: str
    sat: EarthSatellite


def load_catalog(source: str) -> list[CatalogSat]:
    """Load a 2-line/3-line TLE catalog from a URL or local file path."""
    if source.startswith(("http://", "https://")):
        # Identify ourselves; some catalog hosts reject default python UAs.
        text = (
            httpx.get(
                source,
                timeout=60.0,
                follow_redirects=True,
                headers={"User-Agent": "sensorkit-mock-udl"},
            )
            .raise_for_status()
            .text
        )
    else:
        text = Path(source).read_text()

    ts = load.timescale()
    entries: list[CatalogSat] = []
    name: str | None = None
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for i, line in enumerate(lines):
        if line.startswith("1 ") and i + 1 < len(lines) and lines[i + 1].startswith("2 "):
            line2 = lines[i + 1]
            entries.append(
                CatalogSat(name, line, line2, EarthSatellite(line, line2, name, ts))
            )
            name = None
        elif not line.startswith("2 "):
            # Name line: 3LE "0 NAME" or a bare name line.
            name = line.removeprefix("0 ").strip() or None
    if not entries:
        raise RuntimeError(f"No TLEs parsed from {source}")
    return entries


@dataclass
class MockTasking:
    """Serves CollectRequests one at a time, reacting to CollectResponses.

    A generated request is served on every poll until its window ends or a
    resolving response for it arrives; then the next poll generates a fresh
    one. This intentionally re-serves live requests so client-side dedup gets
    exercised.
    """

    id_sensor: str
    target_types: tuple[str, ...]
    site: SitePosition
    catalog: list[CatalogSat]
    # Cooldown between requests: after one closes, wait this long before
    # tasking the next (0 = continuous). Lets slow consumers (SENPAI) keep up.
    idle_s: float = 0.0

    _cooldown_started: datetime | None = field(default=None)
    _records: list[dict[str, Any]] = field(default_factory=list)
    _closed: set[str] = field(default_factory=set)

    def __post_init__(self):
        self._ts = load.timescale()
        self._observer = wgs84.latlon(
            self.site.latitude_degrees,
            self.site.longitude_degrees,
            elevation_m=self.site.altitude_km * 1000.0,
        )

    # ── CollectResponse feedback ──

    def note_response(self, id_request: str, status: str | None) -> None:
        if status in RESOLVING_STATUSES:
            self._closed.add(id_request)

    # ── CollectRequest listing ──

    def list_requests(self, params: dict[str, str]) -> list[dict[str, Any]]:
        now = datetime.now(UTC)
        self._records = [r for r in self._records if self._end(r) > now]

        if not any(r["id"] not in self._closed for r in self._records):
            if self.idle_s and self._cooldown_started is None:
                self._cooldown_started = now
            if self._cooldown_started and (now - self._cooldown_started).total_seconds() < self.idle_s:
                first = int(params.get("firstResult", 0))
                return [r for r in self._records if self._match(r, params)][first:]
            self._cooldown_started = None
            record = self._generate(now)
            self._records.append(record)
            logger.info(
                f"tasked CollectRequest {record['id']}: {record['origObjectId']} "
                f"(satNo {record.get('satNo')}), window {record['startTime']} → "
                f"{record['endTime']}"
            )

        matched = [r for r in self._records if self._match(r, params)]
        first = int(params.get("firstResult", 0))
        limit = int(params.get("maxResults", len(matched)))
        return matched[first : first + limit]

    @staticmethod
    def _end(record: dict[str, Any]) -> datetime:
        return datetime.fromisoformat(record["endTime"].replace("Z", "+00:00"))

    def _match(self, record: dict[str, Any], params: dict[str, str]) -> bool:
        for key in ("idSensor", "origSensorId"):
            if key in params and record.get(key) != params[key]:
                return False
        for key in ("startTime", "endTime"):
            if key in params and not self._match_time(record[key], params[key]):
                return False
        return True

    @staticmethod
    def _match_time(value: str, criterion: str) -> bool:
        """UDL query-operator comparison: '<ISO', '>ISO', or exact."""
        parse = lambda s: datetime.fromisoformat(s.replace("Z", "+00:00"))  # noqa: E731
        if criterion.startswith("<"):
            return parse(value) < parse(criterion[1:])
        if criterion.startswith(">"):
            return parse(value) > parse(criterion[1:])
        return parse(value) == parse(criterion)

    # ── Generation ──

    def _generate(self, now: datetime) -> dict[str, Any]:
        sat = self._pick_satellite(now)
        mode = random.choice(self.target_types)
        record: dict[str, Any] = {
            "id": str(uuid4()),
            "classificationMarking": CLASSIFICATION_MARKING,
            "dataMode": DATA_MODE,
            "source": SOURCE,
            "type": "DIRECTED SEARCH" if mode == "radec" else "OBJECT",
            "startTime": _udl_ts(now),
            "endTime": _udl_ts(now + WINDOW),
            "idSensor": self.id_sensor,
            "origSensorId": self.id_sensor,
            "obType": "EO",
            "priority": "ROUTINE",
            "satNo": sat.sat.model.satnum,
            "origObjectId": sat.name or str(sat.sat.model.satnum),
            "integrationTime": float(random.randint(*EXPOSURE_RANGE_S) * 1000),
            "numFrames": random.randint(*FRAME_RANGE),
            # Binning is not a UDL CollectRequest field; carried in notes.
            "notes": f"binning={random.randint(*BINNING_RANGE)}",
            "createdAt": _udl_ts(now),
            "createdBy": "mock-udl",
        }
        record.update(getattr(self, f"_target_{mode}")(sat, now))
        return record

    def _pick_satellite(self, now: datetime) -> CatalogSat:
        """Random satellite above MIN_ALTITUDE_DEG for the whole window."""
        t0 = self._ts.from_datetime(now)
        t1 = self._ts.from_datetime(now + WINDOW)
        best, best_alt = None, -90.0
        for sat in random.sample(self.catalog, min(PICK_ATTEMPTS, len(self.catalog))):
            alt0 = (sat.sat - self._observer).at(t0).altaz()[0].degrees
            if alt0 > best_alt:
                best, best_alt = sat, alt0
            if alt0 <= MIN_ALTITUDE_DEG:
                continue
            if (sat.sat - self._observer).at(t1).altaz()[0].degrees > MIN_ALTITUDE_DEG:
                return sat
        logger.warning(
            f"no sampled satellite stays above {MIN_ALTITUDE_DEG}° for the next "
            f"{WINDOW.total_seconds():.0f}s; using best available ({best_alt:.1f}°)"
        )
        return best

    # ── Target payloads ──

    def _target_tle(self, sat: CatalogSat, now: datetime) -> dict[str, Any]:
        # line1/line2 are readOnly (server-derived) fields in the UDL schema —
        # correct for served records, which is the role played here.
        return {
            "elset": {
                "classificationMarking": CLASSIFICATION_MARKING,
                "dataMode": DATA_MODE,
                "source": SOURCE,
                "epoch": _udl_ts(sat.sat.epoch.utc_datetime()),
                "satNo": sat.sat.model.satnum,
                "line1": sat.line1,
                "line2": sat.line2,
            }
        }

    def _target_sv(self, sat: CatalogSat, now: datetime) -> dict[str, Any]:
        geocentric = sat.sat.at(self._ts.from_datetime(now))
        pos = geocentric.position.km
        vel = geocentric.velocity.km_per_s
        return {
            "stateVector": {
                "classificationMarking": CLASSIFICATION_MARKING,
                "dataMode": DATA_MODE,
                "source": SOURCE,
                "epoch": _udl_ts(now),
                "referenceFrame": "J2000",
                "satNo": sat.sat.model.satnum,
                "xpos": pos[0],
                "ypos": pos[1],
                "zpos": pos[2],
                "xvel": vel[0],
                "yvel": vel[1],
                "zvel": vel[2],
            }
        }

    def _target_radec(self, sat: CatalogSat, now: datetime) -> dict[str, Any]:
        mid = self._ts.from_datetime(now + WINDOW / 2)
        ra, dec, _ = (sat.sat - self._observer).at(mid).radec()
        return {"ra": ra.hours * 15.0, "dec": dec.degrees}
