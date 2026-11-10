# SPDX-License-Identifier: Apache-2.0
import uuid
from datetime import UTC, datetime

import astropy.units as u
import burr.models.observation as obs
from astropy.coordinates import AltAz, EarthLocation, SkyCoord
from astropy.time import Time
from burr.core.config import AppConfig
from burr.models.run import BurrRun
from burr.models.site import SiteMetadata
from burr.models.tracking import Rates, TrackingMode
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.astro.common import TLE, ReferenceFrame
from sensorkit.astro.coords import Equatorial, Horizontal
from sensorkit.astro.target import AltAzTarget, ICRSTarget, RateTarget, Target, TLETarget
from sensorkit.burr.models import CollectConfig
from sensorkit.data.filesys import FileNameTemplate
from sensorkit.std import CameraParameterSet, FrameType, StandardCollectTask

# Produces e.g. `20260424T031530_photometric_standards_110-364_f0.fits`.
_FILE_NAME_TEMPLATE = (
    'f"{datetime.now(UTC):%Y%m%dT%H%M%S}_{BurrContext.mode}_{BurrContext.targ}_f{Collect.frame_number}.fits"'
)

_SOURCE_FRAME_TYPES = {"twilight_flats": FrameType.FLAT}


@sk.declare_keyword
class BurrContext(BaseModel):
    """Burr sequence identity attached to collect execution context.

    FITS expressions and filename templates access these values through
    `BurrContext.<field>`.

    Attributes:
        seq: Identifier shared by tasks and frames from one requested exposure.
        targ: Filename-safe target name.
        mode: Task source, such as coverage, calsats, or twilight_flats.
    """

    seq: str
    targ: str
    mode: str


def build_sk_tasks(
    run: BurrRun,
    config: AppConfig,
    request: obs.ObservationRequest,
    collect: CollectConfig | None = None,
):
    """Translate a Burr observation request into standard collect tasks.

    Each exposure produces one task for uniform tracking or a rate-then-sidereal
    sequence. A sidereal-then-rate sequence produces two tasks. Other interleavings
    raise `ValueError`.

    Rate-then-sidereal tasks list their sidereal frame indices in `sidereal_frames`.
    Sidereal AltAz and lunar pointings are converted to ICRS at task creation.
    Frames with tracking off retain a fixed AltAz target.
    """
    end_time = run.lighting_schedule.schedule_end_utc
    modes = [spec.tracking_mode for spec in request.exposure_map]
    frame_count = len(modes)
    # Per-request identity (target name + source); seq is added per exposure.
    targ, mode = _burr_identity(request)

    # Target for SIDEREAL/OFF frames. When any frame tracks sidereally we
    # freeze an AltAz/Lunar pointing to the ICRS coords it maps to *now*
    # (emit time): sensorkit tracks an AltAzTarget as FIXED, so coverage and
    # lunar (which map in alt/az but want sidereal tracking) would otherwise
    # hold the horizon coordinate and let the sky drift. Flats use OFF-only,
    # so they stay an AltAz target via to_sk_target and keep fixed tracking.
    if any(m == TrackingMode.SIDEREAL for m in modes):
        sidereal_target = _to_sk_sidereal_target(request.target, site=config.site)
    else:
        sidereal_target = _to_sk_target(request.target)

    # A TLE target carries its own motion — PWI4's follow_tle handles the
    # rate. For non-TLE targets, request.rates must supply the rate vector.
    is_tle_target = isinstance(request.target, obs.TLETarget)
    any_rate = any(m == TrackingMode.RATE for m in modes)
    if any_rate and not is_tle_target and request.rates is None:
        raise ValueError(
            f"{request.source_name}: exposure_map contains RATE frames, "
            f"target is not a TLETarget, and request.rates is None"
        )

    collect = collect or CollectConfig()
    cam_extra = dict(
        instrument=collect.instrument,
        frame_type=_SOURCE_FRAME_TYPES.get(request.source_name, FrameType.LIGHT),
        filter_name=collect.filter_name,
        readout_mode=collect.readout_mode,
        gain=collect.gain,
        binning_x=collect.binning,
        binning_y=collect.binning,
    )

    for exp_seconds in request.exposure_seconds:
        # A fresh BurrContext.seq per exposure: every SK task, and thus every
        # frame, fanned out from THIS exposure shares it — e.g. the ICRS
        # sidereal frame and the RateTarget rate frames of one sidereal-then-
        # rate collect carry the same seq — so downstream fitting regroups the
        # frames of one collect. Distinct exposure_seconds entries differ.
        # The FileNameTemplate resolves the identity via `BurrContext.<field>`
        # when sensorkit's filesys.WriteFile renders it.
        exposure_context = sk.KeywordDict(
            BurrContext(seq=str(uuid.uuid4()), targ=targ, mode=mode),
            FileNameTemplate(template=_FILE_NAME_TEMPLATE),
        )

        # Resolve rates for THIS exposure (non-TLE rate case only).
        # RatesFromStreakDistribution samples a fresh rate per call,
        # which is what we want for streak-training distribution.
        concrete_rates: Rates | None = None
        if any_rate and not is_tle_target:
            if isinstance(request.rates, Rates):
                concrete_rates = request.rates
            else:
                concrete_rates = request.rates.to_concrete_rates(exp_seconds)

        cam = CameraParameterSet(
            integration_time_seconds=exp_seconds,
            frame_count=frame_count,
            **cam_extra,
        )
        tasks: list[StandardCollectTask] = []

        # SIDEREAL and OFF both route through the non-rate target path:
        # the PWI4 generic controller points to ICRSTarget / AltAzTarget
        # without applying a custom rate. Twilight flats use OFF, most
        # other sources use SIDEREAL; from our perspective they're the
        # same "no custom rate" case for SK task emission.
        if all(m != TrackingMode.RATE for m in modes):
            tasks.append(
                StandardCollectTask(
                    target=sidereal_target,
                    camera_params=cam,
                    end_time=end_time,
                )
            )
        elif all(m == TrackingMode.RATE for m in modes):
            tasks.append(
                StandardCollectTask(
                    target=_resolve_rate_target(
                        is_tle_target, sidereal_target, request.target, concrete_rates, config
                    ),
                    camera_params=cam,
                    end_time=end_time,
                )
            )
        elif _is_rates_then_nonrate(modes):
            tasks.append(
                StandardCollectTask(
                    target=_resolve_rate_target(
                        is_tle_target, sidereal_target, request.target, concrete_rates, config
                    ),
                    camera_params=cam,
                    end_time=end_time,
                    sidereal_frames=[i for i, m in enumerate(modes) if m != TrackingMode.RATE],
                )
            )
        elif _is_nonrate_then_rates(modes):
            first_rate = modes.index(TrackingMode.RATE)
            tasks.append(
                StandardCollectTask(
                    target=sidereal_target,
                    camera_params=CameraParameterSet(
                        integration_time_seconds=exp_seconds,
                        frame_count=first_rate,
                        **cam_extra,
                    ),
                    end_time=end_time,
                )
            )
            tasks.append(
                StandardCollectTask(
                    target=_resolve_rate_target(
                        is_tle_target, sidereal_target, request.target, concrete_rates, config
                    ),
                    camera_params=CameraParameterSet(
                        integration_time_seconds=exp_seconds,
                        frame_count=frame_count - first_rate,
                        **cam_extra,
                    ),
                    end_time=end_time,
                )
            )
        else:
            raise ValueError(
                f"{request.source_name}: unsupported exposure_map pattern "
                f"{[m.value for m in modes]} — only uniform or "
                f"rates-then-non-rate / non-rate-then-rates are implemented"
            )

        for task in tasks:
            yield task.submit(context=exposure_context, expiry_time=end_time)


def _resolve_rate_target(
    is_tle_target: bool,
    sidereal_target: Target,
    burr_target: obs.TargetUnion,
    rates: Rates | None,
    config: AppConfig,
) -> Target:
    """Target for RATE-mode frames.

    A TLE target follows the TLE itself (PWI4 follow_tle), so it reuses
    `sidereal_target` (already an SKTLETarget via `to_sk_target`). Otherwise
    build a `RateTarget` from the request's concrete rates. `to_sk_ratetarget`
    converts AltAz/Lunar to ICRS using site+now.
    """
    if is_tle_target:
        return sidereal_target
    frame = ReferenceFrame.CIRF if config.sk.rate_target_frame == "cirf" else ReferenceFrame.ICRF
    return _to_sk_ratetarget(burr_target, rates, site=config.site, frame=frame)


def _to_sk_target(burr_target: obs.Target) -> Target:
    """Convert a burr target to the corresponding sensorkit astro target."""
    match burr_target:
        case obs.RADecTarget():
            return ICRSTarget(
                coords=Equatorial(
                    ra=burr_target.right_ascension_deg,
                    dec=burr_target.declination_deg,
                )
            )
        # LunarTarget extends AltAzTarget - match before AltAzTarget.
        case obs.LunarTarget():
            return AltAzTarget(
                coords=Horizontal(
                    az=burr_target.azimuth_deg,
                    alt=burr_target.altitude_deg,
                )
            )
        case obs.AltAzTarget():
            return AltAzTarget(
                coords=Horizontal(
                    az=burr_target.azimuth_deg,
                    alt=burr_target.altitude_deg,
                )
            )
        case obs.TLETarget():
            # Supply a display name from the NORAD catalog number when line0 is absent.
            norad_id = (
                burr_target.tle_line1[2:7].strip()
                if burr_target.tle_line1 and len(burr_target.tle_line1) >= 7
                else "unknown"
            )
            return TLETarget(
                tle=TLE(
                    line0=f"NORAD {norad_id}",
                    line1=burr_target.tle_line1,
                    line2=burr_target.tle_line2,
                )
            )
        case _:
            raise ValueError(f"Unsupported burr target type: {type(burr_target).__name__}")


def _to_sk_sidereal_target(
    burr_target: obs.TargetUnion,
    initial_time: datetime | None = None,
    site: SiteMetadata | None = None,
) -> Target:
    """SK target for sidereal-tracked frames, freezing AltAz to ICRS.

    RADec and TLE targets pass straight through `to_sk_target`. AltAz/Lunar
    targets are *frozen* to ICRS at `initial_time` (default now) using
    `site`: sensorkit tracks an `AltAzTarget` as **fixed** (alt/az held
    constant), which is wrong for a sidereal collect — the mount would hold the
    horizon coordinate and let the sky drift through the field. Converting the
    alt/az pointing to the RA/Dec it maps to *at emit time* makes the mount lock
    onto that sky position and track it sidereally.

    Coverage and lunar both map in alt/az but want sidereal tracking, so they
    flow through here. Flats (tracking OFF) keep their alt/az target via
    `to_sk_target` and are never passed to this function.
    """
    match burr_target:
        case obs.LunarTarget() | obs.AltAzTarget():
            if site is None:
                raise ValueError(
                    "site is required to freeze an AltAz/Lunar target to ICRS "
                    "for sidereal tracking"
                )
            if initial_time is None:
                initial_time = datetime.now(UTC)
            return ICRSTarget(
                coords=_altaz_to_equatorial(
                    az_deg=burr_target.azimuth_deg,
                    alt_deg=burr_target.altitude_deg,
                    time=initial_time,
                    site=site,
                )
            )
        case _:
            # RADec → ICRSTarget, TLE → SKTLETarget: already sidereal-correct.
            return _to_sk_target(burr_target)


def _to_sk_ratetarget(
    burr_target: obs.TargetUnion,
    rates: Rates,
    initial_time: datetime | None = None,
    site: SiteMetadata | None = None,
    frame: ReferenceFrame = ReferenceFrame.ICRF,
) -> RateTarget:
    """Build a rate target from a Burr position and angular rates.

    Args:
        burr_target: Initial position. AltAz and lunar targets require a site
            and are converted to equatorial coordinates.
        rates: Right ascension and declination rates in arcseconds per second.
        initial_time: UTC epoch of the initial coordinates; defaults to now.
        site: Observer location for AltAz and lunar targets.
        frame: Rate reference frame accepted by the destination mount.

    Returns:
        A rate target with angular values in degrees and degrees per second.

    Raises:
        ValueError: The input is a TLE target or requires a missing site.
    """
    if initial_time is None:
        initial_time = datetime.now(UTC)

    rates_coords = Equatorial(
        ra=rates.right_ascension_rate_arcsec_s / 3600.0,
        dec=rates.declination_rate_arcsec_s / 3600.0,
    )

    match burr_target:
        case obs.RADecTarget():
            initial_coords = Equatorial(
                ra=burr_target.right_ascension_deg,
                dec=burr_target.declination_deg,
            )
        case obs.LunarTarget() | obs.AltAzTarget():
            if site is None:
                raise ValueError(
                    "site is required to convert AltAz/Lunar target to ICRS "
                    "for RateTarget construction"
                )
            initial_coords = _altaz_to_equatorial(
                az_deg=burr_target.azimuth_deg,
                alt_deg=burr_target.altitude_deg,
                time=initial_time,
                site=site,
            )
        case TLETarget():
            raise ValueError("cannot combine custom rates with a TLE target — use TLETarget alone")
        case _:
            raise ValueError(f"Unsupported burr target type: {type(burr_target).__name__}")

    return RateTarget(
        frame=frame,
        rates=rates_coords,
        initial_time=initial_time,
        initial_frame=frame,
        initial_coords=initial_coords,
    )


def _altaz_to_equatorial(
    az_deg: float,
    alt_deg: float,
    time: datetime,
    site: SiteMetadata,
) -> Equatorial:
    """Convert an (az, alt) position at a given time + site to ICRS RA/Dec."""
    location = EarthLocation(
        lat=site.latitude * u.deg,
        lon=site.longitude * u.deg,
        height=(site.altitude_km or 0) * u.km,
    )
    altaz_coord = SkyCoord(
        az=az_deg * u.deg,
        alt=alt_deg * u.deg,
        frame=AltAz(obstime=Time(time), location=location),
    )
    icrs_coord = altaz_coord.icrs
    return Equatorial(
        ra=float(icrs_coord.ra.deg),
        dec=float(icrs_coord.dec.deg),
    )


def _burr_identity(request: obs.ObservationRequest) -> tuple[str, str]:
    """Resolve `(targ, mode)` for a request's BurrContext keyword.

    `mode` is the task source name verbatim. `targ` is the best target
    name the source supplied, picked from its metadata and sanitized for
    filesystem safety (spaces / path separators → underscores) since it also
    lands in the FITS filename. Falls back to the request's `task_id` when a
    source supplies no name (e.g. flats, lunar).
    """
    metadata = request.metadata or {}
    targ = (
        metadata.get("target_name")
        or metadata.get("star_name")  # photometry
        or metadata.get("satellite_name")  # calsats
        or metadata.get("pixel_id")  # coverage
        or metadata.get("norad_id")
        or request.task_id
    )
    safe_targ = str(targ).replace(" ", "_").replace("/", "_").replace("\\", "_")
    return safe_targ, request.source_name


def _is_rates_then_nonrate(modes: list[TrackingMode]) -> bool:
    """True iff `modes` starts with >=1 RATE followed by >=1 non-RATE frames."""
    try:
        first_nonrate = next(i for i, m in enumerate(modes) if m != TrackingMode.RATE)
    except StopIteration:
        return False
    if first_nonrate == 0:
        return False
    return all(m != TrackingMode.RATE for m in modes[first_nonrate:])


def _is_nonrate_then_rates(modes: list[TrackingMode]) -> bool:
    """True iff `modes` starts with >=1 non-RATE followed by >=1 RATE frames."""
    try:
        first_rate = modes.index(TrackingMode.RATE)
    except ValueError:
        return False
    if first_rate == 0:
        return False
    return all(m != TrackingMode.RATE for m in modes[:first_rate]) and all(
        m == TrackingMode.RATE for m in modes[first_rate:]
    )
