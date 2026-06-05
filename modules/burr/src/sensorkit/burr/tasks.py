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
from sensorkit.std import CameraParameterSet, StandardCollectTask

# Produces e.g. ``20260424T031530_photometric_standards_110-364_f0.fits``.
_FILE_NAME_TEMPLATE = (
    'f"{datetime.now(UTC):%Y%m%dT%H%M%S}_{BurrContext.mode}_{BurrContext.targ}_f{frame_num}.fits"'
)


@sk.declare_keyword
class BurrContext(BaseModel):
    """Burr's identity keyword, attached to every SK collect task burr emits.

    Rides in ``StandardCollectTask.context`` as a *declared* keyword (rather
    than loose flat keys), so it round-trips on the wire as a typed object and
    is reachable from sensorkit FITS-header exprs and filename templates via
    ``context.eval`` as ``BurrContext.<field>`` (e.g. ``BurrContext.seq``).
    Being namespaced also stops these values from colliding with the generic
    flat keys the std controller injects (``target_id``, ``track_mode``, ...),
    which otherwise override anything burr puts at the top level.

    Fields
    ------
    seq
        Per-exposure collect id — a fresh value for each entry in the
        request's ``exposure_seconds``. Every SK task and every frame fanned
        out from one exposure (e.g. the sidereal reference frame plus its rate
        frames) shares one ``seq``; distinct exposures differ. This is the unit
        downstream fitting groups on ("I will fit this").
    targ
        Target name for this collect (filename-safe). The manager's best name
        for the thing observed — star name, satellite name, coverage pixel, etc.
    mode
        The task source that produced the collect: ``coverage``, ``calsats``,
        ``twilight_flats``, ``photometric_standards``, ``lunar_background``.
    """

    seq: str
    targ: str
    mode: str


def build_sk_tasks(
    controller_id: str,
    run: BurrRun,
    config: AppConfig,
    request: obs.ObservationRequest,
):
    """Translate an ObservationRequest into one or more StandardCollectTask instances.

    Burr's nested frame model (``exposure_seconds`` × ``exposure_map``) maps
    to one or more SK tasks per request:

    - **Uniform SIDEREAL**: one task per exposure, target=ICRSTarget,
      frame_count=len(exposure_map).
    - **Uniform RATE**: one task per exposure, target=RateTarget (concrete
      rates resolved for that exposure duration), frame_count=len(exposure_map).
    - **[RATE..., SIDEREAL...]** (rates first): one task per exposure,
      target=RateTarget, ``sidereal_track_from_frame`` set to the first
      sidereal frame index. Generic controller switches to ICRF at that frame.
    - **[SIDEREAL..., RATE...]** (sidereal first): two tasks per exposure —
      N sidereal frames at ICRSTarget, then M rate frames at RateTarget.
      The generic controller's ``sidereal_track_from_frame`` only switches
      TO sidereal, so we can't express this with one task.
    - Any other interleaving raises ``ValueError``.

    Targets feeding SIDEREAL frames are resolved through
    ``to_sk_sidereal_target``, which freezes an AltAz/Lunar pointing to ICRS at
    emit time (sensorkit tracks AltAzTarget as fixed). OFF-only requests (flats)
    keep their AltAz target.
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
    is_tle_target = isinstance(request.target, TLETarget)
    any_rate = any(m == TrackingMode.RATE for m in modes)
    if any_rate and not is_tle_target and request.rates is None:
        raise ValueError(
            f"{request.source_name}: exposure_map contains RATE frames, "
            f"target is not a TLETarget, and request.rates is None"
        )

    for exp_seconds in request.exposure_seconds:
        # A fresh BurrContext.seq per exposure: every SK task, and thus every
        # frame, fanned out from THIS exposure shares it — e.g. the ICRS
        # sidereal frame and the RateTarget rate frames of one sidereal-then-
        # rate collect carry the same seq — so downstream fitting regroups the
        # frames of one collect. Distinct exposure_seconds entries differ.
        # file_name stays a flat top-level key because sensorkit's
        # filesys.WriteFile reads ``context["file_name"]`` literally; its
        # template resolves the identity via ``BurrContext.<field>``.
        exposure_context = sk.KeywordDict(
            BurrContext(seq=str(uuid.uuid4()), targ=targ, mode=mode),
            file_name=_FILE_NAME_TEMPLATE,
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

        # Target for RATE-mode frames, built lazily — for all-OFF /
        # all-SIDEREAL requests (e.g. twilight flats) concrete_rates is
        # None and to_sk_ratetarget would crash, so we only construct
        # it on the branches that actually consume it.
        # - TLE target → follow the TLE (PWI4 follow_tle)
        # - otherwise → RateTarget built from request's concrete rates
        # (to_sk_ratetarget converts AltAz/Lunar to ICRS using site+now)
        def _rate_target():
            if is_tle_target:
                return sidereal_target  # already SKTLETarget via to_sk_target
            frame = (
                ReferenceFrame.CIRF
                if config.sk.rate_target_frame == "cirf"
                else ReferenceFrame.ICRF
            )
            return _to_sk_ratetarget(
                request.target,
                concrete_rates,
                site=config.site,
                frame=frame,
            )

        cam = CameraParameterSet(
            integration_time_seconds=exp_seconds,
            frame_count=frame_count,
        )

        # SIDEREAL and OFF both route through the non-rate target path:
        # the PWI4 generic controller points to ICRSTarget / AltAzTarget
        # without applying a custom rate. Twilight flats use OFF, most
        # other sources use SIDEREAL; from our perspective they're the
        # same "no custom rate" case for SK task emission.
        if all(m != TrackingMode.RATE for m in modes):
            yield StandardCollectTask(
                task_id=uuid.uuid4(),
                controller_id=controller_id,
                target=sidereal_target,
                camera_params=cam,
                end_time=end_time,
                context=exposure_context,
            )
        elif all(m == TrackingMode.RATE for m in modes):
            yield StandardCollectTask(
                task_id=uuid.uuid4(),
                controller_id=controller_id,
                target=_rate_target(),
                camera_params=cam,
                end_time=end_time,
                context=exposure_context,
            )
        elif _is_rates_then_nonrate(modes):
            first_nonrate = next(i for i, m in enumerate(modes) if m != TrackingMode.RATE)
            yield StandardCollectTask(
                task_id=uuid.uuid4(),
                controller_id=controller_id,
                target=_rate_target(),
                camera_params=cam,
                end_time=end_time,
                sidereal_track_from_frame=first_nonrate,
                context=exposure_context,
            )
        elif _is_nonrate_then_rates(modes):
            first_rate = modes.index(TrackingMode.RATE)
            yield StandardCollectTask(
                task_id=uuid.uuid4(),
                controller_id=controller_id,
                target=sidereal_target,
                camera_params=CameraParameterSet(
                    integration_time_seconds=exp_seconds,
                    frame_count=first_rate,
                ),
                end_time=end_time,
                context=exposure_context,
            )
            yield StandardCollectTask(
                task_id=uuid.uuid4(),
                controller_id=controller_id,
                target=_rate_target(),
                camera_params=CameraParameterSet(
                    integration_time_seconds=exp_seconds,
                    frame_count=frame_count - first_rate,
                ),
                end_time=end_time,
                context=exposure_context,
            )
        else:
            raise ValueError(
                f"{request.source_name}: unsupported exposure_map pattern "
                f"{[m.value for m in modes]} — only uniform or "
                f"rates-then-non-rate / non-rate-then-rates are implemented"
            )


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
            # Synthesize a line0 ("name line") from the NORAD catalog number
            # in line1[2:7]. SK's newer std controller (device-module-refactors
            # branch) reads target_id via ``line0.split()[1]``, which crashes
            # with AttributeError on line0=None. The old controller read
            # NORAD directly from line1[2:7], which would be more robust —
            # flagged upstream. For now, giving line0 a two-token string
            # (``NORAD <id>``) makes split()[1] yield the NORAD number.
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

    RADec and TLE targets pass straight through ``to_sk_target``. AltAz/Lunar
    targets are *frozen* to ICRS at ``initial_time`` (default now) using
    ``site``: sensorkit tracks an ``AltAzTarget`` as **fixed** (alt/az held
    constant), which is wrong for a sidereal collect — the mount would hold the
    horizon coordinate and let the sky drift through the field. Converting the
    alt/az pointing to the RA/Dec it maps to *at emit time* makes the mount lock
    onto that sky position and track it sidereally.

    Coverage and lunar both map in alt/az but want sidereal tracking, so they
    flow through here. Flats (tracking OFF) keep their alt/az target via
    ``to_sk_target`` and are never passed to this function.
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
    """Build a sensorkit ``RateTarget`` from a burr target + concrete rates.

    Initial position comes from ``burr_target``; rates are converted from
    arcsec/s (burr's unit, stored in ``Rates``) to deg/s (sensorkit's
    ``Coordinates`` unit convention). The frame label on the returned
    ``RateTarget`` is set by ``frame``: ICRF is what pwi4 accepts, CIRF
    is what node_platform accepts. Picking the wrong one sends the
    target through sensorkit's BaseTarget.adapt() → to_ephemeris_target()
    path, which is unimplemented for RateTarget. AltAz/Lunar inputs are
    converted using the provided ``site`` and ``initial_time``.

    Parameters
    ----------
    burr_target : TargetUnion
        The burr target whose *position* to use as the rate-follow
        origin. RADec is passed through directly; AltAz/Lunar are
        converted to ICRS using ``site`` + ``initial_time``.
    rates : Rates
        RA/Dec rate vector in arcsec/s.
    initial_time : datetime, optional
        UTC time at which ``initial_coords`` is valid. Defaults to
        ``datetime.now(UTC)``.
    site : SiteMetadata, optional
        Observer location. Required for AltAz/Lunar conversion; ignored
        for RADec inputs. When omitted for AltAz, raises ValueError.

    TLE targets raise: their motion is intrinsic to the orbit and
    should not be combined with custom rates.
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
    """Resolve ``(targ, mode)`` for a request's BurrContext keyword.

    ``mode`` is the task source name verbatim. ``targ`` is the best target
    name the source supplied, picked from its metadata and sanitized for
    filesystem safety (spaces / path separators → underscores) since it also
    lands in the FITS filename. Falls back to the request's ``task_id`` when a
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
    """True iff ``modes`` starts with >=1 RATE followed by >=1 non-RATE frames."""
    try:
        first_nonrate = next(i for i, m in enumerate(modes) if m != TrackingMode.RATE)
    except StopIteration:
        return False
    if first_nonrate == 0:
        return False
    return all(m != TrackingMode.RATE for m in modes[first_nonrate:])


def _is_nonrate_then_rates(modes: list[TrackingMode]) -> bool:
    """True iff ``modes`` starts with >=1 non-RATE followed by >=1 RATE frames."""
    try:
        first_rate = modes.index(TrackingMode.RATE)
    except ValueError:
        return False
    if first_rate == 0:
        return False
    return all(m != TrackingMode.RATE for m in modes[:first_rate]) and all(
        m == TrackingMode.RATE for m in modes[first_rate:]
    )
