from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field


# ----------------------------
# Helpers to read the flat map
# ----------------------------
def _get(d: Dict[str, Any], key: str, default: Any = None) -> Any:
    return d.get(key, default)

def _to_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in {"true", "1", "yes", "y", "on"}

def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)

def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return int(default)

def _to_str(v: Any, default: str = "") -> str:
    try:
        return str(v)
    except Exception:
        return str("")


# ----------------------------
# Small Pydantic models
# ----------------------------
class PWI4Info(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=False)
    version: str = "<unknown>"
    version_field: Tuple[int, int, int, int] = (0, 0, 0, 0)


class ResponseInfo(BaseModel):
    model_config = ConfigDict(extra="ignore")
    timestamp_utc: Optional[str] = None


class SiteStatus(BaseModel):
    model_config = ConfigDict(extra="ignore")
    latitude_degs: float = 0.0
    longitude_degs: float = 0.0
    height_meters: float = 0.0
    lmst_hours: float = 0.0


class AxisStatus(BaseModel):
    model_config = ConfigDict(extra="ignore")
    is_enabled: bool = False
    is_position_initialized: bool = False
    rms_error_arcsec: float = 0.0
    dist_to_target_arcsec: float = 0.0
    servo_error_arcsec: float = 0.0
    min_mech_position_degs: float = 0.0
    max_mech_position_degs: float = 0.0
    target_mech_position_degs: float = 0.0
    position_degs: float = 0.0
    position_timestamp_str: Optional[str] = None
    max_velocity_degs_per_sec: float = 0.0
    setpoint_velocity_degs_per_sec: float = 0.0
    measured_velocity_degs_per_sec: float = 0.0
    acceleration_degs_per_sec_sqr: float = 0.0
    measured_current_amps: float = 0.0


class MountModel(BaseModel):
    model_config = ConfigDict(extra="ignore")
    filename: Optional[str] = None
    num_points_total: int = 0
    num_points_enabled: int = 0
    rms_error_arcsec: float = 0.0


class ArcsecVector(BaseModel):
    model_config = ConfigDict(extra="ignore")
    total: float = 0.0
    rate: float = 0.0
    gradual_offset_progress: float = 0.0


class MountOffsets(BaseModel):
    model_config = ConfigDict(extra="ignore")
    ra_arcsec: ArcsecVector
    dec_arcsec: ArcsecVector
    axis0_arcsec: ArcsecVector
    axis1_arcsec: ArcsecVector
    path_arcsec: ArcsecVector
    transverse_arcsec: ArcsecVector


class SpiralOffset(BaseModel):
    model_config = ConfigDict(extra="ignore")
    x: int = 0
    y: int = 0
    x_step_arcsec: float = 0.0
    y_step_arcsec: float = 0.0


class MountStatus(BaseModel):
    model_config = ConfigDict(extra="ignore")
    is_connected: bool = False
    geometry: int = 0
    timestamp_utc: Optional[str] = None
    julian_date: float = 0.0
    slew_time_constant: float = 0.0

    ra_apparent_hours: float = 0.0
    dec_apparent_degs: float = 0.0
    ra_j2000_hours: float = 0.0
    dec_j2000_degs: float = 0.0
    target_ra_apparent_hours: float = 0.0
    target_dec_apparent_degs: float = 0.0

    azimuth_degs: float = 0.0
    altitude_degs: float = 0.0
    is_slewing: bool = False
    is_tracking: bool = False

    field_angle_here_degs: float = 0.0
    field_angle_at_target_degs: float = 0.0
    field_angle_rate_at_target_degs_per_sec: float = 0.0
    path_angle_at_target_degs: float = 0.0
    path_angle_rate_at_target_degs_per_sec: float = 0.0
    distance_to_sun_degs: float = 0.0

    axis0_wrap_range_min_degs: float = 0.0

    axis0: AxisStatus = Field(default_factory=AxisStatus)
    axis1: AxisStatus = Field(default_factory=AxisStatus)

    model: MountModel = Field(default_factory=MountModel)
    offsets: Optional[MountOffsets] = None
    spiral_offset: Optional[SpiralOffset] = None


class FocuserStatus(BaseModel):
    model_config = ConfigDict(extra="ignore")
    exists: bool = False
    is_connected: bool = False
    is_enabled: bool = False
    position: float = 0.0
    is_moving: bool = False


class RotatorStatus(BaseModel):
    model_config = ConfigDict(extra="ignore")
    exists: bool = False
    is_connected: bool = False
    is_enabled: bool = False
    mech_position_degs: float = 0.0
    field_angle_degs: float = 0.0
    is_moving: bool = False
    is_slewing: bool = False


class M3Status(BaseModel):
    model_config = ConfigDict(extra="ignore")
    exists: bool = False
    port: int = 0


class AutofocusStatus(BaseModel):
    model_config = ConfigDict(extra="ignore")
    is_running: bool = False
    success: bool = False
    best_position: float = 0.0
    tolerance: float = 0.0


class MirrorCoverStatus(BaseModel):
    model_config = ConfigDict(extra="ignore")
    is_connected: bool = False
    overall_state: int = 0
    overall_state_name: str = "Open"

class PWI4StatusModel(BaseModel):
    """
    Root Pydantic model for PWI4 status.
    """
    model_config = ConfigDict(extra="ignore")
    raw: Dict[str, Any] = Field(default_factory=dict)

    pwi4: PWI4Info
    response: ResponseInfo
    site: SiteStatus
    mount: MountStatus
    focuser: FocuserStatus
    rotator: RotatorStatus
    m3: M3Status
    autofocus: AutofocusStatus
    mirrorcover: MirrorCoverStatus

    # ----------------------------
    # Factory from flat key map
    # ----------------------------
    @classmethod
    def from_flat(cls, flat: Dict[str, Any]) -> "PWI4StatusModel":
        # PWI4
        pwi4 = PWI4Info(
            version=str(_get(flat, "pwi4.version", "<unknown>")),
            version_field=(
                _to_int(_get(flat, "pwi4.version_field[0]", 0)),
                _to_int(_get(flat, "pwi4.version_field[1]", 0)),
                _to_int(_get(flat, "pwi4.version_field[2]", 0)),
                _to_int(_get(flat, "pwi4.version_field[3]", 0)),
            ),
        )

        # Response
        response = ResponseInfo(timestamp_utc=_get(flat, "response.timestamp_utc"))

        # Site
        site = SiteStatus(
            latitude_degs=_to_float(_get(flat, "site.latitude_degs")),
            longitude_degs=_to_float(_get(flat, "site.longitude_degs")),
            height_meters=_to_float(_get(flat, "site.height_meters")),
            lmst_hours=_to_float(_get(flat, "site.lmst_hours")),
        )

        # Axes
        def parse_axis(idx: int) -> AxisStatus:
            p = f"mount.axis{idx}."
            return AxisStatus(
                is_enabled=_to_bool(_get(flat, p + "is_enabled")),
                is_position_initialized=_to_bool(_get(flat, p + "is_position_initialized")),
                rms_error_arcsec=_to_float(_get(flat, p + "rms_error_arcsec")),
                dist_to_target_arcsec=_to_float(_get(flat, p + "dist_to_target_arcsec")),
                servo_error_arcsec=_to_float(_get(flat, p + "servo_error_arcsec")),
                min_mech_position_degs=_to_float(_get(flat, p + "min_mech_position_degs")),
                max_mech_position_degs=_to_float(_get(flat, p + "max_mech_position_degs")),
                target_mech_position_degs=_to_float(_get(flat, p + "target_mech_position_degs")),
                position_degs=_to_float(_get(flat, p + "position_degs")),
                position_timestamp_str=_get(flat, p + "position_timestamp"),
                max_velocity_degs_per_sec=_to_float(_get(flat, p + "max_velocity_degs_per_sec")),
                setpoint_velocity_degs_per_sec=_to_float(_get(flat, p + "setpoint_velocity_degs_per_sec")),
                measured_velocity_degs_per_sec=_to_float(_get(flat, p + "measured_velocity_degs_per_sec")),
                acceleration_degs_per_sec_sqr=_to_float(_get(flat, p + "acceleration_degs_per_sec_sqr")),
                measured_current_amps=_to_float(_get(flat, p + "measured_current_amps")),
            )

        axis0 = parse_axis(0)
        axis1 = parse_axis(1)

        # Offsets (optional)
        def has_offsets() -> bool:
            return "mount.offsets.ra_arcsec.total" in flat

        offsets: Optional[MountOffsets] = None
        if has_offsets():
            def vec(prefix: str) -> ArcsecVector:
                return ArcsecVector(
                    total=_to_float(_get(flat, f"{prefix}.total")),
                    rate=_to_float(_get(flat, f"{prefix}.rate")),
                    gradual_offset_progress=_to_float(_get(flat, f"{prefix}.gradual_offset_progress")),
                )

            offsets = MountOffsets(
                ra_arcsec=vec("mount.offsets.ra_arcsec"),
                dec_arcsec=vec("mount.offsets.dec_arcsec"),
                axis0_arcsec=vec("mount.offsets.axis0_arcsec"),
                axis1_arcsec=vec("mount.offsets.axis1_arcsec"),
                path_arcsec=vec("mount.offsets.path_arcsec"),
                transverse_arcsec=vec("mount.offsets.transverse_arcsec"),
            )

        # Spiral offset (optional)
        spiral_offset: Optional[SpiralOffset] = None
        if "mount.spiral_offset.x" in flat:
            spiral_offset = SpiralOffset(
                x=_to_int(_get(flat, "mount.spiral_offset.x")),
                y=_to_int(_get(flat, "mount.spiral_offset.y")),
                x_step_arcsec=_to_float(_get(flat, "mount.spiral_offset.x_step_arcsec")),
                y_step_arcsec=_to_float(_get(flat, "mount.spiral_offset.y_step_arcsec")),
            )

        # Mount model
        mount_model = MountModel(
            filename=_get(flat, "mount.model.filename"),
            num_points_total=_to_int(_get(flat, "mount.model.num_points_total")),
            num_points_enabled=_to_int(_get(flat, "mount.model.num_points_enabled")),
            rms_error_arcsec=_to_float(_get(flat, "mount.model.rms_error_arcsec")),
        )

        # Mount
        mount = MountStatus(
            is_connected=_to_bool(_get(flat, "mount.is_connected")),
            geometry=_to_int(_get(flat, "mount.geometry")),
            timestamp_utc=_get(flat, "mount.timestamp_utc"),
            julian_date=_to_float(_get(flat, "mount.julian_date")),
            slew_time_constant=_to_float(_get(flat, "mount.slew_time_constant")),
            ra_apparent_hours=_to_float(_get(flat, "mount.ra_apparent_hours")),
            dec_apparent_degs=_to_float(_get(flat, "mount.dec_apparent_degs")),
            ra_j2000_hours=_to_float(_get(flat, "mount.ra_j2000_hours")),
            dec_j2000_degs=_to_float(_get(flat, "mount.dec_j2000_degs")),
            target_ra_apparent_hours=_to_float(_get(flat, "mount.target_ra_apparent_hours")),
            target_dec_apparent_degs=_to_float(_get(flat, "mount.target_dec_apparent_degs")),
            azimuth_degs=_to_float(_get(flat, "mount.azimuth_degs")),
            altitude_degs=_to_float(_get(flat, "mount.altitude_degs")),
            is_slewing=_to_bool(_get(flat, "mount.is_slewing")),
            is_tracking=_to_bool(_get(flat, "mount.is_tracking")),
            field_angle_here_degs=_to_float(_get(flat, "mount.field_angle_here_degs")),
            field_angle_at_target_degs=_to_float(_get(flat, "mount.field_angle_at_target_degs")),
            field_angle_rate_at_target_degs_per_sec=_to_float(_get(flat, "mount.field_angle_rate_at_target_degs_per_sec")),
            path_angle_at_target_degs=_to_float(_get(flat, "mount.path_angle_at_target_degs")),
            path_angle_rate_at_target_degs_per_sec=_to_float(_get(flat, "mount.path_angle_rate_at_target_degs_per_sec")),
            distance_to_sun_degs=_to_float(_get(flat, "mount.distance_to_sun_degs")),
            axis0_wrap_range_min_degs=_to_float(_get(flat, "mount.axis0_wrap_range_min_degs")),
            axis0=axis0,
            axis1=axis1,
            model=mount_model,
            offsets=offsets,
            spiral_offset=spiral_offset,
        )

        # Focuser
        focuser = FocuserStatus(
            exists=_to_bool(_get(flat, "focuser.exists")),
            is_connected=_to_bool(_get(flat, "focuser.is_connected")),
            is_enabled=_to_bool(_get(flat, "focuser.is_enabled")),
            position=_to_float(_get(flat, "focuser.position")),
            is_moving=_to_bool(_get(flat, "focuser.is_moving")),
        )

        # Rotator
        rotator = RotatorStatus(
            exists=_to_bool(_get(flat, "rotator.exists")),
            is_connected=_to_bool(_get(flat, "rotator.is_connected")),
            is_enabled=_to_bool(_get(flat, "rotator.is_enabled")),
            mech_position_degs=_to_float(_get(flat, "rotator.mech_position_degs")),
            field_angle_degs=_to_float(_get(flat, "rotator.field_angle_degs")),
            is_moving=_to_bool(_get(flat, "rotator.is_moving")),
            is_slewing=_to_bool(_get(flat, "rotator.is_slewing")),
        )

        # M3
        m3 = M3Status(
            exists=_to_bool(_get(flat, "m3.exists")),
            port=_to_int(_get(flat, "m3.port")),
        )

        # Autofocus
        autofocus = AutofocusStatus(
            is_running=_to_bool(_get(flat, "autofocus.is_running")),
            success=_to_bool(_get(flat, "autofocus.success")),
            best_position=_to_float(_get(flat, "autofocus.best_position")),
            tolerance=_to_float(_get(flat, "autofocus.tolerance")),
        )

        mirrorcover = MirrorCoverStatus(
            is_connected=_to_bool(_get(flat, "mirrorcover.is_connected")),
            overall_state=_to_int(_get(flat, "mirrorcover.overall_state")),
            overall_state_name=_to_str(_get(flat, "mirrorcover.overall_state_name"))
        )

        return cls(
            raw=dict(flat),  # keep original lines for debugging
            pwi4=pwi4,
            response=response,
            site=site,
            mount=mount,
            focuser=focuser,
            rotator=rotator,
            m3=m3,
            autofocus=autofocus,
            mirrorcover=mirrorcover
        )


class Section(object):
    """
    Simple object for collecting properties in PWI4Status.
    """

    pass


class PWI4Status:
    """
    Wraps the status response for many PWI4 commands in a class with named members.
    """

    def __init__(self, status_dict):
        self.raw = status_dict  # Allow direct access to raw entries as needed

        self.pwi4 = Section()
        self.pwi4.version = "<unknown>"
        self.pwi4.version_field = [0, 0, 0, 0]

        self.pwi4.version = self.raw.get("pwi4.version", "<unknown>")

        # pwi4.version_field[] was added in 4.0.9 beta 2
        def safe_int(key, default=0):
            return int(self.raw.get(key, default))

        self.pwi4.version_field[0] = safe_int("pwi4.version_field[0]")
        self.pwi4.version_field[1] = safe_int("pwi4.version_field[1]")
        self.pwi4.version_field[2] = safe_int("pwi4.version_field[2]")
        self.pwi4.version_field[3] = safe_int("pwi4.version_field[3]")

        def safe_float(key, default=0.0):
            return float(self.raw.get(key, default))

        def safe_bool(key, default=False):
            return self.raw.get(key, str(default)).lower() == "true"

        self.response = Section()
        self.response.timestamp_utc = self.raw.get("response.timestamp_utc")

        self.site = Section()
        self.site.latitude_degs = safe_float("site.latitude_degs")
        self.site.longitude_degs = safe_float("site.longitude_degs")
        self.site.height_meters = safe_float("site.height_meters")
        self.site.lmst_hours = safe_float("site.lmst_hours")

        self.mount = Section()
        self.mount.is_connected = safe_bool("mount.is_connected")
        self.mount.geometry = safe_int("mount.geometry")
        self.mount.timestamp_utc = self.raw.get("mount.timestamp_utc")
        self.mount.julian_date = safe_float("mount.julian_date")
        self.mount.slew_time_constant = safe_float("mount.slew_time_constant")
        self.mount.ra_apparent_hours = safe_float("mount.ra_apparent_hours")
        self.mount.dec_apparent_degs = safe_float("mount.dec_apparent_degs")
        self.mount.ra_j2000_hours = safe_float("mount.ra_j2000_hours")
        self.mount.dec_j2000_degs = safe_float("mount.dec_j2000_degs")
        self.mount.target_ra_apparent_hours = safe_float(
            "mount.target_ra_apparent_hours"
        )
        self.mount.target_dec_apparent_degs = safe_float(
            "mount.target_dec_apparent_degs"
        )
        self.mount.azimuth_degs = safe_float("mount.azimuth_degs")
        self.mount.altitude_degs = safe_float("mount.altitude_degs")
        self.mount.is_slewing = safe_bool("mount.is_slewing")
        self.mount.is_tracking = safe_bool("mount.is_tracking")
        self.mount.field_angle_here_degs = safe_float("mount.field_angle_here_degs")
        self.mount.field_angle_at_target_degs = safe_float(
            "mount.field_angle_at_target_degs"
        )
        self.mount.field_angle_rate_at_target_degs_per_sec = safe_float(
            "mount.field_angle_rate_at_target_degs_per_sec"
        )
        self.mount.path_angle_at_target_degs = safe_float(
            "mount.path_angle_at_target_degs"
        )
        self.mount.path_angle_rate_at_target_degs_per_sec = safe_float(
            "mount.path_angle_rate_at_target_degs_per_sec"
        )
        self.mount.distance_to_sun_degs = safe_float("mount.distance_to_sun_degs")
        self.mount.axis0_wrap_range_min_degs = safe_float(
            "mount.axis0_wrap_range_min_degs"
        )

        self.mount.axis0 = Section()
        self.mount.axis1 = Section()
        self.mount.axis = [self.mount.axis0, self.mount.axis1]

        for axis_index in range(2):
            axis = self.mount.axis[axis_index]
            prefix = f"mount.axis{axis_index}."
            axis.is_enabled = safe_bool(prefix + "is_enabled")
            axis.rms_error_arcsec = safe_float(prefix + "rms_error_arcsec")
            axis.dist_to_target_arcsec = safe_float(prefix + "dist_to_target_arcsec")
            axis.servo_error_arcsec = safe_float(prefix + "servo_error_arcsec")
            axis.min_mech_position_degs = safe_float(prefix + "min_mech_position_degs")
            axis.max_mech_position_degs = safe_float(prefix + "max_mech_position_degs")
            axis.target_mech_position_degs = safe_float(
                prefix + "target_mech_position_degs"
            )
            axis.position_degs = safe_float(prefix + "position_degs")
            axis.position_timestamp_str = self.raw.get(prefix + "position_timestamp")
            axis.max_velocity_degs_per_sec = safe_float(
                prefix + "max_velocity_degs_per_sec"
            )
            axis.setpoint_velocity_degs_per_sec = safe_float(
                prefix + "setpoint_velocity_degs_per_sec"
            )
            axis.measured_velocity_degs_per_sec = safe_float(
                prefix + "measured_velocity_degs_per_sec"
            )
            axis.acceleration_degs_per_sec_sqr = safe_float(
                prefix + "acceleration_degs_per_sec_sqr"
            )
            axis.measured_current_amps = safe_float(prefix + "measured_current_amps")

        self.mount.model = Section()
        self.mount.model.filename = self.raw.get("mount.model.filename")
        self.mount.model.num_points_total = safe_int("mount.model.num_points_total")
        self.mount.model.num_points_enabled = safe_int("mount.model.num_points_enabled")
        self.mount.model.rms_error_arcsec = safe_float("mount.model.rms_error_arcsec")

        if "mount.offsets.ra_arcsec.total" not in self.raw:
            self.mount.offsets = None
        else:
            self.mount.offsets = Section()

            self.mount.offsets.ra_arcsec = Section()
            self.mount.offsets.ra_arcsec.total = safe_float(
                "mount.offsets.ra_arcsec.total"
            )
            self.mount.offsets.ra_arcsec.rate = safe_float(
                "mount.offsets.ra_arcsec.rate"
            )
            self.mount.offsets.ra_arcsec.gradual_offset_progress = safe_float(
                "mount.offsets.ra_arcsec.gradual_offset_progress"
            )

            self.mount.offsets.dec_arcsec = Section()
            self.mount.offsets.dec_arcsec.total = safe_float(
                "mount.offsets.dec_arcsec.total"
            )
            self.mount.offsets.dec_arcsec.rate = safe_float(
                "mount.offsets.dec_arcsec.rate"
            )
            self.mount.offsets.dec_arcsec.gradual_offset_progress = safe_float(
                "mount.offsets.dec_arcsec.gradual_offset_progress"
            )

            self.mount.offsets.axis0_arcsec = Section()
            self.mount.offsets.axis0_arcsec.total = safe_float(
                "mount.offsets.axis0_arcsec.total"
            )
            self.mount.offsets.axis0_arcsec.rate = safe_float(
                "mount.offsets.axis0_arcsec.rate"
            )
            self.mount.offsets.axis0_arcsec.gradual_offset_progress = safe_float(
                "mount.offsets.axis0_arcsec.gradual_offset_progress"
            )

            self.mount.offsets.axis1_arcsec = Section()
            self.mount.offsets.axis1_arcsec.total = safe_float(
                "mount.offsets.axis1_arcsec.total"
            )
            self.mount.offsets.axis1_arcsec.rate = safe_float(
                "mount.offsets.axis1_arcsec.rate"
            )
            self.mount.offsets.axis1_arcsec.gradual_offset_progress = safe_float(
                "mount.offsets.axis1_arcsec.gradual_offset_progress"
            )

            self.mount.offsets.path_arcsec = Section()
            self.mount.offsets.path_arcsec.total = safe_float(
                "mount.offsets.path_arcsec.total"
            )
            self.mount.offsets.path_arcsec.rate = safe_float(
                "mount.offsets.path_arcsec.rate"
            )
            self.mount.offsets.path_arcsec.gradual_offset_progress = safe_float(
                "mount.offsets.path_arcsec.gradual_offset_progress"
            )

            self.mount.offsets.transverse_arcsec = Section()
            self.mount.offsets.transverse_arcsec.total = safe_float(
                "mount.offsets.transverse_arcsec.total"
            )
            self.mount.offsets.transverse_arcsec.rate = safe_float(
                "mount.offsets.transverse_arcsec.rate"
            )
            self.mount.offsets.transverse_arcsec.gradual_offset_progress = safe_float(
                "mount.offsets.transverse_arcsec.gradual_offset_progress"
            )

        if "mount.spiral_offset.x" not in self.raw:
            self.mount.spiral_offset = None
        else:
            self.mount.spiral_offset = Section()
            self.mount.spiral_offset.x = safe_int("mount.spiral_offset.x")
            self.mount.spiral_offset.y = safe_int("mount.spiral_offset.y")
            self.mount.spiral_offset.x_step_arcsec = safe_float(
                "mount.spiral_offset.x_step_arcsec"
            )
            self.mount.spiral_offset.y_step_arcsec = safe_float(
                "mount.spiral_offset.y_step_arcsec"
            )

        self.focuser = Section()
        self.focuser.exists = safe_bool("focuser.exists")
        self.focuser.is_connected = safe_bool("focuser.is_connected")
        self.focuser.is_enabled = safe_bool("focuser.is_enabled")
        self.focuser.position = safe_float("focuser.position")
        self.focuser.is_moving = safe_bool("focuser.is_moving")

        self.rotator = Section()
        self.rotator.exists = safe_bool("rotator.exists")
        self.rotator.is_connected = safe_bool("rotator.is_connected")
        self.rotator.is_enabled = safe_bool("rotator.is_enabled")
        self.rotator.mech_position_degs = safe_float("rotator.mech_position_degs")
        self.rotator.field_angle_degs = safe_float("rotator.field_angle_degs")
        self.rotator.is_moving = safe_bool("rotator.is_moving")
        self.rotator.is_slewing = safe_bool("rotator.is_slewing")

        self.m3 = Section()
        self.m3.exists = safe_bool("m3.exists")
        self.m3.port = safe_int("m3.port")

        self.autofocus = Section()
        self.autofocus.is_running = safe_bool("autofocus.is_running")
        self.autofocus.success = safe_bool("autofocus.success")
        self.autofocus.best_position = safe_float("autofocus.best_position")
        self.autofocus.tolerance = safe_float("autofocus.tolerance")

    def __repr__(self):
        """
        Format all of the keywords and values we have received.
        """
        keys = sorted(self.raw.keys())
        if not keys:
            return "<PWI4Status (empty)>"

        max_key_length = max(len(k) for k in keys)
        lines = []
        fmt = "%-" + str(max_key_length) + "s: %s"
        for key in keys:
            val = self.raw[key]
            lines.append(fmt % (key, val))
        return "\n".join(lines)

