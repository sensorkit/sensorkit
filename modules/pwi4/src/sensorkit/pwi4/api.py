import asyncio
import math
from typing import Any, Iterable

import httpx

from sensorkit.pwi4.models import PWI4Status, PWI4StatusModel


def list_to_comma_separated_string(value_list: Iterable[Any]) -> str:
    return ",".join(str(x) for x in value_list)


class PWI4Connection:
    """Core HTTP connection to a PWI4 instance.

    Provides low-level request helpers and status parsing. High-level device
    APIs (mount, focuser, etc.) should compose this connection.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8220,
        request_timeout: float = 5.0,
    ) -> None:
        self.host = host
        self.port = port
        self.timeout_seconds = request_timeout
        self.base_url = f"http://{self.host}:{self.port}"
        self._client: httpx.AsyncClient | None = httpx.AsyncClient(timeout=self.timeout_seconds)

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def request(self, command: str, params: dict[str, Any] | None = None, postdata: Any | None = None) -> bytes:
        """Issue a request to PWI4 using the given endpoint + params.

        - command: e.g. "/mount/connect"
        - params: dict for query-string (GET) parameters
        - postdata: if not None, POST with `postdata` as the body
        Returns: raw response content (bytes)
        """
        if not self._client:
            raise RuntimeError("PWI4Connection client closed; create a new instance.")
        url = f"{self.base_url}{command}"
        if postdata is not None:
            resp = await self._client.post(url, params=params, data=postdata)
        else:
            resp = await self._client.get(url, params=params)
        resp.raise_for_status()
        return resp.content

    async def request_with_status(self, command: str, params: dict[str, Any] | None = None, postdata: Any | None = None) -> PWI4StatusModel:
        raw_bytes = await self.request(command, params=params, postdata=postdata)
        return self.parse_status(raw_bytes)

    def parse_status_flat(self, response_bytes: bytes) -> PWI4Status:
        """Convert text with key=value lines into a PWI4Status object."""
        text = response_bytes.decode("utf-8", errors="ignore")
        dct: dict[str, str] = {}
        for line in text.split("\n"):
            if not line:
                continue
            name, sep, value = line.partition("=")
            if not sep:
                continue
            dct[name.strip()] = value.strip()
        return PWI4Status(dct)

    def parse_status(self, response_bytes: bytes) -> PWI4StatusModel:
        """Convert key=value lines into a typed Pydantic model tree."""
        text = response_bytes.decode("utf-8", errors="ignore")
        flat: dict[str, Any] = {}
        for line in text.split("\n"):
            if not line:
                continue
            name, sep, value = line.partition("=")
            if not sep:
                continue
            flat[name.strip()] = value.strip()
        return PWI4StatusModel.from_flat(flat)

    async def status(self) -> PWI4StatusModel:
        return await self.request_with_status("/status")

    async def poll_until(self, condition, start_delay: float = 0.1, interval: float = 1.0) -> PWI4StatusModel:
        """Poll status until condition(status) returns True."""
        while True:
            await asyncio.sleep(start_delay)
            st = await self.status()
            if condition(st):
                return st
            await asyncio.sleep(interval)


class MirrorCoverAPI:
    def __init__(self, conn: PWI4Connection) -> None:
        self._c = conn

    async def connect(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/mirrorcover/connect")

    async def disconnect(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/mirrorcover/disconnect")

    async def open(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/mirrorcover/open")

    async def close(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/mirrorcover/close")

    async def stop(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/mirrorcover/stop")

    async def reset(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/mirrorcover/reset")

    async def status(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/status")

class MountAPI:
    def __init__(self, conn: PWI4Connection) -> None:
        self._c = conn

    async def status(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/status")

    # Basic mount commands
    async def connect(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/connect")

    async def disconnect(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/disconnect")

    async def enable(self, axis: int) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/enable", params=dict(axis=axis))

    async def disable(self, axis: int) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/disable", params=dict(axis=axis))

    async def set_slew_time_constant(self, value: float) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/set_slew_time_constant", params=dict(value=value))

    async def set_axis0_wrap_range_min(self, degs: float) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/set_axis0_wrap_range_min", params=dict(degs=degs))

    async def find_home(self) -> PWI4StatusModel:
        await self._c.request_with_status("/mount/find_home")
        return await self._c.poll_until(lambda st: not st.mount.is_slewing)

    async def stop(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/stop")


    # Goto commands with polling
    async def goto_ra_dec_apparent(self, ra_hours: float, dec_degs: float) -> PWI4StatusModel:
        await self._c.request_with_status("/mount/goto_ra_dec_apparent", params=dict(ra_hours=ra_hours, dec_degs=dec_degs))

        def arrived(st: PWI4StatusModel) -> bool:
            return (
                not st.mount.is_slewing
                and st.mount.is_tracking
                and math.isclose(st.mount.ra_apparent_hours, ra_hours, abs_tol=1e-3)
                and math.isclose(st.mount.dec_apparent_degs, dec_degs, abs_tol=2e-2)
            )

        return await self._c.poll_until(arrived)

    async def goto_ra_dec_j2000(self, ra_hours: float, dec_degs: float) -> PWI4StatusModel:
        await self._c.request_with_status("/mount/goto_ra_dec_j2000", params=dict(ra_hours=ra_hours, dec_degs=dec_degs))

        def arrived(st: PWI4StatusModel) -> bool:
            return (
                not st.mount.is_slewing
                and st.mount.is_tracking
                and math.isclose(st.mount.ra_j2000_hours, ra_hours, abs_tol=1e-3)
                and math.isclose(st.mount.dec_j2000_degs, dec_degs, abs_tol=2e-2)
            )

        return await self._c.poll_until(arrived)

    async def goto_alt_az(self, alt_degs: float, az_degs: float, tolerance_deg: float = 0.2) -> PWI4StatusModel:
        await self._c.request_with_status("/mount/goto_alt_az", params=dict(alt_degs=alt_degs, az_degs=az_degs))

        def arrived(st: PWI4StatusModel) -> bool:
            return (
                not st.mount.is_slewing
                and math.isclose(st.mount.altitude_degs, alt_degs, abs_tol=tolerance_deg)
                and math.isclose(st.mount.azimuth_degs, az_degs, abs_tol=tolerance_deg)
            )

        return await self._c.poll_until(arrived)

    async def goto_raw(self, axis0_degs: float, axis1_degs: float, tolerance: float = 0.1) -> PWI4StatusModel:
        await self._c.request_with_status("/mount/goto_coord_pair", params=dict(c0=axis0_degs, c1=axis1_degs, type="raw"))

        def arrived(st: PWI4StatusModel) -> bool:
            dist0 = abs(st.mount.axis0.dist_to_target_arcsec)
            dist1 = abs(st.mount.axis1.dist_to_target_arcsec)
            return dist0 < tolerance and dist1 < tolerance and st.mount.is_tracking

        return await self._c.poll_until(arrived)
    async def goto_coord_pair(self, coord0: float, coord1: float, coord_type: str) -> PWI4StatusModel:
        await self._c.request_with_status("/mount/goto_coord_pair", params=dict(c0=coord0, c1=coord1, type=coord_type))
        return await self._c.poll_until(lambda st: not st.mount.is_slewing)

    # Offsets
    async def offset(self, **kwargs: Any) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/offset", params=kwargs)

    async def spiral_offset_new(self, x_step_arcsec: float, y_step_arcsec: float) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/spiral_offset/new", params=dict(x_step_arcsec=x_step_arcsec, y_step_arcsec=y_step_arcsec))

    async def spiral_offset_next(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/spiral_offset/next")

    async def spiral_offset_previous(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/spiral_offset/previous")

    # Parking / Tracking / TLE
    async def park(self) -> PWI4StatusModel:
        await self._c.request_with_status("/mount/park")
        return await self._c.poll_until(lambda st: not st.mount.is_slewing)

    async def set_park_here(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/set_park_here")

    async def tracking_on(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/tracking_on")

    async def tracking_off(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/tracking_off")

    async def follow_tle(self, line1: str, line2: str, line3: str, tolerance_deg: float = 1) -> PWI4StatusModel:
        await self._c.request_with_status("/mount/follow_tle", params=dict(line1=str(line1), line2=str(line2), line3=str(line3)))

        def is_now_tracking(st: PWI4StatusModel) -> bool:
            dist0 = abs(st.mount.axis0.dist_to_target_arcsec)
            dist1 = abs(st.mount.axis1.dist_to_target_arcsec)
            return dist0 < tolerance_deg and dist1 < tolerance_deg and st.mount.is_tracking

        return await self._c.poll_until(is_now_tracking)

    # RaDecPath / Custom Path
    async def radecpath_new(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/radecpath/new")

    async def radecpath_add_point(self, jd: float, ra_j2000_hours: float, dec_j2000_degs: float) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/radecpath/add_point", params=dict(jd=jd, ra_j2000_hours=ra_j2000_hours, dec_j2000_degs=dec_j2000_degs))

    async def radecpath_apply(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/radecpath/apply")

    async def custom_path_new(self, coord_type: str) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/custom_path/new", params=dict(type=coord_type))

    async def custom_path_add_point_list(self, points: list[tuple[float, float, float]]) -> bytes:
        lines: list[str] = []
        for jd, c0, c1 in points:
            lines.append(f"{jd:.10f},{c0},{c1}")
        data_str = "\n".join(lines)
        return await self._c.request("/mount/custom_path/add_point_list", postdata={"data": data_str})

    async def custom_path_apply(self, update_wrap: bool | None = None) -> PWI4StatusModel:
        params: dict[str, Any] = {}
        if update_wrap is not None:
            params["update_wrap"] = int(update_wrap)
        await self._c.request_with_status("/mount/custom_path/apply", params=params)
        return await self._c.poll_until(lambda st: not st.mount.is_slewing)

    # Mount model
    async def model_add_point(self, ra_j2000_hours: float, dec_j2000_degs: float) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/model/add_point", params=dict(ra_j2000_hours=ra_j2000_hours, dec_j2000_degs=dec_j2000_degs))

    async def model_delete_point(self, *point_indexes_0_based: int) -> PWI4StatusModel:
        idxs = list_to_comma_separated_string(point_indexes_0_based)
        return await self._c.request_with_status("/mount/model/delete_point", params=dict(index=idxs))

    async def model_add_artificial_offset_point(self, offset_degs: float) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/model/add_artificial_offset_point", params=dict(offset_degs=offset_degs))

    async def model_delete_artificial_points(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/model/delete_artificial_points")

    async def model_enable_point(self, *point_indexes_0_based: int) -> PWI4StatusModel:
        idxs = list_to_comma_separated_string(point_indexes_0_based)
        return await self._c.request_with_status("/mount/model/enable_point", params=dict(index=idxs))

    async def model_disable_point(self, *point_indexes_0_based: int) -> PWI4StatusModel:
        idxs = list_to_comma_separated_string(point_indexes_0_based)
        return await self._c.request_with_status("/mount/model/disable_point", params=dict(index=idxs))

    async def model_clear_points(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/model/clear_points")

    async def model_save_as_default(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/model/save_as_default")

    async def model_save(self, filename: str) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/model/save", params=dict(filename=filename))

    async def model_load(self, filename: str) -> PWI4StatusModel:
        return await self._c.request_with_status("/mount/model/load", params=dict(filename=filename))


class FocuserAPI:
    def __init__(self, conn: PWI4Connection) -> None:
        self._c = conn

    async def status(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/status")

    async def connect(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/focuser/connect")

    async def disconnect(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/focuser/disconnect")

    async def enable(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/focuser/enable")

    async def disable(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/focuser/disable")

    async def goto(self, target: int) -> PWI4StatusModel:
        return await self._c.request_with_status("/focuser/goto", params=dict(target=target))

    async def stop(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/focuser/stop")


class RotatorAPI:
    def __init__(self, conn: PWI4Connection) -> None:
        self._c = conn

    async def status(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/status")

    async def connect(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/rotator/connect")

    async def disconnect(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/rotator/disconnect")

    async def enable(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/rotator/enable")

    async def disable(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/rotator/disable")

    async def goto_mech(self, target_degs: float) -> PWI4StatusModel:
        return await self._c.request_with_status("/rotator/goto_mech", params=dict(degs=target_degs))

    async def goto_field(self, target_degs: float) -> PWI4StatusModel:
        return await self._c.request_with_status("/rotator/goto_field", params=dict(degs=target_degs))

    async def offset(self, offset_degs: float) -> PWI4StatusModel:
        return await self._c.request_with_status("/rotator/offset", params=dict(degs=offset_degs))

    async def stop(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/rotator/stop")


class FansHeatersAPI:
    def __init__(self, conn: PWI4Connection) -> None:
        self._c = conn

    async def status(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/status")

    async def fans_on(self, roles: list[str] | None = None) -> PWI4StatusModel:
        roles_param: str | None
        if isinstance(roles, (list, tuple)):
            roles_param = list_to_comma_separated_string(roles)
        else:
            roles_param = None
        return await self._c.request_with_status("/fans/on", params=dict(roles=roles_param))

    async def fans_off(self, roles: list[str] | None = None) -> PWI4StatusModel:
        roles_param: str | None
        if isinstance(roles, (list, tuple)):
            roles_param = list_to_comma_separated_string(roles)
        else:
            roles_param = None
        return await self._c.request_with_status("/fans/off", params=dict(roles=roles_param))

    async def heaters_set(self, role: str, power: int) -> PWI4StatusModel:
        return await self._c.request_with_status("/heaters/set", params=dict(role=role, power=power))


class M3API:
    def __init__(self, conn: PWI4Connection) -> None:
        self._c = conn

    async def status(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/status")

    async def goto(self, target_port: int) -> PWI4StatusModel:
        return await self._c.request_with_status("/m3/goto", params=dict(port=target_port))

    async def stop(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/m3/stop")


class VirtualCameraAPI:
    def __init__(self, conn: PWI4Connection) -> None:
        self._c = conn

    async def status(self) -> PWI4StatusModel:
        return await self._c.request_with_status("/status")

    async def take_image(self) -> bytes:
        return await self._c.request("/virtualcamera/take_image")

    async def take_image_and_save(self, filename: str) -> None:
        contents = await self.take_image()
        with open(filename, "wb") as f:
            f.write(contents)


class PWI4:
    """High-level facade providing organized access to PWI4 device APIs.

    Usage:
        conn = PWI4Connection(host="localhost", port=8220)
        api = PWI4(conn)
        await api.mount.connect()
        ...
        await conn.aclose()
    """

    def __init__(self, connection: PWI4Connection) -> None:
        self.conn = connection
        self.mirrorcover = MirrorCoverAPI(connection)
        self.mount = MountAPI(connection)
        self.focuser = FocuserAPI(connection)
        self.rotator = RotatorAPI(connection)
        self.fans_heaters = FansHeatersAPI(connection)
        self.m3 = M3API(connection)
        self.virtualcamera = VirtualCameraAPI(connection)
