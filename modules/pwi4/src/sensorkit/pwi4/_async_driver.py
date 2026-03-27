import asyncio
import math
from contextlib import suppress
from typing import Any

import httpx

from sensorkit.pwi4.models import PWI4Status, PWI4StatusModel


def list_to_comma_separated_string(value_list):
    """
    Convert list of values (e.g. [3, 1, 5]) into a comma-separated string (e.g. "3,1,5").
    """
    return ",".join(str(x) for x in value_list)

class AsyncPWI4:
    """
    Client to the PWI4 telescope control application, using httpx.AsyncClient for non-blocking I/O.
    Methods that move the mount (e.g. goto_alt_az) will poll until the mount
    is done slewing or tracking is confirmed before returning.

    Unlike a context manager approach, you simply create and later call `await aclose()` to clean up.
    Example usage:

        async def main():
            pwi4 = AsyncPWI4()
            try:
                status = await pwi4.status()
                print("Is connected?", status.mount.is_connected)
                await pwi4.mount_goto_alt_az(45.0, 180.0)
            finally:
                await pwi4.aclose()

        asyncio.run(main())
    """

    def __init__(
        self,
        host="localhost",
        port=8220,
        request_timeout=5.0,
        poll_interval=1.0,
        *,
        keep_wrap_centered: bool = False,
        wrap_interval: float = 1.0,
        wrap_deadband_deg: float = 0.5,
    ):
        """
        :param host: PWI4 host
        :param port: PWI4 port (default: 8220)
        :param request_timeout: per-request timeout (seconds)
        :param poll_interval: how often we poll PWI4 for status, in seconds
        """
        self.host = host
        self.port = port
        self.timeout_seconds = request_timeout
        self.poll_interval = poll_interval

        self.base_url = f"http://{self.host}:{self.port}"
        self._client = httpx.AsyncClient(timeout=self.timeout_seconds)

        self._wrap_enabled = keep_wrap_centered
        self._wrap_interval = wrap_interval
        self._wrap_deadband = wrap_deadband_deg
        self._wrap_task: asyncio.Task | None = None

        if self._wrap_enabled:
            self.start_wrap_autocenter()

    def start_wrap_autocenter(self):
        """Start the background task that keeps azimuth wrap centered."""
        if self._wrap_task is None or self._wrap_task.done():
            loop = asyncio.get_running_loop()
            self._wrap_task = loop.create_task(self._wrap_autocenter_loop())

    async def stop_wrap_autocenter(self):
        """Stop the background wrap-centering task."""
        if self._wrap_task and not self._wrap_task.done():
            self._wrap_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._wrap_task
        self._wrap_task = None

    async def aclose(self):
        await self.stop_wrap_autocenter()
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _autocenter_min_wrap_once(self):
        st = await self.status()
        # if st.mount.is_slewing:
        #     return

        pos = st.mount.axis[0].position_degs
        min_mech = st.mount.axis[0].min_mech_position_degs
        max_mech = st.mount.axis[0].max_mech_position_degs

        desired_min = pos - 180.0
        if desired_min < min_mech:
            desired_min = min_mech
        if desired_min > (max_mech - 360.0):
            desired_min = max_mech - 360.0

        # Current min wrap reported by PWI4 (if available)
        current_min = st.mount.axis0_wrap_range_min_degs

        # Only update if meaningfully different
        if abs(desired_min - current_min) >= self._wrap_deadband:
            await self.mount_set_axis0_wrap_range_min(desired_min)

    async def _wrap_autocenter_loop(self):
        """
        Runs forever until cancelled, periodically keeping the wrap centered.
        Errors are swallowed (with a short backoff) to keep the loop resilient.
        """
        try:
            while True:
                try:
                    await self._autocenter_min_wrap_once()
                except Exception:
                    pass
                await asyncio.sleep(self._wrap_interval)
        except asyncio.CancelledError:
            raise
    # ----------------------------------------------------
    # Low-level request + status parsing
    # ----------------------------------------------------
    async def request(self, command, params=None, postdata=None):
        """
        Issue a request to PWI using the given endpoint + params.

        - command: e.g. "/mount/connect"
        - params: dict for query-string (GET) parameters
        - postdata: if not None, we do a POST with `postdata` as the body
        Returns the raw response content (bytes).
        """
        if not self._client:
            raise RuntimeError(
                "AsyncPWI4 client is closed. Call 'AsyncPWI4()' again to create a new session."
            )

        url = f"{self.base_url}{command}"

        # GET or POST logic
        if postdata is not None:
            # POST
            resp = await self._client.post(url, params=params, data=postdata)
        else:
            # GET
            resp = await self._client.get(url, params=params)

        resp.raise_for_status()
        return resp.content

    async def request_with_status(self, command, params=None, postdata=None):
        """
        Same as request(), but parses the result into a PWI4Status object.
        """
        raw_bytes = await self.request(command, params=params, postdata=postdata)
        return self.parse_status(raw_bytes)

    def parse_status_flat(self, response_bytes):
        """
        Convert text with key=value lines into a PWI4Status object
        """
        text = response_bytes.decode("utf-8", errors="ignore")
        lines = text.split("\n")
        dct = {}
        for line in lines:
            fields = line.split("=", 1)
            if len(fields) == 2:
                name = fields[0].strip()
                value = fields[1].strip()
                dct[name] = value
        return PWI4Status(dct)

    def parse_status(self, response_bytes) -> PWI4StatusModel:
        """
        Convert key=value lines into a typed Pydantic model tree.
        """
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

    async def _poll_until(self, condition_func, start_delay=0.1):
        """
        Poll self.status() until `condition_func(PWI4Status)` returns True.
        """
        while True:
            await asyncio.sleep(start_delay)
            st = await self.status()
            if condition_func(st):
                return st
            await asyncio.sleep(self.poll_interval)

    # ----------------------------------------------------
    # Simple "status" method
    # ----------------------------------------------------
    async def status(self):
        """
        Return a PWI4Status object with current mount/focuser/rotator info.
        """
        return await self.request_with_status("/status")

    async def status_raw(self):
        status = await self.request_with_status("/status")
        return status.raw

    async def mirrorcover_connect(self):
        return await self.request_with_status("/mirrorcover/connect")

    async def mirrorcover_disconnect(self):
        return await self.request_with_status("/mirrorcover/disconnect")

    async def mirrorcover_open(self):
        return await self.request_with_status("/mirrorcover/open")

    async def mirrorcover_close(self):
        return await self.request_with_status("/mirrorcover/close")

    async def mirrorcover_stop(self):
        return await self.request_with_status("/mirrorcover/stop")

    async def mirrorcover_reset(self):
        return await self.request_with_status("/mirrorcover/reset")

    # ----------------------------------------------------
    # Basic mount commands
    # ----------------------------------------------------
    async def mount_connect(self):
        return await self.request_with_status("/mount/connect")

    async def mount_disconnect(self):
        return await self.request_with_status("/mount/disconnect")

    async def mount_enable(self, axisNum):
        return await self.request_with_status(
            "/mount/enable", params=dict(axis=axisNum)
        )

    async def mount_disable(self, axisNum):
        return await self.request_with_status(
            "/mount/disable", params=dict(axis=axisNum)
        )

    async def mount_set_slew_time_constant(self, value):
        return await self.request_with_status(
            "/mount/set_slew_time_constant", params=dict(value=value)
        )

    async def mount_set_axis0_wrap_range_min(self, axis0_wrap_min_degs):
        return await self.request_with_status(
            "/mount/set_axis0_wrap_range_min", params=dict(degs=axis0_wrap_min_degs)
        )

    async def mount_find_home(self):
        """
        Start homing the mount, then poll until it is no longer slewing.
        """
        await self.request_with_status("/mount/find_home")
        return await self._poll_until(lambda st: not st.mount.is_slewing)

    async def mount_stop(self):
        return await self.request_with_status("/mount/stop")

    # ----------------------------------------------------
    # GoTo (RA/Dec, Alt/Az) commands with polling
    # ----------------------------------------------------
    async def mount_goto_ra_dec_apparent(self, ra_hours, dec_degs):
        """
        Slew to the specified RA/Dec (Apparent), then poll until not slewing.
        """
        await self.request_with_status(
            "/mount/goto_ra_dec_apparent",
            params=dict(ra_hours=ra_hours, dec_degs=dec_degs),
        )

        def arrived(st: PWI4Status):
            return (
                not st.mount.is_slewing
                and st.mount.is_tracking
                and math.isclose(st.mount.ra_apparent_hours, ra_hours, abs_tol=1e-3)
                and math.isclose(st.mount.dec_apparent_degs, dec_degs, abs_tol=2e-2)
            )

        return await self._poll_until(arrived)

    async def mount_goto_ra_dec_j2000(self, ra_hours, dec_degs):
        """
        Slew to the specified RA/Dec (J2000), then poll until not slewing.
        """
        await self.request_with_status(
            "/mount/goto_ra_dec_j2000",
            params=dict(ra_hours=ra_hours, dec_degs=dec_degs),
        )

        def arrived(st: PWI4Status):
            return (
                not st.mount.is_slewing
                and st.mount.is_tracking
                and math.isclose(st.mount.ra_j2000_hours, ra_hours, abs_tol=1e-3)
                and math.isclose(st.mount.dec_j2000_degs, dec_degs, abs_tol=2e-2)
            )

        return await self._poll_until(arrived)

    async def mount_goto_alt_az(
        self, alt_degs, az_degs, tolerance_deg=0.2
    ):
        """
        Slew to specified Alt/Az in degrees, then wait until mount is no longer slewing
        and is within `tolerance_deg` of the target.
        """
        await self.request_with_status(
            "/mount/goto_alt_az", params=dict(alt_degs=alt_degs, az_degs=az_degs)
        )

        def arrived(st: PWI4Status):
            return (
                not st.mount.is_slewing
                and math.isclose(st.mount.altitude_degs, alt_degs, abs_tol=tolerance_deg)
                and math.isclose(st.mount.azimuth_degs, az_degs, abs_tol=tolerance_deg)
            )

        return await self._poll_until(arrived)

    async def mount_goto_coord_pair(self, coord0, coord1, coord_type):
        """
        If you want to poll for arrival, you can adapt this method similarly.
        Currently returns immediately.
        coord_type: "altaz" or "raw"
        """
        await self.request_with_status(
            "/mount/goto_coord_pair", params=dict(c0=coord0, c1=coord1, type=coord_type)
        )
        return await self._poll_until(lambda st: not st.mount.is_slewing)

    # ----------------------------------------------------
    # Offsets
    # ----------------------------------------------------
    async def mount_offset(self, **kwargs):
        return await self.request_with_status("/mount/offset", params=kwargs)

    async def mount_spiral_offset_new(self, x_step_arcsec, y_step_arcsec):
        return await self.request_with_status(
            "/mount/spiral_offset/new",
            params=dict(x_step_arcsec=x_step_arcsec, y_step_arcsec=y_step_arcsec),
        )

    async def mount_spiral_offset_next(self):
        return await self.request_with_status("/mount/spiral_offset/next")

    async def mount_spiral_offset_previous(self):
        return await self.request_with_status("/mount/spiral_offset/previous")

    # ----------------------------------------------------
    # Parking, Tracking, TLE
    # ----------------------------------------------------
    async def mount_park(self):
        """
        Park the mount, then wait until not slewing.
        """
        await self.request_with_status("/mount/park")
        return await self._poll_until(lambda st: not st.mount.is_slewing)

    async def mount_set_park_here(self):
        return await self.request_with_status("/mount/set_park_here")

    async def mount_tracking_on(self):
        return await self.request_with_status("/mount/tracking_on")

    async def mount_tracking_off(self):
        return await self.request_with_status("/mount/tracking_off")

    async def tracking_error_callback(self):
        """
        Poll self.status() until `condition_func(PWI4Status)` returns True.
        """
        while True:
            st = await self.status()
            dist0 = abs(st.mount.axis0.dist_to_target_arcsec)
            dist1 = abs(st.mount.axis1.dist_to_target_arcsec)
            if dist0 > 3.0 or dist1 > 3.0:
                raise RuntimeError("Tracking error")

    async def mount_follow_tle(
        self, tle_line_1, tle_line_2, tle_line_3, tolerance_deg=1
    ):
        """
        Issue follow TLE command, then wait until is_tracking=True.
        """
        await self.request_with_status(
            "/mount/follow_tle",
            params=dict(line1=str(tle_line_1), line2=str(tle_line_2), line3=str(tle_line_3)),
        )

        def is_now_tracking(st: PWI4Status):
            dist0 = abs(st.mount.axis0.dist_to_target_arcsec)
            dist1 = abs(st.mount.axis1.dist_to_target_arcsec)
            return (
                dist0 < tolerance_deg and dist1 < tolerance_deg and st.mount.is_tracking
            )

        return await self._poll_until(is_now_tracking)

    # ----------------------------------------------------
    # RaDecPath / custom path
    # ----------------------------------------------------
    async def mount_radecpath_new(self):
        return await self.request_with_status("/mount/radecpath/new")

    async def mount_radecpath_add_point(self, jd, ra_j2000_hours, dec_j2000_degs):
        return await self.request_with_status(
            "/mount/radecpath/add_point",
            params=dict(
                jd=jd, ra_j2000_hours=ra_j2000_hours, dec_j2000_degs=dec_j2000_degs
            ),
        )

    async def mount_radecpath_apply(self):
        await self.request_with_status("/mount/radecpath/apply")
        return await self._poll_until(lambda st: not st.mount.is_slewing, 1.0)

    async def mount_custom_path_new(self, coord_type):
        return await self.request_with_status(
            "/mount/custom_path/new", params=dict(type=coord_type)
        )

    async def mount_custom_path_add_point_list(self, points):
        """
        points: list of (jd, c0, c1)
        We'll POST them in the form data: "data=JD,coord0,coord1" lines
        """
        lines = []
        for jd, c0, c1 in points:
            line = f"{jd:.10f},{c0},{c1}"
            lines.append(line)
        data_str = "\n".join(lines)

        # postdata = f"data={data_str}".encode("utf-8")
        return await self.request(
            "/mount/custom_path/add_point_list", postdata={"data": data_str}
        )

    async def mount_custom_path_apply(self, update_wrap=None):
        params = {}
        if update_wrap is not None:
            params["update_wrap"] = int(update_wrap)
        await self.request_with_status("/mount/custom_path/apply", params=params)
        return await self._poll_until(lambda st: not st.mount.is_slewing, 1.0)

    # ----------------------------------------------------
    # Model points
    # ----------------------------------------------------
    async def mount_model_add_point(self, ra_j2000_hours, dec_j2000_degs):
        return await self.request_with_status(
            "/mount/model/add_point",
            params=dict(ra_j2000_hours=ra_j2000_hours, dec_j2000_degs=dec_j2000_degs),
        )

    async def mount_model_delete_point(self, *point_indexes_0_based):
        idxs = list_to_comma_separated_string(point_indexes_0_based)
        return await self.request_with_status(
            "/mount/model/delete_point", params=dict(index=idxs)
        )

    async def mount_model_add_artificial_offset_point(self, offset_degs):
        return await self.request_with_status(
            "/mount/model/add_artificial_offset_point",
            params=dict(offset_degs=offset_degs),
        )

    async def mount_model_delete_artificial_points(self):
        return await self.request_with_status("/mount/model/delete_artificial_points")

    async def mount_model_enable_point(self, *point_indexes_0_based):
        idxs = list_to_comma_separated_string(point_indexes_0_based)
        return await self.request_with_status(
            "/mount/model/enable_point", params=dict(index=idxs)
        )

    async def mount_model_disable_point(self, *point_indexes_0_based):
        idxs = list_to_comma_separated_string(point_indexes_0_based)
        return await self.request_with_status(
            "/mount/model/disable_point", params=dict(index=idxs)
        )

    async def mount_model_clear_points(self):
        return await self.request_with_status("/mount/model/clear_points")

    async def mount_model_save_as_default(self):
        return await self.request_with_status("/mount/model/save_as_default")

    async def mount_model_save(self, filename):
        return await self.request_with_status(
            "/mount/model/save", params=dict(filename=filename)
        )

    async def mount_model_load(self, filename):
        return await self.request_with_status(
            "/mount/model/load", params=dict(filename=filename)
        )

    # ----------------------------------------------------
    # Focuser
    # ----------------------------------------------------
    async def focuser_connect(self):
        return await self.request_with_status("/focuser/connect")

    async def focuser_disconnect(self):
        return await self.request_with_status("/focuser/disconnect")

    async def focuser_enable(self):
        return await self.request_with_status("/focuser/enable")

    async def focuser_disable(self):
        return await self.request_with_status("/focuser/disable")

    async def focuser_goto(self, target):
        return await self.request_with_status(
            "/focuser/goto", params=dict(target=target)
        )

    async def focuser_stop(self):
        return await self.request_with_status("/focuser/stop")

    # ----------------------------------------------------
    # Rotator
    # ----------------------------------------------------
    async def rotator_connect(self):
        return await self.request_with_status("/rotator/connect")

    async def rotator_disconnect(self):
        return await self.request_with_status("/rotator/disconnect")

    async def rotator_enable(self):
        return await self.request_with_status("/rotator/enable")

    async def rotator_disable(self):
        return await self.request_with_status("/rotator/disable")

    async def rotator_goto_mech(self, target_degs):
        return await self.request_with_status(
            "/rotator/goto_mech", params=dict(degs=target_degs)
        )

    async def rotator_goto_field(self, target_degs):
        return await self.request_with_status(
            "/rotator/goto_field", params=dict(degs=target_degs)
        )

    async def rotator_offset(self, offset_degs):
        return await self.request_with_status(
            "/rotator/offset", params=dict(degs=offset_degs)
        )

    async def rotator_stop(self):
        return await self.request_with_status("/rotator/stop")

    # ----------------------------------------------------
    # Fans and Heaters
    # ----------------------------------------------------
    async def fans_on(self, roles=None):
        """
        roles: if None, turns on all fans.
        Otherwise pass a list of fan roles. Example: ["m1", "m2", "cabinet", ...]
        """
        if isinstance(roles, (list, tuple)):
            roles = list_to_comma_separated_string(roles)
        return await self.request_with_status("/fans/on", params=dict(roles=roles))

    async def fans_off(self, roles=None):
        if isinstance(roles, (list, tuple)):
            roles = list_to_comma_separated_string(roles)
        return await self.request_with_status("/fans/off", params=dict(roles=roles))

    async def heaters_set(self, role, power):
        """
        role: m1, m2, or m3
        power: integer 0..100
        """
        return await self.request_with_status(
            "/heaters/set", params=dict(role=role, power=power)
        )

    # ----------------------------------------------------
    # M3
    # ----------------------------------------------------
    async def m3_goto(self, target_port):
        return await self.request_with_status("/m3/goto", params=dict(port=target_port))

    async def m3_stop(self):
        return await self.request_with_status("/m3/stop")

    # ----------------------------------------------------
    # Virtual Camera
    # ----------------------------------------------------
    async def virtualcamera_take_image(self):
        """
        Returns a bytes object containing a FITS image simulating a starfield at the current telescope position.
        """
        return await self.request("/virtualcamera/take_image")

    async def virtualcamera_take_image_and_save(self, filename):
        """
        Saves the fake FITS image from PWI4 to the specified filename.
        """
        contents = await self.virtualcamera_take_image()
        with open(filename, "wb") as f:
            f.write(contents)

    # ----------------------------------------------------
    # Testing error handling
    # ----------------------------------------------------
    async def test_command_not_found(self):
        """
        Try making a request to a URL that does not exist.
        """
        return await self.request_with_status("/command/notfound")

    async def test_internal_server_error(self):
        """
        Try making a request to a URL that will return 500 server error due to an intentionally unhandled error.
        """
        return await self.request_with_status("/internal/crash")

    async def test_invalid_parameters(self):
        """
        Make a request with intentionally missing parameters for testing error handling.
        """
        return await self.request_with_status("/mount/goto_ra_dec_apparent")
