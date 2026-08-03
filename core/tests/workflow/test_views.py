# SPDX-License-Identifier: Apache-2.0
"""
The derived views. model.py holds the tree; everything here is a
projection of it, and the two projections are independent.
"""
from __future__ import annotations

from sensorkit.workflow import DeviceIndex, SensorModel, Topology

TWO_MOUNTS = {
    "name": "two-mounts",
    "attachments": {"tcs-1": "mount", "tcs-2": "mount"},
    "parts": [],
}


def aliased(parts: list[dict]) -> Topology:
    """A sensor whose OTA carries a filter wheel, over the given leaves."""
    return Topology(SensorModel.model_validate({
        "name": "aliased",
        "attachments": {"tcs": "mount"},
        "parts": [{"name": "ota",
                   "attachments": {"wheel-x": "filter_wheel"},
                   "parts": parts}]}))


def test_neither_view_holds_the_other(topo, devices):
    """Separate classes on separate axes: a caller takes the one it can
    name a use for, and adding a view never widens the other."""
    assert not hasattr(topo, "devices")
    assert not hasattr(devices, "topology")
    assert not hasattr(devices, "instruments")


# ---- ownership: private and shared partition the optical chain ---------

def test_private_and_shared_partition_the_chain(topo):
    for view in topo.instruments.values():
        assert not view.private & set(view.shared)
        chain = view.private | set(view.shared)
        assert chain == set(view.shared) | view.private
        # Every device the instrument reaches, exactly once.
        assert len(view.shared) == len(set(view.shared))
        assert view.assembly.instrument in chain


def test_demo_ownership_is_unchanged_by_the_partition(topo):
    """The demo has no ref claimed both above a leaf and at it, so the
    structural reading (own devices private, ancestors shared) still
    holds there — the partition only differs where a ref aliases."""
    view = topo.instruments[("main-ota", "sel", "port-a")]
    assert view.private == {"cam-sci", "fw-a", "rot-a"}
    assert view.shared == ("tcs-1", "dome-1", "chiller-a", "chiller-b",
                           "cover-1", "sel-1", "fw-shared")


def test_a_ref_claimed_above_is_shared_however_it_was_reached():
    """wheel-x is the OTA's filter wheel *and* cam-a's shutter. It is
    still one device: commanding it moves cam-b's filter too, so it is
    shared even though cam-a attached it itself."""
    topo = aliased([{"name": "cam-a", "instrument": "cam-a",
                     "attachments": {"wheel-x": "shutter"}},
                    {"name": "cam-b", "instrument": "cam-b"}])
    view = topo.instruments[("ota", "cam-a")]
    assert view.private == {"cam-a"}
    assert "wheel-x" in view.shared
    # ...and last, because the chain stays ordered root -> leaf.
    assert view.shared == ("tcs", "wheel-x")


def test_a_ref_claimed_on_a_sibling_leaf_is_shared_with_it():
    """No ancestor relationship at all: fw-1 is cam-a's filter wheel and
    cam-b's shutter. Neither owns it alone."""
    topo = aliased([{"name": "cam-a", "instrument": "cam-a",
                     "attachments": {"fw-1": "filter_wheel"}},
                    {"name": "cam-b", "instrument": "cam-b",
                     "attachments": {"fw-1": "shutter"}}])
    for name in ("cam-a", "cam-b"):
        view = topo.instruments[("ota", name)]
        assert view.private == {name}
        assert "fw-1" in view.shared


def test_a_lone_instrument_still_shares_the_devices_above_it():
    """Ownership is about claim position, not about how many instruments
    happen to exist: the mount is shared infrastructure even when one
    camera is the only thing behind it, so a target belongs on the Step."""
    topo = aliased([{"name": "only", "instrument": "cam-only"}])
    view = topo.instruments[("ota", "only")]
    assert view.private == {"cam-only"}
    assert view.shared == ("tcs", "wheel-x")


def test_by_trait_indexes_every_claim(devices):
    assert sorted(n.ref for n in devices.claiming("filter_wheel")) == [
        "fw-a", "fw-shared", "fw-wide"]
    assert devices.claiming("nonesuch") == ()


def test_traits_of_is_sorted_and_covers_multi_trait_refs(devices):
    assert devices.traits_of("tcs-1") == ("focuser", "mount")
    assert devices.traits_of("dome-1") == ("dome",)
    assert devices.traits_of("nope") == ()


def test_membership_distinguishes_unknown_from_traitless(devices):
    assert "tcs-1" in devices
    assert "nope" not in devices


def test_refs_are_distinct_and_walked_root_first(devices):
    """The order matters: an all_devices op like connect visits the
    sensor root-first, and the golden graphs pin that."""
    refs = list(devices.refs)
    assert len(refs) == len(set(refs))
    assert refs[:4] == ["tcs-1", "dome-1", "chiller-a", "chiller-b"]


def test_path_of_reports_the_root_most_position(devices):
    assert devices.path_of("tcs-1") == ()            # mount at the root
    assert devices.path_of("fw-a") == ("main-ota", "sel", "port-a")


def test_claims_on_chain_walks_the_prefix_chain(devices):
    chain = devices.claims_on_chain(("main-ota", "sel", "port-a"))
    assert chain["filter_wheel"] == ("fw-shared", "fw-a")   # root -> leaf
    assert chain["mount"] == ("tcs-1",)                     # from the root
    assert chain["focuser"] == ("tcs-1",)                   # from main-ota
    assert "rotator" in chain
    # A device on a sibling branch is not on this chain.
    assert "foc-2" not in [r for rs in chain.values() for r in rs]


def test_claims_on_chain_is_cached_but_equal_on_repeat(devices):
    path = ("piggyback", "wide")
    assert devices.claims_on_chain(path) is devices.claims_on_chain(path)


def test_root_claims_are_the_sensor_wide_devices(devices):
    root = devices.root_claims()
    assert root == {"mount": ("tcs-1",), "dome": ("dome-1",),
                    "chiller": ("chiller-a", "chiller-b")}


def test_root_claims_keeps_multiple_claimants_visible():
    """Two mounts is a config error the tasking layer must be able to
    see; the index reports both rather than silently picking one."""
    devices = DeviceIndex(SensorModel.model_validate(TWO_MOUNTS))
    assert devices.root_claims()["mount"] == ("tcs-1", "tcs-2")


def test_both_views_build_from_the_same_sensor_independently(sensor):
    a, b = Topology(sensor), DeviceIndex(sensor)
    assert a.sensor is b.sensor is sensor
