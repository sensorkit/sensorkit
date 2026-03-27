from sensorkit.common.condition import (
    AboveCondition,
    BecomesCondition,
    BelowCondition,
    ChangesCondition,
    CrossesAboveCondition,
    CrossesBelowCondition,
    EqualsCondition,
    resolve_field,
)


def test_resolve_field_dict():
    data = {"a": {"b": {"c": 42}}}
    assert resolve_field(data, "a.b.c") == 42


def test_resolve_field_missing_key():
    assert resolve_field({"a": 1}, "b") is None


def test_resolve_field_nested_none():
    assert resolve_field({"a": None}, "a.b") is None


def test_resolve_field_object():
    class Obj:
        x = 10

    assert resolve_field(Obj(), "x") == 10


def test_changes_fires_on_difference():
    c = ChangesCondition()
    assert c.evaluate("b", "a", False) == (True, True)


def test_changes_silent_when_same():
    c = ChangesCondition()
    assert c.evaluate("a", "a", False) == (False, False)


def test_changes_works_with_numbers():
    c = ChangesCondition()
    assert c.evaluate(2, 1, False) == (True, True)
    assert c.evaluate(1, 1, True) == (False, False)


def test_above_fires_when_over_threshold():
    c = AboveCondition(threshold=50.0)
    assert c.evaluate(55, 40, False) == (True, True)


def test_above_silent_when_under():
    c = AboveCondition(threshold=50.0)
    assert c.evaluate(45, 40, False) == (False, False)


def test_above_at_threshold_not_active():
    c = AboveCondition(threshold=50.0)
    assert c.evaluate(50, 40, False) == (False, False)


def test_above_deadband_stays_active():
    c = AboveCondition(threshold=50.0, deadband=5.0)
    # Active and value is within deadband (>= 45) -> stays active
    assert c.evaluate(47.0, 55.0, True) == (True, True)


def test_above_deadband_clears():
    c = AboveCondition(threshold=50.0, deadband=5.0)
    # Active but value dropped below deadband (< 45) -> clears
    assert c.evaluate(44.0, 47.0, True) == (False, False)


def test_above_non_numeric():
    c = AboveCondition(threshold=50.0)
    assert c.evaluate("not a number", 40, False) == (False, False)


def test_below_fires_when_under_threshold():
    c = BelowCondition(threshold=10.0)
    assert c.evaluate(5, 15, False) == (True, True)


def test_below_silent_when_over():
    c = BelowCondition(threshold=10.0)
    assert c.evaluate(15, 20, False) == (False, False)


def test_below_at_threshold_not_active():
    c = BelowCondition(threshold=10.0)
    assert c.evaluate(10, 15, False) == (False, False)


def test_below_deadband_stays_active():
    c = BelowCondition(threshold=10.0, deadband=2.0)
    # Active and value is within deadband (<= 12) -> stays active
    assert c.evaluate(11.0, 5.0, True) == (True, True)


def test_below_deadband_clears():
    c = BelowCondition(threshold=10.0, deadband=2.0)
    # Active but value rose above deadband (> 12) -> clears
    assert c.evaluate(13.0, 11.0, True) == (False, False)


def test_below_non_numeric():
    c = BelowCondition(threshold=10.0)
    assert c.evaluate(None, 5, False) == (False, False)


def test_equals_fires_on_match():
    c = EqualsCondition(threshold="tracking")
    assert c.evaluate("tracking", "idle", False) == (True, True)


def test_equals_silent_on_mismatch():
    c = EqualsCondition(threshold="tracking")
    assert c.evaluate("slewing", "idle", False) == (False, False)


def test_equals_stays_active_while_matching():
    c = EqualsCondition(threshold="tracking")
    assert c.evaluate("tracking", "tracking", True) == (True, True)


def test_equals_numeric_coercion():
    c = EqualsCondition(threshold=1.0)
    # String "1.0" should coerce to float for comparison
    assert c.evaluate("1.0", "0", False) == (True, True)


def test_equals_bool():
    c = EqualsCondition(threshold=True)
    assert c.evaluate(True, False, False) == (True, True)
    assert c.evaluate(False, True, False) == (False, False)


def test_becomes_fires_on_transition():
    c = BecomesCondition(threshold=False)
    assert c.evaluate(False, True, False) == (True, True)


def test_becomes_silent_when_already_at_threshold():
    c = BecomesCondition(threshold=False)
    assert c.evaluate(False, False, False) == (False, False)


def test_becomes_silent_when_leaving_threshold():
    c = BecomesCondition(threshold=False)
    assert c.evaluate(True, False, False) == (False, False)


def test_becomes_none_threshold():
    c = BecomesCondition(threshold=None)
    assert c.evaluate(None, "something", False) == (True, True)
    assert c.evaluate(None, None, False) == (False, False)


def test_becomes_string():
    c = BecomesCondition(threshold="error")
    assert c.evaluate("error", "ok", False) == (True, True)
    assert c.evaluate("ok", "error", False) == (False, False)


def test_becomes_numeric_coercion():
    c = BecomesCondition(threshold=1.0)
    # "1.0" coerces to 1.0; "0" coerces to 0.0
    assert c.evaluate("1.0", "0", False) == (True, True)
    assert c.evaluate("1.0", "1.0", False) == (False, False)


def test_crosses_above_fires_on_crossing():
    c = CrossesAboveCondition(threshold=50.0)
    assert c.evaluate(51, 49, False) == (True, True)


def test_crosses_above_silent_when_already_above():
    c = CrossesAboveCondition(threshold=50.0)
    # Both above threshold, no crossing
    assert c.evaluate(55, 52, False) == (False, False)


def test_crosses_above_silent_when_below():
    c = CrossesAboveCondition(threshold=50.0)
    assert c.evaluate(48, 45, False) == (False, False)


def test_crosses_above_does_not_refire_while_active():
    c = CrossesAboveCondition(threshold=50.0)
    # Already active -> should_notify is False, is_active stays True
    assert c.evaluate(55, 52, True) == (False, True)


def test_crosses_above_deadband_holds():
    c = CrossesAboveCondition(threshold=50.0, deadband=5.0)
    # Active, in deadband (>= 45) -> stays active
    assert c.evaluate(47.0, 55.0, True) == (False, True)


def test_crosses_above_deadband_clears():
    c = CrossesAboveCondition(threshold=50.0, deadband=5.0)
    # Active, below deadband (< 45) -> clears
    assert c.evaluate(44.0, 47.0, True) == (False, False)


def test_crosses_above_re_fires_after_deadband_clear():
    c = CrossesAboveCondition(threshold=50.0, deadband=5.0)
    # Was cleared, now crosses above again
    assert c.evaluate(51.0, 48.0, False) == (True, True)


def test_crosses_above_non_numeric():
    c = CrossesAboveCondition(threshold=50.0)
    assert c.evaluate("nope", 40, False) == (False, False)


def test_crosses_below_fires_on_crossing():
    c = CrossesBelowCondition(threshold=11.0)
    assert c.evaluate(10, 12, False) == (True, True)


def test_crosses_below_silent_when_already_below():
    c = CrossesBelowCondition(threshold=11.0)
    assert c.evaluate(9, 10, False) == (False, False)


def test_crosses_below_silent_when_above():
    c = CrossesBelowCondition(threshold=11.0)
    assert c.evaluate(13, 14, False) == (False, False)


def test_crosses_below_does_not_refire_while_active():
    c = CrossesBelowCondition(threshold=11.0)
    assert c.evaluate(9, 10, True) == (False, True)


def test_crosses_below_deadband_holds():
    c = CrossesBelowCondition(threshold=11.0, deadband=1.0)
    # Active, in deadband (<= 12) -> stays active
    assert c.evaluate(11.5, 10.0, True) == (False, True)


def test_crosses_below_deadband_clears():
    c = CrossesBelowCondition(threshold=11.0, deadband=1.0)
    # Active, above deadband (> 12) -> clears
    assert c.evaluate(12.5, 11.5, True) == (False, False)


def test_crosses_below_re_fires_after_deadband_clear():
    c = CrossesBelowCondition(threshold=11.0, deadband=1.0)
    assert c.evaluate(10.0, 11.5, False) == (True, True)


def test_crosses_below_non_numeric():
    c = CrossesBelowCondition(threshold=11.0)
    assert c.evaluate(None, 12, False) == (False, False)
