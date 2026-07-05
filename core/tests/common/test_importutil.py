# SPDX-License-Identifier: Apache-2.0
import pathlib
import sys
import types

import pytest

from sensorkit.common.importutil import (
    get_caller_module,
    module_from_file,
    obj_from_spec,
)


def test_get_caller_module():
    # Helper that asks for the caller's module (depth=0 -> immediate caller)
    def helper():
        mod = get_caller_module(depth=0)
        assert mod is not None

        # Ensure we got this test module.
        assert pathlib.Path(mod.__file__).name == "test_importutil.py"
        return mod

    # The caller of helper() is this test function; helper() should
    # therefore see this test module.
    m = helper()
    assert m is not None

    # Use a very large depth to exceed the stack length.
    assert get_caller_module(depth=9999) is None


def test_module_from_file(tmp_path: pathlib.Path):
    # Create a helper module that the main module will import.
    helper = tmp_path / "helper_mod.py"
    helper.write_text("VALUE = 42\n")

    # The main module imports from helper_mod and exposes EXPORTED.
    main = tmp_path / "main_mod.py"
    main.write_text(
        "from helper_mod import VALUE\n"
        "EXPORTED = VALUE\n"
    )

    before = list(sys.path)
    mod = module_from_file(main, name=main.stem)
    after = list(sys.path)

    # Validate import worked and sys.path was restored.
    assert getattr(mod, "EXPORTED", None) == 42
    assert before == after

    # Simulate exception on load.
    bad = tmp_path / "boom.py"
    bad.write_text("raise RuntimeError('kaboom')\n")

    before = list(sys.path)
    with pytest.raises(ImportError):
        module_from_file(bad, name=bad.stem)
    after = list(sys.path)

    # Ensure sys.path was restored even on error
    assert before == after


def test_obj_from_spec_by_name():
    # Create a synthetic module in sys.modules
    mod_name = "_temp_mod_for_importutil_tests_1"
    fake = types.ModuleType(mod_name)
    fake.x = 123
    sys.modules[mod_name] = fake

    try:
        value = obj_from_spec(spec=f"{mod_name}:x", base=int, subclass=False, load_file=False)
        assert value == 123
    finally:
        sys.modules.pop(mod_name, None)


def test_obj_from_spec_by_scan():
    mod_name = "_temp_mod_for_importutil_tests_2"
    fake = types.ModuleType(mod_name)
    # Only one int instance so scanning dir(fake) will eventually find it
    fake.answer = 42
    sys.modules[mod_name] = fake

    try:
        value = obj_from_spec(spec=mod_name, base=int, subclass=False, load_file=False)
        assert value == 42
    finally:
        sys.modules.pop(mod_name, None)


def test_obj_from_spec_subclass():
    mod_name = "_temp_mod_for_importutil_tests_3"
    fake = types.ModuleType(mod_name)

    class MyError(Exception):
        pass

    # Attach the class to the fake module
    fake.MyError = MyError
    sys.modules[mod_name] = fake

    try:
        cls = obj_from_spec(spec=f"{mod_name}:MyError", base=BaseException, subclass=True, load_file=False)
        assert cls is MyError
        assert issubclass(cls, BaseException)
    finally:
        sys.modules.pop(mod_name, None)


def test_obj_from_spec_not_found():
    mod_name = "_temp_mod_for_importutil_tests_4"
    fake = types.ModuleType(mod_name)
    fake.s = "not an int"
    sys.modules[mod_name] = fake

    try:
        # Missing attribute should raise AttributeError.
        with pytest.raises(AttributeError):
            obj_from_spec(spec=f"{mod_name}:missing", base=int, subclass=False, load_file=False)

        # Existing attribute of the wrong type should raise ValueError.
        with pytest.raises(ValueError) as ei:
            obj_from_spec(spec=f"{mod_name}:s", base=int, subclass=False, load_file=False)

        msg = str(ei.value)
        assert "no int instance" in msg
        assert mod_name in msg
    finally:
        sys.modules.pop(mod_name, None)


def test_obj_from_spec_from_file(tmp_path: pathlib.Path):
    mod_file = tmp_path / "mods_from_file.py"
    mod_file.write_text(
        "x = 7\n"
    )

    # When load_file=True and the path exists, obj_from_spec should load from the file
    value = obj_from_spec(spec=f"{str(mod_file)}:x", base=int, subclass=False, load_file=True)
    assert value == 7
