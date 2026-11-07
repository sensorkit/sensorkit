# SPDX-License-Identifier: Apache-2.0
"""Utilities for dynamic module loading and object resolution from string specifiers."""

import importlib
import importlib.util
import inspect
import pathlib
import sys
from importlib.machinery import ModuleSpec
from types import ModuleType


def get_caller_module(*, depth: int):
    """Retrieves the module of the calling function at the specified stack depth."""
    # Account for the direct caller frame.
    depth += 1

    # Find the requested module by inspecting the call stack.
    stack = inspect.stack()

    if len(stack) <= depth:
        return None

    frame = stack[depth][0]
    module = inspect.getmodule(frame)

    return module


def module_from_file(path: pathlib.Path, name: str):
    """Load a Python file with its directory temporarily on the import path."""
    # Dynamic load boilerplate.
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)

    # Temporarily allow imports of sibling modules from the file's directory.
    sys.path.append(str(path.parent))

    try:
        # Evaluate the module code.
        spec.loader.exec_module(module)
    except Exception as e:
        # Wrap the exception so a downstream ModuleNotFoundError won't confuse things.
        raise ImportError from e
    finally:
        # Restore sys.path even if import fails.
        sys.path.pop()

    return module


def import_module_or_file(name: str):
    """Import a module by name or load a file with access to sibling modules.

    Importing the same path again returns the already loaded module.
    """
    path = pathlib.Path(name)

    if path.exists():
        match sys.modules.get(path.stem):
            case ModuleType(__spec__=ModuleSpec(origin=str(origin))) as module if (
                pathlib.Path(origin).resolve() == path.resolve()
            ):
                return module

        module = module_from_file(path, path.stem)
        sys.modules[path.stem] = module
        return module

    return importlib.import_module(name)


def obj_from_spec[T](
    *,
    spec: str,
    base: type[T],
    subclass: bool = False,
    load_file: bool = False,
) -> T:
    """Find and return an object of a given type based on a string specifier."""
    module_name, sep, obj_name = spec.rpartition(":")

    if not sep:
        module_name = spec
        obj_name = None

    module = (
        import_module_or_file(module_name)
        if load_file
        else importlib.import_module(module_name)
    )
    haystack = [obj_name] if obj_name else dir(module)

    for symbol in haystack:
        obj = getattr(module, symbol)

        if subclass:
            if isinstance(obj, type) and obj is not base and issubclass(obj, base):
                return obj
        elif isinstance(obj, base):
            return obj

    kind = "subclass" if subclass else "instance"
    raise ValueError(f"no {base.__name__} {kind} matching spec found in {module_name}")
