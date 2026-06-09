"""Utilities for dynamic module loading and object resolution from string specifiers."""

import importlib
import importlib.util
import inspect
import pathlib
import sys


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
    """Load and return a Python module from a filesystem path, handling relative imports within it."""
    # Dynamic load boilerplate.
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)

    # We must temporarily add the file's containing directory to the path for any relative imports
    # it might contain to work properly.
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
    """Import a module by name or path, handling relative imports within it."""
    path = pathlib.Path(name)

    if path.exists():
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
