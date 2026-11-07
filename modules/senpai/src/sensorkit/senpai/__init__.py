# SPDX-License-Identifier: Apache-2.0
"""SENPAI image analysis integration.

Mute the engine's logging namespace before importing it, and save the current
root handlers so the analyzer can restore them after engine initialization.
The analyzer service redirects engine diagnostics to a separate log file.
"""
import logging

# Whatever owns the root logger before the engine gets to it. Recorded here, while
# `senpai` is still unimported, so `analyzer.quiet_engine_logging()` can take back
# exactly the handlers the engine added and leave an embedding application's own
# logging setup alone.
PRE_IMPORT_ROOT_HANDLERS = tuple(logging.getLogger().handlers)

_engine_logger = logging.getLogger("senpai")
_engine_logger.addHandler(logging.NullHandler())  # keeps `logging.lastResort` off stderr
_engine_logger.propagate = False
