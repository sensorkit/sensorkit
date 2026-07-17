# SPDX-License-Identifier: Apache-2.0
"""SENPAI streak analyzer.

Importing `senpai` runs its `setup_logging()` as an import side effect: it logs
the engine's start-up on the way past and attaches a console handler to the
*root* logger. Every process listing `sensorkit.senpai.service` in
`sensorkit.imports` pays for that, not just the one running the analyzer -- so
an agent, a controller or the config loader each echo the engine's banner, and
then every stdlib record in the process (httpx, matplotlib, astropy) onto a
console that belongs to loguru.

The banner is emitted *during* that import, so it cannot be removed afterwards
-- only pre-empted. Mute the engine's namespace here, before anything imports
`senpai`: this is the package, so it runs before the submodules that pull the
engine in. The root handler it bolts on is dropped after the fact instead, by
`analyzer.quiet_engine_logging()`; the process that actually runs the analyzer
re-enables the namespace into a file via `analyzer.redirect_engine_logging()`.
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
