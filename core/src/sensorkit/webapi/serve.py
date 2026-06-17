import asyncio
import collections
import contextlib
import pathlib
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Literal, NamedTuple, Protocol, override

from loguru import logger
from pydantic import BaseModel
from watchdog.observers import Observer

from sensorkit.common.aio import AsyncObserver
from sensorkit.data.filesys import FileAppearedHandler

type ControllerProductPair = tuple[str, str]
type ServeDataConfig = ServeLocalFITSConfig


class PathMetadataPair(NamedTuple):
    path: pathlib.Path
    metadata: dict


class ServeHandler(ABC):
    """Abstract base for data product serving backends."""

    @abstractmethod
    async def get_listing(self) -> list[ControllerProductPair]:
        """Return all known data product records.

        This method waits until the initial listing is complete before returning.
        """

    @abstractmethod
    def watch_listing(self) -> AsyncIterator[ControllerProductPair]:
        """Yield all data product records as they become known."""

    @abstractmethod
    def get_metadata(self, controller_id: str, product_id: str) -> dict:
        """Return the metadata dict for the given controller and product."""

    @abstractmethod
    async def get_data(self, controller_id: str, product_id: str) -> bytes:
        """Return the raw bytes for the given controller and product."""

    @abstractmethod
    def start_monitor(self, *, task_group: asyncio.TaskGroup):
        """Start the background monitoring task."""

    @abstractmethod
    async def stop_monitor(self):
        """Stop the background monitoring task."""


class ServeConfig(Protocol):
    """Protocol for config objects that produce a ServeHandler."""

    def create_handler(self) -> ServeHandler: ...


class ServeLocalFITSConfig(BaseModel):
    """Configure serving of FITS files."""

    kind: Literal["local_fits"] = "local_fits"
    root_directory: str
    controller_from_subdirectory: bool = False
    controller_from_metadata: str | None = None

    def create_handler(self):
        return ServeLocalFITSHandler(self)


class ServeLocalFITSHandler(ServeHandler):
    """ServeHandler that watches a local directory for FITS files."""

    def __init__(self, config: ServeLocalFITSConfig):
        self.config = config
        self._task: asyncio.Task | None = None
        self._observer: AsyncObserver[ControllerProductPair] = AsyncObserver()
        self._cache = collections.defaultdict(dict[str, PathMetadataPair])
        self._listing_ready = asyncio.Event()

    @override
    async def get_listing(self):
        await self._listing_ready.wait()
        return [
            (controller_id, product_id)
            for controller_id, products in self._cache.items()
            for product_id in products
        ]

    @override
    async def watch_listing(self):
        queue = self._observer.subscribe()

        for controller_id, product_id in await self.get_listing():
            yield controller_id, product_id

        try:
            while True:
                yield await queue.get()
        finally:
            self._observer.unsubscribe(queue)

    @override
    def get_metadata(self, controller_id: str, product_id: str):
        return self._cache[controller_id][product_id].metadata

    @override
    async def get_data(self, controller_id: str, product_id: str):
        path = self._cache[controller_id][product_id].path
        raw_bytes = await asyncio.to_thread(path.read_bytes)
        return raw_bytes

    @override
    def start_monitor(self, *, task_group: asyncio.TaskGroup):
        self._task = task_group.create_task(self._monitor())

    @override
    async def stop_monitor(self):
        if self._task is not None:
            self._task.cancel()

            with contextlib.suppress(asyncio.CancelledError):
                await self._task

        self._task = None

    async def _monitor(self):
        root = pathlib.Path(self.config.root_directory)
        queue: asyncio.Queue[pathlib.Path] = asyncio.Queue()

        while not await asyncio.to_thread(root.exists):
            logger.debug(f"waiting for fits server directory {root} to exist...")
            await asyncio.sleep(30.0)

        handler = FileAppearedHandler(queue, match_suffix=".fits")
        observer = Observer()
        observer.schedule(handler, str(root), recursive=True)
        observer.start()

        try:
            for path in root.rglob("*.fits"):
                if path.is_file():
                    await self._found_file(path)

            self._listing_ready.set()

            while True:
                path = await queue.get()
                await self._found_file(path)
                queue.task_done()
        finally:
            observer.stop()
            queue.shutdown(True)

    async def _found_file(self, path: pathlib.Path):
        try:
            from astropy.io import fits

            def _read_header():
                with fits.open(path) as hdul:
                    return hdul[0].header.copy()

            metadata = await asyncio.to_thread(_read_header)
            controller_id: str | None = None

            # Metadata takes precedence over subdirectory
            if self.config.controller_from_metadata:
                controller_id = metadata.get(self.config.controller_from_metadata)

            if controller_id is None and self.config.controller_from_subdirectory:
                root = pathlib.Path(self.config.root_directory)
                relative_path = path.relative_to(root)
                controller_id = (
                    relative_path.parts[0] if len(relative_path.parts) > 1 else root.parts[-1]
                )

            if controller_id is None:
                logger.warning(f"cannot determine controller for {path}, skipping")
                return

            product_id = path.stem
            self._cache[controller_id][product_id] = PathMetadataPair(path, metadata)
            await self._observer.notify((controller_id, product_id))
        except Exception:
            logger.exception(f"error processing {path}")
