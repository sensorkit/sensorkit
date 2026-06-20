import asyncio
import collections
import contextlib
import pathlib
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Literal, NamedTuple, override

from astropy.io.fits.card import UNDEFINED
from loguru import logger
from pydantic import BaseModel
from watchdog.observers import Observer

import sensorkit.api as sk
from sensorkit.common.aio import AsyncObserver
from sensorkit.common.keyword import KeywordDict
from sensorkit.data.filesys import FileAppearedHandler

DEFAULT_CONTROLLER_ID_FIELD = "SKCTRL"

type ServeDataConfig = ServeLocalFITSConfig


@sk.declare_keyword
class ProductInfo(BaseModel):
    controller_id: str
    product_id: str
    register_time: datetime
    data_size: int


def _header_to_metadata(header) -> KeywordDict:
    """Convert a FITS header into a serializable KeywordDict."""
    result = KeywordDict()

    for card in header.cards:
        keyword = card.keyword

        if not keyword:
            continue

        value = None if card.value is UNDEFINED else card.value

        if keyword in result:
            if not isinstance(result[keyword], list):
                result[keyword] = [result[keyword]]

            result[keyword].append(value)
        else:
            result[keyword] = value

    return result


class _CacheEntry(NamedTuple):
    info: ProductInfo
    path: pathlib.Path
    metadata: KeywordDict


class ServeHandler(ABC):
    """Abstract base for data product serving backends."""

    @abstractmethod
    async def get_listing(self) -> list[ProductInfo]:
        """Return all known data product records.

        This method waits until the initial listing is complete before returning.
        """

    @abstractmethod
    def watch_listing(self) -> AsyncIterator[ProductInfo]:
        """Yield all data product records as they become known."""

    @abstractmethod
    def has_product(self, controller_id: str, product_id: str) -> bool:
        """Return whether the given product is currently known for the controller.

        Reflects only what is known right now; unlike `get_listing`, it does not wait
        for the initial listing to complete.
        """

    @abstractmethod
    def get_metadata(self, controller_id: str, product_id: str) -> KeywordDict:
        """Return the metadata dict for the given controller and product.

        The `ProductInfo` keyword must always be present in the returned metadata,
        whether it was embedded in the persisted metadata or injected by the handler.
        """

    @abstractmethod
    async def get_data(self, controller_id: str, product_id: str) -> bytes:
        """Return the raw bytes for the given controller and product."""

    @abstractmethod
    def start_monitor(self, *, task_group: asyncio.TaskGroup):
        """Start the background monitoring task."""

    @abstractmethod
    async def stop_monitor(self):
        """Stop the background monitoring task."""


class ServeLocalFITSConfig(BaseModel):
    """Configure serving of FITS files."""

    kind: Literal["local_fits"] = "local_fits"
    root_directory: str
    controller_id: Literal["from_path", "from_metadata"] = "from_path"
    controller_id_field: str | None = None

    def create_handler(self):
        return ServeLocalFITSHandler(self)


class ServeLocalFITSHandler(ServeHandler):
    """ServeHandler that watches a local directory for FITS files."""

    def __init__(self, config: ServeLocalFITSConfig):
        self.config = config
        self._task: asyncio.Task | None = None
        self._observer: AsyncObserver[ProductInfo] = AsyncObserver()
        self._cache = collections.defaultdict(dict[str, _CacheEntry])
        self._listing_ready = asyncio.Event()

    @override
    async def get_listing(self):
        await self._listing_ready.wait()
        return [entry.info for products in self._cache.values() for entry in products.values()]

    @override
    async def watch_listing(self):
        queue = self._observer.subscribe()

        for info in await self.get_listing():
            yield info

        try:
            while True:
                yield await queue.get()
        finally:
            self._observer.unsubscribe(queue)

    @override
    def has_product(self, controller_id: str, product_id: str):
        return product_id in cache if (cache := self._cache.get(controller_id)) else False

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

            def read_file():
                with fits.open(path) as hdul:
                    return path.stat(), _header_to_metadata(hdul[0].header)

            stat, metadata = await asyncio.to_thread(read_file)
            controller_id: str | None = None

            # Metadata takes precedence over path-based controller ID resolution.
            match self.config.controller_id:
                case "from_metadata":
                    controller_id = metadata.get(
                        self.config.controller_id_field or DEFAULT_CONTROLLER_ID_FIELD,
                    )
                case "from_path":
                    root = pathlib.Path(self.config.root_directory)
                    relative_path = path.relative_to(root)
                    controller_id = (
                        relative_path.parts[0] if len(relative_path.parts) > 1 else root.parts[-1]
                    )

            if controller_id is None:
                logger.warning(f"cannot determine controller for {path}, skipping")
                return

            # Create ProductInfo and inject into the metadata. The product ID is the name of the
            # file. This makes name clashes with other handlers unlikely, even if the stem of the
            # filename is the same as another product ID (possibly the same one).
            info = ProductInfo(
                controller_id=controller_id,
                product_id=path.name,
                register_time=datetime.fromtimestamp(stat.st_birthtime, UTC),
                data_size=stat.st_size,
            )
            metadata.set(info)

            # Cache and notify observers.
            self._cache[controller_id][info.product_id] = _CacheEntry(info, path, metadata)
            await self._observer.notify(info)
        except Exception:
            logger.exception(f"error processing {path}")
