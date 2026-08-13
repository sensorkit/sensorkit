# SPDX-License-Identifier: Apache-2.0
import asyncio
import collections
import contextlib
import pathlib
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any, Literal, NamedTuple, override

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.common.aio import AsyncObserver
from sensorkit.common.filewatch import FileEventKind, watch_dir
from sensorkit.common.keyword import KeywordDict
from sensorkit.data.fits import FITSHeader

DEFAULT_CONTROLLER_ID_FIELD = "SKCTRL"

# A file is announced as soon as it is created, so the first sighting can precede
# its contents: the writer is still streaming, and a Docker bind mount raises
# `created` when the file appears rather than when its data lands. Reading then
# yields a truncated frame — and a header-only FITS opens *without* error, so the
# damage can be a wrong data_size rather than an exception. Re-read until the whole
# frame is there; a frame already complete passes first time, so the initial scan
# of an existing directory pays nothing.
_SETTLE_POLL_S = 0.25
_SETTLE_ATTEMPTS = 20

type ServeDataConfig = ServeLocalFITSConfig


@sk.declare_keyword
class ProductInfo(BaseModel):
    controller_id: str
    product_id: str
    register_time: datetime
    data_size: int


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
        # Subscribe before reading the listing so that updates in between are not missed.
        with self._observer.subscription() as queue:
            for info in await self.get_listing():
                yield info

            while True:
                yield await queue.get()

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
        root = pathlib.Path(self.config.root_directory).resolve()

        while not await asyncio.to_thread(root.exists):
            logger.debug(f"waiting for fits server directory {root} to exist...")
            await asyncio.sleep(30.0)

        async with watch_dir(
            root,
            recursive=True,
            existing=True,
            existing_done=self._listing_ready,
            kinds=(FileEventKind.CREATED, FileEventKind.MOVED, FileEventKind.EXISTING),
        ) as events:
            async for event in events:
                if event.is_directory or event.path.suffix != ".fits":
                    continue
                await self._found_file(event.path)

    async def _read_whole_frame(self, path: pathlib.Path):
        """Read *path* once all of it is on disk, or None if it never gets there."""
        from astropy.io import fits

        def read_file():
            # The header declares how many bytes the frame occupies, so a
            # half-written one is recognisable rather than merely suspected.
            # (astropy usually raises on the short read before we compare.)
            with fits.open(path) as hdul:
                stat = path.stat()
                if stat.st_size < sum(hdu.filebytes() for hdu in hdul):
                    return None

                # Metadata comes from the HDU that carries the image. A
                # tile-compressed frame leaves an empty stub in the primary HDU
                # and holds the image and its cards in an extension, so the
                # shape test is what distinguishes the two. Neither is_image nor
                # shape touches pixel data, so nothing is decompressed here.
                for hdu in hdul:
                    if hdu.is_image and hdu.shape:
                        return stat, hdu.header

                return stat, hdul[0].header

        for _ in range(_SETTLE_ATTEMPTS):
            try:
                found = await asyncio.to_thread(read_file)
            except Exception:
                found = None  # empty or unparsable: nothing written yet, or garbage
            if found is not None:
                return found
            await asyncio.sleep(_SETTLE_POLL_S)

        return None

    async def _found_file(self, path: pathlib.Path):
        try:
            found = await self._read_whole_frame(path)
            if found is None:
                logger.warning(f"{path} is not a complete FITS frame, skipping")
                return

            stat, header = found
            controller_id: Any = None

            # Metadata takes precedence over path-based controller ID resolution.
            match self.config.controller_id:
                case "from_metadata":
                    controller_id = header.get(
                        self.config.controller_id_field or DEFAULT_CONTROLLER_ID_FIELD
                    )
                case "from_path":
                    root = pathlib.Path(self.config.root_directory).resolve()
                    relative_path = path.relative_to(root)
                    controller_id = (
                        relative_path.parts[0] if len(relative_path.parts) > 1 else root.parts[-1]
                    )

            if controller_id is None or not isinstance(controller_id, str):
                logger.warning(f"cannot determine controller for {path}, skipping")
                return

            # Create ProductInfo and inject into the metadata. The product ID is the name of the
            # file. This makes name clashes with other handlers unlikely, even if the stem of the
            # filename is the same as another product ID (possibly the same one).
            info = ProductInfo(
                controller_id=controller_id,
                product_id=path.name,
                register_time=datetime.fromtimestamp(getattr(stat, "st_birthtime", stat.st_mtime), UTC),
                data_size=stat.st_size,
            )
            metadata = KeywordDict(info, FITSHeader.from_astropy_header(header))

            # Cache and notify observers.
            self._cache[controller_id][info.product_id] = _CacheEntry(info, path, metadata)
            self._observer.notify(info)
        except Exception:
            logger.exception(f"error processing {path}")
