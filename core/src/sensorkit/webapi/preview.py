from __future__ import annotations

import asyncio
import hashlib
import io

_CACHE_MAX = 16
_cache: dict[bytes, asyncio.Task[PreviewJPEG]] = {}


def _discard_if_unsuccessful(checksum: bytes, task: asyncio.Task[PreviewJPEG]):
    """Drop failed/cancelled renders so a later request can retry instead of
    replaying the cached exception forever."""
    if (task.cancelled() or task.exception() is not None) and _cache.get(checksum) is task:
        del _cache[checksum]


class PreviewJPEG:
    """A JPEG preview rendered from a FITS product, tagged with its source checksum.

    Build instances with `from_fits`, which renders off the event loop and
    caches results by checksum so identical content is never re-rendered. Only the
    checksum and JPEG are retained -- never the raw FITS bytes.
    """

    def __init__(self, checksum: bytes, jpeg_bytes: bytes):
        self.checksum = checksum
        self.jpeg_bytes = jpeg_bytes

    @classmethod
    async def from_fits(cls, raw_bytes: bytes) -> PreviewJPEG:
        """Return a preview for `raw_bytes`, rendering it off the event loop.

        Cache bookkeeping runs on the event loop, so concurrent callers are
        serialized without a lock. Identical content in flight is rendered once
        and shared: callers await the same task. Only the hash and the
        astropy/PIL encode -- the CPU-bound work -- are offloaded to threads.
        """
        checksum = (await asyncio.to_thread(hashlib.sha256, raw_bytes)).digest()
        task = _cache.get(checksum)

        if task is not None:
            # Refresh recency: move to the end of the insertion order.
            del _cache[checksum]
            _cache[checksum] = task
        else:
            # A real Task (not a bare coroutine) so the shared render survives any
            # single caller being cancelled, e.g. a client disconnecting.
            task = asyncio.create_task(asyncio.to_thread(cls._encode, raw_bytes, checksum))
            task.add_done_callback(lambda t: _discard_if_unsuccessful(checksum, t))

            # No await between insert and eviction, so the cache can't be observed
            # over-size or torn. The just-inserted task is newest, never evicted.
            _cache[checksum] = task

            if len(_cache) > _CACHE_MAX:
                del _cache[next(iter(_cache))]

        return await task

    @classmethod
    def _encode(cls, raw: bytes, checksum: bytes):
        import numpy as np
        from astropy.io import fits
        from PIL import Image

        with fits.open(io.BytesIO(raw)) as hdul:
            data = None

            for hdu in hdul:
                if hdu.data is not None and hdu.data.ndim >= 2:
                    data = hdu.data
                    break

        if data is None:
            raise ValueError("No 2D image data found in FITS file")

        # Collapse any leading dimensions down to 2D
        while data.ndim > 2:
            data = data[0]

        data = data.astype(np.float32)
        low, high = np.percentile(data, (1, 99))

        if high > low:
            scaled = np.clip((data - low) / (high - low), 0.0, 1.0)
        else:
            scaled = np.zeros_like(data)

        img = Image.fromarray((scaled * 255).astype(np.uint8), mode="L")
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=90)

        return cls(checksum, buf.getvalue())
