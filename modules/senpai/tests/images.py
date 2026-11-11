# SPDX-License-Identifier: Apache-2.0
"""Synthetic star field images for exercising the SENPAI engine."""

import io

import numpy as np
from astropy.io import fits
from astropy.stats import gaussian_fwhm_to_sigma


def star_field_fits(fwhm: float, *, size: int = 384, stars: int = 50, seed: int = 1) -> bytes:
    """Return a FITS image of Gaussian stars of the given width on a noisy background.

    The same seed places the same stars, so only the width differs between images.
    """
    rng = np.random.default_rng(seed)
    rows, columns = np.mgrid[0:size, 0:size]
    image = rng.normal(1000.0, 10.0, (size, size))
    sigma = fwhm * gaussian_fwhm_to_sigma

    for _ in range(stars):
        x, y = rng.uniform(30, size - 30, 2)
        flux = rng.uniform(3000, 20000)
        image += flux * np.exp(-((columns - x) ** 2 + (rows - y) ** 2) / (2 * sigma**2))

    hdu = fits.PrimaryHDU(image.clip(0, 65535).astype(np.uint16))
    hdu.header["DATE-OBS"] = "2026-11-10T08:30:00"
    hdu.header["EXPTIME"] = 2.0

    buffer = io.BytesIO()
    hdu.writeto(buffer)

    return buffer.getvalue()
