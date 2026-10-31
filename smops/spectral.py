"""
Numerical core of smops: fit a spectral polynomial to a stack of
sub-band model images and evaluate it on a new frequency grid.

This module depends only on numpy, so it can be used (and tested)
without any FITS machinery.

The model for each pixel is a polynomial in the scaled frequency

    x = nu / ref_freq - 1

Each input image is treated as the *average* of that polynomial over its
sub-band (see Offringa & Smirnov 2017, arXiv:1706.06786), which is how
wsclean's own sub-band model images behave. Output images are evaluated
the same way, i.e. as averages over each output channel.

The choice of ref_freq does not change the fitted spectrum: changing it
is an affine change of the variable x, which the polynomial absorbs
exactly. It only affects numerical conditioning, so it defaults to the
band centre.
"""

import numpy as np


def band_averaged_basis(freq_lo, freq_hi, ref_freq, order):
    """
    Design matrix of polynomial terms averaged over frequency intervals.

    Parameters
    ----------
    freq_lo, freq_hi: array_like
        Lower and upper edges (Hz) of each interval. Where an interval has
        zero width, the point value of each term is used instead.
    ref_freq: float
        Reference frequency (Hz) used to scale the variable.
    order: int
        Number of polynomial terms (1 = constant, 2 = linear, ...).

    Returns
    -------
    basis: ndarray, shape (n_intervals, order)
        basis[j, k] is the mean of x**k over interval j.
    """
    xlo = np.asarray(freq_lo, dtype=float) / ref_freq - 1.0
    xhi = np.asarray(freq_hi, dtype=float) / ref_freq - 1.0
    dx = xhi - xlo
    point = dx == 0
    safe_dx = np.where(point, 1.0, dx)
    xmid = 0.5 * (xlo + xhi)

    basis = np.empty((xlo.size, order))
    for k in range(order):
        avg = (xhi ** (k + 1) - xlo ** (k + 1)) / ((k + 1) * safe_dx)
        basis[:, k] = np.where(point, xmid ** k, avg)
    return basis


def fit_spectral_poly(cube, wsums, freqs, widths, ref_freq, order):
    """
    Weighted least-squares fit of a spectral polynomial to every pixel.

    Parameters
    ----------
    cube: ndarray, shape (nband, ...)
        Model images stacked along the first axis. Non-finite values are
        treated as zero.
    wsums: array_like, shape (nband,)
        Per-band weights (wsclean's WSCVWSUM).
    freqs: array_like, shape (nband,)
        Band centre frequencies (Hz).
    widths: array_like, shape (nband,)
        Band widths (Hz).
    ref_freq: float
        Reference frequency (Hz).
    order: int
        Number of polynomial terms.

    Returns
    -------
    coeffs: ndarray, shape (order, ...)
        Polynomial coefficients for each pixel.
    """
    cube = np.asarray(cube)
    nband = cube.shape[0]
    if order < 1:
        raise ValueError("polynomial order must be at least 1")
    if order > nband:
        raise ValueError(
            f"polynomial order ({order}) can't be larger than the number of "
            f"input images ({nband})")

    freqs = np.asarray(freqs, dtype=float)
    widths = np.asarray(widths, dtype=float)
    wsums = np.asarray(wsums, dtype=float).reshape(nband)

    basis = band_averaged_basis(freqs - widths / 2, freqs + widths / 2,
                                ref_freq, order)
    data = np.where(np.isfinite(cube), cube, 0.0).reshape(nband, -1)

    weighted_basis = basis * wsums[:, None]
    hess = basis.T @ weighted_basis
    rhs = weighted_basis.T @ data
    coeffs = np.linalg.solve(hess, rhs)
    return coeffs.reshape((order,) + cube.shape[1:])


def eval_spectral_poly(coeffs, freqs, widths, ref_freq):
    """
    Evaluate fitted polynomials averaged over output channels.

    Parameters
    ----------
    coeffs: ndarray, shape (order, ...)
        Output of :func:`fit_spectral_poly`.
    freqs: array_like, shape (nchan,)
        Output channel centre frequencies (Hz).
    widths: array_like or None
        Output channel widths (Hz). None evaluates at the channel centres.
    ref_freq: float
        Must be the same reference frequency used for the fit.

    Returns
    -------
    cube: ndarray, shape (nchan, ...)
    """
    coeffs = np.asarray(coeffs)
    order = coeffs.shape[0]
    freqs = np.asarray(freqs, dtype=float)
    widths = np.zeros_like(freqs) if widths is None else np.asarray(
        widths, dtype=float) * np.ones_like(freqs)
    basis = band_averaged_basis(freqs - widths / 2, freqs + widths / 2,
                                ref_freq, order)
    out = basis @ coeffs.reshape(order, -1)
    return out.reshape((freqs.size,) + coeffs.shape[1:])


def band_edges(freqs, widths):
    """Lowest and highest frequency covered by a set of bands."""
    freqs = np.asarray(freqs, dtype=float)
    widths = np.abs(np.asarray(widths, dtype=float))
    return float(np.min(freqs - widths / 2)), float(np.max(freqs + widths / 2))


def gen_out_freqs(band_start, band_stop, nchan):
    """
    Split [band_start, band_stop] into nchan equal channels.

    Returns
    -------
    centres: ndarray, shape (nchan,)
    width: float
    """
    if nchan < 1:
        raise ValueError("number of output channels must be at least 1")
    width = (band_stop - band_start) / nchan
    centres = band_start + width * (np.arange(nchan) + 0.5)
    return centres, width


def chunk_size(npix, nchan, max_mem_bytes, itemsize=8):
    """
    Number of output channels to compute at once so that one chunk of
    output (nchunk x npix values) stays within max_mem_bytes. Always at
    least 1.
    """
    per_chan = max(1, npix * itemsize)
    return int(min(nchan, max(1, max_mem_bytes // per_chan)))
