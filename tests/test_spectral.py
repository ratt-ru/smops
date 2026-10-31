import numpy as np
import pytest

from smops.spectral import (
    band_averaged_basis, band_edges, chunk_size, eval_spectral_poly,
    fit_spectral_poly, gen_out_freqs)


def make_bands(nband=8, f0=0.9e9, f1=1.6e9):
    freqs = np.linspace(f0, f1, nband)
    width = freqs[1] - freqs[0] if nband > 1 else 1e8
    return freqs, np.full(nband, width)


def legacy_fit_eval(beta, wsums, infreqs, outfreqs, ref_freq, order):
    """The algorithm from smops <= 0.1.7 (interp_cube), for comparison."""
    delta_freq = infreqs[1] - infreqs[0]
    wlow = (infreqs - delta_freq / 2.0) / ref_freq
    whigh = (infreqs + delta_freq / 2.0) / ref_freq
    wdiff = whigh - wlow
    xfit = np.zeros([infreqs.size, order])
    for i in range(1, order + 1):
        xfit[:, i - 1] = (whigh**i - wlow**i) / (i * wdiff)
    dirty = xfit.T.dot(wsums[:, None] * beta)
    hess = xfit.T.dot(wsums[:, None] * xfit)
    comps = np.linalg.solve(hess, dirty)
    w = outfreqs / ref_freq
    xeval = w[:, None]**np.arange(order)[None, :]
    return xeval.dot(comps)


def test_matches_legacy_point_evaluation():
    rng = np.random.default_rng(0)
    freqs, widths = make_bands()
    wsums = rng.uniform(0.5, 2, freqs.size)
    beta = rng.normal(size=(freqs.size, 200))
    out = np.linspace(0.85e9, 1.65e9, 32)
    legacy = legacy_fit_eval(beta, wsums, freqs, out, 1.3e9, 4)
    coeffs = fit_spectral_poly(beta, wsums, freqs, widths, 1.25e9, 4)
    new = eval_spectral_poly(coeffs, out, None, 1.25e9)
    np.testing.assert_allclose(new, legacy, rtol=1e-6, atol=1e-8)


@pytest.mark.parametrize("ref", [0.5e9, 1.0e9, 1.25e9, 3e9])
def test_reference_frequency_does_not_change_result(ref):
    rng = np.random.default_rng(1)
    freqs, widths = make_bands()
    wsums = rng.uniform(0.5, 2, freqs.size)
    cube = rng.normal(size=(freqs.size, 10, 12))
    out, w = gen_out_freqs(*band_edges(freqs, widths), 40)
    base = eval_spectral_poly(
        fit_spectral_poly(cube, wsums, freqs, widths, 1.25e9, 4), out, w,
        1.25e9)
    other = eval_spectral_poly(
        fit_spectral_poly(cube, wsums, freqs, widths, ref, 4), out, w, ref)
    np.testing.assert_allclose(other, base, rtol=1e-7, atol=1e-9)


def test_recovers_exact_polynomial_spectrum():
    # a cubic spectrum is reproduced exactly by a 4-term fit, both at the
    # input bands and when averaged over new channels
    freqs, widths = make_bands()
    ref = 1.2e9
    true = np.array([[2.0, -1.0], [0.5, 3.0], [-0.3, 0.1], [0.05, -0.2]])
    cube = eval_spectral_poly(true, freqs, widths, ref)
    coeffs = fit_spectral_poly(cube, np.ones(freqs.size), freqs, widths,
                               ref, 4)
    np.testing.assert_allclose(coeffs, true, atol=1e-9)
    out, w = gen_out_freqs(*band_edges(freqs, widths), 16)
    np.testing.assert_allclose(eval_spectral_poly(coeffs, out, w, ref),
                               eval_spectral_poly(true, out, w, ref),
                               atol=1e-9)


def test_flux_conserved_when_regridding():
    # averaging the output channels back over the full band gives the
    # weighted mean of the input bands for a linear fit with equal weights
    freqs, widths = make_bands()
    rng = np.random.default_rng(2)
    cube = rng.normal(size=(freqs.size, 5))
    coeffs = fit_spectral_poly(cube, np.ones(freqs.size), freqs, widths,
                               1.25e9, 2)
    out, w = gen_out_freqs(*band_edges(freqs, widths), 64)
    regridded = eval_spectral_poly(coeffs, out, w, 1.25e9)
    np.testing.assert_allclose(regridded.mean(axis=0), cube.mean(axis=0),
                               atol=1e-10)


def test_band_averaged_basis_point_limit():
    b = band_averaged_basis([1e9, 1.1e9], [1e9, 1.1e9], 1e9, 3)
    x = np.array([0.0, 0.1])
    np.testing.assert_allclose(b, np.stack([x**0, x, x**2], axis=1))


def test_single_image_constant_fit():
    cube = np.arange(6.0).reshape(1, 2, 3)
    coeffs = fit_spectral_poly(cube, [1.0], [1e9], [1e8], 1e9, 1)
    out = eval_spectral_poly(coeffs, [0.96e9, 1.04e9], 0.04e9, 1e9)
    np.testing.assert_allclose(out, np.repeat(cube, 2, axis=0))


def test_nan_pixels_treated_as_zero():
    freqs, widths = make_bands(4)
    cube = np.ones((4, 3))
    cube[:, 1] = np.nan
    coeffs = fit_spectral_poly(cube, np.ones(4), freqs, widths, 1.2e9, 2)
    assert np.all(np.isfinite(coeffs))
    np.testing.assert_allclose(coeffs[:, 1], 0.0)


@pytest.mark.parametrize("order", [0, 5])
def test_invalid_order(order):
    freqs, widths = make_bands(4)
    with pytest.raises(ValueError):
        fit_spectral_poly(np.ones((4, 2)), np.ones(4), freqs, widths,
                          1e9, order)


def test_gen_out_freqs():
    centres, width = gen_out_freqs(1.0e9, 1.4e9, 4)
    assert width == pytest.approx(1e8)
    np.testing.assert_allclose(centres, [1.05e9, 1.15e9, 1.25e9, 1.35e9])


def test_band_edges_unsorted_input():
    lo, hi = band_edges([1.3e9, 1.1e9, 1.2e9], [0.1e9] * 3)
    assert (lo, hi) == pytest.approx((1.05e9, 1.35e9))


def test_chunk_size_never_zero():
    assert chunk_size(npix=10**8, nchan=16, max_mem_bytes=1) == 1
    assert chunk_size(npix=100, nchan=16, max_mem_bytes=10**9) == 16
    assert chunk_size(npix=100, nchan=16, max_mem_bytes=800 * 5) == 5
