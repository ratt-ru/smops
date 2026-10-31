"""End-to-end tests on synthetic wsclean-like FITS model images."""
import os

import numpy as np
import pytest

fits = pytest.importorskip("astropy.io.fits")

from smops import smooth  # noqa: E402
from smops.spectral import eval_spectral_poly  # noqa: E402

NBAND = 4
WIDTH = 0.2e9
FREQS = 0.9e9 + WIDTH * (np.arange(NBAND) + 0.5)   # 0.9 - 1.7 GHz band
REF = 1.3e9
# per-pixel linear spectra in x = nu/REF - 1
COEFFS = np.stack([np.arange(12.0).reshape(3, 4),
                   np.full((3, 4), -2.0)])


def write_image(path, freq, data, wsum=1.0, stokes_val=1.0):
    hdu = fits.PrimaryHDU(data=data.astype(np.float32)[None, None])
    h = hdu.header
    for i, (ctype, cunit, crval, cdelt) in enumerate([
            ("RA---SIN", "deg", 0.0, -1e-3),
            ("DEC--SIN", "deg", -30.0, 1e-3),
            ("FREQ", "Hz", freq, WIDTH),
            ("STOKES", "", stokes_val, 1.0)], start=1):
        h[f"CTYPE{i}"] = ctype
        h[f"CUNIT{i}"] = cunit
        h[f"CRVAL{i}"] = crval
        h[f"CDELT{i}"] = cdelt
        h[f"CRPIX{i}"] = 1.0
    h["WSCVWSUM"] = wsum
    hdu.writeto(path, overwrite=True)


@pytest.fixture
def model_dir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    cube = eval_spectral_poly(COEFFS, FREQS, WIDTH, REF)
    # write in reverse order so creation time disagrees with frequency
    for chan in reversed(range(NBAND)):
        write_image(tmp_path / f"img-{chan:04d}-model.fits", FREQS[chan],
                    cube[chan])
    write_image(tmp_path / "img-MFS-model.fits", FREQS.mean(),
                cube.mean(axis=0))
    return tmp_path


def read(path):
    with fits.open(path) as hdul:
        return hdul[0].header, hdul[0].data


def test_end_to_end(model_dir):
    smooth.main(["-ip", "img", "-co", "8", "-order", "2", "-op", "out",
                 "-j", "2", "-mem", "0.000001"])
    names = sorted(os.listdir(model_dir))
    outs = [n for n in names if n.startswith("out-")]
    assert outs == [f"out-{c:04d}-model.fits" for c in range(8)]

    out_freqs = 0.9e9 + 0.1e9 * (np.arange(8) + 0.5)
    expected = eval_spectral_poly(COEFFS, out_freqs, 0.1e9, REF)
    for chan, name in enumerate(outs):
        header, data = read(model_dir / name)
        assert data.shape == (1, 1, 3, 4)
        assert header["CRVAL3"] == pytest.approx(out_freqs[chan])
        assert header["CDELT3"] == pytest.approx(0.1e9)
        np.testing.assert_allclose(data[0, 0], expected[chan], rtol=1e-5,
                                   atol=1e-5)


def test_missing_stokes_is_an_error(model_dir):
    # unlabelled models must not be silently used as Stokes Q
    with pytest.raises(SystemExit) as e:
        smooth.main(["-ip", "img", "-co", "4", "-order", "1", "-s", "IQ"])
    assert e.value.code == 1


def test_labelled_stokes(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for chan, f in enumerate(FREQS):
        for s, val in (("I", 1.0), ("Q", 0.1)):
            write_image(tmp_path / f"p-{chan:04d}-{s}-model.fits", f,
                        np.full((3, 4), val))
    smooth.main(["-ip", "p", "-co", "2", "-order", "1", "-s", "iq"])
    for s, val in (("I", 1.0), ("Q", 0.1)):
        for chan in range(2):
            _, data = read(tmp_path / f"smops-interp-model-{chan:04d}-{s}-"
                                      "model.fits")
            np.testing.assert_allclose(data, val, rtol=1e-6)


def test_order_too_large(model_dir):
    with pytest.raises(SystemExit) as e:
        smooth.main(["-ip", "img", "-co", "4", "-order", "5"])
    assert e.value.code == 1


def test_version(capsys):
    with pytest.raises(SystemExit) as e:
        smooth.main(["--version"])
    assert e.value.code == 0
    assert smooth.__name__ and "version" in capsys.readouterr().out
