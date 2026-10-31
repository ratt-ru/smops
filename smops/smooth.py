#!/usr/bin/env python3
import os
import sys
import glob
import logging
import time
from concurrent.futures import ThreadPoolExecutor

import click
import numpy as np
import psutil
from astropy.io import fits

from smops.spectral import (
    band_edges, chunk_size, eval_spectral_poly, fit_spectral_poly,
    gen_out_freqs)

GB = 2**30
DEFAULT_MEM_FRACTION = 0.2

log = logging.getLogger("smops")


def configure_logger(out_dir="."):
    formatter = logging.Formatter(
        datefmt='%H:%M:%S %d.%m.%Y',
        fmt="%(asctime)s : %(name)s - %(levelname)s - %(message)s")

    os.makedirs(out_dir, exist_ok=True)

    l_handler = logging.FileHandler(
        os.path.join(out_dir, "smops.log"), mode="w")
    l_handler.setFormatter(formatter)

    s_handler = logging.StreamHandler()
    s_handler.setFormatter(formatter)

    log.handlers.clear()
    log.setLevel(logging.INFO)
    log.addHandler(l_handler)
    log.addHandler(s_handler)
    return log


def freq_axis(header):
    """FITS axis number (1-based) of the frequency axis, or None."""
    for i in range(1, header.get("NAXIS", 0) + 1):
        ctype = str(header.get(f"CTYPE{i}", "")).upper()
        cunit = str(header.get(f"CUNIT{i}", "")).strip().lower()
        if ctype.startswith("FREQ") or cunit == "hz":
            return i
    return None


def read_input_image(im_name):
    """
    Read a wsclean model image.

    Returns
    -------
    info: dict
        name, freq (Hz), freq_delta (Hz), wsum, data (2D array), shape
        (original data shape)
    """
    log.debug(f"Reading image: {im_name}")
    with fits.open(im_name, mode="readonly") as hdu_list:
        hdu = hdu_list[0]
        header = hdu.header
        ax = freq_axis(header)
        if ax is None:
            raise ValueError(f"{im_name}: no frequency axis found in header")

        # copy, so nothing refers to the (possibly memory-mapped) file
        # once it is closed
        data = np.array(hdu.data)
        if data.ndim < 2 or np.prod(data.shape[:-2]) != 1:
            raise ValueError(
                f"{im_name}: expected a single image plane, got data of "
                f"shape {data.shape}")

        wsum = header.get("WSCVWSUM")
        if wsum is None:
            log.warning(f"{im_name}: no WSCVWSUM in header, using weight 1")
            wsum = 1.0

        return {
            "name": im_name,
            "freq": float(header[f"CRVAL{ax}"]),
            "freq_delta": abs(float(header[f"CDELT{ax}"])),
            "wsum": float(wsum),
            "data": data.reshape(data.shape[-2:]),
            "shape": data.shape,
        }


def find_images(input_prefix, stokes, allow_unlabelled):
    """
    Find the wsclean channel model images for one Stokes parameter.

    Returns
    -------
    names: list of str
    labelled: bool
        Whether the files carry a Stokes label in their names.
    """
    pref = glob.escape(os.path.abspath(input_prefix))
    chan = "[0-9]" * 4
    names = glob.glob(f"{pref}-{chan}-{stokes}-model.fits")
    if names:
        return names, True
    if allow_unlabelled:
        names = glob.glob(f"{pref}-{chan}-model.fits")
        if names:
            return names, False
    return [], True


def write_model(template_header, template_shape, freq_ax, centre_freq,
                cdelt, data, out_name):
    """Write one output channel, using the first input image's header."""
    header = template_header.copy()
    header[f"CRVAL{freq_ax}"] = centre_freq
    header[f"CDELT{freq_ax}"] = cdelt
    out = np.asarray(data, dtype=np.float32).reshape(template_shape)
    fits.PrimaryHDU(data=out, header=header).writeto(out_name, overwrite=True)
    log.debug(f"New file written to: {out_name}")
    return out_name


def output_name(prefix, chan, stokes=None):
    if stokes is None:
        return f"{prefix}-{chan:04d}-model.fits"
    return f"{prefix}-{chan:04d}-{stokes}-model.fits"


def smooth_stokes(input_prefix, stokes, allow_unlabelled, nchan_out, order,
                  output_prefix, nthreads, max_mem_bytes, ref_freq=None):
    """Interpolate the models of a single Stokes parameter."""
    names, labelled = find_images(input_prefix, stokes, allow_unlabelled)
    if not names:
        raise FileNotFoundError(
            f"No model images found for Stokes {stokes} with prefix "
            f"{input_prefix}")

    images = sorted((read_input_image(n) for n in names),
                    key=lambda im: im["freq"])
    log.info(f"Found {len(images)} model images for Stokes {stokes}:")
    for im in images:
        log.info(f"  {os.path.basename(im['name'])}  "
                 f"{im['freq'] / 1e6:.4f} MHz")

    shapes = {im["data"].shape for im in images}
    if len(shapes) > 1:
        raise ValueError(f"Input images have different shapes: {shapes}")
    freqs = np.array([im["freq"] for im in images])
    if np.unique(freqs).size != freqs.size:
        raise ValueError("Some input images share the same frequency")
    widths = np.array([im["freq_delta"] for im in images])
    wsums = np.array([im["wsum"] for im in images])

    bstart, bstop = band_edges(freqs, widths)
    if ref_freq is None:
        ref_freq = 0.5 * (bstart + bstop)
    log.info(f"Band: {bstart / 1e6:.4f} - {bstop / 1e6:.4f} MHz, "
             f"reference frequency {ref_freq / 1e6:.4f} MHz")

    out_freqs, out_width = gen_out_freqs(bstart, bstop, nchan_out)

    cube = np.stack([im["data"] for im in images])
    ny, nx = cube.shape[1:]
    log.info(f"Fitting {order}-term polynomial to {len(images)} images "
             f"of {ny}x{nx} pixels")
    coeffs = fit_spectral_poly(cube, wsums, freqs, widths, ref_freq, order)
    del cube

    with fits.open(images[0]["name"], mode="readonly") as hdul:
        template_header = hdul[0].header.copy()
    template_shape = images[0]["shape"]
    fax = freq_axis(template_header)
    out_stokes = stokes if labelled else None

    step = chunk_size(ny * nx, nchan_out, max_mem_bytes)
    written = []
    for start in range(0, nchan_out, step):
        stop = min(start + step, nchan_out)
        log.info(f"Computing output channels {start} - {stop - 1}")
        chunk = eval_spectral_poly(coeffs, out_freqs[start:stop], out_width,
                                   ref_freq)
        with ThreadPoolExecutor(nthreads) as executor:
            futures = [
                executor.submit(
                    write_model, template_header, template_shape, fax,
                    out_freqs[chan], out_width, chunk[chan - start],
                    output_name(output_prefix, chan, out_stokes))
                for chan in range(start, stop)]
            written.extend(f.result() for f in futures)
    log.info(f"Wrote {len(written)} images, "
             f"{os.path.basename(written[0])} ... "
             f"{os.path.basename(written[-1])}")
    return written


def main(argv=None):
    import smops.cmdline as cmd

    try:
        args = cmd.get_arguments.main(args=argv, prog_name="smops",
                                      standalone_mode=False)
    except click.ClickException as e:
        e.show()
        sys.exit(e.exit_code)
    except click.exceptions.Abort:
        sys.exit(1)
    # --help / --version return an exit code rather than the arguments
    if not isinstance(args, dict):
        sys.exit(0)

    configure_logger()

    if args["max_mem"] is not None:
        max_mem_bytes = args["max_mem"] * GB
    else:
        max_mem_bytes = DEFAULT_MEM_FRACTION * psutil.virtual_memory().total
    log.info(f"Setting memory cap to: {max_mem_bytes / GB:.2f} GB")

    stokes_list = args["stokes"].upper()
    log.info(f"Specified -stokes: {stokes_list}")

    for stokes in stokes_list:
        t0 = time.perf_counter()
        log.info(f"Running Stokes {stokes}")
        try:
            smooth_stokes(
                args["input_prefix"], stokes,
                allow_unlabelled=(stokes == "I"),
                nchan_out=args["channels_out"],
                order=args["polynomial_order"],
                output_prefix=args["output_prefix"],
                nthreads=args["num_threads"],
                max_mem_bytes=max_mem_bytes,
                ref_freq=args["ref_freq"])
        except (FileNotFoundError, ValueError) as e:
            log.error(str(e))
            sys.exit(1)
        log.info(f"Stokes {stokes} finished in "
                 f"{time.perf_counter() - t0:.3f} secs")


if __name__ == "__main__":
    main()
