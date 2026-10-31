#!/usr/bin/env python3
"""
Compare spectra of the brightest components in the smops output with the
input sub-band model images.

For each component, the plot shows:
  * input sub-band values as points, with horizontal bars spanning each
    sub-band's width;
  * smops output channels as a step line, each step spanning one output
    channel.

Since both are band averages of the same fitted polynomial, the steps should
follow a smooth curve through the input points (exactly through them if the
spectrum is a polynomial of the fitted order or lower). Also printed for each
component: the RMS difference between the input values and the output
channels averaged over each input sub-band.

Individual clean components rarely have physical spectra, because
deconvolution shuffles flux between neighbouring components from band to
band. Use --box N to sum the model over an N x N pixel box around each
position instead, which gives the integrated spectrum of the source.
Images that are entirely NaN (e.g. a fully flagged band) are not plotted.

Requires matplotlib, which is not a smops dependency:
    pip install matplotlib

Examples:
    python plot_spectra.py -ip img -op smops-interp-model -n 6
    python plot_spectra.py -ip img -op smops-interp-model -n 6 --box 41
"""
import argparse
import glob
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
from astropy.io import fits  # noqa: E402


def freq_axis(header):
    for i in range(1, header.get("NAXIS", 0) + 1):
        ctype = str(header.get(f"CTYPE{i}", "")).upper()
        cunit = str(header.get(f"CUNIT{i}", "")).strip().lower()
        if ctype.startswith("FREQ") or cunit == "hz":
            return i
    raise ValueError("no frequency axis in header")


def find_files(prefix, stokes):
    pref = glob.escape(os.path.abspath(prefix))
    chan = "[0-9]" * 4
    names = glob.glob(f"{pref}-{chan}-{stokes}-model.fits")
    if not names and stokes == "I":
        names = glob.glob(f"{pref}-{chan}-model.fits")
    if not names:
        raise SystemExit(f"No model images found for prefix {prefix}, "
                         f"Stokes {stokes}")
    return names


def box_value(hdu, lead, y, x, half):
    """
    Value at (y, x), or the sum over a (2*half+1)^2 box centred there
    (clipped at the image edges). NaN only if every pixel is NaN.
    """
    if half == 0:
        return float(hdu.section[lead + (y, x)])
    ny = hdu.header["NAXIS2"]
    nx = hdu.header["NAXIS1"]
    ys = slice(max(0, y - half), min(ny, y + half + 1))
    xs = slice(max(0, x - half), min(nx, x + half + 1))
    block = np.asarray(hdu.section[lead + (ys, xs)], dtype=float)
    if np.all(np.isnan(block)):
        return np.nan
    return float(np.nansum(block))


def read_stack(names, pixels, half=0):
    """
    Read frequency, width and the values at the given (y, x) pixels (or
    box sums around them) from each image, without loading whole images.
    Sorted by frequency.
    """
    rows = []
    for name in names:
        with fits.open(name, memmap=True) as hdul:
            hdu = hdul[0]
            ax = freq_axis(hdu.header)
            freq = float(hdu.header[f"CRVAL{ax}"])
            width = abs(float(hdu.header[f"CDELT{ax}"]))
            lead = (0,) * (hdu.header["NAXIS"] - 2)
            vals = np.array([box_value(hdu, lead, y, x, half)
                             for y, x in pixels], dtype=float)
        rows.append((freq, width, vals))
    rows.sort(key=lambda r: r[0])
    freqs = np.array([r[0] for r in rows])
    widths = np.array([r[1] for r in rows])
    return freqs, widths, np.stack([r[2] for r in rows])


def band_sum(names):
    """Sum of the input images, read one at a time to limit memory use."""
    total = None
    for name in names:
        with fits.open(name, memmap=True) as hdul:
            hdu = hdul[0]
            lead = (0,) * (hdu.header["NAXIS"] - 2)
            plane = np.nan_to_num(np.asarray(
                hdu.section[lead + (slice(None),) * 2], dtype=np.float32))
        total = plane if total is None else total + plane
    return total


def brightest_components(summed, n, min_sep):
    """n brightest pixels in |sum over bands|, at least min_sep apart."""
    score = np.abs(summed)
    # only rank pixels that are non-zero (model images are mostly empty)
    nz = np.flatnonzero(score)
    order = nz[np.argsort(score.flat[nz])[::-1]]
    picked = []
    for idx in order:
        if len(picked) == n:
            break
        y, x = np.unravel_index(idx, score.shape)
        if all(max(abs(y - py), abs(x - px)) >= min_sep
               for py, px in picked):
            picked.append((int(y), int(x)))
    return picked


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("-ip", "--input-prefix", required=True)
    p.add_argument("-op", "--output-prefix", default="smops-interp-model")
    p.add_argument("-s", "--stokes", default="I")
    p.add_argument("-n", "--ncomp", type=int, default=6,
                   help="number of components to plot (default 6)")
    p.add_argument("--pix", nargs="+", default=None, metavar="X,Y",
                   help="plot these pixels (0-based x,y) instead of the "
                        "brightest ones")
    p.add_argument("--box", type=int, default=1, metavar="N",
                   help="sum the model over an N x N pixel box around each "
                        "position (N odd; default 1, i.e. single pixels)")
    p.add_argument("--min-sep", type=int, default=50,
                   help="minimum separation in pixels between automatically "
                        "chosen components (default 50, and never less "
                        "than the box size)")
    p.add_argument("-o", "--outfile", default="smops-spectra.png")
    args = p.parse_args()
    stokes = args.stokes.upper()
    if args.box < 1:
        raise SystemExit("--box must be at least 1")
    half = args.box // 2
    box = 2 * half + 1

    in_names = find_files(args.input_prefix, stokes)
    out_names = find_files(args.output_prefix, stokes)

    if args.pix:
        pixels = [tuple(int(v) for v in s.split(","))[::-1] for s in args.pix]
    else:
        summed = band_sum(in_names)
        pixels = brightest_components(summed, args.ncomp,
                                      max(args.min_sep, box))
        del summed
        if not pixels:
            raise SystemExit("Input images are all zero")

    in_f, in_w, in_v = read_stack(in_names, pixels, half)
    out_f, out_w, out_v = read_stack(out_names, pixels, half)
    n_nan = int(np.all(np.isnan(in_v), axis=1).sum())
    if n_nan:
        print(f"{n_nan} input image(s) entirely NaN at these positions "
              "(not plotted)")
    print(f"{len(in_names)} input images, {len(out_names)} output images")

    ncol = min(3, len(pixels))
    nrow = int(np.ceil(len(pixels) / ncol))
    fig, axes = plt.subplots(nrow, ncol, figsize=(5 * ncol, 3.6 * nrow),
                             squeeze=False)
    print(f"{'x':>6} {'y':>6} {'input mean':>12} {'rms(in - out)':>14}")
    for k, (y, x) in enumerate(pixels):
        ax = axes.flat[k]
        # step line: edges of each output channel
        edges = np.append(out_f - out_w / 2, out_f[-1] + out_w[-1] / 2)
        ax.stairs(out_v[:, k], edges / 1e6, baseline=None, color="C0",
                  lw=1.5,
                  label=f"smops output ({len(out_f)} ch)")
        ax.errorbar(in_f / 1e6, in_v[:, k], xerr=in_w / 2e6, fmt="o",
                    color="C3", ms=5, capsize=0, zorder=3,
                    label=f"input ({np.isfinite(in_v[:, k]).sum()} bands)")

        # output averaged over each input band, weighted by overlap
        resid = []
        for f, w, v in zip(in_f, in_w, in_v[:, k]):
            lo, hi = f - w / 2, f + w / 2
            overlap = np.clip(np.minimum(edges[1:], hi)
                              - np.maximum(edges[:-1], lo), 0, None)
            if overlap.sum() > 0 and np.isfinite(v):
                resid.append(v - np.sum(overlap * out_v[:, k])
                             / overlap.sum())
        rms = np.sqrt(np.mean(np.square(resid))) if resid else np.nan
        print(f"{x:6d} {y:6d} {np.nanmean(in_v[:, k]):12.4g} {rms:14.4g}")

        if box == 1:
            ax.set_title(f"pixel x={x}, y={y}", fontsize=10)
            ax.set_ylabel("Model flux (Jy/pixel)")
        else:
            ax.set_title(f"{box}x{box} box at x={x}, y={y}", fontsize=10)
            ax.set_ylabel("Summed model flux (Jy)")
        ax.set_xlabel("Frequency (MHz)")
    for ax in axes.flat[len(pixels):]:
        ax.set_visible(False)
    axes.flat[0].legend(fontsize=8)
    fig.suptitle(f"Stokes {stokes}: {os.path.basename(args.input_prefix)} "
                 f"vs {os.path.basename(args.output_prefix)}")
    fig.tight_layout()
    fig.savefig(args.outfile, dpi=120)
    print(f"Plot written to {args.outfile}")


if __name__ == "__main__":
    main()
