#! /bin/python3
import sys
import argparse
import os
import re
import time
import logging
import psutil
import numpy as np
import dask.array as da

from astropy.io import fits
from casacore.tables import table
from glob import glob
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from dask import compute

import smops.cmdline as cmd

GB = 2**30
MAX_MEM = None

def configure_logger(out_dir="."):
    formatter = logging.Formatter(
        datefmt='%H:%M:%S %d.%m.%Y',
        fmt="%(asctime)s : %(name)s - %(levelname)s - %(message)s")
    
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)

    l_handler = logging.FileHandler(
        os.path.join(out_dir, "smops.log"), mode="w")
    l_handler.setLevel(logging.INFO)
    l_handler.setFormatter(formatter)

    s_handler = logging.StreamHandler()
    s_handler.setLevel(logging.INFO)
    s_handler.setFormatter(formatter)

    logger = logging.getLogger("smops")
    logger.setLevel(logging.INFO)

    logger.addHandler(l_handler)
    logger.addHandler(s_handler)
    return logger


def get_ms_ref_freq(ms_name):
    snitch.info("Getting reference frequency from MS")
    with table(f"{ms_name}::SPECTRAL_WINDOW", ack=False) as spw_subtab:
        ref_freq, = spw_subtab.getcol("REF_FREQUENCY")
    return ref_freq


def read_input_image_header(im_name):
    """
    Parameters
    ----------
    im_name: :obj:`string`
        Image name

    Output
    ------
    info: dict
        Dictionary containing center frequency, frequency delta and image wsum
    """
    snitch.debug(f"Reading image: {im_name} header")
    info = {}

    info["name"] = im_name
   
    with fits.open(im_name, readonly=True) as hdu_list:
        # print(f"There are:{len(hdu_list)} HDUs in this image")
        for hdu in hdu_list:
            naxis = hdu.header["NAXIS"]
            # get the center frequency
            for i in range(1, naxis+1):
                if hdu.header[f"CUNIT{i}"].lower() == "hz":
                    info["freq"] = hdu.header[f"CRVAL{i}"]
                    info["freq_delta"] = hdu.header[f"CDELT{i}"]

            #get the wsum
            info["wsum"] = hdu.header["WSCVWSUM"]
            info["data"] = hdu.data
    return info


def get_band_start_and_band_width(freq_delta, first_freq, last_freq):
    """
    Parameters
    ----------
    freq_delta: float
        The value contained in cdelt. Difference between the different
        consecutive bands
    first_freq: float
        Center frequency for the very first image in the band. ie. band 0 image
    last_freq: float
        Center frequency for the last image in the band. ie. band -1 image

    Output
    ------
    band_start: float
        Where the band starts
    band_width: float
        Size of the band
    """
    snitch.info("Calculating the band starting frequency and  band width")
    band_delta = freq_delta/2
    band_start = first_freq - band_delta
    band_stop = last_freq + band_delta
    band_width = band_stop - band_start
    return band_start, band_width


def gen_out_freqs(band_start, band_width, n_bands, return_cdelt=False):
    """
    Parameters
    ----------
    band_start: int or float
        Where the band starts from
    band_width: int or float
        Size of the band
    n_bands: int
        Number of output bands you want
    return_cdelt: bool
        Whether or not to return cdelt

    Output
    ------
    center_freqs: list or array
        iterable containing output center frequencies
    cdelt: int or float
        Frequency delta
    """
    snitch.info("Generating output center frequencies")
    cdelt = band_width/n_bands
    first_center_freq = band_start + (cdelt/2)

    center_freqs = [first_center_freq]
    for i in range(n_bands-1):
        center_freqs.append(center_freqs[-1] + cdelt)

    center_freqs = np.array(center_freqs)
    if return_cdelt:
        return center_freqs, cdelt
    else:
        return center_freqs


def concat_models(models):
    """Concatenate/stack model images over frequency axis"""
    snitch.info(f"Concatenating {len(models)} model images")
    return np.concatenate(models, axis=1).squeeze()


def interp_cube(model, wsums, infreqs, outfreqs, ref_freq, spectral_poly_order):
    """
    Interpolate the model into desired frequency

    Parameters
    ----------

    model: ndarray
        model array containing model image's data for each stokes parameter
    wsums: list or array
        concatenated wsums for each of stokes parameters
    infreqs: list or array
        a list of input frequencies for the images?
    outfreqs: int
        Number of output frequencies i.e how many frequencies you want out
    ref_freq: float
        The reference frequency. A frequency representative of this 
        spectral window, usually the sky frequency corresponding to the DC edge
        of the baseband. Used by the calibration system if a fixed scaling
        frequency is required or **in algorithms to identify the observing band**. 
        see https://casa.nrao.edu/casadocs/casa-5.1.1/reference-material/measurement-set
    spectral_poly_order: int
        the order of the spectral polynomial
    """

    snitch.info("Starting frequency interpolation")

    nchan = outfreqs.size
    nband, nx, ny = model.shape

    result = {"xdims": nx, "ydims": ny}

    # components excluding zeros
    beta = np.ma.masked_equal(model.reshape(nband,-1), 0)
   
    if spectral_poly_order > infreqs.size:
        raise ValueError("spectral-poly-order can't be larger than nband")

    # we are given frequencies at bin centers, convert to bin edges
    #delta_freq is the same as CDELt value in the image header
    delta_freq = infreqs[1] - infreqs[0] 

    wlow = (infreqs - delta_freq/2.0)/ref_freq
    whigh = (infreqs + delta_freq/2.0)/ref_freq
    wdiff = whigh - wlow

    # set design matrix for each component
    # look at Offringa and Smirnov 1706.06786
    xfit = np.zeros([nband, spectral_poly_order])
    for i in range(1, spectral_poly_order+1):
        xfit[:, i-1] = (whigh**i - wlow**i)/(i*wdiff)



    dirty_comps = np.dot(xfit.T, wsums*beta)
    hess_comps = xfit.T.dot(wsums*xfit)

    comps = da.from_array(
        np.linalg.solve(hess_comps, dirty_comps),
        chunks="auto")

    w = outfreqs/ref_freq
    
    xeval = w[:, np.newaxis]**np.arange(spectral_poly_order)[np.newaxis, :]

    # autogenerate step size. x by 3 coz betaout subarray grwos by 3
    step = int((MAX_MEM*GB)//(comps.nbytes*0.8))
    betaout = dict()
    for _i in range(0, nchan, step):
        end = _i+step if _i+step < nchan else nchan
        betaout[_i, end] = da.dot(xeval[_i:end], comps).rechunk("auto")
        if "nbytes" not in result:
            result["nbytes"] = betaout[_i, end].nbytes
        snitch.info(f"Selecting channel {_i:4} >> {end:2}"); 
    result["data"] = betaout
    
    return result


def gen_fits_file_from_template(template_fits, center_freq, cdelt, new_data, out_fits):
    """
    Generate new FITS file from some template file
    
    template_fits: str
        Name of the file to use as template
    center_freq: float
        New center frequency for this image
    cdelt: float
        Channel widths between successive channels
    new_data: ndarray
        The new interpolated data
    out_fits: str
        Name of the new output file
    """
    with fits.open(template_fits, mode="readonly") as temp_hdu_list:
        temp_hdu, = temp_hdu_list

        #update the center frequency
        for i in range(1, temp_hdu.header["NAXIS"]+1):
            if temp_hdu.header[f"CUNIT{i}"].lower() == "hz":
                temp_hdu.header[f"CRVAL{i}"] = center_freq
                temp_hdu.header[f"CDELT{i}"] = cdelt
      
        #update with the new data
        if temp_hdu.data.ndim == 4:
            temp_hdu.data[0,0] = new_data
        elif temp_hdu.data.ndim == 3:
            temp_hdu.data[0] = new_data
        elif temp_hdu.data.ndim == 2:
            temp_hdu.data = new_data
        temp_hdu_list.writeto(out_fits, overwrite=True)
    snitch.info(f"New file written to: {out_fits}")
    return


def write_model_out(chan_num, chan_id, temp_fname, out_pref, cdelt, models, freqs, stokes=None):
    """
    Write the new models output

    Parameters
    ----------
    chan_num: int
        Number of the channel in the current model sub-cube. Will always
        start from 0. This is based on the current workflow where we chunk the
        new array into subs for better memory mgmnt
    chan_id: int
        Actual channel number in the 'grand scheme' of things. Mostly for 
        naming purposes.
    temp_fname: str
        Name of the template file that will be used
    out_pref: str
        Prefix of the output models
    cdelt: float
        Channel width for this channel
    models: n-d array
        Model cube containing data for various channels. The channel number will
        be selected using :obj:`chan_num` above
    freqs: n-d array
        Array containing the corresponding frequencies. Selected using
        :obj:`chan_id` above
    
    """
    # snitch.info(f"Channel number: {chan_num}, id: {chan_id}")
    if stokes is None:
        outname = out_pref + '-' + f"{chan_id}".zfill(4) + "-model.fits"
    else:
        outname = out_pref + '-' + f"{chan_id}".zfill(4) + f"-{stokes.upper()}-model.fits"

    gen_fits_file_from_template(
        temp_fname, freqs[chan_id], cdelt,
        models[chan_num], outname)


def main():
    args = cmd.get_arguments(standalone_mode=False)

    # This is a hack because click fails with -v and -h in standalone_mode=F
    if isinstance(args, int):
        sys.exit()
    global snitch, MAX_MEM

    # output_dir = args.output_dir
    # if not os.path.isdir(output_dir):
    #     snitch.info(f"Creating output directory: {output_dir}")
    #     os.makedirs(output_dir)

    snitch = configure_logger()
    
    

    if args.max_mem is not None:
        MAX_MEM = args.max_mem
    else:
        MAX_MEM = int(.2 *psutil.virtual_memory().total)//GB
    snitch.info(f"Setting memory cap to: {MAX_MEM} GB")
   

    ref_freq = get_ms_ref_freq(args.ms_name)  

    snitch.info(f"Specified -stokes: {args.stokes.upper()}")

    for stokes in args.stokes.upper():
        START_TIME = time.perf_counter()

        snitch.info(f"Running Stoke's {stokes}")
        
        input_pref = os.path.abspath(args.input_prefix)

        EXPLICIT_STOKES = stokes
        images_list = sorted(
            glob(f"{input_pref}-[0-9][0-9][0-9][0-9]-{stokes}-model.fits"),
            key=os.path.getctime)

        if len(images_list) == 0:
            EXPLICIT_STOKES = None
            images_list = sorted(
                glob(f"{input_pref}-[0-9][0-9][0-9][0-9]-model.fits"),
                key=os.path.getctime)
        
    
        if len(images_list) == 0:
            snitch.warning("No image files were found")
            sys.exit(-1)
        else:
            snitch.info(f"Found {len(images_list)} matching selections")
            for _im in images_list:
                snitch.info(os.path.basename(_im))
            snitch.info("."*len(os.path.basename(_im)))

        im_heads = []

        for im_name in images_list:
            im_header = read_input_image_header(im_name)
            im_heads.append(im_header)
        
        bstart, bwidth = get_band_start_and_band_width(
            im_heads[0]["freq_delta"], im_heads[0]["freq"], im_heads[-1]["freq"])

        model = concat_models([image_item["data"] for image_item in im_heads])
        out_freqs = gen_out_freqs(bstart, bwidth, args.channels_out)
        new_cdelt = out_freqs[1] - out_freqs[0]

        # gather the wsums and center frequencies
        w_sums = np.array([item["wsum"] for item in im_heads])
        w_sums = w_sums[:, np.newaxis]
        input_center_freqs = np.array([item["freq"] for item in im_heads])


        mod_out = interp_cube(model, w_sums, input_center_freqs,
                                    out_freqs, ref_freq, args.poly_order)
        mod_data = mod_out["data"]
        
        
        for chan_range, data in mod_data.items():
            data, = compute(data)
            data = data.reshape(-1, mod_out["xdims"], mod_out["ydims"])

            chan_ids = range(*chan_range)
            chan_range = range(len(chan_ids))

            results = []
            with ThreadPoolExecutor(args.nthreads) as executor:
                results = executor.map(
                    partial(write_model_out, temp_fname=images_list[0],
                            out_pref=args.output_pref, cdelt=new_cdelt,
                            models=data, freqs=out_freqs, stokes=EXPLICIT_STOKES), 
                    chan_range, chan_ids)

            results = list(results)
            snitch.info("Chunk change over") 
            snitch.info("*"*50)
        
        snitch.info(f"Stoke's {stokes} finished in {time.perf_counter() - START_TIME:.3f} secs")


if __name__ == "__main__":
    main()
