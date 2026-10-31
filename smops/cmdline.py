import click

from smops import VERSION


def _positive(ctx, param, value):
    if value is not None and value < 1:
        raise click.BadParameter("must be at least 1")
    return value


@click.command(context_settings={"help_option_names": ["-h", "--help"]},
               help="Refine model images in frequency")
@click.version_option(VERSION, "-v", "--version")
@click.option("-ip", "--input-prefix", required=True, metavar="",
              help="The input image prefix. The same as the one used for "
                   "wsclean")
@click.option("-co", "--channels-out", type=int, required=True, metavar="",
              callback=_positive,
              help="Number of channels to generate out")
@click.option("-order", "--polynomial-order", type=int, required=True,
              metavar="", callback=_positive,
              help="Order of the spectral polynomial, i.e. the number of "
                   "terms (1 = constant, 2 = linear, ...). Can't exceed the "
                   "number of input images.")
@click.option("-op", "--output-prefix", default="smops-interp-model",
              show_default=True, metavar="",
              help="What to prefix the new interpolated model name with.")
@click.option("-j", "--num-threads", type=int, default=10, show_default=True,
              metavar="", callback=_positive,
              help="Number of threads to use while writing out output images")
@click.option("-mem", "--max-mem", type=float, default=None, metavar="",
              help="Approximate memory cap in GB for each chunk of output "
                   "channels. Default is 20% of total system memory.")
@click.option("-s", "--stokes", default="I", show_default=True, metavar="",
              help="Which Stokes models to interpolate. Write as a single "
                   "string e.g. IQUV. Unlabelled wsclean models (no Stokes "
                   "in the file name) are used for I only.")
@click.option("-rf", "--ref-freq", type=float, default=None, metavar="",
              help="Reference frequency (Hz) for the polynomial. Does not "
                   "change the result, only numerical conditioning. "
                   "Default is the centre of the band.")
def get_arguments(**kwargs):
    return kwargs
