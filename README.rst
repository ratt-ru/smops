=====
smops
=====


.. image:: https://img.shields.io/pypi/v/smops.svg
        :target: https://pypi.python.org/pypi/smops


smops (aka Smooth Operator) is a python package for interpolating channelised FITS model images onto user-specified higher resolution frequency grid. For example, you can give smops a set of 4 per-subband model FITS images, and ask it to return 16 model images, which can then be fed back into e.g. wsclean_ for a predict operation. Usage:

.. code-block:: bash

        smops -ip prefix-used-for-those-images -co 16 -order 4

Everything smops needs (frequencies, band widths and weights) is read from
the FITS headers of the input images, so no Measurement Set is required.

How it works
------------

For every pixel, smops fits a polynomial in frequency to the input sub-band
images, weighted by each image's ``WSCVWSUM``. Each input image is treated as
the average of the polynomial over its sub-band, as in wsclean's own spectral
fitting (`Offringa & Smirnov 2017`_). The polynomial is then averaged over
each of the ``-co`` equal-width output channels spanning the same band, and
written out as one FITS image per channel, using the lowest-frequency input
image as the header template. Non-finite pixels in the inputs are treated
as zero.

The polynomial is expressed in ``nu/ref_freq - 1``. The reference frequency
does not change the result; it defaults to the band centre for good
numerical conditioning.

Options
-------

.. code-block:: text

        Usage: smops [OPTIONS]

          Refine model images in frequency

        Options:
          -v, --version                Show the version and exit.
          -ip, --input-prefix          The input image prefix. The same as the one
                                       used for wsclean  [required]
          -co, --channels-out          Number of channels to generate out  [required]
          -order, --polynomial-order   Order of the spectral polynomial, i.e. the
                                       number of terms (1 = constant, 2 = linear,
                                       ...). Can't exceed the number of input images.
                                       [required]
          -op, --output-prefix         What to prefix the new interpolated model name
                                       with.  [default: smops-interp-model]
          -j, --num-threads            Number of threads to use while writing out
                                       output images  [default: 10]
          -mem, --max-mem              Approximate memory cap in GB for each chunk of
                                       output channels. Default is 20% of total system
                                       memory.
          -s, --stokes                 Which Stokes models to interpolate. Write as a
                                       single string e.g. IQUV. Unlabelled wsclean
                                       models (no Stokes in the file name) are used
                                       for I only.  [default: I]
          -rf, --ref-freq              Reference frequency (Hz) for the polynomial.
                                       Does not change the result, only numerical
                                       conditioning. Default is the centre of the
                                       band.
          -h, --help                   Show this message and exit.

Checking the output
-------------------

``examples/plot_spectra.py`` plots the spectra of the brightest components in
the smops output alongside the input sub-band values. It reads only the
pixels it needs, so it works on very large output cubes. It needs
matplotlib, which is not installed with smops:

.. code-block:: bash

        pip install matplotlib
        python examples/plot_spectra.py -ip prefix-used-for-those-images -op smops-interp-model -n 6

Tests
-----

.. code-block:: bash

        pip install ".[test]"
        pytest


* Free software: MIT license




Credits
-------

This package is a brain child of `@o-smirnov`_ x `@landmanbester`_ and is under `@ratt-ru`_.

It was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
.. _`@o-smirnov`: https://github.com/o-smirnov
.. _`@landmanbester`: https://github.com/landmanbester
.. _`@ratt-ru`: https://github.com/ratt-ru
.. _wsclean: https://gitlab.com/aroffringa/wsclean
.. _`Offringa & Smirnov 2017`: https://arxiv.org/abs/1706.06786