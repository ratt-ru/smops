=====
smops
=====


.. image:: https://img.shields.io/pypi/v/smops.svg
        :target: https://pypi.python.org/pypi/smops


smops (aka Smooth Operator) is a python package for interpolating channelised FITS model images onto user-specified higher resolution frequency grid. For example, you can give smops a set of 4 per-subband model FITS images, and ask it to return 16 model images, which can then be fed back into e.g. wsclean_ for a predict operation. Usage:

.. code-block:: bash

        smops --ms /ms/used/togen/images.ms -ip prefix-used-for-those-images -co 16 -order 4


Its options are:

.. code-block:: python

        usage: smops [-h] [-v] [-op] [-j] [-s] [-mem] -ms  -ip  -co  [-order]

        Refine model images in frequency

        optional arguments:
        -h, --help            show this help message and exit
        -v, --version         show program's version number and exit
        -op , --output-prefix 
                                What to prefix the new interpolated model name with.
        -j , --num-threads    Number of threads to use while writing out output images
        -s , --stokes         Which stokes model to extrapolate. Write as single string e.g IQUV. Required when there are multiple Stokes
                                images in a directory. Default 'I'.
        -mem , --max-mem      Approximate memory cap in GB

        Required arguments:
        -ms , --ms            Input MS. Used for getting reference frequency
        -ip , --input-prefix 
                                The input image prefix. The same as the one used for wsclean
        -co , --channels-out 
                                Number of channels to generate out
        -order , --polynomial-order 
                                Order of the spectral polynomial



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