=====
smops
=====


.. image:: https://img.shields.io/pypi/v/smops.svg
        :target: https://pypi.python.org/pypi/smops

.. image:: https://img.shields.io/travis/mulan-94/smops.svg
        :target: https://travis-ci.com/mulan-94/smops

.. image:: https://readthedocs.org/projects/smops/badge/?version=latest
        :target: https://smops.readthedocs.io/en/latest/?version=latest
        :alt: Documentation Status




Python script for interpolating FITS model images over frequency. Use as

.. code-block:: python
        usage: smops [-h] [-od] [-nthreads] [-stokes] [-mem] --ms  -ip  -co  [-order]

        Refine model images in frequency

        optional arguments:
        -h, --help            show this help message and exit
        -od , --output-dir    Where to put the output files.
        -nthreads             Number of threads to use while writing out images
        -stokes               Which stokes model to extrapolate. Write as single string e.g IQUV. Required when there are multiple Stokes
                                images in a directory. Default 'I'.
        -mem , --max-mem      Approximate memory cap in GB

        Required arguments:
        --ms                  Input MS. Used for getting reference frequency
        -ip , --input-prefix 
                                The input image prefix. The same as the one used for wsclean
        -co , --channels-out 
                                Number of channels to generate out
        -order , --polynomial-order 
                                Order of the spectral polynomial



* Free software: MIT license





Credits
-------

This package is a brain child of @o-smirnov x @landmanbester and is under @ratt-ru .

It was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
