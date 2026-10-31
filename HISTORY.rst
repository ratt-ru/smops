=======
History
=======
0.2.0 (unreleased)
------------------
* Breaking: removed ``--ms``. The reference frequency, which does not affect
  the result, now defaults to the band centre taken from the image headers
  (new optional ``--ref-freq``). Fixes failure on multi-SPW MSs. Drops the
  python-casacore dependency.
* Input images are ordered by frequency, not file creation time.
* Output channels are band-averaged, consistent with how inputs are fitted.
* Polynomial fitted in ``nu/ref_freq - 1`` for better conditioning
  (same result).
* Fixed zero chunk size when the memory cap is small; ``--max-mem`` accepts
  fractional GB.
* A requested Stokes parameter with no labelled images is now an error,
  instead of silently using unlabelled (Stokes I) images. Unlabelled images
  are only used for I.
* Works with a single input image (order 1); frequency axis found from
  CTYPE or CUNIT; missing WSCVWSUM falls back to equal weights.
* Command line built with plain click; dropped the stimela/scabha, dask and
  OmegaConf dependencies. Same option names as before.
* Numerics split into ``smops.spectral`` (numpy only); added tests.
* Packaging: fixed setup.cfg (long description files, licence file, pytest
  could not parse it), Python 3.9-3.13, updated CI to run tests.

0.1.7 (2023-04-07)
------------------
* Return stimela as a dependency
* Allow NaN values while dealing with interpolations, issue #5


0.1.6 (2022-12-16)
------------------
* Removed stimela2 as a dependency


0.1.5 (2022-12-7)
------------------
* Fixes error with ones rather than zero in the background


0.1.4 (2022-10-26)
------------------
* Fix naming of Stokes parameters


0.1.3 (2022-09-22)
------------------
Switched from `argparse` to `click`
Adds `scabha` as dependency
Introduced schemas for commandline setup


0.1.2 (2022-09-14)
------------------
* Added `output-prefix` argument
* Removed `output-dir` argument


0.1.0 (2022-09-12)
------------------
* First release on PyPI.
