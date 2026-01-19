Introduction
========================

`almaqso` is a Python package to download, process and analyze bunch of calibration data observed by ALMA automatically.
It is designed to collaborate with assets of ALMA community such as CASA, analysisUtils of CASA, ALMA Science Archive, and so on.

This project was originally developed by Yuki Yoshimura and the repository can be found at https://github.com/astroysmr/almaqso.

Why almaqso?
------------------------

`almaqso` was originally developed to study the galactic molecular clouds.
When quasi-stellar objects (QSOs) that are observed as calibration sources by ALMA are located behind the molecular clouds, the calibration data can contain the absorption features caused by them.
By analyzing such absorption features, we can investigate the physical and chemical properties of the molecular clouds.

.. TODO: Add figure showing an example of absorption features in QSO spectra.

However, since QSOs are mainly used for calibration purposes, the absorption features are masked out in the standard calibration process of ALMA data like calibration scripts provided by ALMA.
In addition, when you want to analyze a large number of calibration data (conducting a statistical study, for example), it takes a lot of time and effort to download and process the data one by one.

To solve these problems, `almaqso` was developed to automatically download, process and analyze a large number of calibration data observed by ALMA.

.. TODO: How almaqso save your time and effort?

Features
------------------------

- You can specify which sources, bands, cycles to study.
- This package will automatically download the calibration data from ALMA Science Archive.
- It will also automatically process the data using CASA to create FITS files for you.
- Analysis features such as creating spectrum plots are also provided.

License
------------------------

This project is licensed under the MIT License.
Please see `LICENSE <https://github.com/akimasanishida/almaqso/blob/main/LICENSE>`_ for details.

Citation
------------------------

If `almaqso` helps your research, please cite this software.
Please check the latest citation information at `Zenodo <https://zenodo.org/records/18181096>`_.

.. code-block:: bibtex

    @software{nishida_2025_18181096,
    author       = {Nishida, Akimasa and
                    Yoshimura, Yuki and
                    Narita, Kanako},
    title        = {almaqso},
    month        = apr,
    year         = 2025,
    publisher    = {Zenodo},
    version      = {1.5.1},
    doi          = {10.5281/zenodo.18181096},
    url          = {https://doi.org/10.5281/zenodo.18181096},
    }
