<a href='https://github.com/akimasanishida/almaqso' target="_blank"><img alt='GitHub' src='https://img.shields.io/badge/GitHub_Repository-100000?style=flat&logo=GitHub&logoColor=white&labelColor=black&color=FFFFFF'/></a>
[![Static Badge](https://img.shields.io/badge/docs-GitHub%20Pages-blue?logo=GitHub)](https://akimasanishida.github.io/almaqso/)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.14752250.svg)](https://doi.org/10.5281/zenodo.14752250)
[![Check and Test code](https://github.com/akimasanishida/almaqso/actions/workflows/check_and_test.yml/badge.svg)](https://github.com/akimasanishida/almaqso/actions/workflows/check_and_test.yml)

# almaqso

This repository is a folk of [astroysmr/almaqso](https://github.com/astroysmr/almaqso), which is no longer maintained.
Bugs are being removed and some new feature is being implemented.

If you find something or have questions, please refer, report or ask from [issue](https://github.com/akimasanishida/almaqso/issues)

## About

`almaqso` is an automated package for downloading and analyzing ALMA calibration sources (quasars).
Originally developed for analyzing absorption lines, this package addresses a limitation of the standard calibration scripts attached to ALMA archive data, which often mask absorption lines.
By generating calibration scripts that preserve these lines, this package ensures accurate analysis of absorption features.

## Pre-requisites

### CASA

Please use [CASA](https://casa.nrao.edu/) **with ALMA pipeline**.
Version 6 is only supported.
I am using `CASA version 6.6.6-17-pipeline-2025.1.0.35`.

### CASA Modules

Please install [analysisUtilites](https://zenodo.org/records/17252072).
I strongly recommend you to use the **LATEST** version of it.

## Installation

You can install this package by

```shell
pip install almaqso
```

Then you can use the package like this:

```python
import almaqso
```

## Usage

See sample code in `sample` folder and [documentation](https://akimasanishida.github.io/almaqso/usage.html).

**If you find any problems or questions, feel free to report or ask from [issue](https://github.com/akimasanishida/almaqso/issues).**
I would like you to use English or Japanese.

## Citation

Please cite this package (software) if it helps your work!

```
@software{nishida_2026_18334952,
  author       = {Nishida, Akimasa and
                  Ryo, Kishikawa and
                  Yoshimura, Yuki and
                  Narita, Kanako},
  title        = {almaqso},
  month        = jan,
  year         = 2026,
  publisher    = {Zenodo},
  version      = {1.6.0},
  doi          = {10.5281/zenodo.18334952},
  url          = {https://doi.org/10.5281/zenodo.18334952},
  swhid        = {swh:1:dir:ad33568ee7a116c4377820a5ddc87ec3aa0545ce
                   ;origin=https://doi.org/10.5281/zenodo.14752250;vi
                   sit=swh:1:snp:3931013c0f76f5ddc4a12e857819b7522897
                   d68c;anchor=swh:1:rel:2e78a89c6a5fd103e5a5081e988b
                   3e7eb7ecb1b6;path=almaqso-1.6.0
                  },
}
```

## Developer Guide

### Pre-requisites

**You do not have to install all shown below. Please install only what you need.**

- [uv](https://github.com/astral-sh/uv): *Strongly recommended*. uv will manage everything about Python.
- `plantuml` & `graphviz`: Install if you want to build `docs` or re-render the `docs/diagrams`.
- `imagemagick`: Install if you want to build PDF version documentation.

You can reproduce the environment with uv:

```shell
uv sync --dev
```

Then, you can run `main.py` or something with

```shell
uv run main.py  # or something
```

### Format code

```shell
./scripts/code-check.sh
```

### Test code

For unit tests, run

```shell
uv run pytest -m "not integration"
```

For integration tests, run

```shell
uv run pytest -m "integration"
```

Or, you can run all tests with

```shell
uv run pytest
```

### Render diagrams

(Re-)Render all PUML files in `docs/diagrams` with the command below.
SVG files will be generated in the same directory.

```shell
plantuml --svg docs/diagrams/
```

This is run by documentation building scripts below.

### Build documentation

If you change the files in `docs/diagrams`, please recreate the SVG files first as written in **Render diagrams** section.

**HTML:**
```shell
./scripts/sphinx-build-html.sh
```
If you do not use uv, run with python instead:
```shell
python sphinx-build -b html docs docs/_build/html
```
Then, please open `docs/_build/html/index.html` in your browser.

**PDF:**
The script file will build PDF file and copy it to `docs/almaqso.pdf`.

```shell
./scripts/sphinx-build-pdf.sh
```

<!-- When you need to reproduce the `almaqso.rst` file with the change of codes,
```shell
uv run sphinx-apidoc -o docs almaqso
``` -->
