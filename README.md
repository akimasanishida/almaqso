<a href='https://github.com/akimasanishida/almaqso' target="_blank"><img alt='GitHub' src='https://img.shields.io/badge/GitHub_Repository-100000?style=flat&logo=GitHub&logoColor=white&labelColor=black&color=FFFFFF'/></a>
[![Static Badge](https://img.shields.io/badge/docs-GitHub%20Pages-blue?logo=GitHub)](https://akimasanishida.github.io/almaqso/)

# almaqso

This repository is a folk of [astroysmr/almaqso](https://github.com/astroysmr/almaqso), which is no longer maintained.
Bugs are being removed and some new feature is being implemented.

If you find something or have questions, please refer, report or ask from [issue](https://github.com/akimasanishida/almaqso/issues)

## Pre-requisites

### CASA

Please use CASA with ALMA pipeline. I am using `CASA version 6.6.1-17-pipeline-2024.1.0.8`.

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

See sample code in `sample` folder and [documentation](https://akimasanishida.github.io/almaqso/).

## Developer Guide

I recommend you to use [uv](https://github.com/astral-sh/uv) to manage everything.
After installing CASA and analysisUtils shown in Pre-requisites section, please install [uv](https://github.com/astral-sh/uv).

Then, you can reproduce the environment by

```shell
uv sync --dev
```

You can run `main.py` or something with

```shell
uv run main.py  # or something
```

### Build documentation

**HTML:**
```shell
uv run sphinx-build -b html docs docs/_build/html
```
Then, please open `docs/_build/html/index.html` in your browser.

**PDF:**
The script file will build PDF file and copy it to `docs/almaqso.pdf`.
```shell
./scripts/sphinx-build-pdf.sh
```

When you need to reproduce the `almaqso.rst` file with the change of codes,
```shell
uv run sphinx-apidoc -o docs almaqso
```

### Branches

- `main`: The main branch.
- `with-old-codes`: The branch with old codes (created by original editor). This is for the reference.
