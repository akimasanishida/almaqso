<a href='https://github.com/skrbcr/almaqso' target="_blank"><img alt='GitHub' src='https://img.shields.io/badge/GitHub_Repository-100000?style=flat&logo=GitHub&logoColor=white&labelColor=black&color=FFFFFF'/></a>
[![Static Badge](https://img.shields.io/badge/docs-GitHub%20Pages-blue?logo=GitHub)](https://skrbcr.github.io/almaqso/)

# almaqso

This repository is a folk of [astroysmr/almaqso](https://github.com/astroysmr/almaqso), which is no longer maintained.
So many bugs are still there, and I am trying to fix them.

**PLEASE REFER TO THE [ISSUE](https://github.com/skrbcr/almaqso/issues) SECTION SINCE IT CONTAINS THE BUGS AND INFORMATION.**

## Pre-requisites

### CASA

- Please use CASA with ALMA pipeline. I am using `CASA version 6.6.1-17-pipeline-2024.1.0.8`.

### CASA Modules

- analysisUtilities

Details (e.g., how to install and modify) are described in [Pre-requisites](PreRequisites.md).

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

See `sample` folder and [documentation](https://skrbcr.github.io/almaqso/).

## Test

### CASA to use

First, make sure that you have CASA with ALMA pipeline installed e.g., 6.6.1-17-pipeline-2024.1.0.8.

### Prepare the environment

Then please install pipenv.

```shell
pip install pipenv
```

Then, you can reproduce the environment by

```shell
pipenv install --dev
```

You can enter the environment by

```shell
pipenv shell
```

### Test run

Edit `test/test_download_analysis.py`:

```python
# test/test_download_analysis.py
# Edit the following constants.

...

CASA_PATH = 'casa'  # Path to CASA. Please modify this if necessary.

...

DOWNLOAD = False  # True: Download the tar file, False: Use the existing tar file
MODE = 'aftercal'  # 'all': All Steps, 'calonly': Step 1-4, 'aftercal': Step 5-8 of analysis
```

Then you can run the test by

```
pytest
```

### Usage

To work with this package, you have to download JSON files from [ALMA Calibrator Source Catalogue](https://almascience.nao.ac.jp/sc/).
Explanation on it is [here](https://almascience.nao.ac.jp/alma-data/calibrator-catalogue).

Sample code is at `sample/sample.py`.

### Branches

- `main`: The main branch.
- `with-old-codes`: The branch with old codes (created by original editor). This is for the reference.
